package crawler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jazware/bsky-experiments/pkg/repoarchive"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

var tracer = otel.Tracer("crawler")

// Config holds all configuration for a crawl run.
type Config struct {
	OutputDir     string
	Workers       int
	DefaultPDSRPS float64
	SegmentSize   int64
	ZstdLevel     int
	MaxRetries    int
	MaxRepoSizeMB int
	PageSize      int
	SkipPDS       []string

	ClickHouseConn driver.Conn
	RedisClient    *redis.Client
	Logger         *slog.Logger
}

// Crawler orchestrates the full network crawl.
type Crawler struct {
	config     Config
	progress   *Progress
	dispatcher *Dispatcher
	writer     *repoarchive.SegmentWriter
	logger     *slog.Logger

	repoQueue chan *repoarchive.CrawledRepo
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewCrawler creates a new Crawler. Call Run() to start.
func NewCrawler(config Config) (*Crawler, error) {
	if config.Workers <= 0 {
		config.Workers = 50
	}
	if config.DefaultPDSRPS <= 0 {
		config.DefaultPDSRPS = 5
	}
	if config.SegmentSize <= 0 {
		config.SegmentSize = repoarchive.DefaultSegmentSize
	}
	if config.ZstdLevel <= 0 {
		config.ZstdLevel = repoarchive.DefaultZstdLevel
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	if config.MaxRepoSizeMB <= 0 {
		config.MaxRepoSizeMB = 100
	}
	if config.PageSize <= 0 {
		config.PageSize = 50000
	}

	progress := NewProgress(config.RedisClient, config.Logger)

	return &Crawler{
		config:    config,
		progress:  progress,
		repoQueue: make(chan *repoarchive.CrawledRepo, 100),
		logger:    config.Logger,
	}, nil
}

// Run starts the crawl. It blocks until the crawl is complete or the context is cancelled.
func (c *Crawler) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	defer cancel()

	ctx, runSpan := tracer.Start(ctx, "crawler.run")
	defer runSpan.End()

	// Get current segment number for resume.
	segNum, err := c.progress.GetSegmentNum(ctx)
	if err != nil {
		segNum = 1
	}

	// Open segment writer.
	writer, err := repoarchive.NewSegmentWriter(
		c.config.OutputDir,
		repoarchive.WithSegmentSize(c.config.SegmentSize),
		repoarchive.WithZstdLevel(c.config.ZstdLevel),
		repoarchive.WithStartSegment(segNum),
	)
	if err != nil {
		return fmt.Errorf("creating segment writer: %w", err)
	}
	c.writer = writer

	// Start segment writer goroutine.
	c.wg.Add(1)
	go c.writerLoop(ctx)

	// Start dispatcher and workers.
	c.dispatcher = NewDispatcher(c.config.DefaultPDSRPS, c.logger)
	workerPool := NewWorkerPool(c.config.Workers, c.repoQueue, c.progress, c.dispatcher, c.config, c.logger)
	dispatchErr := make(chan error, 1)
	go func() {
		dispatchErr <- c.dispatcher.Run(ctx, workerPool.WorkQueue())
	}()

	// Load cursor from Redis for resume.
	cursor, err := c.progress.GetCursor(ctx)
	if err != nil {
		c.logger.Warn("failed to load cursor, starting from beginning", "error", err)
	}
	if cursor != "" {
		c.logger.Info("resuming crawl from cursor", "cursor", cursor)
	}

	var totalPages int

	// Page loop: continuously fetch pages and feed to the dispatcher.
	// The dispatcher runs in its own goroutine and processes work concurrently.
	for {
		if ctx.Err() != nil {
			break
		}

		// Backpressure: wait until the dispatcher has drained enough before
		// fetching the next page to avoid unbounded memory growth.
		// Use 10x page size so we always have work from many PDSs even
		// when a few large ones drain slowly.
		for c.dispatcher.Remaining() > int64(c.config.PageSize)*10 {
			// When PDS diversity is low, fetch more pages even if remaining is high.
			// Cap at 50 pages worth (~2.5M DIDs, ~175MB) to prevent OOM.
			if c.dispatcher.ActivePDSCount() < 50 &&
				c.dispatcher.Remaining() < int64(c.config.PageSize)*50 {
				break
			}
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
			}
			if ctx.Err() != nil {
				break
			}
		}
		if ctx.Err() != nil {
			break
		}

		pdsGroups, nextCursor, pageTotal, err := c.fetchPage(ctx, cursor)
		if err != nil {
			c.logger.Error("failed to fetch page", "error", err)
			break
		}
		totalPages++
		pagesFetchedTotal.Inc()

		c.logger.Info("fetched page from ClickHouse",
			"cursor", cursor, "dids", pageTotal, "pds_count", len(pdsGroups))

		// Filter already-completed DIDs.
		filteredCount, err := c.progress.FilterPage(ctx, pdsGroups)
		if err != nil {
			c.logger.Warn("failed to filter page, proceeding without filtering", "error", err)
		} else if filteredCount > 0 {
			c.logger.Info("filtered completed DIDs from page", "skipped", filteredCount)
		}

		// Count remaining DIDs after filtering.
		remaining := 0
		for _, dids := range pdsGroups {
			remaining += len(dids)
		}
		pageDIDs.Observe(float64(remaining))

		if remaining > 0 {
			c.logger.Info("feeding page to dispatcher", "remaining", remaining, "pds_count", len(pdsGroups))
			c.dispatcher.Feed(pdsGroups)
		}

		// Advance cursor.
		if nextCursor != "" {
			cursor = nextCursor
			c.progress.SetCursor(ctx, cursor)
		}

		// End of data: last page was smaller than page size.
		if pageTotal < c.config.PageSize {
			c.logger.Info("reached end of DID data", "pages", totalPages)
			break
		}
	}

	// Wait for the dispatcher to drain all remaining work.
	c.logger.Info("waiting for dispatcher to drain", "remaining", c.dispatcher.Remaining())
	for c.dispatcher.Remaining() > 0 {
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}

	// Stop dispatcher and wait for it to return.
	c.dispatcher.Stop()
	<-dispatchErr

	// Signal workers to drain.
	workerPool.Stop()

	// Close repoQueue to signal writer to finish.
	close(c.repoQueue)
	c.wg.Wait()

	// Finalize segment.
	if err := c.writer.Close(); err != nil {
		return fmt.Errorf("closing segment writer: %w", err)
	}

	// Save final state and clear cursor on successful completion.
	c.progress.SetSegmentNum(ctx, c.writer.SegmentNum())
	c.progress.SaveStats(ctx, c.writer.TotalRepos(), c.writer.BytesWritten())
	c.progress.ClearCursor(ctx)

	runSpan.SetAttributes(
		attribute.Int64("crawl.bytes_written", c.writer.BytesWritten()),
		attribute.Int("crawl.segments", c.writer.SegmentNum()),
		attribute.Int("crawl.pages", totalPages),
	)

	c.logger.Info("crawl complete",
		"total_repos", c.writer.TotalRepos(),
		"bytes_written", c.writer.BytesWritten(),
		"segments", c.writer.SegmentNum(),
		"pages", totalPages)

	return nil
}

// Shutdown gracefully stops the crawl.
func (c *Crawler) Shutdown() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// writerLoop receives crawled repos and writes them to segments.
func (c *Crawler) writerLoop(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	lastSegment := c.writer.SegmentNum()
	lastBytes := c.writer.BytesWritten()

	for {
		select {
		case repo, ok := <-c.repoQueue:
			if !ok {
				return
			}

			if err := c.writer.WriteRepo(repo); err != nil {
				c.logger.Error("failed to write repo", "did", repo.DID, "error", err)
				continue
			}

			// Track compressed bytes per repo via delta.
			currentBytes := c.writer.BytesWritten()
			repoBytes := currentBytes - lastBytes
			lastBytes = currentBytes
			bytesWrittenTotal.Add(float64(repoBytes))
			repoCompressedBytes.Observe(float64(repoBytes))

			// Detect segment finalization.
			currentSegment := c.writer.SegmentNum()
			if currentSegment != lastSegment {
				segmentsFinalizedTotal.Inc()
				lastSegment = currentSegment
			}

			c.progress.SetSegmentNum(ctx, currentSegment)
		case <-ticker.C:
			c.progress.SaveStats(ctx, c.writer.TotalRepos(), c.writer.BytesWritten())
		case <-ctx.Done():
			// Drain remaining repos in the channel.
			for repo := range c.repoQueue {
				if err := c.writer.WriteRepo(repo); err != nil {
					c.logger.Error("failed to write repo during drain", "did", repo.DID, "error", err)
				}
			}
			return
		}
	}
}

// fetchPage queries ClickHouse for one page of DIDs using cursor-based pagination.
// Returns pdsGroups, the last DID seen (next cursor), and total DIDs in the page.
func (c *Crawler) fetchPage(ctx context.Context, cursor string) (map[string][]string, string, int, error) {
	ctx, span := tracer.Start(ctx, "crawler.fetchPage")
	defer span.End()

	span.SetAttributes(
		attribute.String("cursor", cursor),
		attribute.Int("page.size", c.config.PageSize),
	)

	query := "SELECT did, pds FROM plc_did_state FINAL WHERE pds != '' AND did > ?"
	args := []any{cursor}
	if len(c.config.SkipPDS) > 0 {
		query += " AND pds NOT IN (?)"
		args = append(args, c.config.SkipPDS)
	}
	query += " ORDER BY did LIMIT ?"
	args = append(args, c.config.PageSize)

	rows, err := c.config.ClickHouseConn.Query(ctx, query, args...)
	if err != nil {
		return nil, "", 0, fmt.Errorf("querying plc_did_state: %w", err)
	}
	defer rows.Close()

	pdsGroups := make(map[string][]string)
	total := 0
	var lastDID string
	for rows.Next() {
		var did, pds string
		if err := rows.Scan(&did, &pds); err != nil {
			return nil, "", 0, fmt.Errorf("scanning row: %w", err)
		}
		pdsGroups[pds] = append(pdsGroups[pds], did)
		lastDID = did
		total++
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, fmt.Errorf("row iteration: %w", err)
	}

	span.SetAttributes(
		attribute.Int("pds.count", len(pdsGroups)),
		attribute.String("page.last_did", lastDID),
	)

	return pdsGroups, lastDID, total, nil
}
