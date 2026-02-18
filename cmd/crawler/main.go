package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jazware/bsky-experiments/pkg/crawler"
	"github.com/jazware/bsky-experiments/pkg/indexer/store"
	"github.com/jazware/bsky-experiments/pkg/repoarchive"
	"github.com/jazware/bsky-experiments/telemetry"
	"github.com/jazware/bsky-experiments/version"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:    "crawler",
		Usage:   "ATProto network repo crawler and archive tool",
		Version: version.String(),
		Flags: []cli.Flag{
			telemetry.CLIFlagDebug,
			telemetry.CLIFlagMetricsListenAddress,
			telemetry.CLIFlagServiceName,
			telemetry.CLIFlagTracingSampleRatio,
		},
		Commands: []*cli.Command{
			prepareCommand(),
			crawlCommand(),
			replayCommand(),
			inspectCommand(),
			tallyCommand(),
			statusCommand(),
			resetCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func commonFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "redis-address",
			Usage:   "Redis address",
			Value:   "localhost:6379",
			EnvVars: []string{"REDIS_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "clickhouse-address",
			Usage:   "ClickHouse address",
			Value:   "localhost:9000",
			EnvVars: []string{"CLICKHOUSE_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "clickhouse-username",
			Usage:   "ClickHouse username",
			Value:   "default",
			EnvVars: []string{"CLICKHOUSE_USERNAME"},
		},
		&cli.StringFlag{
			Name:    "clickhouse-password",
			Usage:   "ClickHouse password",
			Value:   "",
			EnvVars: []string{"CLICKHOUSE_PASSWORD"},
		},
	}
}

func setupRedis(ctx context.Context, cctx *cli.Context) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: cctx.String("redis-address"),
	})
	if err := redisotel.InstrumentTracing(client); err != nil {
		return nil, fmt.Errorf("redis tracing: %w", err)
	}
	if err := redisotel.InstrumentMetrics(client); err != nil {
		return nil, fmt.Errorf("redis metrics: %w", err)
	}
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return client, nil
}

func setupClickHouse(cctx *cli.Context) (driver.Conn, error) {
	s, err := store.NewStore(
		cctx.String("clickhouse-address"),
		cctx.String("clickhouse-username"),
		cctx.String("clickhouse-password"),
	)
	if err != nil {
		return nil, err
	}
	return s.DB, nil
}

// --- prepare command ---

func prepareCommand() *cli.Command {
	flags := append(commonFlags(),
		&cli.StringFlag{
			Name:    "relay-host",
			Usage:   "Relay host URL for PDS discovery",
			Value:   "https://bsky.network",
			EnvVars: []string{"RELAY_HOST"},
		},
		&cli.IntFlag{
			Name:    "discovery-workers",
			Usage:   "Concurrent PDS enumeration workers",
			Value:   50,
			EnvVars: []string{"DISCOVERY_WORKERS"},
		},
		&cli.Float64Flag{
			Name:    "discovery-rps",
			Usage:   "Per-PDS rate limit for listRepos",
			Value:   5,
			EnvVars: []string{"DISCOVERY_RPS"},
		},
		&cli.Int64Flag{
			Name:    "list-repos-limit",
			Usage:   "Page size for listRepos calls",
			Value:   1000,
			EnvVars: []string{"LIST_REPOS_LIMIT"},
		},
		&cli.StringSliceFlag{
			Name:    "skip-pds",
			Usage:   "PDS URLs to skip (can be repeated)",
			EnvVars: []string{"SKIP_PDS"},
		},
		&cli.BoolFlag{
			Name:    "verify",
			Usage:   "Verify PDS claims against DID documents",
			Value:   true,
			EnvVars: []string{"VERIFY_PDS"},
		},
	)

	return &cli.Command{
		Name:  "prepare",
		Usage: "Build a verified crawl list from relay-discovered PDSs",
		Flags: flags,
		Action: func(cctx *cli.Context) error {
			return runPrepare(cctx)
		},
	}
}

func runPrepare(cctx *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	logger := telemetry.StartLogger(cctx)
	telemetry.StartMetrics(cctx)

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		shutdown, err := telemetry.StartTracing(cctx)
		if err != nil {
			return fmt.Errorf("starting tracing: %w", err)
		}
		defer shutdown(ctx)
	}

	chConn, err := setupClickHouse(cctx)
	if err != nil {
		return fmt.Errorf("clickhouse setup: %w", err)
	}

	config := crawler.PrepareConfig{
		DiscoveryConfig: crawler.DiscoveryConfig{
			RelayHost:      cctx.String("relay-host"),
			Workers:        cctx.Int("discovery-workers"),
			DiscoveryRPS:   cctx.Float64("discovery-rps"),
			ListReposLimit: cctx.Int64("list-repos-limit"),
			SkipPDS:        cctx.StringSlice("skip-pds"),
			Logger:         logger,
		},
		ClickHouseConn: chConn,
		VerifyPDS:      cctx.Bool("verify"),
		Logger:         logger,
	}

	prepareDone := make(chan error, 1)
	go func() {
		prepareDone <- crawler.RunPrepare(ctx, config)
	}()

	select {
	case err := <-prepareDone:
		return err
	case <-signals:
		logger.Info("shutting down on signal...")
		cancel()
		return <-prepareDone
	}
}

// --- crawl command ---

func crawlCommand() *cli.Command {
	flags := append(commonFlags(),
		&cli.StringFlag{
			Name:    "output-dir",
			Usage:   "Directory to write .rca segment files",
			Value:   "./data/rca",
			EnvVars: []string{"OUTPUT_DIR"},
		},
		&cli.IntFlag{
			Name:    "workers",
			Usage:   "Number of concurrent crawl workers",
			Value:   50,
			EnvVars: []string{"WORKERS"},
		},
		&cli.Float64Flag{
			Name:    "default-pds-rps",
			Usage:   "Default requests per second per PDS",
			Value:   5,
			EnvVars: []string{"DEFAULT_PDS_RPS"},
		},
		&cli.StringFlag{
			Name:    "segment-size",
			Usage:   "Target segment file size (e.g. 2GB, 500MB)",
			Value:   "2GB",
			EnvVars: []string{"SEGMENT_SIZE"},
		},
		&cli.IntFlag{
			Name:    "zstd-level",
			Usage:   "Zstd compression level (1-19)",
			Value:   3,
			EnvVars: []string{"ZSTD_LEVEL"},
		},
		&cli.IntFlag{
			Name:    "page-size",
			Usage:   "Number of DIDs to fetch per page from ClickHouse",
			Value:   50000,
			EnvVars: []string{"PAGE_SIZE"},
		},
		&cli.StringSliceFlag{
			Name:    "skip-pds",
			Usage:   "PDS URLs to skip (can be repeated)",
			EnvVars: []string{"SKIP_PDS"},
		},
		&cli.BoolFlag{
			Name:  "resume",
			Usage: "Resume an interrupted crawl",
		},
	)

	return &cli.Command{
		Name:  "crawl",
		Usage: "Crawl all ATProto repos from the network",
		Flags: flags,
		Action: func(cctx *cli.Context) error {
			return runCrawl(cctx)
		},
	}
}

func runCrawl(cctx *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	logger := telemetry.StartLogger(cctx)
	telemetry.StartMetrics(cctx)

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		shutdown, err := telemetry.StartTracing(cctx)
		if err != nil {
			return fmt.Errorf("starting tracing: %w", err)
		}
		defer shutdown(ctx)
	}

	redisClient, err := setupRedis(ctx, cctx)
	if err != nil {
		return fmt.Errorf("redis setup: %w", err)
	}

	chConn, err := setupClickHouse(cctx)
	if err != nil {
		return fmt.Errorf("clickhouse setup: %w", err)
	}

	segmentSize, err := parseSize(cctx.String("segment-size"))
	if err != nil {
		return fmt.Errorf("invalid segment-size: %w", err)
	}

	c, err := crawler.NewCrawler(crawler.Config{
		OutputDir:      cctx.String("output-dir"),
		Workers:        cctx.Int("workers"),
		DefaultPDSRPS:  cctx.Float64("default-pds-rps"),
		SegmentSize:    segmentSize,
		ZstdLevel:      cctx.Int("zstd-level"),
		PageSize:       cctx.Int("page-size"),
		SkipPDS:        cctx.StringSlice("skip-pds"),
		ClickHouseConn: chConn,
		RedisClient:    redisClient,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("creating crawler: %w", err)
	}

	// Run crawl in background, handle signals.
	crawlDone := make(chan error, 1)
	go func() {
		crawlDone <- c.Run(ctx)
	}()

	select {
	case err := <-crawlDone:
		return err
	case <-signals:
		logger.Info("shutting down on signal...")
		cancel()
		// Force shutdown after 30s.
		go func() {
			<-time.After(30 * time.Second)
			log.Fatal("failed to shut down in time, forcing exit")
		}()
		c.Shutdown()
		return <-crawlDone
	}
}

// --- replay command ---

func replayCommand() *cli.Command {
	flags := append(commonFlags(),
		&cli.StringFlag{
			Name:    "input-dir",
			Usage:   "Directory containing .rca segment files",
			Value:   "./data/rca",
			EnvVars: []string{"INPUT_DIR"},
		},
		&cli.StringSliceFlag{
			Name:    "collections",
			Usage:   "Collections to replay (comma-separated). Empty means all.",
			EnvVars: []string{"COLLECTIONS"},
		},
		&cli.IntFlag{
			Name:    "workers",
			Usage:   "Number of concurrent segment replay workers",
			Value:   4,
			EnvVars: []string{"REPLAY_WORKERS"},
		},
		&cli.BoolFlag{
			Name:  "truncate",
			Usage: "Truncate crawl_records before replaying",
		},
	)

	return &cli.Command{
		Name:  "replay",
		Usage: "Replay .rca archives into ClickHouse crawl_records table",
		Flags: flags,
		Action: func(cctx *cli.Context) error {
			return runReplay(cctx)
		},
	}
}

func setupReplayClickHouse(cctx *cli.Context) (driver.Conn, error) {
	opts := &clickhouse.Options{
		Addr: []string{cctx.String("clickhouse-address")},
		Settings: clickhouse.Settings{
			"insert_deduplicate": 0, // skip block-level dedup, ReplacingMergeTree handles it
			"optimize_on_insert": 0, // don't merge inline, let background merges handle it
			"max_insert_threads": 4, // parallel block processing server-side
			"insert_quorum":      0, // no quorum waiting (single-node)
		},
	}
	if username := cctx.String("clickhouse-username"); username != "" {
		opts.Auth = clickhouse.Auth{
			Username: username,
			Password: cctx.String("clickhouse-password"),
		}
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse: %w", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return conn, nil
}

// prepareTableForBulkLoad stops merges and drops secondary indexes on crawl_records
// to maximize insert throughput during replay.
func prepareTableForBulkLoad(ctx context.Context, conn driver.Conn, truncate bool, logger *slog.Logger) error {
	stmts := []string{
		"SYSTEM STOP MERGES crawl_records",
		"ALTER TABLE crawl_records MODIFY SETTING parts_to_delay_insert = 100000, parts_to_throw_insert = 100000",
	}
	if truncate {
		stmts = append(stmts, "TRUNCATE TABLE crawl_records SETTINGS max_table_size_to_drop = 0")
	}
	stmts = append(stmts,
		"ALTER TABLE crawl_records MODIFY COLUMN IF EXISTS record_json String CODEC(LZ4)",
		"ALTER TABLE crawl_records DROP INDEX IF EXISTS idx_repo",
		"ALTER TABLE crawl_records DROP INDEX IF EXISTS idx_collection",
		"ALTER TABLE crawl_records DROP INDEX IF EXISTS idx_crawled_at",
	)
	for _, stmt := range stmts {
		logger.Info("bulk load prep", "stmt", stmt)
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	return nil
}

// finalizeTableAfterBulkLoad re-adds secondary indexes and resumes merges.
func finalizeTableAfterBulkLoad(ctx context.Context, conn driver.Conn, logger *slog.Logger) error {
	stmts := []string{
		"ALTER TABLE crawl_records MODIFY COLUMN IF EXISTS record_json String CODEC(ZSTD(3))",
		"SYSTEM START MERGES crawl_records",
		"OPTIMIZE TABLE crawl_records FINAL",
		"ALTER TABLE crawl_records ADD INDEX IF NOT EXISTS idx_repo repo TYPE bloom_filter(0.01) GRANULARITY 1",
		"ALTER TABLE crawl_records ADD INDEX IF NOT EXISTS idx_collection collection TYPE bloom_filter(0.01) GRANULARITY 1",
		"ALTER TABLE crawl_records ADD INDEX IF NOT EXISTS idx_crawled_at crawled_at TYPE minmax GRANULARITY 1",
		"ALTER TABLE crawl_records MODIFY SETTING parts_to_delay_insert = 150, parts_to_throw_insert = 300",
	}
	for _, stmt := range stmts {
		logger.Info("bulk load finalize", "stmt", stmt)
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	return nil
}

func runReplay(cctx *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-signals
		cancel()
	}()

	logger := telemetry.StartLogger(cctx)

	chConn, err := setupReplayClickHouse(cctx)
	if err != nil {
		return fmt.Errorf("clickhouse setup: %w", err)
	}

	inputDir := cctx.String("input-dir")
	collections := cctx.StringSlice("collections")
	workers := cctx.Int("workers")

	// Find all .rca files.
	segments, err := filepath.Glob(filepath.Join(inputDir, "*.rca"))
	if err != nil {
		return fmt.Errorf("listing segments: %w", err)
	}
	sort.Strings(segments)

	if len(segments) == 0 {
		return fmt.Errorf("no .rca files found in %s", inputDir)
	}

	// Prepare table for bulk loading.
	if err := prepareTableForBulkLoad(ctx, chConn, cctx.Bool("truncate"), logger); err != nil {
		return fmt.Errorf("preparing table: %w", err)
	}
	// Always restore indexes and merges, even on error/signal.
	defer func() {
		logger.Info("finalizing table after bulk load...")
		bgCtx := context.Background()
		if err := finalizeTableAfterBulkLoad(bgCtx, chConn, logger); err != nil {
			logger.Error("failed to finalize table", "error", err)
		}
	}()

	logger.Info("replaying segments", "count", len(segments), "workers", workers, "collections", collections)

	// Fan out segments to workers.
	segCh := make(chan string)
	var totalRecords atomic.Int64
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for segPath := range segCh {
				logger.Info("replaying segment", "path", segPath)
				count, err := replaySegment(ctx, chConn, segPath, collections, logger)
				if err != nil {
					logger.Error("failed to replay segment", "path", segPath, "error", err)
					continue
				}
				totalRecords.Add(int64(count))
				logger.Info("segment replayed", "path", segPath, "records", count)
			}
		}()
	}

	for _, segPath := range segments {
		if ctx.Err() != nil {
			break
		}
		segCh <- segPath
	}
	close(segCh)
	wg.Wait()

	logger.Info("replay complete", "total_records", totalRecords.Load())
	return nil
}

type replayBatch struct {
	repos       []string
	collections []string
	rkeys       []string
	recordJSONs []string
	crawledAts  []time.Time
	timeUSs     []int64
}

func sendReplayBatch(ctx context.Context, db driver.Conn, b replayBatch) error {
	if len(b.repos) == 0 {
		return nil
	}
	batchInsert, err := db.PrepareBatch(ctx,
		"INSERT INTO crawl_records (repo, collection, rkey, record_json, crawled_at, time_us)")
	if err != nil {
		return fmt.Errorf("preparing batch: %w", err)
	}
	for i := range b.repos {
		if err := batchInsert.Append(
			b.repos[i],
			b.collections[i],
			b.rkeys[i],
			b.recordJSONs[i],
			b.crawledAts[i],
			b.timeUSs[i],
		); err != nil {
			return fmt.Errorf("appending to batch: %w", err)
		}
	}
	if err := batchInsert.Send(); err != nil {
		return fmt.Errorf("sending batch: %w", err)
	}
	return nil
}

func replaySegment(ctx context.Context, db driver.Conn, segPath string, collections []string, logger *slog.Logger) (int, error) {
	reader, err := repoarchive.OpenSegment(segPath)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	if len(collections) > 0 {
		reader.SetCollectionFilter(collections...)
	}

	const batchSize = 500_000

	// Pipeline: reader fills batches, sender goroutine ships them to ClickHouse.
	// Buffer of 1 lets the reader prepare the next batch while the current one sends.
	sendCh := make(chan replayBatch, 1)
	var sendErr error
	var sent atomic.Int64
	sendDone := make(chan struct{})

	go func() {
		defer close(sendDone)
		for b := range sendCh {
			if err := sendReplayBatch(ctx, db, b); err != nil {
				sendErr = err
				for range sendCh {
				}
				return
			}
			sent.Add(int64(len(b.repos)))
		}
	}()

	current := replayBatch{}
	for reader.Next() {
		if ctx.Err() != nil {
			break
		}
		// Check for sender errors between repos.
		select {
		case <-sendDone:
			return int(sent.Load()), sendErr
		default:
		}

		repo := reader.Repo()
		for repo.NextCollection() {
			col := repo.Collection()
			for col.NextRecord() {
				rec, err := col.Record()
				if err != nil {
					logger.Warn("skipping record", "did", repo.DID, "collection", col.Name, "error", err)
					continue
				}
				current.repos = append(current.repos, repo.DID)
				current.collections = append(current.collections, col.Name)
				current.rkeys = append(current.rkeys, rec.RKey)
				current.recordJSONs = append(current.recordJSONs, string(rec.JSON))
				current.crawledAts = append(current.crawledAts, repo.CrawledAt)
				current.timeUSs = append(current.timeUSs, repo.CrawledAt.UnixMicro())

				if len(current.repos) >= batchSize {
					sendCh <- current
					current = replayBatch{}
				}
			}
		}
	}

	// Flush remaining records.
	if len(current.repos) > 0 {
		sendCh <- current
	}
	close(sendCh)
	<-sendDone

	return int(sent.Load()), sendErr
}

// --- inspect command ---

func inspectCommand() *cli.Command {
	return &cli.Command{
		Name:      "inspect",
		Usage:     "Inspect an .rca segment file",
		ArgsUsage: "<segment-file>",
		Action: func(cctx *cli.Context) error {
			return runInspect(cctx)
		},
	}
}

func runInspect(cctx *cli.Context) error {
	path := cctx.Args().First()
	if path == "" {
		return fmt.Errorf("segment file path required")
	}

	reader, err := repoarchive.OpenSegment(path)
	if err != nil {
		return fmt.Errorf("opening segment: %w", err)
	}
	defer reader.Close()

	header := reader.Header()
	fmt.Printf("Segment: %s\n", path)
	fmt.Printf("  Version:     %d\n", header.Version)
	fmt.Printf("  Created:     %s\n", time.UnixMicro(header.CreatedAt).Format(time.RFC3339))
	fmt.Printf("  Repo Count:  %d\n", header.RepoCount)
	fmt.Printf("  Index Offset: %d\n", header.IndexOffset)
	fmt.Println()

	// Load index for summary.
	index, err := repoarchive.LoadIndex(path)
	if err != nil {
		fmt.Printf("  (index not readable: %v)\n", err)
	} else {
		fmt.Printf("  Index Entries: %d\n", len(index.Entries))
	}

	// Scan repos and summarize collections.
	collectionStats := make(map[string]struct {
		repos   int
		records int
	})
	repoCount := 0

	for reader.Next() {
		repo := reader.Repo()
		repoCount++
		for repo.NextCollection() {
			col := repo.Collection()
			stats := collectionStats[col.Name]
			stats.repos++
			recordCount := 0
			for col.NextRecord() {
				if _, err := col.Record(); err == nil {
					recordCount++
				}
			}
			stats.records += recordCount
			collectionStats[col.Name] = stats
		}
	}

	fmt.Printf("\n  Repos scanned: %d\n", repoCount)
	fmt.Println("  Collections:")

	// Sort collection names for display.
	names := make([]string, 0, len(collectionStats))
	for name := range collectionStats {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		stats := collectionStats[name]
		fmt.Printf("    %-50s  repos: %6d  records: %8d\n", name, stats.repos, stats.records)
	}

	return nil
}

// --- tally command ---

func tallyCommand() *cli.Command {
	return &cli.Command{
		Name:  "tally",
		Usage: "Count total records across all .rca segment files (fast, no decompression)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "input-dir",
				Usage:   "Directory containing .rca segment files",
				Value:   "./data/rca",
				EnvVars: []string{"INPUT_DIR"},
			},
			&cli.BoolFlag{
				Name:  "by-collection",
				Usage: "Show breakdown by collection",
			},
			&cli.IntFlag{
				Name:  "workers",
				Usage: "Parallel segment readers (keep at 1 for HDD, increase for SSD/NVMe)",
				Value: 1,
			},
		},
		Action: func(cctx *cli.Context) error {
			return runTally(cctx)
		},
	}
}

type segmentTally struct {
	path        string
	repos       int
	records     int64
	dids        []string
	collections map[string]int64 // only populated with --by-collection
	err         error
}

func runTally(cctx *cli.Context) error {
	inputDir := cctx.String("input-dir")
	byCollection := cctx.Bool("by-collection")
	workers := cctx.Int("workers")

	segments, err := filepath.Glob(filepath.Join(inputDir, "*.rca"))
	if err != nil {
		return fmt.Errorf("listing segments: %w", err)
	}
	sort.Strings(segments)

	if len(segments) == 0 {
		return fmt.Errorf("no .rca files found in %s", inputDir)
	}

	total := len(segments)
	fmt.Printf("Scanning %d segments in %s ...\n\n", total, inputDir)

	// Fan out segment reads to workers.
	segCh := make(chan string)
	resultCh := make(chan segmentTally, total)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for segPath := range segCh {
				resultCh <- tallySegment(segPath, byCollection)
			}
		}()
	}

	go func() {
		for _, seg := range segments {
			segCh <- seg
		}
		close(segCh)
	}()

	// Consume results as they arrive, printing progress.
	results := make(map[string]segmentTally, total)
	var runningRecords int64
	for range total {
		r := <-resultCh
		results[r.path] = r
		runningRecords += r.records
		fmt.Fprintf(os.Stderr, "\r  [%d/%d] segments scanned  %s records so far",
			len(results), total, formatCount(runningRecords))
	}
	fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 72)) // clear progress line

	wg.Wait()
	close(resultCh)

	var totalRepos int
	var totalRecords int64
	allDIDs := make(map[string]int) // DID -> number of segments it appears in
	globalCollections := make(map[string]int64)

	for _, segPath := range segments {
		r := results[segPath]
		if r.err != nil {
			fmt.Printf("  %-40s  ERROR: %v\n", filepath.Base(segPath), r.err)
			continue
		}
		fmt.Printf("  %-40s  repos: %8d  records: %12s\n",
			filepath.Base(segPath), r.repos, formatCount(r.records))
		totalRepos += r.repos
		totalRecords += r.records
		for _, did := range r.dids {
			allDIDs[did]++
		}
		for col, count := range r.collections {
			globalCollections[col] += count
		}
	}

	// Compute DID duplication stats.
	uniqueDIDs := len(allDIDs)
	var duplicatedDIDs int
	var duplicateRepoInstances int
	for _, count := range allDIDs {
		if count > 1 {
			duplicatedDIDs++
			duplicateRepoInstances += count - 1 // extra appearances beyond the first
		}
	}

	fmt.Println()
	fmt.Println("Totals:")
	fmt.Printf("  Segments:            %d\n", len(segments))
	fmt.Printf("  Repo instances:      %s\n", formatCount(int64(totalRepos)))
	fmt.Printf("  Total records:       %s\n", formatCount(totalRecords))
	fmt.Printf("  Unique DIDs:         %s\n", formatCount(int64(uniqueDIDs)))
	fmt.Printf("  Duplicated DIDs:     %s  (appear in 2+ segments)\n", formatCount(int64(duplicatedDIDs)))
	fmt.Printf("  Duplicate instances: %s  (extra repo copies from re-crawls)\n", formatCount(int64(duplicateRepoInstances)))

	if duplicatedDIDs > 0 {
		fmt.Println()
		fmt.Println("Note: Duplicated DIDs will be deduplicated by ReplacingMergeTree during replay.")
		fmt.Println("The actual row count after OPTIMIZE TABLE FINAL will be less than the total above.")
	}

	if byCollection && len(globalCollections) > 0 {
		fmt.Println()
		fmt.Println("Records by collection:")
		names := make([]string, 0, len(globalCollections))
		for name := range globalCollections {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Printf("  %-50s  %12s\n", name, formatCount(globalCollections[name]))
		}
	}

	return nil
}

func tallySegment(segPath string, byCollection bool) segmentTally {
	result := segmentTally{path: segPath}

	reader, err := repoarchive.OpenSegment(segPath)
	if err != nil {
		result.err = err
		return result
	}
	defer reader.Close()

	collections := make(map[string]int64)

	for reader.Next() {
		repo := reader.Repo()
		result.repos++
		result.dids = append(result.dids, repo.DID)

		for _, entry := range repo.TOC() {
			result.records += int64(entry.RecordCount)
			if byCollection {
				collections[entry.Name] += int64(entry.RecordCount)
			}
		}
	}

	if byCollection {
		result.collections = collections
	}

	return result
}

func formatCount(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	// Insert commas from the right.
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

// --- status command ---

func statusCommand() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "Show crawl progress from Redis",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "redis-address",
				Usage:   "Redis address",
				Value:   "localhost:6379",
				EnvVars: []string{"REDIS_ADDRESS"},
			},
		},
		Action: func(cctx *cli.Context) error {
			return runStatus(cctx)
		},
	}
}

func runStatus(cctx *cli.Context) error {
	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{
		Addr: cctx.String("redis-address"),
	})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	progress := crawler.NewProgress(redisClient, logger)

	stats, err := progress.GetStats(ctx)
	if err != nil {
		return fmt.Errorf("getting stats: %w", err)
	}

	if len(stats) == 0 {
		fmt.Println("No crawl progress found.")
		return nil
	}

	fmt.Println("Crawl Progress:")
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-25s %s\n", k+":", stats[k])
	}

	return nil
}

// --- reset command ---

func resetCommand() *cli.Command {
	return &cli.Command{
		Name:  "reset",
		Usage: "Clear output data, Redis cursors, and all crawl progress",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "redis-address",
				Usage:   "Redis address",
				Value:   "localhost:6379",
				EnvVars: []string{"REDIS_ADDRESS"},
			},
			&cli.StringFlag{
				Name:    "output-dir",
				Usage:   "Directory containing .rca segment files",
				EnvVars: []string{"OUTPUT_DIR"},
			},
		},
		Action: func(cctx *cli.Context) error {
			return runReset(cctx)
		},
	}
}

func runReset(cctx *cli.Context) error {
	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{
		Addr: cctx.String("redis-address"),
	})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	progress := crawler.NewProgress(redisClient, logger)

	// Clear Redis keys.
	deleted, err := progress.ClearAll(ctx)
	if err != nil {
		return fmt.Errorf("clearing Redis keys: %w", err)
	}
	fmt.Printf("Cleared %d Redis keys\n", deleted)

	// Remove .rca files from output directory.
	outputDir := cctx.String("output-dir")
	if outputDir == "" {
		fmt.Println("No output directory specified, skipping .rca file removal")
		return nil
	}
	
	segments, err := filepath.Glob(filepath.Join(outputDir, "*.rca"))
	if err != nil {
		return fmt.Errorf("listing segments: %w", err)
	}
	for _, seg := range segments {
		if err := os.Remove(seg); err != nil {
			fmt.Printf("  failed to remove %s: %v\n", seg, err)
		} else {
			fmt.Printf("  removed %s\n", seg)
		}
	}
	if len(segments) == 0 {
		fmt.Printf("No .rca files found in %s\n", outputDir)
	} else {
		fmt.Printf("Removed %d .rca files\n", len(segments))
	}

	return nil
}

// --- helpers ---

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	multiplier := int64(1)
	switch {
	case strings.HasSuffix(s, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "TB")
	case strings.HasSuffix(s, "GB"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	}
	var val float64
	if _, err := fmt.Sscanf(s, "%f", &val); err != nil {
		return 0, fmt.Errorf("invalid size: %s", s)
	}
	return int64(val * float64(multiplier)), nil
}
