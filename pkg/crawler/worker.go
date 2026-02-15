package crawler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jazware/bsky-experiments/pkg/crawler/carrepo"
	"github.com/jazware/bsky-experiments/pkg/repoarchive"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// WorkerPool manages a pool of crawl workers.
type WorkerPool struct {
	workQueue  chan *CrawlTask
	repoQueue  chan *repoarchive.CrawledRepo
	progress   *Progress
	dispatcher *Dispatcher
	config     Config
	logger     *slog.Logger
	client     *http.Client
	wg         sync.WaitGroup
	cancel     context.CancelFunc
}

// NewWorkerPool creates and starts a pool of crawl workers.
func NewWorkerPool(
	numWorkers int,
	repoQueue chan *repoarchive.CrawledRepo,
	progress *Progress,
	dispatcher *Dispatcher,
	config Config,
	logger *slog.Logger,
) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	wp := &WorkerPool{
		workQueue:  make(chan *CrawlTask, 1000),
		repoQueue:  repoQueue,
		progress:   progress,
		dispatcher: dispatcher,
		config:     config,
		logger:     logger,
		client: &http.Client{
			Timeout: 20 * time.Minute,
		},
		cancel: cancel,
	}

	wp.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go wp.worker(ctx, i)
	}

	return wp
}

// WorkQueue returns the channel to send CrawlTasks to.
func (wp *WorkerPool) WorkQueue() chan<- *CrawlTask {
	return wp.workQueue
}

// Stop signals workers to drain and waits for them to finish.
func (wp *WorkerPool) Stop() {
	close(wp.workQueue)
	wp.wg.Wait()
}

func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()
	logger := wp.logger.With("worker", id)

	for task := range wp.workQueue {
		if ctx.Err() != nil {
			return
		}
		wp.processTask(ctx, logger, task)
	}
}

func (wp *WorkerPool) processTask(ctx context.Context, logger *slog.Logger, task *CrawlTask) {
	ctx, span := tracer.Start(ctx, "crawler.processRepo")
	defer span.End()

	span.SetAttributes(
		attribute.String("repo.did", task.DID),
		attribute.String("repo.pds", task.PDS),
	)

	start := time.Now()

	crawled, recordCount, collectionCount, err := wp.fetchAndParseRepo(ctx, task)
	duration := time.Since(start).Seconds()
	repoDurationSeconds.Observe(duration)

	if err != nil {
		wp.handleError(ctx, logger, task, err)
		return
	}

	wp.dispatcher.ReportResult(task.PDS, true)

	span.SetAttributes(
		attribute.String("repo.rev", crawled.Rev),
		attribute.Int("repo.record.count", recordCount),
		attribute.Int("repo.collection.count", collectionCount),
	)

	repoRecords.Observe(float64(recordCount))
	repoCollections.Observe(float64(collectionCount))
	reposProcessedTotal.WithLabelValues("success").Inc()

	// Send to writer.
	select {
	case wp.repoQueue <- crawled:
		wp.progress.MarkCompleted(ctx, task.DID)
	case <-ctx.Done():
		return
	}
}

func (wp *WorkerPool) fetchAndParseRepo(ctx context.Context, task *CrawlTask) (*repoarchive.CrawledRepo, int, int, error) {
	url := fmt.Sprintf("%s/xrpc/com.atproto.sync.getRepo?did=%s", task.PDS, task.DID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.ipld.car")
	req.Header.Set("User-Agent", "jaz-crawler/1.0")

	httpStart := time.Now()
	resp, err := wp.client.Do(req)
	httpDuration := time.Since(httpStart).Seconds()
	httpRequestDurationSeconds.Observe(httpDuration)

	if err != nil {
		return nil, 0, 0, classifyNetworkError(err, task)
	}
	defer resp.Body.Close()

	httpRequestsTotal.WithLabelValues(strconv.Itoa(resp.StatusCode)).Inc()
	trace.SpanFromContext(ctx).SetAttributes(attribute.Int("http.status_code", resp.StatusCode))

	switch resp.StatusCode {
	case http.StatusOK:
		// Continue processing.
	case http.StatusNotFound:
		return nil, 0, 0, &RepoError{Code: resp.StatusCode, Category: "not_found", DID: task.DID}
	case http.StatusGone:
		return nil, 0, 0, &RepoError{Code: resp.StatusCode, Category: "deactivated", DID: task.DID}
	case http.StatusTooManyRequests:
		return nil, 0, 0, &RepoError{Code: resp.StatusCode, Category: "rate_limited", DID: task.DID, PDS: task.PDS}
	case http.StatusBadRequest:
		// Bluesky PDSs return 400 for various permanent repo states instead of
		// 404/410. Parse the body to categorize appropriately.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		bodyStr := string(body)
		switch {
		case strings.Contains(bodyStr, "RepoNotFound"), strings.Contains(bodyStr, "NotFound"):
			return nil, 0, 0, &RepoError{Code: resp.StatusCode, Category: "not_found", DID: task.DID}
		case strings.Contains(bodyStr, "RepoTakendown"):
			return nil, 0, 0, &RepoError{Code: resp.StatusCode, Category: "takendown", DID: task.DID}
		case strings.Contains(bodyStr, "RepoDeactivated"):
			return nil, 0, 0, &RepoError{Code: resp.StatusCode, Category: "deactivated", DID: task.DID}
		default:
			return nil, 0, 0, &RepoError{Code: resp.StatusCode, Category: "http_error", DID: task.DID,
				Err: fmt.Errorf("bad request: %s", bodyStr)}
		}
	default:
		if resp.StatusCode >= 500 {
			return nil, 0, 0, &RepoError{Code: resp.StatusCode, Category: "unavailable", DID: task.DID, PDS: task.PDS,
				Err: fmt.Errorf("server error: %s", resp.Status)}
		}
		return nil, 0, 0, &RepoError{Code: resp.StatusCode, Category: "http_error", DID: task.DID,
			Err: fmt.Errorf("unexpected status: %s", resp.Status)}
	}

	// Cap body reading to prevent OOM.
	maxBytes := int64(wp.config.MaxRepoSizeMB) * 1024 * 1024
	limitedBody := io.LimitReader(resp.Body, maxBytes+1)

	r, err := carrepo.ParseRepo(limitedBody)
	if err != nil {
		return nil, 0, 0, &RepoError{Category: "parse_error", DID: task.DID, Err: fmt.Errorf("reading CAR: %w", err)}
	}

	rev := r.Rev

	// Iterate all records, group by collection, marshal to JSON, strip CIDs.
	collections := make(map[string][]repoarchive.Record)
	recordCount := 0

	err = r.ForEach(func(path string, raw []byte) error {
		parts := strings.SplitN(path, "/", 2)
		if len(parts) != 2 {
			return nil
		}
		collection := parts[0]
		rkey := parts[1]

		jsonBytes, err := cborToStrippedJSON(raw)
		if err != nil {
			return nil
		}

		collections[collection] = append(collections[collection], repoarchive.Record{
			RKey: rkey,
			JSON: jsonBytes,
		})
		recordCount++
		return nil
	})
	if err != nil {
		return nil, 0, 0, &RepoError{Category: "parse_error", DID: task.DID, Err: fmt.Errorf("ForEach: %w", err)}
	}

	return &repoarchive.CrawledRepo{
		DID:         task.DID,
		PDS:         task.PDS,
		Rev:         rev,
		CrawledAt:   time.Now(),
		Collections: collections,
	}, recordCount, len(collections), nil
}

// classifyNetworkError categorizes a network-level error from http.Client.Do.
// All errors here are PDS-level (no HTTP response was received).
func classifyNetworkError(err error, task *CrawlTask) *RepoError {
	category := "connection_error"
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		category = "dns_error"
	} else if os.IsTimeout(err) || errors.Is(err, context.DeadlineExceeded) {
		category = "timeout"
	}
	return &RepoError{Category: category, DID: task.DID, PDS: task.PDS, Err: err}
}

// RepoError represents a categorized error during repo processing.
type RepoError struct {
	Code     int
	Category string // "not_found", "deactivated", "takendown", "rate_limited", "http_error", "dns_error", "unavailable", "parse_error", "timeout"
	DID      string
	PDS      string
	Err      error
}

func (e *RepoError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s (did=%s): %v", e.Category, e.DID, e.Err)
	}
	return fmt.Sprintf("%s (did=%s, code=%d)", e.Category, e.DID, e.Code)
}

func (e *RepoError) Unwrap() error {
	return e.Err
}

// isPDSError returns true if the error category indicates a PDS-level issue
// (network failure, server error) rather than a per-repo issue (not found, etc).
func isPDSError(category string) bool {
	switch category {
	case "dns_error", "unavailable", "timeout", "connection_error", "unknown":
		return true
	default:
		return false
	}
}

func (wp *WorkerPool) handleError(ctx context.Context, logger *slog.Logger, task *CrawlTask, err error) {
	if ctx.Err() != nil {
		return // Shutting down, don't report errors.
	}

	repoErr, ok := err.(*RepoError)
	if !ok {
		logger.Error("unexpected error", "did", task.DID, "pds", task.PDS, "error", err)
		wp.progress.MarkFailed(ctx, task.DID, "unknown")
		reposProcessedTotal.WithLabelValues("unknown").Inc()
		wp.dispatcher.ReportResult(task.PDS, false)
		return
	}

	reposProcessedTotal.WithLabelValues(repoErr.Category).Inc()

	// Report PDS-level errors to the dispatcher for blocking decisions.
	if isPDSError(repoErr.Category) {
		wp.dispatcher.ReportResult(task.PDS, false)
	}

	switch repoErr.Category {
	case "not_found", "deactivated", "takendown":
		// Permanent per-repo failures — skip.
		logger.Debug("skipping repo", "did", task.DID, "reason", repoErr.Category)
		wp.progress.MarkFailed(ctx, task.DID, repoErr.Category)
	case "parse_error":
		logger.Warn("parse error", "did", task.DID, "error", repoErr.Err)
		wp.progress.MarkFailed(ctx, task.DID, repoErr.Category)
	case "rate_limited":
		logger.Warn("rate limited by PDS", "pds", task.PDS)
		wp.progress.MarkFailed(ctx, task.DID, repoErr.Category)
	default:
		// PDS-level errors: dns_error, unavailable, timeout, http_error, unknown
		logger.Warn("crawl error", "did", task.DID, "pds", task.PDS, "category", repoErr.Category, "error", repoErr.Err)
		wp.progress.MarkFailed(ctx, task.DID, repoErr.Category)
	}
}
