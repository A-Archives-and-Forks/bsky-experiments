package plc

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"
)

var tracer = otel.Tracer("plc-exporter")

const (
	DefaultPageSize     = 1000
	DefaultRateLimit    = 5.0 // requests per second
	MaxRetries          = 5
	InitialBackoff      = 1 * time.Second
	MaxBackoff          = 60 * time.Second
)

// ExporterConfig contains configuration for the PLC exporter
type ExporterConfig struct {
	PLCHost      string
	RateLimit    float64
	RedisClient  *redis.Client
	RedisPrefix  string
	DB           driver.Conn
	Logger       *slog.Logger
}

// Exporter handles exporting PLC operations to ClickHouse
type Exporter struct {
	config       ExporterConfig
	httpClient   *http.Client
	rateLimiter  *rate.Limiter
	batchInserter *BatchInserter
	cursorKey    string

	cursor       string
	cursorMu     sync.RWMutex

	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// NewExporter creates a new PLC exporter
func NewExporter(config ExporterConfig) (*Exporter, error) {
	if config.RateLimit <= 0 {
		config.RateLimit = DefaultRateLimit
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &Exporter{
		config: config,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		rateLimiter:   rate.NewLimiter(rate.Limit(config.RateLimit), 1),
		batchInserter: NewBatchInserter(config.DB, config.Logger),
		cursorKey:     fmt.Sprintf("%s:plc_cursor", config.RedisPrefix),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Load cursor from Redis
	if err := e.loadCursor(ctx); err != nil {
		config.Logger.Warn("failed to load PLC cursor from Redis, starting from beginning", "error", err)
	}

	return e, nil
}

// Run starts the exporter main loop
func (e *Exporter) Run() {
	e.wg.Add(1)
	defer e.wg.Done()

	logger := e.config.Logger.With("component", "plc_exporter")
	logger.Info("starting PLC exporter", "host", e.config.PLCHost, "rate_limit", e.config.RateLimit)

	backoff := InitialBackoff
	consecutiveErrors := 0

	for {
		select {
		case <-e.ctx.Done():
			logger.Info("PLC exporter shutting down")
			return
		default:
		}

		// Rate limit
		if err := e.rateLimiter.Wait(e.ctx); err != nil {
			if e.ctx.Err() != nil {
				return
			}
			rateLimitHitsCounter.Inc()
			continue
		}

		// Fetch and process a page
		count, err := e.fetchAndProcessPage(e.ctx)
		if err != nil {
			consecutiveErrors++
			fetchErrorsCounter.Inc()
			logger.Error("failed to fetch PLC page", "error", err, "consecutive_errors", consecutiveErrors)

			// Exponential backoff
			if consecutiveErrors >= MaxRetries {
				logger.Error("max retries exceeded, backing off", "backoff", backoff)
				select {
				case <-e.ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, MaxBackoff)
			}
			continue
		}

		// Reset backoff on success
		consecutiveErrors = 0
		backoff = InitialBackoff

		// If we got fewer than expected, we're caught up - wait before polling again
		if count < DefaultPageSize {
			logger.Debug("caught up with PLC, waiting before next poll", "count", count)
			select {
			case <-e.ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
		} else {
			logger.Info("processed PLC page", "count", count, "cursor", e.getCursor())
		}
	}
}

// fetchAndProcessPage fetches a page of PLC operations and processes them
func (e *Exporter) fetchAndProcessPage(ctx context.Context) (int, error) {
	ctx, span := tracer.Start(ctx, "fetchAndProcessPage")
	defer span.End()

	cursor := e.getCursor()

	url := fmt.Sprintf("%s/export?count=%d", e.config.PLCHost, DefaultPageSize)
	if cursor != "" {
		url = fmt.Sprintf("%s&after=%s", url, cursor)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", "jaz-plc-exporter/1.0")
	req.Header.Set("Accept", "application/jsonlines")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		rateLimitHitsCounter.Inc()
		return 0, fmt.Errorf("rate limited by PLC server")
	}

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if readErr != nil {
			return 0, fmt.Errorf("unexpected status %d (failed to read body: %v)", resp.StatusCode, readErr)
		}
		return 0, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	// Parse newline-delimited JSON
	count := 0
	var lastOp *PLCOperation
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB buffer for large lines

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var op PLCOperation
		if err := json.Unmarshal(line, &op); err != nil {
			e.config.Logger.Warn("failed to parse PLC operation", "error", err, "line", string(line[:min(100, len(line))]))
			continue
		}

		// Extract handle and PDS from operation
		handle, pds := ExtractHandleAndPDS(&op)
		opType := GetOperationType(&op)

		// Add to batch
		e.batchInserter.Add(&PLCBatch{
			DID:           op.DID,
			CID:           op.CID,
			OperationType: opType,
			CreatedAt:     op.CreatedAt,
			Handle:        handle,
			PDS:           pds,
			OperationJSON: string(op.Operation),
			TimeUS:        op.CreatedAt.UnixMicro(),
		})

		operationsProcessedCounter.WithLabelValues(opType).Inc()
		count++
		lastOp = &op
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("error reading response: %w", err)
	}

	// Update cursor to last operation's createdAt
	if lastOp != nil {
		newCursor := lastOp.CreatedAt.Format(time.RFC3339Nano)
		e.setCursor(newCursor)
		cursorGauge.Set(float64(lastOp.CreatedAt.Unix()))

		// Persist cursor to Redis
		if err := e.saveCursor(ctx); err != nil {
			e.config.Logger.Error("failed to save PLC cursor to Redis", "error", err)
		}
	}

	return count, nil
}

// getCursor returns the current cursor
func (e *Exporter) getCursor() string {
	e.cursorMu.RLock()
	defer e.cursorMu.RUnlock()
	return e.cursor
}

// setCursor sets the current cursor
func (e *Exporter) setCursor(cursor string) {
	e.cursorMu.Lock()
	defer e.cursorMu.Unlock()
	e.cursor = cursor
}

// loadCursor loads the cursor from Redis
func (e *Exporter) loadCursor(ctx context.Context) error {
	cursor, err := e.config.RedisClient.Get(ctx, e.cursorKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil // No cursor yet, start from beginning
		}
		return fmt.Errorf("failed to get cursor from Redis: %w", err)
	}
	e.setCursor(cursor)
	e.config.Logger.Info("loaded PLC cursor from Redis", "cursor", cursor)
	return nil
}

// saveCursor saves the cursor to Redis
func (e *Exporter) saveCursor(ctx context.Context) error {
	cursor := e.getCursor()
	if cursor == "" {
		return nil
	}
	return e.config.RedisClient.Set(ctx, e.cursorKey, cursor, 0).Err()
}

// Shutdown gracefully shuts down the exporter
func (e *Exporter) Shutdown() {
	e.config.Logger.Info("shutting down PLC exporter")

	// Cancel context to stop main loop
	e.cancel()

	// Wait for main loop to finish
	e.wg.Wait()

	// Shutdown batch inserter (flushes remaining batches)
	e.batchInserter.Shutdown()

	// Save final cursor
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.saveCursor(ctx); err != nil {
		e.config.Logger.Error("failed to save final PLC cursor", "error", err)
	}

	e.config.Logger.Info("PLC exporter shut down successfully")
}
