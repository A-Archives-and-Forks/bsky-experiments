package crawler

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jazware/bsky-experiments/pkg/repoarchive"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	chWriterRecordsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_chwriter_records_total",
		Help: "Total records sent to ClickHouse in direct mode.",
	})

	chWriterBatchesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "crawler_chwriter_batches_total",
		Help: "Batches sent to ClickHouse in direct mode.",
	}, []string{"result"})

	chWriterBatchSizeHist = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "crawler_chwriter_batch_size",
		Help:    "Records per batch sent to ClickHouse in direct mode.",
		Buckets: []float64{1000, 10000, 50000, 100000, 250000, 500000},
	})
)

const (
	chBatchLimit     = 500_000
	chFlushInterval  = 10 * time.Second
	chLogInterval    = 30 * time.Second
)

// chBatch holds columnar arrays for a batch insert into crawl_records.
type chBatch struct {
	repos       []string
	collections []string
	rkeys       []string
	recordJSONs []string
	crawledAts  []time.Time
	timeUSs     []int64
}

func (b *chBatch) len() int {
	return len(b.repos)
}

func (b *chBatch) append(repo, collection, rkey, recordJSON string, crawledAt time.Time) {
	b.repos = append(b.repos, repo)
	b.collections = append(b.collections, collection)
	b.rkeys = append(b.rkeys, rkey)
	b.recordJSONs = append(b.recordJSONs, recordJSON)
	b.crawledAts = append(b.crawledAts, crawledAt)
	b.timeUSs = append(b.timeUSs, crawledAt.UnixMicro())
}

// CHWriter receives CrawledRepos and batch-inserts them into ClickHouse.
type CHWriter struct {
	conn   driver.Conn
	input  <-chan *repoarchive.CrawledRepo
	logger *slog.Logger

	sent atomic.Int64
	err  error
	done chan struct{}
}

// NewCHWriter creates a CHWriter that reads from the given channel.
func NewCHWriter(conn driver.Conn, input <-chan *repoarchive.CrawledRepo, logger *slog.Logger) *CHWriter {
	return &CHWriter{
		conn:   conn,
		input:  input,
		logger: logger.With("component", "chwriter"),
		done:   make(chan struct{}),
	}
}

// Run starts the writer loop. It blocks until the input channel is closed or ctx is cancelled.
func (w *CHWriter) Run(ctx context.Context) {
	defer close(w.done)

	// Pipeline: accumulator fills batches, sender goroutine ships them.
	// Buffer of 1 lets the next batch accumulate while the current one sends.
	sendCh := make(chan chBatch, 1)
	sendDone := make(chan struct{})

	go func() {
		defer close(sendDone)
		for b := range sendCh {
			if err := w.sendBatch(ctx, b); err != nil {
				w.err = err
				for range sendCh {
				}
				return
			}
		}
	}()

	current := chBatch{}
	flushTicker := time.NewTicker(chFlushInterval)
	defer flushTicker.Stop()
	logTicker := time.NewTicker(chLogInterval)
	defer logTicker.Stop()

	flush := func() {
		if current.len() == 0 {
			return
		}
		select {
		case <-sendDone:
			return
		default:
		}
		sendCh <- current
		current = chBatch{}
	}

	for {
		select {
		case repo, ok := <-w.input:
			if !ok {
				flush()
				close(sendCh)
				<-sendDone
				return
			}
			w.flattenRepo(repo, &current)
			if current.len() >= chBatchLimit {
				flush()
			}
		case <-flushTicker.C:
			flush()
		case <-logTicker.C:
			w.logger.Info("chwriter progress", "sent", w.sent.Load())
		case <-ctx.Done():
			for repo := range w.input {
				w.flattenRepo(repo, &current)
				if current.len() >= chBatchLimit {
					flush()
				}
			}
			flush()
			close(sendCh)
			<-sendDone
			return
		}
	}
}

// Wait blocks until the writer has finished.
func (w *CHWriter) Wait() error {
	<-w.done
	return w.err
}

// Sent returns the number of records successfully sent.
func (w *CHWriter) Sent() int64 {
	return w.sent.Load()
}

func (w *CHWriter) flattenRepo(repo *repoarchive.CrawledRepo, b *chBatch) {
	for collection, records := range repo.Collections {
		for _, rec := range records {
			b.append(repo.DID, collection, rec.RKey, string(rec.JSON), repo.CrawledAt)
		}
	}
}

func (w *CHWriter) sendBatch(ctx context.Context, b chBatch) error {
	if b.len() == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx,
		"INSERT INTO crawl_records (repo, collection, rkey, record_json, crawled_at, time_us)")
	if err != nil {
		return fmt.Errorf("preparing batch: %w", err)
	}
	for i := range b.repos {
		if err := batch.Append(
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
	if err := batch.Send(); err != nil {
		chWriterBatchesTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("sending batch: %w", err)
	}
	n := b.len()
	chWriterBatchesTotal.WithLabelValues("success").Inc()
	chWriterBatchSizeHist.Observe(float64(n))
	chWriterRecordsTotal.Add(float64(n))
	w.sent.Add(int64(n))
	return nil
}
