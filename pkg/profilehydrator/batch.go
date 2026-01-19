package profilehydrator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	MaxBatchSize     = 10_000
	FlushInterval    = 5 * time.Second
	DefaultQueueSize = 10_000
)

// ProfileBatch represents a profile to be inserted into ClickHouse
type ProfileBatch struct {
	DID         string
	Handle      string
	DisplayName string
	Description string
	AvatarCID   string
	BannerCID   string
	CreatedAt   time.Time
	ProfileJSON string
	TimeUS      int64
}

// BatchInserter handles batched inserts of profiles to ClickHouse
type BatchInserter struct {
	db     driver.Conn
	logger *slog.Logger

	queue chan *ProfileBatch

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewBatchInserter creates a new profile batch inserter
func NewBatchInserter(db driver.Conn, logger *slog.Logger) *BatchInserter {
	ctx, cancel := context.WithCancel(context.Background())

	bi := &BatchInserter{
		db:     db,
		logger: logger,
		queue:  make(chan *ProfileBatch, DefaultQueueSize),
		ctx:    ctx,
		cancel: cancel,
	}

	// Start batch worker
	bi.wg.Add(1)
	go bi.batchWorker()

	return bi
}

// Add adds a profile to the batch queue
func (bi *BatchInserter) Add(profile *ProfileBatch) {
	bi.queue <- profile
}

// batchWorker processes batches of profiles
func (bi *BatchInserter) batchWorker() {
	defer bi.wg.Done()

	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	var batch []*ProfileBatch

	flush := func() {
		if len(batch) == 0 {
			return
		}

		if err := bi.insertBatch(bi.ctx, batch); err != nil {
			bi.logger.Error("failed to insert profile batch", "error", err, "batch_size", len(batch))
		} else {
			bi.logger.Debug("inserted profile batch", "batch_size", len(batch))
			batchesInsertedCounter.Inc()
			profilesInsertedCounter.Add(float64(len(batch)))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-bi.ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case profile := <-bi.queue:
			batch = append(batch, profile)
			if len(batch) >= MaxBatchSize {
				flush()
			}
		}
	}
}

// insertBatch inserts a batch of profiles into the profiles table
func (bi *BatchInserter) insertBatch(ctx context.Context, batch []*ProfileBatch) error {
	if len(batch) == 0 {
		return nil
	}

	batchInsert, err := bi.db.PrepareBatch(ctx, `INSERT INTO profiles (
		did, handle, display_name, description, avatar_cid, banner_cid,
		created_at, profile_json, time_us
	)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, profile := range batch {
		if err := batchInsert.Append(
			profile.DID,
			profile.Handle,
			profile.DisplayName,
			profile.Description,
			profile.AvatarCID,
			profile.BannerCID,
			profile.CreatedAt,
			profile.ProfileJSON,
			profile.TimeUS,
		); err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	if err := batchInsert.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the batch inserter
func (bi *BatchInserter) Shutdown() {
	bi.cancel()
	bi.wg.Wait()
}

// QueueLen returns the current queue length
func (bi *BatchInserter) QueueLen() int {
	return len(bi.queue)
}
