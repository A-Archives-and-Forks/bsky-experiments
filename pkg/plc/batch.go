package plc

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
	DefaultQueueSize = 50_000
)

// BatchInserter handles batched inserts of PLC operations to ClickHouse
type BatchInserter struct {
	db     driver.Conn
	logger *slog.Logger

	queue chan *PLCBatch

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewBatchInserter creates a new PLC batch inserter
func NewBatchInserter(db driver.Conn, logger *slog.Logger) *BatchInserter {
	ctx, cancel := context.WithCancel(context.Background())

	bi := &BatchInserter{
		db:     db,
		logger: logger,
		queue:  make(chan *PLCBatch, DefaultQueueSize),
		ctx:    ctx,
		cancel: cancel,
	}

	// Start batch worker
	bi.wg.Add(1)
	go bi.batchWorker()

	return bi
}

// Add adds a PLC operation to the batch queue
func (bi *BatchInserter) Add(op *PLCBatch) {
	bi.queue <- op
}

// batchWorker processes batches of PLC operations
func (bi *BatchInserter) batchWorker() {
	defer bi.wg.Done()

	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	var batch []*PLCBatch

	flush := func() {
		if len(batch) == 0 {
			return
		}

		if err := bi.insertBatch(bi.ctx, batch); err != nil {
			bi.logger.Error("failed to insert PLC batch", "error", err, "batch_size", len(batch))
		} else {
			bi.logger.Debug("inserted PLC batch", "batch_size", len(batch))
			batchesInsertedCounter.Inc()
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
		case op := <-bi.queue:
			batch = append(batch, op)
			if len(batch) >= MaxBatchSize {
				flush()
			}
		}
	}
}

// insertBatch inserts a batch of PLC operations into the plc_operations table
func (bi *BatchInserter) insertBatch(ctx context.Context, batch []*PLCBatch) error {
	if len(batch) == 0 {
		return nil
	}

	batchInsert, err := bi.db.PrepareBatch(ctx, "INSERT INTO plc_operations (did, operation_type, created_at, handle, pds, operation_json, time_us)")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, op := range batch {
		if err := batchInsert.Append(
			op.DID,
			op.OperationType,
			op.CreatedAt,
			op.Handle,
			op.PDS,
			op.OperationJSON,
			op.TimeUS,
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
