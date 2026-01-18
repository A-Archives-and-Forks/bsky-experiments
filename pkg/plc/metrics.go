package plc

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	operationsProcessedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "plc_exporter_operations_processed_total",
		Help: "The total number of PLC operations processed",
	}, []string{"operation_type"})

	batchesInsertedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "plc_exporter_batches_inserted_total",
		Help: "The total number of batches inserted into ClickHouse",
	})

	rateLimitHitsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "plc_exporter_rate_limit_hits_total",
		Help: "The total number of rate limit hits",
	})

	fetchErrorsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "plc_exporter_fetch_errors_total",
		Help: "The total number of fetch errors",
	})

	cursorGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "plc_exporter_cursor_timestamp",
		Help: "The timestamp of the current cursor position (Unix seconds)",
	})
)
