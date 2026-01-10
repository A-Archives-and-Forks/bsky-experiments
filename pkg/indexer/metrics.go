package indexer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	eventsProcessedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_events_processed_total",
		Help: "The total number of events processed by the indexer",
	}, []string{"event_type", "socket_url"})

	opsProcessedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_operations_processed_total",
		Help: "The total number of operations processed by the indexer",
	}, []string{"operation", "collection", "socket_url"})

	recordsProcessedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_records_processed_total",
		Help: "The total number of records processed by the indexer",
	}, []string{"operation", "collection", "socket_url"})
)
