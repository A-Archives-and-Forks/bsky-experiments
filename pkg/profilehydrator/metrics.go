package profilehydrator

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	profilesFetchedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_hydrator_profiles_fetched_total",
		Help: "The total number of profiles fetched from the API",
	}, []string{"status"})

	profilesInsertedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "profile_hydrator_profiles_inserted_total",
		Help: "The total number of profiles inserted into ClickHouse",
	})

	batchesInsertedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "profile_hydrator_batches_inserted_total",
		Help: "The total number of batches inserted into ClickHouse",
	})

	rateLimitHitsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "profile_hydrator_rate_limit_hits_total",
		Help: "The total number of rate limit hits",
	})

	fetchErrorsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "profile_hydrator_fetch_errors_total",
		Help: "The total number of fetch errors",
	})

	repoRecordsCursorGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "profile_hydrator_repo_records_cursor_position",
		Help: "The current cursor position (time_us) for repo_records",
	})

	plcOperationsCursorGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "profile_hydrator_plc_operations_cursor_position",
		Help: "The current cursor position (time_us) for plc_operations",
	})

	didsQueuedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "profile_hydrator_dids_queued_total",
		Help: "The total number of DIDs queued for hydration",
	})
)
