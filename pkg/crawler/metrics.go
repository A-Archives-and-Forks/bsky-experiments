package crawler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Repo processing metrics.
var (
	reposProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "crawler_repos_processed_total",
		Help: "Total repos processed by result category.",
	}, []string{"result"})

	repoDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "crawler_repo_duration_seconds",
		Help:    "Total time per repo (fetch + parse).",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	})

	repoRecords = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "crawler_repo_records",
		Help:    "Records per repo.",
		Buckets: []float64{0, 10, 50, 100, 500, 1000, 5000, 10000, 50000},
	})

	repoCollections = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "crawler_repo_collections",
		Help:    "Collections per repo.",
		Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
	})

	repoCompressedBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "crawler_repo_compressed_bytes",
		Help:    "Compressed bytes written per repo.",
		Buckets: []float64{100, 1000, 10000, 100000, 1e6, 10e6, 100e6},
	})
)

// HTTP metrics.
var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "crawler_http_requests_total",
		Help: "HTTP requests to PDSs by status code.",
	}, []string{"status_code"})

	httpRequestDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "crawler_http_request_duration_seconds",
		Help:    "HTTP request latency to PDSs.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
	})
)

// Writer metrics.
var (
	segmentsFinalizedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_segments_finalized_total",
		Help: "Segment files completed.",
	})

	bytesWrittenTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_bytes_written_total",
		Help: "Total bytes written to segments.",
	})
)

// Progress metrics.
var (
	didsProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_dids_processed_total",
		Help: "Running count of processed DIDs.",
	})

	pagesFetchedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_pages_fetched_total",
		Help: "ClickHouse pages fetched.",
	})

	pageDIDs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "crawler_page_dids",
		Help:    "DIDs per page after filtering.",
		Buckets: []float64{0, 100, 500, 1000, 5000, 10000, 25000, 50000},
	})

	pdsCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "crawler_pds_count",
		Help: "Distinct PDSs currently tracked.",
	})

	dispatchedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_dispatched_total",
		Help: "Total DIDs dispatched to workers.",
	})

	dispatchRemaining = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "crawler_dispatch_remaining",
		Help: "DIDs remaining to dispatch in current page.",
	})

	pdsBlockedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "crawler_pds_blocked_total",
		Help: "PDSs blocked due to consecutive failures.",
	})

	activePDSGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "crawler_active_pds",
		Help: "PDSs with remaining undispatched work.",
	})
)
