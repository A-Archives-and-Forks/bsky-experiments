package search

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	totalUsersGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "search_total_users",
		Help: "Total number of users in the system",
	})

	totalPostsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "search_total_posts",
		Help: "Total number of posts in the system",
	})

	totalLikesGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "search_total_likes",
		Help: "Total number of likes in the system",
	})

	totalFollowsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "search_total_follows",
		Help: "Total number of follows in the system",
	})

	statsRefreshDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "search_stats_refresh_duration_seconds",
		Help:    "Duration of stats refresh operations",
		Buckets: prometheus.DefBuckets,
	})

	statsRefreshTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "search_stats_refresh_total",
		Help: "Total number of stats refresh attempts",
	}, []string{"status"})
)
