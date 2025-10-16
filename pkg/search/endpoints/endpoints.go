package endpoints

import (
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/jazware/bsky-experiments/pkg/consumer/store"
	statsqueries "github.com/jazware/bsky-experiments/pkg/stats/stats_queries"
	"github.com/jazware/bsky-experiments/pkg/usercount"
	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"
)

type API struct {
	UserCount *usercount.UserCount

	Store     *store.Store
	Stats     *statsqueries.Queries
	Directory identity.Directory

	StatsCacheTTL   time.Duration
	StatsCache      *StatsCacheEntry
	StatsCacheRWMux *sync.RWMutex

	CheckoutLimiter *rate.Limiter
	MagicHeaderVal  string
}

var tracer = otel.Tracer("search-api")

func NewAPI(
	store *store.Store,
	stats *statsqueries.Queries,
	userCount *usercount.UserCount,
	MagicHeaderVal string,
	statsCacheTTL time.Duration,
) (*API, error) {
	dir := identity.DefaultDirectory()

	return &API{
		UserCount:       userCount,
		Stats:           stats,
		Store:           store,
		Directory:       dir,
		MagicHeaderVal:  MagicHeaderVal,
		StatsCacheTTL:   statsCacheTTL,
		StatsCacheRWMux: &sync.RWMutex{},
		CheckoutLimiter: rate.NewLimiter(rate.Every(2*time.Second), 1),
	}, nil
}
