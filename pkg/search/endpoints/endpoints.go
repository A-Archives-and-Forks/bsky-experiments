package endpoints

import (
	"fmt"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/jazware/bsky-experiments/pkg/consumer"
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

	Bitmapper *consumer.Bitmapper
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

	bitmapper, err := consumer.NewBitmapper(store)
	if err != nil {
		return nil, fmt.Errorf("error initializing bitmapper: %w", err)
	}

	return &API{
		UserCount:       userCount,
		Stats:           stats,
		Store:           store,
		Directory:       dir,
		MagicHeaderVal:  MagicHeaderVal,
		StatsCacheTTL:   statsCacheTTL,
		StatsCacheRWMux: &sync.RWMutex{},
		CheckoutLimiter: rate.NewLimiter(rate.Every(2*time.Second), 1),
		Bitmapper:       bitmapper,
	}, nil
}
