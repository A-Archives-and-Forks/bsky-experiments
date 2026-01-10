package endpoints

import (
	"log/slog"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/jazware/bsky-experiments/pkg/indexer/store"
	"github.com/jazware/bsky-experiments/pkg/search"
	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"
	"time"
)

type API struct {
	Logger          *slog.Logger
	SearchService   *search.SearchService
	Store           *store.Store
	Directory       identity.Directory
	CheckoutLimiter *rate.Limiter
	MagicHeaderVal  string
}

var tracer = otel.Tracer("search-api")

func NewAPI(
	logger *slog.Logger,
	searchService *search.SearchService,
	store *store.Store,
	magicHeaderVal string,
) (*API, error) {
	dir := identity.DefaultDirectory()

	return &API{
		Logger:          logger.With("component", "api"),
		SearchService:   searchService,
		Store:           store,
		Directory:       dir,
		MagicHeaderVal:  magicHeaderVal,
		CheckoutLimiter: rate.NewLimiter(rate.Every(2*time.Second), 1),
	}, nil
}
