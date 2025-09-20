package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/parallel"
	"github.com/bluesky-social/jetstream/pkg/models"
	statsqueries "github.com/jazware/bsky-experiments/pkg/stats/stats_queries"
	"github.com/jazware/bsky-experiments/pkg/telemetry"
	_ "github.com/lib/pq" // Postgres driver
	cli "github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("stats_consumer")

type Consumer struct {
	socketURL *url.URL
	logger    *slog.Logger

	cursorLk   sync.Mutex
	lastCursor *int64
	db         *sql.DB
	queries    *statsqueries.Queries

	cursorSaveShutdown chan struct{}
	lastCursorSaved    chan struct{}

	hllLock sync.Mutex
	hll     map[string]*HLL
}

type HLL struct {
	sketch      *hyperloglog.Sketch
	windowStart time.Time
	windowEnd   time.Time
	deleteAfter time.Time
	activity    string
}

var consumeCmd = &cli.Command{
	Name: "consume",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "jetstream-websocket-url",
			Usage:   "full WebSocket URL to the jetstream subscription endpoint",
			Value:   "wss://jetstream2.us-west.bsky.network/subscribe",
			EnvVars: []string{"JETSTREAM_WEBSOCKET_URL"},
		},
		&cli.StringFlag{
			Name: "db-url",
			Usage: "PostgreSQL connection string for storing the cursor and stats, " +
				"e.g. 'postgres://user:password@localhost:5432/dbname?sslmode=disable'",
			EnvVars: []string{"DB_URL"},
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cctx.Context
		// Flags
		opt := struct {
			JetstreamWebsocketURL string
			DBURL                 string
		}{
			JetstreamWebsocketURL: cctx.String("jetstream-websocket-url"),
			DBURL:                 cctx.String("db-url"),
		}

		logger := telemetry.StartLogger(cctx)
		telemetry.StartMetrics(cctx)

		u, err := url.Parse(opt.JetstreamWebsocketURL)
		if err != nil {
			return errors.Join(err, errors.New("failed to parse jetstream websocket URL"))
		}

		db, err := sql.Open("postgres", opt.DBURL)
		if err != nil {
			return errors.Join(err, errors.New("failed to connect to Postgres"))
		}
		defer db.Close()

		queries := statsqueries.New(db)

		if err := db.PingContext(ctx); err != nil {
			return errors.Join(err, errors.New("failed to ping Postgres"))
		}
		logger.Info("connected to Postgres")

		c := &Consumer{
			socketURL: u,
			logger:    logger,

			db:      db,
			queries: queries,

			cursorSaveShutdown: make(chan struct{}),
			lastCursorSaved:    make(chan struct{}),

			hll: make(map[string]*HLL),
		}

		// Try to load the last saved cursor
		if err := c.loadCursor(ctx); err != nil {
			return errors.Join(err, errors.New("failed to load last cursor"))
		}

		// Try to load existing HLLs
		if err := c.loadHLLs(ctx); err != nil {
			return errors.Join(err, errors.New("failed to load existing HLLs"))
		}

		scheduler := parallel.NewScheduler(50, "jetstream_stats_consumer", logger, c.HandleEvent)

		// Start the cursor save loop
		go c.periodicallySaveCursor(ctx)

		// Periodically trim old HLLs and update daily summary
		go func() {
			ticker := time.NewTicker(15 * time.Minute)
			defer ticker.Stop()
			onStart := make(chan struct{}, 1)
			onStart <- struct{}{}

			for {
				select {
				case <-ticker.C:
					if err := c.trimOldHLLs(ctx); err != nil {
						logger.Error("failed to trim old HLLs", "error", err)
					}
					if err := c.updateDailySummary(ctx, time.Now().UTC()); err != nil {
						logger.Error("failed to update daily summary", "error", err)
					}
				case <-onStart:
					if err := c.trimOldHLLs(ctx); err != nil {
						logger.Error("failed to trim old HLLs", "error", err)
					}
					if err := c.updateDailySummary(ctx, time.Now().UTC()); err != nil {
						logger.Error("failed to update daily summary", "error", err)
					}
				case <-c.cursorSaveShutdown:
					logger.Info("cursor save shutdown received, stopping HLL trim loop")
					return
				}
			}
		}()

		jetstreamConfig := client.DefaultClientConfig()
		jetstreamConfig.WebsocketURL = u.String()
		jetstreamClient, err := client.NewClient(jetstreamConfig, logger, scheduler)
		if err != nil {
			return errors.Join(err, errors.New("failed to create jetstream client"))
		}

		// Create a channel that will be closed when we want to stop the application
		// Usually when a critical routine returns an error
		connectionKill := make(chan struct{})
		shutdownRepoStream := make(chan struct{})
		repoStreamShutdown := make(chan struct{})
		go func() {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			go func() {
				err = jetstreamClient.ConnectAndRead(ctx, c.getCursor())
				if !errors.Is(err, context.Canceled) || !errors.Is(err, net.ErrClosed) {
					logger.Info("ConnectAndRead returned unexpectedly, killing consumer", "error", err)
					close(connectionKill)
				} else {
					logger.Info("ConnectAndRead closed on context cancel")
				}
				close(repoStreamShutdown)
			}()
			<-shutdownRepoStream
			cancel()
		}()

		// Handle exit signals.
		logger.Debug("registering OS exit signal handler")
		quit := make(chan struct{})
		exitSignals := make(chan os.Signal, 1)
		signal.Notify(exitSignals, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			// Trigger the return that causes an exit when we return from this goroutine.
			defer close(quit)

			select {
			case sig := <-exitSignals:
				logger.Info("received OS exit signal", "signal", sig)
			case <-ctx.Done():
				logger.Info("shutting down on context done")
			case <-connectionKill:
				logger.Info("shutting down on events kill")
			}

			// Wait up to 5 seconds for the Jetstream client to finish processing.
			close(shutdownRepoStream)
			select {
			case <-repoStreamShutdown:
				logger.Info("Jetstream client finished processing")
			case <-time.After(5 * time.Second):
				logger.Warn("Jetstream client did not finish processing in time, forcing shutdown")
			}

			// Wait for up to 5 seconds for the cursor save loop to finish.
			close(c.cursorSaveShutdown)
			select {
			case <-c.lastCursorSaved:
				logger.Info("cursor save loop finished")
			case <-time.After(5 * time.Second):
				logger.Warn("cursor save loop did not finish in time, forcing shutdown")
			}
		}()

		<-quit
		logger.Info("graceful shutdown complete")
		return nil
	},
}

func (c *Consumer) updateCursor(seq int64) {
	c.cursorLk.Lock()
	defer c.cursorLk.Unlock()

	if c.lastCursor == nil || seq > *c.lastCursor {
		c.lastCursor = &seq
	}
}

func (c *Consumer) loadCursor(ctx context.Context) error {
	c.cursorLk.Lock()
	defer c.cursorLk.Unlock()

	cursor, err := c.queries.GetCursor(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			c.logger.Info("no previous cursor found")
			return nil
		}
		return fmt.Errorf("failed to load cursor: %w", err)
	}
	c.lastCursor = &cursor
	c.logger.Info("loaded previous cursor", "cursor", cursor)
	return nil
}

func (c *Consumer) getCursor() *int64 {
	c.cursorLk.Lock()
	defer c.cursorLk.Unlock()

	return c.lastCursor
}

func (c *Consumer) periodicallySaveCursor(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	defer func() {
		// Save one last time before exiting
		c.cursorLk.Lock()
		if c.lastCursor != nil {
			if err := c.queries.UpsertCursor(ctx, *c.lastCursor); err != nil {
				c.logger.Error("failed to save cursor on exit", "error", err)
			} else {
				c.logger.Info("saved cursor on exit", "cursor", *c.lastCursor)
			}
			// Also save HLLs
			if err := c.saveHLLs(ctx); err != nil {
				c.logger.Error("failed to save HLLs on exit", "error", err)
			} else {
				c.logger.Info("saved HLLs on exit")
			}
		}
		c.cursorLk.Unlock()
		close(c.lastCursorSaved)
	}()

	for {
		select {
		case <-c.cursorSaveShutdown:
			c.logger.Info("exit signal received, stopping cursor save loop")
			return
		case <-ticker.C:
			c.cursorLk.Lock()
			last := c.lastCursor
			c.cursorLk.Unlock()
			if last != nil {
				if err := c.queries.UpsertCursor(ctx, *last); err != nil {
					c.logger.Error("failed to save cursor", "error", err)
				} else {
					c.logger.Info("saved cursor", "cursor", *last)
				}
				// Also save HLLs
				if err := c.saveHLLs(ctx); err != nil {
					c.logger.Error("failed to save HLLs", "error", err)
				} else {
					c.logger.Info("saved HLLs")
				}
			}
		}
	}
}

func (c *Consumer) saveHLLs(ctx context.Context) error {
	c.hllLock.Lock()
	defer c.hllLock.Unlock()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for key, hll := range c.hll {
		data, err := hll.sketch.MarshalBinary()
		if err != nil {
			c.logger.Error("failed to marshal HLL sketch", "key", key, "error", err)
			continue
		}
		if err := c.queries.WithTx(tx).UpsertHLL(ctx, statsqueries.UpsertHLLParams{
			Hll:         data,
			Summary:     int64(hll.sketch.Estimate()),
			MetricName:  hll.activity,
			WindowStart: hll.windowStart,
			WindowEnd:   hll.windowEnd,
			DeleteAfter: hll.deleteAfter,
		}); err != nil {
			c.logger.Error("failed to upsert HLL sketch", "key", key, "error", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (c *Consumer) loadHLLs(ctx context.Context) error {
	c.logger.Info("loading existing HLLs from database")

	c.hllLock.Lock()
	defer c.hllLock.Unlock()

	hllRows, err := c.queries.GetActiveHLLMetrics(ctx, statsqueries.GetActiveHLLMetricsParams{
		WindowStart: time.Now().Add(-7 * 24 * time.Hour),
		WindowEnd:   time.Now().Add(24 * time.Hour),
	})
	if err != nil {
		return fmt.Errorf("failed to list HLLs: %w", err)
	}

	for _, row := range hllRows {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(row.Hll); err != nil {
			c.logger.Error("failed to unmarshal HLL sketch", "metric", row.MetricName, "window_start", row.WindowStart, "window_end", row.WindowEnd, "error", err)
			continue
		}
		key := fmt.Sprintf("%s|%s", row.MetricName, row.WindowStart.Format("2006-01-02"))
		c.hll[key] = &HLL{
			sketch:      sketch,
			windowStart: row.WindowStart,
			windowEnd:   row.WindowEnd,
			activity:    row.MetricName,
			deleteAfter: row.DeleteAfter,
		}
	}

	c.logger.Info("loaded existing HLLs from database", "count", len(c.hll))

	return nil
}

func (c *Consumer) updateDailySummary(ctx context.Context, date time.Time) error {
	startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour)

	c.logger.Info("updating daily stats summary", "date", startOfDay.Format("2006-01-02"))

	// Get DAU
	dauHLLs, err := c.queries.GetHLLsByMetricInRange(ctx, statsqueries.GetHLLsByMetricInRangeParams{
		MetricName:  "dau",
		WindowStart: startOfDay,
		WindowEnd:   endOfDay,
	})
	if err != nil {
		return fmt.Errorf("failed to get DAU HLLs: %w", err)
	}
	dauSketch := hyperloglog.New16()
	for _, hllRow := range dauHLLs {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(hllRow.Hll); err != nil {
			c.logger.Error("failed to unmarshal DAU HLL sketch", "error", err)
			continue
		}
		if err := dauSketch.Merge(sketch); err != nil {
			c.logger.Error("failed to merge DAU HLL sketch", "error", err)
			continue
		}
	}
	dau := int64(dauSketch.Estimate())

	likesKey := "records_create_app.bsky.feed.like"
	likersKey := "actors_create_app.bsky.feed.like"
	postsKey := "records_create_app.bsky.feed.post"
	postersKey := "actors_create_app.bsky.feed.post"
	followsKey := "records_create_app.bsky.graph.follow"
	followersKey := "actors_create_app.bsky.graph.follow"
	blocksKey := "records_create_app.bsky.graph.block"
	blockersKey := "actors_create_app.bsky.graph.block"

	likesHLLs, err := c.queries.GetHLLsByMetricInRange(ctx, statsqueries.GetHLLsByMetricInRangeParams{
		MetricName:  likesKey,
		WindowStart: startOfDay,
		WindowEnd:   endOfDay,
	})
	if err != nil {
		return fmt.Errorf("failed to get likes HLLs: %w", err)
	}
	likesSketch := hyperloglog.New16()
	for _, hllRow := range likesHLLs {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(hllRow.Hll); err != nil {
			c.logger.Error("failed to unmarshal likes HLL sketch", "error", err)
			continue
		}
		if err := likesSketch.Merge(sketch); err != nil {
			c.logger.Error("failed to merge likes HLL sketch", "error", err)
			continue
		}
	}
	likesPerDay := int64(likesSketch.Estimate())

	likersHLLs, err := c.queries.GetHLLsByMetricInRange(ctx, statsqueries.GetHLLsByMetricInRangeParams{
		MetricName:  likersKey,
		WindowStart: startOfDay,
		WindowEnd:   endOfDay,
	})
	if err != nil {
		return fmt.Errorf("failed to get likers HLLs: %w", err)
	}
	likersSketch := hyperloglog.New16()
	for _, hllRow := range likersHLLs {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(hllRow.Hll); err != nil {
			c.logger.Error("failed to unmarshal likers HLL sketch", "error", err)
			continue
		}
		if err := likersSketch.Merge(sketch); err != nil {
			c.logger.Error("failed to merge likers HLL sketch", "error", err)
			continue
		}
	}
	dailyActiveLikers := int64(likersSketch.Estimate())

	postsHLLs, err := c.queries.GetHLLsByMetricInRange(ctx, statsqueries.GetHLLsByMetricInRangeParams{
		MetricName:  postsKey,
		WindowStart: startOfDay,
		WindowEnd:   endOfDay,
	})
	if err != nil {
		return fmt.Errorf("failed to get posts HLLs: %w", err)
	}
	postsSketch := hyperloglog.New16()
	for _, hllRow := range postsHLLs {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(hllRow.Hll); err != nil {
			c.logger.Error("failed to unmarshal posts HLL sketch", "error", err)
			continue
		}
		if err := postsSketch.Merge(sketch); err != nil {
			c.logger.Error("failed to merge posts HLL sketch", "error", err)
			continue
		}
	}
	postsPerDay := int64(postsSketch.Estimate())

	postersHLLs, err := c.queries.GetHLLsByMetricInRange(ctx, statsqueries.GetHLLsByMetricInRangeParams{
		MetricName:  postersKey,
		WindowStart: startOfDay,
		WindowEnd:   endOfDay,
	})
	if err != nil {
		return fmt.Errorf("failed to get posters HLLs: %w", err)
	}
	postersSketch := hyperloglog.New16()
	for _, hllRow := range postersHLLs {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(hllRow.Hll); err != nil {
			c.logger.Error("failed to unmarshal posters HLL sketch", "error", err)
			continue
		}
		if err := postersSketch.Merge(sketch); err != nil {
			c.logger.Error("failed to merge posters HLL sketch", "error", err)
			continue
		}
	}
	dailyActivePosters := int64(postersSketch.Estimate())

	followsHLLs, err := c.queries.GetHLLsByMetricInRange(ctx, statsqueries.GetHLLsByMetricInRangeParams{
		MetricName:  followsKey,
		WindowStart: startOfDay,
		WindowEnd:   endOfDay,
	})
	if err != nil {
		return fmt.Errorf("failed to get follows HLLs: %w", err)
	}
	followsSketch := hyperloglog.New16()
	for _, hllRow := range followsHLLs {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(hllRow.Hll); err != nil {
			c.logger.Error("failed to unmarshal follows HLL sketch", "error", err)
			continue
		}
		if err := followsSketch.Merge(sketch); err != nil {
			c.logger.Error("failed to merge follows HLL sketch", "error", err)
			continue
		}
	}
	followsPerDay := int64(followsSketch.Estimate())

	followersHLLs, err := c.queries.GetHLLsByMetricInRange(ctx, statsqueries.GetHLLsByMetricInRangeParams{
		MetricName:  followersKey,
		WindowStart: startOfDay,
		WindowEnd:   endOfDay,
	})
	if err != nil {
		return fmt.Errorf("failed to get followers HLLs: %w", err)
	}
	followersSketch := hyperloglog.New16()
	for _, hllRow := range followersHLLs {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(hllRow.Hll); err != nil {
			c.logger.Error("failed to unmarshal followers HLL sketch", "error", err)
			continue
		}
		if err := followersSketch.Merge(sketch); err != nil {
			c.logger.Error("failed to merge followers HLL sketch", "error", err)
			continue
		}
	}
	dailyActiveFollowers := int64(followersSketch.Estimate())

	blocksHLLs, err := c.queries.GetHLLsByMetricInRange(ctx, statsqueries.GetHLLsByMetricInRangeParams{
		MetricName:  blocksKey,
		WindowStart: startOfDay,
		WindowEnd:   endOfDay,
	})
	if err != nil {
		return fmt.Errorf("failed to get blocks HLLs: %w", err)
	}
	blocksSketch := hyperloglog.New16()
	for _, hllRow := range blocksHLLs {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(hllRow.Hll); err != nil {
			c.logger.Error("failed to unmarshal blocks HLL sketch", "error", err)
			continue
		}
		if err := blocksSketch.Merge(sketch); err != nil {
			c.logger.Error("failed to merge blocks HLL sketch", "error", err)
			continue
		}
	}
	blocksPerDay := int64(blocksSketch.Estimate())

	blockersHLLs, err := c.queries.GetHLLsByMetricInRange(ctx, statsqueries.GetHLLsByMetricInRangeParams{
		MetricName:  blockersKey,
		WindowStart: startOfDay,
		WindowEnd:   endOfDay,
	})
	if err != nil {
		return fmt.Errorf("failed to get blockers HLLs: %w", err)
	}
	blockersSketch := hyperloglog.New16()
	for _, hllRow := range blockersHLLs {
		sketch := hyperloglog.New16()
		if err := sketch.UnmarshalBinary(hllRow.Hll); err != nil {
			c.logger.Error("failed to unmarshal blockers HLL sketch", "error", err)
			continue
		}
		if err := blockersSketch.Merge(sketch); err != nil {
			c.logger.Error("failed to merge blockers HLL sketch", "error", err)
			continue
		}
	}
	dailyActiveBlockers := int64(blockersSketch.Estimate())

	// Insert or update the daily stats summary

	if err := c.queries.InsertDailyStatsSummary(ctx, statsqueries.InsertDailyStatsSummaryParams{
		Date:                 startOfDay,
		DailyActiveUsers:     dau,
		LikesPerDay:          likesPerDay,
		DailyActiveLikers:    dailyActiveLikers,
		PostsPerDay:          postsPerDay,
		DailyActivePosters:   dailyActivePosters,
		FollowsPerDay:        followsPerDay,
		DailyActiveFollowers: dailyActiveFollowers,
		BlocksPerDay:         blocksPerDay,
		DailyActiveBlockers:  dailyActiveBlockers,
	}); err != nil {
		return fmt.Errorf("failed to insert/update daily stats summary: %w", err)
	}

	c.logger.Info("updated daily stats summary", "date", startOfDay.Format("2006-01-02"),
		"daily_active_users", dau)

	return nil
}

func (c *Consumer) trimOldHLLs(ctx context.Context) error {
	c.hllLock.Lock()
	defer c.hllLock.Unlock()

	c.logger.Info("trimming old HLLs")

	deletedFromMemory := 0
	now := time.Now().UTC()
	for key, hll := range c.hll {
		if now.After(hll.deleteAfter) {
			delete(c.hll, key)
			deletedFromMemory++
		}
	}

	c.logger.Info("trimmed old HLLs from memory", "count", deletedFromMemory)

	if err := c.queries.DeleteOldHLL(ctx); err != nil {
		return fmt.Errorf("failed to delete old HLLs from database: %w", err)
	}

	c.logger.Info("trimmed old HLLs from database")

	return nil
}

func getHLLKey(activity string, timeUS int64) string {
	t := time.UnixMicro(timeUS).UTC()
	// Group by day
	return fmt.Sprintf("%s|%d-%02d-%02d", activity, t.Year(), t.Month(), t.Day())
}

func parseHLLKey(hllKey string) (string, time.Time, error) {
	parts := strings.Split(hllKey, "|")
	if len(parts) != 2 {
		return "", time.Time{}, fmt.Errorf("invalid HLL key format: %q", hllKey)
	}
	activity := parts[0]
	dateStr := parts[1]
	// Parse date
	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to parse date in HLL key %q: %w", hllKey, err)
	}
	return activity, date, nil
}

func (c *Consumer) addToHLL(hllKey string, valKey string) {
	c.hllLock.Lock()
	defer c.hllLock.Unlock()

	activity, date, err := parseHLLKey(hllKey)
	if err != nil {
		c.logger.Error("failed to parse HLL key", "error", err)
		return
	}

	hll, exists := c.hll[hllKey]
	if !exists {
		sketch := hyperloglog.New16()
		hll = &HLL{
			sketch:      sketch,
			windowStart: date,
			windowEnd:   date.Add(24 * time.Hour),
			activity:    activity,
			deleteAfter: date.Add(7 * 24 * time.Hour),
		}
		c.hll[hllKey] = hll
	}

	hll.sketch.Insert([]byte(valKey))
}

func (c *Consumer) HandleEvent(ctx context.Context, event *models.Event) error {
	ctx, span := tracer.Start(ctx, "HandleEvent")
	defer func() {
		c.updateCursor(event.TimeUS)
		span.End()
	}()

	isActivity := false

	if event.Commit != nil {
		uri := fmt.Sprintf("at://%s/%s/%s", event.Did, event.Commit.Collection, event.Commit.RKey)
		switch event.Commit.Operation {
		case models.CommitOperationCreate:
			isActivity = true
			c.addToHLL(getHLLKey(fmt.Sprintf("actors_create_%s", event.Commit.Collection), event.TimeUS), event.Did)
			c.addToHLL(getHLLKey(fmt.Sprintf("records_create_%s", event.Commit.Collection), event.TimeUS), uri)
		case models.CommitOperationUpdate:
			isActivity = true
			c.addToHLL(getHLLKey(fmt.Sprintf("actors_update_%s", event.Commit.Collection), event.TimeUS), event.Did)
			c.addToHLL(getHLLKey(fmt.Sprintf("records_update_%s", event.Commit.Collection), event.TimeUS), uri)
		case models.CommitOperationDelete:
			isActivity = true
			c.addToHLL(getHLLKey(fmt.Sprintf("actors_delete_%s", event.Commit.Collection), event.TimeUS), event.Did)
			c.addToHLL(getHLLKey(fmt.Sprintf("records_delete_%s", event.Commit.Collection), event.TimeUS), uri)
		}
	}

	if isActivity {
		c.addToHLL(getHLLKey("dau", event.TimeUS), event.Did)
	}

	return nil
}
