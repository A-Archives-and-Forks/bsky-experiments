package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jazware/bsky-experiments/pkg/indexer/store"
	"github.com/jazware/bsky-experiments/pkg/search"
	"github.com/jazware/bsky-experiments/pkg/search/endpoints"
	"github.com/jazware/bsky-experiments/pkg/usercount"
	"github.com/jazware/bsky-experiments/telemetry"
	"github.com/jazware/bsky-experiments/version"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	slogecho "github.com/samber/slog-echo"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
)

func main() {
	app := cli.App{
		Name:    "search",
		Usage:   "bluesky search and stats service",
		Version: version.String(),
	}

	app.Flags = []cli.Flag{
		telemetry.CLIFlagDebug,
		telemetry.CLIFlagMetricsListenAddress,
		telemetry.CLIFlagServiceName,
		telemetry.CLIFlagTracingSampleRatio,
		&cli.StringFlag{
			Name:    "listen-address",
			Usage:   "listen address for HTTP server",
			Value:   "0.0.0.0:8080",
			EnvVars: []string{"LISTEN_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "redis-address",
			Usage:   "redis address for caching",
			Value:   "localhost:6379",
			EnvVars: []string{"REDIS_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "clickhouse-address",
			Usage:   "clickhouse address for storing records",
			Value:   "localhost:9000",
			EnvVars: []string{"CLICKHOUSE_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "clickhouse-username",
			Usage:   "clickhouse username",
			Value:   "default",
			EnvVars: []string{"CLICKHOUSE_USERNAME"},
		},
		&cli.StringFlag{
			Name:    "clickhouse-password",
			Usage:   "clickhouse password",
			Value:   "",
			EnvVars: []string{"CLICKHOUSE_PASSWORD"},
		},
		&cli.StringFlag{
			Name:    "magic-header-val",
			Usage:   "magic header value for protected endpoints",
			Value:   "",
			EnvVars: []string{"MAGIC_HEADER_VAL"},
		},
		&cli.DurationFlag{
			Name:    "stats-cache-ttl",
			Usage:   "duration to cache stats before refresh",
			Value:   30 * time.Second,
			EnvVars: []string{"STATS_CACHE_TTL"},
		},
	}

	app.Action = Search

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
		os.Exit(1)
	}
}

func Search(cctx *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handlers
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Initialize logger
	logger := telemetry.StartLogger(cctx)
	logger.Info("starting search service",
		"version", version.Version,
		"commit", version.GitCommit)

	// Initialize metrics
	telemetry.StartMetrics(cctx)

	// Initialize tracing if configured
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		logger.Info("initializing tracer")
		shutdown, err := telemetry.StartTracing(cctx)
		if err != nil {
			return fmt.Errorf("failed to start tracing: %w", err)
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				logger.Error("failed to shutdown tracer", "error", err)
			}
		}()
	}

	// Connect to ClickHouse
	logger.Info("connecting to clickhouse", "address", cctx.String("clickhouse-address"))
	chStore, err := store.NewStore(
		cctx.String("clickhouse-address"),
		cctx.String("clickhouse-username"),
		cctx.String("clickhouse-password"),
	)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse store: %w", err)
	}
	defer chStore.Close()
	logger.Info("connected to clickhouse")

	// Connect to Redis
	logger.Info("connecting to redis", "address", cctx.String("redis-address"))
	redisClient := redis.NewClient(&redis.Options{
		Addr: cctx.String("redis-address"),
	})

	// Enable tracing instrumentation for Redis
	if err := redisotel.InstrumentTracing(redisClient); err != nil {
		return fmt.Errorf("failed to instrument redis with tracing: %w", err)
	}

	// Enable metrics instrumentation for Redis
	if err := redisotel.InstrumentMetrics(redisClient); err != nil {
		return fmt.Errorf("failed to instrument redis with metrics: %w", err)
	}

	// Test the connection to redis
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to connect to redis: %w", err)
	}
	logger.Info("connected to redis")

	userCount := usercount.NewUserCount(ctx, redisClient)

	// Create search service
	searchService, err := search.NewSearchService(
		ctx,
		logger,
		chStore,
		userCount,
		redisClient,
		cctx.Duration("stats-cache-ttl"),
	)
	if err != nil {
		return fmt.Errorf("failed to create search service: %w", err)
	}

	// Create endpoints
	api, err := endpoints.NewAPI(
		logger,
		searchService,
		chStore,
		cctx.String("magic-header-val"),
	)
	if err != nil {
		return fmt.Errorf("failed to create API: %w", err)
	}

	// Start background workers
	logger.Info("starting stats refresh worker")
	go searchService.StartStatsRefreshWorker(ctx)

	logger.Info("starting cleanup daemon")
	go api.RunCleanupDaemon(ctx)

	// Setup Echo router
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	// Recovery middleware
	e.Use(middleware.Recover())

	// Structured logging middleware
	e.Use(slogecho.NewWithFilters(
		logger,
		slogecho.IgnorePath("/metrics"),
	))

	// Serve static files from the public folder
	e.Static("/public", "./public")

	// OTEL Middleware
	e.Use(otelecho.Middleware(
		"search-api",
		otelecho.WithSkipper(func(c echo.Context) bool {
			return c.Request().URL.Path == "/metrics"
		}),
	))

	// CORS middleware
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"https://bsky.jazco.dev"},
		AllowMethods: []string{http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodOptions},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentLength, echo.HeaderContentType},
		AllowOriginFunc: func(origin string) (bool, error) {
			u, err := url.Parse(origin)
			if err != nil {
				return false, nil
			}
			switch u.Hostname() {
			case "bsky.jazco.dev", "localhost", "10.0.6.40":
				return true, nil
			}

			return false, nil
		},
	}))

	// Prometheus metrics endpoint
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))

	// Register routes
	e.GET("/stats", api.GetStats)
	e.GET("/redir", api.RedirectAtURI)
	e.GET("/repo/:did", api.GetRepoAsJSON)
	e.GET("/list/members", api.GetListMembers)
	e.GET("/repo/cleanup", api.GetCleanupStatus)
	e.POST("/repo/cleanup", api.CleanupOldRecords)
	e.DELETE("/repo/cleanup", api.CancelCleanupJob)
	e.GET("/repo/cleanup/stats", api.GetCleanupStats)

	// Start HTTP server in a goroutine
	serverErr := make(chan error, 1)

	go func() {
		logger.Info("starting HTTP server", "listen_address", cctx.String("listen-address"))
		if err := e.Start(cctx.String("listen-address")); err != nil && err != http.ErrServerClosed {
			serverErr <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	// Wait for shutdown signal or error
	select {
	case <-signals:
		logger.Info("shutting down on signal")
	case <-ctx.Done():
		logger.Info("shutting down on context done")
	case err := <-serverErr:
		logger.Error("server error", "error", err)
		return err
	}

	logger.Info("beginning graceful shutdown")

	// Force shutdown after timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := e.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shut down HTTP server gracefully", "error", err)
		return err
	}

	// Cleanup search service
	if err := searchService.Shutdown(); err != nil {
		logger.Error("failed to shut down search service", "error", err)
		return err
	}

	logger.Info("shut down successfully")
	return nil
}
