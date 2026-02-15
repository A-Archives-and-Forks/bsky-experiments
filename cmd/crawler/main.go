package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jazware/bsky-experiments/pkg/crawler"
	"github.com/jazware/bsky-experiments/pkg/indexer/store"
	"github.com/jazware/bsky-experiments/pkg/repoarchive"
	"github.com/jazware/bsky-experiments/telemetry"
	"github.com/jazware/bsky-experiments/version"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:    "crawler",
		Usage:   "ATProto network repo crawler and archive tool",
		Version: version.String(),
		Flags: []cli.Flag{
			telemetry.CLIFlagDebug,
			telemetry.CLIFlagMetricsListenAddress,
			telemetry.CLIFlagServiceName,
			telemetry.CLIFlagTracingSampleRatio,
		},
		Commands: []*cli.Command{
			crawlCommand(),
			replayCommand(),
			inspectCommand(),
			statusCommand(),
			resetCommand(),
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func commonFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "redis-address",
			Usage:   "Redis address",
			Value:   "localhost:6379",
			EnvVars: []string{"REDIS_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "clickhouse-address",
			Usage:   "ClickHouse address",
			Value:   "localhost:9000",
			EnvVars: []string{"CLICKHOUSE_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "clickhouse-username",
			Usage:   "ClickHouse username",
			Value:   "default",
			EnvVars: []string{"CLICKHOUSE_USERNAME"},
		},
		&cli.StringFlag{
			Name:    "clickhouse-password",
			Usage:   "ClickHouse password",
			Value:   "",
			EnvVars: []string{"CLICKHOUSE_PASSWORD"},
		},
	}
}

func setupRedis(ctx context.Context, cctx *cli.Context) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: cctx.String("redis-address"),
	})
	if err := redisotel.InstrumentTracing(client); err != nil {
		return nil, fmt.Errorf("redis tracing: %w", err)
	}
	if err := redisotel.InstrumentMetrics(client); err != nil {
		return nil, fmt.Errorf("redis metrics: %w", err)
	}
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return client, nil
}

func setupClickHouse(cctx *cli.Context) (driver.Conn, error) {
	s, err := store.NewStore(
		cctx.String("clickhouse-address"),
		cctx.String("clickhouse-username"),
		cctx.String("clickhouse-password"),
	)
	if err != nil {
		return nil, err
	}
	return s.DB, nil
}

// --- crawl command ---

func crawlCommand() *cli.Command {
	flags := append(commonFlags(),
		&cli.StringFlag{
			Name:    "output-dir",
			Usage:   "Directory to write .rca segment files",
			Value:   "./data/rca",
			EnvVars: []string{"OUTPUT_DIR"},
		},
		&cli.IntFlag{
			Name:    "workers",
			Usage:   "Number of concurrent crawl workers",
			Value:   50,
			EnvVars: []string{"WORKERS"},
		},
		&cli.Float64Flag{
			Name:    "default-pds-rps",
			Usage:   "Default requests per second per PDS",
			Value:   5,
			EnvVars: []string{"DEFAULT_PDS_RPS"},
		},
		&cli.StringFlag{
			Name:    "segment-size",
			Usage:   "Target segment file size (e.g. 2GB, 500MB)",
			Value:   "2GB",
			EnvVars: []string{"SEGMENT_SIZE"},
		},
		&cli.IntFlag{
			Name:    "zstd-level",
			Usage:   "Zstd compression level (1-19)",
			Value:   3,
			EnvVars: []string{"ZSTD_LEVEL"},
		},
		&cli.IntFlag{
			Name:    "page-size",
			Usage:   "Number of DIDs to fetch per page from ClickHouse",
			Value:   50000,
			EnvVars: []string{"PAGE_SIZE"},
		},
		&cli.StringSliceFlag{
			Name:    "skip-pds",
			Usage:   "PDS URLs to skip (can be repeated)",
			EnvVars: []string{"SKIP_PDS"},
		},
		&cli.BoolFlag{
			Name:  "resume",
			Usage: "Resume an interrupted crawl",
		},
	)

	return &cli.Command{
		Name:  "crawl",
		Usage: "Crawl all ATProto repos from the network",
		Flags: flags,
		Action: func(cctx *cli.Context) error {
			return runCrawl(cctx)
		},
	}
}

func runCrawl(cctx *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	logger := telemetry.StartLogger(cctx)
	telemetry.StartMetrics(cctx)

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		shutdown, err := telemetry.StartTracing(cctx)
		if err != nil {
			return fmt.Errorf("starting tracing: %w", err)
		}
		defer shutdown(ctx)
	}

	redisClient, err := setupRedis(ctx, cctx)
	if err != nil {
		return fmt.Errorf("redis setup: %w", err)
	}

	chConn, err := setupClickHouse(cctx)
	if err != nil {
		return fmt.Errorf("clickhouse setup: %w", err)
	}

	segmentSize, err := parseSize(cctx.String("segment-size"))
	if err != nil {
		return fmt.Errorf("invalid segment-size: %w", err)
	}

	c, err := crawler.NewCrawler(crawler.Config{
		OutputDir:      cctx.String("output-dir"),
		Workers:        cctx.Int("workers"),
		DefaultPDSRPS:  cctx.Float64("default-pds-rps"),
		SegmentSize:    segmentSize,
		ZstdLevel:      cctx.Int("zstd-level"),
		PageSize:       cctx.Int("page-size"),
		SkipPDS:        cctx.StringSlice("skip-pds"),
		ClickHouseConn: chConn,
		RedisClient:    redisClient,
		Logger:         logger,
	})
	if err != nil {
		return fmt.Errorf("creating crawler: %w", err)
	}

	// Run crawl in background, handle signals.
	crawlDone := make(chan error, 1)
	go func() {
		crawlDone <- c.Run(ctx)
	}()

	select {
	case err := <-crawlDone:
		return err
	case <-signals:
		logger.Info("shutting down on signal...")
		cancel()
		// Force shutdown after 30s.
		go func() {
			<-time.After(30 * time.Second)
			log.Fatal("failed to shut down in time, forcing exit")
		}()
		c.Shutdown()
		return <-crawlDone
	}
}

// --- replay command ---

func replayCommand() *cli.Command {
	flags := append(commonFlags(),
		&cli.StringFlag{
			Name:    "input-dir",
			Usage:   "Directory containing .rca segment files",
			Value:   "./data/rca",
			EnvVars: []string{"INPUT_DIR"},
		},
		&cli.StringSliceFlag{
			Name:    "collections",
			Usage:   "Collections to replay (comma-separated). Empty means all.",
			EnvVars: []string{"COLLECTIONS"},
		},
	)

	return &cli.Command{
		Name:  "replay",
		Usage: "Replay .rca archives into ClickHouse repo_records table",
		Flags: flags,
		Action: func(cctx *cli.Context) error {
			return runReplay(cctx)
		},
	}
}

func runReplay(cctx *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	logger := telemetry.StartLogger(cctx)

	chConn, err := setupClickHouse(cctx)
	if err != nil {
		return fmt.Errorf("clickhouse setup: %w", err)
	}

	inputDir := cctx.String("input-dir")
	collections := cctx.StringSlice("collections")

	// Find all .rca files.
	segments, err := filepath.Glob(filepath.Join(inputDir, "*.rca"))
	if err != nil {
		return fmt.Errorf("listing segments: %w", err)
	}
	sort.Strings(segments)

	if len(segments) == 0 {
		return fmt.Errorf("no .rca files found in %s", inputDir)
	}

	logger.Info("replaying segments", "count", len(segments), "collections", collections)

	totalRecords := 0
	for _, segPath := range segments {
		if ctx.Err() != nil {
			break
		}

		logger.Info("replaying segment", "path", segPath)
		count, err := replaySegment(ctx, chConn, segPath, collections, logger)
		if err != nil {
			logger.Error("failed to replay segment", "path", segPath, "error", err)
			continue
		}
		totalRecords += count
		logger.Info("segment replayed", "path", segPath, "records", count)
	}

	logger.Info("replay complete", "total_records", totalRecords)
	return nil
}

func replaySegment(ctx context.Context, db driver.Conn, segPath string, collections []string, logger *slog.Logger) (int, error) {
	reader, err := repoarchive.OpenSegment(segPath)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	if len(collections) > 0 {
		reader.SetCollectionFilter(collections...)
	}

	const batchSize = 50000
	count := 0

	var batch struct {
		repos       []string
		collections []string
		rkeys       []string
		operations  []string
		recordJSONs []string
		createdAts  []time.Time
		timeUSs     []int64
	}

	flush := func() error {
		if len(batch.repos) == 0 {
			return nil
		}
		batchInsert, err := db.PrepareBatch(ctx,
			"INSERT INTO repo_records (repo, collection, rkey, operation, record_json, created_at, time_us)")
		if err != nil {
			return fmt.Errorf("preparing batch: %w", err)
		}
		for i := range batch.repos {
			if err := batchInsert.Append(
				batch.repos[i],
				batch.collections[i],
				batch.rkeys[i],
				batch.operations[i],
				batch.recordJSONs[i],
				batch.createdAts[i],
				batch.timeUSs[i],
			); err != nil {
				return fmt.Errorf("appending to batch: %w", err)
			}
		}
		if err := batchInsert.Send(); err != nil {
			return fmt.Errorf("sending batch: %w", err)
		}
		count += len(batch.repos)
		batch.repos = batch.repos[:0]
		batch.collections = batch.collections[:0]
		batch.rkeys = batch.rkeys[:0]
		batch.operations = batch.operations[:0]
		batch.recordJSONs = batch.recordJSONs[:0]
		batch.createdAts = batch.createdAts[:0]
		batch.timeUSs = batch.timeUSs[:0]
		return nil
	}

	for reader.Next() {
		if ctx.Err() != nil {
			break
		}
		repo := reader.Repo()
		for repo.NextCollection() {
			col := repo.Collection()
			for col.NextRecord() {
				rec, err := col.Record()
				if err != nil {
					logger.Warn("skipping record", "did", repo.DID, "collection", col.Name, "error", err)
					continue
				}
				batch.repos = append(batch.repos, repo.DID)
				batch.collections = append(batch.collections, col.Name)
				batch.rkeys = append(batch.rkeys, rec.RKey)
				batch.operations = append(batch.operations, "create")
				batch.recordJSONs = append(batch.recordJSONs, string(rec.JSON))
				batch.createdAts = append(batch.createdAts, repo.CrawledAt)
				batch.timeUSs = append(batch.timeUSs, repo.CrawledAt.UnixMicro())

				if len(batch.repos) >= batchSize {
					if err := flush(); err != nil {
						return count, err
					}
				}
			}
		}
	}

	if err := flush(); err != nil {
		return count, err
	}

	return count, nil
}

// --- inspect command ---

func inspectCommand() *cli.Command {
	return &cli.Command{
		Name:      "inspect",
		Usage:     "Inspect an .rca segment file",
		ArgsUsage: "<segment-file>",
		Action: func(cctx *cli.Context) error {
			return runInspect(cctx)
		},
	}
}

func runInspect(cctx *cli.Context) error {
	path := cctx.Args().First()
	if path == "" {
		return fmt.Errorf("segment file path required")
	}

	reader, err := repoarchive.OpenSegment(path)
	if err != nil {
		return fmt.Errorf("opening segment: %w", err)
	}
	defer reader.Close()

	header := reader.Header()
	fmt.Printf("Segment: %s\n", path)
	fmt.Printf("  Version:     %d\n", header.Version)
	fmt.Printf("  Created:     %s\n", time.UnixMicro(header.CreatedAt).Format(time.RFC3339))
	fmt.Printf("  Repo Count:  %d\n", header.RepoCount)
	fmt.Printf("  Index Offset: %d\n", header.IndexOffset)
	fmt.Println()

	// Load index for summary.
	index, err := repoarchive.LoadIndex(path)
	if err != nil {
		fmt.Printf("  (index not readable: %v)\n", err)
	} else {
		fmt.Printf("  Index Entries: %d\n", len(index.Entries))
	}

	// Scan repos and summarize collections.
	collectionStats := make(map[string]struct {
		repos   int
		records int
	})
	repoCount := 0

	for reader.Next() {
		repo := reader.Repo()
		repoCount++
		for repo.NextCollection() {
			col := repo.Collection()
			stats := collectionStats[col.Name]
			stats.repos++
			recordCount := 0
			for col.NextRecord() {
				if _, err := col.Record(); err == nil {
					recordCount++
				}
			}
			stats.records += recordCount
			collectionStats[col.Name] = stats
		}
	}

	fmt.Printf("\n  Repos scanned: %d\n", repoCount)
	fmt.Println("  Collections:")

	// Sort collection names for display.
	names := make([]string, 0, len(collectionStats))
	for name := range collectionStats {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		stats := collectionStats[name]
		fmt.Printf("    %-50s  repos: %6d  records: %8d\n", name, stats.repos, stats.records)
	}

	return nil
}

// --- status command ---

func statusCommand() *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "Show crawl progress from Redis",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "redis-address",
				Usage:   "Redis address",
				Value:   "localhost:6379",
				EnvVars: []string{"REDIS_ADDRESS"},
			},
		},
		Action: func(cctx *cli.Context) error {
			return runStatus(cctx)
		},
	}
}

func runStatus(cctx *cli.Context) error {
	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{
		Addr: cctx.String("redis-address"),
	})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	progress := crawler.NewProgress(redisClient, logger)

	stats, err := progress.GetStats(ctx)
	if err != nil {
		return fmt.Errorf("getting stats: %w", err)
	}

	if len(stats) == 0 {
		fmt.Println("No crawl progress found.")
		return nil
	}

	fmt.Println("Crawl Progress:")
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-25s %s\n", k+":", stats[k])
	}

	return nil
}

// --- reset command ---

func resetCommand() *cli.Command {
	return &cli.Command{
		Name:  "reset",
		Usage: "Clear output data, Redis cursors, and all crawl progress",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "redis-address",
				Usage:   "Redis address",
				Value:   "localhost:6379",
				EnvVars: []string{"REDIS_ADDRESS"},
			},
			&cli.StringFlag{
				Name:    "output-dir",
				Usage:   "Directory containing .rca segment files",
				Value:   "./data/rca",
				EnvVars: []string{"OUTPUT_DIR"},
			},
		},
		Action: func(cctx *cli.Context) error {
			return runReset(cctx)
		},
	}
}

func runReset(cctx *cli.Context) error {
	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{
		Addr: cctx.String("redis-address"),
	})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	progress := crawler.NewProgress(redisClient, logger)

	// Clear Redis keys.
	deleted, err := progress.ClearAll(ctx)
	if err != nil {
		return fmt.Errorf("clearing Redis keys: %w", err)
	}
	fmt.Printf("Cleared %d Redis keys\n", deleted)

	// Remove .rca files from output directory.
	outputDir := cctx.String("output-dir")
	segments, err := filepath.Glob(filepath.Join(outputDir, "*.rca"))
	if err != nil {
		return fmt.Errorf("listing segments: %w", err)
	}
	for _, seg := range segments {
		if err := os.Remove(seg); err != nil {
			fmt.Printf("  failed to remove %s: %v\n", seg, err)
		} else {
			fmt.Printf("  removed %s\n", seg)
		}
	}
	if len(segments) == 0 {
		fmt.Printf("No .rca files found in %s\n", outputDir)
	} else {
		fmt.Printf("Removed %d .rca files\n", len(segments))
	}

	return nil
}

// --- helpers ---

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	multiplier := int64(1)
	switch {
	case strings.HasSuffix(s, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "TB")
	case strings.HasSuffix(s, "GB"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	}
	var val float64
	if _, err := fmt.Sscanf(s, "%f", &val); err != nil {
		return 0, fmt.Errorf("invalid size: %s", s)
	}
	return int64(val * float64(multiplier)), nil
}
