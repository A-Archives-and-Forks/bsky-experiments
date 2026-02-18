package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jazware/bsky-experiments/pkg/crawler"
	"github.com/jazware/bsky-experiments/telemetry"
	"github.com/urfave/cli/v2"
)

func prepareCommand() *cli.Command {
	flags := append(commonFlags(),
		&cli.StringFlag{
			Name:    "relay-host",
			Usage:   "Relay host URL for PDS discovery",
			Value:   "https://bsky.network",
			EnvVars: []string{"RELAY_HOST"},
		},
		&cli.IntFlag{
			Name:    "discovery-workers",
			Usage:   "Concurrent PDS enumeration workers",
			Value:   50,
			EnvVars: []string{"DISCOVERY_WORKERS"},
		},
		&cli.Float64Flag{
			Name:    "discovery-rps",
			Usage:   "Per-PDS rate limit for listRepos",
			Value:   5,
			EnvVars: []string{"DISCOVERY_RPS"},
		},
		&cli.Int64Flag{
			Name:    "list-repos-limit",
			Usage:   "Page size for listRepos calls",
			Value:   1000,
			EnvVars: []string{"LIST_REPOS_LIMIT"},
		},
		&cli.StringSliceFlag{
			Name:    "skip-pds",
			Usage:   "PDS URLs to skip (can be repeated)",
			EnvVars: []string{"SKIP_PDS"},
		},
		&cli.BoolFlag{
			Name:    "verify",
			Usage:   "Verify PDS claims against DID documents",
			Value:   true,
			EnvVars: []string{"VERIFY_PDS"},
		},
	)

	return &cli.Command{
		Name:  "prepare",
		Usage: "Build a verified crawl list from relay-discovered PDSs",
		Flags: flags,
		Action: func(cctx *cli.Context) error {
			return runPrepare(cctx)
		},
	}
}

func runPrepare(cctx *cli.Context) error {
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

	chConn, err := setupClickHouse(cctx)
	if err != nil {
		return fmt.Errorf("clickhouse setup: %w", err)
	}

	config := crawler.PrepareConfig{
		DiscoveryConfig: crawler.DiscoveryConfig{
			RelayHost:      cctx.String("relay-host"),
			Workers:        cctx.Int("discovery-workers"),
			DiscoveryRPS:   cctx.Float64("discovery-rps"),
			ListReposLimit: cctx.Int64("list-repos-limit"),
			SkipPDS:        cctx.StringSlice("skip-pds"),
			Logger:         logger,
		},
		ClickHouseConn: chConn,
		VerifyPDS:      cctx.Bool("verify"),
		Logger:         logger,
	}

	prepareDone := make(chan error, 1)
	go func() {
		prepareDone <- crawler.RunPrepare(ctx, config)
	}()

	select {
	case err := <-prepareDone:
		return err
	case <-signals:
		logger.Info("shutting down on signal...")
		cancel()
		return <-prepareDone
	}
}
