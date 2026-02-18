package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jazware/bsky-experiments/pkg/crawler"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v2"
)

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
	if outputDir == "" {
		fmt.Println("No output directory specified, skipping .rca file removal")
		return nil
	}

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
