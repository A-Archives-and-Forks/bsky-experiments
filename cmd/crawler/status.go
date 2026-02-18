package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"

	"github.com/jazware/bsky-experiments/pkg/crawler"
	"github.com/redis/go-redis/v9"
	"github.com/urfave/cli/v2"
)

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
