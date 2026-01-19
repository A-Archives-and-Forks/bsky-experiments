package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/jazware/bsky-experiments/pkg/migrate"
	"github.com/urfave/cli/v2"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	app := &cli.App{
		Name:  "migrate",
		Usage: "ClickHouse schema migrations for atproto",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "clickhouse-address",
				Usage:   "ClickHouse address (host:port)",
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
			&cli.StringFlag{
				Name:    "clickhouse-database",
				Usage:   "ClickHouse database",
				Value:   "default",
				EnvVars: []string{"CLICKHOUSE_DATABASE"},
			},
			&cli.IntFlag{
				Name:    "read-timeout",
				Usage:   "ClickHouse read timeout in seconds (for long migrations)",
				Value:   0,
				EnvVars: []string{"CLICKHOUSE_READ_TIMEOUT"},
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "up",
				Usage: "Run all pending migrations",
				Action: func(c *cli.Context) error {
					m := migrate.NewMigrator(configFromCLI(c), logger)
					if err := m.Up(); err != nil {
						return fmt.Errorf("migration failed: %w", err)
					}
					return nil
				},
			},
			{
				Name:      "down",
				Usage:     "Rollback migrations",
				ArgsUsage: "[steps]",
				Action: func(c *cli.Context) error {
					steps := 1
					if c.NArg() > 0 {
						var err error
						steps, err = strconv.Atoi(c.Args().First())
						if err != nil {
							return fmt.Errorf("invalid steps argument: %w", err)
						}
					}

					m := migrate.NewMigrator(configFromCLI(c), logger)
					if err := m.Down(steps); err != nil {
						return fmt.Errorf("rollback failed: %w", err)
					}
					return nil
				},
			},
			{
				Name:  "version",
				Usage: "Show current migration version",
				Action: func(c *cli.Context) error {
					m := migrate.NewMigrator(configFromCLI(c), logger)
					version, dirty, err := m.Version()
					if err != nil {
						return fmt.Errorf("failed to get version: %w", err)
					}
					fmt.Printf("Version: %d\n", version)
					fmt.Printf("Dirty: %v\n", dirty)
					return nil
				},
			},
			{
				Name:      "force",
				Usage:     "Force set migration version (use to fix dirty state)",
				ArgsUsage: "<version>",
				Action: func(c *cli.Context) error {
					if c.NArg() < 1 {
						return fmt.Errorf("version argument required")
					}
					version, err := strconv.Atoi(c.Args().First())
					if err != nil {
						return fmt.Errorf("invalid version argument: %w", err)
					}

					m := migrate.NewMigrator(configFromCLI(c), logger)
					if err := m.Force(version); err != nil {
						return fmt.Errorf("force failed: %w", err)
					}
					return nil
				},
			},
			{
				Name:  "dump-schema",
				Usage: "Dump current schema from ClickHouse (requires running database)",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "output",
						Usage:   "Output file (default: stdout)",
						Aliases: []string{"o"},
					},
				},
				Action: func(c *cli.Context) error {
					m := migrate.NewMigrator(configFromCLI(c), logger)

					var w *os.File = os.Stdout
					if output := c.String("output"); output != "" {
						f, err := os.Create(output)
						if err != nil {
							return fmt.Errorf("failed to create output file: %w", err)
						}
						defer f.Close()
						w = f
					}

					if err := m.DumpSchema(context.Background(), w); err != nil {
						return fmt.Errorf("dump failed: %w", err)
					}
					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		logger.Error("command failed", "error", err)
		os.Exit(1)
	}
}

func configFromCLI(c *cli.Context) migrate.Config {
	return migrate.Config{
		Address:     c.String("clickhouse-address"),
		Username:    c.String("clickhouse-username"),
		Password:    c.String("clickhouse-password"),
		Database:    c.String("clickhouse-database"),
		ReadTimeout: c.Int("read-timeout"),
	}
}
