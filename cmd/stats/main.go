package main

import (
	"fmt"
	"os"

	"github.com/jazware/bsky-experiments/pkg/telemetry"
	cli "github.com/urfave/cli/v2"
	_ "go.uber.org/automaxprocs"
)

func main() {
	app := cli.App{
		Name:  "stats",
		Usage: "A Jetstream consumer that generates stats from the ATmosphere",
	}
	app.Flags = []cli.Flag{
		telemetry.CLIFlagDebug,
		telemetry.CLIFlagMetricsListenAddress,
	}
	app.Commands = []*cli.Command{
		consumeCmd,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
		os.Exit(1)
	}
}
