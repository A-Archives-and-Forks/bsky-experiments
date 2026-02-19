package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"
)

func resetCommand() *cli.Command {
	return &cli.Command{
		Name:  "reset",
		Usage: "Clear output .rca segment files",
		Flags: []cli.Flag{
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
	outputDir := cctx.String("output-dir")
	if outputDir == "" {
		fmt.Println("No output directory specified, nothing to do")
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
