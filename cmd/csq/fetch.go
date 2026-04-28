// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/snapshot"
)

func runFetch(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	var (
		from       string
		outputPath string
		noVerify   bool
		force      bool
	)
	fs.StringVar(&from, "from", "", "URL to fetch from (http://, https://, or file://)")
	fs.StringVar(&outputPath, "output", "", "Destination DuckDB path (default: manifest's duckdb_filename in the current directory)")
	fs.BoolVar(&noVerify, "no-verify", false, "Skip SHA-256 verification")
	fs.BoolVar(&force, "force", false, "Overwrite --output if it exists")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if from == "" {
		return fmt.Errorf("--from is required")
	}

	m, err := snapshot.Fetch(context.Background(), snapshot.ConsumerOptions{
		URL:        from,
		OutputPath: outputPath,
		NoVerify:   noVerify,
		Force:      force,
	})
	if err != nil {
		return err
	}
	resolved := outputPath
	if resolved == "" {
		resolved = m.DuckDBFilename
	}
	fmt.Fprintf(os.Stderr,
		"[csq] fetched %s: portal=%s snapshot=%s datasets=%d rows=%d → %s\n",
		from, m.Portal, m.SnapshotID, m.DatasetCount, m.TotalRowCount, resolved)
	return nil
}
