// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/snapshot"
)

func runSnapshot(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	var (
		dbPath      string
		outputPath  string
		portal      string
		keepStaging bool
		force       bool
	)
	fs.StringVar(&dbPath, "db", "", "Source DuckDB to package (required)")
	fs.StringVar(&outputPath, "output", "", "Destination tarball (required, .tar.zst)")
	fs.StringVar(&portal, "portal", "", "Portal name in manifest (default: derived from --db filename)")
	fs.BoolVar(&keepStaging, "keep-staging", false, "Skip _csq_staging cleanup")
	fs.BoolVar(&force, "force", false, "Overwrite --output if it exists")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if dbPath == "" {
		return fmt.Errorf("--db is required")
	}
	if outputPath == "" {
		return fmt.Errorf("--output is required")
	}

	m, err := snapshot.Pack(context.Background(), snapshot.ProducerOptions{
		DBPath:      dbPath,
		OutputPath:  outputPath,
		Portal:      portal,
		KeepStaging: keepStaging,
		Force:       force,
		CSQVersion:  "0.4.0",
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr,
		"[csq] snapshot %s: portal=%s datasets=%d rows=%d size=%d sha256=%s\n",
		outputPath, m.Portal, m.DatasetCount, m.TotalRowCount,
		m.DuckDBSizeBytes, m.DuckDBSHA256[:12])
	return nil
}
