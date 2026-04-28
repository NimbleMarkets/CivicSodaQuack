// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/portallock"
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
		noLock      bool
		lockWait    time.Duration
	)
	fs.StringVar(&dbPath, "db", "", "Source DuckDB to package (required)")
	fs.StringVar(&outputPath, "output", "", "Destination tarball (required, .tar.zst)")
	fs.StringVar(&portal, "portal", "", "Portal name in manifest (default: derived from --db filename)")
	fs.BoolVar(&keepStaging, "keep-staging", false, "Skip _csq_staging cleanup")
	fs.BoolVar(&force, "force", false, "Overwrite --output if it exists")
	fs.BoolVar(&noLock, "no-lock", false, "Skip portal lock acquisition")
	fs.DurationVar(&lockWait, "lock-wait", 0,
		"Retry lock acquisition for up to this duration before giving up")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if dbPath == "" {
		return fmt.Errorf("--db is required")
	}
	if outputPath == "" {
		return fmt.Errorf("--output is required")
	}

	lock, err := portallock.Acquire(dbPath, portallock.Options{NoLock: noLock, LockWait: lockWait})
	if err != nil {
		return err
	}
	defer lock.Release()

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
