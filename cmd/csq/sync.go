// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
	syncpkg "github.com/neomantra/CivicSodaQuack/internal/sync"
)

func runSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	var (
		configPath     string
		dryRun         bool
		refreshCatalog bool
		concurrency    int
		only           string
		verbose        bool
	)
	fs.StringVar(&configPath, "config", "", "Portal YAML config (required)")
	fs.BoolVar(&dryRun, "dry-run", false, "Resolve selectors and print, don't write")
	fs.BoolVar(&refreshCatalog, "refresh-catalog", false, "Force refetch catalog before resolution")
	fs.IntVar(&concurrency, "concurrency", 0, "Override YAML concurrency (0 = use YAML)")
	fs.StringVar(&only, "only", "", "Comma-separated 4x4 ids to intersect with the resolved set")
	fs.BoolVarP(&verbose, "verbose", "v", false, "Verbose progress")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if configPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if concurrency > 0 {
		cfg.Concurrency = concurrency
	}

	w, err := duckdb.Open(cfg.DB)
	if err != nil {
		return err
	}
	defer w.Close()

	if n, err := w.IncompleteSyncRunCount(); err == nil && n > 0 {
		fmt.Fprintf(os.Stderr, "[csq] warning: %d prior sync_runs appear incomplete\n", n)
	}

	client := &socrata.Client{AppToken: cfg.AppToken}

	var onlyIDs []string
	if only != "" {
		for _, id := range strings.Split(only, ",") {
			id = strings.TrimSpace(id)
			if id != "" {
				onlyIDs = append(onlyIDs, id)
			}
		}
	}

	scheme := "https"
	if s := os.Getenv("CSQ_SCHEME"); s != "" {
		scheme = s
	}
	summary, err := syncpkg.Run(context.Background(), cfg, syncpkg.Deps{
		DB:             w,
		Client:         client,
		Scheme:         scheme,
		Reporter:       &syncpkg.StderrReporter{Out: os.Stderr},
		Only:           onlyIDs,
		DryRun:         dryRun,
		RefreshCatalog: refreshCatalog,
	})
	if dryRun {
		fmt.Fprintf(os.Stderr, "[csq] dry-run: would sync %d datasets\n", summary.Planned)
		return nil
	}
	fmt.Fprintf(os.Stderr, "[csq] summary: %d ok, %d failed, %d aborted, wall %s\n",
		summary.OK, summary.Failed, summary.Aborted, summary.Wall)
	if err != nil {
		return err
	}
	return nil
}
