// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
	syncpkg "github.com/neomantra/CivicSodaQuack/internal/sync"
)

// SyncDatasetArgs are the inputs to sync_dataset.
type SyncDatasetArgs struct {
	Portal      string `json:"portal" jsonschema:"alias of an attached portal that has a registered config"`
	DatasetID   string `json:"dataset_id" jsonschema:"4x4 Socrata id; must be in the YAML's resolved selector set"`
	FullRefresh bool   `json:"full_refresh,omitempty" jsonschema:"true to bootstrap (full-replace) instead of delta"`
}

// SyncDatasetResult is the output of sync_dataset.
type SyncDatasetResult struct {
	Portal      string `json:"portal"`
	DatasetID   string `json:"dataset_id"`
	Status      string `json:"status"`
	RowsWritten int64  `json:"rows_written"`
	DurationMs  int64  `json:"duration_ms"`
	RunID       string `json:"run_id"`
	Error       string `json:"error,omitempty"`
}

// syncDatasetHandler runs sync.Run for one dataset using the registered config
// for the portal. Returns nil error for in-band failures; the result's Status
// and Error fields carry the outcome.
func syncDatasetHandler(ctx context.Context, configs map[string]*config.Config,
	args SyncDatasetArgs) (SyncDatasetResult, error) {

	cfg, ok := configs[args.Portal]
	if !ok {
		return SyncDatasetResult{}, fmt.Errorf(
			"sync_dataset: no config registered for portal %q; restart csq mcp with --db ... --config",
			args.Portal)
	}

	res := SyncDatasetResult{
		Portal:    args.Portal,
		DatasetID: args.DatasetID,
	}
	started := time.Now().UTC()

	w, err := duckdb.Open(cfg.DB)
	if err != nil {
		res.Status = "failed"
		res.Error = fmt.Sprintf("open db: %v", err)
		res.DurationMs = time.Since(started).Milliseconds()
		return res, nil
	}
	defer w.Close()

	client := &socrata.Client{AppToken: cfg.AppToken}

	scheme := "https"
	if s := os.Getenv("CSQ_SCHEME"); s != "" {
		scheme = s
	}
	deps := syncpkg.Deps{
		DB:       w,
		Client:   client,
		Scheme:   scheme,
		Reporter: &syncpkg.RecordingReporter{},
		Only:     []string{args.DatasetID},
	}
	if args.FullRefresh {
		deps.FullRefreshIDs = []string{args.DatasetID}
	}

	summary, runErr := syncpkg.Run(ctx, cfg, deps)
	res.RunID = summary.RunID
	res.DurationMs = time.Since(started).Milliseconds()

	if runErr != nil {
		res.Status = "failed"
		res.Error = runErr.Error()
		return res, nil
	}
	if summary.Failed > 0 {
		res.Status = "failed"
		res.Error = "sync reported failure (see _csq.sync_runs)"
		return res, nil
	}
	if summary.Aborted > 0 {
		res.Status = "aborted"
		res.Error = "sync aborted (see _csq.sync_runs)"
		return res, nil
	}

	res.Status = "ok"
	// Best-effort: pull the rows_written from the sync_runs row we just wrote.
	_ = w.DB.QueryRow(
		`SELECT COALESCE(rows_written, 0) FROM _csq.sync_runs
		 WHERE run_id = $1 AND dataset_id = $2`,
		summary.RunID, args.DatasetID).Scan(&res.RowsWritten)
	return res, nil
}
