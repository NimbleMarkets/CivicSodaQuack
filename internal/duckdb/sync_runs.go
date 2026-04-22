// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"database/sql"
	"fmt"
	"time"
)

// StartSyncRun inserts a row with finished_at=NULL and status='running'.
// Returns the DuckDB rowid of the inserted row so it can be updated on finish.
func (w *Writer) StartSyncRun(runID, datasetID, tableName, configHash string, startedAt time.Time) (int64, error) {
	_, err := w.DB.Exec(
		`INSERT INTO _csq.sync_runs (run_id, dataset_id, table_name, started_at, status, config_hash)
		 VALUES ($1, $2, $3, $4, 'running', $5)`,
		runID, datasetID, tableName, startedAt, configHash,
	)
	if err != nil {
		return 0, fmt.Errorf("insert sync_run start: %w", err)
	}
	var rowid int64
	err = w.DB.QueryRow(
		`SELECT rowid FROM _csq.sync_runs WHERE run_id = $1 AND dataset_id = $2 AND started_at = $3
		 ORDER BY rowid DESC LIMIT 1`,
		runID, datasetID, startedAt,
	).Scan(&rowid)
	if err != nil {
		return 0, fmt.Errorf("fetch sync_run rowid: %w", err)
	}
	return rowid, nil
}

// FinishSyncRun updates the row to a terminal status with timing and row count.
// errMsg is empty for ok.
func (w *Writer) FinishSyncRun(rowid int64, status string, rowsWritten int64, errMsg string, finishedAt time.Time) error {
	var startedAt time.Time
	if err := w.DB.QueryRow(
		`SELECT started_at FROM _csq.sync_runs WHERE rowid = $1`, rowid,
	).Scan(&startedAt); err != nil {
		return fmt.Errorf("lookup started_at: %w", err)
	}
	durMs := finishedAt.Sub(startedAt).Milliseconds()

	var rowsArg any
	if status == "ok" {
		rowsArg = rowsWritten
	} else {
		rowsArg = nil
	}
	var errArg any
	if errMsg != "" {
		errArg = errMsg
	} else {
		errArg = nil
	}

	_, err := w.DB.Exec(
		`UPDATE _csq.sync_runs
		 SET finished_at = $1, status = $2, rows_written = $3, error = $4, duration_ms = $5
		 WHERE rowid = $6`,
		finishedAt, status, rowsArg, errArg, durMs, rowid,
	)
	if err != nil {
		return fmt.Errorf("update sync_run finish: %w", err)
	}
	return nil
}

// IncompleteSyncRunCount returns the number of sync_runs rows with finished_at IS NULL.
// Used on startup to log "N prior runs appear incomplete".
func (w *Writer) IncompleteSyncRunCount() (int, error) {
	var n int
	err := w.DB.QueryRow(`SELECT COUNT(*) FROM _csq.sync_runs WHERE finished_at IS NULL`).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return n, err
}
