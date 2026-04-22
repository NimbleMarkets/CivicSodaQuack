// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"
	"time"
)

func TestSyncRuns_StartAndFinish_OK(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	start := time.Now().UTC().Truncate(time.Second)
	id, err := w.StartSyncRun("run-1", "aaaa-bbbb", "foo", "cfghash", start)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if id < 0 {
		t.Errorf("want non-negative rowid, got %d", id)
	}

	finish := start.Add(2 * time.Second)
	if err := w.FinishSyncRun(id, "ok", 42, "", finish); err != nil {
		t.Fatalf("finish: %v", err)
	}

	var status string
	var rows int64
	var durMs int64
	err = w.DB.QueryRow(
		`SELECT status, rows_written, duration_ms FROM _csq.sync_runs WHERE rowid = ?`, id,
	).Scan(&status, &rows, &durMs)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if status != "ok" || rows != 42 {
		t.Errorf("got status=%q rows=%d", status, rows)
	}
	if durMs != 2000 {
		t.Errorf("duration_ms: got %d, want 2000", durMs)
	}
}

func TestSyncRuns_Failed(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	now := time.Now().UTC()
	id, _ := w.StartSyncRun("run-2", "xxxx-yyyy", "foo", "h", now)
	if err := w.FinishSyncRun(id, "failed", 0, "boom: HTTP 500", now.Add(time.Second)); err != nil {
		t.Fatalf("finish: %v", err)
	}
	var errStr string
	_ = w.DB.QueryRow(`SELECT error FROM _csq.sync_runs WHERE rowid = ?`, id).Scan(&errStr)
	if errStr != "boom: HTTP 500" {
		t.Errorf("error: got %q", errStr)
	}
}
