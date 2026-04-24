// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"strings"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// mkIncrDataset builds a fakeDataset with :id and :updated_at populated per row.
func mkIncrDataset(id string, n int, hwmBase string) fakeDataset {
	return fakeDataset{
		ID: id, Name: "Ds " + id,
		Columns: []map[string]string{
			{"fieldName": "score", "dataTypeName": "number"},
		},
		Rows: makeRows(n, func(i int) map[string]any {
			return map[string]any{
				":id":         id + "-" + itoa(i),
				":updated_at": hwmBase + "T00:0" + itoa(i) + ":00.000",
				"score":       float64(i),
			}
		}),
	}
}

func TestIncremental_Bootstrap(t *testing.T) {
	ds := mkIncrDataset("aaaa-0001", 5, "2026-04-22")
	srv := newFakeSocrata(t, ds)
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 10}

	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run1"}
	target := DatasetTarget{
		ID: "aaaa-0001",
		Effective: config.Effective{
			DatasetID: "aaaa-0001", Table: "crimes", BatchSize: 10,
		},
	}

	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "ok" {
		t.Fatalf("status: got %q, err=%v", res.Status, res.Err)
	}

	var n int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM main.crimes`).Scan(&n)
	if n != 5 {
		t.Errorf("rows: got %d, want 5", n)
	}

	state, err := w.ReadDatasetState("aaaa-0001")
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if state == nil {
		t.Fatal("dataset_state row missing")
	}
	if state.HWMUpdatedAt == nil {
		t.Errorf("HWM not set")
	}
	if state.LastFullReplaceAt == nil {
		t.Errorf("LastFullReplaceAt not set")
	}
	if state.LastRunID != "run1" {
		t.Errorf("LastRunID: got %q", state.LastRunID)
	}
}

func TestIncremental_BootstrapInstallsPK(t *testing.T) {
	ds := mkIncrDataset("aaaa-0001", 2, "2026-04-22")
	srv := newFakeSocrata(t, ds)
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 10}

	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run1"}
	target := DatasetTarget{ID: "aaaa-0001",
		Effective: config.Effective{DatasetID: "aaaa-0001", Table: "crimes"}}
	_, _ = strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)

	// Confirm socrata_id column exists and PK enforces uniqueness
	_, err := w.DB.Exec(`INSERT INTO main.crimes (socrata_id, score) VALUES ('aaaa-0001-0', 999)`)
	if err == nil {
		t.Errorf("expected PK violation on duplicate socrata_id")
	}
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "constraint") &&
		!strings.Contains(strings.ToLower(err.Error()), "duplicate") {
		t.Errorf("unexpected error kind: %v", err)
	}
}
