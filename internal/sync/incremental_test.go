// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"strings"
	"testing"
	"time"

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

// helper that runs an IncrementalStrategy against the fake portal and returns the result.
func runIncr(t *testing.T, ds fakeDataset, w *duckdb.Writer, runID string, prevState *duckdb.DatasetState, mode string) DatasetResult {
	t.Helper()
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 10}
	if prevState != nil {
		_ = w.UpsertDatasetState(*prevState)
	}
	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: runID}
	target := DatasetTarget{ID: ds.ID,
		Effective: config.Effective{
			DatasetID: ds.ID, Table: "crimes", BatchSize: 10, Mode: mode,
		}}
	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	return res
}

func TestIncremental_DeltaInsert(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	// Bootstrap with 3 rows at 2026-04-22.
	ds1 := mkIncrDataset("aaaa-0001", 3, "2026-04-22")
	res1 := runIncr(t, ds1, w, "run1", nil, "")
	if res1.Status != "ok" {
		t.Fatalf("bootstrap: %v", res1.Err)
	}

	// Second run: source now has 3 old rows + 2 new rows at 2026-04-23.
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001",
		Columns: ds1.Columns,
		Rows: append(append([]map[string]any{}, ds1.Rows...),
			map[string]any{":id": "new-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(100)},
			map[string]any{":id": "new-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(101)},
		),
	}
	res2 := runIncr(t, ds2, w, "run2", nil, "")
	if res2.Status != "ok" {
		t.Fatalf("delta: %v", res2.Err)
	}

	var n int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM main.crimes`).Scan(&n)
	if n != 5 {
		t.Errorf("count: got %d, want 5", n)
	}

	state, _ := w.ReadDatasetState("aaaa-0001")
	if state.HWMUpdatedAt == nil || state.HWMUpdatedAt.Year() != 2026 || state.HWMUpdatedAt.Day() != 23 {
		t.Errorf("HWM not advanced; got %v", state.HWMUpdatedAt)
	}
}

func TestIncremental_DeltaUpdate(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds1 := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, ds1, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}

	// Same :id, newer :updated_at, different score.
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001",
		Columns: ds1.Columns,
		Rows: []map[string]any{
			{":id": "aaaa-0001-0", ":updated_at": "2026-04-22T00:00:00.000", "score": float64(0)},
			{":id": "aaaa-0001-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(999)},
		},
	}
	if res := runIncr(t, ds2, w, "run2", nil, ""); res.Status != "ok" {
		t.Fatalf("delta: %v", res.Err)
	}

	var n int
	var score float64
	_ = w.DB.QueryRow(`SELECT COUNT(*), MAX(score) FROM main.crimes`).Scan(&n, &score)
	if n != 1 || score != 999 {
		t.Errorf("upsert: count=%d score=%v want 1/999", n, score)
	}
}

func TestIncremental_NoNewRows(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds := mkIncrDataset("aaaa-0001", 2, "2026-04-22")
	res1 := runIncr(t, ds, w, "run1", nil, "")
	if res1.Status != "ok" {
		t.Fatalf("bootstrap: %v", res1.Err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")

	// Second run with the same data — nothing newer than HWM.
	res2 := runIncr(t, ds, w, "run2", nil, "")
	if res2.Status != "ok" {
		t.Fatalf("delta no-op: %v", res2.Err)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if !state2.HWMUpdatedAt.Equal(*state1.HWMUpdatedAt) {
		t.Errorf("HWM moved: %v -> %v", state1.HWMUpdatedAt, state2.HWMUpdatedAt)
	}
	if state2.LastRunID != "run2" {
		t.Errorf("LastRunID not bumped: %q", state2.LastRunID)
	}
}

func TestIncremental_SchemaDriftFails(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds1 := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, ds1, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}

	// Drift: portal removes the score column.
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001",
		Columns: []map[string]string{},
		Rows: []map[string]any{
			{":id": "new", ":updated_at": "2026-04-23T00:00:00.000"},
		},
	}
	res2 := runIncr(t, ds2, w, "run2", nil, "")
	if res2.Status != "failed" {
		t.Errorf("status: got %q, want failed", res2.Status)
	}
	if res2.Err == nil || !containsAll(res2.Err.Error(), "schema drift", "score") {
		t.Errorf("err: %v (want schema drift mentioning score)", res2.Err)
	}
}

func TestIncremental_FullReplaceOptOut(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds1 := mkIncrDataset("aaaa-0001", 2, "2026-04-22")
	if res := runIncr(t, ds1, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")

	// Source totally different; mode=full_replace forces re-bootstrap.
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001",
		Columns: ds1.Columns,
		Rows: []map[string]any{
			{":id": "fresh-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(7)},
		},
	}
	res2 := runIncr(t, ds2, w, "run2", nil, "full_replace")
	if res2.Status != "ok" {
		t.Fatalf("opt-out: %v", res2.Err)
	}

	var n int
	var first string
	_ = w.DB.QueryRow(`SELECT COUNT(*), MIN(socrata_id) FROM main.crimes`).Scan(&n, &first)
	if n != 1 || first != "fresh-0" {
		t.Errorf("table not rebootstrapped: count=%d first=%q", n, first)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if !state2.LastFullReplaceAt.After(*state1.LastFullReplaceAt) {
		t.Errorf("LastFullReplaceAt did not advance: %v -> %v",
			state1.LastFullReplaceAt, state2.LastFullReplaceAt)
	}
}

func TestIncremental_StreamFailMidPage(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds1 := mkIncrDataset("aaaa-0001", 2, "2026-04-22")
	if res := runIncr(t, ds1, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")

	// Second run: lots of new rows but the fake fails at offset 3.
	// BatchSize must be < FailAtOffset so pagination actually hits the error.
	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(0)},
		{":id": "x-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(1)},
		{":id": "x-2", ":updated_at": "2026-04-23T00:00:02.000", "score": float64(2)},
		{":id": "x-3", ":updated_at": "2026-04-23T00:00:03.000", "score": float64(3)},
		{":id": "x-4", ":updated_at": "2026-04-23T00:00:04.000", "score": float64(4)},
	}
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001", Columns: ds1.Columns,
		Rows:         rows,
		FailAtOffset: 3,
	}
	srv2 := newFakeSocrata(t, ds2)
	strat2 := &IncrementalStrategy{Portal: fakeHost(srv2), Scheme: "http", RunID: "run2"}
	target2 := DatasetTarget{ID: ds2.ID,
		Effective: config.Effective{DatasetID: ds2.ID, Table: "crimes", BatchSize: 2}}
	client2 := &socrata.Client{BatchSize: 2, MaxRetries: 1, RetryWait: time.Millisecond}
	res2, _ := strat2.Sync(context.Background(), target2, client2, w, &RecordingReporter{}, 1, 1)
	if res2.Status != "failed" {
		t.Errorf("status: got %q, want failed", res2.Status)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if !state2.HWMUpdatedAt.Equal(*state1.HWMUpdatedAt) {
		t.Errorf("HWM advanced on failure: %v -> %v", state1.HWMUpdatedAt, state2.HWMUpdatedAt)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

func TestIncremental_DeltaCheckpoints_PersistsMidStream(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	bootstrap := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, bootstrap, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}

	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(0)},
		{":id": "x-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(1)},
		{":id": "x-2", ":updated_at": "2026-04-23T00:00:02.000", "score": float64(2)},
		{":id": "x-3", ":updated_at": "2026-04-23T00:00:03.000", "score": float64(3)},
		{":id": "x-4", ":updated_at": "2026-04-23T00:00:04.000", "score": float64(4)},
	}
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Ds", Columns: bootstrap.Columns,
		Rows: rows, FailAtOffset: 4,
	}

	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 1, MaxRetries: 1, RetryWait: time.Millisecond}
	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run2"}
	target := DatasetTarget{ID: ds.ID,
		Effective: config.Effective{
			DatasetID: ds.ID, Table: "crimes", BatchSize: 1,
			CheckpointEveryNPages: 2,
		}}
	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "failed" {
		t.Fatalf("status: got %q, want failed", res.Status)
	}

	// Pages 1..4 succeed (offsets 0..3). Checkpoint fires after pages 2 and 4.
	// Persisted HWM should reflect page 4's row (T3).
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if state2.HWMUpdatedAt == nil {
		t.Fatal("HWM nil")
	}
	want := time.Date(2026, 4, 23, 0, 0, 3, 0, time.UTC)
	if !state2.HWMUpdatedAt.Equal(want) {
		t.Errorf("HWM: got %v, want %v (T3, after page 4 checkpoint)", state2.HWMUpdatedAt, want)
	}
}

func TestIncremental_DeltaCheckpoints_DisabledByDefault(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	bootstrap := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, bootstrap, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")
	priorHWM := *state1.HWMUpdatedAt

	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(0)},
		{":id": "x-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(1)},
		{":id": "x-2", ":updated_at": "2026-04-23T00:00:02.000", "score": float64(2)},
		{":id": "x-3", ":updated_at": "2026-04-23T00:00:03.000", "score": float64(3)},
	}
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Ds", Columns: bootstrap.Columns,
		Rows: rows, FailAtOffset: 4,
	}
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 1, MaxRetries: 1, RetryWait: time.Millisecond}
	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run2"}
	target := DatasetTarget{ID: ds.ID,
		Effective: config.Effective{
			DatasetID: ds.ID, Table: "crimes", BatchSize: 1,
		}}
	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "failed" {
		t.Fatalf("status: got %q", res.Status)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if !state2.HWMUpdatedAt.Equal(priorHWM) {
		t.Errorf("HWM advanced without checkpoint flag: was %v now %v", priorHWM, state2.HWMUpdatedAt)
	}
}

func TestIncremental_DeltaCheckpoints_FinalWriteSubsumes(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	bootstrap := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, bootstrap, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}
	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(0)},
		{":id": "x-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(1)},
		{":id": "x-2", ":updated_at": "2026-04-23T00:00:02.000", "score": float64(2)},
		{":id": "x-3", ":updated_at": "2026-04-23T00:00:03.000", "score": float64(3)},
		{":id": "x-4", ":updated_at": "2026-04-23T00:00:04.000", "score": float64(4)},
	}
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Ds", Columns: bootstrap.Columns,
		Rows: rows,
	}
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 1}
	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run2"}
	target := DatasetTarget{ID: ds.ID,
		Effective: config.Effective{
			DatasetID: ds.ID, Table: "crimes", BatchSize: 1,
			CheckpointEveryNPages: 2,
		}}
	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "ok" {
		t.Fatalf("status: %v", res.Err)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	want := time.Date(2026, 4, 23, 0, 0, 4, 0, time.UTC)
	if !state2.HWMUpdatedAt.Equal(want) {
		t.Errorf("HWM after success: got %v, want %v", state2.HWMUpdatedAt, want)
	}
}
