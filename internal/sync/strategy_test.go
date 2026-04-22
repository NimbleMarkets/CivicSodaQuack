// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestFullReplaceStrategy_HappyPath(t *testing.T) {
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Crimes",
		Columns: []map[string]string{
			{"fieldName": "id", "dataTypeName": "text"},
			{"fieldName": "score", "dataTypeName": "number"},
		},
		Rows: makeRows(7, func(i int) map[string]any {
			return map[string]any{"id": "r" + itoa(i), "score": float64(i)}
		}),
	}
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 3}

	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	strat := &FullReplaceStrategy{
		Portal: fakeHost(srv), Scheme: "http", RunID: "run1",
	}
	target := DatasetTarget{
		ID: "aaaa-0001", Name: "Crimes",
		Effective: config.Effective{
			DatasetID: "aaaa-0001", Table: "crimes",
			OrderBy: ":id", BatchSize: 3,
		},
	}

	res, err := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if res.Status != "ok" {
		t.Errorf("status: got %q", res.Status)
	}
	if res.RowsWritten != 7 {
		t.Errorf("rows: got %d, want 7", res.RowsWritten)
	}

	var n int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM main.crimes`).Scan(&n)
	if n != 7 {
		t.Errorf("main.crimes rowcount: got %d, want 7", n)
	}
}

func TestFullReplaceStrategy_FailureLeavesPriorTableIntact(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	// Prior successful sync: seed main.crimes with 100 rows
	if _, err := w.DB.Exec(`CREATE TABLE main.crimes (id VARCHAR, score DOUBLE)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < 100; i++ {
		if _, err := w.DB.Exec(`INSERT INTO main.crimes VALUES (?, ?)`, "prior"+itoa(i), float64(i)); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
	}

	// New run fails mid-stream
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Crimes",
		Columns: []map[string]string{
			{"fieldName": "id", "dataTypeName": "text"},
			{"fieldName": "score", "dataTypeName": "number"},
		},
		Rows:         makeRows(20, func(i int) map[string]any { return map[string]any{"id": "new" + itoa(i), "score": float64(i)} }),
		FailAtOffset: 5,
	}
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 5, MaxRetries: 1}
	strat := &FullReplaceStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run2"}
	target := DatasetTarget{
		ID: "aaaa-0001", Name: "Crimes",
		Effective: config.Effective{DatasetID: "aaaa-0001", Table: "crimes", OrderBy: ":id", BatchSize: 5},
	}

	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "failed" {
		t.Errorf("status: got %q, want failed", res.Status)
	}
	var n int
	var firstID string
	_ = w.DB.QueryRow(`SELECT COUNT(*), MIN(id) FROM main.crimes`).Scan(&n, &firstID)
	if n != 100 {
		t.Errorf("main.crimes rowcount: got %d, want 100 (prior data preserved)", n)
	}
	if firstID == "new0" {
		t.Error("main.crimes was overwritten by failed run")
	}
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = digits[i%10]
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
