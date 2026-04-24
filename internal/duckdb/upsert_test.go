// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestUpsertRows_Insert(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	if _, err := w.DB.Exec(ts.CreateTableSQLIn("main")); err != nil {
		t.Fatalf("create: %v", err)
	}

	rows := []socrata.Row{
		{":id": "a", "score": float64(1)},
		{":id": "b", "score": float64(2)},
	}
	if err := w.UpsertRows("main", ts, rows); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	var n int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM main.crimes`).Scan(&n)
	if n != 2 {
		t.Errorf("count: got %d, want 2", n)
	}
}

func TestUpsertRows_OnConflictReplaces(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	_, _ = w.DB.Exec(ts.CreateTableSQLIn("main"))

	_ = w.UpsertRows("main", ts, []socrata.Row{
		{":id": "a", "score": float64(1)},
	})
	_ = w.UpsertRows("main", ts, []socrata.Row{
		{":id": "a", "score": float64(99)},
	})

	var n int
	var score float64
	_ = w.DB.QueryRow(`SELECT COUNT(*), MAX(score) FROM main.crimes`).Scan(&n, &score)
	if n != 1 {
		t.Errorf("count: got %d, want 1 (upsert should replace)", n)
	}
	if score != 99 {
		t.Errorf("score: got %v, want 99", score)
	}
}

func TestUpsertRows_EmptyIsNoop(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	_, _ = w.DB.Exec(ts.CreateTableSQLIn("main"))

	if err := w.UpsertRows("main", ts, nil); err != nil {
		t.Errorf("nil rows: %v", err)
	}
}
