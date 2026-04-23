// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestDiffSchema_Identical(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{
		{FieldName: "score", DataTypeName: "number"},
		{FieldName: "name", DataTypeName: "text"},
	}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	_, _ = w.DB.Exec(ts.CreateTableSQLIn("main"))

	got, err := DiffSchema(ts, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("identical schemas should diff empty, got %+v", got)
	}
}

func TestDiffSchema_ColumnAdded(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	live := BuildSchemaWithSocrataID("crimes",
		[]socrata.Column{{FieldName: "score", DataTypeName: "number"}})
	_, _ = w.DB.Exec(live.CreateTableSQLIn("main"))

	want := BuildSchemaWithSocrataID("crimes", []socrata.Column{
		{FieldName: "score", DataTypeName: "number"},
		{FieldName: "newcol", DataTypeName: "text"},
	})

	got, err := DiffSchema(want, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 1 || got[0].Column != "newcol" || got[0].Kind != "added" {
		t.Errorf("got %+v, want one 'added' diff for newcol", got)
	}
}

func TestDiffSchema_ColumnRemoved(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	live := BuildSchemaWithSocrataID("crimes", []socrata.Column{
		{FieldName: "score", DataTypeName: "number"},
		{FieldName: "extra", DataTypeName: "text"},
	})
	_, _ = w.DB.Exec(live.CreateTableSQLIn("main"))

	want := BuildSchemaWithSocrataID("crimes",
		[]socrata.Column{{FieldName: "score", DataTypeName: "number"}})

	got, err := DiffSchema(want, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 1 || got[0].Column != "extra" || got[0].Kind != "removed" {
		t.Errorf("got %+v, want one 'removed' diff for extra", got)
	}
}

func TestDiffSchema_ColumnRetyped(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	live := BuildSchemaWithSocrataID("crimes",
		[]socrata.Column{{FieldName: "score", DataTypeName: "text"}})
	_, _ = w.DB.Exec(live.CreateTableSQLIn("main"))

	want := BuildSchemaWithSocrataID("crimes",
		[]socrata.Column{{FieldName: "score", DataTypeName: "number"}})

	got, err := DiffSchema(want, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 1 || got[0].Column != "score" || got[0].Kind != "retyped" {
		t.Errorf("got %+v, want one 'retyped' diff for score", got)
	}
}

func TestDiffSchema_IgnoresSocrataIDOnBothSides(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	_, _ = w.DB.Exec(ts.CreateTableSQLIn("main"))

	got, err := DiffSchema(ts, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("socrata_id should be excluded from diff, got %+v", got)
	}
}
