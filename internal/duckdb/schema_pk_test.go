// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"strings"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestBuildSchemaWithSocrataID_PrependsColumn(t *testing.T) {
	cols := []socrata.Column{
		{FieldName: "score", DataTypeName: "number"},
	}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	if len(ts.Columns) != 2 {
		t.Fatalf("col count: got %d, want 2", len(ts.Columns))
	}
	if ts.Columns[0].Name != "socrata_id" {
		t.Errorf("first col: got %q, want socrata_id", ts.Columns[0].Name)
	}
	if ts.PrimaryKey != "socrata_id" {
		t.Errorf("primary key: got %q, want socrata_id", ts.PrimaryKey)
	}
}

func TestCreateTableSQL_EmitsPrimaryKey(t *testing.T) {
	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	sql := ts.CreateTableSQL()
	if !strings.Contains(sql, "PRIMARY KEY") {
		t.Errorf("missing PRIMARY KEY in SQL:\n%s", sql)
	}
	if !strings.Contains(sql, `"socrata_id" VARCHAR`) {
		t.Errorf("missing socrata_id col in SQL:\n%s", sql)
	}
}

func TestCreateTableSQL_NoPKWhenUnset(t *testing.T) {
	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchema("crimes", cols)
	sql := ts.CreateTableSQL()
	if strings.Contains(sql, "PRIMARY KEY") {
		t.Errorf("Phase 1 path should not emit PRIMARY KEY:\n%s", sql)
	}
}

func TestSocrataIDExtractor_ReadsColonID(t *testing.T) {
	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	row := socrata.Row{":id": "row-abc", "score": float64(42)}
	got, err := ts.Columns[0].Extract(row)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if got != "row-abc" {
		t.Errorf("got %v, want row-abc", got)
	}
}
