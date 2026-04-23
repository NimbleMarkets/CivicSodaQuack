// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"
)

func TestApplyMigrations_CreatesSchemasAndTables(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	// Both schemas exist
	for _, schema := range []string{"_csq", "_csq_staging"} {
		var n int
		row := w.DB.QueryRow(
			`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?`, schema)
		if err := row.Scan(&n); err != nil {
			t.Fatalf("query schema %q: %v", schema, err)
		}
		if n != 1 {
			t.Errorf("schema %q: want 1 row, got %d", schema, n)
		}
	}

	// Catalog + sync_runs tables exist
	for _, table := range []string{"catalog", "sync_runs"} {
		var n int
		row := w.DB.QueryRow(
			`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '_csq' AND table_name = ?`, table)
		if err := row.Scan(&n); err != nil {
			t.Fatalf("query table _csq.%s: %v", table, err)
		}
		if n != 1 {
			t.Errorf("table _csq.%s: want 1 row, got %d", table, n)
		}
	}
}

func TestApplyMigrations_CreatesIndex(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	var n int
	err = w.DB.QueryRow(
		`SELECT COUNT(*) FROM duckdb_indexes() WHERE schema_name = '_csq' AND index_name = 'sync_runs_by_dataset'`,
	).Scan(&n)
	if err != nil {
		t.Fatalf("query indexes: %v", err)
	}
	if n != 1 {
		t.Errorf("sync_runs_by_dataset index: want 1 row, got %d", n)
	}
}

func TestApplyMigrations_Idempotent(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	if err := Apply(w.DB); err != nil {
		t.Fatalf("apply second time: %v", err)
	}
}
