// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"
)

func TestSwapIn_ReplacesExistingTable(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	// Pre-existing "foo" in main with old data
	if _, err := w.DB.Exec(`CREATE TABLE main.foo (v INT)`); err != nil {
		t.Fatalf("seed old: %v", err)
	}
	if _, err := w.DB.Exec(`INSERT INTO main.foo VALUES (1), (2)`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	// Staging table with new data
	if _, err := w.DB.Exec(`CREATE TABLE _csq_staging.foo_run1 (v INT)`); err != nil {
		t.Fatalf("staging: %v", err)
	}
	if _, err := w.DB.Exec(`INSERT INTO _csq_staging.foo_run1 VALUES (100), (200), (300)`); err != nil {
		t.Fatalf("staging rows: %v", err)
	}

	if err := w.SwapIn("foo_run1", "foo"); err != nil {
		t.Fatalf("swap: %v", err)
	}

	var n int
	if err := w.DB.QueryRow(`SELECT COUNT(*) FROM main.foo`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Errorf("main.foo rowcount: got %d, want 3", n)
	}
	// Staging should be empty of that table name
	var ns int
	_ = w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '_csq_staging' AND table_name = 'foo_run1'`,
	).Scan(&ns)
	if ns != 0 {
		t.Errorf("_csq_staging.foo_run1 should be gone; got %d", ns)
	}
}

func TestSwapIn_CreatesNewTable(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	if _, err := w.DB.Exec(`CREATE TABLE _csq_staging.bar_runX (v INT)`); err != nil {
		t.Fatalf("staging: %v", err)
	}
	if err := w.SwapIn("bar_runX", "bar"); err != nil {
		t.Fatalf("swap: %v", err)
	}
	var n int
	_ = w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'bar'`,
	).Scan(&n)
	if n != 1 {
		t.Errorf("main.bar not created")
	}
}
