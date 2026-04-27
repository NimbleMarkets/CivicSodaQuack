// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"path/filepath"
	"testing"
)

func makeEmptyCSQDB(t *testing.T, path string) {
	t.Helper()
	db, err := openDB(path)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	defer db.Close()
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS _csq`,
		`CREATE TABLE IF NOT EXISTS _csq.catalog (
			id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, description VARCHAR,
			category VARCHAR, tags JSON, row_count BIGINT, updated_at TIMESTAMP,
			fetched_at TIMESTAMP NOT NULL, raw JSON NOT NULL)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

func TestOpenPools_Success(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.duckdb")
	makeEmptyCSQDB(t, path)

	pools, err := OpenPools([]DBSpec{{Alias: "test", Path: path}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	defer pools.Close()
	if pools.Host == nil {
		t.Error("host DB nil")
	}
	if _, ok := pools.Portals["test"]; !ok {
		t.Errorf("portal 'test' missing")
	}
	if pools.Portals["test"].DB == nil {
		t.Errorf("portal DB nil")
	}
	// Host should see the ATTACHed schema
	var n int
	if err := pools.Host.QueryRow(`SELECT COUNT(*) FROM test._csq.catalog`).Scan(&n); err != nil {
		t.Errorf("query attached: %v", err)
	}
}

func TestOpenPools_MissingFile(t *testing.T) {
	_, err := OpenPools([]DBSpec{{Alias: "x", Path: "/nonexistent/foo.duckdb"}})
	if err == nil {
		t.Fatal("want error for missing file")
	}
}

func TestOpenPools_NotCSQDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wrong.duckdb")
	// Open without seeding the _csq schema
	db, err := openDB(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.Close()

	_, err = OpenPools([]DBSpec{{Alias: "x", Path: path}})
	if err == nil {
		t.Fatal("want 'not a CivicSodaQuack DuckDB' error")
	}
}

func TestOpenPools_PortalDBReadsWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.duckdb")
	makeEmptyCSQDB(t, path)

	pools, err := OpenPools([]DBSpec{{Alias: "test", Path: path}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	defer pools.Close()

	// Write
	_, err = pools.Portals["test"].DB.Exec(
		`INSERT INTO _csq.catalog (id, name, fetched_at, raw)
		 VALUES ('aaaa-0001', 'Test', NOW(), '{}')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Read
	var name string
	err = pools.Portals["test"].DB.QueryRow(
		`SELECT name FROM _csq.catalog WHERE id = 'aaaa-0001'`).Scan(&name)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if name != "Test" {
		t.Errorf("got %q", name)
	}
}
