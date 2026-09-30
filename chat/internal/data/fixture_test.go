// Copyright (c) 2026 Neomantra Corp

package data

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
)

// seedDB writes a csq-shaped database with one catalogued, synced dataset
// holding a few rows, and returns its path.
func seedDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.duckdb")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := duckdb.Apply(db); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	stmts := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO _csq.catalog (id, name, description, category, tags, fetched_at, raw)
		  VALUES ('aaaa-0001', 'Crimes', 'Reported crimes', 'Public Safety', '["crime"]', $1, '{}')`, []any{now}},
		{`CREATE TABLE main.crimes (socrata_id VARCHAR, ward BIGINT, amount DOUBLE, seen TIMESTAMP, note VARCHAR)`, nil},
		{`INSERT INTO main.crimes VALUES
		  ('a', 1, 1.5, TIMESTAMP '2026-01-02 00:00:00', NULL),
		  ('b', 2, 2, TIMESTAMP '2026-01-02 13:45:00', 'x')`, nil},
		{`INSERT INTO _csq.sync_runs (run_id, dataset_id, table_name, started_at, finished_at, status, rows_written, duration_ms)
		  VALUES ('01RUN', 'aaaa-0001', 'crimes', $1, $1, 'ok', 2, 10)`, []any{now}},
		{`INSERT INTO _csq.dataset_state (dataset_id, hwm_updated_at, last_run_id, hwm_column)
		  VALUES ('aaaa-0001', $1, '01RUN', ':updated_at')`, []any{now}},
	}
	for _, s := range stmts {
		if _, err := db.Exec(s.sql, s.args...); err != nil {
			t.Fatalf("%s: %v", s.sql, err)
		}
	}
	return path
}
