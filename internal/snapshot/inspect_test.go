// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openFixtureDB(t *testing.T, datasets ...FixtureDataset) (*sql.DB, string) {
	t.Helper()
	dir := t.TempDir()
	path := seedFixtureDB(t, dir, "fixture.duckdb", datasets...)
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}

func TestAssertIsCSQDB_Valid(t *testing.T) {
	db, path := openFixtureDB(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	if err := assertIsCSQDB(db, path); err != nil {
		t.Errorf("want nil err, got %v", err)
	}
}

func TestAssertIsCSQDB_Invalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wrong.duckdb")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE foo (x INT)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err = assertIsCSQDB(db, path)
	if err == nil || !strings.Contains(err.Error(), "not a CivicSodaQuack DuckDB") {
		t.Errorf("got %v", err)
	}
}

func TestCountDatasets(t *testing.T) {
	db, _ := openFixtureDB(t,
		FixtureDataset{ID: "aaaa-0001", Name: "A"},
		FixtureDataset{ID: "bbbb-0002", Name: "B"},
		FixtureDataset{ID: "cccc-0003", Name: "C"})
	got, err := countDatasets(db)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if got != 3 {
		t.Errorf("got %d, want 3", got)
	}
}

func TestCountTotalRows_LatestOKPerDataset(t *testing.T) {
	hwm := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	db, _ := openFixtureDB(t,
		FixtureDataset{
			ID: "aaaa-0001", Name: "A", TableName: "a",
			ColumnDefs: []string{"v INT"},
			Rows:       []map[string]any{{"v": 1}, {"v": 2}, {"v": 3}},
			Synced:     true, HWM: hwm,
		},
		FixtureDataset{
			ID: "bbbb-0002", Name: "B", TableName: "b",
			ColumnDefs: []string{"v INT"},
			Rows:       []map[string]any{{"v": 1}, {"v": 2}, {"v": 3}, {"v": 4}, {"v": 5}},
			Synced:     true, HWM: hwm,
		},
		FixtureDataset{ID: "cccc-0003", Name: "C", Synced: false}) // never synced

	got, err := countTotalRows(db)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	// 3 + 5 = 8; cccc-0003 contributes 0 (no sync_runs row).
	if got != 8 {
		t.Errorf("got %d, want 8", got)
	}
}

func TestCountTotalRows_IgnoresFailedRuns(t *testing.T) {
	hwm := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	db, _ := openFixtureDB(t,
		FixtureDataset{
			ID: "aaaa-0001", Name: "A", TableName: "a",
			ColumnDefs: []string{"v INT"},
			Rows:       []map[string]any{{"v": 1}, {"v": 2}},
			Synced:     true, HWM: hwm,
		})
	// Add a later 'failed' run with a higher rows_written; it must NOT be counted.
	_, err := db.Exec(
		`INSERT INTO _csq.sync_runs
		   (run_id, dataset_id, table_name, started_at, status, rows_written, config_hash)
		 VALUES ('01HLATER', 'aaaa-0001', 'a', $1, 'failed', 999, 'sha256:other')`,
		hwm.Add(time.Hour))
	if err != nil {
		t.Fatalf("seed failed run: %v", err)
	}
	got, _ := countTotalRows(db)
	if got != 2 {
		t.Errorf("got %d, want 2 (failed run must be ignored)", got)
	}
}
