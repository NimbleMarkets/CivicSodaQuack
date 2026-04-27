// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"database/sql"
	"fmt"
)

// assertIsCSQDB returns nil if db has a _csq.catalog table.
func assertIsCSQDB(db *sql.DB, path string) error {
	var n int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema = '_csq' AND table_name = 'catalog'`).Scan(&n)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", path, err)
	}
	if n == 0 {
		return fmt.Errorf("not a CivicSodaQuack DuckDB (no _csq.catalog in %s)", path)
	}
	return nil
}

// countDatasets returns the count of rows in _csq.catalog.
func countDatasets(db *sql.DB) (int64, error) {
	var n int64
	err := db.QueryRow(`SELECT COUNT(*) FROM _csq.catalog`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count datasets: %w", err)
	}
	return n, nil
}

// countTotalRows returns the SUM of rows_written across the most recent
// status='ok' row per dataset_id. Datasets that have never successfully synced
// contribute 0. Failed/aborted runs are ignored.
func countTotalRows(db *sql.DB) (int64, error) {
	var n sql.NullInt64
	err := db.QueryRow(`
		SELECT SUM(rows_written) FROM (
			SELECT FIRST(rows_written ORDER BY started_at DESC) AS rows_written
			FROM _csq.sync_runs
			WHERE status = 'ok'
			GROUP BY dataset_id
		) latest`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count total rows: %w", err)
	}
	if !n.Valid {
		return 0, nil
	}
	return n.Int64, nil
}
