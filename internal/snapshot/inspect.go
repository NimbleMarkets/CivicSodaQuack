// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
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

// countTotalRows returns the SUM of live row counts across the tables named by
// the most recent status='ok' sync_runs row per dataset_id. Datasets that have
// never successfully synced contribute 0. Failed/aborted runs are ignored.
// rows_written is deliberately not used: it records the last batch, not the
// table's size.
func countTotalRows(db *sql.DB) (int64, error) {
	rows, err := db.Query(`
		SELECT FIRST(table_name ORDER BY started_at DESC)
		FROM _csq.sync_runs
		WHERE status = 'ok'
		GROUP BY dataset_id`)
	if err != nil {
		return 0, fmt.Errorf("count total rows: %w", err)
	}
	var tables []string
	for rows.Next() {
		var t sql.NullString
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return 0, fmt.Errorf("count total rows: %w", err)
		}
		if t.Valid {
			tables = append(tables, t.String)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("count total rows: %w", err)
	}
	rows.Close()

	counts, err := duckdb.CountMainTables(context.Background(), db, tables)
	if err != nil {
		return 0, fmt.Errorf("count total rows: %w", err)
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	return total, nil
}
