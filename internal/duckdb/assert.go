// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"database/sql"
	"fmt"
)

// AssertCSQ returns nil if db has a _csq.catalog table, the marker that the
// file is a CivicSodaQuack portal database. path is used only in errors.
func AssertCSQ(db *sql.DB, path string) error {
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
