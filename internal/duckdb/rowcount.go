// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// CountMainTables returns the live row count of each named table in schema
// main. Names with no such table (a dataset that never synced) are absent from
// the result. A base-table COUNT(*) is answered from DuckDB metadata, so this
// stays cheap even for million-row tables.
//
// This is the dataset's cardinality. sync_runs.rows_written is sync activity:
// after a bootstrap plus a small delta it is the delta's size, not the table's.
func CountMainTables(ctx context.Context, db *sql.DB, names []string) (map[string]int64, error) {
	out := make(map[string]int64, len(names))
	if len(names) == 0 {
		return out, nil
	}
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = 'main' AND table_type = 'BASE TABLE'`)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	existing := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return nil, err
		}
		existing[n] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	for _, name := range names {
		if !existing[name] {
			continue
		}
		var n int64
		q := `SELECT COUNT(*) FROM main."` + strings.ReplaceAll(name, `"`, `""`) + `"`
		if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			return nil, fmt.Errorf("count %s: %w", name, err)
		}
		out[name] = n
	}
	return out, nil
}
