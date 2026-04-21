// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// Writer owns a DuckDB connection for a single portal database file.
type Writer struct {
	DB *sql.DB
}

// Open opens (creating if needed) a DuckDB file at path.
func Open(path string) (*Writer, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("open duckdb %q: %w", path, err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb %q: %w", path, err)
	}
	return &Writer{DB: db}, nil
}

// Close releases the underlying connection.
func (w *Writer) Close() error {
	if w.DB == nil {
		return nil
	}
	return w.DB.Close()
}

// EnsureTable runs CREATE TABLE IF NOT EXISTS for the schema.
//
// NOTE: Phase 0 assumes the schema is stable across runs. If a later call
// changes the schema, the existing table will NOT be migrated — that belongs
// to a later phase.
func (w *Writer) EnsureTable(ts TableSchema) error {
	if _, err := w.DB.Exec(ts.CreateTableSQL()); err != nil {
		return fmt.Errorf("create table %q: %w", ts.Table, err)
	}
	return nil
}

// ReplaceTable drops and recreates the target table. Useful for Phase 0 full syncs.
func (w *Writer) ReplaceTable(ts TableSchema) error {
	if _, err := w.DB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, ts.Table)); err != nil {
		return fmt.Errorf("drop table %q: %w", ts.Table, err)
	}
	return w.EnsureTable(ts)
}

// InsertRows inserts a page of Socrata rows into the table described by ts,
// inside a single transaction using a prepared statement.
func (w *Writer) InsertRows(ts TableSchema, rows []socrata.Row) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(ts.InsertSQL())
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	vals := make([]any, len(ts.Columns))
	for rowIdx, row := range rows {
		for i, col := range ts.Columns {
			v, err := col.Extract(row)
			if err != nil {
				return fmt.Errorf("row %d col %q: %w", rowIdx, col.Name, err)
			}
			vals[i] = v
		}
		if _, err := stmt.Exec(vals...); err != nil {
			return fmt.Errorf("insert row %d: %w", rowIdx, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// RowCount returns the count of rows currently in the target table.
func (w *Writer) RowCount(table string) (int64, error) {
	var n int64
	err := w.DB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, table)).Scan(&n)
	return n, err
}
