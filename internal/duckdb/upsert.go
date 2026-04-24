// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"fmt"
	"strings"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// UpsertRows inserts rows into "<schemaName>"."<ts.Table>", upserting on the
// table's PrimaryKey. ts.PrimaryKey must be non-empty (use BuildSchemaWithSocrataID
// to construct an upsert-capable TableSchema). Empty rows is a no-op.
func (w *Writer) UpsertRows(schemaName string, ts TableSchema, rows []socrata.Row) error {
	if len(rows) == 0 {
		return nil
	}
	if ts.PrimaryKey == "" {
		return fmt.Errorf("UpsertRows requires ts.PrimaryKey to be set")
	}

	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(buildUpsertSQL(schemaName, ts))
	if err != nil {
		return fmt.Errorf("prepare upsert: %w", err)
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
			return fmt.Errorf("upsert row %d: %w", rowIdx, err)
		}
	}
	return tx.Commit()
}

func buildUpsertSQL(schemaName string, ts TableSchema) string {
	var b strings.Builder
	fmt.Fprintf(&b, `INSERT INTO "%s"."%s" (`, schemaName, ts.Table)
	for i, c := range ts.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `"%s"`, c.Name)
	}
	b.WriteString(") VALUES (")
	for i := range ts.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "$%d", i+1)
	}
	fmt.Fprintf(&b, `) ON CONFLICT ("%s") DO UPDATE SET `, ts.PrimaryKey)
	first := true
	for _, c := range ts.Columns {
		if c.Name == ts.PrimaryKey {
			continue
		}
		if !first {
			b.WriteString(", ")
		}
		first = false
		fmt.Fprintf(&b, `"%s" = excluded."%s"`, c.Name, c.Name)
	}
	return b.String()
}
