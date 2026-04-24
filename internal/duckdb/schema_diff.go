// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"database/sql"
	"fmt"
	"strings"
)

// SchemaDiff describes one column-level discrepancy between a desired schema
// and the live table.
type SchemaDiff struct {
	Column string
	Kind   string // "added" | "removed" | "retyped"
	Want   string // type we'd build now (empty for "removed")
	Have   string // type currently in the table (empty for "added")
}

// DiffSchema returns the per-column differences between want and the live table
// at "<schemaName>"."<table>". The synthetic socrata_id column is excluded on
// both sides so it never trips drift detection.
func DiffSchema(want TableSchema, db *sql.DB, schemaName, table string) ([]SchemaDiff, error) {
	rows, err := db.Query(
		`SELECT column_name, data_type FROM information_schema.columns
		 WHERE table_schema = $1 AND table_name = $2`,
		schemaName, table,
	)
	if err != nil {
		return nil, fmt.Errorf("read information_schema: %w", err)
	}
	defer rows.Close()

	have := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		if name == "socrata_id" {
			continue
		}
		have[name] = strings.ToUpper(typ)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	wantMap := map[string]string{}
	for _, c := range want.Columns {
		if c.Name == "socrata_id" {
			continue
		}
		wantMap[c.Name] = string(c.Type)
	}

	var diffs []SchemaDiff
	for name, wantType := range wantMap {
		haveType, ok := have[name]
		if !ok {
			diffs = append(diffs, SchemaDiff{Column: name, Kind: "added", Want: wantType})
			continue
		}
		if !typesEquivalent(wantType, haveType) {
			diffs = append(diffs, SchemaDiff{Column: name, Kind: "retyped", Want: wantType, Have: haveType})
		}
	}
	for name, haveType := range have {
		if _, ok := wantMap[name]; !ok {
			diffs = append(diffs, SchemaDiff{Column: name, Kind: "removed", Have: haveType})
		}
	}
	return diffs, nil
}

// typesEquivalent treats VARCHAR and STRING as the same, since DuckDB reports
// VARCHAR columns as "VARCHAR" in information_schema regardless of input spelling.
func typesEquivalent(want, have string) bool {
	if want == have {
		return true
	}
	// DuckDB normalises some types
	norm := func(s string) string {
		switch s {
		case "STRING":
			return "VARCHAR"
		}
		return s
	}
	return norm(want) == norm(have)
}
