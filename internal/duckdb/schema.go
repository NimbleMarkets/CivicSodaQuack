// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// TargetColumn is one physical DuckDB column derived from a Socrata source column.
// A single Socrata point/location column expands to two targets (lon, lat) plus
// an optional raw column preserving the original GeoJSON.
type TargetColumn struct {
	Name    string // DuckDB column name (unquoted)
	Type    socrata.DuckDBType
	Source  socrata.Column // the Socrata column this derives from
	Extract func(row socrata.Row) (any, error)
}

// TableSchema is the set of TargetColumns for a dataset plus the target table name.
type TableSchema struct {
	Table      string
	Columns    []TargetColumn
	PrimaryKey string // optional; when set, emitted as table-level PRIMARY KEY in CreateTableSQL
}

// BuildSchema translates a Socrata dataset's columns into a DuckDB TableSchema.
// Geo types are handled here: point/location become <field>_lon, <field>_lat,
// <field>_raw; polygon-family columns are stored as JSON strings.
func BuildSchema(table string, cols []socrata.Column) TableSchema {
	ts := TableSchema{Table: table, Columns: make([]TargetColumn, 0, len(cols))}

	for _, c := range cols {
		if strings.HasPrefix(c.FieldName, ":@") || c.FieldName == "" {
			// skip computed-region system columns for now
			continue
		}

		if socrata.IsPointLike(c.DataTypeName) {
			src := c
			ts.Columns = append(ts.Columns,
				TargetColumn{
					Name:    c.FieldName + "_lon",
					Type:    socrata.TypeDouble,
					Source:  src,
					Extract: extractPointCoord(c.FieldName, 0),
				},
				TargetColumn{
					Name:    c.FieldName + "_lat",
					Type:    socrata.TypeDouble,
					Source:  src,
					Extract: extractPointCoord(c.FieldName, 1),
				},
				TargetColumn{
					Name:    c.FieldName + "_raw",
					Type:    socrata.TypeJSON,
					Source:  src,
					Extract: extractRawJSON(c.FieldName),
				},
			)
			continue
		}

		dt := socrata.DuckDBTypeFor(c.DataTypeName)
		ts.Columns = append(ts.Columns, TargetColumn{
			Name:    c.FieldName,
			Type:    dt,
			Source:  c,
			Extract: extractScalar(c.FieldName, dt),
		})
	}
	return ts
}

// BuildSchemaWithSocrataID returns a TableSchema with the synthetic socrata_id
// PRIMARY KEY column prepended. The socrata_id value is read from the row's :id
// system field, which Phase 2 callers fetch via $select=:*,*.
func BuildSchemaWithSocrataID(table string, cols []socrata.Column) TableSchema {
	ts := BuildSchema(table, cols)
	socrataIDCol := TargetColumn{
		Name:    "socrata_id",
		Type:    socrata.TypeVarchar,
		Extract: extractSocrataID,
	}
	ts.Columns = append([]TargetColumn{socrataIDCol}, ts.Columns...)
	ts.PrimaryKey = "socrata_id"
	return ts
}

func extractSocrataID(row socrata.Row) (any, error) {
	v, ok := row[":id"]
	if !ok || v == nil {
		return nil, nil
	}
	if s, ok := v.(string); ok {
		return s, nil
	}
	return fmt.Sprintf("%v", v), nil
}

// CreateTableSQL returns a CREATE TABLE statement for the schema.
// Table and column names are double-quoted to tolerate reserved words.
func (s TableSchema) CreateTableSQL() string {
	var b strings.Builder
	fmt.Fprintf(&b, `CREATE TABLE IF NOT EXISTS "%s" (`, s.Table)
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `"%s" %s`, c.Name, c.Type)
	}
	if s.PrimaryKey != "" {
		fmt.Fprintf(&b, `, PRIMARY KEY ("%s")`, s.PrimaryKey)
	}
	b.WriteString(")")
	return b.String()
}

// InsertSQL returns an INSERT statement with positional ($1, $2, ...) placeholders.
func (s TableSchema) InsertSQL() string {
	var b strings.Builder
	fmt.Fprintf(&b, `INSERT INTO "%s" (`, s.Table)
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `"%s"`, c.Name)
	}
	b.WriteString(") VALUES (")
	for i := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "$%d", i+1)
	}
	b.WriteString(")")
	return b.String()
}

// CreateTableSQLIn returns a CREATE TABLE statement targeting "<schemaName>"."<table>".
func (s TableSchema) CreateTableSQLIn(schemaName string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `CREATE TABLE IF NOT EXISTS "%s"."%s" (`, schemaName, s.Table)
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `"%s" %s`, c.Name, c.Type)
	}
	if s.PrimaryKey != "" {
		fmt.Fprintf(&b, `, PRIMARY KEY ("%s")`, s.PrimaryKey)
	}
	b.WriteString(")")
	return b.String()
}

// InsertSQLIn returns an INSERT INTO "<schemaName>"."<table>" with positional placeholders.
func (s TableSchema) InsertSQLIn(schemaName string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `INSERT INTO "%s"."%s" (`, schemaName, s.Table)
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `"%s"`, c.Name)
	}
	b.WriteString(") VALUES (")
	for i := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "$%d", i+1)
	}
	b.WriteString(")")
	return b.String()
}

// --- extractors -----------------------------------------------------------

func extractScalar(field string, dt socrata.DuckDBType) func(socrata.Row) (any, error) {
	return func(row socrata.Row) (any, error) {
		v, ok := row[field]
		if !ok || v == nil {
			return nil, nil
		}
		switch dt {
		case socrata.TypeDouble:
			return toFloat(v)
		case socrata.TypeBigint:
			return toInt(v)
		case socrata.TypeBoolean:
			return toBool(v)
		case socrata.TypeTimestamp:
			return toTimestamp(v)
		case socrata.TypeJSON:
			return toJSONString(v)
		default:
			return toString(v)
		}
	}
}

func extractPointCoord(field string, idx int) func(socrata.Row) (any, error) {
	return func(row socrata.Row) (any, error) {
		v, ok := row[field]
		if !ok || v == nil {
			return nil, nil
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, nil
		}
		coords, ok := m["coordinates"].([]any)
		if !ok || len(coords) <= idx {
			return nil, nil
		}
		return toFloat(coords[idx])
	}
}

func extractRawJSON(field string) func(socrata.Row) (any, error) {
	return func(row socrata.Row) (any, error) {
		v, ok := row[field]
		if !ok || v == nil {
			return nil, nil
		}
		return toJSONString(v)
	}
}

// --- value coercion -------------------------------------------------------

func toString(v any) (any, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case nil:
		return nil, nil
	default:
		return fmt.Sprintf("%v", x), nil
	}
}

func toFloat(v any) (any, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case string:
		if x == "" {
			return nil, nil
		}
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return nil, fmt.Errorf("parse float %q: %w", x, err)
		}
		return f, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected number value %T", v)
	}
}

func toInt(v any) (any, error) {
	switch x := v.(type) {
	case float64:
		return int64(x), nil
	case string:
		if x == "" {
			return nil, nil
		}
		i, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse int %q: %w", x, err)
		}
		return i, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected int value %T", v)
	}
}

func toBool(v any) (any, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case string:
		if x == "" {
			return nil, nil
		}
		return strconv.ParseBool(x)
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected bool value %T", v)
	}
}

// Socrata emits ISO-8601-like "2024-01-15T00:00:00.000" (no zone).
var timestampLayouts = []string{
	"2006-01-02T15:04:05.000",
	"2006-01-02T15:04:05",
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02",
}

func toTimestamp(v any) (any, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected timestamp value %T", v)
	}
	if s == "" {
		return nil, nil
	}
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return nil, fmt.Errorf("parse timestamp %q: no matching layout", s)
}

func toJSONString(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}
