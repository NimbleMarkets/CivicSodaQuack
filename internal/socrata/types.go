// Copyright (c) 2026 Neomantra Corp

package socrata

// Column describes one Socrata dataset column, as reported by /api/views/{id}.json.
type Column struct {
	FieldName    string `json:"fieldName"`
	Name         string `json:"name"`
	DataTypeName string `json:"dataTypeName"`
	Description  string `json:"description,omitempty"`
}

// DuckDBType is the DuckDB column type chosen for a Socrata Column.
type DuckDBType string

const (
	TypeVarchar   DuckDBType = "VARCHAR"
	TypeDouble    DuckDBType = "DOUBLE"
	TypeBigint    DuckDBType = "BIGINT"
	TypeBoolean   DuckDBType = "BOOLEAN"
	TypeTimestamp DuckDBType = "TIMESTAMP"
	TypeJSON      DuckDBType = "JSON"
)

// DuckDBTypeFor maps a Socrata dataTypeName to a DuckDB column type.
// Geo types (point, location, polygon, ...) are handled separately by the
// writer and are not returned here.
func DuckDBTypeFor(dataTypeName string) DuckDBType {
	switch dataTypeName {
	case "number", "percent", "money":
		return TypeDouble
	case "calendar_date", "date":
		return TypeTimestamp
	case "checkbox":
		return TypeBoolean
	case "json", "multipolygon", "polygon", "line", "multiline", "multipoint":
		return TypeJSON
	default:
		// text, url, html, document, photo, phone, email, unknown → VARCHAR
		return TypeVarchar
	}
}

// IsPointLike reports whether the given Socrata type is a single-point geo type
// that should be flattened into <field>_lon / <field>_lat columns.
func IsPointLike(dataTypeName string) bool {
	return dataTypeName == "point" || dataTypeName == "location"
}
