// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// UpsertCatalog replaces the entire _csq.catalog with the given entries,
// stamping fetched_at = now. Done in a single transaction.
func (w *Writer) UpsertCatalog(entries []socrata.CatalogEntry, now time.Time) error {
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM _csq.catalog`); err != nil {
		return fmt.Errorf("delete catalog: %w", err)
	}

	stmt, err := tx.Prepare(`INSERT INTO _csq.catalog
		(id, name, description, category, tags, row_count, updated_at, fetched_at, raw)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, e := range entries {
		tagsJSON, err := json.Marshal(e.Tags)
		if err != nil {
			return fmt.Errorf("marshal tags %q: %w", e.ID, err)
		}
		var updatedAt any
		if e.UpdatedAt != nil {
			updatedAt = *e.UpdatedAt
		}
		var rowCount any
		if e.RowCount != nil {
			rowCount = *e.RowCount
		}
		raw := []byte(e.Raw)
		if len(raw) == 0 {
			raw = []byte("null")
		}
		if _, err := stmt.Exec(e.ID, e.Name, nullIfEmpty(e.Description),
			nullIfEmpty(e.Category), string(tagsJSON), rowCount, updatedAt, now, string(raw)); err != nil {
			return fmt.Errorf("insert %q: %w", e.ID, err)
		}
	}
	return tx.Commit()
}

// ReadCatalog returns every entry currently in _csq.catalog.
func (w *Writer) ReadCatalog() ([]socrata.CatalogEntry, error) {
	rows, err := w.DB.Query(`SELECT id, name, description, category, tags, row_count, updated_at, raw
		FROM _csq.catalog ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query catalog: %w", err)
	}
	defer rows.Close()

	var out []socrata.CatalogEntry
	for rows.Next() {
		var e socrata.CatalogEntry
		var description, category sql.NullString
		var rowCount sql.NullInt64
		var updatedAt sql.NullTime
		// DuckDB returns JSON columns as native Go values ([]interface{}, map, etc.),
		// not as strings, so scan tags and raw into any.
		var tagsVal, rawVal any
		if err := rows.Scan(&e.ID, &e.Name, &description, &category,
			&tagsVal, &rowCount, &updatedAt, &rawVal); err != nil {
			return nil, fmt.Errorf("scan catalog row: %w", err)
		}
		e.Description = description.String
		e.Category = category.String
		if tagsVal != nil {
			if b, err := json.Marshal(tagsVal); err == nil {
				_ = json.Unmarshal(b, &e.Tags)
			}
		}
		if rowCount.Valid {
			rc := rowCount.Int64
			e.RowCount = &rc
		}
		if updatedAt.Valid {
			ua := updatedAt.Time
			e.UpdatedAt = &ua
		}
		if rawVal != nil {
			if b, err := json.Marshal(rawVal); err == nil {
				e.Raw = json.RawMessage(b)
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
