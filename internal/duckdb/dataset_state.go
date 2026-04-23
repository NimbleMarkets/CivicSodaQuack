// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"database/sql"
	"fmt"
	"time"
)

// DatasetState is the per-dataset incremental-sync state stored in _csq.dataset_state.
type DatasetState struct {
	DatasetID         string
	HWMUpdatedAt      *time.Time // nil when the source dataset has no usable :updated_at
	LastFullReplaceAt *time.Time
	LastRunID         string
	HWMColumn         string // ":updated_at" by default
}

// ReadDatasetState returns the state row for id, or (nil, nil) when no row exists.
func (w *Writer) ReadDatasetState(id string) (*DatasetState, error) {
	var s DatasetState
	var hwm sql.NullTime
	var lastFull sql.NullTime
	err := w.DB.QueryRow(
		`SELECT dataset_id, hwm_updated_at, last_full_replace_at, last_run_id, hwm_column
		 FROM _csq.dataset_state WHERE dataset_id = $1`, id,
	).Scan(&s.DatasetID, &hwm, &lastFull, &s.LastRunID, &s.HWMColumn)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read dataset_state: %w", err)
	}
	if hwm.Valid {
		t := hwm.Time
		s.HWMUpdatedAt = &t
	}
	if lastFull.Valid {
		t := lastFull.Time
		s.LastFullReplaceAt = &t
	}
	return &s, nil
}

// UpsertDatasetState inserts or replaces the row for s.DatasetID.
func (w *Writer) UpsertDatasetState(s DatasetState) error {
	var hwmArg any
	if s.HWMUpdatedAt != nil {
		hwmArg = *s.HWMUpdatedAt
	}
	var lastFullArg any
	if s.LastFullReplaceAt != nil {
		lastFullArg = *s.LastFullReplaceAt
	}
	_, err := w.DB.Exec(
		`INSERT INTO _csq.dataset_state
		   (dataset_id, hwm_updated_at, last_full_replace_at, last_run_id, hwm_column)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (dataset_id) DO UPDATE SET
		   hwm_updated_at       = excluded.hwm_updated_at,
		   last_full_replace_at = excluded.last_full_replace_at,
		   last_run_id          = excluded.last_run_id,
		   hwm_column           = excluded.hwm_column`,
		s.DatasetID, hwmArg, lastFullArg, s.LastRunID, s.HWMColumn,
	)
	if err != nil {
		return fmt.Errorf("upsert dataset_state: %w", err)
	}
	return nil
}
