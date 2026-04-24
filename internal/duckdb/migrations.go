// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"database/sql"
	"fmt"
)

// Apply creates the _csq and _csq_staging schemas and the _csq.catalog and
// _csq.sync_runs tables if they do not already exist. Safe to run repeatedly.
func Apply(db *sql.DB) error {
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS _csq`,
		`CREATE SCHEMA IF NOT EXISTS _csq_staging`,
		`CREATE TABLE IF NOT EXISTS _csq.catalog (
			id          VARCHAR PRIMARY KEY,
			name        VARCHAR NOT NULL,
			description VARCHAR,
			category    VARCHAR,
			tags        JSON,
			row_count   BIGINT,
			updated_at  TIMESTAMP,
			fetched_at  TIMESTAMP NOT NULL,
			raw         JSON NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS _csq.sync_runs (
			run_id       VARCHAR NOT NULL,
			dataset_id   VARCHAR NOT NULL,
			table_name   VARCHAR NOT NULL,
			started_at   TIMESTAMP NOT NULL,
			finished_at  TIMESTAMP,
			status       VARCHAR NOT NULL,
			rows_written BIGINT,
			error        VARCHAR,
			duration_ms  BIGINT,
			config_hash  VARCHAR,
			PRIMARY KEY (run_id, dataset_id)
		)`,
		`CREATE INDEX IF NOT EXISTS sync_runs_by_dataset ON _csq.sync_runs (dataset_id, started_at DESC)`,
		`CREATE TABLE IF NOT EXISTS _csq.dataset_state (
			dataset_id           VARCHAR PRIMARY KEY,
			hwm_updated_at       TIMESTAMP,
			last_full_replace_at TIMESTAMP,
			last_run_id          VARCHAR,
			hwm_column           VARCHAR NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("apply migration: %w\nSQL: %s", err, s)
		}
	}
	return nil
}
