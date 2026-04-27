// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// FixtureDataset describes one dataset to seed into a fixture DB.
type FixtureDataset struct {
	ID         string
	Name       string
	Category   string
	TableName  string
	ColumnDefs []string
	Rows       []map[string]any
	Synced     bool
	HWM        time.Time
}

// seedFixtureDB creates a CivicSodaQuack-shaped DuckDB file at dir/filename.
func seedFixtureDB(t *testing.T, dir, filename string, datasets ...FixtureDataset) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	defer db.Close()

	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS _csq`,
		`CREATE SCHEMA IF NOT EXISTS _csq_staging`,
		`CREATE TABLE _csq.catalog (
			id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, description VARCHAR,
			category VARCHAR, tags JSON, row_count BIGINT, updated_at TIMESTAMP,
			fetched_at TIMESTAMP NOT NULL, raw JSON NOT NULL)`,
		`CREATE TABLE _csq.sync_runs (
			run_id VARCHAR NOT NULL, dataset_id VARCHAR NOT NULL,
			table_name VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL,
			finished_at TIMESTAMP, status VARCHAR NOT NULL,
			rows_written BIGINT, error VARCHAR, duration_ms BIGINT,
			config_hash VARCHAR, PRIMARY KEY (run_id, dataset_id))`,
		`CREATE TABLE _csq.dataset_state (
			dataset_id VARCHAR PRIMARY KEY,
			hwm_updated_at TIMESTAMP, last_full_replace_at TIMESTAMP,
			last_run_id VARCHAR, hwm_column VARCHAR NOT NULL)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	for _, d := range datasets {
		_, err := db.Exec(
			`INSERT INTO _csq.catalog (id, name, description, category, tags, fetched_at, raw)
			 VALUES ($1, $2, '', $3, '[]', $4, '{}')`,
			d.ID, d.Name, d.Category, now)
		if err != nil {
			t.Fatalf("seed catalog %s: %v", d.ID, err)
		}
		if d.TableName != "" && len(d.ColumnDefs) > 0 {
			create := `CREATE TABLE main."` + d.TableName + `" (`
			for i, def := range d.ColumnDefs {
				if i > 0 {
					create += ", "
				}
				create += def
			}
			create += `)`
			if _, err := db.Exec(create); err != nil {
				t.Fatalf("create %s: %v", d.TableName, err)
			}
			for _, row := range d.Rows {
				cols, ph, vals := buildInsert(row, d.ColumnDefs)
				if _, err := db.Exec(`INSERT INTO main."`+d.TableName+`" (`+cols+`) VALUES (`+ph+`)`, vals...); err != nil {
					t.Fatalf("insert %s: %v", d.TableName, err)
				}
			}
		}
		if d.Synced {
			_, err = db.Exec(
				`INSERT INTO _csq.sync_runs
				   (run_id, dataset_id, table_name, started_at, finished_at,
				    status, rows_written, duration_ms, config_hash)
				 VALUES ($1, $2, $3, $4, $5, 'ok', $6, 1234, 'sha256:fake')`,
				"01HFAKE", d.ID, d.TableName, now, now.Add(time.Second), int64(len(d.Rows)),
			)
			if err != nil {
				t.Fatalf("seed sync_runs %s: %v", d.ID, err)
			}
			_, err = db.Exec(
				`INSERT INTO _csq.dataset_state
				   (dataset_id, hwm_updated_at, last_full_replace_at, last_run_id, hwm_column)
				 VALUES ($1, $2, $3, '01HFAKE', ':updated_at')`,
				d.ID, d.HWM, now,
			)
			if err != nil {
				t.Fatalf("seed dataset_state %s: %v", d.ID, err)
			}
		}
	}
	return path
}

func buildInsert(row map[string]any, columnDefs []string) (cols, placeholders string, vals []any) {
	for i, def := range columnDefs {
		name := def
		for j := 0; j < len(def); j++ {
			if def[j] == ' ' {
				name = def[:j]
				break
			}
		}
		if i > 0 {
			cols += ", "
			placeholders += ", "
		}
		cols += `"` + name + `"`
		placeholders += `$` + itoaSimple(i+1)
		vals = append(vals, row[name])
	}
	return
}

func itoaSimple(i int) string {
	if i == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = digits[i%10]
		i /= 10
	}
	return string(buf[pos:])
}
