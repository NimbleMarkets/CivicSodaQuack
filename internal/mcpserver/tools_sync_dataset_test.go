// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/config"
)

// startFakePortal stands up a fake Socrata portal that serves catalog,
// metadata, and resource for the named dataset. Returns the host (no scheme).
func startFakePortal(t *testing.T, datasetID string, rows []map[string]any) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/catalog/v1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{
				"resource": map[string]any{
					"id": datasetID, "name": "Smoke",
					"description":   "",
					"rowsUpdatedAt": "2026-04-22T00:00:00.000",
				},
				"classification": map[string]any{
					"domain_category": "Test", "domain_tags": []string{"x"},
				},
			}},
			"resultSetSize": 1,
		})
	})
	mux.HandleFunc("/api/views/"+datasetID+".json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": datasetID, "name": "Smoke",
			"columns": []map[string]string{
				{"fieldName": "id", "dataTypeName": "text"},
				{"fieldName": "score", "dataTypeName": "number"},
			},
		})
	})
	mux.HandleFunc("/resource/"+datasetID+".json", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		offset, _ := strconv.Atoi(q.Get("$offset"))
		limit, _ := strconv.Atoi(q.Get("$limit"))
		end := offset + limit
		if end > len(rows) {
			end = len(rows)
		}
		if offset > len(rows) {
			offset = len(rows)
		}
		_ = json.NewEncoder(w).Encode(rows[offset:end])
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// seedEmptyCSQDB creates a minimal CivicSodaQuack-shaped DuckDB so that
// duckdb.Open + sync.Run will accept it.
func seedEmptyCSQDB(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open seed: %v", err)
	}
	defer db.Close()
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS _csq`,
		`CREATE TABLE IF NOT EXISTS _csq.catalog (
			id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, description VARCHAR,
			category VARCHAR, tags JSON, row_count BIGINT, updated_at TIMESTAMP,
			fetched_at TIMESTAMP NOT NULL, raw JSON NOT NULL)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

func makeFakePortalCfg(dbPath, host, datasetID string) *config.Config {
	return &config.Config{
		Portal:      host,
		DB:          dbPath,
		Concurrency: 1,
		OnError:     "continue",
		Defaults:    config.Defaults{BatchSize: 5, OrderBy: ":id"},
		Include:     []config.Selector{{ID: datasetID}},
	}
}

func TestSyncDataset_NoConfig_Errors(t *testing.T) {
	_, err := syncDatasetHandler(context.Background(), map[string]*config.Config{},
		SyncDatasetArgs{Portal: "missing", DatasetID: "aaaa-0001"})
	if err == nil || !strings.Contains(err.Error(), "no config registered") {
		t.Errorf("want no-config error, got %v", err)
	}
}

func TestSyncDataset_HappyPath(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.duckdb")
	seedEmptyCSQDB(t, dbPath)

	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-22T00:00:00.000", "id": "x-0", "score": 1.0},
		{":id": "x-1", ":updated_at": "2026-04-22T00:00:01.000", "id": "x-1", "score": 2.0},
	}
	host := startFakePortal(t, "aaaa-0001", rows)
	cfg := makeFakePortalCfg(dbPath, host, "aaaa-0001")
	configs := map[string]*config.Config{"test": cfg}

	t.Setenv("CSQ_SCHEME", "http")
	res, err := syncDatasetHandler(context.Background(), configs,
		SyncDatasetArgs{Portal: "test", DatasetID: "aaaa-0001"})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if res.Status != "ok" {
		t.Errorf("status: got %q (err=%q)", res.Status, res.Error)
	}
	if res.RowsWritten != 2 {
		t.Errorf("rows: got %d, want 2", res.RowsWritten)
	}
	if res.RunID == "" {
		t.Errorf("RunID empty")
	}

	// Verify dataset_state row exists.
	db, _ := sql.Open("duckdb", dbPath)
	defer db.Close()
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM _csq.dataset_state WHERE dataset_id = 'aaaa-0001'`).Scan(&n)
	if n != 1 {
		t.Errorf("dataset_state row missing")
	}
}

func TestSyncDataset_FullRefresh(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.duckdb")
	seedEmptyCSQDB(t, dbPath)

	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-22T00:00:00.000", "id": "x-0", "score": 1.0},
	}
	host := startFakePortal(t, "aaaa-0001", rows)
	cfg := makeFakePortalCfg(dbPath, host, "aaaa-0001")
	configs := map[string]*config.Config{"test": cfg}

	t.Setenv("CSQ_SCHEME", "http")
	// First sync (bootstrap).
	if _, err := syncDatasetHandler(context.Background(), configs,
		SyncDatasetArgs{Portal: "test", DatasetID: "aaaa-0001"}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	db, _ := sql.Open("duckdb", dbPath)
	var first1 string
	_ = db.QueryRow(`SELECT last_full_replace_at::VARCHAR FROM _csq.dataset_state WHERE dataset_id = 'aaaa-0001'`).Scan(&first1)
	db.Close()

	// Second sync with full_refresh=true.
	res, err := syncDatasetHandler(context.Background(), configs,
		SyncDatasetArgs{Portal: "test", DatasetID: "aaaa-0001", FullRefresh: true})
	if err != nil {
		t.Fatalf("full refresh: %v", err)
	}
	if res.Status != "ok" {
		t.Errorf("status: %q (err=%q)", res.Status, res.Error)
	}

	db, _ = sql.Open("duckdb", dbPath)
	defer db.Close()
	var second1 string
	_ = db.QueryRow(`SELECT last_full_replace_at::VARCHAR FROM _csq.dataset_state WHERE dataset_id = 'aaaa-0001'`).Scan(&second1)
	if second1 == first1 {
		t.Errorf("LastFullReplaceAt should advance: was=%q now=%q", first1, second1)
	}
}

func TestSyncDataset_UnknownDataset_Failed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.duckdb")
	seedEmptyCSQDB(t, dbPath)

	host := startFakePortal(t, "aaaa-0001", nil)
	cfg := makeFakePortalCfg(dbPath, host, "aaaa-0001")
	configs := map[string]*config.Config{"test": cfg}

	t.Setenv("CSQ_SCHEME", "http")
	res, err := syncDatasetHandler(context.Background(), configs,
		SyncDatasetArgs{Portal: "test", DatasetID: "zzzz-9999"})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if res.Status != "failed" {
		t.Errorf("status: got %q, want failed", res.Status)
	}
	if !strings.Contains(res.Error, "zzzz-9999") {
		t.Errorf("error should mention zzzz-9999: %q", res.Error)
	}
}
