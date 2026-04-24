// Copyright (c) 2026 Neomantra Corp

package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestMain(m *testing.M) {
	bin := filepath.Join(os.TempDir(), "csq-smoke")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build csq: %v\n%s\n", err, out)
		os.Exit(1)
	}
	defer os.Remove(bin)
	os.Setenv("CSQ_BIN", bin)
	os.Exit(m.Run())
}

func startFakePortal(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/catalog/v1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{
				"resource": map[string]any{
					"id": "aaaa-0001", "name": "Smoke DS",
					"description":   "",
					"rowsUpdatedAt": "2024-01-15T00:00:00.000",
				},
				"classification": map[string]any{
					"domain_category": "Test", "domain_tags": []string{"smoke"},
				},
			}},
			"resultSetSize": 1,
		})
	})
	mux.HandleFunc("/api/views/aaaa-0001.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "aaaa-0001", "name": "Smoke DS",
			"columns": []map[string]string{
				{"fieldName": "id", "dataTypeName": "text"},
				{"fieldName": "score", "dataTypeName": "number"},
			},
		})
	})
	mux.HandleFunc("/resource/aaaa-0001.json", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		offset, _ := strconv.Atoi(q.Get("$offset"))
		limit, _ := strconv.Atoi(q.Get("$limit"))
		selectClause := q.Get("$select")
		includeSystem := selectClause == ":*,*"
		rows := make([]map[string]any, 0)
		for i := offset; i < offset+limit && i < 8; i++ {
			row := map[string]any{
				"id": "r" + strconv.Itoa(i), "score": float64(i),
			}
			if includeSystem {
				row[":id"] = "aaaa-0001-" + strconv.Itoa(i)
				row[":updated_at"] = "2026-04-22T00:0" + strconv.Itoa(i%10) + ":00.000"
			}
			rows = append(rows, row)
		}
		_ = json.NewEncoder(w).Encode(rows)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestCSQ_IncrementalSmoke(t *testing.T) {
	// Mutable row store: the handler reads from this per request.
	rows := []map[string]any{
		{":id": "smoke-0", ":updated_at": "2026-04-22T00:00:00.000", "id": "smoke-0", "score": float64(0)},
		{":id": "smoke-1", ":updated_at": "2026-04-22T00:00:01.000", "id": "smoke-1", "score": float64(1)},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/catalog/v1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{
				"resource":       map[string]any{"id": "aaaa-0001", "name": "Smoke DS", "rowsUpdatedAt": "2026-04-22T00:00:00.000"},
				"classification": map[string]any{"domain_category": "Test", "domain_tags": []string{"smoke"}},
			}},
			"resultSetSize": 1,
		})
	})
	mux.HandleFunc("/api/views/aaaa-0001.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "aaaa-0001", "name": "Smoke DS",
			"columns": []map[string]string{
				{"fieldName": "id", "dataTypeName": "text"},
				{"fieldName": "score", "dataTypeName": "number"},
			},
		})
	})
	mux.HandleFunc("/resource/aaaa-0001.json", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		offset, _ := strconv.Atoi(q.Get("$offset"))
		limit, _ := strconv.Atoi(q.Get("$limit"))
		whereClause := q.Get("$where")

		filtered := rows
		if whereClause != "" {
			// Only one predicate shape supported: ":updated_at > 'TS'"
			cutoff := strings.TrimSuffix(strings.TrimPrefix(whereClause, ":updated_at > '"), "'")
			filtered = filtered[:0:0]
			for _, row := range rows {
				if ts, _ := row[":updated_at"].(string); ts > cutoff {
					filtered = append(filtered, row)
				}
			}
		}
		end := offset + limit
		if end > len(filtered) {
			end = len(filtered)
		}
		if offset > len(filtered) {
			offset = len(filtered)
		}
		_ = json.NewEncoder(w).Encode(filtered[offset:end])
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "incr.duckdb")
	cfgPath := filepath.Join(dir, "portal.yaml")

	tpl, err := os.ReadFile("testdata/portal.yaml.tmpl")
	if err != nil {
		t.Fatalf("read tmpl: %v", err)
	}
	yaml := strings.ReplaceAll(string(tpl), "{{HOST}}", host)
	yaml = strings.ReplaceAll(yaml, "{{DB}}", dbPath)
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write cfg: %v", err)
	}

	// Bootstrap run
	cmd := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath)
	cmd.Env = append(os.Environ(), "CSQ_SCHEME=http")
	var stderr1 bytes.Buffer
	cmd.Stderr = &stderr1
	if err := cmd.Run(); err != nil {
		t.Fatalf("first csq sync: %v\nstderr:\n%s", err, stderr1.String())
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM main.aaaa_0001`).Scan(&n)
	if n != 2 {
		t.Errorf("after bootstrap: got %d rows, want 2", n)
	}
	db.Close()

	// Add rows server-side, then run again.
	rows = append(rows,
		map[string]any{":id": "smoke-2", ":updated_at": "2026-04-23T00:00:00.000", "id": "smoke-2", "score": float64(2)},
		map[string]any{":id": "smoke-3", ":updated_at": "2026-04-23T00:00:01.000", "id": "smoke-3", "score": float64(3)},
	)

	cmd2 := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath)
	cmd2.Env = append(os.Environ(), "CSQ_SCHEME=http")
	var stderr2 bytes.Buffer
	cmd2.Stderr = &stderr2
	if err := cmd2.Run(); err != nil {
		t.Fatalf("second csq sync: %v\nstderr:\n%s", err, stderr2.String())
	}

	db, err = sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db.Close()
	_ = db.QueryRow(`SELECT COUNT(*) FROM main.aaaa_0001`).Scan(&n)
	if n != 4 {
		t.Errorf("after delta: got %d rows, want 4", n)
	}
	// dataset_state row should exist
	_ = db.QueryRow(`SELECT COUNT(*) FROM _csq.dataset_state WHERE dataset_id = 'aaaa-0001'`).Scan(&n)
	if n != 1 {
		t.Errorf("dataset_state row missing")
	}
}

func TestCSQ_SyncSmoke(t *testing.T) {
	srv := startFakePortal(t)
	host := strings.TrimPrefix(srv.URL, "http://")

	if os.Getenv("CSQ_SKIP_HTTP_SCHEME") != "" {
		t.Skip("CSQ sync is https-only in this build")
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.duckdb")
	cfgPath := filepath.Join(dir, "portal.yaml")

	tpl, err := os.ReadFile("testdata/portal.yaml.tmpl")
	if err != nil {
		t.Fatalf("read tmpl: %v", err)
	}
	yaml := strings.ReplaceAll(string(tpl), "{{HOST}}", host)
	yaml = strings.ReplaceAll(yaml, "{{DB}}", dbPath)
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write cfg: %v", err)
	}

	cmd := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath)
	cmd.Env = append(os.Environ(), "CSQ_SCHEME=http")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("csq sync: %v\nstderr:\n%s", err, stderr.String())
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM main.aaaa_0001`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 8 {
		t.Errorf("row count: got %d, want 8", n)
	}
}
