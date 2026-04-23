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
		rows := make([]map[string]any, 0)
		for i := offset; i < offset+limit && i < 8; i++ {
			rows = append(rows, map[string]any{
				"id": "r" + strconv.Itoa(i), "score": float64(i),
			})
		}
		_ = json.NewEncoder(w).Encode(rows)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
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
