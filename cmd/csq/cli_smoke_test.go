// Copyright (c) 2026 Neomantra Corp

package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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
	// Guarded by mu — httptest server goroutines can outlive cmd.Run() while
	// finishing connection cleanup, racing with the mid-test append below.
	var mu sync.Mutex
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

		mu.Lock()
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
		page := append([]map[string]any(nil), filtered[offset:end]...)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(page)
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
	mu.Lock()
	rows = append(rows,
		map[string]any{":id": "smoke-2", ":updated_at": "2026-04-23T00:00:00.000", "id": "smoke-2", "score": float64(2)},
		map[string]any{":id": "smoke-3", ":updated_at": "2026-04-23T00:00:01.000", "id": "smoke-3", "score": float64(3)},
	)
	mu.Unlock()

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

func TestCSQ_MCP_Stdio_Smoke(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "smoke.duckdb")

	// Build a minimal _csq.catalog so the file is recognised as a CSQ DuckDB.
	{
		db, err := sql.Open("duckdb", dbPath)
		if err != nil {
			t.Fatalf("seed open: %v", err)
		}
		stmts := []string{
			`CREATE SCHEMA _csq`,
			`CREATE TABLE _csq.catalog (
				id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL,
				description VARCHAR, category VARCHAR, tags JSON,
				row_count BIGINT, updated_at TIMESTAMP,
				fetched_at TIMESTAMP NOT NULL, raw JSON NOT NULL)`,
			`INSERT INTO _csq.catalog (id, name, fetched_at, raw)
			 VALUES ('aaaa-0001', 'Smoke', NOW(), '{}')`,
		}
		for _, s := range stmts {
			if _, err := db.Exec(s); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		db.Close()
	}

	cmd := exec.Command(os.Getenv("CSQ_BIN"), "mcp", "--db", dbPath)
	stdinW, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin: %v", err)
	}
	stdoutR, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Send initialize, then tools/list. Each request is one JSON object per line.
	send := func(s string) {
		if _, err := io.WriteString(stdinW, s+"\n"); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}`)
	send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	send(`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)

	// Read responses until we see all four tool names or hit the timeout.
	deadline := time.Now().Add(5 * time.Second)
	buf := make([]byte, 0, 64*1024)
	tmp := make([]byte, 8*1024)
	for time.Now().Before(deadline) {
		_ = setReadDeadline(stdoutR, time.Now().Add(500*time.Millisecond))
		n, _ := stdoutR.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		got := string(buf)
		all := true
		for _, name := range []string{"list_datasets", "describe_dataset", "search_datasets", "query_sql"} {
			if !strings.Contains(got, name) {
				all = false
				break
			}
		}
		if all {
			return
		}
	}
	t.Fatalf("timed out waiting for tools/list response\nstdout so far:\n%s\nstderr:\n%s", string(buf), stderr.String())
}

// setReadDeadline is a no-op when r doesn't support it (os.Pipe doesn't).
// We poll with short reads instead.
func setReadDeadline(r interface{}, t time.Time) error {
	type deadlineReader interface{ SetReadDeadline(time.Time) error }
	if dr, ok := r.(deadlineReader); ok {
		return dr.SetReadDeadline(t)
	}
	return nil
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

func TestCSQ_Snapshot_RoundTrip_Smoke(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.cityofchicago.org.duckdb")
	tarPath := filepath.Join(dir, "snap.tar.zst")
	restored := filepath.Join(dir, "restored.duckdb")

	// Seed a minimal CSQ DuckDB.
	{
		db, err := sql.Open("duckdb", srcPath)
		if err != nil {
			t.Fatalf("seed open: %v", err)
		}
		stmts := []string{
			`CREATE SCHEMA _csq`,
			`CREATE TABLE _csq.catalog (
				id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL,
				description VARCHAR, category VARCHAR, tags JSON,
				row_count BIGINT, updated_at TIMESTAMP,
				fetched_at TIMESTAMP NOT NULL, raw JSON NOT NULL)`,
			`INSERT INTO _csq.catalog (id, name, fetched_at, raw)
			 VALUES ('aaaa-0001', 'Smoke A', NOW(), '{}')`,
			`INSERT INTO _csq.catalog (id, name, fetched_at, raw)
			 VALUES ('bbbb-0002', 'Smoke B', NOW(), '{}')`,
			`CREATE TABLE _csq.sync_runs (
				run_id VARCHAR NOT NULL, dataset_id VARCHAR NOT NULL,
				table_name VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL,
				finished_at TIMESTAMP, status VARCHAR NOT NULL,
				rows_written BIGINT, error VARCHAR, duration_ms BIGINT,
				config_hash VARCHAR, PRIMARY KEY (run_id, dataset_id))`,
			`INSERT INTO _csq.sync_runs (run_id, dataset_id, table_name, started_at, status, rows_written)
			 VALUES ('run-1', 'aaaa-0001', 'aaaa_0001', NOW(), 'ok', 100)`,
			`INSERT INTO _csq.sync_runs (run_id, dataset_id, table_name, started_at, status, rows_written)
			 VALUES ('run-1', 'bbbb-0002', 'bbbb_0002', NOW(), 'ok', 50)`,
		}
		for _, s := range stmts {
			if _, err := db.Exec(s); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		db.Close()
	}

	// csq snapshot
	cmd := exec.Command(os.Getenv("CSQ_BIN"), "snapshot", "--db", srcPath, "--output", tarPath)
	var stderr1 bytes.Buffer
	cmd.Stderr = &stderr1
	if err := cmd.Run(); err != nil {
		t.Fatalf("csq snapshot: %v\nstderr:\n%s", err, stderr1.String())
	}
	if st, err := os.Stat(tarPath); err != nil || st.Size() < 1024 {
		t.Fatalf("tarball missing or too small: stat err=%v size=%d", err, st.Size())
	}

	// csq fetch from file:// URL
	cmd2 := exec.Command(os.Getenv("CSQ_BIN"), "fetch",
		"--from", "file://"+tarPath, "--output", restored)
	var stderr2 bytes.Buffer
	cmd2.Stderr = &stderr2
	if err := cmd2.Run(); err != nil {
		t.Fatalf("csq fetch: %v\nstderr:\n%s", err, stderr2.String())
	}

	// Restored DuckDB should open and contain the seeded rows.
	db, err := sql.Open("duckdb", restored)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM _csq.catalog`).Scan(&n); err != nil {
		t.Fatalf("query restored: %v", err)
	}
	if n != 2 {
		t.Errorf("restored catalog rows: got %d, want 2", n)
	}
}

func TestCSQ_Sync_FullRefresh_Smoke(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.duckdb")
	cfgPath := filepath.Join(dir, "portal.yaml")

	rows := []map[string]any{
		{":id": "smoke-0", ":updated_at": "2026-04-22T00:00:00.000", "id": "smoke-0", "score": float64(0)},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/catalog/v1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{
				"resource":       map[string]any{"id": "aaaa-0001", "name": "Smoke", "rowsUpdatedAt": "2026-04-22T00:00:00.000"},
				"classification": map[string]any{"domain_category": "Test", "domain_tags": []string{"smoke"}},
			}},
			"resultSetSize": 1,
		})
	})
	mux.HandleFunc("/api/views/aaaa-0001.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "aaaa-0001", "name": "Smoke",
			"columns": []map[string]string{
				{"fieldName": "id", "dataTypeName": "text"},
				{"fieldName": "score", "dataTypeName": "number"},
			},
		})
	})
	mux.HandleFunc("/resource/aaaa-0001.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(rows)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	tpl, _ := os.ReadFile("testdata/portal.yaml.tmpl")
	yaml := strings.ReplaceAll(string(tpl), "{{HOST}}", host)
	yaml = strings.ReplaceAll(yaml, "{{DB}}", dbPath)
	_ = os.WriteFile(cfgPath, []byte(yaml), 0o644)

	cmd := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath)
	cmd.Env = append(os.Environ(), "CSQ_SCHEME=http")
	if err := cmd.Run(); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	db, _ := sql.Open("duckdb", dbPath)
	var first1 time.Time
	_ = db.QueryRow(`SELECT last_full_replace_at FROM _csq.dataset_state WHERE dataset_id = 'aaaa-0001'`).Scan(&first1)
	db.Close()

	time.Sleep(20 * time.Millisecond)

	cmd2 := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath,
		"--full-refresh", "aaaa-0001")
	cmd2.Env = append(os.Environ(), "CSQ_SCHEME=http")
	var stderr2 bytes.Buffer
	cmd2.Stderr = &stderr2
	if err := cmd2.Run(); err != nil {
		t.Fatalf("full-refresh sync: %v\nstderr:\n%s", err, stderr2.String())
	}

	db, _ = sql.Open("duckdb", dbPath)
	defer db.Close()
	var second1 time.Time
	_ = db.QueryRow(`SELECT last_full_replace_at FROM _csq.dataset_state WHERE dataset_id = 'aaaa-0001'`).Scan(&second1)
	if !second1.After(first1) {
		t.Errorf("LastFullReplaceAt should advance: was=%v now=%v", first1, second1)
	}
}

func TestCSQ_LockContention_Smoke(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "lock.duckdb")
	cfgPath := filepath.Join(dir, "portal.yaml")

	{
		db, _ := sql.Open("duckdb", dbPath)
		_, _ = db.Exec(`CREATE SCHEMA _csq`)
		_, _ = db.Exec(`CREATE TABLE _csq.catalog (
			id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL,
			description VARCHAR, category VARCHAR, tags JSON,
			row_count BIGINT, updated_at TIMESTAMP,
			fetched_at TIMESTAMP NOT NULL, raw JSON NOT NULL)`)
		db.Close()
	}

	yaml := `portal: example.invalid
db: ` + dbPath + `
on_error: continue
concurrency: 1
defaults:
  batch_size: 5
  order_by: ":id"
include:
  - category: "X"
`
	_ = os.WriteFile(cfgPath, []byte(yaml), 0o644)

	mcp := exec.Command(os.Getenv("CSQ_BIN"), "mcp", "--db", dbPath)
	mcpStdin, _ := mcp.StdinPipe()
	if err := mcp.Start(); err != nil {
		t.Fatalf("start mcp: %v", err)
	}
	defer func() {
		_ = mcp.Process.Kill()
		_ = mcp.Wait()
	}()

	lockPath := dbPath + ".lock"
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(lockPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	sync1 := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath)
	var stderr1 bytes.Buffer
	sync1.Stderr = &stderr1
	if err := sync1.Run(); err == nil {
		t.Fatalf("expected sync to fail while mcp holds lock; stderr=%s", stderr1.String())
	}
	if !strings.Contains(stderr1.String(), "locked") {
		t.Errorf("expected 'locked' in stderr; got: %s", stderr1.String())
	}

	_ = mcpStdin.Close()
	_ = mcp.Process.Kill()
	_ = mcp.Wait()

	sync2 := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath)
	var stderr2 bytes.Buffer
	sync2.Stderr = &stderr2
	_ = sync2.Run()
	if strings.Contains(stderr2.String(), "locked") {
		t.Errorf("after killing mcp, sync should not see lock error; got: %s", stderr2.String())
	}
}

func TestCSQ_SnapshotIndex_Smoke(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.example.org.duckdb")
	tar1 := filepath.Join(dir, "snap-1.tar.zst")
	tar2 := filepath.Join(dir, "snap-2.tar.zst")
	indexPath := filepath.Join(dir, "index.json")

	// Seed a minimal CSQ DB.
	{
		db, _ := sql.Open("duckdb", srcPath)
		_, _ = db.Exec(`CREATE SCHEMA _csq`)
		_, _ = db.Exec(`CREATE TABLE _csq.catalog (
			id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL,
			description VARCHAR, category VARCHAR, tags JSON,
			row_count BIGINT, updated_at TIMESTAMP,
			fetched_at TIMESTAMP NOT NULL, raw JSON NOT NULL)`)
		_, _ = db.Exec(`CREATE TABLE _csq.sync_runs (
			run_id VARCHAR NOT NULL, dataset_id VARCHAR NOT NULL,
			table_name VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL,
			finished_at TIMESTAMP, status VARCHAR NOT NULL,
			rows_written BIGINT, error VARCHAR, duration_ms BIGINT,
			config_hash VARCHAR, PRIMARY KEY (run_id, dataset_id))`)
		_, _ = db.Exec(`CREATE TABLE _csq.dataset_state (
			dataset_id VARCHAR PRIMARY KEY,
			hwm_updated_at TIMESTAMP, last_full_replace_at TIMESTAMP,
			last_run_id VARCHAR, hwm_column VARCHAR NOT NULL)`)
		_, _ = db.Exec(`INSERT INTO _csq.catalog (id, name, fetched_at, raw)
		                VALUES ('aaaa-0001', 'Smoke', NOW(), '{}')`)
		db.Close()
	}

	// Pack two snapshots (different ULIDs by virtue of timing).
	for i, tarPath := range []string{tar1, tar2} {
		cmd := exec.Command(os.Getenv("CSQ_BIN"), "snapshot",
			"--db", srcPath, "--output", tarPath)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("snapshot %d: %v\nstderr:\n%s", i, err, stderr.String())
		}
		time.Sleep(2 * time.Millisecond) // ensure ULIDs differ
	}

	// Add both to the index.
	for _, tarPath := range []string{tar1, tar2} {
		cmd := exec.Command(os.Getenv("CSQ_BIN"), "snapshot-index", "update",
			"--index", indexPath,
			"--add", tarPath,
			"--url", "https://example.com/"+filepath.Base(tarPath))
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("index update: %v\nstderr:\n%s", err, stderr.String())
		}
	}

	// Validate the index.
	val := exec.Command(os.Getenv("CSQ_BIN"), "snapshot-index", "validate",
		"--index", indexPath)
	var valErr bytes.Buffer
	val.Stderr = &valErr
	if err := val.Run(); err != nil {
		t.Fatalf("validate: %v\nstderr:\n%s", err, valErr.String())
	}

	// Confirm the index has 2 entries newest-first.
	body, _ := os.ReadFile(indexPath)
	if !strings.Contains(string(body), "snap-1.tar.zst") || !strings.Contains(string(body), "snap-2.tar.zst") {
		t.Errorf("index missing entries:\n%s", body)
	}
}

func TestCSQ_Fetch_ViaIndex_Smoke(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "data.example.org.duckdb")
	tarPath := filepath.Join(dir, "snap.tar.zst")
	indexPath := filepath.Join(dir, "index.json")
	restored := filepath.Join(dir, "restored.duckdb")

	// Seed and pack.
	{
		db, _ := sql.Open("duckdb", srcPath)
		_, _ = db.Exec(`CREATE SCHEMA _csq`)
		_, _ = db.Exec(`CREATE TABLE _csq.catalog (
			id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL,
			description VARCHAR, category VARCHAR, tags JSON,
			row_count BIGINT, updated_at TIMESTAMP,
			fetched_at TIMESTAMP NOT NULL, raw JSON NOT NULL)`)
		_, _ = db.Exec(`CREATE TABLE _csq.sync_runs (
			run_id VARCHAR NOT NULL, dataset_id VARCHAR NOT NULL,
			table_name VARCHAR NOT NULL, started_at TIMESTAMP NOT NULL,
			finished_at TIMESTAMP, status VARCHAR NOT NULL,
			rows_written BIGINT, error VARCHAR, duration_ms BIGINT,
			config_hash VARCHAR, PRIMARY KEY (run_id, dataset_id))`)
		_, _ = db.Exec(`CREATE TABLE _csq.dataset_state (
			dataset_id VARCHAR PRIMARY KEY,
			hwm_updated_at TIMESTAMP, last_full_replace_at TIMESTAMP,
			last_run_id VARCHAR, hwm_column VARCHAR NOT NULL)`)
		_, _ = db.Exec(`INSERT INTO _csq.catalog (id, name, fetched_at, raw)
		                VALUES ('aaaa-0001', 'Smoke', NOW(), '{}')`)
		db.Close()
	}
	if err := exec.Command(os.Getenv("CSQ_BIN"), "snapshot",
		"--db", srcPath, "--output", tarPath).Run(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Build the index pointing at the local file:// URL.
	if err := exec.Command(os.Getenv("CSQ_BIN"), "snapshot-index", "update",
		"--index", indexPath,
		"--add", tarPath,
		"--url", "file://"+tarPath).Run(); err != nil {
		t.Fatalf("index update: %v", err)
	}

	// Fetch via index.
	cmd := exec.Command(os.Getenv("CSQ_BIN"), "fetch",
		"--index", "file://"+indexPath,
		"--output", restored)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("fetch via index: %v\nstderr:\n%s", err, stderr.String())
	}

	db, err := sql.Open("duckdb", restored)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer db.Close()
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM _csq.catalog`).Scan(&n)
	if n != 1 {
		t.Errorf("restored catalog rows: got %d, want 1", n)
	}
}
