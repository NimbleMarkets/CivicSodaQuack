# Phase 4 — Snapshot Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `csq snapshot` (package an existing per-portal DuckDB into a `.tar.zst` with manifest) and `csq fetch` (download/verify/unpack a snapshot from any HTTP/HTTPS/file URL) so synced data is portable across hosts.

**Architecture:** A new `internal/snapshot` package owns the format. `Pack(ctx, ProducerOptions)` does temp-copy → optional `_csq_staging` cleanup → SHA-256 + count computation → tar+zst stream with `manifest.json` first, DuckDB second. `Fetch(ctx, ConsumerOptions)` streams the inverse, validating filename/size/SHA-256 against the manifest before declaring success. CLI subcommands are thin wrappers.

**Tech Stack:** Go 1.24, DuckDB (`duckdb-go/v2`), `github.com/klauspost/compress/zstd` (currently transitive — promoted by `go mod tidy` on first import), `github.com/oklog/ulid/v2` (already direct), `archive/tar` from stdlib, pflag.

---

## File Structure

**Create:**
- `internal/snapshot/manifest.go` — `Manifest` struct + `SchemaVersion` constant + `(*Manifest).MarshalIndent` + `ParseManifest`.
- `internal/snapshot/manifest_test.go`
- `internal/snapshot/tarzst.go` — `tarZstWriter` and `tarZstReader` wrapping `archive/tar` + `klauspost/compress/zstd`.
- `internal/snapshot/tarzst_test.go`
- `internal/snapshot/inspect.go` — `assertIsCSQDB`, `countDatasets`, `countTotalRows`.
- `internal/snapshot/inspect_test.go`
- `internal/snapshot/fixtures_test.go` — `seedFixtureDB` + `FixtureDataset` (adapted from `internal/mcpserver/fixtures_test.go`).
- `internal/snapshot/producer.go` — `ProducerOptions`, `Pack(ctx, opts) (*Manifest, error)`.
- `internal/snapshot/producer_test.go`
- `internal/snapshot/consumer.go` — `ConsumerOptions`, `Fetch(ctx, opts) (*Manifest, error)`.
- `internal/snapshot/consumer_test.go`
- `cmd/csq/snapshot.go` — `runSnapshot(args)`.
- `cmd/csq/fetch.go` — `runFetch(args)`.

**Modify:**
- `cmd/csq/main.go` — dispatch `case "snapshot"` and `case "fetch"`; update `usage`.
- `cmd/csq/cli_smoke_test.go` — append `TestCSQ_Snapshot_RoundTrip_Smoke`.
- `README.md` — append a "Distribution" section.
- `go.mod` / `go.sum` — `klauspost/compress` promoted from indirect to direct after first import (handled automatically by `go mod tidy`).

---

## Task 1: Manifest types + JSON helpers

**Files:**
- Create: `internal/snapshot/manifest.go`
- Create: `internal/snapshot/manifest_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/snapshot/manifest_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"strings"
	"testing"
	"time"
)

func TestManifest_MarshalIndentRoundTrip(t *testing.T) {
	in := &Manifest{
		SchemaVersion:   1,
		Portal:          "data.cityofchicago.org",
		CSQVersion:      "0.4.0",
		SnapshotID:      "01HXYZABCDEFGHJKMNPQRSTVWX",
		CreatedAt:       time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC),
		DuckDBFilename:  "data.cityofchicago.org.duckdb",
		DuckDBSHA256:    "abc123",
		DuckDBSizeBytes: 12345678,
		DatasetCount:    47,
		TotalRowCount:   12345678,
	}
	b, err := in.MarshalIndent()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := ParseManifest(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if out.Portal != in.Portal || out.SchemaVersion != in.SchemaVersion {
		t.Errorf("round-trip mismatch: in=%+v out=%+v", in, out)
	}
	if !out.CreatedAt.Equal(in.CreatedAt) {
		t.Errorf("CreatedAt: in=%v out=%v", in.CreatedAt, out.CreatedAt)
	}
	if out.DuckDBSizeBytes != in.DuckDBSizeBytes {
		t.Errorf("DuckDBSizeBytes: %d vs %d", in.DuckDBSizeBytes, out.DuckDBSizeBytes)
	}
}

func TestManifest_ParseRejectsInvalidJSON(t *testing.T) {
	_, err := ParseManifest([]byte("{ not json"))
	if err == nil {
		t.Fatal("want parse error")
	}
}

func TestManifest_MarshalIsIndented(t *testing.T) {
	m := &Manifest{SchemaVersion: 1, Portal: "p", CSQVersion: "v", SnapshotID: "i", DuckDBFilename: "f.duckdb", DuckDBSHA256: "h"}
	b, err := m.MarshalIndent()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), "\n  ") {
		t.Errorf("expected indented output with leading spaces, got:\n%s", b)
	}
}

func TestSchemaVersion_IsOne(t *testing.T) {
	if SchemaVersion != 1 {
		t.Errorf("SchemaVersion: got %d, want 1", SchemaVersion)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/snapshot/ -run TestManifest -v`
Expected: FAIL — `Manifest` / `MarshalIndent` / `ParseManifest` / `SchemaVersion` undefined (or build error if package doesn't exist yet).

- [ ] **Step 3: Write manifest.go**

Create `internal/snapshot/manifest.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"encoding/json"
	"fmt"
	"time"
)

// SchemaVersion is the manifest schema this build emits and accepts.
// Bump on breaking format changes; consumers reject other values.
const SchemaVersion = 1

// Manifest is the JSON sidecar at the head of every Phase 4 snapshot tarball.
type Manifest struct {
	SchemaVersion   int       `json:"schema_version"`
	Portal          string    `json:"portal"`
	CSQVersion      string    `json:"csq_version"`
	SnapshotID      string    `json:"snapshot_id"`
	CreatedAt       time.Time `json:"created_at"`
	DuckDBFilename  string    `json:"duckdb_filename"`
	DuckDBSHA256    string    `json:"duckdb_sha256"`
	DuckDBSizeBytes int64     `json:"duckdb_size_bytes"`
	DatasetCount    int64     `json:"dataset_count"`
	TotalRowCount   int64     `json:"total_row_count"`
}

// MarshalIndent renders the manifest as 2-space-indented JSON suitable for the
// tarball's manifest.json entry.
func (m *Manifest) MarshalIndent() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// ParseManifest decodes a manifest from JSON bytes.
func ParseManifest(b []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	return &m, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/snapshot/ -run TestManifest -v`
Expected: all 4 pass.

- [ ] **Step 5: Commit**

```bash
git add internal/snapshot/manifest.go internal/snapshot/manifest_test.go
git commit -m "snapshot: add Manifest type with JSON round-trip"
```

---

## Task 2: tar+zst codec wrapper

**Files:**
- Create: `internal/snapshot/tarzst.go`
- Create: `internal/snapshot/tarzst_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/snapshot/tarzst_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"
)

func TestTarZst_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := newTarZstWriter(&buf)

	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	if err := w.WriteEntry("manifest.json", 11, now, strings.NewReader(`hello world`)); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	body := bytes.Repeat([]byte("X"), 1024)
	if err := w.WriteEntry("payload.bin", int64(len(body)), now, bytes.NewReader(body)); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	r, err := newTarZstReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer r.Close()

	hdr, body1, err := r.Next()
	if err != nil {
		t.Fatalf("next 1: %v", err)
	}
	if hdr.Name != "manifest.json" || hdr.Size != 11 {
		t.Errorf("entry 1 header: %+v", hdr)
	}
	got1, _ := io.ReadAll(body1)
	if string(got1) != "hello world" {
		t.Errorf("entry 1 body: %q", got1)
	}

	hdr2, body2, err := r.Next()
	if err != nil {
		t.Fatalf("next 2: %v", err)
	}
	if hdr2.Name != "payload.bin" || hdr2.Size != 1024 {
		t.Errorf("entry 2 header: %+v", hdr2)
	}
	got2, _ := io.ReadAll(body2)
	if !bytes.Equal(got2, body) {
		t.Errorf("entry 2 body length=%d", len(got2))
	}

	if _, _, err := r.Next(); err != io.EOF {
		t.Errorf("want EOF after 2 entries, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/snapshot/ -run TestTarZst -v`
Expected: FAIL — `newTarZstWriter` / `newTarZstReader` undefined.

- [ ] **Step 3: Write tarzst.go**

Create `internal/snapshot/tarzst.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"archive/tar"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"
)

// tarZstWriter wraps an io.Writer with a streaming tar+zstd codec.
// Each WriteEntry call writes one tar entry; Close flushes both layers.
type tarZstWriter struct {
	zw *zstd.Encoder
	tw *tar.Writer
}

func newTarZstWriter(w io.Writer) *tarZstWriter {
	zw, _ := zstd.NewWriter(w) // err is always nil for default options per docs
	tw := tar.NewWriter(zw)
	return &tarZstWriter{zw: zw, tw: tw}
}

// WriteEntry writes one regular-file entry. body is read until EOF; it must
// produce exactly size bytes (tar enforces this and returns an error otherwise).
func (w *tarZstWriter) WriteEntry(name string, size int64, modTime time.Time, body io.Reader) error {
	hdr := &tar.Header{
		Name:     name,
		Size:     size,
		Mode:     0o644,
		ModTime:  modTime,
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	}
	if err := w.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar header %q: %w", name, err)
	}
	if _, err := io.Copy(w.tw, body); err != nil {
		return fmt.Errorf("tar body %q: %w", name, err)
	}
	return nil
}

// Close flushes the tar trailer and the zstd frame. Safe to call once.
func (w *tarZstWriter) Close() error {
	if err := w.tw.Close(); err != nil {
		return fmt.Errorf("close tar: %w", err)
	}
	if err := w.zw.Close(); err != nil {
		return fmt.Errorf("close zstd: %w", err)
	}
	return nil
}

// tarZstReader decodes a tar+zstd stream entry by entry.
type tarZstReader struct {
	zr *zstd.Decoder
	tr *tar.Reader
}

func newTarZstReader(r io.Reader) (*tarZstReader, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("zstd reader: %w", err)
	}
	tr := tar.NewReader(zr)
	return &tarZstReader{zr: zr, tr: tr}, nil
}

// Next advances to the next entry and returns its header plus a body reader
// scoped to that entry's bytes. Returns io.EOF after the last entry.
func (r *tarZstReader) Next() (*tar.Header, io.Reader, error) {
	hdr, err := r.tr.Next()
	if err != nil {
		return nil, nil, err
	}
	return hdr, r.tr, nil
}

// Close releases the zstd decoder. The underlying io.Reader is not closed.
func (r *tarZstReader) Close() error {
	r.zr.Close()
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/snapshot/ -run TestTarZst -v`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add internal/snapshot/tarzst.go internal/snapshot/tarzst_test.go
git commit -m "snapshot: add tar+zst streaming codec wrapper"
```

---

## Task 3: SQL inspection helpers

`assertIsCSQDB`, `countDatasets`, `countTotalRows` — used by the producer to validate the source DB and populate the manifest.

**Files:**
- Create: `internal/snapshot/inspect.go`
- Create: `internal/snapshot/inspect_test.go`
- Create: `internal/snapshot/fixtures_test.go`

### Step 1: Write fixtures_test.go

Create `internal/snapshot/fixtures_test.go`:

```go
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
```

### Step 2: Write the failing inspect test

Create `internal/snapshot/inspect_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openFixtureDB(t *testing.T, datasets ...FixtureDataset) (*sql.DB, string) {
	t.Helper()
	dir := t.TempDir()
	path := seedFixtureDB(t, dir, "fixture.duckdb", datasets...)
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}

func TestAssertIsCSQDB_Valid(t *testing.T) {
	db, path := openFixtureDB(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	if err := assertIsCSQDB(db, path); err != nil {
		t.Errorf("want nil err, got %v", err)
	}
}

func TestAssertIsCSQDB_Invalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wrong.duckdb")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE foo (x INT)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err = assertIsCSQDB(db, path)
	if err == nil || !strings.Contains(err.Error(), "not a CivicSodaQuack DuckDB") {
		t.Errorf("got %v", err)
	}
}

func TestCountDatasets(t *testing.T) {
	db, _ := openFixtureDB(t,
		FixtureDataset{ID: "aaaa-0001", Name: "A"},
		FixtureDataset{ID: "bbbb-0002", Name: "B"},
		FixtureDataset{ID: "cccc-0003", Name: "C"})
	got, err := countDatasets(db)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if got != 3 {
		t.Errorf("got %d, want 3", got)
	}
}

func TestCountTotalRows_LatestOKPerDataset(t *testing.T) {
	hwm := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	db, _ := openFixtureDB(t,
		FixtureDataset{
			ID: "aaaa-0001", Name: "A", TableName: "a",
			ColumnDefs: []string{"v INT"},
			Rows:       []map[string]any{{"v": 1}, {"v": 2}, {"v": 3}},
			Synced:     true, HWM: hwm,
		},
		FixtureDataset{
			ID: "bbbb-0002", Name: "B", TableName: "b",
			ColumnDefs: []string{"v INT"},
			Rows:       []map[string]any{{"v": 1}, {"v": 2}, {"v": 3}, {"v": 4}, {"v": 5}},
			Synced:     true, HWM: hwm,
		},
		FixtureDataset{ID: "cccc-0003", Name: "C", Synced: false}) // never synced

	got, err := countTotalRows(db)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	// 3 + 5 = 8; cccc-0003 contributes 0 (no sync_runs row).
	if got != 8 {
		t.Errorf("got %d, want 8", got)
	}
}

func TestCountTotalRows_IgnoresFailedRuns(t *testing.T) {
	hwm := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	db, _ := openFixtureDB(t,
		FixtureDataset{
			ID: "aaaa-0001", Name: "A", TableName: "a",
			ColumnDefs: []string{"v INT"},
			Rows:       []map[string]any{{"v": 1}, {"v": 2}},
			Synced:     true, HWM: hwm,
		})
	// Add a later 'failed' run with a higher rows_written; it must NOT be counted.
	_, err := db.Exec(
		`INSERT INTO _csq.sync_runs
		   (run_id, dataset_id, table_name, started_at, status, rows_written, config_hash)
		 VALUES ('01HLATER', 'aaaa-0001', 'a', $1, 'failed', 999, 'sha256:other')`,
		hwm.Add(time.Hour))
	if err != nil {
		t.Fatalf("seed failed run: %v", err)
	}
	got, _ := countTotalRows(db)
	if got != 2 {
		t.Errorf("got %d, want 2 (failed run must be ignored)", got)
	}
}
```

### Step 3: Run test to verify it fails

Run: `go test ./internal/snapshot/ -run "TestAssertIsCSQDB|TestCountDatasets|TestCountTotalRows" -v`
Expected: FAIL — `assertIsCSQDB` / `countDatasets` / `countTotalRows` undefined.

### Step 4: Write inspect.go

Create `internal/snapshot/inspect.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"database/sql"
	"fmt"
)

// assertIsCSQDB returns nil if db has a _csq.catalog table.
func assertIsCSQDB(db *sql.DB, path string) error {
	var n int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema = '_csq' AND table_name = 'catalog'`).Scan(&n)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", path, err)
	}
	if n == 0 {
		return fmt.Errorf("not a CivicSodaQuack DuckDB (no _csq.catalog in %s)", path)
	}
	return nil
}

// countDatasets returns the count of rows in _csq.catalog.
func countDatasets(db *sql.DB) (int64, error) {
	var n int64
	err := db.QueryRow(`SELECT COUNT(*) FROM _csq.catalog`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count datasets: %w", err)
	}
	return n, nil
}

// countTotalRows returns the SUM of rows_written across the most recent
// status='ok' row per dataset_id. Datasets that have never successfully synced
// contribute 0. Failed/aborted runs are ignored.
func countTotalRows(db *sql.DB) (int64, error) {
	var n sql.NullInt64
	err := db.QueryRow(`
		SELECT SUM(rows_written) FROM (
			SELECT FIRST(rows_written ORDER BY started_at DESC) AS rows_written
			FROM _csq.sync_runs
			WHERE status = 'ok'
			GROUP BY dataset_id
		) latest`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count total rows: %w", err)
	}
	if !n.Valid {
		return 0, nil
	}
	return n.Int64, nil
}
```

### Step 5: Run tests to verify they pass

Run: `go test ./internal/snapshot/ -v`
Expected: all (Manifest + TarZst + Inspect) pass.

### Step 6: Commit

```bash
git add internal/snapshot/inspect.go internal/snapshot/inspect_test.go internal/snapshot/fixtures_test.go
git commit -m "snapshot: add SQL inspection helpers (assert/count) + test fixtures"
```

---

## Task 4: Producer (`Pack`)

**Files:**
- Create: `internal/snapshot/producer.go`
- Create: `internal/snapshot/producer_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/snapshot/producer_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// readTarball opens a .tar.zst, returns the manifest plus a map of remaining
// entry name → body bytes (for assertions).
func readTarball(t *testing.T, path string) (*Manifest, map[string][]byte) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open tarball: %v", err)
	}
	defer f.Close()
	r, err := newTarZstReader(f)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer r.Close()

	hdr, body, err := r.Next()
	if err != nil {
		t.Fatalf("first entry: %v", err)
	}
	if hdr.Name != "manifest.json" {
		t.Fatalf("first entry name: %q", hdr.Name)
	}
	mb, _ := io.ReadAll(body)
	m, err := ParseManifest(mb)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	rest := map[string][]byte{}
	for {
		hdr, body, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		b, _ := io.ReadAll(body)
		rest[hdr.Name] = b
	}
	return m, rest
}

func TestPack_HappyPath(t *testing.T) {
	hwm := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	srcPath := seedFixtureDB(t, dir, "data.cityofchicago.org.duckdb",
		FixtureDataset{
			ID: "aaaa-0001", Name: "Crimes", TableName: "a",
			ColumnDefs: []string{"v INT"},
			Rows:       []map[string]any{{"v": 1}, {"v": 2}},
			Synced:     true, HWM: hwm,
		})
	outPath := filepath.Join(dir, "snap.tar.zst")

	m, err := Pack(context.Background(), ProducerOptions{
		DBPath: srcPath, OutputPath: outPath,
	})
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if m.SchemaVersion != 1 || m.Portal != "data_cityofchicago_org" {
		t.Errorf("manifest: %+v", m)
	}
	if m.DuckDBFilename != "data.cityofchicago.org.duckdb" {
		t.Errorf("filename: %q", m.DuckDBFilename)
	}
	if m.DatasetCount != 1 || m.TotalRowCount != 2 {
		t.Errorf("counts: ds=%d rows=%d", m.DatasetCount, m.TotalRowCount)
	}
	if m.SnapshotID == "" || m.CreatedAt.IsZero() {
		t.Errorf("snapshot_id or created_at empty: %+v", m)
	}
	if m.DuckDBSizeBytes <= 0 {
		t.Errorf("size: %d", m.DuckDBSizeBytes)
	}
	if len(m.DuckDBSHA256) != 64 {
		t.Errorf("sha256 length: %d", len(m.DuckDBSHA256))
	}

	// Inspect tarball: manifest first (already verified by readTarball), DuckDB second.
	mFromFile, rest := readTarball(t, outPath)
	if mFromFile.SnapshotID != m.SnapshotID {
		t.Errorf("manifest id mismatch: %q vs %q", mFromFile.SnapshotID, m.SnapshotID)
	}
	body, ok := rest[m.DuckDBFilename]
	if !ok {
		t.Fatalf("payload entry %q missing; have %v", m.DuckDBFilename, keys(rest))
	}
	if int64(len(body)) != m.DuckDBSizeBytes {
		t.Errorf("body size %d vs manifest %d", len(body), m.DuckDBSizeBytes)
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != m.DuckDBSHA256 {
		t.Errorf("sha256 mismatch")
	}
}

func TestPack_PortalOverride(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "anything.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "snap.tar.zst")
	m, err := Pack(context.Background(), ProducerOptions{
		DBPath: src, OutputPath: out, Portal: "custom-portal-name",
	})
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if m.Portal != "custom-portal-name" {
		t.Errorf("portal: %q", m.Portal)
	}
}

func TestPack_DropsStaging(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "test.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	// Inject a stray staging table.
	{
		db, _ := sql.Open("duckdb", src)
		if _, err := db.Exec(`CREATE TABLE _csq_staging.leftover (x INT)`); err != nil {
			t.Fatalf("seed leftover: %v", err)
		}
		db.Close()
	}
	out := filepath.Join(dir, "snap.tar.zst")
	if _, err := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out}); err != nil {
		t.Fatalf("pack: %v", err)
	}
	// Unpack and verify _csq_staging is empty in the result.
	_, rest := readTarball(t, out)
	tmp := filepath.Join(dir, "restored.duckdb")
	if err := os.WriteFile(tmp, rest["test.duckdb"], 0o644); err != nil {
		t.Fatalf("write restored: %v", err)
	}
	db, _ := sql.Open("duckdb", tmp)
	defer db.Close()
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '_csq_staging'`,
	).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 0 {
		t.Errorf("staging not cleaned: %d tables remain", n)
	}
}

func TestPack_KeepStaging(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "test.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	{
		db, _ := sql.Open("duckdb", src)
		if _, err := db.Exec(`CREATE TABLE _csq_staging.leftover (x INT)`); err != nil {
			t.Fatalf("seed leftover: %v", err)
		}
		db.Close()
	}
	out := filepath.Join(dir, "snap.tar.zst")
	if _, err := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out, KeepStaging: true}); err != nil {
		t.Fatalf("pack: %v", err)
	}
	_, rest := readTarball(t, out)
	tmp := filepath.Join(dir, "restored.duckdb")
	_ = os.WriteFile(tmp, rest["test.duckdb"], 0o644)
	db, _ := sql.Open("duckdb", tmp)
	defer db.Close()
	var n int
	_ = db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema = '_csq_staging' AND table_name = 'leftover'`,
	).Scan(&n)
	if n != 1 {
		t.Errorf("staging cleaned despite KeepStaging=true; got %d", n)
	}
}

func TestPack_NotCSQDB(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.duckdb")
	db, _ := sql.Open("duckdb", bad)
	_, _ = db.Exec(`CREATE TABLE foo (x INT)`)
	db.Close()
	out := filepath.Join(dir, "snap.tar.zst")
	_, err := Pack(context.Background(), ProducerOptions{DBPath: bad, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "not a CivicSodaQuack DuckDB") {
		t.Errorf("got %v", err)
	}
	if _, statErr := os.Stat(out); statErr == nil {
		t.Errorf("output file should not exist on failure")
	}
}

func TestPack_OutputExists_NoForce(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "src.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "snap.tar.zst")
	if err := os.WriteFile(out, []byte("preexisting"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "exists") {
		t.Errorf("want exists error, got %v", err)
	}
	// Existing content should be untouched.
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, []byte("preexisting")) {
		t.Errorf("existing file overwritten without --force")
	}
}

func TestPack_OutputExists_Force(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "src.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "snap.tar.zst")
	_ = os.WriteFile(out, []byte("preexisting"), 0o644)
	_, err := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out, Force: true})
	if err != nil {
		t.Fatalf("pack with force: %v", err)
	}
	got, _ := os.ReadFile(out)
	if bytes.Equal(got, []byte("preexisting")) {
		t.Errorf("expected overwrite")
	}
}

func TestPack_DBMissing(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "snap.tar.zst")
	_, err := Pack(context.Background(), ProducerOptions{DBPath: "/nonexistent/x.duckdb", OutputPath: out})
	if err == nil {
		t.Fatal("want error for missing db")
	}
}

func TestPack_CSQVersionDefault(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "src.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "snap.tar.zst")
	m, _ := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out})
	if m.CSQVersion != "0.4.0-dev" {
		t.Errorf("default CSQVersion: got %q, want 0.4.0-dev", m.CSQVersion)
	}
}

func keys(m map[string][]byte) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	return out
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/snapshot/ -run TestPack -v`
Expected: FAIL — `Pack`/`ProducerOptions` undefined.

- [ ] **Step 3: Write producer.go**

Create `internal/snapshot/producer.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	_ "github.com/duckdb/duckdb-go/v2"
)

// ProducerOptions configures Pack.
type ProducerOptions struct {
	DBPath      string // source DuckDB; required
	OutputPath  string // destination tarball (.tar.zst); required
	Portal      string // overrides portal name in manifest; "" derives from filename
	KeepStaging bool   // skip _csq_staging cleanup
	Force       bool   // overwrite existing OutputPath
	CSQVersion  string // injected by CLI; "" → "0.4.0-dev"
}

// Pack copies the source DuckDB to a temp file, optionally cleans
// _csq_staging, computes counts and SHA-256, then streams a tar+zst archive
// to OutputPath with manifest.json followed by the DuckDB.
//
// Returns the populated manifest. On error, any partial OutputPath is removed.
func Pack(ctx context.Context, opts ProducerOptions) (*Manifest, error) {
	if _, err := os.Stat(opts.DBPath); err != nil {
		return nil, fmt.Errorf("snapshot: --db %s: %w", opts.DBPath, err)
	}
	if !opts.Force {
		if _, err := os.Stat(opts.OutputPath); err == nil {
			return nil, fmt.Errorf("snapshot: %s exists; pass --force to overwrite", opts.OutputPath)
		}
	}

	csqVersion := opts.CSQVersion
	if csqVersion == "" {
		csqVersion = "0.4.0-dev"
	}
	portal := opts.Portal
	if portal == "" {
		portal = portalFromPath(opts.DBPath)
	}

	// Temp file in the same directory as OutputPath so a future rename is atomic.
	tmpDir := filepath.Dir(opts.OutputPath)
	tmp, err := os.CreateTemp(tmpDir, "csq-snapshot-*.duckdb")
	if err != nil {
		return nil, fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)

	if err := copyFile(opts.DBPath, tmpPath); err != nil {
		return nil, fmt.Errorf("copy db: %w", err)
	}

	// Open the temp DB for assertion + optional staging cleanup.
	tmpDB, err := sql.Open("duckdb", tmpPath)
	if err != nil {
		return nil, fmt.Errorf("open temp db: %w", err)
	}
	if err := assertIsCSQDB(tmpDB, opts.DBPath); err != nil {
		tmpDB.Close()
		return nil, err
	}
	if !opts.KeepStaging {
		if _, err := tmpDB.ExecContext(ctx, `DROP SCHEMA IF EXISTS _csq_staging CASCADE`); err != nil {
			tmpDB.Close()
			return nil, fmt.Errorf("drop staging: %w", err)
		}
		if _, err := tmpDB.ExecContext(ctx, `CREATE SCHEMA _csq_staging`); err != nil {
			tmpDB.Close()
			return nil, fmt.Errorf("recreate staging schema: %w", err)
		}
	}
	dsCount, err := countDatasets(tmpDB)
	if err != nil {
		tmpDB.Close()
		return nil, err
	}
	rowCount, err := countTotalRows(tmpDB)
	if err != nil {
		tmpDB.Close()
		return nil, err
	}
	if err := tmpDB.Close(); err != nil {
		return nil, fmt.Errorf("close temp db: %w", err)
	}

	// Hash + size of the temp DB bytes.
	sum, size, err := sha256AndSize(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("hash temp db: %w", err)
	}

	manifest := &Manifest{
		SchemaVersion:   SchemaVersion,
		Portal:          portal,
		CSQVersion:      csqVersion,
		SnapshotID:      newSnapshotID(),
		CreatedAt:       time.Now().UTC(),
		DuckDBFilename:  filepath.Base(opts.DBPath),
		DuckDBSHA256:    sum,
		DuckDBSizeBytes: size,
		DatasetCount:    dsCount,
		TotalRowCount:   rowCount,
	}

	if err := writeTarball(opts.OutputPath, manifest, tmpPath); err != nil {
		_ = os.Remove(opts.OutputPath)
		return nil, err
	}
	return manifest, nil
}

// portalFromPath strips directory and .duckdb suffix, then replaces dots with underscores.
func portalFromPath(p string) string {
	base := filepath.Base(p)
	base = strings.TrimSuffix(base, ".duckdb")
	return strings.ReplaceAll(base, ".", "_")
}

// copyFile streams src to dst, replacing dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// sha256AndSize streams the file once, returning hex-encoded SHA-256 and byte size.
func sha256AndSize(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// writeTarball opens outputPath (truncating) and streams manifest + DuckDB.
func writeTarball(outputPath string, manifest *Manifest, dbPath string) error {
	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	w := newTarZstWriter(out)

	mb, err := manifest.MarshalIndent()
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := w.WriteEntry("manifest.json", int64(len(mb)), manifest.CreatedAt, strings.NewReader(string(mb))); err != nil {
		return err
	}

	dbF, err := os.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open temp for tar: %w", err)
	}
	defer dbF.Close()
	if err := w.WriteEntry(manifest.DuckDBFilename, manifest.DuckDBSizeBytes, manifest.CreatedAt, dbF); err != nil {
		return err
	}
	return w.Close()
}

// newSnapshotID returns a fresh ULID string.
func newSnapshotID() string {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/snapshot/ -run TestPack -v`
Expected: all 9 pass.

- [ ] **Step 5: Run full snapshot suite**

Run: `go test ./internal/snapshot/ -v`
Expected: all (Manifest + TarZst + Inspect + Pack) pass.

- [ ] **Step 6: Commit**

```bash
git add internal/snapshot/producer.go internal/snapshot/producer_test.go go.mod go.sum
git commit -m "snapshot: add Pack producer with manifest + temp-copy + staging cleanup"
```

---

## Task 5: Consumer (`Fetch`)

**Files:**
- Create: `internal/snapshot/consumer.go`
- Create: `internal/snapshot/consumer_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/snapshot/consumer_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// packTo uses the producer to write a snapshot we then exercise.
func packTo(t *testing.T, dir, outName string, datasets ...FixtureDataset) (srcPath, outPath string) {
	t.Helper()
	srcPath = seedFixtureDB(t, dir, "src.duckdb", datasets...)
	outPath = filepath.Join(dir, outName)
	if _, err := Pack(context.Background(), ProducerOptions{DBPath: srcPath, OutputPath: outPath}); err != nil {
		t.Fatalf("pack: %v", err)
	}
	return
}

func TestFetch_FileURL_HappyPath(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})

	out := filepath.Join(dir, "restored.duckdb")
	m, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + tarPath, OutputPath: out,
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if m.DatasetCount != 1 {
		t.Errorf("manifest: %+v", m)
	}
	// Output should be a valid CSQ DuckDB.
	db, _ := sql.Open("duckdb", out)
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM _csq.catalog`).Scan(&n); err != nil {
		t.Fatalf("query restored: %v", err)
	}
	if n != 1 {
		t.Errorf("restored catalog rows: %d", n)
	}
}

func TestFetch_HTTPHappyPath(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, tarPath)
	}))
	defer srv.Close()

	out := filepath.Join(dir, "restored.duckdb")
	if _, err := Fetch(context.Background(), ConsumerOptions{
		URL: srv.URL, OutputPath: out,
	}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Errorf("output missing: %v", err)
	}
}

func TestFetch_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "kaboom", 500)
	}))
	defer srv.Close()

	dir := t.TempDir()
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: srv.URL, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("want HTTP 500 in error, got %v", err)
	}
}

func TestFetch_UnsupportedScheme(t *testing.T) {
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "s3://bucket/foo", OutputPath: "/tmp/out.duckdb",
	})
	if err == nil || !strings.Contains(err.Error(), "scheme") {
		t.Errorf("want scheme error, got %v", err)
	}
}

func TestFetch_BadFirstEntry(t *testing.T) {
	// Hand-craft a tarball where the first entry is NOT manifest.json.
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.tar.zst")
	{
		f, _ := os.Create(bad)
		w := newTarZstWriter(f)
		_ = w.WriteEntry("payload.bin", 4, timeFixed(), bytes.NewReader([]byte("data")))
		_ = w.Close()
		f.Close()
	}
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + bad, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "manifest.json") {
		t.Errorf("want first-entry error, got %v", err)
	}
}

func TestFetch_UnsupportedSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.tar.zst")
	{
		f, _ := os.Create(bad)
		w := newTarZstWriter(f)
		body := []byte(`{"schema_version":99,"portal":"x","csq_version":"v","snapshot_id":"i","duckdb_filename":"x.duckdb","duckdb_sha256":"00","duckdb_size_bytes":0}`)
		_ = w.WriteEntry("manifest.json", int64(len(body)), timeFixed(), bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + bad, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Errorf("want schema_version error, got %v", err)
	}
}

func TestFetch_UnsafeFilename(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.tar.zst")
	{
		f, _ := os.Create(bad)
		w := newTarZstWriter(f)
		body := []byte(`{"schema_version":1,"portal":"x","csq_version":"v","snapshot_id":"i","duckdb_filename":"../etc/passwd","duckdb_sha256":"00","duckdb_size_bytes":0}`)
		_ = w.WriteEntry("manifest.json", int64(len(body)), timeFixed(), bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + bad, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe filename") {
		t.Errorf("want unsafe-filename error, got %v", err)
	}
}

func TestFetch_FilenameMismatch(t *testing.T) {
	// Pack normally, then rebuild a tarball with the payload renamed.
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})

	mfst, rest := readTarball(t, tarPath)
	rebuilt := filepath.Join(dir, "rebuilt.tar.zst")
	{
		f, _ := os.Create(rebuilt)
		w := newTarZstWriter(f)
		mb, _ := mfst.MarshalIndent()
		_ = w.WriteEntry("manifest.json", int64(len(mb)), mfst.CreatedAt, bytes.NewReader(mb))
		body := rest[mfst.DuckDBFilename]
		_ = w.WriteEntry("renamed.duckdb", int64(len(body)), mfst.CreatedAt, bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + rebuilt, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "unexpected payload") {
		t.Errorf("want unexpected-payload error, got %v", err)
	}
}

func TestFetch_SizeMismatch(t *testing.T) {
	// Build a tarball where the manifest says 999 bytes but the payload entry has 4 bytes.
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.tar.zst")
	{
		f, _ := os.Create(bad)
		w := newTarZstWriter(f)
		body := []byte(`{"schema_version":1,"portal":"x","csq_version":"v","snapshot_id":"i","duckdb_filename":"x.duckdb","duckdb_sha256":"00","duckdb_size_bytes":999}`)
		_ = w.WriteEntry("manifest.json", int64(len(body)), timeFixed(), bytes.NewReader(body))
		_ = w.WriteEntry("x.duckdb", 4, timeFixed(), bytes.NewReader([]byte("abcd")))
		_ = w.Close()
		f.Close()
	}
	out := filepath.Join(dir, "out.duckdb")
	_, err := Fetch(context.Background(), ConsumerOptions{URL: "file://" + bad, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "size mismatch") {
		t.Errorf("want size-mismatch error, got %v", err)
	}
	if _, statErr := os.Stat(out); statErr == nil {
		t.Errorf("partial output should be removed")
	}
}

func TestFetch_SHA256Mismatch(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	// Mutate the tarball: re-write manifest with a wrong SHA so size still matches.
	mfst, rest := readTarball(t, tarPath)
	mfst.DuckDBSHA256 = strings.Repeat("0", 64)
	mutated := filepath.Join(dir, "mut.tar.zst")
	{
		f, _ := os.Create(mutated)
		w := newTarZstWriter(f)
		mb, _ := mfst.MarshalIndent()
		_ = w.WriteEntry("manifest.json", int64(len(mb)), mfst.CreatedAt, bytes.NewReader(mb))
		body := rest[mfst.DuckDBFilename]
		_ = w.WriteEntry(mfst.DuckDBFilename, int64(len(body)), mfst.CreatedAt, bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	out := filepath.Join(dir, "out.duckdb")
	_, err := Fetch(context.Background(), ConsumerOptions{URL: "file://" + mutated, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
		t.Errorf("want sha256-mismatch error, got %v", err)
	}
	if _, statErr := os.Stat(out); statErr == nil {
		t.Errorf("partial output should be removed")
	}
}

func TestFetch_NoVerifySkipsSHA(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	mfst, rest := readTarball(t, tarPath)
	mfst.DuckDBSHA256 = strings.Repeat("0", 64)
	mutated := filepath.Join(dir, "mut.tar.zst")
	{
		f, _ := os.Create(mutated)
		w := newTarZstWriter(f)
		mb, _ := mfst.MarshalIndent()
		_ = w.WriteEntry("manifest.json", int64(len(mb)), mfst.CreatedAt, bytes.NewReader(mb))
		body := rest[mfst.DuckDBFilename]
		_ = w.WriteEntry(mfst.DuckDBFilename, int64(len(body)), mfst.CreatedAt, bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	out := filepath.Join(dir, "out.duckdb")
	if _, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + mutated, OutputPath: out, NoVerify: true,
	}); err != nil {
		t.Fatalf("fetch with NoVerify should succeed: %v", err)
	}
}

func TestFetch_OutputExists_NoForce(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "out.duckdb")
	_ = os.WriteFile(out, []byte("hi"), 0o644)
	_, err := Fetch(context.Background(), ConsumerOptions{URL: "file://" + tarPath, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "exists") {
		t.Errorf("want exists error, got %v", err)
	}
}

func TestFetch_OutputExists_Force(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "out.duckdb")
	_ = os.WriteFile(out, []byte("hi"), 0o644)
	if _, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + tarPath, OutputPath: out, Force: true,
	}); err != nil {
		t.Fatalf("fetch with Force: %v", err)
	}
}

func timeFixed() time.Time {
	return time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
}
```

Add `"time"` to the test file's imports as needed (used by `timeFixed`).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/snapshot/ -run TestFetch -v`
Expected: FAIL — `Fetch`/`ConsumerOptions` undefined.

- [ ] **Step 3: Write consumer.go**

Create `internal/snapshot/consumer.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// ConsumerOptions configures Fetch.
type ConsumerOptions struct {
	URL        string // http(s):// or file:// URL; required
	OutputPath string // destination DuckDB; "" means current dir + manifest.duckdb_filename
	NoVerify   bool   // skip SHA-256 check after extraction
	Force      bool   // overwrite existing OutputPath
}

// Fetch downloads (or opens) the snapshot at opts.URL, validates the manifest,
// streams the DuckDB payload to OutputPath, and verifies SHA-256.
func Fetch(ctx context.Context, opts ConsumerOptions) (*Manifest, error) {
	body, err := openURL(ctx, opts.URL)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	r, err := newTarZstReader(body)
	if err != nil {
		return nil, fmt.Errorf("fetch: decode: %w", err)
	}
	defer r.Close()

	// Entry 1: manifest.json
	hdr, mb, err := r.Next()
	if err != nil {
		return nil, fmt.Errorf("fetch: read first entry: %w", err)
	}
	if hdr.Name != "manifest.json" {
		return nil, fmt.Errorf("fetch: unexpected first entry %q; want manifest.json", hdr.Name)
	}
	manifestBytes, err := io.ReadAll(mb)
	if err != nil {
		return nil, fmt.Errorf("fetch: read manifest: %w", err)
	}
	manifest, err := ParseManifest(manifestBytes)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	if manifest.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("fetch: unsupported schema_version %d (this build supports %d)", manifest.SchemaVersion, SchemaVersion)
	}
	if !isSafeFilename(manifest.DuckDBFilename) {
		return nil, fmt.Errorf("fetch: manifest declares unsafe filename %q", manifest.DuckDBFilename)
	}

	outPath := opts.OutputPath
	if outPath == "" {
		outPath = manifest.DuckDBFilename
	}
	if !opts.Force {
		if _, err := os.Stat(outPath); err == nil {
			return nil, fmt.Errorf("fetch: %s exists; pass --force to overwrite", outPath)
		}
	}

	// Entry 2: DuckDB payload
	hdr2, payload, err := r.Next()
	if err != nil {
		return nil, fmt.Errorf("fetch: read payload entry: %w", err)
	}
	if hdr2.Name != manifest.DuckDBFilename {
		return nil, fmt.Errorf("fetch: unexpected payload entry %q; manifest declared %q", hdr2.Name, manifest.DuckDBFilename)
	}
	if hdr2.Size != manifest.DuckDBSizeBytes {
		return nil, fmt.Errorf("fetch: size mismatch: tar header %d, manifest %d", hdr2.Size, manifest.DuckDBSizeBytes)
	}

	if err := writeWithSHA(outPath, payload, manifest, opts.NoVerify); err != nil {
		_ = os.Remove(outPath)
		return nil, err
	}
	return manifest, nil
}

// openURL returns a ReadCloser for opts.URL. http(s) and file are supported.
func openURL(ctx context.Context, url string) (io.ReadCloser, error) {
	switch {
	case strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://"):
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("fetch: build request: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch: %w", err)
		}
		if resp.StatusCode >= 400 {
			snippet := readSnippet(resp.Body, 200)
			resp.Body.Close()
			return nil, fmt.Errorf("fetch: HTTP %d: %s", resp.StatusCode, snippet)
		}
		return resp.Body, nil
	case strings.HasPrefix(url, "file://"):
		path := strings.TrimPrefix(url, "file://")
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("fetch: %w", err)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("fetch: unsupported scheme %q (want http, https, or file)", schemeOf(url))
	}
}

func readSnippet(r io.Reader, n int) string {
	buf := make([]byte, n)
	got, _ := io.ReadFull(r, buf)
	return string(buf[:got])
}

func schemeOf(url string) string {
	if i := strings.Index(url, ":"); i >= 0 {
		return url[:i]
	}
	return url
}

// isSafeFilename rejects empty, absolute, or path-traversing names.
func isSafeFilename(name string) bool {
	if name == "" {
		return false
	}
	if filepath.Clean(name) != name {
		return false
	}
	if filepath.Base(name) != name {
		return false
	}
	return true
}

// writeWithSHA streams payload into outPath while computing SHA-256, then
// verifies against the manifest unless NoVerify.
func writeWithSHA(outPath string, payload io.Reader, manifest *Manifest, noVerify bool) error {
	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("fetch: create output: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			out.Close()
		}
	}()

	h := hashOrDiscard(noVerify)
	tee := teeWriter(out, h)
	written, err := io.Copy(tee, payload)
	if err != nil {
		out.Close()
		closed = true
		return fmt.Errorf("fetch: write output: %w", err)
	}
	if err := out.Close(); err != nil {
		closed = true
		return fmt.Errorf("fetch: close output: %w", err)
	}
	closed = true

	if written != manifest.DuckDBSizeBytes {
		return fmt.Errorf("fetch: size mismatch: wrote %d, manifest %d", written, manifest.DuckDBSizeBytes)
	}
	if !noVerify {
		got := hex.EncodeToString(h.Sum(nil))
		if got != manifest.DuckDBSHA256 {
			return fmt.Errorf("fetch: sha256 mismatch: got %s, manifest %s", got, manifest.DuckDBSHA256)
		}
	}
	return nil
}

func hashOrDiscard(noVerify bool) hash.Hash {
	if noVerify {
		return discardHash{}
	}
	return sha256.New()
}

type discardHash struct{}

func (discardHash) Write(p []byte) (int, error) { return len(p), nil }
func (discardHash) Sum(b []byte) []byte         { return b }
func (discardHash) Reset()                      {}
func (discardHash) Size() int                   { return 0 }
func (discardHash) BlockSize() int              { return 1 }

func teeWriter(a io.Writer, b io.Writer) io.Writer { return io.MultiWriter(a, b) }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/snapshot/ -run TestFetch -v`
Expected: all 12 pass.

- [ ] **Step 5: Run full snapshot suite + cross-package**

Run: `go test ./...`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add internal/snapshot/consumer.go internal/snapshot/consumer_test.go
git commit -m "snapshot: add Fetch consumer with SHA-256 verification"
```

---

## Task 6: `csq snapshot` CLI subcommand

**Files:**
- Create: `cmd/csq/snapshot.go`

- [ ] **Step 1: Write snapshot.go**

Create `cmd/csq/snapshot.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/snapshot"
)

func runSnapshot(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	var (
		dbPath       string
		outputPath   string
		portal       string
		keepStaging  bool
		force        bool
	)
	fs.StringVar(&dbPath, "db", "", "Source DuckDB to package (required)")
	fs.StringVar(&outputPath, "output", "", "Destination tarball (required, .tar.zst)")
	fs.StringVar(&portal, "portal", "", "Portal name in manifest (default: derived from --db filename)")
	fs.BoolVar(&keepStaging, "keep-staging", false, "Skip _csq_staging cleanup")
	fs.BoolVar(&force, "force", false, "Overwrite --output if it exists")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if dbPath == "" {
		return fmt.Errorf("--db is required")
	}
	if outputPath == "" {
		return fmt.Errorf("--output is required")
	}

	m, err := snapshot.Pack(context.Background(), snapshot.ProducerOptions{
		DBPath:      dbPath,
		OutputPath:  outputPath,
		Portal:      portal,
		KeepStaging: keepStaging,
		Force:       force,
		CSQVersion:  "0.4.0",
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr,
		"[csq] snapshot %s: portal=%s datasets=%d rows=%d size=%d sha256=%s\n",
		outputPath, m.Portal, m.DatasetCount, m.TotalRowCount,
		m.DuckDBSizeBytes, m.DuckDBSHA256[:12])
	return nil
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./cmd/csq/`
Expected: clean (the dispatch case in main.go isn't added yet, but the file compiles standalone).

- [ ] **Step 3: Commit**

```bash
git add cmd/csq/snapshot.go
git commit -m "cli: add runSnapshot for csq snapshot subcommand"
```

---

## Task 7: `csq fetch` CLI subcommand

**Files:**
- Create: `cmd/csq/fetch.go`

- [ ] **Step 1: Write fetch.go**

Create `cmd/csq/fetch.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/snapshot"
)

func runFetch(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	var (
		from       string
		outputPath string
		noVerify   bool
		force      bool
	)
	fs.StringVar(&from, "from", "", "URL to fetch from (http://, https://, or file://)")
	fs.StringVar(&outputPath, "output", "", "Destination DuckDB path (default: manifest's duckdb_filename in the current directory)")
	fs.BoolVar(&noVerify, "no-verify", false, "Skip SHA-256 verification")
	fs.BoolVar(&force, "force", false, "Overwrite --output if it exists")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if from == "" {
		return fmt.Errorf("--from is required")
	}

	m, err := snapshot.Fetch(context.Background(), snapshot.ConsumerOptions{
		URL:        from,
		OutputPath: outputPath,
		NoVerify:   noVerify,
		Force:      force,
	})
	if err != nil {
		return err
	}
	resolved := outputPath
	if resolved == "" {
		resolved = m.DuckDBFilename
	}
	fmt.Fprintf(os.Stderr,
		"[csq] fetched %s: portal=%s snapshot=%s datasets=%d rows=%d → %s\n",
		from, m.Portal, m.SnapshotID, m.DatasetCount, m.TotalRowCount, resolved)
	return nil
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./cmd/csq/`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add cmd/csq/fetch.go
git commit -m "cli: add runFetch for csq fetch subcommand"
```

---

## Task 8: Wire dispatch in main.go

**Files:**
- Modify: `cmd/csq/main.go`

- [ ] **Step 1: Update usage and add dispatch cases**

Edit `cmd/csq/main.go`. Replace the `usage` const with:

```go
const usage = `csq — CivicSodaQuack

Usage:
  csq extract  --portal <host> --dataset <4x4> [options]
  csq catalog  --portal <host> [--refresh] [--json] [--output FILE]
  csq sync     --config <portal.yaml> [--dry-run] [--only IDs]
  csq mcp      --db <portal.duckdb> [--db ...] [--http <addr>]
  csq snapshot --db <portal.duckdb> --output <snap.tar.zst> [--portal NAME] [--force]
  csq fetch    --from <url> [--output <path.duckdb>] [--no-verify] [--force]

Examples:
  csq extract  --portal data.cityofchicago.org --dataset 6zsd-86xi --limit 10000
  csq catalog  --portal data.cityofchicago.org --category "Public Safety"
  csq sync     --config data.cityofchicago.org.yaml
  csq mcp      --db data.cityofchicago.org.duckdb
  csq snapshot --db data.cityofchicago.org.duckdb --output chicago-2026-04-23.tar.zst
  csq fetch    --from https://example.com/snapshots/chicago-2026-04-23.tar.zst
`
```

In the `switch os.Args[1]` block, add two cases after the existing `"mcp"` case:

```go
	case "snapshot":
		if err := runSnapshot(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "csq snapshot: %v\n", err)
			os.Exit(1)
		}
	case "fetch":
		if err := runFetch(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "csq fetch: %v\n", err)
			os.Exit(1)
		}
```

- [ ] **Step 2: Build**

Run: `go build -o csq ./cmd/csq`
Expected: clean.

- [ ] **Step 3: Smoke arg validation**

Run: `./csq` (no args)
Expected: usage prints both `csq snapshot` and `csq fetch` lines; exit 2.

Run: `./csq snapshot` (no flags)
Expected: prints `csq snapshot: --db is required`; exit 1.

Run: `./csq fetch` (no flags)
Expected: prints `csq fetch: --from is required`; exit 1.

- [ ] **Step 4: Commit**

```bash
git add cmd/csq/main.go
git commit -m "cli: dispatch snapshot and fetch subcommands"
```

---

## Task 9: End-to-end CLI smoke (snapshot → fetch round-trip)

**Files:**
- Modify: `cmd/csq/cli_smoke_test.go`

- [ ] **Step 1: Append the smoke test**

Append to `cmd/csq/cli_smoke_test.go`:

```go
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
```

- [ ] **Step 2: Run the smoke test**

Run: `go test ./cmd/csq/ -run TestCSQ_Snapshot_RoundTrip_Smoke -v`
Expected: PASS.

- [ ] **Step 3: Run full CLI suite to confirm Phase 1/2/3 smokes still pass**

Run: `go test ./cmd/csq/ -v`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add cmd/csq/cli_smoke_test.go
git commit -m "cli: add snapshot+fetch round-trip smoke test"
```

---

## Task 10: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update Status and add Distribution section**

Edit `README.md`. Replace the `## Status` line:

```markdown
## Status

**Phase 4** — snapshot publishing. After syncing one or more portals into per-portal DuckDB files, run `csq snapshot` to package one as a `.tar.zst` for distribution; consume with `csq fetch --from <url>`.
```

After the existing "Serve via MCP" subsection, append:

```markdown
### Distribute via snapshot

Package an existing synced DuckDB into a portable tarball:

```bash
./csq snapshot --db data.cityofchicago.org.duckdb \
               --output chicago-2026-04-23.tar.zst
```

The tarball contains a `manifest.json` (portal, snapshot id, dataset/row counts, SHA-256 of the DuckDB) and the DuckDB file itself, all zstd-compressed.

Upload the tarball anywhere your agents can reach (S3, GitHub Releases, an internal CDN, a local file). To restore on another host:

```bash
./csq fetch --from https://example.com/snapshots/chicago-2026-04-23.tar.zst
# or
./csq fetch --from file:///path/to/chicago-2026-04-23.tar.zst
```

`csq fetch` verifies the SHA-256 against the manifest before declaring success. Pass `--no-verify` to skip (not recommended).
```

Update the Layout block:

```markdown
## Layout

```
cmd/csq/              # CLI entrypoint
internal/socrata/     # SODA2 client: metadata + paginated row streaming
internal/duckdb/      # DuckDB writer + Socrata→DuckDB schema mapping
internal/config/      # YAML loader + per-dataset effective config
internal/sync/        # Sync orchestrator + strategies (FullReplace, Incremental)
internal/mcpserver/   # MCP server: pools, ATTACH, tools, transports
internal/snapshot/    # Snapshot publishing: tar+zst format, Pack producer, Fetch consumer
```
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: Phase 4 README — snapshot publishing"
```

---

## Final verification

- [ ] **Run the full build + test + vet**

Run:
```bash
task build
task test
task vet
```
Expected: all green.

- [ ] **Manual smoke (optional, not CI)**

```bash
# Use any synced DuckDB from Phases 1/2.
./csq snapshot --db data.cityofchicago.org.duckdb --output /tmp/snap.tar.zst
ls -lh /tmp/snap.tar.zst

# Round-trip via local file:// URL.
./csq fetch --from file:///tmp/snap.tar.zst --output /tmp/restored.duckdb
duckdb /tmp/restored.duckdb "SELECT COUNT(*) FROM _csq.catalog"
```
