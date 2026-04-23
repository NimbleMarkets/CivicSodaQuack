# Phase 2 — Incremental Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make daily and hourly syncs cheap on million-row datasets by adding an `IncrementalStrategy` that fetches only rows newer than the last successful sync, upserts them by Socrata `:id`, and tracks per-dataset high-water marks in a new `_csq.dataset_state` table.

**Architecture:** New `IncrementalStrategy` plugs into the existing `WriteStrategy` interface introduced in Phase 1. It auto-detects bootstrap-vs-delta by reading `_csq.dataset_state` and the live table's PK presence, then either does a Phase-1-style staging swap (bootstrap) or a stream + native `INSERT ... ON CONFLICT` upsert (delta). Schema drift between runs aborts loudly. A `mode: full_replace` per-dataset YAML override forces re-bootstrapping.

**Tech Stack:** Go 1.24, DuckDB (`duckdb-go/v2`), pflag, `_csq` audit schema, ULIDs for run IDs (already in `go.mod`). No new external dependencies.

---

## File Structure

**Create:**
- `internal/duckdb/dataset_state.go` — `DatasetState` struct + `ReadDatasetState` / `UpsertDatasetState`.
- `internal/duckdb/dataset_state_test.go` — round-trip tests.
- `internal/duckdb/upsert.go` — `UpsertRows` with `INSERT ... ON CONFLICT (socrata_id)`.
- `internal/duckdb/upsert_test.go` — insert + conflict-update tests.
- `internal/duckdb/schema_diff.go` — `DiffSchema` against `information_schema.columns`.
- `internal/duckdb/schema_diff_test.go` — added/removed/retyped tests.
- `internal/sync/incremental.go` — `IncrementalStrategy` (bootstrap + delta + drift detection).
- `internal/sync/incremental_test.go` — seven scenario tests.

**Modify:**
- `internal/duckdb/migrations.go` — add `_csq.dataset_state` to the migration list.
- `internal/duckdb/migrations_test.go` — assert the new table exists.
- `internal/duckdb/schema.go` — add `PrimaryKey string` field to `TableSchema`; emit `PRIMARY KEY (...)` in `CreateTableSQL` / `CreateTableSQLIn` when set; add `BuildSchemaWithSocrataID` constructor.
- `internal/socrata/ext.go` — `StreamRowsCtx` gains a `selectClause string` parameter; empty string preserves Phase 1 behavior, `":*,*"` opts into system fields.
- `internal/sync/strategy.go` — `streamInto` callsite updated to pass `""` for the new `selectClause` argument (Phase 1 behavior unchanged).
- `internal/sync/fakesocrata_test.go` — emit `:id` (auto-assigned) and `:updated_at` (per-row optional) when `$select=:*,*` is present; parse `$where` for the single supported predicate `<col> > 'TS'`.
- `internal/config/config.go` — add `Mode` + `HWMColumn` to `Override`.
- `internal/config/effective.go` — propagate `Mode` + `HWMColumn` into `Effective`.
- `internal/sync/run.go` — default strategy switches from `FullReplaceStrategy` to `IncrementalStrategy{Portal, Scheme, RunID}`.
- `internal/sync/run_test.go` — fake-portal datasets gain `:updated_at` so bootstrap captures real HWMs.
- `cmd/csq/cli_smoke_test.go` — add `TestCSQ_IncrementalSmoke` (two `csq sync` invocations against the fake portal, asserting growth).

---

## Task 1: `_csq.dataset_state` migration

Add the new table to the migration list so every fresh DuckDB has it on `Open`.

**Files:**
- Modify: `internal/duckdb/migrations.go`
- Modify: `internal/duckdb/migrations_test.go`

- [ ] **Step 1: Write the failing test**

Edit `internal/duckdb/migrations_test.go`. Find the test that asserts `_csq.catalog` and `_csq.sync_runs` exist. Below it, add:

```go
func TestApply_CreatesDatasetStateTable(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	var n int
	err = w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema = '_csq' AND table_name = 'dataset_state'`,
	).Scan(&n)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 1 {
		t.Errorf("dataset_state table missing")
	}

	// PK column should exist
	err = w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.columns
		 WHERE table_schema = '_csq' AND table_name = 'dataset_state'
		   AND column_name = 'dataset_id'`,
	).Scan(&n)
	if err != nil {
		t.Fatalf("col query: %v", err)
	}
	if n != 1 {
		t.Errorf("dataset_id column missing")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/duckdb/ -run TestApply_CreatesDatasetStateTable -v`
Expected: FAIL — `dataset_state table missing`.

- [ ] **Step 3: Add the migration**

Edit `internal/duckdb/migrations.go`. Inside `Apply`, append a new statement to the `stmts` slice (before the closing `}`):

```go
		`CREATE TABLE IF NOT EXISTS _csq.dataset_state (
			dataset_id           VARCHAR PRIMARY KEY,
			hwm_updated_at       TIMESTAMP,
			last_full_replace_at TIMESTAMP,
			last_run_id          VARCHAR,
			hwm_column           VARCHAR NOT NULL
		)`,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/duckdb/ -run TestApply -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/duckdb/migrations.go internal/duckdb/migrations_test.go
git commit -m "duckdb: add _csq.dataset_state migration for HWM tracking"
```

---

## Task 2: DatasetState store

A thin store mirroring `catalog_store.go` and `sync_runs.go`.

**Files:**
- Create: `internal/duckdb/dataset_state.go`
- Create: `internal/duckdb/dataset_state_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/duckdb/dataset_state_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"
	"time"
)

func TestDatasetState_MissingReturnsNil(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	got, err := w.ReadDatasetState("aaaa-0001")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != nil {
		t.Errorf("want nil for missing row, got %+v", got)
	}
}

func TestDatasetState_UpsertRoundTrip(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	hwm := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	full := time.Date(2026, 4, 22, 11, 0, 0, 0, time.UTC)
	in := DatasetState{
		DatasetID:         "aaaa-0001",
		HWMUpdatedAt:      &hwm,
		LastFullReplaceAt: &full,
		LastRunID:         "01HXYZ",
		HWMColumn:         ":updated_at",
	}
	if err := w.UpsertDatasetState(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := w.ReadDatasetState("aaaa-0001")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got == nil {
		t.Fatal("want row, got nil")
	}
	if !got.HWMUpdatedAt.Equal(hwm) {
		t.Errorf("hwm: got %v, want %v", got.HWMUpdatedAt, hwm)
	}
	if got.LastRunID != "01HXYZ" || got.HWMColumn != ":updated_at" {
		t.Errorf("got %+v", got)
	}
}

func TestDatasetState_UpsertReplaces(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	t1 := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 4, 22, 13, 0, 0, 0, time.UTC)

	_ = w.UpsertDatasetState(DatasetState{
		DatasetID: "aaaa-0001", HWMUpdatedAt: &t1, LastRunID: "run1", HWMColumn: ":updated_at",
	})
	_ = w.UpsertDatasetState(DatasetState{
		DatasetID: "aaaa-0001", HWMUpdatedAt: &t2, LastRunID: "run2", HWMColumn: ":updated_at",
	})

	got, _ := w.ReadDatasetState("aaaa-0001")
	if !got.HWMUpdatedAt.Equal(t2) || got.LastRunID != "run2" {
		t.Errorf("replace: got %+v", got)
	}
}

func TestDatasetState_NullableHWM(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	in := DatasetState{
		DatasetID: "aaaa-0001",
		// HWMUpdatedAt nil — datasets without :updated_at land here
		HWMColumn: ":updated_at",
		LastRunID: "run1",
	}
	if err := w.UpsertDatasetState(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := w.ReadDatasetState("aaaa-0001")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.HWMUpdatedAt != nil {
		t.Errorf("want nil HWM, got %v", got.HWMUpdatedAt)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/duckdb/ -run TestDatasetState -v`
Expected: FAIL — `DatasetState` / `ReadDatasetState` / `UpsertDatasetState` undefined.

- [ ] **Step 3: Write dataset_state.go**

Create `internal/duckdb/dataset_state.go`:

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/duckdb/ -run TestDatasetState -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/duckdb/dataset_state.go internal/duckdb/dataset_state_test.go
git commit -m "duckdb: add DatasetState store for HWM tracking"
```

---

## Task 3: Schema PK + `BuildSchemaWithSocrataID`

Add a `PrimaryKey` field to `TableSchema` and a constructor that prepends the synthetic `socrata_id` column.

**Files:**
- Modify: `internal/duckdb/schema.go`
- Create: `internal/duckdb/schema_pk_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/duckdb/schema_pk_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"strings"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestBuildSchemaWithSocrataID_PrependsColumn(t *testing.T) {
	cols := []socrata.Column{
		{FieldName: "score", DataTypeName: "number"},
	}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	if len(ts.Columns) != 2 {
		t.Fatalf("col count: got %d, want 2", len(ts.Columns))
	}
	if ts.Columns[0].Name != "socrata_id" {
		t.Errorf("first col: got %q, want socrata_id", ts.Columns[0].Name)
	}
	if ts.PrimaryKey != "socrata_id" {
		t.Errorf("primary key: got %q, want socrata_id", ts.PrimaryKey)
	}
}

func TestCreateTableSQL_EmitsPrimaryKey(t *testing.T) {
	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	sql := ts.CreateTableSQL()
	if !strings.Contains(sql, "PRIMARY KEY") {
		t.Errorf("missing PRIMARY KEY in SQL:\n%s", sql)
	}
	if !strings.Contains(sql, `"socrata_id" VARCHAR`) {
		t.Errorf("missing socrata_id col in SQL:\n%s", sql)
	}
}

func TestCreateTableSQL_NoPKWhenUnset(t *testing.T) {
	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchema("crimes", cols)
	sql := ts.CreateTableSQL()
	if strings.Contains(sql, "PRIMARY KEY") {
		t.Errorf("Phase 1 path should not emit PRIMARY KEY:\n%s", sql)
	}
}

func TestSocrataIDExtractor_ReadsColonID(t *testing.T) {
	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	row := socrata.Row{":id": "row-abc", "score": float64(42)}
	got, err := ts.Columns[0].Extract(row)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if got != "row-abc" {
		t.Errorf("got %v, want row-abc", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/duckdb/ -run "TestBuildSchemaWithSocrataID|TestCreateTableSQL|TestSocrataIDExtractor" -v`
Expected: FAIL — `BuildSchemaWithSocrataID` undefined; `PrimaryKey` field missing.

- [ ] **Step 3: Modify TableSchema and add the constructor**

Edit `internal/duckdb/schema.go`. Find the `TableSchema` struct definition and add a `PrimaryKey` field:

```go
// TableSchema is the set of TargetColumns for a dataset plus the target table name.
type TableSchema struct {
	Table      string
	Columns    []TargetColumn
	PrimaryKey string // optional; when set, emitted as table-level PRIMARY KEY in CreateTableSQL
}
```

Below the existing `BuildSchema` function, add:

```go
// BuildSchemaWithSocrataID returns a TableSchema with the synthetic socrata_id
// PRIMARY KEY column prepended. The socrata_id value is read from the row's :id
// system field, which Phase 2 callers fetch via $select=:*,*.
func BuildSchemaWithSocrataID(table string, cols []socrata.Column) TableSchema {
	ts := BuildSchema(table, cols)
	socrataIDCol := TargetColumn{
		Name:    "socrata_id",
		Type:    socrata.TypeVarchar,
		Extract: extractSocrataID,
	}
	ts.Columns = append([]TargetColumn{socrataIDCol}, ts.Columns...)
	ts.PrimaryKey = "socrata_id"
	return ts
}

func extractSocrataID(row socrata.Row) (any, error) {
	v, ok := row[":id"]
	if !ok || v == nil {
		return nil, nil
	}
	if s, ok := v.(string); ok {
		return s, nil
	}
	return fmt.Sprintf("%v", v), nil
}
```

Then modify `CreateTableSQL` and `CreateTableSQLIn` to emit the PK clause. In `CreateTableSQL`, change:

```go
	b.WriteString(")")
	return b.String()
}
```

to:

```go
	if s.PrimaryKey != "" {
		fmt.Fprintf(&b, `, PRIMARY KEY ("%s")`, s.PrimaryKey)
	}
	b.WriteString(")")
	return b.String()
}
```

Apply the same edit to `CreateTableSQLIn`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/duckdb/ -v`
Expected: all pass (existing tests unaffected because `PrimaryKey` defaults to `""`).

- [ ] **Step 5: Commit**

```bash
git add internal/duckdb/schema.go internal/duckdb/schema_pk_test.go
git commit -m "duckdb: add PrimaryKey field + BuildSchemaWithSocrataID constructor"
```

---

## Task 4: socrata `StreamRowsCtx` `selectClause` parameter

Allow callers to opt into `$select=:*,*` so `:id` and `:updated_at` appear in row payloads.

**Files:**
- Modify: `internal/socrata/ext.go`
- Modify: `internal/sync/strategy.go` (the only existing caller)

- [ ] **Step 1: Modify the StreamRowsCtx signature**

Edit `internal/socrata/ext.go`. Change the function signature and add the `$select` query parameter:

```go
// StreamRowsCtx is a context-aware, scheme-parameterised version of StreamRows.
// Cancellation via ctx aborts between pages. selectClause, if non-empty, is sent
// as $select; pass ":*,*" to include Socrata system fields (:id, :updated_at).
func (c *Client) StreamRowsCtx(
	ctx context.Context,
	scheme, portal, datasetID, orderBy, whereClause, selectClause string,
	limit int,
	handler PageHandler,
) error {
```

Inside the loop, add the `$select` clause alongside the others:

```go
		q := url.Values{}
		q.Set("$limit", strconv.Itoa(remaining))
		q.Set("$offset", strconv.Itoa(offset))
		if orderBy != "" {
			q.Set("$order", orderBy)
		}
		if whereClause != "" {
			q.Set("$where", whereClause)
		}
		if selectClause != "" {
			q.Set("$select", selectClause)
		}
		base.RawQuery = q.Encode()
```

- [ ] **Step 2: Update the existing caller**

Edit `internal/sync/strategy.go`. In `streamInto`, pass `""` as the new `selectClause` argument:

```go
	return client.StreamRowsCtx(ctx, scheme, portal, target.ID,
		target.Effective.OrderBy, target.Effective.Where, "",
		target.Effective.Limit,
		func(page []socrata.Row) error {
```

- [ ] **Step 3: Run tests to verify nothing regressed**

Run: `go test ./...`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add internal/socrata/ext.go internal/sync/strategy.go
git commit -m "socrata: add selectClause to StreamRowsCtx for system-field fetches"
```

---

## Task 5: Fake portal — `:id`, `:updated_at`, `$where` parser

Extend the test fake so Phase 2 strategy tests can drive realistic incremental scenarios.

**Files:**
- Modify: `internal/sync/fakesocrata_test.go`

- [ ] **Step 1: Add per-row :updated_at + $select handling + $where parser**

Edit `internal/sync/fakesocrata_test.go`. Replace the entire `/resource/` handler with:

```go
	// /resource/{id}.json
	mux.HandleFunc("/resource/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/resource/"), ".json")
		d, ok := byID[id]
		if !ok {
			http.Error(w, "not found", 404)
			return
		}
		q := r.URL.Query()
		offset, _ := strconv.Atoi(q.Get("$offset"))
		limit, _ := strconv.Atoi(q.Get("$limit"))
		selectClause := q.Get("$select")
		whereClause := q.Get("$where")
		includeSystem := selectClause == ":*,*"

		if d.FailAtOffset > 0 && offset >= d.FailAtOffset {
			http.Error(w, "synthetic failure", 500)
			return
		}

		// Apply $where filter if present
		filtered := d.Rows
		if whereClause != "" {
			cutoff, ok := parseSimpleGreaterThan(whereClause)
			if !ok {
				http.Error(w, "fake portal: unsupported $where: "+whereClause, 400)
				return
			}
			filtered = filtered[:0:0]
			for _, row := range d.Rows {
				ts, ok := row[":updated_at"].(string)
				if !ok {
					continue
				}
				if ts > cutoff { // string compare on ISO-8601 is order-preserving
					filtered = append(filtered, row)
				}
			}
		}

		// Page slice
		end := offset + limit
		if end > len(filtered) {
			end = len(filtered)
		}
		if offset > len(filtered) {
			offset = len(filtered)
		}
		page := filtered[offset:end]

		// Strip or include system fields per $select
		out := make([]map[string]any, 0, len(page))
		for i, row := range page {
			cleaned := map[string]any{}
			for k, v := range row {
				if strings.HasPrefix(k, ":") {
					if includeSystem {
						cleaned[k] = v
					}
					continue
				}
				cleaned[k] = v
			}
			if includeSystem {
				if _, has := cleaned[":id"]; !has {
					cleaned[":id"] = fmt.Sprintf("%s-row-%d", d.ID, offset+i)
				}
			}
			out = append(out, cleaned)
		}
		_ = json.NewEncoder(w).Encode(out)
	})
```

Then add the helper at the bottom of the file:

```go
// parseSimpleGreaterThan recognises the single Phase 2 predicate shape:
//   <col> > '<value>'   (with surrounding whitespace ignored)
// Returns the value if matched. Anything else returns ok=false.
func parseSimpleGreaterThan(where string) (string, bool) {
	re := regexp.MustCompile(`^\s*[A-Za-z_:][A-Za-z0-9_:]*\s*>\s*'([^']*)'\s*$`)
	m := re.FindStringSubmatch(where)
	if m == nil {
		return "", false
	}
	return m[1], true
}
```

Add `"fmt"` and `"regexp"` to the imports if not already present.

- [ ] **Step 2: Run existing tests to confirm no regression**

Run: `go test ./internal/sync/ -v`
Expected: all existing tests still pass. The fake's Phase 1 callers don't pass `$select` or `$where`, so the new code paths are dormant for them.

- [ ] **Step 3: Commit**

```bash
git add internal/sync/fakesocrata_test.go
git commit -m "sync: extend fake portal with :id/:updated_at + \$where parser"
```

---

## Task 6: `UpsertRows` — native `INSERT ... ON CONFLICT`

**Files:**
- Create: `internal/duckdb/upsert.go`
- Create: `internal/duckdb/upsert_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/duckdb/upsert_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestUpsertRows_Insert(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	if _, err := w.DB.Exec(ts.CreateTableSQLIn("main")); err != nil {
		t.Fatalf("create: %v", err)
	}

	rows := []socrata.Row{
		{":id": "a", "score": float64(1)},
		{":id": "b", "score": float64(2)},
	}
	if err := w.UpsertRows("main", ts, rows); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	var n int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM main.crimes`).Scan(&n)
	if n != 2 {
		t.Errorf("count: got %d, want 2", n)
	}
}

func TestUpsertRows_OnConflictReplaces(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	_, _ = w.DB.Exec(ts.CreateTableSQLIn("main"))

	_ = w.UpsertRows("main", ts, []socrata.Row{
		{":id": "a", "score": float64(1)},
	})
	_ = w.UpsertRows("main", ts, []socrata.Row{
		{":id": "a", "score": float64(99)},
	})

	var n int
	var score float64
	_ = w.DB.QueryRow(`SELECT COUNT(*), MAX(score) FROM main.crimes`).Scan(&n, &score)
	if n != 1 {
		t.Errorf("count: got %d, want 1 (upsert should replace)", n)
	}
	if score != 99 {
		t.Errorf("score: got %v, want 99", score)
	}
}

func TestUpsertRows_EmptyIsNoop(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	_, _ = w.DB.Exec(ts.CreateTableSQLIn("main"))

	if err := w.UpsertRows("main", ts, nil); err != nil {
		t.Errorf("nil rows: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/duckdb/ -run TestUpsertRows -v`
Expected: FAIL — `UpsertRows` undefined.

- [ ] **Step 3: Write upsert.go**

Create `internal/duckdb/upsert.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"fmt"
	"strings"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// UpsertRows inserts rows into "<schemaName>"."<ts.Table>", upserting on the
// table's PrimaryKey. ts.PrimaryKey must be non-empty (use BuildSchemaWithSocrataID
// to construct an upsert-capable TableSchema). Empty rows is a no-op.
func (w *Writer) UpsertRows(schemaName string, ts TableSchema, rows []socrata.Row) error {
	if len(rows) == 0 {
		return nil
	}
	if ts.PrimaryKey == "" {
		return fmt.Errorf("UpsertRows requires ts.PrimaryKey to be set")
	}

	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(buildUpsertSQL(schemaName, ts))
	if err != nil {
		return fmt.Errorf("prepare upsert: %w", err)
	}
	defer stmt.Close()

	vals := make([]any, len(ts.Columns))
	for rowIdx, row := range rows {
		for i, col := range ts.Columns {
			v, err := col.Extract(row)
			if err != nil {
				return fmt.Errorf("row %d col %q: %w", rowIdx, col.Name, err)
			}
			vals[i] = v
		}
		if _, err := stmt.Exec(vals...); err != nil {
			return fmt.Errorf("upsert row %d: %w", rowIdx, err)
		}
	}
	return tx.Commit()
}

func buildUpsertSQL(schemaName string, ts TableSchema) string {
	var b strings.Builder
	fmt.Fprintf(&b, `INSERT INTO "%s"."%s" (`, schemaName, ts.Table)
	for i, c := range ts.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `"%s"`, c.Name)
	}
	b.WriteString(") VALUES (")
	for i := range ts.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "$%d", i+1)
	}
	fmt.Fprintf(&b, `) ON CONFLICT ("%s") DO UPDATE SET `, ts.PrimaryKey)
	first := true
	for _, c := range ts.Columns {
		if c.Name == ts.PrimaryKey {
			continue
		}
		if !first {
			b.WriteString(", ")
		}
		first = false
		fmt.Fprintf(&b, `"%s" = excluded."%s"`, c.Name, c.Name)
	}
	return b.String()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/duckdb/ -run TestUpsertRows -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/duckdb/upsert.go internal/duckdb/upsert_test.go
git commit -m "duckdb: add UpsertRows with native ON CONFLICT (socrata_id)"
```

---

## Task 7: `DiffSchema` — drift detection

**Files:**
- Create: `internal/duckdb/schema_diff.go`
- Create: `internal/duckdb/schema_diff_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/duckdb/schema_diff_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestDiffSchema_Identical(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{
		{FieldName: "score", DataTypeName: "number"},
		{FieldName: "name", DataTypeName: "text"},
	}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	_, _ = w.DB.Exec(ts.CreateTableSQLIn("main"))

	got, err := DiffSchema(ts, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("identical schemas should diff empty, got %+v", got)
	}
}

func TestDiffSchema_ColumnAdded(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	live := BuildSchemaWithSocrataID("crimes",
		[]socrata.Column{{FieldName: "score", DataTypeName: "number"}})
	_, _ = w.DB.Exec(live.CreateTableSQLIn("main"))

	want := BuildSchemaWithSocrataID("crimes", []socrata.Column{
		{FieldName: "score", DataTypeName: "number"},
		{FieldName: "newcol", DataTypeName: "text"},
	})

	got, err := DiffSchema(want, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 1 || got[0].Column != "newcol" || got[0].Kind != "added" {
		t.Errorf("got %+v, want one 'added' diff for newcol", got)
	}
}

func TestDiffSchema_ColumnRemoved(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	live := BuildSchemaWithSocrataID("crimes", []socrata.Column{
		{FieldName: "score", DataTypeName: "number"},
		{FieldName: "extra", DataTypeName: "text"},
	})
	_, _ = w.DB.Exec(live.CreateTableSQLIn("main"))

	want := BuildSchemaWithSocrataID("crimes",
		[]socrata.Column{{FieldName: "score", DataTypeName: "number"}})

	got, err := DiffSchema(want, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 1 || got[0].Column != "extra" || got[0].Kind != "removed" {
		t.Errorf("got %+v, want one 'removed' diff for extra", got)
	}
}

func TestDiffSchema_ColumnRetyped(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	live := BuildSchemaWithSocrataID("crimes",
		[]socrata.Column{{FieldName: "score", DataTypeName: "text"}})
	_, _ = w.DB.Exec(live.CreateTableSQLIn("main"))

	want := BuildSchemaWithSocrataID("crimes",
		[]socrata.Column{{FieldName: "score", DataTypeName: "number"}})

	got, err := DiffSchema(want, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 1 || got[0].Column != "score" || got[0].Kind != "retyped" {
		t.Errorf("got %+v, want one 'retyped' diff for score", got)
	}
}

func TestDiffSchema_IgnoresSocrataIDOnBothSides(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	cols := []socrata.Column{{FieldName: "score", DataTypeName: "number"}}
	ts := BuildSchemaWithSocrataID("crimes", cols)
	_, _ = w.DB.Exec(ts.CreateTableSQLIn("main"))

	got, err := DiffSchema(ts, w.DB, "main", "crimes")
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("socrata_id should be excluded from diff, got %+v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/duckdb/ -run TestDiffSchema -v`
Expected: FAIL — `DiffSchema` and `SchemaDiff` undefined.

- [ ] **Step 3: Write schema_diff.go**

Create `internal/duckdb/schema_diff.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"database/sql"
	"fmt"
	"strings"
)

// SchemaDiff describes one column-level discrepancy between a desired schema
// and the live table.
type SchemaDiff struct {
	Column string
	Kind   string // "added" | "removed" | "retyped"
	Want   string // type we'd build now (empty for "removed")
	Have   string // type currently in the table (empty for "added")
}

// DiffSchema returns the per-column differences between want and the live table
// at "<schemaName>"."<table>". The synthetic socrata_id column is excluded on
// both sides so it never trips drift detection.
func DiffSchema(want TableSchema, db *sql.DB, schemaName, table string) ([]SchemaDiff, error) {
	rows, err := db.Query(
		`SELECT column_name, data_type FROM information_schema.columns
		 WHERE table_schema = $1 AND table_name = $2`,
		schemaName, table,
	)
	if err != nil {
		return nil, fmt.Errorf("read information_schema: %w", err)
	}
	defer rows.Close()

	have := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		if name == "socrata_id" {
			continue
		}
		have[name] = strings.ToUpper(typ)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	wantMap := map[string]string{}
	for _, c := range want.Columns {
		if c.Name == "socrata_id" {
			continue
		}
		wantMap[c.Name] = string(c.Type)
	}

	var diffs []SchemaDiff
	for name, wantType := range wantMap {
		haveType, ok := have[name]
		if !ok {
			diffs = append(diffs, SchemaDiff{Column: name, Kind: "added", Want: wantType})
			continue
		}
		if !typesEquivalent(wantType, haveType) {
			diffs = append(diffs, SchemaDiff{Column: name, Kind: "retyped", Want: wantType, Have: haveType})
		}
	}
	for name, haveType := range have {
		if _, ok := wantMap[name]; !ok {
			diffs = append(diffs, SchemaDiff{Column: name, Kind: "removed", Have: haveType})
		}
	}
	return diffs, nil
}

// typesEquivalent treats VARCHAR and STRING as the same, since DuckDB reports
// VARCHAR columns as "VARCHAR" in information_schema regardless of input spelling.
func typesEquivalent(want, have string) bool {
	if want == have {
		return true
	}
	// DuckDB normalises some types
	norm := func(s string) string {
		switch s {
		case "STRING":
			return "VARCHAR"
		}
		return s
	}
	return norm(want) == norm(have)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/duckdb/ -run TestDiffSchema -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/duckdb/schema_diff.go internal/duckdb/schema_diff_test.go
git commit -m "duckdb: add DiffSchema for incremental drift detection"
```

---

## Task 8: Config — `Mode` + `HWMColumn` overrides

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/effective.go`
- Modify: `internal/config/effective_test.go`

- [ ] **Step 1: Add fields to the YAML schema**

Edit `internal/config/config.go`. Modify the `Override` struct:

```go
// Override is per-dataset configuration (keyed by 4x4 id in YAML).
type Override struct {
	Table     string  `yaml:"table"`
	Where     string  `yaml:"where"`
	OrderBy   string  `yaml:"order_by"`
	BatchSize int     `yaml:"batch_size"`
	Limit     int     `yaml:"limit"`
	Columns   Columns `yaml:"columns"`
	Mode      string  `yaml:"mode"`       // "" | "incremental" | "full_replace"
	HWMColumn string  `yaml:"hwm_column"` // "" defaults to ":updated_at"
}
```

- [ ] **Step 2: Add fields to Effective and propagate**

Edit `internal/config/effective.go`. Modify the `Effective` struct:

```go
// Effective is the fully-merged per-dataset configuration.
type Effective struct {
	DatasetID   string
	Table       string
	Where       string
	OrderBy     string
	BatchSize   int
	Limit       int
	SkipColumns []string
	Mode        string // "" | "incremental" | "full_replace"
	HWMColumn   string // "" defaults to ":updated_at" at use sites
}
```

In `EffectiveFor`, after the existing field-merging logic, before the return, add:

```go
	if ov.Mode != "" {
		eff.Mode = ov.Mode
	}
	if ov.HWMColumn != "" {
		eff.HWMColumn = ov.HWMColumn
	}
```

Also extend the `Hash` function's canonical struct to include the new fields, so changes to mode/hwm_column invalidate the hash:

```go
func (e Effective) Hash() string {
	canonical := struct {
		Table       string   `json:"table"`
		Where       string   `json:"where"`
		OrderBy     string   `json:"order_by"`
		BatchSize   int      `json:"batch_size"`
		Limit       int      `json:"limit"`
		SkipColumns []string `json:"skip_columns"`
		Mode        string   `json:"mode"`
		HWMColumn   string   `json:"hwm_column"`
	}{e.Table, e.Where, e.OrderBy, e.BatchSize, e.Limit, e.SkipColumns, e.Mode, e.HWMColumn}
	b, _ := json.Marshal(canonical)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
```

- [ ] **Step 3: Add tests**

Edit `internal/config/effective_test.go`. Append:

```go
func TestEffectiveFor_ModeAndHWMColumnOverride(t *testing.T) {
	cfg := &Config{
		Defaults: Defaults{BatchSize: 100, OrderBy: ":id"},
		Overrides: map[string]Override{
			"a-a": {Mode: "full_replace", HWMColumn: ":created_at"},
		},
	}
	eff := cfg.EffectiveFor("a-a")
	if eff.Mode != "full_replace" {
		t.Errorf("mode: got %q", eff.Mode)
	}
	if eff.HWMColumn != ":created_at" {
		t.Errorf("hwm_column: got %q", eff.HWMColumn)
	}
}

func TestEffectiveFor_ModeAndHWMColumnDefaults(t *testing.T) {
	cfg := &Config{}
	eff := cfg.EffectiveFor("missing")
	if eff.Mode != "" {
		t.Errorf("mode default: got %q, want empty", eff.Mode)
	}
	if eff.HWMColumn != "" {
		t.Errorf("hwm_column default: got %q, want empty", eff.HWMColumn)
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/config/ -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/effective.go internal/config/effective_test.go
git commit -m "config: add Mode + HWMColumn per-dataset overrides"
```

---

## Task 9: `IncrementalStrategy` — bootstrap path

The first half of the strategy: when no prior state exists (or `mode: full_replace` is set), stream rows into staging with `socrata_id` PK installed, swap into main, and write the dataset_state row with the observed HWM. `IncrementalStrategy` is structurally independent of `FullReplaceStrategy` — they share helper functions but neither delegates to the other. This avoids piping HWM tracking through the Phase-1 strategy.

**Files:**
- Create: `internal/sync/incremental.go`
- Create: `internal/sync/incremental_test.go`

- [ ] **Step 1: Write the failing bootstrap test**

Create `internal/sync/incremental_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"strings"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// mkIncrDataset builds a fakeDataset with :id and :updated_at populated per row.
func mkIncrDataset(id string, n int, hwmBase string) fakeDataset {
	return fakeDataset{
		ID: id, Name: "Ds " + id,
		Columns: []map[string]string{
			{"fieldName": "score", "dataTypeName": "number"},
		},
		Rows: makeRows(n, func(i int) map[string]any {
			return map[string]any{
				":id":          id + "-" + itoa(i),
				":updated_at":  hwmBase + "T00:0" + itoa(i) + ":00.000",
				"score":        float64(i),
			}
		}),
	}
}

func TestIncremental_Bootstrap(t *testing.T) {
	ds := mkIncrDataset("aaaa-0001", 5, "2026-04-22")
	srv := newFakeSocrata(t, ds)
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 10}

	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run1"}
	target := DatasetTarget{
		ID: "aaaa-0001",
		Effective: config.Effective{
			DatasetID: "aaaa-0001", Table: "crimes", BatchSize: 10,
		},
	}

	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "ok" {
		t.Fatalf("status: got %q, err=%v", res.Status, res.Err)
	}

	var n int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM main.crimes`).Scan(&n)
	if n != 5 {
		t.Errorf("rows: got %d, want 5", n)
	}

	state, err := w.ReadDatasetState("aaaa-0001")
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if state == nil {
		t.Fatal("dataset_state row missing")
	}
	if state.HWMUpdatedAt == nil {
		t.Errorf("HWM not set")
	}
	if state.LastFullReplaceAt == nil {
		t.Errorf("LastFullReplaceAt not set")
	}
	if state.LastRunID != "run1" {
		t.Errorf("LastRunID: got %q", state.LastRunID)
	}
}

func TestIncremental_BootstrapInstallsPK(t *testing.T) {
	ds := mkIncrDataset("aaaa-0001", 2, "2026-04-22")
	srv := newFakeSocrata(t, ds)
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 10}

	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run1"}
	target := DatasetTarget{ID: "aaaa-0001",
		Effective: config.Effective{DatasetID: "aaaa-0001", Table: "crimes"}}
	_, _ = strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)

	// Confirm socrata_id column exists and PK enforces uniqueness
	_, err := w.DB.Exec(`INSERT INTO main.crimes (socrata_id, score) VALUES ('aaaa-0001-0', 999)`)
	if err == nil {
		t.Errorf("expected PK violation on duplicate socrata_id")
	}
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "constraint") &&
		!strings.Contains(strings.ToLower(err.Error()), "duplicate") {
		t.Errorf("unexpected error kind: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/ -run TestIncremental -v`
Expected: FAIL — `IncrementalStrategy` undefined.

- [ ] **Step 3: Write incremental.go (bootstrap only)**

Create `internal/sync/incremental.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// IncrementalStrategy implements WriteStrategy. It auto-detects bootstrap vs.
// delta based on _csq.dataset_state and the live table's socrata_id column,
// then either bootstraps via a staging-swap (Phase-1-style with HWM tracking)
// or performs a delta upsert pass.
//
// `mode: full_replace` in the per-dataset YAML override forces the bootstrap
// path on every run.
type IncrementalStrategy struct {
	Portal string
	Scheme string // "" defaults to "https"
	RunID  string
}

func (s *IncrementalStrategy) scheme() string {
	if s.Scheme != "" {
		return s.Scheme
	}
	return "https"
}

func (s *IncrementalStrategy) Sync(
	ctx context.Context,
	target DatasetTarget,
	client *socrata.Client,
	w *duckdb.Writer,
	prog ProgressReporter,
	idx, total int,
) (DatasetResult, error) {
	state, err := w.ReadDatasetState(target.ID)
	if err != nil {
		return failResult(target, "failed", fmt.Errorf("read dataset_state: %w", err)), nil
	}

	useBootstrap, err := shouldBootstrap(state, target, w)
	if err != nil {
		return failResult(target, "failed", err), nil
	}

	if useBootstrap {
		return s.bootstrap(ctx, target, client, w, prog, idx, total)
	}
	// Delta path lands in Task 10.
	return failResult(target, "failed",
		fmt.Errorf("incremental delta path not yet implemented")), nil
}

func shouldBootstrap(state *duckdb.DatasetState, target DatasetTarget, w *duckdb.Writer) (bool, error) {
	if target.Effective.Mode == "full_replace" {
		return true, nil
	}
	if state == nil {
		return true, nil
	}
	hasPK, err := tableHasSocrataIDPK(w, target.Effective.Table)
	if err != nil {
		return false, fmt.Errorf("check pk on %s: %w", target.Effective.Table, err)
	}
	return !hasPK, nil
}

// tableHasSocrataIDPK reports whether main.<table> exists AND has a socrata_id column.
// (PK presence is implied by the socrata_id column; we don't introspect constraints.)
func tableHasSocrataIDPK(w *duckdb.Writer, table string) (bool, error) {
	var n int
	err := w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.columns
		 WHERE table_schema = 'main' AND table_name = $1 AND column_name = 'socrata_id'`,
		table,
	).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// bootstrap streams the full dataset into _csq_staging.<table>_<runID> with the
// socrata_id PK installed, swaps it into main, then writes dataset_state with
// the observed max(:updated_at). Mirrors FullReplaceStrategy.Sync but tracks
// HWM during streaming so we don't have to reread the source.
func (s *IncrementalStrategy) bootstrap(
	ctx context.Context,
	target DatasetTarget,
	client *socrata.Client,
	w *duckdb.Writer,
	prog ProgressReporter,
	idx, total int,
) (DatasetResult, error) {
	started := time.Now().UTC()
	prog.DatasetStart(idx, total, target)
	result := DatasetResult{Target: target, StartedAt: started}

	hwmCol := target.Effective.HWMColumn
	if hwmCol == "" {
		hwmCol = ":updated_at"
	}

	meta, err := fetchMetadata(ctx, client, s.scheme(), s.Portal, target.ID)
	if err != nil {
		return failResult(target, "failed", fmt.Errorf("fetch metadata: %w", err)), nil
	}
	cols := filterColumns(meta.Columns, target.Effective.SkipColumns)
	stagingName := target.Effective.Table + "_" + s.RunID
	schema := duckdb.BuildSchemaWithSocrataID(stagingName, cols)

	if _, err := w.DB.ExecContext(ctx, schema.CreateTableSQLIn("_csq_staging")); err != nil {
		return failResult(target, "failed", fmt.Errorf("create staging: %w", err)), nil
	}

	var rowsWritten int64
	var maxHWM *time.Time
	err = client.StreamRowsCtx(ctx, s.scheme(), s.Portal, target.ID,
		target.Effective.OrderBy, target.Effective.Where, ":*,*",
		target.Effective.Limit,
		func(page []socrata.Row) error {
			if err := w.InsertRowsInto("_csq_staging", schema, page); err != nil {
				return err
			}
			for _, row := range page {
				if t := extractRowHWM(row, hwmCol); t != nil {
					if maxHWM == nil || t.After(*maxHWM) {
						maxHWM = t
					}
				}
			}
			rowsWritten += int64(len(page))
			prog.DatasetProgress(idx, total, target, rowsWritten)
			return nil
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			return failResult(target, "aborted", ctx.Err()), nil
		}
		return failResult(target, "failed", err), nil
	}

	if err := w.SwapIn(stagingName, target.Effective.Table); err != nil {
		return failResult(target, "failed", fmt.Errorf("swap: %w", err)), nil
	}

	now := time.Now().UTC()
	stateRow := duckdb.DatasetState{
		DatasetID:         target.ID,
		HWMUpdatedAt:      maxHWM,
		LastFullReplaceAt: &now,
		LastRunID:         s.RunID,
		HWMColumn:         hwmCol,
	}

	result.Status = "ok"
	result.RowsWritten = rowsWritten
	result.FinishedAt = time.Now().UTC()

	if err := w.UpsertDatasetState(stateRow); err != nil {
		// Data is in main but state didn't land. Surface as ok with err set;
		// sync_runs records the message. Next run will re-bootstrap (idempotent).
		result.Err = fmt.Errorf("write dataset_state: %w", err)
	}
	return result, nil
}

func extractRowHWM(row socrata.Row, hwmCol string) *time.Time {
	v, ok := row[hwmCol]
	if !ok || v == nil {
		return nil
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return nil
	}
	for _, layout := range []string{
		"2006-01-02T15:04:05.000",
		"2006-01-02T15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t
		}
	}
	return nil
}

// failResult is a small wrapper around fail() for the incremental strategy.
func failResult(target DatasetTarget, status string, err error) DatasetResult {
	res := DatasetResult{Target: target, StartedAt: time.Now().UTC()}
	res.Status = status
	res.Err = err
	res.FinishedAt = time.Now().UTC()
	return res
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sync/ -run TestIncremental -v`
Expected: `TestIncremental_Bootstrap` and `TestIncremental_BootstrapInstallsPK` pass.

- [ ] **Step 5: Run full sync suite to confirm no regression**

Run: `go test ./internal/sync/ -v`
Expected: all pass — `FullReplaceStrategy` is unchanged so its tests remain green.

- [ ] **Step 6: Commit**

```bash
git add internal/sync/incremental.go internal/sync/incremental_test.go
git commit -m "sync: add IncrementalStrategy bootstrap path with HWM tracking"
```

---

## Task 10: `IncrementalStrategy` — delta path

The second half: when prior state exists and the table has the PK, do a `$where=hwm > 'TS'` stream + upsert. Includes schema-drift detection.

**Files:**
- Modify: `internal/sync/incremental.go`
- Modify: `internal/sync/incremental_test.go`

- [ ] **Step 1: Write the failing delta tests**

Append to `internal/sync/incremental_test.go`:

```go
// helper that runs an IncrementalStrategy against the fake portal and returns the result.
func runIncr(t *testing.T, ds fakeDataset, w *duckdb.Writer, runID string, prevState *duckdb.DatasetState, mode string) DatasetResult {
	t.Helper()
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 10}
	if prevState != nil {
		_ = w.UpsertDatasetState(*prevState)
	}
	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: runID}
	target := DatasetTarget{ID: ds.ID,
		Effective: config.Effective{
			DatasetID: ds.ID, Table: "crimes", BatchSize: 10, Mode: mode,
		}}
	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	return res
}

func TestIncremental_DeltaInsert(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	// Bootstrap with 3 rows at 2026-04-22.
	ds1 := mkIncrDataset("aaaa-0001", 3, "2026-04-22")
	res1 := runIncr(t, ds1, w, "run1", nil, "")
	if res1.Status != "ok" {
		t.Fatalf("bootstrap: %v", res1.Err)
	}

	// Second run: source now has 3 old rows + 2 new rows at 2026-04-23.
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001",
		Columns: ds1.Columns,
		Rows: append(append([]map[string]any{}, ds1.Rows...),
			map[string]any{":id": "new-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(100)},
			map[string]any{":id": "new-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(101)},
		),
	}
	res2 := runIncr(t, ds2, w, "run2", nil, "")
	if res2.Status != "ok" {
		t.Fatalf("delta: %v", res2.Err)
	}

	var n int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM main.crimes`).Scan(&n)
	if n != 5 {
		t.Errorf("count: got %d, want 5", n)
	}

	state, _ := w.ReadDatasetState("aaaa-0001")
	if state.HWMUpdatedAt == nil || state.HWMUpdatedAt.Year() != 2026 || state.HWMUpdatedAt.Day() != 23 {
		t.Errorf("HWM not advanced; got %v", state.HWMUpdatedAt)
	}
}

func TestIncremental_DeltaUpdate(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds1 := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, ds1, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}

	// Same :id, newer :updated_at, different score.
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001",
		Columns: ds1.Columns,
		Rows: []map[string]any{
			{":id": "aaaa-0001-0", ":updated_at": "2026-04-22T00:00:00.000", "score": float64(0)},
			{":id": "aaaa-0001-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(999)},
		},
	}
	if res := runIncr(t, ds2, w, "run2", nil, ""); res.Status != "ok" {
		t.Fatalf("delta: %v", res.Err)
	}

	var n int
	var score float64
	_ = w.DB.QueryRow(`SELECT COUNT(*), MAX(score) FROM main.crimes`).Scan(&n, &score)
	if n != 1 || score != 999 {
		t.Errorf("upsert: count=%d score=%v want 1/999", n, score)
	}
}

func TestIncremental_NoNewRows(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds := mkIncrDataset("aaaa-0001", 2, "2026-04-22")
	res1 := runIncr(t, ds, w, "run1", nil, "")
	if res1.Status != "ok" {
		t.Fatalf("bootstrap: %v", res1.Err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")

	// Second run with the same data — nothing newer than HWM.
	res2 := runIncr(t, ds, w, "run2", nil, "")
	if res2.Status != "ok" {
		t.Fatalf("delta no-op: %v", res2.Err)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if !state2.HWMUpdatedAt.Equal(*state1.HWMUpdatedAt) {
		t.Errorf("HWM moved: %v -> %v", state1.HWMUpdatedAt, state2.HWMUpdatedAt)
	}
	if state2.LastRunID != "run2" {
		t.Errorf("LastRunID not bumped: %q", state2.LastRunID)
	}
}

func TestIncremental_SchemaDriftFails(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds1 := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, ds1, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}

	// Drift: portal removes the score column.
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001",
		Columns: []map[string]string{},
		Rows: []map[string]any{
			{":id": "new", ":updated_at": "2026-04-23T00:00:00.000"},
		},
	}
	res2 := runIncr(t, ds2, w, "run2", nil, "")
	if res2.Status != "failed" {
		t.Errorf("status: got %q, want failed", res2.Status)
	}
	if res2.Err == nil || !containsAll(res2.Err.Error(), "schema drift", "score") {
		t.Errorf("err: %v (want schema drift mentioning score)", res2.Err)
	}
}

func TestIncremental_FullReplaceOptOut(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds1 := mkIncrDataset("aaaa-0001", 2, "2026-04-22")
	if res := runIncr(t, ds1, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")

	// Source totally different; mode=full_replace forces re-bootstrap.
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001",
		Columns: ds1.Columns,
		Rows: []map[string]any{
			{":id": "fresh-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(7)},
		},
	}
	res2 := runIncr(t, ds2, w, "run2", nil, "full_replace")
	if res2.Status != "ok" {
		t.Fatalf("opt-out: %v", res2.Err)
	}

	var n int
	var first string
	_ = w.DB.QueryRow(`SELECT COUNT(*), MIN(socrata_id) FROM main.crimes`).Scan(&n, &first)
	if n != 1 || first != "fresh-0" {
		t.Errorf("table not rebootstrapped: count=%d first=%q", n, first)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if !state2.LastFullReplaceAt.After(*state1.LastFullReplaceAt) {
		t.Errorf("LastFullReplaceAt did not advance: %v -> %v",
			state1.LastFullReplaceAt, state2.LastFullReplaceAt)
	}
}

func TestIncremental_StreamFailMidPage(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	ds1 := mkIncrDataset("aaaa-0001", 2, "2026-04-22")
	if res := runIncr(t, ds1, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")

	// Second run: lots of new rows but the fake fails at offset 3.
	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(0)},
		{":id": "x-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(1)},
		{":id": "x-2", ":updated_at": "2026-04-23T00:00:02.000", "score": float64(2)},
		{":id": "x-3", ":updated_at": "2026-04-23T00:00:03.000", "score": float64(3)},
		{":id": "x-4", ":updated_at": "2026-04-23T00:00:04.000", "score": float64(4)},
	}
	ds2 := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001", Columns: ds1.Columns,
		Rows:         rows,
		FailAtOffset: 3,
	}
	res2 := runIncr(t, ds2, w, "run2", nil, "")
	if res2.Status != "failed" {
		t.Errorf("status: got %q, want failed", res2.Status)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if !state2.HWMUpdatedAt.Equal(*state1.HWMUpdatedAt) {
		t.Errorf("HWM advanced on failure: %v -> %v", state1.HWMUpdatedAt, state2.HWMUpdatedAt)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
```

Add `"strings"` to the test file's imports if not already present (TestIncremental_BootstrapInstallsPK already uses it).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/ -run TestIncremental -v`
Expected: bootstrap tests still pass; the new tests fail with "delta path not yet implemented".

- [ ] **Step 3: Implement the delta path**

Edit `internal/sync/incremental.go`. Replace the placeholder branch in `Sync`:

```go
	// Delta path lands in Task 10.
	return failResult(target, "failed",
		fmt.Errorf("incremental delta path not yet implemented")), nil
```

with a call to a new `delta` method:

```go
	return s.delta(ctx, target, client, w, prog, idx, total, state)
```

Add the `delta` method to the file:

```go
func (s *IncrementalStrategy) delta(
	ctx context.Context,
	target DatasetTarget,
	client *socrata.Client,
	w *duckdb.Writer,
	prog ProgressReporter,
	idx, total int,
	state *duckdb.DatasetState,
) (DatasetResult, error) {
	started := time.Now().UTC()
	prog.DatasetStart(idx, total, target)
	result := DatasetResult{Target: target, StartedAt: started}

	hwmCol := state.HWMColumn
	if hwmCol == "" {
		hwmCol = ":updated_at"
	}

	// Fetch metadata + check drift before any writes.
	meta, err := fetchMetadata(ctx, client, s.scheme(), s.Portal, target.ID)
	if err != nil {
		return failResult(target, "failed", fmt.Errorf("fetch metadata: %w", err)), nil
	}
	cols := filterColumns(meta.Columns, target.Effective.SkipColumns)
	wantSchema := duckdb.BuildSchemaWithSocrataID(target.Effective.Table, cols)

	diffs, err := duckdb.DiffSchema(wantSchema, w.DB, "main", target.Effective.Table)
	if err != nil {
		return failResult(target, "failed", fmt.Errorf("diff schema: %w", err)), nil
	}
	if len(diffs) > 0 {
		return failResult(target, "failed", schemaDriftError(target.Effective.Table, diffs)), nil
	}

	// Build $where = "<hwm> > 'TS'", AND-combined with target.Effective.Where if set.
	whereClause := ""
	if state.HWMUpdatedAt != nil {
		whereClause = fmt.Sprintf("%s > '%s'", hwmCol, state.HWMUpdatedAt.UTC().Format("2006-01-02T15:04:05.000"))
	}
	if target.Effective.Where != "" {
		if whereClause != "" {
			whereClause = "(" + whereClause + ") AND (" + target.Effective.Where + ")"
		} else {
			whereClause = target.Effective.Where
		}
	}

	// Compound order: hwmCol then :id, for stable pagination across same-timestamp rows.
	orderBy := target.Effective.OrderBy
	if orderBy == "" {
		orderBy = hwmCol + ",:id"
	}

	var rowsWritten int64
	maxHWM := state.HWMUpdatedAt
	err = client.StreamRowsCtx(ctx, s.scheme(), s.Portal, target.ID,
		orderBy, whereClause, ":*,*", target.Effective.Limit,
		func(page []socrata.Row) error {
			if err := w.UpsertRows("main", wantSchema, page); err != nil {
				return err
			}
			for _, row := range page {
				if t := extractRowHWM(row, hwmCol); t != nil {
					if maxHWM == nil || t.After(*maxHWM) {
						maxHWM = t
					}
				}
			}
			rowsWritten += int64(len(page))
			prog.DatasetProgress(idx, total, target, rowsWritten)
			return nil
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			return failResult(target, "aborted", ctx.Err()), nil
		}
		return failResult(target, "failed", err), nil
	}

	// Stream succeeded — persist new HWM (preserving last_full_replace_at).
	state.HWMUpdatedAt = maxHWM
	state.LastRunID = s.RunID
	if err := w.UpsertDatasetState(*state); err != nil {
		// Mirror bootstrap behavior: data is in main; surface as failed-state-write.
		result.Status = "ok"
		result.RowsWritten = rowsWritten
		result.FinishedAt = time.Now().UTC()
		result.Err = fmt.Errorf("write dataset_state: %w", err)
		return result, nil
	}

	result.Status = "ok"
	result.RowsWritten = rowsWritten
	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func schemaDriftError(table string, diffs []duckdb.SchemaDiff) error {
	parts := make([]string, 0, len(diffs))
	for _, d := range diffs {
		switch d.Kind {
		case "added":
			parts = append(parts, fmt.Sprintf("%s added", d.Column))
		case "removed":
			parts = append(parts, fmt.Sprintf("%s removed", d.Column))
		case "retyped":
			parts = append(parts, fmt.Sprintf("%s retyped from %s to %s", d.Column, d.Have, d.Want))
		}
	}
	return fmt.Errorf("schema drift on %s: %s; set mode: full_replace in YAML to rebootstrap",
		table, strings.Join(parts, ", "))
}
```

Add `"strings"` to the imports of `incremental.go`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sync/ -run TestIncremental -v`
Expected: all seven Incremental tests pass.

- [ ] **Step 5: Run the full sync suite to confirm Phase 1 still passes**

Run: `go test ./internal/sync/ -v`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add internal/sync/incremental.go internal/sync/incremental_test.go
git commit -m "sync: add IncrementalStrategy delta path with drift detection"
```

---

## Task 11: Wire `IncrementalStrategy` as the default in `run.go`

Existing Phase 1 YAMLs get incremental for free.

**Files:**
- Modify: `internal/sync/run.go`
- Modify: `internal/sync/run_test.go`

- [ ] **Step 1: Update existing TestRun_* to provide :updated_at**

Edit `internal/sync/run_test.go`. Modify `mkDataset` to include `:id` and `:updated_at` per row so the auto-bootstrap path captures a real HWM:

```go
func mkDataset(id string, rows int, failAt int) fakeDataset {
	return fakeDataset{
		ID: id, Name: "Ds " + id,
		Columns: []map[string]string{
			{"fieldName": "id", "dataTypeName": "text"},
			{"fieldName": "score", "dataTypeName": "number"},
		},
		Rows: makeRows(rows, func(i int) map[string]any {
			return map[string]any{
				":id":         id + "-" + itoa(i),
				":updated_at": "2026-04-22T00:0" + itoa(i%10) + ":00.000",
				"id":          id + "-" + itoa(i),
				"score":       float64(i),
			}
		}),
		FailAtOffset: failAt,
	}
}
```

- [ ] **Step 2: Switch the default strategy in run.go**

Edit `internal/sync/run.go`. Replace the strategy default block:

```go
	if d.Strategy == nil {
		d.Strategy = &FullReplaceStrategy{Portal: cfg.Portal, Scheme: scheme, RunID: runID}
	}
```

with:

```go
	if d.Strategy == nil {
		d.Strategy = &IncrementalStrategy{
			Portal: cfg.Portal, Scheme: scheme, RunID: runID,
		}
	}
```

- [ ] **Step 3: Run the full sync suite**

Run: `go test ./internal/sync/ -v`
Expected: all pass — TestRun_* now exercise the incremental bootstrap path; TestFullReplaceStrategy_* are unaffected because they construct strategies directly.

- [ ] **Step 4: Run all tests for cross-package regressions**

Run: `go test ./...`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/run.go internal/sync/run_test.go
git commit -m "sync: default to IncrementalStrategy in Run orchestrator"
```

---

## Task 12: End-to-end CLI smoke test for incremental

Two `csq sync` invocations against the fake portal: bootstrap, then add rows, then delta.

**Files:**
- Modify: `cmd/csq/cli_smoke_test.go`

- [ ] **Step 1: Write the smoke test**

Edit `cmd/csq/cli_smoke_test.go`. Append a new test function:

```go
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
```

- [ ] **Step 2: Run the smoke test**

Run: `go test ./cmd/csq/ -run TestCSQ_IncrementalSmoke -v`
Expected: PASS.

- [ ] **Step 3: Run the full CLI suite to confirm Phase 1 smoke still passes**

Run: `go test ./cmd/csq/ -v`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add cmd/csq/cli_smoke_test.go
git commit -m "cli: add incremental sync smoke test (bootstrap + delta)"
```

---

## Task 13: README update for Phase 2

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the status and config-shape sections**

Edit `README.md`. Replace the `## Status` section:

```markdown
## Status

**Phase 2** — incremental sync via per-dataset high-water marks. The first run of any dataset bootstraps with a full-replace; subsequent runs fetch only rows updated since the last successful sync.
```

In the `### Config shape` block, add the two new override fields to the example:

```yaml
overrides:
  6zsd-86xi:
    table: crimes
    where: "date >= '2015-01-01'"
    batch_size: 10000
    columns:
      skip: [location_description_raw]
    # Phase 2 fields (both optional):
    mode: full_replace        # force full-replace on every run; default is incremental
    hwm_column: ":updated_at" # override the high-water-mark column
```

After the existing `_csq.catalog` / `_csq.sync_runs` SQL examples, add:

```sql
-- Per-dataset incremental-sync state (Phase 2)
SELECT dataset_id, hwm_updated_at, last_full_replace_at, last_run_id
  FROM _csq.dataset_state ORDER BY hwm_updated_at DESC;
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: Phase 2 README — incremental sync + dataset_state"
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

- [ ] **Manual smoke against a real portal (optional, not CI)**

```bash
export SOCRATA_APP_TOKEN=...
./csq catalog --portal data.cityofchicago.org --category "Public Safety" --output /tmp/chi.yaml
# uncomment 1-2 small datasets in /tmp/chi.yaml
./csq sync --config /tmp/chi.yaml      # bootstrap
./csq sync --config /tmp/chi.yaml      # delta (should be a no-op or near-no-op)
```

Inspect the per-dataset state:
```bash
duckdb data.cityofchicago.org.duckdb \
  "SELECT dataset_id, hwm_updated_at, last_full_replace_at FROM _csq.dataset_state"
```
