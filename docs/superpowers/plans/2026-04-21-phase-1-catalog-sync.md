# Phase 1 — Catalog-driven bulk sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `csq sync` and `csq catalog` subcommands that materialize YAML-selected datasets from any Socrata portal into a per-portal DuckDB, with wildcard selectors, atomic staging-and-swap writes, and a `_csq` state schema that carries the catalog and sync history.

**Architecture:** Three interface seams (`SelectorResolver`, `WriteStrategy`, `ProgressReporter`) in a new `internal/sync` package, backed by a new `internal/config` package for YAML and additions to `internal/socrata` (catalog fetch) and `internal/duckdb` (`_csq` schema + staging swap). Orchestrator uses `errgroup` with `SetLimit(concurrency)` for dataset-level parallelism; each dataset streams through a per-run staging table and is rename-swapped into place inside a single transaction.

**Tech Stack:** Go 1.24, DuckDB via `github.com/duckdb/duckdb-go/v2`, YAML via `gopkg.in/yaml.v3`, ULID via `github.com/oklog/ulid/v2`, concurrency via `golang.org/x/sync/errgroup` (already indirect), globs via stdlib `path.Match`.

**Spec:** `docs/superpowers/specs/2026-04-21-phase-1-catalog-sync-design.md`

---

## File Structure

**New files:**

```
internal/socrata/catalog.go              # CatalogEntry + Client.FetchCatalog
internal/socrata/catalog_test.go

internal/duckdb/migrations.go            # _csq + _csq_staging schema setup
internal/duckdb/migrations_test.go
internal/duckdb/catalog_store.go         # UpsertCatalog / ReadCatalog
internal/duckdb/catalog_store_test.go
internal/duckdb/sync_runs.go             # Insert/Update sync_runs rows
internal/duckdb/sync_runs_test.go
internal/duckdb/swap.go                  # SwapIn(stagingName, runID, targetTable)
internal/duckdb/swap_test.go

internal/config/config.go                # Config, Rules, Selector, Overrides, Effective
internal/config/load.go                  # Load(path) — YAML parse + validate + ${ENV}
internal/config/load_test.go
internal/config/effective.go             # EffectiveFor(id) — merge defaults + override
internal/config/effective_test.go
internal/config/testdata/valid.yaml
internal/config/testdata/invalid_unknown_key.yaml
internal/config/testdata/invalid_bad_on_error.yaml

internal/sync/types.go                   # DatasetTarget, DatasetResult
internal/sync/selector.go                # SelectorResolver + DefaultSelectorResolver
internal/sync/selector_test.go
internal/sync/progress.go                # ProgressReporter interface + StderrReporter + RecordingReporter (test helper)
internal/sync/progress_test.go
internal/sync/strategy.go                # WriteStrategy interface + FullReplaceStrategy
internal/sync/strategy_test.go
internal/sync/run.go                     # Run(ctx, Config, deps) — orchestrator
internal/sync/run_test.go
internal/sync/fakesocrata_test.go        # test helper: in-memory httptest server

cmd/csq/catalog.go                       # runCatalog subcommand
cmd/csq/sync.go                          # runSync subcommand
cmd/csq/cli_smoke_test.go                # black-box: build binary, run against fake server
cmd/csq/testdata/portal.yaml
```

**Modified files:**

```
cmd/csq/main.go                          # dispatch "catalog" and "sync" subcommands
internal/duckdb/writer.go                # Open() calls migrations.Apply()
go.mod / go.sum                          # + yaml.v3, + ulid/v2 as direct deps
README.md                                # Phase 1 quickstart section
```

---

## Task 0: Bootstrap dependencies

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`

- [ ] **Step 1: Add YAML and ULID as direct dependencies**

Run:
```bash
cd /Users/evan/projects/cannabis_research/CivicSodaQuack
go get gopkg.in/yaml.v3@v3.0.1
go get github.com/oklog/ulid/v2@v2.1.0
go mod tidy
```

- [ ] **Step 2: Verify build still works**

Run: `go build ./...`
Expected: no errors, no output.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: add yaml.v3 and ulid/v2 for Phase 1"
```

---

## Task 1: DuckDB `_csq` schema migrations

The `_csq` schema holds catalog cache and sync history; `_csq_staging` holds in-flight tables. Both are created idempotently on `duckdb.Open()`.

**Files:**
- Create: `internal/duckdb/migrations.go`
- Create: `internal/duckdb/migrations_test.go`
- Modify: `internal/duckdb/writer.go` (wire `Apply` into `Open`)

- [ ] **Step 1: Write the failing test**

Create `internal/duckdb/migrations_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"
)

func TestApplyMigrations_CreatesSchemasAndTables(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	// Both schemas exist
	for _, schema := range []string{"_csq", "_csq_staging"} {
		var n int
		row := w.DB.QueryRow(
			`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?`, schema)
		if err := row.Scan(&n); err != nil {
			t.Fatalf("query schema %q: %v", schema, err)
		}
		if n != 1 {
			t.Errorf("schema %q: want 1 row, got %d", schema, n)
		}
	}

	// Catalog + sync_runs tables exist
	for _, table := range []string{"catalog", "sync_runs"} {
		var n int
		row := w.DB.QueryRow(
			`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '_csq' AND table_name = ?`, table)
		if err := row.Scan(&n); err != nil {
			t.Fatalf("query table _csq.%s: %v", table, err)
		}
		if n != 1 {
			t.Errorf("table _csq.%s: want 1 row, got %d", table, n)
		}
	}
}

func TestApplyMigrations_Idempotent(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	if err := Apply(w.DB); err != nil {
		t.Fatalf("apply second time: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/duckdb/ -run TestApplyMigrations -v`
Expected: FAIL — `Apply` undefined and `Open` doesn't create the schemas yet.

- [ ] **Step 3: Write migrations.go**

Create `internal/duckdb/migrations.go`:

```go
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
			config_hash  VARCHAR
		)`,
		`CREATE INDEX IF NOT EXISTS sync_runs_by_dataset ON _csq.sync_runs (dataset_id, started_at DESC)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("apply migration: %w\nSQL: %s", err, s)
		}
	}
	return nil
}
```

- [ ] **Step 4: Wire `Apply` into `Open`**

Modify `internal/duckdb/writer.go`. Find the `Open` function (around line 20-30). After the existing `db.Ping()` success branch, before `return &Writer{DB: db}, nil`, call `Apply`:

```go
func Open(path string) (*Writer, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("open duckdb %q: %w", path, err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb %q: %w", path, err)
	}
	if err := Apply(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply migrations %q: %w", path, err)
	}
	return &Writer{DB: db}, nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/duckdb/ -run TestApplyMigrations -v`
Expected: PASS.

- [ ] **Step 6: Run the full package tests to make sure nothing else broke**

Run: `go test ./internal/duckdb/ -v`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add internal/duckdb/migrations.go internal/duckdb/migrations_test.go internal/duckdb/writer.go
git commit -m "duckdb: add _csq + _csq_staging schema migrations on Open"
```

---

## Task 2: Socrata catalog fetch

Fetches `/api/catalog/v1?domains=<portal>&limit=N&offset=M` page-by-page, returns `[]CatalogEntry`.

**Files:**
- Create: `internal/socrata/catalog.go`
- Create: `internal/socrata/catalog_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/socrata/catalog_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package socrata

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestFetchCatalog_Paginates(t *testing.T) {
	total := 5
	pageSize := 2
	mux := http.NewServeMux()
	mux.HandleFunc("/api/catalog/v1", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		offset, _ := strconv.Atoi(q.Get("offset"))
		limit, _ := strconv.Atoi(q.Get("limit"))
		results := []map[string]any{}
		for i := offset; i < offset+limit && i < total; i++ {
			results = append(results, map[string]any{
				"resource": map[string]any{
					"id":              "abcd-000" + strconv.Itoa(i),
					"name":            "Dataset " + strconv.Itoa(i),
					"description":     "desc",
					"rowsUpdatedAt":   "2024-01-15T00:00:00.000",
				},
				"classification": map[string]any{
					"domain_category": "Public Safety",
					"domain_tags":     []string{"crime"},
				},
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results":       results,
			"resultSetSize": total,
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	c := &Client{BatchSize: pageSize}
	entries, err := c.fetchCatalogScheme(host, "http")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(entries) != total {
		t.Fatalf("got %d entries, want %d", len(entries), total)
	}
	if entries[0].ID != "abcd-0000" {
		t.Errorf("first id: got %q, want abcd-0000", entries[0].ID)
	}
	if entries[0].Category != "Public Safety" {
		t.Errorf("category: got %q, want %q", entries[0].Category, "Public Safety")
	}
	if len(entries[0].Tags) != 1 || entries[0].Tags[0] != "crime" {
		t.Errorf("tags: got %v", entries[0].Tags)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/socrata/ -run TestFetchCatalog -v`
Expected: FAIL — `fetchCatalogScheme` and `CatalogEntry` undefined.

- [ ] **Step 3: Write catalog.go**

Create `internal/socrata/catalog.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package socrata

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// CatalogEntry is a single dataset as returned by /api/catalog/v1.
type CatalogEntry struct {
	ID          string
	Name        string
	Description string
	Category    string
	Tags        []string
	RowCount    *int64
	UpdatedAt   *time.Time
	Raw         json.RawMessage
}

// FetchCatalog returns every dataset the portal reports, following pagination.
func (c *Client) FetchCatalog(portal string) ([]CatalogEntry, error) {
	return c.fetchCatalogScheme(portal, "https")
}

// fetchCatalogScheme is the scheme-parameterised form used in tests with httptest.
func (c *Client) fetchCatalogScheme(portal, scheme string) ([]CatalogEntry, error) {
	base := &url.URL{Scheme: scheme, Host: portal, Path: "/api/catalog/v1"}

	var all []CatalogEntry
	offset := 0
	pageSize := c.batchSize()

	for {
		q := url.Values{}
		q.Set("domains", portal)
		q.Set("limit", strconv.Itoa(pageSize))
		q.Set("offset", strconv.Itoa(offset))
		base.RawQuery = q.Encode()

		page, total, err := c.getCatalogPage(base.String())
		if err != nil {
			return nil, err
		}
		all = append(all, page...)

		offset += len(page)
		if len(page) == 0 || offset >= total {
			return all, nil
		}
	}
}

type rawCatalogEntry struct {
	Resource struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		Description   string `json:"description"`
		RowsUpdatedAt string `json:"rowsUpdatedAt"`
	} `json:"resource"`
	Classification struct {
		DomainCategory string   `json:"domain_category"`
		DomainTags     []string `json:"domain_tags"`
	} `json:"classification"`
}

type catalogResponse struct {
	Results       []json.RawMessage `json:"results"`
	ResultSetSize int               `json:"resultSetSize"`
}

func (c *Client) getCatalogPage(fullURL string) ([]CatalogEntry, int, error) {
	req, err := http.NewRequest(http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build catalog request: %w", err)
	}
	if c.AppToken != "" {
		req.Header.Set("X-App-Token", c.AppToken)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("catalog request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("catalog HTTP %d: %s", resp.StatusCode, string(body))
	}

	var cr catalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, 0, fmt.Errorf("decode catalog: %w", err)
	}

	entries := make([]CatalogEntry, 0, len(cr.Results))
	for _, raw := range cr.Results {
		var r rawCatalogEntry
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, 0, fmt.Errorf("decode catalog entry: %w", err)
		}
		e := CatalogEntry{
			ID:          r.Resource.ID,
			Name:        r.Resource.Name,
			Description: r.Resource.Description,
			Category:    r.Classification.DomainCategory,
			Tags:        r.Classification.DomainTags,
			Raw:         raw,
		}
		if r.Resource.RowsUpdatedAt != "" {
			if t, err := time.Parse("2006-01-02T15:04:05.000", r.Resource.RowsUpdatedAt); err == nil {
				e.UpdatedAt = &t
			}
		}
		entries = append(entries, e)
	}
	return entries, cr.ResultSetSize, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/socrata/ -run TestFetchCatalog -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/socrata/catalog.go internal/socrata/catalog_test.go
git commit -m "socrata: add paged /api/catalog/v1 fetcher"
```

---

## Task 3: DuckDB catalog store

Upsert and read `_csq.catalog`. Upsert = delete-all-then-insert in one transaction.

**Files:**
- Create: `internal/duckdb/catalog_store.go`
- Create: `internal/duckdb/catalog_store_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/duckdb/catalog_store_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestCatalogStore_UpsertAndRead(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	updated := time.Date(2025, 12, 15, 12, 0, 0, 0, time.UTC)

	in := []socrata.CatalogEntry{
		{
			ID: "abcd-0001", Name: "Crimes", Category: "Public Safety",
			Tags: []string{"crime"}, UpdatedAt: &updated,
			Raw: json.RawMessage(`{"resource":{"id":"abcd-0001"}}`),
		},
		{
			ID: "abcd-0002", Name: "Permits",
			Raw: json.RawMessage(`{"resource":{"id":"abcd-0002"}}`),
		},
	}

	if err := w.UpsertCatalog(in, now); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := w.ReadCatalog()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d", len(got))
	}

	// Second upsert replaces, doesn't duplicate
	in2 := []socrata.CatalogEntry{{ID: "abcd-0003", Name: "Parks", Raw: json.RawMessage(`{}`)}}
	if err := w.UpsertCatalog(in2, now); err != nil {
		t.Fatalf("upsert2: %v", err)
	}
	got, err = w.ReadCatalog()
	if err != nil {
		t.Fatalf("read2: %v", err)
	}
	if len(got) != 1 || got[0].ID != "abcd-0003" {
		t.Errorf("replace failed: got %+v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/duckdb/ -run TestCatalogStore -v`
Expected: FAIL — `UpsertCatalog` / `ReadCatalog` undefined.

- [ ] **Step 3: Write catalog_store.go**

Create `internal/duckdb/catalog_store.go`:

```go
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
		var description, category, tagsJSON, rawJSON sql.NullString
		var rowCount sql.NullInt64
		var updatedAt sql.NullTime
		if err := rows.Scan(&e.ID, &e.Name, &description, &category,
			&tagsJSON, &rowCount, &updatedAt, &rawJSON); err != nil {
			return nil, fmt.Errorf("scan catalog row: %w", err)
		}
		e.Description = description.String
		e.Category = category.String
		if tagsJSON.Valid && tagsJSON.String != "" {
			_ = json.Unmarshal([]byte(tagsJSON.String), &e.Tags)
		}
		if rowCount.Valid {
			rc := rowCount.Int64
			e.RowCount = &rc
		}
		if updatedAt.Valid {
			ua := updatedAt.Time
			e.UpdatedAt = &ua
		}
		if rawJSON.Valid {
			e.Raw = json.RawMessage(rawJSON.String)
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/duckdb/ -run TestCatalogStore -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/duckdb/catalog_store.go internal/duckdb/catalog_store_test.go
git commit -m "duckdb: add catalog store (upsert + read _csq.catalog)"
```

---

## Task 4: Config package — types and YAML loader

Loads, validates, and `${ENV}`-expands the per-portal YAML.

**Files:**
- Create: `internal/config/config.go`
- Create: `internal/config/load.go`
- Create: `internal/config/load_test.go`
- Create: `internal/config/testdata/valid.yaml`
- Create: `internal/config/testdata/invalid_unknown_key.yaml`
- Create: `internal/config/testdata/invalid_bad_on_error.yaml`

- [ ] **Step 1: Write config.go (types only — no behavior yet)**

Create `internal/config/config.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package config

// Config is the parsed portal YAML.
type Config struct {
	Portal      string             `yaml:"portal"`
	AppToken    string             `yaml:"app_token"`
	DB          string             `yaml:"db"`
	Concurrency int                `yaml:"concurrency"`
	OnError     string             `yaml:"on_error"` // "continue" | "abort"
	Defaults    Defaults           `yaml:"defaults"`
	Include     []Selector         `yaml:"include"`
	Exclude     []Selector         `yaml:"exclude"`
	Overrides   map[string]Override `yaml:"overrides"`
}

// Defaults are per-dataset values applied when no override is set.
type Defaults struct {
	BatchSize int    `yaml:"batch_size"`
	OrderBy   string `yaml:"order_by"`
}

// Selector matches one or more datasets by id, name glob, category glob,
// or tag glob. Exactly one of Id/Name/Category/Tag must be set.
type Selector struct {
	ID       string `yaml:"id"`
	Name     string `yaml:"name"`
	Category string `yaml:"category"`
	Tag      string `yaml:"tag"`
}

// Override is per-dataset configuration (keyed by 4x4 id in YAML).
type Override struct {
	Table     string   `yaml:"table"`
	Where     string   `yaml:"where"`
	OrderBy   string   `yaml:"order_by"`
	BatchSize int      `yaml:"batch_size"`
	Limit     int      `yaml:"limit"`
	Columns   Columns  `yaml:"columns"`
}

// Columns carries column-level overrides.
type Columns struct {
	Skip []string `yaml:"skip"`
}
```

- [ ] **Step 2: Write valid/invalid testdata fixtures**

Create `internal/config/testdata/valid.yaml`:

```yaml
portal: data.cityofchicago.org
app_token: ${SOCRATA_APP_TOKEN}
db: data.cityofchicago.org.duckdb
concurrency: 4
on_error: continue
defaults:
  batch_size: 5000
  order_by: ":id"
include:
  - id: 6zsd-86xi
  - name: "Crimes*"
  - category: "Public Safety"
  - tag: "311*"
exclude:
  - id: 85ca-t3if
  - name: "*Archive*"
overrides:
  6zsd-86xi:
    table: crimes
    where: "date >= '2015-01-01'"
    order_by: ":updated_at"
    batch_size: 10000
    columns:
      skip:
        - location_description_raw
  ijzp-q8t2:
    limit: 100000
```

Create `internal/config/testdata/invalid_unknown_key.yaml`:

```yaml
portal: data.example.org
mystery_key: whatever
include:
  - id: aaaa-bbbb
```

Create `internal/config/testdata/invalid_bad_on_error.yaml`:

```yaml
portal: data.example.org
on_error: panic
include:
  - id: aaaa-bbbb
```

- [ ] **Step 3: Write the failing test**

Create `internal/config/load_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoad_Valid(t *testing.T) {
	t.Setenv("SOCRATA_APP_TOKEN", "test-token-abc")
	cfg, err := Load("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Portal != "data.cityofchicago.org" {
		t.Errorf("portal: got %q", cfg.Portal)
	}
	if cfg.AppToken != "test-token-abc" {
		t.Errorf("app_token: got %q, want expanded", cfg.AppToken)
	}
	if cfg.Concurrency != 4 {
		t.Errorf("concurrency: got %d", cfg.Concurrency)
	}
	if cfg.OnError != "continue" {
		t.Errorf("on_error: got %q", cfg.OnError)
	}
	if len(cfg.Include) != 4 {
		t.Errorf("include: got %d selectors", len(cfg.Include))
	}
	if cfg.Overrides["6zsd-86xi"].Table != "crimes" {
		t.Errorf("override 6zsd-86xi.table: got %q", cfg.Overrides["6zsd-86xi"].Table)
	}
	if len(cfg.Overrides["6zsd-86xi"].Columns.Skip) != 1 {
		t.Errorf("columns.skip: got %v", cfg.Overrides["6zsd-86xi"].Columns.Skip)
	}
}

func TestLoad_Defaults(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "cfg-*.yaml")
	if err != nil {
		t.Fatalf("tempfile: %v", err)
	}
	f.WriteString("portal: data.example.org\ninclude:\n  - id: aaaa-bbbb\n")
	f.Close()

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Concurrency != 4 {
		t.Errorf("concurrency default: got %d, want 4", cfg.Concurrency)
	}
	if cfg.OnError != "continue" {
		t.Errorf("on_error default: got %q", cfg.OnError)
	}
}

func TestLoad_UnknownKey(t *testing.T) {
	_, err := Load("testdata/invalid_unknown_key.yaml")
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "mystery_key") {
		t.Errorf("error should name the unknown key: %v", err)
	}
}

func TestLoad_BadOnError(t *testing.T) {
	_, err := Load("testdata/invalid_bad_on_error.yaml")
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "on_error") {
		t.Errorf("error should name the field: %v", err)
	}
}

func TestLoad_EmptyInclude(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "cfg-*.yaml")
	if err != nil {
		t.Fatalf("tempfile: %v", err)
	}
	f.WriteString("portal: data.example.org\n")
	f.Close()

	_, err = Load(f.Name())
	if err == nil {
		t.Fatal("want error for missing include, got nil")
	}
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `go test ./internal/config/ -v`
Expected: FAIL — `Load` undefined.

- [ ] **Step 5: Write load.go**

Create `internal/config/load.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package config

import (
	"bytes"
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// Load reads, validates, and ${ENV}-expands a portal config YAML.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}

	// Expand ${ENV_VAR} in the app_token line only — identify by YAML key
	// on the line to avoid touching e.g. SoQL $where clauses.
	data = expandAppTokenEnv(data)

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse %q: %w", path, err)
	}

	applyDefaults(&cfg)
	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validate %q: %w", path, err)
	}
	return &cfg, nil
}

var appTokenEnvPattern = regexp.MustCompile(`(?m)^(\s*app_token\s*:\s*)\$\{([A-Z0-9_]+)\}\s*$`)

func expandAppTokenEnv(data []byte) []byte {
	return appTokenEnvPattern.ReplaceAllFunc(data, func(match []byte) []byte {
		sub := appTokenEnvPattern.FindSubmatch(match)
		prefix := sub[1]
		envVar := string(sub[2])
		return append(append([]byte{}, prefix...), []byte(os.Getenv(envVar))...)
	})
}

func applyDefaults(cfg *Config) {
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 4
	}
	if cfg.OnError == "" {
		cfg.OnError = "continue"
	}
	if cfg.DB == "" && cfg.Portal != "" {
		cfg.DB = cfg.Portal + ".duckdb"
	}
	if cfg.Defaults.BatchSize == 0 {
		cfg.Defaults.BatchSize = 5000
	}
	if cfg.Defaults.OrderBy == "" {
		cfg.Defaults.OrderBy = ":id"
	}
}

func validate(cfg *Config) error {
	if cfg.Portal == "" {
		return fmt.Errorf("portal: required")
	}
	if cfg.OnError != "continue" && cfg.OnError != "abort" {
		return fmt.Errorf("on_error: must be 'continue' or 'abort', got %q", cfg.OnError)
	}
	if cfg.Concurrency < 1 {
		return fmt.Errorf("concurrency: must be >= 1, got %d", cfg.Concurrency)
	}
	if len(cfg.Include) == 0 {
		return fmt.Errorf("include: at least one selector required")
	}
	for i, s := range cfg.Include {
		if err := s.validate(); err != nil {
			return fmt.Errorf("include[%d]: %w", i, err)
		}
	}
	for i, s := range cfg.Exclude {
		if err := s.validate(); err != nil {
			return fmt.Errorf("exclude[%d]: %w", i, err)
		}
	}
	return nil
}

func (s Selector) validate() error {
	n := 0
	if s.ID != "" {
		n++
	}
	if s.Name != "" {
		n++
	}
	if s.Category != "" {
		n++
	}
	if s.Tag != "" {
		n++
	}
	if n != 1 {
		return fmt.Errorf("exactly one of id, name, category, tag must be set (got %d)", n)
	}
	return nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/config/ -v`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add internal/config/
git commit -m "config: add YAML loader with validation and \${ENV} expansion"
```

---

## Task 5: Effective per-dataset config merging

Given a dataset id, produce the merged (built-in → defaults → override) `Effective` config that the write strategy uses.

**Files:**
- Create: `internal/config/effective.go`
- Create: `internal/config/effective_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/config/effective_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package config

import (
	"strings"
	"testing"
)

func TestEffectiveFor_OverrideWins(t *testing.T) {
	cfg := &Config{
		Defaults: Defaults{BatchSize: 5000, OrderBy: ":id"},
		Overrides: map[string]Override{
			"aaaa-bbbb": {
				Table:     "foo",
				Where:     "id > 0",
				OrderBy:   ":updated_at",
				BatchSize: 10000,
				Limit:     100,
				Columns:   Columns{Skip: []string{"big_col"}},
			},
		},
	}
	eff := cfg.EffectiveFor("aaaa-bbbb")
	if eff.Table != "foo" {
		t.Errorf("table: got %q", eff.Table)
	}
	if eff.Where != "id > 0" {
		t.Errorf("where: got %q", eff.Where)
	}
	if eff.OrderBy != ":updated_at" {
		t.Errorf("order_by: got %q", eff.OrderBy)
	}
	if eff.BatchSize != 10000 {
		t.Errorf("batch: got %d", eff.BatchSize)
	}
	if eff.Limit != 100 {
		t.Errorf("limit: got %d", eff.Limit)
	}
	if len(eff.SkipColumns) != 1 || eff.SkipColumns[0] != "big_col" {
		t.Errorf("skip: got %v", eff.SkipColumns)
	}
}

func TestEffectiveFor_NoOverride(t *testing.T) {
	cfg := &Config{
		Defaults: Defaults{BatchSize: 5000, OrderBy: ":id"},
	}
	eff := cfg.EffectiveFor("cccc-dddd")
	if eff.Table != "cccc_dddd" {
		t.Errorf("default table: got %q, want cccc_dddd", eff.Table)
	}
	if eff.OrderBy != ":id" {
		t.Errorf("order_by: got %q", eff.OrderBy)
	}
	if eff.BatchSize != 5000 {
		t.Errorf("batch: got %d", eff.BatchSize)
	}
}

func TestEffectiveFor_Hash_Deterministic(t *testing.T) {
	cfg := &Config{
		Defaults:  Defaults{BatchSize: 5000, OrderBy: ":id"},
		Overrides: map[string]Override{"a-a": {Where: "x=1"}},
	}
	h1 := cfg.EffectiveFor("a-a").Hash()
	h2 := cfg.EffectiveFor("a-a").Hash()
	if h1 != h2 {
		t.Errorf("hash not deterministic: %s vs %s", h1, h2)
	}
	if !strings.HasPrefix(h1, "sha256:") {
		t.Errorf("hash format: got %q", h1)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestEffectiveFor -v`
Expected: FAIL — `EffectiveFor` / `Effective` undefined.

- [ ] **Step 3: Write effective.go**

Create `internal/config/effective.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// Effective is the fully-merged per-dataset configuration.
type Effective struct {
	DatasetID   string
	Table       string
	Where       string
	OrderBy     string
	BatchSize   int
	Limit       int
	SkipColumns []string
}

// EffectiveFor merges built-in defaults, cfg.Defaults, and cfg.Overrides[id].
func (c *Config) EffectiveFor(id string) Effective {
	eff := Effective{
		DatasetID: id,
		Table:     strings.ReplaceAll(id, "-", "_"),
		OrderBy:   c.Defaults.OrderBy,
		BatchSize: c.Defaults.BatchSize,
	}

	ov, ok := c.Overrides[id]
	if !ok {
		return eff
	}
	if ov.Table != "" {
		eff.Table = ov.Table
	}
	if ov.Where != "" {
		eff.Where = ov.Where
	}
	if ov.OrderBy != "" {
		eff.OrderBy = ov.OrderBy
	}
	if ov.BatchSize != 0 {
		eff.BatchSize = ov.BatchSize
	}
	if ov.Limit != 0 {
		eff.Limit = ov.Limit
	}
	if len(ov.Columns.Skip) > 0 {
		eff.SkipColumns = append([]string(nil), ov.Columns.Skip...)
	}
	return eff
}

// Hash returns a sha256 hex digest of the effective config, for drift detection
// in _csq.sync_runs.config_hash.
func (e Effective) Hash() string {
	canonical := struct {
		Table       string   `json:"table"`
		Where       string   `json:"where"`
		OrderBy     string   `json:"order_by"`
		BatchSize   int      `json:"batch_size"`
		Limit       int      `json:"limit"`
		SkipColumns []string `json:"skip_columns"`
	}{e.Table, e.Where, e.OrderBy, e.BatchSize, e.Limit, e.SkipColumns}
	b, _ := json.Marshal(canonical)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/ -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/config/effective.go internal/config/effective_test.go
git commit -m "config: add effective per-dataset config merge + hash"
```

---

## Task 6: Selector resolver

Expand `include`/`exclude` selectors against a catalog listing into `[]DatasetTarget`.

**Files:**
- Create: `internal/sync/types.go`
- Create: `internal/sync/selector.go`
- Create: `internal/sync/selector_test.go`

- [ ] **Step 1: Write types.go**

Create `internal/sync/types.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/config"
)

// DatasetTarget is a single dataset the orchestrator will sync.
type DatasetTarget struct {
	ID        string
	Name      string
	Effective config.Effective
}

// DatasetResult is the outcome of one dataset sync.
type DatasetResult struct {
	Target      DatasetTarget
	Status      string // "ok" | "failed" | "aborted"
	RowsWritten int64
	Err         error
	StartedAt   time.Time
	FinishedAt  time.Time
}
```

- [ ] **Step 2: Write the failing test**

Create `internal/sync/selector_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"sort"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func sampleCatalog() []socrata.CatalogEntry {
	return []socrata.CatalogEntry{
		{ID: "aaaa-0001", Name: "Crimes 2020", Category: "Public Safety", Tags: []string{"crime", "311"}},
		{ID: "aaaa-0002", Name: "Crimes 2021", Category: "Public Safety", Tags: []string{"crime"}},
		{ID: "bbbb-0003", Name: "Park Events", Category: "Parks", Tags: []string{"events"}},
		{ID: "cccc-0004", Name: "Building Permits", Category: "Buildings", Tags: []string{"permits"}},
		{ID: "dddd-0005", Name: "311 Archive 2015", Category: "Public Safety", Tags: []string{"311", "archive"}},
	}
}

func ids(targets []DatasetTarget) []string {
	out := make([]string, len(targets))
	for i, t := range targets {
		out[i] = t.ID
	}
	sort.Strings(out)
	return out
}

func TestResolve_LiteralID(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{ID: "cccc-0004"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got := ids(out); len(got) != 1 || got[0] != "cccc-0004" {
		t.Errorf("got %v, want [cccc-0004]", got)
	}
}

func TestResolve_NameGlob(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Name: "Crimes*"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	got := ids(out)
	want := []string{"aaaa-0001", "aaaa-0002"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestResolve_CategoryGlob(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Category: "Public Safety"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(out) != 3 {
		t.Errorf("got %d, want 3", len(out))
	}
}

func TestResolve_TagGlob(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Tag: "311*"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	got := ids(out)
	if len(got) != 2 {
		t.Errorf("got %v, want 2 entries", got)
	}
}

func TestResolve_ExcludeAfterInclude(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{
		Include: []config.Selector{{Category: "Public Safety"}},
		Exclude: []config.Selector{{Name: "*Archive*"}},
	}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	got := ids(out)
	want := []string{"aaaa-0001", "aaaa-0002"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestResolve_Union_Dedup(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{
		Include: []config.Selector{
			{Category: "Public Safety"},
			{Tag: "crime"},
		},
	}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(out) != 3 {
		t.Errorf("got %d, want 3 (deduped)", len(out))
	}
}

func TestResolve_OnlyFilter(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Category: "Public Safety"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), []string{"aaaa-0001"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got := ids(out); len(got) != 1 || got[0] != "aaaa-0001" {
		t.Errorf("got %v, want [aaaa-0001]", got)
	}
}

func TestResolve_OnlyUnknown_Errors(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Category: "Public Safety"}}}
	_, err := r.Resolve(context.Background(), cfg, sampleCatalog(), []string{"zzzz-9999"})
	if err == nil {
		t.Fatal("want error for --only id not in resolved set")
	}
}

func TestResolve_EmptyMatch_Errors(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Category: "Nonexistent"}}}
	_, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err == nil {
		t.Fatal("want error for empty match set")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/sync/ -run TestResolve -v`
Expected: FAIL — `DefaultSelectorResolver` undefined.

- [ ] **Step 4: Write selector.go**

Create `internal/sync/selector.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"fmt"
	"path"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// SelectorResolver expands YAML selectors against a catalog listing.
type SelectorResolver interface {
	Resolve(ctx context.Context, cfg *config.Config, catalog []socrata.CatalogEntry, only []string) ([]DatasetTarget, error)
}

// DefaultSelectorResolver is the Phase 1 implementation.
type DefaultSelectorResolver struct{}

func (r *DefaultSelectorResolver) Resolve(
	ctx context.Context, cfg *config.Config, catalog []socrata.CatalogEntry, only []string,
) ([]DatasetTarget, error) {
	byID := make(map[string]socrata.CatalogEntry, len(catalog))
	for _, e := range catalog {
		byID[e.ID] = e
	}

	included := make(map[string]struct{})
	for _, sel := range cfg.Include {
		for _, e := range catalog {
			if matchSelector(sel, e) {
				included[e.ID] = struct{}{}
			}
		}
	}
	for _, sel := range cfg.Exclude {
		for id := range included {
			if matchSelector(sel, byID[id]) {
				delete(included, id)
			}
		}
	}

	if len(only) > 0 {
		onlySet := make(map[string]struct{}, len(only))
		for _, id := range only {
			onlySet[id] = struct{}{}
			if _, ok := included[id]; !ok {
				return nil, fmt.Errorf("--only %s: not in resolved selector set", id)
			}
		}
		for id := range included {
			if _, ok := onlySet[id]; !ok {
				delete(included, id)
			}
		}
	}

	if len(included) == 0 {
		return nil, fmt.Errorf("no datasets matched the include selectors")
	}

	out := make([]DatasetTarget, 0, len(included))
	for id := range included {
		out = append(out, DatasetTarget{
			ID:        id,
			Name:      byID[id].Name,
			Effective: cfg.EffectiveFor(id),
		})
	}
	return out, nil
}

func matchSelector(sel config.Selector, e socrata.CatalogEntry) bool {
	switch {
	case sel.ID != "":
		return sel.ID == e.ID
	case sel.Name != "":
		return globMatch(sel.Name, e.Name)
	case sel.Category != "":
		return globMatch(sel.Category, e.Category)
	case sel.Tag != "":
		for _, t := range e.Tags {
			if globMatch(sel.Tag, t) {
				return true
			}
		}
		return false
	}
	return false
}

func globMatch(pattern, s string) bool {
	ok, err := path.Match(pattern, s)
	if err != nil {
		return false // malformed pattern — treat as non-match
	}
	return ok
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/sync/ -v`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add internal/sync/types.go internal/sync/selector.go internal/sync/selector_test.go
git commit -m "sync: add SelectorResolver with id/name/category/tag + exclude + --only"
```

---

## Task 7: Sync run store

Insert a `started` row, then update to `ok`/`failed`/`aborted` on finish.

**Files:**
- Create: `internal/duckdb/sync_runs.go`
- Create: `internal/duckdb/sync_runs_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/duckdb/sync_runs_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"
	"time"
)

func TestSyncRuns_StartAndFinish_OK(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	start := time.Now().UTC().Truncate(time.Second)
	id, err := w.StartSyncRun("run-1", "aaaa-bbbb", "foo", "cfghash", start)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if id == 0 {
		t.Error("want nonzero internal row id")
	}

	finish := start.Add(2 * time.Second)
	if err := w.FinishSyncRun(id, "ok", 42, "", finish); err != nil {
		t.Fatalf("finish: %v", err)
	}

	var status string
	var rows int64
	var durMs int64
	err = w.DB.QueryRow(
		`SELECT status, rows_written, duration_ms FROM _csq.sync_runs WHERE rowid = ?`, id,
	).Scan(&status, &rows, &durMs)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if status != "ok" || rows != 42 {
		t.Errorf("got status=%q rows=%d", status, rows)
	}
	if durMs != 2000 {
		t.Errorf("duration_ms: got %d, want 2000", durMs)
	}
}

func TestSyncRuns_Failed(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	now := time.Now().UTC()
	id, _ := w.StartSyncRun("run-2", "xxxx-yyyy", "foo", "h", now)
	if err := w.FinishSyncRun(id, "failed", 0, "boom: HTTP 500", now.Add(time.Second)); err != nil {
		t.Fatalf("finish: %v", err)
	}
	var errStr string
	_ = w.DB.QueryRow(`SELECT error FROM _csq.sync_runs WHERE rowid = ?`, id).Scan(&errStr)
	if errStr != "boom: HTTP 500" {
		t.Errorf("error: got %q", errStr)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/duckdb/ -run TestSyncRuns -v`
Expected: FAIL — `StartSyncRun` / `FinishSyncRun` undefined.

- [ ] **Step 3: Write sync_runs.go**

Create `internal/duckdb/sync_runs.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"database/sql"
	"fmt"
	"time"
)

// StartSyncRun inserts a row with finished_at=NULL and status='running'.
// Returns the DuckDB rowid of the inserted row so it can be updated on finish.
func (w *Writer) StartSyncRun(runID, datasetID, tableName, configHash string, startedAt time.Time) (int64, error) {
	_, err := w.DB.Exec(
		`INSERT INTO _csq.sync_runs (run_id, dataset_id, table_name, started_at, status, config_hash)
		 VALUES ($1, $2, $3, $4, 'running', $5)`,
		runID, datasetID, tableName, startedAt, configHash,
	)
	if err != nil {
		return 0, fmt.Errorf("insert sync_run start: %w", err)
	}
	var rowid int64
	err = w.DB.QueryRow(
		`SELECT rowid FROM _csq.sync_runs WHERE run_id = $1 AND dataset_id = $2 AND started_at = $3
		 ORDER BY rowid DESC LIMIT 1`,
		runID, datasetID, startedAt,
	).Scan(&rowid)
	if err != nil {
		return 0, fmt.Errorf("fetch sync_run rowid: %w", err)
	}
	return rowid, nil
}

// FinishSyncRun updates the row to a terminal status with timing and row count.
// errMsg is empty for ok.
func (w *Writer) FinishSyncRun(rowid int64, status string, rowsWritten int64, errMsg string, finishedAt time.Time) error {
	var startedAt time.Time
	if err := w.DB.QueryRow(
		`SELECT started_at FROM _csq.sync_runs WHERE rowid = $1`, rowid,
	).Scan(&startedAt); err != nil {
		return fmt.Errorf("lookup started_at: %w", err)
	}
	durMs := finishedAt.Sub(startedAt).Milliseconds()

	var rowsArg any
	if status == "ok" {
		rowsArg = rowsWritten
	} else {
		rowsArg = nil
	}
	var errArg any
	if errMsg != "" {
		errArg = errMsg
	} else {
		errArg = nil
	}

	_, err := w.DB.Exec(
		`UPDATE _csq.sync_runs
		 SET finished_at = $1, status = $2, rows_written = $3, error = $4, duration_ms = $5
		 WHERE rowid = $6`,
		finishedAt, status, rowsArg, errArg, durMs, rowid,
	)
	if err != nil {
		return fmt.Errorf("update sync_run finish: %w", err)
	}
	return nil
}

// IncompleteSyncRunCount returns the number of sync_runs rows with finished_at IS NULL.
// Used on startup to log "N prior runs appear incomplete".
func (w *Writer) IncompleteSyncRunCount() (int, error) {
	var n int
	err := w.DB.QueryRow(`SELECT COUNT(*) FROM _csq.sync_runs WHERE finished_at IS NULL`).Scan(&n)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return n, err
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/duckdb/ -run TestSyncRuns -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/duckdb/sync_runs.go internal/duckdb/sync_runs_test.go
git commit -m "duckdb: add sync_runs Start/Finish + incomplete-count"
```

---

## Task 8: Staging table swap

Encapsulate the three-statement swap: `DROP main.<table> | RENAME staging.<table>_<runid> → staging.<table> | SET SCHEMA main`, all in one transaction.

**Files:**
- Create: `internal/duckdb/swap.go`
- Create: `internal/duckdb/swap_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/duckdb/swap_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"
)

func TestSwapIn_ReplacesExistingTable(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	// Pre-existing "foo" in main with old data
	if _, err := w.DB.Exec(`CREATE TABLE main.foo (v INT)`); err != nil {
		t.Fatalf("seed old: %v", err)
	}
	if _, err := w.DB.Exec(`INSERT INTO main.foo VALUES (1), (2)`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	// Staging table with new data
	if _, err := w.DB.Exec(`CREATE TABLE _csq_staging.foo_run1 (v INT)`); err != nil {
		t.Fatalf("staging: %v", err)
	}
	if _, err := w.DB.Exec(`INSERT INTO _csq_staging.foo_run1 VALUES (100), (200), (300)`); err != nil {
		t.Fatalf("staging rows: %v", err)
	}

	if err := w.SwapIn("foo_run1", "foo"); err != nil {
		t.Fatalf("swap: %v", err)
	}

	var n int
	if err := w.DB.QueryRow(`SELECT COUNT(*) FROM main.foo`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Errorf("main.foo rowcount: got %d, want 3", n)
	}
	// Staging should be empty of that table name
	var ns int
	_ = w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '_csq_staging' AND table_name = 'foo_run1'`,
	).Scan(&ns)
	if ns != 0 {
		t.Errorf("_csq_staging.foo_run1 should be gone; got %d", ns)
	}
}

func TestSwapIn_CreatesNewTable(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	if _, err := w.DB.Exec(`CREATE TABLE _csq_staging.bar_runX (v INT)`); err != nil {
		t.Fatalf("staging: %v", err)
	}
	if err := w.SwapIn("bar_runX", "bar"); err != nil {
		t.Fatalf("swap: %v", err)
	}
	var n int
	_ = w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'bar'`,
	).Scan(&n)
	if n != 1 {
		t.Errorf("main.bar not created")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/duckdb/ -run TestSwapIn -v`
Expected: FAIL — `SwapIn` undefined.

- [ ] **Step 3: Write swap.go**

Create `internal/duckdb/swap.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package duckdb

import "fmt"

// SwapIn replaces main.<target> with the contents of _csq_staging.<stagingName>,
// in a single transaction. The staging table is renamed to <target> within the
// staging schema, then moved to main. On success, the prior main.<target> is
// gone and the staging slot is empty.
func (w *Writer) SwapIn(stagingName, target string) error {
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin swap tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS main."%s"`, target)); err != nil {
		return fmt.Errorf("drop main.%s: %w", target, err)
	}
	if _, err := tx.Exec(fmt.Sprintf(
		`ALTER TABLE _csq_staging."%s" RENAME TO "%s"`, stagingName, target)); err != nil {
		return fmt.Errorf("rename staging.%s → %s: %w", stagingName, target, err)
	}
	if _, err := tx.Exec(fmt.Sprintf(
		`ALTER TABLE _csq_staging."%s" SET SCHEMA main`, target)); err != nil {
		return fmt.Errorf("move staging.%s → main: %w", target, err)
	}
	return tx.Commit()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/duckdb/ -run TestSwapIn -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/duckdb/swap.go internal/duckdb/swap_test.go
git commit -m "duckdb: add transactional staging → main swap"
```

---

## Task 9: Progress reporter

Event interface, stderr impl, recording impl for tests.

**Files:**
- Create: `internal/sync/progress.go`
- Create: `internal/sync/progress_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/sync/progress_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestStderrReporter_Writes(t *testing.T) {
	var buf bytes.Buffer
	r := &StderrReporter{Out: &buf}
	target := DatasetTarget{ID: "aaaa-0001", Name: "Crimes"}
	r.DatasetStart(1, 1, target)
	r.DatasetProgress(1, 1, target, 123)
	r.DatasetDone(1, 1, target, DatasetResult{
		Target: target, Status: "ok", RowsWritten: 123,
		StartedAt: time.Now().Add(-time.Second), FinishedAt: time.Now(),
	})

	s := buf.String()
	for _, want := range []string{"aaaa-0001", "Crimes", "starting", "done"} {
		if !strings.Contains(s, want) {
			t.Errorf("output missing %q:\n%s", want, s)
		}
	}
}

func TestRecordingReporter_Records(t *testing.T) {
	r := &RecordingReporter{}
	target := DatasetTarget{ID: "aaaa-0001"}
	r.DatasetStart(1, 2, target)
	r.DatasetDone(1, 2, target, DatasetResult{Target: target, Status: "ok"})
	if len(r.Events) != 2 {
		t.Fatalf("events: got %d, want 2", len(r.Events))
	}
	if r.Events[0].Kind != "start" || r.Events[1].Kind != "done" {
		t.Errorf("kinds: got %v", r.Events)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/ -run TestStderrReporter -v`
Expected: FAIL — reporter types undefined.

- [ ] **Step 3: Write progress.go**

Create `internal/sync/progress.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// ProgressReporter receives lifecycle events for the sync orchestrator.
// Implementations must be safe for concurrent calls from worker goroutines.
type ProgressReporter interface {
	DatasetStart(idx, total int, t DatasetTarget)
	DatasetProgress(idx, total int, t DatasetTarget, rowsSoFar int64)
	DatasetDone(idx, total int, t DatasetTarget, res DatasetResult)
}

// StderrReporter writes plain-text progress lines to Out (default os.Stderr).
type StderrReporter struct {
	Out io.Writer
	mu  sync.Mutex
}

func (r *StderrReporter) line(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Fprintf(r.Out, format+"\n", args...)
}

func (r *StderrReporter) DatasetStart(idx, total int, t DatasetTarget) {
	r.line("[csq] [%d/%d]  %s  %-30s  starting", idx, total, t.ID, t.Effective.Table)
}

func (r *StderrReporter) DatasetProgress(idx, total int, t DatasetTarget, rowsSoFar int64) {
	r.line("[csq] [%d/%d]  %s  %-30s  %d rows", idx, total, t.ID, t.Effective.Table, rowsSoFar)
}

func (r *StderrReporter) DatasetDone(idx, total int, t DatasetTarget, res DatasetResult) {
	dur := res.FinishedAt.Sub(res.StartedAt).Round(time.Millisecond)
	if res.Status == "ok" {
		r.line("[csq] [%d/%d]  %s  %-30s  done: %d rows in %s",
			idx, total, t.ID, t.Effective.Table, res.RowsWritten, dur)
		return
	}
	msg := "(no error)"
	if res.Err != nil {
		msg = res.Err.Error()
	}
	r.line("[csq] [%d/%d]  %s  %-30s  %s: %s",
		idx, total, t.ID, t.Effective.Table, upperStatus(res.Status), msg)
}

func upperStatus(s string) string {
	switch s {
	case "failed":
		return "FAILED"
	case "aborted":
		return "ABORTED"
	default:
		return s
	}
}

// RecordingReporter captures events for assertions in tests.
type RecordingReporter struct {
	mu     sync.Mutex
	Events []ReporterEvent
}

type ReporterEvent struct {
	Kind   string // "start" | "progress" | "done"
	Target DatasetTarget
	Rows   int64
	Result DatasetResult
}

func (r *RecordingReporter) DatasetStart(idx, total int, t DatasetTarget) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, ReporterEvent{Kind: "start", Target: t})
}

func (r *RecordingReporter) DatasetProgress(idx, total int, t DatasetTarget, rowsSoFar int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, ReporterEvent{Kind: "progress", Target: t, Rows: rowsSoFar})
}

func (r *RecordingReporter) DatasetDone(idx, total int, t DatasetTarget, res DatasetResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, ReporterEvent{Kind: "done", Target: t, Result: res})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/sync/ -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/sync/progress.go internal/sync/progress_test.go
git commit -m "sync: add ProgressReporter with stderr and recording impls"
```

---

## Task 10: Full-replace write strategy

For one dataset: fetch metadata → build schema honoring `SkipColumns` → create staging table → stream rows → swap. Returns a `DatasetResult`.

**Files:**
- Create: `internal/sync/strategy.go`
- Create: `internal/sync/fakesocrata_test.go`
- Create: `internal/sync/strategy_test.go`

- [ ] **Step 1: Write the fake Socrata server helper**

Create `internal/sync/fakesocrata_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// fakeDataset is one dataset the fake server will serve.
type fakeDataset struct {
	ID      string
	Name    string
	Columns []map[string]string // each has fieldName + dataTypeName
	Rows    []map[string]any
	// FailAtOffset: if > 0, return 500 when $offset >= FailAtOffset
	FailAtOffset int
}

func newFakeSocrata(t *testing.T, datasets ...fakeDataset) *httptest.Server {
	t.Helper()
	byID := map[string]fakeDataset{}
	for _, d := range datasets {
		byID[d.ID] = d
	}
	mux := http.NewServeMux()

	// /api/catalog/v1
	mux.HandleFunc("/api/catalog/v1", func(w http.ResponseWriter, r *http.Request) {
		results := make([]map[string]any, 0, len(datasets))
		for _, d := range datasets {
			results = append(results, map[string]any{
				"resource": map[string]any{
					"id":            d.ID,
					"name":          d.Name,
					"description":   "",
					"rowsUpdatedAt": "2024-01-15T00:00:00.000",
				},
				"classification": map[string]any{
					"domain_category": "Test",
					"domain_tags":     []string{"test"},
				},
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results":       results,
			"resultSetSize": len(datasets),
		})
	})

	// /api/views/{id}.json
	mux.HandleFunc("/api/views/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/views/"), ".json")
		d, ok := byID[id]
		if !ok {
			http.Error(w, "not found", 404)
			return
		}
		cols := make([]map[string]string, 0, len(d.Columns))
		for _, c := range d.Columns {
			cols = append(cols, c)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": d.ID, "name": d.Name, "columns": cols,
		})
	})

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
		if d.FailAtOffset > 0 && offset >= d.FailAtOffset {
			http.Error(w, "synthetic failure", 500)
			return
		}
		end := offset + limit
		if end > len(d.Rows) {
			end = len(d.Rows)
		}
		if offset > len(d.Rows) {
			offset = len(d.Rows)
		}
		_ = json.NewEncoder(w).Encode(d.Rows[offset:end])
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// fakeHost returns the host:port of an httptest.Server (strips "http://").
func fakeHost(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

// makeRows is a small helper to generate n rows of the given shape.
func makeRows(n int, mk func(i int) map[string]any) []map[string]any {
	out := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		out[i] = mk(i)
	}
	return out
}

var _ = fmt.Sprintf // silence unused import if future tests drop fmt
```

- [ ] **Step 2: Write the failing strategy test**

Create `internal/sync/strategy_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestFullReplaceStrategy_HappyPath(t *testing.T) {
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Crimes",
		Columns: []map[string]string{
			{"fieldName": "id", "dataTypeName": "text"},
			{"fieldName": "score", "dataTypeName": "number"},
		},
		Rows: makeRows(7, func(i int) map[string]any {
			return map[string]any{"id": "r" + itoa(i), "score": float64(i)}
		}),
	}
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 3}

	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	strat := &FullReplaceStrategy{
		Portal: fakeHost(srv), Scheme: "http", RunID: "run1",
	}
	target := DatasetTarget{
		ID: "aaaa-0001", Name: "Crimes",
		Effective: config.Effective{
			DatasetID: "aaaa-0001", Table: "crimes",
			OrderBy: ":id", BatchSize: 3,
		},
	}

	res, err := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if res.Status != "ok" {
		t.Errorf("status: got %q", res.Status)
	}
	if res.RowsWritten != 7 {
		t.Errorf("rows: got %d, want 7", res.RowsWritten)
	}

	var n int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM main.crimes`).Scan(&n)
	if n != 7 {
		t.Errorf("main.crimes rowcount: got %d, want 7", n)
	}
}

func TestFullReplaceStrategy_FailureLeavesPriorTableIntact(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	// Prior successful sync: seed main.crimes with 100 rows
	if _, err := w.DB.Exec(`CREATE TABLE main.crimes (id VARCHAR, score DOUBLE)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < 100; i++ {
		if _, err := w.DB.Exec(`INSERT INTO main.crimes VALUES (?, ?)`, "prior"+itoa(i), float64(i)); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
	}

	// New run fails mid-stream
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Crimes",
		Columns: []map[string]string{
			{"fieldName": "id", "dataTypeName": "text"},
			{"fieldName": "score", "dataTypeName": "number"},
		},
		Rows:         makeRows(20, func(i int) map[string]any { return map[string]any{"id": "new" + itoa(i), "score": float64(i)} }),
		FailAtOffset: 5,
	}
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 5, MaxRetries: 1}
	strat := &FullReplaceStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run2"}
	target := DatasetTarget{
		ID: "aaaa-0001", Name: "Crimes",
		Effective: config.Effective{DatasetID: "aaaa-0001", Table: "crimes", OrderBy: ":id", BatchSize: 5},
	}

	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "failed" {
		t.Errorf("status: got %q, want failed", res.Status)
	}
	var n int
	var firstID string
	_ = w.DB.QueryRow(`SELECT COUNT(*), MIN(id) FROM main.crimes`).Scan(&n, &firstID)
	if n != 100 {
		t.Errorf("main.crimes rowcount: got %d, want 100 (prior data preserved)", n)
	}
	if firstID == "new0" {
		t.Error("main.crimes was overwritten by failed run")
	}
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = digits[i%10]
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/sync/ -run TestFullReplaceStrategy -v`
Expected: FAIL — `FullReplaceStrategy` undefined.

- [ ] **Step 4a: Add schema-qualified SQL helpers to `internal/duckdb/schema.go`**

The strategy writes into `_csq_staging.<name>` and the existing `TableSchema.CreateTableSQL` / `InsertSQL` only target the default schema. Add qualified variants.

Edit `internal/duckdb/schema.go`. Below the existing `InsertSQL` method, add:

```go
// CreateTableSQLIn returns a CREATE TABLE statement targeting "<schemaName>"."<table>".
func (s TableSchema) CreateTableSQLIn(schemaName string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `CREATE TABLE IF NOT EXISTS "%s"."%s" (`, schemaName, s.Table)
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `"%s" %s`, c.Name, c.Type)
	}
	b.WriteString(")")
	return b.String()
}

// InsertSQLIn returns an INSERT INTO "<schemaName>"."<table>" with positional placeholders.
func (s TableSchema) InsertSQLIn(schemaName string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `INSERT INTO "%s"."%s" (`, schemaName, s.Table)
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `"%s"`, c.Name)
	}
	b.WriteString(") VALUES (")
	for i := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "$%d", i+1)
	}
	b.WriteString(")")
	return b.String()
}
```

Then add a schema-qualified variant of `InsertRows` to `internal/duckdb/writer.go`. Below the existing `InsertRows` method, add:

```go
// InsertRowsInto inserts rows into "<schemaName>"."<ts.Table>".
func (w *Writer) InsertRowsInto(schemaName string, ts TableSchema, rows []socrata.Row) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := w.DB.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(ts.InsertSQLIn(schemaName))
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
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
			return fmt.Errorf("insert row %d: %w", rowIdx, err)
		}
	}
	return tx.Commit()
}
```

- [ ] **Step 4b: Write strategy.go**

Create `internal/sync/strategy.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// WriteStrategy owns how one dataset's rows land in DuckDB.
type WriteStrategy interface {
	Sync(
		ctx context.Context,
		target DatasetTarget,
		client *socrata.Client,
		w *duckdb.Writer,
		prog ProgressReporter,
		idx, total int,
	) (DatasetResult, error)
}

// FullReplaceStrategy writes into _csq_staging.<table>_<runID>, then swaps
// the staging table into place as main.<table>.
// Scheme defaults to "https" when empty (tests override with "http").
type FullReplaceStrategy struct {
	Portal string
	Scheme string
	RunID  string
}

func (s *FullReplaceStrategy) scheme() string {
	if s.Scheme != "" {
		return s.Scheme
	}
	return "https"
}

func (s *FullReplaceStrategy) Sync(
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

	// 1. Fetch metadata from the scheme we were configured with.
	meta, err := fetchMetadata(ctx, client, s.scheme(), s.Portal, target.ID)
	if err != nil {
		return fail(result, "failed", fmt.Errorf("fetch metadata: %w", err)), nil
	}

	// 2. Filter columns honoring SkipColumns.
	cols := filterColumns(meta.Columns, target.Effective.SkipColumns)

	// 3. Build schema keyed to the staging table name.
	stagingName := target.Effective.Table + "_" + s.RunID
	schema := duckdb.BuildSchema(stagingName, cols)

	// 4. Create the staging table in _csq_staging.
	if _, err := w.DB.ExecContext(ctx, schema.CreateTableSQLIn("_csq_staging")); err != nil {
		return fail(result, "failed", fmt.Errorf("create staging: %w", err)), nil
	}

	// 5. Stream rows, inserting into _csq_staging.<stagingName>.
	var rowsWritten int64
	err = streamInto(ctx, client, s.scheme(), s.Portal, target, w, schema, &rowsWritten, prog, idx, total)
	if err != nil {
		if ctx.Err() != nil {
			return fail(result, "aborted", ctx.Err()), nil
		}
		return fail(result, "failed", err), nil
	}

	// 6. Swap into main.
	if err := w.SwapIn(stagingName, target.Effective.Table); err != nil {
		return fail(result, "failed", fmt.Errorf("swap: %w", err)), nil
	}

	result.Status = "ok"
	result.RowsWritten = rowsWritten
	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func fail(res DatasetResult, status string, err error) DatasetResult {
	res.Status = status
	res.Err = err
	res.FinishedAt = time.Now().UTC()
	return res
}

func fetchMetadata(ctx context.Context, c *socrata.Client, scheme, portal, id string) (*socrata.DatasetMetadata, error) {
	// Socrata client currently has a host-only FetchMetadata. For tests we need
	// http scheme, so build the URL inline and reuse its HTTP client.
	u := &url.URL{Scheme: scheme, Host: portal, Path: "/api/views/" + id + ".json"}
	return c.FetchMetadataURL(ctx, u.String())
}

func filterColumns(cols []socrata.Column, skip []string) []socrata.Column {
	if len(skip) == 0 {
		return cols
	}
	skipSet := make(map[string]struct{}, len(skip))
	for _, s := range skip {
		skipSet[s] = struct{}{}
	}
	out := cols[:0:0]
	for _, c := range cols {
		if _, drop := skipSet[c.FieldName]; drop {
			continue
		}
		out = append(out, c)
	}
	return out
}

func streamInto(
	ctx context.Context,
	client *socrata.Client,
	scheme, portal string,
	target DatasetTarget,
	w *duckdb.Writer,
	schema duckdb.TableSchema,
	rowsWritten *int64,
	prog ProgressReporter,
	idx, total int,
) error {
	return client.StreamRowsCtx(ctx, scheme, portal, target.ID,
		target.Effective.OrderBy, target.Effective.Where, target.Effective.Limit,
		func(page []socrata.Row) error {
			if err := w.InsertRowsInto("_csq_staging", schema, page); err != nil {
				return err
			}
			*rowsWritten += int64(len(page))
			prog.DatasetProgress(idx, total, target, *rowsWritten)
			return nil
		},
	)
}
```

- [ ] **Step 4c: Add `FetchMetadataURL` and `StreamRowsCtx` in `internal/socrata`**

`internal/sync/strategy.go` references two helpers on `*socrata.Client` that don't exist yet. Add them.

Create `internal/socrata/ext.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package socrata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// FetchMetadataURL is like FetchMetadata but takes a full URL. Used by callers
// that need to control the scheme (e.g. httptest).
func (c *Client) FetchMetadataURL(ctx context.Context, fullURL string) (*DatasetMetadata, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build metadata request: %w", err)
	}
	if c.AppToken != "" {
		req.Header.Set("X-App-Token", c.AppToken)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("metadata request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("metadata HTTP %d: %s", resp.StatusCode, string(body))
	}
	var md DatasetMetadata
	if err := json.NewDecoder(resp.Body).Decode(&md); err != nil {
		return nil, fmt.Errorf("decode metadata: %w", err)
	}
	return &md, nil
}

// StreamRowsCtx is a context-aware, scheme-parameterised version of StreamRows.
// Cancellation via ctx aborts between pages.
func (c *Client) StreamRowsCtx(
	ctx context.Context,
	scheme, portal, datasetID, orderBy, whereClause string,
	limit int,
	handler PageHandler,
) error {
	base := &url.URL{Scheme: scheme, Host: portal, Path: "/resource/" + datasetID + ".json"}

	fetched := 0
	offset := 0
	batch := c.batchSize()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		remaining := batch
		if limit > 0 && limit-fetched < batch {
			remaining = limit - fetched
		}
		if remaining <= 0 {
			return nil
		}

		q := url.Values{}
		q.Set("$limit", strconv.Itoa(remaining))
		q.Set("$offset", strconv.Itoa(offset))
		if orderBy != "" {
			q.Set("$order", orderBy)
		}
		if whereClause != "" {
			q.Set("$where", whereClause)
		}
		base.RawQuery = q.Encode()

		page, err := c.getPage(base.String())
		if err != nil {
			return err
		}
		if len(page) > 0 {
			if err := handler(page); err != nil {
				return err
			}
		}
		fetched += len(page)
		offset += len(page)
		if len(page) < remaining {
			return nil
		}
		if limit > 0 && fetched >= limit {
			return nil
		}
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/sync/ -run TestFullReplaceStrategy -v`
Expected: all pass.

- [ ] **Step 6: Run the full test suite for regressions**

Run: `go test ./...`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add internal/sync/strategy.go internal/sync/strategy_test.go internal/sync/fakesocrata_test.go internal/socrata/ext.go internal/duckdb/schema.go internal/duckdb/writer.go
git commit -m "sync: add FullReplaceStrategy with staging + swap"
```

---

## Task 11: Sync orchestrator (`Run`)

Wires config → catalog fetch/read → resolve → worker pool → `sync_runs` accounting → summary.

**Files:**
- Create: `internal/sync/run.go`
- Create: `internal/sync/run_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/sync/run_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func mkDataset(id string, rows int, failAt int) fakeDataset {
	return fakeDataset{
		ID: id, Name: "Ds " + id,
		Columns: []map[string]string{
			{"fieldName": "id", "dataTypeName": "text"},
			{"fieldName": "score", "dataTypeName": "number"},
		},
		Rows: makeRows(rows, func(i int) map[string]any {
			return map[string]any{"id": id + "-" + itoa(i), "score": float64(i)}
		}),
		FailAtOffset: failAt,
	}
}

func baseCfg(portal string) *config.Config {
	cfg := &config.Config{
		Portal:      portal,
		Concurrency: 2,
		OnError:     "continue",
		Defaults:    config.Defaults{BatchSize: 5, OrderBy: ":id"},
		Include:     []config.Selector{{Category: "Test"}},
	}
	return cfg
}

func TestRun_AllSucceed(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 3, 0), mkDataset("aaaa-0002", 7, 0))
	cfg := baseCfg(fakeHost(srv))

	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	client := &socrata.Client{BatchSize: 5}
	summary, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if summary.OK != 2 || summary.Failed != 0 {
		t.Errorf("summary: %+v", summary)
	}
}

func TestRun_OnErrorContinue_OneFails(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 3, 0), mkDataset("aaaa-0002", 20, 5))
	cfg := baseCfg(fakeHost(srv))

	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	client := &socrata.Client{BatchSize: 5, MaxRetries: 1}
	summary, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
	})
	if err == nil {
		t.Fatal("expected non-nil err indicating at least one failure")
	}
	if summary.OK != 1 || summary.Failed != 1 {
		t.Errorf("summary: %+v", summary)
	}

	// _csq.sync_runs has two rows: 1 ok, 1 failed
	var ok, failed int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM _csq.sync_runs WHERE status = 'ok'`).Scan(&ok)
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM _csq.sync_runs WHERE status = 'failed'`).Scan(&failed)
	if ok != 1 || failed != 1 {
		t.Errorf("sync_runs: ok=%d failed=%d", ok, failed)
	}
}

func TestRun_DryRun_NoWrites(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 3, 0))
	cfg := baseCfg(fakeHost(srv))

	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	client := &socrata.Client{BatchSize: 5}
	summary, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{}, DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if summary.Planned != 1 || summary.OK != 0 {
		t.Errorf("summary: %+v", summary)
	}
	var tables int
	_ = w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'`,
	).Scan(&tables)
	if tables != 0 {
		t.Errorf("main schema has %d tables after dry-run, want 0", tables)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sync/ -run TestRun -v`
Expected: FAIL — `Run` / `Deps` / `Summary` undefined.

- [ ] **Step 3: Write run.go**

Create `internal/sync/run.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// Deps are the collaborators the orchestrator needs. All fields are required
// except Resolver (defaults to DefaultSelectorResolver), Strategy (defaults to
// FullReplaceStrategy), and Reporter (defaults to StderrReporter to os.Stderr).
type Deps struct {
	DB            *duckdb.Writer
	Client        *socrata.Client
	Scheme        string // "http" for tests, "https" otherwise
	Resolver      SelectorResolver
	Strategy      WriteStrategy
	Reporter      ProgressReporter
	Only          []string // --only IDs
	DryRun        bool
	RefreshCatalog bool
}

// Summary is the end-of-run tally.
type Summary struct {
	RunID   string
	Planned int           // number resolved
	OK      int
	Failed  int
	Aborted int
	Wall    time.Duration
}

// Run executes the sync described by cfg. Returns a non-nil error iff any
// dataset failed (even under on_error=continue), so callers can map to exit 1.
func Run(ctx context.Context, cfg *config.Config, d Deps) (Summary, error) {
	started := time.Now()
	runID := newRunID()
	sum := Summary{RunID: runID}

	if d.Resolver == nil {
		d.Resolver = &DefaultSelectorResolver{}
	}
	scheme := d.Scheme
	if scheme == "" {
		scheme = "https"
	}
	if d.Strategy == nil {
		d.Strategy = &FullReplaceStrategy{Portal: cfg.Portal, Scheme: scheme, RunID: runID}
	}

	// Catalog: refresh if asked or cache empty.
	catalog, err := d.DB.ReadCatalog()
	if err != nil {
		return sum, fmt.Errorf("read cached catalog: %w", err)
	}
	if d.RefreshCatalog || len(catalog) == 0 {
		if scheme == "http" {
			catalog, err = d.Client.FetchCatalogScheme(cfg.Portal, scheme)
		} else {
			catalog, err = d.Client.FetchCatalog(cfg.Portal)
		}
		if err != nil {
			return sum, fmt.Errorf("fetch catalog: %w", err)
		}
		if err := d.DB.UpsertCatalog(catalog, time.Now().UTC()); err != nil {
			return sum, fmt.Errorf("upsert catalog: %w", err)
		}
	}

	targets, err := d.Resolver.Resolve(ctx, cfg, catalog, d.Only)
	if err != nil {
		return sum, err
	}
	sum.Planned = len(targets)

	if d.DryRun {
		sum.Wall = time.Since(started)
		return sum, nil
	}

	if d.Reporter == nil {
		d.Reporter = &StderrReporter{Out: stderrWriter{}}
	}

	total := len(targets)
	var ok, failed, aborted int64

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Concurrency)

	for i, t := range targets {
		i, t := i, t
		g.Go(func() error {
			if gctx.Err() != nil && cfg.OnError == "abort" {
				recordAborted(d.DB, runID, t, time.Now().UTC(), gctx.Err())
				atomic.AddInt64(&aborted, 1)
				return nil
			}
			res, _ := runOne(gctx, d, runID, t, i+1, total)
			switch res.Status {
			case "ok":
				atomic.AddInt64(&ok, 1)
			case "aborted":
				atomic.AddInt64(&aborted, 1)
			default:
				atomic.AddInt64(&failed, 1)
				if cfg.OnError == "abort" {
					return fmt.Errorf("abort on first failure: %w", res.Err)
				}
			}
			return nil
		})
	}
	_ = g.Wait()

	sum.OK = int(ok)
	sum.Failed = int(failed)
	sum.Aborted = int(aborted)
	sum.Wall = time.Since(started)

	if sum.Failed > 0 || sum.Aborted > 0 {
		return sum, errors.New("one or more datasets failed")
	}
	return sum, nil
}

func runOne(ctx context.Context, d Deps, runID string, t DatasetTarget, idx, total int) (DatasetResult, error) {
	started := time.Now().UTC()
	rowid, err := d.DB.StartSyncRun(runID, t.ID, t.Effective.Table, t.Effective.Hash(), started)
	if err != nil {
		return DatasetResult{Target: t, Status: "failed", Err: err, StartedAt: started, FinishedAt: time.Now().UTC()}, err
	}

	res, _ := d.Strategy.Sync(ctx, t, d.Client, d.DB, d.Reporter, idx, total)
	errStr := ""
	if res.Err != nil {
		errStr = res.Err.Error()
	}
	if ferr := d.DB.FinishSyncRun(rowid, res.Status, res.RowsWritten, errStr, time.Now().UTC()); ferr != nil {
		// Best-effort: log-shaped failure in the result err but don't overwrite primary error
		if res.Err == nil {
			res.Err = ferr
		}
	}
	d.Reporter.DatasetDone(idx, total, t, res)
	return res, nil
}

func recordAborted(db *duckdb.Writer, runID string, t DatasetTarget, now time.Time, cause error) {
	rowid, err := db.StartSyncRun(runID, t.ID, t.Effective.Table, t.Effective.Hash(), now)
	if err != nil {
		return
	}
	msg := "aborted"
	if cause != nil {
		msg = cause.Error()
	}
	_ = db.FinishSyncRun(rowid, "aborted", 0, msg, now)
}

func newRunID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// stderrWriter lazy-imports os.Stderr, keeping this file import-light for tests.
type stderrWriter struct{}

func (stderrWriter) Write(p []byte) (int, error) {
	return writeStderr(p)
}
```

- [ ] **Step 4: Wire `FetchCatalogScheme` as an exported alias in socrata**

Strategy/orchestrator calls `d.Client.FetchCatalogScheme` in the `scheme == "http"` branch. Expose it.

Edit `internal/socrata/catalog.go`. Find the unexported function `fetchCatalogScheme` and add an exported wrapper at the bottom of the file:

```go
// FetchCatalogScheme is FetchCatalog with an explicit URL scheme (for tests).
func (c *Client) FetchCatalogScheme(portal, scheme string) ([]CatalogEntry, error) {
	return c.fetchCatalogScheme(portal, scheme)
}
```

- [ ] **Step 5: Add `writeStderr` helper**

Create `internal/sync/stderr.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package sync

import "os"

func writeStderr(p []byte) (int, error) {
	return os.Stderr.Write(p)
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/sync/ -run TestRun -v`
Expected: all pass.

- [ ] **Step 7: Run full test suite**

Run: `go test ./...`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add internal/sync/run.go internal/sync/run_test.go internal/sync/stderr.go internal/socrata/catalog.go
git commit -m "sync: add Run orchestrator with worker pool and sync_runs accounting"
```

---

## Task 12: `csq catalog` subcommand

List mode (table/JSON) and `--output` starter-YAML mode.

**Files:**
- Create: `cmd/csq/catalog.go`
- Modify: `cmd/csq/main.go` (dispatch `catalog`)

- [ ] **Step 1: Write catalog.go**

Create `cmd/csq/catalog.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"text/tabwriter"
	"time"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func runCatalog(args []string) error {
	fs := flag.NewFlagSet("catalog", flag.ContinueOnError)
	var (
		portal   string
		dbPath   string
		appToken string
		refresh  bool
		asJSON   bool
		output   string
		force    bool
	)
	var ids, names, cats, tags []string

	fs.StringVar(&portal, "portal", "", "Socrata portal host")
	fs.StringVar(&dbPath, "db", "", "DuckDB file (default: <portal>.duckdb)")
	fs.StringVarP(&appToken, "token", "t", os.Getenv("SOCRATA_APP_TOKEN"), "Socrata app token")
	fs.BoolVar(&refresh, "refresh", false, "Force refetch of catalog")
	fs.BoolVar(&asJSON, "json", false, "Emit JSON instead of a table")
	fs.StringVar(&output, "output", "", "Write a starter YAML to this path instead of printing")
	fs.BoolVar(&force, "force", false, "Overwrite --output file if it exists")
	fs.StringArrayVar(&ids, "id", nil, "Literal 4x4 id filter (repeatable)")
	fs.StringArrayVar(&names, "name", nil, "Glob match on name (repeatable)")
	fs.StringArrayVar(&cats, "category", nil, "Glob match on category (repeatable)")
	fs.StringArrayVar(&tags, "tag", nil, "Glob match on tag (repeatable)")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if portal == "" {
		return fmt.Errorf("--portal is required")
	}
	if dbPath == "" {
		dbPath = portal + ".duckdb"
	}

	w, err := duckdb.Open(dbPath)
	if err != nil {
		return err
	}
	defer w.Close()

	client := &socrata.Client{AppToken: appToken}

	catalog, err := w.ReadCatalog()
	if err != nil {
		return fmt.Errorf("read cached catalog: %w", err)
	}
	if refresh || len(catalog) == 0 {
		fmt.Fprintf(os.Stderr, "[csq] fetching catalog from %s\n", portal)
		catalog, err = client.FetchCatalog(portal)
		if err != nil {
			return fmt.Errorf("fetch catalog: %w", err)
		}
		if err := w.UpsertCatalog(catalog, time.Now().UTC()); err != nil {
			return fmt.Errorf("cache catalog: %w", err)
		}
	}

	filtered := filterCatalog(catalog, ids, names, cats, tags)

	if output != "" {
		return writeStarterYAML(output, portal, filtered, force)
	}
	if asJSON {
		return json.NewEncoder(os.Stdout).Encode(filtered)
	}
	return printCatalogTable(os.Stdout, filtered)
}

func filterCatalog(catalog []socrata.CatalogEntry, ids, names, cats, tags []string) []socrata.CatalogEntry {
	out := make([]socrata.CatalogEntry, 0, len(catalog))
next:
	for _, e := range catalog {
		if len(ids) > 0 {
			matched := false
			for _, id := range ids {
				if id == e.ID {
					matched = true
					break
				}
			}
			if !matched {
				continue next
			}
		}
		if !anyGlob(names, e.Name) {
			continue next
		}
		if !anyGlob(cats, e.Category) {
			continue next
		}
		if len(tags) > 0 {
			matched := false
			for _, tag := range tags {
				for _, t := range e.Tags {
					if ok, _ := path.Match(tag, t); ok {
						matched = true
						break
					}
				}
				if matched {
					break
				}
			}
			if !matched {
				continue next
			}
		}
		out = append(out, e)
	}
	return out
}

// anyGlob returns true if patterns is empty, or if any pattern matches s.
func anyGlob(patterns []string, s string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if ok, _ := path.Match(p, s); ok {
			return true
		}
	}
	return false
}

func printCatalogTable(out *os.File, entries []socrata.CatalogEntry) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tNAME\tCATEGORY\tROWS\tUPDATED")
	for _, e := range entries {
		rc := "-"
		if e.RowCount != nil {
			rc = fmt.Sprintf("%d", *e.RowCount)
		}
		upd := "-"
		if e.UpdatedAt != nil {
			upd = e.UpdatedAt.Format("2006-01-02")
		}
		name := e.Name
		if len(name) > 40 {
			name = name[:37] + "..."
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", e.ID, name, e.Category, rc, upd)
	}
	return tw.Flush()
}

func writeStarterYAML(path, portal string, entries []socrata.CatalogEntry, force bool) error {
	if _, err := os.Stat(path); err == nil && !force {
		return fmt.Errorf("%s exists; pass --force to overwrite", path)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Generated by csq catalog --output\n")
	fmt.Fprintf(&b, "portal: %s\n", portal)
	fmt.Fprintf(&b, "on_error: continue\n")
	fmt.Fprintf(&b, "concurrency: 4\n\n")
	fmt.Fprintf(&b, "defaults:\n  batch_size: 5000\n  order_by: \":id\"\n\n")
	fmt.Fprintf(&b, "include:\n")
	for _, e := range entries {
		fmt.Fprintf(&b, "  # - id: %s   # %s\n", e.ID, e.Name)
	}
	fmt.Fprintf(&b, "\nexclude: []\n\noverrides: {}\n")
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
```

- [ ] **Step 2: Dispatch `catalog` from main.go**

Edit `cmd/csq/main.go`. Find the `switch os.Args[1]` block (around line 38). Add a `case "catalog"` branch after `case "extract"`:

```go
	case "catalog":
		if err := runCatalog(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "csq catalog: %v\n", err)
			os.Exit(1)
		}
```

Also update the `usage` const near the top of main.go:

```go
const usage = `csq — CivicSodaQuack

Usage:
  csq extract --portal <host> --dataset <4x4> [options]
  csq catalog --portal <host> [--refresh] [--json] [--output FILE]
  csq sync    --config <portal.yaml> [--dry-run] [--only IDs]

Examples:
  csq extract --portal data.cityofchicago.org --dataset 6zsd-86xi --limit 10000
  csq catalog --portal data.cityofchicago.org --category "Public Safety"
  csq sync    --config data.cityofchicago.org.yaml
`
```

- [ ] **Step 3: Verify the build and smoke the help**

Run:
```bash
go build -o csq ./cmd/csq
./csq
```
Expected: prints the new usage, exit code 2.

Run: `./csq catalog` (no args)
Expected: prints `csq catalog: --portal is required`, exit 1.

- [ ] **Step 4: Commit**

```bash
git add cmd/csq/catalog.go cmd/csq/main.go
git commit -m "cli: add csq catalog (list + --output starter YAML)"
```

---

## Task 13: `csq sync` subcommand

**Files:**
- Create: `cmd/csq/sync.go`
- Modify: `cmd/csq/main.go` (dispatch `sync`)

- [ ] **Step 1: Write sync.go**

Create `cmd/csq/sync.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
	syncpkg "github.com/neomantra/CivicSodaQuack/internal/sync"
)

func runSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	var (
		configPath     string
		dryRun         bool
		refreshCatalog bool
		concurrency    int
		only           string
		verbose        bool
	)
	fs.StringVar(&configPath, "config", "", "Portal YAML config (required)")
	fs.BoolVar(&dryRun, "dry-run", false, "Resolve selectors and print, don't write")
	fs.BoolVar(&refreshCatalog, "refresh-catalog", false, "Force refetch catalog before resolution")
	fs.IntVar(&concurrency, "concurrency", 0, "Override YAML concurrency (0 = use YAML)")
	fs.StringVar(&only, "only", "", "Comma-separated 4x4 ids to intersect with the resolved set")
	fs.BoolVarP(&verbose, "verbose", "v", false, "Verbose progress")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if configPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if concurrency > 0 {
		cfg.Concurrency = concurrency
	}

	w, err := duckdb.Open(cfg.DB)
	if err != nil {
		return err
	}
	defer w.Close()

	if n, err := w.IncompleteSyncRunCount(); err == nil && n > 0 {
		fmt.Fprintf(os.Stderr, "[csq] warning: %d prior sync_runs appear incomplete\n", n)
	}

	client := &socrata.Client{AppToken: cfg.AppToken}

	var onlyIDs []string
	if only != "" {
		for _, id := range strings.Split(only, ",") {
			id = strings.TrimSpace(id)
			if id != "" {
				onlyIDs = append(onlyIDs, id)
			}
		}
	}

	summary, err := syncpkg.Run(context.Background(), cfg, syncpkg.Deps{
		DB:             w,
		Client:         client,
		Scheme:         "https",
		Reporter:       &syncpkg.StderrReporter{Out: os.Stderr},
		Only:           onlyIDs,
		DryRun:         dryRun,
		RefreshCatalog: refreshCatalog,
	})
	if dryRun {
		fmt.Fprintf(os.Stderr, "[csq] dry-run: would sync %d datasets\n", summary.Planned)
		return nil
	}
	fmt.Fprintf(os.Stderr, "[csq] summary: %d ok, %d failed, %d aborted, wall %s\n",
		summary.OK, summary.Failed, summary.Aborted, summary.Wall)
	if err != nil {
		return err
	}
	return nil
}
```

- [ ] **Step 2: Dispatch `sync` from main.go**

Edit `cmd/csq/main.go`. After the `case "catalog"` branch added in Task 12, add:

```go
	case "sync":
		if err := runSync(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "csq sync: %v\n", err)
			os.Exit(1)
		}
```

- [ ] **Step 3: Verify build**

Run: `go build -o csq ./cmd/csq`
Expected: builds cleanly.

- [ ] **Step 4: Commit**

```bash
git add cmd/csq/sync.go cmd/csq/main.go
git commit -m "cli: add csq sync subcommand"
```

---

## Task 14: End-to-end CLI smoke test

Black-box test: build the binary, run it against an `httptest.Server`-backed portal (injected into the YAML), assert exit code and DuckDB contents.

**Files:**
- Create: `cmd/csq/testdata/portal.yaml.tmpl`
- Create: `cmd/csq/cli_smoke_test.go`

- [ ] **Step 1: Write the template**

Create `cmd/csq/testdata/portal.yaml.tmpl`:

```yaml
portal: {{HOST}}
db: {{DB}}
on_error: continue
concurrency: 2
defaults:
  batch_size: 5
  order_by: ":id"
include:
  - category: "Test"
```

- [ ] **Step 2: Write the smoke test**

Create `cmd/csq/cli_smoke_test.go`:

```go
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

	// CSQ uses https by default. For the smoke test we still want to drive the
	// fake http server; we do that by pointing the CSQ binary at the CSQ_SCHEME
	// env var which sync.go respects. If sync.go doesn't read an env override
	// yet, skip: this test only activates when that hook is wired.
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
```

- [ ] **Step 3: Wire the `CSQ_SCHEME` env hook in `cmd/csq/sync.go`**

Edit `cmd/csq/sync.go` — find the block that constructs `syncpkg.Deps{...}` and change the `Scheme` field to read an env override:

```go
	scheme := "https"
	if s := os.Getenv("CSQ_SCHEME"); s != "" {
		scheme = s
	}
	summary, err := syncpkg.Run(context.Background(), cfg, syncpkg.Deps{
		DB:             w,
		Client:         client,
		Scheme:         scheme,
		Reporter:       &syncpkg.StderrReporter{Out: os.Stderr},
		Only:           onlyIDs,
		DryRun:         dryRun,
		RefreshCatalog: refreshCatalog,
	})
```

- [ ] **Step 4: Run the smoke test**

Run: `go test ./cmd/csq/ -run TestCSQ_SyncSmoke -v`
Expected: PASS.

- [ ] **Step 5: Run everything**

Run: `go test ./... && go vet ./...`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add cmd/csq/cli_smoke_test.go cmd/csq/testdata/portal.yaml.tmpl cmd/csq/sync.go
git commit -m "cli: add sync smoke test (black-box build + fake portal)"
```

---

## Task 15: README quickstart for Phase 1

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README.md**

Replace the `## Status` and `## Quickstart` sections with:

```markdown
## Status

**Phase 1** — catalog discovery + YAML-driven bulk sync into a per-portal DuckDB.

## Quickstart

```bash
go build -o csq ./cmd/csq

# Discover what's on a portal
./csq catalog --portal data.cityofchicago.org --category "Public Safety"

# Generate a starter YAML
./csq catalog --portal data.cityofchicago.org --category "Public Safety" \
  --output data.cityofchicago.org.yaml

# Sync the datasets enumerated in the YAML
./csq sync --config data.cityofchicago.org.yaml --dry-run    # preview
./csq sync --config data.cityofchicago.org.yaml              # execute
```

Set `SOCRATA_APP_TOKEN` (referenced in the YAML as `${SOCRATA_APP_TOKEN}`) to lift anonymous rate limits.

### Config shape

```yaml
portal: data.cityofchicago.org
app_token: ${SOCRATA_APP_TOKEN}
concurrency: 4
on_error: continue

defaults:
  batch_size: 5000
  order_by: ":id"

include:
  - category: "Public Safety"
  - tag: "311*"
exclude:
  - id: 85ca-t3if     # giant, skip

overrides:
  6zsd-86xi:
    table: crimes
    where: "date >= '2015-01-01'"
    batch_size: 10000
    columns:
      skip: [location_description_raw]
```

Catalog and per-dataset sync history live in the `_csq` schema inside the portal's DuckDB:

```sql
SELECT id, name, category FROM _csq.catalog LIMIT 10;
SELECT dataset_id, status, rows_written, duration_ms
  FROM _csq.sync_runs ORDER BY started_at DESC LIMIT 10;
```
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: Phase 1 README quickstart"
```

---

## Final verification

- [ ] **Run the full build + test + vet**

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
# edit /tmp/chi.yaml to uncomment 2-3 small datasets
./csq sync --config /tmp/chi.yaml --dry-run
./csq sync --config /tmp/chi.yaml
```
