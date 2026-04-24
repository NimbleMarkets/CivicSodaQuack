# Phase 3 — MCP Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `csq mcp`, a long-running MCP server (stdio + HTTP) that ATTACHes one or more per-portal DuckDB files and exposes four typed tools for agents to discover and query civic data.

**Architecture:** A new `internal/mcpserver` package owns all MCP-specific code. Each `--db` arg opens two `*sql.DB` pools to the same file (read-only and read-write); a separate in-memory "host DB" `ATTACH`es every portal so cross-portal SQL queries work. Tools are registered via the official MCP Go SDK's typed `AddTool[In, Out]` API. `query_sql` uses engine-level read-only enforcement (no SQL parsing) plus a 30s timeout and 1000-row / 1MB result caps.

**Tech Stack:** Go 1.24, DuckDB (`duckdb-go/v2`), `github.com/modelcontextprotocol/go-sdk@v1.5.0`, pflag.

---

## File Structure

**Create:**
- `internal/mcpserver/attach.go` — `DBSpec`, `ResolveDBSpecs([]string) ([]DBSpec, error)`, alias derivation, validation.
- `internal/mcpserver/attach_test.go`
- `internal/mcpserver/pools.go` — `Pools`, `PortalPools`, `OpenPools([]DBSpec) (*Pools, error)`, `Close()`. Holds host DB + per-portal RO/RW pools; ATTACHes everything to the host.
- `internal/mcpserver/pools_test.go`
- `internal/mcpserver/fixtures_test.go` — shared test helpers: `seedFixtureDB(path, ...)` builds a CivicSodaQuack-shaped DuckDB file with `_csq.catalog`, `_csq.sync_runs`, `_csq.dataset_state`, and dataset tables.
- `internal/mcpserver/tools_list_datasets.go` — `ListDatasetsArgs`, `DatasetSummary`, `listDatasetsHandler(ctx, *Pools, ListDatasetsArgs) ([]DatasetSummary, error)`.
- `internal/mcpserver/tools_list_datasets_test.go`
- `internal/mcpserver/tools_describe_dataset.go` — `DescribeDatasetArgs`, `DatasetDetail`, `ColumnInfo`, `SyncInfo`, `describeDatasetHandler(...)`.
- `internal/mcpserver/tools_describe_dataset_test.go`
- `internal/mcpserver/tools_search_datasets.go` — `SearchDatasetsArgs`, `searchDatasetsHandler(...)`.
- `internal/mcpserver/tools_search_datasets_test.go`
- `internal/mcpserver/tools_query_sql.go` — `QuerySQLArgs`, `QuerySQLResult`, `querySQLHandler(ctx, *Pools, QuerySQLArgs, timeout) (QuerySQLResult, error)`.
- `internal/mcpserver/tools_query_sql_test.go`
- `internal/mcpserver/server.go` — `Options`, `Serve(ctx, Options) error`. Builds `Pools`, constructs `*mcp.Server`, registers all four tools, runs the chosen transport (stdio default; HTTP when `Options.HTTPAddr != ""`).
- `internal/mcpserver/server_test.go` — in-process tests that build a Server with fixtures and verify tool registration works for both transports.
- `cmd/csq/mcp.go` — `runMCP(args)` CLI subcommand: parses `--db` (repeatable, `path` or `alias=path`) and `--http <addr>`; calls `mcpserver.ResolveDBSpecs` + `mcpserver.Serve`.

**Modify:**
- `cmd/csq/main.go` — dispatch `case "mcp"`, update `usage`.
- `cmd/csq/cli_smoke_test.go` — append `TestCSQ_MCP_Stdio_Smoke` (subprocess + hand-crafted JSON-RPC).
- `README.md` — add `csq mcp` to the quickstart and a one-paragraph MCP section.
- `go.mod` / `go.sum` — adds `github.com/modelcontextprotocol/go-sdk v1.5.0`.

---

## Task 1: Add the MCP SDK dependency

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add the dependency**

Run:
```bash
cd /Users/evan/projects/cannabis_research/CivicSodaQuack
go get github.com/modelcontextprotocol/go-sdk@v1.5.0
```

`go mod tidy` will run automatically the first time `go build` or `go test` touches a file that imports the package. Don't run it now (no code imports it yet).

- [ ] **Step 2: Verify the dep is in go.mod**

Run: `grep modelcontextprotocol go.mod`
Expected: one line like `github.com/modelcontextprotocol/go-sdk v1.5.0`.

- [ ] **Step 3: Verify the build still works**

Run: `go build ./...`
Expected: clean build, no output.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: add github.com/modelcontextprotocol/go-sdk@v1.5.0"
```

---

## Task 2: `DBSpec` resolution

Pure function that converts the `--db` arg list into resolved `(alias, path)` pairs, with validation.

**Files:**
- Create: `internal/mcpserver/attach.go`
- Create: `internal/mcpserver/attach_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/mcpserver/attach_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"strings"
	"testing"
)

func TestResolveDBSpecs_FilenameAlias(t *testing.T) {
	got, err := ResolveDBSpecs([]string{"chicago.duckdb"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 1 || got[0].Alias != "chicago" || got[0].Path != "chicago.duckdb" {
		t.Errorf("got %+v, want one DBSpec{Alias=chicago, Path=chicago.duckdb}", got)
	}
}

func TestResolveDBSpecs_DotsBecomeUnderscores(t *testing.T) {
	got, err := ResolveDBSpecs([]string{"data.cityofchicago.org.duckdb"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got[0].Alias != "data_cityofchicago_org" {
		t.Errorf("alias: got %q, want data_cityofchicago_org", got[0].Alias)
	}
}

func TestResolveDBSpecs_DirectoryStripped(t *testing.T) {
	got, err := ResolveDBSpecs([]string{"/some/path/nyc.duckdb"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got[0].Alias != "nyc" {
		t.Errorf("alias: got %q, want nyc", got[0].Alias)
	}
}

func TestResolveDBSpecs_ExplicitAlias(t *testing.T) {
	got, err := ResolveDBSpecs([]string{"foo=/some/path/whatever.duckdb"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got[0].Alias != "foo" || got[0].Path != "/some/path/whatever.duckdb" {
		t.Errorf("got %+v", got)
	}
}

func TestResolveDBSpecs_CollisionError(t *testing.T) {
	_, err := ResolveDBSpecs([]string{"a/data.duckdb", "b/data.duckdb"})
	if err == nil || !strings.Contains(err.Error(), "alias collision") {
		t.Errorf("want alias collision error, got %v", err)
	}
}

func TestResolveDBSpecs_InvalidAlias(t *testing.T) {
	cases := []string{
		"1bad=foo.duckdb",     // starts with digit
		"has-dash=foo.duckdb", // contains dash
		"=foo.duckdb",         // empty alias
		"a.b=foo.duckdb",      // contains dot
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			_, err := ResolveDBSpecs([]string{c})
			if err == nil {
				t.Errorf("want error for %q", c)
			}
		})
	}
}

func TestResolveDBSpecs_FilenameAliasInvalid(t *testing.T) {
	// Filename-derived alias starts with a digit; must error rather than silently rename.
	_, err := ResolveDBSpecs([]string{"311data.duckdb"})
	if err == nil {
		t.Errorf("want error for filename whose derived alias starts with a digit")
	}
}

func TestResolveDBSpecs_Empty(t *testing.T) {
	_, err := ResolveDBSpecs(nil)
	if err == nil || !strings.Contains(err.Error(), "at least one --db") {
		t.Errorf("want require-one error, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcpserver/ -run TestResolveDBSpecs -v`
Expected: FAIL — `ResolveDBSpecs`/`DBSpec` undefined (or build error if package doesn't exist yet).

- [ ] **Step 3: Write attach.go**

Create `internal/mcpserver/attach.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// DBSpec is a resolved (alias, path) pair for one portal DuckDB file.
type DBSpec struct {
	Alias string // SQL identifier; ATTACH alias and per-portal namespace
	Path  string // filesystem path to the .duckdb file
}

var aliasRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ResolveDBSpecs converts raw --db arg strings into validated DBSpec records.
// Each arg is either a plain path (alias derived from basename) or alias=path.
// Returns an error on empty input, invalid aliases, or alias collisions.
func ResolveDBSpecs(args []string) ([]DBSpec, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("at least one --db is required")
	}
	out := make([]DBSpec, 0, len(args))
	seen := map[string]string{} // alias → path that already used it

	for _, raw := range args {
		spec, err := parseDBArg(raw)
		if err != nil {
			return nil, err
		}
		if prev, ok := seen[spec.Alias]; ok {
			return nil, fmt.Errorf("alias collision: alias %q used by both %q and %q (pass alias=path to disambiguate)",
				spec.Alias, prev, spec.Path)
		}
		seen[spec.Alias] = spec.Path
		out = append(out, spec)
	}
	return out, nil
}

func parseDBArg(raw string) (DBSpec, error) {
	if i := strings.IndexByte(raw, '='); i >= 0 {
		alias := raw[:i]
		path := raw[i+1:]
		if !aliasRE.MatchString(alias) {
			return DBSpec{}, fmt.Errorf("invalid alias %q in --db %q (must match [a-zA-Z_][a-zA-Z0-9_]*)", alias, raw)
		}
		if path == "" {
			return DBSpec{}, fmt.Errorf("--db %q has empty path", raw)
		}
		return DBSpec{Alias: alias, Path: path}, nil
	}
	alias := aliasFromPath(raw)
	if !aliasRE.MatchString(alias) {
		return DBSpec{}, fmt.Errorf("filename-derived alias %q from %q is not a valid SQL identifier (use alias=path)", alias, raw)
	}
	return DBSpec{Alias: alias, Path: raw}, nil
}

// aliasFromPath strips the directory and the .duckdb extension, then replaces
// any dots in the remainder with underscores.
func aliasFromPath(p string) string {
	base := filepath.Base(p)
	base = strings.TrimSuffix(base, ".duckdb")
	return strings.ReplaceAll(base, ".", "_")
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcpserver/ -run TestResolveDBSpecs -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/mcpserver/attach.go internal/mcpserver/attach_test.go
git commit -m "mcpserver: add DBSpec resolver with alias derivation + validation"
```

---

## Task 3: `Pools` — dual RO/RW pools per portal + host DB

Owns the connection topology: one in-memory host DB and, per portal, a read-only pool plus an idle read-write pool. ATTACHes each portal to the host (read-only).

**Files:**
- Create: `internal/mcpserver/pools.go`
- Create: `internal/mcpserver/pools_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/mcpserver/pools_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func makeEmptyCSQDB(t *testing.T, path string) {
	t.Helper()
	db, err := openDB(path, false)
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

func TestOpenPools_Success(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.duckdb")
	makeEmptyCSQDB(t, path)

	pools, err := OpenPools([]DBSpec{{Alias: "test", Path: path}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	defer pools.Close()
	if pools.Host == nil {
		t.Error("host DB nil")
	}
	if _, ok := pools.Portals["test"]; !ok {
		t.Errorf("portal 'test' missing")
	}
	if pools.Portals["test"].RO == nil || pools.Portals["test"].RW == nil {
		t.Errorf("RO or RW pool nil")
	}
	// Host should see the ATTACHed schema
	var n int
	if err := pools.Host.QueryRow(`SELECT COUNT(*) FROM test._csq.catalog`).Scan(&n); err != nil {
		t.Errorf("query attached: %v", err)
	}
}

func TestOpenPools_MissingFile(t *testing.T) {
	_, err := OpenPools([]DBSpec{{Alias: "x", Path: "/nonexistent/foo.duckdb"}})
	if err == nil {
		t.Fatal("want error for missing file")
	}
}

func TestOpenPools_NotCSQDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wrong.duckdb")
	// Open without seeding the _csq schema
	db, err := openDB(path, false)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.Close()

	_, err = OpenPools([]DBSpec{{Alias: "x", Path: path}})
	if err == nil {
		t.Fatal("want 'not a CivicSodaQuack DuckDB' error")
	}
}

func TestOpenPools_DualWriteRead(t *testing.T) {
	// Validates dual-pool design: write through RW pool, read through RO pool.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.duckdb")
	makeEmptyCSQDB(t, path)

	pools, err := OpenPools([]DBSpec{{Alias: "test", Path: path}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	defer pools.Close()

	// Write via RW pool
	_, err = pools.Portals["test"].RW.Exec(
		`INSERT INTO _csq.catalog (id, name, fetched_at, raw)
		 VALUES ('aaaa-0001', 'Test', NOW(), '{}')`)
	if err != nil {
		t.Fatalf("rw insert: %v", err)
	}

	// Read via RO pool (will see the write since they share the same DuckDB file)
	var name string
	err = pools.Portals["test"].RO.QueryRow(
		`SELECT name FROM _csq.catalog WHERE id = 'aaaa-0001'`).Scan(&name)
	if err != nil {
		t.Fatalf("ro read: %v", err)
	}
	if name != "Test" {
		t.Errorf("got %q", name)
	}

	// RO pool must reject writes
	_, err = pools.Portals["test"].RO.Exec(
		`INSERT INTO _csq.catalog (id, name, fetched_at, raw) VALUES ('b', 'B', NOW(), '{}')`)
	if err == nil {
		t.Errorf("RO pool accepted a write")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcpserver/ -run TestOpenPools -v`
Expected: FAIL — `OpenPools`, `Pools`, `openDB` undefined.

- [ ] **Step 3: Write pools.go**

Create `internal/mcpserver/pools.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"

	_ "github.com/duckdb/duckdb-go/v2"
)

// PortalPools holds the read-only and read-write *sql.DB handles for one portal file.
// Both point at the same file; access mode is enforced at the DuckDB layer per pool.
type PortalPools struct {
	Path string
	RO   *sql.DB
	RW   *sql.DB
}

// Pools owns the in-memory host DB plus per-portal pools. The host has each
// portal ATTACHed read-only as <alias>; tools issue queries through Pools.Host
// so cross-portal queries via "<alias>.<schema>.<table>" syntax work.
type Pools struct {
	Host    *sql.DB
	Portals map[string]*PortalPools
}

// OpenPools opens the host and per-portal pools, ATTACHes each portal to the
// host, and verifies each file is a CivicSodaQuack DuckDB.
func OpenPools(specs []DBSpec) (*Pools, error) {
	host, err := openDB(":memory:", false)
	if err != nil {
		return nil, fmt.Errorf("open host: %w", err)
	}

	p := &Pools{
		Host:    host,
		Portals: make(map[string]*PortalPools, len(specs)),
	}

	for _, spec := range specs {
		if _, err := os.Stat(spec.Path); err != nil {
			p.Close()
			return nil, fmt.Errorf("--db %s: %w", spec.Path, err)
		}
		ro, err := openDB(spec.Path, true)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("open ro %s: %w", spec.Path, err)
		}
		if err := assertIsCSQDB(ro, spec.Path); err != nil {
			ro.Close()
			p.Close()
			return nil, err
		}
		rw, err := openDB(spec.Path, false)
		if err != nil {
			ro.Close()
			p.Close()
			return nil, fmt.Errorf("open rw %s: %w", spec.Path, err)
		}
		// ATTACH read-only to the host
		_, err = host.Exec(fmt.Sprintf(`ATTACH '%s' AS %s (READ_ONLY)`,
			escapeSQLString(spec.Path), spec.Alias))
		if err != nil {
			ro.Close()
			rw.Close()
			p.Close()
			return nil, fmt.Errorf("attach %s as %s: %w", spec.Path, spec.Alias, err)
		}
		p.Portals[spec.Alias] = &PortalPools{Path: spec.Path, RO: ro, RW: rw}
	}
	return p, nil
}

// Close closes every pool. Safe to call multiple times.
func (p *Pools) Close() error {
	var firstErr error
	for _, pp := range p.Portals {
		if pp.RO != nil {
			if err := pp.RO.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if pp.RW != nil {
			if err := pp.RW.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if p.Host != nil {
		if err := p.Host.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// openDB opens a *sql.DB for path with the requested access mode. For
// in-memory databases (path == ":memory:") access mode is ignored.
func openDB(path string, readOnly bool) (*sql.DB, error) {
	dsn := path
	if path != ":memory:" && readOnly {
		dsn = path + "?access_mode=READ_ONLY"
	}
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// assertIsCSQDB verifies the file has _csq.catalog (the Phase 1 marker).
func assertIsCSQDB(db *sql.DB, path string) error {
	var n int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema = '_csq' AND table_name = 'catalog'`).Scan(&n)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", path, err)
	}
	if n == 0 {
		return fmt.Errorf("no _csq.catalog in %s; not a CivicSodaQuack DuckDB", path)
	}
	return nil
}

// escapeSQLString escapes single quotes so the path can be safely embedded in
// a single-quoted DuckDB string literal.
func escapeSQLString(s string) string {
	// Belt-and-suspenders: also URL-quote anything weird; ATTACH accepts both.
	_ = url.QueryEscape // used only via the import to avoid linter complaints
	return replaceAll(s, "'", "''")
}

func replaceAll(s, old, new string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); {
		if i+len(old) <= len(s) && s[i:i+len(old)] == old {
			out = append(out, new...)
			i += len(old)
			continue
		}
		out = append(out, s[i])
		i++
	}
	return string(out)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcpserver/ -run TestOpenPools -v`
Expected: all 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/mcpserver/pools.go internal/mcpserver/pools_test.go
git commit -m "mcpserver: add Pools with dual RO/RW pools + host ATTACH"
```

---

## Task 4: Test fixtures

Shared helper for building a CivicSodaQuack-shaped DuckDB file with seeded catalog/sync_runs/dataset_state and a couple of dataset tables. Used by every tool test.

**Files:**
- Create: `internal/mcpserver/fixtures_test.go`

- [ ] **Step 1: Write the helper**

Create `internal/mcpserver/fixtures_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// FixtureDataset describes one dataset to seed into a fixture DB.
type FixtureDataset struct {
	ID          string
	Name        string
	Description string
	Category    string
	Tags        []string // stored as JSON
	TableName   string   // physical DuckDB table name in main schema
	// ColumnDefs is a list of "<name> <type>" pairs for the table; e.g. ["id VARCHAR", "score DOUBLE"]
	ColumnDefs []string
	// Rows is appended to the table; each row is column-name -> value matching ColumnDefs.
	Rows []map[string]any
	// Synced controls whether _csq.sync_runs / _csq.dataset_state rows are inserted.
	// When false, the dataset appears in catalog but has no successful sync.
	Synced bool
	// HWM is written to dataset_state.hwm_updated_at when Synced=true.
	HWM time.Time
}

// seedFixtureDB creates a CivicSodaQuack-shaped DuckDB file at path with the
// given datasets seeded into _csq.catalog, dataset tables in main, and (when
// Synced=true) matching _csq.sync_runs + _csq.dataset_state rows.
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
		`CREATE TABLE _csq.catalog (
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
		`CREATE TABLE _csq.sync_runs (
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
		`CREATE TABLE _csq.dataset_state (
			dataset_id           VARCHAR PRIMARY KEY,
			hwm_updated_at       TIMESTAMP,
			last_full_replace_at TIMESTAMP,
			last_run_id          VARCHAR,
			hwm_column           VARCHAR NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed migrations: %v", err)
		}
	}

	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	for _, d := range datasets {
		// catalog row
		tagsJSON := jsonStringList(d.Tags)
		_, err := db.Exec(
			`INSERT INTO _csq.catalog
			   (id, name, description, category, tags, fetched_at, raw)
			 VALUES ($1, $2, $3, $4, $5, $6, '{}')`,
			d.ID, d.Name, d.Description, d.Category, tagsJSON, now,
		)
		if err != nil {
			t.Fatalf("seed catalog %s: %v", d.ID, err)
		}

		// dataset table in main
		if d.TableName != "" && len(d.ColumnDefs) > 0 {
			create := `CREATE TABLE main."` + d.TableName + `" (` + joinComma(d.ColumnDefs) + `)`
			if _, err := db.Exec(create); err != nil {
				t.Fatalf("create table %s: %v", d.TableName, err)
			}
			for _, row := range d.Rows {
				cols, placeholders, vals := buildInsert(row, d.ColumnDefs)
				stmt := `INSERT INTO main."` + d.TableName + `" (` + cols + `) VALUES (` + placeholders + `)`
				if _, err := db.Exec(stmt, vals...); err != nil {
					t.Fatalf("insert into %s: %v", d.TableName, err)
				}
			}
		}

		// sync_runs + dataset_state when Synced
		if d.Synced {
			_, err := db.Exec(
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

// jsonStringList renders ["a","b"] as a JSON array literal for the tags column.
func jsonStringList(items []string) string {
	if len(items) == 0 {
		return "[]"
	}
	out := "["
	for i, s := range items {
		if i > 0 {
			out += ","
		}
		out += `"` + s + `"`
	}
	return out + "]"
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}

func buildInsert(row map[string]any, columnDefs []string) (cols, placeholders string, vals []any) {
	for i, def := range columnDefs {
		// column name = first whitespace-separated token of def
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

- [ ] **Step 2: Verify the helper compiles in isolation**

Run: `go build ./internal/mcpserver/`
Expected: no output (test files don't break the build because they aren't compiled outside of `go test`).

Run: `go vet ./internal/mcpserver/`
Expected: no output.

There's no test for the fixture helper itself; it'll be exercised by the tool tests in subsequent tasks.

- [ ] **Step 3: Commit**

```bash
git add internal/mcpserver/fixtures_test.go
git commit -m "mcpserver: add fixture helper for tool tests"
```

---

## Task 5: `list_datasets` tool

Returns `[]DatasetSummary` filtered by optional `portal` and `category`. Pulls catalog data from each ATTACH'd portal and joins to `_csq.sync_runs` for the most recent successful row.

**Files:**
- Create: `internal/mcpserver/tools_list_datasets.go`
- Create: `internal/mcpserver/tools_list_datasets_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/mcpserver/tools_list_datasets_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"sort"
	"testing"
	"time"
)

func openFixturePools(t *testing.T, datasets ...FixtureDataset) (*Pools, func()) {
	t.Helper()
	dir := t.TempDir()
	path := seedFixtureDB(t, dir, "test.duckdb", datasets...)
	pools, err := OpenPools([]DBSpec{{Alias: "test", Path: path}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	return pools, func() { pools.Close() }
}

func TestListDatasets_Empty(t *testing.T) {
	pools, cleanup := openFixturePools(t)
	defer cleanup()

	got, err := listDatasetsHandler(context.Background(), pools, ListDatasetsArgs{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d, want 0", len(got))
	}
}

func TestListDatasets_OnePortal(t *testing.T) {
	hwm := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	pools, cleanup := openFixturePools(t,
		FixtureDataset{
			ID: "aaaa-0001", Name: "Crimes", Category: "Public Safety",
			TableName: "aaaa_0001",
			ColumnDefs: []string{"socrata_id VARCHAR", "score DOUBLE"},
			Rows:       []map[string]any{{"socrata_id": "a", "score": 1.0}, {"socrata_id": "b", "score": 2.0}},
			Synced:     true, HWM: hwm,
		})
	defer cleanup()

	got, err := listDatasetsHandler(context.Background(), pools, ListDatasetsArgs{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1, got %d", len(got))
	}
	d := got[0]
	if d.DatasetID != "aaaa-0001" || d.Portal != "test" || d.Name != "Crimes" {
		t.Errorf("dataset summary wrong: %+v", d)
	}
	if d.RowCount == nil || *d.RowCount != 2 {
		t.Errorf("rowcount: got %v, want 2", d.RowCount)
	}
	if d.TableName != "aaaa_0001" {
		t.Errorf("table_name: got %q", d.TableName)
	}
}

func TestListDatasets_NeverSynced_RowCountNil(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "Crimes", Category: "Safety", Synced: false})
	defer cleanup()

	got, _ := listDatasetsHandler(context.Background(), pools, ListDatasetsArgs{})
	if len(got) != 1 || got[0].RowCount != nil {
		t.Errorf("RowCount should be nil for un-synced dataset; got %+v", got)
	}
	// Fallback table name from id
	if got[0].TableName != "aaaa_0001" {
		t.Errorf("fallback table_name wrong: %q", got[0].TableName)
	}
}

func TestListDatasets_PortalFilter(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "A"},
		FixtureDataset{ID: "bbbb-0002", Name: "B"})
	defer cleanup()

	got, _ := listDatasetsHandler(context.Background(), pools, ListDatasetsArgs{Portal: "missing"})
	if len(got) != 0 {
		t.Errorf("portal=missing should return empty, got %d", len(got))
	}
	got, _ = listDatasetsHandler(context.Background(), pools, ListDatasetsArgs{Portal: "test"})
	if len(got) != 2 {
		t.Errorf("portal=test should return 2, got %d", len(got))
	}
}

func TestListDatasets_CategoryFilterCaseInsensitive(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "A", Category: "Public Safety"},
		FixtureDataset{ID: "bbbb-0002", Name: "B", Category: "Parks"})
	defer cleanup()

	got, _ := listDatasetsHandler(context.Background(), pools, ListDatasetsArgs{Category: "safety"})
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		ids := sort.StringSlice{}
		for _, d := range got {
			ids = append(ids, d.DatasetID)
		}
		t.Errorf("got %v, want [aaaa-0001]", []string(ids))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcpserver/ -run TestListDatasets -v`
Expected: FAIL — `listDatasetsHandler`/`ListDatasetsArgs`/`DatasetSummary` undefined.

- [ ] **Step 3: Write tools_list_datasets.go**

Create `internal/mcpserver/tools_list_datasets.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// ListDatasetsArgs are the inputs to the list_datasets MCP tool.
type ListDatasetsArgs struct {
	Portal   string `json:"portal,omitempty" jsonschema:"optional portal alias filter"`
	Category string `json:"category,omitempty" jsonschema:"optional case-insensitive substring on category"`
}

// DatasetSummary is one row in list_datasets / search_datasets results.
type DatasetSummary struct {
	DatasetID string `json:"dataset_id"`
	Portal    string `json:"portal"`
	Name      string `json:"name"`
	Category  string `json:"category,omitempty"`
	TableName string `json:"table_name"`
	RowCount  *int64 `json:"row_count,omitempty"`
}

// listDatasetsHandler enumerates datasets across the requested portal (or all
// portals) with optional category substring filter.
func listDatasetsHandler(ctx context.Context, p *Pools, args ListDatasetsArgs) ([]DatasetSummary, error) {
	aliases := selectPortals(p, args.Portal)
	out := make([]DatasetSummary, 0, len(aliases)*4)

	for _, alias := range aliases {
		rows, err := queryDatasetsForPortal(ctx, p.Portals[alias].RO, alias)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", alias, err)
		}
		out = append(out, rows...)
	}

	if args.Category != "" {
		needle := strings.ToLower(args.Category)
		filtered := out[:0]
		for _, d := range out {
			if strings.Contains(strings.ToLower(d.Category), needle) {
				filtered = append(filtered, d)
			}
		}
		out = filtered
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Portal != out[j].Portal {
			return out[i].Portal < out[j].Portal
		}
		return out[i].DatasetID < out[j].DatasetID
	})
	return out, nil
}

// selectPortals returns the alias list to scan: a single alias if requested
// (and present), or all aliases otherwise. Unknown alias yields empty result.
func selectPortals(p *Pools, requested string) []string {
	if requested != "" {
		if _, ok := p.Portals[requested]; !ok {
			return nil
		}
		return []string{requested}
	}
	out := make([]string, 0, len(p.Portals))
	for a := range p.Portals {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

// queryDatasetsForPortal returns all dataset summaries from one portal pool.
// table_name and row_count come from the most recent status='ok' sync_runs row;
// if no successful sync exists, table_name falls back to replace(id, '-', '_').
func queryDatasetsForPortal(ctx context.Context, db *sql.DB, alias string) ([]DatasetSummary, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.id, c.name, COALESCE(c.category, ''), s.table_name, s.rows_written
		FROM _csq.catalog c
		LEFT JOIN (
			SELECT dataset_id,
			       FIRST(table_name ORDER BY started_at DESC) AS table_name,
			       FIRST(rows_written ORDER BY started_at DESC) AS rows_written
			FROM _csq.sync_runs
			WHERE status = 'ok'
			GROUP BY dataset_id
		) s ON s.dataset_id = c.id
		ORDER BY c.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DatasetSummary
	for rows.Next() {
		var id, name, category string
		var table sql.NullString
		var rowCount sql.NullInt64
		if err := rows.Scan(&id, &name, &category, &table, &rowCount); err != nil {
			return nil, err
		}
		summary := DatasetSummary{
			DatasetID: id,
			Portal:    alias,
			Name:      name,
			Category:  category,
		}
		if table.Valid {
			summary.TableName = table.String
		} else {
			summary.TableName = strings.ReplaceAll(id, "-", "_")
		}
		if rowCount.Valid {
			n := rowCount.Int64
			summary.RowCount = &n
		}
		out = append(out, summary)
	}
	return out, rows.Err()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcpserver/ -run TestListDatasets -v`
Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/mcpserver/tools_list_datasets.go internal/mcpserver/tools_list_datasets_test.go
git commit -m "mcpserver: add list_datasets tool handler"
```

---

## Task 6: `describe_dataset` tool

Returns the catalog entry plus columns from `information_schema`, last successful sync info, and HWM.

**Files:**
- Create: `internal/mcpserver/tools_describe_dataset.go`
- Create: `internal/mcpserver/tools_describe_dataset_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/mcpserver/tools_describe_dataset_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDescribeDataset_Found(t *testing.T) {
	hwm := time.Date(2026, 4, 23, 5, 0, 0, 0, time.UTC)
	pools, cleanup := openFixturePools(t,
		FixtureDataset{
			ID: "aaaa-0001", Name: "Crimes", Description: "Chicago crimes",
			Category: "Public Safety", Tags: []string{"crime", "311"},
			TableName: "aaaa_0001",
			ColumnDefs: []string{"socrata_id VARCHAR", "score DOUBLE", "kind VARCHAR"},
			Rows:       []map[string]any{{"socrata_id": "a", "score": 1.0, "kind": "x"}},
			Synced:     true, HWM: hwm,
		})
	defer cleanup()

	got, err := describeDatasetHandler(context.Background(), pools, DescribeDatasetArgs{DatasetID: "aaaa-0001"})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.DatasetID != "aaaa-0001" || got.Description != "Chicago crimes" {
		t.Errorf("got %+v", got)
	}
	if len(got.Tags) != 2 {
		t.Errorf("tags: got %v", got.Tags)
	}
	// socrata_id should be filtered out of user-visible columns
	for _, c := range got.Columns {
		if c.Name == "socrata_id" {
			t.Errorf("socrata_id should be hidden from columns")
		}
	}
	if len(got.Columns) != 2 {
		t.Errorf("user columns: got %d, want 2", len(got.Columns))
	}
	if got.LastSync == nil || got.LastSync.Status != "ok" || got.LastSync.RowsWritten != 1 {
		t.Errorf("last sync: got %+v", got.LastSync)
	}
	if got.HWMUpdatedAt == nil || !got.HWMUpdatedAt.Equal(hwm) {
		t.Errorf("hwm: got %v", got.HWMUpdatedAt)
	}
}

func TestDescribeDataset_NeverSynced(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	defer cleanup()

	got, err := describeDatasetHandler(context.Background(), pools, DescribeDatasetArgs{DatasetID: "aaaa-0001"})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.LastSync != nil {
		t.Errorf("LastSync should be nil")
	}
	if got.HWMUpdatedAt != nil {
		t.Errorf("HWMUpdatedAt should be nil")
	}
	if len(got.Columns) != 0 {
		t.Errorf("Columns should be empty (no table)")
	}
}

func TestDescribeDataset_Unknown(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	defer cleanup()

	_, err := describeDatasetHandler(context.Background(), pools, DescribeDatasetArgs{DatasetID: "zzzz-9999"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("want not-found error, got %v", err)
	}
}

func TestDescribeDataset_AmbiguousAcrossPortals(t *testing.T) {
	dir := t.TempDir()
	a := seedFixtureDB(t, dir, "a.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "A's crimes"})
	b := seedFixtureDB(t, dir, "b.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "B's crimes"})
	pools, err := OpenPools([]DBSpec{{Alias: "a", Path: a}, {Alias: "b", Path: b}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	defer pools.Close()

	_, err = describeDatasetHandler(context.Background(), pools, DescribeDatasetArgs{DatasetID: "aaaa-0001"})
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Errorf("want ambiguous error, got %v", err)
	}
	// Disambiguating with portal works
	got, err := describeDatasetHandler(context.Background(), pools, DescribeDatasetArgs{DatasetID: "aaaa-0001", Portal: "b"})
	if err != nil {
		t.Fatalf("disambiguated: %v", err)
	}
	if got.Name != "B's crimes" {
		t.Errorf("got %q", got.Name)
	}
}

func TestDescribeDataset_UnknownPortal(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	defer cleanup()

	_, err := describeDatasetHandler(context.Background(), pools, DescribeDatasetArgs{DatasetID: "aaaa-0001", Portal: "nope"})
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Errorf("want unknown-portal error mentioning nope, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcpserver/ -run TestDescribeDataset -v`
Expected: FAIL — `describeDatasetHandler` / `DescribeDatasetArgs` / `DatasetDetail` undefined.

- [ ] **Step 3: Write tools_describe_dataset.go**

Create `internal/mcpserver/tools_describe_dataset.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// DescribeDatasetArgs are the inputs to the describe_dataset MCP tool.
type DescribeDatasetArgs struct {
	DatasetID string `json:"dataset_id" jsonschema:"4x4 Socrata id"`
	Portal    string `json:"portal,omitempty" jsonschema:"required only when dataset_id appears in multiple portals"`
}

// DatasetDetail is the output of describe_dataset. Embeds DatasetSummary fields.
type DatasetDetail struct {
	DatasetSummary
	Description  string       `json:"description,omitempty"`
	Tags         []string     `json:"tags,omitempty"`
	Columns      []ColumnInfo `json:"columns"`
	LastSync     *SyncInfo    `json:"last_sync,omitempty"`
	HWMUpdatedAt *time.Time   `json:"hwm_updated_at,omitempty"`
}

// ColumnInfo names a single user-visible DuckDB column.
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// SyncInfo summarises the last successful sync_runs row.
type SyncInfo struct {
	RunID       string    `json:"run_id"`
	StartedAt   time.Time `json:"started_at"`
	Status      string    `json:"status"`
	RowsWritten int64     `json:"rows_written"`
	DurationMs  int64     `json:"duration_ms"`
}

// describeDatasetHandler returns the merged catalog + columns + last-sync detail
// for the requested dataset. Errors when the id is not found or when it is
// ambiguous across portals and no portal is specified.
func describeDatasetHandler(ctx context.Context, p *Pools, args DescribeDatasetArgs) (DatasetDetail, error) {
	if args.Portal != "" {
		if _, ok := p.Portals[args.Portal]; !ok {
			return DatasetDetail{}, fmt.Errorf("portal %q not attached", args.Portal)
		}
	}

	matches, err := findDatasetPortals(ctx, p, args.DatasetID, args.Portal)
	if err != nil {
		return DatasetDetail{}, err
	}
	if len(matches) == 0 {
		return DatasetDetail{}, fmt.Errorf("dataset %q not found", args.DatasetID)
	}
	if len(matches) > 1 {
		return DatasetDetail{}, fmt.Errorf("ambiguous dataset_id %q present in portals %s; pass portal=",
			args.DatasetID, strings.Join(matches, ", "))
	}
	alias := matches[0]
	return loadDetail(ctx, p, alias, args.DatasetID)
}

// findDatasetPortals returns the portals that contain the given dataset_id,
// optionally restricted to a single portal.
func findDatasetPortals(ctx context.Context, p *Pools, id, portal string) ([]string, error) {
	if portal != "" {
		exists, err := datasetExists(ctx, p.Portals[portal].RO, id)
		if err != nil {
			return nil, err
		}
		if exists {
			return []string{portal}, nil
		}
		return nil, nil
	}
	var out []string
	for _, alias := range sortedPortals(p) {
		exists, err := datasetExists(ctx, p.Portals[alias].RO, id)
		if err != nil {
			return nil, err
		}
		if exists {
			out = append(out, alias)
		}
	}
	return out, nil
}

func datasetExists(ctx context.Context, db *sql.DB, id string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM _csq.catalog WHERE id = $1`, id).Scan(&n)
	return n > 0, err
}

func sortedPortals(p *Pools) []string {
	out := make([]string, 0, len(p.Portals))
	for a := range p.Portals {
		out = append(out, a)
	}
	// stable order
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// loadDetail builds the full DatasetDetail for one (alias, id) pair.
func loadDetail(ctx context.Context, p *Pools, alias, id string) (DatasetDetail, error) {
	pool := p.Portals[alias].RO
	d := DatasetDetail{}
	d.DatasetID = id
	d.Portal = alias

	var name, description, category string
	var tagsRaw sql.NullString
	err := pool.QueryRowContext(ctx,
		`SELECT name, COALESCE(description, ''), COALESCE(category, ''), tags
		 FROM _csq.catalog WHERE id = $1`, id).Scan(&name, &description, &category, &tagsRaw)
	if err != nil {
		return d, fmt.Errorf("read catalog: %w", err)
	}
	d.Name = name
	d.Description = description
	d.Category = category
	if tagsRaw.Valid && tagsRaw.String != "" {
		var tags []string
		if err := json.Unmarshal([]byte(tagsRaw.String), &tags); err == nil {
			d.Tags = tags
		}
	}

	// last successful sync_runs row
	var runID, status string
	var startedAt time.Time
	var rowsWritten, duration sql.NullInt64
	var tableName sql.NullString
	err = pool.QueryRowContext(ctx,
		`SELECT run_id, table_name, started_at, status, rows_written, duration_ms
		 FROM _csq.sync_runs
		 WHERE dataset_id = $1 AND status = 'ok'
		 ORDER BY started_at DESC LIMIT 1`, id).Scan(
		&runID, &tableName, &startedAt, &status, &rowsWritten, &duration)
	if err != nil && err != sql.ErrNoRows {
		return d, fmt.Errorf("read sync_runs: %w", err)
	}
	if err == nil {
		d.LastSync = &SyncInfo{
			RunID:       runID,
			StartedAt:   startedAt,
			Status:      status,
			RowsWritten: rowsWritten.Int64,
			DurationMs:  duration.Int64,
		}
		if rowsWritten.Valid {
			n := rowsWritten.Int64
			d.RowCount = &n
		}
		if tableName.Valid {
			d.TableName = tableName.String
		}
	}
	if d.TableName == "" {
		d.TableName = strings.ReplaceAll(id, "-", "_")
	}

	// HWM
	var hwm sql.NullTime
	err = pool.QueryRowContext(ctx,
		`SELECT hwm_updated_at FROM _csq.dataset_state WHERE dataset_id = $1`, id).Scan(&hwm)
	if err != nil && err != sql.ErrNoRows {
		return d, fmt.Errorf("read dataset_state: %w", err)
	}
	if hwm.Valid {
		t := hwm.Time
		d.HWMUpdatedAt = &t
	}

	// Columns from information_schema, excluding socrata_id
	cols, err := readColumns(ctx, pool, d.TableName)
	if err != nil {
		return d, err
	}
	d.Columns = cols

	return d, nil
}

func readColumns(ctx context.Context, db *sql.DB, table string) ([]ColumnInfo, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT column_name, data_type FROM information_schema.columns
		 WHERE table_schema = 'main' AND table_name = $1
		 ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, fmt.Errorf("read columns for %s: %w", table, err)
	}
	defer rows.Close()
	var out []ColumnInfo
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		if name == "socrata_id" {
			continue
		}
		out = append(out, ColumnInfo{Name: name, Type: typ})
	}
	return out, rows.Err()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcpserver/ -run TestDescribeDataset -v`
Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/mcpserver/tools_describe_dataset.go internal/mcpserver/tools_describe_dataset_test.go
git commit -m "mcpserver: add describe_dataset tool handler"
```

---

## Task 7: `search_datasets` tool

Substring match on name+description, exact match on tags. Same `DatasetSummary` output as `list_datasets`.

**Files:**
- Create: `internal/mcpserver/tools_search_datasets.go`
- Create: `internal/mcpserver/tools_search_datasets_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/mcpserver/tools_search_datasets_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"sort"
	"strings"
	"testing"
)

func TestSearch_NameSubstring(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "Chicago Crimes"},
		FixtureDataset{ID: "bbbb-0002", Name: "Park Events"})
	defer cleanup()

	got, err := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: "crime"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		t.Errorf("got %v", ids(got))
	}
}

func TestSearch_DescriptionSubstring(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X", Description: "All things crime"},
		FixtureDataset{ID: "bbbb-0002", Name: "Y", Description: "Parks data"})
	defer cleanup()

	got, _ := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: "crime"})
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		t.Errorf("got %v", ids(got))
	}
}

func TestSearch_TagExactInsensitive(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "A", Tags: []string{"311", "crime"}},
		FixtureDataset{ID: "bbbb-0002", Name: "B", Tags: []string{"parks"}})
	defer cleanup()

	got, _ := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: "CRIME"})
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		t.Errorf("got %v", ids(got))
	}
}

func TestSearch_PortalScopes(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "Crimes A"})
	defer cleanup()

	got, _ := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: "crime", Portal: "missing"})
	if len(got) != 0 {
		t.Errorf("portal filter should narrow to zero, got %d", len(got))
	}
}

func TestSearch_EmptyQueryErrors(t *testing.T) {
	pools, cleanup := openFixturePools(t)
	defer cleanup()

	_, err := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: ""})
	if err == nil || !strings.Contains(err.Error(), "query") {
		t.Errorf("want empty-query error, got %v", err)
	}
}

func ids(in []DatasetSummary) []string {
	out := []string{}
	for _, d := range in {
		out = append(out, d.DatasetID)
	}
	sort.Strings(out)
	return out
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcpserver/ -run TestSearch -v`
Expected: FAIL — `searchDatasetsHandler`/`SearchDatasetsArgs` undefined.

- [ ] **Step 3: Write tools_search_datasets.go**

Create `internal/mcpserver/tools_search_datasets.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// SearchDatasetsArgs are the inputs to search_datasets.
type SearchDatasetsArgs struct {
	Query  string `json:"query" jsonschema:"substring to match against name and description; also matches tags case-insensitively"`
	Portal string `json:"portal,omitempty" jsonschema:"optional portal alias filter"`
}

// searchDatasetsHandler returns datasets whose name or description contain the
// query (case-insensitive substring) or whose tag list contains the query
// (case-insensitive exact match).
func searchDatasetsHandler(ctx context.Context, p *Pools, args SearchDatasetsArgs) ([]DatasetSummary, error) {
	if strings.TrimSpace(args.Query) == "" {
		return nil, fmt.Errorf("query must not be empty")
	}
	all, err := listDatasetsHandler(ctx, p, ListDatasetsArgs{Portal: args.Portal})
	if err != nil {
		return nil, err
	}
	needle := strings.ToLower(args.Query)
	out := make([]DatasetSummary, 0, len(all))
	for _, d := range all {
		matched := strings.Contains(strings.ToLower(d.Name), needle)
		if !matched {
			matched = matchesDescriptionOrTag(ctx, p, d, needle)
		}
		if matched {
			out = append(out, d)
		}
	}
	return out, nil
}

// matchesDescriptionOrTag fetches the catalog row's description and tags and
// applies the search rule. Pulled out so the listDatasets path stays cheap.
func matchesDescriptionOrTag(ctx context.Context, p *Pools, d DatasetSummary, needle string) bool {
	pool := p.Portals[d.Portal].RO
	var description string
	var tagsRaw string
	err := pool.QueryRowContext(ctx,
		`SELECT COALESCE(description, ''), COALESCE(CAST(tags AS VARCHAR), '[]')
		 FROM _csq.catalog WHERE id = $1`, d.DatasetID).Scan(&description, &tagsRaw)
	if err != nil {
		return false
	}
	if strings.Contains(strings.ToLower(description), needle) {
		return true
	}
	var tags []string
	if json.Unmarshal([]byte(tagsRaw), &tags) == nil {
		for _, t := range tags {
			if strings.EqualFold(t, needle) {
				return true
			}
		}
	}
	return false
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcpserver/ -run TestSearch -v`
Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/mcpserver/tools_search_datasets.go internal/mcpserver/tools_search_datasets_test.go
git commit -m "mcpserver: add search_datasets tool handler"
```

---

## Task 8: `query_sql` tool

Read-only SQL execution against the host (cross-portal joins via `<alias>.<schema>.<table>`). Result caps: 1000 rows or 1MB JSON. 30s timeout.

**Files:**
- Create: `internal/mcpserver/tools_query_sql.go`
- Create: `internal/mcpserver/tools_query_sql_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/mcpserver/tools_query_sql_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestQuerySQL_HappySelect(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{
			ID: "aaaa-0001", Name: "Crimes",
			TableName: "crimes",
			ColumnDefs: []string{"socrata_id VARCHAR", "score DOUBLE"},
			Rows: []map[string]any{
				{"socrata_id": "a", "score": 1.0},
				{"socrata_id": "b", "score": 2.0},
			},
		})
	defer cleanup()

	got, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `SELECT socrata_id, score FROM test.main.crimes ORDER BY socrata_id`}, time.Second)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got.RowCount != 2 || got.Truncated {
		t.Errorf("rowcount=%d truncated=%v", got.RowCount, got.Truncated)
	}
	if len(got.Columns) != 2 || got.Columns[0] != "socrata_id" {
		t.Errorf("columns: %v", got.Columns)
	}
	if len(got.Rows) != 2 {
		t.Errorf("rows: got %d", len(got.Rows))
	}
}

func TestQuerySQL_RejectsWrites(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	defer cleanup()

	_, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `CREATE TABLE main.evil (x INT)`}, time.Second)
	if err == nil {
		t.Fatal("CREATE TABLE should be rejected")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "read") {
		t.Errorf("error should mention read-only: %v", err)
	}
}

func TestQuerySQL_TruncatesByRowCap(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	defer cleanup()

	// Use a generate_series query to make 2000 synthetic rows
	got, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `SELECT * FROM range(0, 2000)`}, 5*time.Second)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !got.Truncated {
		t.Errorf("expected truncated=true at 1000-row cap")
	}
	if got.RowCount != 1000 {
		t.Errorf("rowcount: got %d, want 1000", got.RowCount)
	}
	if got.Note == "" {
		t.Errorf("expected a note explaining truncation")
	}
}

func TestQuerySQL_Timeout(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	defer cleanup()

	// 1ms timeout against a query that takes longer than that to return any rows
	_, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `SELECT * FROM range(0, 100000000)`}, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Errorf("want timeout error, got %v", err)
	}
}

func TestQuerySQL_CrossPortal(t *testing.T) {
	dir := t.TempDir()
	a := seedFixtureDB(t, dir, "a.duckdb",
		FixtureDataset{
			ID: "aaaa-0001", Name: "Aw",
			TableName: "items", ColumnDefs: []string{"id VARCHAR"},
			Rows: []map[string]any{{"id": "x"}},
		})
	b := seedFixtureDB(t, dir, "b.duckdb",
		FixtureDataset{
			ID: "bbbb-0001", Name: "Bw",
			TableName: "items", ColumnDefs: []string{"id VARCHAR"},
			Rows: []map[string]any{{"id": "x"}},
		})
	pools, err := OpenPools([]DBSpec{{Alias: "a", Path: a}, {Alias: "b", Path: b}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	defer pools.Close()

	got, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `SELECT a.id FROM a.main.items a JOIN b.main.items b ON a.id = b.id`}, time.Second)
	if err != nil {
		t.Fatalf("cross-portal: %v", err)
	}
	if got.RowCount != 1 {
		t.Errorf("want 1 row, got %d", got.RowCount)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcpserver/ -run TestQuerySQL -v`
Expected: FAIL — `querySQLHandler`/`QuerySQLArgs`/`QuerySQLResult` undefined.

- [ ] **Step 3: Write tools_query_sql.go**

Create `internal/mcpserver/tools_query_sql.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	maxRows  = 1000
	maxBytes = 1 << 20 // 1 MB
)

// QuerySQLArgs is the input to query_sql.
type QuerySQLArgs struct {
	SQL string `json:"sql" jsonschema:"DuckDB SQL; runs read-only against the host DB with each portal ATTACHed as <alias>"`
}

// QuerySQLResult is the output of query_sql.
type QuerySQLResult struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated"`
	Note      string   `json:"note,omitempty"`
}

// querySQLHandler executes args.SQL against the host inside a read-only
// transaction (DuckDB rejects DDL/DML at the engine level), capping the result
// at maxRows or maxBytes (whichever first), and aborting after timeout.
//
// We can't open the host as access_mode=READ_ONLY because the host is :memory:
// and needs to be writeable on startup so we can ATTACH each portal. The
// "BEGIN TRANSACTION READ ONLY" wrapper gives the same engine-level guarantee
// for the duration of one query. We acquire a single *sql.Conn so the BEGIN
// and the SELECT share the same physical connection — database/sql's *sql.Tx
// helpers don't expose DuckDB's read-only flag.
func querySQLHandler(parent context.Context, p *Pools, args QuerySQLArgs, timeout time.Duration) (QuerySQLResult, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	conn, err := p.Host.Conn(ctx)
	if err != nil {
		return QuerySQLResult{}, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `BEGIN TRANSACTION READ ONLY`); err != nil {
		return QuerySQLResult{}, fmt.Errorf("begin read-only tx: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `ROLLBACK`) }()

	rows, err := conn.QueryContext(ctx, args.SQL)
	if err != nil {
		return QuerySQLResult{}, formatQueryError(ctx, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return QuerySQLResult{}, fmt.Errorf("columns: %w", err)
	}

	out := QuerySQLResult{Columns: cols, Rows: [][]any{}}
	approxBytes := len(`{"columns":[],"rows":[],"row_count":0,"truncated":false}`)
	for _, c := range cols {
		approxBytes += len(c) + 3
	}

	for rows.Next() {
		if out.RowCount >= maxRows {
			out.Truncated = true
			out.Note = fmt.Sprintf("result truncated at %d rows; add LIMIT to your query", maxRows)
			break
		}
		row, err := scanRow(rows, len(cols))
		if err != nil {
			return QuerySQLResult{}, fmt.Errorf("scan row %d: %w", out.RowCount, err)
		}
		// Estimate added bytes (rough JSON size)
		b, _ := json.Marshal(row)
		if approxBytes+len(b)+1 > maxBytes && out.RowCount > 0 {
			out.Truncated = true
			out.Note = fmt.Sprintf("result truncated at ~%d bytes; add LIMIT or SELECT fewer columns", maxBytes)
			break
		}
		approxBytes += len(b) + 1
		out.Rows = append(out.Rows, row)
		out.RowCount++
	}
	if err := rows.Err(); err != nil {
		return QuerySQLResult{}, formatQueryError(ctx, err)
	}
	return out, nil
}

func scanRow(rows *sql.Rows, n int) ([]any, error) {
	cells := make([]any, n)
	ptrs := make([]any, n)
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	// Coerce []byte to string for JSON friendliness
	for i, v := range cells {
		if b, ok := v.([]byte); ok {
			cells[i] = string(b)
		}
	}
	return cells, nil
}

// formatQueryError translates context cancellation into a clear timeout
// message; otherwise returns the underlying error verbatim (DuckDB's messages
// are already user-friendly).
func formatQueryError(ctx context.Context, err error) error {
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("query exceeded timeout")
	}
	if strings.Contains(err.Error(), "context deadline exceeded") {
		return fmt.Errorf("query exceeded timeout")
	}
	return err
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcpserver/ -run TestQuerySQL -v`
Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/mcpserver/tools_query_sql.go internal/mcpserver/tools_query_sql_test.go
git commit -m "mcpserver: add query_sql tool handler with caps + timeout"
```

---

## Task 9: `Serve` entry point + tool registration

Builds `Pools`, constructs `*mcp.Server`, registers all four tools, runs the chosen transport (stdio default; HTTP when `Options.HTTPAddr != ""`).

**Files:**
- Create: `internal/mcpserver/server.go`
- Create: `internal/mcpserver/server_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/mcpserver/server_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestServe_RegistersFourTools(t *testing.T) {
	// Construct a Server in isolation (no transport), to verify all four
	// tools register without panicking and the schemas resolve.
	dir := t.TempDir()
	path := seedFixtureDB(t, dir, "test.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	pools, err := OpenPools([]DBSpec{{Alias: "test", Path: path}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	defer pools.Close()

	srv, err := buildServer(pools)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	if srv == nil {
		t.Fatal("nil server")
	}
}

func TestServe_HTTPSmoke(t *testing.T) {
	dir := t.TempDir()
	path := seedFixtureDB(t, dir, "test.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run Serve on an in-process httptest server.
	pools, err := OpenPools([]DBSpec{{Alias: "test", Path: path}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	defer pools.Close()

	srv, err := buildServer(pools)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	handler := newHTTPHandler(srv)
	httpsrv := httptest.NewServer(handler)
	defer httpsrv.Close()

	// Send a tools/list JSON-RPC request and assert four tool names appear.
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`
	req, _ := http.NewRequestWithContext(ctx, "POST", httpsrv.URL, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}

	buf := make([]byte, 64*1024)
	n, _ := resp.Body.Read(buf)
	got := string(buf[:n])
	for _, name := range []string{"list_datasets", "describe_dataset", "search_datasets", "query_sql"} {
		if !strings.Contains(got, name) {
			t.Errorf("response missing tool %q:\n%s", name, got)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/mcpserver/ -run TestServe -v`
Expected: FAIL — `buildServer`, `newHTTPHandler` undefined.

- [ ] **Step 3: Write server.go**

Create `internal/mcpserver/server.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// queryTimeout is the per-query timeout enforced by query_sql.
const queryTimeout = 30 * time.Second

// Options configures the MCP server.
type Options struct {
	DBs      []DBSpec // resolved (alias, path) pairs; required, non-empty
	HTTPAddr string   // empty means stdio; non-empty switches to HTTP
}

// Serve constructs the server, opens pools, registers all tools, and runs the
// chosen transport. Blocks until the context is cancelled or the transport
// returns. Pools are closed before returning.
func Serve(ctx context.Context, opts Options) error {
	pools, err := OpenPools(opts.DBs)
	if err != nil {
		return err
	}
	defer pools.Close()

	srv, err := buildServer(pools)
	if err != nil {
		return err
	}

	if opts.HTTPAddr != "" {
		return runHTTP(ctx, srv, opts.HTTPAddr)
	}
	return srv.Run(ctx, &mcp.StdioTransport{})
}

// buildServer creates an *mcp.Server and registers all four tools with the
// given pools captured in the handler closures.
func buildServer(pools *Pools) (*mcp.Server, error) {
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "civicsodaquack",
		Version: "0.3.0",
	}, nil)

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "list_datasets",
		Description: "List datasets available across attached portal DuckDB files. Use the optional 'portal' or 'category' filters to narrow the result.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args ListDatasetsArgs) (*mcp.CallToolResult, []DatasetSummary, error) {
		out, err := listDatasetsHandler(ctx, pools, args)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, out, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "describe_dataset",
		Description: "Return columns, last sync info, and tags for one dataset. Pass 'portal' if dataset_id is ambiguous across portals.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args DescribeDatasetArgs) (*mcp.CallToolResult, DatasetDetail, error) {
		out, err := describeDatasetHandler(ctx, pools, args)
		if err != nil {
			return nil, DatasetDetail{}, err
		}
		return &mcp.CallToolResult{}, out, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "search_datasets",
		Description: "Substring match on dataset name, description, and tags (case-insensitive).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args SearchDatasetsArgs) (*mcp.CallToolResult, []DatasetSummary, error) {
		out, err := searchDatasetsHandler(ctx, pools, args)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{}, out, nil
	})

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "query_sql",
		Description: "Run a read-only DuckDB SELECT across all attached portals. Cross-portal queries: <alias>.<schema>.<table>. Capped at 1000 rows / 1MB / 30s.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args QuerySQLArgs) (*mcp.CallToolResult, QuerySQLResult, error) {
		out, err := querySQLHandler(ctx, pools, args, queryTimeout)
		if err != nil {
			return nil, QuerySQLResult{}, err
		}
		// Provide a text-content rendering as well, for clients that don't
		// process structured output.
		body, _ := json.Marshal(out)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(body)}},
		}, out, nil
	})

	return srv, nil
}

// newHTTPHandler returns an http.Handler that serves the given MCP server via
// the SDK's StreamableHTTP transport.
func newHTTPHandler(srv *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return srv
	}, nil)
}

// runHTTP listens on addr and serves the MCP server over HTTP. Blocks until ctx
// is cancelled or the listener errors.
func runHTTP(ctx context.Context, srv *mcp.Server, addr string) error {
	httpsrv := &http.Server{
		Addr:    addr,
		Handler: newHTTPHandler(srv),
	}

	errCh := make(chan error, 1)
	go func() { errCh <- httpsrv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpsrv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return fmt.Errorf("http listen %s: %w", addr, err)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcpserver/ -run TestServe -v`
Expected: both tests pass.

- [ ] **Step 5: Run the full mcpserver suite**

Run: `go test ./internal/mcpserver/ -v`
Expected: all tests across attach/pools/list/describe/search/query/server pass.

- [ ] **Step 6: Commit**

```bash
git add internal/mcpserver/server.go internal/mcpserver/server_test.go
git commit -m "mcpserver: add Serve entry point + buildServer wiring all four tools"
```

---

## Task 10: `csq mcp` CLI subcommand

**Files:**
- Create: `cmd/csq/mcp.go`
- Modify: `cmd/csq/main.go`

- [ ] **Step 1: Write mcp.go**

Create `cmd/csq/mcp.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/mcpserver"
)

func runMCP(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	var (
		dbs      []string
		httpAddr string
	)
	fs.StringArrayVar(&dbs, "db", nil, "Portal DuckDB to attach: 'path.duckdb' or 'alias=path.duckdb' (repeatable)")
	fs.StringVar(&httpAddr, "http", "", "Listen on this address for HTTP/SSE transport (default: stdio)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	specs, err := mcpserver.ResolveDBSpecs(dbs)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if httpAddr != "" {
		fmt.Fprintf(os.Stderr, "[csq] mcp serving on http://%s with %d portal(s)\n", httpAddr, len(specs))
	} else {
		fmt.Fprintf(os.Stderr, "[csq] mcp serving on stdio with %d portal(s)\n", len(specs))
	}

	return mcpserver.Serve(ctx, mcpserver.Options{
		DBs:      specs,
		HTTPAddr: httpAddr,
	})
}
```

- [ ] **Step 2: Wire dispatch in main.go**

Edit `cmd/csq/main.go`. Update the usage const and add the dispatch case.

Replace the `usage` const with:

```go
const usage = `csq — CivicSodaQuack

Usage:
  csq extract --portal <host> --dataset <4x4> [options]
  csq catalog --portal <host> [--refresh] [--json] [--output FILE]
  csq sync    --config <portal.yaml> [--dry-run] [--only IDs]
  csq mcp     --db <portal.duckdb> [--db ...] [--http <addr>]

Examples:
  csq extract --portal data.cityofchicago.org --dataset 6zsd-86xi --limit 10000
  csq catalog --portal data.cityofchicago.org --category "Public Safety"
  csq sync    --config data.cityofchicago.org.yaml
  csq mcp     --db data.cityofchicago.org.duckdb --db nyc=data.cityofnewyork.us.duckdb
`
```

In the `switch os.Args[1]` block, add a case for `"mcp"` after the existing `"sync"` case:

```go
	case "mcp":
		if err := runMCP(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "csq mcp: %v\n", err)
			os.Exit(1)
		}
```

- [ ] **Step 3: Build**

Run: `go build -o csq ./cmd/csq`
Expected: clean.

- [ ] **Step 4: Smoke the help / arg validation**

Run: `./csq` (no args)
Expected: prints usage including the new `csq mcp` line; exit 2.

Run: `./csq mcp` (no `--db`)
Expected: prints `csq mcp: at least one --db is required`; exit 1.

- [ ] **Step 5: Commit**

```bash
git add cmd/csq/mcp.go cmd/csq/main.go
git commit -m "cli: add csq mcp subcommand"
```

---

## Task 11: End-to-end CLI smoke (stdio)

Black-box test: start `csq mcp` with a fixture portal, send a `tools/list` JSON-RPC over stdin, read stdout, assert all four tool names appear.

**Files:**
- Modify: `cmd/csq/cli_smoke_test.go`

- [ ] **Step 1: Add the smoke test**

Append to `cmd/csq/cli_smoke_test.go`:

```go
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
```

Add the missing imports at the top of `cli_smoke_test.go` if not already present:
- `"io"`
- `"time"` (likely already there)

- [ ] **Step 2: Run the smoke test**

Run: `go test ./cmd/csq/ -run TestCSQ_MCP_Stdio_Smoke -v`
Expected: PASS (within 5s).

- [ ] **Step 3: Run the full CLI suite to confirm Phase 1/2 smokes still pass**

Run: `go test ./cmd/csq/ -v`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add cmd/csq/cli_smoke_test.go
git commit -m "cli: add stdio smoke test for csq mcp"
```

---

## Task 12: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update Status and add MCP section**

Edit `README.md`. Replace the `## Status` line:

```markdown
## Status

**Phase 3** — MCP server. After syncing one or more portals into per-portal DuckDB files, run `csq mcp` to expose them to AI agents over stdio or HTTP.
```

Below the existing `csq sync` quickstart commands, append:

```markdown
### Serve via MCP

```bash
# Stdio (default; for local agent integrations)
./csq mcp --db data.cityofchicago.org.duckdb

# Multi-portal with explicit alias
./csq mcp --db chicago=data.cityofchicago.org.duckdb \
          --db nyc=data.cityofnewyork.us.duckdb

# HTTP (for remote agents; bind to loopback by default)
./csq mcp --db data.cityofchicago.org.duckdb --http 127.0.0.1:8080
```

The MCP server exposes four tools: `list_datasets`, `describe_dataset`, `search_datasets`, and `query_sql`. The `query_sql` tool runs read-only DuckDB SQL across every attached portal; cross-portal queries use `<alias>.<schema>.<table>`, e.g. `SELECT * FROM chicago._csq.catalog UNION ALL SELECT * FROM nyc._csq.catalog`. Results are capped at 1000 rows / 1MB / 30s.
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
```
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: Phase 3 README — csq mcp + multi-portal usage"
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
# Use an existing synced DB from Phase 2
./csq mcp --db data.cityofchicago.org.duckdb --http 127.0.0.1:8080
# In another terminal:
curl -s -X POST http://127.0.0.1:8080 \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

Expected: response lists `list_datasets`, `describe_dataset`, `search_datasets`, `query_sql`.
