# Phase 3 — MCP server with multi-portal ATTACH

**Status:** design approved, awaiting implementation plan.
**Date:** 2026-04-23.
**Prior art:** Phase 1 (`csq sync`, full-replace bulk sync), Phase 2 (`csq mcp` would be the third subcommand; per-dataset HWMs in `_csq.dataset_state`).

## Summary

Phase 3 adds `csq mcp`, a long-running MCP server that exposes the per-portal DuckDB files an agent can use to discover and query civic data. One server process can attach multiple portal DBs at once (via repeated `--db` flags) so cross-portal SQL joins work naturally. Four typed tools cover discovery (`list_datasets`, `describe_dataset`, `search_datasets`) and ad-hoc analysis (`query_sql`). Both stdio (default) and HTTP transports are supported.

The server uses two `*sql.DB` pools per portal file: one read-only (for all Phase 3 tools) and one read-write (idle in Phase 3, reserved for future write tools like `sync_dataset`). This dual-pool pattern lets the MCP server hold the file lock for its lifetime while still enforcing read-only semantics on `query_sql` at the engine level.

Phase 3 is read-only. No tool writes to `_csq.*` or to dataset tables. Future phases can add write tools without restructuring the server.

## Goals

- Single MCP server process that attaches one or more per-portal DuckDB files.
- Cross-portal SQL queries via DuckDB ATTACH (`SELECT ... FROM <alias>.<table>`).
- Four agent-friendly tools mapped to typical exploration flow: list → describe → search → query.
- Engine-enforced read-only `query_sql` (no SQL parsing or allowlist regex).
- Result-size and time bounds on `query_sql` to keep agents safe from runaway queries.
- Both stdio and HTTP transports; same tool surface either way.
- Filename-derived portal aliases with an explicit-alias escape hatch (`--db alias=path.duckdb`).
- Dual read-only / read-write pools per portal, so future write tools can be added without rewriting connection handling.

## Non-goals

- Write tools (`sync_dataset`, `refresh_catalog`) — Phase 4.
- Any persistent server state (query log, session storage). The MCP server is stateless.
- Authentication / authorization. The MCP server runs as a local process under the user's identity; HTTP transport binds to a user-chosen address (typically loopback).
- SQL parsing, allowlists, or query rewriting. DuckDB's `access_mode=READ_ONLY` is the only enforcement layer.
- A `list_portals()` tool. The portal alias is on every result row from the discovery tools, which is sufficient.
- A `sample_rows(id, n)` tool. One `SELECT * FROM <portal>.<table> LIMIT n` via `query_sql` covers it.
- Snapshot publishing (Phase 4).
- Concurrency / connection pooling beyond what `*sql.DB` provides out of the box. One in-flight query per pool at a time is fine for Phase 3.

## Architecture

### Process model

`csq mcp` is a long-running stdio (or HTTP) MCP server. On startup it:

1. Parses `--db <path>` flags (repeatable) and `--db <alias>=<path>` overrides.
2. Opens an in-memory DuckDB ("host DB"). All MCP tools route through the host's read-only connection.
3. For each `--db` arg, opens two `*sql.DB` pools to the same file:
   - Read-only pool (`access_mode=READ_ONLY`) — used by tool implementations.
   - Read-write pool (`access_mode=READ_WRITE`) — opened so the file lock is held for the process lifetime; idle in Phase 3.
4. ATTACHes each portal DB to the host as `<alias>` (`ATTACH '<path>' AS <alias> (READ_ONLY)`). The host gets read-only access to every portal table.
5. Validates the file is a CivicSodaQuack DB by querying for `_csq.catalog`.
6. Registers the four tools and runs the chosen transport (stdio default, `--http :8080` switches to HTTP).

The dual-pool pattern matters because DuckDB's access mode is per-database-handle, not per-connection. The read-only pool gives engine-level guarantees for `query_sql`; the idle read-write pool exists so future write tools can be added without changing the connection topology.

### Transport choice

- Default: stdio.
- `--http <addr>` (e.g. `--http 127.0.0.1:8080`) → switches to the SDK's HTTP/SSE transport. No separate `--transport` flag; presence of `--http` is the signal.

Tool surface and behavior are identical across transports.

### Boundaries

- `internal/mcpserver` package: MCP protocol handling and tool implementations. Imports `database/sql` and `github.com/modelcontextprotocol/go-sdk/mcp`. Does not import `internal/sync`, `internal/socrata`, or `internal/config`.
- `cmd/csq`: CLI parsing only; calls `mcpserver.Serve(...)`.
- `internal/duckdb`: untouched. Phase 3 reads `_csq.catalog`, `_csq.dataset_state`, and `information_schema.columns` directly via SQL.

## Components

### `internal/mcpserver/server.go`

`Serve(ctx context.Context, opts Options) error` — entry point. Constructs `Pools`, ATTACHes each portal, registers tools via `mcp.AddTool[In,Out]`, runs the chosen transport.

```go
type Options struct {
    DBs      []DBSpec // resolved (path + alias) pairs
    HTTPAddr string   // empty means stdio; non-empty switches to HTTP
}

type DBSpec struct {
    Path  string
    Alias string
}
```

### `internal/mcpserver/pools.go`

`Pools` struct owns the host DB plus per-portal read-only/read-write pairs:

```go
type Pools struct {
    Host    *sql.DB                       // in-memory; ATTACHes everything read-only
    Portals map[string]*PortalPools       // keyed by alias
}

type PortalPools struct {
    Path string
    RO   *sql.DB
    RW   *sql.DB
}

func OpenPools(specs []DBSpec) (*Pools, error)
func (p *Pools) Close() error
```

`OpenPools` validates each file (presence of `_csq.catalog`), opens both pools, opens the host, and ATTACHes each portal to the host.

### `internal/mcpserver/attach.go`

`ResolveDBSpecs(args []string) ([]DBSpec, error)` — converts the raw `--db` arg list into resolved `DBSpec` records. Handles:
- Plain path → filename-derived alias (basename minus extension, dots replaced with underscores).
- `alias=path` → explicit alias.
- Validates aliases match `^[a-zA-Z_][a-zA-Z0-9_]*$`.
- Rejects collisions across the resolved set.

### `internal/mcpserver/tools_list_datasets.go`

```go
type ListDatasetsArgs struct {
    Portal   string `json:"portal,omitempty" jsonschema:"optional portal alias filter"`
    Category string `json:"category,omitempty" jsonschema:"optional category substring (case-insensitive)"`
}

type DatasetSummary struct {
    DatasetID string `json:"dataset_id"`
    Portal    string `json:"portal"`
    Name      string `json:"name"`
    Category  string `json:"category,omitempty"`
    TableName string `json:"table_name"`
    RowCount  *int64 `json:"row_count,omitempty"` // last successful sync's rows_written; nil if no successful run
}
```

Implementation: iterate the requested portal (or all portals when unset), `SELECT id, name, category FROM <alias>._csq.catalog`, LEFT JOIN `_csq.sync_runs` for the most recent `status='ok'` row to pull `rows_written` and `table_name`. When no successful sync row exists yet, fall back to `replace(id, '-', '_')` for the table name (Phase 1 default) and `nil` for `RowCount`. Apply `category` substring filter in Go (DuckDB doesn't have a built-in case-insensitive `LIKE` we want to depend on; a Go filter is simpler and the catalog is small).

Note: per-dataset YAML `table:` overrides are visible to the MCP server only after the first successful sync writes the resolved name into `_csq.sync_runs.table_name`. Datasets that have never synced report the default-derived name. This is acceptable for Phase 3 because the user has no reason to call MCP tools on never-synced datasets.

### `internal/mcpserver/tools_describe_dataset.go`

```go
type DescribeDatasetArgs struct {
    DatasetID string `json:"dataset_id" jsonschema:"4x4 Socrata id"`
    Portal    string `json:"portal,omitempty" jsonschema:"required only when dataset_id appears in multiple portals"`
}

type DatasetDetail struct {
    DatasetSummary
    Description  string       `json:"description,omitempty"`
    Tags         []string     `json:"tags,omitempty"`
    Columns      []ColumnInfo `json:"columns"`
    LastSync     *SyncInfo    `json:"last_sync,omitempty"`
    HWMUpdatedAt *time.Time   `json:"hwm_updated_at,omitempty"`
}

type ColumnInfo struct {
    Name string `json:"name"`
    Type string `json:"type"`
}

type SyncInfo struct {
    RunID       string    `json:"run_id"`
    StartedAt   time.Time `json:"started_at"`
    Status      string    `json:"status"`
    RowsWritten int64     `json:"rows_written"`
    DurationMs  int64     `json:"duration_ms"`
}
```

Implementation: identify the portal (search all when not specified; error if `dataset_id` matches in multiple portals without `portal`), pull catalog row, `information_schema.columns` for the table (excluding `socrata_id` from the user-visible list — Phase 2 added it but it's a system column), most recent `_csq.sync_runs` row for the dataset, and `_csq.dataset_state` HWM.

### `internal/mcpserver/tools_search_datasets.go`

```go
type SearchDatasetsArgs struct {
    Query  string `json:"query" jsonschema:"substring to match against name and description"`
    Portal string `json:"portal,omitempty" jsonschema:"optional portal alias filter"`
}
// Returns []DatasetSummary
```

Implementation: case-insensitive substring on `name` and `description`; exact (case-insensitive) match on tags. Same JOIN as `list_datasets` for the row count.

### `internal/mcpserver/tools_query_sql.go`

```go
type QuerySQLArgs struct {
    SQL string `json:"sql" jsonschema:"DuckDB SQL; runs read-only against the host DB with all portals ATTACHed"`
}
```

Returns a single text content block containing JSON:

```json
{
  "columns": ["socrata_id", "primary_type"],
  "rows": [["abc", "THEFT"], ["def", "BATTERY"]],
  "row_count": 2,
  "truncated": false
}
```

Implementation:
- Run with a derived context: `ctx, cancel := context.WithTimeout(parent, 30*time.Second)`.
- `db.QueryContext(ctx, sql)` against the host's read-only pool.
- Stream rows into a `[]any[]` slice; bail when row count hits 1000 OR serialized JSON exceeds 1MB.
- On truncation set `truncated: true` and a `note` field suggesting `LIMIT`.
- On any error (parse, runtime, timeout, read-only violation) return as a tool error with the DuckDB message verbatim.

### `cmd/csq/mcp.go`

`runMCP(args)`:
- `--db` (repeatable, required at least once)
- `--http <addr>` (optional; absence means stdio)
- Parses args, calls `mcpserver.ResolveDBSpecs` + `mcpserver.Serve`.
- On `Serve` error returns it for `cmd/csq/main.go` to log and exit 1.

### `cmd/csq/main.go`

Add `case "mcp"` dispatch and update the usage block.

## Tool semantics in detail

### `list_datasets`

- No arg → all datasets across all attached portals.
- `portal=chicago` → only Chicago.
- `category=safety` → case-insensitive substring on category.
- Both filters → AND.
- Empty result → returns `[]`, not an error.

### `describe_dataset`

- `dataset_id` only, present in one portal → return its detail.
- `dataset_id` present in multiple portals, no `portal` arg → tool error: `"ambiguous dataset_id 'X' present in portals A, B; pass portal="`.
- `dataset_id` not found anywhere → tool error: `"dataset 'X' not found"`.
- `portal=Y` for a portal not in the attached set → tool error naming Y.
- `dataset_id` is in the catalog but the table doesn't exist yet (no successful sync) → return detail with `Columns: []`, `LastSync: nil`. The `RowCount` from the embedded `DatasetSummary` is nil.

### `search_datasets`

- `query` is required; empty string is rejected with a tool error.
- Match logic: case-insensitive substring on name OR description, OR exact case-insensitive match on any tag.
- Empty result → returns `[]`.

### `query_sql`

- Engine read-only mode rejects writes; we surface the DuckDB error message verbatim.
- Cross-portal queries supported via `<alias>.<table>` syntax.
- `SELECT * FROM _csq.catalog` works (catalog is in the host as ATTACHed schema; spell as `<alias>.main._csq.catalog` — see the Open Question note below).
- Results cap: 1000 rows OR 1 MB JSON, whichever hits first. Truncated results return `truncated: true` and a `note` field, not an error.
- Timeout: 30s per call. Exceeded → tool error: `"query exceeded 30s timeout"`.
- The query runs against the **host DB** so it can join across portals. Tools should not query individual portal pools directly.

## Errors & failure modes

| Situation | Behavior |
|---|---|
| `--db` arg references a non-existent file | `Serve` errors before listening; CLI exits 1 with the path. |
| `--db` filename produces an alias that collides with another `--db` | Startup error naming both files; user resolves with `alias=path`. |
| Explicit alias not a valid SQL identifier (e.g. `--db 1bad=...`, `--db has-dash=...`) | Startup error with the offending alias. |
| `--db` file lacks `_csq.catalog` | Startup error: `"no _csq.catalog in <path>; not a CivicSodaQuack DuckDB"`. |
| Two portals have the same `dataset_id`, `describe_dataset` called without `portal` | Tool error: `"ambiguous dataset_id 'X' present in portals A, B; pass portal="`. |
| `dataset_id` not found anywhere | Tool error: `"dataset 'X' not found"`. |
| `query_sql` attempts to write | DuckDB returns `"cannot execute statement in read-only mode"`; propagated as tool error. |
| `query_sql` exceeds 30s | Context cancellation; tool error: `"query exceeded 30s timeout"`. |
| `query_sql` exceeds 1000 rows or 1MB JSON | Truncated; `truncated: true` and `note` returned (not an error). |
| MCP client disconnects mid-query | Context propagates cancellation; in-flight DuckDB query interrupted. |
| stdio: read error / EOF on stdin | Server shuts down cleanly. |
| HTTP: port already in use | Startup error from the listener. |
| One portal's read-only pool fails to open (corrupt file) | Startup error naming the portal; no partial start. |
| `--http <addr>` with malformed `<addr>` | CLI error from `net.Listen` propagated via `Serve`. |

**No audit / observability for Phase 3.** The MCP server doesn't write anywhere. A future `query_sql_log` table is out of scope.

## Testing

### Unit tests per tool (`internal/mcpserver/*_test.go`)

Each tool gets in-process tests against real ATTACH'd DuckDB files seeded with `_csq.catalog` / `_csq.dataset_state` / `_csq.sync_runs` / dataset-table fixtures. The MCP SDK exposes typed handlers, so tests call them directly without a transport.

- `list_datasets`: empty catalog → `[]`; one portal → expected rows; two portals → both portals' rows with correct aliases; `portal` filter narrows correctly; `category` filter is case-insensitive substring; `row_count` is nil when no successful sync exists.
- `describe_dataset`: returns columns from `information_schema` minus `socrata_id`; merges `_csq.sync_runs` last successful into `LastSync`; pulls `HWMUpdatedAt` from `_csq.dataset_state`; ambiguous dataset across two portals errors when `portal` is omitted; unknown id errors clearly; bootstrapped-but-no-data dataset returns nil `LastSync`.
- `search_datasets`: substring match on name AND description (case-insensitive); tag match (case-insensitive exact); `portal` scoping works; empty `query` errors.
- `query_sql`: happy SELECT; cross-portal join via aliases; write attempt (`CREATE TABLE x...`) errors with read-only message; >1000 rows → `truncated: true`; >1MB JSON → `truncated: true`; short ctx → timeout error; SQL parse error returns DuckDB message.

### Pool / attach unit tests

- `pools_test.go`: open one portal → both pools work; the read-write pool can `INSERT INTO _csq.dataset_state` and the read-only pool sees the change (validates dual-pool design on a real file).
- `attach_test.go`: filename → alias derivation (`data.cityofchicago.org.duckdb` → `data_cityofchicago_org`); alias collision detection across two `--db` args; explicit `alias=path` override; invalid alias rejection (digit-leading, dash, empty); missing `_csq.catalog` rejection.

### End-to-end CLI smoke

`TestCSQ_MCP_Stdio_Smoke` (in `cmd/csq/cli_smoke_test.go`):
- Build the binary via `TestMain` (already done for Phase 1/2 smoke tests).
- Spawn `csq mcp --db <fixture.duckdb>` with piped stdin/stdout.
- Send a hand-crafted MCP `initialize` request, assert response shape.
- Send `tools/list`, assert all four tool names present.
- Doesn't exercise tool execution (covered by unit tests); only validates the process boots and speaks JSON-RPC.

`TestCSQ_MCP_HTTP_Smoke`:
- In-process: call `mcpserver.Serve` with `HTTPAddr: "127.0.0.1:0"` (random port discovered after listener starts).
- One HTTP MCP `tools/list` request; assert four tools.
- No subprocess; runs in the test process for speed.

### Regression risk for Phase 1/2 tests

None. The MCP server uses its own dual-pool setup and reads only. Existing sync code paths and tests are untouched.

## Open questions / minor decisions

- **Catalog access via SQL**: when a user runs `query_sql` to SELECT from `_csq.catalog`, the right syntax under ATTACH is `<alias>._csq.catalog` (DuckDB qualifies attached DB schemas with the alias). Worth a one-liner in the README. No code work.
- **HTTP transport details**: the SDK exposes both SSE and Streamable HTTP. The implementation will use whichever is the SDK's recommended HTTP transport at the time of writing. Either works for Phase 3.
- **`Description` field shape on tools**: each tool gets a one-paragraph `Description` aimed at agent consumption (job-to-be-done framing), kept under ~200 characters.

## Future work (not Phase 3)

- Write tools: `sync_dataset(id)` / `refresh_catalog()` — runs the existing sync orchestrator via the read-write pool. Needs Phase 2's `sync.Run` to be callable as an in-process function (it already is).
- Query log persisted to `_csq.mcp_query_log` for later analysis.
- Resource subscriptions (MCP `resources/subscribe`) so an agent gets notified when a dataset is re-synced.
- Auth / TLS for the HTTP transport when not bound to loopback.
- Connection pool tuning (currently relies on `*sql.DB` defaults).
- Snapshot publishing (Phase 4).
