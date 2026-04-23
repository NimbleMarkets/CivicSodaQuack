# Phase 1 — Catalog-driven bulk sync with YAML manifest

**Status:** design approved, awaiting implementation plan.
**Date:** 2026-04-21.
**Prior art:** Phase 0 (`csq extract`) — single-dataset extractor with runtime schema inference.

## Summary

Phase 1 adds a YAML-driven bulk sync to CivicSodaQuack. One YAML file per portal enumerates which datasets to materialize and how. Selectors support glob matching on dataset name, category, and tag, plus literal 4x4 ids and an exclude list. A new `csq sync` subcommand drives a worker pool that writes each dataset atomically via a staging schema and swap. A new `csq catalog` subcommand fetches and caches `/api/catalog/v1`, with filters and a starter-YAML generator. Catalog and sync history live in a `_csq` schema inside the per-portal DuckDB.

Phase 1 is explicitly full-replace only. Incremental sync, append semantics, and MCP serving belong to later phases. The internal structure uses three small interfaces (`SelectorResolver`, `WriteStrategy`, `ProgressReporter`) so Phase 2 incremental sync and a planned BubbleTea TUI can plug in without rewriting the orchestrator.

## Goals

- Curated bulk sync: a user checks in one YAML per portal; `csq sync` materializes the matching datasets into that portal's DuckDB.
- Wildcard selection: globs on name, category, tag, combined with literal ids.
- Atomic per-dataset writes: a failed sync never destroys the last successful sync of that dataset.
- Concurrent dataset syncs with configurable on-error behavior (continue vs. abort).
- Self-describing state: the DuckDB file knows its own catalog and sync history.
- TUI-friendly internals: orchestration, progress, and config are callable Go APIs, not CLI-shell-outs.

## Non-goals

- Incremental sync / high-water marks (Phase 2).
- Append-mode writes (Phase 2, behind proper incrementality).
- MCP server (Phase 3).
- Snapshot publishing (Phase 4).
- Column renames or type overrides in YAML — deferred until we see real failure cases.
- Cross-portal single-file config. One YAML per portal.
- Live-portal tests in CI.

## Architecture

### Package layout

```
cmd/csq/              # CLI: extract (existing), sync (new), catalog (new)
internal/socrata/     # existing + new: catalog fetch (/api/catalog/v1)
internal/duckdb/      # existing + new: CatalogStore, SyncRunStore, staging swap
internal/config/      # new: YAML load/validate; Config, Rules, defaults merging
internal/sync/        # new: orchestrator + three interfaces
  selector.go         #   SelectorResolver
  strategy.go         #   WriteStrategy (FullReplace today; Incremental in Phase 2)
  progress.go         #   ProgressReporter (stderr today; TUI later)
  run.go              #   Run(ctx, Config, deps) — the orchestrator
```

Rationale:

- `internal/config` is separate from `internal/sync` so the config loader has no DuckDB dependency. The TUI's "edit YAML" panel can import config alone.
- Catalog fetch lives in `internal/socrata` next to existing metadata/rows code; they share the HTTP client.

### Core interfaces

```go
// SelectorResolver expands YAML selectors against a catalog listing.
type SelectorResolver interface {
    Resolve(ctx context.Context, rules config.Rules, catalog []socrata.CatalogEntry) ([]DatasetTarget, error)
}

// WriteStrategy owns how a dataset's rows land in DuckDB.
// FullReplace (Phase 1): stage table → stream rows → rename swap.
// Incremental (Phase 2): look up high-water mark → fetch delta → upsert.
type WriteStrategy interface {
    Sync(ctx context.Context, target DatasetTarget, client *socrata.Client, w *duckdb.Writer, prog ProgressReporter) (DatasetResult, error)
}

// ProgressReporter receives lifecycle events. CLI impl writes to stderr;
// TUI impl pushes to a tea.Msg channel. Zero presentation logic in core.
type ProgressReporter interface {
    DatasetStart(t DatasetTarget)
    DatasetProgress(t DatasetTarget, rowsSoFar int64)
    DatasetDone(t DatasetTarget, res DatasetResult)
}
```

`sync.Run` wires one of each, drives the worker pool, records sync runs, returns a summary.

## YAML config schema

One file per portal. Full example:

```yaml
# data.cityofchicago.org.yaml
portal: data.cityofchicago.org
app_token: ${SOCRATA_APP_TOKEN}     # literal, or ${ENV_VAR}
db: data.cityofchicago.org.duckdb   # optional; default = <portal>.duckdb

# Runtime knobs
concurrency: 4          # default 4
on_error: continue      # continue | abort, default continue

# Global defaults applied to every dataset unless overridden
defaults:
  batch_size: 5000
  order_by: ":id"
  # where / limit / columns.skip: no global default

# Selectors — union of all matches, minus exclude
include:
  - id: 6zsd-86xi                     # literal 4x4
  - id: ijzp-q8t2
  - name: "Crimes*"                   # glob on human name
  - category: "Public Safety"         # glob on classification.domain_category
  - tag: "311*"                       # glob on any tag
exclude:
  - id: 85ca-t3if                     # giant tax-parcels dataset
  - name: "*Archive*"

# Per-dataset overrides — keyed by 4x4 id only
overrides:
  6zsd-86xi:
    table: crimes
    where: "date >= '2015-01-01'"
    order_by: ":updated_at"
    batch_size: 10000
    columns:
      skip: [location_description_raw]
  ijzp-q8t2:
    limit: 100000
```

### Rules

1. **Selector semantics.** `include` is a union (any match = included). `exclude` is applied after include. Literal `id:` always allowed; `name`, `category`, `tag` use `path.Match` globs (`*`, `?`, `[abc]`). Empty `include` is an error. Empty `exclude` is fine.
2. **Override precedence.** For each resolved dataset: start with built-in defaults, layer `defaults:`, layer `overrides.<id>`. Last writer wins. Missing override = inherited.
3. **Allowed per-dataset override keys:** `table`, `where`, `order_by`, `batch_size`, `limit`, `columns.skip`. No `replace` (Phase 1 is always full-replace).
4. **Validation at load time.** Unknown top-level keys error out. Unknown override keys error out. `on_error` not in `{continue, abort}` errors out.
5. **`${ENV_VAR}` expansion** only on `app_token`. Not a general templating system.
6. **Overrides keyed by 4x4 id only.** No selector-keyed overrides in Phase 1 — keeps precedence rules unambiguous.

## CLI

### `csq extract` (unchanged)

Existing Phase 0 subcommand for single-dataset debugging. Untouched.

### `csq catalog`

Portal discovery. Caches results into `_csq.catalog` in the portal's DuckDB.

```
csq catalog --portal X [--id G] [--name G] [--category G] [--tag G] [--json] [--refresh]
csq catalog --portal X --output portal.yaml [--force]
```

- `--portal X` (required) — hits `/api/catalog/v1?domains=X` (paginated), caches into `_csq.catalog`.
- Filters (client-side, after fetch): `--id` (literal 4x4 match, repeatable), `--name`, `--category`, `--tag` (all glob via `path.Match`, repeatable). Different filter kinds combine with AND; repeats of the same kind combine with OR.
- `--json` — emit full catalog entries as JSON. Default is a human table (id, name, category, rows, updated_at).
- `--refresh` — force refetch, overwrite cache.
- `--output FILE` — emit a starter YAML: `portal:`, empty `defaults:`, commented-out `include:` block listing every matched dataset (id + name comment), empty `overrides:`. Refuses if `FILE` exists unless `--force`.

### `csq sync`

Executes the manifest.

```
csq sync --config FILE [--dry-run] [--refresh-catalog] [--concurrency N] [--only IDs] [-v]
```

- `--config FILE` (required) — the portal YAML. `portal` is read from the YAML.
- `--dry-run` — fetch/cache catalog, resolve selectors, print "would sync these N datasets" with per-dataset effective config, exit 0. No writes.
- `--refresh-catalog` — force catalog refetch before resolution. Without this flag, a cached catalog is used when present; an absent cache triggers a single fetch.
- `--concurrency N` — overrides YAML `concurrency`.
- `--only ID[,ID,...]` — comma-separated list of literal 4x4 ids, intersected with the selector-resolved set. Useful for retrying specific datasets after a partial failure. Ids not in the resolved set are errors, not silent drops.
- `-v / --verbose` — noisier progress.

### Exit codes (sync)

| Code | Meaning |
|------|---------|
| 0 | All targeted datasets synced ok. |
| 1 | At least one dataset failed under `on_error: continue`. Summary printed. |
| 2 | Config error / catalog fetch failed / no datasets matched / CLI misuse. |

### Sample progress output (stderr, default reporter)

```
[csq] catalog: 2147 datasets on data.cityofchicago.org (cached)
[csq] resolving selectors → 23 datasets match, 2 excluded → 21 to sync
[csq] [1/21]  6zsd-86xi  crimes                        starting
[csq] [1/21]  6zsd-86xi  crimes                        420000 rows (elapsed 34s)
[csq] [1/21]  6zsd-86xi  crimes                        done: 8.2M rows in 14m03s
[csq] [3/21]  abcd-1234  business-licenses             FAILED: HTTP 500 after 5 retries
...
[csq] summary: 20 ok, 1 failed, 8m42s wall
```

## Sync orchestration

### End-to-end flow

1. Load and validate YAML.
2. Open portal DuckDB (create if absent). Run `_csq` schema migrations idempotently.
3. Fetch-or-read catalog. If uncached or `--refresh-catalog`, fetch `/api/catalog/v1` paged, upsert into `_csq.catalog` in one transaction.
4. Resolve selectors: apply `include` union, subtract `exclude`, subtract datasets not present in catalog, intersect with `--only` if set. Result: `[]DatasetTarget` each carrying an effective per-dataset config.
5. If `--dry-run`: print table, exit 0.
6. Start worker pool (`errgroup.WithContext` + `SetLimit(concurrency)`).
7. For each target, in parallel:
   1. Fetch `/api/views/{id}.json` → `socrata.DatasetMetadata`.
   2. `BuildSchema(target.Table, metadata.Columns)` honoring `columns.skip`.
   3. `CREATE TABLE "_csq_staging"."<table>_<runid>"` (fresh every run).
   4. Stream rows via `socrata.Client.StreamRows(order_by, where, limit, …)`, inserting into the staging table. Emit `DatasetProgress` per page.
   5. Swap into place (see "The swap" below).
   6. Insert ok row into `_csq.sync_runs`.
   7. On error: leave staging table in place for debugging; insert failure row into `_csq.sync_runs`; honor `on_error` (continue → next dataset; abort → cancel shared context).
8. After pool drains: print summary. Exit 0 if all ok, 1 if any failed.

### Concurrency model

Single `errgroup.Group` with `SetLimit(concurrency)` for dataset-level parallelism. Pagination within a dataset is strictly serial (Socrata `$offset` pagination isn't parallel-safe; each dataset has its own prepared-statement transaction). `*sql.DB` is goroutine-safe; each dataset sync opens its own transaction(s).

### Staging schema lifecycle

`CREATE SCHEMA IF NOT EXISTS _csq_staging` at DB open. Staging tables named `<table>_<runid>` (`runid` = short ULID, shared across all datasets in one sync invocation) so concurrent syncs and failed-run leftovers don't collide. On startup, `csq sync` logs and skips any staging tables older than 24h but does not auto-drop them — a future `csq gc` command can clean them up. Keeps forensic value without unbounded growth.

### The swap

Replacing the previous-run table with the newly-staged one is a three-statement sequence (DuckDB's `ALTER TABLE ... RENAME` keeps the table in its current schema; moving schemas is a separate `SET SCHEMA`):

```sql
BEGIN;
DROP TABLE IF EXISTS main."<table>";
ALTER TABLE _csq_staging."<table>_<runid>" RENAME TO "<table>";
ALTER TABLE _csq_staging."<table>" SET SCHEMA main;
COMMIT;
```

DuckDB supports transactional DDL, so the whole sequence is atomic from a concurrent reader's perspective — no window where the table is missing or half-named. The file is single-writer anyway; the planned TUI and Phase 3 MCP are read-mostly and can tolerate the write transaction briefly locking out reads.

### `on_error` semantics

- **`continue`** (default): a dataset's error is recorded in `_csq.sync_runs`, worker picks up the next target. Summary lists failures. Exit 1.
- **`abort`**: first error cancels the shared context. Workers in flight finish their current page, their next `InsertRows` returns the context error, that dataset is recorded as `status='aborted'`, and `sync.Run` returns. Already-committed datasets stay (their swap already happened). Exit 1.

### What counts as an error

- Non-2xx after retry exhaustion on rows/metadata endpoints.
- Row extraction errors (e.g., unparseable timestamp).
- DuckDB exec errors.
- HTTP 404 on metadata — "dataset removed from portal since catalog cache" — is logged as a dataset-level error even though arguably benign. It surfaces that the cache is stale.

## State tables (`_csq` schema)

Both tables created idempotently on `duckdb.Open()` after the ping.

### `_csq.catalog`

Snapshot of `/api/catalog/v1` at last fetch.

```sql
CREATE TABLE IF NOT EXISTS _csq.catalog (
    id            VARCHAR PRIMARY KEY,   -- 4x4, e.g. '6zsd-86xi'
    name          VARCHAR NOT NULL,
    description   VARCHAR,
    category      VARCHAR,               -- classification.domain_category
    tags          JSON,                  -- array of strings
    row_count     BIGINT,                -- reserved; Socrata catalog rarely exposes row counts, so NULL by default
    updated_at    TIMESTAMP,             -- dataset's rowsUpdatedAt
    fetched_at    TIMESTAMP NOT NULL,    -- when we pulled this row into cache
    raw           JSON NOT NULL          -- full catalog entry, for forward-compat
);
```

- Refetch is delete-all-then-insert inside one transaction, so a failed fetch doesn't leave a half-updated cache.
- `raw` preserves fields we don't promote to columns yet; Phase 2/3 features read from `raw` without a schema migration.
- No indexes beyond the PK. Portals top out around 10k rows; DuckDB scans are fine.

### `_csq.sync_runs`

One row per (dataset, run).

```sql
CREATE TABLE IF NOT EXISTS _csq.sync_runs (
    run_id        VARCHAR NOT NULL,      -- ULID; same across all datasets in one sync invocation
    dataset_id    VARCHAR NOT NULL,
    table_name    VARCHAR NOT NULL,      -- resolved target table name
    started_at    TIMESTAMP NOT NULL,
    finished_at   TIMESTAMP,             -- NULL while running; NULL after crash
    status        VARCHAR NOT NULL,      -- 'ok' | 'failed' | 'aborted'
    rows_written  BIGINT,                -- NULL on failure
    error         VARCHAR,               -- NULL on ok
    duration_ms   BIGINT,
    config_hash   VARCHAR,                -- hash of effective per-dataset config
    PRIMARY KEY (run_id, dataset_id)
);
CREATE INDEX IF NOT EXISTS sync_runs_by_dataset ON _csq.sync_runs (dataset_id, started_at DESC);
```

- Insert with `finished_at=NULL` at dataset start; `UPDATE … finished_at=now(), status=…` on completion. A crashed process leaves `finished_at=NULL` rows; the next `csq sync` logs "N prior runs appear incomplete" on startup.
- `config_hash` = sha256 of canonical-JSON effective config (flags, `columns.skip`, `where`, etc.). Phase 2 uses this to invalidate incrementally-synced data when the config changes.
- Never truncated automatically. 20 datasets × daily × year ≈ 7,300 rows. Trivial.

### Phase 2 hook

Incremental sync will add `_csq.high_water_marks (dataset_id PK, column_name, max_value, updated_at)`. Nothing in Phase 1 needs to change; that table simply appears in Phase 2.

## Testing strategy

Three kinds of code, three treatments. Scaled to what matters.

### Pure logic — unit tests, no network, no DuckDB

- `internal/config`: YAML fixtures → `Config` struct; effective-config merging (built-in → defaults → per-dataset override); validation errors (unknown keys, missing `include`, bad `on_error`). Table-driven valid/invalid. `${ENV}` expansion.
- `internal/sync` selector resolution: `[]CatalogEntry` + `Rules` fixtures → expected `[]DatasetTarget`. Covers literal id, glob on name/category/tag, exclude after include, `--only` intersection, empty-result error.
- Wildcard matcher: separate tests for `path.Match` quirks (`[` literal, case sensitivity).

### Socrata client — recorded-response tests

- `httptest.Server` fixtures for `/api/catalog/v1` (paginated), `/api/views/{id}.json`, `/resource/{id}.json`. Cover: happy path, 429 with `Retry-After`, 500→retry→200, 4xx terminal, short-page termination, `$offset`/`$limit` accounting.
- No live-portal fixtures in CI.

### Orchestration + DuckDB — integration tests against in-memory DuckDB

- `internal/sync.Run` end-to-end with a fake `socrata.Client` (serves in-memory rows) and a real `:memory:` DuckDB. Assertions:
  - Staging table created, rename swap succeeded, `_csq.sync_runs` has `status='ok'`, `_csq.catalog` populated.
  - Failure injection mid-stream: staging table left behind, `_csq.sync_runs` has `status='failed'` with error text, main table either absent (first run) or unchanged from prior successful run (subsequent run).
  - `on_error: continue` — N-1 succeed when 1 of N fails; exit code 1.
  - `on_error: abort` — later datasets recorded as `status='aborted'`.
  - Concurrency: run 4 datasets with `concurrency=2`; assert max-in-flight ≤ 2 via a counter in the fake client.
- Dry-run: `Run(ctx, cfg, DryRun: true)` resolves selectors, returns targets, writes no DuckDB state.

### CLI smoke — one test per subcommand

- `cmd/csq` black-box: build binary in `TestMain`, run `csq sync --config testdata/portal.yaml` against an `httptest.Server` whose host is injected into the YAML. Assert exit code and DuckDB row counts.

### Explicit non-goals for testing

- No live-portal tests in CI. Keep a manual `scripts/smoke.sh` against Chicago + NYC for human verification.
- No coverage targets. Value is in the selector-resolution table and orchestrator integration suite; line coverage on CLI glue is noise.
- No mocks for DuckDB. In-memory DuckDB is fast and real; mocking it would test the mock.

### Reusable helpers

- `testdata/catalog_*.json` — captured snippets of real portal catalogs.
- `fakesocrata.Server(t, datasets ...Dataset)` — `httptest.Server` with correct catalog/metadata/pagination.
- `newDuckDB(t)` — opens `:memory:`, runs `_csq` migrations, returns `*Writer` + auto-closes.

## Open questions (resolved during design)

| Question | Decision |
|---|---|
| YAML-as-manifest vs catalog-driven-all-datasets | YAML-as-manifest (with wildcards) |
| Selector match fields | id, name, category, tag — all combinable |
| Include-only or include+exclude | Both |
| Per-dataset override keys | `table`, `where`, `order_by`, `batch_size`, `limit`, `columns.skip` |
| Concurrency default | 4 |
| `on_error` default | `continue` |
| CLI shape | new `sync` and `catalog` subcommands; portal in YAML |
| Catalog cache location | `_csq.catalog` in per-portal DuckDB |
| Sync history location | `_csq.sync_runs` in per-portal DuckDB |
| Atomicity | staging schema + transactional rename |
| Override keying | 4x4 id only |
| Naming | `_csq` schema, not `_csq_*` table prefix |
| `replace: false` in Phase 1 | dropped — full-replace only |
| `catalog --output` overwrite | refuse unless `--force` |
| `sync --only` flag | kept |
