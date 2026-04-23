# Phase 2 — Incremental sync via high-water marks

**Status:** design approved, awaiting implementation plan.
**Date:** 2026-04-22.
**Prior art:** Phase 1 (`csq sync`) — full-replace bulk sync with staging swap, `_csq.sync_runs` audit log, and a `WriteStrategy` interface designed to host alternative write strategies.

## Summary

Phase 2 makes daily and hourly syncs cheap on million-row datasets by adding an `IncrementalStrategy` that fetches only rows newer than the last successful sync. Each dataset's high-water mark (`:updated_at`) is stored in a new `_csq.dataset_state` table and used to drive a `$where` filter on subsequent runs. New rows are upserted into the existing main table by Socrata `:id` (surfaced as a `socrata_id PRIMARY KEY` column), so updates to existing rows replace cleanly.

Incremental is the default for any dataset that has a successful prior run; the first run still does a full-replace bootstrap. Schema drift between runs aborts the dataset's sync with an actionable error rather than silently rebootstrapping. A `mode: full_replace` per-dataset override forces re-bootstrapping when the user wants it.

## Goals

- Incremental sync as the default: existing Phase 1 YAMLs get incremental "for free" on their second run, no config changes required.
- Per-dataset HWM tracking that survives partial failures: a failed delta run leaves the HWM untouched so the next run resumes from the same point.
- Safe handling of row updates: an updated source row replaces the old row in DuckDB rather than producing duplicates.
- Loud, actionable schema-drift detection: never silently destroy a million rows because a column was added.
- Per-dataset opt-out: `mode: full_replace` forces full-replace even with prior history.
- Per-dataset HWM column override (`hwm_column`) for datasets where `:updated_at` is unreliable, plumbed through but undocumented until a real dataset needs it.
- Reuse Phase 1 plumbing: `_csq.sync_runs`, `WriteStrategy`, `ProgressReporter`, the orchestrator, and the fake portal all extend rather than fork.

## Non-goals

- Tracking deletes from the source portal. Phase 2 is insert/update only.
- Streaming HWM updates per page (mid-stream checkpointing). The full-page HWM strategy is sufficient for normal daily/hourly diffs; long backfills can use `mode: full_replace` for now.
- Auto-handling of schema drift (additive or otherwise). All drift requires explicit user action.
- Cross-dataset coordination (e.g. parent/child foreign-key syncs).
- Backfilling history older than the bootstrap run. Bootstrap is the floor; we never reach back below the first observed `:updated_at` from the bootstrap fetch.
- MCP serving (Phase 3) and snapshot publishing (Phase 4).

## Data model

### New table: `_csq.dataset_state`

Created by `migrations.go` on `Open`. One row per dataset that has ever been synced.

```sql
CREATE TABLE IF NOT EXISTS _csq.dataset_state (
  dataset_id           VARCHAR PRIMARY KEY,
  hwm_updated_at       TIMESTAMP,        -- max :updated_at seen in last successful run
  last_full_replace_at TIMESTAMP,        -- when the table was last bootstrapped
  last_run_id          VARCHAR,          -- ULID of the run that last wrote this row
  hwm_column           VARCHAR NOT NULL  -- ":updated_at" by default; per-dataset override
)
```

### `socrata_id` PK on per-dataset main tables

Every Socrata row carries a `:id` system field. Phase 1's `BuildSchema` skipped `:@`-prefixed computed-region columns; Socrata's `/api/views/{id}.json` metadata generally does not list `:id` / `:updated_at` either, since they are implicit system fields. Phase 2 manufactures the `socrata_id VARCHAR PRIMARY KEY NOT NULL` column inside `BuildSchema` (it is not driven from metadata), positioned first in the column list. The PK enables native upsert via `ON CONFLICT (socrata_id) DO UPDATE SET ...`.

Phase 1 main tables (which lack the PK) are bootstrapped on first incremental run — see "Bootstrap path" below.

### Fetching system columns from `/resource`

Socrata's `/resource/{id}.json` endpoint returns only user-defined columns by default. To get `:id` and `:updated_at` in the row payload, both Phase 2 paths (bootstrap and delta) must request them explicitly via `$select=:*,*`. This is a small extension to `socrata.StreamRowsCtx`: a new optional `selectClause` parameter (or a `withSystemFields bool` flag) that adds `$select=:*,*` when set. The strategy passes it for Phase 2; Phase 0's `csq extract` keeps the existing default (no `$select`, user columns only).

The extractor for `socrata_id` reads `row[":id"]` and stores it as a string. The HWM tracker reads `row[":updated_at"]` and parses it with the existing `toTimestamp` helper.

### Schema diff scope

`DiffSchema` compares the *user* columns in `BuildSchema(metadata)` against the *user* columns in the live table. The synthetic `socrata_id` column (always present in the live table, never present in metadata) is excluded from the diff on both sides. Without this exclusion every delta run would report `socrata_id` as drift.

### `_csq.sync_runs`: unchanged

Phase 2 keeps the Phase 1 sync_runs schema as-is. It remains a pure append-only event log; HWM lives in `dataset_state`.

## Components

### `internal/duckdb/dataset_state.go`

Thin store mirroring `catalog_store.go` and `sync_runs.go`:

- `ReadDatasetState(id string) (*DatasetState, error)` — returns nil + nil error when the row is missing.
- `UpsertDatasetState(s DatasetState) error` — INSERT ... ON CONFLICT update.

`DatasetState` is a plain struct: `DatasetID, HWMUpdatedAt (*time.Time), LastFullReplaceAt (*time.Time), LastRunID, HWMColumn`.

### `internal/duckdb/upsert.go`

`UpsertRows(table string, ts TableSchema, rows []socrata.Row) error` — generates and executes:

```sql
INSERT INTO main."<table>" (col1, col2, ...) VALUES ($1, $2, ...)
ON CONFLICT (socrata_id) DO UPDATE SET col1 = excluded.col1, col2 = excluded.col2, ...
```

Lives next to `InsertRows` / `InsertRowsInto`. Uses prepared statements per page, mirroring `InsertRows`.

### `internal/duckdb/schema_diff.go`

`DiffSchema(want TableSchema, db *sql.DB, schema, table string) ([]SchemaDiff, error)` — pure read against `information_schema.columns`. Returns a slice of diffs:

```go
type SchemaDiff struct {
    Column string
    Kind   string // "added" | "removed" | "retyped"
    Want   string // type we'd build now
    Have   string // type currently in the table
}
```

Empty slice means schemas match. Used only by `IncrementalStrategy` for drift detection.

### `internal/sync/incremental.go`

`IncrementalStrategy` implementing the existing `WriteStrategy` interface. Decides bootstrap vs. delta and either delegates to `FullReplaceStrategy` or runs the delta path itself. See "Control flow" below.

### Config additions (`internal/config/config.go`)

```go
type Override struct {
    // ... existing Phase 1 fields ...
    Mode       string `yaml:"mode"`        // "" | "full_replace" | "incremental"
    HWMColumn  string `yaml:"hwm_column"`  // "" defaults to ":updated_at"
}
```

`Mode` accepts `""` (auto, the default), `"incremental"` (explicit, same as auto), and `"full_replace"` (force bootstrap on every run). `EffectiveFor` carries these through into `Effective`.

### Orchestrator change (`internal/sync/run.go`)

The default strategy switches from `FullReplaceStrategy` to `IncrementalStrategy{ Inner: &FullReplaceStrategy{...} }`. Existing callers that explicitly inject a `WriteStrategy` are unaffected.

### Boundary check

- `duckdb` package: storage primitives only (state row, upsert SQL, schema diff). No knowledge of portals or sync orchestration.
- `sync` package: orchestration. Decides bootstrap vs. delta vs. drift-fail.
- `socrata` package: small extension to `StreamRowsCtx` to opt into `$select=:*,*` (so `:id` and `:updated_at` appear in row payloads). Otherwise unchanged.

## Control flow

```
IncrementalStrategy.Sync(target):
  1. state ← duckdb.ReadDatasetState(target.ID)
  2. mode  ← target.Effective.Mode  (default "" = auto)
  3. Decide path:
       - mode == "full_replace"          → bootstrap
       - state == nil                    → bootstrap  (first run)
       - main.<table> missing            → bootstrap  (table dropped externally)
       - main.<table> lacks socrata_id PK → bootstrap (Phase 1 leftover)
       - else                            → delta
  4. Bootstrap path:
       a. Run FullReplaceStrategy (Phase 1 behavior; BuildSchema now emits socrata_id PK)
       b. On success, compute max(:updated_at) over the fetched rows
       c. UpsertDatasetState{
            HWMUpdatedAt:      observed max,
            LastFullReplaceAt: now,
            LastRunID:         runID,
            HWMColumn:         effective hwm_column,
          }
       d. Return result
  5. Delta path:
       a. FetchMetadata
       b. DiffSchema(BuildSchema(meta), live)
          → if non-empty: return "failed" with
            "schema drift on <table>: <col> <added|removed|retyped from X to Y>;
             set mode: full_replace in YAML to rebootstrap"
       c. hwmCol ← state.HWMColumn
          hwm    ← state.HWMUpdatedAt formatted as Socrata timestamp
       d. orderBy ← hwmCol + ",:id"
          where   ← hwmCol + " > '" + hwm + "'"
                    (AND-combined with target.Effective.Where if set)
       e. StreamRowsCtx(...) with that order/where
          per page: UpsertRows + advance running max(:updated_at)
       f. On stream success: UpsertDatasetState{
            HWMUpdatedAt: max(state.HWMUpdatedAt, runningMax),
            LastRunID:    runID,
            // last_full_replace_at unchanged
          }
       g. On any error: return "failed", DO NOT touch dataset_state
```

### Edge cases

- **Empty delta page** — stream returns immediately, running max equals current HWM. We still upsert `dataset_state` to advance `last_run_id`; cheap and keeps the audit trail honest.
- **Strictly-greater-than vs. greater-equal** — we use `>` and store the exact max we observed. Any source row updated at the exact same millisecond as our HWM after we read it would be missed. Phase 2 accepts this; Socrata timestamps are millisecond-resolution and bursts at the same instant are rare. A future phase can switch to `>=` plus PK-upsert idempotency. Worth a code comment.
- **Compound `$order`** — `hwmCol,:id` is required for stable pagination when many rows share the same `:updated_at`. Without `:id` as a tiebreaker, page boundaries can repeat or skip rows.

## Failure modes

| Failure | Detection | Behavior | State change |
|---|---|---|---|
| Bootstrap fetch fails mid-stream | FullReplaceStrategy returns `failed` (Phase 1: staging dropped, main untouched) | Propagate as `Status="failed"` | dataset_state not written → next run retries bootstrap |
| Bootstrap succeeds but dataset_state upsert fails | DB error after swap | Status `ok` (data is in main); `Result.Err` carries the dataset_state failure; sync_runs records the error | Next run sees no state → re-bootstraps (full-replace is idempotent) |
| Delta stream fails mid-page | First failed `getPage` after retries | `Status="failed"`, error includes HTTP code/message | dataset_state untouched → next run re-fetches from same HWM |
| Delta page upsert fails | DuckDB transaction error | Same as above | Same as above |
| Schema drift | `DiffSchema` non-empty before any writes | `failed` with the drift message | Untouched |
| HWM column not present in dataset | Diff catches it as "removed" | Treated as schema drift | Untouched |
| Configured `hwm_column` doesn't exist | Caught at first delta run when reading metadata | `"hwm_column 'foo' not found in dataset metadata"` | Untouched |
| Network/HTTP retries | Existing `socrata.Client` retry loop (429/5xx, exponential backoff) | Transparent to strategy | n/a |
| Context cancellation | `ctx.Err()` inside `StreamRowsCtx` | `Status="aborted"` (Phase 1 convention) | Untouched |

**Audit trail invariant:** every dataset gets exactly one `_csq.sync_runs` row regardless of which path ran. The `error` column captures the human-readable failure.

**No silent data loss invariant:** the only writes that bump `hwm_updated_at` happen after the dataset's stream completed cleanly. There is no scenario where data lands in main but the HWM advances past data we haven't fetched.

## Testing

### Unit tests

- `internal/duckdb/dataset_state_test.go` — round-trip read/upsert; nil state for missing row; NULL `hwm_updated_at` allowed (a freshly bootstrapped dataset whose source has no `:updated_at` column would land here, though we error out earlier in that case).
- `internal/duckdb/upsert_test.go` — happy insert; conflict-update path; null handling for nullable cols.
- `internal/duckdb/schema_diff_test.go` — column added; column removed; column retyped (VARCHAR → BIGINT); all-equal returns empty.

### Strategy tests (`internal/sync/incremental_test.go`)

1. **`TestIncremental_Bootstrap`** — no prior state, 5 rows → falls through to FullReplaceStrategy; asserts `main.<t>` has 5 rows + socrata_id PK + `dataset_state` row with correct HWM.
2. **`TestIncremental_DeltaInsert`** — bootstrap with rows at `T0`, second run with new rows at `T1` → asserts only T1 rows fetched (`$where` was applied), main grows, HWM advances to T1.
3. **`TestIncremental_DeltaUpdate`** — bootstrap with `id=A score=1`, second run with `id=A score=99 :updated_at=T1` → asserts main still has 1 row, score is 99, HWM is T1.
4. **`TestIncremental_NoNewRows`** — bootstrap, second run with empty delta → no error, HWM and last_run_id update, no schema activity.
5. **`TestIncremental_SchemaDrift_Fails`** — bootstrap, then portal removes a column → `failed` with the drift message; main and HWM unchanged.
6. **`TestIncremental_FullReplaceOptOut`** — bootstrap, second run with `mode: full_replace` → asserts FullReplaceStrategy ran (table fully recreated, `last_full_replace_at` advanced).
7. **`TestIncremental_StreamFailMidPage`** — bootstrap, second run with `FailAtOffset` on the delta stream → `failed`, HWM unchanged.

### Fake portal extension (`fakesocrata_test.go`)

- Each `fakeDataset` row gets `:id` (auto-assigned if missing) and an optional `:updated_at`.
- The `/resource/{id}.json` handler:
  - When `$select=:*,*` is present, includes `:id` and `:updated_at` in each emitted row; otherwise omits them (mirrors real Socrata).
  - Parses `$where` for the single supported predicate `<col> > '<RFC3339-ish timestamp>'` (regex-level, not full SoQL). Rows with `:updated_at <= hwm` are filtered out before pagination logic.
  - Rejects any other `$where` content (or a malformed predicate) with HTTP 400 so a typo in production code surfaces in tests rather than silently scanning everything.
- A helper swaps a dataset's `Columns` mid-test for the schema-drift scenario.

### End-to-end smoke (`cmd/csq/cli_smoke_test.go`)

- New `TestCSQ_IncrementalSmoke`: runs `csq sync` twice against the fake portal (rows added between runs); asserts row count grows from N to N+M and the binary exits 0 on both runs. Reuses the existing `CSQ_SCHEME=http` hook.

### Regression risk for existing tests

Phase 1's `TestFullReplaceStrategy_*` tests construct `FullReplaceStrategy` directly and never go through `IncrementalStrategy`, so they are unaffected. `TestRun_*` tests use the orchestrator's default strategy, which switches to `IncrementalStrategy`. The existing fake portal already returns rows that include a synthetic `rowsUpdatedAt` at the dataset level; the test datasets will need `:updated_at` populated per row so the bootstrap captures a real HWM. This is a one-line change to `mkDataset` and equivalent.

## Open questions

None. All decisions resolved during brainstorming:

- **HWM source:** hybrid; default `:updated_at`, `hwm_column` override plumbed but undocumented.
- **Write semantics:** native `INSERT ... ON CONFLICT (socrata_id)` upsert; bootstrap installs the PK.
- **Strategy selection:** auto-detect (state row + PK presence), with `mode: full_replace` opt-out.
- **HWM storage:** dedicated `_csq.dataset_state` table; HWM updated only on clean run.
- **Schema drift:** detect and fail loudly with the offending column named.
- **Test fake:** extend the existing one with regex-level `$where` parsing.

## Future work (not Phase 2)

- `--full-refresh <id>` CLI flag: forces re-bootstrap for a single dataset without editing YAML.
- `--checkpoint-every-n-pages`: streaming HWM updates for very large catch-up runs.
- Delete tracking: detect rows present in DuckDB but missing from a fresh full-snapshot pull.
- `:created_at` as an alternative HWM for append-only datasets where `:updated_at` lags.
- Per-portal sync schedule + lock file (we already log incomplete runs on startup; an actual lock would prevent concurrent invocations).
