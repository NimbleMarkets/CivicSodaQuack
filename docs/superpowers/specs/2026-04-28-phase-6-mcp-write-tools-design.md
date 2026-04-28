# Phase 6 — MCP write tools, version, paired configs

**Status:** design approved, awaiting implementation plan.
**Date:** 2026-04-28.
**Prior art:** Phase 1 (`sync.Run`), Phase 3 (MCP server with read tools), Phase 4 (snapshot manifest carries `csq_version`), Phase 5 (`<dbpath>.lock` portal lock).

## Summary

Phase 6 adds two write-capable MCP tools on top of the Phase 3 read surface, plus build-time version injection.

- **`sync_dataset(portal, dataset_id, full_refresh?) → SyncResult`** — synchronously runs `sync.Run` for one dataset using a YAML config registered at server startup.
- **`refresh_catalog(portal?) → []CatalogRefreshResult`** — fetches `/api/catalog/v1` and upserts `_csq.catalog`.
- **`csq mcp --config <path>`** flag (repeatable, paired with `--db`) registers per-portal configs. Without `--config`, the write tools error for that portal but the read tools still work.
- **`internal/version`** package with `Version` injected at build time via `-ldflags`. The hardcoded `"0.4.0"` in `cmd/csq/snapshot.go` and `"0.3.0"` in the MCP server's `Implementation.Version` switch to `version.Version`.

The MCP server's process-lifetime portal lock from Phase 5 covers the new write tools — no second `flock.Acquire` inside the handlers.

## Goals

- One MCP call rebuilds one dataset (`sync_dataset`) or refetches one portal's catalog (`refresh_catalog`) without leaving the server.
- Backward compatibility: existing read-only `csq mcp` invocations (no `--config`) keep working unchanged; `sync_dataset` and `refresh_catalog` simply error for unregistered portals.
- The Phase 4 snapshot manifest's `csq_version` field carries the real build version (git tag) instead of a hardcoded literal.
- Phase 5 portal-lock invariants preserved: a standalone `csq sync` against the same DB still fails-fast (or waits) while the MCP server is up.

## Non-goals

- Asynchronous / job-queue execution. `sync_dataset` blocks the JSON-RPC call until the sync finishes (or fails). MCP clients already render long-running tools as spinners.
- Cancellation as a separate tool. Client-side context cancellation propagates into `sync.Run` via the existing context plumbing.
- `_csq.mcp_query_log` — deferred. Not load-bearing for any user request.
- Per-dataset progress streaming via MCP notifications. A future capability.
- Auto-resolving `--config` from the DuckDB file. Phase 6 is explicit pairing only.
- Multi-dataset `sync_dataset(portal, dataset_ids[])`. One ID per call. Agents can compose loops.

## Architecture

### CLI surface

```
csq mcp --db <portal.duckdb> --config <portal.yaml>
        [--db ... --config ...]
        [--http <addr>]
        [--no-lock] [--lock-wait <duration>]
```

`--config` is repeatable and positionally paired with `--db`. Either both or neither for each portal. Mixed (some have `--config`, some don't) is allowed: portals without configs serve read tools only.

### Tool: `sync_dataset`

```go
type SyncDatasetArgs struct {
    Portal      string `json:"portal"      jsonschema:"alias of the attached portal"`
    DatasetID   string `json:"dataset_id"  jsonschema:"4x4 Socrata id"`
    FullRefresh bool   `json:"full_refresh,omitempty" jsonschema:"true to bootstrap (full-replace) instead of delta"`
}

type SyncDatasetResult struct {
    Portal      string `json:"portal"`
    DatasetID   string `json:"dataset_id"`
    Status      string `json:"status"`           // "ok" | "failed" | "aborted"
    RowsWritten int64  `json:"rows_written"`
    DurationMs  int64  `json:"duration_ms"`
    RunID       string `json:"run_id"`
    Error       string `json:"error,omitempty"`
}
```

Handler flow:

1. Look up `cfg = configs[args.Portal]`. If missing, error: `"sync_dataset: no config registered for portal X; restart csq mcp with --db ... --config ..."`.
2. Open a fresh `*duckdb.Writer` against `cfg.DB` (within-process duckdb cache returns the same underlying instance as the MCP pool — same access mode → no conflict).
3. Build `sync.Deps` with `Only: [dataset_id]` and, if `full_refresh`, `FullRefreshIDs: [dataset_id]`. Use `sync.RecordingReporter{}` (we don't surface progress).
4. Call `sync.Run(ctx, cfg, deps)`.
5. Map `summary` + per-dataset SQL lookup to `SyncDatasetResult`. Tool returns `nil` error for any in-band failure (`Status: "failed"` carries the message).

### Tool: `refresh_catalog`

```go
type RefreshCatalogArgs struct {
    Portal string `json:"portal,omitempty" jsonschema:"optional alias to limit refresh; default: all registered"`
}

type RefreshCatalogResult struct {
    Portal       string    `json:"portal"`
    DatasetCount int64     `json:"dataset_count"`
    FetchedAt    time.Time `json:"fetched_at"`
    Error        string    `json:"error,omitempty"`
}
```

For each target portal: open writer, `client.FetchCatalog(cfg.Portal)`, `w.UpsertCatalog(catalog, time.Now().UTC())`, append `RefreshCatalogResult`. Per-portal errors don't abort the batch; they appear in the result list.

### `internal/version` package

```go
// Package version exposes the build-time-injected csq version.
package version

// Version is overridden at build time via:
//   -ldflags "-X github.com/neomantra/CivicSodaQuack/internal/version.Version=<value>"
var Version = "0.6.0-dev"
```

Used by:
- `internal/mcpserver/server.go` — `mcp.NewServer(&mcp.Implementation{Version: version.Version}, ...)`.
- `cmd/csq/snapshot.go` — `snapshot.Pack(... CSQVersion: version.Version, ...)`.

`Taskfile.yml` build task:

```yaml
build:
  vars:
    GIT_VERSION:
      sh: git describe --tags --always --dirty 2>/dev/null || echo 0.6.0-dev
  cmds:
    - >
      go build
      -ldflags "-X github.com/neomantra/CivicSodaQuack/internal/version.Version={{.GIT_VERSION}}"
      -o csq ./cmd/csq
```

### Boundaries (changed)

- `internal/mcpserver` now imports `internal/sync`, `internal/socrata`, `internal/config`, `internal/portallock`, `internal/version`. The "MCP server is read-only of project state" boundary from Phase 3 is intentionally relaxed for the write tools; documented in this spec.
- `internal/version` has zero internal-pkg dependencies. Imported by `cmd/csq` and `internal/mcpserver`.

## Components

### `internal/version/version.go` (new)

Single var as shown above.

### `internal/mcpserver/configs.go` (new)

```go
func LoadConfigs(specs []DBSpec, configPaths []string) (map[string]*config.Config, error)
```

Pairs positionally. Errors on count mismatch. Loads each YAML via `config.Load(path)`. Overrides `cfg.DB` to the spec's path so writes land in the right file even if the YAML's `db:` field is stale.

### `internal/mcpserver/tools_sync_dataset.go` (new)

`SyncDatasetArgs`, `SyncDatasetResult`, `syncDatasetHandler(ctx, *Pools, configs, args) (SyncDatasetResult, error)`.

### `internal/mcpserver/tools_refresh_catalog.go` (new)

`RefreshCatalogArgs`, `RefreshCatalogResult`, `refreshCatalogHandler(...)`.

### `internal/mcpserver/server.go` (modified)

```go
type Options struct {
    DBs      []DBSpec
    HTTPAddr string
    Configs  map[string]*config.Config // Phase 6: per-portal configs for write tools
}
```

`buildServer` registers the two write tools when `len(opts.Configs) > 0` (the read tools always register). Replaces hardcoded `"0.3.0"` with `version.Version`.

### `cmd/csq/mcp.go` (modified)

Adds `--config` (repeatable). Calls `mcpserver.LoadConfigs` after `ResolveDBSpecs`. Passes the resulting map into `Options.Configs`.

### `cmd/csq/snapshot.go` (modified)

`CSQVersion: version.Version` instead of `"0.4.0"`.

### `Taskfile.yml` (modified)

Build task injects ldflags as shown.

### `cmd/csq/main.go` (modified)

Usage adds `--config` to the `csq mcp` line.

### `README.md` (modified)

Adds a Phase 6 paragraph in the MCP section about `--config`, the two write tools, and the embedded `csq_version` from `git describe`.

## Errors & failure modes

| Situation | Behavior |
|---|---|
| `sync_dataset` with portal lacking config | Tool error naming the portal. |
| `sync_dataset` with unknown dataset_id | Tool returns `Status: "failed"` with selector-resolver error message. |
| `sync_dataset` Socrata 5xx after retries | `Status: "failed"`. Main table unchanged (Phase 1 invariant). |
| `sync_dataset` client disconnects | `ctx.Done()` propagates; `Status: "aborted"`. |
| `sync_dataset(full_refresh=true)` for never-synced dataset | Bootstrap path runs (no-op for the rewrite — would have bootstrapped anyway). |
| Two simultaneous `sync_dataset` for same portal in same MCP process | `database/sql` serializes against the single DuckDB instance. Both run, sequentially. |
| `refresh_catalog(portal=X)` for unregistered portal | Tool error naming X. |
| `refresh_catalog` with no args, mixed registered/unregistered portals | Per-portal results: registered ones run, unregistered ones get `Error: "no config registered"`. Tool returns `nil` overall. |
| `refresh_catalog` HTTP failure for one portal | Per-portal `Error` populated; other portals continue. |
| `--db` and `--config` arg counts differ | `LoadConfigs` errors at startup naming both counts. |
| `--config` YAML's `db:` differs from paired `--db` | Warn to stderr at startup; override `cfg.DB` to the actual `--db` path. |
| `version.Version` empty (ldflags omitted) | Default `"0.6.0-dev"` from package var. |

## Testing

### `internal/version/version_test.go`

- `TestVersion_DefaultIsDev` — confirm the unset-build default.

### `internal/mcpserver/configs_test.go`

- `TestLoadConfigs_PairsAreEqual`
- `TestLoadConfigs_LengthMismatch`
- `TestLoadConfigs_BadYAML`
- `TestLoadConfigs_DBPathOverride`
- `TestLoadConfigs_EmptyConfigs`

### `internal/mcpserver/tools_sync_dataset_test.go`

- `TestSyncDataset_NoConfig_Errors`
- `TestSyncDataset_HappyPath` — fixture portal serves a dataset; assert ok + rows + state row.
- `TestSyncDataset_FullRefresh` — bootstrap then full-refresh, assert `last_full_replace_at` advanced.
- `TestSyncDataset_UnknownDataset_Failed` — id not in selector set, assert `Status: "failed"`.

### `internal/mcpserver/tools_refresh_catalog_test.go`

- `TestRefreshCatalog_HappyPath`
- `TestRefreshCatalog_UnknownPortal_Errors`
- `TestRefreshCatalog_PartialFailure` — one portal good, one bad URL; per-portal results carry the split.

### End-to-end CLI smoke

- `TestCSQ_MCP_SyncDataset_Smoke` — `csq mcp --db <fixture> --config <yaml>` against an httptest portal; JSON-RPC `tools/call` for `sync_dataset`; assert `status: "ok"` and dataset_state row exists.
- `TestCSQ_Version_Ldflags_Smoke` — build the test binary with `-ldflags "-X .../version.Version=v9.9.9-test"`; run `csq snapshot --db <seeded> --output <tmp>`; assert manifest `csq_version == "v9.9.9-test"`.

### Regression risk

None expected. The two new tools register only when `Options.Configs` is populated; existing `csq mcp` invocations (no `--config`) work unchanged. `version.Version` defaults to a literal when ldflags are absent, so plain `go test` behavior is preserved.

## Open questions

None. All decisions resolved during brainstorming.

## Future work (not Phase 6)

- Async `sync_dataset` with `job_id` + `sync_status` polling, for syncs that take longer than a typical agent will tolerate.
- `_csq.mcp_query_log` for usage analytics.
- Resource subscriptions (MCP `resources/subscribe`) so an agent gets notified when a dataset finishes syncing.
- Reading `csq_version` from `runtime/debug.ReadBuildInfo()` instead of ldflags (simpler; works without Taskfile changes for `go install`).
