# Phase 5 — Incremental sync polish

**Status:** design approved, awaiting implementation plan.
**Date:** 2026-04-28.
**Prior art:** Phase 2 incremental sync; Phase 3 MCP server (holds the DB open for the process lifetime).

## Summary

Phase 5 is three quality-of-life improvements to incremental sync, shipped together because they interact:

- **`--full-refresh`** family on `csq sync` — one-shot CLI override that promotes named datasets (or all resolved datasets) to `mode: full_replace` for a single run.
- **`checkpoint_every_n_pages`** per-dataset YAML override — periodically persists the running HWM during the delta path so a long catch-up can resume after failure without re-fetching everything.
- **Portal lock file** — `<dbpath>.lock` acquired via `flock` by every subcommand that opens a per-portal DuckDB. Prevents `csq sync` and `csq mcp` from racing on the same file with opaque DuckDB lock errors.

No storage-format changes. All three are surface-level extensions of Phase 2/3 code.

## Goals

- Force-rebootstrap one or more datasets without editing YAML (`--full-refresh aaaa-0001 --full-refresh bbbb-0002`).
- Force-rebootstrap every resolved dataset in one shot (`--full-refresh-all`).
- Opt giant catch-up datasets into mid-stream HWM persistence (`checkpoint_every_n_pages: 100`) so a network blip on page 1500 of a 2000-page catch-up doesn't waste the work.
- Replace DuckDB's opaque "could not connect" lock errors with a clear, actionable message naming the lock file and offering `--no-lock` / `--lock-wait` escape hatches.

## Non-goals

- Cross-machine locking. The lock file is process-local; NFS or other distributed filesystems may behave inconsistently. Out of scope.
- Sub-page checkpointing. The smallest checkpoint unit is "one page completed."
- Auto-detecting "this dataset is huge, enable checkpointing" — explicit YAML opt-in.
- Locking from inside `internal/sync` or `internal/mcpserver`. The CLI is responsible for acquiring/releasing.
- Read-only locking modes (multiple readers, one writer). DuckDB's instance cache already prevents the most useful sharing patterns within a process; we don't fight it.

## Architecture

### `--full-refresh` family

Lives in the orchestrator. After `SelectorResolver` produces `[]DatasetTarget`, a new `Run` step rewrites the `Effective.Mode` of any target whose ID is in `Deps.FullRefreshIDs`, or all targets when `Deps.FullRefreshAll`. Then `IncrementalStrategy` (Phase 2) takes over unchanged — its existing logic already does the right thing when `Effective.Mode == "full_replace"`.

The validation step (analogous to `--only`) runs before the rewrite: every ID in `FullRefreshIDs` must appear in the resolved set, otherwise `Run` errors before any sync work.

### `checkpoint_every_n_pages`

Lives inside `IncrementalStrategy.delta`. New `int` field on `config.Effective` (`CheckpointEveryNPages`) is read at delta-stream time. When `> 0`, after each page upsert, the per-dataset handler increments a page counter and, when `(pageIdx+1) % N == 0`, writes the running `maxHWM` to `_csq.dataset_state` (preserving `last_full_replace_at`). Failure mid-stream still leaves the table consistent (idempotent socrata_id upserts) and the HWM partially advanced.

A checkpoint write that fails is *not* fatal — the page's data is already committed, and the next checkpoint or the final HWM-on-success write will catch up. Aborting the whole stream because dataset_state had a transient error would discard already-committed page work.

### Portal lock file

A new `internal/portallock` package wraps `github.com/gofrs/flock`. The acquire path:

1. Compute `<dbpath>.lock` (sibling to the DuckDB file).
2. Open the lock file (creating if needed).
3. Try `flock(LOCK_EX|LOCK_NB)`.
4. On success: return a `*Lock` whose `Release()` closes the fd (kernel releases the lock).
5. On failure: if `LockWait > 0`, retry with exponential backoff (50ms initial, 1.5x growth, capped at 1s) until success or timeout. Otherwise error immediately.
6. `--no-lock` short-circuits everything — `Acquire` returns a no-op `*Lock` whose `Release()` is a noop.

Every CLI subcommand that opens a per-portal DuckDB acquires before opening, releases on exit (or on `os.Exit` paths). `csq fetch` doesn't lock — it writes a fresh file with no shared state. `csq mcp` acquires once per `--db` arg.

### Boundaries

- `internal/portallock` (new): pure file-locking primitives. Imports `github.com/gofrs/flock`. No knowledge of DuckDB, sync, or config.
- `internal/sync`: gains `Deps.FullRefreshIDs` / `Deps.FullRefreshAll` and the per-target `Mode` rewrite step; gains the per-N-pages HWM checkpoint inside `IncrementalStrategy.delta`.
- `internal/config`: `Effective` and `Override` get the `CheckpointEveryNPages` field; `Hash()` includes it.
- `cmd/csq`: each subcommand that opens a DuckDB acquires the lock and accepts the new flags.

## Components

### `internal/portallock/portallock.go`

```go
type Options struct {
    NoLock   bool          // skip locking entirely
    LockWait time.Duration // max retry duration; 0 = fail-fast
}

type Lock struct { /* unexported */ }

// Acquire takes an exclusive lock on <dbpath>.lock. Caller must Release.
// When opts.NoLock is true, returns a sentinel Lock whose Release is a no-op.
func Acquire(dbpath string, opts Options) (*Lock, error)

// Release releases the lock and closes the underlying file descriptor.
// Safe to call multiple times.
func (l *Lock) Release() error
```

Internal: when `Acquire` succeeds with a real lock, store `*flock.Flock` plus the path. When `NoLock`, store nil. `Release` checks for nil.

### `cmd/csq/sync.go` additions

```go
fs.StringArrayVar(&fullRefresh, "full-refresh", nil,
    "Force named dataset(s) to bootstrap this run; repeatable")
fs.BoolVar(&fullRefreshAll, "full-refresh-all", false,
    "Force every resolved dataset to bootstrap this run")
fs.BoolVar(&noLock, "no-lock", false, "Skip portal lock acquisition")
fs.DurationVar(&lockWait, "lock-wait", 0,
    "Retry lock acquisition for up to this duration before giving up")
```

Mutually-exclusive validation between `--full-refresh*` flags happens after `fs.Parse`. Lock acquisition wraps the existing `Open(cfg.DB)` → `sync.Run(...)` → `Close` block.

### `cmd/csq/{mcp,extract,catalog,snapshot}.go` additions

Each: `--no-lock` and `--lock-wait` flags only. Lock acquired before `duckdb.Open` (or, in the case of `csq snapshot`, before the temp-copy phase). `csq mcp` acquires per `--db` arg, releases all on shutdown.

### `internal/sync/run.go` additions

```go
type Deps struct {
    // ... existing fields ...
    FullRefreshIDs  []string
    FullRefreshAll  bool
}
```

After `Resolve` and the `--only` filter, before the goroutine pool dispatch:

```go
if d.FullRefreshAll && len(d.FullRefreshIDs) > 0 {
    return sum, errors.New("FullRefreshAll and FullRefreshIDs are mutually exclusive")
}
if len(d.FullRefreshIDs) > 0 {
    resolved := make(map[string]struct{}, len(targets))
    for _, t := range targets {
        resolved[t.ID] = struct{}{}
    }
    for _, id := range d.FullRefreshIDs {
        if _, ok := resolved[id]; !ok {
            return sum, fmt.Errorf("--full-refresh %s: not in resolved selector set", id)
        }
    }
    refreshSet := make(map[string]struct{}, len(d.FullRefreshIDs))
    for _, id := range d.FullRefreshIDs {
        refreshSet[id] = struct{}{}
    }
    for i := range targets {
        if _, ok := refreshSet[targets[i].ID]; ok {
            targets[i].Effective.Mode = "full_replace"
        }
    }
}
if d.FullRefreshAll {
    for i := range targets {
        targets[i].Effective.Mode = "full_replace"
    }
}
```

### `internal/sync/incremental.go` checkpoint hook

In `delta`, replace the existing per-page handler with a version that tracks page index and conditionally writes dataset_state:

```go
pageIdx := 0
err = client.StreamRowsCtx(ctx, ..., func(page []socrata.Row) error {
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
    pageIdx++
    if n := target.Effective.CheckpointEveryNPages; n > 0 && pageIdx%n == 0 {
        // Best-effort checkpoint; failure logged but not fatal.
        _ = w.UpsertDatasetState(duckdb.DatasetState{
            DatasetID:         target.ID,
            HWMUpdatedAt:      maxHWM,
            LastFullReplaceAt: state.LastFullReplaceAt,
            LastRunID:         s.RunID,
            HWMColumn:         hwmCol,
        })
    }
    prog.DatasetProgress(idx, total, target, rowsWritten)
    return nil
})
```

The post-stream `UpsertDatasetState` (already present in Phase 2) still runs on success; checkpoints are subsumed.

### `internal/config/config.go` and `effective.go` additions

```go
type Override struct {
    // ... existing fields ...
    CheckpointEveryNPages int `yaml:"checkpoint_every_n_pages"`
}

type Effective struct {
    // ... existing fields ...
    CheckpointEveryNPages int
}
```

`EffectiveFor` propagates the field. `Hash()` includes it in the canonical struct so config drift is detectable.

## Errors & failure modes

| Situation | Behavior |
|---|---|
| `--full-refresh aaaa-0001` where the id isn't in the resolved selector set | `Run` errors before sync work: `"--full-refresh aaaa-0001: not in resolved selector set"`. |
| Both `--full-refresh ID` and `--full-refresh-all` set | CLI errors at flag parse: `"--full-refresh and --full-refresh-all are mutually exclusive"`. |
| `--full-refresh-all` with empty resolved set | Selector resolver already errors with "no datasets matched"; refresh path never reached. |
| `--full-refresh <id>` for a never-synced dataset | No-op for the rewrite (mode was already going to bootstrap). Idempotent. |
| Checkpoint write fails mid-stream | Page data is committed; checkpoint failure is logged via reporter context but not fatal. Stream continues; next checkpoint or final HWM-on-success write catches up. |
| Stream completes successfully after checkpoint failures | Final HWM-on-success write subsumes; no data loss. |
| `checkpoint_every_n_pages: 0` | Phase 2 behavior preserved: HWM advances only on clean dataset completion. |
| `checkpoint_every_n_pages: 1` | Every page persists HWM. Maximum durability, maximum dataset_state write traffic. Allowed but warned-against in README. |
| `--no-lock` passed | Lock acquisition skipped. Risk noted in help text but not gated. |
| `--lock-wait 30s`, lock held longer than 30s | CLI errors: `"csq <cmd>: portal database is locked: <dbpath>.lock; waited 30s"`. Exit 1. |
| `--lock-wait 0` (default), lock held | CLI errors immediately: `"csq <cmd>: portal database is locked by another process: <dbpath>.lock (pass --no-lock to bypass, or --lock-wait <duration> to retry)"`. |
| Lock file path not writable | Error: `"csq <cmd>: cannot create lock file <dbpath>.lock: <os error>"`. |
| Process killed (SIGKILL) while holding lock | OS releases the file lock when the fd closes. Next process acquires immediately. The `<dbpath>.lock` sentinel file remains (zero bytes); harmless. |
| Process exits via `os.Exit` | `cmd/csq/main.go`'s subcommand wrappers explicitly call `Release` before any exit; OS would also release on process exit. |
| Two `csq mcp` against the same `--db` | Second errors at lock acquisition with the standard message. |
| `csq mcp --db a --db b` where `a` is locked | Second-DB lock attempt fails; first-DB lock released cleanly via reverse-order defer chain. |
| `csq fetch` writing to a path currently MCP-served | Not defended (`csq fetch` doesn't lock). User gets DuckDB error from MCP at next access. |
| `csq snapshot` while `csq sync` is running on the same DB | Second snapshot acquisition waits or fails per `--lock-wait`. |

## Testing

### `internal/portallock/portallock_test.go`

- `TestLock_AcquireRelease` — acquire on a temp path; release; re-acquire succeeds.
- `TestLock_Contention_NoWait` — A holds; B with `LockWait: 0` errors with "locked".
- `TestLock_Contention_WaitSucceeds` — A holds for 100ms then releases; B with `LockWait: 1s` succeeds within ~150ms.
- `TestLock_Contention_WaitTimeout` — A holds; B with `LockWait: 100ms` errors after ~100ms.
- `TestLock_NoLock_Bypass` — `Acquire(path, Options{NoLock: true})` returns a no-op lock that never touches the filesystem.
- `TestLock_PathNotWritable` — point at a path under a non-existent directory; error names the path.
- `TestLock_ConcurrentSameProcess` — two goroutines both `Acquire` with `LockWait: 1s`; both succeed sequentially; race detector clean.

### `internal/sync/run_test.go` additions

- `TestRun_FullRefresh_PerID` — fixture portal with 3 datasets, all bootstrapped previously. `Deps.FullRefreshIDs = ["aaaa-0001"]`. Assert: aaaa-0001's `last_full_replace_at` advanced; the other two's didn't.
- `TestRun_FullRefreshAll` — same setup; `Deps.FullRefreshAll = true`. Assert all three advanced.
- `TestRun_FullRefresh_UnknownID_Errors` — `FullRefreshIDs = ["zzzz-9999"]`; assert `Run` errors before sync work.
- `TestRun_FullRefresh_AndAll_BothSet_Errors` — both fields set; assert `Run` errors at the start.

### `internal/sync/incremental_test.go` additions

- `TestIncremental_DeltaCheckpoints_PersistsMidStream` — bootstrap with rows at T0; second run with 5 new rows at T1..T5, `BatchSize: 1`, `CheckpointEveryNPages: 2`, `FailAtOffset: 4`. Assert: dataset_state HWM is T4 (last checkpoint), status `failed`.
- `TestIncremental_DeltaCheckpoints_DisabledByDefault` — same data, no checkpoint flag, force-fail at page 4. Assert HWM unchanged (Phase 2 invariant).
- `TestIncremental_DeltaCheckpoints_FinalWriteSubsumes` — checkpoint every 2 pages, full success. Assert HWM is T5.

### `internal/config/effective_test.go` additions

- Extend `TestEffectiveFor_ModeAndHWMColumnOverride` to set and assert `CheckpointEveryNPages`.
- Extend the hash test to confirm changing `CheckpointEveryNPages` produces a different hash.

### End-to-end CLI smoke (`cmd/csq/cli_smoke_test.go`)

- `TestCSQ_Sync_FullRefresh_Smoke` — seed a fixture with one dataset already in `_csq.dataset_state`; run `csq sync --config <yaml> --full-refresh aaaa-0001`; assert exit 0 and `last_full_replace_at` advanced.
- `TestCSQ_LockContention_Smoke` — start a `csq mcp` subprocess holding a fixture DB; run `csq sync --config <yaml>` (default `lock-wait=0`); assert exit 1 with "locked" in stderr. Kill the MCP subprocess; re-run sync; assert exit 0.

### Regression risk

None expected:
- `--full-refresh` defaults off → `TestRun_*` paths unchanged.
- `checkpoint_every_n_pages` defaults to 0 → existing incremental tests preserve Phase 2 invariants.
- Lock acquisition is opt-in for the existing CLI smoke tests; their fixtures live in `t.TempDir()` so each test's lock file is isolated.

## Open questions

None. Design decisions resolved during brainstorming.

## Future work (not Phase 5)

- Cross-machine / NFS-safe locking (would need a lease-based protocol).
- Lock file holding the PID of the lock-holder for diagnostic display in the contention error message.
- "Resume from checkpoint" on a per-run basis (the YAML toggle is currently per-dataset; a `--checkpoint-every` CLI override could apply it to all datasets in one run without YAML edits).
- Lock for `csq fetch` when the `--output` path corresponds to an existing portal DB.
