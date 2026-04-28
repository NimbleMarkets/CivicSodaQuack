# Phase 5 — Incremental Sync Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `--full-refresh{,-all}` to `csq sync`, opt-in `checkpoint_every_n_pages` for the delta path, and a portal lock file (`<dbpath>.lock` via `flock`) acquired by every CLI subcommand that opens a per-portal DuckDB.

**Architecture:** Three independent additions, no storage-format changes. `internal/portallock` (new package) wraps `gofrs/flock`. `internal/sync/run.go` rewrites `Effective.Mode` for matching targets after resolution. `internal/sync/incremental.go` adds a per-page checkpoint hook in `delta`. CLI subcommands gain `--no-lock`/`--lock-wait` (everywhere) and `--full-refresh`/`--full-refresh-all` (sync only).

**Tech Stack:** Go 1.24, `github.com/gofrs/flock` (new direct dep), DuckDB, pflag.

---

## File Structure

**Create:**
- `internal/portallock/portallock.go` — `Options`, `Lock`, `Acquire(dbpath, opts)`, `(*Lock).Release()`.
- `internal/portallock/portallock_test.go`

**Modify:**
- `go.mod` / `go.sum` — adds `github.com/gofrs/flock`.
- `internal/config/config.go` — `CheckpointEveryNPages int` on `Override`.
- `internal/config/effective.go` — `CheckpointEveryNPages int` on `Effective`; propagated; in `Hash()`.
- `internal/config/effective_test.go` — extend override + hash tests.
- `internal/sync/run.go` — `Deps.FullRefreshIDs`, `Deps.FullRefreshAll`; rewrite step.
- `internal/sync/run_test.go` — `TestRun_FullRefresh_*`.
- `internal/sync/incremental.go` — checkpoint hook in `delta`.
- `internal/sync/incremental_test.go` — `TestIncremental_DeltaCheckpoints_*`.
- `cmd/csq/sync.go` — flags + lock acquire.
- `cmd/csq/extract.go`, `cmd/csq/catalog.go`, `cmd/csq/mcp.go`, `cmd/csq/snapshot.go` — flags + lock acquire.
- `cmd/csq/main.go` — usage update.
- `cmd/csq/cli_smoke_test.go` — `TestCSQ_Sync_FullRefresh_Smoke` + `TestCSQ_LockContention_Smoke`.
- `README.md` — small additions.

---

## Task 1: Add `gofrs/flock` dependency

- [ ] Run: `go get github.com/gofrs/flock@latest`
- [ ] Verify: `grep gofrs/flock go.mod`
- [ ] Verify: `go build ./...`
- [ ] Commit:
  ```bash
  git add go.mod go.sum
  git commit -m "deps: add github.com/gofrs/flock for portal lock"
  ```

---

## Task 2: `internal/portallock` package

**Files:**
- Create: `internal/portallock/portallock.go`
- Create: `internal/portallock/portallock_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/portallock/portallock_test.go`:

```go
// Copyright (c) 2026 Neomantra Corp

package portallock

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLock_AcquireRelease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, err := Acquire(path, Options{})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Errorf("release: %v", err)
	}
	l2, err := Acquire(path, Options{})
	if err != nil {
		t.Errorf("re-acquire: %v", err)
	}
	_ = l2.Release()
}

func TestLock_Contention_NoWait(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, err := Acquire(path, Options{})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer l.Release()

	_, err = Acquire(path, Options{LockWait: 0})
	if err == nil {
		t.Fatal("want contention error")
	}
	if !strings.Contains(err.Error(), "locked") {
		t.Errorf("error should mention locked: %v", err)
	}
}

func TestLock_Contention_WaitSucceeds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, _ := Acquire(path, Options{})

	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = l.Release()
	}()

	start := time.Now()
	l2, err := Acquire(path, Options{LockWait: time.Second})
	if err != nil {
		t.Fatalf("acquire-with-wait: %v", err)
	}
	defer l2.Release()
	if time.Since(start) > 500*time.Millisecond {
		t.Errorf("acquire took too long: %v", time.Since(start))
	}
}

func TestLock_Contention_WaitTimeout(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, _ := Acquire(path, Options{})
	defer l.Release()

	start := time.Now()
	_, err := Acquire(path, Options{LockWait: 100 * time.Millisecond})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want timeout error")
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("returned too fast (%v)", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("timeout overshoot (%v)", elapsed)
	}
}

func TestLock_NoLock_Bypass(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	l, err := Acquire(path, Options{NoLock: true})
	if err != nil {
		t.Fatalf("noop acquire: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Errorf("noop release: %v", err)
	}
	// Even when "held", a real acquire should succeed because NoLock didn't touch the FS.
	l2, err := Acquire(path, Options{})
	if err != nil {
		t.Errorf("real acquire after noop: %v", err)
	}
	_ = l2.Release()
}

func TestLock_PathNotWritable(t *testing.T) {
	_, err := Acquire("/nonexistent-dir-xyz/x.duckdb", Options{})
	if err == nil {
		t.Fatal("want error for non-writable path")
	}
}

func TestLock_ConcurrentSameProcess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.duckdb")
	var wg sync.WaitGroup
	successes := 0
	var mu sync.Mutex
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := Acquire(path, Options{LockWait: 2 * time.Second})
			if err != nil {
				t.Errorf("concurrent acquire: %v", err)
				return
			}
			mu.Lock()
			successes++
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			_ = l.Release()
		}()
	}
	wg.Wait()
	if successes != 2 {
		t.Errorf("both goroutines should have eventually acquired; got %d", successes)
	}
}
```

- [ ] **Step 2: Run, expect FAIL (package doesn't exist)**

Run: `go test ./internal/portallock/ -v`

- [ ] **Step 3: Write portallock.go**

Create `internal/portallock/portallock.go`:

```go
// Copyright (c) 2026 Neomantra Corp

// Package portallock provides advisory file-based locking for the per-portal
// DuckDB files. Every CLI subcommand that opens a portal DB acquires
// <dbpath>.lock before opening, releases on exit. NoLock skips the lock; LockWait
// retries with exponential backoff before failing.
package portallock

import (
	"fmt"
	"time"

	"github.com/gofrs/flock"
)

// Options controls lock acquisition behavior.
type Options struct {
	NoLock   bool          // skip locking entirely; Acquire returns a no-op Lock
	LockWait time.Duration // max retry duration; 0 = fail-fast
}

// Lock represents an acquired portal lock. Caller must Release.
// A Lock returned with NoLock=true is a no-op sentinel.
type Lock struct {
	flock *flock.Flock // nil when NoLock
	path  string       // for error messages
}

// Acquire takes an exclusive lock on <dbpath>.lock. When opts.NoLock is true,
// returns a sentinel Lock whose Release is a no-op.
func Acquire(dbpath string, opts Options) (*Lock, error) {
	if opts.NoLock {
		return &Lock{path: dbpath}, nil
	}

	lockPath := dbpath + ".lock"
	fl := flock.New(lockPath)

	// Try once first.
	got, err := fl.TryLock()
	if err != nil {
		return nil, fmt.Errorf("cannot create lock file %s: %w", lockPath, err)
	}
	if got {
		return &Lock{flock: fl, path: dbpath}, nil
	}

	// Contention. If LockWait == 0, fail immediately.
	if opts.LockWait <= 0 {
		return nil, fmt.Errorf("portal database is locked by another process: %s (pass --no-lock to bypass, or --lock-wait <duration> to retry)", lockPath)
	}

	// Retry with exponential backoff (50ms initial, 1.5x growth, 1s cap).
	deadline := time.Now().Add(opts.LockWait)
	wait := 50 * time.Millisecond
	for time.Now().Before(deadline) {
		time.Sleep(wait)
		got, err := fl.TryLock()
		if err != nil {
			return nil, fmt.Errorf("cannot acquire lock %s: %w", lockPath, err)
		}
		if got {
			return &Lock{flock: fl, path: dbpath}, nil
		}
		wait = wait * 3 / 2
		if wait > time.Second {
			wait = time.Second
		}
	}
	return nil, fmt.Errorf("portal database is locked: %s; waited %v", lockPath, opts.LockWait)
}

// Release releases the lock and closes the underlying file descriptor.
// Safe to call multiple times; safe on a NoLock sentinel.
func (l *Lock) Release() error {
	if l == nil || l.flock == nil {
		return nil
	}
	err := l.flock.Unlock()
	l.flock = nil
	return err
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run: `go test ./internal/portallock/ -v`

- [ ] **Step 5: Commit**

```bash
git add internal/portallock/portallock.go internal/portallock/portallock_test.go
git commit -m "portallock: add advisory file-based lock for per-portal DBs"
```

---

## Task 3: Config — `CheckpointEveryNPages`

**Files:**
- Modify: `internal/config/config.go`, `internal/config/effective.go`, `internal/config/effective_test.go`

- [ ] **Step 1: Add field to `Override` and `Effective`**

In `internal/config/config.go`, add to the `Override` struct:

```go
type Override struct {
	// ... existing fields ...
	CheckpointEveryNPages int `yaml:"checkpoint_every_n_pages"`
}
```

In `internal/config/effective.go`, add to the `Effective` struct:

```go
type Effective struct {
	// ... existing fields ...
	CheckpointEveryNPages int
}
```

- [ ] **Step 2: Propagate in `EffectiveFor`**

In `EffectiveFor`, after the existing `if ov.HWMColumn != ""` block, add:

```go
if ov.CheckpointEveryNPages != 0 {
    eff.CheckpointEveryNPages = ov.CheckpointEveryNPages
}
```

- [ ] **Step 3: Include in `Hash`**

Update the `canonical` struct in `Hash()` to include the field as the last entry, and update the struct literal to pass `e.CheckpointEveryNPages`. Match the existing pattern.

- [ ] **Step 4: Add tests**

Append to `internal/config/effective_test.go`:

```go
func TestEffectiveFor_CheckpointEveryNPages(t *testing.T) {
	cfg := &Config{
		Overrides: map[string]Override{
			"a-a": {CheckpointEveryNPages: 100},
		},
	}
	eff := cfg.EffectiveFor("a-a")
	if eff.CheckpointEveryNPages != 100 {
		t.Errorf("got %d, want 100", eff.CheckpointEveryNPages)
	}
	eff2 := cfg.EffectiveFor("missing")
	if eff2.CheckpointEveryNPages != 0 {
		t.Errorf("default: got %d, want 0", eff2.CheckpointEveryNPages)
	}
}

func TestEffectiveFor_Hash_IncludesCheckpoint(t *testing.T) {
	a := Effective{Table: "t", BatchSize: 100, CheckpointEveryNPages: 0}.Hash()
	b := Effective{Table: "t", BatchSize: 100, CheckpointEveryNPages: 50}.Hash()
	if a == b {
		t.Errorf("hash should differ when CheckpointEveryNPages changes")
	}
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/config/ -v`

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/config/effective.go internal/config/effective_test.go
git commit -m "config: add CheckpointEveryNPages per-dataset override"
```

---

## Task 4: Sync orchestrator — `--full-refresh*` rewrite

**Files:**
- Modify: `internal/sync/run.go`, `internal/sync/run_test.go`

- [ ] **Step 1: Add fields to `Deps`**

In `internal/sync/run.go`, modify the `Deps` struct:

```go
type Deps struct {
	// ... existing fields ...
	FullRefreshIDs []string
	FullRefreshAll bool
}
```

- [ ] **Step 2: Add the rewrite step in `Run`**

In `internal/sync/run.go`, find the block right after the `--only` filter (after `targets, err := d.Resolver.Resolve(...)`, after `sum.Planned = len(targets)`, before `if d.DryRun { ... }`). Insert:

```go
if d.FullRefreshAll && len(d.FullRefreshIDs) > 0 {
    return sum, errors.New("FullRefreshAll and FullRefreshIDs are mutually exclusive")
}
if len(d.FullRefreshIDs) > 0 {
    resolved := make(map[string]struct{}, len(targets))
    for _, t := range targets {
        resolved[t.ID] = struct{}{}
    }
    refreshSet := make(map[string]struct{}, len(d.FullRefreshIDs))
    for _, id := range d.FullRefreshIDs {
        if _, ok := resolved[id]; !ok {
            return sum, fmt.Errorf("--full-refresh %s: not in resolved selector set", id)
        }
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

(`errors` is already imported by run.go.)

- [ ] **Step 3: Add tests**

Append to `internal/sync/run_test.go`:

```go
func TestRun_FullRefresh_PerID(t *testing.T) {
	srv := newFakeSocrata(t,
		mkDataset("aaaa-0001", 3, 0),
		mkDataset("bbbb-0002", 3, 0),
		mkDataset("cccc-0003", 3, 0))
	cfg := baseCfg(fakeHost(srv))

	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 5}

	// First run bootstraps all three.
	_, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// Capture initial last_full_replace_at for each.
	state1, _ := w.ReadDatasetState("aaaa-0001")
	state2, _ := w.ReadDatasetState("bbbb-0002")
	state3, _ := w.ReadDatasetState("cccc-0003")
	first1 := *state1.LastFullReplaceAt

	// Sleep so any new last_full_replace_at differs.
	time.Sleep(10 * time.Millisecond)

	// Second run: --full-refresh aaaa-0001 only.
	_, err = Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
		FullRefreshIDs: []string{"aaaa-0001"},
	})
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}

	state1b, _ := w.ReadDatasetState("aaaa-0001")
	state2b, _ := w.ReadDatasetState("bbbb-0002")
	state3b, _ := w.ReadDatasetState("cccc-0003")

	if !state1b.LastFullReplaceAt.After(first1) {
		t.Errorf("aaaa-0001 LastFullReplaceAt should advance; was=%v now=%v", first1, *state1b.LastFullReplaceAt)
	}
	if !state2b.LastFullReplaceAt.Equal(*state2.LastFullReplaceAt) {
		t.Errorf("bbbb-0002 LastFullReplaceAt should be unchanged; was=%v now=%v", *state2.LastFullReplaceAt, *state2b.LastFullReplaceAt)
	}
	if !state3b.LastFullReplaceAt.Equal(*state3.LastFullReplaceAt) {
		t.Errorf("cccc-0003 LastFullReplaceAt should be unchanged")
	}
}

func TestRun_FullRefreshAll(t *testing.T) {
	srv := newFakeSocrata(t,
		mkDataset("aaaa-0001", 3, 0),
		mkDataset("bbbb-0002", 3, 0))
	cfg := baseCfg(fakeHost(srv))
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 5}

	_, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")
	state2, _ := w.ReadDatasetState("bbbb-0002")
	first1, first2 := *state1.LastFullReplaceAt, *state2.LastFullReplaceAt

	time.Sleep(10 * time.Millisecond)

	_, err = Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
		FullRefreshAll: true,
	})
	if err != nil {
		t.Fatalf("refresh-all: %v", err)
	}

	state1b, _ := w.ReadDatasetState("aaaa-0001")
	state2b, _ := w.ReadDatasetState("bbbb-0002")
	if !state1b.LastFullReplaceAt.After(first1) || !state2b.LastFullReplaceAt.After(first2) {
		t.Errorf("both LastFullReplaceAt should advance under FullRefreshAll")
	}
}

func TestRun_FullRefresh_UnknownID_Errors(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 1, 0))
	cfg := baseCfg(fakeHost(srv))
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 5}

	_, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
		FullRefreshIDs: []string{"zzzz-9999"},
	})
	if err == nil || !strings.Contains(err.Error(), "zzzz-9999") {
		t.Errorf("want error mentioning zzzz-9999, got %v", err)
	}
}

func TestRun_FullRefresh_AndAll_BothSet_Errors(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 1, 0))
	cfg := baseCfg(fakeHost(srv))
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 5}

	_, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
		FullRefreshIDs: []string{"aaaa-0001"},
		FullRefreshAll: true,
	})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("want mutually-exclusive error, got %v", err)
	}
}
```

Add `"strings"` and `"time"` to imports if not already present.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/sync/ -run TestRun_FullRefresh -v`

- [ ] **Step 5: Run full sync suite**

Run: `go test ./internal/sync/ -v`

- [ ] **Step 6: Commit**

```bash
git add internal/sync/run.go internal/sync/run_test.go
git commit -m "sync: add --full-refresh ID and --full-refresh-all to Run orchestrator"
```

---

## Task 5: Incremental delta — checkpoint hook

**Files:**
- Modify: `internal/sync/incremental.go`, `internal/sync/incremental_test.go`

- [ ] **Step 1: Add the checkpoint hook in `delta`**

In `internal/sync/incremental.go`, find the `delta` method's `client.StreamRowsCtx(...)` call. Replace its handler body with the checkpoint-aware version:

```go
pageIdx := 0
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
        pageIdx++
        if n := target.Effective.CheckpointEveryNPages; n > 0 && pageIdx%n == 0 {
            // Best-effort checkpoint; failure is logged via reporter context but not fatal.
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
    },
)
```

- [ ] **Step 2: Add tests**

Append to `internal/sync/incremental_test.go`:

```go
func TestIncremental_DeltaCheckpoints_PersistsMidStream(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	// Bootstrap with one row at T0.
	bootstrap := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, bootstrap, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}

	// Second run: 5 new rows, BatchSize=1 (so 5 pages), CheckpointEveryNPages=2.
	// Force-fail at offset 4 (so pages 1,2,3,4 succeed and page 5 fails on offset 4).
	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(0)},
		{":id": "x-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(1)},
		{":id": "x-2", ":updated_at": "2026-04-23T00:00:02.000", "score": float64(2)},
		{":id": "x-3", ":updated_at": "2026-04-23T00:00:03.000", "score": float64(3)},
		{":id": "x-4", ":updated_at": "2026-04-23T00:00:04.000", "score": float64(4)},
	}
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Ds aaaa-0001", Columns: bootstrap.Columns,
		Rows: rows, FailAtOffset: 4,
	}

	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 1, MaxRetries: 1, RetryWait: time.Millisecond}
	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run2"}
	target := DatasetTarget{ID: ds.ID,
		Effective: config.Effective{
			DatasetID: ds.ID, Table: "crimes", BatchSize: 1,
			CheckpointEveryNPages: 2,
		}}
	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "failed" {
		t.Fatalf("status: got %q, want failed", res.Status)
	}

	// HWM should have advanced to T3 (the last checkpoint at page 4 actually
	// fires after page 4 succeeds; pages 1..4 commit, then page 5 fails at offset 4).
	// Actually with BatchSize=1 and FailAtOffset=4, offsets 0,1,2,3 succeed (pages 1..4)
	// and offset 4 fails (would-be page 5). Checkpoint fires after pages 2 and 4,
	// so the persisted HWM should reflect page 4's row (T3).
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if state2.HWMUpdatedAt == nil {
		t.Fatal("HWM nil")
	}
	want := time.Date(2026, 4, 23, 0, 0, 3, 0, time.UTC)
	if !state2.HWMUpdatedAt.Equal(want) {
		t.Errorf("HWM: got %v, want %v (T3, after page 4 checkpoint)", state2.HWMUpdatedAt, want)
	}
}

func TestIncremental_DeltaCheckpoints_DisabledByDefault(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	bootstrap := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, bootstrap, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")
	priorHWM := *state1.HWMUpdatedAt

	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(0)},
		{":id": "x-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(1)},
		{":id": "x-2", ":updated_at": "2026-04-23T00:00:02.000", "score": float64(2)},
		{":id": "x-3", ":updated_at": "2026-04-23T00:00:03.000", "score": float64(3)},
	}
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Ds", Columns: bootstrap.Columns,
		Rows: rows, FailAtOffset: 4,
	}
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 1, MaxRetries: 1, RetryWait: time.Millisecond}
	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run2"}
	target := DatasetTarget{ID: ds.ID,
		Effective: config.Effective{
			DatasetID: ds.ID, Table: "crimes", BatchSize: 1,
			// CheckpointEveryNPages: 0 (default)
		}}
	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "failed" {
		t.Fatalf("status: got %q", res.Status)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	if !state2.HWMUpdatedAt.Equal(priorHWM) {
		t.Errorf("HWM advanced without checkpoint flag: was %v now %v", priorHWM, state2.HWMUpdatedAt)
	}
}

func TestIncremental_DeltaCheckpoints_FinalWriteSubsumes(t *testing.T) {
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	bootstrap := mkIncrDataset("aaaa-0001", 1, "2026-04-22")
	if res := runIncr(t, bootstrap, w, "run1", nil, ""); res.Status != "ok" {
		t.Fatalf("bootstrap: %v", res.Err)
	}
	rows := []map[string]any{
		{":id": "x-0", ":updated_at": "2026-04-23T00:00:00.000", "score": float64(0)},
		{":id": "x-1", ":updated_at": "2026-04-23T00:00:01.000", "score": float64(1)},
		{":id": "x-2", ":updated_at": "2026-04-23T00:00:02.000", "score": float64(2)},
		{":id": "x-3", ":updated_at": "2026-04-23T00:00:03.000", "score": float64(3)},
		{":id": "x-4", ":updated_at": "2026-04-23T00:00:04.000", "score": float64(4)},
	}
	ds := fakeDataset{
		ID: "aaaa-0001", Name: "Ds", Columns: bootstrap.Columns,
		Rows: rows,
	}
	srv := newFakeSocrata(t, ds)
	client := &socrata.Client{BatchSize: 1}
	strat := &IncrementalStrategy{Portal: fakeHost(srv), Scheme: "http", RunID: "run2"}
	target := DatasetTarget{ID: ds.ID,
		Effective: config.Effective{
			DatasetID: ds.ID, Table: "crimes", BatchSize: 1,
			CheckpointEveryNPages: 2,
		}}
	res, _ := strat.Sync(context.Background(), target, client, w, &RecordingReporter{}, 1, 1)
	if res.Status != "ok" {
		t.Fatalf("status: %v", res.Err)
	}
	state2, _ := w.ReadDatasetState("aaaa-0001")
	want := time.Date(2026, 4, 23, 0, 0, 4, 0, time.UTC)
	if !state2.HWMUpdatedAt.Equal(want) {
		t.Errorf("HWM after success: got %v, want %v", state2.HWMUpdatedAt, want)
	}
}
```

`time` is already imported in `incremental_test.go`.

- [ ] **Step 3: Run tests**

Run: `go test ./internal/sync/ -run TestIncremental_DeltaCheckpoints -v`

- [ ] **Step 4: Run full sync suite**

Run: `go test ./internal/sync/ -v`

- [ ] **Step 5: Commit**

```bash
git add internal/sync/incremental.go internal/sync/incremental_test.go
git commit -m "sync: add per-N-pages HWM checkpoint hook in delta path"
```

---

## Task 6: CLI — `csq sync` flags + lock acquire

**Files:**
- Modify: `cmd/csq/sync.go`

- [ ] **Step 1: Add the four new flags**

In `cmd/csq/sync.go`, add to the `runSync` flag declarations:

```go
var (
    // ... existing vars ...
    fullRefresh    []string
    fullRefreshAll bool
    noLock         bool
    lockWait       time.Duration
)
fs.StringArrayVar(&fullRefresh, "full-refresh", nil,
    "Force named dataset(s) to bootstrap this run (repeatable)")
fs.BoolVar(&fullRefreshAll, "full-refresh-all", false,
    "Force every resolved dataset to bootstrap this run")
fs.BoolVar(&noLock, "no-lock", false, "Skip portal lock acquisition")
fs.DurationVar(&lockWait, "lock-wait", 0,
    "Retry lock acquisition for up to this duration before giving up")
```

Add `"time"` to the imports if not present.

- [ ] **Step 2: Validate mutual exclusion**

After `fs.Parse`, before constructing `cfg`:

```go
if fullRefreshAll && len(fullRefresh) > 0 {
    return fmt.Errorf("--full-refresh and --full-refresh-all are mutually exclusive")
}
```

- [ ] **Step 3: Acquire portal lock around the existing Open + Run**

Add `"github.com/neomantra/CivicSodaQuack/internal/portallock"` to imports.

Right before `w, err := duckdb.Open(cfg.DB)`:

```go
lock, err := portallock.Acquire(cfg.DB, portallock.Options{
    NoLock:   noLock,
    LockWait: lockWait,
})
if err != nil {
    return err
}
defer lock.Release()
```

- [ ] **Step 4: Pass FullRefresh* into Deps**

In the existing `syncpkg.Run(...)` call, add to the `Deps`:

```go
FullRefreshIDs: fullRefresh,
FullRefreshAll: fullRefreshAll,
```

- [ ] **Step 5: Build + smoke**

Run: `go build -o csq ./cmd/csq && ./csq sync --help 2>&1 | grep -E "full-refresh|lock"`
Expected: shows the new flags.

Run: `./csq sync 2>&1; echo "exit=$?"`
Expected: existing `--config is required` error; exit 1.

- [ ] **Step 6: Commit**

```bash
git add cmd/csq/sync.go
git commit -m "cli: add --full-refresh* and --no-lock/--lock-wait to csq sync"
```

---

## Task 7: CLI — lock acquire on extract/catalog/mcp/snapshot

**Files:**
- Modify: `cmd/csq/extract.go`, `cmd/csq/catalog.go`, `cmd/csq/mcp.go`, `cmd/csq/snapshot.go`

For each file, add `--no-lock` / `--lock-wait` flags and acquire/release around the DuckDB-opening section. Pattern is the same in each:

**`cmd/csq/extract.go`** (`runExtract`):

Add to flag declarations:

```go
var (
    // ... existing ...
    noLock   bool
    lockWait time.Duration
)
fs.BoolVar(&noLock, "no-lock", false, "Skip portal lock acquisition")
fs.DurationVar(&lockWait, "lock-wait", 0,
    "Retry lock acquisition for up to this duration before giving up")
```

After computing `dbPath` (just before `w, err := duckdb.Open(dbPath)`):

```go
lock, err := portallock.Acquire(dbPath, portallock.Options{NoLock: noLock, LockWait: lockWait})
if err != nil {
    return err
}
defer lock.Release()
```

Add the `portallock` import and `"time"` if missing.

**`cmd/csq/catalog.go`** (`runCatalog`): same flags; acquire just before `w, err := duckdb.Open(dbPath)`.

**`cmd/csq/snapshot.go`** (`runSnapshot`): same flags; acquire just before `snapshot.Pack(...)`.

**`cmd/csq/mcp.go`** (`runMCP`): same flags; acquire one lock per `--db` arg AFTER `ResolveDBSpecs` succeeds, BEFORE `mcpserver.Serve`. Releases happen in reverse order via deferred closures. Code shape:

```go
locks := make([]*portallock.Lock, 0, len(specs))
defer func() {
    for i := len(locks) - 1; i >= 0; i-- {
        _ = locks[i].Release()
    }
}()
for _, spec := range specs {
    l, err := portallock.Acquire(spec.Path, portallock.Options{NoLock: noLock, LockWait: lockWait})
    if err != nil {
        return err
    }
    locks = append(locks, l)
}
```

- [ ] **Step 1: Apply edits to all four files**

- [ ] **Step 2: Build**

Run: `go build -o csq ./cmd/csq`

- [ ] **Step 3: Smoke that --no-lock parses everywhere**

Run: `./csq extract --help 2>&1 | grep no-lock` (and similarly for catalog, mcp, snapshot)
Expected: each shows the flag.

- [ ] **Step 4: Commit**

```bash
git add cmd/csq/extract.go cmd/csq/catalog.go cmd/csq/mcp.go cmd/csq/snapshot.go
git commit -m "cli: add --no-lock/--lock-wait to extract, catalog, mcp, snapshot"
```

---

## Task 8: Update `cmd/csq/main.go` usage block

**Files:**
- Modify: `cmd/csq/main.go`

- [ ] **Step 1: Update usage** to mention the new flags. Replace the `Usage:` and `Examples:` lines with versions that note `--full-refresh`, `--no-lock`, `--lock-wait`. Compact, not exhaustive — the per-subcommand `--help` covers details.

```
Usage:
  csq extract  --portal <host> --dataset <4x4> [options]
  csq catalog  --portal <host> [--refresh] [--json] [--output FILE]
  csq sync     --config <portal.yaml> [--dry-run] [--only IDs] [--full-refresh ID ...] [--full-refresh-all]
  csq mcp      --db <portal.duckdb> [--db ...] [--http <addr>]
  csq snapshot --db <portal.duckdb> --output <snap.tar.zst> [--portal NAME] [--force]
  csq fetch    --from <url> [--output <path.duckdb>] [--no-verify] [--force]

All subcommands except 'fetch' acquire <dbpath>.lock; pass --no-lock to bypass or
--lock-wait <duration> to retry.

Examples:
  csq sync --config data.cityofchicago.org.yaml --full-refresh 6zsd-86xi
  csq sync --config data.cityofchicago.org.yaml --full-refresh-all --lock-wait 30s
  csq mcp  --db data.cityofchicago.org.duckdb --no-lock
```

- [ ] **Step 2: Build + smoke**

Run: `go build -o csq ./cmd/csq && ./csq | head -20`

- [ ] **Step 3: Commit**

```bash
git add cmd/csq/main.go
git commit -m "cli: usage mentions --full-refresh and --no-lock/--lock-wait"
```

---

## Task 9: End-to-end CLI smoke tests

**Files:**
- Modify: `cmd/csq/cli_smoke_test.go`

- [ ] **Step 1: Append `TestCSQ_Sync_FullRefresh_Smoke`**

```go
func TestCSQ_Sync_FullRefresh_Smoke(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.duckdb")
	cfgPath := filepath.Join(dir, "portal.yaml")

	// Build a fake portal that serves one dataset.
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

	// First sync (bootstrap).
	cmd := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath)
	cmd.Env = append(os.Environ(), "CSQ_SCHEME=http")
	if err := cmd.Run(); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	// Capture initial last_full_replace_at.
	db, _ := sql.Open("duckdb", dbPath)
	var first1 time.Time
	_ = db.QueryRow(`SELECT last_full_replace_at FROM _csq.dataset_state WHERE dataset_id = 'aaaa-0001'`).Scan(&first1)
	db.Close()

	time.Sleep(20 * time.Millisecond) // ensure timestamps differ

	// Second sync with --full-refresh aaaa-0001.
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
```

- [ ] **Step 2: Append `TestCSQ_LockContention_Smoke`**

```go
func TestCSQ_LockContention_Smoke(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "lock.duckdb")
	cfgPath := filepath.Join(dir, "portal.yaml")

	// Seed minimal CSQ DB so 'csq mcp' will accept it.
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

	// Build a portal yaml pointing at a dummy host; sync will fail at the
	// fetch step but only AFTER acquiring the lock — perfect for our test.
	yaml := `portal: example.com
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

	// Start csq mcp holding the DB.
	mcp := exec.Command(os.Getenv("CSQ_BIN"), "mcp", "--db", dbPath)
	mcpStdin, _ := mcp.StdinPipe()
	if err := mcp.Start(); err != nil {
		t.Fatalf("start mcp: %v", err)
	}
	defer func() {
		_ = mcp.Process.Kill()
		_ = mcp.Wait()
	}()

	// Wait for the lock file to appear (mcp acquires before serving).
	lockPath := dbPath + ".lock"
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(lockPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Run csq sync while mcp holds the lock; expect failure.
	sync1 := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath)
	var stderr1 bytes.Buffer
	sync1.Stderr = &stderr1
	err := sync1.Run()
	if err == nil {
		t.Fatalf("expected sync to fail while mcp holds lock; stderr=%s", stderr1.String())
	}
	if !strings.Contains(stderr1.String(), "locked") {
		t.Errorf("expected 'locked' in stderr; got: %s", stderr1.String())
	}

	// Kill MCP, wait, retry sync.
	_ = mcpStdin.Close()
	_ = mcp.Process.Kill()
	_ = mcp.Wait()

	// The original yaml points at example.com which won't resolve;
	// re-running sync still produces an error, but it should NOT be a lock error.
	sync2 := exec.Command(os.Getenv("CSQ_BIN"), "sync", "--config", cfgPath)
	var stderr2 bytes.Buffer
	sync2.Stderr = &stderr2
	_ = sync2.Run()
	if strings.Contains(stderr2.String(), "locked") {
		t.Errorf("after killing mcp, sync should not see lock error; got: %s", stderr2.String())
	}
}
```

- [ ] **Step 3: Run new tests**

Run: `go test ./cmd/csq/ -run "TestCSQ_Sync_FullRefresh_Smoke|TestCSQ_LockContention_Smoke" -v`

- [ ] **Step 4: Run full CLI suite**

Run: `go test ./cmd/csq/ -v`

- [ ] **Step 5: Commit**

```bash
git add cmd/csq/cli_smoke_test.go
git commit -m "cli: smoke tests for --full-refresh and lock contention"
```

---

## Task 10: README update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a short paragraph** about `--full-refresh` and the lock file. Place it after the existing "Distribute via snapshot" subsection. Keep it tight — this is reference, not tutorial:

```markdown
### Full-refresh and locking

Force one or more datasets to re-bootstrap on the next sync without editing YAML:

```bash
./csq sync --config data.cityofchicago.org.yaml --full-refresh 6zsd-86xi
./csq sync --config data.cityofchicago.org.yaml --full-refresh-all
```

All subcommands that open a per-portal DuckDB acquire `<dbpath>.lock` (advisory `flock`). If another `csq` process is holding the lock, the second errors with a message naming the lock file. Pass `--no-lock` to bypass or `--lock-wait 30s` to retry briefly. `csq fetch` does not lock (it writes a fresh file).

For very long catch-up runs on large datasets, opt into mid-stream HWM persistence in YAML:

```yaml
overrides:
  6zsd-86xi:
    checkpoint_every_n_pages: 100   # 0 = disabled (Phase 2 default)
```

A failure on page 1500 of a 2000-page catch-up then resumes from the most recent checkpoint instead of from the original HWM.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: Phase 5 README — full-refresh, lock, checkpointing"
```

---

## Final verification

- [ ] Run: `task build && task test && task vet`
- [ ] All packages pass.
