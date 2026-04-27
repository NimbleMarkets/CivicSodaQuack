# Phase 4 — Snapshot publishing

**Status:** design approved, awaiting implementation plan.
**Date:** 2026-04-23.
**Prior art:** Phase 1 (`csq sync`) produces the per-portal DuckDB files this phase packages; Phase 3 (`csq mcp`) consumes them. Phase 4 closes the loop: produce a portable tarball from a synced DB; download and verify one on a fresh host.

## Summary

Phase 4 adds two orthogonal subcommands to `csq`:

- `csq snapshot` packages an existing per-portal DuckDB into a `.tar.zst` tarball with a `manifest.json` sidecar. The tarball can be uploaded anywhere (S3, GitHub Releases, an internal CDN, a local file).
- `csq fetch` downloads a published tarball, verifies its SHA-256 against the manifest, and unpacks the DuckDB into the working directory (or a path of the user's choice). The `csq mcp` and `csq sync` workflows then use the unpacked file as if it had been synced locally.

Distribution itself — where snapshots live — is out of scope. The producer writes a tarball; the consumer reads from any HTTP(S) URL or `file://` path. A future phase can layer a project-hosted index over the same format.

The new code lives in a self-contained `internal/snapshot/` package. No changes to `internal/sync/`, `internal/mcpserver/`, `internal/duckdb/`, `internal/socrata/`, or `internal/config/`.

## Goals

- One-shot producer: `csq snapshot --db X.duckdb --output X.tar.zst` packages a synced DB into a portable tarball.
- One-shot consumer: `csq fetch --from <url>` downloads, verifies, and unpacks a tarball into a usable DuckDB file.
- Engine-agnostic format: `.tar.zst` with a tiny `manifest.json` (~500 bytes) followed by the DuckDB payload. Anyone with `tar` and `zstd` can extract it manually.
- Streaming both ends: producer streams compression directly to the output file (no temp uncompressed tar); consumer streams decompression directly into the output DuckDB (multi-GB downloads must not buffer).
- Verifiable: SHA-256 of the DuckDB payload is in the manifest and checked after extraction (opt-out via `--no-verify`).
- Composable with the rest of the CLI: snapshot is a separate command, not bundled with sync. CI scripts compose them: `csq sync && csq snapshot && curl -T tarball s3://...`.

## Non-goals

- Hosting infrastructure. No project-run index, S3 bucket, or CI workflow for publishing CivicSodaQuack-team snapshots. Users publish their own.
- Defaulting `csq fetch --from <url>` from a portal name. `--from` is required in Phase 4. A future "snapshot index" phase can resolve names to URLs.
- Per-dataset diffs / incremental snapshots. Each snapshot is a full DuckDB.
- Reproducibility. The snapshot embeds the *result* of a sync, not the YAML config that produced it. Source data on the portal changes between syncs; we don't pretend otherwise.
- Encryption / signing. Phase 4 is plaintext zstd. Authenticity comes from delivery channel (HTTPS) plus the consumer's SHA-256 verification.
- Authentication of the download. Use an authenticated URL (S3 presigned, GitHub Releases, etc.) at the URL layer if you need access control.

## Architecture

### CLI surface

**`csq snapshot`**

```
csq snapshot --db <path.duckdb> --output <path.tar.zst> [--portal <name>] [--keep-staging] [--force]
```

- `--db` (required) — source DuckDB.
- `--output` (required) — destination tarball path.
- `--portal` (optional) — overrides the portal name written to the manifest. Default: derived from the DB filename using the same rule as `csq mcp` (basename minus `.duckdb`, dots → underscores).
- `--keep-staging` (optional) — skip the `_csq_staging` cleanup. Default: clean it.
- `--force` (optional) — overwrite an existing `--output`. Default: error if it exists.

Producer flow:
1. Validate `--db` exists; reject if not.
2. Copy the source DB to a temp file in the same directory as `--output` (so any later rename is atomic).
3. Open the temp DB read-write. Validate `_csq.catalog` exists (assertIsCSQDB pattern from Phase 3).
4. Unless `--keep-staging`: `DROP SCHEMA IF EXISTS _csq_staging CASCADE; CREATE SCHEMA _csq_staging;`.
5. Compute counts via SQL: `dataset_count = COUNT(*) FROM _csq.catalog`; `total_row_count = SUM(latest_ok.rows_written)` joined from `_csq.sync_runs`.
6. Compute `duckdb_sha256` and `duckdb_size_bytes` by streaming the temp file through SHA-256 / counting bytes.
7. Build manifest in memory.
8. Stream tar+zst to `--output`: write `manifest.json` first, then the temp DuckDB.
9. Close + delete temp file.

**`csq fetch`**

```
csq fetch --from <url> [--output <path>] [--no-verify] [--force]
```

- `--from` (required) — `http://`, `https://`, or `file://` URL.
- `--output` (optional) — destination DuckDB path. Default: current directory + `manifest.duckdb_filename`.
- `--no-verify` (optional) — skip the SHA-256 check. Default: verify.
- `--force` (optional) — overwrite an existing `--output`. Default: error if it exists.

Consumer flow:
1. Open URL: HTTP request for `http(s)://`, `os.Open` for `file://`. Reject other schemes.
2. Wrap reader in `zstd.NewReader` then `tar.NewReader`.
3. Read first tar entry; assert filename `manifest.json`; parse JSON; reject `schema_version != 1`.
4. Resolve `--output`: if unset, use manifest's `duckdb_filename`. Reject if absolute path traversal (`..`).
5. If `--output` exists and not `--force`, error.
6. Read second tar entry; assert filename matches `manifest.duckdb_filename`; assert size matches `manifest.duckdb_size_bytes`.
7. Stream-write to `--output` while computing SHA-256.
8. After write, verify `manifest.duckdb_sha256` matches. On mismatch (and not `--no-verify`), delete the output and error.
9. Print summary to stderr: portal, snapshot_id, dataset_count, total_row_count, output path.

### Boundaries

- `internal/snapshot` package: tar+zst codec, manifest, producer, consumer. Imports `database/sql`, `archive/tar`, `crypto/sha256`, `encoding/json`, `net/http`, `os`, the zstd codec. Does not import `internal/sync`, `internal/socrata`, `internal/mcpserver`, or `internal/config`.
- `cmd/csq`: CLI parsing only; calls `snapshot.Pack` and `snapshot.Fetch`.
- `internal/duckdb`: untouched.

## Components

### `internal/snapshot/manifest.go`

```go
const SchemaVersion = 1

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
```

Helpers: `(m *Manifest) MarshalIndent() ([]byte, error)` and `ParseManifest(b []byte) (*Manifest, error)`.

### `internal/snapshot/tarzst.go`

Thin wrappers around `archive/tar` + `github.com/klauspost/compress/zstd`:

- `tarZstWriter` — `NewWriter(w io.Writer) *tarZstWriter`, `WriteEntry(name string, size int64, modTime time.Time, body io.Reader) error`, `Close() error`. Internally chains `zstd.NewWriter` → `tar.NewWriter` and exposes one method per entry.
- `tarZstReader` — `NewReader(r io.Reader) (*tarZstReader, error)`, `Next() (header *tar.Header, body io.Reader, err error)`, `Close() error`. Returns each entry's tar header plus a body reader scoped to that entry's bytes.

Two entries per archive in Phase 4 (manifest then DuckDB), but the codec doesn't enforce that count; it just gives the higher-level files a clean primitive.

### `internal/snapshot/inspect.go`

Pure SQL helpers used by the producer to populate the manifest:

```go
func countDatasets(db *sql.DB) (int64, error)            // SELECT COUNT(*) FROM _csq.catalog
func countTotalRows(db *sql.DB) (int64, error)           // SUM(latest ok.rows_written) per dataset
func assertIsCSQDB(db *sql.DB, path string) error        // mirrors mcpserver.assertIsCSQDB
```

### `internal/snapshot/producer.go`

```go
type ProducerOptions struct {
    DBPath       string  // source DuckDB
    OutputPath   string  // destination tarball (.tar.zst)
    Portal       string  // override; "" means derive from filename
    KeepStaging  bool
    Force        bool
    CSQVersion   string  // injected by CLI; defaults to "0.4.0-dev" if empty
}

func Pack(ctx context.Context, opts ProducerOptions) (*Manifest, error)
```

`Pack` is the entry point. Returns the populated manifest (so the CLI can print a summary) plus any error.

Steps in code:
1. Stat `--db`; error on missing.
2. If `--output` exists and not `--force`, error.
3. Create temp file in same dir as `--output`. Use `os.CreateTemp` with a known prefix so leftover temp files are diagnosable.
4. Stream-copy source DB to temp file.
5. Open temp DB read-write; `assertIsCSQDB`; staging cleanup unless `KeepStaging`; close.
6. Compute SHA-256 + size by streaming the temp file.
7. Compute counts by reopening the temp DB read-only.
8. Build `Manifest` with `Portal` derived (or overridden), `SnapshotID = ulid.Make().String()`, `CreatedAt = time.Now().UTC()`.
9. Open `--output` (truncating if `--force`), wrap in `tarZstWriter`. Write `manifest.json` entry, then the temp DB entry.
10. Close writer; delete temp file.
11. Return manifest.

If any step fails after the output file is created, `Pack` deletes it before returning the error.

### `internal/snapshot/consumer.go`

```go
type ConsumerOptions struct {
    URL        string
    OutputPath string  // "" = derive from manifest.duckdb_filename, written to current dir
    NoVerify   bool
    Force      bool
}

func Fetch(ctx context.Context, opts ConsumerOptions) (*Manifest, error)
```

Steps:
1. Open URL: `http.Get` for `http(s)://`, `os.Open` after stripping `file://` prefix. Reject other schemes.
2. For HTTP, error on non-2xx with the body trimmed to ~200 chars.
3. Wrap in `tarZstReader`.
4. Read first entry → must be `manifest.json` → parse → reject `schema_version != 1`.
5. Resolve output path. Reject if it contains `..` after `filepath.Clean` (prevent path traversal even though the manifest is the only source).
6. Check existing file vs `--force`.
7. Read second entry → assert filename + size match manifest.
8. Stream body to output file while computing SHA-256.
9. After full write, check SHA-256 (unless `NoVerify`). On mismatch, delete output.
10. Return manifest.

### `cmd/csq/snapshot.go` and `cmd/csq/fetch.go`

Standard pflag-based subcommand handlers. Each constructs its `ProducerOptions` / `ConsumerOptions` and calls into `internal/snapshot`. On success they print a one-line summary to stderr.

### `cmd/csq/main.go`

Add `case "snapshot"` and `case "fetch"` to the dispatch switch. Update the `usage` const with the new lines.

## Manifest format & tarball layout

`manifest.json` (schema_version=1):

```json
{
  "schema_version": 1,
  "portal": "data.cityofchicago.org",
  "csq_version": "0.4.0",
  "snapshot_id": "01HXYZABCDEFGHJKMNPQRSTVWX",
  "created_at": "2026-04-23T12:00:00Z",
  "duckdb_filename": "data.cityofchicago.org.duckdb",
  "duckdb_sha256": "abc123def456...",
  "duckdb_size_bytes": 12345678,
  "dataset_count": 47,
  "total_row_count": 12345678
}
```

Field semantics:
- `schema_version` — int. Bumps on breaking format changes. Consumer rejects unknown values.
- `portal` — string. Filename-derived or `--portal` override. Display only; not parsed by the consumer.
- `csq_version` — string. Build-time constant for now (hardcoded `"0.4.0"`); a future task can wire `-ldflags`.
- `snapshot_id` — ULID string. Time-ordered for chronological sorting.
- `created_at` — RFC3339 UTC.
- `duckdb_filename` — basename only, no directories. `csq fetch` uses this to derive the default `--output`.
- `duckdb_sha256` — hex-encoded SHA-256 of the DuckDB bytes.
- `duckdb_size_bytes` — uncompressed byte size; sanity-checked by the consumer against the tar header.
- `dataset_count` — `SELECT COUNT(*) FROM _csq.catalog`.
- `total_row_count` — `SUM` of `rows_written` over the most-recent successful sync_runs row per dataset. Approximate by design (a dataset that's been bootstrapped but never re-synced contributes its bootstrap count).

Tarball layout (POSIX `tar.FormatPAX`):

1. `manifest.json` (small, ~500 bytes).
2. `<duckdb_filename>` (the DuckDB bytes).

No directory entries, no extra metadata files.

## Errors & failure modes

| Situation | Behavior |
|---|---|
| `csq snapshot --db <missing>` | Producer errors before temp-copy: `"snapshot: --db <path>: no such file"`. |
| Source DB lacks `_csq.catalog` | `"not a CivicSodaQuack DuckDB (no _csq.catalog in <path>)"`. |
| Temp-copy fails (disk full / permission) | OS error wrapped; no partial output. |
| Tar/zst write fails mid-stream | Partial `--output` deleted; underlying error returned. |
| `--output` exists, no `--force` | `"snapshot: <path> exists; pass --force to overwrite"`. |
| Source DB has staging tables AND `--keep-staging` | Producer leaves them in; manifest counts unchanged. |
| `csq fetch --from <bad-url>` | Underlying `net/http` or `os.Open` error wrapped. |
| Unsupported scheme (e.g. `s3://`) | `"fetch: unsupported scheme %q (want http, https, or file)"`. |
| HTTP non-2xx | `"fetch: HTTP <code>: <body-snippet>"` (body ~200 chars). |
| Truncated stream / corrupt zstd | Underlying decoder error wrapped, prefixed `"fetch: decode:"`. |
| First tar entry isn't `manifest.json` | `"fetch: unexpected first entry %q; want manifest.json"`. Output not written. |
| Manifest JSON parse error | `"fetch: manifest: <err>"`. Output not written. |
| `manifest.schema_version != 1` | `"fetch: unsupported schema_version %d (this build supports 1)"`. |
| Manifest `duckdb_filename` contains `/` or `..` | `"fetch: manifest declares unsafe filename %q"`. |
| Second tar entry filename ≠ manifest's | `"fetch: unexpected payload entry %q; manifest declared %q"`. |
| Tar header size ≠ `manifest.duckdb_size_bytes` | `"fetch: size mismatch: tar header %d, manifest %d"`. |
| sha256 mismatch after extraction | Output deleted; `"fetch: sha256 mismatch: got %s, manifest %s"`. |
| `--no-verify` and the file is corrupt | Consumer succeeds. Downstream `csq mcp --db <path>` fails at open time. |
| `--output` exists, no `--force` | `"fetch: <path> exists; pass --force to overwrite"`. |
| Mid-download network failure | Streaming write produces partial output; size-mismatch or sha256 catches it; partial deleted. |

## Testing

### Unit tests (`internal/snapshot/`)

`producer_test.go`:
- `TestPack_HappyPath` — fixture DB → pack → manually inspect tarball entries: first is `manifest.json` with expected fields, second matches `duckdb_filename` and is byte-equal to the source after staging cleanup.
- `TestPack_DropsStaging` — fixture with stray `_csq_staging.foo` table → after pack, the unpacked DB has no tables in `_csq_staging`.
- `TestPack_KeepStaging` — same source with `KeepStaging=true` → unpacked DB still has `_csq_staging.foo`.
- `TestPack_NotCSQDB` — DuckDB without `_csq.catalog` → error mentions "not a CivicSodaQuack DuckDB".
- `TestPack_OutputExists_NoForce` — pre-existing output → error.
- `TestPack_OutputExists_Force` — same, `Force=true` → overwrites.
- `TestPack_DatasetCountAndRowCount` — fixture seeded with 3 datasets and known `_csq.sync_runs.rows_written` → manifest counts match.

`consumer_test.go`:
- `TestFetch_FileURL_HappyPath` — pack to a temp file, fetch from `file://...` → output DuckDB opens, has expected catalog rows; returned manifest non-nil with correct fields.
- `TestFetch_HTTPHappyPath` — same but via `httptest.NewServer` serving the tarball bytes.
- `TestFetch_HTTPError` — server returns 500 → consumer errors with status code in message.
- `TestFetch_BadFirstEntry` — hand-craft a tarball where the first entry is the DuckDB → error.
- `TestFetch_UnsupportedSchemaVersion` — manifest with `schema_version: 99` → error mentions schema_version.
- `TestFetch_FilenameMismatch` — manifest declares `foo.duckdb` but tarball's payload is `bar.duckdb` → error.
- `TestFetch_UnsafeFilename` — manifest declares `../etc/passwd` → error before any write.
- `TestFetch_SizeMismatch` — pack normally, then truncate the tarball's payload bytes → consumer errors and partial output removed.
- `TestFetch_SHA256Mismatch` — pack normally, then mutate one byte in the payload region → consumer errors and partial output removed.
- `TestFetch_NoVerifySkipsSHA` — same, `NoVerify=true` → succeeds.
- `TestFetch_OutputExists_NoForce` / `TestFetch_OutputExists_Force` — analogous to producer.
- `TestFetch_UnsupportedScheme` — `s3://...` → error.

`tarzst_test.go`:
- `TestTarZst_RoundTrip` — write two entries through `tarZstWriter`, read back through `tarZstReader`, byte-equal.

`inspect_test.go`:
- `TestCountDatasets` — fixture DB with N catalog rows → returns N.
- `TestCountTotalRows` — fixture with mixed sync_runs (some failed, multiple ok per dataset) → returns SUM over latest-ok-per-dataset.

### Test fixtures

`internal/snapshot/fixtures_test.go` — adapt `seedFixtureDB` from Phase 3's `internal/mcpserver/fixtures_test.go`. Slight duplication is intentional (test packages independent). Both fixtures seed the same `_csq.*` schema.

### End-to-end CLI smoke (`cmd/csq/cli_smoke_test.go`)

`TestCSQ_Snapshot_RoundTrip_Smoke`:
1. Build the binary (already done in `TestMain`).
2. Seed a fixture DuckDB with two catalog rows + matching sync_runs.
3. Run `./csq snapshot --db <fixture> --output <tarball>` → exit 0; tarball file exists and is non-empty.
4. Run `./csq fetch --from file://<tarball> --output <restored.duckdb>` → exit 0.
5. Open the restored DuckDB and verify `_csq.catalog` contains the two seeded rows.

### Regression risk for Phase 1/2/3 tests

None. The snapshot package is isolated; the new CLI subcommands add `case` branches to `cmd/csq/main.go`'s dispatch but don't alter existing commands.

## Open questions

None. All decisions resolved during brainstorming:

- **Scope:** producer + consumer (B), not just producer (A) and not full hosted pipeline (C).
- **Payload:** DuckDB + `manifest.json` (B), no embedded YAML config.
- **Compression:** `.tar.zst` (A), Go-native via `klauspost/compress/zstd`.
- **Source URL discovery:** `--from` is required (C); a future indexed-name resolver is out of scope.
- **Snapshot input:** existing `.duckdb` file (A), not a wrapped sync.
- **Staging cleanup:** drop `_csq_staging` content via temp-copy by default; `--keep-staging` opts out.

## Future work (not Phase 4)

- Per-portal "snapshot index" hosted by the project (or by a user) so `csq fetch --portal chicago` resolves a name to a URL.
- GitHub Actions workflow that nightly runs sync + snapshot for popular portals.
- Snapshot signing (cosign / minisign) for distribution channels that don't provide content authentication.
- Wire `-ldflags` to inject the real `csq_version` into the manifest.
- Per-dataset "diff snapshots" that ship only changed rows since a baseline. Requires a content-addressable storage scheme and is significantly more complex.
- `csq fetch --output -` to stream the DuckDB to stdout (tricky because `*sql.Open("duckdb", ...)` needs a path).
