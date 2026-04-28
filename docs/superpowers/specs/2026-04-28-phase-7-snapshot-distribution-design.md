# Phase 7 — Snapshot distribution: index + reusable workflow

**Status:** design approved, awaiting implementation plan.
**Date:** 2026-04-28.
**Prior art:** Phase 4 (`csq snapshot` / `csq fetch`).

## Summary

Phase 7 closes the snapshot distribution loop with two additions:

- A **per-portal `index.json`** format and a `csq snapshot-index` subcommand to maintain it. `csq fetch --index <url>` resolves the latest snapshot (or a named one) and downloads it.
- A **reusable GitHub Actions workflow** (`workflow_call`) downstream repos invoke on a schedule to sync, snapshot, update the index, and publish to GitHub Releases or S3.

Snapshot signing is explicitly **not** in this phase. Without a verifier and a trust model, signing is performance, not security; we add it when there's a concrete user.

## Goals

- `csq fetch --index <url>` "just works" without the user knowing today's snapshot ID.
- `csq snapshot-index update` is a small, composable shell step that drops into any publishing pipeline (GitHub Actions, GitLab CI, a cron'd S3 sync).
- Reusable workflow that downstream repos invoke without forking.
- The CivicSodaQuack repo itself doesn't run any nightly anything — no project-wide reliability burden.

## Non-goals

- Project-hosted snapshots. Phase 7 publishes the *pattern*; the project doesn't run a bucket.
- Snapshot signing / verification. Out of scope.
- Cross-portal mega-indexes. One `index.json` per portal.
- HTTP listing or directory crawls. The index file is authoritative.
- Sharded / paginated indexes. A single JSON document per portal is fine for the foreseeable thousands of snapshots.
- `csq fetch --portal NAME` resolving via project-hosted lookup. The user passes the index URL explicitly.

## Architecture

### `csq fetch --index <url>`

New flag on the existing `csq fetch` subcommand. Mutually exclusive with `--from`.

Resolution:
1. Open the index URL via the existing `openURL` helper (`http(s)://` or `file://`).
2. Parse as `Index` via `LoadIndex`.
3. Pick an entry: `--snapshot <id>` if set, else `Latest()`.
4. Call existing `snapshot.Fetch` with the entry's `URL`.
5. Manifest SHA-256 verification (Phase 4) is unchanged.

The index entry's `sha256` is informational. The authoritative SHA-256 lives in the manifest, verified by `Fetch`.

### `csq snapshot-index` subcommand

```
csq snapshot-index update --index <path> --add <tarball> --url <url> [--max-keep N]
csq snapshot-index validate --index <path>
```

`update` reads the manifest from `<tarball>`, prepends an entry, optionally trims to N entries, atomic-writes back.

`validate` parses + sanity-checks an existing index file.

### Index format (schema_version = 1)

```json
{
  "schema_version": 1,
  "portal": "data.cityofchicago.org",
  "snapshots": [
    {
      "snapshot_id": "01HZ...",
      "created_at": "2026-04-28T03:00:00Z",
      "url": "https://snapshots.example.com/chicago/01HZ...tar.zst",
      "size_bytes": 12345678,
      "sha256": "abc..."
    }
  ]
}
```

Sorted newest-first by `snapshot_id` (ULIDs sort chronologically).

### `.github/workflows/snapshot.yml` (reusable)

`on: workflow_call` with inputs:
- `portal-yaml-path` (required)
- `output-tarball-name` (default `{portal}-{date}.tar.zst`)
- `release-target` (`github-release` | `s3`, required)
- `s3-bucket` (required when `release-target=s3`)
- `index-url-base` (required)
- `index-path` (default `snapshots/index.json`)

Secrets:
- `socrata-app-token` (optional)
- `aws-access-key-id` / `aws-secret-access-key` (required for `s3` target)

Steps: install `csq` → `csq sync` → `csq snapshot` → `csq snapshot-index update` → publish (gh release create | aws s3 cp).

### `docs/snapshot-publishing.md`

Short guide: example caller workflow + consuming via `csq fetch --index`.

## Components

### `internal/snapshot/index.go` (new)

```go
const IndexSchemaVersion = 1

type Index struct {
    SchemaVersion int           `json:"schema_version"`
    Portal        string        `json:"portal"`
    Snapshots     []IndexEntry  `json:"snapshots"`
}

type IndexEntry struct {
    SnapshotID string    `json:"snapshot_id"`
    CreatedAt  time.Time `json:"created_at"`
    URL        string    `json:"url"`
    SizeBytes  int64     `json:"size_bytes"`
    SHA256     string    `json:"sha256"`
}

func LoadIndex(r io.Reader) (*Index, error)
func (i *Index) MarshalIndent() ([]byte, error)
func (i *Index) Add(e IndexEntry, maxKeep int) // prepend + sort + trim
func (i *Index) Latest() (IndexEntry, bool)
func (i *Index) FindByID(id string) (IndexEntry, bool)
func ValidateIndex(i *Index) error
```

### `internal/snapshot/index_test.go` (new)

Round-trip; add+sort+max-keep; latest-empty; find-by-id; bad-JSON; validate.

### `cmd/csq/snapshot_index.go` (new)

`runSnapshotIndex(args)` dispatching `update` or `validate`. Uses existing tarball reading from `internal/snapshot` (open file → `newTarZstReader` → first entry → `ParseManifest`).

Mode `update`:
- Required flags: `--index`, `--add`, `--url`
- Optional: `--max-keep <int>`
- Validates `index.portal == manifest.portal` if file pre-exists.
- Atomic write via temp + rename.

Mode `validate`:
- Required: `--index`
- Errors with the offending entry index and field name.

### `cmd/csq/fetch.go` (modified)

Add `--index <url>` and `--snapshot <id>`. Mutually exclusive with `--from`. Resolves the entry, then calls existing `snapshot.Fetch` with the resolved URL.

### `cmd/csq/main.go` (modified)

Dispatch `case "snapshot-index"`. Usage update.

### `.github/workflows/snapshot.yml` (new)

Reusable workflow as specified. Tests: review-only.

### `docs/snapshot-publishing.md` (new)

Short markdown guide.

### `cmd/csq/cli_smoke_test.go` (modified)

`TestCSQ_SnapshotIndex_Smoke`: pack two tarballs, `csq snapshot-index update` twice, validate, assert order + count.

`TestCSQ_Fetch_ViaIndex_Smoke`: pack a tarball, write a hand-crafted index pointing at `file://<tarball>`, `csq fetch --index file://<index.json>`, assert restored DuckDB opens.

### `README.md` (modified)

Phase 7 paragraph in the Distribution section: `--index` example + pointer to the new doc.

## Errors & failure modes

| Situation | Behavior |
|---|---|
| `csq fetch --from X --index Y` | Parse error: `"--from and --index are mutually exclusive"`. |
| `csq fetch --index <bad-url>` | Underlying open/HTTP error wrapped. |
| `csq fetch --index <url>` index has zero snapshots | `"index <url>: no snapshots available"`. |
| `csq fetch --index <url> --snapshot <id>` id not in index | `"snapshot %q not in index <url>"`. |
| Index parse failure | JSON decoder error wrapped. |
| `csq snapshot-index update` tarball unreadable | Same as `csq fetch` tarball errors. |
| `csq snapshot-index update` `--url` missing | Parse error: `"--url is required"`. |
| Existing index `portal` differs from tarball manifest | `"index portal %q != tarball portal %q"`. |
| `csq snapshot-index validate` schema_version != 1 | Error names the version. |
| `csq snapshot-index validate` malformed entry | Error names entry index and field. |
| Atomic write fails (disk full) | Underlying OS error wrapped; original index untouched. |

## Testing

Per-component tests as listed above. The reusable workflow YAML is reviewed by inspection (no test infrastructure for that). The Go primitives it invokes have their own coverage.

## Open questions

None. Decisions resolved during brainstorming.

## Future work (not Phase 7)

- Snapshot signing (cosign/minisign).
- A project-hosted snapshot index for a curated portal list.
- Pagination / sharding the index when it crosses some millions-of-entries threshold (likely never).
- An `IndexEntry.tags` field for marking snapshots as `latest-stable`, `release-candidate`, etc.
- `csq fetch --index <url> --snapshot latest` syntactic sugar (currently just omitting `--snapshot` selects latest).
