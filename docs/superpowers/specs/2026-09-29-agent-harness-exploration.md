# Agent harness: exploration report

Date: 2026-09-29. Branch: `harness`, cut from `main` at `3eb4a35`.

This is a read-only survey. It is not a design and approves nothing. It records
what already exists across the repositories the harness would draw on, what is
missing, and the decisions that are the owner's to make.

## What we are building

A chat-first terminal application over civic open data. An LLM agent reads the
DuckDB files csq materialises from Socrata portals, explores and syncs datasets,
and pushes widgets into the UI: tables, charts, 3D charts, maps, and document
viewers. The agent keeps a scratchpad of working notes and writes to a knowledge
base that models use to remember things about datasets and share them.

csq is the instrument. The agent is a user of csq that orchestrates analysis and
presentation on top of it.

## Sources read

| Repository | State read |
|---|---|
| `neomantra/CivicSodaQuack` | `main` at `3eb4a35` |
| `AgentDank/dank-bot420` (private) | default branch, shallow clone |
| `NimbleMarkets/ds4go-apps` (private) | default branch, shallow clone |
| `NimbleMarkets/ds4go` | default branch, `scratchtool/` |
| `NimbleMarkets/flint-ntcharts` | local checkout, `main` at `b3e8768`, with uncommitted changes |
| `NimbleMarkets/flint-chart` | local checkout |
| `NimbleMarkets/ntcharts` | local `spec` branch at `7b5f1c4` |
| `NimbleMarkets/ntcharts3d` | `origin/main`, v0.1.0 |
| `NimbleMarkets/ntcharts-osm`, `-pdf`, `-svg` | default branches |

Claims marked **verified** were checked by reading the cited code or by running
something. Claims marked **inferred** were not.

## Findings

### 1. dank-bot420 already has the push mechanism

**Verified.**

- `Presentation` is a single struct with a string `Kind` and fields for every
  kind side by side (`internal/agent/agent.go:183-190`).
- A tool pushes one by calling `present(ctx, ...)`, which looks up a
  `PresentFunc` carried in the context (`agent.go:193-201`).
- `present_table` runs the SQL host-side, pushes the rows to the UI, and
  returns only a row and column summary to the model (`agent.go:395-441`).
  The rows never enter the model's context.
- The TUI receives presentations over a channel and switches on `Kind`. Only
  `table` and `product` are handled; anything else becomes a placeholder line
  (`internal/tui/model.go:671-696`, `internal/tui/present.go:53-59`).
- The TUI depends on the agent through a six-method `Chat` interface
  (`internal/tui/model.go:23-30`). Three of the six are product-specific.
- `internal/tui/model.go` is 1,639 lines and mixes generic chat behaviour
  with cannabis product behaviour.

### 2. flint-ntcharts is an importable compile and render path

**Verified.**

- `compile.New` and `(*Runner).Compile` turn a Flint `ChartAssemblyInput` into
  an ntcharts `spec.Spec` (`compile/compile.go:34`, `:116`).
- `tui.Render(compiler, raw, w, h)` returns the rendered terminal string for
  a document sized to a cell area (`tui/render.go:118-121`). It accepts Flint
  input, a compiled envelope, or a raw `spec.Spec`, chosen by top-level key
  (`tui/render.go:26-42`).
- The terminal backend has seven templates: bar, stacked bar, line, scatter,
  heatmap, sparkline, candlestick (`js/src/ntcharts/templates/`).
- `go.mod` carries `replace github.com/NimbleMarkets/ntcharts/v2 => ../ntcharts`
  and the repository has no release tag.

### 3. csq's tool handlers are clean but unexported

**Verified.**

- Each MCP tool is a thin closure over a handler with typed arguments and a
  typed result (`internal/mcpserver/server.go:60-140`).
- The handlers are lowercase (`listDatasetsHandler`, `querySQLHandler`, and
  so on), so nothing outside `internal/mcpserver` can call them.
- `sync_dataset` blocks until the sync ends and passes a `RecordingReporter`,
  so progress events are recorded and then discarded
  (`internal/mcpserver/tools_sync_dataset.go:69-80`).
- `sync.ProgressReporter` is an interface with start, progress, and done
  callbacks (`internal/sync/progress.go:14`). A reporter that posts messages
  to the TUI fits it without changes to `sync`.

### 4. `query_sql` lets a model read local files

**Verified by running it** against DuckDB 1.5.5.

- `query_sql` relies on `BEGIN TRANSACTION READ ONLY`
  (`internal/mcpserver/tools_query_sql.go:53`).
- A read-only transaction blocks writes. It does not block
  `read_csv('/any/path')`. The test read a file outside the database and
  returned its contents.
- `SET enable_external_access=false` blocks the same query with a permission
  error.
- Nothing in csq sets `enable_external_access`.
- dank-bot420 latches it in the connector's init callback after loading its
  one external file (`internal/dank/db_native.go:38-56`), and also rejects
  non-SELECT statements by keyword (`internal/dank/query.go:189-216`).

This affects the existing `csq mcp` server today, independent of the harness.

### 5. Socrata's column semantics are stored but not surfaced

**Verified** against the live Chicago catalog API.

- Every catalog entry carries `columns_field_name`, `columns_datatype`,
  `columns_name`, `columns_description`, and `columns_format`.
- csq stores the whole entry in `_csq.catalog.raw`
  (`internal/duckdb/migrations.go:16-26`, `internal/socrata/catalog.go:133`).
- `describe_dataset` returns only DuckDB column names and types from
  `information_schema` (`internal/mcpserver/tools_describe_dataset.go:200-221`).
- The type mapping collapses `number`, `percent`, and `money` into `DOUBLE`
  (`internal/socrata/types.go:28-42`), so the distinction is lost in the
  table but still present in `raw`.

### 6. The scratchpad package exists but is bound to ds4go

**Verified.**

- `ds4go/scratchtool` is a flat key to text store, one file per key, with
  atomic writes, size limits, and symlink rejection (`scratchtool/doc.go`).
- Its store operations are unexported (`store.go:66`, `:80`, `:91`, `:158`).
  The only public surface registers tools on a `ds4.ToolRegistry`
  (`scratchtool.go:113`).
- `ds4go-apps/internal/trippad/memory` adds session and global scopes,
  host-owned keys, a bounded brief, and a CLI that needs no model.
- The non-test code is about 530 lines.

### 7. There is no knowledge base precedent

**Verified.** dank-bot420 keeps its dataset knowledge in
`internal/agent/system_prompt.md`, including a regime change in how flower was
recorded before and after September 2023.

### 8. Dependency weight differs by a wide margin

**Verified.**

| Module | `go.mod` lines | Go floor |
|---|---|---|
| csq `main` | 39 | 1.26.0 |
| dank-bot420 | 180, of which 156 indirect | 1.26.3 |
| ntcharts v2.4.0, ntcharts-osm, -pdf, -svg | | 1.26.8 |
| ntcharts3d v0.1.0 | | 1.26.0 |
| flint-ntcharts | | 1.26.4 |

dank-bot420 pins Fantasy v0.28.0. The latest published is v0.45.2.

### 9. There is no csq-format database on this machine

**Verified.** `data.cityofchicago.org.duckdb` in the repository root dates from
2026-04-19 and has no `_csq` schema, so `csq mcp` would reject it. A first run
of anything needs a sync.

## Reuse map

| Capability | Where it is today | How to reuse |
|---|---|---|
| Agent loop, provider switching | dank-bot420 `internal/agent/agent.go` | Copy and strip. It is `internal` in another module and carries product logic. |
| Push channel from tool to UI | dank-bot420 `agent.go:183-201` | Copy the pattern; replace the struct with a typed payload. |
| Chat transcript, cells, docked table, focus cycle, overlay | dank-bot420 `internal/tui` | Copy and split. `model.go` needs the product code removed. |
| Slash-command registry and menu | dank-bot420 `internal/tui/commands.go`, `command_menu.go` | Copy. |
| Session JSONL telemetry | dank-bot420 `internal/telemetry` | Copy. |
| Catalog, describe, search, SQL, sync | csq `internal/mcpserver` | Import, after the handlers are exported. |
| Sync progress in the UI | csq `internal/sync` `ProgressReporter` | Import. Write a new reporter. |
| Flint to `spec.Spec` | flint-ntcharts `compile` | Import, once it no longer needs `replace`. |
| `spec.Spec` to terminal chart | ntcharts `spec.Build` | Import, once released. |
| 3D charts | ntcharts3d v0.1.0 | Import. |
| Maps, PDF, SVG | ntcharts-osm `mapview`, ntcharts-pdf `pdfview`, ntcharts-svg `svg` | Import. Each is a Bubble Tea model with `SetSize`, `Update`, `View`. |
| Scratchpad | ds4go `scratchtool` | Port, or export the store upstream. |
| Knowledge base | nothing | New. |
| Headless one-shot mode | ds4go-apps `cmd/svgpad/oneshot.go` | Copy the pattern. |
| Render-then-inspect review | ds4go-apps `cmd/svgpad/visual.go` | Later. Needs a vision model. |

## Answers to the exploration questions

### Presentation protocol

Replace the flat struct with a kind plus a typed payload per kind. For charts,
the agent sends SQL, a Flint `chart_spec`, and `semantic_types`. The host runs
the SQL, binds the rows as `data.values`, compiles through flint-ntcharts, and
builds the widget. The model gets back a summary: row count, columns, and any
compiler warnings.

| Payload | Who resolves data | Fit |
|---|---|---|
| Flint input plus SQL | Host | Default for the seven supported chart types. |
| ntcharts `spec.Spec` plus SQL | Host | Escape hatch when Flint cannot express the chart. |
| Direct widget config plus SQL | Host | Needed for 3D, maps, and viewers, which neither Flint nor `spec` covers. |

### Semantic layer

Socrata datatypes give a first mapping to Flint types at no model cost:

| Socrata | Flint |
|---|---|
| Calendar date | `DateTime` |
| Money | `Amount` |
| Percent | `Proportion` |
| Number | `GenericMeasure` |
| Checkbox | `Coded` |
| Point, Location | `GeoCoordinate` |
| Text | `Categorical` family |

Flint's coarsest tier is designed to be inferred by heuristics, so an unmapped
column degrades to generic formatting instead of failing.

The mapping is wrong in a predictable way: civic data stores codes as numbers.
Ward, community area, and ZIP code arrive as `Number` and would be summed.
This is the first job for the knowledge base.

### Widget fit

| Widget | Suits | Misrepresents |
|---|---|---|
| Bar | Counts by category, top-N | Many categories; terminal bars cannot be grouped side by side |
| Time series | Monthly or yearly counts and totals | Series with gaps presented as continuous |
| Heatmap | Hour by weekday, category by period | Anything needing row labels, which the terminal heatmap does not draw |
| Scatter | Two measures per record | Point weight; `DataPoint.Size` is ignored |
| 3D bars | Two categorical axes and one measure | Comparisons where occlusion hides bars |
| 3D surface, textured | Gridded values over an area | Point data that has not been gridded |
| Map | Geocoded records | Large point counts without aggregation |
| PDF, SVG | Source documents linked from a dataset | |

Axis titles are ignored on both ntcharts surfaces, so a chart cannot state its
units on the axis.

### Agent runtime

| Option | Local models | Hosted models | Cost |
|---|---|---|---|
| Fantasy | Yes, through Kronk | Yes | Large dependency graph |
| ds4go driver | Yes, needs libds4 and a large GPU | No | Ties the harness to one engine |
| csq MCP tools over the wire | | | A second process, and the DuckDB lock blocks in-process sync |

Fantasy is the one already proven for this shape of application.

### Actions with side effects

Reads need no confirmation. A sync hits a public API and writes to disk, and a
full refresh replaces a table, so both should ask first.

An attached database is a snapshot taken at attach time. After a sync the host
must re-attach to see new rows. Because `enable_external_access=false` cannot
be turned back on, re-attaching means rebuilding the host connection.

### Layout

Carries over unchanged: transcript, numbered cells, the docked panel, Tab focus
cycling, the fullscreen overlay, slash commands.

Needs new work:

- Widgets own key bindings that collide with typing. The map pans on `hjkl`
  and the 3D chart orbits on the same keys. Input must go to a widget only
  while it has focus.
- Glyph charts are text and can sit inline in the transcript. Image-backed
  widgets are Kitty placements and belong in the docked panel or overlay.
- Several live image widgets need distinct Kitty IDs.

### Testability

For a first version: a headless one-shot mode that takes a prompt and emits
the session JSONL and the presentations as JSON, plus golden renders for
charts. The render-then-inspect loop can wait.

### Browser

A later step. It rules out nothing now if data access sits behind an interface,
as dank-bot420 does with build-tagged database openers. Syncing from a browser
would meet cross-origin limits.

## Candidate architectures

### A. A `csq chat` subcommand in the csq module

One binary. csq's `go.mod` grows from 39 lines to several hundred and its Go
floor rises to 1.26.8. Every csq user pays for an agent runtime and a GPU
stack they may not use.

### B. A nested Go module in the csq repository

A directory with its own `go.mod` and its own binary. Go's `internal` rule is
based on import path, so a module at `github.com/neomantra/CivicSodaQuack/...`
can import csq's `internal` packages. csq keeps its small graph. ntcharts
already uses this layout for `examples` and `chartpicture`.

Cost: two modules to tag, and a workspace file for local development.

### C. A separate repository

The shape dank-bot420 has relative to dank-mcp. csq's packages would have to
move out of `internal` and become a public API with compatibility obligations.

### Recommendation

B. It keeps csq small, shares code without publishing an API, and keeps the
harness next to the data layer it depends on.

## Smallest vertical slice

One portal, one question, one pushed chart.

1. Sync one Chicago dataset into a csq-format database.
2. Start the harness against it with a hosted model.
3. Ask how a count has changed by month.
4. The agent calls `describe_dataset`, then a new `present_chart` tool with
   SQL, a Flint chart spec, and semantic types.
5. A time series chart appears inline as a numbered cell.
6. The session JSONL records the turn and the tool call.

Out of the slice: 3D, maps, viewers, scratchpad, knowledge base, sync from
inside the chat, local models. Each is a following increment.

## Upstream blockers, in order

1. **ntcharts.** The `spec` package exists only on a local branch, 32 commits
   ahead of `origin/v2`. It needs pushing and a release.
2. **flint-ntcharts.** Commit the pending work, replace the `replace`
   directive with a requirement on the released ntcharts, and tag a release.
3. **ntcharts3d.** It pins ntcharts at a commit before v2.4.0. The next
   ntcharts release moves image decoder registration into `picture/decoders`,
   which is a breaking change, so ntcharts3d needs checking against it.
4. **csq.** Export the tool handlers. Add the external-access latch.

A Go workspace with sibling checkouts lets harness work start before any of
these are released. It does not let the harness be released.

## Decisions for the owner

1. Where the harness lives, and what the binary is called.
2. Fantasy as the agent runtime.
3. Knowledge base storage: plain files that diff and merge through git, or
   DuckDB tables that are queryable with SQL. Inside a portal's database, notes
   would be replaced along with the file when a snapshot is fetched.
4. Scratchpad: port `scratchtool` into the harness, or export its store
   operations upstream in ds4go.
5. Whether to fix the `query_sql` file-read gap in `csq mcp` now, as its own
   change.
6. Whether to release ntcharts `spec` and flint-ntcharts first, or develop
   against a workspace.
7. Which agent actions ask for confirmation.
