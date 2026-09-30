# csq-chat: first slice design

Date: 2026-09-30. Follows the
[exploration report](2026-09-29-agent-harness-exploration.md).

Decided by the owner: a nested Go module in this repository, with Fantasy as
the agent runtime. The ntcharts and flint-ntcharts relationship is being worked
out separately, so this slice stops at the chart contract and does not render
charts.

## Goal

A terminal chat over csq databases. A model answers questions by calling csq's
catalog, describe, search, and SQL tools in-process and by pushing tables to the
screen. Every turn is recorded to a session file. A headless mode runs one
prompt without a terminal, for tests and scripts.

## Layout

```
chat/                        module github.com/neomantra/CivicSodaQuack/chat
  go.mod                     go 1.27 (Fantasy's floor); replace parent => ../
  cmd/csq-chat/              the binary
  internal/
    present/                 what a tool can push to the screen
    data/                    csq databases behind one small interface
    agent/                   Fantasy runner, tools, system prompt
    session/                 JSONL recorder
    tui/                     Bubble Tea chat model
```

The module imports the parent's `internal` packages. Go's `internal` rule is by
import path, and `.../CivicSodaQuack/chat` is inside `.../CivicSodaQuack/`.

## Changes to csq itself

1. `internal/mcpserver` exports its six handlers. The MCP registration keeps
   calling them; nothing else changes.
2. `OpenPools` sets `enable_external_access=false` on the host after the last
   `ATTACH`. A read-only transaction blocks writes but not `read_csv` on an
   arbitrary path; this closes that. It also applies to `csq mcp`.

## Presentation contract

```go
type Presentation struct {
    Kind  Kind   // "table" | "chart"
    Title string
    Table *Table // Kind == table
    Chart *Chart // Kind == chart
}
type Table struct { Columns []string; Rows [][]string; Total int; Truncated bool }
type Chart struct {
    Table                        // the rows the chart is drawn from
    ChartSpec     json.RawMessage // Flint chart_spec
    SemanticTypes json.RawMessage // Flint semantic_types
}
```

A tool pushes a presentation through a `PresentFunc` carried in the context,
as dank-bot420 does. The rows are resolved host-side; the model receives a
summary, never the rows.

`Chart` is defined so the tool surface and the session format are settled now.
The `present_chart` tool registers only when the runner is given a renderer,
and this slice provides none.

## Tools

| Tool | Backed by | Returns to the model |
|---|---|---|
| `list_datasets` | `mcpserver.ListDatasets` | dataset summaries as JSON |
| `search_datasets` | `mcpserver.SearchDatasets` | dataset summaries as JSON |
| `describe_dataset` | `mcpserver.DescribeDataset` | columns, last sync, tags |
| `query_sql` | `mcpserver.QuerySQL` | CSV, capped by bytes |
| `present_table` | `mcpserver.QuerySQL` then push | title, row count, columns |

Sync is not in this slice. It writes to disk and reaches a public API, so it
needs a confirmation path in the UI first.

## Agent

- The model string picks the provider: `anthropic/…`, `openai/…`,
  `openrouter/…`, `google/…`, or `compat/…` with `--base-url` for any
  OpenAI-compatible server, including a local Kronk server. Embedded Kronk is
  not in this slice.
- History is bounded by message count and characters, as in dank-bot420.
- Each turn streams so the UI can show progress.
- The system prompt describes csq's shape: attached portals, the `_csq`
  schema, `<alias>.main.<table>` naming, and table-first answering.

## Session file

One JSONL file per run under `--session-dir`, default `~/.csq/sessions`.
Events: `session_start`, `turn`, `tool_call`, `present`. A presentation event
records the kind, title, columns, and row count, not the rows.

## TUI

Transcript with user, assistant, and presentation entries; tables render
inline as numbered cells; a one-line prompt; a header with model and portal
names; slash commands `/help`, `/clear`, `/status`, `/quit`. The docked panel,
filtering, and focus cycling from dank-bot420 come in a later slice once there
is more than one widget kind.

## Headless mode

`csq-chat --db … -m … --prompt "…"` runs one turn and prints the response,
then each presentation as JSON, to stdout. It exits non-zero on error. This is
the test surface for the agent and the sample surface for scripts.

## Testing

- `present`: contract marshalling.
- `data`: against a seeded csq-format fixture, copied from the mcpserver tests.
- `agent`: a fake `fantasy.LanguageModel` that returns scripted tool calls,
  then text. No network.
- `tui`: rendering of a transcript with one table cell.
- `cmd`: headless run against the fixture and the fake model.

## Out of this slice

Charts, 3D, maps, viewers, scratchpad, knowledge base, sync from chat, embedded
models, the browser build.
