# csq-chat

A terminal chat over CivicSodaQuack databases. A model answers questions
about the attached portals by calling csq's catalog, describe, search, and
SQL tools in-process, and by pushing tables to your screen.

This is a nested Go module (`github.com/neomantra/CivicSodaQuack/chat`) so
that `csq` itself keeps a small dependency graph. It imports csq's internal
packages directly and needs no published csq version.

## Run

```sh
# From the repository root:
task build-chat            # -> bin/csq-chat

# Hosted model
ANTHROPIC_API_KEY=… bin/csq-chat --db chicago=.csq/chicago.duckdb -m anthropic/claude-sonnet-4-6

# Local model through any OpenAI-compatible server (Ollama shown)
bin/csq-chat --db chicago=.csq/chicago.duckdb -m compat/qwen3:14b --base-url http://localhost:11434/v1

# One turn, no terminal: prints the reply and the tables as JSON
bin/csq-chat --db chicago=.csq/chicago.duckdb -m compat/qwen3:14b --base-url http://localhost:11434/v1 \
  --prompt "How many COPA cases per year for the last 5 years?"
```

A headless run prints the tables and charts as JSON; each chart is also drawn
at 80×20 with styling removed in the `rendered` array, so a run can be read
without a terminal.

Models are named `vendor/model`: `anthropic/`, `openai/`, `google/`,
`openrouter/`, or `compat/` with `--base-url`. Keys come from `--api-key` or
the vendor's environment variable. There is no embedded model runtime yet.

`--db` takes a csq database made by `csq sync`, as a path or `alias=path`,
and repeats for several portals. Tables are addressed as
`<alias>.main.<table>` in SQL, the same as `csq mcp`.

Environment variables mirror the main flags: `CSQ_CHAT_MODEL`,
`CSQ_CHAT_BASE_URL`, `CSQ_CHAT_API_KEY`, `CSQ_CHAT_SESSION_DIR`, `CSQ_CHAT_LOG`.

## In the window

| Command | Does |
|---|---|
| `/help` | list commands |
| `/status` | model, portals, session file, last turn's counts |
| `/tables` | the tables shown this session, numbered |
| `/clear` | forget the conversation (also ctrl+l) |
| `/quit` | exit (also ctrl+c when idle; ctrl+c cancels a running turn) |

↑/↓ and PgUp/PgDn scroll the transcript.

## What the model can do

| Tool | Backed by | The model receives |
|---|---|---|
| `list_datasets` | `mcpserver.ListDatasets` | dataset summaries |
| `search_datasets` | `mcpserver.SearchDatasets` | dataset summaries |
| `describe_dataset` | `mcpserver.DescribeDataset` | columns, tags, last sync |
| `query_sql` | `mcpserver.QuerySQL` | CSV, capped at 24 KB |
| `present_table` | `mcpserver.QuerySQL`, then the screen | title, row count, columns |
| `present_chart` | `mcpserver.QuerySQL`, flint-ntcharts, then the screen | title, row count, columns, compiler notes |

`present_chart` takes SQL, a typed Flint `chart_spec` (a `chartType` from an
enum plus `encodings` mapping result columns to channels) and a list of
`semantic_types`. The host binds the rows as `data.values`, compiles with the
embedded Flint compiler, and draws the result as a numbered cell that is
re-fitted on resize. If a chart cannot be drawn the model receives the
compiler's reason, which names the supported types, and nothing is shown.
Text charts only: pie, boxplot, waterfall and the like are refused.

`present_table` is the model's default way to answer. The rows are resolved
here and shown to you; the model is told only the shape, so a large result
costs it nothing. SQL runs read-only with DuckDB external access disabled, so
the model cannot write, read local files, or fetch URLs.

## Notes (the scratchpad)

The model keeps small text notes in two scopes, under `--scratch-dir`
(default `~/.csq/scratch`; empty disables):

- **session** notes (`sessions/<name>/`) are its plan, findings and open
  questions. `--session NAME` resumes them; without it a fresh name is used.
  The host writes the person's latest message to the read-only note
  `latest-request`, so the model can recover it after history is trimmed.
- **global** notes (`global/`) are shared by every session and every model.
  They are where the model records what it learns about the data.

One file per key (`a-z 0-9 . _ -`, 64 characters), 16 KB per note, 32 notes /
128 KB per session pad and 128 notes / 512 KB for global. Tools: `scratch_list`,
`scratch_get`, `scratch_set`, `scratch_append`, `scratch_delete`, each with an
optional `scope`. A session that starts with notes present is told their keys
and sizes, never their contents. Notes are written by models, so they are leads
to verify, not facts; nothing in them feeds csq's own arithmetic. The store is
adapted from ds4go's `scratchtool`.

## Session files

Each run appends one JSON object per line to
`~/.csq/sessions/<timestamp>-<pid>.jsonl` (`--session-dir`; empty disables):
`session_start`, then per turn `present`, `tool_call`, and `turn` records.
Presentations are recorded by title, columns, and row count, never rows.

```sh
duckdb -c "SELECT turn, tool, duration_ms, input FROM read_json_auto('~/.csq/sessions/*.jsonl') WHERE type='tool_call' ORDER BY duration_ms DESC"
```

## Layout

```
cmd/csq-chat/       the binary; --prompt is the headless path
internal/present/   what a tool can push to the screen, and the summary the model sees
internal/data/      csq databases behind one interface (datatest/ seeds fixtures)
internal/agent/     Fantasy runner, tools, system prompt (agenttest/ has a scripted model)
internal/scratch/   the model's notes, session and global scope
internal/session/   JSONL recorder
internal/tui/       Bubble Tea window
```

## Develop

```sh
cd chat
go test ./...
go vet ./...
```

The tests need no model and no network: `agenttest.FakeModel` plays a script
of tool calls and replies, and `datatest.SeedDB` writes a csq-shaped
database. The binary's own test runs the headless path through both.

## Not yet

Raster charts (flint-ntcharts v0.3.0 can draw more chart types as images over
Kitty graphics; this build uses the text renderer only), 3D, maps, document
viewers,
a knowledge base, syncing from inside the chat, embedded models, and the
browser build.
