# The notes exercise

A reproducible protocol for watching how a model uses csq-chat's scratchpad on
real data. It produced the findings in
`docs/superpowers/specs/2026-10-03-knowledge-base-findings.md` with a local
`qwen3:14b`; run it with another model to see whether those findings hold.

Everything is headless: one `csq-chat --prompt` per session, with the notes
and the session record written under `.csq/ex-<label>/`.

## Setup (once)

```sh
task build-chat                                   # bin/csq-chat
mkdir -p .csq && go build -o csq ./cmd/csq && ./csq sync --config chat/exercise/chicago.yaml
```

`chicago.yaml` syncs four Chicago datasets (about 260,000 rows, 1–2 minutes)
into `.csq/chicago.duckdb`: police stations, fire stations, COPA complaint
summaries (`mft5-nfa8`) and homicide / non-fatal shooting victims
(`gumc-mgzr`). `truth.sh` prints the ground truth the answers are graded
against; it needs the `duckdb` CLI and no model. Row counts drift as the portal
updates, so compare answers with `truth.sh` run the same day.

## Run

Pick a model and give the run a label, then run the three batches in order.
Every batch must use the same `MODEL` and `LABEL`, because batch 2 and 3 read
notes the earlier ones left behind.

```sh
export MODEL=anthropic/claude-sonnet-4-6    # vendor/model; compat/<name> + BASE_URL for a local server
export LABEL=sonnet                         # outputs: .csq/ex-sonnet/
# keys: ANTHROPIC_API_KEY, OPENAI_API_KEY, GEMINI_API_KEY, OPENROUTER_API_KEY

chat/exercise/sessions.sh 1
chat/exercise/sessions.sh 2     # plants a deliberately FALSE note; that is part of the test
chat/exercise/sessions.sh 3
python3 chat/exercise/analyze.py .csq/ex-$LABEL
chat/exercise/truth.sh
```

`analyze.py` prints, per session, the tool sequence, every note the model wrote
(in full), and its final answer. The per-session `.json` files hold the full
output including tables, and `.err` the progress log.

## What each batch tests

| Batch | Sessions | Question |
|---|---|---|
| 1 | copa-1, copa-2, victims-1 | Without being asked, does the model write notes, read notes, or look at a column's real values before filtering on it? |
| 2 | victims-2, victims-3, copa-3 | Told to record notes, does it check the data first, and are its notes right? Does a fresh session benefit? Does it read a planted false note unprompted? |
| 3 | copa-4, victims-4 | Told to read notes first, does it list them, and does it trust a wrong one? |

The planted note (`planted-note.txt`) claims that COPA's `complaint_date` is a
case-closing date, that the data is complete through 2026-12-31, and that
counts are about 12,000 a year. All three are false.

## Reading the results

Grade against `truth.sh`, not against what the model says. The traps are: the
gunshot flag is `YES`/`NO` (not `Y`); `ward`/`district` are numbers that are
really codes; `age` is a text bucket; 2026 is a partial year; and DuckDB has no
`TO_CHAR`. Note whether a model states a number it never queried.
