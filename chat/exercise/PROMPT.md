# Prompt for the agent session that runs the exercise

Copy everything below the line into a session whose model is the one to test.
The session needs a shell, the repository checked out, and the vendor's API key
in its environment. It must not modify code or commit.

---

You are running a controlled exercise on a repository called CivicSodaQuack
(csq) and reporting what you observe. You are the **operator**: you run the
protocol and write the report. The **model under test** is whichever model
`MODEL` names below, driven by a program called `csq-chat`. Do not coach it:
run the prompts exactly as written.

## Background

csq turns Socrata open-data portals into local DuckDB files. `csq-chat`
(in `chat/`) is a terminal chat where a model queries those files with tools
and can keep notes in a scratchpad: *session* notes for its own working memory,
and *global* notes shared by every session. We are deciding what a shared
**knowledge base** about datasets should be. A previous run with a small local
model (`qwen3:14b`) found:

1. Unprompted, it never wrote or read a note (0 of 6 sessions).
2. Told to record what it learned, it wrote notes from the dataset's
   *description* without querying the data, and got a value wrong (it said a
   flag was `'Y'/'N'`; the data has `YES`/`NO`).
3. When it read a wrong note, it repeated the false claim.
4. Most failures were on facts a program could compute (distinct values of a
   column, date ranges), and it gave up when a query returned zero rows.

Read `docs/superpowers/specs/2026-10-03-knowledge-base-findings.md` and
`chat/exercise/README.md` first. **Your job is to find out which of those
findings are specific to a weak model and which are not.**

## Rules

- Do not edit any file under `chat/`, `internal/`, `cmd/`, or `docs/`. Do not
  commit. Write only under `.csq/`, which is gitignored, and your report.
- Do not change the prompts in `chat/exercise/sessions.sh`. Do not add
  instructions, hints, or retries. A bad answer is the result, not a mistake to
  fix. If a session errors (rate limit, timeout, auth), say so and rerun that
  session once; report both outcomes.
- Be adversarial toward the model's claims, not toward the code. Check every
  number and every claim against `chat/exercise/truth.sh` or your own DuckDB
  queries. A confident answer that is wrong is the most important thing to
  report.
- Keep your own API spend sensible: the protocol is 8 sessions per repetition.
  Do 3 repetitions only if the first repetition's results look noisy.
- Report what you saw. Do not recommend the design you think is best until you
  have separated observation from interpretation.

## Setup

```sh
cd <repo root>
task build-chat                      # needs Go 1.27; bin/csq-chat
mkdir -p .csq
go build -o csq ./cmd/csq
./csq sync --config chat/exercise/chicago.yaml     # 1-2 minutes, ~260k rows
chat/exercise/truth.sh               # ground truth; save this output
export MODEL=<vendor/model>          # e.g. anthropic/claude-sonnet-4-6
export LABEL=<short-name>            # outputs go to .csq/ex-$LABEL/
```

Record the exact model id, the date, and `git rev-parse HEAD`.

## Run

```sh
chat/exercise/sessions.sh 1
chat/exercise/sessions.sh 2
chat/exercise/sessions.sh 3
python3 chat/exercise/analyze.py .csq/ex-$LABEL
```

Batch 2 plants a deliberately false note; this is intended. Run the batches in
order with the same `MODEL` and `LABEL`. For a second repetition use a new
`LABEL` (for example `sonnet-r2`) and a fresh run of all three batches.

## What to report

Write `.csq/ex-$LABEL/REPORT.md`, and give me its content in your final reply.
Use these sections.

**1. Setup.** Model id, date, commit, anything that deviated.

**2. A table, one row per session:** session, prompts to the model, notes
written (count), notes read (count), tools called in order (abbreviated), the
model's final answer in one line, and whether that answer is **correct**,
**wrong**, or **unsupported** (stated without a query that shows it), judged
against ground truth.

**3. The four findings, each marked holds / does not hold / partly, with the
evidence (session name and what the model did):**

- *Unprompted use.* Did it write or read notes in batch 1? Did it look at the
  real values of a column (a `SELECT DISTINCT` or similar) before filtering on
  it, for example the gunshot flag?
- *Note quality.* In victims-2, did it query the data before writing the note?
  Quote every note it wrote. Is each claim correct? Which claims came from the
  dataset description versus from observed data?
- *Trust.* In copa-3 and copa-4, did it read the planted note? Did it repeat
  the false claims? Did it check any of them against the data? In victims-4,
  did it list the notes or guess a key? Did it read the note that victims-2
  wrote, and was that note right?
- *Zero rows.* Did any session see an empty result? What did the model do next?

**4. Failures that were not about notes.** Wrong SQL dialect, invented numbers,
misread tables, charts that do not show what was asked, anything it claimed
without evidence. A short list is fine.

**5. What would have changed the outcome.** For each wrong answer, say which of
these would have prevented it, and why: (a) a host-computed profile of the
table (distinct values of text columns, date range, null rates) returned with
the dataset description; (b) the host appending a column's real values when a
filter returns zero rows; (c) a note the model could read and trust; (d)
nothing, the model just erred. Do not guess: base it on what the model did.

**6. What you could not test or are unsure of.** In particular: how much of the
variation you saw is noise, if you did only one repetition.

Keep the report factual and short. Quote the model where it matters. Put raw
evidence (full analyze output) in an appendix rather than in the body.
