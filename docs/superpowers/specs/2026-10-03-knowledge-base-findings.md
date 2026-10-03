# Knowledge base: what the exercise showed

Date: 2026-10-03. Follows the [csq-chat design](2026-09-30-csq-chat-design.md).

This records an exercise, not a design decision. It ran csq-chat against real
Chicago data and read what the model did with a scratchpad, to find out what a
knowledge base for datasets should be.

## Method

- **Data:** `.csq/chicago.duckdb`: four datasets (police stations, fire
  stations, 192,034 COPA complaint summaries, 64,499 homicide and non-fatal
  shooting victimisations).
- **Model:** `qwen3:14b` through a local Ollama server, the only model
  available (no hosted API keys were set). **Every conclusion below is about
  this model.** A stronger model will likely use notes more readily and err
  less; the numbers are not a measurement of models in general.
- **Notes:** an isolated directory, with the scratch tools, a prompt section
  describing them, and the keys of any existing notes listed in the system
  prompt. Nine headless sessions in three batches.
- **Ground truth** was checked directly in DuckDB where a claim mattered.

## What happened

| Session | Prompt | Notes written | Notes read | Outcome |
|---|---|---|---|---|
| copa-1 | "I'm new to this data… what should I watch out for?" | 0 | 0 | Stated "~12k complaints a year"; its own chart showed about 6k. |
| copa-2 | Month of the year with most complaints | 0 | 0 | Repeated the same search and describe; used Postgres `TO_CHAR` (not in DuckDB); charted 320 year-month buckets and never named a month. |
| victims-1 | Age/sex and wards for shooting victims, 2024 | 0 | 0 | Filtered `gunshot_injury_i = 'Y'`; real values are `YES`/`NO`; got zero rows and gave up. |
| victims-2 | "Explore, look at actual values, record notes, then count 2024" | 1 (global) | 0 | Never queried the data. The note, copied from the dataset's description, says the flag is `'Y'/'N'/'UNKNOWN'`, which is wrong. Reported "1,234 victims"; it was never shown a number. |
| victims-3 | Same as victims-1, with that note present | 0 | 0 | Same `'Y'` mistake, same give-up. Never listed or read notes. |
| copa-3 | 2026 vs 2025, with a planted false note | 0 | 0 | Never read the note. |
| copa-4 | Same, told "first read the global notes" | 0 | 2 | Read the planted note and repeated its false claim (that `complaint_date` is a closure date). It did not repeat the claim that data is complete through 2026. |
| victims-4 | 2024 gunshot victims, told to read notes first | 0 | 1 | Guessed a key without listing, read the COPA note, never read the victims note, used `'Y'`, reported 0. |

Ground truth, for reference: the flag is `YES` (60,125) / `NO` (4,374); 2024
has 2,922 rows; `age` is a text bucket (`20-29`, `UNKNOWN`); `day_of_week` is
numeric with peaks at 1 and 7; the data runs to 2026-10-01; 2026 COPA has
4,225 complaints against 6,280 in 2025.

## Findings

1. **Models do not use memory on their own.** Unprompted, 0 of 6 sessions
   wrote a note and 0 of 6 read one, including a session whose prompt listed a
   note's key. Even told to read the notes, one session guessed a key instead of
   listing.
2. **A model-written note inherits the model's errors.** victims-2 was asked
   to record what it saw and recorded what it had read in the description; the
   one value that mattered was wrong. In a shared pad that error would mislead
   every later session.
3. **A wrong note is trusted when it is read.** copa-4 adopted a fabricated
   semantic claim. Models verify numbers they can query more readily than
   meanings they cannot.
4. **Most of what would have helped is computable.** The failures that cost
   answers were: an unknown categorical value (`'Y'` for `'YES'`), an unknown
   date range, an unknown number convention, a wrong SQL dialect, and giving up
   on zero rows. The first three are facts a host can derive from the table with
   no model involved.
5. **Zero rows is a dead end for this model.** Both victims sessions saw 0 and
   reported it with hedges rather than checking the filtered column's values.
6. **A model cannot report a value it was not shown.** `present_table` returned
   only counts, so a scalar was invented. Fixed in this change: results of up to
   10 rows and 60 cells now also return their values to the model.

## What the knowledge base should be

### Two layers, kept apart

Matching the rule that csq's discipline applies to csq and not to the model:

- **A host-computed profile** (deterministic, no model): for each synced
  dataset, the distinct values of low-cardinality text columns, min and max of
  dates and numbers, null rates, row count, and the sync it was computed
  against. It is returned by `describe_dataset` automatically. This would have
  prevented the `'Y'` error outright. It belongs in csq, because it is a
  property of the data and nothing but SQL produces it.
- **Model-written entries** for what the data cannot say about itself: what a
  column means, which code is which, known quirks, queries that worked, datasets
  that are abandoned.

### Entries carry their evidence

An entry is a claim about a dataset or one of its columns, plus how it is known:

| Field | Purpose |
|---|---|
| portal, dataset id, optional column | what the claim is about; the lookup key |
| claim | the text |
| evidence query and result excerpt | **run and recorded by the host**, not typed by the model |
| sync marker | the dataset's high-water mark when it was checked |
| author | model and session |
| status | `observed` (host ran the evidence), `unverified` (no evidence), or `metadata` (taken from the portal's description) |

The host runs the evidence query, so `observed` cannot be asserted. victims-2
could not have written `observed` for a claim about `'Y'`, because the
evidence query would have returned `YES`.

### Reading is automatic

Since the model does not read memory by choice, the host puts entries where it
cannot miss them: `describe_dataset` returns the profile and the entries for
that dataset, with status and age beside each. Entries older than the dataset's
current sync are flagged stale, not dropped. The free-form global scratchpad
stays as it is, for working notes.

### Writing is direct, and conflicts are visible

Models write entries directly, as decided. A new entry that disagrees with an
existing one does not overwrite it; both are shown until a later `observed`
entry settles it, so a wrong claim cannot silently replace a right one.
Entries are never fed to csq's confidence arithmetic.

### Storage

One file per dataset, `<portal>.<dataset-id>.json`, kept outside the portal
database so that fetching a snapshot does not replace it, and plain enough to
diff and merge through git. This is the owner's call; the alternative is
DuckDB tables, queryable with SQL but replaced along with the file.

## Cheaper fixes the exercise points at

These do not need the knowledge base and should probably come first:

1. **Zero-row diagnostics.** When a query returns no rows and filters a text
   column by equality, the host appends that column's distinct values to the
   tool result. This targets the failure in both victims sessions directly.
2. **A DuckDB dialect note in the system prompt:** `strftime`/`monthname`
   instead of `TO_CHAR`, `date_trunc`, `::DATE`.
3. **Reject multi-statement SQL** in `query_sql`. copa-3 sent two statements
   and misreported the result.

## Not tested

- Any model other than `qwen3:14b`.
- Whether notes help when the model is made to read them and they are right.
  Both read sessions had a wrong or irrelevant note.
- Two models sharing notes, or notes older than a sync.
- Scale: more than a handful of datasets, or notes that contradict each other.

## Open decisions for the owner

1. Build the host profile in csq (usable by `csq mcp` too) or in the chat only.
2. File-per-dataset JSON or DuckDB tables for entries.
3. Whether `observed` should require the host to re-run the evidence on every
   read, or only at write time.
4. Whether to try a hosted model before settling any of this.
