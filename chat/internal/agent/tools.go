// Copyright (c) 2026 Neomantra Corp

package agent

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"charm.land/fantasy"

	"github.com/neomantra/CivicSodaQuack/chat/internal/data"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

type listDatasetsInput struct {
	Portal   string `json:"portal,omitempty" description:"Attached portal alias. Omit to list every portal."`
	Category string `json:"category,omitempty" description:"Exact category name to filter on."`
}

type searchDatasetsInput struct {
	Query  string `json:"query" description:"Case-insensitive substring matched against dataset name, description, and tags."`
	Portal string `json:"portal,omitempty" description:"Attached portal alias. Omit to search every portal."`
}

type describeDatasetInput struct {
	DatasetID string `json:"dataset_id" description:"Socrata 4x4 dataset id, e.g. 6zsd-86xi."`
	Portal    string `json:"portal,omitempty" description:"Required only when the id exists in more than one attached portal."`
}

type querySQLInput struct {
	SQL string `json:"sql" description:"One read-only DuckDB SELECT or WITH statement. Qualify tables as <alias>.main.<table>."`
}

type presentTableInput struct {
	SQL   string `json:"sql" description:"One read-only DuckDB SELECT or WITH statement whose rows are shown to the person."`
	Title string `json:"title" description:"Short, specific title. The person finds tables again by title."`
}

// tools builds the tool set over store. Every tool reports what it did
// through the progress func in ctx and answers the model with text.
func tools(store data.Store, opts Options) []fantasy.AgentTool {
	listDatasets := fantasy.NewAgentTool("list_datasets",
		"List datasets known to the attached portals: id, portal, name, category, table name, and row count for synced ones. Filter by portal alias or exact category.",
		func(ctx context.Context, in listDatasetsInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "list_datasets", in, opts, func() (string, int, error) {
				out, err := store.ListDatasets(ctx, in.Portal, in.Category)
				if err != nil {
					return "", 0, err
				}
				return asJSON(out), len(out), nil
			})
		})

	searchDatasets := fantasy.NewAgentTool("search_datasets",
		"Find datasets by a case-insensitive substring of their name, description, or tags.",
		func(ctx context.Context, in searchDatasetsInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "search_datasets", in, opts, func() (string, int, error) {
				out, err := store.SearchDatasets(ctx, in.Portal, in.Query)
				if err != nil {
					return "", 0, err
				}
				return asJSON(out), len(out), nil
			})
		})

	describeDataset := fantasy.NewAgentTool("describe_dataset",
		"Describe one dataset: table name, columns with DuckDB types, description, tags, and last sync. Call this before querying a table you have not seen; a dataset with no last_sync has no table.",
		func(ctx context.Context, in describeDatasetInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "describe_dataset", in, opts, func() (string, int, error) {
				out, err := store.DescribeDataset(ctx, in.DatasetID, in.Portal)
				if err != nil {
					return "", 0, err
				}
				return asJSON(out), len(out.Columns), nil
			})
		})

	querySQL := fantasy.NewAgentTool("query_sql",
		"Run read-only DuckDB SQL across the attached portals and get CSV back. Use it when you need the values yourself. Qualify every table as <alias>.main.<table>; capped at 1000 rows / 1 MB / 30 s, so use LIMIT and aggregates.",
		func(ctx context.Context, in querySQLInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "query_sql", in, opts, func() (string, int, error) {
				res, err := store.Query(ctx, in.SQL)
				if err != nil {
					return "", 0, err
				}
				text := toCSV(res.Columns, data.TableFromResult(res).Rows)
				if res.RowCount == 0 {
					text = "(no rows)"
				}
				if res.Truncated {
					text += "\n# " + res.Note
				}
				return text, res.RowCount, nil
			})
		})

	presentTable := fantasy.NewAgentTool("present_table",
		"Show the rows of a read-only SQL query to the person as a titled table on their screen. This is the default way to answer: lists, rankings, top-N, counts by category, totals by period. You receive only the row count and columns, so do not repeat the rows in your reply.",
		func(ctx context.Context, in presentTableInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "present_table", in, opts, func() (string, int, error) {
				tbl, err := store.QueryTable(ctx, in.SQL)
				if err != nil {
					return "", 0, err
				}
				title := strings.TrimSpace(in.Title)
				if title == "" {
					title = "results"
				}
				if opts.PresentRowCap > 0 && len(tbl.Rows) > opts.PresentRowCap {
					tbl.Rows = tbl.Rows[:opts.PresentRowCap]
					tbl.Truncated = true
				}
				p := present.Presentation{Kind: present.KindTable, Title: title, Table: &tbl}
				shown := present.Push(ctx, p)
				emit(ctx, Event{Kind: EventPresent, Message: fmt.Sprintf("presented %q (%d rows)", title, len(tbl.Rows)), Presentation: &p})

				var b strings.Builder
				if shown {
					fmt.Fprintf(&b, "Displayed a table titled %q with %d row(s); columns: %s. The rows are on the person's screen; add at most a one-line takeaway and do not list them.",
						title, tbl.Total, strings.Join(tbl.Columns, ", "))
				} else {
					fmt.Fprintf(&b, "No screen is attached, so the table %q (%d row(s); columns: %s) was recorded but not displayed. Summarise the result briefly.",
						title, tbl.Total, strings.Join(tbl.Columns, ", "))
				}
				if tbl.Truncated {
					fmt.Fprintf(&b, " Only the first %d rows are shown; suggest narrowing the query if the person needs more.", len(tbl.Rows))
				}
				return b.String(), tbl.Total, nil
			})
		})

	return []fantasy.AgentTool{listDatasets, searchDatasets, describeDataset, querySQL, presentTable}
}

// run wraps one tool call: it announces the call, times it, records the
// outcome, bounds the text sent back to the model, and turns a failure into
// an error response the model can read rather than an aborted turn.
func run(ctx context.Context, name string, input any, opts Options, fn func() (text string, rows int, err error)) (fantasy.ToolResponse, error) {
	inputJSON := asJSON(input)
	emit(ctx, Event{Kind: EventProgress, Message: name + " · " + oneLine(inputJSON, 120)})
	start := time.Now()
	text, rows, err := fn()
	ev := Event{Kind: EventToolCall, Tool: name, Input: inputJSON, Rows: rows, Duration: time.Since(start)}
	if err != nil {
		ev.Err = err.Error()
		emit(ctx, ev)
		emit(ctx, Event{Kind: EventProgress, Message: name + " failed: " + err.Error()})
		return fantasy.NewTextErrorResponse(err.Error()), nil
	}
	text = truncateToolOutput(text, opts.MaxToolBytes)
	ev.OutputBytes = len(text)
	emit(ctx, ev)
	return fantasy.NewTextResponse(text), nil
}

func asJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

func toCSV(columns []string, rows [][]string) string {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(columns)
	for _, r := range rows {
		_ = w.Write(r)
	}
	w.Flush()
	return strings.TrimRight(buf.String(), "\n")
}

// truncateToolOutput keeps a tool result inside the model's budget and says
// so, rather than handing the model a silently clipped answer.
func truncateToolOutput(text string, maxBytes int) string {
	if maxBytes <= 0 || len(text) <= maxBytes {
		return text
	}
	cut := text[:maxBytes]
	if i := strings.LastIndexByte(cut, '\n'); i > maxBytes/2 {
		cut = cut[:i]
	}
	return cut + fmt.Sprintf("\n# output truncated at %d bytes; narrow the query", maxBytes)
}

func oneLine(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > max {
		return s[:max-1] + "…"
	}
	return s
}
