// Copyright (c) 2026 Neomantra Corp

package agent

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"charm.land/fantasy"

	"github.com/neomantra/CivicSodaQuack/chat/internal/data"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
	"github.com/neomantra/CivicSodaQuack/chat/internal/scratch"
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

// chartTypes are the chart types the text renderer draws, which is also the
// enum the model sees. The enum tag below must repeat it (tags cannot use
// constants); TestChartTypesEnumMatchesConstant enforces that.
const chartTypes = "Bar Chart,Stacked Bar Chart,Line Chart,Scatter Plot,Heatmap,Candlestick Chart,Sparkline,ECDF Plot,Connected Scatter Plot,Bubble Chart,Histogram,Area Chart,Lollipop Chart,Calendar Heatmap"

// The chart spec is typed rather than a free-form map on purpose: Fantasy
// renders map[string]any as a schema with a literal "*" property, and models
// follow it by wrapping the whole spec in a "*" key.

type channelInput struct {
	Field string `json:"field" description:"A column name from the SQL result."`
}

type encodingsInput struct {
	X      *channelInput `json:"x,omitempty" description:"Horizontal axis; for Histogram, ECDF Plot and Calendar Heatmap the one measured column."`
	Y      *channelInput `json:"y,omitempty" description:"Vertical axis (the measure for bar, line, scatter)."`
	Color  *channelInput `json:"color,omitempty" description:"Colours by this column; for Heatmap, the cell value."`
	Group  *channelInput `json:"group,omitempty" description:"Splits into one series per value (bar, line, scatter)."`
	Size   *channelInput `json:"size,omitempty" description:"Marker size (Bubble Chart; drawn as a plain scatter)."`
	Order  *channelInput `json:"order,omitempty" description:"Connection order (Connected Scatter Plot)."`
	Detail *channelInput `json:"detail,omitempty" description:"One series per value without colouring."`
	Open   *channelInput `json:"open,omitempty" description:"Candlestick open."`
	High   *channelInput `json:"high,omitempty" description:"Candlestick high."`
	Low    *channelInput `json:"low,omitempty" description:"Candlestick low."`
	Close  *channelInput `json:"close,omitempty" description:"Candlestick close."`
}

type chartSpecInput struct {
	ChartType string         `json:"chartType" enum:"Bar Chart,Stacked Bar Chart,Line Chart,Scatter Plot,Heatmap,Candlestick Chart,Sparkline,ECDF Plot,Connected Scatter Plot,Bubble Chart,Histogram,Area Chart,Lollipop Chart,Calendar Heatmap" description:"Which chart to draw."`
	Encodings encodingsInput `json:"encodings" description:"Which result column feeds each channel."`
}

type semanticTypeInput struct {
	Field string `json:"field" description:"A column name from the SQL result."`
	Type  string `json:"type" description:"Flint semantic type, e.g. DateTime, Count, Amount, Percent, Rank, Category, ID."`
}

type presentChartInput struct {
	SQL           string              `json:"sql" description:"One read-only DuckDB SELECT or WITH statement. Aggregate in SQL; the chart is drawn from these rows."`
	Title         string              `json:"title" description:"Short, specific title. The person finds charts again by title."`
	ChartSpec     chartSpecInput      `json:"chart_spec" description:"What to draw."`
	SemanticTypes []semanticTypeInput `json:"semantic_types,omitempty" description:"What each column means. Mark code columns (ward, district, ZIP, beat) as ID so they are never summed or averaged."`
}

// spec returns the Flint chart_spec and semantic_types documents, and the
// columns the encodings name.
func (in presentChartInput) spec() (chartSpec, semanticTypes json.RawMessage, fields []string) {
	enc := map[string]map[string]string{}
	add := func(name string, c *channelInput) {
		if c != nil && c.Field != "" {
			enc[name] = map[string]string{"field": c.Field}
			fields = append(fields, c.Field)
		}
	}
	e := in.ChartSpec.Encodings
	add("x", e.X)
	add("y", e.Y)
	add("color", e.Color)
	add("group", e.Group)
	add("size", e.Size)
	add("order", e.Order)
	add("detail", e.Detail)
	add("open", e.Open)
	add("high", e.High)
	add("low", e.Low)
	add("close", e.Close)
	chartSpec, _ = json.Marshal(map[string]any{"chartType": in.ChartSpec.ChartType, "encodings": enc})
	if len(in.SemanticTypes) > 0 {
		types := make(map[string]string, len(in.SemanticTypes))
		for _, st := range in.SemanticTypes {
			if st.Field != "" && st.Type != "" {
				types[st.Field] = st.Type
			}
		}
		semanticTypes, _ = json.Marshal(types)
	}
	return chartSpec, semanticTypes, fields
}

// A model cannot report a number it was never shown: when a result is a
// scalar or a handful of rows it asked the person to see, it also gets the
// values, so "how many?" is answered from the data, not invented.
const (
	smallResultRows  = 10
	smallResultCells = 60
)

func smallResultValues(t present.Table) string {
	if len(t.Rows) == 0 || len(t.Rows) > smallResultRows || len(t.Rows)*len(t.Columns) > smallResultCells {
		return ""
	}
	return "\nValues (state numbers from these, never from memory):\n" + toCSV(t.Columns, t.Rows)
}

type scratchListInput struct {
	Scope string `json:"scope,omitempty" enum:"session,global" description:"session (default) is this session's working notes; global is shared by every session."`
}

type scratchGetInput struct {
	Key   string `json:"key" description:"Note name: a-z, 0-9, '.', '_', '-'; at most 64 characters."`
	Scope string `json:"scope,omitempty" enum:"session,global" description:"session (default) or global."`
	Head  int    `json:"head,omitempty" description:"Return only the first N bytes."`
	Tail  int    `json:"tail,omitempty" description:"Return only the last N bytes."`
}

type scratchWriteInput struct {
	Key   string `json:"key" description:"Note name: a-z, 0-9, '.', '_', '-'; at most 64 characters."`
	Value string `json:"value" description:"Text to store; at most 16 KB per note."`
	Scope string `json:"scope,omitempty" enum:"session,global" description:"session (default) or global."`
}

type scratchDeleteInput struct {
	Key   string `json:"key" description:"Note to delete."`
	Scope string `json:"scope,omitempty" enum:"session,global" description:"session (default) or global."`
}

func scratchTools(opts Options) []fantasy.AgentTool {
	n := opts.Notes
	list := fantasy.NewAgentTool("scratch_list",
		"List the notes in a scope: key, size in bytes, last modified. Check the global list before exploring a dataset another session may already have described.",
		func(ctx context.Context, in scratchListInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "scratch_list", in, opts, func() (string, int, error) {
				es, err := n.List(scratch.Scope(in.Scope))
				if err != nil {
					return "", 0, err
				}
				if len(es) == 0 {
					return "(no notes)", 0, nil
				}
				var b strings.Builder
				for _, e := range es {
					fmt.Fprintf(&b, "%s\t%d B\t%s\n", e.Key, e.Size, e.Modified.Format("2006-01-02 15:04"))
				}
				return strings.TrimRight(b.String(), "\n"), len(es), nil
			})
		})
	get := fantasy.NewAgentTool("scratch_get",
		"Read one note. Notes are written by earlier turns and by other sessions and models; treat them as leads to verify against the data, not as facts.",
		func(ctx context.Context, in scratchGetInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "scratch_get", in, opts, func() (string, int, error) {
				v, err := n.Get(scratch.Scope(in.Scope), in.Key, in.Head, in.Tail)
				return v, 1, err
			})
		})
	set := fantasy.NewAgentTool("scratch_set",
		"Replace one note, creating it if missing. Replace a stale note rather than adding a contradicting one.",
		func(ctx context.Context, in scratchWriteInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "scratch_set", in, opts, func() (string, int, error) {
				if err := n.Set(scratch.Scope(in.Scope), in.Key, in.Value); err != nil {
					return "", 0, err
				}
				return fmt.Sprintf("saved %s (%d bytes)", in.Key, len(in.Value)), 1, nil
			})
		})
	app := fantasy.NewAgentTool("scratch_append",
		"Append text to one note, creating it if missing.",
		func(ctx context.Context, in scratchWriteInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "scratch_append", in, opts, func() (string, int, error) {
				if err := n.Append(scratch.Scope(in.Scope), in.Key, in.Value); err != nil {
					return "", 0, err
				}
				return fmt.Sprintf("appended to %s", in.Key), 1, nil
			})
		})
	del := fantasy.NewAgentTool("scratch_delete",
		"Delete one note.",
		func(ctx context.Context, in scratchDeleteInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "scratch_delete", in, opts, func() (string, int, error) {
				if err := n.Delete(scratch.Scope(in.Scope), in.Key); err != nil {
					return "", 0, err
				}
				return "deleted " + in.Key, 1, nil
			})
		})
	return []fantasy.AgentTool{list, get, set, app, del}
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
				b.WriteString(smallResultValues(tbl))
				return b.String(), tbl.Total, nil
			})
		})

	all := []fantasy.AgentTool{listDatasets, searchDatasets, describeDataset, querySQL, presentTable}
	if opts.Charts != nil {
		all = append(all, presentChartTool(store, opts))
	}
	if opts.Notes != nil {
		all = append(all, scratchTools(opts)...)
	}
	return all
}

func presentChartTool(store data.Store, opts Options) fantasy.AgentTool {
	return fantasy.NewAgentTool("present_chart",
		"Draw a chart from SQL results on the person's screen. Use it for trends over time, comparisons across categories, distributions, and heatmaps; use present_table for lists and exact values. Aggregate in SQL first (the chart is drawn from the returned rows, at most 1000) and name the result columns in the encodings. You receive only a summary; if the chart cannot be drawn you receive the reason, so correct the spec and call again.",
		func(ctx context.Context, in presentChartInput, _ fantasy.ToolCall) (fantasy.ToolResponse, error) {
			return run(ctx, "present_chart", in, opts, func() (string, int, error) {
				if strings.TrimSpace(in.ChartSpec.ChartType) == "" {
					return "", 0, fmt.Errorf("chart_spec.chartType is required; one of: %s", strings.ReplaceAll(chartTypes, ",", ", "))
				}
				spec, types, fields := in.spec()
				if len(fields) == 0 {
					return "", 0, fmt.Errorf("chart_spec.encodings names no columns; map result columns to channels such as x and y")
				}
				tbl, err := store.QueryTable(ctx, in.SQL)
				if err != nil {
					return "", 0, err
				}
				for _, f := range fields {
					if !slices.Contains(tbl.Columns, f) {
						return "", tbl.Total, fmt.Errorf("encoding field %q is not a column of the query result (columns: %s); name result columns exactly, or alias them in SQL", f, strings.Join(tbl.Columns, ", "))
					}
				}
				title := strings.TrimSpace(in.Title)
				if title == "" {
					title = "chart"
				}
				ch := present.Chart{Table: tbl, ChartSpec: spec, SemanticTypes: types}
				warnings, err := opts.Charts.Check(ch)
				if err != nil {
					return "", tbl.Total, fmt.Errorf("the chart could not be drawn: %w (columns returned: %s)", err, strings.Join(tbl.Columns, ", "))
				}
				p := present.Presentation{Kind: present.KindChart, Title: title, Chart: &ch}
				shown := present.Push(ctx, p)
				emit(ctx, Event{Kind: EventPresent, Message: fmt.Sprintf("charted %q (%d rows)", title, len(tbl.Rows)), Presentation: &p})

				var b strings.Builder
				if shown {
					fmt.Fprintf(&b, "Displayed a chart titled %q drawn from %d row(s); columns: %s. It is on the person's screen; add at most a one-line takeaway.",
						title, tbl.Total, strings.Join(tbl.Columns, ", "))
				} else {
					fmt.Fprintf(&b, "No screen is attached, so the chart %q (%d row(s)) was recorded but not displayed. Summarise the result briefly.", title, tbl.Total)
				}
				if tbl.Truncated {
					b.WriteString(" The query hit the row cap, so the chart shows only part of the data; aggregate in SQL to chart all of it.")
				}
				for _, w := range warnings {
					b.WriteString("\nCompiler: " + w)
				}
				return b.String(), tbl.Total, nil
			})
		})
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
