// Copyright (c) 2026 Neomantra Corp

package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"testing"
	"time"

	"charm.land/fantasy"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/neomantra/CivicSodaQuack/chat/internal/data"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
)

// fakeModel plays a script: each Generate call returns the next response and
// records the prompt it was given, so a test can see what the model saw.
type fakeModel struct {
	script []fantasy.Response
	calls  []fantasy.Call
}

func (f *fakeModel) Generate(_ context.Context, call fantasy.Call) (*fantasy.Response, error) {
	f.calls = append(f.calls, call)
	if len(f.script) == 0 {
		return nil, errors.New("fake model: script exhausted")
	}
	r := f.script[0]
	f.script = f.script[1:]
	return &r, nil
}

func (f *fakeModel) Stream(ctx context.Context, call fantasy.Call) (fantasy.StreamResponse, error) {
	r, err := f.Generate(ctx, call)
	if err != nil {
		return nil, err
	}
	return func(yield func(fantasy.StreamPart) bool) {
		for _, c := range r.Content {
			switch c := c.(type) {
			case fantasy.TextContent:
				if !yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeTextDelta, Delta: c.Text}) {
					return
				}
			case fantasy.ToolCallContent:
				if !yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeToolCall, ID: c.ToolCallID, ToolCallName: c.ToolName, ToolCallInput: c.Input}) {
					return
				}
			}
		}
		yield(fantasy.StreamPart{Type: fantasy.StreamPartTypeFinish, FinishReason: r.FinishReason, Usage: r.Usage})
	}, nil
}

func (f *fakeModel) GenerateObject(context.Context, fantasy.ObjectCall) (*fantasy.ObjectResponse, error) {
	return nil, errors.New("not supported")
}

func (f *fakeModel) StreamObject(context.Context, fantasy.ObjectCall) (fantasy.ObjectStreamResponse, error) {
	return nil, errors.New("not supported")
}

func (f *fakeModel) Provider() string { return "fake" }
func (f *fakeModel) Model() string    { return "scripted" }

var _ fantasy.LanguageModel = (*fakeModel)(nil)
var _ iter.Seq[fantasy.StreamPart] = fantasy.StreamResponse(nil)

func textResponse(text string) fantasy.Response {
	return fantasy.Response{
		Content:      fantasy.ResponseContent{fantasy.TextContent{Text: text}},
		FinishReason: fantasy.FinishReasonStop,
		Usage:        fantasy.Usage{InputTokens: 10, OutputTokens: 5},
	}
}

func toolCallResponse(id, name, input string) fantasy.Response {
	return fantasy.Response{
		Content:      fantasy.ResponseContent{fantasy.ToolCallContent{ToolCallID: id, ToolName: name, Input: input}},
		FinishReason: fantasy.FinishReasonToolCalls,
	}
}

// toolResultText finds the tool result for id in the last prompt the model
// received, which is what the model was told about that call.
func toolResultText(t *testing.T, call fantasy.Call, id string) string {
	t.Helper()
	for _, m := range call.Prompt {
		for _, part := range m.Content {
			tr, ok := part.(fantasy.ToolResultPart)
			if !ok || tr.ToolCallID != id {
				continue
			}
			switch out := tr.Output.(type) {
			case fantasy.ToolResultOutputContentText:
				return out.Text
			case fantasy.ToolResultOutputContentError:
				return "ERROR: " + out.Error.Error()
			default:
				return fmt.Sprintf("%T", out)
			}
		}
	}
	t.Fatalf("no tool result for %s in prompt", id)
	return ""
}

// seedStore opens a data.Store over a csq-shaped database with one synced
// dataset: table test.main.crimes with two rows.
func seedStore(t *testing.T) data.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.duckdb")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := duckdb.Apply(db); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	for _, s := range []string{
		`INSERT INTO _csq.catalog (id, name, description, category, tags, fetched_at, raw)
		 VALUES ('aaaa-0001', 'Crimes', 'Reported crimes', 'Public Safety', '["crime"]', TIMESTAMP '2026-09-30 12:00:00', '{}')`,
		`CREATE TABLE main.crimes (socrata_id VARCHAR, ward BIGINT, amount DOUBLE)`,
		`INSERT INTO main.crimes VALUES ('a', 1, 1.5), ('b', 2, 2)`,
		fmt.Sprintf(`INSERT INTO _csq.sync_runs (run_id, dataset_id, table_name, started_at, finished_at, status, rows_written, duration_ms)
		 VALUES ('01RUN', 'aaaa-0001', 'crimes', '%[1]s', '%[1]s', 'ok', 2, 10)`, now.Format("2006-01-02 15:04:05")),
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	db.Close()
	st, err := data.Open([]string{"test=" + path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}
