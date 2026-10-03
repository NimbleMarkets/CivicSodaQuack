// Copyright (c) 2026 Neomantra Corp

package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"charm.land/fantasy"

	"github.com/neomantra/CivicSodaQuack/chat/internal/agent/agenttest"
	"github.com/neomantra/CivicSodaQuack/chat/internal/data"
	"github.com/neomantra/CivicSodaQuack/chat/internal/data/datatest"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

func seedStore(t *testing.T) data.Store {
	t.Helper()
	st, err := data.Open([]string{"test=" + datatest.SeedDB(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func toolResultText(t *testing.T, call fantasy.Call, id string) string {
	t.Helper()
	s := agenttest.ToolResultText(call, id)
	if s == "" {
		t.Fatalf("no tool result for %s in prompt", id)
	}
	return s
}

func TestAsk_PresentTablePushesRowsAndTellsModelOnlyTheShape(t *testing.T) {
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "present_table", `{"sql":"SELECT socrata_id, ward FROM test.main.crimes ORDER BY socrata_id","title":"Crimes by ward"}`),
		agenttest.Text("Two records, one per ward."),
	)
	r := New(model, seedStore(t), Options{})

	var shown []present.Presentation
	var events []Event
	resp, err := r.Ask(context.Background(), "show crimes by ward",
		func(e Event) { events = append(events, e) },
		func(p present.Presentation) { shown = append(shown, p) })
	if err != nil {
		t.Fatal(err)
	}

	if resp.Text != "Two records, one per ward." || resp.ToolCalls != 1 || resp.Presented != 1 || resp.Steps != 2 {
		t.Errorf("response = %+v", resp)
	}
	if len(shown) != 1 || shown[0].Kind != present.KindTable || shown[0].Title != "Crimes by ward" {
		t.Fatalf("shown = %+v", shown)
	}
	if got := shown[0].Table.Rows; len(got) != 2 || got[0][0] != "a" || got[1][1] != "2" {
		t.Errorf("rows = %v", got)
	}

	// The model's second call saw a summary, not the rows.
	told := toolResultText(t, model.Calls[1], "c1")
	if !strings.Contains(told, `"Crimes by ward"`) || !strings.Contains(told, "2 row(s)") {
		t.Errorf("model was told: %q", told)
	}
	if strings.Contains(told, `"a"`) || strings.Contains(told, "1.5") {
		t.Errorf("model was told cell values: %q", told)
	}

	var toolEvents, presentEvents int
	for _, e := range events {
		switch e.Kind {
		case EventToolCall:
			toolEvents++
			if e.Tool != "present_table" || e.Rows != 2 || e.Err != "" {
				t.Errorf("tool event = %+v", e)
			}
		case EventPresent:
			presentEvents++
		}
	}
	if toolEvents != 1 || presentEvents != 1 {
		t.Errorf("events: %d tool, %d present", toolEvents, presentEvents)
	}
	if r.HistoryLen() != 2 {
		t.Errorf("history = %d messages, want 2", r.HistoryLen())
	}
}

func TestAsk_ToolErrorReachesModelNotCaller(t *testing.T) {
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "query_sql", `{"sql":"SELECT nope FROM test.main.crimes"}`),
		agenttest.Text("That column does not exist."),
	)
	r := New(model, seedStore(t), Options{})
	resp, err := r.Ask(context.Background(), "count nope", nil, nil)
	if err != nil {
		t.Fatalf("a bad query must not fail the turn: %v", err)
	}
	if resp.Text != "That column does not exist." {
		t.Errorf("text = %q", resp.Text)
	}
	told := toolResultText(t, model.Calls[1], "c1")
	if !strings.Contains(strings.ToLower(told), "nope") {
		t.Errorf("model should see the SQL error, got %q", told)
	}
}

func TestAsk_QuerySQLReturnsCSV(t *testing.T) {
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "query_sql", `{"sql":"SELECT count(*) AS n FROM test.main.crimes"}`),
		agenttest.Text("There are 2."),
	)
	r := New(model, seedStore(t), Options{})
	if _, err := r.Ask(context.Background(), "how many", nil, nil); err != nil {
		t.Fatal(err)
	}
	if told := toolResultText(t, model.Calls[1], "c1"); told != "n\n2" {
		t.Errorf("csv = %q", told)
	}
}

func TestAsk_HeadlessPresentTellsModelNothingWasShown(t *testing.T) {
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "present_table", `{"sql":"SELECT ward FROM test.main.crimes","title":"Wards"}`),
		agenttest.Text(""),
	)
	r := New(model, seedStore(t), Options{})
	resp, err := r.Ask(context.Background(), "wards", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Nothing was shown, so nothing counts as presented and the model is told.
	if resp.Presented != 0 {
		t.Errorf("presented = %d", resp.Presented)
	}
	if told := toolResultText(t, model.Calls[1], "c1"); !strings.Contains(told, "No screen") {
		t.Errorf("model was told: %q", told)
	}
	if resp.Text != "(the model returned no text)" {
		t.Errorf("text = %q", resp.Text)
	}
}

func TestAsk_DescribeThenHistoryIsBounded(t *testing.T) {
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "describe_dataset", `{"dataset_id":"aaaa-0001"}`),
		agenttest.Text("It has three columns."),
		agenttest.Text("Second turn."),
	)
	r := New(model, seedStore(t), Options{MaxHistory: 2})
	if _, err := r.Ask(context.Background(), "describe it", nil, nil); err != nil {
		t.Fatal(err)
	}
	told := toolResultText(t, model.Calls[1], "c1")
	if !strings.Contains(told, `"table_name":"crimes"`) || !strings.Contains(told, `"ward"`) {
		t.Errorf("describe result = %q", told)
	}
	if _, err := r.Ask(context.Background(), "again", nil, nil); err != nil {
		t.Fatal(err)
	}
	if r.HistoryLen() != 2 {
		t.Errorf("history should be capped at 2, got %d", r.HistoryLen())
	}
}

func TestParseModel(t *testing.T) {
	cases := map[string][2]string{
		"anthropic/claude-x":       {"anthropic", "claude-x"},
		"compat/qwen":              {"compat", "qwen"},
		"Qwen3-0.6B":               {"", "Qwen3-0.6B"},
		"moonshotai/kimi":          {"", "moonshotai/kimi"},
		"openrouter/moonshot/kimi": {"openrouter", "moonshot/kimi"},
	}
	for in, want := range cases {
		v, id := ParseModel(in)
		if v != want[0] || id != want[1] {
			t.Errorf("ParseModel(%q) = %q, %q; want %q, %q", in, v, id, want[0], want[1])
		}
	}
}

func TestSystemPrompt_NamesPortals(t *testing.T) {
	r := New(agenttest.NewFakeModel(), seedStore(t), Options{})
	_ = r
	got := systemPrompt([]string{"chicago", "nyc"}, testDate(), false, "off")
	if !strings.Contains(got, "Attached portals: chicago, nyc") || !strings.Contains(got, "Today is 2026-09-30") {
		t.Errorf("prompt: %q", got[:200])
	}
}

func TestTruncateToolOutput(t *testing.T) {
	long := strings.Repeat("row\n", 100)
	got := truncateToolOutput(long, 50)
	if len(got) > 50+len("\n# output truncated at 50 bytes; narrow the query") || !strings.Contains(got, "truncated") {
		t.Errorf("got %q", got)
	}
	if truncateToolOutput("short", 50) != "short" {
		t.Error("short output changed")
	}
}

func testDate() time.Time { return time.Date(2026, 9, 30, 15, 0, 0, 0, time.UTC) }

func TestAsk_EmptyReplyAfterTablePointsAtIt(t *testing.T) {
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "present_table", `{"sql":"SELECT ward FROM test.main.crimes","title":"Wards"}`),
		agenttest.Text(""),
	)
	r := New(model, seedStore(t), Options{})
	resp, err := r.Ask(context.Background(), "wards", nil, func(present.Presentation) {})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Presented != 1 || resp.Text != "See the table above." {
		t.Errorf("response = %+v", resp)
	}
}

func TestSystemPrompt_ChartsOnlyWhenARendererExists(t *testing.T) {
	without := systemPrompt([]string{"a"}, testDate(), false, "off")
	with := systemPrompt([]string{"a"}, testDate(), true, "off")
	if strings.Contains(without, "present_chart") || strings.Contains(without, "## Charts") || strings.Contains(without, "{{") {
		t.Errorf("prompt without charts mentions them or has unfilled placeholders:\n%s", without)
	}
	if !strings.Contains(with, "present_chart") || !strings.Contains(with, "## Charts") || strings.Contains(with, "{{") {
		t.Errorf("prompt with charts lacks the tool or guidance, or has placeholders:\n%s", with)
	}
}
