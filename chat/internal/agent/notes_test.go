// Copyright (c) 2026 Neomantra Corp

package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/neomantra/CivicSodaQuack/chat/internal/agent/agenttest"
	"github.com/neomantra/CivicSodaQuack/chat/internal/scratch"
)

func newNotes(t *testing.T) *scratch.Scratch {
	t.Helper()
	n, err := scratch.Open(t.TempDir(), "s1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

func toolNames(opts Options, t *testing.T) []string {
	var out []string
	for _, tool := range tools(seedStore(t), opts) {
		out = append(out, tool.Info().Name)
	}
	return out
}

func TestScratchTools_OnlyOfferedWithANotepad(t *testing.T) {
	for _, n := range toolNames(Options{}, t) {
		if strings.HasPrefix(n, "scratch_") {
			t.Errorf("%s offered without a notepad", n)
		}
	}
	got := strings.Join(toolNames(Options{Notes: newNotes(t)}, t), ",")
	for _, want := range []string{"scratch_list", "scratch_get", "scratch_set", "scratch_append", "scratch_delete"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in %s", want, got)
		}
	}
}

func TestScratch_WriteReadRoundTripAcrossScopes(t *testing.T) {
	notes := newNotes(t)
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "scratch_set", `{"key":"chicago.mft5-nfa8","value":"COPA cases; complaint_date is the date; 2026 is partial","scope":"global"}`),
		agenttest.ToolCall("c2", "scratch_get", `{"key":"chicago.mft5-nfa8","scope":"global"}`),
		agenttest.ToolCall("c3", "scratch_list", `{}`),
		agenttest.Text("noted"),
	)
	r := New(model, seedStore(t), Options{Notes: notes})
	if _, err := r.Ask(context.Background(), "remember this", nil, nil); err != nil {
		t.Fatal(err)
	}
	if v, _ := notes.Get(scratch.Global, "chicago.mft5-nfa8", 0, 0); !strings.Contains(v, "2026 is partial") {
		t.Errorf("global note = %q", v)
	}
	last := model.Calls[len(model.Calls)-1]
	if got := agenttest.ToolResultText(last, "c2"); !strings.Contains(got, "complaint_date") {
		t.Errorf("get result = %q", got)
	}
	// Session scope list shows the host key and nothing the model wrote globally.
	if got := agenttest.ToolResultText(last, "c3"); !strings.Contains(got, scratch.KeyLatestRequest) || strings.Contains(got, "chicago") {
		t.Errorf("session list = %q", got)
	}
}

func TestScratch_HostRecordsLatestRequestAndModelCannotChangeIt(t *testing.T) {
	notes := newNotes(t)
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "scratch_set", `{"key":"latest-request","value":"forged"}`),
		agenttest.Text("ok"),
	)
	r := New(model, seedStore(t), Options{Notes: notes})
	if _, err := r.Ask(context.Background(), "the real question", nil, nil); err != nil {
		t.Fatal(err)
	}
	if v, _ := notes.Get(scratch.Session, scratch.KeyLatestRequest, 0, 0); v != "the real question" {
		t.Errorf("latest-request = %q", v)
	}
	if told := agenttest.ToolResultText(model.Calls[1], "c1"); !strings.Contains(told, "written by the host") {
		t.Errorf("model was told %q", told)
	}
}

func TestScratch_LimitErrorsReachTheModelAsText(t *testing.T) {
	notes := newNotes(t)
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "scratch_set", `{"key":"Bad Key","value":"x"}`),
		agenttest.Text("ok"),
	)
	r := New(model, seedStore(t), Options{Notes: notes})
	if _, err := r.Ask(context.Background(), "x", nil, nil); err != nil {
		t.Fatalf("a bad key must not fail the turn: %v", err)
	}
	if told := agenttest.ToolResultText(model.Calls[1], "c1"); !strings.Contains(told, "keys use only") {
		t.Errorf("told %q", told)
	}
}

func TestSystemPrompt_NotesSection(t *testing.T) {
	off := systemPrompt([]string{"a"}, testDate(), false, "off")
	if strings.Contains(off, "## Notes") || strings.Contains(off, "scratch_") || strings.Contains(off, "{{") {
		t.Errorf("notes leaked into a prompt without a notepad:\n%s", off)
	}
	on := systemPrompt([]string{"a"}, testDate(), false, "")
	if !strings.Contains(on, "## Notes") || !strings.Contains(on, "scratch_list") || strings.Contains(on, "Notes present at the start") || strings.Contains(on, "{{") {
		t.Errorf("fresh notepad prompt:\n%s", on)
	}
	resumed := systemPrompt([]string{"a"}, testDate(), false, "global notes: chicago.crimes (120 B)")
	if !strings.Contains(resumed, "Notes present at the start of this session:\nglobal notes: chicago.crimes (120 B)") {
		t.Errorf("resumed prompt lacks the brief:\n%s", resumed)
	}
}
