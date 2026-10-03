// Copyright (c) 2026 Neomantra Corp

package session

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neomantra/CivicSodaQuack/chat/internal/agent"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

func readAll(t *testing.T, path string) []Record {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []Record
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		out = append(out, r)
	}
	return out
}

func TestRecorder_WritesOneLinePerEventWithoutRows(t *testing.T) {
	rec, err := Open(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	rec.Start("fake/scripted", []string{"test"})
	rec.BeginTurn()
	rec.Event(agent.Event{Kind: agent.EventProgress, Message: "not recorded"})
	rec.Event(agent.Event{Kind: agent.EventToolCall, Tool: "present_table", Input: `{"sql":"SELECT 1"}`, Rows: 2, Duration: 5 * time.Millisecond})
	p := present.Presentation{Kind: present.KindTable, Title: "T", Table: &present.Table{
		Columns: []string{"a"}, Rows: [][]string{{"secret"}, {"cells"}}, Total: 2}}
	rec.Event(agent.Event{Kind: agent.EventPresent, Presentation: &p})
	rec.Turn("hi", agent.Response{Text: "there", ToolCalls: 1, Presented: 1, Steps: 2, Duration: time.Second}, nil)
	rec.BeginTurn()
	rec.Turn("again", agent.Response{}, errors.New("boom"))
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}

	got := readAll(t, rec.Path())
	types := []string{}
	for _, r := range got {
		types = append(types, r.Type)
	}
	want := []string{"session_start", "tool_call", "present", "turn", "turn"}
	if len(got) != len(want) {
		t.Fatalf("types = %v, want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] || got[i].Seq != i+1 || got[i].Session == "" {
			t.Errorf("record %d = %+v", i, got[i])
		}
	}
	if got[1].Turn != 1 || got[1].Tool != "present_table" || got[1].DurationMS != 5 {
		t.Errorf("tool_call = %+v", got[1])
	}
	if got[2].Presentation == nil || got[2].Presentation.Rows != 2 || got[2].Presentation.Title != "T" {
		t.Errorf("present = %+v", got[2])
	}
	if got[3].Usage == nil || got[3].Response != "there" || got[3].PromptChars != 2 {
		t.Errorf("turn = %+v", got[3])
	}
	if got[4].Turn != 2 || got[4].Error != "boom" || got[4].Usage != nil {
		t.Errorf("failed turn = %+v", got[4])
	}

	raw, _ := os.ReadFile(rec.Path())
	if string(raw) == "" || containsAny(string(raw), "secret", "cells") {
		t.Errorf("file must not contain cell values:\n%s", raw)
	}
}

func TestRecorder_NilIsSafe(t *testing.T) {
	var rec *Recorder
	rec.Start("m", nil)
	rec.BeginTurn()
	rec.Event(agent.Event{Kind: agent.EventToolCall})
	rec.Turn("p", agent.Response{}, nil)
	if rec.Path() != "" || rec.Close() != nil {
		t.Error("nil recorder should be inert")
	}
	if r, err := Open("", ""); r != nil || err != nil {
		t.Errorf("Open(\"\", \"\") = %v, %v", r, err)
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func TestOpen_NamedSessionCanBeResumedIntoANewFile(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(dir, "chicago-study")
	if err != nil {
		t.Fatal(err)
	}
	a.Start("m", nil)
	a.Close()
	time.Sleep(1100 * time.Millisecond) // file names carry whole seconds
	b, err := Open(dir, "chicago-study")
	if err != nil {
		t.Fatalf("resuming a session name must not collide with its earlier file: %v", err)
	}
	b.Start("m", nil)
	b.Close()
	if a.Path() == b.Path() {
		t.Error("expected a new file per run")
	}
	if got := readAll(t, b.Path()); got[0].Session != "chicago-study" {
		t.Errorf("session id = %q", got[0].Session)
	}
}
