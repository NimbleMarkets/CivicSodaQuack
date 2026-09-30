// Copyright (c) 2026 Neomantra Corp

package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/neomantra/CivicSodaQuack/chat/internal/agent"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

// fakeChat shows one table and answers with a fixed line.
type fakeChat struct {
	cleared int
	prompts []string
}

func (f *fakeChat) Ask(_ context.Context, prompt string, progress agent.ProgressFunc, show present.Func) (agent.Response, error) {
	f.prompts = append(f.prompts, prompt)
	progress(agent.Event{Kind: agent.EventProgress, Message: "querying"})
	show(present.Presentation{Kind: present.KindTable, Title: "Crimes by ward", Table: &present.Table{
		Columns: []string{"ward", "n"}, Rows: [][]string{{"1", "40"}, {"2", "35"}}, Total: 2,
	}})
	return agent.Response{Text: "Ward 1 leads.", ToolCalls: 1, Presented: 1, Steps: 2}, nil
}
func (f *fakeChat) ClearHistory() { f.cleared++ }
func (f *fakeChat) Model() string { return "fake/scripted" }

func typeText(m Model, s string) Model {
	for _, r := range s {
		next, _ := m.Update(tea.KeyPressMsg{Code: r, Text: string(r)})
		m = next.(Model)
	}
	return m
}

func press(m Model, key rune) (Model, tea.Cmd) {
	next, cmd := m.Update(tea.KeyPressMsg{Code: key})
	return next.(Model), cmd
}

// runCmds executes a command tree synchronously and feeds every message back
// into the model until nothing is left, which is what the program loop does.
func runCmds(m Model, cmd tea.Cmd) Model {
	queue := []tea.Cmd{cmd}
	for len(queue) > 0 {
		c := queue[0]
		queue = queue[1:]
		if c == nil {
			continue
		}
		msg := c()
		switch v := msg.(type) {
		case tea.BatchMsg:
			queue = append(queue, v...)
			continue
		case tickMsg:
			continue // the ticker would spin forever while busy
		case nil:
			continue
		}
		next, more := m.Update(msg)
		m = next.(Model)
		queue = append(queue, more)
	}
	return m
}

// plain is the screen without styling, for substring checks.
func plain(m Model) string { return ansi.Strip(m.render()) }

func newTestModel(chat Chat) Model {
	m := New(context.Background(), chat, Info{Portals: []string{"test"}})
	next, _ := m.Update(tea.WindowSizeMsg{Width: 80, Height: 24})
	return next.(Model)
}

func TestSubmit_ShowsTableCellAndReply(t *testing.T) {
	chat := &fakeChat{}
	m := newTestModel(chat)
	m = typeText(m, "crimes by ward")
	m, cmd := press(m, tea.KeyEnter)
	if !m.busy {
		t.Fatal("submit should mark the model busy")
	}
	m = runCmds(m, cmd)

	if m.busy {
		t.Error("turn should be finished")
	}
	if len(chat.prompts) != 1 || chat.prompts[0] != "crimes by ward" {
		t.Errorf("prompts = %v", chat.prompts)
	}
	out := plain(m)
	for _, want := range []string{"you: crimes by ward", "[1] Crimes by ward", "ward", "40", "2 row(s)", "csq: Ward 1 leads."} {
		if !strings.Contains(out, want) {
			t.Errorf("screen lacks %q:\n%s", want, out)
		}
	}
	if strings.Index(out, "[1] Crimes by ward") > strings.Index(out, "csq: Ward 1 leads.") {
		t.Errorf("table should appear above the reply that comments on it:\n%s", out)
	}
	if len(m.cells) != 1 {
		t.Errorf("cells = %d", len(m.cells))
	}
}

func TestCommands(t *testing.T) {
	chat := &fakeChat{}
	m := newTestModel(chat)

	m = typeText(m, "/help")
	m, _ = press(m, tea.KeyEnter)
	if out := plain(m); !strings.Contains(out, "/status") {
		t.Errorf("help missing:\n%s", out)
	}

	m = typeText(m, "/tables")
	m, _ = press(m, tea.KeyEnter)
	if out := plain(m); !strings.Contains(out, "no tables shown yet") {
		t.Errorf("tables:\n%s", out)
	}

	m = typeText(m, "/status")
	m, _ = press(m, tea.KeyEnter)
	if out := plain(m); !strings.Contains(out, "model: fake/scripted") || !strings.Contains(out, "portals: test") {
		t.Errorf("status:\n%s", out)
	}

	m = typeText(m, "/clear")
	m, _ = press(m, tea.KeyEnter)
	if chat.cleared != 1 || len(m.entries) != 1 {
		t.Errorf("clear: cleared=%d entries=%d", chat.cleared, len(m.entries))
	}

	m = typeText(m, "/bogus")
	m, _ = press(m, tea.KeyEnter)
	if out := plain(m); !strings.Contains(out, "unknown command /bogus") {
		t.Errorf("bogus:\n%s", out)
	}

	m = typeText(m, "/quit")
	_, cmd := press(m, tea.KeyEnter)
	if cmd == nil {
		t.Fatal("quit should return a command")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Error("quit should produce tea.QuitMsg")
	}
}

func TestInputEditing(t *testing.T) {
	m := newTestModel(&fakeChat{})
	m = typeText(m, "helo")
	m, _ = press(m, tea.KeyLeft)
	m = typeText(m, "l")
	if got := string(m.input); got != "hello" {
		t.Errorf("input = %q", got)
	}
	m, _ = press(m, tea.KeyBackspace)
	m, _ = press(m, tea.KeyBackspace)
	if got := string(m.input); got != "heo" {
		t.Errorf("after backspace = %q", got)
	}
	next, _ := m.Update(tea.KeyPressMsg{Code: 'u', Mod: tea.ModCtrl})
	m = next.(Model)
	if got := string(m.input); got != "o" {
		t.Errorf("after ctrl+u = %q", got)
	}
	if out := plain(m); !strings.Contains(out, "> o") {
		t.Errorf("cursor should sit at the start:\n%s", out)
	}
}

func TestScrollKeepsBottomByDefault(t *testing.T) {
	m := newTestModel(&fakeChat{})
	for i := 0; i < 40; i++ {
		m.entries = append(m.entries, entry{role: roleSystem, text: "line " + string(rune('a'+i%26))})
	}
	bottom := plain(m)
	if !strings.Contains(bottom, "line n") {
		t.Errorf("bottom of transcript should be visible:\n%s", bottom)
	}
	m, _ = press(m, tea.KeyPgUp)
	if m.scroll == 0 {
		t.Error("pgup should scroll")
	}
	m, _ = press(m, tea.KeyEnd)
	if m.scroll != 0 {
		t.Error("end should return to the bottom")
	}
}

func TestFitColumns_ShrinksWidestFirst(t *testing.T) {
	cols := fitColumns([]string{"name", "n"}, [][]string{{strings.Repeat("x", 60), "12"}}, 30)
	if cols[1].Width != 3 {
		t.Errorf("narrow column should keep its width, got %d", cols[1].Width)
	}
	if cols[0].Width+cols[1].Width+4 > 30 {
		t.Errorf("columns overflow: %+v", cols)
	}
}

func TestRenderCell_ChartSaysNotWired(t *testing.T) {
	m := newTestModel(&fakeChat{})
	m.addCell(present.Presentation{Kind: present.KindChart, Title: "trend", Chart: &present.Chart{
		Table: present.Table{Columns: []string{"m", "n"}, Rows: [][]string{{"1", "2"}}, Total: 1},
	}})
	if out := plain(m); !strings.Contains(out, "[1] trend") || !strings.Contains(out, "not wired") {
		t.Errorf("chart cell:\n%s", out)
	}
	_ = time.Now
}
