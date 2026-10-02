// Copyright (c) 2026 Neomantra Corp

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"charm.land/fantasy"

	"github.com/neomantra/CivicSodaQuack/chat/internal/agent"
	"github.com/neomantra/CivicSodaQuack/chat/internal/agent/agenttest"
	"github.com/neomantra/CivicSodaQuack/chat/internal/data/datatest"
)

func TestHeadless_PrintsReplyAndPresentationsAndRecordsSession(t *testing.T) {
	newModel = func(context.Context, agent.Options) (fantasy.LanguageModel, error) {
		return agenttest.NewFakeModel(
			agenttest.ToolCall("c1", "present_table", `{"sql":"SELECT ward, amount FROM test.main.crimes ORDER BY ward","title":"Amount by ward"}`),
			agenttest.Text("Ward 2 has more."),
		), nil
	}
	t.Cleanup(func() { newModel = agent.NewLanguageModel })

	sessions := t.TempDir()
	var stdout, stderr bytes.Buffer
	err := run([]string{
		"--db", "test=" + datatest.SeedDB(t),
		"-m", "fake/scripted",
		"--session-dir", sessions,
		"--prompt", "amount by ward",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v\nstderr: %s", err, stderr.String())
	}

	var out headlessOutput
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if out.Response != "Ward 2 has more." || out.ToolCalls != 1 || len(out.Presentations) != 1 {
		t.Errorf("output = %+v", out)
	}
	p := out.Presentations[0]
	if p.Title != "Amount by ward" || p.Table == nil || len(p.Table.Rows) != 2 || p.Table.Rows[1][1] != "2" {
		t.Errorf("presentation = %+v", p)
	}

	files, _ := filepath.Glob(filepath.Join(sessions, "*.jsonl"))
	if len(files) != 1 {
		t.Fatalf("session files = %v", files)
	}
	f, err := os.Open(files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var types []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		types = append(types, rec.Type)
	}
	// The presentation lands before the tool call that produced it finishes.
	if got := strings.Join(types, ","); got != "session_start,present,tool_call,turn" {
		t.Errorf("session record types = %s", got)
	}
}

func TestFlags_RequireDBAndModel(t *testing.T) {
	var stderr bytes.Buffer
	if _, err := parseFlags([]string{"-m", "x/y"}, &stderr); err == nil || !strings.Contains(err.Error(), "--db") {
		t.Errorf("missing --db: %v", err)
	}
	t.Setenv("CSQ_CHAT_MODEL", "")
	if _, err := parseFlags([]string{"--db", "a.duckdb"}, &stderr); err == nil || !strings.Contains(err.Error(), "--model") {
		t.Errorf("missing --model: %v", err)
	}
}

func TestBareModelNameIsRefused(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"--db", datatest.SeedDB(t), "-m", "Qwen3-0.6B", "--session-dir", "", "--prompt", "hi"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "vendor prefix") {
		t.Errorf("bare model should be refused with guidance, got %v", err)
	}
}

func TestHeadless_ChartIsRenderedAsPlainText(t *testing.T) {
	newModel = func(context.Context, agent.Options) (fantasy.LanguageModel, error) {
		return agenttest.NewFakeModel(
			agenttest.ToolCall("c1", "present_chart", `{"sql":"SELECT socrata_id, ward FROM test.main.crimes ORDER BY socrata_id","title":"Wards",
			 "chart_spec":{"chartType":"Bar Chart","encodings":{"x":{"field":"socrata_id"},"y":{"field":"ward"}}}}`),
			agenttest.Text("Ward 2 is higher."),
		), nil
	}
	t.Cleanup(func() { newModel = agent.NewLanguageModel })

	var stdout, stderr bytes.Buffer
	err := run([]string{"--db", "test=" + datatest.SeedDB(t), "-m", "fake/scripted", "--session-dir", "", "--prompt", "chart"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v\n%s", err, stderr.String())
	}
	var out headlessOutput
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Presentations) != 1 || out.Presentations[0].Kind != "chart" || len(out.Rendered) != 1 {
		t.Fatalf("output = %+v", out)
	}
	r := out.Rendered[0]
	if r.Error != "" || !strings.Contains(r.Text, "█") || strings.Contains(r.Text, "\x1b") {
		t.Errorf("rendered = %+v", r)
	}
}
