// Copyright (c) 2026 Neomantra Corp

package agent

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/neomantra/CivicSodaQuack/chat/internal/agent/agenttest"
	"github.com/neomantra/CivicSodaQuack/chat/internal/chart"
	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

func newCharts(t *testing.T) *chart.Renderer {
	t.Helper()
	r, err := chart.New(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	return r
}

const barCall = `{"sql":"SELECT socrata_id, ward FROM test.main.crimes ORDER BY socrata_id","title":"Wards",
 "chart_spec":{"chartType":"Bar Chart","encodings":{"x":{"field":"socrata_id"},"y":{"field":"ward"}}},
 "semantic_types":[{"field":"ward","type":"Count"}]}`

func TestPresentChart_NotOfferedWithoutARenderer(t *testing.T) {
	for _, tool := range tools(seedStore(t), Options{}) {
		if tool.Info().Name == "present_chart" {
			t.Fatal("present_chart must not be offered when no chart renderer is configured")
		}
	}
}

func TestPresentChart_SchemaDescribesObjectsNotBytes(t *testing.T) {
	var info string
	for _, tool := range tools(seedStore(t), Options{Charts: newCharts(t)}) {
		if tool.Info().Name == "present_chart" {
			b, _ := json.Marshal(tool.Info())
			info = string(b)
		}
	}
	if info == "" {
		t.Fatal("present_chart missing")
	}
	if !strings.Contains(info, `"chart_spec"`) || !strings.Contains(info, `"object"`) {
		t.Errorf("schema = %s", info)
	}
	if strings.Contains(info, `"integer"`) {
		t.Errorf("chart_spec looks like a byte array in the schema: %s", info)
	}
}

func TestPresentChart_PushesChartAndTellsModelOnlyTheShape(t *testing.T) {
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "present_chart", barCall),
		agenttest.Text("Ward 2 is higher."),
	)
	r := New(model, seedStore(t), Options{Charts: newCharts(t)})

	var shown []present.Presentation
	resp, err := r.Ask(context.Background(), "chart wards", nil, func(p present.Presentation) { shown = append(shown, p) })
	if err != nil {
		t.Fatal(err)
	}
	if resp.Presented != 1 || len(shown) != 1 || shown[0].Kind != present.KindChart || shown[0].Chart == nil {
		t.Fatalf("shown = %+v resp = %+v", shown, resp)
	}
	ch := shown[0].Chart
	if len(ch.Rows) != 2 || !strings.Contains(string(ch.ChartSpec), "Bar Chart") || !strings.Contains(string(ch.SemanticTypes), "Count") {
		t.Errorf("chart = %+v", ch)
	}
	told := agenttest.ToolResultText(model.Calls[1], "c1")
	if !strings.Contains(told, `"Wards"`) || !strings.Contains(told, "2 row(s)") {
		t.Errorf("model was told: %q", told)
	}
	if strings.Contains(told, `"a"`) {
		t.Errorf("model was told cell values: %q", told)
	}
}

func TestPresentChart_BadSpecGivesTheModelTheReasonAndShowsNothing(t *testing.T) {
	bad := `{"sql":"SELECT ward FROM test.main.crimes","title":"x",
	  "chart_spec":{"chartType":"Pie Chart","encodings":{"color":{"field":"ward"}}}}`
	model := agenttest.NewFakeModel(
		agenttest.ToolCall("c1", "present_chart", bad),
		agenttest.Text("I could not chart that."),
	)
	r := New(model, seedStore(t), Options{Charts: newCharts(t)})
	var shown int
	if _, err := r.Ask(context.Background(), "pie", nil, func(present.Presentation) { shown++ }); err != nil {
		t.Fatalf("a bad spec must not fail the turn: %v", err)
	}
	if shown != 0 {
		t.Errorf("an undrawable chart was shown")
	}
	told := agenttest.ToolResultText(model.Calls[1], "c1")
	if !strings.Contains(told, "could not be drawn") || !strings.Contains(told, "Supported:") || !strings.Contains(told, "ward") {
		t.Errorf("model should get the reason, the supported list, and the columns: %q", told)
	}
}

func TestPresentChart_MissingOrEmptySpecIsAnError(t *testing.T) {
	for name, args := range map[string]string{
		"missing": `{"sql":"SELECT ward FROM test.main.crimes","title":"x"}`,
		"empty":   `{"sql":"SELECT ward FROM test.main.crimes","title":"x","chart_spec":{}}`,
	} {
		model := agenttest.NewFakeModel(
			agenttest.ToolCall("c1", "present_chart", args),
			agenttest.Text("ok"),
		)
		r := New(model, seedStore(t), Options{Charts: newCharts(t)})
		if _, err := r.Ask(context.Background(), "chart", nil, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if told := agenttest.ToolResultText(model.Calls[1], "c1"); !strings.Contains(told, "chart_spec") {
			t.Errorf("%s: told %q", name, told)
		}
	}
}

func TestChartTypesEnumMatchesConstant(t *testing.T) {
	f, ok := reflect.TypeOf(chartSpecInput{}).FieldByName("ChartType")
	if !ok {
		t.Fatal("no ChartType field")
	}
	if got := f.Tag.Get("enum"); got != chartTypes {
		t.Errorf("enum tag drifted from chartTypes:\n tag:   %s\n const: %s", got, chartTypes)
	}
}

func TestPresentChart_SchemaHasNoStarProperty(t *testing.T) {
	for _, tool := range tools(seedStore(t), Options{Charts: newCharts(t)}) {
		if tool.Info().Name != "present_chart" {
			continue
		}
		b, _ := json.Marshal(tool.Info())
		s := string(b)
		if strings.Contains(s, `"*"`) {
			t.Errorf("schema has a literal * property models will copy: %s", s)
		}
		for _, want := range []string{`"chartType"`, `"Bar Chart"`, `"encodings"`, `"semantic_types"`} {
			if !strings.Contains(s, want) {
				t.Errorf("schema lacks %s: %s", want, s)
			}
		}
	}
}

func TestPresentChart_WrongFieldNameListsTheColumns(t *testing.T) {
	call := `{"sql":"SELECT ward FROM test.main.crimes","title":"x","chart_spec":{"chartType":"Bar Chart","encodings":{"x":{"field":"wardd"},"y":{"field":"ward"}}}}`
	model := agenttest.NewFakeModel(agenttest.ToolCall("c1", "present_chart", call), agenttest.Text("ok"))
	r := New(model, seedStore(t), Options{Charts: newCharts(t)})
	if _, err := r.Ask(context.Background(), "chart", nil, nil); err != nil {
		t.Fatal(err)
	}
	told := agenttest.ToolResultText(model.Calls[1], "c1")
	if !strings.Contains(told, `"wardd"`) || !strings.Contains(told, "columns: ward") {
		t.Errorf("told: %q", told)
	}
}
