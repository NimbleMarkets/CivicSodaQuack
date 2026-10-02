// Copyright (c) 2026 Neomantra Corp

package chart

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"

	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

func barChart() present.Chart {
	return present.Chart{
		Table: present.Table{
			Columns: []string{"year", "n"},
			Rows:    [][]string{{"2022", "5277"}, {"2023", "6073"}, {"2024", "6375"}},
			Total:   3,
		},
		ChartSpec:     json.RawMessage(`{"chartType":"Bar Chart","encodings":{"x":{"field":"year"},"y":{"field":"n"}}}`),
		SemanticTypes: json.RawMessage(`{"n":"Count"}`),
	}
}

func TestRecords_TypesColumnsConservatively(t *testing.T) {
	recs := Records(present.Table{
		Columns: []string{"zip", "hour", "n", "rate", "note", "mixed"},
		Rows: [][]string{
			{"60614", "09", "5", "1.5", "", "1"},
			{"60615", "10", "6", "0.25", "x", "abc"},
			{"02134", "11", "", "-3", "", "2"},
		},
	})
	// zip has a leading-zero value, so it stays text for every row.
	if recs[0]["zip"] != "60614" || recs[2]["zip"] != "02134" {
		t.Errorf("zip must stay text: %v", recs)
	}
	if recs[0]["hour"] != "09" {
		t.Errorf("hour must stay text: %v", recs[0]["hour"])
	}
	if recs[0]["n"] != 5.0 || recs[2]["n"] != nil {
		t.Errorf("n: %v / %v", recs[0]["n"], recs[2]["n"])
	}
	if recs[2]["rate"] != -3.0 || recs[1]["rate"] != 0.25 {
		t.Errorf("rate: %v", recs)
	}
	if recs[0]["note"] != nil || recs[1]["note"] != "x" {
		t.Errorf("note: %v", recs)
	}
	if recs[0]["mixed"] != "1" {
		t.Errorf("a column with any non-number stays text: %v", recs[0]["mixed"])
	}
}

func TestCanonicalNumber(t *testing.T) {
	for s, want := range map[string]bool{
		"0": true, "7": true, "-7": true, "0.5": true, "12.75": true, "1e3": true,
		"07": false, "+7": false, "NaN": false, "Inf": false, "0x10": false, "1_000": false, "1,000": false, " 1": false, "": false, "-": false,
	} {
		if got := canonicalNumber(s); got != want {
			t.Errorf("canonicalNumber(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestBuildInput_Shape(t *testing.T) {
	raw, err := BuildInput(barChart())
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Data struct {
			Values []map[string]any `json:"values"`
		} `json:"data"`
		Spec  map[string]any `json:"chart_spec"`
		Types map[string]any `json:"semantic_types"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Data.Values) != 3 || doc.Data.Values[0]["n"] != 5277.0 || doc.Data.Values[0]["year"] != 2022.0 {
		t.Errorf("values = %v", doc.Data.Values)
	}
	if doc.Spec["chartType"] != "Bar Chart" || doc.Types["n"] != "Count" {
		t.Errorf("spec/types = %v / %v", doc.Spec, doc.Types)
	}
}

func TestBuildInput_RefusesEmptyThings(t *testing.T) {
	c := barChart()
	c.ChartSpec = nil
	if _, err := BuildInput(c); err == nil || !strings.Contains(err.Error(), "chart_spec") {
		t.Errorf("no spec: %v", err)
	}
	c = barChart()
	c.Rows = nil
	if _, err := BuildInput(c); err == nil || !strings.Contains(err.Error(), "no rows") {
		t.Errorf("no rows: %v", err)
	}
}

func TestRender_DrawsABarChart(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())

	view, _, err := r.Render(barChart(), 60, 14)
	if err != nil {
		t.Fatal(err)
	}
	plain := ansi.Strip(view)
	for _, want := range []string{"2022", "2023", "2024", "█"} {
		if !strings.Contains(plain, want) {
			t.Errorf("chart lacks %q:\n%s", want, plain)
		}
	}
}

func TestRender_UnsupportedTypeReturnsTheCompilersGuidance(t *testing.T) {
	r, err := New(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())

	c := barChart()
	c.ChartSpec = json.RawMessage(`{"chartType":"Pie Chart","encodings":{"color":{"field":"year"},"size":{"field":"n"}}}`)
	_, err = r.Check(c)
	if err == nil || !strings.Contains(err.Error(), "Supported:") {
		t.Errorf("want an error naming the supported types, got %v", err)
	}
}
