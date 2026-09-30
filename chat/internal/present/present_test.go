// Copyright (c) 2026 Neomantra Corp

package present

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestSummary_TableCountsRowsNotContents(t *testing.T) {
	p := Presentation{Kind: KindTable, Title: "t", Table: &Table{
		Columns: []string{"a", "b"}, Rows: [][]string{{"1", "2"}}, Total: 500, Truncated: true,
	}}
	s := p.Summary()
	if s.Rows != 1 || s.Total != 500 || !s.Truncated || len(s.Columns) != 2 {
		t.Errorf("summary = %+v", s)
	}
	b, _ := json.Marshal(s)
	if strings.Contains(string(b), `"1"`) {
		t.Errorf("summary must not carry cell values: %s", b)
	}
}

func TestSummary_ChartUsesEmbeddedTable(t *testing.T) {
	p := Presentation{Kind: KindChart, Title: "c", Chart: &Chart{
		Table:     Table{Columns: []string{"x"}, Rows: [][]string{{"1"}, {"2"}}, Total: 2},
		ChartSpec: json.RawMessage(`{"chartType":"Bar Chart"}`),
	}}
	if s := p.Summary(); s.Rows != 2 || s.Kind != KindChart {
		t.Errorf("summary = %+v", s)
	}
}

func TestPush_ReportsWhetherAnyoneListened(t *testing.T) {
	if Push(context.Background(), Presentation{Kind: KindTable}) {
		t.Error("push with no listener reported delivery")
	}
	var got []Presentation
	ctx := WithFunc(context.Background(), func(p Presentation) { got = append(got, p) })
	if !Push(ctx, Presentation{Kind: KindTable, Title: "x"}) {
		t.Error("push with listener reported no delivery")
	}
	if len(got) != 1 || got[0].Title != "x" {
		t.Errorf("listener got %+v", got)
	}
}
