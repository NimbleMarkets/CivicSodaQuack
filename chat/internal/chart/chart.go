// Copyright (c) 2026 Neomantra Corp

// Package chart turns a present.Chart into terminal text with flint-ntcharts.
//
// The model supplies a Flint chart_spec and semantic_types; this package
// supplies the data from the rows the host already holds and asks the
// embedded compiler to draw it. Nothing here talks to a model.
package chart

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/NimbleMarkets/flint-ntcharts/compile"
	"github.com/NimbleMarkets/flint-ntcharts/tui"

	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
)

// Renderer draws charts. It holds a compiled WebAssembly module, so build one
// and share it; it is safe for concurrent use.
type Renderer struct {
	runner *compile.Runner
}

// New compiles the embedded Flint compiler. It takes tens of milliseconds.
func New(ctx context.Context) (*Renderer, error) {
	r, err := compile.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("flint compiler: %w", err)
	}
	return &Renderer{runner: r}, nil
}

// Close releases the compiler.
func (r *Renderer) Close(ctx context.Context) error { return r.runner.Close(ctx) }

// Render draws c into a w×h cell area. Warnings are the compiler's own,
// formatted one per string; they say when a chart was approximated or when a
// property could not be honoured.
func (r *Renderer) Render(c present.Chart, w, h int) (view string, warnings []string, err error) {
	input, err := BuildInput(c)
	if err != nil {
		return "", nil, err
	}
	view, ws, err := tui.Render(r.runner, input, w, h)
	if err != nil {
		return "", nil, err
	}
	for _, x := range ws {
		warnings = append(warnings, formatWarning(x.Severity, x.Code, x.Message))
	}
	return view, warnings, nil
}

// Check renders at a typical size and reports what the model should hear:
// the compiler's error verbatim (it names the supported chart types), or its
// warnings. The rendered text is discarded.
func (r *Renderer) Check(c present.Chart) (warnings []string, err error) {
	_, warnings, err = r.Render(c, 80, 20)
	return warnings, err
}

func formatWarning(severity, code, message string) string {
	return strings.TrimSpace(fmt.Sprintf("%s %s: %s", severity, code, message))
}

// BuildInput assembles the Flint ChartAssemblyInput for c: the model's
// chart_spec and semantic_types around the rows as data.values.
func BuildInput(c present.Chart) ([]byte, error) {
	if len(c.ChartSpec) == 0 || string(c.ChartSpec) == "null" {
		return nil, errors.New("chart_spec is required")
	}
	if len(c.Columns) == 0 {
		return nil, errors.New("the query returned no columns to chart")
	}
	if len(c.Rows) == 0 {
		return nil, errors.New("the query returned no rows to chart")
	}
	doc := map[string]any{
		"data":       map[string]any{"values": Records(c.Table)},
		"chart_spec": c.ChartSpec,
	}
	if len(c.SemanticTypes) > 0 && string(c.SemanticTypes) != "null" {
		doc["semantic_types"] = c.SemanticTypes
	}
	return json.Marshal(doc)
}

// Records converts display strings back to typed values, column by column. A
// column becomes numeric only when every non-empty cell is a canonical
// number, so a code like "09" or "60614-1" stays text and an empty cell
// becomes null rather than zero.
func Records(t present.Table) []map[string]any {
	numeric := make([]bool, len(t.Columns))
	for i := range t.Columns {
		numeric[i] = columnIsNumeric(t.Rows, i)
	}
	out := make([]map[string]any, 0, len(t.Rows))
	for _, row := range t.Rows {
		rec := make(map[string]any, len(t.Columns))
		for i, name := range t.Columns {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			switch {
			case cell == "":
				rec[name] = nil
			case numeric[i]:
				f, _ := strconv.ParseFloat(cell, 64)
				rec[name] = f
			default:
				rec[name] = cell
			}
		}
		out = append(out, rec)
	}
	return out
}

func columnIsNumeric(rows [][]string, i int) bool {
	seen := false
	for _, row := range rows {
		if i >= len(row) || row[i] == "" {
			continue
		}
		if !canonicalNumber(row[i]) {
			return false
		}
		seen = true
	}
	return seen
}

// canonicalNumber reports whether s is a number written the way a number is
// written: no sign prefix, no leading zero before another digit, no spaces.
func canonicalNumber(s string) bool {
	if _, err := strconv.ParseFloat(s, 64); err != nil {
		return false
	}
	t := strings.TrimPrefix(s, "-")
	if t == "" || t[0] == '+' {
		return false
	}
	if len(t) > 1 && t[0] == '0' && t[1] != '.' {
		return false
	}
	switch strings.ToLower(t) {
	case "inf", "infinity", "nan":
		return false
	}
	return !strings.ContainsAny(t, "xXpP_")
}
