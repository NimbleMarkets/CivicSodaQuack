// Copyright (c) 2026 Neomantra Corp

// Package present is the contract between agent tools and the screen.
//
// A tool that wants the person to see something builds a Presentation and
// hands it to the PresentFunc carried in its context. The rows are resolved
// host-side; the model only ever receives a summary, so a large result costs
// no context.
package present

import (
	"context"
	"encoding/json"
)

// Kind names what a Presentation carries.
type Kind string

const (
	// KindTable is a titled grid of rows.
	KindTable Kind = "table"
	// KindChart is a Flint chart specification plus the rows it is drawn from.
	KindChart Kind = "chart"
)

// Presentation is one thing pushed to the screen. Exactly one of Table or
// Chart is set, matching Kind.
type Presentation struct {
	Kind  Kind   `json:"kind"`
	Title string `json:"title"`
	Table *Table `json:"table,omitempty"`
	Chart *Chart `json:"chart,omitempty"`
}

// Table is a titled grid. Total is the row count before any cap; Truncated
// says whether Rows holds fewer than Total.
type Table struct {
	Columns   []string   `json:"columns"`
	Rows      [][]string `json:"rows"`
	Total     int        `json:"total"`
	Truncated bool       `json:"truncated,omitempty"`
}

// Chart is a Flint chart over a table. ChartSpec and SemanticTypes are the
// chart_spec and semantic_types members of a Flint ChartAssemblyInput; the
// renderer binds Table.Rows as data.values.
type Chart struct {
	Table
	ChartSpec     json.RawMessage `json:"chart_spec"`
	SemanticTypes json.RawMessage `json:"semantic_types,omitempty"`
}

// Summary describes a Presentation without its rows, for session records and
// for what the model is told.
type Summary struct {
	Kind      Kind     `json:"kind"`
	Title     string   `json:"title"`
	Columns   []string `json:"columns,omitempty"`
	Rows      int      `json:"rows"`
	Total     int      `json:"total"`
	Truncated bool     `json:"truncated,omitempty"`
}

// Summary returns the row-free description of p.
func (p Presentation) Summary() Summary {
	s := Summary{Kind: p.Kind, Title: p.Title}
	var t *Table
	switch {
	case p.Table != nil:
		t = p.Table
	case p.Chart != nil:
		t = &p.Chart.Table
	}
	if t != nil {
		s.Columns = t.Columns
		s.Rows = len(t.Rows)
		s.Total = t.Total
		s.Truncated = t.Truncated
	}
	return s
}

// Func delivers a Presentation to whatever owns the screen.
type Func func(Presentation)

type ctxKey struct{}

// WithFunc returns a context that carries fn for Push to find.
func WithFunc(ctx context.Context, fn Func) context.Context {
	return context.WithValue(ctx, ctxKey{}, fn)
}

// Push delivers p through the Func in ctx. It reports whether anything was
// listening; a tool running headless with no screen gets false and should
// still answer the model.
func Push(ctx context.Context, p Presentation) bool {
	fn, ok := ctx.Value(ctxKey{}).(Func)
	if !ok || fn == nil {
		return false
	}
	fn(p)
	return true
}
