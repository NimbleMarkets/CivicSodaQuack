// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// ProgressReporter receives lifecycle events for the sync orchestrator.
// Implementations must be safe for concurrent calls from worker goroutines.
type ProgressReporter interface {
	DatasetStart(idx, total int, t DatasetTarget)
	DatasetProgress(idx, total int, t DatasetTarget, rowsSoFar int64)
	DatasetDone(idx, total int, t DatasetTarget, res DatasetResult)
}

// StderrReporter writes plain-text progress lines to Out (default os.Stderr).
type StderrReporter struct {
	Out io.Writer
	mu  sync.Mutex
}

func (r *StderrReporter) line(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Fprintf(r.Out, format+"\n", args...)
}

func (r *StderrReporter) DatasetStart(idx, total int, t DatasetTarget) {
	r.line("[csq] [%d/%d]  %s  %-30s  starting", idx, total, t.ID, t.Effective.Table)
}

func (r *StderrReporter) DatasetProgress(idx, total int, t DatasetTarget, rowsSoFar int64) {
	r.line("[csq] [%d/%d]  %s  %-30s  %d rows", idx, total, t.ID, t.Effective.Table, rowsSoFar)
}

func (r *StderrReporter) DatasetDone(idx, total int, t DatasetTarget, res DatasetResult) {
	dur := res.FinishedAt.Sub(res.StartedAt).Round(time.Millisecond)
	if res.Status == "ok" {
		r.line("[csq] [%d/%d]  %s  %-30s  done: %d rows in %s",
			idx, total, t.ID, t.Effective.Table, res.RowsWritten, dur)
		return
	}
	msg := "(no error)"
	if res.Err != nil {
		msg = res.Err.Error()
	}
	r.line("[csq] [%d/%d]  %s  %-30s  %s: %s",
		idx, total, t.ID, t.Effective.Table, upperStatus(res.Status), msg)
}

func upperStatus(s string) string {
	switch s {
	case "failed":
		return "FAILED"
	case "aborted":
		return "ABORTED"
	default:
		return s
	}
}

// RecordingReporter captures events for assertions in tests.
type RecordingReporter struct {
	mu     sync.Mutex
	Events []ReporterEvent
}

type ReporterEvent struct {
	Kind   string // "start" | "progress" | "done"
	Target DatasetTarget
	Rows   int64
	Result DatasetResult
}

func (r *RecordingReporter) DatasetStart(idx, total int, t DatasetTarget) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, ReporterEvent{Kind: "start", Target: t})
}

func (r *RecordingReporter) DatasetProgress(idx, total int, t DatasetTarget, rowsSoFar int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, ReporterEvent{Kind: "progress", Target: t, Rows: rowsSoFar})
}

func (r *RecordingReporter) DatasetDone(idx, total int, t DatasetTarget, res DatasetResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Events = append(r.Events, ReporterEvent{Kind: "done", Target: t, Result: res})
}
