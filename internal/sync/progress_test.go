// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestStderrReporter_Writes(t *testing.T) {
	var buf bytes.Buffer
	r := &StderrReporter{Out: &buf}
	target := DatasetTarget{ID: "aaaa-0001", Name: "Crimes"}
	r.DatasetStart(1, 1, target)
	r.DatasetProgress(1, 1, target, 123)
	r.DatasetDone(1, 1, target, DatasetResult{
		Target: target, Status: "ok", RowsWritten: 123,
		StartedAt: time.Now().Add(-time.Second), FinishedAt: time.Now(),
	})

	s := buf.String()
	for _, want := range []string{"aaaa-0001", "starting", "done"} {
		if !strings.Contains(s, want) {
			t.Errorf("output missing %q:\n%s", want, s)
		}
	}
}

func TestRecordingReporter_Records(t *testing.T) {
	r := &RecordingReporter{}
	target := DatasetTarget{ID: "aaaa-0001"}
	r.DatasetStart(1, 2, target)
	r.DatasetDone(1, 2, target, DatasetResult{Target: target, Status: "ok"})
	if len(r.Events) != 2 {
		t.Fatalf("events: got %d, want 2", len(r.Events))
	}
	if r.Events[0].Kind != "start" || r.Events[1].Kind != "done" {
		t.Errorf("kinds: got %v", r.Events)
	}
}
