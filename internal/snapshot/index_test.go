// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func mkEntry(id string) IndexEntry {
	return IndexEntry{
		SnapshotID: id,
		CreatedAt:  time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC),
		URL:        "https://example.com/" + id + ".tar.zst",
		SizeBytes:  100,
		SHA256:     "deadbeef",
	}
}

func TestIndex_RoundTrip(t *testing.T) {
	in := &Index{
		SchemaVersion: 1, Portal: "p",
		Snapshots: []IndexEntry{mkEntry("01HZ-A"), mkEntry("01HZ-B")},
	}
	b, err := in.MarshalIndent()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := LoadIndex(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if out.Portal != "p" || len(out.Snapshots) != 2 {
		t.Errorf("got %+v", out)
	}
}

func TestIndex_AddPrependsAndSorts(t *testing.T) {
	idx := &Index{SchemaVersion: 1, Portal: "p"}
	idx.Add(mkEntry("01HZ-B"), 0)
	idx.Add(mkEntry("01HZ-D"), 0)
	idx.Add(mkEntry("01HZ-A"), 0) // older than the others; should sort to last
	got := []string{}
	for _, e := range idx.Snapshots {
		got = append(got, e.SnapshotID)
	}
	want := []string{"01HZ-D", "01HZ-B", "01HZ-A"}
	for i, id := range want {
		if got[i] != id {
			t.Errorf("position %d: got %q want %q (full %v)", i, got[i], id, got)
		}
	}
}

func TestIndex_AddMaxKeep(t *testing.T) {
	idx := &Index{SchemaVersion: 1, Portal: "p"}
	for _, id := range []string{"A", "B", "C", "D", "E"} {
		idx.Add(mkEntry("01HZ-"+id), 3)
	}
	if len(idx.Snapshots) != 3 {
		t.Errorf("len: got %d, want 3", len(idx.Snapshots))
	}
	if idx.Snapshots[0].SnapshotID != "01HZ-E" {
		t.Errorf("newest: got %q", idx.Snapshots[0].SnapshotID)
	}
}

func TestIndex_AddMaxKeep_Zero_Unbounded(t *testing.T) {
	idx := &Index{SchemaVersion: 1, Portal: "p"}
	for i := 0; i < 50; i++ {
		idx.Add(mkEntry("01HZ-"+string(rune('A'+i))), 0)
	}
	if len(idx.Snapshots) != 50 {
		t.Errorf("len: got %d, want 50 (unbounded)", len(idx.Snapshots))
	}
}

func TestIndex_Latest_Empty(t *testing.T) {
	idx := &Index{SchemaVersion: 1, Portal: "p"}
	if _, ok := idx.Latest(); ok {
		t.Errorf("empty Latest should return ok=false")
	}
}

func TestIndex_FindByID(t *testing.T) {
	idx := &Index{SchemaVersion: 1, Portal: "p"}
	idx.Add(mkEntry("01HZ-A"), 0)
	idx.Add(mkEntry("01HZ-B"), 0)
	if _, ok := idx.FindByID("01HZ-A"); !ok {
		t.Errorf("present id not found")
	}
	if _, ok := idx.FindByID("01HZ-MISSING"); ok {
		t.Errorf("missing id reported as found")
	}
}

func TestIndex_LoadIndex_BadJSON(t *testing.T) {
	_, err := LoadIndex(strings.NewReader("{not json"))
	if err == nil {
		t.Fatal("want error")
	}
}

func TestIndex_Validate_Happy(t *testing.T) {
	idx := &Index{SchemaVersion: 1, Portal: "p"}
	idx.Add(mkEntry("01HZ-A"), 0)
	if err := ValidateIndex(idx); err != nil {
		t.Errorf("happy: %v", err)
	}
}

func TestIndex_Validate_BadSchema(t *testing.T) {
	idx := &Index{SchemaVersion: 99, Portal: "p"}
	if err := ValidateIndex(idx); err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Errorf("got %v", err)
	}
}

func TestIndex_Validate_EmptyPortal(t *testing.T) {
	idx := &Index{SchemaVersion: 1}
	if err := ValidateIndex(idx); err == nil {
		t.Errorf("want portal error")
	}
}

func TestIndex_Validate_BadEntry(t *testing.T) {
	idx := &Index{SchemaVersion: 1, Portal: "p", Snapshots: []IndexEntry{{}}}
	if err := ValidateIndex(idx); err == nil || !strings.Contains(err.Error(), "snapshots[0]") {
		t.Errorf("got %v", err)
	}
}
