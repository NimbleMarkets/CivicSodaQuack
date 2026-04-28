// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"strings"
	"testing"
	"time"
)

func TestManifest_MarshalIndentRoundTrip(t *testing.T) {
	in := &Manifest{
		SchemaVersion:   1,
		Portal:          "data.cityofchicago.org",
		CSQVersion:      "0.4.0",
		SnapshotID:      "01HXYZABCDEFGHJKMNPQRSTVWX",
		CreatedAt:       time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC),
		DuckDBFilename:  "data.cityofchicago.org.duckdb",
		DuckDBSHA256:    "abc123",
		DuckDBSizeBytes: 12345678,
		DatasetCount:    47,
		TotalRowCount:   12345678,
	}
	b, err := in.MarshalIndent()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := ParseManifest(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if out.Portal != in.Portal || out.SchemaVersion != in.SchemaVersion {
		t.Errorf("round-trip mismatch: in=%+v out=%+v", in, out)
	}
	if !out.CreatedAt.Equal(in.CreatedAt) {
		t.Errorf("CreatedAt: in=%v out=%v", in.CreatedAt, out.CreatedAt)
	}
	if out.DuckDBSizeBytes != in.DuckDBSizeBytes {
		t.Errorf("DuckDBSizeBytes: %d vs %d", in.DuckDBSizeBytes, out.DuckDBSizeBytes)
	}
}

func TestManifest_ParseRejectsInvalidJSON(t *testing.T) {
	_, err := ParseManifest([]byte("{ not json"))
	if err == nil {
		t.Fatal("want parse error")
	}
}

func TestManifest_MarshalIsIndented(t *testing.T) {
	m := &Manifest{SchemaVersion: 1, Portal: "p", CSQVersion: "v", SnapshotID: "i", DuckDBFilename: "f.duckdb", DuckDBSHA256: "h"}
	b, err := m.MarshalIndent()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), "\n  ") {
		t.Errorf("expected indented output with leading spaces, got:\n%s", b)
	}
}

func TestSchemaVersion_IsOne(t *testing.T) {
	if SchemaVersion != 1 {
		t.Errorf("SchemaVersion: got %d, want 1", SchemaVersion)
	}
}
