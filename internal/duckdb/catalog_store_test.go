// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func TestCatalogStore_UpsertAndRead(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	updated := time.Date(2025, 12, 15, 12, 0, 0, 0, time.UTC)

	in := []socrata.CatalogEntry{
		{
			ID: "abcd-0001", Name: "Crimes", Category: "Public Safety",
			Tags: []string{"crime"}, UpdatedAt: &updated,
			Raw: json.RawMessage(`{"resource":{"id":"abcd-0001"}}`),
		},
		{
			ID: "abcd-0002", Name: "Permits",
			Raw: json.RawMessage(`{"resource":{"id":"abcd-0002"}}`),
		},
	}

	if err := w.UpsertCatalog(in, now); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := w.ReadCatalog()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d", len(got))
	}

	// Second upsert replaces, doesn't duplicate
	in2 := []socrata.CatalogEntry{{ID: "abcd-0003", Name: "Parks", Raw: json.RawMessage(`{}`)}}
	if err := w.UpsertCatalog(in2, now); err != nil {
		t.Fatalf("upsert2: %v", err)
	}
	got, err = w.ReadCatalog()
	if err != nil {
		t.Fatalf("read2: %v", err)
	}
	if len(got) != 1 || got[0].ID != "abcd-0003" {
		t.Errorf("replace failed: got %+v", got)
	}
}

func TestCatalogStore_RoundTrip(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	updated := time.Date(2025, 12, 15, 12, 0, 0, 0, time.UTC).Truncate(time.Microsecond)

	in := []socrata.CatalogEntry{
		{
			ID:          "abcd-0001",
			Name:        "Crimes",
			Description: "Reported incidents",
			Category:    "Public Safety",
			Tags:        []string{"crime", "311"},
			UpdatedAt:   &updated,
			Raw:         json.RawMessage(`{"resource":{"id":"abcd-0001"}}`),
		},
		{
			// All-optional-unset entry
			ID:   "abcd-0002",
			Name: "Permits",
			Raw:  json.RawMessage(`{}`),
		},
	}
	if err := w.UpsertCatalog(in, now); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := w.ReadCatalog()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}

	// First entry: fully populated
	e0 := got[0]
	if e0.ID != "abcd-0001" {
		t.Errorf("e0.ID = %q", e0.ID)
	}
	if e0.Description != "Reported incidents" {
		t.Errorf("e0.Description = %q", e0.Description)
	}
	if e0.Category != "Public Safety" {
		t.Errorf("e0.Category = %q", e0.Category)
	}
	if len(e0.Tags) != 2 || e0.Tags[0] != "crime" || e0.Tags[1] != "311" {
		t.Errorf("e0.Tags = %v", e0.Tags)
	}
	if e0.UpdatedAt == nil || !e0.UpdatedAt.Equal(updated) {
		t.Errorf("e0.UpdatedAt = %v, want %v", e0.UpdatedAt, updated)
	}
	if len(e0.Raw) == 0 {
		t.Error("e0.Raw is empty")
	}

	// Second entry: nothing optional set — should round-trip cleanly
	e1 := got[1]
	if e1.ID != "abcd-0002" {
		t.Errorf("e1.ID = %q", e1.ID)
	}
	if e1.Description != "" {
		t.Errorf("e1.Description = %q, want empty", e1.Description)
	}
	if e1.Category != "" {
		t.Errorf("e1.Category = %q, want empty", e1.Category)
	}
	if e1.UpdatedAt != nil {
		t.Errorf("e1.UpdatedAt = %v, want nil", e1.UpdatedAt)
	}
	if e1.RowCount != nil {
		t.Errorf("e1.RowCount = %v, want nil", e1.RowCount)
	}
}

func TestCatalogStore_EmptyListIsNoop(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	// Seed with one entry
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := w.UpsertCatalog([]socrata.CatalogEntry{
		{ID: "abcd-0001", Name: "Seed", Raw: json.RawMessage(`{}`)},
	}, now); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Empty upsert must NOT wipe the seed entry
	if err := w.UpsertCatalog(nil, now); err != nil {
		t.Fatalf("upsert nil: %v", err)
	}
	if err := w.UpsertCatalog([]socrata.CatalogEntry{}, now); err != nil {
		t.Fatalf("upsert empty: %v", err)
	}

	got, err := w.ReadCatalog()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 || got[0].ID != "abcd-0001" {
		t.Errorf("empty upsert wiped cache; got %+v", got)
	}
}
