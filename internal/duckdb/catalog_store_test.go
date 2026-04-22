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
