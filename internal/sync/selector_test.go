// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"sort"
	"testing"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func sampleCatalog() []socrata.CatalogEntry {
	return []socrata.CatalogEntry{
		{ID: "aaaa-0001", Name: "Crimes 2020", Category: "Public Safety", Tags: []string{"crime", "311"}},
		{ID: "aaaa-0002", Name: "Crimes 2021", Category: "Public Safety", Tags: []string{"crime"}},
		{ID: "bbbb-0003", Name: "Park Events", Category: "Parks", Tags: []string{"events"}},
		{ID: "cccc-0004", Name: "Building Permits", Category: "Buildings", Tags: []string{"permits"}},
		{ID: "dddd-0005", Name: "311 Archive 2015", Category: "Public Safety", Tags: []string{"311", "archive"}},
	}
}

func ids(targets []DatasetTarget) []string {
	out := make([]string, len(targets))
	for i, t := range targets {
		out[i] = t.ID
	}
	sort.Strings(out)
	return out
}

func TestResolve_LiteralID(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{ID: "cccc-0004"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got := ids(out); len(got) != 1 || got[0] != "cccc-0004" {
		t.Errorf("got %v, want [cccc-0004]", got)
	}
}

func TestResolve_NameGlob(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Name: "Crimes*"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	got := ids(out)
	want := []string{"aaaa-0001", "aaaa-0002"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestResolve_CategoryGlob(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Category: "Public Safety"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(out) != 3 {
		t.Errorf("got %d, want 3", len(out))
	}
}

func TestResolve_TagGlob(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Tag: "311*"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	got := ids(out)
	if len(got) != 2 {
		t.Errorf("got %v, want 2 entries", got)
	}
}

func TestResolve_ExcludeAfterInclude(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{
		Include: []config.Selector{{Category: "Public Safety"}},
		Exclude: []config.Selector{{Name: "*Archive*"}},
	}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	got := ids(out)
	want := []string{"aaaa-0001", "aaaa-0002"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestResolve_Union_Dedup(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{
		Include: []config.Selector{
			{Category: "Public Safety"},
			{Tag: "crime"},
		},
	}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(out) != 3 {
		t.Errorf("got %d, want 3 (deduped)", len(out))
	}
}

func TestResolve_OnlyFilter(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Category: "Public Safety"}}}
	out, err := r.Resolve(context.Background(), cfg, sampleCatalog(), []string{"aaaa-0001"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got := ids(out); len(got) != 1 || got[0] != "aaaa-0001" {
		t.Errorf("got %v, want [aaaa-0001]", got)
	}
}

func TestResolve_OnlyUnknown_Errors(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Category: "Public Safety"}}}
	_, err := r.Resolve(context.Background(), cfg, sampleCatalog(), []string{"zzzz-9999"})
	if err == nil {
		t.Fatal("want error for --only id not in resolved set")
	}
}

func TestResolve_EmptyMatch_Errors(t *testing.T) {
	r := &DefaultSelectorResolver{}
	cfg := &config.Config{Include: []config.Selector{{Category: "Nonexistent"}}}
	_, err := r.Resolve(context.Background(), cfg, sampleCatalog(), nil)
	if err == nil {
		t.Fatal("want error for empty match set")
	}
}
