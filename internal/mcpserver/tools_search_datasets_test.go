// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"sort"
	"strings"
	"testing"
)

func TestSearch_NameSubstring(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "Chicago Crimes"},
		FixtureDataset{ID: "bbbb-0002", Name: "Park Events"})
	defer cleanup()

	got, err := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: "crime"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		t.Errorf("got %v", ids(got))
	}
}

func TestSearch_DescriptionSubstring(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X", Description: "All things crime"},
		FixtureDataset{ID: "bbbb-0002", Name: "Y", Description: "Parks data"})
	defer cleanup()

	got, _ := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: "crime"})
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		t.Errorf("got %v", ids(got))
	}
}

func TestSearch_TagExactInsensitive(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "A", Tags: []string{"311", "crime"}},
		FixtureDataset{ID: "bbbb-0002", Name: "B", Tags: []string{"parks"}})
	defer cleanup()

	got, _ := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: "CRIME"})
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		t.Errorf("got %v", ids(got))
	}
}

func TestSearch_PortalScopes(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "Crimes A"})
	defer cleanup()

	got, _ := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: "crime", Portal: "missing"})
	if len(got) != 0 {
		t.Errorf("portal filter should narrow to zero, got %d", len(got))
	}
}

func TestSearch_EmptyQueryErrors(t *testing.T) {
	pools, cleanup := openFixturePools(t)
	defer cleanup()

	_, err := searchDatasetsHandler(context.Background(), pools, SearchDatasetsArgs{Query: ""})
	if err == nil || !strings.Contains(err.Error(), "query") {
		t.Errorf("want empty-query error, got %v", err)
	}
}

func ids(in []DatasetSummary) []string {
	out := []string{}
	for _, d := range in {
		out = append(out, d.DatasetID)
	}
	sort.Strings(out)
	return out
}
