// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestSearch_NameSubstring(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "Chicago Crimes"},
		FixtureDataset{ID: "bbbb-0002", Name: "Park Events"})
	defer cleanup()

	got, err := SearchDatasets(context.Background(), pools, SearchDatasetsArgs{Query: "crime"})
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

	got, _ := SearchDatasets(context.Background(), pools, SearchDatasetsArgs{Query: "crime"})
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		t.Errorf("got %v", ids(got))
	}
}

func TestSearch_TagExactInsensitive(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "A", Tags: []string{"311", "crime"}},
		FixtureDataset{ID: "bbbb-0002", Name: "B", Tags: []string{"parks"}})
	defer cleanup()

	got, _ := SearchDatasets(context.Background(), pools, SearchDatasetsArgs{Query: "CRIME"})
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		t.Errorf("got %v", ids(got))
	}
}

func TestSearch_PortalScopes(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "Crimes A"})
	defer cleanup()

	got, _ := SearchDatasets(context.Background(), pools, SearchDatasetsArgs{Query: "crime", Portal: "missing"})
	if len(got) != 0 {
		t.Errorf("portal filter should narrow to zero, got %d", len(got))
	}
}

func TestSearch_EmptyQueryErrors(t *testing.T) {
	pools, cleanup := openFixturePools(t)
	defer cleanup()

	_, err := SearchDatasets(context.Background(), pools, SearchDatasetsArgs{Query: ""})
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

func TestSearch_MultiWordEveryWordMustMatch(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "COPA Cases", Description: "Citizen complaints against officers"},
		FixtureDataset{ID: "bbbb-0002", Name: "COPA Budget", Description: "Spending", Tags: []string{"Finance"}},
		FixtureDataset{ID: "cccc-0003", Name: "Park Events", Description: "complaints about noise"})
	defer cleanup()

	cases := map[string][]string{
		"copa complaints": {"aaaa-0001"}, // words match in different fields
		"COPA   finance":  {"bbbb-0002"}, // case-insensitive, tag word, extra spaces
		"copa":            {"aaaa-0001", "bbbb-0002"},
		"copa parks":      {}, // one word misses everywhere
	}
	for q, want := range cases {
		got, err := SearchDatasets(context.Background(), pools, SearchDatasetsArgs{Query: q})
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		g := ids(got)
		sort.Strings(g)
		if strings.Join(g, ",") != strings.Join(want, ",") {
			t.Errorf("%q: got %v, want %v", q, g, want)
		}
	}
}

// Counting is deferred until after matching, but matches must still carry
// their live row counts.
func TestSearch_MatchesCarryRowCounts(t *testing.T) {
	hwm := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	pools, cleanup := openFixturePools(t,
		FixtureDataset{
			ID: "aaaa-0001", Name: "Chicago Crimes", TableName: "aaaa_0001",
			ColumnDefs: []string{"socrata_id VARCHAR"},
			Rows:       []map[string]any{{"socrata_id": "a"}, {"socrata_id": "b"}},
			Synced:     true, HWM: hwm,
		},
		FixtureDataset{
			ID: "bbbb-0002", Name: "Park Events", TableName: "bbbb_0002",
			ColumnDefs: []string{"socrata_id VARCHAR"},
			Rows:       []map[string]any{{"socrata_id": "x"}},
			Synced:     true, HWM: hwm,
		})
	defer cleanup()

	got, err := SearchDatasets(context.Background(), pools, SearchDatasetsArgs{Query: "crimes"})
	if err != nil || len(got) != 1 {
		t.Fatalf("search: %v %v", got, err)
	}
	if got[0].RowCount == nil || *got[0].RowCount != 2 {
		t.Errorf("row_count: got %v, want 2", got[0].RowCount)
	}
}
