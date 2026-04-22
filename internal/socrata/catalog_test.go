// Copyright (c) 2026 Neomantra Corp

package socrata

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestFetchCatalog_Paginates(t *testing.T) {
	total := 5
	pageSize := 2
	mux := http.NewServeMux()
	mux.HandleFunc("/api/catalog/v1", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		offset, _ := strconv.Atoi(q.Get("offset"))
		limit, _ := strconv.Atoi(q.Get("limit"))
		results := []map[string]any{}
		for i := offset; i < offset+limit && i < total; i++ {
			results = append(results, map[string]any{
				"resource": map[string]any{
					"id":            "abcd-000" + strconv.Itoa(i),
					"name":          "Dataset " + strconv.Itoa(i),
					"description":   "desc",
					"rowsUpdatedAt": "2024-01-15T00:00:00.000",
				},
				"classification": map[string]any{
					"domain_category": "Public Safety",
					"domain_tags":     []string{"crime"},
				},
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results":       results,
			"resultSetSize": total,
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	c := &Client{BatchSize: pageSize}
	entries, err := c.fetchCatalogScheme(host, "http")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(entries) != total {
		t.Fatalf("got %d entries, want %d", len(entries), total)
	}
	if entries[0].ID != "abcd-0000" {
		t.Errorf("first id: got %q, want abcd-0000", entries[0].ID)
	}
	if entries[0].Category != "Public Safety" {
		t.Errorf("category: got %q, want %q", entries[0].Category, "Public Safety")
	}
	if len(entries[0].Tags) != 1 || entries[0].Tags[0] != "crime" {
		t.Errorf("tags: got %v", entries[0].Tags)
	}
	if entries[0].UpdatedAt == nil {
		t.Error("UpdatedAt: got nil, want non-nil")
	} else if entries[0].UpdatedAt.Year() != 2024 {
		t.Errorf("UpdatedAt year: got %d, want 2024", entries[0].UpdatedAt.Year())
	}
	if len(entries[0].Raw) == 0 {
		t.Error("Raw: expected non-empty JSON")
	}
}
