// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// fakeDataset is one dataset the fake server will serve.
type fakeDataset struct {
	ID      string
	Name    string
	Columns []map[string]string // each has fieldName + dataTypeName
	Rows    []map[string]any
	// FailAtOffset: if > 0, return 500 when $offset >= FailAtOffset
	FailAtOffset int
}

func newFakeSocrata(t *testing.T, datasets ...fakeDataset) *httptest.Server {
	t.Helper()
	byID := map[string]fakeDataset{}
	for _, d := range datasets {
		byID[d.ID] = d
	}
	mux := http.NewServeMux()

	// /api/catalog/v1
	mux.HandleFunc("/api/catalog/v1", func(w http.ResponseWriter, r *http.Request) {
		results := make([]map[string]any, 0, len(datasets))
		for _, d := range datasets {
			results = append(results, map[string]any{
				"resource": map[string]any{
					"id":            d.ID,
					"name":          d.Name,
					"description":   "",
					"rowsUpdatedAt": "2024-01-15T00:00:00.000",
				},
				"classification": map[string]any{
					"domain_category": "Test",
					"domain_tags":     []string{"test"},
				},
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results":       results,
			"resultSetSize": len(datasets),
		})
	})

	// /api/views/{id}.json
	mux.HandleFunc("/api/views/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/views/"), ".json")
		d, ok := byID[id]
		if !ok {
			http.Error(w, "not found", 404)
			return
		}
		cols := make([]map[string]string, 0, len(d.Columns))
		for _, c := range d.Columns {
			cols = append(cols, c)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": d.ID, "name": d.Name, "columns": cols,
		})
	})

	// /resource/{id}.json
	mux.HandleFunc("/resource/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/resource/"), ".json")
		d, ok := byID[id]
		if !ok {
			http.Error(w, "not found", 404)
			return
		}
		q := r.URL.Query()
		offset, _ := strconv.Atoi(q.Get("$offset"))
		limit, _ := strconv.Atoi(q.Get("$limit"))
		if d.FailAtOffset > 0 && offset >= d.FailAtOffset {
			http.Error(w, "synthetic failure", 500)
			return
		}
		end := offset + limit
		if end > len(d.Rows) {
			end = len(d.Rows)
		}
		if offset > len(d.Rows) {
			offset = len(d.Rows)
		}
		_ = json.NewEncoder(w).Encode(d.Rows[offset:end])
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// fakeHost returns the host:port of an httptest.Server (strips "http://").
func fakeHost(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

// makeRows is a small helper to generate n rows of the given shape.
func makeRows(n int, mk func(i int) map[string]any) []map[string]any {
	out := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		out[i] = mk(i)
	}
	return out
}
