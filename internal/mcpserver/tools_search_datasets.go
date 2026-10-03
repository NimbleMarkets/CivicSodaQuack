// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// SearchDatasetsArgs are the inputs to search_datasets.
type SearchDatasetsArgs struct {
	Query  string `json:"query" jsonschema:"words to find; every word must appear (case-insensitive) in the name or description, or equal a tag"`
	Portal string `json:"portal,omitempty" jsonschema:"optional portal alias filter"`
}

// SearchDatasets returns datasets matching every whitespace-separated word of
// the query. A word matches when it is a case-insensitive substring of the
// name or description, or case-insensitively equals one of the tags. Words may
// match in different fields, so "copa complaints" finds a dataset named
// "COPA Cases" whose description mentions complaints. This is the single
// search policy for csq and the chat; adapters pass the query through.
func SearchDatasets(ctx context.Context, p *Pools, args SearchDatasetsArgs) ([]DatasetSummary, error) {
	words := strings.Fields(strings.ToLower(args.Query))
	if len(words) == 0 {
		return nil, fmt.Errorf("query must not be empty")
	}
	all, err := ListDatasets(ctx, p, ListDatasetsArgs{Portal: args.Portal})
	if err != nil {
		return nil, err
	}

	// One catalog read per portal, not one per dataset.
	byPortal := map[string]map[string]searchRow{}
	for _, alias := range selectPortals(p, args.Portal) {
		m, err := loadSearchable(ctx, p.Portals[alias].DB)
		if err != nil {
			return nil, fmt.Errorf("search %s: %w", alias, err)
		}
		byPortal[alias] = m
	}

	out := make([]DatasetSummary, 0, len(all))
	for _, d := range all {
		meta, ok := byPortal[d.Portal][d.DatasetID]
		if !ok {
			continue
		}
		matched := true
		for _, w := range words {
			if !strings.Contains(meta.text, w) && !meta.tags[w] {
				matched = false
				break
			}
		}
		if matched {
			out = append(out, d)
		}
	}
	return out, nil
}

type searchRow struct {
	text string
	tags map[string]bool
}

// loadSearchable reads id, name, description and tags for every catalog row in
// one query.
func loadSearchable(ctx context.Context, db *sql.DB) (map[string]searchRow, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id, COALESCE(name, ''), COALESCE(description, ''), tags FROM _csq.catalog`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]searchRow{}
	for rows.Next() {
		var id, name, description string
		var tagsRaw any
		if err := rows.Scan(&id, &name, &description, &tagsRaw); err != nil {
			return nil, err
		}
		r := searchRow{text: strings.ToLower(name + "\n" + description), tags: map[string]bool{}}
		// DuckDB returns JSON columns as native Go values; re-marshal to a string
		// then unmarshal as []string. See project_duckdb_json_scan.md.
		if tagsRaw != nil {
			if b, err := json.Marshal(tagsRaw); err == nil {
				var tags []string
				if json.Unmarshal(b, &tags) == nil {
					for _, t := range tags {
						r.tags[strings.ToLower(t)] = true
					}
				}
			}
		}
		out[id] = r
	}
	return out, rows.Err()
}
