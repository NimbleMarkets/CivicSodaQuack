// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
)

// ListDatasetsArgs are the inputs to the list_datasets MCP tool.
type ListDatasetsArgs struct {
	Portal   string `json:"portal,omitempty" jsonschema:"optional portal alias filter"`
	Category string `json:"category,omitempty" jsonschema:"optional case-insensitive substring on category"`
}

// DatasetSummary is one row in list_datasets / search_datasets results.
type DatasetSummary struct {
	DatasetID string `json:"dataset_id"`
	Portal    string `json:"portal"`
	Name      string `json:"name"`
	Category  string `json:"category,omitempty"`
	TableName string `json:"table_name"`
	RowCount  *int64 `json:"row_count,omitempty"`

	synced bool // table_name came from a successful sync, so RowCount can be filled
}

// ListDatasets enumerates datasets across the requested portal (or all
// portals) with optional category substring filter.
func ListDatasets(ctx context.Context, p *Pools, args ListDatasetsArgs) ([]DatasetSummary, error) {
	out, err := listDatasetSummaries(ctx, p, args)
	if err != nil {
		return nil, err
	}
	if err := fillRowCounts(ctx, p, out); err != nil {
		return nil, err
	}
	return out, nil
}

// listDatasetSummaries is ListDatasets without row counts, which cost a table
// count per dataset. Callers that filter further (search) fill counts after.
func listDatasetSummaries(ctx context.Context, p *Pools, args ListDatasetsArgs) ([]DatasetSummary, error) {
	aliases := selectPortals(p, args.Portal)
	out := make([]DatasetSummary, 0, len(aliases)*4)

	for _, alias := range aliases {
		rows, err := queryDatasetsForPortal(ctx, p.Portals[alias].DB, alias)
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", alias, err)
		}
		out = append(out, rows...)
	}

	if args.Category != "" {
		needle := strings.ToLower(args.Category)
		filtered := out[:0]
		for _, d := range out {
			if strings.Contains(strings.ToLower(d.Category), needle) {
				filtered = append(filtered, d)
			}
		}
		out = filtered
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Portal != out[j].Portal {
			return out[i].Portal < out[j].Portal
		}
		return out[i].DatasetID < out[j].DatasetID
	})
	return out, nil
}

// selectPortals returns the alias list to scan: a single alias if requested
// (and present), or all aliases otherwise. Unknown alias yields empty result.
func selectPortals(p *Pools, requested string) []string {
	if requested != "" {
		if _, ok := p.Portals[requested]; !ok {
			return nil
		}
		return []string{requested}
	}
	out := make([]string, 0, len(p.Portals))
	for a := range p.Portals {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

// queryDatasetsForPortal returns all dataset summaries from one portal pool,
// without row counts (see fillRowCounts). table_name comes from the most recent
// status='ok' sync_runs row, falling back to replace(id, '-', '_') when none
// exists.
func queryDatasetsForPortal(ctx context.Context, db *sql.DB, alias string) ([]DatasetSummary, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.id, c.name, COALESCE(c.category, ''), s.table_name
		FROM _csq.catalog c
		LEFT JOIN (
			SELECT dataset_id,
			       FIRST(table_name ORDER BY started_at DESC) AS table_name
			FROM _csq.sync_runs
			WHERE status = 'ok'
			GROUP BY dataset_id
		) s ON s.dataset_id = c.id
		ORDER BY c.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DatasetSummary
	for rows.Next() {
		var id, name, category string
		var table sql.NullString
		if err := rows.Scan(&id, &name, &category, &table); err != nil {
			return nil, err
		}
		summary := DatasetSummary{
			DatasetID: id,
			Portal:    alias,
			Name:      name,
			Category:  category,
			synced:    table.Valid,
		}
		if table.Valid {
			summary.TableName = table.String
		} else {
			summary.TableName = strings.ReplaceAll(id, "-", "_")
		}
		out = append(out, summary)
	}
	return out, rows.Err()
}

// fillRowCounts sets RowCount on each synced summary to its table's live row
// count, one batched lookup per portal. Summaries whose table does not exist
// keep a nil RowCount.
func fillRowCounts(ctx context.Context, p *Pools, ds []DatasetSummary) error {
	tablesByPortal := map[string][]string{}
	for _, d := range ds {
		if d.synced {
			tablesByPortal[d.Portal] = append(tablesByPortal[d.Portal], d.TableName)
		}
	}
	countsByPortal := make(map[string]map[string]int64, len(tablesByPortal))
	for alias, tables := range tablesByPortal {
		counts, err := duckdb.CountMainTables(ctx, p.Portals[alias].DB, tables)
		if err != nil {
			return fmt.Errorf("count %s: %w", alias, err)
		}
		countsByPortal[alias] = counts
	}
	for i := range ds {
		if n, ok := countsByPortal[ds[i].Portal][ds[i].TableName]; ok && ds[i].synced {
			ds[i].RowCount = &n
		}
	}
	return nil
}
