// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// RefreshCatalogArgs are the inputs to refresh_catalog.
type RefreshCatalogArgs struct {
	Portal string `json:"portal,omitempty" jsonschema:"optional alias to limit refresh; default: all registered"`
}

// RefreshCatalogResult is one entry in the per-portal result list.
type RefreshCatalogResult struct {
	Portal       string    `json:"portal"`
	DatasetCount int64     `json:"dataset_count"`
	FetchedAt    time.Time `json:"fetched_at"`
	Error        string    `json:"error,omitempty"`
}

// RefreshCatalogResultList wraps the slice in an object so the MCP SDK accepts
// it as a structured output (the SDK rejects array-typed root outputs).
type RefreshCatalogResultList struct {
	Results []RefreshCatalogResult `json:"results"`
}

// refreshCatalogHandler refetches /api/catalog/v1 for the requested portal (or
// every registered portal) and upserts _csq.catalog. Per-portal failures don't
// abort the batch; they're reflected in the per-result Error field.
func refreshCatalogHandler(ctx context.Context, configs map[string]*config.Config,
	args RefreshCatalogArgs) (RefreshCatalogResultList, error) {

	var aliases []string
	if args.Portal != "" {
		if _, ok := configs[args.Portal]; !ok {
			return RefreshCatalogResultList{}, fmt.Errorf(
				"refresh_catalog: no config registered for portal %q", args.Portal)
		}
		aliases = []string{args.Portal}
	} else {
		aliases = make([]string, 0, len(configs))
		for a := range configs {
			aliases = append(aliases, a)
		}
		sort.Strings(aliases)
	}

	results := make([]RefreshCatalogResult, 0, len(aliases))
	for _, alias := range aliases {
		cfg := configs[alias]
		results = append(results, refreshOnePortal(ctx, alias, cfg))
	}
	return RefreshCatalogResultList{Results: results}, nil
}

func refreshOnePortal(ctx context.Context, alias string, cfg *config.Config) RefreshCatalogResult {
	res := RefreshCatalogResult{Portal: alias, FetchedAt: time.Now().UTC()}

	w, err := duckdb.Open(cfg.DB)
	if err != nil {
		res.Error = fmt.Sprintf("open db: %v", err)
		return res
	}
	defer w.Close()

	client := &socrata.Client{AppToken: cfg.AppToken}
	catalog, err := client.FetchCatalog(cfg.Portal)
	if err != nil {
		res.Error = fmt.Sprintf("fetch catalog: %v", err)
		return res
	}
	if err := w.UpsertCatalog(catalog, time.Now().UTC()); err != nil {
		res.Error = fmt.Sprintf("upsert catalog: %v", err)
		return res
	}
	res.DatasetCount = int64(len(catalog))
	return res
}
