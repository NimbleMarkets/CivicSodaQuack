// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"net/url"

	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// WriteStrategy owns how one dataset's rows land in DuckDB.
type WriteStrategy interface {
	Sync(
		ctx context.Context,
		target DatasetTarget,
		client *socrata.Client,
		w *duckdb.Writer,
		prog ProgressReporter,
		idx, total int,
	) (DatasetResult, error)
}

func fetchMetadata(ctx context.Context, c *socrata.Client, scheme, portal, id string) (*socrata.DatasetMetadata, error) {
	u := &url.URL{Scheme: scheme, Host: portal, Path: "/api/views/" + id + ".json"}
	return c.FetchMetadataURL(ctx, u.String())
}

func filterColumns(cols []socrata.Column, skip []string) []socrata.Column {
	if len(skip) == 0 {
		return cols
	}
	skipSet := make(map[string]struct{}, len(skip))
	for _, s := range skip {
		skipSet[s] = struct{}{}
	}
	out := cols[:0:0]
	for _, c := range cols {
		if _, drop := skipSet[c.FieldName]; drop {
			continue
		}
		out = append(out, c)
	}
	return out
}
