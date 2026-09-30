// Copyright (c) 2026 Neomantra Corp

// Package data puts csq's attached portal databases behind one small
// interface for the agent's tools. It wraps the same handlers the MCP server
// serves, so the chat and the MCP surface cannot drift.
package data

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/neomantra/CivicSodaQuack/chat/internal/present"
	"github.com/neomantra/CivicSodaQuack/internal/mcpserver"
)

// DefaultQueryTimeout bounds one SQL statement, matching the MCP server.
const DefaultQueryTimeout = 30 * time.Second

// Store is the agent's view of the attached portals.
type Store interface {
	// Portals lists attached aliases in sorted order.
	Portals() []string
	ListDatasets(ctx context.Context, portal, category string) ([]mcpserver.DatasetSummary, error)
	SearchDatasets(ctx context.Context, portal, query string) ([]mcpserver.DatasetSummary, error)
	DescribeDataset(ctx context.Context, id, portal string) (mcpserver.DatasetDetail, error)
	// Query runs read-only SQL and returns typed cells.
	Query(ctx context.Context, sql string) (mcpserver.QuerySQLResult, error)
	// QueryTable runs read-only SQL and returns cells as display strings.
	QueryTable(ctx context.Context, sql string) (present.Table, error)
	Close() error
}

type store struct {
	pools   *mcpserver.Pools
	timeout time.Duration
}

// Open attaches the databases named by --db style arguments ("path" or
// "alias=path"). It refuses files that are not csq databases.
func Open(dbArgs []string) (Store, error) {
	specs, err := mcpserver.ResolveDBSpecs(dbArgs)
	if err != nil {
		return nil, err
	}
	pools, err := mcpserver.OpenPools(specs)
	if err != nil {
		return nil, err
	}
	return &store{pools: pools, timeout: DefaultQueryTimeout}, nil
}

func (s *store) Close() error { return s.pools.Close() }

func (s *store) Portals() []string {
	out := make([]string, 0, len(s.pools.Portals))
	for alias := range s.pools.Portals {
		out = append(out, alias)
	}
	sort.Strings(out)
	return out
}

func (s *store) ListDatasets(ctx context.Context, portal, category string) ([]mcpserver.DatasetSummary, error) {
	return mcpserver.ListDatasets(ctx, s.pools, mcpserver.ListDatasetsArgs{Portal: portal, Category: category})
}

func (s *store) SearchDatasets(ctx context.Context, portal, query string) ([]mcpserver.DatasetSummary, error) {
	return mcpserver.SearchDatasets(ctx, s.pools, mcpserver.SearchDatasetsArgs{Portal: portal, Query: query})
}

func (s *store) DescribeDataset(ctx context.Context, id, portal string) (mcpserver.DatasetDetail, error) {
	return mcpserver.DescribeDataset(ctx, s.pools, mcpserver.DescribeDatasetArgs{DatasetID: id, Portal: portal})
}

func (s *store) Query(ctx context.Context, sql string) (mcpserver.QuerySQLResult, error) {
	return mcpserver.QuerySQL(ctx, s.pools, mcpserver.QuerySQLArgs{SQL: sql}, s.timeout)
}

func (s *store) QueryTable(ctx context.Context, sql string) (present.Table, error) {
	res, err := s.Query(ctx, sql)
	if err != nil {
		return present.Table{}, err
	}
	return TableFromResult(res), nil
}

// TableFromResult renders a typed result as display strings.
func TableFromResult(res mcpserver.QuerySQLResult) present.Table {
	t := present.Table{
		Columns:   res.Columns,
		Rows:      make([][]string, 0, len(res.Rows)),
		Total:     res.RowCount,
		Truncated: res.Truncated,
	}
	for _, row := range res.Rows {
		cells := make([]string, len(row))
		for i, v := range row {
			cells[i] = FormatCell(v)
		}
		t.Rows = append(t.Rows, cells)
	}
	return t
}

// FormatCell renders one DuckDB value for a person to read. Nulls are empty,
// dates without a time of day drop it, and floats keep only the digits they
// need.
func FormatCell(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case bool:
		return strconv.FormatBool(x)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case time.Time:
		if x.Hour() == 0 && x.Minute() == 0 && x.Second() == 0 && x.Nanosecond() == 0 {
			return x.Format("2006-01-02")
		}
		return x.Format("2006-01-02 15:04:05")
	case map[string]any, []any:
		// DuckDB hands JSON columns back as native values.
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprint(x)
		}
		return string(b)
	default:
		return fmt.Sprint(x)
	}
}
