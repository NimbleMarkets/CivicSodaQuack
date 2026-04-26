// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestQuerySQL_HappySelect(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{
			ID: "aaaa-0001", Name: "Crimes",
			TableName: "crimes",
			ColumnDefs: []string{"socrata_id VARCHAR", "score DOUBLE"},
			Rows: []map[string]any{
				{"socrata_id": "a", "score": 1.0},
				{"socrata_id": "b", "score": 2.0},
			},
		})
	defer cleanup()

	got, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `SELECT socrata_id, score FROM test.main.crimes ORDER BY socrata_id`}, time.Second)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got.RowCount != 2 || got.Truncated {
		t.Errorf("rowcount=%d truncated=%v", got.RowCount, got.Truncated)
	}
	if len(got.Columns) != 2 || got.Columns[0] != "socrata_id" {
		t.Errorf("columns: %v", got.Columns)
	}
	if len(got.Rows) != 2 {
		t.Errorf("rows: got %d", len(got.Rows))
	}
}

func TestQuerySQL_RejectsWrites(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	defer cleanup()

	_, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `CREATE TABLE main.evil (x INT)`}, time.Second)
	if err == nil {
		t.Fatal("CREATE TABLE should be rejected")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "read") {
		t.Errorf("error should mention read-only: %v", err)
	}
}

func TestQuerySQL_TruncatesByRowCap(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	defer cleanup()

	// Use a generate_series query to make 2000 synthetic rows
	got, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `SELECT * FROM range(0, 2000)`}, 5*time.Second)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !got.Truncated {
		t.Errorf("expected truncated=true at 1000-row cap")
	}
	if got.RowCount != 1000 {
		t.Errorf("rowcount: got %d, want 1000", got.RowCount)
	}
	if got.Note == "" {
		t.Errorf("expected a note explaining truncation")
	}
}

func TestQuerySQL_Timeout(t *testing.T) {
	pools, cleanup := openFixturePools(t,
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	defer cleanup()

	// 1ms timeout against a query that takes longer than that to return any rows
	_, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `SELECT * FROM range(0, 100000000)`}, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Errorf("want timeout error, got %v", err)
	}
}

func TestQuerySQL_CrossPortal(t *testing.T) {
	dir := t.TempDir()
	a := seedFixtureDB(t, dir, "a.duckdb",
		FixtureDataset{
			ID: "aaaa-0001", Name: "Aw",
			TableName: "items", ColumnDefs: []string{"id VARCHAR"},
			Rows: []map[string]any{{"id": "x"}},
		})
	b := seedFixtureDB(t, dir, "b.duckdb",
		FixtureDataset{
			ID: "bbbb-0001", Name: "Bw",
			TableName: "items", ColumnDefs: []string{"id VARCHAR"},
			Rows: []map[string]any{{"id": "x"}},
		})
	pools, err := OpenPools([]DBSpec{{Alias: "a", Path: a}, {Alias: "b", Path: b}})
	if err != nil {
		t.Fatalf("OpenPools: %v", err)
	}
	defer pools.Close()

	got, err := querySQLHandler(context.Background(), pools,
		QuerySQLArgs{SQL: `SELECT a.id FROM a.main.items a JOIN b.main.items b ON a.id = b.id`}, time.Second)
	if err != nil {
		t.Fatalf("cross-portal: %v", err)
	}
	if got.RowCount != 1 {
		t.Errorf("want 1 row, got %d", got.RowCount)
	}
}
