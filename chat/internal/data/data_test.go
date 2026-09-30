// Copyright (c) 2026 Neomantra Corp

package data

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestStore_DescribeAndQueryTable(t *testing.T) {
	st, err := Open([]string{"test=" + seedDB(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	if got := st.Portals(); len(got) != 1 || got[0] != "test" {
		t.Errorf("portals = %v", got)
	}

	d, err := st.DescribeDataset(ctx, "aaaa-0001", "")
	if err != nil {
		t.Fatal(err)
	}
	if d.TableName != "crimes" || len(d.Columns) != 4 || d.LastSync == nil {
		t.Errorf("detail = %+v", d)
	}

	tbl, err := st.QueryTable(ctx, `SELECT socrata_id, ward, amount, seen, note FROM test.main.crimes ORDER BY socrata_id`)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"a", "1", "1.5", "2026-01-02", ""},
		{"b", "2", "2", "2026-01-02 13:45:00", "x"},
	}
	if tbl.Total != 2 || tbl.Truncated {
		t.Errorf("total=%d truncated=%v", tbl.Total, tbl.Truncated)
	}
	for i, row := range want {
		if strings.Join(tbl.Rows[i], "|") != strings.Join(row, "|") {
			t.Errorf("row %d = %v, want %v", i, tbl.Rows[i], row)
		}
	}
}

func TestStore_SearchFindsByName(t *testing.T) {
	st, err := Open([]string{seedDB(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	got, err := st.SearchDatasets(context.Background(), "", "crime")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].DatasetID != "aaaa-0001" {
		t.Errorf("search = %+v", got)
	}
}

func TestFormatCell(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, ""},
		{"s", "s"},
		{int64(7), "7"},
		{3.0, "3"},
		{0.1, "0.1"},
		{true, "true"},
		{time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), "2026-01-02"},
		{map[string]any{"a": float64(1)}, `{"a":1}`},
	}
	for _, c := range cases {
		if got := FormatCell(c.in); got != c.want {
			t.Errorf("FormatCell(%#v) = %q, want %q", c.in, got, c.want)
		}
	}
}
