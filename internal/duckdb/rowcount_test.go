// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"context"
	"testing"
)

func TestCountMainTables(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, q := range []string{
		`CREATE TABLE main.a (v INT)`,
		`INSERT INTO main.a SELECT * FROM range(7)`,
		`CREATE TABLE main."we""ird" (v INT)`,
	} {
		if _, err := w.DB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	got, err := CountMainTables(context.Background(), w.DB, []string{"a", `we"ird`, "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if got["a"] != 7 || got[`we"ird`] != 0 {
		t.Errorf("counts: %v", got)
	}
	if _, ok := got["missing"]; ok {
		t.Errorf("missing table should be absent: %v", got)
	}
}
