// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeYAML(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	return path
}

const sampleYAML = `portal: data.example.org
db: ignored.duckdb
on_error: continue
defaults:
  batch_size: 5000
  order_by: ":id"
include:
  - id: aaaa-0001
`

func TestLoadConfigs_PairsAreEqual(t *testing.T) {
	dir := t.TempDir()
	a := writeYAML(t, dir, "a.yaml", sampleYAML)
	b := writeYAML(t, dir, "b.yaml", sampleYAML)
	got, err := LoadConfigs(
		[]DBSpec{{Alias: "x", Path: "/x.duckdb"}, {Alias: "y", Path: "/y.duckdb"}},
		[]string{a, b})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 2 || got["x"] == nil || got["y"] == nil {
		t.Errorf("got %v", got)
	}
}

func TestLoadConfigs_LengthMismatch(t *testing.T) {
	dir := t.TempDir()
	a := writeYAML(t, dir, "a.yaml", sampleYAML)
	_, err := LoadConfigs(
		[]DBSpec{{Alias: "x", Path: "/x.duckdb"}, {Alias: "y", Path: "/y.duckdb"}},
		[]string{a})
	if err == nil || !strings.Contains(err.Error(), "must be paired") {
		t.Errorf("want pairing error, got %v", err)
	}
}

func TestLoadConfigs_BadYAML(t *testing.T) {
	dir := t.TempDir()
	bad := writeYAML(t, dir, "bad.yaml", `not: : valid`)
	_, err := LoadConfigs(
		[]DBSpec{{Alias: "x", Path: "/x.duckdb"}},
		[]string{bad})
	if err == nil {
		t.Fatal("want error for bad yaml")
	}
}

func TestLoadConfigs_DBPathOverride(t *testing.T) {
	dir := t.TempDir()
	a := writeYAML(t, dir, "a.yaml", sampleYAML) // declares db: ignored.duckdb
	got, err := LoadConfigs(
		[]DBSpec{{Alias: "x", Path: "/actual/x.duckdb"}},
		[]string{a})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got["x"].DB != "/actual/x.duckdb" {
		t.Errorf("DB override: got %q, want /actual/x.duckdb", got["x"].DB)
	}
}

func TestLoadConfigs_EmptyConfigs(t *testing.T) {
	got, err := LoadConfigs(
		[]DBSpec{{Alias: "x", Path: "/x.duckdb"}},
		nil)
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("expected empty non-nil map, got %v", got)
	}
}
