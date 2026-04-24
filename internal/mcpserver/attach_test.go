// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"strings"
	"testing"
)

func TestResolveDBSpecs_FilenameAlias(t *testing.T) {
	got, err := ResolveDBSpecs([]string{"chicago.duckdb"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 1 || got[0].Alias != "chicago" || got[0].Path != "chicago.duckdb" {
		t.Errorf("got %+v, want one DBSpec{Alias=chicago, Path=chicago.duckdb}", got)
	}
}

func TestResolveDBSpecs_DotsBecomeUnderscores(t *testing.T) {
	got, err := ResolveDBSpecs([]string{"data.cityofchicago.org.duckdb"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got[0].Alias != "data_cityofchicago_org" {
		t.Errorf("alias: got %q, want data_cityofchicago_org", got[0].Alias)
	}
}

func TestResolveDBSpecs_DirectoryStripped(t *testing.T) {
	got, err := ResolveDBSpecs([]string{"/some/path/nyc.duckdb"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got[0].Alias != "nyc" {
		t.Errorf("alias: got %q, want nyc", got[0].Alias)
	}
}

func TestResolveDBSpecs_ExplicitAlias(t *testing.T) {
	got, err := ResolveDBSpecs([]string{"foo=/some/path/whatever.duckdb"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got[0].Alias != "foo" || got[0].Path != "/some/path/whatever.duckdb" {
		t.Errorf("got %+v", got)
	}
}

func TestResolveDBSpecs_CollisionError(t *testing.T) {
	_, err := ResolveDBSpecs([]string{"a/data.duckdb", "b/data.duckdb"})
	if err == nil || !strings.Contains(err.Error(), "alias collision") {
		t.Errorf("want alias collision error, got %v", err)
	}
}

func TestResolveDBSpecs_InvalidAlias(t *testing.T) {
	cases := []string{
		"1bad=foo.duckdb",     // starts with digit
		"has-dash=foo.duckdb", // contains dash
		"=foo.duckdb",         // empty alias
		"a.b=foo.duckdb",      // contains dot
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			_, err := ResolveDBSpecs([]string{c})
			if err == nil {
				t.Errorf("want error for %q", c)
			}
		})
	}
}

func TestResolveDBSpecs_FilenameAliasInvalid(t *testing.T) {
	// Filename-derived alias starts with a digit; must error rather than silently rename.
	_, err := ResolveDBSpecs([]string{"311data.duckdb"})
	if err == nil {
		t.Errorf("want error for filename whose derived alias starts with a digit")
	}
}

func TestResolveDBSpecs_Empty(t *testing.T) {
	_, err := ResolveDBSpecs(nil)
	if err == nil || !strings.Contains(err.Error(), "at least one --db") {
		t.Errorf("want require-one error, got %v", err)
	}
}
