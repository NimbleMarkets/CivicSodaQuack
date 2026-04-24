// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

// DBSpec is a resolved (alias, path) pair for one portal DuckDB file.
type DBSpec struct {
	Alias string // SQL identifier; ATTACH alias and per-portal namespace
	Path  string // filesystem path to the .duckdb file
}

var aliasRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ResolveDBSpecs converts raw --db arg strings into validated DBSpec records.
// Each arg is either a plain path (alias derived from basename) or alias=path.
// Returns an error on empty input, invalid aliases, or alias collisions.
func ResolveDBSpecs(args []string) ([]DBSpec, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("at least one --db is required")
	}
	out := make([]DBSpec, 0, len(args))
	seen := map[string]string{} // alias → path that already used it

	for _, raw := range args {
		spec, err := parseDBArg(raw)
		if err != nil {
			return nil, err
		}
		if prev, ok := seen[spec.Alias]; ok {
			return nil, fmt.Errorf("alias collision: alias %q used by both %q and %q (pass alias=path to disambiguate)",
				spec.Alias, prev, spec.Path)
		}
		seen[spec.Alias] = spec.Path
		out = append(out, spec)
	}
	return out, nil
}

func parseDBArg(raw string) (DBSpec, error) {
	if i := strings.IndexByte(raw, '='); i >= 0 {
		alias := raw[:i]
		path := raw[i+1:]
		if !aliasRE.MatchString(alias) {
			return DBSpec{}, fmt.Errorf("invalid alias %q in --db %q (must match [a-zA-Z_][a-zA-Z0-9_]*)", alias, raw)
		}
		if path == "" {
			return DBSpec{}, fmt.Errorf("--db %q has empty path", raw)
		}
		return DBSpec{Alias: alias, Path: path}, nil
	}
	alias := aliasFromPath(raw)
	if !aliasRE.MatchString(alias) {
		return DBSpec{}, fmt.Errorf("filename-derived alias %q from %q is not a valid SQL identifier (use alias=path)", alias, raw)
	}
	return DBSpec{Alias: alias, Path: raw}, nil
}

// aliasFromPath strips the directory and the .duckdb extension, then replaces
// any dots in the remainder with underscores.
func aliasFromPath(p string) string {
	base := filepath.Base(p)
	base = strings.TrimSuffix(base, ".duckdb")
	return strings.ReplaceAll(base, ".", "_")
}
