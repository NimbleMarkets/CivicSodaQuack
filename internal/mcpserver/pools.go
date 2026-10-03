// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/neomantra/CivicSodaQuack/internal/duckdb"

	_ "github.com/duckdb/duckdb-go/v2"
)

// PortalPools holds the *sql.DB handle for one portal file.
// Single pool per portal: DuckDB's per-process instance cache rejects opening
// the same file twice with different access_mode settings, and we don't need
// per-portal read-only enforcement — the per-portal pools only run hardcoded
// internal queries (list/describe/search). User SQL goes through Pools.Host
// where BEGIN TRANSACTION READ ONLY enforces the read-only contract.
type PortalPools struct {
	Path string
	DB   *sql.DB
}

// Pools owns the in-memory host DB plus per-portal pools. The host has each
// portal ATTACHed read-only as <alias>; tools issue queries through Pools.Host
// so cross-portal queries via "<alias>.<schema>.<table>" syntax work.
type Pools struct {
	Host    *sql.DB
	Portals map[string]*PortalPools
}

// OpenPools opens the host and per-portal pools, ATTACHes each portal to the
// host, and verifies each file is a CivicSodaQuack DuckDB.
func OpenPools(specs []DBSpec) (*Pools, error) {
	host, err := openDB(":memory:")
	if err != nil {
		return nil, fmt.Errorf("open host: %w", err)
	}

	p := &Pools{
		Host:    host,
		Portals: make(map[string]*PortalPools, len(specs)),
	}

	for _, spec := range specs {
		if _, err := os.Stat(spec.Path); err != nil {
			p.Close()
			return nil, fmt.Errorf("--db %s: %w", spec.Path, err)
		}
		db, err := openDB(spec.Path)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("open %s: %w", spec.Path, err)
		}
		if err := duckdb.AssertCSQ(db, spec.Path); err != nil {
			db.Close()
			p.Close()
			return nil, err
		}
		// ATTACH read-only to the host
		_, err = host.Exec(fmt.Sprintf(`ATTACH '%s' AS %s (READ_ONLY)`,
			escapeSQLString(spec.Path), spec.Alias))
		if err != nil {
			db.Close()
			p.Close()
			return nil, fmt.Errorf("attach %s as %s: %w", spec.Path, spec.Alias, err)
		}
		p.Portals[spec.Alias] = &PortalPools{Path: spec.Path, DB: db}
	}

	// User SQL runs inside BEGIN TRANSACTION READ ONLY, which stops writes but
	// not read_csv('/any/path') or httpfs reads. Latch external access off now
	// that every ATTACH is done; DuckDB does not allow turning it back on, so
	// a host that needs a new attach must be rebuilt. The setting is global to
	// the host instance, so it covers every pooled connection.
	if _, err := host.Exec(`SET enable_external_access = false`); err != nil {
		p.Close()
		return nil, fmt.Errorf("disable external access on host: %w", err)
	}
	return p, nil
}

// Close closes every pool. Safe to call multiple times.
func (p *Pools) Close() error {
	var firstErr error
	for _, pp := range p.Portals {
		if pp.DB != nil {
			if err := pp.DB.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if p.Host != nil {
		if err := p.Host.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// openDB opens a *sql.DB for the given path with read-write access.
// Phase 3 uses a single writeable pool per portal; engine-level read-only
// enforcement for user SQL happens at the host via BEGIN TRANSACTION READ ONLY
// (see QuerySQL).
func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// escapeSQLString escapes single quotes so the path can be safely embedded in
// a single-quoted DuckDB string literal.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
