// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// PortalPools holds the read-only and read-write *sql.DB handles for one portal file.
// Both point at the same file; writes are blocked at the Go driver layer on the RO pool.
type PortalPools struct {
	Path string
	RO   *sql.DB
	RW   *sql.DB
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
		// Open RW first to establish the DuckDB instance in the shared cache.
		rw, err := openDB(spec.Path)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("open rw %s: %w", spec.Path, err)
		}
		if err := assertIsCSQDB(rw, spec.Path); err != nil {
			rw.Close()
			p.Close()
			return nil, err
		}
		// RO pool shares the same DuckDB instance; writes are blocked at the Go layer.
		ro, err := openRODB(spec.Path)
		if err != nil {
			rw.Close()
			p.Close()
			return nil, fmt.Errorf("open ro %s: %w", spec.Path, err)
		}
		// ATTACH read-only to the host in-memory DB.
		_, err = host.Exec(fmt.Sprintf(`ATTACH '%s' AS %s (READ_ONLY)`,
			escapeSQLString(spec.Path), spec.Alias))
		if err != nil {
			ro.Close()
			rw.Close()
			p.Close()
			return nil, fmt.Errorf("attach %s as %s: %w", spec.Path, spec.Alias, err)
		}
		p.Portals[spec.Alias] = &PortalPools{Path: spec.Path, RO: ro, RW: rw}
	}
	return p, nil
}

// Close closes every pool. Safe to call multiple times.
func (p *Pools) Close() error {
	var firstErr error
	for _, pp := range p.Portals {
		if pp.RO != nil {
			if err := pp.RO.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if pp.RW != nil {
			if err := pp.RW.Close(); err != nil && firstErr == nil {
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

// openDB opens a *sql.DB for path (read-write). For in-memory databases
// (path == ":memory:") a plain in-memory instance is returned.
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

// openRODB opens a *sql.DB backed by the same DuckDB instance as the RW pool
// but with writes blocked at the Go driver layer. The returned *sql.DB shares
// the underlying DuckDB database opened via the global connector cache.
func openRODB(path string) (*sql.DB, error) {
	connector, err := duckdb.NewConnector(path, nil)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(&roConnector{inner: connector})
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// assertIsCSQDB verifies the file has _csq.catalog (the Phase 1 marker).
func assertIsCSQDB(db *sql.DB, path string) error {
	var n int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema = '_csq' AND table_name = 'catalog'`).Scan(&n)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", path, err)
	}
	if n == 0 {
		return fmt.Errorf("no _csq.catalog in %s; not a CivicSodaQuack DuckDB", path)
	}
	return nil
}

// escapeSQLString escapes single quotes so the path can be safely embedded in
// a single-quoted DuckDB string literal.
func escapeSQLString(s string) string {
	return replaceAll(s, "'", "''")
}

func replaceAll(s, old, new string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); {
		if i+len(old) <= len(s) && s[i:i+len(old)] == old {
			out = append(out, new...)
			i += len(old)
			continue
		}
		out = append(out, s[i])
		i++
	}
	return string(out)
}

// roConnector wraps a *duckdb.Connector and returns roConn instances that
// reject write operations at the driver level.
type roConnector struct {
	inner *duckdb.Connector
}

func (c *roConnector) Connect(ctx context.Context) (driver.Conn, error) {
	inner, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &roConn{inner: inner}, nil
}

func (c *roConnector) Driver() driver.Driver { return c.inner.Driver() }

// roConn wraps a driver.Conn and rejects ExecContext calls to prevent writes.
type roConn struct {
	inner driver.Conn
}

func (c *roConn) Prepare(query string) (driver.Stmt, error) { return c.inner.Prepare(query) }
func (c *roConn) Close() error                              { return c.inner.Close() }
func (c *roConn) Begin() (driver.Tx, error)                 { return c.inner.Begin() }

// ExecContext always returns an error to prevent mutations through the RO pool.
func (c *roConn) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return nil, fmt.Errorf("read-only pool: writes are not permitted")
}
