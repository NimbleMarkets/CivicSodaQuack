// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	_ "github.com/duckdb/duckdb-go/v2"
)

// ProducerOptions configures Pack.
type ProducerOptions struct {
	DBPath      string // source DuckDB; required
	OutputPath  string // destination tarball (.tar.zst); required
	Portal      string // overrides portal name in manifest; "" derives from filename
	KeepStaging bool   // skip _csq_staging cleanup
	Force       bool   // overwrite existing OutputPath
	CSQVersion  string // injected by CLI; "" → "0.4.0-dev"
}

// Pack copies the source DuckDB to a temp file, optionally cleans
// _csq_staging, computes counts and SHA-256, then streams a tar+zst archive
// to OutputPath with manifest.json followed by the DuckDB.
//
// Returns the populated manifest. On error, any partial OutputPath is removed.
func Pack(ctx context.Context, opts ProducerOptions) (*Manifest, error) {
	if _, err := os.Stat(opts.DBPath); err != nil {
		return nil, fmt.Errorf("snapshot: --db %s: %w", opts.DBPath, err)
	}
	if !opts.Force {
		if _, err := os.Stat(opts.OutputPath); err == nil {
			return nil, fmt.Errorf("snapshot: %s exists; pass --force to overwrite", opts.OutputPath)
		}
	}

	csqVersion := opts.CSQVersion
	if csqVersion == "" {
		csqVersion = "0.4.0-dev"
	}
	portal := opts.Portal
	if portal == "" {
		portal = portalFromPath(opts.DBPath)
	}

	// Temp file in the same directory as OutputPath so a future rename is atomic.
	tmpDir := filepath.Dir(opts.OutputPath)
	tmp, err := os.CreateTemp(tmpDir, "csq-snapshot-*.duckdb")
	if err != nil {
		return nil, fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)

	if err := copyFile(opts.DBPath, tmpPath); err != nil {
		return nil, fmt.Errorf("copy db: %w", err)
	}

	// Open the temp DB for assertion + optional staging cleanup.
	tmpDB, err := sql.Open("duckdb", tmpPath)
	if err != nil {
		return nil, fmt.Errorf("open temp db: %w", err)
	}
	if err := assertIsCSQDB(tmpDB, opts.DBPath); err != nil {
		tmpDB.Close()
		return nil, err
	}
	if !opts.KeepStaging {
		if _, err := tmpDB.ExecContext(ctx, `DROP SCHEMA IF EXISTS _csq_staging CASCADE`); err != nil {
			tmpDB.Close()
			return nil, fmt.Errorf("drop staging: %w", err)
		}
		if _, err := tmpDB.ExecContext(ctx, `CREATE SCHEMA _csq_staging`); err != nil {
			tmpDB.Close()
			return nil, fmt.Errorf("recreate staging schema: %w", err)
		}
	}
	dsCount, err := countDatasets(tmpDB)
	if err != nil {
		tmpDB.Close()
		return nil, err
	}
	rowCount, err := countTotalRows(tmpDB)
	if err != nil {
		tmpDB.Close()
		return nil, err
	}
	if err := tmpDB.Close(); err != nil {
		return nil, fmt.Errorf("close temp db: %w", err)
	}

	// Hash + size of the temp DB bytes.
	sum, size, err := sha256AndSize(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("hash temp db: %w", err)
	}

	manifest := &Manifest{
		SchemaVersion:   SchemaVersion,
		Portal:          portal,
		CSQVersion:      csqVersion,
		SnapshotID:      newSnapshotID(),
		CreatedAt:       time.Now().UTC(),
		DuckDBFilename:  filepath.Base(opts.DBPath),
		DuckDBSHA256:    sum,
		DuckDBSizeBytes: size,
		DatasetCount:    dsCount,
		TotalRowCount:   rowCount,
	}

	if err := writeTarball(opts.OutputPath, manifest, tmpPath); err != nil {
		_ = os.Remove(opts.OutputPath)
		return nil, err
	}
	return manifest, nil
}

// portalFromPath strips directory and .duckdb suffix, then replaces dots with underscores.
func portalFromPath(p string) string {
	base := filepath.Base(p)
	base = strings.TrimSuffix(base, ".duckdb")
	return strings.ReplaceAll(base, ".", "_")
}

// copyFile streams src to dst, replacing dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// sha256AndSize streams the file once, returning hex-encoded SHA-256 and byte size.
func sha256AndSize(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// writeTarball opens outputPath (truncating) and streams manifest + DuckDB.
func writeTarball(outputPath string, manifest *Manifest, dbPath string) error {
	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	w := newTarZstWriter(out)

	mb, err := manifest.MarshalIndent()
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := w.WriteEntry("manifest.json", int64(len(mb)), manifest.CreatedAt, strings.NewReader(string(mb))); err != nil {
		return err
	}

	dbF, err := os.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open temp for tar: %w", err)
	}
	defer dbF.Close()
	if err := w.WriteEntry(manifest.DuckDBFilename, manifest.DuckDBSizeBytes, manifest.CreatedAt, dbF); err != nil {
		return err
	}
	return w.Close()
}

// newSnapshotID returns a fresh ULID string.
func newSnapshotID() string {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
}
