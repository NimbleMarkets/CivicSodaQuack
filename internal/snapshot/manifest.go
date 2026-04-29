// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// SchemaVersion is the manifest schema this build emits and accepts.
// Bump on breaking format changes; consumers reject other values.
const SchemaVersion = 1

// Manifest is the JSON sidecar at the head of every Phase 4 snapshot tarball.
type Manifest struct {
	SchemaVersion   int       `json:"schema_version"`
	Portal          string    `json:"portal"`
	CSQVersion      string    `json:"csq_version"`
	SnapshotID      string    `json:"snapshot_id"`
	CreatedAt       time.Time `json:"created_at"`
	DuckDBFilename  string    `json:"duckdb_filename"`
	DuckDBSHA256    string    `json:"duckdb_sha256"`
	DuckDBSizeBytes int64     `json:"duckdb_size_bytes"`
	DatasetCount    int64     `json:"dataset_count"`
	TotalRowCount   int64     `json:"total_row_count"`
}

// MarshalIndent renders the manifest as 2-space-indented JSON suitable for the
// tarball's manifest.json entry.
func (m *Manifest) MarshalIndent() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// ParseManifest decodes a manifest from JSON bytes.
func ParseManifest(b []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	return &m, nil
}

// ReadManifest reads a snapshot tarball from r and returns the parsed manifest
// (the first entry). Useful for inspection without unpacking the DuckDB.
//
// Skips closing the tar/zstd readers — zstd's reader-ahead goroutine can block
// in some scenarios when not drained, and the caller is expected to close the
// underlying r (typically an *os.File) when done.
func ReadManifest(r io.Reader) (*Manifest, error) {
	tr, err := newTarZstReader(r)
	if err != nil {
		return nil, fmt.Errorf("read tarball: %w", err)
	}
	hdr, body, err := tr.Next()
	if err != nil {
		return nil, fmt.Errorf("read first entry: %w", err)
	}
	if hdr.Name != "manifest.json" {
		return nil, fmt.Errorf("first entry is %q, want manifest.json", hdr.Name)
	}
	mb, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read manifest bytes: %w", err)
	}
	return ParseManifest(mb)
}
