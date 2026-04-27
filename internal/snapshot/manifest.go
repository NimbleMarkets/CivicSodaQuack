// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"encoding/json"
	"fmt"
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
