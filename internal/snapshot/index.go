// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"
)

// IndexSchemaVersion is the current snapshot index format version.
const IndexSchemaVersion = 1

// Index is the per-portal `index.json` listing available snapshots, sorted
// newest-first by snapshot_id (ULIDs sort chronologically).
type Index struct {
	SchemaVersion int          `json:"schema_version"`
	Portal        string       `json:"portal"`
	Snapshots     []IndexEntry `json:"snapshots"`
}

// IndexEntry is one snapshot listed in an Index. The SHA256 here mirrors the
// authoritative value in the snapshot's manifest; consumers verify against the
// manifest, not the entry.
type IndexEntry struct {
	SnapshotID string    `json:"snapshot_id"`
	CreatedAt  time.Time `json:"created_at"`
	URL        string    `json:"url"`
	SizeBytes  int64     `json:"size_bytes"`
	SHA256     string    `json:"sha256"`
}

// LoadIndex parses an Index from JSON bytes.
func LoadIndex(r io.Reader) (*Index, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}
	var idx Index
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, fmt.Errorf("parse index: %w", err)
	}
	return &idx, nil
}

// MarshalIndent renders the Index as 2-space-indented JSON.
func (i *Index) MarshalIndent() ([]byte, error) {
	return json.MarshalIndent(i, "", "  ")
}

// Add prepends entry e and re-sorts newest-first by SnapshotID. When maxKeep > 0,
// truncates to that many entries.
func (i *Index) Add(e IndexEntry, maxKeep int) {
	i.Snapshots = append([]IndexEntry{e}, i.Snapshots...)
	sort.SliceStable(i.Snapshots, func(a, b int) bool {
		return i.Snapshots[a].SnapshotID > i.Snapshots[b].SnapshotID
	})
	if maxKeep > 0 && len(i.Snapshots) > maxKeep {
		i.Snapshots = i.Snapshots[:maxKeep]
	}
}

// Latest returns the newest snapshot entry (Snapshots[0]). Returns ok=false
// when the index is empty.
func (i *Index) Latest() (IndexEntry, bool) {
	if len(i.Snapshots) == 0 {
		return IndexEntry{}, false
	}
	return i.Snapshots[0], true
}

// FindByID returns the entry whose SnapshotID matches id.
func (i *Index) FindByID(id string) (IndexEntry, bool) {
	for _, e := range i.Snapshots {
		if e.SnapshotID == id {
			return e, true
		}
	}
	return IndexEntry{}, false
}

// ValidateIndex sanity-checks an Index. Returns nil when fields are well-formed.
func ValidateIndex(i *Index) error {
	if i.SchemaVersion != IndexSchemaVersion {
		return fmt.Errorf("unsupported schema_version %d (want %d)", i.SchemaVersion, IndexSchemaVersion)
	}
	if i.Portal == "" {
		return fmt.Errorf("portal must not be empty")
	}
	for n, e := range i.Snapshots {
		if e.SnapshotID == "" {
			return fmt.Errorf("snapshots[%d]: snapshot_id empty", n)
		}
		if e.URL == "" {
			return fmt.Errorf("snapshots[%d] (%s): url empty", n, e.SnapshotID)
		}
		if e.SHA256 == "" {
			return fmt.Errorf("snapshots[%d] (%s): sha256 empty", n, e.SnapshotID)
		}
		if e.SizeBytes <= 0 {
			return fmt.Errorf("snapshots[%d] (%s): size_bytes must be > 0", n, e.SnapshotID)
		}
		if e.CreatedAt.IsZero() {
			return fmt.Errorf("snapshots[%d] (%s): created_at empty/zero", n, e.SnapshotID)
		}
	}
	return nil
}
