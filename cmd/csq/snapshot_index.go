// Copyright (c) 2026 Neomantra Corp

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/snapshot"
)

func runSnapshotIndex(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("snapshot-index: missing subcommand (update | validate)")
	}
	switch args[0] {
	case "update":
		return runSnapshotIndexUpdate(args[1:])
	case "validate":
		return runSnapshotIndexValidate(args[1:])
	default:
		return fmt.Errorf("snapshot-index: unknown subcommand %q (want update | validate)", args[0])
	}
}

func runSnapshotIndexUpdate(args []string) error {
	fs := flag.NewFlagSet("snapshot-index update", flag.ContinueOnError)
	var (
		indexPath string
		addPath   string
		url       string
		maxKeep   int
	)
	fs.StringVar(&indexPath, "index", "", "Path to index.json (created if absent)")
	fs.StringVar(&addPath, "add", "", "Path to a snapshot tarball to add")
	fs.StringVar(&url, "url", "", "Public URL of the snapshot (required)")
	fs.IntVar(&maxKeep, "max-keep", 0, "Trim to this many entries (0 = unbounded)")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if indexPath == "" {
		return fmt.Errorf("--index is required")
	}
	if addPath == "" {
		return fmt.Errorf("--add is required")
	}
	if url == "" {
		return fmt.Errorf("--url is required")
	}

	manifest, err := readManifestFromTarball(addPath)
	if err != nil {
		return err
	}

	idx, err := loadOrInitIndex(indexPath, manifest.Portal)
	if err != nil {
		return err
	}
	if idx.Portal != manifest.Portal {
		return fmt.Errorf("index portal %q != tarball portal %q", idx.Portal, manifest.Portal)
	}

	idx.Add(snapshot.IndexEntry{
		SnapshotID: manifest.SnapshotID,
		CreatedAt:  manifest.CreatedAt,
		URL:        url,
		SizeBytes:  manifest.DuckDBSizeBytes,
		SHA256:     manifest.DuckDBSHA256,
	}, maxKeep)

	if err := writeIndexAtomic(indexPath, idx); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "[csq] updated %s: portal=%s entries=%d\n",
		indexPath, idx.Portal, len(idx.Snapshots))
	return nil
}

func runSnapshotIndexValidate(args []string) error {
	fs := flag.NewFlagSet("snapshot-index validate", flag.ContinueOnError)
	var indexPath string
	fs.StringVar(&indexPath, "index", "", "Path to index.json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if indexPath == "" {
		return fmt.Errorf("--index is required")
	}
	f, err := os.Open(indexPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", indexPath, err)
	}
	defer f.Close()
	idx, err := snapshot.LoadIndex(f)
	if err != nil {
		return err
	}
	if err := snapshot.ValidateIndex(idx); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "[csq] %s ok: portal=%s entries=%d\n",
		indexPath, idx.Portal, len(idx.Snapshots))
	return nil
}

// readManifestFromTarball opens a .tar.zst, reads only the first entry
// (manifest.json), and returns the parsed manifest.
func readManifestFromTarball(path string) (*snapshot.Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	mfst, err := snapshot.ReadManifest(f)
	if err != nil {
		return nil, fmt.Errorf("read manifest from %s: %w", path, err)
	}
	return mfst, nil
}

// loadOrInitIndex reads an existing index file or returns a fresh Index when
// the file doesn't exist. portal is used to populate the new Index.
func loadOrInitIndex(path, portal string) (*snapshot.Index, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &snapshot.Index{SchemaVersion: snapshot.IndexSchemaVersion, Portal: portal}, nil
		}
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return snapshot.LoadIndex(f)
}

// writeIndexAtomic writes idx to path via a temp file + rename.
func writeIndexAtomic(path string, idx *snapshot.Index) error {
	body, err := idx.MarshalIndent()
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".csq-index-*.json")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := io.Copy(tmp, bytes.NewReader(body)); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename to %s: %w", path, err)
	}
	return nil
}
