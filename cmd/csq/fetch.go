// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/snapshot"
)

func runFetch(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	var (
		from       string
		indexURL   string
		snapshotID string
		outputPath string
		noVerify   bool
		force      bool
	)
	fs.StringVar(&from, "from", "", "URL of a snapshot tarball (http://, https://, or file://)")
	fs.StringVar(&indexURL, "index", "", "URL of a snapshot index.json (mutually exclusive with --from)")
	fs.StringVar(&snapshotID, "snapshot", "", "Pin a specific snapshot_id from --index (default: latest)")
	fs.StringVar(&outputPath, "output", "", "Destination DuckDB path (default: manifest's duckdb_filename in the current directory)")
	fs.BoolVar(&noVerify, "no-verify", false, "Skip SHA-256 verification")
	fs.BoolVar(&force, "force", false, "Overwrite --output if it exists")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if from == "" && indexURL == "" {
		return fmt.Errorf("--from or --index is required")
	}
	if from != "" && indexURL != "" {
		return fmt.Errorf("--from and --index are mutually exclusive")
	}

	ctx := context.Background()
	resolvedURL := from
	if indexURL != "" {
		entry, err := resolveIndex(ctx, indexURL, snapshotID)
		if err != nil {
			return err
		}
		resolvedURL = entry.URL
		fmt.Fprintf(os.Stderr, "[csq] index %s → snapshot %s (%s)\n",
			indexURL, entry.SnapshotID, resolvedURL)
	}

	m, err := snapshot.Fetch(ctx, snapshot.ConsumerOptions{
		URL:        resolvedURL,
		OutputPath: outputPath,
		NoVerify:   noVerify,
		Force:      force,
	})
	if err != nil {
		return err
	}
	resolvedOut := outputPath
	if resolvedOut == "" {
		resolvedOut = m.DuckDBFilename
	}
	fmt.Fprintf(os.Stderr,
		"[csq] fetched %s: portal=%s snapshot=%s datasets=%d rows=%d → %s\n",
		resolvedURL, m.Portal, m.SnapshotID, m.DatasetCount, m.TotalRowCount, resolvedOut)
	return nil
}

// resolveIndex fetches the index at indexURL and selects the entry to download.
// When snapshotID is empty, returns the latest entry.
func resolveIndex(ctx context.Context, indexURL, snapshotID string) (snapshot.IndexEntry, error) {
	body, err := snapshot.OpenURL(ctx, indexURL)
	if err != nil {
		return snapshot.IndexEntry{}, err
	}
	defer body.Close()
	idx, err := snapshot.LoadIndex(body)
	if err != nil {
		return snapshot.IndexEntry{}, fmt.Errorf("index %s: %w", indexURL, err)
	}
	if snapshotID != "" {
		entry, ok := idx.FindByID(snapshotID)
		if !ok {
			return snapshot.IndexEntry{}, fmt.Errorf("snapshot %q not in index %s", snapshotID, indexURL)
		}
		return entry, nil
	}
	entry, ok := idx.Latest()
	if !ok {
		return snapshot.IndexEntry{}, fmt.Errorf("index %s: no snapshots available", indexURL)
	}
	return entry, nil
}
