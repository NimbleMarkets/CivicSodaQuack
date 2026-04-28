// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// readTarball opens a .tar.zst, returns the manifest plus a map of remaining
// entry name → body bytes (for assertions).
func readTarball(t *testing.T, path string) (*Manifest, map[string][]byte) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open tarball: %v", err)
	}
	defer f.Close()
	r, err := newTarZstReader(f)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer r.Close()

	hdr, body, err := r.Next()
	if err != nil {
		t.Fatalf("first entry: %v", err)
	}
	if hdr.Name != "manifest.json" {
		t.Fatalf("first entry name: %q", hdr.Name)
	}
	mb, _ := io.ReadAll(body)
	m, err := ParseManifest(mb)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	rest := map[string][]byte{}
	for {
		hdr, body, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		b, _ := io.ReadAll(body)
		rest[hdr.Name] = b
	}
	return m, rest
}

func TestPack_HappyPath(t *testing.T) {
	hwm := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	srcPath := seedFixtureDB(t, dir, "data.cityofchicago.org.duckdb",
		FixtureDataset{
			ID: "aaaa-0001", Name: "Crimes", TableName: "a",
			ColumnDefs: []string{"v INT"},
			Rows:       []map[string]any{{"v": 1}, {"v": 2}},
			Synced:     true, HWM: hwm,
		})
	outPath := filepath.Join(dir, "snap.tar.zst")

	m, err := Pack(context.Background(), ProducerOptions{
		DBPath: srcPath, OutputPath: outPath,
	})
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if m.SchemaVersion != 1 || m.Portal != "data_cityofchicago_org" {
		t.Errorf("manifest: %+v", m)
	}
	if m.DuckDBFilename != "data.cityofchicago.org.duckdb" {
		t.Errorf("filename: %q", m.DuckDBFilename)
	}
	if m.DatasetCount != 1 || m.TotalRowCount != 2 {
		t.Errorf("counts: ds=%d rows=%d", m.DatasetCount, m.TotalRowCount)
	}
	if m.SnapshotID == "" || m.CreatedAt.IsZero() {
		t.Errorf("snapshot_id or created_at empty: %+v", m)
	}
	if m.DuckDBSizeBytes <= 0 {
		t.Errorf("size: %d", m.DuckDBSizeBytes)
	}
	if len(m.DuckDBSHA256) != 64 {
		t.Errorf("sha256 length: %d", len(m.DuckDBSHA256))
	}

	// Inspect tarball: manifest first (already verified by readTarball), DuckDB second.
	mFromFile, rest := readTarball(t, outPath)
	if mFromFile.SnapshotID != m.SnapshotID {
		t.Errorf("manifest id mismatch: %q vs %q", mFromFile.SnapshotID, m.SnapshotID)
	}
	body, ok := rest[m.DuckDBFilename]
	if !ok {
		t.Fatalf("payload entry %q missing; have %v", m.DuckDBFilename, keys(rest))
	}
	if int64(len(body)) != m.DuckDBSizeBytes {
		t.Errorf("body size %d vs manifest %d", len(body), m.DuckDBSizeBytes)
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != m.DuckDBSHA256 {
		t.Errorf("sha256 mismatch")
	}
}

func TestPack_PortalOverride(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "anything.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "snap.tar.zst")
	m, err := Pack(context.Background(), ProducerOptions{
		DBPath: src, OutputPath: out, Portal: "custom-portal-name",
	})
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if m.Portal != "custom-portal-name" {
		t.Errorf("portal: %q", m.Portal)
	}
}

func TestPack_DropsStaging(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "test.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	// Inject a stray staging table.
	{
		db, _ := sql.Open("duckdb", src)
		if _, err := db.Exec(`CREATE TABLE _csq_staging.leftover (x INT)`); err != nil {
			t.Fatalf("seed leftover: %v", err)
		}
		db.Close()
	}
	out := filepath.Join(dir, "snap.tar.zst")
	if _, err := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out}); err != nil {
		t.Fatalf("pack: %v", err)
	}
	// Unpack and verify _csq_staging is empty in the result.
	_, rest := readTarball(t, out)
	tmp := filepath.Join(dir, "restored.duckdb")
	if err := os.WriteFile(tmp, rest["test.duckdb"], 0o644); err != nil {
		t.Fatalf("write restored: %v", err)
	}
	db, _ := sql.Open("duckdb", tmp)
	defer db.Close()
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '_csq_staging'`,
	).Scan(&n); err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 0 {
		t.Errorf("staging not cleaned: %d tables remain", n)
	}
}

func TestPack_KeepStaging(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "test.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	{
		db, _ := sql.Open("duckdb", src)
		if _, err := db.Exec(`CREATE TABLE _csq_staging.leftover (x INT)`); err != nil {
			t.Fatalf("seed leftover: %v", err)
		}
		db.Close()
	}
	out := filepath.Join(dir, "snap.tar.zst")
	if _, err := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out, KeepStaging: true}); err != nil {
		t.Fatalf("pack: %v", err)
	}
	_, rest := readTarball(t, out)
	tmp := filepath.Join(dir, "restored.duckdb")
	_ = os.WriteFile(tmp, rest["test.duckdb"], 0o644)
	db, _ := sql.Open("duckdb", tmp)
	defer db.Close()
	var n int
	_ = db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema = '_csq_staging' AND table_name = 'leftover'`,
	).Scan(&n)
	if n != 1 {
		t.Errorf("staging cleaned despite KeepStaging=true; got %d", n)
	}
}

func TestPack_NotCSQDB(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.duckdb")
	db, _ := sql.Open("duckdb", bad)
	_, _ = db.Exec(`CREATE TABLE foo (x INT)`)
	db.Close()
	out := filepath.Join(dir, "snap.tar.zst")
	_, err := Pack(context.Background(), ProducerOptions{DBPath: bad, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "not a CivicSodaQuack DuckDB") {
		t.Errorf("got %v", err)
	}
	if _, statErr := os.Stat(out); statErr == nil {
		t.Errorf("output file should not exist on failure")
	}
}

func TestPack_OutputExists_NoForce(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "src.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "snap.tar.zst")
	if err := os.WriteFile(out, []byte("preexisting"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "exists") {
		t.Errorf("want exists error, got %v", err)
	}
	// Existing content should be untouched.
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, []byte("preexisting")) {
		t.Errorf("existing file overwritten without --force")
	}
}

func TestPack_OutputExists_Force(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "src.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "snap.tar.zst")
	_ = os.WriteFile(out, []byte("preexisting"), 0o644)
	_, err := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out, Force: true})
	if err != nil {
		t.Fatalf("pack with force: %v", err)
	}
	got, _ := os.ReadFile(out)
	if bytes.Equal(got, []byte("preexisting")) {
		t.Errorf("expected overwrite")
	}
}

func TestPack_DBMissing(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "snap.tar.zst")
	_, err := Pack(context.Background(), ProducerOptions{DBPath: "/nonexistent/x.duckdb", OutputPath: out})
	if err == nil {
		t.Fatal("want error for missing db")
	}
}

func TestPack_CSQVersionDefault(t *testing.T) {
	dir := t.TempDir()
	src := seedFixtureDB(t, dir, "src.duckdb",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "snap.tar.zst")
	m, _ := Pack(context.Background(), ProducerOptions{DBPath: src, OutputPath: out})
	if m.CSQVersion != "0.4.0-dev" {
		t.Errorf("default CSQVersion: got %q, want 0.4.0-dev", m.CSQVersion)
	}
}

func keys(m map[string][]byte) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	return out
}
