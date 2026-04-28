// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// packTo uses the producer to write a snapshot we then exercise.
func packTo(t *testing.T, dir, outName string, datasets ...FixtureDataset) (srcPath, outPath string) {
	t.Helper()
	srcPath = seedFixtureDB(t, dir, "src.duckdb", datasets...)
	outPath = filepath.Join(dir, outName)
	if _, err := Pack(context.Background(), ProducerOptions{DBPath: srcPath, OutputPath: outPath}); err != nil {
		t.Fatalf("pack: %v", err)
	}
	return
}

func TestFetch_FileURL_HappyPath(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})

	out := filepath.Join(dir, "restored.duckdb")
	m, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + tarPath, OutputPath: out,
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if m.DatasetCount != 1 {
		t.Errorf("manifest: %+v", m)
	}
	// Output should be a valid CSQ DuckDB.
	db, _ := sql.Open("duckdb", out)
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM _csq.catalog`).Scan(&n); err != nil {
		t.Fatalf("query restored: %v", err)
	}
	if n != 1 {
		t.Errorf("restored catalog rows: %d", n)
	}
}

func TestFetch_HTTPHappyPath(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, tarPath)
	}))
	defer srv.Close()

	out := filepath.Join(dir, "restored.duckdb")
	if _, err := Fetch(context.Background(), ConsumerOptions{
		URL: srv.URL, OutputPath: out,
	}); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Errorf("output missing: %v", err)
	}
}

func TestFetch_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "kaboom", 500)
	}))
	defer srv.Close()

	dir := t.TempDir()
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: srv.URL, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("want HTTP 500 in error, got %v", err)
	}
}

func TestFetch_UnsupportedScheme(t *testing.T) {
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "s3://bucket/foo", OutputPath: "/tmp/out.duckdb",
	})
	if err == nil || !strings.Contains(err.Error(), "scheme") {
		t.Errorf("want scheme error, got %v", err)
	}
}

func TestFetch_BadFirstEntry(t *testing.T) {
	// Hand-craft a tarball where the first entry is NOT manifest.json.
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.tar.zst")
	{
		f, _ := os.Create(bad)
		w := newTarZstWriter(f)
		_ = w.WriteEntry("payload.bin", 4, timeFixed(), bytes.NewReader([]byte("data")))
		_ = w.Close()
		f.Close()
	}
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + bad, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "manifest.json") {
		t.Errorf("want first-entry error, got %v", err)
	}
}

func TestFetch_UnsupportedSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.tar.zst")
	{
		f, _ := os.Create(bad)
		w := newTarZstWriter(f)
		body := []byte(`{"schema_version":99,"portal":"x","csq_version":"v","snapshot_id":"i","duckdb_filename":"x.duckdb","duckdb_sha256":"00","duckdb_size_bytes":0}`)
		_ = w.WriteEntry("manifest.json", int64(len(body)), timeFixed(), bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + bad, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "schema_version") {
		t.Errorf("want schema_version error, got %v", err)
	}
}

func TestFetch_UnsafeFilename(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.tar.zst")
	{
		f, _ := os.Create(bad)
		w := newTarZstWriter(f)
		body := []byte(`{"schema_version":1,"portal":"x","csq_version":"v","snapshot_id":"i","duckdb_filename":"../etc/passwd","duckdb_sha256":"00","duckdb_size_bytes":0}`)
		_ = w.WriteEntry("manifest.json", int64(len(body)), timeFixed(), bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + bad, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe filename") {
		t.Errorf("want unsafe-filename error, got %v", err)
	}
}

func TestFetch_FilenameMismatch(t *testing.T) {
	// Pack normally, then rebuild a tarball with the payload renamed.
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})

	mfst, rest := readTarball(t, tarPath)
	rebuilt := filepath.Join(dir, "rebuilt.tar.zst")
	{
		f, _ := os.Create(rebuilt)
		w := newTarZstWriter(f)
		mb, _ := mfst.MarshalIndent()
		_ = w.WriteEntry("manifest.json", int64(len(mb)), mfst.CreatedAt, bytes.NewReader(mb))
		body := rest[mfst.DuckDBFilename]
		_ = w.WriteEntry("renamed.duckdb", int64(len(body)), mfst.CreatedAt, bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	_, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + rebuilt, OutputPath: filepath.Join(dir, "out.duckdb"),
	})
	if err == nil || !strings.Contains(err.Error(), "unexpected payload") {
		t.Errorf("want unexpected-payload error, got %v", err)
	}
}

func TestFetch_SizeMismatch(t *testing.T) {
	// Build a tarball where the manifest says 999 bytes but the payload entry has 4 bytes.
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.tar.zst")
	{
		f, _ := os.Create(bad)
		w := newTarZstWriter(f)
		body := []byte(`{"schema_version":1,"portal":"x","csq_version":"v","snapshot_id":"i","duckdb_filename":"x.duckdb","duckdb_sha256":"00","duckdb_size_bytes":999}`)
		_ = w.WriteEntry("manifest.json", int64(len(body)), timeFixed(), bytes.NewReader(body))
		_ = w.WriteEntry("x.duckdb", 4, timeFixed(), bytes.NewReader([]byte("abcd")))
		_ = w.Close()
		f.Close()
	}
	out := filepath.Join(dir, "out.duckdb")
	_, err := Fetch(context.Background(), ConsumerOptions{URL: "file://" + bad, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "size mismatch") {
		t.Errorf("want size-mismatch error, got %v", err)
	}
	if _, statErr := os.Stat(out); statErr == nil {
		t.Errorf("partial output should be removed")
	}
}

func TestFetch_SHA256Mismatch(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	// Mutate the tarball: re-write manifest with a wrong SHA so size still matches.
	mfst, rest := readTarball(t, tarPath)
	mfst.DuckDBSHA256 = strings.Repeat("0", 64)
	mutated := filepath.Join(dir, "mut.tar.zst")
	{
		f, _ := os.Create(mutated)
		w := newTarZstWriter(f)
		mb, _ := mfst.MarshalIndent()
		_ = w.WriteEntry("manifest.json", int64(len(mb)), mfst.CreatedAt, bytes.NewReader(mb))
		body := rest[mfst.DuckDBFilename]
		_ = w.WriteEntry(mfst.DuckDBFilename, int64(len(body)), mfst.CreatedAt, bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	out := filepath.Join(dir, "out.duckdb")
	_, err := Fetch(context.Background(), ConsumerOptions{URL: "file://" + mutated, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
		t.Errorf("want sha256-mismatch error, got %v", err)
	}
	if _, statErr := os.Stat(out); statErr == nil {
		t.Errorf("partial output should be removed")
	}
}

func TestFetch_NoVerifySkipsSHA(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	mfst, rest := readTarball(t, tarPath)
	mfst.DuckDBSHA256 = strings.Repeat("0", 64)
	mutated := filepath.Join(dir, "mut.tar.zst")
	{
		f, _ := os.Create(mutated)
		w := newTarZstWriter(f)
		mb, _ := mfst.MarshalIndent()
		_ = w.WriteEntry("manifest.json", int64(len(mb)), mfst.CreatedAt, bytes.NewReader(mb))
		body := rest[mfst.DuckDBFilename]
		_ = w.WriteEntry(mfst.DuckDBFilename, int64(len(body)), mfst.CreatedAt, bytes.NewReader(body))
		_ = w.Close()
		f.Close()
	}
	out := filepath.Join(dir, "out.duckdb")
	if _, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + mutated, OutputPath: out, NoVerify: true,
	}); err != nil {
		t.Fatalf("fetch with NoVerify should succeed: %v", err)
	}
}

func TestFetch_OutputExists_NoForce(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "out.duckdb")
	_ = os.WriteFile(out, []byte("hi"), 0o644)
	_, err := Fetch(context.Background(), ConsumerOptions{URL: "file://" + tarPath, OutputPath: out})
	if err == nil || !strings.Contains(err.Error(), "exists") {
		t.Errorf("want exists error, got %v", err)
	}
}

func TestFetch_OutputExists_Force(t *testing.T) {
	dir := t.TempDir()
	_, tarPath := packTo(t, dir, "snap.tar.zst",
		FixtureDataset{ID: "aaaa-0001", Name: "X"})
	out := filepath.Join(dir, "out.duckdb")
	_ = os.WriteFile(out, []byte("hi"), 0o644)
	if _, err := Fetch(context.Background(), ConsumerOptions{
		URL: "file://" + tarPath, OutputPath: out, Force: true,
	}); err != nil {
		t.Fatalf("fetch with Force: %v", err)
	}
}

func timeFixed() time.Time {
	return time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
}
