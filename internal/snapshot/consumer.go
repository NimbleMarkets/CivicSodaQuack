// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// ConsumerOptions configures Fetch.
type ConsumerOptions struct {
	URL        string // http(s):// or file:// URL; required
	OutputPath string // destination DuckDB; "" means current dir + manifest.duckdb_filename
	NoVerify   bool   // skip SHA-256 check after extraction
	Force      bool   // overwrite existing OutputPath
}

// Fetch downloads (or opens) the snapshot at opts.URL, validates the manifest,
// streams the DuckDB payload to OutputPath, and verifies SHA-256.
func Fetch(ctx context.Context, opts ConsumerOptions) (*Manifest, error) {
	body, err := openURL(ctx, opts.URL)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	r, err := newTarZstReader(body)
	if err != nil {
		return nil, fmt.Errorf("fetch: decode: %w", err)
	}
	defer r.Close()

	// Entry 1: manifest.json
	hdr, mb, err := r.Next()
	if err != nil {
		return nil, fmt.Errorf("fetch: read first entry: %w", err)
	}
	if hdr.Name != "manifest.json" {
		return nil, fmt.Errorf("fetch: unexpected first entry %q; want manifest.json", hdr.Name)
	}
	manifestBytes, err := io.ReadAll(mb)
	if err != nil {
		return nil, fmt.Errorf("fetch: read manifest: %w", err)
	}
	manifest, err := ParseManifest(manifestBytes)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	if manifest.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("fetch: unsupported schema_version %d (this build supports %d)", manifest.SchemaVersion, SchemaVersion)
	}
	if !isSafeFilename(manifest.DuckDBFilename) {
		return nil, fmt.Errorf("fetch: manifest declares unsafe filename %q", manifest.DuckDBFilename)
	}

	outPath := opts.OutputPath
	if outPath == "" {
		outPath = manifest.DuckDBFilename
	}
	if !opts.Force {
		if _, err := os.Stat(outPath); err == nil {
			return nil, fmt.Errorf("fetch: %s exists; pass --force to overwrite", outPath)
		}
	}

	// Entry 2: DuckDB payload
	hdr2, payload, err := r.Next()
	if err != nil {
		return nil, fmt.Errorf("fetch: read payload entry: %w", err)
	}
	if hdr2.Name != manifest.DuckDBFilename {
		return nil, fmt.Errorf("fetch: unexpected payload entry %q; manifest declared %q", hdr2.Name, manifest.DuckDBFilename)
	}
	if hdr2.Size != manifest.DuckDBSizeBytes {
		return nil, fmt.Errorf("fetch: size mismatch: tar header %d, manifest %d", hdr2.Size, manifest.DuckDBSizeBytes)
	}

	if err := writeWithSHA(outPath, payload, manifest, opts.NoVerify); err != nil {
		_ = os.Remove(outPath)
		return nil, err
	}
	return manifest, nil
}

// openURL returns a ReadCloser for opts.URL. http(s) and file are supported.
func openURL(ctx context.Context, url string) (io.ReadCloser, error) {
	switch {
	case strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://"):
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("fetch: build request: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch: %w", err)
		}
		if resp.StatusCode >= 400 {
			snippet := readSnippet(resp.Body, 200)
			resp.Body.Close()
			return nil, fmt.Errorf("fetch: HTTP %d: %s", resp.StatusCode, snippet)
		}
		return resp.Body, nil
	case strings.HasPrefix(url, "file://"):
		path := strings.TrimPrefix(url, "file://")
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("fetch: %w", err)
		}
		return f, nil
	default:
		return nil, fmt.Errorf("fetch: unsupported scheme %q (want http, https, or file)", schemeOf(url))
	}
}

func readSnippet(r io.Reader, n int) string {
	buf := make([]byte, n)
	got, _ := io.ReadFull(r, buf)
	return string(buf[:got])
}

func schemeOf(url string) string {
	if i := strings.Index(url, ":"); i >= 0 {
		return url[:i]
	}
	return url
}

// isSafeFilename rejects empty, absolute, or path-traversing names.
func isSafeFilename(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if filepath.Clean(name) != name {
		return false
	}
	if filepath.Base(name) != name {
		return false
	}
	return true
}

// writeWithSHA streams payload into outPath while computing SHA-256, then
// verifies against the manifest unless NoVerify.
func writeWithSHA(outPath string, payload io.Reader, manifest *Manifest, noVerify bool) error {
	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("fetch: create output: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			out.Close()
		}
	}()

	h := hashOrDiscard(noVerify)
	tee := teeWriter(out, h)
	written, err := io.Copy(tee, payload)
	if err != nil {
		out.Close()
		closed = true
		return fmt.Errorf("fetch: write output: %w", err)
	}
	if err := out.Close(); err != nil {
		closed = true
		return fmt.Errorf("fetch: close output: %w", err)
	}
	closed = true

	if written != manifest.DuckDBSizeBytes {
		return fmt.Errorf("fetch: size mismatch: wrote %d, manifest %d", written, manifest.DuckDBSizeBytes)
	}
	if !noVerify {
		got := hex.EncodeToString(h.Sum(nil))
		if got != manifest.DuckDBSHA256 {
			return fmt.Errorf("fetch: sha256 mismatch: got %s, manifest %s", got, manifest.DuckDBSHA256)
		}
	}
	return nil
}

func hashOrDiscard(noVerify bool) hash.Hash {
	if noVerify {
		return discardHash{}
	}
	return sha256.New()
}

type discardHash struct{}

func (discardHash) Write(p []byte) (int, error) { return len(p), nil }
func (discardHash) Sum(b []byte) []byte         { return b }
func (discardHash) Reset()                      {}
func (discardHash) Size() int                   { return 0 }
func (discardHash) BlockSize() int              { return 1 }

func teeWriter(a io.Writer, b io.Writer) io.Writer { return io.MultiWriter(a, b) }
