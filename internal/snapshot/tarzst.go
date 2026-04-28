// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"archive/tar"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"
)

// tarZstWriter wraps an io.Writer with a streaming tar+zstd codec.
// Each WriteEntry call writes one tar entry; Close flushes both layers.
type tarZstWriter struct {
	zw *zstd.Encoder
	tw *tar.Writer
}

func newTarZstWriter(w io.Writer) *tarZstWriter {
	zw, _ := zstd.NewWriter(w) // err is always nil for default options per docs
	tw := tar.NewWriter(zw)
	return &tarZstWriter{zw: zw, tw: tw}
}

// WriteEntry writes one regular-file entry. body is read until EOF; it must
// produce exactly size bytes (tar enforces this and returns an error otherwise).
func (w *tarZstWriter) WriteEntry(name string, size int64, modTime time.Time, body io.Reader) error {
	hdr := &tar.Header{
		Name:     name,
		Size:     size,
		Mode:     0o644,
		ModTime:  modTime,
		Typeflag: tar.TypeReg,
		Format:   tar.FormatPAX,
	}
	if err := w.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar header %q: %w", name, err)
	}
	if _, err := io.Copy(w.tw, body); err != nil {
		return fmt.Errorf("tar body %q: %w", name, err)
	}
	return nil
}

// Close flushes the tar trailer and the zstd frame. Safe to call once.
func (w *tarZstWriter) Close() error {
	if err := w.tw.Close(); err != nil {
		return fmt.Errorf("close tar: %w", err)
	}
	if err := w.zw.Close(); err != nil {
		return fmt.Errorf("close zstd: %w", err)
	}
	return nil
}

// tarZstReader decodes a tar+zstd stream entry by entry.
type tarZstReader struct {
	zr *zstd.Decoder
	tr *tar.Reader
}

func newTarZstReader(r io.Reader) (*tarZstReader, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("zstd reader: %w", err)
	}
	tr := tar.NewReader(zr)
	return &tarZstReader{zr: zr, tr: tr}, nil
}

// Next advances to the next entry and returns its header plus a body reader
// scoped to that entry's bytes. Returns io.EOF after the last entry.
func (r *tarZstReader) Next() (*tar.Header, io.Reader, error) {
	hdr, err := r.tr.Next()
	if err != nil {
		return nil, nil, err
	}
	return hdr, r.tr, nil
}

// Close releases the zstd decoder. The underlying io.Reader is not closed.
func (r *tarZstReader) Close() error {
	r.zr.Close()
	return nil
}
