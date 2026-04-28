// Copyright (c) 2026 Neomantra Corp

package snapshot

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"
)

func TestTarZst_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := newTarZstWriter(&buf)

	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	if err := w.WriteEntry("manifest.json", 11, now, strings.NewReader(`hello world`)); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	body := bytes.Repeat([]byte("X"), 1024)
	if err := w.WriteEntry("payload.bin", int64(len(body)), now, bytes.NewReader(body)); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	r, err := newTarZstReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer r.Close()

	hdr, body1, err := r.Next()
	if err != nil {
		t.Fatalf("next 1: %v", err)
	}
	if hdr.Name != "manifest.json" || hdr.Size != 11 {
		t.Errorf("entry 1 header: %+v", hdr)
	}
	got1, _ := io.ReadAll(body1)
	if string(got1) != "hello world" {
		t.Errorf("entry 1 body: %q", got1)
	}

	hdr2, body2, err := r.Next()
	if err != nil {
		t.Fatalf("next 2: %v", err)
	}
	if hdr2.Name != "payload.bin" || hdr2.Size != 1024 {
		t.Errorf("entry 2 header: %+v", hdr2)
	}
	got2, _ := io.ReadAll(body2)
	if !bytes.Equal(got2, body) {
		t.Errorf("entry 2 body length=%d", len(got2))
	}

	if _, _, err := r.Next(); err != io.EOF {
		t.Errorf("want EOF after 2 entries, got %v", err)
	}
}
