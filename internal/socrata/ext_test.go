// Copyright (c) 2026 Neomantra Corp

package socrata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStreamRowsCtx_CancelInterruptsStalledRequest(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	c := &Client{HTTPClient: srv.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := c.StreamRowsCtx(ctx, "http", strings.TrimPrefix(srv.URL, "http://"), "abcd-1234", ":id", "", "", 0,
		func([]Row) error { return nil })
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("want prompt cancellation, got err=%v after %v", err, time.Since(start))
	}
}

func TestStreamRowsCtx_CancelInterruptsBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := &Client{HTTPClient: srv.Client(), RetryWait: time.Minute}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := c.StreamRowsCtx(ctx, "http", strings.TrimPrefix(srv.URL, "http://"), "abcd-1234", ":id", "", "", 0,
		func([]Row) error { return nil })
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("want prompt cancellation, got err=%v after %v", err, time.Since(start))
	}
}
