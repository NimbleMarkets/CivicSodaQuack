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

func TestFetchCatalogCtx_CancelInterruptsStalledRequest(t *testing.T) {
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
	_, err := c.FetchCatalogCtx(ctx, strings.TrimPrefix(srv.URL, "http://"), "http")
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("want prompt cancellation, got err=%v after %v", err, time.Since(start))
	}
}

func TestWithBatchSize_CopiesInsteadOfMutating(t *testing.T) {
	shared := &Client{BatchSize: 5000, AppToken: "tok"}
	small := shared.WithBatchSize(10)
	if small.BatchSize != 10 || small.AppToken != "tok" {
		t.Errorf("copy: %+v", small)
	}
	if shared.BatchSize != 5000 {
		t.Errorf("shared client mutated: %d", shared.BatchSize)
	}
	if got := shared.WithBatchSize(0).BatchSize; got != 5000 {
		t.Errorf("n<=0 should keep the client's setting, got %d", got)
	}
}
