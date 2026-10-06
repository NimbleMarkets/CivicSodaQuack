// Copyright (c) 2026 Neomantra Corp

package socrata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is a minimal Socrata Open Data API (SODA2) client.
//
// It is goroutine-unsafe in that the embedded fields should be set before use.
// A zero value is usable (default http.Client, no app token, 5000 batch size).
type Client struct {
	HTTPClient *http.Client
	AppToken   string
	BatchSize  int           // rows per page; 0 → 5000
	MaxRetries int           // 429/5xx retries; 0 → 5
	RetryWait  time.Duration // initial backoff; 0 → 1s
}

// defaultHTTPClient backs a zero-value Client. http.DefaultClient has no
// timeout, so a stalled portal would hang a request forever when the caller's
// context has no deadline.
var defaultHTTPClient = &http.Client{Timeout: 5 * time.Minute}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return defaultHTTPClient
}

// WithBatchSize returns a copy of c that pages n rows at a time (n <= 0 keeps
// c's setting). The copy lets each dataset use its own effective batch size
// without mutating a client shared by concurrent workers.
func (c *Client) WithBatchSize(n int) *Client {
	cp := *c
	if n > 0 {
		cp.BatchSize = n
	}
	return &cp
}

func (c *Client) batchSize() int {
	if c.BatchSize > 0 {
		return c.BatchSize
	}
	return 5000
}

func (c *Client) maxRetries() int {
	if c.MaxRetries > 0 {
		return c.MaxRetries
	}
	return 5
}

func (c *Client) retryWait() time.Duration {
	if c.RetryWait > 0 {
		return c.RetryWait
	}
	return time.Second
}

// Row is a single JSON object returned by the Socrata rows endpoint.
type Row = map[string]any

// PageHandler receives each page of rows. Return an error to abort the stream.
type PageHandler func(page []Row) error

// StreamRows pages through /resource/{id}.json and invokes handler for each page.
//
// limit caps total rows (0 = unlimited). orderBy is appended to $order (required
// for stable pagination across pages per Socrata docs). whereClause, if set, is
// passed as $where.
func (c *Client) StreamRows(portal, datasetID, orderBy, whereClause string, limit int, handler PageHandler) error {
	return c.StreamRowsCtx(context.Background(), "https", portal, datasetID, orderBy, whereClause, "", limit, handler)
}

// sleepCtx waits d or until ctx is done, whichever is first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// getPage fetches one page, retrying 429/5xx/transport errors with exponential
// backoff. Both the requests and the backoff waits honor ctx.
func (c *Client) getPage(ctx context.Context, fullURL string) ([]Row, error) {
	var lastErr error
	wait := c.retryWait()

	for attempt := 0; attempt <= c.maxRetries(); attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		if c.AppToken != "" {
			req.Header.Set("X-App-Token", c.AppToken)
		}

		resp, err := c.httpClient().Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = err
			if err := sleepCtx(ctx, wait); err != nil {
				return nil, err
			}
			wait *= 2
			continue
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			defer resp.Body.Close()
			var page []Row
			if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("decode page: %w", err)
			}
			return page, nil

		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			if err := sleepCtx(ctx, wait); err != nil {
				return nil, err
			}
			wait *= 2
			continue

		default:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}
	}
	return nil, fmt.Errorf("exhausted retries: %w", lastErr)
}
