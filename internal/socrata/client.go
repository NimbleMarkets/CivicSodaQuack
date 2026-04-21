// Copyright (c) 2026 Neomantra Corp

package socrata

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
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

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
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
	base := &url.URL{
		Scheme: "https",
		Host:   portal,
		Path:   fmt.Sprintf("/resource/%s.json", datasetID),
	}

	fetched := 0
	offset := 0
	batch := c.batchSize()

	for {
		remaining := batch
		if limit > 0 && limit-fetched < batch {
			remaining = limit - fetched
		}
		if remaining <= 0 {
			return nil
		}

		q := url.Values{}
		q.Set("$limit", strconv.Itoa(remaining))
		q.Set("$offset", strconv.Itoa(offset))
		if orderBy != "" {
			q.Set("$order", orderBy)
		}
		if whereClause != "" {
			q.Set("$where", whereClause)
		}
		base.RawQuery = q.Encode()

		page, err := c.getPage(base.String())
		if err != nil {
			return err
		}

		if len(page) > 0 {
			if err := handler(page); err != nil {
				return err
			}
		}

		fetched += len(page)
		offset += len(page)

		if len(page) < remaining {
			return nil // short page → end of data
		}
		if limit > 0 && fetched >= limit {
			return nil
		}
	}
}

func (c *Client) getPage(fullURL string) ([]Row, error) {
	var lastErr error
	wait := c.retryWait()

	for attempt := 0; attempt <= c.maxRetries(); attempt++ {
		req, err := http.NewRequest(http.MethodGet, fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		if c.AppToken != "" {
			req.Header.Set("X-App-Token", c.AppToken)
		}

		resp, err := c.httpClient().Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(wait)
			wait *= 2
			continue
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			defer resp.Body.Close()
			var page []Row
			if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
				return nil, fmt.Errorf("decode page: %w", err)
			}
			return page, nil

		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			time.Sleep(wait)
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
