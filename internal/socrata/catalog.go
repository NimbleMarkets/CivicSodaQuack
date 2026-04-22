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

// CatalogEntry is a single dataset as returned by /api/catalog/v1.
type CatalogEntry struct {
	ID          string
	Name        string
	Description string
	Category    string
	Tags        []string
	RowCount    *int64
	UpdatedAt   *time.Time
	Raw         json.RawMessage
}

// FetchCatalog returns every dataset the portal reports, following pagination.
func (c *Client) FetchCatalog(portal string) ([]CatalogEntry, error) {
	return c.fetchCatalogScheme(portal, "https")
}

// fetchCatalogScheme is the scheme-parameterised form used in tests with httptest.
func (c *Client) fetchCatalogScheme(portal, scheme string) ([]CatalogEntry, error) {
	base := &url.URL{Scheme: scheme, Host: portal, Path: "/api/catalog/v1"}

	var all []CatalogEntry
	offset := 0
	pageSize := c.batchSize()

	for {
		q := url.Values{}
		q.Set("domains", portal)
		q.Set("limit", strconv.Itoa(pageSize))
		q.Set("offset", strconv.Itoa(offset))
		base.RawQuery = q.Encode()

		page, total, err := c.getCatalogPage(base.String())
		if err != nil {
			return nil, err
		}
		all = append(all, page...)

		offset += len(page)
		if len(page) == 0 || offset >= total {
			return all, nil
		}
	}
}

type rawCatalogEntry struct {
	Resource struct {
		ID            string `json:"id"`
		Name          string `json:"name"`
		Description   string `json:"description"`
		RowsUpdatedAt string `json:"rowsUpdatedAt"`
	} `json:"resource"`
	Classification struct {
		DomainCategory string   `json:"domain_category"`
		DomainTags     []string `json:"domain_tags"`
	} `json:"classification"`
}

type catalogResponse struct {
	Results       []json.RawMessage `json:"results"`
	ResultSetSize int               `json:"resultSetSize"`
}

func (c *Client) getCatalogPage(fullURL string) ([]CatalogEntry, int, error) {
	req, err := http.NewRequest(http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build catalog request: %w", err)
	}
	if c.AppToken != "" {
		req.Header.Set("X-App-Token", c.AppToken)
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("catalog request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("catalog HTTP %d: %s", resp.StatusCode, string(body))
	}

	var cr catalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return nil, 0, fmt.Errorf("decode catalog: %w", err)
	}

	entries := make([]CatalogEntry, 0, len(cr.Results))
	for _, raw := range cr.Results {
		var r rawCatalogEntry
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, 0, fmt.Errorf("decode catalog entry: %w", err)
		}
		e := CatalogEntry{
			ID:          r.Resource.ID,
			Name:        r.Resource.Name,
			Description: r.Resource.Description,
			Category:    r.Classification.DomainCategory,
			Tags:        r.Classification.DomainTags,
			Raw:         raw,
		}
		if r.Resource.RowsUpdatedAt != "" {
			if t, err := time.Parse("2006-01-02T15:04:05.000", r.Resource.RowsUpdatedAt); err == nil {
				e.UpdatedAt = &t
			}
		}
		entries = append(entries, e)
	}
	return entries, cr.ResultSetSize, nil
}
