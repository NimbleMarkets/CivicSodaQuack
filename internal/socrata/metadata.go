// Copyright (c) 2026 Neomantra Corp

package socrata

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// DatasetMetadata is a small subset of the /api/views/{id}.json response.
type DatasetMetadata struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	RowsUpdated int64    `json:"rowsUpdatedAt,omitempty"`
	Columns     []Column `json:"columns"`
}

// FetchMetadata retrieves dataset metadata from https://{portal}/api/views/{id}.json.
// portal is the bare host (e.g. "data.cityofchicago.org"); appToken is optional.
func (c *Client) FetchMetadata(portal, datasetID string) (*DatasetMetadata, error) {
	u := &url.URL{
		Scheme: "https",
		Host:   portal,
		Path:   fmt.Sprintf("/api/views/%s.json", datasetID),
	}

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build metadata request: %w", err)
	}
	if c.AppToken != "" {
		req.Header.Set("X-App-Token", c.AppToken)
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("metadata request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("metadata HTTP %d: %s", resp.StatusCode, string(body))
	}

	var md DatasetMetadata
	if err := json.NewDecoder(resp.Body).Decode(&md); err != nil {
		return nil, fmt.Errorf("decode metadata: %w", err)
	}
	return &md, nil
}
