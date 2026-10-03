// Copyright (c) 2026 Neomantra Corp

package socrata

import (
	"context"
	"fmt"
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
	return c.FetchMetadataURL(context.Background(), u.String())
}
