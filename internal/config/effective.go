// Copyright (c) 2026 Neomantra Corp

package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// Effective is the fully-merged per-dataset configuration.
type Effective struct {
	DatasetID   string
	Table       string
	Where       string
	OrderBy     string
	BatchSize   int
	Limit       int
	SkipColumns []string
	Mode        string // "" | "incremental" | "full_replace"
	HWMColumn   string // "" defaults to ":updated_at" at use sites
	// CheckpointEveryNPages, when > 0, persists running HWM every N delta pages.
	CheckpointEveryNPages int
}

// EffectiveFor merges built-in defaults, cfg.Defaults, and cfg.Overrides[id].
func (c *Config) EffectiveFor(id string) Effective {
	eff := Effective{
		DatasetID: id,
		Table:     strings.ReplaceAll(id, "-", "_"),
		OrderBy:   c.Defaults.OrderBy,
		BatchSize: c.Defaults.BatchSize,
	}

	ov, ok := c.Overrides[id]
	if !ok {
		return eff
	}
	if ov.Table != "" {
		eff.Table = ov.Table
	}
	if ov.Where != "" {
		eff.Where = ov.Where
	}
	if ov.OrderBy != "" {
		eff.OrderBy = ov.OrderBy
	}
	if ov.BatchSize != 0 {
		eff.BatchSize = ov.BatchSize
	}
	if ov.Limit != 0 {
		eff.Limit = ov.Limit
	}
	if len(ov.Columns.Skip) > 0 {
		eff.SkipColumns = append([]string(nil), ov.Columns.Skip...)
	}
	if ov.Mode != "" {
		eff.Mode = ov.Mode
	}
	if ov.HWMColumn != "" {
		eff.HWMColumn = ov.HWMColumn
	}
	if ov.CheckpointEveryNPages != 0 {
		eff.CheckpointEveryNPages = ov.CheckpointEveryNPages
	}
	return eff
}

// Hash returns a sha256 hex digest of the effective config, for drift detection
// in _csq.sync_runs.config_hash.
func (e Effective) Hash() string {
	canonical := struct {
		Table                 string   `json:"table"`
		Where                 string   `json:"where"`
		OrderBy               string   `json:"order_by"`
		BatchSize             int      `json:"batch_size"`
		Limit                 int      `json:"limit"`
		SkipColumns           []string `json:"skip_columns"`
		Mode                  string   `json:"mode"`
		HWMColumn             string   `json:"hwm_column"`
		CheckpointEveryNPages int      `json:"checkpoint_every_n_pages"`
	}{e.Table, e.Where, e.OrderBy, e.BatchSize, e.Limit, e.SkipColumns, e.Mode, e.HWMColumn, e.CheckpointEveryNPages}
	b, _ := json.Marshal(canonical)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
