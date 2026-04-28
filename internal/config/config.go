// Copyright (c) 2026 Neomantra Corp

package config

// Config is the parsed portal YAML.
type Config struct {
	Portal      string              `yaml:"portal"`
	AppToken    string              `yaml:"app_token"`
	DB          string              `yaml:"db"`
	Concurrency int                 `yaml:"concurrency"`
	OnError     string              `yaml:"on_error"` // "continue" | "abort"
	Defaults    Defaults            `yaml:"defaults"`
	Include     []Selector          `yaml:"include"`
	Exclude     []Selector          `yaml:"exclude"`
	Overrides   map[string]Override `yaml:"overrides"`
}

// Defaults are per-dataset values applied when no override is set.
type Defaults struct {
	BatchSize int    `yaml:"batch_size"`
	OrderBy   string `yaml:"order_by"`
}

// Selector matches one or more datasets by id, name glob, category glob,
// or tag glob. Exactly one of Id/Name/Category/Tag must be set.
type Selector struct {
	ID       string `yaml:"id"`
	Name     string `yaml:"name"`
	Category string `yaml:"category"`
	Tag      string `yaml:"tag"`
}

// Override is per-dataset configuration (keyed by 4x4 id in YAML).
type Override struct {
	Table     string  `yaml:"table"`
	Where     string  `yaml:"where"`
	OrderBy   string  `yaml:"order_by"`
	BatchSize int     `yaml:"batch_size"`
	Limit     int     `yaml:"limit"`
	Columns   Columns `yaml:"columns"`
	Mode      string  `yaml:"mode"`       // "" | "incremental" | "full_replace"
	HWMColumn string  `yaml:"hwm_column"` // "" defaults to ":updated_at"
	// CheckpointEveryNPages, when > 0, persists the running HWM to dataset_state
	// every N delta pages (Phase 5). 0 = disabled (Phase 2 invariant: HWM only
	// advances on clean dataset completion).
	CheckpointEveryNPages int `yaml:"checkpoint_every_n_pages"`
}

// Columns carries column-level overrides.
type Columns struct {
	Skip []string `yaml:"skip"`
}
