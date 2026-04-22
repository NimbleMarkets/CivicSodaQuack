// Copyright (c) 2026 Neomantra Corp

package config

import (
	"bytes"
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// Load reads, validates, and ${ENV}-expands a portal config YAML.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}

	// Expand ${ENV_VAR} in the app_token line only — identify by YAML key
	// on the line to avoid touching e.g. SoQL $where clauses.
	data = expandAppTokenEnv(data)

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse %q: %w", path, err)
	}

	applyDefaults(&cfg)
	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validate %q: %w", path, err)
	}
	return &cfg, nil
}

var appTokenEnvPattern = regexp.MustCompile(`(?m)^(\s*app_token\s*:\s*)\$\{([A-Z0-9_]+)\}\s*$`)

func expandAppTokenEnv(data []byte) []byte {
	return appTokenEnvPattern.ReplaceAllFunc(data, func(match []byte) []byte {
		sub := appTokenEnvPattern.FindSubmatch(match)
		prefix := sub[1]
		envVar := string(sub[2])
		return append(append([]byte{}, prefix...), []byte(os.Getenv(envVar))...)
	})
}

func applyDefaults(cfg *Config) {
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 4
	}
	if cfg.OnError == "" {
		cfg.OnError = "continue"
	}
	if cfg.DB == "" && cfg.Portal != "" {
		cfg.DB = cfg.Portal + ".duckdb"
	}
	if cfg.Defaults.BatchSize == 0 {
		cfg.Defaults.BatchSize = 5000
	}
	if cfg.Defaults.OrderBy == "" {
		cfg.Defaults.OrderBy = ":id"
	}
}

func validate(cfg *Config) error {
	if cfg.Portal == "" {
		return fmt.Errorf("portal: required")
	}
	if cfg.OnError != "continue" && cfg.OnError != "abort" {
		return fmt.Errorf("on_error: must be 'continue' or 'abort', got %q", cfg.OnError)
	}
	if cfg.Concurrency < 1 {
		return fmt.Errorf("concurrency: must be >= 1, got %d", cfg.Concurrency)
	}
	if len(cfg.Include) == 0 {
		return fmt.Errorf("include: at least one selector required")
	}
	for i, s := range cfg.Include {
		if err := s.validate(); err != nil {
			return fmt.Errorf("include[%d]: %w", i, err)
		}
	}
	for i, s := range cfg.Exclude {
		if err := s.validate(); err != nil {
			return fmt.Errorf("exclude[%d]: %w", i, err)
		}
	}
	return nil
}

func (s Selector) validate() error {
	n := 0
	if s.ID != "" {
		n++
	}
	if s.Name != "" {
		n++
	}
	if s.Category != "" {
		n++
	}
	if s.Tag != "" {
		n++
	}
	if n != 1 {
		return fmt.Errorf("exactly one of id, name, category, tag must be set (got %d)", n)
	}
	return nil
}
