// Copyright (c) 2026 Neomantra Corp

package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoad_Valid(t *testing.T) {
	t.Setenv("SOCRATA_APP_TOKEN", "test-token-abc")
	cfg, err := Load("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Portal != "data.cityofchicago.org" {
		t.Errorf("portal: got %q", cfg.Portal)
	}
	if cfg.AppToken != "test-token-abc" {
		t.Errorf("app_token: got %q, want expanded", cfg.AppToken)
	}
	if cfg.Concurrency != 4 {
		t.Errorf("concurrency: got %d", cfg.Concurrency)
	}
	if cfg.OnError != "continue" {
		t.Errorf("on_error: got %q", cfg.OnError)
	}
	if len(cfg.Include) != 4 {
		t.Errorf("include: got %d selectors", len(cfg.Include))
	}
	if cfg.Overrides["6zsd-86xi"].Table != "crimes" {
		t.Errorf("override 6zsd-86xi.table: got %q", cfg.Overrides["6zsd-86xi"].Table)
	}
	if len(cfg.Overrides["6zsd-86xi"].Columns.Skip) != 1 {
		t.Errorf("columns.skip: got %v", cfg.Overrides["6zsd-86xi"].Columns.Skip)
	}
}

func TestLoad_Defaults(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "cfg-*.yaml")
	if err != nil {
		t.Fatalf("tempfile: %v", err)
	}
	f.WriteString("portal: data.example.org\ninclude:\n  - id: aaaa-bbbb\n")
	f.Close()

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Concurrency != 4 {
		t.Errorf("concurrency default: got %d, want 4", cfg.Concurrency)
	}
	if cfg.OnError != "continue" {
		t.Errorf("on_error default: got %q", cfg.OnError)
	}
	if cfg.DB != "data.example.org.duckdb" {
		t.Errorf("db default: got %q, want data.example.org.duckdb", cfg.DB)
	}
	if cfg.Defaults.BatchSize != 5000 {
		t.Errorf("defaults.batch_size: got %d, want 5000", cfg.Defaults.BatchSize)
	}
	if cfg.Defaults.OrderBy != ":id" {
		t.Errorf("defaults.order_by: got %q, want :id", cfg.Defaults.OrderBy)
	}
}

func TestLoad_UnknownKey(t *testing.T) {
	_, err := Load("testdata/invalid_unknown_key.yaml")
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "mystery_key") {
		t.Errorf("error should name the unknown key: %v", err)
	}
}

func TestLoad_BadOnError(t *testing.T) {
	_, err := Load("testdata/invalid_bad_on_error.yaml")
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !strings.Contains(err.Error(), "on_error") {
		t.Errorf("error should name the field: %v", err)
	}
}

func TestLoad_EmptyInclude(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "cfg-*.yaml")
	if err != nil {
		t.Fatalf("tempfile: %v", err)
	}
	f.WriteString("portal: data.example.org\n")
	f.Close()

	_, err = Load(f.Name())
	if err == nil {
		t.Fatal("want error for missing include, got nil")
	}
}

func TestLoad_AppTokenEnvUnset(t *testing.T) {
	// Ensure the env var is NOT set.
	t.Setenv("SOCRATA_APP_TOKEN_FOR_TEST_ONLY", "")
	os.Unsetenv("SOCRATA_APP_TOKEN_FOR_TEST_ONLY")

	f, err := os.CreateTemp(t.TempDir(), "cfg-*.yaml")
	if err != nil {
		t.Fatalf("tempfile: %v", err)
	}
	f.WriteString("portal: data.example.org\napp_token: ${SOCRATA_APP_TOKEN_FOR_TEST_ONLY}\ninclude:\n  - id: aaaa-bbbb\n")
	f.Close()

	_, err = Load(f.Name())
	if err == nil {
		t.Fatal("want error for unset env var, got nil")
	}
	if !strings.Contains(err.Error(), "app_token") {
		t.Errorf("error should name app_token: %v", err)
	}
}

func TestLoad_SelectorMustHaveExactlyOne(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "cfg-*.yaml")
	if err != nil {
		t.Fatalf("tempfile: %v", err)
	}
	// Both id AND name set on the same selector — invalid.
	f.WriteString("portal: data.example.org\ninclude:\n  - id: aaaa-bbbb\n    name: \"Crimes*\"\n")
	f.Close()

	_, err = Load(f.Name())
	if err == nil {
		t.Fatal("want error for selector with multiple fields, got nil")
	}
	if !strings.Contains(err.Error(), "include[0]") {
		t.Errorf("error should reference include[0]: %v", err)
	}
}
