// Copyright (c) 2026 Neomantra Corp

package config

import (
	"strings"
	"testing"
)

func TestEffectiveFor_OverrideWins(t *testing.T) {
	cfg := &Config{
		Defaults: Defaults{BatchSize: 5000, OrderBy: ":id"},
		Overrides: map[string]Override{
			"aaaa-bbbb": {
				Table:     "foo",
				Where:     "id > 0",
				OrderBy:   ":updated_at",
				BatchSize: 10000,
				Limit:     100,
				Columns:   Columns{Skip: []string{"big_col"}},
			},
		},
	}
	eff := cfg.EffectiveFor("aaaa-bbbb")
	if eff.Table != "foo" {
		t.Errorf("table: got %q", eff.Table)
	}
	if eff.Where != "id > 0" {
		t.Errorf("where: got %q", eff.Where)
	}
	if eff.OrderBy != ":updated_at" {
		t.Errorf("order_by: got %q", eff.OrderBy)
	}
	if eff.BatchSize != 10000 {
		t.Errorf("batch: got %d", eff.BatchSize)
	}
	if eff.Limit != 100 {
		t.Errorf("limit: got %d", eff.Limit)
	}
	if len(eff.SkipColumns) != 1 || eff.SkipColumns[0] != "big_col" {
		t.Errorf("skip: got %v", eff.SkipColumns)
	}
}

func TestEffectiveFor_NoOverride(t *testing.T) {
	cfg := &Config{
		Defaults: Defaults{BatchSize: 5000, OrderBy: ":id"},
	}
	eff := cfg.EffectiveFor("cccc-dddd")
	if eff.Table != "cccc_dddd" {
		t.Errorf("default table: got %q, want cccc_dddd", eff.Table)
	}
	if eff.OrderBy != ":id" {
		t.Errorf("order_by: got %q", eff.OrderBy)
	}
	if eff.BatchSize != 5000 {
		t.Errorf("batch: got %d", eff.BatchSize)
	}
}

func TestEffectiveFor_Hash_Deterministic(t *testing.T) {
	cfg := &Config{
		Defaults:  Defaults{BatchSize: 5000, OrderBy: ":id"},
		Overrides: map[string]Override{"a-a": {Where: "x=1"}},
	}
	h1 := cfg.EffectiveFor("a-a").Hash()
	h2 := cfg.EffectiveFor("a-a").Hash()
	if h1 != h2 {
		t.Errorf("hash not deterministic: %s vs %s", h1, h2)
	}
	if !strings.HasPrefix(h1, "sha256:") {
		t.Errorf("hash format: got %q", h1)
	}
}

func TestEffectiveFor_ModeAndHWMColumnOverride(t *testing.T) {
	cfg := &Config{
		Defaults: Defaults{BatchSize: 100, OrderBy: ":id"},
		Overrides: map[string]Override{
			"a-a": {Mode: "full_replace", HWMColumn: ":created_at"},
		},
	}
	eff := cfg.EffectiveFor("a-a")
	if eff.Mode != "full_replace" {
		t.Errorf("mode: got %q", eff.Mode)
	}
	if eff.HWMColumn != ":created_at" {
		t.Errorf("hwm_column: got %q", eff.HWMColumn)
	}
}

func TestEffectiveFor_ModeAndHWMColumnDefaults(t *testing.T) {
	cfg := &Config{}
	eff := cfg.EffectiveFor("missing")
	if eff.Mode != "" {
		t.Errorf("mode default: got %q, want empty", eff.Mode)
	}
	if eff.HWMColumn != "" {
		t.Errorf("hwm_column default: got %q, want empty", eff.HWMColumn)
	}
}

func TestEffectiveFor_CheckpointEveryNPages(t *testing.T) {
	cfg := &Config{
		Overrides: map[string]Override{
			"a-a": {CheckpointEveryNPages: 100},
		},
	}
	eff := cfg.EffectiveFor("a-a")
	if eff.CheckpointEveryNPages != 100 {
		t.Errorf("got %d, want 100", eff.CheckpointEveryNPages)
	}
	eff2 := cfg.EffectiveFor("missing")
	if eff2.CheckpointEveryNPages != 0 {
		t.Errorf("default: got %d, want 0", eff2.CheckpointEveryNPages)
	}
}

func TestEffectiveFor_Hash_IncludesCheckpoint(t *testing.T) {
	a := Effective{Table: "t", BatchSize: 100, CheckpointEveryNPages: 0}.Hash()
	b := Effective{Table: "t", BatchSize: 100, CheckpointEveryNPages: 50}.Hash()
	if a == b {
		t.Errorf("hash should differ when CheckpointEveryNPages changes")
	}
}
