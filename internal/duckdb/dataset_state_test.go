// Copyright (c) 2026 Neomantra Corp

package duckdb

import (
	"testing"
	"time"
)

func TestDatasetState_MissingReturnsNil(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	got, err := w.ReadDatasetState("aaaa-0001")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != nil {
		t.Errorf("want nil for missing row, got %+v", got)
	}
}

func TestDatasetState_UpsertRoundTrip(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	hwm := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	full := time.Date(2026, 4, 22, 11, 0, 0, 0, time.UTC)
	in := DatasetState{
		DatasetID:         "aaaa-0001",
		HWMUpdatedAt:      &hwm,
		HWMID:             "row-abc",
		LastFullReplaceAt: &full,
		LastRunID:         "01HXYZ",
		HWMColumn:         ":updated_at",
	}
	if err := w.UpsertDatasetState(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := w.ReadDatasetState("aaaa-0001")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got == nil {
		t.Fatal("want row, got nil")
	}
	if !got.HWMUpdatedAt.Equal(hwm) {
		t.Errorf("hwm: got %v, want %v", got.HWMUpdatedAt, hwm)
	}
	if got.HWMID != "row-abc" {
		t.Errorf("hwm_id: got %q, want row-abc", got.HWMID)
	}
	if got.LastRunID != "01HXYZ" || got.HWMColumn != ":updated_at" {
		t.Errorf("got %+v", got)
	}
	if got.LastFullReplaceAt == nil || !got.LastFullReplaceAt.Equal(full) {
		t.Errorf("last_full_replace_at: got %v, want %v", got.LastFullReplaceAt, full)
	}
}

func TestDatasetState_UpsertReplaces(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	t1 := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 4, 22, 13, 0, 0, 0, time.UTC)

	_ = w.UpsertDatasetState(DatasetState{
		DatasetID: "aaaa-0001", HWMUpdatedAt: &t1, LastRunID: "run1", HWMColumn: ":updated_at",
	})
	_ = w.UpsertDatasetState(DatasetState{
		DatasetID: "aaaa-0001", HWMUpdatedAt: &t2, LastRunID: "run2", HWMColumn: ":updated_at",
	})

	got, _ := w.ReadDatasetState("aaaa-0001")
	if !got.HWMUpdatedAt.Equal(t2) || got.LastRunID != "run2" {
		t.Errorf("replace: got %+v", got)
	}
}

func TestDatasetState_NullableHWM(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	in := DatasetState{
		DatasetID: "aaaa-0001",
		// HWMUpdatedAt nil — datasets without :updated_at land here
		HWMColumn: ":updated_at",
		LastRunID: "run1",
	}
	if err := w.UpsertDatasetState(in); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := w.ReadDatasetState("aaaa-0001")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.HWMUpdatedAt != nil {
		t.Errorf("want nil HWM, got %v", got.HWMUpdatedAt)
	}
}

// A database created before hwm_id existed gains the column on the next Apply,
// keeps its rows, and reads them back with an empty cursor id.
func TestApply_AddsHWMIDToExistingDatasetState(t *testing.T) {
	w, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, q := range []string{
		`DROP TABLE _csq.dataset_state`,
		`CREATE TABLE _csq.dataset_state (
			dataset_id VARCHAR PRIMARY KEY, hwm_updated_at TIMESTAMP,
			last_full_replace_at TIMESTAMP, last_run_id VARCHAR, hwm_column VARCHAR NOT NULL)`,
		`INSERT INTO _csq.dataset_state VALUES ('aaaa-0001', TIMESTAMP '2026-04-22 12:00:00', NULL, 'r1', ':updated_at')`,
	} {
		if _, err := w.DB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if err := Apply(w.DB); err != nil {
		t.Fatalf("apply on old schema: %v", err)
	}
	if err := Apply(w.DB); err != nil {
		t.Fatalf("apply is not idempotent: %v", err)
	}
	got, err := w.ReadDatasetState("aaaa-0001")
	if err != nil || got == nil {
		t.Fatalf("read: %v %v", got, err)
	}
	if got.HWMID != "" || got.LastRunID != "r1" {
		t.Errorf("legacy row: %+v", got)
	}
}
