// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/config"
	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func mkDataset(id string, rows int, failAt int) fakeDataset {
	return fakeDataset{
		ID: id, Name: "Ds " + id,
		Columns: []map[string]string{
			{"fieldName": "id", "dataTypeName": "text"},
			{"fieldName": "score", "dataTypeName": "number"},
		},
		Rows: makeRows(rows, func(i int) map[string]any {
			return map[string]any{
				":id":         id + "-" + itoa(i),
				":updated_at": "2026-04-22T00:0" + itoa(i%10) + ":00.000",
				"id":          id + "-" + itoa(i),
				"score":       float64(i),
			}
		}),
		FailAtOffset: failAt,
	}
}

func baseCfg(portal string) *config.Config {
	cfg := &config.Config{
		Portal:      portal,
		Concurrency: 2,
		OnError:     "continue",
		Defaults:    config.Defaults{BatchSize: 5, OrderBy: ":id"},
		Include:     []config.Selector{{Category: "Test"}},
	}
	return cfg
}

func TestRun_AllSucceed(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 3, 0), mkDataset("aaaa-0002", 7, 0))
	cfg := baseCfg(fakeHost(srv))

	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	client := &socrata.Client{BatchSize: 5}
	summary, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if summary.OK != 2 || summary.Failed != 0 {
		t.Errorf("summary: %+v", summary)
	}
}

func TestRun_OnErrorContinue_OneFails(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 3, 0), mkDataset("aaaa-0002", 20, 5))
	cfg := baseCfg(fakeHost(srv))

	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	client := &socrata.Client{BatchSize: 5, MaxRetries: 1}
	summary, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
	})
	if err == nil {
		t.Fatal("expected non-nil err indicating at least one failure")
	}
	if summary.OK != 1 || summary.Failed != 1 {
		t.Errorf("summary: %+v", summary)
	}

	// _csq.sync_runs has two rows: 1 ok, 1 failed
	var ok, failed int
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM _csq.sync_runs WHERE status = 'ok'`).Scan(&ok)
	_ = w.DB.QueryRow(`SELECT COUNT(*) FROM _csq.sync_runs WHERE status = 'failed'`).Scan(&failed)
	if ok != 1 || failed != 1 {
		t.Errorf("sync_runs: ok=%d failed=%d", ok, failed)
	}
}

func TestRun_DryRun_NoWrites(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 3, 0))
	cfg := baseCfg(fakeHost(srv))

	w, _ := duckdb.Open(":memory:")
	defer w.Close()

	client := &socrata.Client{BatchSize: 5}
	summary, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{}, DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if summary.Planned != 1 || summary.OK != 0 {
		t.Errorf("summary: %+v", summary)
	}
	var tables int
	_ = w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'`,
	).Scan(&tables)
	if tables != 0 {
		t.Errorf("main schema has %d tables after dry-run, want 0", tables)
	}
}

func TestRun_FullRefresh_PerID(t *testing.T) {
	srv := newFakeSocrata(t,
		mkDataset("aaaa-0001", 3, 0),
		mkDataset("bbbb-0002", 3, 0),
		mkDataset("cccc-0003", 3, 0))
	cfg := baseCfg(fakeHost(srv))

	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 5}

	if _, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")
	state2, _ := w.ReadDatasetState("bbbb-0002")
	state3, _ := w.ReadDatasetState("cccc-0003")
	first1 := *state1.LastFullReplaceAt

	time.Sleep(10 * time.Millisecond)

	if _, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
		FullRefreshIDs: []string{"aaaa-0001"},
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	state1b, _ := w.ReadDatasetState("aaaa-0001")
	state2b, _ := w.ReadDatasetState("bbbb-0002")
	state3b, _ := w.ReadDatasetState("cccc-0003")
	if !state1b.LastFullReplaceAt.After(first1) {
		t.Errorf("aaaa-0001 LastFullReplaceAt should advance; was=%v now=%v", first1, *state1b.LastFullReplaceAt)
	}
	if !state2b.LastFullReplaceAt.Equal(*state2.LastFullReplaceAt) {
		t.Errorf("bbbb-0002 LastFullReplaceAt should be unchanged")
	}
	if !state3b.LastFullReplaceAt.Equal(*state3.LastFullReplaceAt) {
		t.Errorf("cccc-0003 LastFullReplaceAt should be unchanged")
	}
}

func TestRun_FullRefreshAll(t *testing.T) {
	srv := newFakeSocrata(t,
		mkDataset("aaaa-0001", 3, 0),
		mkDataset("bbbb-0002", 3, 0))
	cfg := baseCfg(fakeHost(srv))
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 5}

	if _, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	state1, _ := w.ReadDatasetState("aaaa-0001")
	state2, _ := w.ReadDatasetState("bbbb-0002")
	first1, first2 := *state1.LastFullReplaceAt, *state2.LastFullReplaceAt

	time.Sleep(10 * time.Millisecond)

	if _, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
		FullRefreshAll: true,
	}); err != nil {
		t.Fatalf("refresh-all: %v", err)
	}

	state1b, _ := w.ReadDatasetState("aaaa-0001")
	state2b, _ := w.ReadDatasetState("bbbb-0002")
	if !state1b.LastFullReplaceAt.After(first1) || !state2b.LastFullReplaceAt.After(first2) {
		t.Errorf("both LastFullReplaceAt should advance under FullRefreshAll")
	}
}

func TestRun_FullRefresh_UnknownID_Errors(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 1, 0))
	cfg := baseCfg(fakeHost(srv))
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 5}

	_, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
		FullRefreshIDs: []string{"zzzz-9999"},
	})
	if err == nil || !strings.Contains(err.Error(), "zzzz-9999") {
		t.Errorf("want error mentioning zzzz-9999, got %v", err)
	}
}

func TestRun_FullRefresh_AndAll_BothSet_Errors(t *testing.T) {
	srv := newFakeSocrata(t, mkDataset("aaaa-0001", 1, 0))
	cfg := baseCfg(fakeHost(srv))
	w, _ := duckdb.Open(":memory:")
	defer w.Close()
	client := &socrata.Client{BatchSize: 5}

	_, err := Run(context.Background(), cfg, Deps{
		DB: w, Client: client, Scheme: "http", Reporter: &RecordingReporter{},
		FullRefreshIDs: []string{"aaaa-0001"},
		FullRefreshAll: true,
	})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("want mutually-exclusive error, got %v", err)
	}
}
