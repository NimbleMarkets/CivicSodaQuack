// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"testing"

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
			return map[string]any{"id": id + "-" + itoa(i), "score": float64(i)}
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
