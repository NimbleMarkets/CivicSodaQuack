// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/config"
)

// DatasetTarget is a single dataset the orchestrator will sync.
type DatasetTarget struct {
	ID        string
	Name      string
	Effective config.Effective
}

// DatasetResult is the outcome of one dataset sync.
type DatasetResult struct {
	Target      DatasetTarget
	Status      string // "ok" | "failed" | "aborted"
	RowsWritten int64
	Err         error
	StartedAt   time.Time
	FinishedAt  time.Time
}
