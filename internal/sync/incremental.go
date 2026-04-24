// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/duckdb"
	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

// IncrementalStrategy implements WriteStrategy. It auto-detects bootstrap vs.
// delta based on _csq.dataset_state and the live table's socrata_id column,
// then either bootstraps via a staging-swap (Phase-1-style with HWM tracking)
// or performs a delta upsert pass.
//
// `mode: full_replace` in the per-dataset YAML override forces the bootstrap
// path on every run.
type IncrementalStrategy struct {
	Portal string
	Scheme string // "" defaults to "https"
	RunID  string
}

func (s *IncrementalStrategy) scheme() string {
	if s.Scheme != "" {
		return s.Scheme
	}
	return "https"
}

func (s *IncrementalStrategy) Sync(
	ctx context.Context,
	target DatasetTarget,
	client *socrata.Client,
	w *duckdb.Writer,
	prog ProgressReporter,
	idx, total int,
) (DatasetResult, error) {
	state, err := w.ReadDatasetState(target.ID)
	if err != nil {
		return failResult(target, "failed", fmt.Errorf("read dataset_state: %w", err)), nil
	}

	useBootstrap, err := shouldBootstrap(state, target, w)
	if err != nil {
		return failResult(target, "failed", err), nil
	}

	if useBootstrap {
		return s.bootstrap(ctx, target, client, w, prog, idx, total)
	}
	// Delta path lands in Task 10.
	return failResult(target, "failed",
		fmt.Errorf("incremental delta path not yet implemented")), nil
}

func shouldBootstrap(state *duckdb.DatasetState, target DatasetTarget, w *duckdb.Writer) (bool, error) {
	if target.Effective.Mode == "full_replace" {
		return true, nil
	}
	if state == nil {
		return true, nil
	}
	hasPK, err := tableHasSocrataIDPK(w, target.Effective.Table)
	if err != nil {
		return false, fmt.Errorf("check pk on %s: %w", target.Effective.Table, err)
	}
	return !hasPK, nil
}

// tableHasSocrataIDPK reports whether main.<table> exists AND has a socrata_id column.
// (PK presence is implied by the socrata_id column; we don't introspect constraints.)
func tableHasSocrataIDPK(w *duckdb.Writer, table string) (bool, error) {
	var n int
	err := w.DB.QueryRow(
		`SELECT COUNT(*) FROM information_schema.columns
		 WHERE table_schema = 'main' AND table_name = $1 AND column_name = 'socrata_id'`,
		table,
	).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// bootstrap streams the full dataset into _csq_staging.<table>_<runID> with the
// socrata_id PK installed, swaps it into main, then writes dataset_state with
// the observed max(:updated_at). Mirrors FullReplaceStrategy.Sync but tracks
// HWM during streaming so we don't have to reread the source.
func (s *IncrementalStrategy) bootstrap(
	ctx context.Context,
	target DatasetTarget,
	client *socrata.Client,
	w *duckdb.Writer,
	prog ProgressReporter,
	idx, total int,
) (DatasetResult, error) {
	started := time.Now().UTC()
	prog.DatasetStart(idx, total, target)
	result := DatasetResult{Target: target, StartedAt: started}

	hwmCol := target.Effective.HWMColumn
	if hwmCol == "" {
		hwmCol = ":updated_at"
	}

	meta, err := fetchMetadata(ctx, client, s.scheme(), s.Portal, target.ID)
	if err != nil {
		return failResult(target, "failed", fmt.Errorf("fetch metadata: %w", err)), nil
	}
	cols := filterColumns(meta.Columns, target.Effective.SkipColumns)
	stagingName := target.Effective.Table + "_" + s.RunID
	schema := duckdb.BuildSchemaWithSocrataID(stagingName, cols)

	if _, err := w.DB.ExecContext(ctx, schema.CreateTableSQLIn("_csq_staging")); err != nil {
		return failResult(target, "failed", fmt.Errorf("create staging: %w", err)), nil
	}

	var rowsWritten int64
	var maxHWM *time.Time
	err = client.StreamRowsCtx(ctx, s.scheme(), s.Portal, target.ID,
		target.Effective.OrderBy, target.Effective.Where, ":*,*",
		target.Effective.Limit,
		func(page []socrata.Row) error {
			if err := w.InsertRowsInto("_csq_staging", schema, page); err != nil {
				return err
			}
			for _, row := range page {
				if t := extractRowHWM(row, hwmCol); t != nil {
					if maxHWM == nil || t.After(*maxHWM) {
						maxHWM = t
					}
				}
			}
			rowsWritten += int64(len(page))
			prog.DatasetProgress(idx, total, target, rowsWritten)
			return nil
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			return failResult(target, "aborted", ctx.Err()), nil
		}
		return failResult(target, "failed", err), nil
	}

	if err := w.SwapIn(stagingName, target.Effective.Table); err != nil {
		return failResult(target, "failed", fmt.Errorf("swap: %w", err)), nil
	}

	// CTAS doesn't preserve PRIMARY KEY constraints, so re-install after swap.
	if schema.PrimaryKey != "" {
		alterSQL := fmt.Sprintf(`ALTER TABLE main."%s" ADD PRIMARY KEY ("%s")`,
			target.Effective.Table, schema.PrimaryKey)
		if _, err := w.DB.ExecContext(ctx, alterSQL); err != nil {
			return failResult(target, "failed", fmt.Errorf("install pk on %s: %w", target.Effective.Table, err)), nil
		}
	}

	now := time.Now().UTC()
	stateRow := duckdb.DatasetState{
		DatasetID:         target.ID,
		HWMUpdatedAt:      maxHWM,
		LastFullReplaceAt: &now,
		LastRunID:         s.RunID,
		HWMColumn:         hwmCol,
	}

	result.Status = "ok"
	result.RowsWritten = rowsWritten
	result.FinishedAt = time.Now().UTC()

	if err := w.UpsertDatasetState(stateRow); err != nil {
		// Data is in main but state didn't land. Surface as ok with err set;
		// sync_runs records the message. Next run will re-bootstrap (idempotent).
		result.Err = fmt.Errorf("write dataset_state: %w", err)
	}
	return result, nil
}

func extractRowHWM(row socrata.Row, hwmCol string) *time.Time {
	v, ok := row[hwmCol]
	if !ok || v == nil {
		return nil
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return nil
	}
	for _, layout := range []string{
		"2006-01-02T15:04:05.000",
		"2006-01-02T15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t
		}
	}
	return nil
}

// failResult is a small wrapper around fail() for the incremental strategy.
func failResult(target DatasetTarget, status string, err error) DatasetResult {
	res := DatasetResult{Target: target, StartedAt: time.Now().UTC()}
	res.Status = status
	res.Err = err
	res.FinishedAt = time.Now().UTC()
	return res
}
