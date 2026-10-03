// Copyright (c) 2026 Neomantra Corp

package mcpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	maxRows  = 1000
	maxBytes = 1 << 20 // 1 MB
)

// QuerySQLArgs is the input to query_sql.
type QuerySQLArgs struct {
	SQL string `json:"sql" jsonschema:"DuckDB SQL; runs read-only against the host DB with each portal ATTACHed as <alias>"`
}

// QuerySQLResult is the output of query_sql.
type QuerySQLResult struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated"`
	Note      string   `json:"note,omitempty"`
}

// QuerySQL executes args.SQL against the host inside a read-only
// transaction (DuckDB rejects DDL/DML at the engine level), capping the result
// at maxRows or maxBytes (whichever first), and aborting after timeout.
//
// We can't open the host as access_mode=READ_ONLY because the host is :memory:
// and needs to be writeable on startup so we can ATTACH each portal. The
// "BEGIN TRANSACTION READ ONLY" wrapper gives the same engine-level guarantee
// for the duration of one query. We acquire a single *sql.Conn so the BEGIN
// and the SELECT share the same physical connection — database/sql's *sql.Tx
// helpers don't expose DuckDB's read-only flag.
func QuerySQL(parent context.Context, p *Pools, args QuerySQLArgs, timeout time.Duration) (QuerySQLResult, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	conn, err := p.Host.Conn(ctx)
	if err != nil {
		return QuerySQLResult{}, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Close()

	if err := requireSingleStatement(ctx, conn, args.SQL); err != nil {
		return QuerySQLResult{}, err
	}

	if _, err := conn.ExecContext(ctx, `BEGIN TRANSACTION READ ONLY`); err != nil {
		return QuerySQLResult{}, fmt.Errorf("begin read-only tx: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `ROLLBACK`) }()

	rows, err := conn.QueryContext(ctx, args.SQL)
	if err != nil {
		return QuerySQLResult{}, formatQueryError(ctx, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return QuerySQLResult{}, fmt.Errorf("columns: %w", err)
	}

	out := QuerySQLResult{Columns: cols, Rows: [][]any{}}
	approxBytes := len(`{"columns":[],"rows":[],"row_count":0,"truncated":false}`)
	for _, c := range cols {
		approxBytes += len(c) + 3
	}

	for rows.Next() {
		if out.RowCount >= maxRows {
			out.Truncated = true
			out.Note = fmt.Sprintf("result truncated at %d rows; add LIMIT to your query", maxRows)
			break
		}
		row, err := scanRow(rows, len(cols))
		if err != nil {
			return QuerySQLResult{}, fmt.Errorf("scan row %d: %w", out.RowCount, err)
		}
		// Estimate added bytes (rough JSON size)
		b, _ := json.Marshal(row)
		if approxBytes+len(b)+1 > maxBytes {
			out.Truncated = true
			if out.RowCount == 0 {
				out.Note = fmt.Sprintf("first row exceeds ~%d bytes and was omitted; SELECT fewer or smaller columns", maxBytes)
			} else {
				out.Note = fmt.Sprintf("result truncated at ~%d bytes; add LIMIT or SELECT fewer columns", maxBytes)
			}
			break
		}
		approxBytes += len(b) + 1
		out.Rows = append(out.Rows, row)
		out.RowCount++
	}
	if err := rows.Err(); err != nil {
		return QuerySQLResult{}, formatQueryError(ctx, err)
	}
	return out, nil
}

// requireSingleStatement parses sqlText with DuckDB's own parser and rejects
// anything other than exactly one read-shaped statement. The read-only
// transaction is not enough on its own: the driver executes a multi-statement
// string statement by statement, so `COMMIT; CREATE TABLE ...` would end the
// transaction and then write to the in-memory host. json_serialize_sql only
// accepts SELECT-shaped statements (SELECT, WITH, FROM, DESCRIBE, SHOW,
// SUMMARIZE), so DDL, DML, transaction control, PRAGMA and EXPLAIN fail here.
func requireSingleStatement(ctx context.Context, conn *sql.Conn, sqlText string) error {
	literal := "'" + strings.ReplaceAll(sqlText, "'", "''") + "'"
	var raw any
	if err := conn.QueryRowContext(ctx, `SELECT json_serialize_sql(`+literal+`)`).Scan(&raw); err != nil {
		return formatQueryError(ctx, err)
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("parse sql: %w", err)
	}
	var parsed struct {
		Error        bool              `json:"error"`
		ErrorMessage string            `json:"error_message"`
		Statements   []json.RawMessage `json:"statements"`
	}
	if err := json.Unmarshal(b, &parsed); err != nil {
		return fmt.Errorf("parse sql: %w", err)
	}
	if parsed.Error {
		return fmt.Errorf("query_sql accepts a single read-only SELECT-style statement: %s", parsed.ErrorMessage)
	}
	switch n := len(parsed.Statements); {
	case n == 0:
		return fmt.Errorf("query_sql: empty statement")
	case n > 1:
		return fmt.Errorf("query_sql accepts exactly one statement, got %d", n)
	}
	return nil
}

func scanRow(rows *sql.Rows, n int) ([]any, error) {
	cells := make([]any, n)
	ptrs := make([]any, n)
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	// Coerce []byte to string for JSON friendliness
	for i, v := range cells {
		if b, ok := v.([]byte); ok {
			cells[i] = string(b)
		}
	}
	return cells, nil
}

// formatQueryError translates context cancellation into a clear timeout
// message; otherwise returns the underlying error verbatim (DuckDB's messages
// are already user-friendly).
func formatQueryError(ctx context.Context, err error) error {
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("query exceeded timeout")
	}
	if strings.Contains(err.Error(), "context deadline exceeded") {
		return fmt.Errorf("query exceeded timeout")
	}
	return err
}
