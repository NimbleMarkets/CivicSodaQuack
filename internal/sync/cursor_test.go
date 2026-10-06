// Copyright (c) 2026 Neomantra Corp

package sync

import (
	"math"
	"testing"
	"time"

	"github.com/neomantra/CivicSodaQuack/internal/socrata"
)

func tsMs() *time.Time {
	t := time.Date(2026, 4, 22, 1, 2, 3, 500_000_000, time.UTC)
	return &t
}

// tsFine returns 01:02:03.123456789 truncated to the given number of fractional
// digits (6 = microseconds, 9 = nanoseconds).
func tsFine(digits int) *time.Time {
	t := time.Date(2026, 4, 22, 1, 2, 3, 123456789, time.UTC).Truncate(time.Duration(math.Pow10(9 - digits)))
	return &t
}

func TestResumeCursor_Where(t *testing.T) {
	ts := time.Date(2026, 4, 22, 1, 2, 3, 4_000_000, time.UTC)
	cases := []struct {
		name string
		cur  resumeCursor
		want string
	}{
		{"none", resumeCursor{}, ""},
		{"timestamp only (legacy state)", resumeCursor{ts: &ts},
			":updated_at >= '2026-04-22T01:02:03.004'"},
		{"timestamp and id", resumeCursor{ts: &ts, id: "row-a"},
			"(:updated_at > '2026-04-22T01:02:03.004') OR (:updated_at = '2026-04-22T01:02:03.004' AND :id > 'row-a')"},
		{"millisecond-aligned keeps three digits", resumeCursor{ts: tsMs(), id: "row-a"},
			"(:updated_at > '2026-04-22T01:02:03.500') OR (:updated_at = '2026-04-22T01:02:03.500' AND :id > 'row-a')"},
		{"microseconds are preserved", resumeCursor{ts: tsFine(6), id: "row-a"},
			"(:updated_at > '2026-04-22T01:02:03.123456') OR (:updated_at = '2026-04-22T01:02:03.123456' AND :id > 'row-a')"},
		{"nanoseconds are preserved", resumeCursor{ts: tsFine(9), id: "row-a"},
			"(:updated_at > '2026-04-22T01:02:03.123456789') OR (:updated_at = '2026-04-22T01:02:03.123456789' AND :id > 'row-a')"},
		{"id with a quote is escaped", resumeCursor{ts: &ts, id: "o'brien"},
			"(:updated_at > '2026-04-22T01:02:03.004') OR (:updated_at = '2026-04-22T01:02:03.004' AND :id > 'o''brien')"},
	}
	for _, c := range cases {
		if got := c.cur.where(":updated_at"); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.name, got, c.want)
		}
	}
}

// The cursor is the maximum (timestamp, :id) pair no matter what order rows
// arrive in.
func TestResumeCursor_ObserveKeepsMaximum(t *testing.T) {
	row := func(ts, id string) socrata.Row {
		return socrata.Row{":updated_at": ts, ":id": id}
	}
	rows := []socrata.Row{
		row("2026-04-22T00:00:05.000", "b"),
		row("2026-04-22T00:00:05.000", "c"),
		row("2026-04-22T00:00:05.000", "a"), // same ts, smaller id: ignored
		row("2026-04-22T00:00:01.000", "z"), // older ts: ignored
		{":id": "q"},                        // no timestamp: ignored
	}
	var cur resumeCursor
	for _, r := range rows {
		cur.observe(r, ":updated_at")
	}
	if cur.ts == nil || cur.ts.Format("15:04:05") != "00:00:05" || cur.id != "c" {
		t.Errorf("cursor = %v / %q, want 00:00:05 / c", cur.ts, cur.id)
	}
}
