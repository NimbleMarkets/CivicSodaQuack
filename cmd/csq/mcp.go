// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/mcpserver"
	"github.com/neomantra/CivicSodaQuack/internal/portallock"
)

func runMCP(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	var (
		dbs      []string
		httpAddr string
		noLock   bool
		lockWait time.Duration
	)
	fs.StringArrayVar(&dbs, "db", nil, "Portal DuckDB to attach: 'path.duckdb' or 'alias=path.duckdb' (repeatable)")
	fs.StringVar(&httpAddr, "http", "", "Listen on this address for HTTP/SSE transport (default: stdio)")
	fs.BoolVar(&noLock, "no-lock", false, "Skip portal lock acquisition")
	fs.DurationVar(&lockWait, "lock-wait", 0,
		"Retry lock acquisition for up to this duration before giving up")

	if err := fs.Parse(args); err != nil {
		return err
	}

	specs, err := mcpserver.ResolveDBSpecs(dbs)
	if err != nil {
		return err
	}

	// Acquire one portal lock per --db. Release in reverse order on any failure.
	locks := make([]*portallock.Lock, 0, len(specs))
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			_ = locks[i].Release()
		}
	}()
	for _, spec := range specs {
		l, err := portallock.Acquire(spec.Path, portallock.Options{NoLock: noLock, LockWait: lockWait})
		if err != nil {
			return err
		}
		locks = append(locks, l)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if httpAddr != "" {
		fmt.Fprintf(os.Stderr, "[csq] mcp serving on http://%s with %d portal(s)\n", httpAddr, len(specs))
	} else {
		fmt.Fprintf(os.Stderr, "[csq] mcp serving on stdio with %d portal(s)\n", len(specs))
	}

	return mcpserver.Serve(ctx, mcpserver.Options{
		DBs:      specs,
		HTTPAddr: httpAddr,
	})
}
