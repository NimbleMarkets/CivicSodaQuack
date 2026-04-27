// Copyright (c) 2026 Neomantra Corp

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	flag "github.com/spf13/pflag"

	"github.com/neomantra/CivicSodaQuack/internal/mcpserver"
)

func runMCP(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	var (
		dbs      []string
		httpAddr string
	)
	fs.StringArrayVar(&dbs, "db", nil, "Portal DuckDB to attach: 'path.duckdb' or 'alias=path.duckdb' (repeatable)")
	fs.StringVar(&httpAddr, "http", "", "Listen on this address for HTTP/SSE transport (default: stdio)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	specs, err := mcpserver.ResolveDBSpecs(dbs)
	if err != nil {
		return err
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
