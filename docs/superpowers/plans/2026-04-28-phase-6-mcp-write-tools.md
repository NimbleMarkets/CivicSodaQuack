# Phase 6 — MCP Write Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans.

**Goal:** Add `sync_dataset` and `refresh_catalog` MCP tools, paired `--db`/`--config` flags on `csq mcp`, and build-time `-ldflags` injection of `version.Version`.

**Architecture:** New `internal/version` package (one var). New `internal/mcpserver/configs.go` pairs `--db`/`--config` and loads YAMLs. Two new tool handlers in `internal/mcpserver` import `internal/sync` + `internal/socrata`. CLI wires the new flag and passes a `map[alias]*config.Config` into `Options.Configs`. Taskfile injects `version.Version` via `-X` ldflag.

**Tech Stack:** Go 1.24. No new external deps.

---

## Tasks

1. `internal/version` package + tests
2. `internal/mcpserver/configs.go` — `LoadConfigs` + tests
3. `tools_sync_dataset.go` + tests
4. `tools_refresh_catalog.go` + tests
5. Wire `Options.Configs` and tool registration in `server.go`; replace hardcoded version
6. `cmd/csq/mcp.go` — `--config` flag + LoadConfigs
7. `cmd/csq/snapshot.go` — `version.Version`
8. `Taskfile.yml` — `-ldflags` injection
9. `cmd/csq/main.go` — usage
10. CLI smoke tests
11. README update
12. Final verification + merge

(Plan steps are tight — implementer reads the spec at `docs/superpowers/specs/2026-04-28-phase-6-mcp-write-tools-design.md` for code-shape detail.)
