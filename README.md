# CivicSodaQuack

Turn any Socrata Open Data API portal into a fast, local, queryable DuckDB + MCP
surface for AI agents. See [AGENTS.md](./AGENTS.md) for the full project brief.

## Status

**Phase 3** — MCP server. After syncing one or more portals into per-portal DuckDB files, run `csq mcp` to expose them to AI agents over stdio or HTTP.

## Quickstart

```bash
go build -o csq ./cmd/csq

# Discover what's on a portal
./csq catalog --portal data.cityofchicago.org --category "Public Safety"

# Generate a starter YAML
./csq catalog --portal data.cityofchicago.org --category "Public Safety" \
  --output data.cityofchicago.org.yaml

# Sync the datasets enumerated in the YAML
./csq sync --config data.cityofchicago.org.yaml --dry-run    # preview
./csq sync --config data.cityofchicago.org.yaml              # execute
```

Set `SOCRATA_APP_TOKEN` (referenced in the YAML as `${SOCRATA_APP_TOKEN}`) to lift anonymous rate limits.

### Serve via MCP

```bash
# Stdio (default; for local agent integrations)
./csq mcp --db data.cityofchicago.org.duckdb

# Multi-portal with explicit alias
./csq mcp --db chicago=data.cityofchicago.org.duckdb \
          --db nyc=data.cityofnewyork.us.duckdb

# HTTP (for remote agents; bind to loopback by default)
./csq mcp --db data.cityofchicago.org.duckdb --http 127.0.0.1:8080
```

The MCP server exposes four tools: `list_datasets`, `describe_dataset`, `search_datasets`, and `query_sql`. The `query_sql` tool runs read-only DuckDB SQL across every attached portal; cross-portal queries use `<alias>.<schema>.<table>`, e.g. `SELECT * FROM chicago._csq.catalog UNION ALL SELECT * FROM nyc._csq.catalog`. Results are capped at 1000 rows / 1MB / 30s.

### Config shape

```yaml
portal: data.cityofchicago.org
app_token: ${SOCRATA_APP_TOKEN}
concurrency: 4
on_error: continue

defaults:
  batch_size: 5000
  order_by: ":id"

include:
  - category: "Public Safety"
  - tag: "311*"
exclude:
  - id: 85ca-t3if     # giant, skip

overrides:
  6zsd-86xi:
    table: crimes
    where: "date >= '2015-01-01'"
    batch_size: 10000
    columns:
      skip: [location_description_raw]
    # Phase 2 fields (both optional):
    mode: full_replace        # force full-replace on every run; default is incremental
    hwm_column: ":updated_at" # override the high-water-mark column
```

Catalog and per-dataset sync history live in the `_csq` schema inside the portal's DuckDB:

```sql
SELECT id, name, category FROM _csq.catalog LIMIT 10;
SELECT dataset_id, status, rows_written, duration_ms
  FROM _csq.sync_runs ORDER BY started_at DESC LIMIT 10;

-- Per-dataset incremental-sync state (Phase 2)
SELECT dataset_id, hwm_updated_at, last_full_replace_at, last_run_id
  FROM _csq.dataset_state ORDER BY hwm_updated_at DESC;
```

## Layout

```
cmd/csq/              # CLI entrypoint
internal/socrata/     # SODA2 client: metadata + paginated row streaming
internal/duckdb/      # DuckDB writer + Socrata→DuckDB schema mapping
internal/config/      # YAML loader + per-dataset effective config
internal/sync/        # Sync orchestrator + strategies (FullReplace, Incremental)
internal/mcpserver/   # MCP server: pools, ATTACH, tools, transports
```

## License

Released under the [MIT License](https://en.wikipedia.org/wiki/MIT_License), see [LICENSE.txt](./LICENSE.txt).

Copyright (c) 2026 [Neomantra Corp](https://www.neomantra.com).   

----
Made with :heart: and :fire: by the team behind [Nimble.Markets](https://nimble.markets).
