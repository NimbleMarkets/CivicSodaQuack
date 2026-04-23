# CivicSodaQuack

Turn any Socrata Open Data API portal into a fast, local, queryable DuckDB + MCP
surface for AI agents. See [AGENTS.md](./AGENTS.md) for the full project brief.

## Status

**Phase 1** — catalog discovery + YAML-driven bulk sync into a per-portal DuckDB.

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
```

Catalog and per-dataset sync history live in the `_csq` schema inside the portal's DuckDB:

```sql
SELECT id, name, category FROM _csq.catalog LIMIT 10;
SELECT dataset_id, status, rows_written, duration_ms
  FROM _csq.sync_runs ORDER BY started_at DESC LIMIT 10;
```

## Layout

```
cmd/csq/              # CLI entrypoint (extract subcommand)
internal/socrata/     # SODA2 client: metadata + paginated row streaming
internal/duckdb/      # DuckDB writer + Socrata→DuckDB schema mapping
```

## License

Released under the [MIT License](https://en.wikipedia.org/wiki/MIT_License), see [LICENSE.txt](./LICENSE.txt).

Copyright (c) 2026 [Neomantra Corp](https://www.neomantra.com).   

----
Made with :heart: and :fire: by the team behind [Nimble.Markets](https://nimble.markets).
