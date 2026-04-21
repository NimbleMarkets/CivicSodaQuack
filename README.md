# CivicSodaQuack

Turn any Socrata Open Data API portal into a fast, local, queryable DuckDB + MCP
surface for AI agents. See [AGENTS.md](./AGENTS.md) for the full project brief.

## Status

**Phase 0** — single-portal, single-dataset extractor into a per-portal DuckDB.

## Quickstart

```bash
go build -o csq ./cmd/csq

# Extract the Chicago crimes dataset into ./data.cityofchicago.org.duckdb
./csq extract \
  --portal data.cityofchicago.org \
  --dataset 6zsd-86xi \
  --limit 10000 \
  --verbose
```

Set `SOCRATA_APP_TOKEN` (or pass `--token`) to lift anonymous rate limits.

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
