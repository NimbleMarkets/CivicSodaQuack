Here's a polished, comprehensive, ready-to-use **system / project prompt** that captures everything we've discussed about **CivicSodaQuack**. You can use this directly as a GitHub repo README intro, a Claude/Grok coding prompt, or an internal project brief.

### CivicSodaQuack Project Prompt

**Project Name:** CivicSodaQuack (csq)  
**One-liner:** Turn any Socrata Open Data API portal into a fast, local, queryable DuckDB + MCP surface for AI agents.

**Mission**  
Build a lightweight, opinionated Go tool that lets anyone (especially AI agents) discover, sync, and analytically query thousands of public datasets from 40+ Socrata-powered government portals (NYC, Chicago, Seattle, Connecticut, data.gov sections, etc.) without fighting SoQL limits, rate limits, or messy JSON.

The end result: one CLI + one MCP server that gives agents powerful local SQL (with spatial extensions) over materialized civic data.

**Core Architecture (inspired by dank-* tools + dbn-go-mcp-data patterns)**

```
SODA portals (data.nyc.gov, data.cityofchicago.org, ...)
        ↓ (catalog + paginated JSON with $order, $offset, app token)
Generic Extractor CLI (runtime schema inference + YAML overrides)
        ↓
Per-portal DuckDB files (recommended: one .duckdb per portal)
        ↓
MCP Server (list_datasets, describe_dataset, search_datasets, query_sql)
```

**Key Design Principles**
- **Catalog-driven**: Automatically discover all datasets via `/api/catalog/v1` + rich metadata.
- **Generic + Escapable**: Runtime schema inference for most datasets. YAML overrides only for exceptions (column renames, cleaning rules, incremental keys, skipped fields, custom geo handling).
- **Incremental-first**: Use `$order=:updated_at` + high-water-mark tracking per dataset for efficient daily/ hourly syncs (critical for million-row datasets like 311, violations, crimes).
- **Smart Geo Handling**:
  - `point` / `location` → flatten to `lon`, `lat` + keep raw GeoJSON
  - `polygon` / `multipolygon` → DuckDB `GEOMETRY` type via spatial extension
- **Minimal Cleaning by Default**: Trim whitespace, empty strings → NULL. Opt-in cleaners via overrides.
- **Per-portal DuckDB files**: Smaller, parallelizable syncs, easier distribution/snapshots. (Alternative: single DB with schemas — decide early.)
- **MCP-native**: Expose clean agent-friendly tools. Draw heavy inspiration from `dbn-go-mcp-data` (discovery tools, cache/query layer, STDIO + SSE transport, read-only mode).
- **Snapshot Publishing** (future): Pre-built, periodically refreshed `.duckdb` tarballs so agents can just download and attach.

**MCP Tools (target set)**
- `list_datasets(portal?)` — cached catalog
- `describe_dataset(id)` — schema, description, sample rows, last synced
- `search_datasets(portal?, query)`
- `query_sql(sql)` — runs against attached DuckDB files (full DuckDB power + spatial)

**Stack**
- Go (matches your existing toolchain: `dank-extract`, `dank-mcp`, `dbn-go-mcp-data`)
- `go-duckdb` + spatial extension
- MCP Go SDK
- YAML for per-dataset overrides
- App token support per-portal + 429 backoff

**Phase Plan**
- **Phase 0**: Generic SODA extractor CLI + schema inference → single portal → one DuckDB
- **Phase 1**: Catalog-driven bulk sync + overrides config
- **Phase 2**: Incremental sync via high-water marks
- **Phase 3**: Full MCP server (attach multiple per-portal DuckDBs)
- **Phase 4**: Snapshot publishing (like dank-data)

**Design Decisions to Resolve Early**
- One DuckDB per portal vs. one big DB with schemas
- Default cleaning policy vs. opt-in
- How aggressively to cache catalog + metadata inside each DuckDB
- Unified MCP view (auto-ATTACH all portals under one server)

**What Makes This Unique**
No existing tool combines:
- Automatic catalog discovery across any Socrata portal
- Runtime schema inference + targeted YAML overrides
- Thoughtful civic/geo mapping
- Incremental materialization into DuckDB
- A clean MCP agent interface on top

Closest pieces (ingestr, dlt + Socrata, OpenGov MCP servers, Mage/Polars examples, your own `dbn-go-mcp-data`) each solve only part of the puzzle. CivicSodaQuack glues them into something production-ready and agent-native.

**Success Looks Like**
A developer or AI agent can run:
```bash
civicsodaquack sync data.nyc.gov --token=xxx
civicsodaquack serve
```
…and immediately start asking powerful analytical + spatial questions over NYC open data via MCP.

---

You can copy-paste this directly. Want me to adjust it for:
- A shorter GitHub README version?
- A detailed coding prompt for implementing Phase 0?
- Adding specific YAML override examples?
- Or include more patterns from `dbn-go-mcp-data` (e.g., cache structure, tool registration style)?

Just say the word and I’ll refine it. 🦆