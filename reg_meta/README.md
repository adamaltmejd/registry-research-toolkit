# reg_meta

Search and query SCB registry metadata from a local SQLite database.
Designed for programmatic use by LLM agents and tools; terminal use
is secondary.

## Install

```bash
uv tool install reg-meta
reg-meta update      # downloads package + database (~400 MB compressed)
```

Alternatively, build from raw SCB CSV exports (maintainer flow — requires
access to mikrometadata.scb.se and a repo checkout, since `build-db`
reads `classifications.toml`, `fqid_slugs/`, and `input_data/SCB/` from
the working tree; none of those ship in the wheel):

```bash
git clone https://github.com/adamaltmejd/registry-research-toolkit
cd registry-research-toolkit
uv sync
uv run reg-meta-build build-db --input-dir reg_meta_build/input_data/
```

## Quick start

```bash
reg-meta search --query "kommun"                     # search all fields
reg-meta search --query "kommun" --field datacolumn  # column headers only
reg-meta get register LISA                           # register overview
reg-meta get schema --register LISA --years 2020     # columns for a year
reg-meta get varinfo "Kön"                           # variable details
reg-meta resolve --columns "Kon,FodelseAr"           # map column names
reg-meta docs search "disponibel inkomst"            # search documentation
```

Use `--help` on any command or subcommand for full flag documentation.

## Commands

### Query

| Command | Purpose |
|---|---|
| `search` | Free-text search across registers, variables, columns, and value codes |
| `get register` | Register overview with variants |
| `get schema` | Column listing per register edition (validity window), with `--years`, `--columns-like`, `--summary`, `--flat` |
| `get varinfo` | Variable details with per-era `variable_state` history |
| `get values` | Value-set members for a variable, year-projected per `variable_state` era |
| `get datacolumns` | All column aliases for a variable across registers |
| `get coded-variables` | Variables with value sets, ranked by usage |
| `get diff` | Schema changes between two years for a register |
| `get lineage` | Variable provenance across registers |
| `get availability` | Temporal coverage for a variable or register |
| `get classification` | Normalized code systems (SUN, SSYK, SNI, LKF, ...) with `--list`, `--codes`, `--variables` |
| `resolve` | Exact alias lookup for column names (batch) |

### Documentation

| Command | Purpose |
|---|---|
| `docs search` | Full-text search over curated register documentation |
| `docs get` | Retrieve full documentation for a variable or topic |
| `docs list` | Browse available documentation by type, topic, or register |

### Maintenance

| Command | Purpose |
|---|---|
| `update` | Update package and database to the latest version |
| `info` | Database stats and import metadata |

Build commands (`build-db`, `build-docs`, `seed-slugs`, `precheck-slugs`,
`parse-sos`) live in the separate `reg_meta_build` package. They are
maintainer-only and require a repo checkout (the wheel does not ship
`classifications.toml`, `fqid_slugs/`, or `docs/`); see the
`reg_meta_build` README for the build flow.

## Output formats

All commands support `--format {table,list,json}`. Default is table,
which auto-switches to list for narrow results. Add `-v` for the full
JSON envelope (contract version, timing, database info).

Register arguments accept numeric IDs, exact names, or substring matches.

## Database

Stored at `~/.local/share/reg_meta/` by default.
Override with `--db` or `$REG_META_DB`.

## Files

| Path | Purpose |
|---|---|
| [DESIGN.md](DESIGN.md) | Design rationale, constraints, and the object model |
| [../reg_meta_build/DESIGN.md](../reg_meta_build/DESIGN.md) | Build pipeline and per-provider source-delivery shapes |
| [../reg_meta_build/docs/SCHEMA.md](../reg_meta_build/docs/SCHEMA.md) | Documentation markdown file format |
| `src/reg_meta/` | Package source |
| `tests/` | Test suite |
