---
name: regmeta-query
description: Query SCB registry metadata using the regmeta CLI. Use when answering questions about Swedish register data — variable definitions, value codes, register schemas, column names, or how data is structured across registers and years.
---

# regmeta — Registry Metadata Queries

You have access to `regmeta`, a CLI tool for querying SCB (Statistics Sweden) registry metadata. The database contains metadata for **238 registers**, **42,000+ variables**, and **710,000+ value codes**. It does NOT contain microdata — only structural metadata about registers.

## Install

If `regmeta` is not yet installed, run these commands:

```bash
# Install regmeta
uv add "regmeta @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=regmeta"

# Download the pre-built metadata database (~400 MB download, ~1.6 GB on disk)
uv run regmeta maintain download --yes
```

If you also need mock data generation for MONA projects:

```bash
uv add "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"
```

Verify the install works:

```bash
uv run regmeta --format json search --query "kommun" --datacolumn --limit 1
```

## When to use regmeta

- User asks "what variables are in LISA?" → `get schema`
- User asks "what does column Kon mean?" → `resolve` or `search --datacolumn`
- User asks "what are the valid values for kommun?" → `get values`
- User asks "what changed in LISA between 2015 and 2020?" → `get diff`
- User asks "which registers have a variable called Kön?" → `get lineage` or `search`
- User has a CSV with column headers and needs to understand them → `resolve`

## Always use `--format json`

Put `--format json` **before** the subcommand (it's a global flag):

```bash
regmeta --format json search --query "kommun"
```

## Commands

### search — Find variables, columns, registers, or value codes

```bash
# Broad search across all fields
regmeta --format json search --query "inkomst"

# Search column headers only (what appears in data files)
regmeta --format json search --query "kommun" --datacolumn

# Search canonical variable names
regmeta --format json search --query "Kön" --varname

# Search value codes and labels
regmeta --format json search --query "0180" --value

# Narrow to a specific register
regmeta --format json search --query "kommun" --datacolumn --register LISA
```

Returns `{ "results": [...], "total_count": N }`. Each result has `type`, `register_id`, `register_name`, `var_id`, `variable_name`.

### resolve — Map column names to variables (batch, exact match)

The fastest way to identify what columns in a data file mean:

```bash
regmeta --format json resolve --columns "Kon,FodelseAr,Kommun" --register LISA
```

Returns `{ "columns": [{ "column_name": "Kon", "status": "matched", "matches": [{ "var_id": 44, "variable_name": "Kön", "register_id": 34 }] }, ...] }`.

Status is `matched` (1 match), `ambiguous` (multiple), or `no_match`.

Can also read a JSON array from stdin:
```bash
echo '["Kon","FodelseAr"]' | regmeta --format json resolve --register LISA
```

### get register — Register overview

```bash
regmeta --format json get register LISA
```

Returns register metadata including `register_id`, `registernamn`, `registersyfte`, and `variants` (each with `regvar_id`, name, description, secrecy level).

### get schema — Column listing for a register

```bash
# All variants and years
regmeta --format json get schema --register LISA

# Specific years
regmeta --format json get schema --register LISA --years 2020-2023

# Specific variant (by regvar_id from get register)
regmeta --format json get schema 153 --years 2022
```

Returns variants → versions → columns. Each column has `var_id`, `variabelnamn`, `datatyp`, `aliases` (column header names in data files), and `cvid` (link to value set).

### get varinfo — Variable details and history

```bash
regmeta --format json get varinfo "Kön"
regmeta --format json get varinfo 44              # by var_id
regmeta --format json get varinfo "Kön" --register LISA
```

Returns variable definition, description, and `instances` — every register version where this variable appears, with `cvid`, data type, aliases, and value set count.

### get values — Value code lookup

Requires a CVID (get it from `get varinfo` or `get schema`):

```bash
regmeta --format json get values 1001
regmeta --format json get values 1001 --valid-at 2020-01-01
```

Returns `[{ "vardekod": "1", "vardebenamning": "Man" }, ...]`. Use `--valid-at` for codes valid at a specific date.

### get datacolumns — All aliases for a variable

```bash
regmeta --format json get datacolumns "Kommun"
```

Shows every column header this variable appears under across all registers and versions. Useful for understanding naming inconsistencies.

### get coded-variables — Find categorical variables

```bash
regmeta --format json get coded-variables --min-registers 5 --min-codes 10
```

Lists variables that have coded value sets, ranked by usage.

### get diff — Schema changes between years

```bash
regmeta --format json get diff --register LISA --from 2015 --to 2020
regmeta --format json get diff --register LISA --from 2015 --to 2020 --variable Kon
```

Returns added, removed, and changed variables between two versions.

### get lineage — Variable provenance

```bash
regmeta --format json get lineage "Kön"
```

Shows which register is the **source** (producer) and which registers **consume** the variable, with year ranges and instance counts.

## Typical workflows

### "What's in this register?"
```bash
regmeta --format json get register LISA          # overview + variants
regmeta --format json get schema --register LISA --years 2022  # columns
```

### "What does this column mean?"
```bash
regmeta --format json resolve --columns "Kon,AstKommun" --register LISA
# Then for value codes:
regmeta --format json get varinfo 44 --register LISA  # get CVIDs
regmeta --format json get values 1001                  # get code labels
```

### "What are the valid values for variable X?"
```bash
regmeta --format json get varinfo "Kommun" --register LISA  # find CVID
regmeta --format json get values <cvid> --valid-at 2022-01-01
```

### "How has this register changed over time?"
```bash
regmeta --format json get diff --register LISA --from 2010 --to 2022
```

### "Which registers contain income data?"
```bash
regmeta --format json search --query "inkomst" --varname
```

## Key concepts

- **register** — A statistical register (e.g. LISA, RTB). Has an integer `register_id`.
- **variant** — A sub-table within a register (e.g. LISA/Individer, LISA/Företag). Has `regvar_id`.
- **version** — A year-specific release of a variant. Named by year (e.g. "2022").
- **variable** — A logical concept (e.g. "Kön"). Has `var_id`. Appears across registers.
- **alias / kolumnnamn** — The column header in the actual data file. A variable may have different aliases in different registers or versions.
- **CVID** — Links a variable instance to its value set. Use with `get values`.
- **value set** — The valid coded values for a categorical variable (e.g. 1=Man, 2=Kvinna).

## Notes

- All query commands are offline. The database must be installed first with `regmeta maintain download`.
- Register arguments accept numeric IDs, exact names, or substring matches.
- Variable arguments accept `var_id` (integer) or variable name (string).
- Search uses substring matching for most fields; `--description` uses full-text search.
- Value sets are a historical union — use `--valid-at` for temporal filtering.
