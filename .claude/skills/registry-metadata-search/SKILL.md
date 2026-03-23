---
name: registry-metadata-search
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

Verify the install works:

```bash
regmeta search --query "kommun" --datacolumn --limit 1
```

## Output formats

The default output is `table` (compact, auto-switches to `list` when too wide). Use `--format json` when you need structured data for further processing. Format flags are global — place them **before** the subcommand:

```bash
regmeta --format json search --query "kommun"
regmeta --format list get varinfo "Kön"
```

## When to use regmeta

- User asks "what variables are in LISA?" → `get schema`
- User asks "what does column Kon mean?" → `resolve` or `search --datacolumn`
- User asks "what are the valid values for kommun?" → `get values`
- User asks "what changed in LISA between 2015 and 2020?" → `get diff`
- User asks "which registers have a variable called Kön?" → `get lineage` or `search`
- User has a CSV with column headers and needs to understand them → `resolve`

## Commands

### search — Find variables, columns, registers, or value codes

```bash
# Broad search across all fields
regmeta search --query "inkomst"

# Search column headers only (what appears in data files)
regmeta search --query "kommun" --datacolumn

# Search canonical variable names
regmeta search --query "Kön" --varname

# Search value codes and labels
regmeta search --query "0180" --value

# Narrow to a specific register
regmeta search --query "kommun" --datacolumn --register LISA
```

Results include `type`, `register_id`, `register_name`, `var_id`, `variable_name`.

### resolve — Map column names to variables (batch, exact match)

The fastest way to identify what columns in a data file mean:

```bash
regmeta resolve --columns "Kon,FodelseAr,Kommun" --register LISA
```

Each column gets status `matched` or `no_match`. Matches include `var_id`, `variable_name`, `matched_column`, and `register_id`.

Can also read a JSON array from stdin:
```bash
echo '["Kon","FodelseAr"]' | regmeta resolve --register LISA
```

### get register — Register overview

```bash
regmeta get register LISA
```

Returns register metadata including variants (each with `regvar_id`, name, description, secrecy level).

### get schema — Column listing for a register

```bash
# All variants and years
regmeta get schema --register LISA

# Specific years
regmeta get schema --register LISA --years 2020-2023

# Specific variant (by regvar_id from get register)
regmeta get schema 153 --years 2022
```

Returns variants → versions → columns. Each column has `var_id`, `variabelnamn`, `datatyp`, `aliases` (column header names in data files), and `cvid` (link to value set).

### get varinfo — Variable details and history

```bash
regmeta get varinfo "Kön"
regmeta get varinfo 44              # by var_id
regmeta get varinfo "Kön" --register LISA
```

Returns variable definition, description, and instances — every register version where this variable appears, with `cvid`, data type, aliases, and value set count.

### get values — Value code lookup

Requires a CVID (get it from `get varinfo` or `get schema`):

```bash
regmeta get values 1001
regmeta get values 1001 --valid-at 2020-01-01
```

Returns code/label pairs. Use `--valid-at` for codes valid at a specific date.

### get datacolumns — All aliases for a variable

```bash
regmeta get datacolumns "Kommun"
```

Shows every column header this variable appears under across all registers and versions.

### get coded-variables — Find categorical variables

```bash
regmeta get coded-variables --min-registers 5 --min-codes 10
```

Lists variables that have coded value sets, ranked by usage.

### get diff — Schema changes between years

```bash
regmeta get diff --register LISA --from 2015 --to 2020
regmeta get diff --register LISA --from 2015 --to 2020 --variable Kon
```

Returns added, removed, and changed variables between two versions.

### get lineage — Variable provenance

```bash
regmeta get lineage "Kön"
```

Shows which register is the source (producer) and which registers consume the variable, with year ranges and instance counts.

### maintain — Setup and maintenance

```bash
# Download pre-built database from GitHub Releases
regmeta maintain download                  # interactive confirmation
regmeta maintain download --yes            # skip confirmation
regmeta maintain download --tag v0.1.0     # specific release
regmeta maintain download --force --yes    # overwrite existing DB

# Build database from raw SCB CSV exports (alternative to download)
regmeta maintain build-db --csv-dir path/to/SCB-data/

# Database stats and import metadata
regmeta maintain info
```

## Typical workflows

### "What's in this register?"
```bash
regmeta get register LISA          # overview + variants
regmeta get schema --register LISA --years 2022  # columns
```

### "What does this column mean?"
```bash
regmeta resolve --columns "Kon,AstKommun" --register LISA
# Then for value codes:
regmeta get varinfo 44 --register LISA  # get CVIDs
regmeta get values 1001                  # get code labels
```

### "What are the valid values for variable X?"
```bash
regmeta get varinfo "Kommun" --register LISA  # find CVID
regmeta get values <cvid> --valid-at 2022-01-01
```

### "How has this register changed over time?"
```bash
regmeta get diff --register LISA --from 2010 --to 2022
```

### "Which registers contain income data?"
```bash
regmeta search --query "inkomst" --varname
```

## Key concepts

- **register** — A statistical register (e.g. LISA, RTB). Has an integer `register_id`.
- **variant** — A sub-table within a register (e.g. LISA/Individer, LISA/Företag). Has `regvar_id`.
- **version** — A year-specific release of a variant. Named by year (e.g. "2022").
- **variable** — A logical concept (e.g. "Kön"). Has `var_id`. Appears across registers.
- **alias / kolumnnamn** — The column header in the actual data file. May differ across registers/versions.
- **CVID** — Links a variable instance to its value set. Use with `get values`.
- **value set** — The valid coded values for a categorical variable (e.g. 1=Man, 2=Kvinna).

## Notes

- All query commands are offline. The database must be installed first with `regmeta maintain download`.
- Register arguments accept numeric IDs, exact names, or substring matches.
- Variable arguments accept `var_id` (integer) or variable name (string).
- Search uses substring matching for most fields; `--description` uses full-text search.
- Value sets are a historical union — use `--valid-at` for temporal filtering.
