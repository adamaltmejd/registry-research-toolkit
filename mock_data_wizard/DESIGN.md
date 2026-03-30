# Design: mock_data_wizard

Design rationale and constraints. For usage, see `mock-data-wizard --help`.

## Two-step workflow

1. `generate-script` produces an R script to run on MONA
2. User runs the script on MONA, exports `stats.json`
3. `generate` produces mock CSVs locally from `stats.json`

This separation exists because MONA has no internet access and no Python.
The R script runs inside MONA; everything else runs locally.

## PII safety

The R script exports **only** aggregate statistics. This is the core safety
invariant — no individual-level data leaves MONA.

| Column type | What gets exported |
|---|---|
| Numeric | min, max, mean, sd, quantiles, null_rate |
| Low-cardinality categorical | frequency table `{value: count}` |
| High-cardinality string | n_distinct, min/max length, null_rate |
| Date | min, max, null_rate |
| ID-like | n_distinct, null_rate |

**Low-cardinality threshold:** `n_distinct <= min(50, n_rows * 0.01)`.

Cells with 5 or fewer individuals are censored in frequency tables.

## Generation strategy

| Type | Method |
|---|---|
| Numeric | `normal(mean, sd)` clamped to `[min, max]` |
| Categorical (with frequencies) | Sample from frequency weights |
| Categorical (with regmeta codes) | Sample from regmeta value set |
| High-cardinality string | `val_000001` placeholders |
| Date | Uniform between min and max |
| Shared ID | Shared pool of synthetic IDs across files |
| Nulls | Boolean mask at observed `null_rate` |

## Determinism and seeding

All randomness is seeded. Sub-seeds are derived via
`sha256(f"{master_seed}:{file}:{column}")`. Same seed produces identical
output. This makes mock data reproducible for CI and testing.

## Population spine

Birth-invariant attributes (Kön, Födelseår, Födelselän, Födelseland) are
generated once per individual and reused across files. Without this, the
same person could have different sex or birth year in different files.

Spine-eligible variables are a hardcoded set of regmeta `var_id`s. The
authority file (which stats drive generation) is selected by highest
`n_distinct` for the shared ID column — proxy for largest population.

Without regmeta enrichment, the spine is empty and behavior is identical
to pre-spine generation.

## Value code drift warnings

After enrichment, frequency codes from stats are cross-checked against
regmeta value sets. Codes absent from the value set trigger stderr
warnings. This catches column name typos and wrong-year stats exports.

Warnings don't block generation. Unseen regmeta codes (codes in metadata
but absent from stats) are deliberately not warned on — registers
legitimately contain rare codes.

## Manifest

Generation produces a `manifest.json` alongside the mock CSVs. The
manifest includes per-file column lists, register and year hints, and
header hashes. `mock-data-wizard compare` reads this to verify local
files against registry schema without requiring separate input.

## Deliberate exclusions

- Household structures, time-varying attributes, employer links
- Interactive wizard / state machine
- HTTP portal for metadata browsing
- SQL Server / non-CSV data sources
- Per-column type info in manifest (misleading for mock data)
