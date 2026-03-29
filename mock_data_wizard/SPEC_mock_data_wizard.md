# SPEC: Mock Data Wizard v1

Status: Frozen
Version: 1.0.0
Created: 2026-02-28
Frozen: 2026-03-15
Owner: Research engineering

## 1. Purpose

Generate mock CSV data from MONA project metadata without exporting personal data. Two commands:

1. `generate-script` — produce an R script to run on MONA that extracts aggregate stats
2. `generate` — produce mock CSV files from the stats JSON output

Primary consumer: LLM agent skill (not a human at a terminal).

## 2. PII Safety

The R script runs on MONA (personal data). It exports **only** aggregate statistics:

| Column type | Exports | Does NOT export |
|---|---|---|
| Numeric | min, max, mean, sd, quantiles, null_rate | individual values |
| Low-cardinality categorical | frequency table {value: count} | — |
| High-cardinality string | n_distinct, min/max length, null_rate | actual values |
| Date | min, max, null_rate | individual dates |
| ID-like | n_distinct, null_rate | actual IDs |

Low-cardinality threshold: `n_distinct ≤ min(50, n_rows × 0.01)`.

## 3. Stats JSON Contract (R → Python)

```json
{
  "contract_version": "1.0.0",
  "generated_at": "ISO-8601",
  "project_paths": ["\\\\micro.intra\\projekt\\..."],
  "files": [{
    "file_name": "data.csv",
    "relative_path": "data.csv",
    "row_count": 50000,
    "columns": [{
      "column_name": "Kon",
      "inferred_type": "categorical|numeric|high_cardinality|date|id",
      "nullable": false,
      "null_count": 0,
      "null_rate": 0.0,
      "n_distinct": 2,
      "stats": { "frequencies": {"1": 25000, "2": 25000} }
    }]
  }],
  "shared_columns": [
    {"column_name": "LopNr", "files": ["f1.csv", "f2.csv"], "max_n_distinct": 50000}
  ]
}
```

## 4. Generation Strategy

| Type | Method |
|---|---|
| Numeric | `normal(mean, sd)` clamped to [min, max] |
| Categorical (with frequencies) | sample from frequency weights |
| Categorical (with regmeta codes) | sample from regmeta value set |
| High-cardinality string | `val_000001` placeholders |
| Date | uniform between min and max |
| Shared ID | shared pool of synthetic IDs across files |
| Nulls | boolean mask at observed null_rate |

Determinism: sub-seeds derived via `sha256(f"{master_seed}:{file}:{column}")`.

## 5. CLI

```bash
mock-data-wizard generate-script --project-dir PATH [PATH...] [-o OUTPUT]
mock-data-wizard generate --stats PATH [--seed N] [--sample-pct F] [--output-dir DIR] [--db DIR] [--register NAME]
```

## 6. Modules

| Module | Responsibility |
|---|---|
| `stats.py` | Parse/validate stats JSON |
| `script_gen.py` | R script generation |
| `enrich.py` | Combine stats with regmeta metadata |
| `generate.py` | Mock data generation + CSV writing |
| `cli.py` | Thin argparse wrapper |

## 7. Deliberate Exclusions

No state machine, no interactive wizard, no metadata portal HTTP, no two-script workflow, no `state.json`, no `_mock_data/` workspace, no `.gitignore` management, no contract schema JSON files, no reconciliation/staleness, no SQL Server support.
