# PLAN: Mock Data Wizard v1

Status: Complete
Linked spec: `SPEC_mock_data_wizard.md`
Created: 2026-03-15

## Phases

| Phase | Status |
|---|---|
| 1. Foundation — `pyproject.toml`, `stats.py` | Complete |
| 2. R Script Generation — `script_gen.py` | Complete |
| 3. Enrichment — `enrich.py` | Complete |
| 4. Generation — `generate.py` | Complete |
| 5. CLI + Tests | Complete |

## Verification

- [x] `uv run mock-data-wizard generate-script --project-dir ...` produces valid R script
- [x] `uv run mock-data-wizard generate --stats ... --seed 42` produces mock CSVs
- [x] Deterministic: same seed → identical SHA-256 checksums
- [x] 31 tests pass (`uv run --directory mock_data_wizard python -m pytest tests/ -v`)
- [x] `ruff check` clean
- [x] `ruff format` clean
