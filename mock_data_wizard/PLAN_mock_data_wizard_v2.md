# PLAN: Mock Data Wizard v2

Status: Complete
Linked spec: `SPEC_mock_data_wizard_v2.md`
Created: 2026-03-20

## Phases

| Phase | Status |
|---|---|
| 1. Population spine — `enrich.py`, `generate.py` | Complete |
| 2. Value code drift warnings — `enrich.py` | Complete |

## Verification

- [x] Spine: shared birth-invariant columns produce identical values per individual across files
- [x] Spine: non-spine columns are generated independently
- [x] Spine: deterministic (same seed → same output)
- [x] Spine: no regmeta → no spine, behavior identical to v1
- [x] Drift: warns on frequency codes absent from regmeta value set
- [x] Drift: ignores `_other` bucket
- [x] Drift: no warning when all codes match or no value_codes available
- [x] All existing v1 tests still pass (no regressions)
- [x] 51 tests pass (`uv run --directory mock_data_wizard python -m pytest tests/ -v`)
- [x] `ruff check` clean
- [x] `ruff format` clean
