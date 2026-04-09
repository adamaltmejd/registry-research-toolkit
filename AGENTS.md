# MONA Mock Data Wizard
- [MONA](https://www.scb.se/mona) is Statistics Sweden's platform for access to microdata
- Agents are not allowed on MONA.
- To enable agentic local work with MONA projects, the `MONA Mock Data Wizard` helps the user to generate authentic mock data.

# CRITICAL: PERSONAL DATA MAY NOT BE EXPORTED FROM MONA
- MONA contains personal data. Under no circumstances may any PII ever leave MONA.
- Only **aggregate statistics** may ever be exported.

# Products
- Python package `mock_data_wizard` (CLI `mock-data-wizard`) for SCB MONA mock-data generation workflows.
- Python package `regmeta` (CLI `regmeta`) for searching and querying registry metadata.
- Tools are proper python project packages called with `uv`.

# Governance
- `DESIGN.md` per package documents design rationale and constraints.
- No frozen specs or implementation trackers — design decisions live in DESIGN.md, implementation history lives in git.

# Maturity and compatibility
- All tools in this repo are **early-stage** with a small group of testers. Breaking changes are acceptable but should be deliberate — prefer clean breaks over silent behavior changes.
- Do not write migration code, shims, deprecation wrappers, or backwards-compatibility layers. If something needs to change, change it directly.
- Do not preserve old code "just in case." Dead code gets deleted.

# Coding principles
- Deterministic behavior with explicit seed/config.
- Fail fast with actionable errors and stable exit codes.
- Keep domain logic separate from IO/prompts/integrations.
- Validate JSON contracts at read/write boundaries.
- Avoid leaking sensitive row-level content.

# Lint and test
- `uv run ruff check` — python lint
- `uv run ruff format --check` — python format check
- `bunx markdownlint-cli2` — markdown lint (config in `.markdownlint-cli2.yaml`)
- `uv run python -m pytest regmeta/` — regmeta tests
- `uv run python -m pytest mock_data_wizard/` — mock_data_wizard tests
- `regmeta/docs/lisa/*.md` are build artifacts — fix `scripts/parse_lisa_docs.py`, not the output

# Target structure
- `regmeta/DESIGN.md` — design rationale
- `regmeta/STRUCTURE.md` — domain model
- `regmeta/docs/` — curated register documentation (build artifacts + schema)
- `regmeta/src/regmeta/` — package source
- `mock_data_wizard/DESIGN.md` — design rationale, PII safety rules
- `mock_data_wizard/src/mock_data_wizard/` — package source
