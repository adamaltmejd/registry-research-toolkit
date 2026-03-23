# Contributing

## Setup

```bash
git clone https://github.com/adamaltmejd/registry-research-toolkit.git
cd registry-research-toolkit
uv sync --group dev
```

## Testing

```bash
uv run python -m pytest                            # unit tests only
uv run python -m pytest --run-integration           # include Docker integration tests
```

Expensive test suites are gated behind `--run-<name>` flags. To add a new category, add an entry to `OPTIONAL_MARKERS` in `conftest.py` and decorate tests with `@pytest.mark.<name>`.

## Linting

```bash
uv run ruff check regmeta/ mock_data_wizard/
uv run ruff format regmeta/ mock_data_wizard/
```

## Releasing a new database version

```bash
# Build DB from SCB CSV exports
regmeta maintain build-db --csv-dir path/to/SCB-data/

# Compress
zstd -3 -T0 ~/.local/share/regmeta/regmeta.db -o regmeta.db.zst

# Create release (semver pre-release tag)
gh release create v0.X.0-alpha.N regmeta.db.zst --prerelease --title "v0.X.0-alpha.N"
```
