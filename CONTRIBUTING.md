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

## Releasing

Use the `/release` skill in Claude Code, which handles version bumps, tagging,
and publishing. For manual database releases:

```bash
# Build DB from SCB CSV exports
regmeta maintain build-db --csv-dir regmeta/input_data/SCB/

# Compress and attach to an existing release
zstd -3 -T0 ~/.local/share/regmeta/regmeta.db -o regmeta.db.zst
gh release upload regmeta/vX.Y.Z regmeta.db.zst
```
