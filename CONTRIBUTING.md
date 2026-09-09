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
uv run python -m pytest --run-integration --install-mode workspace
```

Expensive test suites are gated behind `--run-<name>` flags. To add a new category, add
an entry to `OPTIONAL_MARKERS` in `conftest.py` and decorate tests with
`@pytest.mark.<name>`.

`--install-mode` picks which installation the Docker module builds. `registry` (the
default) installs reg_meta alone with every dependency resolved from PyPI, so it is red
for as long as a sibling release is owed; `workspace` builds this checkout's reg_schema
and reg_meta wheels and installs both. The pre-push hook uses `workspace`.

## Linting

```bash
uv run ruff check      # lint (config in pyproject.toml covers every package)
uv run ruff format     # format
```

## Releasing

Use the `/release` skill in Claude Code, which handles version bumps, tagging, and
publishing. For manual database releases:

```bash
# Build DB from SCB CSV exports
reg-meta-build build-db --input-dir reg_meta_build/input_data/

# Compress and attach to an existing release
zstd -3 -T0 ~/.local/share/reg_meta/reg_meta.db -o reg_meta.db.zst
gh release upload reg_meta/vX.Y.Z reg_meta.db.zst
```
