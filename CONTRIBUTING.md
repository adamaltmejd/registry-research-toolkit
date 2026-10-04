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
uv run python -m pytest --run-integration           # include native container integration tests
uv run python -m pytest --run-integration --install-mode workspace
```

Expensive test suites are gated behind `--run-<name>` flags. To add a new category, add
an entry to `OPTIONAL_MARKERS` in `conftest.py` and decorate tests with
`@pytest.mark.<name>`.

`--install-mode` picks which installation the package integration module builds.
`registry` (the default) installs reg_meta alone with every dependency resolved from
PyPI, so it is red for as long as a sibling release is owed; `workspace` builds this
checkout's reg_schema and reg_meta wheels and installs both. The pre-push hook uses
`workspace`.

Package integration tests require Apple `container` on macOS (`container system start`)
or native Podman on Linux, preferably rootless (`podman info` must succeed). An opted-in
test fails if the required runtime is missing or unhealthy. It never falls back to
Docker. Builds use the shared Apple builder, so coordinate with other active builds; do
not restart or prune unrelated resources. Linux runs use temporary, owned Podman storage
so interrupted builds can clean up their intermediate containers without touching the
shared store. Linux image caches are not reused between runs.

The pre-push range gate covers the whole push, including deleted code/data paths. A new
branch includes ancestors not yet present on the remote, so a Markdown-only tip on an
unpublished code base still runs the required Python/TOML/JSON gate. A docs-only range
skips it.

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
