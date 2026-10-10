# Contributing

## Setup

Prerequisites: **Python 3.14+** and [uv](https://docs.astral.sh/uv/) for the builder and
the test tooling, a stable **Rust** toolchain for `crates/` (`uv sync` builds the
`reg-core-py` extension with it), and [bun](https://bun.sh/) for the frontend.

```bash
git clone https://github.com/adamaltmejd/registry-research-toolkit.git
cd registry-research-toolkit
uv sync --group dev
```

## Testing

```bash
uv run --no-project scripts/gate.py all   # the full gate before a PR
uv run python -m pytest                   # the Python suites (builder, conformance, tooling)
cargo test --workspace                    # the crates
```

Expensive test suites are gated behind `--run-<name>` flags. To add a new category, add
an entry to `OPTIONAL_MARKERS` in `conftest.py` and decorate tests with
`@pytest.mark.<name>`. Today the only one is `--run-release`: the conformance checks on
a real artifact (see `conformance/README.md`).

There is no pre-push test hook: CI (`ci.yml`) runs the full suite on every PR to main,
and merges require it green. Run the relevant tests locally before pushing; the commit
hook only runs the fast checks (ruff, ty, panache, cargo fmt).

## Linting

```bash
uv run ruff check      # lint (config in pyproject.toml covers every package)
uv run ruff format     # format
```

## Releasing

Use the `release` skill (`.agents/skills/release/SKILL.md`, `/release` in Claude Code).
It covers the version bump, the tag, building or copying forward the DB assets, and the
release workflows.
