"""Unit tests for scripts/_gh.py — the shared git process primitives.

Pins the two git primitives the git-runner consolidation hoisted onto `_gh`:
`scrubbed_git_env` (the SINGLE home for the GIT_* hijack scrub) and `run_git` (prepend
`git`, run in an explicit cwd with that scrub, tolerate a non-zero exit). Loaded via the
shared spec-loader (like every other `scripts/` unit test) so `_gh` resolves under one
process-wide identity rather than a divergent second copy.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from conftest import load_scripts_module, make_git_repo

if TYPE_CHECKING:
    import pytest

gh = load_scripts_module("_gh")


# --- scrubbed_git_env ----------------------------------------------------------------


def test_scrubbed_git_env_drops_all_git_prefixed_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The blanket drop, not a trio-only one: GIT_SSH_COMMAND is outside the historical
    # GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE trio yet must still be scrubbed, or a future
    # repo-targeting/config-affecting GIT_* var would leak past the guard.
    monkeypatch.setenv("GIT_DIR", "/decoy/.git")
    monkeypatch.setenv("GIT_WORK_TREE", "/decoy")
    monkeypatch.setenv("GIT_SSH_COMMAND", "false")

    env = gh.scrubbed_git_env()

    assert not any(k.startswith("GIT_") for k in env)
    assert "GIT_DIR" not in env
    assert "GIT_WORK_TREE" not in env
    assert "GIT_SSH_COMMAND" not in env
    # Non-GIT vars survive — the scrub is surgical, not a bare four-var env.
    assert "PATH" in env


# --- run_git -------------------------------------------------------------------------


def test_run_git_prepends_git_and_runs_in_cwd(tmp_path: Path) -> None:
    repo = make_git_repo(tmp_path)

    result = gh.run_git(["rev-parse", "--show-toplevel"], cwd=repo)

    assert result.returncode == 0
    assert Path(result.stdout.strip()).resolve() == repo.resolve()


def test_run_git_tolerates_nonzero_exit(tmp_path: Path) -> None:
    # A non-zero exit is a signal the caller inspects, not a fatal error: run_git returns
    # the CompletedProcess rather than raising. `show-ref --verify` on a missing ref exits
    # non-zero without writing to stdout.
    repo = make_git_repo(tmp_path)

    result = gh.run_git(
        ["show-ref", "--verify", "--quiet", "refs/heads/nonexistent"], cwd=repo
    )

    assert result.returncode != 0
