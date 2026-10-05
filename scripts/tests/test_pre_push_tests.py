"""Pre-push range gating retains code/data deletions and unpublished ancestors."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from conftest import load_scripts_module

push = load_scripts_module("pre_push_tests")


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def _commit() -> str:
    _git("add", "--all")
    _git("commit", "-qm", "fixture")
    return _git("rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.chdir(tmp_path)
    _git("init", "-q")
    _git("config", "user.name", "Fixture")
    _git("config", "user.email", "fixture@example.invalid")
    for filename in ("code.py", "curation.toml", "fixture.json", "README.md"):
        Path(filename).write_text("initial\n")
    base = _commit()
    _git("update-ref", "refs/remotes/origin/main", base)
    return base


def test_docs_only_published_base_skips(repo: str) -> None:
    Path("README.md").write_text("updated\n")
    assert not push.requires_tests(repo, _commit())


@pytest.mark.parametrize("filename", ["code.py", "curation.toml", "fixture.json"])
@pytest.mark.parametrize("operation", ["edit", "delete", "rename"])
def test_code_and_data_changes_require_tests(
    repo: str, filename: str, operation: str
) -> None:
    path = Path(filename)
    if operation == "delete":
        path.unlink()
    elif operation == "rename":
        path.rename("document.md")
    else:
        path.write_text("changed\n")
    assert push.requires_tests(repo, _commit())


def test_docs_tip_includes_unpublished_code_ancestor(repo: str) -> None:
    Path("code.py").write_text("unpublished\n")
    code_commit = _commit()
    Path("README.md").write_text("docs-only tip\n")
    tip = _commit()
    # This is pre-commit's new-branch range selection, not just HEAD's parent.
    first_unpublished = _git(
        "rev-list", tip, "--topo-order", "--reverse", "--not", "--remotes=origin"
    ).splitlines()[0]
    assert first_unpublished == code_commit
    assert _git("rev-parse", f"{first_unpublished}^") == repo
    assert push.requires_tests(repo, tip)
    assert not push.requires_tests(code_commit, tip)


@pytest.mark.parametrize("refs", [(None, None), ("base", None), (None, "tip")])
def test_unknown_or_all_files_context_keeps_full_gate(refs: tuple) -> None:
    assert push.requires_tests(*refs)


def test_invalid_range_blocks_gate(
    repo: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.setenv("PRE_COMMIT_FROM_REF", "missing-ref")
    monkeypatch.setenv("PRE_COMMIT_TO_REF", repo)
    assert push.main() == 1
    assert "required gate blocked" in capsys.readouterr().err


def test_full_gate_preserves_workspace_flags_and_exit_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PRE_COMMIT_FROM_REF", raising=False)
    monkeypatch.delenv("PRE_COMMIT_TO_REF", raising=False)
    calls: list[tuple[str, ...]] = []

    def run(command: tuple[str, ...]) -> int:
        calls.append(command)
        return 7

    monkeypatch.setattr(subprocess, "call", run)
    assert push.main() == 7
    assert calls == [
        (
            "uv",
            "run",
            "python",
            "-m",
            "pytest",
            "-n",
            "auto",
            "-q",
            "--run-integration",
            "--install-mode",
            "workspace",
        )
    ]
