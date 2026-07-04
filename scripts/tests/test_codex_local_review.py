"""Unit tests for scripts/codex_local_review.py — the local `codex review` launcher.

The deterministic core (`parse_transcript` + the precondition logic) is pinned here; the
codex launch leaf (`run_codex`) is never invoked against the real binary — tests stub it.
Fixtures below are captured from live runs (codex-cli 0.142.5). Load-bearing regressions:
  - a findings transcript parses priorities, single + span line ranges, abs-path
    normalization, and multi-line bodies attached to the right finding;
  - a clean (no-header) transcript reads clean;
  - format drift (header with no parsable findings; a `- [P` line with no header) fails
    fast (exit 2) rather than a false `clean`;
  - preconditions (codex on PATH, git work tree, clean tracked worktree, resolvable base)
    exercised against hermetic tmp git repos.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from conftest import load_scripts_module

if TYPE_CHECKING:
    import pytest as _pytest

clr = load_scripts_module("codex_local_review")

WORKTREE = Path("/Users/adam/Code/registry-research-toolkit/.claude/worktrees/adoring")

# --- captured fixtures (codex-cli 0.142.5) -------------------------------------------

FINDINGS_TRANSCRIPT = f"""\
codex
The patch adds a file that explicitly must not be merged, and the helper functions in \
that file have observable correctness bugs. It should not be considered correct as-is.

Full review comments:

- [P1] Remove the temporary probe before merging — {WORKTREE}/scripts/tmp_review_probe.py:2-2
  This new script explicitly says it is a temporary review-timing probe and that the PR \
should be closed without merging.

- [P2] Fix rolling_mean's shared default output — {WORKTREE}/scripts/tmp_review_probe.py:13-13
  When callers omit `out`, the default list is created once and reused across calls, so \
independent calls accumulate previous results.
"""

CLEAN_TRANSCRIPT = """\
codex
The diff against the requested base is empty, so there are no introduced code changes to \
flag.
"""


# --- parse_transcript: findings ------------------------------------------------------


def test_findings_transcript_parses_both_findings() -> None:
    out = clr.parse_transcript(FINDINGS_TRANSCRIPT, worktree_root=WORKTREE)

    assert out["verdict"] == "findings"
    assert [f["priority"] for f in out["findings"]] == ["P1", "P2"]
    assert out["findings"][0]["title"] == "Remove the temporary probe before merging"
    assert out["findings"][1]["title"] == "Fix rolling_mean's shared default output"


def test_findings_paths_are_repo_relative() -> None:
    # The captured paths are absolute under the worktree root; they must be normalized.
    out = clr.parse_transcript(FINDINGS_TRANSCRIPT, worktree_root=WORKTREE)

    assert all(f["path"] == "scripts/tmp_review_probe.py" for f in out["findings"])


def test_findings_single_line_range() -> None:
    out = clr.parse_transcript(FINDINGS_TRANSCRIPT, worktree_root=WORKTREE)

    assert out["findings"][0]["line_start"] == 2
    assert out["findings"][0]["line_end"] == 2


def test_findings_span_line_range() -> None:
    transcript = (
        "codex\nSummary.\n\nFull review comments:\n\n"
        "- [P1] Span title — pkg/mod.py:13-40\n  Body line.\n"
    )
    out = clr.parse_transcript(transcript, worktree_root=WORKTREE)

    assert out["findings"][0]["line_start"] == 13
    assert out["findings"][0]["line_end"] == 40


def test_findings_multi_line_body_attaches_to_right_finding() -> None:
    transcript = (
        "codex\nSummary.\n\nFull review comments:\n\n"
        "- [P1] First — a.py:1-1\n  Body A line one.\n  Body A line two.\n\n"
        "- [P2] Second — b.py:2-2\n  Body B only.\n"
    )
    out = clr.parse_transcript(transcript, worktree_root=WORKTREE)

    assert out["findings"][0]["body"] == "Body A line one.\nBody A line two."
    assert out["findings"][1]["body"] == "Body B only."


def test_findings_title_with_em_dash_splits_on_last_dash() -> None:
    # A title containing an em dash must keep its text; only the trailing `path:line` splits.
    transcript = (
        "codex\nSummary.\n\nFull review comments:\n\n"
        "- [P3] Guard nil — really — the edge case — src/x.py:5-9\n  Body.\n"
    )
    out = clr.parse_transcript(transcript, worktree_root=WORKTREE)

    assert out["findings"][0]["title"] == "Guard nil — really — the edge case"
    assert out["findings"][0]["path"] == "src/x.py"
    assert out["findings"][0]["line_start"] == 5


def test_relative_path_finding_is_left_as_is() -> None:
    transcript = (
        "codex\nSummary.\n\nFull review comments:\n\n"
        "- [P2] Rel path — reg_meta/db.py:7-7\n  Body.\n"
    )
    out = clr.parse_transcript(transcript, worktree_root=WORKTREE)

    assert out["findings"][0]["path"] == "reg_meta/db.py"


# --- parse_transcript: clean ---------------------------------------------------------


def test_clean_transcript_no_header_is_clean() -> None:
    out = clr.parse_transcript(CLEAN_TRANSCRIPT, worktree_root=WORKTREE)

    assert out["verdict"] == "clean"
    assert out["findings"] == []


def test_prose_clean_with_reviewed_diff_is_clean() -> None:
    # A reviewed-but-clean diff is also prose with no header — must read clean.
    transcript = (
        "codex\nI reviewed the introduced changes and found no issues worth flagging.\n"
    )
    out = clr.parse_transcript(transcript, worktree_root=WORKTREE)

    assert out["verdict"] == "clean"


# --- parse_transcript: format drift → exit 2 -----------------------------------------


def test_header_with_no_parsable_findings_is_format_drift() -> None:
    transcript = "codex\nSummary.\n\nFull review comments:\n\nSome prose, no entries.\n"

    with pytest.raises(clr.PreconditionError, match="header present but no findings"):
        clr.parse_transcript(transcript, worktree_root=WORKTREE)


def test_finding_line_without_header_is_format_drift() -> None:
    # A `- [P` line after the last codex marker but with NO header must fail fast, not
    # silently read as clean.
    transcript = "codex\nSummary text.\n- [P1] Stray finding — a.py:1-1\n  Body.\n"

    with pytest.raises(clr.PreconditionError, match="no .* header"):
        clr.parse_transcript(transcript, worktree_root=WORKTREE)


# --- preconditions (hermetic tmp git repos) ------------------------------------------


@pytest.fixture
def git_repo(tmp_path: Path, monkeypatch: _pytest.MonkeyPatch) -> Path:
    """A hermetic tmp git repo with one commit, cwd chdir'd into it, GIT_* env cleared.

    Mirrors the scripts/tests hermetic pattern: delete GIT_DIR/GIT_WORK_TREE so an ambient
    worktree env (the pre-push hook hijack) can't leak in and point git at the real repo.
    """
    monkeypatch.delenv("GIT_DIR", raising=False)
    monkeypatch.delenv("GIT_WORK_TREE", raising=False)
    monkeypatch.chdir(tmp_path)
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@e", "GIT_COMMITTER_NAME": "t",
           "GIT_COMMITTER_EMAIL": "t@e"}  # fmt: skip
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=tmp_path, check=True)
    (tmp_path / "f.txt").write_text("hello\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "commit", "-q", "-m", "init"], cwd=tmp_path, check=True, env={**env}
    )
    return tmp_path


def _codex_on_path(monkeypatch: _pytest.MonkeyPatch, present: bool = True) -> None:
    monkeypatch.setattr(
        clr.shutil, "which", lambda _name: "/usr/bin/codex" if present else None
    )


def test_precondition_missing_codex_errors(
    git_repo: Path, monkeypatch: _pytest.MonkeyPatch
) -> None:
    _codex_on_path(monkeypatch, present=False)

    with pytest.raises(clr.PreconditionError, match="codex CLI not found"):
        clr.check_preconditions("main", cwd=git_repo)


def test_precondition_not_a_git_worktree_errors(
    tmp_path: Path, monkeypatch: _pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("GIT_DIR", raising=False)
    monkeypatch.delenv("GIT_WORK_TREE", raising=False)
    _codex_on_path(monkeypatch)

    with pytest.raises(clr.PreconditionError, match="not inside a git work tree"):
        clr.check_preconditions("main", cwd=tmp_path)


def test_precondition_dirty_tracked_worktree_errors(
    git_repo: Path, monkeypatch: _pytest.MonkeyPatch
) -> None:
    _codex_on_path(monkeypatch)
    (git_repo / "f.txt").write_text("modified\n", encoding="utf-8")  # tracked edit

    with pytest.raises(clr.PreconditionError, match="tracked worktree is dirty"):
        clr.check_preconditions("main", cwd=git_repo)


def test_precondition_untracked_file_is_ok(
    git_repo: Path, monkeypatch: _pytest.MonkeyPatch
) -> None:
    # Untracked files are invisible to the committed diff, so they must NOT block the review.
    _codex_on_path(monkeypatch)
    (git_repo / "untracked.txt").write_text("scratch\n", encoding="utf-8")

    head, merge_base = clr.check_preconditions("main", cwd=git_repo)

    assert head and merge_base  # both resolve; no error


def test_precondition_unresolvable_explicit_base_errors(
    git_repo: Path, monkeypatch: _pytest.MonkeyPatch
) -> None:
    _codex_on_path(monkeypatch)

    with pytest.raises(clr.PreconditionError, match="does not resolve"):
        clr.check_preconditions("no-such-ref", cwd=git_repo)


def test_default_base_falls_back_to_main(
    git_repo: Path, monkeypatch: _pytest.MonkeyPatch
) -> None:
    # origin/main doesn't exist in the hermetic repo; the default base must fall back to main
    # (which does), so preconditions pass and merge-base resolves to HEAD.
    _codex_on_path(monkeypatch)

    head, merge_base = clr.check_preconditions("origin/main", cwd=git_repo)

    assert head == merge_base  # single commit: merge-base of HEAD and main is HEAD


# --- review(): parse is driven by the (stubbed) codex transcript ---------------------


def test_review_stubs_codex_and_reports_verdict(
    git_repo: Path, monkeypatch: _pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # End-to-end wiring without the real binary: stub run_codex to write + return a fixture
    # transcript, and assert review() threads the parse + metadata into the result dict.
    _codex_on_path(monkeypatch)
    out_path = tmp_path / "transcript.md"

    def fake_run_codex(merge_base, out, *, cwd, timeout_s):
        out.write_text(FINDINGS_TRANSCRIPT, encoding="utf-8")
        return FINDINGS_TRANSCRIPT

    monkeypatch.setattr(clr, "run_codex", fake_run_codex)

    result = clr.review(base="main", out_path=out_path, cwd=git_repo, timeout_s=1800.0)

    assert result["verdict"] == "findings"
    assert len(result["findings"]) == 2
    assert result["output_path"] == str(out_path)
    assert result["head"] == result["merge_base"]  # single commit
    assert "duration_s" in result


def test_run_codex_timeout_maps_to_precondition_error(
    git_repo: Path, monkeypatch: _pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # A codex timeout must surface as a PreconditionError (→ exit 2), not an uncaught
    # TimeoutExpired. Stub subprocess.run to raise it.
    def raise_timeout(*_a, **_k):
        raise subprocess.TimeoutExpired(cmd="codex", timeout=1.0)

    monkeypatch.setattr(clr.subprocess, "run", raise_timeout)

    with pytest.raises(clr.PreconditionError, match="timed out"):
        clr.run_codex("deadbeef", tmp_path / "t.md", cwd=git_repo, timeout_s=1.0)
