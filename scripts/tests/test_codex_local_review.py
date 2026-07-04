"""Unit tests for scripts/codex_local_review.py — the local `codex review` launcher.

The deterministic core (`parse_transcript` + the precondition logic) is pinned here; the
codex launch leaf (`run_codex`) is never invoked against the real binary — tests stub the
`subprocess.Popen` it drives. Fixtures below are captured from live runs (codex-cli
0.142.5). Load-bearing regressions:
  - a findings transcript parses priorities, single + span line ranges, abs-path
    normalization, and multi-line bodies attached to the right finding, under BOTH the
    `Full review comments:` (multi) and singular `Review comment:` (one-finding) headers;
  - a clean (no-header) transcript reads clean;
  - format drift (header with no parsable findings; a header whose `- [P` count disagrees
    with the parsed findings; `[P10]`; a `- [P` line with no header) fails fast (exit 2,
    kind format_drift) rather than a false `clean`;
  - fail-closed on a codex failure: empty transcript / nonzero exit surface as exit 2 with
    a classified kind (usage_limit vs tool_failure), never a silent clean;
  - only stdout is parsed — a `- [P…]` line in stderr neither creates a finding nor trips
    the drift guard, and stderr lands after the `--- stderr ---` delimiter in the evidence;
  - preconditions (codex on PATH, git work tree, clean tracked worktree, resolvable base)
    exercised against hermetic tmp git repos.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from conftest import load_scripts_module, make_git_repo

clr = load_scripts_module("codex_local_review")

# A neutral repo-relative-path root for parse tests (no real machine path).
WORKTREE = Path("/repo/worktree")

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

# codex-cli 0.142.5 emits the SINGULAR `Review comment:` header (no "Full") when there is
# exactly one finding. Captured live from a real single-finding review.
SINGLE_FINDING_TRANSCRIPT = f"""\
codex
The local Codex review launcher and instructions default to origin/main, which produces \
incorrect merge-gate review evidence for stacked or non-main-based PRs.

Review comment:

- [P2] Require callers to pass the PR base — {WORKTREE}/scripts/codex_local_review.py:468-470
  When the PR is stacked or otherwise targets a non-main base, every documented caller \
that omits `--base` falls back here, so the launcher reviews \
`merge-base(HEAD, origin/main)..HEAD` instead of the actual PR diff.
"""


# --- captured STDERR shapes (codex-cli 0.142.5) --------------------------------------
# codex emits its exec activity on STDERR: `exec` blocks, ` succeeded in <N>ms:` success
# markers, and (in a nested sandbox) the `sandbox_apply: Operation not permitted` denial.
# These mirror the shapes captured from the real PR #1078 run (success) and its sandboxed
# variant (denial); the SUCCESS marker is what parse_transcript never sees (it reads stdout).

# A real successful run's stderr: a codex banner + an exec block whose result line reports
# ` succeeded in 0ms:` — the marker the no-op-review backstop keys off.
SUCCESS_STDERR = """\
[2026-07-04T00:00:00] OpenAI Codex v0.142.5
[2026-07-04T00:00:00] exec bash -lc 'git diff --stat' in /repo
[2026-07-04T00:00:00] bash -lc 'git diff --stat' succeeded in 0ms:
 scripts/x.py | 2 +-
"""

# The nested-sandbox false-clean pair. stdout is the ~1KB prose codex prints when every exec
# failed (no findings header, no `- [P` lines — so all existing guards pass); stderr shows the
# exec block failing with the sandbox denial and NO ` succeeded in` marker.
SANDBOXED_CLEAN_STDOUT = """\
codex
I could not inspect the patch because the review environment refused to run any commands. \
No actionable code findings can be produced from the unavailable diff.
"""
SANDBOXED_DENIAL_STDERR = """\
[2026-07-04T00:00:00] OpenAI Codex v0.142.5
[2026-07-04T00:00:00] exec bash -lc 'git diff --stat' in /repo
[2026-07-04T00:00:00] bash -lc 'git diff --stat' failed: \
sandbox-exec: sandbox_apply: Operation not permitted
"""


# --- parse_transcript: findings ------------------------------------------------------


def test_findings_transcript_parses_both_findings() -> None:
    out = clr.parse_transcript(FINDINGS_TRANSCRIPT, worktree_root=WORKTREE)

    assert out["verdict"] == "findings"
    assert [f["priority"] for f in out["findings"]] == ["P1", "P2"]
    assert out["findings"][0]["title"] == "Remove the temporary probe before merging"
    assert out["findings"][1]["title"] == "Fix rolling_mean's shared default output"


def test_single_finding_review_comment_header_parses() -> None:
    # The SINGULAR `Review comment:` header (codex's one-finding form) must parse the same
    # as `Full review comments:`: one P2 finding, repo-relative path, body attached.
    out = clr.parse_transcript(SINGLE_FINDING_TRANSCRIPT, worktree_root=WORKTREE)

    assert out["verdict"] == "findings"
    assert len(out["findings"]) == 1
    finding = out["findings"][0]
    assert finding["priority"] == "P2"
    assert finding["title"] == "Require callers to pass the PR base"
    assert finding["path"] == "scripts/codex_local_review.py"
    assert finding["line_start"] == 468
    assert finding["line_end"] == 470
    assert finding["body"].startswith("When the PR is stacked")


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


def test_findings_single_number_ref_has_equal_start_end() -> None:
    # A `path:NN` ref (no range) parses with line_end == line_start.
    transcript = (
        "codex\nSummary.\n\nFull review comments:\n\n"
        "- [P1] Single line ref — path.py:13\n  Body.\n"
    )
    out = clr.parse_transcript(transcript, worktree_root=WORKTREE)

    assert out["findings"][0]["line_start"] == 13
    assert out["findings"][0]["line_end"] == 13


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


# --- parse_transcript: format drift → exit 2 (kind format_drift) ---------------------


def test_header_with_no_parsable_findings_is_format_drift() -> None:
    transcript = "codex\nSummary.\n\nFull review comments:\n\nSome prose, no entries.\n"

    with pytest.raises(
        clr.PreconditionError, match="header present but no findings"
    ) as e:
        clr.parse_transcript(transcript, worktree_root=WORKTREE)
    assert e.value.kind == clr.KIND_FORMAT_DRIFT


def test_finding_line_without_header_is_format_drift() -> None:
    # A `- [P` line after the last codex marker but with NO header must fail fast, not
    # silently read as clean.
    transcript = "codex\nSummary text.\n- [P1] Stray finding — a.py:1-1\n  Body.\n"

    with pytest.raises(clr.PreconditionError, match="no .* header") as e:
        clr.parse_transcript(transcript, worktree_root=WORKTREE)
    assert e.value.kind == clr.KIND_FORMAT_DRIFT


def test_separator_variant_line_is_format_drift_via_count_guard() -> None:
    # An en-dash (–, U+2013) separator instead of the em dash (—) fails FINDING_RE, so the
    # line is absorbed into the previous body and dropped from the findings list. The
    # count guard (bullet lines != parsed findings) must catch it rather than under-report.
    transcript = (
        "codex\nSummary.\n\nFull review comments:\n\n"
        "- [P1] Real finding — a.py:1-1\n  Body A.\n\n"
        "- [P2] Variant separator – b.py:2-2\n  Body B.\n"
    )
    with pytest.raises(clr.PreconditionError, match="separator variant") as e:
        clr.parse_transcript(transcript, worktree_root=WORKTREE)
    assert e.value.kind == clr.KIND_FORMAT_DRIFT


def test_p10_priority_is_format_drift_via_count_guard() -> None:
    # FINDING_RE pins P<single-digit>; a `[P10]` bullet fails to parse. With a parsable
    # `[P1]` finding alongside it, the header-present and zero-findings guards are both
    # satisfied, so it is specifically the COUNT guard (bullet lines != parsed findings)
    # that must fire — a regression lock against silently dropping a two-digit priority.
    transcript = (
        "codex\nSummary.\n\nFull review comments:\n\n"
        "- [P1] Real finding — a.py:1-1\n  Body.\n\n"
        "- [P10] Two-digit priority — b.py:2-2\n  Body.\n"
    )
    with pytest.raises(
        clr.PreconditionError, match="separator variant was dropped"
    ) as e:
        clr.parse_transcript(transcript, worktree_root=WORKTREE)
    assert e.value.kind == clr.KIND_FORMAT_DRIFT


# --- run_codex: fail-closed on codex failure -----------------------------------------


class _FakePopen:
    """Minimal Popen stand-in returning canned (stdout, stderr, returncode)."""

    def __init__(self, stdout: str, stderr: str, returncode: int) -> None:
        self._out, self._err, self.returncode, self.pid = (
            stdout,
            stderr,
            returncode,
            4242,
        )

    def communicate(self, timeout: float | None = None):  # noqa: ARG002
        return self._out, self._err


def _stub_popen(monkeypatch: pytest.MonkeyPatch, stdout, stderr, returncode) -> None:
    def factory(*_a, **kwargs):
        # Pin the Popen wiring the fail-closed guards depend on: a refactor to
        # stderr=subprocess.STDOUT (merging the streams parsing must keep apart) or dropping
        # start_new_session (so a timeout can't killpg the grandchild group) would keep every
        # test green while resurrecting the exact bugs round 1 fixed. Assert it here.
        assert kwargs["stdout"] is subprocess.PIPE
        assert kwargs["stderr"] is subprocess.PIPE
        assert kwargs.get("start_new_session") is True
        return _FakePopen(stdout, stderr, returncode)

    monkeypatch.setattr(clr.subprocess, "Popen", factory)


def test_run_codex_empty_transcript_is_tool_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Returncode 0 but no stdout must NOT parse as clean — it's a tool failure.
    _stub_popen(monkeypatch, "   \n", "", 0)

    with pytest.raises(clr.PreconditionError, match="no transcript") as e:
        clr.run_codex("deadbeef", tmp_path / "t.md", cwd=tmp_path, timeout_s=1.0)
    assert e.value.kind == clr.KIND_TOOL_FAILURE


def test_run_codex_usage_limit_on_stderr_is_usage_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The exact codex phrase on STDERR (its real channel for this message) classifies as the
    # exhausted-analog usage_limit.
    _stub_popen(monkeypatch, "", "You've reached your Codex usage limits.", 1)

    with pytest.raises(clr.PreconditionError) as e:
        clr.run_codex("deadbeef", tmp_path / "t.md", cwd=tmp_path, timeout_s=1.0)
    assert e.value.kind == clr.KIND_USAGE_LIMIT


def test_run_codex_usage_limit_on_stdout_only_is_tool_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Fail-closed direction: the SAME phrase on STDOUT only (PR-controlled transcript
    # content, e.g. codex quoting rate-limiting code from the diff) must NOT downgrade the
    # hard-blocker tool_failure into a merge-passable usage_limit — classification reads
    # stderr only.
    _stub_popen(monkeypatch, "You've reached your Codex usage limits.", "", 1)

    with pytest.raises(clr.PreconditionError) as e:
        clr.run_codex("deadbeef", tmp_path / "t.md", cwd=tmp_path, timeout_s=1.0)
    assert e.value.kind == clr.KIND_TOOL_FAILURE


def test_run_codex_nonzero_other_text_is_tool_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _stub_popen(monkeypatch, "", "some auth error", 1)

    with pytest.raises(clr.PreconditionError) as e:
        clr.run_codex("deadbeef", tmp_path / "t.md", cwd=tmp_path, timeout_s=1.0)
    assert e.value.kind == clr.KIND_TOOL_FAILURE


def test_run_codex_stderr_is_not_parsed_and_is_delimited(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # A `- [P…]` line and a `codex` marker in STDERR must not affect the parse: run_codex
    # returns stdout only, and the evidence file puts stderr after the delimiter. The
    # ` succeeded in 0ms:` line keeps the no-op-review backstop satisfied (a real run has one).
    stdout = "codex\nReviewed, no issues.\n"
    stderr = "codex\n- [P1] Stray in stderr — a.py:1-1\n git diff succeeded in 0ms:\n"
    _stub_popen(monkeypatch, stdout, stderr, 0)
    out_path = tmp_path / "t.md"

    returned = clr.run_codex("deadbeef", out_path, cwd=tmp_path, timeout_s=1.0)

    assert returned == stdout  # stderr excluded from the parsed stream
    # Parsing the returned stdout alone reads clean (the stderr finding is invisible).
    assert clr.parse_transcript(returned, worktree_root=tmp_path)["verdict"] == "clean"
    evidence = out_path.read_text(encoding="utf-8")
    assert clr.STDERR_DELIMITER in evidence
    assert evidence.index(clr.STDERR_DELIMITER) > evidence.index("no issues")


def test_run_codex_timeout_kills_group_and_writes_partial(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # A timeout must killpg the group (grandchildren inherit the pipes), write the partial
    # transcript as evidence, and raise kind timeout — not leave communicate() blocked.
    killed: list[int] = []

    class _TimingOutPopen:
        pid = 4242

        def __init__(self, *_a, **_k) -> None:
            self._calls = 0

        def communicate(self, timeout: float | None = None):  # noqa: ARG002
            self._calls += 1
            if self._calls == 1:
                raise subprocess.TimeoutExpired(cmd="codex", timeout=1.0)
            return "partial stdout", "partial stderr"

    monkeypatch.setattr(clr.subprocess, "Popen", lambda *a, **k: _TimingOutPopen())
    monkeypatch.setattr(clr.os, "killpg", lambda pid, sig: killed.append(pid))
    out_path = tmp_path / "t.md"

    with pytest.raises(clr.PreconditionError, match="timed out") as e:
        clr.run_codex("deadbeef", out_path, cwd=tmp_path, timeout_s=1.0)

    assert e.value.kind == clr.KIND_TIMEOUT
    assert killed == [4242]
    assert "partial stdout" in out_path.read_text(encoding="utf-8")


def test_run_codex_nested_sandbox_denial_is_tool_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The #1049 nested-sandbox false clean: codex exits 0 with a prose "could not inspect"
    # message (no findings header) while its stderr shows every exec denied by
    # `sandbox_apply: Operation not permitted`. Must fail-closed as tool_failure, not clean.
    # SANDBOXED_DENIAL_STDERR also has zero `succeeded in` markers, so both no-op guard
    # conditions hold — this pins guard ORDER: the denial guard firing first (match="sandbox")
    # proves it wins over the generic backstop.
    _stub_popen(monkeypatch, SANDBOXED_CLEAN_STDOUT, SANDBOXED_DENIAL_STDERR, 0)

    with pytest.raises(clr.PreconditionError, match="sandbox") as e:
        clr.run_codex("deadbeef", tmp_path / "t.md", cwd=tmp_path, timeout_s=1.0)
    assert e.value.kind == clr.KIND_TOOL_FAILURE


def test_run_codex_no_successful_exec_is_tool_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # A general no-op review with NO sandbox_apply denial: an exec block that merely "exited 1"
    # and NO ` succeeded in` marker anywhere. The backstop must still reject it as a failed
    # review (the initial `git diff` never succeeded, so nothing was inspected).
    stdout = "codex\nI was unable to inspect the diff; no findings can be produced.\n"
    stderr = (
        "[2026-07-04T00:00:00] exec bash -lc 'git diff --stat' in /repo\n"
        "[2026-07-04T00:00:00] bash -lc 'git diff --stat' exited 1\n"
    )
    _stub_popen(monkeypatch, stdout, stderr, 0)

    with pytest.raises(clr.PreconditionError, match="no successful exec") as e:
        clr.run_codex("deadbeef", tmp_path / "t.md", cwd=tmp_path, timeout_s=1.0)
    assert e.value.kind == clr.KIND_TOOL_FAILURE


def test_run_codex_success_marker_on_stdout_only_is_tool_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Fail-closed direction (mirrors test_run_codex_usage_limit_on_stdout_only_is_tool_failure):
    # a PR/prose-controlled `succeeded in <N>ms` phrase on STDOUT must NOT satisfy the no-op
    # backstop — only a real STDERR exec-success marker counts. Here stderr has none (and no
    # sandbox denial), so the backstop must fire even though stdout carries the phrase.
    stdout = "codex\nThe analysis succeeded in 75ms: but no command ran.\n"
    stderr = "[t] exec bash -lc 'git diff' in /repo\n[t] exec failed: some error\n"
    _stub_popen(monkeypatch, stdout, stderr, 0)

    with pytest.raises(clr.PreconditionError, match="no successful exec") as e:
        clr.run_codex("deadbeef", tmp_path / "t.md", cwd=tmp_path, timeout_s=1.0)
    assert e.value.kind == clr.KIND_TOOL_FAILURE


def test_run_codex_findings_transcript_passes_guards_unchanged(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The new stderr-scoped backstop must be a no-op ahead of a legitimate findings run: a real
    # findings stdout + a stderr with a `succeeded in` marker returns stdout unchanged (doesn't
    # eat real findings), and the pass-through stays parseable as findings.
    _stub_popen(monkeypatch, FINDINGS_TRANSCRIPT, SUCCESS_STDERR, 0)

    returned = clr.run_codex("deadbeef", tmp_path / "t.md", cwd=tmp_path, timeout_s=1.0)

    assert returned == FINDINGS_TRANSCRIPT
    assert (
        clr.parse_transcript(returned, worktree_root=tmp_path)["verdict"] == "findings"
    )


def test_run_codex_with_successful_exec_returns_stdout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Positive path: a real run has at least one ` succeeded in …` marker on stderr, so the
    # no-op-review guards must NOT fire — run_codex returns stdout for parsing.
    stdout = "codex\nReviewed the changes, no issues worth flagging.\n"
    _stub_popen(monkeypatch, stdout, SUCCESS_STDERR, 0)

    returned = clr.run_codex("deadbeef", tmp_path / "t.md", cwd=tmp_path, timeout_s=1.0)

    assert returned == stdout


@pytest.mark.parametrize(
    ("text", "matches"),
    [
        ("bash -lc 'git diff' succeeded in 0ms:", True),
        ("something succeeded in 1.2s:", True),
        ("long command succeeded in 3m:", True),
        ("the review succeeded in the end", False),
    ],
)
def test_exec_success_re_matches_timing_units_only(text: str, matches: bool) -> None:
    # The success marker regex accepts ms/s/m with an optional decimal, but not a prose
    # "succeeded in the …" — so a chatty final message can't masquerade as a run exec.
    assert bool(clr.EXEC_SUCCESS_RE.search(text)) is matches


# --- preconditions (hermetic tmp git repos) ------------------------------------------


@pytest.fixture
def git_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A hermetic tmp git repo with one commit, cwd chdir'd into it (see conftest)."""
    monkeypatch.chdir(tmp_path)
    return make_git_repo(tmp_path)


def _codex_on_path(monkeypatch: pytest.MonkeyPatch, present: bool = True) -> None:
    monkeypatch.setattr(
        clr.shutil, "which", lambda _name: "/usr/bin/codex" if present else None
    )


def test_precondition_missing_codex_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No git repo needed — the codex-on-PATH check is first.
    _codex_on_path(monkeypatch, present=False)

    with pytest.raises(clr.PreconditionError, match="codex CLI not found"):
        clr.check_preconditions("main", cwd=tmp_path)


def test_precondition_not_a_git_worktree_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _codex_on_path(monkeypatch)

    with pytest.raises(clr.PreconditionError, match="not inside a git work tree"):
        clr.check_preconditions("main", cwd=tmp_path)


def test_precondition_dirty_tracked_worktree_errors(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _codex_on_path(monkeypatch)
    (git_repo / "f.txt").write_text("modified\n", encoding="utf-8")  # tracked edit

    with pytest.raises(clr.PreconditionError, match="tracked worktree is dirty"):
        clr.check_preconditions("main", cwd=git_repo)


def test_precondition_untracked_file_is_ok(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Untracked files are invisible to the committed diff, so they must NOT block the review.
    _codex_on_path(monkeypatch)
    (git_repo / "untracked.txt").write_text("scratch\n", encoding="utf-8")

    head, merge_base = clr.check_preconditions("main", cwd=git_repo)

    assert head and merge_base  # both resolve; no error


def test_precondition_unresolvable_explicit_base_errors(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _codex_on_path(monkeypatch)

    with pytest.raises(clr.PreconditionError, match="does not resolve"):
        clr.check_preconditions("no-such-ref", cwd=git_repo)


def test_precondition_missing_origin_main_no_longer_falls_back(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The origin/main→main fallback was removed: origin/main doesn't resolve in a hermetic
    # repo, so it is a hard error now (no silent review against local main).
    _codex_on_path(monkeypatch)

    with pytest.raises(clr.PreconditionError, match="does not resolve"):
        clr.check_preconditions("origin/main", cwd=git_repo)


def test_precondition_explicit_main_base_resolves(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Callers pass --base main explicitly in hermetic repos; merge-base of HEAD and main is
    # HEAD in a single-commit repo.
    _codex_on_path(monkeypatch)

    head, merge_base = clr.check_preconditions("main", cwd=git_repo)

    assert head == merge_base


# --- review(): parse is driven by the (stubbed) codex transcript ---------------------


def test_review_stubs_codex_and_reports_verdict(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
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


# --- main(): argv wiring + exit codes + error JSON -----------------------------------


def _stub_review(monkeypatch: pytest.MonkeyPatch, verdict: str) -> None:
    def fake_review(*, base, out_path, cwd, timeout_s):
        return {
            "head": "h",
            "base": base,
            "merge_base": "h",
            "verdict": verdict,
            "findings": [],
            "output_path": str(out_path or "/tmp/x.md"),
            "duration_s": 0.1,
        }

    monkeypatch.setattr(clr, "review", fake_review)


def test_main_clean_exits_0(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _stub_review(monkeypatch, "clean")

    assert clr.main(["--base", "main"]) == 0
    assert '"verdict": "clean"' in capsys.readouterr().out


def test_main_findings_exits_1(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _stub_review(monkeypatch, "findings")

    assert clr.main(["--base", "main"]) == 1
    assert '"verdict": "findings"' in capsys.readouterr().out


def test_main_error_exits_2_and_emits_error_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # A PreconditionError (any exit-2 path) must exit 2, print the one-line message on
    # stderr, AND emit a machine-readable error object on stdout with the classified kind.
    def raise_usage_limit(*, base, out_path, cwd, timeout_s):
        raise clr.PreconditionError("usage limit reached", kind=clr.KIND_USAGE_LIMIT)

    monkeypatch.setattr(clr, "review", raise_usage_limit)

    assert clr.main(["--base", "main"]) == 2
    captured = capsys.readouterr()
    assert "usage limit reached" in captured.err
    payload = clr.json.loads(captured.out)
    assert payload["verdict"] == "error"
    assert payload["error"]["kind"] == clr.KIND_USAGE_LIMIT


def test_main_missing_base_is_argparse_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # --base is REQUIRED (no default): omitting it must be an argparse error, never a
    # silent review against a defaulted base. review() must never even be reached.
    monkeypatch.setattr(
        clr, "review", lambda **_k: pytest.fail("review() must not run without --base")
    )

    with pytest.raises(SystemExit) as e:
        clr.main([])
    assert e.value.code == 2  # argparse usage error


def test_main_uncaught_oserror_exits_2_as_tool_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # An uncaught OSError from review() (e.g. an unwritable --out parent, or mkdir onto a
    # file path) must NOT crash with Python's exit 1 — the contract reserves 1 for findings.
    # main() maps any non-PreconditionError to the exit-2 error contract, kind tool_failure.
    def raise_oserror(*, base, out_path, cwd, timeout_s):
        raise OSError("Not a directory: /some/file/parent/out.md")

    monkeypatch.setattr(clr, "review", raise_oserror)

    assert clr.main(["--base", "main"]) == 2
    captured = capsys.readouterr()
    assert "Not a directory" in captured.err
    payload = clr.json.loads(captured.out)
    assert payload["verdict"] == "error"
    assert payload["error"]["kind"] == clr.KIND_TOOL_FAILURE
