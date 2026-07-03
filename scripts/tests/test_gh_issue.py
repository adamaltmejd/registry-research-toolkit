"""Unit tests for scripts/gh_issue.py — the maintainer-author trust gate.

The gate is fail-closed: a missing/None/non-maintainer author is dropped, never
surfaced. These pin that on the two ingestion reads (`fetch_open_issues`, the `view`
CLI), the fork check (`is_own_pr`), and the `REGISTRY_MAINTAINER_LOGIN` override. The gh
calls are stubbed the same way the sibling tests stub `gh_json` / `subprocess.run`.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("gh_issue", _SCRIPTS / "gh_issue.py")
assert _SPEC and _SPEC.loader
gi = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gi
_SPEC.loader.exec_module(gi)

MAINT = "adamaltmejd"


@pytest.fixture(autouse=True)
def _pin_maintainer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the maintainer via the env override so no test needs a live `gh repo view`."""
    monkeypatch.setenv("REGISTRY_MAINTAINER_LOGIN", MAINT)


def _issue(number: int, login: str | None) -> dict:
    """An `issue list` row; login=None models a missing/null author (fail-closed case)."""
    row = {"number": number, "title": f"t{number}", "labels": [], "body": "b"}
    row["author"] = {"login": login} if login is not None else None
    return row


# --- maintainer_login ----------------------------------------------------------------


def test_maintainer_login_uses_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REGISTRY_MAINTAINER_LOGIN", "someone-else")
    assert gi.maintainer_login() == "someone-else"


def test_maintainer_login_falls_back_to_repo_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REGISTRY_MAINTAINER_LOGIN", raising=False)
    monkeypatch.setattr(gi, "repo_owner_name", lambda: ("theowner", "therepo"))
    assert gi.maintainer_login() == "theowner"


def test_maintainer_login_empty_override_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An empty env var is not a login — fall back, don't allowlist "" (which matches no
    # author but would still be a footgun).
    monkeypatch.setenv("REGISTRY_MAINTAINER_LOGIN", "")
    monkeypatch.setattr(gi, "repo_owner_name", lambda: ("theowner", "therepo"))
    assert gi.maintainer_login() == "theowner"


# --- fetch_open_issues (fail-closed allowlist) ---------------------------------------


def test_fetch_open_issues_keeps_only_maintainer(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    rows = [
        _issue(1, MAINT),
        _issue(2, "stranger"),
        _issue(3, MAINT.upper()),  # case-insensitive match → kept
        _issue(4, None),  # null author → dropped (fail-closed)
    ]
    monkeypatch.setattr(gi, "gh_json", lambda args: rows)
    kept = gi.fetch_open_issues()
    assert sorted(r["number"] for r in kept) == [1, 3]
    # The drop count is reported to stderr (observability — never silent).
    assert "dropped 2 non-maintainer issue(s)" in capsys.readouterr().err


def test_fetch_open_issues_missing_author_key_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = {"number": 5, "title": "t", "labels": [], "body": "b"}  # no author key at all
    monkeypatch.setattr(gi, "gh_json", lambda args: [row])
    assert gi.fetch_open_issues() == []


def test_fetch_open_issues_requests_author_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, list[str]] = {}

    def fake(args: list[str]):
        captured["args"] = args
        return []

    monkeypatch.setattr(gi, "gh_json", fake)
    gi.fetch_open_issues()
    assert "author" in captured["args"][-1]  # the --json field list carries author


def test_fetch_open_issues_preserves_build_records_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # build_records reads number/title/labels/body — kept rows must still carry them.
    monkeypatch.setattr(gi, "gh_json", lambda args: [_issue(1, MAINT)])
    (row,) = gi.fetch_open_issues()
    assert {"number", "title", "labels", "body"} <= row.keys()


# --- is_own_pr (fork gate, fail-closed) ----------------------------------------------


@pytest.mark.parametrize(
    ("pr", "own"),
    [
        ({"isCrossRepository": False}, True),
        ({"isCrossRepository": True}, False),
        ({"isCrossRepository": None}, False),
        ({}, False),  # missing field → not own
    ],
)
def test_is_own_pr(pr: dict, own: bool) -> None:
    assert gi.is_own_pr(pr) is own


# --- view CLI ------------------------------------------------------------------------


def _stub_view(monkeypatch: pytest.MonkeyPatch, payload: dict | None) -> None:
    """Stub `gh issue view`: payload=None models a non-zero exit (a genuinely missing
    number). A PR number does NOT error — it resolves with returncode 0 and a payload —
    so a PR case is modelled by passing a PR-shaped payload, never by payload=None.
    """

    def fake_run(cmd, capture_output, text):
        if payload is None:
            return types.SimpleNamespace(returncode=1, stdout="", stderr="not found")
        return types.SimpleNamespace(
            returncode=0, stdout=json.dumps(payload), stderr=""
        )

    # `_fetch_issue` now routes through the shared `_gh.gh_issue_view_or_none`, which runs
    # `_gh.subprocess.run` — patch there, not `gi.subprocess`.
    monkeypatch.setattr(gi._gh.subprocess, "run", fake_run)


def test_view_maintainer_issue_prints_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _stub_view(
        monkeypatch,
        {"number": 328, "title": "epic", "body": "plan", "author": {"login": MAINT}},
    )
    assert gi.main(["view", "328"]) == gi.EXIT_OK
    out = json.loads(capsys.readouterr().out)
    assert out == {"number": 328, "title": "epic", "body": "plan"}


def test_view_non_maintainer_issue_refuses(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _stub_view(
        monkeypatch,
        {"number": 9, "title": "x", "body": "evil", "author": {"login": "stranger"}},
    )
    assert gi.main(["view", "9"]) == gi.EXIT_REFUSED
    captured = capsys.readouterr()
    assert captured.out == ""  # nothing surfaced
    assert "not maintainer-authored" in captured.err


def test_view_missing_number_refuses(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # A genuinely missing number → `gh issue view` exits non-zero → None → refused.
    _stub_view(monkeypatch, None)
    assert gi.main(["view", "1"]) == gi.EXIT_REFUSED
    assert capsys.readouterr().out == ""


def test_view_non_maintainer_pr_refuses(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # A PR number does NOT error — `gh issue view` resolves it with exit 0. So the PR case
    # reaches the author gate exactly like an issue: a stranger's fork PR (non-maintainer
    # author) is refused there, NOT because it "can't reach here". This is the test that
    # proves authorship — not issue-vs-PR — is the trust boundary.
    _stub_view(
        monkeypatch,
        {
            "number": 1024,
            "title": "pr",
            "body": "evil",
            "author": {"login": "stranger"},
        },
    )
    assert gi.main(["view", "1024"]) == gi.EXIT_REFUSED
    captured = capsys.readouterr()
    assert captured.out == ""  # nothing surfaced
    assert "not maintainer-authored" in captured.err


def test_view_comments_strips_non_maintainer(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _stub_view(
        monkeypatch,
        {
            "number": 328,
            "title": "epic",
            "body": "plan",
            "author": {"login": MAINT},
            "comments": [
                {"author": {"login": MAINT}, "body": "trusted"},
                {"author": {"login": "stranger"}, "body": "INJECT"},
                {"author": None, "body": "null-author"},  # fail-closed → dropped
            ],
        },
    )
    assert gi.main(["view", "328", "--comments"]) == gi.EXIT_OK
    out = json.loads(capsys.readouterr().out)
    assert [c["body"] for c in out["comments"]] == ["trusted"]


def test_view_without_comments_omits_field(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _stub_view(
        monkeypatch,
        {"number": 5, "title": "t", "body": "b", "author": {"login": MAINT}},
    )
    gi.main(["view", "5"])
    assert "comments" not in json.loads(capsys.readouterr().out)


def test_view_comments_requests_comments_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, list[str]] = {}

    def fake_run(cmd, capture_output, text):
        captured["cmd"] = cmd
        return types.SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {"number": 1, "title": "t", "body": "b", "author": {"login": MAINT}}
            ),
            stderr="",
        )

    monkeypatch.setattr(gi._gh.subprocess, "run", fake_run)
    gi.main(["view", "1", "--comments"])
    assert "comments" in captured["cmd"][-1]


def test_cli_usage_error_exits_2() -> None:
    assert gi.main([]) == gi.EXIT_USAGE  # no subcommand
    assert gi.main(["bogus"]) == gi.EXIT_USAGE


# --- is_maintainer_authored (public predicate, fail-closed) --------------------------


def test_is_maintainer_authored_true_for_maintainer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_view(
        monkeypatch,
        {"number": 328, "title": "epic", "body": "plan", "author": {"login": MAINT}},
    )
    assert gi.is_maintainer_authored(328) is True


def test_is_maintainer_authored_case_insensitive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_view(
        monkeypatch,
        {"number": 5, "title": "t", "body": "b", "author": {"login": MAINT.upper()}},
    )
    assert gi.is_maintainer_authored(5) is True


def test_is_maintainer_authored_false_for_stranger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_view(
        monkeypatch,
        {"number": 9, "title": "x", "body": "evil", "author": {"login": "stranger"}},
    )
    assert gi.is_maintainer_authored(9) is False


def test_is_maintainer_authored_false_for_missing_number(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A non-zero `gh issue view` (genuinely missing number) → _fetch_issue None → False.
    _stub_view(monkeypatch, None)
    assert gi.is_maintainer_authored(1) is False


def test_is_maintainer_authored_false_for_null_author(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_view(monkeypatch, {"number": 7, "title": "t", "body": "b", "author": None})
    assert gi.is_maintainer_authored(7) is False


def test_is_maintainer_authored_true_for_maintainer_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `gh issue view` resolves a PR number too (exit 0 + payload): a maintainer-authored PR
    # passes exactly like a maintainer issue — authorship, not issue-ness, is the boundary.
    _stub_view(
        monkeypatch,
        {"number": 1024, "title": "pr", "body": "b", "author": {"login": MAINT}},
    )
    assert gi.is_maintainer_authored(1024) is True


# --- maintainer-login subcommand -----------------------------------------------------


def test_maintainer_login_subcommand_prints_login(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The env override (autouse fixture) pins the login; the subcommand prints exactly it,
    # so skill text can name one deterministic command for the author check.
    assert gi.main(["maintainer-login"]) == gi.EXIT_OK
    assert capsys.readouterr().out.strip() == MAINT
