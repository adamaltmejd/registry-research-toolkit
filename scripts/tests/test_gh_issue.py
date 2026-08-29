"""Unit tests for scripts/gh_issue.py — the maintainer-author trust gate.

The gate is fail-closed: a missing/None/non-maintainer author is dropped, never
surfaced. These pin that on the one ingestion read (the `view` CLI) and the
`REGISTRY_MAINTAINER_LOGIN` override. The gh calls are stubbed by patching
`gh_issue.subprocess.run`.
"""

from __future__ import annotations

import json
import types

import pytest

from conftest import load_scripts_module

gi = load_scripts_module("gh_issue")

MAINT = "adamaltmejd"


@pytest.fixture(autouse=True)
def _pin_maintainer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the maintainer via the env override so no test needs a live `gh repo view`."""
    monkeypatch.setenv("REGISTRY_MAINTAINER_LOGIN", MAINT)


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


# --- view CLI ------------------------------------------------------------------------


def _stub_view(monkeypatch: pytest.MonkeyPatch, payload: dict | None) -> None:
    """Stub `gh issue view`: payload=None models a non-zero exit (a genuinely missing
    number). A PR number does NOT error — it resolves with returncode 0 and a payload —
    so a PR case is modelled by passing a PR-shaped payload, never by payload=None.
    """

    def fake_run(cmd, capture_output, text, check):
        if payload is None:
            return types.SimpleNamespace(returncode=1, stdout="", stderr="not found")
        return types.SimpleNamespace(
            returncode=0, stdout=json.dumps(payload), stderr=""
        )

    monkeypatch.setattr(gi.subprocess, "run", fake_run)


def test_view_maintainer_issue_prints_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _stub_view(
        monkeypatch,
        {
            "number": 328,
            "title": "epic",
            "state": "OPEN",
            "body": "plan",
            "author": {"login": MAINT},
        },
    )
    assert gi.main(["view", "328"]) == gi.EXIT_OK
    out = json.loads(capsys.readouterr().out)
    assert out == {"number": 328, "title": "epic", "state": "OPEN", "body": "plan"}


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

    def fake_run(cmd, capture_output, text, check):
        captured["cmd"] = cmd
        return types.SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {"number": 1, "title": "t", "body": "b", "author": {"login": MAINT}}
            ),
            stderr="",
        )

    monkeypatch.setattr(gi.subprocess, "run", fake_run)
    gi.main(["view", "1", "--comments"])
    assert "state" in captured["cmd"][-1]
    assert "comments" in captured["cmd"][-1]


def test_cli_usage_error_exits_2() -> None:
    assert gi.main([]) == gi.EXIT_USAGE  # no subcommand
    assert gi.main(["bogus"]) == gi.EXIT_USAGE


# --- maintainer-login subcommand -----------------------------------------------------


def test_maintainer_login_subcommand_prints_login(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The env override (autouse fixture) pins the login; the subcommand prints exactly it,
    # so skill text can name one deterministic command for the author check.
    assert gi.main(["maintainer-login"]) == gi.EXIT_OK
    assert capsys.readouterr().out.strip() == MAINT
