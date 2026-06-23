"""Unit tests for scripts/pr_review_status.py — the Codex bot-review-window classifier.

The deterministic core (`classify` signal resolution + the `messages` extraction +
`_head_push_ts`) is pinned here; the gh fetchers (`evaluate`, `poll`, `main`) are covered
by a live run, matching the sibling scripts. Load-bearing regressions pinned below:
  - the login split (reviews `chatgpt-codex-connector`, reactions `…[bot]`);
  - the two merge-gating verdicts are bound to the head commit by SHA, never a timestamp,
    so a stale verdict from a prior push can't read as fresh (a bare 👍 is NOT clean);
  - the #692 false-positive (clean verdict with stale off-head findings reviews).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "pr_review_status", _SCRIPTS / "pr_review_status.py"
)
assert _SPEC and _SPEC.loader
prs = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = prs
_SPEC.loader.exec_module(prs)

BOT_REVIEW = "chatgpt-codex-connector"  # reviews + comments
BOT_REACT = "chatgpt-codex-connector[bot]"  # reactions
HUMAN = "octocat"
HEAD = "abcdef1234567890"  # the PR's current head commit (hex, like a real SHA)
OLD = "fedcba0987654321"  # a pre-fix commit that is no longer head
PUSH = "2026-06-22T12:00:00Z"  # the head commit's committedDate (window-start floor)
BEFORE = "2026-06-22T11:00:00Z"
AFTER1 = "2026-06-22T12:30:00Z"
AFTER2 = "2026-06-22T13:00:00Z"
AFTER3 = "2026-06-22T13:30:00Z"
USAGE_LIMIT = "You have reached your Codex usage limits for code reviews."


def _review(
    login, *, state="COMMENTED", commit_id=HEAD, at=AFTER1, url="r", body="summary"
):
    return {
        "user": {"login": login},
        "state": state,
        "commit_id": commit_id,
        "submitted_at": at,
        "html_url": url,
        "body": body,
    }


def _comment(login, body, *, reviewed_commit=None, at=AFTER1, url="c"):
    if reviewed_commit:
        body = f"{body}\n\n**Reviewed commit:** `{reviewed_commit}`"
    return {"user": {"login": login}, "body": body, "created_at": at, "html_url": url}


def _clean_comment(*, commit_id=HEAD, at=AFTER1, url="c"):
    """Codex's no-issues narration comment, stamped with the reviewed commit SHA."""
    return _comment(
        BOT_REVIEW, "Codex Review: Didn't find any major issues.",
        reviewed_commit=commit_id, at=at, url=url,
    )  # fmt: skip


def _reaction(content, login, *, at=AFTER1):
    return {"content": content, "user": {"login": login}, "created_at": at}


def _review_request(*, login=HUMAN, at=AFTER1):
    """A human `@codex review` comment — re-opens the window (raises the window start)."""
    return {"user": {"login": login}, "body": "@codex review", "created_at": at}


def _review_comment(
    login, body, *, commit_id=HEAD, at=AFTER1, url="rc", path="f.py", line=1
):
    return {
        "user": {"login": login},
        "commit_id": commit_id,
        "created_at": at,
        "html_url": url,
        "path": path,
        "line": line,
        "body": body,
    }


def _classify(
    *,
    committed_date=PUSH,
    reviews=None,
    comments=None,
    reactions=None,
    review_comments=None,
):
    return prs.classify(
        head_oid=HEAD,
        committed_date=committed_date,
        reviews=reviews or [],
        comments=comments or [],
        reactions=reactions or [],
        review_comments=review_comments or [],
    )


# --- core signals --------------------------------------------------------------------


def test_clean_is_head_bound_narration_comment() -> None:
    out = _classify(comments=[_clean_comment()])
    assert out["signal"] == "clean"
    assert out["settled"] is True
    assert out["verdict_ts"] == "2026-06-22T12:30:00+00:00"


def test_clean_matches_an_abbreviated_sha() -> None:
    # Codex stamps an abbreviated SHA; a prefix of the full head_oid must still bind.
    out = _classify(comments=[_clean_comment(commit_id=HEAD[:10])])
    assert out["signal"] == "clean"


def test_review_from_no_suffix_login_is_findings() -> None:
    # Regression: the review login has NO `[bot]` suffix; the matcher must still catch it.
    out = _classify(reviews=[_review(BOT_REVIEW, state="CHANGES_REQUESTED")])
    assert out["signal"] == "findings"
    assert out["detail"] == {"review_state": "CHANGES_REQUESTED", "url": "r"}


def test_both_login_forms_are_recognized() -> None:
    # The no-suffix review/comment login AND the `[bot]` reaction login must both resolve —
    # pinning that the matcher sees both halves of the same bot.
    assert _classify(reviews=[_review(BOT_REVIEW)])["signal"] == "findings"
    assert _classify(reactions=[_reaction("eyes", BOT_REACT)])["signal"] == "reviewing"


def test_impostor_login_is_rejected() -> None:
    # The login is matched EXACTLY (not as a substring), so an actor like
    # `chatgpt-codex-connector-test` cannot pose as the bot to settle the gate.
    impostor = f"{BOT_REVIEW}-test"
    out = _classify(
        comments=[_comment(impostor, "Codex Review: no issues", reviewed_commit=HEAD)],
        reactions=[_reaction("+1", impostor)],
        reviews=[_review(impostor, state="CHANGES_REQUESTED")],
    )
    assert out["signal"] == "none"


def test_eyes_only_is_reviewing_and_unsettled() -> None:
    out = _classify(reactions=[_reaction("eyes", BOT_REACT)])
    assert out["signal"] == "reviewing"
    assert out["settled"] is False


def test_usage_limit_comment_is_exhausted_and_settled() -> None:
    out = _classify(comments=[_comment(BOT_REVIEW, USAGE_LIMIT)])
    assert out["signal"] == "exhausted"
    assert out["settled"] is True


def test_empty_is_none() -> None:
    out = _classify()
    assert out["signal"] == "none"
    assert out["settled"] is False
    assert out["verdict_ts"] is None


# --- staleness gates (head-bound verdicts; timestamp only for the safe-direction ones) --


def test_review_on_non_head_commit_is_ignored() -> None:
    # Reviews are bound to the head commit by `commit_id`, not by timestamp — a review on a
    # superseded commit (even one "after" the push clock) is stale and can't read as fresh.
    out = _classify(reviews=[_review(BOT_REVIEW, commit_id=OLD, at=AFTER2)])
    assert out["signal"] == "none"


def test_clean_comment_for_non_head_commit_is_ignored() -> None:
    # The clean verdict is bound to the head via its "Reviewed commit:" SHA — a no-issues
    # comment stamped with a prior commit must not settle the current HEAD as clean.
    out = _classify(comments=[_clean_comment(commit_id=OLD, at=AFTER2)])
    assert out["signal"] == "none"


def test_thumbs_up_within_window_is_clean() -> None:
    # When Codex finishes a no-suggestions review it may post ONLY a 👍 (no SHA comment).
    # An in-window 👍 is that clean verdict — missing it would hang the gate to the ceiling.
    out = _classify(reactions=[_reaction("+1", BOT_REACT, at=AFTER1)])
    assert out["signal"] == "clean"
    assert out["settled"] is True


def test_thumbs_up_before_window_start_is_ignored() -> None:
    # A 👍 from a *prior* review window (before the latest @codex-review request) is stale
    # and must not settle the re-triggered run — the window-start gate, not a commit clock.
    out = _classify(
        comments=[_review_request(at=AFTER2)],  # raises window start to AFTER2
        reactions=[_reaction("+1", BOT_REACT, at=AFTER1)],  # older 👍 → excluded
    )
    assert out["signal"] == "none"


def test_thumbs_up_clean_trusts_the_committed_date_floor() -> None:
    # Documents the accepted ceiling for the commit-unbound 👍-clean: with no @codex-review
    # request, the window floor is the head's committedDate, so a 👍 after it reads clean.
    # (A date-preserving rebase that lowers committedDate below a stale 👍 is the residual
    # the `simplify:` note names; the workflow re-triggers @codex-review to raise the floor.)
    out = _classify(
        committed_date=PUSH,
        reactions=[_reaction("+1", BOT_REACT, at=AFTER1)],
    )
    assert out["signal"] == "clean"
    # a 👍 at/before the committedDate floor is excluded (strict >)
    stale = _classify(
        committed_date=AFTER1, reactions=[_reaction("+1", BOT_REACT, at=BEFORE)]
    )
    assert stale["signal"] == "none"


def test_stale_exhausted_before_review_window_is_ignored() -> None:
    # Codex finding A: a usage-limit comment after the head commit but before the current
    # @codex-review request is from a prior window — it must NOT settle the new run as
    # exhausted (which would prematurely end the bot-review window on unreviewed changes).
    out = _classify(
        comments=[
            _comment(BOT_REVIEW, USAGE_LIMIT, at=AFTER1),  # stale, pre-request
            _review_request(at=AFTER2),  # window reopens here
        ],
    )
    assert out["signal"] == "none"
    # but a usage-limit AFTER the request is in-window → exhausted
    fresh = _classify(
        comments=[
            _review_request(at=AFTER2),
            _comment(BOT_REVIEW, USAGE_LIMIT, at=AFTER3),
        ],
    )
    assert fresh["signal"] == "exhausted"


def test_exhausted_before_committed_date_is_ignored() -> None:
    # Floor of the window is the head commit time; an out-of-tokens note before it is stale.
    out = _classify(comments=[_comment(BOT_REVIEW, USAGE_LIMIT, at=BEFORE)])
    assert out["signal"] == "none"


def test_pending_and_dismissed_reviews_are_ignored() -> None:
    out = _classify(
        reviews=[
            {
                "user": {"login": BOT_REVIEW},
                "state": "PENDING",
                "commit_id": HEAD,
                "submitted_at": None,
            },  # fmt: skip
            _review(BOT_REVIEW, state="DISMISSED"),
        ]
    )
    assert out["signal"] == "none"


# --- precedence: newest-eyes vs strongest-verdict ------------------------------------


def test_eyes_then_clean_comment_is_clean() -> None:
    # 👀 then a later clean verdict: review finished → clean (newest non-reviewing event).
    out = _classify(
        comments=[_clean_comment(at=AFTER2)],
        reactions=[_reaction("eyes", BOT_REACT, at=AFTER1)],
    )
    assert out["signal"] == "clean"


def test_clean_then_eyes_is_reviewing() -> None:
    # A clean verdict then a later 👀: Codex re-opened the review → never conclude.
    out = _classify(
        comments=[_clean_comment(at=AFTER1)],
        reactions=[_reaction("eyes", BOT_REACT, at=AFTER2)],
    )
    assert out["signal"] == "reviewing"


def test_findings_outrank_a_newer_clean() -> None:
    # Priority surfaces actionable feedback over a no-issues note, even if the note is newer.
    out = _classify(
        reviews=[_review(BOT_REVIEW, at=AFTER1)],
        comments=[_clean_comment(at=AFTER2)],
    )
    assert out["signal"] == "findings"


def test_findings_outrank_exhausted() -> None:
    out = _classify(
        reviews=[_review(BOT_REVIEW, at=AFTER1)],
        comments=[_comment(BOT_REVIEW, USAGE_LIMIT, at=AFTER2)],
    )
    assert out["signal"] == "findings"


def test_clean_outranks_exhausted() -> None:
    # Pins the `clean > exhausted` priority rung: a clean verdict and an out-of-tokens note
    # on the same HEAD must read clean. Without this, reordering the tuple regresses silently.
    out = _classify(
        comments=[
            _clean_comment(at=AFTER1),
            _comment(BOT_REVIEW, USAGE_LIMIT, at=AFTER2),
        ],
    )
    assert out["signal"] == "clean"


def test_clean_with_stale_off_head_findings_reviews() -> None:
    # The PR #692 false-positive: a clean verdict, preceded by findings reviews on the
    # *pre-fix* commits. Must read clean — the stale reviews are off-head (gated out).
    out = _classify(
        reviews=[
            _review(BOT_REVIEW, commit_id=OLD),
            _review(BOT_REVIEW, commit_id=OLD),
        ],
        comments=[_clean_comment()],
    )
    assert out["signal"] == "clean"
    assert out["settled"] is True


def test_findings_keeps_most_recent_instance() -> None:
    out = _classify(
        reviews=[
            _review(BOT_REVIEW, at=AFTER1, url="old"),
            _review(BOT_REVIEW, at=AFTER2, url="new"),
        ]
    )
    assert out["detail"]["url"] == "new"


# --- non-bot noise -------------------------------------------------------------------


def test_human_activity_is_ignored() -> None:
    out = _classify(
        reviews=[_review(HUMAN, state="CHANGES_REQUESTED")],
        comments=[
            _comment(HUMAN, "looks good", reviewed_commit=HEAD),
            _comment(HUMAN, USAGE_LIMIT),
        ],
        reactions=[_reaction("eyes", HUMAN)],
    )
    assert out["signal"] == "none"


# --- messages (the bodies the caller would otherwise re-fetch) -----------------------


def test_clean_surfaces_narration_comment_in_messages() -> None:
    # The reported scenario: a clean verdict whose narration text rides along in `messages`
    # so the caller never re-reads it to "confirm it's not a finding".
    out = _classify(comments=[_clean_comment()], reactions=[_reaction("+1", BOT_REACT)])
    assert out["signal"] == "clean"
    assert [m["kind"] for m in out["messages"]] == ["comment"]
    assert "Didn't find any major issues" in out["messages"][0]["body"]


def test_findings_messages_carry_review_summary_and_inline_comments() -> None:
    out = _classify(
        reviews=[_review(BOT_REVIEW, body="Automated review suggestions")],
        review_comments=[
            _review_comment(BOT_REVIEW, "Guard the nil case", path="a.py", line=7)
        ],
    )
    assert out["signal"] == "findings"
    assert {m["kind"] for m in out["messages"]} == {"review", "review_comment"}
    inline = next(m for m in out["messages"] if m["kind"] == "review_comment")
    assert (inline["body"], inline["path"], inline["line"]) == (
        "Guard the nil case",
        "a.py",
        7,
    )


def test_messages_exclude_off_head_and_pre_push_and_human() -> None:
    out = _classify(
        reviews=[_review(BOT_REVIEW, commit_id=OLD, body="stale review")],
        review_comments=[_review_comment(BOT_REVIEW, "stale inline", commit_id=OLD)],
        comments=[
            _comment(BOT_REVIEW, "pre-push narration", at=BEFORE),
            _comment(HUMAN, "human comment"),
        ],
    )
    assert out["messages"] == []


# --- _head_push_ts -------------------------------------------------------------------


def test_head_push_ts_picks_the_head_commit() -> None:
    pr = {
        "headRefOid": "bbb",
        "commits": [
            {"oid": "aaa", "committedDate": AFTER1},
            {"oid": "bbb", "committedDate": PUSH},
        ],
    }
    assert prs._head_push_ts(pr) == PUSH


def test_head_push_ts_falls_back_to_newest_commit() -> None:
    pr = {
        "headRefOid": "missing",
        "commits": [
            {"oid": "aaa", "committedDate": BEFORE},
            {"oid": "bbb", "committedDate": AFTER2},
        ],
    }
    assert prs._head_push_ts(pr) == AFTER2


def test_head_push_ts_empty_commits_exits_with_tool_error() -> None:
    # Degenerate (near-unreachable) PR with no commits must fail fast as a tool error
    # (exit 2), not die with a bare ValueError under a misleading exit code.
    with pytest.raises(SystemExit) as exc:
        prs._head_push_ts({"headRefOid": "x", "commits": []})
    assert exc.value.code == 2
