"""Unit tests for scripts/pr_review_status.py — the Codex bot-review-window classifier.

The deterministic core (`classify` signal resolution + `_head_push_ts`) is pinned here;
the gh fetchers (`evaluate`, `poll`, `main`) are covered by a live run, matching the
sibling scripts. The load-bearing regression is the login split: Codex submits
reviews/comments as `chatgpt-codex-connector` but reacts as
`chatgpt-codex-connector[bot]`, and a poller keyed on one login silently misses the other.
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

# Codex's two distinct logins + a human, and a fixed push reference with before/after ticks.
BOT_REVIEW = "chatgpt-codex-connector"  # reviews + comments
BOT_REACT = "chatgpt-codex-connector[bot]"  # reactions
HUMAN = "octocat"
HEAD = "headsha000"  # the PR's current head commit
OLD = "oldsha111"  # a pre-fix commit that is no longer head
PUSH = "2026-06-22T12:00:00Z"
BEFORE = "2026-06-22T11:00:00Z"
AFTER1 = "2026-06-22T12:30:00Z"
AFTER2 = "2026-06-22T13:00:00Z"
USAGE_LIMIT = "You have reached your Codex usage limits for code reviews."


def _review(login, *, state="COMMENTED", commit_id=HEAD, at=AFTER1, url="r"):
    return {
        "user": {"login": login},
        "state": state,
        "commit_id": commit_id,
        "submitted_at": at,
        "html_url": url,
    }


def _comment(login, body, *, at=AFTER1, url="c"):
    return {"user": {"login": login}, "body": body, "created_at": at, "html_url": url}


def _reaction(content, login, *, at=AFTER1):
    return {"content": content, "user": {"login": login}, "created_at": at}


def _classify(*, reviews=None, comments=None, reactions=None):
    return prs.classify(
        head_oid=HEAD,
        push_ts=PUSH,
        reviews=reviews or [],
        comments=comments or [],
        reactions=reactions or [],
    )


# --- core signals --------------------------------------------------------------------


def test_clean_is_thumbs_up_on_body() -> None:
    out = _classify(reactions=[_reaction("+1", BOT_REACT)])
    assert out["signal"] == "clean"
    assert out["settled"] is True
    assert out["verdict_ts"] == "2026-06-22T12:30:00+00:00"


def test_review_from_no_suffix_login_is_findings() -> None:
    # Regression: the review login has NO `[bot]` suffix; substring match must still catch.
    out = _classify(reviews=[_review(BOT_REVIEW, state="CHANGES_REQUESTED")])
    assert out["signal"] == "findings"
    assert out["detail"] == {"review_state": "CHANGES_REQUESTED", "url": "r"}


def test_both_login_forms_are_recognized() -> None:
    # The single substring match must cover the no-suffix review login AND the [bot] react
    # login — pinning that one poller sees both halves of the same bot.
    assert _classify(reviews=[_review(BOT_REVIEW)])["signal"] == "findings"
    assert _classify(reactions=[_reaction("+1", BOT_REACT)])["signal"] == "clean"


def test_narration_comment_alone_is_not_findings() -> None:
    # Codex's clean verdict ships as a "Codex Review: …" comment + a 👍. The comment is
    # narration — a top-level comment is never the findings vehicle (that's the review),
    # so on its own it must NOT read as findings.
    out = _classify(
        comments=[_comment(BOT_REVIEW, "Codex Review: Didn't find any issues")]
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


# --- staleness gates -----------------------------------------------------------------


def test_review_on_non_head_commit_is_ignored() -> None:
    # Reviews are bound to the head commit by `commit_id`, not by timestamp — a review on
    # a superseded commit (even one submitted "after" the push clock) is stale. This is the
    # rebase-proof gate: a stale verdict can never read as fresh findings.
    out = _classify(reviews=[_review(BOT_REVIEW, commit_id=OLD, at=AFTER2)])
    assert out["signal"] == "none"


def test_reactions_before_push_are_ignored() -> None:
    # Body reactions/comments are commit-unbound, so they fall back to the push timestamp.
    out = _classify(
        reactions=[_reaction("+1", BOT_REACT, at=BEFORE)],
        comments=[_comment(BOT_REVIEW, USAGE_LIMIT, at=BEFORE)],
    )
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


def test_eyes_then_thumbs_is_clean() -> None:
    # 👀 then a later 👍: review finished → clean (newest non-reviewing event).
    out = _classify(
        reactions=[
            _reaction("eyes", BOT_REACT, at=AFTER1),
            _reaction("+1", BOT_REACT, at=AFTER2),
        ]  # fmt: skip
    )
    assert out["signal"] == "clean"


def test_thumbs_then_eyes_is_reviewing() -> None:
    # 👍 then a later 👀: Codex re-opened the review → never conclude.
    out = _classify(
        reactions=[
            _reaction("+1", BOT_REACT, at=AFTER1),
            _reaction("eyes", BOT_REACT, at=AFTER2),
        ]  # fmt: skip
    )
    assert out["signal"] == "reviewing"


def test_findings_outrank_a_newer_thumbs_up() -> None:
    # Priority surfaces actionable feedback over a bare 👍, even if the 👍 is newer.
    out = _classify(
        reviews=[_review(BOT_REVIEW, at=AFTER1)],
        reactions=[_reaction("+1", BOT_REACT, at=AFTER2)],
    )
    assert out["signal"] == "findings"


def test_findings_outrank_exhausted() -> None:
    out = _classify(
        reviews=[_review(BOT_REVIEW, at=AFTER1)],
        comments=[_comment(BOT_REVIEW, USAGE_LIMIT, at=AFTER2)],
    )
    assert out["signal"] == "findings"


def test_clean_outranks_exhausted() -> None:
    # Pins the `clean > exhausted` priority rung: a 👍 and an out-of-tokens note on the
    # same HEAD must read clean (Codex both reviewed-clean and later re-ran out), not
    # exhausted. Without this, reordering the priority tuple regresses silently.
    out = _classify(
        reactions=[_reaction("+1", BOT_REACT, at=AFTER1)],
        comments=[_comment(BOT_REVIEW, USAGE_LIMIT, at=AFTER2)],
    )
    assert out["signal"] == "clean"


def test_clean_when_thumbs_up_accompanies_narration_comment() -> None:
    # The PR #692 false-positive: a clean verdict (👍 + narration comment), preceded by
    # findings reviews on the *pre-fix* commits. Must read clean, not findings — the stale
    # reviews are off-head (gated out) and the narration comment is not a verdict.
    out = _classify(
        reviews=[
            _review(BOT_REVIEW, commit_id=OLD),  # review on a pre-fix commit
            _review(BOT_REVIEW, commit_id=OLD),
        ],
        comments=[_comment(BOT_REVIEW, "Codex Review: Didn't find any major issues.")],
        reactions=[_reaction("+1", BOT_REACT)],
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
        comments=[_comment(HUMAN, "looks good")],
        reactions=[_reaction("+1", HUMAN), _reaction("eyes", HUMAN)],
    )
    assert out["signal"] == "none"


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
