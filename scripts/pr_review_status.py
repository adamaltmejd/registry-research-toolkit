#!/usr/bin/env python3
"""Codex bot-review-window poller for the merge gate.

The AGENTS.md "PR merge gate" makes us poll Codex's signal on the *current HEAD* every PR
cycle — in `/pr-pipeline` Step E and in ad-hoc agent-driven PR work alike. The signal is
fiddly and has been shipped wrong before (Codex submits reviews/comments as login
`chatgpt-codex-connector` but reacts as `chatgpt-codex-connector[bot]`, so a poller keyed
on one login misses the other). This script is the get-it-right-once classifier: given a
PR number it fetches reviews, issue comments, and PR-body reactions, then reports the
Codex signal as JSON — reviews bound to the head commit, body reactions/comments gated on
the head commit's timestamp, so a verdict from before the latest push is stale.

Scope: this computes the **Codex** signal only — Codex is the bot whose 👍/👀-reaction
verdict is invisible to `gh pr view`. Copilot (the gate's other bot) posts ordinary
reviews that `gh pr view --json reviews` already surfaces, so it needs no special poller;
a `none`/`exhausted` here says nothing about Copilot. `exhausted` (Codex out of tokens) is
"settled" per the gate's "not a blocker", NOT "a bot reviewed and approved" — independent
review is a separate gate line the agent still owns.

Signals (see the gate doc):
  clean      — a 👍 reaction on the PR body from the Codex bot (its no-findings verdict,
               invisible to `gh pr view`).
  findings   — a submitted Codex review (its suggestions vehicle): go read it.
  reviewing  — a 👀 reaction from Codex with nothing newer: still reviewing, never
               conclude.
  exhausted  — a "reached your Codex usage limits" comment: definitive end-of-wait, NOT a
               blocker.
  none       — no Codex signal after the latest push yet.

Exit code answers only "is the window settled?": 0 settled (clean | findings | exhausted),
1 not settled (reviewing | none). 2 is a tool error (gh failed, or bad args). The agent
reads the JSON `signal`/`detail` for what to *do* — route findings to a fix, request
`@codex review` after a push, decide to merge. The script reports; the agent acts.

Stdlib only — run with `uv run --no-project python scripts/pr_review_status.py <pr>`. The
deterministic resolver (`classify`) is unit-tested; the gh fetchers are covered by a live
run, matching the sibling scripts.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# gh process primitives (run/gh_json/repo_owner_name) live in the shared _gh module, loaded
# via spec so it resolves regardless of sys.path (same idiom as plan_sequence.py).
_GHSPEC = importlib.util.spec_from_file_location(
    "_gh", Path(__file__).with_name("_gh.py")
)
assert _GHSPEC and _GHSPEC.loader
_gh = importlib.util.module_from_spec(_GHSPEC)
_GHSPEC.loader.exec_module(_gh)
gh_json = _gh.gh_json
repo_owner_name = _gh.repo_owner_name

# Codex's review/comment login is `chatgpt-codex-connector`; its reaction login is
# `chatgpt-codex-connector[bot]`. A case-insensitive substring match catches both — the
# exact bug a one-login poller hits. Override with --bot for a different reviewer bot.
DEFAULT_BOT = "codex"
# The out-of-tokens comment Codex posts instead of a review ("You have reached your Codex
# usage limits for code reviews"). Matched on a Codex-authored comment → `exhausted`.
USAGE_LIMIT_RE = re.compile(r"usage limit", re.IGNORECASE)
# GitHub reaction content strings for 👍 / 👀.
THUMBS_UP = "+1"
EYES = "eyes"
# Review states that count as a submitted verdict. PENDING (never submitted) is excluded by
# the submitted_at gate; DISMISSED is a withdrawn verdict, not an active one.
VERDICT_REVIEW_STATES = {"APPROVED", "CHANGES_REQUESTED", "COMMENTED"}

SETTLED = {"clean", "findings", "exhausted"}


def _parse_ts(ts: str) -> datetime:
    """Parse a GitHub ISO-8601 timestamp (…Z or …+00:00) to an aware datetime.

    `fromisoformat` handles the trailing `Z` natively on the repo's 3.14 floor.
    """
    return datetime.fromisoformat(ts)


def classify(
    *,
    head_oid: str,
    push_ts: str,
    reviews: list[dict[str, Any]],
    comments: list[dict[str, Any]],
    reactions: list[dict[str, Any]],
    bot: str = DEFAULT_BOT,
) -> dict[str, Any]:
    """Resolve the Codex signal from raw GitHub records. Pure — the tested core.

    Staleness is gated two ways. A **review** carries the `commit_id` it was submitted
    against, so we bind findings exactly to `head_oid` — rebase-proof, no timestamp guess.
    A **reaction/comment** is on the PR body, not a commit, so it can only be gated on
    `created_at > push_ts` (the head commit's timestamp). Resolution: if the *newest* Codex
    event is a 👀, Codex is mid-review → `reviewing` (the gate must never conclude then).
    Otherwise report the strongest verdict, by priority `findings > clean > exhausted` —
    surface actionable feedback over a bare 👍, a real verdict over an out-of-tokens note.

    How Codex actually signals (observed): it delivers **findings** as a submitted *review*
    on the head commit (body "automated review suggestions" + inline comments). It signals
    **clean** with a 👍 reaction on the PR body, accompanied by a narration *comment*
    ("Codex Review: Didn't find any major issues"). So a top-level comment is narration or
    the usage-limit note — never the findings vehicle; we read comments only for
    `exhausted`, and trust the review/👍 for findings/clean. (If Codex ever posts findings
    as a bare comment with no review, this misses them — unobserved; the agent reads the PR
    regardless.)

    simplify: the clean 👍 / 👀 reactions are gated on `committedDate`, not the true push
    time. Git stamps the committer date at rebase/amend/cherry-pick time (≈ the push), so
    this is accurate in the normal case; only a date-preserving rebase
    (`--committer-date-is-author-date`, replaying old commits) can leave `committedDate`
    earlier than the real push and let a stale 👍 read as fresh. Findings are commit-exact
    so that edge can't surface a false `findings`; the residual is a narrow false-`clean`,
    backstopped by the gate's independent `/code-review` pass and the `@codex review`
    re-request after a push. Tighten with the timeline force-push event if it ever bites.

    Record shapes (the subset used; GitHub's REST field names):
      review : {"user": {"login"}, "state", "commit_id", "submitted_at", "html_url"}
      comment: {"user": {"login"}, "body", "created_at", "html_url"}
      reaction:{"content", "user": {"login"}, "created_at"}
    """
    bot_re = re.compile(re.escape(bot), re.IGNORECASE)
    push = _parse_ts(push_ts)

    def is_bot(rec: dict[str, Any]) -> bool:
        return bool(bot_re.search((rec.get("user") or {}).get("login", "")))

    def after_push(ts: str | None) -> bool:
        return bool(ts) and _parse_ts(ts) > push

    # Every Codex signal we keep, as (ts, kind, detail). `reviewing` is the 👀 in-progress
    # marker; the rest are verdicts. Reviews are bound to the head commit; body
    # reactions/comments are gated on the push timestamp.
    events: list[tuple[datetime, str, dict[str, Any]]] = []

    for r in reviews:
        # A verdict-state review always carries submitted_at (only PENDING is null, and
        # that's filtered by state); the truthiness check keeps _parse_ts total regardless.
        if (
            is_bot(r)
            and r.get("state") in VERDICT_REVIEW_STATES
            and r.get("commit_id") == head_oid
            and r.get("submitted_at")
        ):
            events.append(
                (
                    _parse_ts(r["submitted_at"]),
                    "findings",
                    {"review_state": r.get("state"), "url": r.get("html_url")},
                )
            )

    for c in comments:
        if (
            is_bot(c)
            and after_push(c.get("created_at"))
            and USAGE_LIMIT_RE.search(c.get("body") or "")
        ):
            events.append(
                (_parse_ts(c["created_at"]), "exhausted", {"url": c.get("html_url")})
            )

    for x in reactions:
        if is_bot(x) and after_push(x.get("created_at")):
            ts = _parse_ts(x["created_at"])
            if x.get("content") == THUMBS_UP:
                events.append((ts, "clean", {}))
            elif x.get("content") == EYES:
                events.append((ts, "reviewing", {}))

    signal: str = "none"
    verdict_ts: datetime | None = None
    detail: dict[str, Any] = {}
    if events:
        newest_ts, newest_kind, _ = max(events, key=lambda e: e[0])
        if newest_kind == "reviewing":
            signal, verdict_ts = "reviewing", newest_ts
        else:
            # Strongest verdict wins; within a kind keep the most recent (its url/state).
            latest: dict[str, tuple[datetime, str, dict[str, Any]]] = {}
            for e in sorted(events, key=lambda e: e[0]):
                latest[e[1]] = e
            for kind in ("findings", "clean", "exhausted"):
                if kind in latest:
                    verdict_ts, signal, detail = latest[kind]
                    break

    return {
        "signal": signal,
        "settled": signal in SETTLED,
        "verdict_ts": verdict_ts.isoformat() if verdict_ts else None,
        "detail": detail,
    }


def _head_push_ts(pr: dict[str, Any]) -> str:
    """The head commit's committedDate — the push reference for body reactions/comments.

    Only reactions/comments lean on this (reviews are commit-bound); see `classify`'s
    note for why committedDate is the right approximation. Fails fast (exit 2) on the
    degenerate empty-commits PR rather than dying with a bare ValueError.
    """
    commits = pr.get("commits") or []
    if not commits:
        sys.stderr.write(f"PR has no commits: {pr.get('headRefOid')!r}\n")
        raise SystemExit(2)
    by_oid = {c["oid"]: c["committedDate"] for c in commits}
    if pr["headRefOid"] in by_oid:
        return by_oid[pr["headRefOid"]]
    # Detached/unknown head: fall back to the newest commit we can see.
    return max(c["committedDate"] for c in commits)


def evaluate(owner: str, name: str, pr: int, bot: str = DEFAULT_BOT) -> dict[str, Any]:
    """Fetch the PR's review/comment/reaction context and classify it. (Live gh calls.)"""
    base = f"repos/{owner}/{name}"
    pr_view = gh_json(["pr", "view", str(pr), "--json", "state,headRefOid,commits"])
    reviews = gh_json(["api", "--paginate", f"{base}/pulls/{pr}/reviews"])
    comments = gh_json(["api", "--paginate", f"{base}/issues/{pr}/comments"])
    reactions = gh_json(["api", "--paginate", f"{base}/issues/{pr}/reactions"])
    push_ts = _head_push_ts(pr_view)
    result = classify(
        head_oid=pr_view["headRefOid"],
        push_ts=push_ts,
        reviews=reviews,
        comments=comments,
        reactions=reactions,
        bot=bot,
    )
    return {
        "pr": pr,
        "pr_state": pr_view["state"],
        "head_oid": pr_view["headRefOid"],
        "push_ts": push_ts,
        **result,
    }


def poll(
    owner: str,
    name: str,
    pr: int,
    *,
    bot: str,
    timeout_min: float,
    interval_sec: float,
) -> dict[str, Any]:
    """Re-evaluate until the window settles or `timeout_min` elapses (the gate ceiling).

    A timeout is not a failure: AGENTS.md treats absence at the ceiling as "not a blocker".
    The caller reads the returned signal (likely `reviewing`/`none`) and proceeds.
    """
    interval_sec = max(1.0, interval_sec)  # floor: never busy-loop hammering the gh API
    deadline = time.monotonic() + timeout_min * 60
    while True:
        result = evaluate(owner, name, pr, bot)
        if result["settled"] or time.monotonic() >= deadline:
            result["timed_out"] = not result["settled"]
            return result
        remaining = deadline - time.monotonic()
        sys.stderr.write(
            f"pr #{pr}: {result['signal']} — waiting "
            f"{min(interval_sec, remaining):.0f}s "
            f"(~{remaining / 60:.1f} min left)\n"
        )
        time.sleep(min(interval_sec, max(0.0, remaining)))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Report the Codex bot-review-window signal for a PR (JSON to stdout)."
    )
    ap.add_argument("pr", type=int, help="PR number")
    ap.add_argument(
        "--wait",
        action="store_true",
        help="poll until the window settles or --timeout-min elapses",
    )
    ap.add_argument(
        "--timeout-min",
        type=float,
        default=10.0,
        help="--wait ceiling in minutes (default 10, the gate's window)",
    )
    ap.add_argument(
        "--interval",
        type=float,
        default=30.0,
        help="--wait poll interval in seconds (default 30)",
    )
    ap.add_argument(
        "--bot",
        default=DEFAULT_BOT,
        help=f"case-insensitive reviewer-bot login substring (default {DEFAULT_BOT!r})",
    )
    args = ap.parse_args(argv)

    owner, name = repo_owner_name()
    if args.wait:
        result = poll(
            owner,
            name,
            args.pr,
            bot=args.bot,
            timeout_min=args.timeout_min,
            interval_sec=args.interval,
        )
    else:
        result = evaluate(owner, name, args.pr, args.bot)

    print(json.dumps(result, indent=2))
    return 0 if result["settled"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
