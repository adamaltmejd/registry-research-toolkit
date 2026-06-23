#!/usr/bin/env python3
"""Codex bot-review-window poller for the merge gate.

The AGENTS.md "PR merge gate" makes us poll Codex's signal on the *current HEAD* every PR
cycle — in `/pr-pipeline` Step E and in ad-hoc agent-driven PR work alike. The signal is
fiddly and has been shipped wrong before (Codex submits reviews/comments as login
`chatgpt-codex-connector` but reacts as `chatgpt-codex-connector[bot]`, so a poller keyed
on one login misses the other). This script is the get-it-right-once classifier: given a
PR number it fetches reviews, issue comments, and PR-body reactions, then reports the
Codex signal as JSON. The two merge-gating verdicts (findings, clean) are bound to the
head commit by SHA, so a verdict from a prior push is never mistaken for fresh.

Scope: this computes the **Codex** signal only — Codex is the bot whose verdict lands as a
reaction + a comment rather than as a normal review. Copilot (the gate's other bot) posts
ordinary reviews that `gh pr view --json reviews` already surfaces, so it needs no special
poller; a `none`/`exhausted` here says nothing about Copilot. `exhausted` (Codex out of
tokens) is "settled" per the gate's "not a blocker", NOT "a bot reviewed and approved" —
independent review is a separate gate line the agent still owns.

Signals (see the gate doc):
  clean      — Codex's "Codex Review: no issues" comment stamped with the head commit's
               SHA (its no-findings verdict; it also reacts 👍).
  findings   — a submitted Codex review on the head commit (its suggestions vehicle): read.
  reviewing  — a 👀 reaction from Codex with nothing newer: still reviewing, never
               conclude.
  exhausted  — a "reached your Codex usage limits" comment: definitive end-of-wait, NOT a
               blocker.
  none       — no Codex verdict on the current HEAD yet.

The JSON also carries `messages` — the bodies of Codex's activity on the current HEAD
(head-bound review summaries + inline comments, and post-push issue comments) — so the
agent has the actual text in one shot and never re-runs `gh pr view`. A `clean` signal is
authoritative: Codex reviewed this commit and found no issues, so the narration comment in
`messages` is not a finding to re-read and second-guess.

By default it **polls** (re-fetches every `--interval`, default 30 s) until the window
settles or `--timeout-min` (default 15) elapses — there are no GitHub webhooks here, so a
fresh verdict is seen only by re-asking. One poll per HEAD covers the whole window; launch
it **backgrounded** (the default wait outlasts a 10-min foreground command cap), once after
the PR goes ready and again after each `@codex review` on a new push. Pass `--once` for a
single non-blocking snapshot.

Exit code answers only "is the window settled?": 0 settled (clean | findings | exhausted),
1 not settled (reviewing | none — including a poll that hit the ceiling). 2 is a tool error
(gh failed, or bad args). The agent reads the JSON `signal`/`detail`/`messages` for what to
*do* — read findings, request `@codex review` after a push, decide to merge. The script
reports; the agent acts.

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
# `chatgpt-codex-connector[bot]`. The default is a case-insensitive substring that catches
# BOTH (the bug a one-login poller hits) yet is specific enough to reject an unrelated actor
# whose login merely contains "codex". Override with --bot for a different reviewer bot.
DEFAULT_BOT = "chatgpt-codex-connector"
# The out-of-tokens comment Codex posts instead of a review ("You have reached your Codex
# usage limits for code reviews"). Matched on a Codex-authored comment → `exhausted`.
USAGE_LIMIT_RE = re.compile(r"usage limit", re.IGNORECASE)
# Codex stamps every verdict comment/review body with "Reviewed commit: `<abbrev-sha>`".
# We bind `clean` to the head via this SHA (commit-exact, like findings' commit_id) rather
# than trusting the 👍 reaction's timestamp — GitHub exposes no reliable push time
# (`pushedDate` is null here), so a 👍 from a prior head could otherwise read as fresh.
REVIEWED_COMMIT_RE = re.compile(
    r"reviewed commit:[^0-9a-fA-F]*([0-9a-fA-F]{7,40})", re.IGNORECASE
)
# GitHub reaction content string for 👀 (the in-progress marker).
EYES = "eyes"
# Review states that count as a submitted verdict. PENDING (never submitted) is excluded by
# the submitted_at gate; DISMISSED is a withdrawn verdict, not an active one.
VERDICT_REVIEW_STATES = {"APPROVED", "CHANGES_REQUESTED", "COMMENTED"}

SETTLED = {"clean", "findings", "exhausted"}


def _reviewed_commit(body: str | None) -> str | None:
    """The abbreviated SHA from Codex's "Reviewed commit: `<sha>`" stamp, if present."""
    m = REVIEWED_COMMIT_RE.search(body or "")
    return m.group(1) if m else None


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
    review_comments: list[dict[str, Any]] | None = None,
    bot: str = DEFAULT_BOT,
) -> dict[str, Any]:
    """Resolve the Codex signal from raw GitHub records. Pure — the tested core.

    Returns the `signal` plus a `messages` list carrying the bodies of Codex's activity on
    the current HEAD (head-bound review summaries + inline comments, and post-push issue
    comments) so the caller has the actual text in one shot and never re-runs `gh`. A
    `clean` signal is authoritative — it means Codex reviewed *this* commit and found no
    issues; any accompanying comment (also in `messages`) is narration, not something to
    re-read and second-guess.

    The two verdicts that gate merge are bound to the head commit, not to a timestamp:
    **findings** = a submitted *review* whose `commit_id == head_oid`; **clean** = Codex's
    "Codex Review: …" narration *comment* stamped with `Reviewed commit: <head>`. Both are
    rebase-proof. (GitHub exposes no reliable push time — `pushedDate` is null — so a
    timestamp gate on the 👍 reaction could let a 👍 from a *prior* head read as fresh; the
    SHA stamp avoids that.) Only the lower-stakes signals fall back to the push timestamp:
    **exhausted** (the out-of-tokens comment carries no SHA) and **reviewing** (a 👀
    reaction — and a stale 👀 only over-waits, the safe direction).

    Resolution: if the *newest* event is a 👀, Codex is mid-review → `reviewing` (never
    conclude then). Otherwise the strongest verdict wins, by priority `findings > clean >
    exhausted` — actionable feedback over a no-issues note, a real verdict over out-of-tokens.

    How Codex actually signals (observed): **findings** = a submitted review on the head
    commit (summary body + inline comments); **clean** = a 👍 reaction *and* the SHA-stamped
    narration comment. A top-level comment is therefore narration or the usage-limit note —
    never the findings vehicle. (If Codex ever delivers findings as a bare comment with no
    review, this misses them — unobserved; the agent reads the PR regardless.)

    Record shapes (the subset used; GitHub's REST field names):
      review        : {"user": {"login"}, "state", "commit_id", "submitted_at",
                       "html_url", "body"}
      comment       : {"user": {"login"}, "body", "created_at", "html_url"}
      reaction      : {"content", "user": {"login"}, "created_at"}
      review_comment: {"user": {"login"}, "commit_id", "created_at", "html_url",
                       "path", "line", "body"}  (inline; GET /pulls/{pr}/comments)
    """
    review_comments = review_comments or []
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
        if not is_bot(c) or not c.get("created_at"):
            continue
        ts, body = c["created_at"], c.get("body") or ""
        sha = _reviewed_commit(body)
        if USAGE_LIMIT_RE.search(body):
            # Out-of-tokens note carries no reviewed-commit, so gate it on the push time.
            if after_push(ts):
                events.append((_parse_ts(ts), "exhausted", {"url": c.get("html_url")}))
        elif sha and head_oid.startswith(sha):
            # The "Codex Review: no issues" narration, stamped with the head SHA → clean.
            events.append((_parse_ts(ts), "clean", {"url": c.get("html_url")}))

    for x in reactions:
        # Only 👀 is consulted (the in-progress marker); the clean 👍 is superseded by the
        # commit-bound narration comment above. A stale 👀 only over-waits — the safe way.
        if is_bot(x) and x.get("content") == EYES and after_push(x.get("created_at")):
            events.append((_parse_ts(x["created_at"]), "reviewing", {}))

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

    # The text the caller would otherwise re-fetch: Codex's review summaries + inline
    # comments on the head commit, and its post-push issue comments (clean narration /
    # usage-limit). Newest last.
    messages: list[dict[str, Any]] = []
    for r in reviews:
        if (
            is_bot(r)
            and r.get("state") in VERDICT_REVIEW_STATES
            and r.get("commit_id") == head_oid
            and (r.get("body") or "").strip()
        ):
            messages.append({"kind": "review", "state": r.get("state"),
                             "ts": r.get("submitted_at"), "url": r.get("html_url"),
                             "body": r.get("body")})  # fmt: skip
    for rc in review_comments:
        if is_bot(rc) and rc.get("commit_id") == head_oid:
            messages.append({"kind": "review_comment", "ts": rc.get("created_at"),
                             "url": rc.get("html_url"), "path": rc.get("path"),
                             "line": rc.get("line"), "body": rc.get("body")})  # fmt: skip
    for c in comments:
        if not is_bot(c):
            continue
        sha = _reviewed_commit(c.get("body"))
        head_bound = bool(sha and head_oid.startswith(sha))
        if head_bound or after_push(c.get("created_at")):
            messages.append({"kind": "comment", "ts": c.get("created_at"),
                             "url": c.get("html_url"), "body": c.get("body")})  # fmt: skip
    messages.sort(key=lambda m: m["ts"] or "")

    return {
        "signal": signal,
        "settled": signal in SETTLED,
        "verdict_ts": verdict_ts.isoformat() if verdict_ts else None,
        "detail": detail,
        "messages": messages,
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


def _paginated(endpoint: str) -> list[dict[str, Any]]:
    """All items from a paginated `gh api` array endpoint, as one flat list.

    Plain `--paginate` concatenates one JSON array *per page* (invalid combined JSON that
    `json.loads` rejects on a >1-page PR — exactly when the gate poller is needed most).
    `--slurp` wraps the pages into an outer array `[[page1…], [page2…]]`; flatten it.
    """
    pages = gh_json(["api", "--paginate", "--slurp", endpoint])
    return [item for page in pages for item in page]


def evaluate(owner: str, name: str, pr: int, bot: str = DEFAULT_BOT) -> dict[str, Any]:
    """Fetch the PR's review/comment/reaction context and classify it. (Live gh calls.)"""
    base = f"repos/{owner}/{name}"
    pr_view = gh_json(["pr", "view", str(pr), "--json", "state,headRefOid,commits"])
    reviews = _paginated(f"{base}/pulls/{pr}/reviews")
    comments = _paginated(f"{base}/issues/{pr}/comments")
    reactions = _paginated(f"{base}/issues/{pr}/reactions")
    review_comments = _paginated(f"{base}/pulls/{pr}/comments")
    push_ts = _head_push_ts(pr_view)
    result = classify(
        head_oid=pr_view["headRefOid"],
        push_ts=push_ts,
        reviews=reviews,
        comments=comments,
        reactions=reactions,
        review_comments=review_comments,
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
    """Re-evaluate every `interval_sec` until the window settles or `timeout_min` elapses.

    There are no webhooks — a new Codex verdict is seen only by re-fetching, so this is a
    timed poll, not a push subscription. A timeout is not a failure: AGENTS.md treats
    absence at the ceiling as "not a blocker"; the result carries `timed_out: true` and the
    caller reads the (likely `reviewing`/`none`) signal and proceeds.
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
        description="Report the Codex bot-review-window signal for a PR (JSON to stdout). "
        "Polls until the window settles or --timeout-min elapses; pass --once for a single "
        "snapshot. A default poll outlasts a 10-min foreground command cap — run it "
        "backgrounded."
    )
    ap.add_argument("pr", type=int, help="PR number")
    ap.add_argument(
        "--once",
        action="store_true",
        help="report the current signal once and exit, instead of polling",
    )
    ap.add_argument(
        "--timeout-min",
        type=float,
        default=15.0,
        help="poll ceiling in minutes (default 15; Codex can take >13 min)",
    )
    ap.add_argument(
        "--interval",
        type=float,
        default=30.0,
        help="poll interval in seconds (default 30)",
    )
    ap.add_argument(
        "--bot",
        default=DEFAULT_BOT,
        help=f"case-insensitive reviewer-bot login substring (default {DEFAULT_BOT!r})",
    )
    args = ap.parse_args(argv)

    owner, name = repo_owner_name()
    if args.once:
        result = evaluate(owner, name, args.pr, args.bot)
    else:
        result = poll(
            owner,
            name,
            args.pr,
            bot=args.bot,
            timeout_min=args.timeout_min,
            interval_sec=args.interval,
        )

    print(json.dumps(result, indent=2))
    return 0 if result["settled"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
