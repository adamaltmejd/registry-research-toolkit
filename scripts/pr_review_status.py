#!/usr/bin/env python3
"""Codex bot-review-window poller for the merge gate.

The AGENTS.md "PR merge gate" makes us poll Codex's signal on the *current HEAD* every PR
cycle — in `/pr-pipeline` Step E and in ad-hoc agent-driven PR work alike. The signal is
fiddly and has been shipped wrong before (Codex submits reviews/comments as login
`chatgpt-codex-connector` but reacts as `chatgpt-codex-connector[bot]`, so a poller keyed
on one login misses the other). This script is the get-it-right-once classifier: given a
PR number it fetches reviews, issue comments, PR-body reactions, and reactions on human
`@codex review` comments, then reports the Codex signal as JSON. The two merge-gating
verdicts (findings, clean) are bound to the head commit by SHA, so a verdict from a prior
push is never mistaken for fresh.

Scope: this computes the **Codex** signal only — Codex is the bot whose verdict lands as a
reaction + a comment rather than as a normal review. Copilot (the gate's other bot) posts
ordinary reviews that `gh pr view --json reviews` already surfaces, so it needs no special
poller; a `none`/`exhausted` here says nothing about Copilot. `exhausted` (Codex out of
tokens) is "settled" per the gate's "not a blocker", NOT "a bot reviewed and approved" —
independent review is a separate gate line the agent still owns.

Signals (see the gate doc):
  clean      — Codex's no-findings verdict: a "Codex Review: no issues" comment stamped
               with the head SHA, or (when it posts no comment) a 👍 within the window.
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

The full ceiling is the wait for a *verdict*; the wait for Codex to even *start* is
short-circuited. If no 👀/verdict appears within `--engage-grace-sec` (default 90 s), the
poll returns early with `no_engagement: true` + a `recommendation` to post `@codex review`
— its auto-review fires a 👀 within ~a minute when it triggers at all, so a persistent
`none` past the grace means it didn't fire (flaky open/ready trigger or rate-limit) and
blocking the whole ceiling on a review that never began is wasted. Once a 👀 lands the grace
no longer applies.

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

# Codex authors its reviews, comments, and reactions as `chatgpt-codex-connector` and/or
# `chatgpt-codex-connector[bot]`. We match the login EXACTLY against those two forms (not a
# substring) so an impostor like `chatgpt-codex-connector-test` can't post a clean/exhausted
# signal and settle the gate. Override with --bot for a different reviewer bot.
DEFAULT_BOT = "chatgpt-codex-connector"
# The out-of-tokens comment Codex posts instead of a review ("You have reached your Codex
# usage limits for code reviews"). Matched on a Codex-authored comment → `exhausted`.
USAGE_LIMIT_RE = re.compile(r"usage limit", re.IGNORECASE)
# A human's `@codex review` request re-opens the bot-review window on an unchanged head; its
# timestamp is the window start for signals that aren't bound to the head commit (below).
CODEX_REVIEW_REQUEST_RE = re.compile(r"@codex\s+review", re.IGNORECASE)
# Codex stamps a no-findings verdict comment/review body with "Reviewed commit: `<sha>`".
# We bind `clean` to the head via this SHA (commit-exact, like findings' commit_id).
REVIEWED_COMMIT_RE = re.compile(
    r"reviewed commit:[^0-9a-fA-F]*([0-9a-fA-F]{7,40})", re.IGNORECASE
)
# GitHub reaction content strings: 👍 (a clean verdict when Codex posts no comment) / 👀
# (the in-progress marker).
THUMBS_UP = "+1"
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
    committed_date: str,
    reviews: list[dict[str, Any]],
    comments: list[dict[str, Any]],
    reactions: list[dict[str, Any]],
    review_comments: list[dict[str, Any]] | None = None,
    bot: str = DEFAULT_BOT,
) -> dict[str, Any]:
    """Resolve the Codex signal from raw GitHub records. Pure — the tested core.

    Returns the `signal` plus a `messages` list carrying the bodies of Codex's activity on
    the current HEAD (head-bound review summaries + inline comments, and in-window issue
    comments) so the caller has the actual text in one shot and never re-runs `gh`. A
    `clean` signal is authoritative — Codex reviewed *this* commit and found no issues; any
    accompanying comment (also in `messages`) is narration, not something to second-guess.

    Each signal is scoped one of two ways:

    - **Head-bound** (rebase-proof, no timestamp): **findings** = a submitted *review* with
      `commit_id == head_oid`; a SHA-stamped **clean** = a "Codex Review: …" comment carrying
      `Reviewed commit: <head>`.
    - **Window-bound** for the signals GitHub doesn't tie to a commit — a 👍 reaction, the
      out-of-tokens **exhausted** comment, and the 👀 **reviewing** marker. These count only
      if they land after the *review window start* = the later of the head commit's
      `committed_date` and the most recent human `@codex review` request. That start, not a
      bare commit timestamp, is what stops a stale 👍/usage-limit from a *prior* window
      settling a re-triggered run (GitHub exposes no reliable push time — `pushedDate` is
      null — so the commit date alone would admit them).

    The 👍 path is load-bearing, not a fallback: Codex's **automatic** open/ready review
    signals a clean verdict with a bare 👍 and NO comment; only an explicit `@codex review`
    makes it post the SHA-stamped "Codex Review: …" comment (confirmed across the PR corpus).
    So the common first-window clean is 👍-only — reading just the comment would miss it.

    Resolution: if the *newest* event is a 👀, Codex is mid-review → `reviewing` (never
    conclude then). Otherwise the strongest verdict wins, by priority `findings > clean >
    exhausted` — actionable feedback over a no-issues note, a real verdict over out-of-tokens.

    Record shapes (the subset used; GitHub's REST field names):
      review        : {"user": {"login"}, "state", "commit_id", "submitted_at",
                       "html_url", "body"}
      comment       : {"user": {"login"}, "body", "created_at", "html_url"}
      reaction      : {"content", "user": {"login"}, "created_at"}
      review_comment: {"user": {"login"}, "commit_id", "created_at", "html_url",
                       "path", "line", "body"}  (inline; GET /pulls/{pr}/comments)
    """
    review_comments = review_comments or []
    bot_logins = {bot.lower(), f"{bot}[bot]".lower()}

    def is_bot(rec: dict[str, Any]) -> bool:
        return (rec.get("user") or {}).get("login", "").lower() in bot_logins

    # Window start = the later of the head commit time and the newest human @codex-review
    # request (the bot's own comments quote "@codex review" in their footer — exclude them).
    window_start = _parse_ts(committed_date)
    for c in comments:
        if (
            not is_bot(c)
            and c.get("created_at")
            and CODEX_REVIEW_REQUEST_RE.search(c.get("body") or "")
        ):
            window_start = max(window_start, _parse_ts(c["created_at"]))

    def in_window(ts: str | None) -> bool:
        return bool(ts) and _parse_ts(ts) > window_start

    # Every Codex signal we keep, as (ts, kind, detail). `reviewing` is the 👀 in-progress
    # marker; the rest are verdicts. See the docstring for head-bound vs window-bound.
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
            # Out-of-tokens note carries no reviewed-commit → gate it on the window start.
            if in_window(ts):
                events.append((_parse_ts(ts), "exhausted", {"url": c.get("html_url")}))
        elif sha and head_oid.startswith(sha):
            # The "Codex Review: no issues" narration, stamped with the head SHA → clean.
            events.append((_parse_ts(ts), "clean", {"url": c.get("html_url")}))

    for x in reactions:
        # A 👍 is a clean verdict when Codex left no comment; 👀 means still reviewing. Both
        # are commit-unbound, so they count only within the current review window.
        # simplify: a 👍 carries no commit ref, so its only staleness guard is the window
        # floor (committedDate, raised by @codex-review). A date-preserving rebase whose new
        # head has an *earlier* committedDate than a stale 👍 — with no @codex-review
        # re-trigger — could read that 👍 as clean (false-settle). The workflow (re-trigger
        # @codex-review after every push, which raises the floor) and the independent
        # /code-review gate cover it; close it here only if a real push-time ever appears.
        # A SHA-stamped clean comment, when present, is preferred and is rebase-proof.
        if not is_bot(x) or not in_window(x.get("created_at")):
            continue
        if x.get("content") == THUMBS_UP:
            events.append((_parse_ts(x["created_at"]), "clean", {}))
        elif x.get("content") == EYES:
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
        if head_bound or in_window(c.get("created_at")):
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
    """The head commit's committedDate — the floor of `classify`'s review-window start.

    Window-bound signals (👍 / exhausted / 👀) must post no earlier than this; a later
    `@codex review` request raises the floor. Fails fast (exit 2) on the degenerate
    empty-commits PR rather than dying with a bare ValueError.
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


def _review_request_reactions(
    base: str, comments: list[dict[str, Any]], bot: str = DEFAULT_BOT
) -> list[dict[str, Any]]:
    """Fetch reactions attached to human `@codex review` issue comments.

    GitHub's PR-level reactions endpoint covers reactions on the PR body only. Codex marks
    explicit review requests as started by reacting 👀 to the *request comment*, so the
    poller must include those comment-level reactions or it falsely reports "no
    engagement" after Codex has already begun.
    """
    bot_logins = {bot.lower(), f"{bot}[bot]".lower()}

    def is_bot(rec: dict[str, Any]) -> bool:
        return (rec.get("user") or {}).get("login", "").lower() in bot_logins

    reactions: list[dict[str, Any]] = []
    for c in comments:
        comment_id = c.get("id")
        if (
            comment_id
            and not is_bot(c)
            and CODEX_REVIEW_REQUEST_RE.search(c.get("body") or "")
        ):
            reactions.extend(
                _paginated(f"{base}/issues/comments/{comment_id}/reactions")
            )
    return reactions


def evaluate(owner: str, name: str, pr: int, bot: str = DEFAULT_BOT) -> dict[str, Any]:
    """Fetch the PR's review/comment/reaction context and classify it. (Live gh calls.)"""
    base = f"repos/{owner}/{name}"
    pr_view = gh_json(["pr", "view", str(pr), "--json", "state,headRefOid,commits"])
    reviews = _paginated(f"{base}/pulls/{pr}/reviews")
    comments = _paginated(f"{base}/issues/{pr}/comments")
    reactions = [
        *_paginated(f"{base}/issues/{pr}/reactions"),
        *_review_request_reactions(base, comments, bot),
    ]
    review_comments = _paginated(f"{base}/pulls/{pr}/comments")
    committed_date = _head_push_ts(pr_view)
    result = classify(
        head_oid=pr_view["headRefOid"],
        committed_date=committed_date,
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
        "committed_date": committed_date,
        **result,
    }


def should_bail_no_engagement(
    signal: str, engaged: bool, elapsed_sec: float, grace_sec: float
) -> bool:
    """True when the bot has shown NO sign of starting — never a 👀 nor any verdict — past
    the short `grace_sec` start window.

    There are two distinct waits hiding behind one ceiling: waiting for the bot to *start*
    (it posts a 👀 within ~a minute when its auto-review triggers at all) and waiting for
    it to *finish* (the verdict, which legitimately takes many minutes). Only the second
    deserves the long `--timeout-min`. A persistent `none` past the grace means the
    auto-review almost certainly didn't fire — a flaky open/ready webhook or rate-limiting —
    so blocking the full ceiling on a review that never began is wasted time. Bail and let
    the caller post `@codex review`. Once a 👀 lands (`engaged` latches True) this never
    fires again — the long wait is now for the verdict. `grace_sec <= 0` disables the bail.
    """
    return (
        grace_sec > 0 and signal == "none" and not engaged and elapsed_sec >= grace_sec
    )


def poll(
    owner: str,
    name: str,
    pr: int,
    *,
    bot: str,
    timeout_min: float,
    interval_sec: float,
    engage_grace_sec: float = 90.0,
) -> dict[str, Any]:
    """Re-evaluate every `interval_sec` until the window settles or `timeout_min` elapses.

    There are no webhooks — a new Codex verdict is seen only by re-fetching, so this is a
    timed poll, not a push subscription. A timeout is not a failure: AGENTS.md treats
    absence at the ceiling as "not a blocker"; the result carries `timed_out: true` and the
    caller reads the (likely `reviewing`/`none`) signal and proceeds.

    The full ceiling is for the *verdict* wait. The *start* wait is short-circuited by
    `engage_grace_sec`: if the bot never engages (no 👀, no verdict) within the grace, the
    poll returns early with `no_engagement: true` + a `recommendation` to post
    `@codex review`, instead of blocking the whole ceiling on an auto-review that didn't
    fire (see `should_bail_no_engagement`). That early bail keeps `timed_out: false` — the
    ceiling was NOT reached — so a caller can't read it as the at-ceiling "absence is not a
    blocker, proceed"; `no_engagement` says the opposite (the bot never started, retry it).
    """
    interval_sec = max(1.0, interval_sec)  # floor: never busy-loop hammering the gh API
    start = time.monotonic()
    deadline = start + timeout_min * 60
    engaged = (
        False  # latches once a 👀/verdict is seen — then the grace no longer applies
    )
    while True:
        result = evaluate(owner, name, pr, bot)
        if result["signal"] != "none":
            engaged = True
        if result["settled"] or time.monotonic() >= deadline:
            result["timed_out"] = not result["settled"]
            return result
        elapsed = time.monotonic() - start
        if should_bail_no_engagement(
            result["signal"], engaged, elapsed, engage_grace_sec
        ):
            # NOT `timed_out`: the ceiling was not reached. `timed_out` means "absence at
            # the --timeout-min ceiling", which the gate reads as not-a-blocker → proceed.
            # The early bail is the opposite — Codex never started, so the caller should
            # post `@codex review`, not proceed. `no_engagement` carries that distinctly.
            result["timed_out"] = False
            result["no_engagement"] = True
            result["recommendation"] = (
                f"No {bot} engagement (no 👀, no verdict) {elapsed:.0f}s into the poll — its "
                "auto-review likely didn't fire (flaky open/ready trigger or rate-limit). "
                "Post an `@codex review` comment to start it, then re-run this poller."
            )
            sys.stderr.write(
                f"pr #{pr}: no engagement after {elapsed:.0f}s — bailing the start wait; "
                "post `@codex review`\n"
            )
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
        "--engage-grace-sec",
        type=float,
        default=90.0,
        help="bail the START wait after this many seconds with no bot engagement (no 👀 / "
        "verdict) and recommend `@codex review` (default 90). Once a 👀 lands, the full "
        "--timeout-min applies for the verdict. 0 disables the early bail.",
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
            engage_grace_sec=args.engage_grace_sec,
        )

    print(json.dumps(result, indent=2))
    return 0 if result["settled"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
