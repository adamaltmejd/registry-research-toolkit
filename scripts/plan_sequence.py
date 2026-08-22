#!/usr/bin/env python3
"""plan-sequence — generate the sequencing projection for the issue-tracker epic.

Reads the structured open-issue corpus (area labels, `Relationships`, native sub-issues,
`touches` globs, open PRs) and renders a deterministic **ready / running / parked /
blocked / parallel + pending-release** view. The render is a MARKED block

    <!-- plan-sequence:start --> … <!-- plan-sequence:end -->

spliced into the epic body, so the human's lane narrative around it is preserved and the
generated region is overwritten (never appended) each run. No timestamp in the block —
the issue-edit history records when; the body stays diff-stable when nothing changed.

Read-only by default (prints the block to stdout). `--write <epic#>` splices it into that
epic's body via `gh issue edit`.

A SECOND marked region — `<!-- plan-lanes:start --> … <!-- plan-lanes:end -->` — carries
the *ranked* lanes. Their content is agentic (the `/plan-lanes` skill ranks what
set-intersection over `touches` can't see), so the script doesn't generate it: `--write-
lanes` reads the agent's text from stdin, frames it with a machine-readable `basis` (the
ready/running sets the ranking was computed against, plus a content `sig` over the
lane-affecting projection — see `lanes_content_signature`), and splices it as its own
region. `--lanes-stale` prints the live basis and exit-codes whether the stored block
matches it — the trigger the `/loop` heartbeat keys off, since once CI event-refreshes the
*projection* block the projection delta no longer signals that the ready set moved (the
refresh absorbed it). The `sig` extends staleness past membership: an
area/`touches`/`priority`/`Relationships` edit that re-shapes the lane graph without moving
an issue between sections still flips it. The heartbeat passes the printed basis back via
`--write-lanes --basis` so the stamp reflects the state the rank actually saw, not a
post-rank recompute (which could mark a stale ranking fresh).

Staleness is **three-way**, not boolean — because the running set is in-flight work, never
a lane member (`/plan-lanes` ranks only the ready candidates). A delta confined to the
running set (a PR merges, its issue closes, the claim clears) can't change lane *content*,
so it routes to a cheap **re-stamp** (rewrite the basis stamp, keep the ranked lanes —
`--restamp-lanes`) instead of an expensive, non-deterministic **re-rank**. The content
`sig` excludes running issues' own projection precisely so this common tick doesn't trip a
re-rank; their one content effect (HOLDING a ready candidate) is folded in via the free
set. The classifier (`lanes_freshness`) returns fresh / re-stamp / re-rank, surfaced as the
exit code: **0** fresh · **1** re-rank · **2** re-stamp.

`--tick` is the heartbeat's one-fetch combined read: from a single corpus build it prints
the projection status delta (stderr) and the live lanes basis (stdout) and exit-codes the
lanes freshness (0/1/2 above) — so the `/loop` tick does one fetch instead of `--diff` +
`--lanes-stale`. It never writes: CI (`plan-sequence.yml`) + the daily cron own the
projection block; the loop's only write is the lanes block, when it moves.

The gh/git process primitives live in the shared `_gh` module (lifted there once a third
consumer joined, alongside the shared `load_sibling` loader); the issue-domain parsers
(label sets, the relationship/touches regexes) are still reused from the sibling validator
(check_issue_hygiene.py).
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import importlib.util
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


def _load_gh() -> ModuleType:
    # The one leaf that can't go through _gh.load_sibling: _gh can't load itself. Kept a
    # tiny sys.modules-guarded spec-load, identical in every sibling script, so the whole
    # process shares ONE _gh instance (a single patch target, not one copy per loader).
    if (mod := sys.modules.get("_gh")) is not None:
        return mod
    spec = importlib.util.spec_from_file_location(
        "_gh", Path(__file__).with_name("_gh.py")
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_gh"] = mod
    spec.loader.exec_module(mod)
    return mod


# _gh's sys.modules-guarded loader pulls in the other siblings as single process-wide
# instances (so this gh_issue IS the one cos_dispatch/check_issue_hygiene use, and a
# monkeypatch of ps.gh_issue is visible everywhere). check_issue_hygiene supplies the
# issue-domain parsers (label sets, relationship/touches regexes).
_gh = _load_gh()
_h = _gh.load_sibling("check_issue_hygiene")

# The maintainer-author trust gate: this repo is public, so the ingestion set must only
# ever carry maintainer-authored issue text (a stranger's issue is a prompt-injection
# surface). gh_issue.fetch_open_issues drops non-maintainer rows; is_own_pr drops fork PRs
# from the running-claim computation.
gh_issue = _gh.load_sibling("gh_issue")

AREA_LABELS = _h.AREA_LABELS
PRIORITY_LABELS = _h.PRIORITY_LABELS
BLOCKING_KEYWORDS = _h.BLOCKING_KEYWORDS
FETCH_CAP = _h.FETCH_CAP
run = _gh.run
gh_json = _gh.gh_json
repo_owner_name = _gh.repo_owner_name
parse_relationships = _h.parse_relationships
parse_touches = _h.parse_touches

START = "<!-- plan-sequence:start -->"
END = "<!-- plan-sequence:end -->"

# The lanes block is a SECOND, independent marked region in the same epic body. Its
# content is agent-supplied (`/plan-lanes` ranks; the script only frames + splices it),
# which is why this is a separate region from the deterministic plan-sequence block.
LANES_START = "<!-- plan-lanes:start -->"
LANES_END = "<!-- plan-lanes:end -->"


@dataclass
class Rec:
    number: int
    title: str
    area: str | None
    is_epic: bool
    blocked_label: bool
    parked_label: bool
    touches: list[str]
    parent: int | None
    open_blockers: list[int]
    open_prs: list[int]
    # All parsed `Relationships` ties (keyword, target) — the full graph `/plan-lanes`
    # reads for coherence/ordering, not just the blocking subset in `open_blockers`.
    relationships: list[tuple[str, int]] = field(default_factory=list)
    # "high" | "normal" | "low" — the maintainer's `priority:*` label (default normal).
    priority: str = field(default="normal")
    status: str = field(default="")


def priority_of(labels: set[str]) -> str:
    """The priority bucket from `priority:*` labels (default "normal").

    At most one is expected (hygiene flags >1); if both slip through, high wins.
    """
    if "priority:high" in labels:
        return "high"
    if "priority:low" in labels:
        return "low"
    return "normal"


def classify(rec: Rec) -> str:
    """running (has an open linked PR) > parked > blocked > ready."""
    if rec.open_prs:
        return "running"
    if rec.parked_label:
        return "parked"
    if rec.blocked_label or rec.open_blockers:
        return "blocked"
    return "ready"


def _norm(pattern: str) -> str:
    return pattern.rstrip("/")


def _is_glob(pattern: str) -> bool:
    return any(c in pattern for c in "*?[")


def _literal_prefix(pattern: str) -> str:
    """The path up to the first glob metacharacter (the part both sides must share)."""
    cut = min((pattern.find(c) for c in "*?[" if c in pattern), default=len(pattern))
    return pattern[:cut].rstrip("/")


def _pair_overlap(x: str, y: str) -> bool:
    if x == y:
        return True
    # A glob pattern conflicts with any path it matches. fnmatchcase is case-sensitive
    # and treats `*` greedily across `/`, so it's deliberately conservative (over- rather
    # than under-detecting conflicts — serialize when unsure is the safe error).
    if _is_glob(x) and fnmatch.fnmatchcase(y, x):
        return True
    if _is_glob(y) and fnmatch.fnmatchcase(x, y):
        return True
    if not _is_glob(x) and not _is_glob(y):
        return x.startswith(y + "/") or y.startswith(x + "/")
    # glob vs glob: conflict if their fixed prefixes nest (or one starts with a wildcard).
    px, py = _literal_prefix(x), _literal_prefix(y)
    if not px or not py:
        return True
    return px == py or px.startswith(py + "/") or py.startswith(px + "/")


def touches_overlap(a: list[str], b: list[str]) -> bool:
    """Whether two `touches` sets could edit a common file (glob-aware)."""
    return any(_pair_overlap(_norm(pa), _norm(pb)) for pa in a for pb in b)


def parallel_groups(ready: list[Rec]) -> list[list[int]]:
    """Connected components of the file-overlap graph over ready issues with `touches`.

    Issues in different groups are file-disjoint (parallel-safe); within a group they
    share files and must serialize. Issues without `touches` are not grouped here — their
    parallel-safety is unknown.
    """
    have = [r for r in ready if r.touches]
    parent = {r.number: r.number for r in have}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for i, ri in enumerate(have):
        for rj in have[i + 1 :]:
            if touches_overlap(ri.touches, rj.touches):
                parent[find(ri.number)] = find(rj.number)

    groups: dict[int, list[int]] = {}
    for r in have:
        groups.setdefault(find(r.number), []).append(r.number)
    return sorted(sorted(g) for g in groups.values())


def splice_block(body: str, block: str, start: str = START, end: str = END) -> str:
    """Replace the `start`…`end` marked region with `block`, or append it if absent.

    Defaults to the plan-sequence markers; the lanes write passes the plan-lanes pair.
    Only a well-formed, ordered start…end pair is replaced. A lone or reversed marker
    (e.g. a half-copied end-comment in the human narrative) falls through to append, so a
    malformed body is never scrambled on write.
    """
    s, e = body.find(start), body.find(end)
    if s != -1 and e != -1 and e > s:
        return body[:s] + block + body[e + len(end) :]
    sep = "" if not body else ("\n" if body.endswith("\n") else "\n\n")
    return f"{body}{sep}{block}\n"


# --- data ----------------------------------------------------------------------------


CLOSING_CLAUSE_RE = re.compile(
    r"(?im)\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s*:?\s+"
    r"("
    r"(?:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#\d+"
    r"(?:(?:\s*,\s*(?:and\s+)?|\s+and\s+)"
    r"(?:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)?#\d+)*"
    r")"
)
ISSUE_REF_RE = re.compile(
    r"(?:(?P<repo>[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+))?#(?P<number>\d+)"
)


def closing_issue_numbers_from_body(
    body: str | None, current_repo: str | None = None
) -> set[int]:
    """Issue numbers named by closing-keyword clauses in a PR body.

    GitHub's `closingIssuesReferences` only covers PRs whose base can close the issue.
    Stacked successor PRs often target a predecessor branch, but their draft still needs
    to hold the issue out of dispatch. Parse the body as a fallback for those claims.
    """
    current = current_repo.casefold() if current_repo else None
    found: set[int] = set()
    for clause in CLOSING_CLAUSE_RE.finditer(body or ""):
        for match in ISSUE_REF_RE.finditer(clause.group(1)):
            repo = match.group("repo")
            if repo and (current is None or repo.casefold() != current):
                continue
            found.add(int(match.group("number")))
    return found


def epic_body(epic: int) -> str:
    """The epic issue's body, or '' if unset — the read every body-edit path starts from."""
    return gh_json(["issue", "view", str(epic), "--json", "body"])["body"] or ""


def fetch_open_prs_by_issue(current_repo: str | None = None) -> dict[int, list[int]]:
    prs = gh_json(["pr", "list", "--state", "open", "--limit", str(FETCH_CAP),
                   "--json", "number,body,closingIssuesReferences,isCrossRepository"])  # fmt: skip
    by_issue: dict[int, list[int]] = {}
    for pr in prs:
        # A fork PR's `Closes #N` is an untrusted claim on this repo's issues (a stranger
        # could otherwise hold an issue out of dispatch by opening a fork PR). Only count
        # own-branch PRs into the running-claim set; skip forks entirely.
        if not gh_issue.is_own_pr(pr):
            continue
        numbers = {ref["number"] for ref in pr.get("closingIssuesReferences") or []}
        numbers.update(closing_issue_numbers_from_body(pr.get("body"), current_repo))
        for number in sorted(numbers):
            by_issue.setdefault(number, []).append(pr["number"])
    return by_issue


def build_records(
    issues: list[dict],
    open_numbers: set[int],
    parent_of: dict[int, int],
    current_repo: str | None = None,
) -> list[Rec]:
    prs_by_issue = fetch_open_prs_by_issue(current_repo)
    recs: list[Rec] = []
    for it in issues:
        num = it["number"]
        labels = {label["name"] for label in it["labels"]}
        body = it.get("body") or ""
        rels = parse_relationships(body)
        rec = Rec(
            number=num,
            title=it["title"],
            area=next(iter(sorted(labels & AREA_LABELS)), None),
            is_epic="epic" in labels,
            blocked_label="blocked" in labels,
            parked_label="parked" in labels,
            touches=parse_touches(body),
            parent=parent_of.get(num),
            open_blockers=sorted(
                {t for kw, t in rels if kw in BLOCKING_KEYWORDS and t in open_numbers}
            ),
            open_prs=sorted(prs_by_issue.get(num, [])),
            relationships=sorted(rels),
            priority=priority_of(labels),
        )
        rec.status = classify(rec)
        recs.append(rec)
    return sorted(recs, key=lambda r: r.number)


# --- dispatch (read-only input for the agent's lane judgment) ------------------------


def _prio_tag(rec: Rec) -> str:
    """`[high] `/`[low] ` inline marker for non-normal priority, else "" (normal = quiet)."""
    return f"[{rec.priority}] " if rec.priority != "normal" else ""


def _holding_prs(rec: Rec, running: list[Rec]) -> list[int]:
    """Open PR numbers of the in-flight issues whose `touches` hold `rec` back."""
    return sorted(
        {
            p
            for r in running
            if touches_overlap(rec.touches, r.touches)
            for p in r.open_prs
        }
    )


def free_candidates(records: list[Rec]) -> list[Rec]:
    """Ready issues NOT held back by in-flight (running) work — the rank-able candidates.

    Mirrors `dispatch_view`'s candidate set: a ready issue whose `touches` overlap any
    running issue's `touches` is HELD (would collide with work already in flight) and
    excluded. Held is a pure function of the live running set, so this is the one channel
    by which the running set reaches lane *content* — `lanes_content_signature` folds the
    running set in via this free set rather than its raw membership (see there).
    """
    in_flight = [t for r in records if r.status == "running" for t in r.touches]
    ready = [r for r in records if r.status == "ready" and not r.is_epic]
    return [
        r for r in ready if not (r.touches and touches_overlap(r.touches, in_flight))
    ]


def dispatch_view(records: list[Rec]) -> str:
    """Read-only dispatch candidates for the agent to compose a lane from.

    Lists ready issues that DON'T touch in-flight (running) work, grouped by area, with
    `touches`, `priority:*` (the agent's primary ranking key), and must-serialize groups.
    Which issues form a lane, and how many, is a judgment call left to the agent — the
    script supplies the deterministic facts and excludes only what would collide with work
    already in flight.
    """
    running = [r for r in records if r.status == "running"]
    in_flight_prs = sorted({p for r in running for p in r.open_prs})
    ready = [r for r in records if r.status == "ready" and not r.is_epic]
    free = free_candidates(records)
    free_nums = {r.number for r in free}
    held = [r for r in ready if r.number not in free_nums]
    if not free:
        return "No ready issues free of in-flight conflicts."

    out = [
        "Dispatch candidates — ready, not touching in-flight work. Compose a coherent",
        "lane (you judge which issues go together and how many), then run",
        "`/pr-pipeline #…` — it opens a draft PR per issue up front, marking the lane",
        "in-flight so the next dispatch skips it.",
        "",
        # Flat, copy-pasteable candidate set — the authoritative ranking floor. The issue
        # numbers are ON this line (not the next) so `/plan-lanes`'s self-check can verify
        # "every ranked number is on the Candidate set line" literally. Ranking an issue
        # NOT on this line is contamination (it came from stale epic narrative, not the
        # live ready set).
        f"Candidate set ({len(free)}) — rank ONLY these; any other number is "
        "contamination: " + " ".join(f"#{n}" for n in sorted(r.number for r in free)),
        "Declared open blockers among candidates: none. If a candidate body still names "
        "`Blocked by #N` / `Depends on #N`, that target was not open in this fetch; "
        "do not infer open-blocker status from floor absence.",
        "",
    ]
    by_area: dict[str, list[Rec]] = {}
    for r in free:
        by_area.setdefault(r.area or "(no area)", []).append(r)
    for area in sorted(by_area):
        out.append(f"{area} ({len(by_area[area])}):")
        out += [
            f"  #{r.number} {_prio_tag(r)}{r.title}  —  "
            + (", ".join(r.touches) if r.touches else "(no touches declared)")
            for r in sorted(by_area[area], key=lambda r: r.number)
        ]
        out.append("")
    by_prio = {
        p: sorted(r.number for r in free if r.priority == p) for p in ("high", "low")
    }
    if by_prio["high"] or by_prio["low"]:
        out.append(
            "Priority (rank by this first): "
            + "; ".join(
                f"{p}: " + ", ".join(f"#{n}" for n in by_prio[p])
                for p in ("high", "low")
                if by_prio[p]
            )
        )
    serial = [g for g in parallel_groups(free) if len(g) > 1]
    if serial:
        out.append("Must serialize (share files): " +
                   "; ".join("+".join(f"#{n}" for n in g) for g in serial))  # fmt: skip
    # In-flight PR numbers come from the live `running` set — give them to the agent so
    # the return-format header's `in-flight PRs:` field has a fresh source, never the
    # stale epic narrative.
    if in_flight_prs:
        out.append("In-flight PRs: " + ", ".join(f"#{p}" for p in in_flight_prs))
    if held:
        out.append("Held — touch in-flight work: " + "; ".join(
            f"#{r.number} ← PR " + ", ".join(f"#{p}" for p in _holding_prs(r, running))
            for r in sorted(held, key=lambda r: r.number)
        ))  # fmt: skip
    return "\n".join(out).rstrip()


# --- render --------------------------------------------------------------------------


def _line(rec: Rec) -> str:
    area = f"`{rec.area}`" if rec.area else "_no area_"
    return f"- #{rec.number} {rec.title} · {area}"


def render_block(recs: list[Rec], debt: str | None) -> str:
    # Epics are containers, not work — list them separately, never as ready/blocked work.
    epics = sorted((r for r in recs if r.is_epic), key=lambda r: r.number)
    work = [r for r in recs if not r.is_epic]
    # Sort here (not just upstream) so the block is order-stable for any caller.
    by_status = {s: sorted((r for r in work if r.status == s), key=lambda r: r.number)
                 for s in ("ready", "running", "parked", "blocked")}  # fmt: skip
    ready, running, parked, blocked = (
        by_status["ready"],
        by_status["running"],
        by_status["parked"],
        by_status["blocked"],
    )
    out: list[str] = [
        START,
        "## Sequencing — generated",
        "",
        "<!-- Generated by `/plan-sequence`; edits inside this block are overwritten. "
        "Lanes, decisions, and narrative live OUTSIDE it. -->",
        "",
        f"_{len(work)} work · {len(ready)} ready · {len(running)} running · "
        f"{len(parked)} parked · {len(blocked)} blocked"
        + (f" · {len(epics)} epics" if epics else "")
        + "_",
    ]
    if epics:
        out += ["", "**Epics:** " + ", ".join(f"#{r.number}" for r in epics)]
    out += ["", "### Ready now"]
    if ready:
        for r in ready:
            out.append(_line(r))
        groups = parallel_groups(ready)
        serialized = [g for g in groups if len(g) > 1]
        if serialized:
            out.append("")
            out.append("**Serialize** (share files per `touches`): " +
                       "; ".join("+".join(f"#{n}" for n in g) for g in serialized))  # fmt: skip
        no_touches = sorted(r.number for r in ready if not r.touches)
        if no_touches:
            out.append("")
            out.append("Parallel-safety unknown (no `touches`): " +
                       ", ".join(f"#{n}" for n in no_touches))  # fmt: skip
    else:
        out.append("_none_")

    out += ["", "### Running"]
    out += [f"- #{r.number} {r.title} ← " + ", ".join(f"PR #{p}" for p in r.open_prs)
            for r in running] or ["_none_"]  # fmt: skip

    out += ["", "### Parked"]
    out += [_line(r) for r in parked] or ["_none_"]

    out += ["", "### Blocked"]
    if blocked:
        for r in blocked:
            why = (
                "blocked by " + ", ".join(f"#{b}" for b in r.open_blockers)
                if r.open_blockers
                else "`blocked` label (no open blocker — stale?)"
            )
            out.append(f"- #{r.number} {r.title} ← {why}")
    else:
        out.append("_none_")

    out += ["", "### Pending release"]
    out.append(f"⚠️ {debt}" if debt else "_none_")
    out += ["", END]
    return "\n".join(out)


# `sig` is optional in the pattern so a pre-signature basis (written before this field
# existed) still parses — it reads back with an empty sig, which differs from any live
# signature, so the lanes re-rank once and self-upgrade to the new stamp.
_BASIS_RE = re.compile(
    r"<!-- plan-lanes:basis ready=([\d,]*) running=([\d,]*)"
    r"(?: sig=(\w+))?(?: free=([\d,]*))? -->"
)


def lanes_content_signature(records: list[Rec]) -> str:
    """A short stable hash of only what determines lane CONTENT — never the running churn.

    The lanes block is ranked over the `ready` candidates (see `dispatch_view`). A `running`
    issue is in-flight, never a candidate, so a PR merging — its issue closing, the claim
    clearing — moves the rendered in-flight line but NOT which issues get ranked or their
    order. The *one* way the running set reaches content is by HOLDING a ready issue
    (touches-overlap → excluded from candidates); that is folded in by signing the FREE
    candidate set (`free_candidates`, recomputed against the live running set), rather than
    the raw running membership. So the signed projection, per non-running work issue:

      number | status | free? | area | touches | priority | relationships

    A running issue's own *self* fields — status/area/touches/priority — sign nothing (it's
    never a candidate), so a running issue arriving or leaving (the common PR-merge tick)
    does not by itself flip the signature. One that actually changes which ready issue is
    held flips a `free?` flag and so still re-ranks; one whose merge unblocks a dependent
    moves that dependent into the ready set, which the projection also catches.

    Two classes of edge *into* the candidate graph are signed so an order-changing rewrite
    re-ranks even with no section move:

    - Blocked and parked issues are signed in full (not just ready) because blocked issues'
      `Blocked by #N` edges set the unblocking power of the candidates, and both blocked
      and parked issues' `Related to`/`Follow-up to` ties signal coherence — the same
      reason the prior all-work signature included them.
    - A *running* issue's relationships are otherwise dropped (signing them in full would
      re-rank every merge, as the closing issue's `Part of #<epic>`/coherence ties leave the
      corpus — the churn this signature exists to kill), EXCEPT a blocking edge it points at
      a ready candidate: that confers unblocking power on the candidate, so it is signed. A
      normal sub-issue merge (whose only tie is `Part of #<epic>`) signs none of these, so it
      re-stamps; rewriting which candidate an in-flight issue depends on re-ranks.

    Membership (*which* issues are ready/running) is also in the basis's `ready`/`running`
    sets; this signature adds the rest of the ranking inputs.

    Structured-inputs-only — never title/body prose, which doesn't change the lane graph.
    Sorted throughout so it's deterministic; flows through `--basis` verbatim (it's part of
    the stamped comment string), so the TOCTOU capture path needs no change.
    """
    free = {r.number for r in free_candidates(records)}
    parts = [
        "{}|{}|{}|{}|{}|{}|{}".format(
            r.number,
            r.status,
            "free" if r.number in free else "",
            r.area or "",
            ",".join(sorted(r.touches)),
            r.priority,
            ",".join(f"{kw}#{t}" for kw, t in sorted(r.relationships)),
        )
        for r in sorted(records, key=lambda r: r.number)
        if r.status != "running" and not r.is_epic
    ]
    # The one running-issue input that still ranks: a blocking edge onto a ready candidate
    # (unblocking power). Keyed by source so two in-flight dependents of the same candidate
    # are distinguishable, and so rewriting one's target re-ranks even if another still
    # points there. Non-blocking ties (e.g. `Part of #<epic>`) are NOT here, so the common
    # merge stays a re-stamp.
    unblock = sorted(
        (r.number, kw, t)
        for r in records
        if r.status == "running"
        for kw, t in r.relationships
        if kw in BLOCKING_KEYWORDS and t in free
    )
    parts.append("unblock|" + ",".join(f"{src}:{kw}#{t}" for src, kw, t in unblock))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:12]


def basis_comment(
    ready: set[int], running: set[int], sig: str, free: set[int] | None = None
) -> str:
    """The machine-readable freshness basis embedded in the lanes block.

    Records the ready + running issue numbers AND a content `sig`
    (`lanes_content_signature`) — the state the ranking was computed against — so
    `--lanes-stale` can tell whether the agentic lanes still match the live state. The
    projection block can't serve as that signal once CI refreshes it on every event (a
    refresh there would absorb the delta the loop keys off). The `sig` makes a
    `touches`/area/`priority`/`Relationships` edit that moves no issue trip staleness too;
    keeping `running` as its own field lets `lanes_freshness` tell a content move (re-rank)
    apart from a running-set-only move (cheap re-stamp).

    `free` (optional) records the candidate floor `/plan-lanes` was handed
    (`free_candidates` — ready issues not held by in-flight work). It is stamped ONLY for
    the completeness guard (`reject_incomplete_lanes`); `lanes_freshness` ignores it (a
    free-set move already flips `sig`). Omitted ⟹ no `free=` field (a legacy basis the
    guard then abstains on).
    """
    r = ",".join(str(n) for n in sorted(ready))
    g = ",".join(str(n) for n in sorted(running))
    out = f"<!-- plan-lanes:basis ready={r} running={g} sig={sig}"
    if free is not None:
        out += " free=" + ",".join(str(n) for n in sorted(free))
    return out + " -->"


def parse_basis(block: str) -> tuple[set[int], set[int], str] | None:
    """The (ready, running, sig) a lanes block was computed against, or None if absent.

    `sig` is "" for a pre-signature basis (the field was absent) — distinct from any live
    signature, so such a block reads as stale and re-ranks once to gain the new stamp.
    """
    m = _BASIS_RE.search(block)
    if not m:
        return None
    nums = lambda s: {int(x) for x in s.split(",") if x}
    return nums(m.group(1)), nums(m.group(2)), m.group(3) or ""


def _basis_free(basis: str) -> set[int] | None:
    """The `free=` candidate floor stamped in a basis, or None if the field is absent.

    An empty set (`free=` present but listing no numbers — the all-held case, where nothing
    must be placed) is distinct from None (a pre-`free=` basis the completeness guard can't
    check and so abstains on).
    """
    m = _BASIS_RE.search(basis)
    if not m or m.group(4) is None:
        return None
    return {int(x) for x in m.group(4).split(",") if x}


def lanes_freshness(block: str, ready: set[int], running: set[int], sig: str) -> str:
    """Classify the lanes block vs the live state: 'fresh' | 'restamp' | 'rerank'.

    - **rerank** — lane CONTENT moved (the content `sig` differs, the ready set differs, or
      there's no parsable basis): the ranking must be recomputed by `/plan-lanes`.
    - **restamp** — only the in-flight (running) set moved (a PR merged/opened); content is
      identical, so the existing ranking stands and just its basis stamp needs refreshing
      (no `/plan-lanes`). This is the common PR-merge tick.
    - **fresh** — nothing relevant moved.

    The ready check is belt-and-suspenders: a ready-set move already flips the content `sig`
    (every non-running issue is signed with its status), but comparing the set too guards
    against a hash blind spot and keeps the re-stamp path provably running-set-only.

    A block whose basis predates the `free=` field (legacy) is forced to **rerank** even
    when ready/running/sig match: `reject_incomplete_lanes` can't completeness-check it
    without `free=`, so an already-incomplete legacy block would otherwise read `fresh`
    forever and never face the guard. The one-time re-rank upgrades it (stamps `free=`) and
    runs the guard on the result.
    """
    basis = parse_basis(block)
    if basis is None:
        return "rerank"
    if _basis_free(block) is None:
        return "rerank"  # legacy (pre-`free=`) block — upgrade so the guard can run.
    b_ready, b_running, b_sig = basis
    if b_sig != sig or b_ready != set(ready):
        return "rerank"
    return "fresh" if b_running == set(running) else "restamp"


def extract_lanes_content(block: str) -> str:
    """The agentic ranked-lane text inside a framed lanes block, minus the framing.

    Inverse of `render_lanes_block`'s wrapping: everything between the `basis` stamp and
    `LANES_END`. Returns '' if there's no parsable basis (a pre-stamp/legacy block) — the
    re-stamp caller then falls back to a full re-rank rather than guess where content starts
    (and `lanes_freshness` already classifies a basis-less block as `rerank`, so re-stamp is
    never reached for one in practice).
    """
    m = _BASIS_RE.search(block)
    end = block.find(LANES_END)
    if not m or end == -1 or end < m.end():
        return ""
    return block[m.end() : end].strip()


def restamp_lanes_block(epic: int, basis: str) -> int:
    """Refresh ONLY the lanes block's basis stamp, keeping the existing ranked content.

    The re-stamp path for a running-set-only delta: lane content is unchanged, so the
    expensive, non-deterministic `/plan-lanes` re-rank is skipped — we rewrite the machine
    basis (its `running=`/`sig=` fields) so the next staleness check reads fresh, leaving the
    ranked lanes verbatim. The human-visible in-flight prose inside the block may lag until
    the next genuine re-rank — cosmetic, and never a contamination source (`/plan-lanes`
    sources its candidate floor from the live `--lane` view, not the stored block).

    Falls back to exit 1 (caller should re-rank) if there's no existing content to keep, or
    if that content is itself incomplete vs the new basis. The latter guards a block written
    before the completeness guard (or before `free=` existed): re-stamping would bless an
    already-incomplete body with a fresh `free=`-bearing basis, so it would read fresh
    forever and `/pr-pipeline` would never see the dropped candidate. A re-stamp only fires
    on a running-set-only delta (the free set is unchanged — `lanes_content_signature` folds
    it in), so the new basis's `free=` is the set the content should already cover.
    """
    block = extract_block(epic_body(epic), LANES_START, LANES_END)
    content = extract_lanes_content(block)
    if not content:
        print("no existing lanes content to re-stamp; re-rank instead", file=sys.stderr)
        return 1
    if (why := reject_incomplete_lanes(content, basis)) is not None:
        print(f"preserved lanes incomplete ({why}); re-rank instead", file=sys.stderr)
        return 1
    return write_lanes_block(epic, render_lanes_block(content, basis))


def reject_lanes_stdin(content: str) -> str | None:
    """Why agent-supplied lanes `content` can't be spliced, or None if it's fine.

    A literal region marker inside the content would make a later splice mis-detect the
    region (truncate/orphan it), so refuse rather than corrupt the epic body.
    """
    if not content.strip():
        return "empty stdin (nothing to splice)"
    if LANES_START in content or LANES_END in content:
        return "stdin contains a plan-lanes marker; refusing to splice"
    return None


def reject_incomplete_lanes(content: str, basis: str) -> str | None:
    """Why agent-supplied lanes `content` silently drops a free candidate, or None if fine.

    This is a write-boundary **backstop for the catastrophic case**: a free candidate that
    vanishes from the body ENTIRELY. Such a silent drop is invisible to `lanes_freshness`
    (it compares only the basis stamp, not the body), so the stamp reads fresh while
    `/pr-pipeline` never sees the issue (the original #662 failure). The rule is therefore
    deliberately coarse — every free candidate must appear *somewhere* in `content`. Exact
    placement quality (ranked exactly once, not merely name-dropped, no duplicate lanes) is
    `/plan-lanes`' own step-5 self-check's job, not this net's: a mispositioned-but-present
    candidate is still visible in the body and recoverable, unlike a silent vanish. (Kept
    simple by design — see PR #663; a structured exactly-once validator was considered and
    declined as overkill for a safety net.)

    The reference set is the `free=` set stamped in the basis — the floor `/plan-lanes` was
    handed at the same `--tick`, so the check is TOCTOU-free and excludes held issues: a
    disjoint in-flight PR leaves most candidates free (still checked), while the all-held
    case stamps `free=` empty (nothing required). A basis with no `free=` field (legacy)
    can't be checked, so abstain; the next `--tick` re-stamps with the field. `#(\\d+)`
    matches a whole digit run, so no candidate is masked by a longer id (#42 ≠ #420); a PR
    reference can't equal a free candidate (GitHub shares the issue/PR number space).
    """
    free = _basis_free(basis)
    if free is None:
        return None  # legacy/unstamped basis — re-stamped on the next tick.
    present = {int(n) for n in re.findall(r"#(\d+)", content)}
    missing = sorted(free - present)
    if missing:
        nums = ", ".join(f"#{n}" for n in missing)
        return f"lanes drop free candidate(s) {nums} — every candidate must appear in the body"
    return None


def render_lanes_block(content: str, basis: str = "") -> str:
    """Frame agent-supplied ranked-lane text as the epic's plan-lanes block.

    `/plan-lanes` produces the ranking (the judgment the script can't); this only wraps it
    in the marked region + the do-not-edit header + the deterministic `basis` stamp,
    mirroring `render_block`'s framing. No timestamp — the issue-edit history records when;
    the body stays diff-stable.
    """
    head = [
        LANES_START,
        "## Lanes — generated",
        "",
        "<!-- Generated by `/plan-lanes` (run by `/issue-pulse` when the lanes go stale); "
        "edits inside this block are overwritten. The ranking is agentic, not "
        "deterministic. -->",
    ]
    if basis:
        head.append(basis)
    return "\n".join(head + ["", content.strip(), "", LANES_END])


def write_lanes_block(epic: int, block: str) -> int:
    """Splice a framed lanes `block` into the epic body; no-op if unchanged. Exit code."""
    current = epic_body(epic)
    new_body = splice_block(current, block, LANES_START, LANES_END)
    if new_body == current:
        print(f"#{epic} lanes already up to date.")
        return 0
    subprocess.run(
        ["gh", "issue", "edit", str(epic), "--body-file", "-"],
        input=new_body,
        text=True,
        check=True,
    )
    print(f"Updated #{epic} lanes.")
    return 0


def build_debt_line() -> str | None:
    """The reg_meta_build rebuild-pending signal, reusing the validator's classifier."""
    out = _h.Findings()
    _h.check_unreleased_build_debt(
        Path(run(["git", "rev-parse", "--show-toplevel"]).strip()), out
    )
    # check_unreleased_build_debt emits at most one WARN; take it if present.
    debts = [msg for level, _, msg in out.items if level == "WARN"]
    return debts[0] if debts else None


# --- delta (for the /loop heartbeat) -------------------------------------------------

_SECTIONS = ("Ready now", "Running", "Parked", "Blocked")
_SECTION_LABEL = {
    "Ready now": "ready",
    "Running": "running",
    "Parked": "parked",
    "Blocked": "blocked",
}


def extract_block(body: str, start: str = START, end: str = END) -> str:
    """The marked block in `body` (between `start`…`end`), or '' if absent.

    Defaults to the plan-sequence markers; pass the plan-lanes pair to pull that region.
    """
    s, e = body.find(start), body.find(end)
    return body[s : e + len(end)] if (s != -1 and e > s) else ""


def _section_numbers(block: str) -> dict[str, set[int]]:
    by_section: dict[str, set[int]] = {s: set() for s in _SECTIONS}
    current: str | None = None
    for line in block.splitlines():
        header = re.match(r"### (.+)", line)
        if header:
            current = header.group(1).strip()
        elif current in by_section:
            item = re.match(r"- #(\d+)\b", line)
            if item:
                by_section[current].add(int(item.group(1)))
    return by_section


def diff_report(old_block: str, new_block: str) -> str:
    """Per-section issue-number delta between two rendered blocks — the tick's signal."""
    old, new = _section_numbers(old_block), _section_numbers(new_block)
    lines: list[str] = []
    for section in _SECTIONS:
        label = _SECTION_LABEL[section]
        added = sorted(new[section] - old[section])
        gone = sorted(old[section] - new[section])
        if added:
            lines.append(f"newly {label}: " + ", ".join(f"#{n}" for n in added))
        if gone:
            lines.append(f"left {label}: " + ", ".join(f"#{n}" for n in gone))
    return "\n".join(lines) if lines else "no status changes"


# --- main ----------------------------------------------------------------------------

# The three-way lanes freshness, surfaced as the `--tick`/`--lanes-stale` exit code so the
# heartbeat can branch: 0 skip · 1 re-rank via /plan-lanes · 2 cheap re-stamp (running-set-
# only). `restamp` is between fresh and rerank — work moved, but not lane content.
_FRESHNESS_EXIT = {"fresh": 0, "rerank": 1, "restamp": 2}
_FRESHNESS_MSG = {
    "fresh": "fresh",
    "rerank": "stale (re-rank)",
    "restamp": "stale (re-stamp — running-set-only; no re-rank)",
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--epic", type=int, default=328,
                    help="the epic issue to render for / write to (default: 328)")  # fmt: skip
    ap.add_argument("--write", action="store_true",
                    help="splice the block into the epic's body (default: print)")  # fmt: skip
    ap.add_argument("--diff", action="store_true",
                    help="print the status delta vs the epic's current block (no write)")  # fmt: skip
    ap.add_argument("--lane", action="store_true",
                    help="print read-only dispatch candidates (ready, not touching in-flight work)")  # fmt: skip
    ap.add_argument("--write-lanes", action="store_true",
                    help="frame the ranked-lanes block (read from stdin) + splice it into the epic body")  # fmt: skip
    ap.add_argument("--restamp-lanes", action="store_true",
                    help="re-stamp the epic's existing lanes block with --basis, keeping the ranked content (running-set-only delta; no re-rank)")  # fmt: skip
    ap.add_argument("--lanes-stale", action="store_true",
                    help="print the live basis; exit 0 fresh / 1 re-rank / 2 re-stamp (running-set-only)")  # fmt: skip
    ap.add_argument("--tick", action="store_true",
                    help="read-only heartbeat: one fetch emits the projection delta (stderr) + the lanes basis (stdout); exit 0 fresh / 1 re-rank / 2 re-stamp")  # fmt: skip
    ap.add_argument("--basis", default="",
                    help="for --write-lanes/--restamp-lanes: stamp this exact basis (captured from --lanes-stale/--tick) instead of recomputing")  # fmt: skip
    args = ap.parse_args()

    # Fast path: --write-lanes with a basis captured at staleness-check time needs no
    # corpus — and MUST stamp THAT basis, not a fresh recompute. If the ready/running set
    # moved while the forked /plan-lanes pass was ranking, recomputing here would stamp the
    # newer set onto an older ranking, so the next --lanes-stale would read it as fresh and
    # the stale ranking would persist. Stamping the ranked-against basis avoids that.
    if args.write_lanes and args.basis:
        content = sys.stdin.read()
        if (why := reject_lanes_stdin(content)) is not None:
            print(f"--write-lanes: {why}", file=sys.stderr)
            return 2
        if not _BASIS_RE.search(args.basis):
            print(f"--write-lanes: malformed --basis: {args.basis!r}", file=sys.stderr)
            return 2
        if (why := reject_incomplete_lanes(content, args.basis)) is not None:
            print(f"--write-lanes: {why}", file=sys.stderr)
            return 2
        return write_lanes_block(args.epic, render_lanes_block(content, args.basis))

    # Fast path: re-stamp keeps the existing ranking and only swaps the basis stamp, so it
    # also needs no corpus — the heartbeat passes the live basis captured from --tick. A
    # missing/malformed basis is refused (not stamped): a basis that fails _BASIS_RE would
    # write a block that parses as no-basis, pinning every later tick to `rerank`.
    if args.restamp_lanes:
        if not args.basis or not _BASIS_RE.search(args.basis):
            print(f"--restamp-lanes needs a well-formed --basis (got {args.basis!r})",
                  file=sys.stderr)  # fmt: skip
            return 2
        return restamp_lanes_block(args.epic, args.basis)

    owner, name = repo_owner_name()
    _known, _issue_state, open_numbers = _h.fetch_number_states()
    parent_of = _h.fetch_parents(owner, name)
    recs = build_records(
        gh_issue.fetch_open_issues(), open_numbers, parent_of, f"{owner}/{name}"
    )
    work = [r for r in recs if not r.is_epic]
    ready_nums = {r.number for r in work if r.status == "ready"}
    running_nums = {r.number for r in work if r.status == "running"}
    # The free candidate floor /plan-lanes is handed (ready minus held-by-in-flight-work);
    # stamped into the basis for reject_incomplete_lanes' completeness guard.
    free_nums = {r.number for r in free_candidates(work)}
    # The lanes' freshness basis: the ready/running sets + a content signature over the
    # lane-affecting projection (so an edit that re-shapes ranking without moving a section
    # still re-ranks, while a running-set-only delta only re-stamps — see
    # lanes_content_signature / lanes_freshness).
    content_sig = lanes_content_signature(work)
    live_basis = basis_comment(ready_nums, running_nums, content_sig, free_nums)

    if args.lane:
        print(dispatch_view(recs))
        return 0

    if args.lanes_stale:
        body = epic_body(args.epic)
        freshness = lanes_freshness(
            extract_block(body, LANES_START, LANES_END),
            ready_nums,
            running_nums,
            content_sig,
        )
        # stdout = the basis to re-pass to --write-lanes/--restamp-lanes (so the stamp
        # matches what was ranked); stderr = the human verdict; exit code = the machine
        # signal (0 fresh / 1 re-rank / 2 re-stamp).
        print(live_basis)
        print(_FRESHNESS_MSG[freshness], file=sys.stderr)
        return _FRESHNESS_EXIT[freshness]

    if args.tick:
        # One-fetch heartbeat: emit BOTH the projection delta and the lanes verdict from
        # the single corpus build above. Read-only — CI (plan-sequence.yml) + the daily
        # cron own the projection WRITE now; the loop's only write is the lanes block when
        # it moves. stdout = the basis to re-pass to --write-lanes/--restamp-lanes; stderr =
        # the human report (projection delta + verdict); exit code = the lanes freshness
        # (0 fresh / 1 re-rank / 2 re-stamp).
        body = epic_body(args.epic)
        delta = diff_report(extract_block(body), render_block(recs, build_debt_line()))
        freshness = lanes_freshness(
            extract_block(body, LANES_START, LANES_END),
            ready_nums,
            running_nums,
            content_sig,
        )
        print(live_basis)
        print(f"projection delta:\n{delta}", file=sys.stderr)
        print(f"lanes: {_FRESHNESS_MSG[freshness]}", file=sys.stderr)
        return _FRESHNESS_EXIT[freshness]

    if args.write_lanes:
        content = sys.stdin.read()
        if (why := reject_lanes_stdin(content)) is not None:
            print(f"--write-lanes: {why}", file=sys.stderr)
            return 2
        if (why := reject_incomplete_lanes(content, live_basis)) is not None:
            print(f"--write-lanes: {why}", file=sys.stderr)
            return 2
        return write_lanes_block(args.epic, render_lanes_block(content, live_basis))

    block = render_block(recs, build_debt_line())

    if not args.write and not args.diff:
        print(block)
        return 0

    current = epic_body(args.epic)
    delta = diff_report(extract_block(current), block)  # computed before any write

    if args.diff:
        print(delta)
        return 0

    new_body = splice_block(current, block)
    if new_body == current:
        print(f"#{args.epic} already up to date.")
        return 0
    subprocess.run(
        ["gh", "issue", "edit", str(args.epic), "--body-file", "-"],
        input=new_body,
        text=True,
        check=True,
    )
    print(f"Updated #{args.epic}.\n{delta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
