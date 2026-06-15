#!/usr/bin/env python3
"""plan-sequence — generate the sequencing projection for the issue-tracker epic.

Reads the structured open-issue corpus (area labels, `Relationships`, native sub-issues,
`touches` globs, open PRs) and renders a deterministic **ready / running / blocked /
parallel + pending-release** view. The render is a MARKED block

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
ready/running sets the ranking was computed against, plus a `sig` over those issues'
`touches`/blockers), and splices it as its own region. `--lanes-stale` prints the live
basis and exit-codes whether the stored block matches it — the trigger the `/loop`
heartbeat keys off, since once CI event-refreshes the *projection* block the projection
delta no longer signals that the ready/running sets moved (the refresh absorbed it). The
`sig` extends staleness past membership: a `touches`/`Relationships` edit that re-shapes
the lane graph without moving an issue between sections still flips it. The heartbeat
passes the printed basis back via `--write-lanes --basis` so the stamp reflects the state
the rank actually saw, not a post-rank recompute (which could mark a stale ranking fresh).

`--tick` is the heartbeat's one-fetch combined read: from a single corpus build it prints
the projection status delta (stderr) and the live lanes basis (stdout) and exit-codes the
lanes staleness — so the `/loop` tick does one fetch instead of `--diff` + `--lanes-stale`.
It never writes: CI (`plan-sequence.yml`) + the daily cron own the projection block; the
loop's only write is the lanes block, when stale.

The hardened parsers + gh fetchers are reused from the sibling validator
(check_issue_hygiene.py); if a third consumer appears, lift them into a shared module.
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

_HSPEC = importlib.util.spec_from_file_location(
    "check_issue_hygiene", Path(__file__).with_name("check_issue_hygiene.py")
)
assert _HSPEC and _HSPEC.loader
_h = importlib.util.module_from_spec(_HSPEC)
_HSPEC.loader.exec_module(_h)

AREA_LABELS = _h.AREA_LABELS
PRIORITY_LABELS = _h.PRIORITY_LABELS
BLOCKING_KEYWORDS = _h.BLOCKING_KEYWORDS
FETCH_CAP = _h.FETCH_CAP
gh_json = _h.gh_json
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
    touches: list[str]
    parent: int | None
    open_blockers: list[int]
    open_prs: list[int]
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
    """running (has an open linked PR) > blocked (label or open blocker) > ready."""
    if rec.open_prs:
        return "running"
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


def fetch_open_prs_by_issue() -> dict[int, list[int]]:
    prs = gh_json(["pr", "list", "--state", "open", "--limit", str(FETCH_CAP),
                   "--json", "number,closingIssuesReferences"])  # fmt: skip
    by_issue: dict[int, list[int]] = {}
    for pr in prs:
        for ref in pr.get("closingIssuesReferences") or []:
            by_issue.setdefault(ref["number"], []).append(pr["number"])
    return by_issue


def build_records(
    issues: list[dict], open_numbers: set[int], parent_of: dict[int, int]
) -> list[Rec]:
    prs_by_issue = fetch_open_prs_by_issue()
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
            touches=parse_touches(body),
            parent=parent_of.get(num),
            open_blockers=sorted(
                {t for kw, t in rels if kw in BLOCKING_KEYWORDS and t in open_numbers}
            ),
            open_prs=sorted(prs_by_issue.get(num, [])),
            priority=priority_of(labels),
        )
        rec.status = classify(rec)
        recs.append(rec)
    return sorted(recs, key=lambda r: r.number)


# --- dispatch (read-only input for the agent's lane judgment) ------------------------


def _prio_tag(rec: Rec) -> str:
    """`[high] `/`[low] ` inline marker for non-normal priority, else "" (normal = quiet)."""
    return f"[{rec.priority}] " if rec.priority != "normal" else ""


def dispatch_view(records: list[Rec]) -> str:
    """Read-only dispatch candidates for the agent to compose a lane from.

    Lists ready issues that DON'T touch in-flight (running) work, grouped by area, with
    `touches`, `priority:*` (the agent's primary ranking key), and must-serialize groups.
    Which issues form a lane, and how many, is a judgment call left to the agent — the
    script supplies the deterministic facts and excludes only what would collide with work
    already in flight.
    """
    in_flight = [t for r in records if r.status == "running" for t in r.touches]
    ready = [r for r in records if r.status == "ready" and not r.is_epic]
    held = {
        r.number for r in ready if r.touches and touches_overlap(r.touches, in_flight)
    }
    free = [r for r in ready if r.number not in held]
    if not free:
        return "No ready issues free of in-flight conflicts."

    out = [
        "Dispatch candidates — ready, not touching in-flight work. Compose a coherent",
        "lane (you judge which issues go together and how many), then run",
        "`/pr-pipeline #…` — it opens a draft PR per issue up front, marking the lane",
        "in-flight so the next dispatch skips it.",
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
    if held:
        out.append("Held — touch in-flight work: "
                   + ", ".join(f"#{n}" for n in sorted(held)))  # fmt: skip
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
                 for s in ("ready", "running", "blocked")}  # fmt: skip
    ready, running, blocked = (
        by_status["ready"],
        by_status["running"],
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
        f"{len(blocked)} blocked" + (f" · {len(epics)} epics" if epics else "") + "_",
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
    r"<!-- plan-lanes:basis ready=([\d,]*) running=([\d,]*)(?: sig=(\w+))? -->"
)


def lanes_signature(records: list[Rec]) -> str:
    """A short stable hash of the lane-affecting inputs for the ready/running set.

    The ranking depends on more than *which* issues are ready/running (that membership is
    already in the basis's `ready`/`running` sets): it also depends on each issue's
    `touches` globs (the parallel-safety graph), its open blockers (implicit ordering), and
    its `priority` (the primary ranking key). An edit to any of those that moves no issue
    between sections leaves the sets unchanged, so a membership-only basis misses it.
    Folding those per-issue inputs into this signature closes that gap: the signature
    flips, the basis differs, the lanes re-rank.

    Deliberately structured-inputs-only (number + `touches` + open blockers + priority),
    never title/body prose — those don't change the lane graph and would churn the
    signature. Sorted throughout so it's deterministic; flows through `--basis` verbatim
    (it's part of the stamped comment string), so the TOCTOU capture path needs no change.
    """
    parts = [
        "{}|{}|{}|{}".format(
            r.number,
            ",".join(sorted(r.touches)),
            ",".join(str(b) for b in sorted(r.open_blockers)),
            r.priority,
        )
        for r in sorted(records, key=lambda r: r.number)
    ]
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:12]


def basis_comment(ready: set[int], running: set[int], sig: str) -> str:
    """The machine-readable freshness basis embedded in the lanes block.

    Records the ready + running issue numbers AND a `sig` over their lane-affecting inputs
    (`lanes_signature`) — the state the ranking was computed against — so `--lanes-stale`
    can tell whether the agentic lanes still match the live state. The projection block
    can't serve as that signal once CI refreshes it on every event (a refresh there would
    absorb the delta the loop keys off). The `sig` makes a `touches`/`Relationships` edit
    that moves no issue trip staleness too — no longer an accepted miss.
    """
    r = ",".join(str(n) for n in sorted(ready))
    g = ",".join(str(n) for n in sorted(running))
    return f"<!-- plan-lanes:basis ready={r} running={g} sig={sig} -->"


def parse_basis(block: str) -> tuple[set[int], set[int], str] | None:
    """The (ready, running, sig) a lanes block was computed against, or None if absent.

    `sig` is "" for a pre-signature basis (the field was absent) — distinct from any live
    signature, so such a block reads as stale and re-ranks once to gain the new stamp.
    """
    m = _BASIS_RE.search(block)
    if not m:
        return None
    nums = lambda s: {int(x) for x in s.split(",") if x}  # noqa: E731
    return nums(m.group(1)), nums(m.group(2)), m.group(3) or ""


def lanes_are_stale(block: str, ready: set[int], running: set[int], sig: str) -> bool:
    """Whether the lanes block needs re-ranking vs the current ready/running set + sig."""
    basis = parse_basis(block)
    return basis is None or basis != (set(ready), set(running), sig)


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
    current = gh_json(["issue", "view", str(epic), "--json", "body"])["body"] or ""
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
        Path(_h.run(["git", "rev-parse", "--show-toplevel"]).strip()), out
    )
    # check_unreleased_build_debt emits at most one WARN; take it if present.
    debts = [msg for level, _, msg in out.items if level == "WARN"]
    return debts[0] if debts else None


# --- delta (for the /loop heartbeat) -------------------------------------------------

_SECTIONS = ("Ready now", "Running", "Blocked")
_SECTION_LABEL = {"Ready now": "ready", "Running": "running", "Blocked": "blocked"}


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
    ap.add_argument("--lanes-stale", action="store_true",
                    help="print the live basis; exit 1 if the epic's lanes block is stale vs it, else 0")  # fmt: skip
    ap.add_argument("--tick", action="store_true",
                    help="read-only heartbeat: one fetch emits the projection delta (stderr) + the lanes basis (stdout); exit 1 if lanes stale")  # fmt: skip
    ap.add_argument("--basis", default="",
                    help="for --write-lanes: stamp this exact basis (captured from --lanes-stale/--tick) instead of recomputing")  # fmt: skip
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
        return write_lanes_block(args.epic, render_lanes_block(content, args.basis))

    owner, name = _h.repo_owner_name()
    _known, _issue_state, open_numbers = _h.fetch_number_states()
    parent_of = _h.fetch_parents(owner, name)
    recs = build_records(_h.fetch_open_issues(), open_numbers, parent_of)
    work = [r for r in recs if not r.is_epic]
    ready_nums = {r.number for r in work if r.status == "ready"}
    running_nums = {r.number for r in work if r.status == "running"}
    # The lanes' freshness basis: the ready/running sets + a signature over their
    # touches/blockers (so an edit that moves no issue still re-ranks — see FU-2).
    sig = lanes_signature([r for r in work if r.status in ("ready", "running")])
    live_basis = basis_comment(ready_nums, running_nums, sig)

    if args.lane:
        print(dispatch_view(recs))
        return 0

    if args.lanes_stale:
        body = (
            gh_json(["issue", "view", str(args.epic), "--json", "body"])["body"] or ""
        )
        stale = lanes_are_stale(
            extract_block(body, LANES_START, LANES_END), ready_nums, running_nums, sig
        )
        # stdout = the basis to re-pass to --write-lanes (so the stamp matches what was
        # ranked); stderr = the human verdict; exit code = the machine signal.
        print(live_basis)
        print("stale" if stale else "fresh", file=sys.stderr)
        return 1 if stale else 0

    if args.tick:
        # One-fetch heartbeat: emit BOTH the projection delta and the lanes verdict from
        # the single corpus build above. Read-only — CI (plan-sequence.yml) + the daily
        # cron own the projection WRITE now; the loop's only write is the lanes block when
        # stale. stdout = the basis to re-pass to --write-lanes; stderr = the human report
        # (projection delta + verdict); exit code = the lanes staleness signal.
        body = (
            gh_json(["issue", "view", str(args.epic), "--json", "body"])["body"] or ""
        )
        delta = diff_report(extract_block(body), render_block(recs, build_debt_line()))
        stale = lanes_are_stale(
            extract_block(body, LANES_START, LANES_END), ready_nums, running_nums, sig
        )
        print(live_basis)
        print(f"projection delta:\n{delta}", file=sys.stderr)
        print("lanes: stale" if stale else "lanes: fresh", file=sys.stderr)
        return 1 if stale else 0

    if args.write_lanes:
        content = sys.stdin.read()
        if (why := reject_lanes_stdin(content)) is not None:
            print(f"--write-lanes: {why}", file=sys.stderr)
            return 2
        return write_lanes_block(args.epic, render_lanes_block(content, live_basis))

    block = render_block(recs, build_debt_line())

    if not args.write and not args.diff:
        print(block)
        return 0

    current = gh_json(["issue", "view", str(args.epic), "--json", "body"])["body"] or ""
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
