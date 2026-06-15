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

The hardened parsers + gh fetchers are reused from the sibling validator
(check_issue_hygiene.py); if a third consumer appears, lift them into a shared module.
"""

from __future__ import annotations

import argparse
import fnmatch
import importlib.util
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

_HSPEC = importlib.util.spec_from_file_location(
    "check_issue_hygiene", Path(__file__).with_name("check_issue_hygiene.py")
)
assert _HSPEC and _HSPEC.loader
_h = importlib.util.module_from_spec(_HSPEC)
_HSPEC.loader.exec_module(_h)

AREA_LABELS = _h.AREA_LABELS
BLOCKING_KEYWORDS = _h.BLOCKING_KEYWORDS
FETCH_CAP = _h.FETCH_CAP
gh_json = _h.gh_json
parse_relationships = _h.parse_relationships
parse_touches = _h.parse_touches

START = "<!-- plan-sequence:start -->"
END = "<!-- plan-sequence:end -->"


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
    status: str = field(default="")


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


def splice_block(body: str, block: str) -> str:
    """Replace the marked region with `block`, or append it if the markers are absent.

    Only a well-formed, ordered START…END pair is replaced. A lone or reversed marker
    (e.g. a half-copied end-comment in the human narrative) falls through to append, so a
    malformed body is never scrambled on write.
    """
    start, end = body.find(START), body.find(END)
    if start != -1 and end != -1 and end > start:
        return body[:start] + block + body[end + len(END) :]
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
        )
        rec.status = classify(rec)
        recs.append(rec)
    return sorted(recs, key=lambda r: r.number)


# --- dispatch (read-only input for the agent's lane judgment) ------------------------


def dispatch_view(records: list[Rec]) -> str:
    """Read-only dispatch candidates for the agent to compose a lane from.

    Lists ready issues that DON'T touch in-flight (running) work, grouped by area, with
    `touches` and must-serialize groups. Which issues form a lane, and how many, is a
    judgment call left to the agent — the script supplies the deterministic facts and
    excludes only what would collide with work already in flight.
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
            f"  #{r.number} {r.title}  —  "
            + (", ".join(r.touches) if r.touches else "(no touches declared)")
            for r in sorted(by_area[area], key=lambda r: r.number)
        ]
        out.append("")
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


def extract_block(body: str) -> str:
    """The current generated block in `body` (between the markers), or '' if absent."""
    start, end = body.find(START), body.find(END)
    return body[start : end + len(END)] if (start != -1 and end > start) else ""


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
    args = ap.parse_args()

    owner, name = _h.repo_owner_name()
    _known, _issue_state, open_numbers = _h.fetch_number_states()
    parent_of = _h.fetch_parents(owner, name)
    recs = build_records(_h.fetch_open_issues(), open_numbers, parent_of)

    if args.lane:
        print(dispatch_view(recs))
        return 0

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
