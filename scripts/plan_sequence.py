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
import importlib.util
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


def touches_overlap(a: list[str], b: list[str]) -> bool:
    """Two touch sets conflict if a pattern is shared or one contains the other's dir."""
    for pa in a:
        for pb in b:
            x, y = _norm(pa), _norm(pb)
            if x == y or x.startswith(y + "/") or y.startswith(x + "/"):
                return True
    return False


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


# --- render --------------------------------------------------------------------------


def _line(rec: Rec) -> str:
    area = f"`{rec.area}`" if rec.area else "_no area_"
    return f"- #{rec.number} {rec.title} · {area}"


def render_block(recs: list[Rec], debt: str | None) -> str:
    # Sort here (not just upstream) so the block is order-stable for any caller.
    by_status = {s: sorted((r for r in recs if r.status == s), key=lambda r: r.number)
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
        f"_{len(recs)} open · {len(ready)} ready · {len(running)} running · "
        f"{len(blocked)} blocked_",
        "",
        "### Ready now",
    ]
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


# --- main ----------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--write", type=int, metavar="EPIC",
                    help="splice the block into epic <EPIC>'s body (default: print)")  # fmt: skip
    args = ap.parse_args()

    owner, name = _h.repo_owner_name()
    _known, _issue_state, open_numbers = _h.fetch_number_states()
    parent_of = _h.fetch_parents(owner, name)
    recs = build_records(_h.fetch_open_issues(), open_numbers, parent_of)
    block = render_block(recs, build_debt_line())

    if args.write is None:
        print(block)
        return 0

    current = (
        gh_json(["issue", "view", str(args.write), "--json", "body"])["body"] or ""
    )
    new_body = splice_block(current, block)
    if new_body == current:
        print(f"#{args.write} already up to date.")
        return 0
    subprocess.run(
        ["gh", "issue", "edit", str(args.write), "--body-file", "-"],
        input=new_body,
        text=True,
        check=True,
    )
    print(f"Spliced the sequencing block into #{args.write}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
