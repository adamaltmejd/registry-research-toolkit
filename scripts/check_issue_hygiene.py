#!/usr/bin/env python3
"""Issue-tracker hygiene check — enforces the AGENTS.md "# Issue tracker" conventions.

Read-only validator over the GitHub issue corpus. It reports; it never edits.

Checks (per open issue):
  - exactly one area label + one type label;
  - every `Relationships` target (`Depends on`/`Blocked by`/`Part of`/… #N) resolves
    to a real issue or PR;
  - the `blocked` label agrees with whether an open blocker actually exists;
  - native sub-issue ↔ `Part of #N` prose agree (catches half-wired epics);
  - `touches` globs resolve to real paths (a zero-match glob is a warning, not an
    error — new-file paths are legitimate).

Plus two corpus-wide drift alerts (`--all` only):
  - merged-but-still-open: a merged PR closed #N (via closing keyword) yet #N is open;
  - merged-but-unreleased: reg_meta_build DB content changed since the latest
    `reg_meta_build/v*` tag, so a rebuild+release is pending (the #373-class debt).

Modes:
  --issue N   validate one issue; exit non-zero on any ERROR (the write-time nudge,
              fired by the `issues` event).
  --all       validate every open issue + the corpus alerts; report-only, exit 0
              (the scheduled drift report).

Data comes from `gh` (issues/PRs + the sub-issue GraphQL) and `git` (the release-tag
diff); both are present and authenticated in CI. Stdlib only — run with
`uv run --no-project python scripts/check_issue_hygiene.py …`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

AREA_LABELS = {
    "reg_meta",
    "reg_meta_build",
    "reg_schema",
    "reg_monabundle",
    "reg_webapp",
    "mock_data_wizard",
    "cross-package",
}
TYPE_LABELS = {"enhancement", "bug", "documentation"}

# Relationship keywords from the AGENTS.md "# Issue tracker" convention. Order the
# alternation longest-first so "Follow-up to" wins over a bare "to"-prefix mismatch.
REL_RE = re.compile(
    r"\b(Part of|Depends on|Blocked by|Follow-up to|Supersedes|Related to)\s+#(\d+)",
    re.IGNORECASE,
)
BLOCKING_KEYWORDS = {"depends on", "blocked by"}

# A ```touches fence and its body; tolerant of indentation (nested under a list item).
# Bodies are CRLF-normalized before matching (GitHub web edits carry \r\n).
TOUCHES_RE = re.compile(
    r"^[ \t]*```touches[ \t]*\n(.*?)^[ \t]*```", re.MULTILINE | re.DOTALL
)
# A fenced code block of N≥3 backticks and its matching close — stripped before the
# relationship scan so example/quoted `… #N` inside fences can't mint false targets.
FENCE_RE = re.compile(r"^(`{3,}).*?^\1", re.MULTILINE | re.DOTALL)

# reg_meta_build paths whose change alters the GLOBAL built DB (the released
# reg_meta_build/v* asset): src/**, top-level *.toml, and the top-level slug snapshots.
# Excluded: tests, DESIGN.md, the version bump (pyproject.toml), and steward-flavor
# content under fqid_slugs/<steward>/ — that rides a separate release line (#365).
BUILD_CONTENT_RE = re.compile(r"^reg_meta_build/(src/.+|[^/]+\.toml|fqid_slugs/[^/]+)$")
BUILD_IGNORE = {"reg_meta_build/pyproject.toml"}


class Findings:
    def __init__(self) -> None:
        self.items: list[tuple[str, int | None, str]] = []

    def error(self, num: int | None, msg: str) -> None:
        self.items.append(("ERROR", num, msg))

    def warn(self, num: int | None, msg: str) -> None:
        self.items.append(("WARN", num, msg))

    @property
    def errors(self) -> int:
        return sum(1 for level, _, _ in self.items if level == "ERROR")


def run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(f"command failed: {' '.join(cmd)}\n{proc.stderr}\n")
        raise SystemExit(2)
    return proc.stdout


def gh_json(args: list[str]) -> Any:
    return json.loads(run(["gh", *args]))


def repo_owner_name() -> tuple[str, str]:
    slug = os.environ.get("GITHUB_REPOSITORY")
    if not slug:
        slug = json.loads(run(["gh", "repo", "view", "--json", "nameWithOwner"]))[
            "nameWithOwner"
        ]
    owner, name = slug.split("/", 1)
    return owner, name


def _normalize(body: str | None) -> str:
    return (body or "").replace("\r\n", "\n").replace("\r", "\n")


def parse_relationships(body: str) -> list[tuple[str, int]]:
    # Strip fenced code so a quoted/example `… #N` can't mint a false target.
    text = FENCE_RE.sub("", _normalize(body))
    return [(kw.lower(), int(num)) for kw, num in REL_RE.findall(text)]


def parse_touches(body: str) -> list[str]:
    globs: list[str] = []
    for block in TOUCHES_RE.findall(_normalize(body)):
        for line in block.splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                globs.append(line)
    return globs


def fetch_open_issues() -> list[dict]:
    return gh_json(
        ["issue", "list", "--state", "open", "--limit", "500",
         "--json", "number,title,labels,body"]
    )  # fmt: skip


def fetch_number_states() -> tuple[set[int], dict[int, str]]:
    """All issue+PR numbers that exist, and each issue's open/closed state.

    PRs share the issue number space, so a `Related to #<pr>` must resolve too — but
    only issues carry the open/closed semantics the blocked-label check needs.
    """
    issues = gh_json(["issue", "list", "--state", "all", "--limit", "2000",
                      "--json", "number,state"])  # fmt: skip
    prs = gh_json(["pr", "list", "--state", "all", "--limit", "2000",
                   "--json", "number"])  # fmt: skip
    issue_state = {i["number"]: i["state"] for i in issues}
    known = set(issue_state) | {p["number"] for p in prs}
    return known, issue_state


def fetch_parents(owner: str, name: str) -> dict[int, int]:
    """Map each open issue to its native sub-issue parent (for the `Part of` check)."""
    parent_of: dict[int, int] = {}
    query = (
        "query($owner:String!,$name:String!,$cursor:String){"
        "repository(owner:$owner,name:$name){"
        "issues(first:100,after:$cursor,states:OPEN){"
        "pageInfo{hasNextPage endCursor}"
        "nodes{number parent{number}}}}}"
    )
    cursor: str | None = None
    while True:
        args = ["api", "graphql", "-f", f"query={query}",
                "-F", f"owner={owner}", "-F", f"name={name}"]  # fmt: skip
        if cursor:
            args += ["-F", f"cursor={cursor}"]
        conn = gh_json(args)["data"]["repository"]["issues"]
        for node in conn["nodes"]:
            if node.get("parent"):
                parent_of[node["number"]] = node["parent"]["number"]
        if conn["pageInfo"]["hasNextPage"]:
            cursor = conn["pageInfo"]["endCursor"]
        else:
            return parent_of


def check_issue(
    issue: dict,
    known: set[int],
    issue_state: dict[int, str],
    parent_of: dict[int, int],
    repo_root: Path,
    out: Findings,
) -> None:
    num = issue["number"]
    labels = {label["name"] for label in issue["labels"]}
    body = issue.get("body") or ""

    area = labels & AREA_LABELS
    if len(area) != 1:
        out.error(num, f"needs exactly one area label (has: {sorted(area) or 'none'})")
    type_ = labels & TYPE_LABELS
    if len(type_) != 1:
        out.error(num, f"needs exactly one type label (has: {sorted(type_) or 'none'})")

    rels = parse_relationships(body)
    for kw, target in rels:
        if target not in known:
            out.error(num, f"'{kw} #{target}' points to a non-existent issue/PR")

    open_blockers = sorted(
        {
            t
            for kw, t in rels
            if kw in BLOCKING_KEYWORDS and issue_state.get(t) == "OPEN"
        }
    )
    if "blocked" in labels and not open_blockers:
        out.warn(num, "has 'blocked' label but no open Depends-on/Blocked-by target — "
                      "remove it if unblocked")  # fmt: skip
    if open_blockers and "blocked" not in labels:
        out.warn(num, f"open blocker(s) {open_blockers} but no 'blocked' label")

    part_of = [t for kw, t in rels if kw == "part of"]
    native_parent = parent_of.get(num)
    for target in part_of:
        if native_parent != target:
            out.warn(num, f"says 'Part of #{target}' but native parent is "
                          f"{native_parent or 'unset'} — wire: gh issue edit {num} "
                          f"--parent {target}")  # fmt: skip
    if native_parent and native_parent not in part_of:
        out.warn(num, f"native sub-issue of #{native_parent} but body has no "
                      f"'Part of #{native_parent}'")  # fmt: skip

    for pattern in parse_touches(body):
        if not any(repo_root.glob(pattern)):
            out.warn(
                num, f"touches '{pattern}' matches no files (ok if it's a new file)"
            )


def check_done_but_open(issue_state: dict[int, str], out: Findings) -> None:
    # Limit comfortably exceeds total merged PRs — covers the full closing history.
    prs = gh_json(["pr", "list", "--state", "merged", "--limit", "2000",
                   "--json", "number,closingIssuesReferences"])  # fmt: skip
    for pr in prs:
        for ref in pr.get("closingIssuesReferences") or []:
            if issue_state.get(ref["number"]) == "OPEN":
                out.warn(ref["number"], f"still open but merged PR #{pr['number']} "
                                        f"closes it — verify and close")  # fmt: skip


def check_unreleased_build_debt(repo_root: Path, out: Findings) -> None:
    tags = run(["git", "-C", str(repo_root), "tag", "--list", "reg_meta_build/v*",
                "--sort=-version:refname"]).split()  # fmt: skip
    if not tags:
        return
    tag = tags[0]
    changed = run(["git", "-C", str(repo_root), "diff", "--name-only",
                   f"{tag}..HEAD", "--", "reg_meta_build"]).splitlines()  # fmt: skip
    content = [
        f for f in changed if f not in BUILD_IGNORE and BUILD_CONTENT_RE.match(f)
    ]
    if content:
        preview = ", ".join(sorted(content)[:6])
        more = "" if len(content) <= 6 else f" (+{len(content) - 6} more)"
        out.warn(None, f"reg_meta_build DB content changed since {tag} "
                       f"({len(content)} files: {preview}{more}) — a rebuild+release "
                       f"is pending")  # fmt: skip


def emit(out: Findings, scope: str) -> None:
    lines = [f"## Issue hygiene — {scope}", ""]
    if not out.items:
        lines.append("✓ all checks passed")
    else:
        errs = out.errors
        warns = len(out.items) - errs
        lines.append(f"**{errs} error(s), {warns} warning(s)**")
        lines.append("")
        for level, num, msg in out.items:
            where = f"#{num}: " if num is not None else ""
            lines.append(f"- **{level}** {where}{msg}")
    report = "\n".join(lines)
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as fh:
            fh.write(report + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--issue", type=int, help="validate one issue; fail on ERROR")
    group.add_argument("--all", action="store_true", help="validate the whole corpus")
    args = ap.parse_args()

    repo_root = Path(run(["git", "rev-parse", "--show-toplevel"]).strip())
    owner, name = repo_owner_name()
    known, issue_state = fetch_number_states()
    parent_of = fetch_parents(owner, name)
    out = Findings()

    if args.issue is not None:
        issues = fetch_open_issues()
        issue = next((i for i in issues if i["number"] == args.issue), None)
        if issue is None:
            # Not an open issue (closed, or a PR) — nothing to validate, not a failure.
            print(f"#{args.issue} is not an open issue; skipping.")
            return 0
        check_issue(issue, known, issue_state, parent_of, repo_root, out)
        emit(out, f"#{args.issue}")
        return 1 if out.errors else 0

    for issue in fetch_open_issues():
        check_issue(issue, known, issue_state, parent_of, repo_root, out)
    check_done_but_open(issue_state, out)
    check_unreleased_build_debt(repo_root, out)
    emit(out, "all open issues")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
