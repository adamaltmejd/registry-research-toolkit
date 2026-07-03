#!/usr/bin/env python3
"""Issue-tracker hygiene check — enforces the AGENTS.md "# Issue tracker" conventions.

Read-only validator over the GitHub issue corpus. It reports; it never edits.

Checks (per open issue):
  - exactly one area label + one type label;
  - every `Relationships` target (`Depends on`/`Blocked by`/`Part of`/… #N) resolves
    to a real issue or PR;
  - the `blocked` label agrees with whether an open blocker actually exists;
  - the `parked` label is not combined with `blocked`;
  - native sub-issue ↔ `Part of #N` prose agree (catches half-wired epics);
  - `touches` globs resolve to real paths (a zero-match glob is a warning, not an
    error — new-file paths are legitimate).

Plus three corpus-wide drift alerts (`--all` only):
  - merged-but-still-open: a merged PR closed #N (via closing keyword) yet #N is open;
  - merged-but-unreleased: reg_meta_build DB content changed since the latest
    `reg_meta_build/v*` tag, so a rebuild+release is pending (the #373-class debt);
  - stale-steward-catalog: a `reg_webapp/stewards/<id>/steward.project_data.json` was
    generated against a reg_meta release older than the latest *published* `reg_meta/v*`
    release, so it may have drifted and should be regenerated (coverage hygiene, not a
    hard gate).

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
import importlib.util
import json
import os
import re
from pathlib import Path

# gh/git process primitives live in the shared _gh module (loaded via spec so it resolves
# regardless of sys.path — same idiom plan_sequence.py uses to load this module).
_GHSPEC = importlib.util.spec_from_file_location(
    "_gh", Path(__file__).with_name("_gh.py")
)
assert _GHSPEC and _GHSPEC.loader
_gh = importlib.util.module_from_spec(_GHSPEC)
_GHSPEC.loader.exec_module(_gh)

run = _gh.run
gh_json = _gh.gh_json
repo_owner_name = _gh.repo_owner_name
# Corpus-fetch plumbing now lives in _gh (domain-neutral, shared with gh_issue.py).
# Re-exported so existing `_h.FETCH_CAP` / `_h._warn_if_truncated` importers
# (plan_sequence.py) keep resolving unchanged.
FETCH_CAP = _gh.FETCH_CAP
_warn_if_truncated = _gh._warn_if_truncated

AREA_LABELS = {
    "reg_meta",
    "reg_meta_build",
    "reg_schema",
    # reg_monabundle + mock_data_wizard were archived to `archive/mona-subsystem`
    # pending a from-scratch MONA rebuild; their labels are retained here so the
    # historical (open + closed) issues that carry them keep validating.
    "reg_monabundle",
    "reg_webapp",
    "mock_data_wizard",
    "cross-package",
}
TYPE_LABELS = {"enhancement", "bug", "documentation"}
# Optional ranking signal for /plan-lanes (default = neither label = normal). At most one;
# coarse + stable enough to be a label (unlike the churny S/L/G lanes, which stay prose).
PRIORITY_LABELS = {"priority:high", "priority:low"}
# Optional status labels. `blocked` means an open dependency stalls the issue; `parked`
# means maintainer-deferred and excluded from /plan-lanes dispatch without needing a
# synthetic blocker.
STATUS_LABELS = {"blocked", "parked"}

# A relationship tie from the AGENTS.md "# Issue tracker" convention. Anchored to the
# start of a line (after optional bullet/quote markup) so a casual prose mention
# ("…related to #99 in passing…") does NOT mint a tie. Keyword whitespace is `\s+` so a
# reflow-inserted double space / newline still matches; the number group captures a
# comma list (`Blocked by #1, #2`) so every target is seen.
REL_RE = re.compile(
    r"(?im)^[ \t>*+-]*"
    r"(Part\s+of|Depends\s+on|Blocked\s+by|Follow-up\s+to|Supersedes|Related\s+to)"
    r"\s+(#\d+(?:\s*,\s*#\d+)*)",
)
BLOCKING_KEYWORDS = {"depends on", "blocked by"}

# A ```touches fence and its body; tolerant of indentation (nested under a list item).
# Bodies are CRLF-normalized before matching (GitHub web edits carry \r\n).
TOUCHES_RE = re.compile(
    r"^[ \t]*```touches[ \t]*\n(.*?)^[ \t]*```", re.MULTILINE | re.DOTALL
)
# A fenced code block of N≥3 backticks and its matching close — stripped before the
# relationship scan so example/quoted `… #N` inside fences can't mint false targets.
# Leading indentation is allowed on both fences: GitHub renders a fence nested under a
# list item, and REL_RE tolerates indentation, so a column-0-only strip would leak it.
FENCE_RE = re.compile(r"^[ \t]*(`{3,}).*?^[ \t]*\1", re.MULTILINE | re.DOTALL)

# reg_meta_build paths whose change alters the GLOBAL built DB (the released
# reg_meta_build/v* asset): src/**, top-level *.toml, and the top-level slug snapshots.
# Excluded: tests, DESIGN.md, the version bump (pyproject.toml + the __version__-only
# __init__.py — both move on every release and would otherwise trip the alert forever),
# and steward-flavor content under fqid_slugs/<steward>/ (separate release line, #365).
BUILD_CONTENT_RE = re.compile(r"^reg_meta_build/(src/.+|[^/]+\.toml|fqid_slugs/[^/]+)$")
BUILD_IGNORE = {
    "reg_meta_build/pyproject.toml",
    "reg_meta_build/src/reg_meta_build/__init__.py",
}


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


def _normalize(body: str | None) -> str:
    return (body or "").replace("\r\n", "\n").replace("\r", "\n")


def parse_relationships(body: str) -> list[tuple[str, int]]:
    # Strip fenced code so a quoted/example `… #N` can't mint a false target.
    text = FENCE_RE.sub("", _normalize(body))
    rels: list[tuple[str, int]] = []
    for kw, nums in REL_RE.findall(text):
        keyword = re.sub(r"\s+", " ", kw.lower())
        rels += [(keyword, int(n)) for n in re.findall(r"#(\d+)", nums)]
    return rels


def parse_touches(body: str) -> list[str]:
    globs: list[str] = []
    for block in TOUCHES_RE.findall(_normalize(body)):
        for line in block.splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                globs.append(line)
    return globs


def fetch_open_issues() -> list[dict]:
    rows = gh_json(["issue", "list", "--state", "open", "--limit", str(FETCH_CAP),
                    "--json", "number,title,labels,body"])  # fmt: skip
    _warn_if_truncated(rows, "open issues")
    return rows


def fetch_one_open_issue(number: int) -> dict | None:
    """The issue's fields if #number is an OPEN issue, else None (closed / PR / missing).

    Fetched directly so one issue's body/labels don't require listing the whole corpus.
    The caller still gates on `issue_state` (PR-excluding) before trusting OPEN here —
    `gh issue view` resolves a PR number too. Shares _gh's non-zero-tolerant view
    primitive; keeps its own `state == "OPEN"` post-filter and field set.
    """
    data = _gh.gh_issue_view_or_none(number, "number,title,labels,body,state")
    if data is None:  # not an issue (a PR, or it doesn't exist)
        return None
    return data if data.get("state") == "OPEN" else None


def fetch_number_states() -> tuple[set[int], dict[int, str], set[int]]:
    """`(known, issue_state, open_numbers)`.

    `known` — every issue+PR number that exists (PRs share the number space, so a
    `Related to #<pr>` must resolve). `issue_state` — issues only (number → state), the
    PR-excluding authority for the `--issue` gate. `open_numbers` — open issues AND open
    PRs, since `Blocked by #<open PR>` is a real blocker (blocked until the PR merges).
    """
    issues = gh_json(["issue", "list", "--state", "all", "--limit", str(FETCH_CAP),
                      "--json", "number,state"])  # fmt: skip
    prs = gh_json(["pr", "list", "--state", "all", "--limit", str(FETCH_CAP),
                   "--json", "number,state"])  # fmt: skip
    _warn_if_truncated(issues, "issues")
    _warn_if_truncated(prs, "PRs")
    issue_state = {i["number"]: i["state"] for i in issues}
    pr_state = {p["number"]: p["state"] for p in prs}
    known = set(issue_state) | set(pr_state)
    open_numbers = {n for n, s in (issue_state | pr_state).items() if s == "OPEN"}
    return known, issue_state, open_numbers


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
    open_numbers: set[int],
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
    # Priority is optional (absence = normal) but mutually exclusive — at most one bucket.
    prio = labels & PRIORITY_LABELS
    if len(prio) > 1:
        out.error(
            num, f"has multiple priority labels ({sorted(prio)}); keep at most one"
        )

    rels = parse_relationships(body)
    for kw, target in rels:
        if target not in known:
            out.error(num, f"'{kw} #{target}' points to a non-existent issue/PR")

    open_blockers = sorted(
        {t for kw, t in rels if kw in BLOCKING_KEYWORDS and t in open_numbers}
    )
    if "blocked" in labels and "parked" in labels:
        out.warn(num, "has both 'blocked' and 'parked' labels — keep one status reason")
    if "blocked" in labels and not open_blockers:
        out.warn(num, "has 'blocked' label but no open Depends-on/Blocked-by target — "
                      "remove it if unblocked")  # fmt: skip
    if open_blockers and "blocked" not in labels and "parked" not in labels:
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
        # Must be repo-relative: an absolute / "." pattern raises in Path.glob, and ".."
        # escapes the repo (could spuriously match a sibling) — flag, never glob those.
        if pattern.startswith("/") or pattern == "." or ".." in pattern.split("/"):
            out.warn(num, f"touches '{pattern}' is not a repo-relative path")
            continue
        try:
            matched = any(repo_root.glob(pattern))
        except OSError, ValueError, NotImplementedError:
            out.warn(num, f"touches '{pattern}' is not a valid glob")
            continue
        if not matched:
            out.warn(
                num, f"touches '{pattern}' matches no files (ok if it's a new file)"
            )


def check_done_but_open(issue_state: dict[int, str], out: Findings) -> None:
    prs = gh_json(["pr", "list", "--state", "merged", "--limit", str(FETCH_CAP),
                   "--json", "number,closingIssuesReferences"])  # fmt: skip
    _warn_if_truncated(prs, "merged PRs")
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


# The reg_meta release a steward catalog was generated against, as `reg_meta/vX.Y.Z`.
# Parse the X.Y.Z into an int tuple so comparison is numeric, not lexical (v0.10 > v0.9).
_REG_META_TAG_RE = re.compile(r"^reg_meta/v(\d+)\.(\d+)\.(\d+)$")


def _reg_meta_version_tuple(value: str) -> tuple[int, int, int] | None:
    m = _REG_META_TAG_RE.match(value.strip())
    return (int(m[1]), int(m[2]), int(m[3])) if m else None


def latest_published_reg_meta_tag() -> str | None:
    """The newest *published* (non-draft) `reg_meta/v*` release tag, else None.

    Mirrors `.github/workflows/container-build.yml`'s resolution: the webapp container
    bakes the newest published `reg_meta/v*` release's DB asset, so the steward catalog
    must be keyed on the *published* release — not the git tag. A git tag exists for a
    draft/failed release the deploy can't resolve, so a tag-based lookup would false-warn
    during the draft window between cutting the tag and publishing the release.
    """
    releases = gh_json(["release", "list", "--limit", "100",
                        "--json", "tagName,isDraft"])  # fmt: skip
    candidates = [
        _reg_meta_version_tuple(r["tagName"])
        for r in releases
        if not r.get("isDraft") and r.get("tagName", "").startswith("reg_meta/v")
    ]
    parsed = [v for v in candidates if v is not None]
    if not parsed:
        return None
    major, minor, patch = max(parsed)
    return f"reg_meta/v{major}.{minor}.{patch}"


def check_steward_catalog_staleness(repo_root: Path, out: Findings) -> None:
    """Warn when a committed steward catalog lags the latest published `reg_meta/v*` release.

    A `reg_webapp/stewards/<id>/steward.project_data.json` records the reg_meta release it
    was generated against (`reg_meta_version`). A newer release can drift it (slug churn,
    new content, overlap fixes); the webapp still boots through the drift, so this is
    coverage hygiene, not a hard gate. Regenerate per `stewards/<id>/README.md`.

    Keyed on the latest *published* release (not the git tag) so it mirrors what the
    webapp container actually bakes — see `latest_published_reg_meta_tag`.
    """
    tag = latest_published_reg_meta_tag()
    if tag is None:  # no published reg_meta release yet (or no gh) → nothing to compare
        return
    latest = _reg_meta_version_tuple(tag)
    if latest is None:  # unexpected tag shape — don't crash, just skip
        return
    catalogs = (repo_root / "reg_webapp").glob("stewards/*/steward.project_data.json")
    for catalog in sorted(catalogs):
        steward = catalog.parent.name
        try:
            data = json.loads(catalog.read_text(encoding="utf-8"))
        except OSError, ValueError:
            out.warn(None, f"steward '{steward}' catalog {catalog.name} is unreadable "
                           f"or not valid JSON — cannot check reg_meta_version")  # fmt: skip
            continue
        recorded = data.get("reg_meta_version")
        version = (
            _reg_meta_version_tuple(recorded) if isinstance(recorded, str) else None
        )
        if version is None:
            out.warn(None, f"steward '{steward}' catalog has a missing or malformed "
                           f"reg_meta_version ({recorded!r}) — expected 'reg_meta/vX.Y.Z'")  # fmt: skip
            continue
        if version < latest:  # strictly behind; equal or (defensively) ahead → silent
            out.warn(None, f"steward '{steward}' catalog was generated against {recorded} "
                           f"but the latest reg_meta release is {tag} — regenerate per "
                           f"reg_webapp/stewards/{steward}/README.md")  # fmt: skip


def emit(out: Findings, scope: str) -> None:
    lines = [f"## Issue hygiene — {scope}", ""]
    if not out.items:
        lines.append("✓ all checks passed")
    else:
        errs = out.errors
        warns = len(out.items) - errs
        lines.append(f"**{errs} error(s), {warns} warning(s)**")
        lines.append("")
        # Deterministic order (stable diffs): by issue number, ERROR before WARN,
        # corpus-wide alerts (num=None) last.
        ordered = sorted(
            out.items, key=lambda it: (it[1] is None, it[1] or 0, it[0] == "WARN")
        )
        for level, num, msg in ordered:
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
    known, issue_state, open_numbers = fetch_number_states()
    parent_of = fetch_parents(owner, name)
    out = Findings()

    if args.issue is not None:
        # issue_state comes from `gh issue list` (PR-excluding), so it is the authority
        # on "is this an OPEN issue" — `gh issue view <n>` resolves a PR number too.
        is_open_issue = issue_state.get(args.issue) == "OPEN"
        issue = fetch_one_open_issue(args.issue) if is_open_issue else None
        if issue is None:
            print(f"#{args.issue} is not an open issue; skipping.")
            return 0
        check_issue(issue, known, open_numbers, parent_of, repo_root, out)
        emit(out, f"#{args.issue}")
        return 1 if out.errors else 0

    for issue in fetch_open_issues():
        check_issue(issue, known, open_numbers, parent_of, repo_root, out)
    check_done_but_open(issue_state, out)
    check_unreleased_build_debt(repo_root, out)
    check_steward_catalog_staleness(repo_root, out)
    emit(out, "all open issues")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
