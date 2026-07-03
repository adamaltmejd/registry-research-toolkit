#!/usr/bin/env python3
"""Maintainer-author trust gate for the automation ingestion path.

This repo is PUBLIC. A stranger can open an issue, comment on one, or file a fork PR —
and that free text is untrusted: routing it into a model (the `/plan-lanes`, `/pr-
pipeline`, `/chief-of-staff` reads) or splicing it into the epic body / candidate floor
is a prompt-injection surface. This helper is the choke point: the ingestion path only
ever surfaces issue/PR text authored by the single trusted maintainer (repo owner, or
`REGISTRY_MAINTAINER_LOGIN`), so a non-maintainer's content is never read by a model and
never rendered into the projection.

It is deliberately an **allowlist helper, not a transparent `gh` shim.** A shim that
merely forwarded arbitrary `gh` invocations could not soundly filter — `gh api …`,
`gh search …`, GraphQL, and future subcommands return author-bearing payloads in shapes
this gate can't enumerate, so a "filter everything gh returns" promise would be a lie the
first time an un-modelled shape slipped through. Instead this exposes exactly the two
ingestion reads the automation needs (`fetch_open_issues` for the work-set,
`view <n> [--comments]` for a single issue) and gates each on maintainer authorship.

**Fail-closed** everywhere: a row/issue/comment with a missing, None, or non-maintainer
author is DROPPED, never surfaced. A drop is counted to stderr (observability — the gate
never *silently* discards), but the untrusted content itself is never printed.

Stdlib only. Loadable two ways, matching the sibling scripts:
  - as an importable module via `importlib` spec (plan_sequence.py loads it this way to
    swap in the gated `fetch_open_issues`);
  - as a CLI: `uv run --no-project python scripts/gh_issue.py view <n> [--comments]`.

Reuses `_gh.py`'s process primitives and `check_issue_hygiene.py`'s `FETCH_CAP` /
truncation warning (loaded via the same spec idiom) rather than re-pasting them — leaf
duplication is this repo's named anti-pattern.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

# gh/git process primitives (shared _gh module) and the corpus-fetch cap + truncation
# warning (check_issue_hygiene) are loaded via spec so they resolve under
# `uv run --no-project python scripts/gh_issue.py` and spec-loaded pytest alike — the same
# idiom plan_sequence.py uses. Importing FETCH_CAP rather than redefining it keeps the cap
# single-sourced.
_GHSPEC = importlib.util.spec_from_file_location(
    "_gh", Path(__file__).with_name("_gh.py")
)
assert _GHSPEC and _GHSPEC.loader
_gh = importlib.util.module_from_spec(_GHSPEC)
_GHSPEC.loader.exec_module(_gh)

_HSPEC = importlib.util.spec_from_file_location(
    "check_issue_hygiene", Path(__file__).with_name("check_issue_hygiene.py")
)
assert _HSPEC and _HSPEC.loader
_h = importlib.util.module_from_spec(_HSPEC)
_HSPEC.loader.exec_module(_h)

gh_json = _gh.gh_json
repo_owner_name = _gh.repo_owner_name
FETCH_CAP = _h.FETCH_CAP
_warn_if_truncated = _h._warn_if_truncated

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_REFUSED = 3


def maintainer_login() -> str:
    """The single trusted author: `REGISTRY_MAINTAINER_LOGIN`, else the repo owner.

    The env override lets a test or a fork of this tooling name a different owner without
    a live `gh repo view`; absent it, the repo owner is the maintainer (this is a single-
    maintainer repo whose owner == the trusted login).
    """
    override = os.environ.get("REGISTRY_MAINTAINER_LOGIN")
    if override:
        return override
    return repo_owner_name()[0]


def _login_of(obj: dict) -> str | None:
    """The author login of an issue/PR/comment dict, or None if absent/malformed.

    None is the fail-closed sentinel: a payload with no `author`, a null author, or no
    `login` is untrusted and must be dropped, never matched against the maintainer.
    """
    author = obj.get("author") or {}
    login = author.get("login")
    return login if isinstance(login, str) and login else None


def _is_maintainer(obj: dict, maintainer: str) -> bool:
    login = _login_of(obj)
    return login is not None and login.casefold() == maintainer.casefold()


def fetch_open_issues() -> list[dict]:
    """Open issues authored by the maintainer, in `fetch_open_issues`'s original shape.

    The gated replacement for `check_issue_hygiene.fetch_open_issues` on the ingestion
    path (plan_sequence's `build_records` consumes the result). Fetches the same fields
    plus `author`, then keeps ONLY maintainer-authored rows (fail-closed: a missing/None
    author is dropped). Rows keep the `number,title,labels,body` shape `build_records`
    reads — the extra `author` key is left in place and ignored downstream. The count of
    dropped non-maintainer issues is written to stderr (never silently discarded).
    """
    maintainer = maintainer_login()
    rows = gh_json(["issue", "list", "--state", "open", "--limit", str(FETCH_CAP),
                    "--json", "number,title,labels,body,author"])  # fmt: skip
    _warn_if_truncated(rows, "open issues")
    kept = [r for r in rows if _is_maintainer(r, maintainer)]
    dropped = len(rows) - len(kept)
    if dropped:
        sys.stderr.write(
            f"gh_issue: dropped {dropped} non-maintainer issue(s) "
            f"(author != {maintainer}) from the ingestion set\n"
        )
    return kept


def is_own_pr(pr: dict) -> bool:
    """Whether a PR is from a branch in THIS repository (not a fork).

    Fail-closed: only an explicit `isCrossRepository is False` is own-branch; None, True,
    or a missing field all read as not-own, so a fork PR's closing claims are never
    trusted into the running set.
    """
    return pr.get("isCrossRepository") is False


def _fetch_issue(number: int, comments: bool) -> dict | None:
    """The raw `gh issue view` payload for #number, or None if it isn't an issue.

    `gh issue view` resolves a PR number too and errors on a missing one; a non-zero exit
    (PR / missing) returns None so the caller refuses rather than surfacing PR text here.
    """
    fields = "number,title,body,author" + (",comments" if comments else "")
    proc = subprocess.run(
        ["gh", "issue", "view", str(number), "--json", fields],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return None
    return json.loads(proc.stdout)


def view(number: int, comments: bool) -> tuple[int, str]:
    """Gate a single `issue view` read. Returns (exit_code, stdout_text).

    Refuses (exit 3, no stdout) when the issue is missing, is a PR, or is NOT maintainer-
    authored — the untrusted body/comments are never surfaced. When maintainer-authored,
    returns JSON `{number,title,body[,comments]}` with `comments` (present only when
    requested) filtered to maintainer-authored entries (fail-closed: non-maintainer and
    author-less comments dropped).
    """
    maintainer = maintainer_login()
    data = _fetch_issue(number, comments)
    if data is None or not _is_maintainer(data, maintainer):
        sys.stderr.write(
            f"gh_issue: issue #{number} is not maintainer-authored; "
            f"refusing to surface untrusted content\n"
        )
        return EXIT_REFUSED, ""
    out: dict = {
        "number": data.get("number"),
        "title": data.get("title"),
        "body": data.get("body"),
    }
    if comments:
        raw = data.get("comments") or []
        out["comments"] = [c for c in raw if _is_maintainer(c, maintainer)]
    return EXIT_OK, json.dumps(out, ensure_ascii=False)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser(
        "view", help="print a maintainer-authored issue as JSON, or refuse"
    )
    v.add_argument("number", type=int, help="issue number")
    v.add_argument("--comments", action="store_true",
                   help="include maintainer-authored comments")  # fmt: skip
    try:
        args = ap.parse_args(argv)
    except SystemExit:
        return EXIT_USAGE

    code, text = view(args.number, args.comments)
    if text:
        print(text)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
