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

Reuses `_gh.py`'s process primitives, corpus-fetch cap + truncation warning
(`FETCH_CAP` / `_warn_if_truncated`), and the non-zero-tolerant single-issue view
primitive (`gh_issue_view_or_none`) rather than re-pasting them — leaf duplication is
this repo's named anti-pattern.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path

# gh/git process primitives plus the shared corpus-fetch cap + truncation warning live in
# the _gh module, loaded via spec so they resolve under
# `uv run --no-project python scripts/gh_issue.py` and spec-loaded pytest alike — the same
# idiom plan_sequence.py uses. Importing FETCH_CAP rather than redefining it keeps the cap
# single-sourced.
_GHSPEC = importlib.util.spec_from_file_location(
    "_gh", Path(__file__).with_name("_gh.py")
)
assert _GHSPEC and _GHSPEC.loader
_gh = importlib.util.module_from_spec(_GHSPEC)
_GHSPEC.loader.exec_module(_gh)

gh_json = _gh.gh_json
repo_owner_name = _gh.repo_owner_name
FETCH_CAP = _gh.FETCH_CAP
_warn_if_truncated = _gh._warn_if_truncated

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
    """The raw `gh issue view` payload for #number, or None if the number doesn't exist.

    A non-zero exit (a missing number) returns None. But a zero exit is NOT proof of an
    issue: `gh issue view` silently resolves a PR number too, returning its payload with
    exit 0 — so a maintainer PR arrives here as trusted content and a stranger's fork PR
    arrives as untrusted content, exactly like issues. That's fine: the trust boundary is
    NOT "is this an issue" — it's `view`'s `_is_maintainer` author check. This function
    only forwards the payload; whether to surface it is decided there.
    """
    fields = "number,title,state,body,author" + (",comments" if comments else "")
    return _gh.gh_issue_view_or_none(number, fields)


def view(number: int, comments: bool) -> tuple[int, str]:
    """Gate a single `issue view` read. Returns (exit_code, stdout_text).

    Authorship is the sole trust gate. Refuses (exit 3, no stdout) when the number is
    missing OR its content is not maintainer-authored — the untrusted body/comments are
    never surfaced. `gh issue view` may resolve a PR number, and that is harmless here: a
    non-maintainer PR (e.g. a stranger's fork PR) is refused by the same `_is_maintainer`
    author check as any non-maintainer issue, and a maintainer-authored PR is trusted
    content just like a maintainer issue. When maintainer-authored, returns JSON
    `{number,title,state,body[,comments]}` with `comments` (present only when requested)
    filtered to maintainer-authored entries (fail-closed: non-maintainer and author-less
    comments dropped).
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
        "state": data.get("state"),
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
