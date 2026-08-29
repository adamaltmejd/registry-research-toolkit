"""Shared `gh` process primitives for the scripts/ tooling.

`run` (checked subprocess → stdout) and `repo_owner_name` (owner/name from
$GITHUB_REPOSITORY, else `gh repo view`) are the thin, domain-neutral wrappers the
gh-driven scripts in here need. `gh_issue_view_or_none` is the single-issue `gh issue
view` primitive whose non-zero exit is a NORMAL signal (not an issue / missing) rather
than an error.

Stdlib only, and loaded by `gh_issue.py` via `importlib` spec (not a plain `import`), so
it resolves under `uv run --no-project python scripts/<name>.py` and under pytest's
spec-loaded test modules alike, regardless of what's on sys.path. The loader is a tiny
`sys.modules`-guarded `_load_gh()` preamble in the consumer — `_gh` can't load itself, and
one guarded entry keeps the whole process on a SINGLE `_gh` instance (one patch target,
not one copy per loader).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys


def run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        sys.stderr.write(f"command failed: {' '.join(cmd)}\n{proc.stderr}\n")
        raise SystemExit(2)
    return proc.stdout


def gh_issue_view_or_none(number: int, fields: str) -> dict | None:
    """`gh issue view <number> --json <fields>` decoded, or None on non-zero exit.

    Unlike `run` (which fatally `SystemExit`s on a non-zero exit), a non-zero exit here is
    a NORMAL signal — the number isn't a resolvable issue (a PR, or missing) — so it
    returns None instead of aborting. `gh issue view` also resolves a PR number, so a
    non-None result is NOT proof the number is an issue; the caller applies its own
    trust/state gate on top.
    """
    proc = subprocess.run(
        ["gh", "issue", "view", str(number), "--json", fields],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    return json.loads(proc.stdout)


def repo_owner_name() -> tuple[str, str]:
    slug = os.environ.get("GITHUB_REPOSITORY")
    if not slug:
        slug = json.loads(run(["gh", "repo", "view", "--json", "nameWithOwner"]))[
            "nameWithOwner"
        ]
    owner, name = slug.split("/", 1)
    return owner, name
