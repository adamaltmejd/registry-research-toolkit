#!/usr/bin/env python3
"""Shared `gh`/`git` process primitives for the scripts/ tooling.

`run` (checked subprocess → stdout), `gh_json` (run + JSON-decode), and `repo_owner_name`
(owner/name from $GITHUB_REPOSITORY, else `gh repo view`) are the thin, domain-neutral
wrappers every gh-driven script in here needs. They were born in `check_issue_hygiene.py`
and reused by `plan_sequence.py`; `pr_review_status.py` is the third consumer — the trigger
the note in `plan_sequence.py` named for lifting them into a shared module. The
issue-domain parsers (label sets, the relationship/touches regexes) stay in
`check_issue_hygiene.py`, still shared by only two consumers.

Stdlib only, and loaded by sibling scripts via `importlib` spec (not a plain `import`), so
it resolves under `uv run --no-project python scripts/<name>.py` and under pytest's
spec-loaded test modules alike, regardless of what's on sys.path.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any


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
