#!/usr/bin/env python3
"""Shared `gh`/`git` process primitives for the scripts/ tooling.

`run` (checked subprocess → stdout), `gh_json` (run + JSON-decode), and `repo_owner_name`
(owner/name from $GITHUB_REPOSITORY, else `gh repo view`) are the thin, domain-neutral
wrappers every gh-driven script in here needs. They were born in `check_issue_hygiene.py`
and reused by `plan_sequence.py`; `pr_review_status.py` is the third consumer — the trigger
the note in `plan_sequence.py` named for lifting them into a shared module. The
issue-domain parsers (label sets, the relationship/touches regexes) stay in
`check_issue_hygiene.py`, still shared by only two consumers.

`run_tolerant` is the non-zero-tolerant counterpart to `run`: it hands back the
`CompletedProcess` (a non-zero exit is a signal the caller inspects, not a fatal error)
and only SystemExits on a MISSING executable. Lifted out of `cos_preflight.py`, whose
`git`/`gh`/sibling-probe calls all read a meaningful non-zero exit.

The corpus-fetch plumbing lives here too — `FETCH_CAP` (the list-fetch ceiling) and
`_warn_if_truncated` (its overflow warning) are domain-neutral and shared by both
`check_issue_hygiene.py` and `gh_issue.py`; `check_issue_hygiene.py` re-exports them so
its existing importers resolve unchanged. `gh_issue_view_or_none` is the single-issue
`gh issue view` primitive whose non-zero exit is a NORMAL signal (not an issue / missing),
shared by both single-issue readers rather than re-pasted — leaf duplication is this
repo's named anti-pattern.

Stdlib only, and loaded by sibling scripts via `importlib` spec (not a plain `import`), so
it resolves under `uv run --no-project python scripts/<name>.py` and under pytest's
spec-loaded test modules alike, regardless of what's on sys.path.

`load_sibling` is the ONE shared, `sys.modules`-guarded spec-loader every sibling script
uses to pull in its other siblings (`gh_issue`, `plan_sequence`, `cos_preflight`, …). It
returns the existing `sys.modules[name]` when present rather than re-executing, so a name
loaded once (by any script or by a spec-loading test) is a SINGLE process-wide instance —
`cos_preflight`'s `gh_issue` and its `_gh` are the same objects `gh_issue`/`plan_sequence`
loaded, so a monkeypatch through one copy is visible through the other (the two-`_gh`-copy
footgun the pre-guard code had). `_gh` itself can't be loaded via its own helper (a module
can't import itself before it finishes executing), so each script keeps a tiny
`sys.modules`-guarded `_load_gh()` preamble — the single leaf that cannot be hoisted;
everything downstream of `_gh` goes through `load_sibling`.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from types import ModuleType

FETCH_CAP = (
    5000  # well above the live corpus; a hit is reported, never silently dropped
)


def load_sibling(name: str) -> ModuleType:
    """Spec-load sibling script `<name>.py`, `sys.modules`-guarded — one instance/process.

    The shared loader for the scripts/ tooling: siblings import each other via `importlib`
    spec (not a plain `import`) so they resolve under `uv run --no-project python
    scripts/<name>.py` and under pytest's spec-loaded test modules alike, regardless of
    what's on sys.path. Guarding on `sys.modules` is what makes it single-instance: if
    `name` is already loaded (by another script or a spec-loading test), that same module
    object is returned instead of a second, divergent copy — so a monkeypatch or attribute
    read through one consumer is visible through every other. Registers the fresh module in
    `sys.modules` BEFORE exec, so a self-referential construct (e.g. `@dataclass`, which
    resolves `sys.modules[__module__]` during class build) resolves during load.
    """
    existing = sys.modules.get(name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).with_name(f"{name}.py")
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(f"command failed: {' '.join(cmd)}\n{proc.stderr}\n")
        raise SystemExit(2)
    return proc.stdout


def run_tolerant(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    """Run `cmd`, tolerate a non-zero exit, and hand back the CompletedProcess.

    Unlike `run` (which fatally SystemExits on non-zero), a non-zero exit here is a normal
    signal the caller inspects — `cos_preflight.py` runs `git`/`gh`/sibling probes whose
    non-zero exits carry meaning (a lane-freshness verdict, an absent ref) rather than a
    fatal error. Only a MISSING executable is fatal: it maps to SystemExit with an
    actionable `missing executable` message so a broken PATH surfaces as a setup error
    instead of an uncaught traceback.
    """
    try:
        return subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise SystemExit(f"missing executable {cmd[0]!r}: {exc}") from exc


def gh_json(args: list[str]) -> Any:
    return json.loads(run(["gh", *args]))


def gh_issue_view_or_none(number: int, fields: str) -> dict | None:
    """`gh issue view <number> --json <fields>` decoded, or None on non-zero exit.

    Unlike `run`/`gh_json` (which fatally `SystemExit` on a non-zero exit), a non-zero
    exit here is a NORMAL signal — the number isn't a resolvable issue (a PR, or missing)
    — so it returns None instead of aborting. `gh issue view` also resolves a PR number,
    so a non-None result is NOT proof the number is an issue; the caller applies its own
    trust/state gate on top.
    """
    proc = subprocess.run(
        ["gh", "issue", "view", str(number), "--json", fields],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return None
    return json.loads(proc.stdout)


def _warn_if_truncated(rows: list, what: str) -> None:
    if len(rows) >= FETCH_CAP:
        sys.stderr.write(
            f"warning: {what} fetch hit the {FETCH_CAP} cap; results may be "
            f"truncated — raise FETCH_CAP or paginate\n"
        )


def repo_owner_name() -> tuple[str, str]:
    slug = os.environ.get("GITHUB_REPOSITORY")
    if not slug:
        slug = json.loads(run(["gh", "repo", "view", "--json", "nameWithOwner"]))[
            "nameWithOwner"
        ]
    owner, name = slug.split("/", 1)
    return owner, name
