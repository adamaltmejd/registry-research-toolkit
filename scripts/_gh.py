"""Shared `gh`/`git` process primitives for the scripts/ tooling.

`run` (checked subprocess → stdout), `gh_json` (run + JSON-decode),
`gh_api_paginated` (paginated gh-api array flattening), and `repo_owner_name`
(owner/name from $GITHUB_REPOSITORY, else `gh repo view`) are the thin,
domain-neutral wrappers every gh-driven script in here needs. The original process
wrappers were born in `check_issue_hygiene.py` and reused by `plan_sequence.py`; a third
consumer was the trigger the note in `plan_sequence.py` named for lifting them into a
shared module, and the cos_* tooling (`cos_preflight.py`, `cos_watch.py`, …) consumes them
now. The issue-domain parsers (label sets, the relationship/touches regexes) stay in
`check_issue_hygiene.py`, shared by its own set of consumers.

`run_tolerant` is the non-zero-tolerant counterpart to `run`: it hands back the
`CompletedProcess` (a non-zero exit is a signal the caller inspects, not a fatal error)
and only SystemExits on a MISSING executable. Lifted out of `cos_preflight.py`, whose
`git`/`gh`/sibling-probe calls all read a meaningful non-zero exit.

`scrubbed_git_env` + `run_git` are the git-specific primitives. `scrubbed_git_env` is the
SINGLE home for the GIT_* hijack scrub (a pre-push hook exports GIT_DIR/GIT_WORK_TREE/… and
would otherwise point every child git call at the hook's repo regardless of cwd); `run_git`
runs `git <args>` in an explicit cwd with that env scrubbed and tolerates a non-zero exit.
Lifted out of the three private copies that had grown in `cos_dispatch.py` and
`codex_local_review.py` — `run_git` deliberately does NOT map a missing git binary, leaving
that to each caller (cos_dispatch surfaces it, codex_local_review maps a PreconditionError).

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
    resolves `sys.modules[__module__]` during class build) resolves during load. If exec
    raises, the half-initialized module is popped back out of `sys.modules` (mirroring
    CPython's import machinery) so a later caller re-attempts a clean load rather than
    getting the broken instance back through the guard.
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
    try:
        spec.loader.exec_module(module)
    except BaseException:
        # A failed exec leaves a half-initialized module registered; drop it (mirroring
        # CPython's import machinery) so a later caller re-attempts a clean load instead of
        # getting the broken instance back through the sys.modules guard.
        sys.modules.pop(name, None)
        raise
    return module


def run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
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
        return subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise SystemExit(f"missing executable {cmd[0]!r}: {exc}") from exc


def scrubbed_git_env() -> dict[str, str]:
    """`os.environ` with every `GIT_*` key dropped — the SINGLE home for the hijack scrub.

    A git hook (this repo's pre-push runs the test suite) exports GIT_DIR / GIT_WORK_TREE /
    GIT_INDEX_FILE etc. into the child environment; git then targets the HOOK's repo
    regardless of a subprocess's cwd or `-C`. So passing an explicit cwd is NOT enough —
    every git call (and any launched agent that runs git internally) must run with these
    scrubbed. We drop all GIT_* keys wholesale: none of git's config-affecting env vars
    belong in a fresh subprocess, and a blanket rule can't miss a newly added repo-targeting
    var. (GIT_SSH/GIT_ASKPASS auth helpers live in the user's shell config, not this exported
    hook set, so dropping them here is harmless.)
    """
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def run_git(
    args: list[str], *, cwd: str | os.PathLike[str]
) -> subprocess.CompletedProcess[str]:
    """Run `git <args>` in `cwd` with the GIT_* hijack env scrubbed; tolerate non-zero exit.

    The git-specific counterpart to `run_tolerant`: like it, a non-zero exit is a signal the
    caller inspects (an absent ref, a failed ff-merge), not a fatal error — the CompletedProcess
    is handed back. Unlike `run_tolerant` it prepends `git`, scrubs the GIT_* hijack env
    (`scrubbed_git_env`), and runs in an explicit `cwd`. It does NOT catch `FileNotFoundError`:
    a missing git binary propagates so each caller owns its own mapping (cos_dispatch lets it
    surface as-is; codex_local_review maps it to a kind=precondition PreconditionError).
    """
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=scrubbed_git_env(),
        capture_output=True,
        text=True,
        check=False,
    )


def gh_json(args: list[str]) -> Any:
    return json.loads(run(["gh", *args]))


def gh_api_paginated(endpoint: str) -> list[dict[str, Any]]:
    """All rows from a paginated `gh api` array endpoint, flattened.

    Plain `gh api --paginate` emits one JSON array per page, which is not one valid JSON
    document once the result spans pages. `--slurp` wraps those page arrays in an outer
    array; flatten that shape so callers do not re-implement the same pagination glue.
    """
    pages = gh_json(["api", "--paginate", "--slurp", endpoint])
    return [row for page in pages for row in page]


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
        check=False,
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
