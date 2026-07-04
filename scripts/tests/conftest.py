"""Shared spec-loader for the `scripts/` unit tests.

The `scripts/` tooling modules import each other via `_gh.load_sibling`, a
`sys.modules`-guarded spec-loader that hands out ONE module object per name across the
whole process (so a monkeypatch or attribute read through one consumer is visible through
every other — see `scripts/_gh.py`). A test that loads its target module with an
unconditional `sys.modules[name] = fresh` breaks that: if an earlier-collected test file
already pulled the same module in as a `load_sibling` sibling (e.g. `test_cos_preflight`
loads `cos_preflight` -> `plan_sequence` -> `check_issue_hygiene`), the overwrite splits
the singleton — a consumer holds the old object while later `load_sibling` calls return
the new one, so identity tests pass or fail depending on collection order.

`load_scripts_module` is the one idiom every test file uses: it reuses an existing
`sys.modules` entry when present and only spec-loads + registers when absent — the same
guard `load_sibling` applies. Kept here (not re-pasted per file) so the pattern can't
drift.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

_SCRIPTS = Path(__file__).resolve().parents[1]

# Identity env for hermetic git commits, merged over os.environ so PATH (and thus the git
# binary) survives — a bare four-var env would drop PATH and break `git` invocation.
_GIT_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@e",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@e",
}


def make_git_repo(tmp_path: Path) -> Path:
    """A hermetic tmp git repo with one commit on `main`, GIT_* hijack env scrubbed.

    Deletes GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE from the child env (an ambient worktree env
    — the pre-push hook hijack exports all three — would otherwise point git at the real
    repo, and an ambient GIT_INDEX_FILE would stage the fixture's commits into the real
    repo's index). This scrubs ONLY that historical trio ON PURPOSE — it's a fixture-local
    hermeticity helper, not the production scrub. Production's `_gh.scrubbed_git_env` drops
    every `GIT_*` key wholesale; here the named trio is all this fixture needs to isolate its
    own git ops. Merges over os.environ so PATH survives; returns the repo root (the caller
    chdirs if it needs to).
    """
    env = {
        k: v
        for k, v in {**os.environ, **_GIT_ENV}.items()
        if k not in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE")
    }
    subprocess.run(
        ["git", "init", "-q", "-b", "main"], cwd=tmp_path, check=True, env=env
    )
    (tmp_path / "f.txt").write_text("hello\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, env=env)
    subprocess.run(
        ["git", "commit", "-q", "-m", "init"], cwd=tmp_path, check=True, env=env
    )
    return tmp_path


def load_scripts_module(name: str) -> ModuleType:
    """Load `scripts/<name>.py` under a single process-wide identity.

    Reuses an existing `sys.modules[name]` (a consumer test may already have loaded it as a
    `load_sibling` sibling) instead of overwriting it. Registers before exec so a
    self-referential `@dataclass` that resolves `sys.modules[__module__]` during class
    build sees the module.
    """
    if (existing := sys.modules.get(name)) is not None:
        return existing
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
