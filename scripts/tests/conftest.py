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
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

_SCRIPTS = Path(__file__).resolve().parents[1]


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
