"""Shared spec-loader for the `scripts/` unit tests.

A `scripts/` tooling module may pull a sibling in by `sys.modules`-guarded spec-load
(`gh_issue.py`'s `_load_gh()` preamble does this for `_gh`), so that name is already
registered as ONE module object shared across the process — a monkeypatch or attribute
read through one consumer is visible through every other. A test that loads its target
with an unconditional `sys.modules[name] = fresh` breaks that: if an earlier-collected
test file already pulled the same module in as a consumer's sibling, the overwrite splits
the singleton — the consumer holds the old object while later loads return the new one,
so tests pass or fail depending on collection order.

`load_scripts_module` is the one idiom every test file uses: it reuses an existing
`sys.modules` entry when present and only spec-loads + registers when absent — the same
guard the scripts apply. Kept here (not re-pasted per file) so the pattern can't drift.
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

    Reuses an existing `sys.modules[name]` (a consumer test may already have pulled it in
    as its target's sibling) instead of overwriting it. Registers before exec so a
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
