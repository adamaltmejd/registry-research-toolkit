"""Shared spec-loader for the `scripts/` unit tests.

The `scripts/` tooling is standalone files, not an installed package, so a test loads its
target by `importlib` spec rather than a plain `import` — the same way the scripts run
under `uv run --no-project python scripts/<name>.py`, regardless of what's on sys.path.

`load_scripts_module` is the one idiom every test file uses. Kept here (not re-pasted per
file) so the pattern can't drift.
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
    """Load `scripts/<name>.py` as module `name`.

    Registers in `sys.modules` before exec so a self-referential `@dataclass` that
    resolves `sys.modules[__module__]` during class build sees the module.
    """
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
