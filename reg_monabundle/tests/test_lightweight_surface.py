"""Enforce the lightweight/runtime split (§15 step 5 phase 2c).

The local CLI / webapp / bundle builder must be able to import
``reg_monabundle`` (and its submodules) without transitively pulling
``reg_monabundle.runtime.*`` — the runtime tier depends on ``duckdb``
and ``pyodbc`` at use time, which are MONA-only deps not in the
workspace lock.

We run the probe in a subprocess so the test isn't fooled by another
test having already imported the runtime in this interpreter.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap


def _runtime_modules_after(probe: str) -> list[str]:
    """Run ``probe`` in a fresh interpreter, return loaded runtime modules."""
    script = textwrap.dedent(probe) + textwrap.dedent("""
        import sys
        loaded = sorted(
            m for m in sys.modules if m.startswith("reg_monabundle.runtime")
        )
        print("\\n".join(loaded))
    """)
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def test_top_level_import_does_not_load_runtime() -> None:
    loaded = _runtime_modules_after("import reg_monabundle")
    assert loaded == [], (
        f"importing reg_monabundle pulled in runtime modules: {loaded}. "
        "The runtime subpackage is MONA-only (duckdb / pyodbc) and must "
        "stay behind an explicit import. See reg_monabundle/DESIGN.md."
    )


def test_build_import_does_not_load_runtime() -> None:
    """``build_bundle`` reads runtime modules off disk via AST, never imports them."""
    loaded = _runtime_modules_after("import reg_monabundle.build")
    assert loaded == [], (
        f"importing reg_monabundle.build pulled in runtime modules: {loaded}. "
        "The amalgamator must slice the runtime via ast.parse on the source "
        "files — never via Python import — so the local builder works "
        "without duckdb / pyodbc installed."
    )


def test_scan_import_does_not_load_runtime() -> None:
    loaded = _runtime_modules_after("import reg_monabundle.scan")
    assert loaded == [], f"reg_monabundle.scan pulled in runtime: {loaded}"


def test_validate_import_does_not_load_runtime() -> None:
    loaded = _runtime_modules_after("import reg_monabundle.validate")
    assert loaded == [], f"reg_monabundle.validate pulled in runtime: {loaded}"
