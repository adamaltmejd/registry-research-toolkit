"""Enforce the lightweight/runtime split (§15 step 5 phase 2c).

The local CLI / webapp / bundle builder must be able to import
``reg_monabundle`` (and its submodules) without transitively pulling
``reg_monabundle.runtime.*`` — the runtime tier uses ``duckdb`` and
``pyodbc`` (lazily, inside function bodies), which are MONA-only deps
not in the workspace lock.

One subprocess runs all four import probes and emits the per-probe
results as JSON; pytest then parses once and runs four cheap
in-process assertions. (Each subprocess startup is ~30 ms; running
four separately added ~90 ms per test run for the same guarantee —
``reg_monabundle/__init__.py`` eagerly imports ``.build`` / ``.scan``
/ ``.validate`` so the four probes load identical module sets today.
The probes survive the redundancy on purpose: a future lazy-init
refactor would let them diverge, and the test would catch the wrong
one regressing.)
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

_PROBES: tuple[str, ...] = (
    "reg_monabundle",
    "reg_monabundle.build",
    "reg_monabundle.scan",
    "reg_monabundle.validate",
)

# Probe names are passed in via argv (rather than baked into the script
# with str.format) to dodge ``{}``-literal collisions with format-spec
# placeholders and keep the script trivially auditable.
_PROBE_SCRIPT = """\
import importlib, json, sys
out = {}
for name in sys.argv[1:]:
    importlib.import_module(name)
    out[name] = sorted(
        m for m in sys.modules if m.startswith("reg_monabundle.runtime")
    )
print(json.dumps(out))
"""


@pytest.fixture(scope="module")
def runtime_modules_per_probe() -> dict[str, list[str]]:
    """Run all four import probes in one subprocess; return {probe: loaded}."""
    result = subprocess.run(
        [sys.executable, "-c", _PROBE_SCRIPT, *_PROBES],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_top_level_import_does_not_load_runtime(
    runtime_modules_per_probe: dict[str, list[str]],
) -> None:
    loaded = runtime_modules_per_probe["reg_monabundle"]
    assert loaded == [], (
        f"importing reg_monabundle pulled in runtime modules: {loaded}. "
        "The runtime subpackage is MONA-only (duckdb / pyodbc) and must "
        "stay behind an explicit import. See reg_monabundle/DESIGN.md."
    )


def test_build_import_does_not_load_runtime(
    runtime_modules_per_probe: dict[str, list[str]],
) -> None:
    """``build_bundle`` reads runtime modules off disk via AST, never imports them."""
    loaded = runtime_modules_per_probe["reg_monabundle.build"]
    assert loaded == [], (
        f"importing reg_monabundle.build pulled in runtime modules: {loaded}. "
        "The amalgamator must slice the runtime via ast.parse on the source "
        "files — never via Python import — so the local builder works "
        "without duckdb / pyodbc installed."
    )


def test_scan_import_does_not_load_runtime(
    runtime_modules_per_probe: dict[str, list[str]],
) -> None:
    loaded = runtime_modules_per_probe["reg_monabundle.scan"]
    assert loaded == [], f"reg_monabundle.scan pulled in runtime: {loaded}"


def test_validate_import_does_not_load_runtime(
    runtime_modules_per_probe: dict[str, list[str]],
) -> None:
    loaded = runtime_modules_per_probe["reg_monabundle.validate"]
    assert loaded == [], f"reg_monabundle.validate pulled in runtime: {loaded}"
