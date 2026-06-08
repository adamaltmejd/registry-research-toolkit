"""Enforce the lightweight/runtime split (see DESIGN.md → The two halves).

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


def test_build_import_does_not_load_reg_schema_or_pydantic() -> None:
    """Boundary (see DESIGN.md → The two halves): importing
    ``reg_monabundle.build`` (the local amalgamator)
    must not pull ``reg_schema`` / Pydantic / ``build.spec_loader``.

    ``build.spec_loader`` is the only build-side module that imports
    ``reg_schema`` (the build-time validation gate); callers import it
    directly and ``build/__init__`` must NOT — otherwise the Pydantic
    dependency leaks into the lightweight surface (and the amalgamator's
    import graph), the very break A3.4 closed. See DESIGN.md → The two halves.
    """
    script = (
        "import importlib, json, sys\n"
        "importlib.import_module('reg_monabundle.build')\n"
        "print(json.dumps(sorted(\n"
        "    m for m in sys.modules\n"
        "    if m.split('.')[0] in ('reg_schema', 'pydantic')\n"
        "    or m == 'reg_monabundle.build.spec_loader'\n"
        ")))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    leaked = json.loads(result.stdout)
    assert leaked == [], (
        f"importing reg_monabundle.build pulled in boundary modules: {leaked}. "
        "build.spec_loader (the only reg_schema/Pydantic importer) must be imported "
        "directly by callers, never by build/__init__. See DESIGN.md → The two halves."
    )
