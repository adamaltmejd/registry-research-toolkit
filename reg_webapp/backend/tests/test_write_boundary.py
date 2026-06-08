"""Import-graph boundary + provenance-DB route confinement (A5.2b-ii).

Two structural invariants the write surface must not break:

- **Import boundary** (see reg_monabundle/DESIGN.md → The two halves).
  ``reg_webapp`` must NOT pull the MONA-only bundle
  RUNTIME (``reg_monabundle.runtime.*``) or its heavy deps (``duckdb`` /
  ``pyodbc``) into its import graph. The bundle endpoint imports
  ``reg_monabundle.build.spec_loader`` (the Pydantic BUILD side, never
  amalgamated), whose ``project_data_to_loadedspec`` imports the runtime LAZILY —
  so a full import of the webapp app must leave the runtime + heavy deps OUT of
  ``sys.modules``.
- **Provenance-DB confinement** (see DESIGN.md → input-validation gates (security
  boundary)). No FastAPI route handler may reference the
  maintainer-only ``reg_meta.provenance.db`` path (see reg_meta_build/DESIGN.md →
  Provenance DB sibling; not shipped). The route-introspection assertion
  (forward criterion #2).

Both are pinned here so a future write-surface change that reaches for the
runtime or the provenance DB fails loudly.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

# Run the import-graph probe in a CLEAN subprocess: by the time this module's
# tests run in-session, other tests (test_bundle) have already POSTed /api/bundle,
# which triggers the LAZY runtime import and pollutes this process's sys.modules.
# A fresh interpreter that only builds the app is the honest probe of the static
# import graph.
_IMPORT_PROBE = """
import sys
from reg_webapp.app import create_app
create_app()  # registers every router → imports every handler module
forbidden = sorted(
    n for n in sys.modules
    if n in ("duckdb", "pyodbc") or n.startswith("reg_monabundle.runtime")
)
print(",".join(forbidden))
"""


@pytest.fixture
def app(catalog_db):
    """The full app (all routers registered) so route introspection sees every
    handler module. ``catalog_db`` points the lifespan at a real DB, but the test
    doesn't enter the lifespan — registering the routers imports every handler."""
    from reg_webapp.app import create_app

    return create_app()


def test_webapp_does_not_import_bundle_runtime_or_heavy_deps():
    """A full webapp import must NOT pull ``reg_monabundle.runtime.*`` / duckdb /
    pyodbc. The bundle endpoint's deps import the runtime LAZILY, so
    building the app (in a clean interpreter) must keep them out of
    ``sys.modules``."""
    result = subprocess.run(  # noqa: S603 — fixed, trusted probe
        [sys.executable, "-c", _IMPORT_PROBE],
        capture_output=True,
        text=True,
        check=True,
    )
    forbidden = [m for m in result.stdout.strip().split(",") if m]
    assert not forbidden, f"import boundary breached — webapp imported: {forbidden}"


def test_no_route_handler_references_provenance_db(app):
    """Forward criterion #2: no route handler references the provenance DB
    path. Introspect every route's endpoint function (its module source + any
    referenced globals) for the ``reg_meta.provenance.db`` token / a
    ``provenance`` reg_meta accessor."""
    import inspect

    from starlette.routing import Route

    offenders: list[str] = []
    for route in app.routes:
        if not isinstance(route, Route):
            continue
        endpoint = route.endpoint
        module = inspect.getmodule(endpoint)
        if module is None:
            continue
        try:
            source = inspect.getsource(module)
        except (OSError, TypeError):  # pragma: no cover — source always available here
            continue
        # Match the actual provenance-DB reference (`reg_meta.provenance` import or
        # attribute), NOT the bare word "provenance" — a docstring/comment mentioning
        # it (e.g. this very boundary) must not false-positive.
        if "reg_meta.provenance" in source:
            offenders.append(f"{route.path} -> {module.__name__}")
    assert not offenders, (
        f"route handler module references the provenance DB: {offenders}"
    )
