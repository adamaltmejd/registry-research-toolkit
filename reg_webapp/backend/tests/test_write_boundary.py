"""Provenance-DB route confinement (A5.2b-ii).

A structural invariant the write surface must not break:

- **Provenance-DB confinement** (see DESIGN.md → input-validation gates (security
  boundary)). No FastAPI route handler may reference the
  maintainer-only ``reg_meta.provenance.db`` path (see reg_meta_build/DESIGN.md →
  Provenance DB sibling; not shipped). The route-introspection assertion
  (forward criterion #2).

Pinned here so a future write-surface change that reaches for the provenance DB
fails loudly.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def app(catalog_db):
    """The full app (all routers registered) so route introspection sees every
    handler module. ``catalog_db`` points the lifespan at a real DB, but the test
    doesn't enter the lifespan — registering the routers imports every handler."""
    from reg_webapp.app import create_app

    return create_app()


def test_no_route_handler_references_provenance_db(app):
    """Forward criterion #2: no route handler references the provenance DB
    path. Introspect every route's endpoint function (its module source + any
    referenced globals) for the ``reg_meta.provenance.db`` token / a
    ``provenance`` reg_meta accessor."""
    import inspect

    from _route_helpers import flat_routes
    from starlette.routing import Route

    offenders: list[str] = []
    # flat_routes flattens the FastAPI 0.137 _IncludedRouter tree: pre-0.137
    # app.routes held every APIRoute directly, but now the included catalog/search/
    # project/docs/context handlers are nested, so a bare `app.routes` walk would
    # scan only the directly-added routes and silently skip the API handlers.
    for route in flat_routes(app):
        if not isinstance(route, Route):
            continue
        endpoint = route.endpoint
        module = inspect.getmodule(endpoint)
        if module is None:
            continue
        try:
            source = inspect.getsource(module)
        except OSError, TypeError:  # pragma: no cover — source always available here
            continue
        # Match the actual provenance-DB reference (`reg_meta.provenance` import or
        # attribute), NOT the bare word "provenance" — a docstring/comment mentioning
        # it (e.g. this very boundary) must not false-positive.
        if "reg_meta.provenance" in source:
            offenders.append(f"{route.path} -> {module.__name__}")
    assert not offenders, (
        f"route handler module references the provenance DB: {offenders}"
    )
