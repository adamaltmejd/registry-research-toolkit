"""Boot-path guards: schema-mismatch rejection + the catalog catch-all invariant.

A5.1a shipped NO `:path` catch-all (asserted negatively). A5.1b-ii legitimately
adds the `/api/catalog/{fqid:path}` catch-all behind the per-segment guard (see
DESIGN.md → FQID path guard (catalog_fqid.py)),
so this test is INVERTED on purpose: it now asserts the catch-all IS present and
LAST among the catalog routes, and that the guard runs before any DB access
(a traversal probe 422s with zero SQL).
"""

from __future__ import annotations

import sqlite3

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from reg_meta.errors import RegMetaError
from reg_meta.fqid import RESERVED_HTTP_SUFFIX_SLUGS, RESERVED_VARIANTS_SLUG
from reg_webapp.app import create_app


def test_startup_rejects_incompatible_schema(mismatched_db):
    # open_db's _check_schema_compat raises RegMetaError on the major mismatch;
    # the lifespan propagates it, so entering the TestClient context fails.
    with pytest.raises(RegMetaError), TestClient(create_app()):
        pass


_CATCH_ALL = "/api/catalog/{fqid:path}"

# Router ordering (A5.2a-ii, see DESIGN.md → Catalog router structure): the 7
# suffixed / sub-resource routes that MUST be declared
# BEFORE the `{fqid:path}` catch-all — Starlette matches in declaration order and
# the `{fqid:path}` converter greedy-consumes any suffix into `fqid`, so any of
# these declared after the catch-all would never fire. Six are `{fqid:path}/...`
# binding suffixes; the seventh is the fixed-shape `/{provider}/{register}/
# variants` register sub-resource.
_ROUTES_BEFORE_CATCH_ALL = [
    "/api/catalog/{provider}/{register}/variants",
    "/api/catalog/{fqid:path}/states",
    "/api/catalog/{fqid:path}/predecessors",
    "/api/catalog/{fqid:path}/successors",
    "/api/catalog/{fqid:path}/related",
    "/api/catalog/{fqid:path}/lineage",
    "/api/catalog/{fqid:path}/lineage_warnings",
]


def test_catalog_catch_all_route_present_and_last():
    # A5.1b-ii OWNS the bare `{fqid:path}` catch-all; A5.2a-ii adds 6 suffixed
    # `{fqid:path}/...` routes plus the `/variants` sub-resource. Assert the bare
    # catch-all exists and is declared LAST among /api/catalog* routes (the
    # router-ordering seam — Starlette matches in declaration order, so the
    # suffixed routes must precede the greedy catch-all).
    app = create_app()
    catalog_routes = [
        r.path
        for r in app.routes
        if isinstance(r, APIRoute) and r.path.startswith("/api/catalog")
    ]
    assert _CATCH_ALL in catalog_routes, (
        f"the bare catalog catch-all must be present; routes were {catalog_routes}"
    )
    assert catalog_routes[-1] == _CATCH_ALL, (
        f"the catch-all must be declared last; routes were {catalog_routes}"
    )


def test_suffixed_routes_declared_before_catch_all():
    # `routes_declared_before`: every suffixed / sub-resource route is
    # declared BEFORE the `{fqid:path}` catch-all. This is the CI regression guard
    # the spec calls for — a future route added after the catch-all (or the
    # catch-all moved up) is caught here, not in production as a silently-shadowed
    # endpoint.
    app = create_app()
    declaration_order = [r.path for r in app.routes if isinstance(r, APIRoute)]
    catch_all_index = declaration_order.index(_CATCH_ALL)
    for path in _ROUTES_BEFORE_CATCH_ALL:
        assert path in declaration_order, f"missing suffixed route: {path}"
        assert declaration_order.index(path) < catch_all_index, (
            f"{path} is declared AFTER the catch-all {_CATCH_ALL!r} — the catch-all "
            f"would greedy-consume it; declaration order was {declaration_order}"
        )


def test_reserved_slug_set_mirrors_catalog_routes():
    # Reserved-slug drift guard (#228, see reg_meta/DESIGN.md → FQID grammar): the
    # reserved-slug sets in reg_meta.fqid exist ONLY
    # to stop a slug from shadowing one of these catalog sub-resource routes, so
    # the two MUST stay in lockstep. Derive the tails from the LIVE FastAPI routes
    # (not a curated Python list) so a new catalog sub-resource route added to
    # routes/catalog.py but forgotten from the reserved set fails loudly here —
    # the curated-list version was blind to that gap (it would pass while the
    # suffix stayed un-reserved, re-opening the #228 slug-shadow collision). Set
    # equality gives both directions at once: every live route's tail IS reserved,
    # and every reserved token HAS a live route.
    app = create_app()
    catalog_routes = [
        r.path
        for r in app.routes
        if isinstance(r, APIRoute) and r.path.startswith("/api/catalog")
    ]
    # The `{fqid:path}/<suffix>` binding routes → their suffix tails must equal
    # RESERVED_HTTP_SUFFIX_SLUGS. The bare catch-all `/api/catalog/{fqid:path}`
    # contains `{fqid:path}` but NOT `{fqid:path}/`, so it's correctly excluded.
    suffix_tails = {
        path.rsplit("{fqid:path}/", 1)[1]
        for path in catalog_routes
        if "{fqid:path}/" in path
    }
    assert suffix_tails == RESERVED_HTTP_SUFFIX_SLUGS, (
        "RESERVED_HTTP_SUFFIX_SLUGS drifted from the catalog binding-suffix "
        f"routes: live routes have {sorted(suffix_tails)}, reserved set has "
        f"{sorted(RESERVED_HTTP_SUFFIX_SLUGS)}. Update reg_meta.fqid or routes/catalog.py."
    )
    # The literal `/{provider}/{register}/variants` register sub-resource (a
    # fixed-shape route with no `{fqid:path}`, distinct from the `/api/catalog`
    # root) → its last-segment tail must equal RESERVED_VARIANTS_SLUG.
    variants_tails = {
        path.rsplit("/", 1)[1]
        for path in catalog_routes
        if "{fqid:path}" not in path and path != "/api/catalog"
    }
    assert variants_tails == {RESERVED_VARIANTS_SLUG}, (
        "RESERVED_VARIANTS_SLUG drifted from the `/variants` register sub-resource "
        f"route: live routes have {sorted(variants_tails)}, reserved value is "
        f"{RESERVED_VARIANTS_SLUG!r}. Update reg_meta.fqid or routes/catalog.py."
    )


def test_section16_guard_runs_before_resolution(catalog_db):
    # The per-segment guard must reject a traversal probe BEFORE any Catalog
    # query — 422 with zero SQL executed (full coverage lives in
    # test_fqid_validation.py; this is the boot-level smoke).
    count = [0]
    orig = sqlite3.connect

    def traced(*args, **kwargs):
        conn = orig(*args, **kwargs)
        conn.set_trace_callback(lambda _stmt: count.__setitem__(0, count[0] + 1))
        return conn

    sqlite3.connect = traced
    try:
        with TestClient(create_app()) as client:
            count[0] = 0  # reset after boot's schema-check SQL
            resp = client.get("/api/catalog/scb/lisa/%2e%2e")
    finally:
        sqlite3.connect = orig
    assert resp.status_code == 422
    assert count[0] == 0
