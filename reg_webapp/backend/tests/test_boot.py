"""Boot-path guards: schema-mismatch rejection + the catalog catch-all invariant.

A5.1a shipped NO `:path` catch-all (asserted negatively). A5.1b-ii legitimately
adds the `/api/catalog/{fqid:path}` catch-all behind the §16 per-segment guard,
so this test is INVERTED on purpose: it now asserts the catch-all IS present and
LAST among the catalog routes, and that the §16 guard runs before any DB access
(a traversal probe 422s with zero SQL).
"""

from __future__ import annotations

import sqlite3

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from reg_meta.errors import RegMetaError
from reg_webapp.app import create_app


def test_startup_rejects_incompatible_schema(mismatched_db):
    # open_db's _check_schema_compat raises RegMetaError on the major mismatch;
    # the lifespan propagates it, so entering the TestClient context fails.
    with pytest.raises(RegMetaError), TestClient(create_app()):
        pass


def test_catalog_catch_all_route_present_and_last():
    # INVERTED from A5.1a: A5.1b-ii OWNS the `{fqid:path}` catch-all. Assert it
    # exists, is the catalog catch-all, and is declared LAST among /api/catalog*
    # routes (the A5.2 router-ordering seam — Starlette matches in declaration
    # order, so suffixed routes must precede the greedy catch-all).
    app = create_app()
    catch_alls = [
        r.path for r in app.routes if isinstance(r, APIRoute) and ":path}" in r.path
    ]
    assert catch_alls == ["/api/catalog/{fqid:path}"], (
        f"expected exactly the catalog catch-all, got {catch_alls}"
    )
    catalog_routes = [
        r.path
        for r in app.routes
        if isinstance(r, APIRoute) and r.path.startswith("/api/catalog")
    ]
    assert catalog_routes[-1] == "/api/catalog/{fqid:path}", (
        f"the catch-all must be declared last; routes were {catalog_routes}"
    )


def test_section16_guard_runs_before_resolution(catalog_db):
    # The §16 per-segment guard must reject a traversal probe BEFORE any Catalog
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
