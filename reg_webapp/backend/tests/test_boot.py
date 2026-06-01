"""Boot-path guards: schema-mismatch rejection + the no-catch-all invariant."""

from __future__ import annotations

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


def test_no_path_converter_catch_all_route():
    # A5.1a invariant: no unguarded catch-all. The `{fqid:path}` catalog
    # catch-all (with its per-segment grammar + path-traversal guards) lands in
    # A5.1b; until then no route may use a `:path` converter.
    app = create_app()
    offenders = [
        r.path for r in app.routes if isinstance(r, APIRoute) and ":path}" in r.path
    ]
    assert not offenders, f"unexpected path-converter route(s): {offenders}"
