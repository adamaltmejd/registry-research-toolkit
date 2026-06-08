"""ETag middleware wiring, end-to-end through the app.

See DESIGN.md → ETag / Cache-Control (etag.py + middleware.py). The pure logic is
unit-tested in ``test_etag.py``; here we pin the middleware
behavior: GET reads get ETag + Cache-Control, error responses do NOT (an error
body is not a cacheable representation), a matching If-None-Match yields a 304
with no body, and the ETag prefix is the INSTALLED reg_meta version (NOT the DB
schema_version manifest). The per-endpoint ETag/304 parametrization lives in
``test_catalog_subendpoints.py``.

A non-GET (write) endpoint would be SKIPPED by the method gate — A5.2b adds the
first write endpoint, so the skip is asserted here against a synthetic route to
pin the contract before then.
"""

from __future__ import annotations

from fastapi.testclient import TestClient
from reg_webapp.app import create_app
from reg_webapp.middleware import ETagMiddleware

import reg_meta


def test_etag_prefix_is_installed_reg_meta_version_not_manifest(catalog_db):
    # The ETag's version component is `reg_meta.__version__` (the v1.x Model
    # A package release), NOT the DB build's schema_version (the fixture stamps a
    # `5.1.999` manifest — distinct, and must NOT appear in the ETag).
    with TestClient(create_app()) as client:
        etag = client.get("/api/catalog").headers["etag"]
    assert etag.startswith(f'"{reg_meta.__version__}-global-')
    assert "5.1.999" not in etag


def test_error_response_has_no_etag(catalog_db):
    # A 404 / 422 is not a cacheable representation — the middleware skips it (no
    # ETag, no Cache-Control), so a client never caches a transient error.
    with TestClient(create_app()) as client:
        not_found = client.get("/api/catalog/nope")
        assert not_found.status_code == 404
        assert "etag" not in not_found.headers
        assert "cache-control" not in not_found.headers

        bad = client.get("/api/catalog/scb/Lisa")  # path guard → 422
        assert bad.status_code == 422
        assert "etag" not in bad.headers


def test_304_drops_content_type_and_length(catalog_db):
    with TestClient(create_app()) as client:
        etag = client.get("/api/context").headers["etag"]
        resp = client.get("/api/context", headers={"If-None-Match": etag})
    assert resp.status_code == 304
    assert resp.content == b""
    # The empty 304 entity carries no content-type/length, but keeps the validator.
    assert "content-type" not in resp.headers
    assert resp.headers["etag"] == etag


def test_non_get_method_is_skipped(catalog_db):
    # The method gate skips writes (A5.2b). Pin it now with a synthetic POST route
    # mounted alongside the real middleware: the POST response must carry NO ETag.
    app = create_app()

    @app.post("/api/_probe_write")
    def _probe_write() -> dict[str, bool]:  # pragma: no cover - wiring probe
        return {"ok": True}

    # The middleware is added in create_app; assert it's actually mounted so this
    # test fails loudly if the wiring is dropped.
    assert any(m.cls is ETagMiddleware for m in app.user_middleware)

    with TestClient(app) as client:
        resp = client.post("/api/_probe_write")
    assert resp.status_code == 200
    assert "etag" not in resp.headers
    assert "cache-control" not in resp.headers
