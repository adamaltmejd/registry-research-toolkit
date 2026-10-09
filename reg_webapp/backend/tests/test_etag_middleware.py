"""ETag / Cache-Control, end-to-end through the app.

See DESIGN.md → ETag / Cache-Control (etag.py + middleware.py). GET reads get ETag +
Cache-Control, error responses and writes do NOT (an error body is not a cacheable
representation), a matching If-None-Match (weak, list or wildcard form) yields a 304
with no body, and the ETag prefix is the INSTALLED reg_meta version (NOT the DB
schema_version manifest). ``/openapi.json`` is the GET read whose body does not depend
on the artifact, so it isolates the validator's artifact components.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app

import reg_meta


def test_etag_prefix_is_installed_reg_meta_version_not_manifest(catalog_db):
    # The ETag's version component is `reg_meta.__version__` (the v1.x Model
    # A package release), NOT the DB build's schema_version (the fixture stamps a
    # `5.1.999` manifest — distinct, and must NOT appear in the ETag).
    with TestClient(create_app()) as client:
        etag = client.get("/openapi.json").headers["etag"]
    assert etag.startswith(f'"{reg_meta.__version__}-global-')
    assert "5.1.999" not in etag


def test_error_response_has_no_etag(catalog_db):
    # A 404 is not a cacheable representation — the middleware skips it (no ETag,
    # no Cache-Control), so a client never caches a transient error. FastAPI serves
    # no GET read under /api any more (the Rust server does), so any /api GET 404s.
    with TestClient(create_app()) as client:
        not_found = client.get("/api/catalog")
    assert not_found.status_code == 404
    assert "etag" not in not_found.headers
    assert "cache-control" not in not_found.headers


def test_304_drops_content_type_and_length(catalog_db):
    with TestClient(create_app()) as client:
        etag = client.get("/openapi.json").headers["etag"]
        resp = client.get("/openapi.json", headers={"If-None-Match": etag})
    assert resp.status_code == 304
    assert resp.content == b""
    # The empty 304 entity carries no content-type/length, but keeps the validator.
    assert "content-type" not in resp.headers
    assert resp.headers["etag"] == etag


def test_read_cache_control(catalog_db):
    # Fails if the read tier changes from the 24-hour window.
    with TestClient(create_app()) as client:
        resp = client.get("/openapi.json")
    assert resp.status_code == 200
    assert resp.headers["cache-control"] == "public, max-age=86400, must-revalidate"


@pytest.mark.parametrize(
    ("if_none_match", "status"),
    [
        # RFC 7232 §3.2: If-None-Match is a WEAK comparison, so an edge that
        # weakened the validator (Cloudflare) still gets a 304.
        ("W/{etag}", 304),
        ('"other", {etag}', 304),
        ("*", 304),
        ('"other"', 200),
        ('W/"other"', 200),
    ],
)
def test_if_none_match_forms(catalog_db, if_none_match, status):
    with TestClient(create_app()) as client:
        etag = client.get("/openapi.json").headers["etag"]
        resp = client.get(
            "/openapi.json",
            headers={"If-None-Match": if_none_match.format(etag=etag)},
        )
    assert resp.status_code == status


def test_write_response_has_no_etag(catalog_db):
    # The method gate skips writes: a validation answer is never cached.
    with TestClient(create_app()) as client:
        resp = client.post("/api/project/validate", json={})
    assert resp.status_code == 200
    assert "etag" not in resp.headers
    assert "cache-control" not in resp.headers


def test_generation_invalidates_an_identical_body(tmp_path, monkeypatch):
    from webapp_fixture_support import fixture_db

    responses = []
    for index, prepared_commit in enumerate(("a" * 40, "b" * 40)):
        path = fixture_db.build_reader_fixture_db(
            tmp_path / str(index),
            kind="catalog",
            identity_overrides={"prepared_commit": prepared_commit},
        )
        monkeypatch.setenv("REG_META_DB", str(path.parent))
        monkeypatch.setenv("REG_WEBAPP_STEWARD", "global")
        with TestClient(create_app()) as client:
            responses.append(client.get("/openapi.json"))
    assert responses[0].status_code == responses[1].status_code == 200
    assert responses[0].content == responses[1].content
    assert responses[0].headers["etag"] != responses[1].headers["etag"]


def test_different_bodies_under_identical_metadata_do_not_share_a_validator(
    docs_db,
):
    # Fails if compute_etag drops the body digest: version, steward and generation
    # are identical here, so only the body can tell the two apart, and an ETag
    # cached for one read would otherwise turn the next into a stale 304.
    with TestClient(create_app()) as client:
        kon = client.get("/api/docs/doc/Kon")
        other = client.get(
            "/api/docs/search",
            params={"q": "kon"},
            headers={"If-None-Match": kon.headers["etag"]},
        )
    assert kon.status_code == other.status_code == 200
    assert kon.content != other.content
    assert kon.headers["etag"] != other.headers["etag"]
