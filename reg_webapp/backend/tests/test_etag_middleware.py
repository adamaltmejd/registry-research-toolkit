"""ETag / Cache-Control, end-to-end through the app.

See DESIGN.md → ETag / Cache-Control (etag.py + middleware.py). GET reads get ETag + the per-route Cache-Control tier, error
responses and writes do NOT (an error body is not a cacheable representation), a
matching If-None-Match (weak, list or wildcard form) yields a 304 with no body,
and the ETag prefix is the INSTALLED reg_meta version (NOT the DB
schema_version manifest).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app

import reg_meta

_SHORT = "public, max-age=60, must-revalidate"


def test_etag_prefix_is_installed_reg_meta_version_not_manifest(catalog_db):
    # The ETag's version component is `reg_meta.__version__` (the v1.x Model
    # A package release), NOT the DB build's schema_version (the fixture stamps a
    # `5.1.999` manifest — distinct, and must NOT appear in the ETag).
    with TestClient(create_app()) as client:
        etag = client.get("/api/catalog").headers["etag"]
        generation = client.get("/api/context").json()["reg_meta"]["generation_id"]
    assert etag.startswith(f'"{reg_meta.__version__}-global-')
    assert f"-{generation}-reference-" in etag
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


@pytest.mark.parametrize(
    ("path", "cache_control"),
    [
        # The vintage footer asserts a deploy version/date: always revalidate.
        ("/api/context", "no-cache"),
        # Fold- or steward-dependent reads get the short window (#499, #506, #726).
        ("/api/catalog/scb/lisa/kon/states", _SHORT),
        ("/api/search?q=lisa", _SHORT),
        ("/api/stats", _SHORT),
        # Rebuild-stable doc reads keep 24h; `/api/docs/search` must not collide
        # with the `/api/search` prefix.
        ("/api/docs/search?q=kon", "public, max-age=86400, must-revalidate"),
    ],
)
def test_read_cache_control_tier(docs_db, path, cache_control):
    with TestClient(create_app()) as client:
        resp = client.get(path)
    assert resp.status_code == 200
    assert resp.headers["cache-control"] == cache_control


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
        etag = client.get("/api/context").headers["etag"]
        resp = client.get(
            "/api/context",
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


def test_identical_reference_body_has_distinct_scope_validators(tmp_path, monkeypatch):
    from http_cases import CASES
    from webapp_fixture_support import fixture_db

    path = fixture_db.build_reader_fixture_db(
        tmp_path / "steward", kind="steward", fixture=CASES / "fixtures/compiled"
    )
    monkeypatch.setenv("REG_META_DB", str(path.parent))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "swecov")
    with TestClient(create_app()) as client:
        held = client.get("/api/catalog/class/example-codes")
        reference = client.get(
            "/api/catalog/class/example-codes", params={"scope": "reference"}
        )
        default = client.get(
            "/api/catalog/class/example-codes", params={"scope": "holdings"}
        )
    assert held.status_code == reference.status_code == default.status_code == 200
    assert held.content == reference.content == default.content
    assert held.headers["etag"] != reference.headers["etag"]
    assert held.headers["etag"] == default.headers["etag"]


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
            responses.append(client.get("/api/catalog"))
    assert responses[0].status_code == responses[1].status_code == 200
    assert responses[0].content == responses[1].content
    assert responses[0].headers["etag"] != responses[1].headers["etag"]


def test_different_bodies_under_identical_metadata_do_not_share_a_validator(
    catalog_db,
):
    # Fails if compute_etag drops the body digest: version, steward, generation
    # and scope are identical here, so only the body can tell the two apart, and
    # an ETag cached for `?q=lisa` would otherwise turn `?q=rams` into a stale 304.
    with TestClient(create_app()) as client:
        lisa = client.get("/api/search", params={"q": "lisa"})
        rams = client.get(
            "/api/search",
            params={"q": "rams"},
            headers={"If-None-Match": lisa.headers["etag"]},
        )
    assert lisa.status_code == rams.status_code == 200
    assert lisa.content != rams.content
    assert lisa.headers["etag"] != rams.headers["etag"]
