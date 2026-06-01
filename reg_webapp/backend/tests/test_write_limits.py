"""Write-endpoint cost protection: body cap + rate limit + content-type (§9.4).

Covers ``limits.py``:

- a body > 1 MB → 413, AND that the guard is STREAMING (it rejects an oversized
  body even when ``Content-Length`` lies / is absent — not Content-Length-trusting);
- the per-IP rate limiter → 429 after the bucket drains;
- reads (GET) pass through both middlewares untouched (the method gate);
- wrong / missing content-type handling on the write endpoints.

These drive ``/api/project/validate`` (the cheapest write path — its body parse
happens after the cap, so an oversized body never reaches it).
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app
from reg_webapp.limits import MAX_BODY_BYTES


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def _tiny_spec() -> dict:
    return {
        "schema_version": "2.0.0",
        "steward": "ifau",
        "reg_meta_version": "5.1.0",
        "name": "t",
        "sources": [],
    }


def test_oversized_body_is_413(client):
    # A JSON body comfortably over the 1 MB cap. Valid JSON so a 413 can only come
    # from the body cap, not a parse error.
    big = {"name": "x" * (MAX_BODY_BYTES + 1024)}
    resp = client.post("/api/project/validate", json=big)
    assert resp.status_code == 413


def test_body_cap_is_streaming_not_content_length_trusting(client):
    """The cap counts bytes as they stream — it does NOT trust ``Content-Length``.
    Send an oversized body with a deliberately-LYING small ``Content-Length``
    header; a Content-Length-trusting guard would wave it through, the streaming
    guard rejects it (413)."""
    payload = json.dumps({"name": "x" * (MAX_BODY_BYTES + 1024)}).encode("utf-8")
    resp = client.post(
        "/api/project/validate",
        content=payload,
        headers={"content-type": "application/json", "content-length": "10"},
    )
    assert resp.status_code == 413


def test_under_cap_body_passes(client):
    """A body comfortably under the cap is NOT 413'd (the cap doesn't reject
    normal traffic)."""
    resp = client.post("/api/project/validate", json=_tiny_spec())
    assert resp.status_code == 200


def test_rate_limit_returns_429_after_bucket_drains(catalog_db):
    """A tight per-IP budget drains after N requests → 429. Build a dedicated app
    with a small budget so the test is fast and deterministic (the token bucket is
    per-IP; TestClient is one IP)."""
    with TestClient(create_app(rate_limit_per_minute=5)) as c:
        codes = [
            c.post("/api/project/validate", json=_tiny_spec()).status_code
            for _ in range(12)
        ]
    # The first ~5 succeed (full bucket), then the bucket drains to 429. (Refill is
    # ~5/60 per second, so within a fast test loop essentially nothing refills.)
    assert 429 in codes, codes
    assert codes[0] == 200, "the first request should be within budget"
    assert codes.count(200) <= 6, f"more 200s than the bucket allows: {codes}"


def test_rate_limit_includes_retry_after(catalog_db):
    with TestClient(create_app(rate_limit_per_minute=1)) as c:
        c.post("/api/project/validate", json=_tiny_spec())
        resp = c.post("/api/project/validate", json=_tiny_spec())
    assert resp.status_code == 429
    assert "retry-after" in {k.lower() for k in resp.headers}


def test_reads_are_not_rate_limited(catalog_db):
    """GET reads pass through the limiter untouched (method gate) — a tight write
    budget must NOT throttle reads, which are edge-cached on a different axis."""
    with TestClient(create_app(rate_limit_per_minute=1)) as c:
        codes = [c.get("/api/catalog").status_code for _ in range(10)]
    assert all(code == 200 for code in codes), codes


def test_reads_are_not_body_capped(catalog_db):
    """A GET passes the body-cap middleware untouched (it only gates write
    methods) — exercised here as a sanity check that reads still work with the
    write middlewares installed."""
    with TestClient(create_app()) as c:
        assert c.get("/api/context").status_code == 200


def test_wrong_content_type_on_validate(client):
    """``/validate`` reads the raw body and json.loads it regardless of the
    declared content-type — a JSON body sent as text/plain still validates (the
    endpoint doesn't gate on content-type; it gates on parseability). A
    non-JSON body under any content-type is a 400 (malformed request)."""
    # JSON bytes mislabeled text/plain → still parsed → 200.
    ok = client.post(
        "/api/project/validate",
        content=json.dumps(_tiny_spec()).encode("utf-8"),
        headers={"content-type": "text/plain"},
    )
    assert ok.status_code == 200
    # Non-JSON bytes → 400.
    bad = client.post(
        "/api/project/validate",
        content=b"\x00\x01not json",
        headers={"content-type": "application/octet-stream"},
    )
    assert bad.status_code == 400
