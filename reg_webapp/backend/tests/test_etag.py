"""Unit tests for the ETag / Cache-Control LOGIC (``etag.py``).

See DESIGN.md → ETag / Cache-Control (etag.py + middleware.py). Pure-function
layer — no app, no DB. The middleware wiring + the end-to-end
304 behavior live in ``test_etag_middleware.py``. The Cloudflare edge-cache
round-trip is a MAINTAINER task, explicitly NOT tested here.
"""

from __future__ import annotations

import hashlib

from reg_webapp.etag import (
    CACHE_CONTROL,
    CACHE_CONTROL_REVALIDATE,
    CACHE_CONTROL_SHORT,
    cache_control_for,
    compute_etag,
    etag_matches,
)


def test_compute_etag_shape():
    body = b'{"kind":"root"}'
    etag = compute_etag(body, "1.2.3", "global")
    digest = hashlib.sha256(body).hexdigest()[:16]
    assert etag == f'"1.2.3-global-{digest}"'
    # Quoted (RFC 7232 strong validator), version + steward prefix human-readable.
    assert etag.startswith('"1.2.3-global-')
    assert etag.endswith('"')


def test_compute_etag_is_body_sensitive():
    # The body-hash component makes per-URL ETags coherent — different bodies →
    # different ETags even with the same version + steward.
    a = compute_etag(b"a", "1.0.0", "global")
    b = compute_etag(b"b", "1.0.0", "global")
    assert a != b


def test_compute_etag_keyspace_invalidates_on_version_or_steward():
    body = b"same"
    assert compute_etag(body, "1.0.0", "global") != compute_etag(
        body, "2.0.0", "global"
    )
    assert compute_etag(body, "1.0.0", "global") != compute_etag(body, "1.0.0", "ifau")


def test_compute_etag_is_deterministic():
    body = b'{"x":1}'
    assert compute_etag(body, "1.0.0", "global") == compute_etag(
        body, "1.0.0", "global"
    )


def test_cache_control_value():
    assert CACHE_CONTROL == "public, max-age=86400, must-revalidate"
    assert CACHE_CONTROL_SHORT == "public, max-age=60, must-revalidate"


def test_cache_control_for_per_route():
    # Three tiers: /api/context revalidates every request (the vintage footer
    # asserts a deploy version/date — a stale copy lies); the fold-bearing
    # /api/catalog/* and /api/search reads get the short 60s window (curated folds
    # must surface promptly); the rebuild-stable /api/docs/* reads keep the 24h
    # policy.
    assert cache_control_for("/api/context") == CACHE_CONTROL_REVALIDATE

    # Catalog root + sub-endpoints (prefix match) all get the short window.
    assert cache_control_for("/api/catalog") == CACHE_CONTROL_SHORT
    assert cache_control_for("/api/catalog/sos/lova") == CACHE_CONTROL_SHORT
    assert (
        cache_control_for("/api/catalog/scb/lisa/sun2000/states") == CACHE_CONTROL_SHORT
    )

    # The variable/code search route embeds the same #322 concept-group folds, so
    # it joins the short window (#506).
    assert cache_control_for("/api/search") == CACHE_CONTROL_SHORT

    # The doc-library search is rebuild-stable and lives under /api/docs → keeps
    # the 24h policy. It does NOT collide with the /api/search prefix
    # (`/api/docs/search`.startswith(`/api/search`) is False).
    assert cache_control_for("/api/docs/search") == CACHE_CONTROL


def test_etag_matches_exact():
    etag = compute_etag(b"body", "1.0.0", "global")
    assert etag_matches(etag, etag)


def test_etag_matches_none_or_empty_is_false():
    etag = compute_etag(b"body", "1.0.0", "global")
    assert not etag_matches(None, etag)
    assert not etag_matches("", etag)


def test_etag_matches_mismatch_is_false():
    etag = compute_etag(b"body", "1.0.0", "global")
    other = compute_etag(b"different", "1.0.0", "global")
    assert not etag_matches(other, etag)


def test_etag_matches_list_form():
    etag = compute_etag(b"body", "1.0.0", "global")
    other = compute_etag(b"other", "1.0.0", "global")
    # RFC 7232 comma-separated If-None-Match list — match if any member matches.
    assert etag_matches(f"{other}, {etag}", etag)
    assert not etag_matches(f"{other}, {other}", etag)


def test_etag_matches_wildcard():
    etag = compute_etag(b"body", "1.0.0", "global")
    assert etag_matches("*", etag)


def test_etag_matches_weak_validator_matches_strong():
    # RFC 7232 Section 3.2: If-None-Match uses WEAK comparison, so a weak-validator
    # request value (W/"...") matches our strong ETag with the same opaque-tag —
    # this is what lets an edge (Cloudflare) that weakened our ETag still 304.
    etag = compute_etag(b"body", "1.0.0", "global")
    assert etag_matches(f"W/{etag}", etag)
    # A weak validator with a DIFFERENT opaque-tag still does not match.
    other = compute_etag(b"different", "1.0.0", "global")
    assert not etag_matches(f"W/{other}", etag)
