"""`/api/catalog` renamed/dead-slug redirects against the slugged ``catalog_db`` fixture.

Covers the #355 301-to-terminal redirects for registers, bindings, period-bearing
binding URLs and suffixed binding sub-endpoints, and the 404s for slugs that
never existed.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def test_renamed_binding_redirects_301_to_terminal(client):
    """#355 PART 2: a citation of a renamed/dead binding slug 301-redirects to its
    TERMINAL successor. The fixture seeds the chain
    `scb/lisa/renamed-head → scb/lisa/renamed-mid → scb/rams/syss` (head + mid
    have no `variable` row); a GET on the head must land at the terminal.

    `follow_redirects=False` is REQUIRED — TestClient follows 301s by default, so
    without it we'd see the followed 200 from the terminal, not the redirect."""
    resp = client.get("/api/catalog/scb/lisa/renamed-head", follow_redirects=False)
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/rams/syss"


def test_renamed_register_redirects_301_to_terminal(client):
    """#412: a citation of a renamed/dead REGISTER slug 301-redirects to its
    terminal successor, the same way a renamed binding does. The fixture seeds
    `scb/oldreg → scb/lisa` (`oldreg` has no `register` row); a GET on the dead
    register must land at the live `scb/lisa` register node.

    `follow_redirects=False` is REQUIRED — TestClient follows 301s by default, so
    without it we'd see the followed 200 from the terminal, not the redirect."""
    resp = client.get("/api/catalog/scb/oldreg", follow_redirects=False)
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/lisa"


def test_unknown_dead_register_still_404(client):
    """#412: a truly-unknown dead REGISTER slug with NO successor edge stays 404
    (not a redirect) — `resolve_terminal_successor` returns None, so the original
    404 re-raises unchanged (mirrors `test_unknown_dead_binding_still_404`)."""
    resp = client.get("/api/catalog/scb/never-existed-reg", follow_redirects=False)
    assert resp.status_code == 404


def test_renamed_binding_redirect_walks_to_absolute_chain_end(client):
    """The redirect always resolves to the ABSOLUTE chain end, never one hop: a
    GET on the MIDDLE dead slug also lands at the terminal `scb/rams/syss`."""
    resp = client.get("/api/catalog/scb/lisa/renamed-mid", follow_redirects=False)
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/rams/syss"


def test_unknown_dead_binding_still_404(client):
    """A truly-unknown dead slug with NO successor edge stays 404 (not a
    redirect) — `resolve_terminal_successor` returns None, so the original 404
    re-raises unchanged."""
    resp = client.get("/api/catalog/scb/lisa/never-existed", follow_redirects=False)
    assert resp.status_code == 404


def test_dead_binding_with_period_redirects_301_preserving_query(client):
    """#411: a dead/renamed binding cited WITH `?period` now 301s to its terminal
    successor (previously a deferred 404). The query string rides along, so the
    Location keeps `?period=2019`. The chain head `renamed-head` → terminal
    `scb/rams/syss`."""
    resp = client.get(
        "/api/catalog/scb/lisa/renamed-head?period=2019", follow_redirects=False
    )
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/rams/syss?period=2019"


def test_live_binding_inverted_period_range_stays_422(client):
    """#411: a syntactically-valid but lo>hi `?period` range on a LIVE binding must
    stay 422 — `_redirect_or_4xx` redirects ONLY on `fqid_not_found`; the inverted
    range passes the syntactic `_validated_period` dependency (reg_meta is the
    semantic backstop) and is rejected inside `resolve_at` as an EXIT_USAGE
    `invalid_period` error, which falls back to `_http_4xx_from_regmeta` (422),
    never a 301."""
    resp = client.get(
        "/api/catalog/scb/lisa/kon?period=2020..2019", follow_redirects=False
    )
    assert resp.status_code == 422


# The 6 suffixed binding sub-endpoints — the redirect must preserve the suffix.
_SUB_ENDPOINTS = [
    "states",
    "predecessors",
    "successors",
    "lineage",
    "lineage_warnings",
]


@pytest.mark.parametrize("suffix", _SUB_ENDPOINTS)
def test_dead_binding_subendpoint_redirects_301_to_same_suffix(client, suffix):
    """#411: a dead/renamed binding cited on any of the 6 suffixed sub-endpoints
    301s to the SAME suffix on its terminal successor. The chain head
    `renamed-head/<suffix>` → `scb/rams/syss/<suffix>`."""
    resp = client.get(
        f"/api/catalog/scb/lisa/renamed-head/{suffix}", follow_redirects=False
    )
    assert resp.status_code == 301
    assert resp.headers["location"] == f"/api/catalog/scb/rams/syss/{suffix}"


def test_dead_binding_with_period_redirect_walks_to_absolute_chain_end(client):
    """#411: the `?period` redirect resolves to the ABSOLUTE chain end, never one
    hop — a GET on the MIDDLE dead slug also lands at the terminal, query
    preserved."""
    resp = client.get(
        "/api/catalog/scb/lisa/renamed-mid?period=2019", follow_redirects=False
    )
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/rams/syss?period=2019"


def test_unknown_dead_binding_with_period_still_404(client):
    """#411: an UNKNOWN dead binding (no successor edge) WITH `?period` still 404s —
    `resolve_terminal_successor` returns None, so `_redirect_or_4xx` falls back to
    the 404 (a 422 usage / 500 build-invariant error would NEVER redirect either)."""
    resp = client.get(
        "/api/catalog/scb/lisa/never-existed?period=2019", follow_redirects=False
    )
    assert resp.status_code == 404


@pytest.mark.parametrize("suffix", _SUB_ENDPOINTS)
def test_unknown_dead_binding_subendpoint_still_404(client, suffix):
    """#411: an UNKNOWN dead binding (no successor edge) on a sub-endpoint still
    404s — no terminal to redirect to."""
    resp = client.get(
        f"/api/catalog/scb/lisa/never-existed/{suffix}", follow_redirects=False
    )
    assert resp.status_code == 404
