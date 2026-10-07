"""`/api/catalog` renamed/dead-slug redirects against the slugged ``catalog_db`` fixture.

Covers the #355 redirect to the ABSOLUTE chain end and the 404 for a slug that
never existed. Register, suffixed and query-preserving redirects are pinned by
``conformance/cases/http_catalog/redirects``; absent bindings on each suffix by
``test_catalog_subendpoints.py``.
"""

from __future__ import annotations


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
