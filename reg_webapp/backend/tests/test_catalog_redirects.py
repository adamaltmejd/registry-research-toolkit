"""`/api/catalog` renamed/dead-slug redirects against the slugged ``catalog_db`` fixture.

Covers the #355 redirect to the ABSOLUTE chain end, the 404 for a slug that never
existed, and the #411 redirect on every binding-suffix handler. Register and
bare-binding redirects are pinned by ``conformance/cases/http_catalog/redirects``;
absent bindings on each suffix by ``test_catalog_subendpoints.py``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from http_cases import case_artifact
from reg_webapp.app import create_app

# Every `/api/catalog/{fqid}/<suffix>` route in the committed schema, which
# `test_openapi_snapshot.py` pins to the router: a new suffix handler joins the
# redirect test below without anyone copying its name here.
_OPENAPI = Path(__file__).resolve().parents[1] / "openapi.json"
_BINDING_PREFIX = "/api/catalog/{fqid}/"
_BINDING_SUFFIXES = sorted(
    path.removeprefix(_BINDING_PREFIX)
    for path in json.loads(_OPENAPI.read_text(encoding="utf-8"))["paths"]
    if path.startswith(_BINDING_PREFIX)
)
assert _BINDING_SUFFIXES, f"no {_BINDING_PREFIX}<suffix> paths in {_OPENAPI}"


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


@pytest.mark.parametrize("scope", ["holdings", "reference"])
@pytest.mark.parametrize("suffix", _BINDING_SUFFIXES)
def test_retired_binding_suffix_redirects_to_successor_suffix(
    suffix, scope, monkeypatch
):
    """#411: a retired binding cited on any suffix 301s to the SAME suffix on its
    successor, query string kept. The compiled steward fixture retires
    `scb/example/retired` in favour of `scb/example/second`.

    Each handler spells its own suffix literal at two redirect sites:
    `_require_admitted` (holdings scope) and `_redirect_or_4xx` (the accessor's
    not-found). A handler that drops its redirect, or passes another suffix,
    fails its case here."""
    case_artifact({}, monkeypatch)  # steward artifact from fixtures/compiled
    with TestClient(create_app()) as client:
        resp = client.get(
            f"/api/catalog/scb/example/retired/{suffix}",
            params={"scope": scope},
            follow_redirects=False,
        )
    assert resp.status_code == 301
    assert (
        resp.headers["location"]
        == f"/api/catalog/scb/example/second/{suffix}?scope={scope}"
    )
