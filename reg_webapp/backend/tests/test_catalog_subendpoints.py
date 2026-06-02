"""A5.2a-ii catalog-READ sub-endpoints against the slugged ``catalog_db`` fixture.

Covers the 7 suffixed / sub-resource routes (`/states`, `/predecessors`,
`/successors`, `/related`, `/lineage`, `/lineage_warnings`, and the
`/{provider}/{register}/variants` register sub-resource), the `?period` query on
the catch-all (the `{states: [...]}` resolve_at shape), the `@version`-vs-
`?value_set_version` reconciliation, and a per-DB-backed-route ThreadPoolExecutor
concurrency smoke (the A5.1b-ii P1 cross-thread guard). The §16 security gate
(malformed period/variant/traversal → 422 + zero SQL) lives in
``test_fqid_validation.py``.

The ``catalog_db`` fixture seeds ``scb/lisa/kon`` with a same_as edge, a
succession edge (kon→rams/syss), a related-to edge, a lineage edge (kon's state
consumes syss's), and a lineage warning — so the suffixed endpoints return
non-empty bodies.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app

_KON = "scb/lisa/kon"
_SYSS = "scb/rams/syss"


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


# ── The 6 binding-suffix sub-endpoints ──────────────────────────────────────


def test_states_endpoint(client):
    resp = client.get(f"/api/catalog/{_KON}/states")
    assert resp.status_code == 200
    body = resp.json()
    assert body["binding"] == _KON
    assert len(body["states"]) == 1
    state = body["states"][0]
    assert state["variant"] == "individer-15plus"
    # Uniform with the leaf-embed + the ?period response: value_set hydrated.
    assert state["value_set"] == [
        {"code": "1", "label": "Man"},
        {"code": "2", "label": "Kvinna"},
    ]


def test_successors_endpoint(client):
    resp = client.get(f"/api/catalog/{_KON}/successors")
    assert resp.status_code == 200
    body = resp.json()
    assert body["binding"] == _KON
    assert any(r["fqid"] == _SYSS for r in body["successors"])
    # #142 succession fields ride through; `register` is the wire alias.
    ref = next(r for r in body["successors"] if r["fqid"] == _SYSS)
    assert ref["register"] == "rams"
    assert ref["effective_year"] == 2019
    assert ref["reason"] == "kon→syss"


def test_predecessors_endpoint(client):
    # The succession edge is kon→syss, so syss has kon as a PREDECESSOR.
    resp = client.get(f"/api/catalog/{_SYSS}/predecessors")
    assert resp.status_code == 200
    body = resp.json()
    assert body["binding"] == _SYSS
    assert any(r["fqid"] == _KON for r in body["predecessors"])


def test_related_endpoint(client):
    resp = client.get(f"/api/catalog/{_KON}/related")
    assert resp.status_code == 200
    body = resp.json()
    assert body["binding"] == _KON
    rel = next(r for r in body["related"] if r["fqid"] == _SYSS)
    assert rel["relation_kind"] == "same_definition_different_column"


def test_lineage_endpoint(client):
    resp = client.get(f"/api/catalog/{_KON}/lineage")
    assert resp.status_code == 200
    body = resp.json()
    assert body["binding"] == _KON
    assert len(body["lineage_edges"]) == 1
    edge = body["lineage_edges"][0]
    assert edge["source_fqid"] == _SYSS
    assert edge["valid_from"] == "2018-01-01"


def test_lineage_warnings_endpoint(client):
    resp = client.get(f"/api/catalog/{_KON}/lineage_warnings")
    assert resp.status_code == 200
    body = resp.json()
    assert body["binding"] == _KON
    assert len(body["lineage_warnings"]) == 1
    w = body["lineage_warnings"][0]
    assert w["warning_kind"] == "no_source_state"
    assert "2017" in w["message"]


def test_clean_binding_has_empty_lineage_and_warnings(client):
    # syss has no consumer lineage / warnings — empty lists, 200 (not 404).
    assert client.get(f"/api/catalog/{_SYSS}/lineage").json()["lineage_edges"] == []
    assert (
        client.get(f"/api/catalog/{_SYSS}/lineage_warnings").json()["lineage_warnings"]
        == []
    )


# ── The variant browser (register sub-resource) ─────────────────────────────


def test_variants_endpoint(client):
    resp = client.get("/api/catalog/scb/lisa/variants")
    assert resp.status_code == 200
    body = resp.json()
    assert body["register"] == "scb/lisa"  # wire alias
    slugs = {v["slug"] for v in body["variants"]}
    assert "individer-15plus" in slugs


def test_variants_unknown_register_404(client):
    # A typo'd register is a 404, NOT a 200 with an empty list (the resolve guards
    # it before list_variants, so an absent register isn't silently empty).
    resp = client.get("/api/catalog/scb/nope/variants")
    assert resp.status_code == 404


@pytest.mark.parametrize(
    "path",
    [
        "/api/catalog/class/foo/variants",  # `class` as provider
        "/api/catalog/scb/class/variants",  # `class` as register
        "/api/catalog/scb/_default/variants",  # `_default` as register
    ],
)
def test_variants_reserved_segment_422_not_500(client, path: str):
    # `class`/`_default` are reserved and NOT valid provider/register slugs. The
    # variants route validates its segments via `Fqid.register_fqid` (reg_meta's
    # authoritative slug check) BEFORE opening a connection, so a reserved segment
    # is a clean 422 — not the uncaught-FqidError 500 the old path-guard reuse gave
    # (`class/<slug>` is a legal classification PATH, but never a valid provider).
    resp = client.get(path)
    assert resp.status_code == 422, f"{path} → {resp.status_code}"


# ── Sub-endpoints are binding-only: non-binding FQID → 422; absent → 404 ─────


@pytest.mark.parametrize(
    "suffix",
    ["states", "predecessors", "successors", "related", "lineage", "lineage_warnings"],
)
def test_subendpoint_on_register_fqid_is_422(client, suffix: str):
    # A 2-seg (register) FQID is not a binding — reg_meta raises
    # `not_a_binding_fqid` (EXIT_USAGE), mapped to 422 (a usage error, not 500).
    resp = client.get(f"/api/catalog/scb/lisa/{suffix}")
    # `scb/lisa/<suffix>` parses as a 3-seg binding FQID (suffix is the variable
    # slug); the suffixed route matches first, so the FQID handed to the accessor
    # is the 2-seg `scb/lisa`. It resolves to a register → 422.
    assert resp.status_code == 422


@pytest.mark.parametrize(
    "suffix",
    ["states", "predecessors", "successors", "related", "lineage", "lineage_warnings"],
)
def test_subendpoint_on_absent_binding_is_404(client, suffix: str):
    resp = client.get(f"/api/catalog/scb/lisa/doesnotexist/{suffix}")
    assert resp.status_code == 404


@pytest.mark.parametrize(
    "suffix",
    ["states", "predecessors", "successors", "related", "lineage", "lineage_warnings"],
)
def test_subendpoint_rejects_at_version_pin(client, suffix: str):
    # The suffixed endpoints return the FULL history / edge set and don't narrow by
    # value-set-version. An @version pin in the FQID would be inert — so it 422s
    # rather than silently returning unfiltered data (same as the catch-all 422ing
    # a narrowing modifier without ?period).
    resp = client.get(f"/api/catalog/{_KON}@v1/{suffix}")
    assert resp.status_code == 422, f"{suffix} → {resp.status_code}"


# ── The ?period query on the catch-all (the {states:[...]} shape) ────────────


def test_period_query_on_binding_returns_states(client):
    resp = client.get(f"/api/catalog/{_KON}?period=2020")
    assert resp.status_code == 200
    body = resp.json()
    # Uniform with /states: a {binding, states} envelope, NOT the full leaf node.
    assert set(body) == {"binding", "states"}
    assert body["binding"] == _KON
    assert len(body["states"]) == 1


def test_period_range_query(client):
    resp = client.get(f"/api/catalog/{_KON}?period=2018..2020")
    assert resp.status_code == 200
    assert len(resp.json()["states"]) == 1


def test_period_query_with_variant(client):
    resp = client.get(f"/api/catalog/{_KON}?period=2020&variant=individer-15plus")
    assert resp.status_code == 200
    assert len(resp.json()["states"]) == 1
    # A variant that names no variant under the register → empty (no exception).
    resp_empty = client.get(f"/api/catalog/{_KON}?period=2020&variant=nonesuch")
    assert resp_empty.status_code == 200
    assert resp_empty.json()["states"] == []


def test_period_query_empty_when_no_state_covers(client):
    # The binding exists but no state covers 1900 → 200 {states: []}, not 404.
    resp = client.get(f"/api/catalog/{_KON}?period=1900")
    assert resp.status_code == 200
    assert resp.json()["states"] == []


def test_period_query_on_absent_binding_is_404(client):
    resp = client.get("/api/catalog/scb/lisa/doesnotexist?period=2020")
    assert resp.status_code == 404


def test_period_query_ignored_on_non_binding(client):
    # §9.5: the period query is IGNORED on non-binding kinds — a register still
    # resolves to its full register node, not a states envelope.
    resp = client.get("/api/catalog/scb/lisa?period=2020")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "register"
    assert "states" not in body


def test_no_period_query_returns_full_leaf(client):
    # Without ?period, the catch-all still returns the FULL embedded leaf node.
    resp = client.get(f"/api/catalog/{_KON}")
    body = resp.json()
    assert body["kind"] == "binding"
    assert "same_as" in body  # the full record, not the states envelope


# ── @version pin vs ?value_set_version reconciliation (LOCKED) ───────────────


def test_value_set_version_query_alone_is_accepted(client):
    # No state carries a value_set_version_label, so narrowing yields empty — but
    # the request is well-formed (200), proving the query is wired.
    resp = client.get(f"/api/catalog/{_KON}?period=2020&value_set_version=v1")
    assert resp.status_code == 200
    assert resp.json()["states"] == []


def test_value_set_version_query_accepts_a_free_text_label(client):
    # [A5.3b] ?value_set_version is matched against the FREE-TEXT
    # value_set_version_label (a Python filter, not SQL), so a real label with
    # spaces/commas/case/non-ASCII must NOT be 422'd by the §16 gate (the old
    # slug-grammar gate rejected every real label — the SPA's version picker).
    resp = client.get(
        f"/api/catalog/{_KON}",
        params={
            "period": "2020",
            "value_set_version": "SUN 1996, 5 positioner, brutto",
        },
    )
    assert resp.status_code == 200, resp.json()
    assert resp.json()["states"] == []  # no fixture state carries that label


def test_value_set_version_query_rejects_control_chars(client):
    # The §16 sanity gate still 422s a NUL/control char (smuggling vector).
    resp = client.get(
        f"/api/catalog/{_KON}",
        params={"period": "2020", "value_set_version": "bad\x00label"},
    )
    assert resp.status_code == 422


def test_at_version_pin_alone_is_accepted(client):
    resp = client.get(f"/api/catalog/{_KON}@v1?period=2020")
    assert resp.status_code == 200
    assert resp.json()["states"] == []


def test_at_version_and_query_equal_is_accepted(client):
    resp = client.get(f"/api/catalog/{_KON}@v1?period=2020&value_set_version=v1")
    assert resp.status_code == 200
    assert resp.json()["states"] == []


def test_at_version_and_query_conflict_is_422(client):
    # LOCKED: @version pin AND ?value_set_version both present but DIFFERENT →
    # 422 (ambiguous). This is a client contradiction, not a silent precedence.
    resp = client.get(f"/api/catalog/{_KON}@v1?period=2020&value_set_version=v2")
    assert resp.status_code == 422


def test_at_version_and_query_conflict_without_period_is_422(client):
    # The conflict is a client contradiction regardless of ?period — even on the
    # full-leaf path (no ?period) a differing @version vs ?value_set_version 422s.
    resp = client.get(f"/api/catalog/{_KON}@v1?value_set_version=v2")
    assert resp.status_code == 422


@pytest.mark.parametrize(
    "url",
    [
        f"/api/catalog/{_KON}@v1",  # @version pin, no ?period
        f"/api/catalog/{_KON}?value_set_version=v1",  # ?value_set_version, no ?period
        f"/api/catalog/{_KON}?variant=individer-15plus",  # ?variant, no ?period
    ],
)
def test_narrowing_modifier_without_period_is_422(client, url: str):
    # A narrowing modifier (@version / ?value_set_version / ?variant) is inert
    # without ?period — it only takes effect inside resolve_at. Rather than
    # silently no-op (return the full leaf as if the modifier were absent), the
    # catch-all 422s "requires ?period" so the param never silently does nothing.
    resp = client.get(url)
    assert resp.status_code == 422, f"{url} → {resp.status_code}"


def test_inverted_period_range_is_422_not_500(client):
    # `2021..2020` is a syntactically valid range (two valid period tokens) so the
    # §16 allow-list admits it, but resolve_at rejects lo>hi with a USAGE error.
    # That's client input → 422, NOT an uncaught-RegMetaError 500.
    resp = client.get(f"/api/catalog/{_KON}?period=2021..2020")
    assert resp.status_code == 422, f"inverted range → {resp.status_code}"


# ── ETag / Cache-Control + 304 on every read endpoint ────────────────────────

_READ_PATHS = [
    "/api/context",
    "/api/catalog",
    f"/api/catalog/{_KON}",
    f"/api/catalog/{_KON}?period=2020",
    f"/api/catalog/{_KON}/states",
    f"/api/catalog/{_KON}/predecessors",
    f"/api/catalog/{_KON}/successors",
    f"/api/catalog/{_KON}/related",
    f"/api/catalog/{_KON}/lineage",
    f"/api/catalog/{_KON}/lineage_warnings",
    "/api/catalog/scb/lisa/variants",
]


@pytest.mark.parametrize("path", _READ_PATHS)
def test_read_endpoint_sets_etag_and_cache_control(client, path: str):
    resp = client.get(path)
    assert resp.status_code == 200
    etag = resp.headers.get("etag")
    assert etag and etag.startswith('"') and etag.endswith('"')
    assert resp.headers.get("cache-control") == "public, max-age=86400, must-revalidate"


@pytest.mark.parametrize("path", _READ_PATHS)
def test_if_none_match_returns_304(client, path: str):
    etag = client.get(path).headers["etag"]
    resp = client.get(path, headers={"If-None-Match": etag})
    assert resp.status_code == 304
    assert resp.content == b""
    # The validating headers survive on the 304.
    assert resp.headers["etag"] == etag
    assert resp.headers.get("cache-control") == "public, max-age=86400, must-revalidate"


def test_period_in_etag_cache_key(client):
    # The ?period query is part of the URL → part of the cache key: different
    # periods are different ETags. (Different bodies here: 1 state vs 0.)
    a = client.get(f"/api/catalog/{_KON}?period=2020").headers["etag"]
    b = client.get(f"/api/catalog/{_KON}?period=1900").headers["etag"]
    assert a != b


# ── Concurrency: the A5.1b-ii P1 cross-thread guard, per DB-backed route ─────
# Each new DB-backed route opens its sqlite connection INSIDE the sync handler
# body (one thread). The TestClient's sequential default masks the bug, so we hit
# each route from a ThreadPoolExecutor (>=50 concurrent). A generator-Depends-
# opened connection used cross-thread would raise sqlite3.ProgrammingError →
# non-200 (reproduced 72/80 on #168 before the fix).

_CONCURRENT_DB_ROUTES = [
    f"/api/catalog/{_KON}/states",
    f"/api/catalog/{_KON}/predecessors",
    f"/api/catalog/{_SYSS}/predecessors",
    f"/api/catalog/{_KON}/successors",
    f"/api/catalog/{_KON}/related",
    f"/api/catalog/{_KON}/lineage",
    f"/api/catalog/{_KON}/lineage_warnings",
    "/api/catalog/scb/lisa/variants",
    f"/api/catalog/{_KON}?period=2020",
    f"/api/catalog/{_KON}?period=2018..2020&variant=individer-15plus",
    # The other in-handler-open paths (root, no-period binding leaf, class root) —
    # same `_catalog_conn` pattern, so a future Depends-opened conn on any of them
    # would regress the cross-thread guard.
    "/api/catalog",
    f"/api/catalog/{_KON}",
    "/api/catalog/class",
]


@pytest.mark.parametrize("path", _CONCURRENT_DB_ROUTES)
def test_concurrent_no_cross_thread_error(client, path: str):
    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(pool.map(lambda _: client.get(path).status_code, range(50)))
    failures = [c for c in codes if c != 200]
    assert not failures, f"{path}: cross-thread failures under concurrency: {failures}"
