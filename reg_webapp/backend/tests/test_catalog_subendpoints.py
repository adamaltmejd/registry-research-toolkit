"""A5.2a-ii catalog-READ sub-endpoints against the slugged ``catalog_db`` fixture.

Covers the 7 suffixed / sub-resource routes (`/states`, `/predecessors`,
`/successors`, `/related`, `/lineage`, `/lineage_warnings`, and the
`/{provider}/{register}/variants` register sub-resource), the `?period` query on
the catch-all (the `{states: [...]}` resolve_at shape), the read-only
`?value_set_version` browse-narrowing label filter, and a per-DB-backed-route
ThreadPoolExecutor concurrency smoke (the A5.1b-ii P1 cross-thread guard). (The
`@version` FQID pin is retired — a bare leaf is the only form.) The security
gate
(malformed period/variant/traversal → 422 + zero SQL; see DESIGN.md → FQID path
guard (catalog_fqid.py)) lives in ``test_fqid_validation.py``.

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
from reg_webapp.etag import CACHE_CONTROL, CACHE_CONTROL_REVALIDATE

_KON = "scb/lisa/kon"
_SYSS = "scb/rams/syss"
# `inkjan`/`inkfeb` are the two members of the `ink` token concept group on
# scb/rams (seeded by `_seed_concept_groups`); `syss` is on scb/rams but in NO
# variable concept group (the #489 dimensions tests pin both).
_INKJAN = "scb/rams/inkjan"


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


def test_dimensions_endpoint(client):
    # `inkjan` is a member of the `ink` token concept group on scb/rams (#489) —
    # the endpoint returns that group, with `inkjan` among its members.
    resp = client.get(f"/api/catalog/{_INKJAN}/dimensions")
    assert resp.status_code == 200
    body = resp.json()
    assert body["binding"] == _INKJAN
    keys = {g["key"] for g in body["dimensions"]}
    assert keys == {"ink"}, body
    ink = next(g for g in body["dimensions"] if g["key"] == "ink")
    member_fqids = {m["fqid"] for m in ink["members"]}
    assert _INKJAN in member_fqids
    assert "scb/rams/inkfeb" in member_fqids  # the group is fully populated


def test_dimensions_endpoint_excludes_non_member_groups(client):
    # `syss` lives on scb/rams (which HAS the `ink` group) but is itself in NO
    # group — so its dimensions are empty (the filter excludes groups that don't
    # contain this exact binding, NOT every group on the register).
    resp = client.get(f"/api/catalog/{_SYSS}/dimensions")
    assert resp.status_code == 200
    assert resp.json()["dimensions"] == []


def test_dimensions_endpoint_resolves_through_same_as(client):
    # #489 P2-A: `scb/lisa/inkjan-alias` has no live row — it resolves only via
    # `variable_same_as` to the grouped target `scb/rams/inkjan`. The endpoint must
    # cite the TARGET register's `ink` group (the old handler keyed the filter on
    # the REQUESTED register/fqid and returned []). `binding` echoes the request.
    resp = client.get("/api/catalog/scb/lisa/inkjan-alias/dimensions")
    assert resp.status_code == 200
    body = resp.json()
    assert body["binding"] == "scb/lisa/inkjan-alias"
    assert {g["key"] for g in body["dimensions"]} == {"ink"}, body
    member_fqids = {m["fqid"] for g in body["dimensions"] for m in g["members"]}
    assert _INKJAN in member_fqids  # the resolved target's group, not lisa's


def test_dimensions_endpoint_dead_binding_301s_to_successor(client):
    # A dead/renamed binding 301s to /dimensions on its terminal successor (#411),
    # uniform with the other sub-endpoints. `renamed-head` → … → scb/rams/syss.
    resp = client.get(
        "/api/catalog/scb/lisa/renamed-head/dimensions", follow_redirects=False
    )
    assert resp.status_code == 301
    assert resp.headers["location"] == f"/api/catalog/{_SYSS}/dimensions"


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


def test_variants_endpoint_serializes_panel_fields(client):
    # A4.4c: the `standard` variant on rams carries curated panel data; the
    # composite entity key serializes as a JSON list. lisa's `_default` variant
    # has no panel data → the three fields are absent/None.
    variants = client.get("/api/catalog/scb/rams/variants").json()["variants"]
    std = next(v for v in variants if v["slug"] == "standard")
    assert std["panel_entity_key"] == ["foretag", "arbetsstalle"]
    assert std["panel_time_key"] == "period"
    assert std["panel_time_grain"] == "delivery"

    lisa = client.get("/api/catalog/scb/lisa/variants").json()["variants"]
    default = next(v for v in lisa if v["slug"] == "individer-15plus")
    assert default["panel_entity_key"] is None
    assert default["panel_time_key"] is None
    assert default["panel_time_grain"] is None


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
    [
        "states",
        "predecessors",
        "successors",
        "related",
        "lineage",
        "lineage_warnings",
        "dimensions",
    ],
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
    [
        "states",
        "predecessors",
        "successors",
        "related",
        "lineage",
        "lineage_warnings",
        "dimensions",
    ],
)
def test_subendpoint_on_absent_binding_is_404(client, suffix: str):
    resp = client.get(f"/api/catalog/scb/lisa/doesnotexist/{suffix}")
    assert resp.status_code == 404


@pytest.mark.parametrize(
    "suffix",
    [
        "states",
        "predecessors",
        "successors",
        "related",
        "lineage",
        "lineage_warnings",
        "dimensions",
    ],
)
def test_subendpoint_rejects_at_version_pin(client, suffix: str):
    # The `@version` pin is retired — a binding leaf is a bare slug, so the `@` is a
    # non-slug character the path gate rejects (422) before the suffixed handler
    # runs, on every sub-resource route.
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


# ── The #307/#340 comma LIST form: per-segment resolve, state_id-deduped union ─
# The fixture kon has ONE state (2018→open), so the union assertions pin the
# union/dedup MECHANICS: a covering segment's states must survive regardless of
# its position (catches first-only/last-only bugs), and two segments hitting
# the SAME state must count it once (catches concat-without-dedup).


def test_period_list_query_unions_per_segment(client):
    # First segment covers, second doesn't — and vice versa: both orders return
    # the covered segment's state.
    for wire in ("2020,1900", "1900,2020"):
        resp = client.get(f"/api/catalog/{_KON}?period={wire}")
        assert resp.status_code == 200, wire
        assert len(resp.json()["states"]) == 1, wire


def test_period_list_query_dedupes_by_state_id(client):
    # Both segments intersect the SAME state → once, not twice.
    resp = client.get(f"/api/catalog/{_KON}?period=2019,2020")
    assert resp.status_code == 200
    assert len(resp.json()["states"]) == 1


# #319: the comma-list dedup keys on the COMPOUND (state_id, delivery_column_name,
# valid_from), NOT state_id alone — a merged monthly-family variable expands ONE
# annual state into per-month windows that share a state_id. The seeded `lonfink`
# variable has 3 month windows (jan/feb/mars) for 2018.
_LONFINK = "scb/lisa/lonfink"


def test_period_list_merged_family_not_collapsed_by_shared_state_id(client):
    # `?period=2018,2018`: both segments hit the same annual state, which expands
    # to 3 month windows. Keying on state_id alone would collapse them to 1; the
    # compound key keeps all 3 (deduped across the two identical segments).
    resp = client.get(f"/api/catalog/{_LONFINK}?period=2018,2018")
    assert resp.status_code == 200
    states = resp.json()["states"]
    assert len(states) == 3, states
    cols = sorted(s["delivery_column_name"] for s in states)
    assert cols == ["LonFinkFeb", "LonFinkJan", "LonFinkMars"]


def test_period_single_month_resolves_one_column(client):
    # A single month period on the merged variable → exactly the one month column.
    resp = client.get(f"/api/catalog/{_LONFINK}?period=2018-02")
    assert resp.status_code == 200
    states = resp.json()["states"]
    assert len(states) == 1
    assert states[0]["delivery_column_name"] == "LonFinkFeb"


def test_period_list_query_rejects_malformed(client):
    # The list-member gate 422s before any connection opens: empty members,
    # the whole-value-only `_default` sentinel, junk members.
    for wire in ("2020,", "2020,,2021", "2005..2010,_default", "2020,abc"):
        resp = client.get(f"/api/catalog/{_KON}?period={wire}")
        assert resp.status_code == 422, wire


def test_period_query_on_absent_binding_is_404(client):
    resp = client.get("/api/catalog/scb/lisa/doesnotexist?period=2020")
    assert resp.status_code == 404


def test_period_query_ignored_on_non_binding(client):
    # The period query is IGNORED on non-binding kinds — a register still
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


# ── ?value_set_version read-only browse-narrowing label filter ───────────────


def test_value_set_version_query_alone_is_accepted(client):
    # No state carries a value_set_version_label, so narrowing yields empty — but
    # the request is well-formed (200), proving the query is wired.
    resp = client.get(f"/api/catalog/{_KON}?period=2020&value_set_version=v1")
    assert resp.status_code == 200
    assert resp.json()["states"] == []


def test_value_set_version_query_accepts_a_free_text_label(client):
    # [A5.3b] ?value_set_version is matched against the FREE-TEXT
    # value_set_version_label (a Python filter, not SQL), so a real label with
    # spaces/commas/case/non-ASCII must NOT be 422'd by the gate (the old
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
    # The sanity gate still 422s a NUL/control char (smuggling vector).
    resp = client.get(
        f"/api/catalog/{_KON}",
        params={"period": "2020", "value_set_version": "bad\x00label"},
    )
    assert resp.status_code == 422


def test_value_set_version_none_sentinel_selects_the_empty_label(client):
    # [A5.3b] The `_none` sentinel selects the empty/default-label state (the
    # handler maps it to "" before resolve_at). The fixture's kon states carry the
    # empty label, so `?value_set_version=v1` narrows to NOTHING but `_none`
    # narrows to those states — proving the sentinel ≠ "no narrowing".
    labeled = client.get(f"/api/catalog/{_KON}?period=2020&value_set_version=v1")
    assert labeled.status_code == 200
    assert labeled.json()["states"] == []  # no state has the label "v1"

    sentinel = client.get(f"/api/catalog/{_KON}?period=2020&value_set_version=_none")
    assert sentinel.status_code == 200
    assert len(sentinel.json()["states"]) > 0  # the empty-label states matched


@pytest.mark.parametrize(
    "url",
    [
        f"/api/catalog/{_KON}?value_set_version=v1",  # ?value_set_version, no ?period
        f"/api/catalog/{_KON}?variant=individer-15plus",  # ?variant, no ?period
    ],
)
def test_narrowing_modifier_without_period_is_422(client, url: str):
    # A narrowing modifier (?value_set_version / ?variant) is inert without ?period
    # — it only takes effect inside resolve_at. Rather than silently no-op (return
    # the full leaf as if the modifier were absent), the catch-all 422s "requires
    # ?period" so the param never silently does nothing.
    resp = client.get(url)
    assert resp.status_code == 422, f"{url} → {resp.status_code}"


def test_inverted_period_range_is_422_not_500(client):
    # `2021..2020` is a syntactically valid range (two valid period tokens) so the
    # the allow-list admits it, but resolve_at rejects lo>hi with a USAGE error.
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
    f"/api/catalog/{_INKJAN}/dimensions",
    "/api/catalog/scb/lisa/variants",
]


@pytest.mark.parametrize("path", _READ_PATHS)
def test_read_endpoint_sets_etag_and_cache_control(client, path: str):
    resp = client.get(path)
    assert resp.status_code == 200
    etag = resp.headers.get("etag")
    assert etag and etag.startswith('"') and etag.endswith('"')
    # /api/context (the vintage-footer source, #447) revalidates always; other reads cache 24h.
    expected_cc = CACHE_CONTROL_REVALIDATE if path == "/api/context" else CACHE_CONTROL
    assert resp.headers.get("cache-control") == expected_cc


@pytest.mark.parametrize("path", _READ_PATHS)
def test_if_none_match_returns_304(client, path: str):
    etag = client.get(path).headers["etag"]
    resp = client.get(path, headers={"If-None-Match": etag})
    assert resp.status_code == 304
    assert resp.content == b""
    # The validating headers survive on the 304.
    assert resp.headers["etag"] == etag
    # /api/context (the vintage-footer source, #447) revalidates always; other reads cache 24h.
    expected_cc = CACHE_CONTROL_REVALIDATE if path == "/api/context" else CACHE_CONTROL
    assert resp.headers.get("cache-control") == expected_cc


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
    f"/api/catalog/{_INKJAN}/dimensions",
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
