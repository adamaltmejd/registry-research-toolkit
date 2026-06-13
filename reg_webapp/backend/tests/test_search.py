"""`GET /api/search` — typed FTS result groups (#350).

Runs against the slugged ``catalog_db`` fixture (its FTS indexes are rebuilt in
conftest). Covers: the typed-groups shape + extension contract, register /
variable / classification hits, concept-group folding (#322), the diacritic
parity (å→a) with the SPA filter, the input gates (too-long / NUL / FTS-operator
neutralization), the empty-query degradation, and the ETag round-trip.

The pure helpers (`_build_fts_query`, `_validated_limit`) are unit-tested below
without the app.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app
from reg_webapp.routes.search import _has_searchable_token, _validated_limit


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def _group(body: dict, name: str) -> dict:
    (g,) = [g for g in body["groups"] if g["group"] == name]
    return g


# ── shape / contract ────────────────────────────────────────────────────────


def test_response_has_typed_groups(client):
    body = client.get("/api/search", params={"q": "lisa"}).json()
    assert body["kind"] == "search"
    assert body["query"] == "lisa"
    assert {g["group"] for g in body["groups"]} == {
        "registers",
        "variables",
        "classifications",
        "codes",
    }
    # Every group carries its own total_count + results list (the extensible
    # per-group envelope docs will reuse).
    for g in body["groups"]:
        assert "total_count" in g
        assert isinstance(g["results"], list)


# ── leaf hits + navigable FQIDs ──────────────────────────────────────────────


def test_register_hit_carries_fqid(client):
    g = _group(client.get("/api/search", params={"q": "LISA"}).json(), "registers")
    fqids = [r["fqid"] for r in g["results"]]
    assert "scb/lisa" in fqids
    assert all(r["type"] == "register" for r in g["results"])


def test_variable_hit_carries_binding_fqid(client):
    g = _group(client.get("/api/search", params={"q": "Kön"}).json(), "variables")
    hit = next(r for r in g["results"] if r["type"] == "variable")
    assert hit["fqid"] == "scb/lisa/kon"
    # The owning register name rides under the wire key `register`.
    assert hit["register"] == "LISA"


def test_classification_leaf_hit(client):
    # `SUN2020` matches only the sun2020 short_name → a single leaf (no fold).
    g = _group(
        client.get("/api/search", params={"q": "SUN2020"}).json(), "classifications"
    )
    leaves = [r for r in g["results"] if r["type"] == "classification"]
    hit = next(r for r in leaves if r["fqid"] == "class/sun2020")
    # A lone member keeps its family hint (symmetric with variable leaves).
    assert hit["concept_group"] == "sun"
    assert hit["concept_group_label"]


# ── codes group (#352) ───────────────────────────────────────────────────────


def test_codes_group_always_present(client):
    # Present even when nothing matches (keep all groups in the envelope).
    g = _group(client.get("/api/search", params={"q": "zzqq"}).json(), "codes")
    assert g["total_count"] == 0
    assert g["results"] == []


def test_code_label_hit_carries_owning_variable(client):
    # "Man" is a value label on the kon binding's value set (seeded in conftest)
    # → a code hit annotated with its owning variable.
    g = _group(client.get("/api/search", params={"q": "Man"}).json(), "codes")
    hit = next(r for r in g["results"] if r["label"] == "Man")
    assert hit["type"] == "code"
    assert hit["code"] == "1"
    # The owning variable carries the binding FQID + register context.
    owner = next(v for v in hit["variables"] if v["fqid"] == "scb/lisa/kon")
    assert owner["register"] == "LISA"
    assert hit["variable_count"] >= 1


def test_code_hit_carries_owning_classification(client):
    # The "Man" code is also linked to the sun2020 classification (seeded in
    # conftest) → the hit carries a non-empty classification owner + count.
    g = _group(client.get("/api/search", params={"q": "Man"}).json(), "codes")
    hit = next(r for r in g["results"] if r["label"] == "Man")
    assert hit["classification_count"] >= 1
    owner = next(c for c in hit["classifications"] if c["fqid"] == "class/sun2020")
    assert owner["short_name"] == "SUN2020"


def test_code_shaped_query_well_formed(client):
    # A code-shaped query (digit + len>=3) drives the value_code.code exact/prefix
    # path. The fixture has no "0180" code, so this asserts the group stays
    # well-formed (no 500); the code-match resolution itself is covered by the
    # reg_meta query-layer unit test.
    g = _group(client.get("/api/search", params={"q": "0180"}).json(), "codes")
    assert isinstance(g["results"], list)


# ── concept-group folding (#322) ─────────────────────────────────────────────


def test_variable_concept_group_folds(client):
    # inkjan + inkfeb both named "Inkomst" fold into the `ink` group row.
    g = _group(client.get("/api/search", params={"q": "Inkomst"}).json(), "variables")
    groups = [r for r in g["results"] if r["type"] == "group"]
    assert groups, "expected a folded concept-group row in the variables group"
    grp = next(r for r in groups if r["group_key"] == "ink")
    assert grp["kind"] == "variable"
    assert grp["member_count"] == 2
    assert {m["fqid"] for m in grp["members"]} == {"scb/rams/inkjan", "scb/rams/inkfeb"}


def test_classification_group_folds_without_duplicate_leaves(client):
    # sun2000 + sun2020 share the name "Svensk utbildningsnomenklatur", which is
    # also the `sun` group label → the family folds into ONE group row AND the
    # member leaves are SUBSUMED (not emitted standalone too — the #350 review
    # bug: classification leaves were duplicated as both leaf and folded member).
    g = _group(
        client.get("/api/search", params={"q": "utbildningsnomenklatur"}).json(),
        "classifications",
    )
    groups = [r for r in g["results"] if r["type"] == "group"]
    grp = next(r for r in groups if r["kind"] == "classification")
    member_fqids = {m["fqid"] for m in grp["members"]}
    assert {"class/sun2000", "class/sun2020"} <= member_fqids
    leaf_fqids = {r["fqid"] for r in g["results"] if r["type"] == "classification"}
    # No member appears as a standalone leaf alongside its folded group row.
    assert not (member_fqids & leaf_fqids)


# ── diacritic parity with the SPA filter (å→a) ───────────────────────────────


def test_diacritic_folding_matches_spa(client):
    # unicode61 folds both index + query side, so "kon" (no umlaut) finds "Kön" —
    # the same fold the SPA's foldText applies client-side.
    folded = _group(client.get("/api/search", params={"q": "kon"}).json(), "variables")
    exact = _group(client.get("/api/search", params={"q": "Kön"}).json(), "variables")
    assert any(r.get("fqid") == "scb/lisa/kon" for r in folded["results"])
    assert any(r.get("fqid") == "scb/lisa/kon" for r in exact["results"])


# ── input gates + degradation ────────────────────────────────────────────────


def test_empty_query_returns_empty_groups(client):
    body = client.get("/api/search", params={"q": ""}).json()
    assert {g["group"] for g in body["groups"]} == {
        "registers",
        "variables",
        "classifications",
        "codes",
    }
    assert all(g["total_count"] == 0 and g["results"] == [] for g in body["groups"])


def test_whitespace_query_returns_empty_groups(client):
    body = client.get("/api/search", params={"q": "   "}).json()
    assert all(g["total_count"] == 0 for g in body["groups"])


def test_missing_q_is_422(client):
    assert client.get("/api/search").status_code == 422


def test_too_long_query_is_422(client):
    assert client.get("/api/search", params={"q": "x" * 201}).status_code == 422


def test_nul_byte_query_is_422(client):
    assert client.get("/api/search", params={"q": "ab\x00cd"}).status_code == 422


def test_fts_operators_neutralized_not_500(client):
    # Quoting each token neutralizes FTS5 syntax — these must not error.
    hostile = [
        'foo"bar',
        "AND OR NOT",
        "kon*",
        "(a b)",
        "ssyk:1",
        "-kon",
        '"',
    ]
    for q in hostile:
        r = client.get("/api/search", params={"q": q})
        assert r.status_code == 200, f"{q!r} -> {r.status_code}"


# ── ETag round-trip (the GET-read cache axis) ────────────────────────────────


def test_etag_roundtrip_304(client):
    first = client.get("/api/search", params={"q": "lisa"})
    assert first.status_code == 200
    etag = first.headers["etag"]
    second = client.get(
        "/api/search", params={"q": "lisa"}, headers={"If-None-Match": etag}
    )
    assert second.status_code == 304


def test_etag_covers_query(client):
    # The ETag is body-derived and the query is part of the body, so a different
    # query yields a different ETag (the edge keys by full URL incl. ?q).
    a = client.get("/api/search", params={"q": "lisa"}).headers["etag"]
    b = client.get("/api/search", params={"q": "rams"}).headers["etag"]
    assert a != b


# ── pure helpers ─────────────────────────────────────────────────────────────


def test_has_searchable_token():
    assert _has_searchable_token("inkomst")
    assert _has_searchable_token("Kön")
    assert not _has_searchable_token("")
    assert not _has_searchable_token("   ")
    assert not _has_searchable_token('"" -- ;')


def test_validated_limit_clamps():
    assert _validated_limit(0) == 1
    assert _validated_limit(999) == 50
    assert _validated_limit(10) == 10


def test_limit_param_clamped_end_to_end(client):
    # limit applies PER GROUP; total_count still reflects the full folded count.
    body = client.get("/api/search", params={"q": "kon", "limit": 1}).json()
    for g in body["groups"]:
        assert len(g["results"]) <= 1
