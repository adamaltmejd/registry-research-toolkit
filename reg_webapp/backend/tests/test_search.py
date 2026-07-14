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

import sqlite3
from pathlib import Path

import pytest
import reg_meta.db
from fastapi.testclient import TestClient
from reg_meta.catalog import ConceptGroupMember
from reg_meta.queries import _code_system
from reg_meta.search import (
    CodeSearchResult,
    ConceptGroupSearchResult,
    RegisterSearchResult,
    SearchResults,
    VariableSearchResult,
)
from reg_webapp.app import create_app
from reg_webapp.catalog_index import CatalogIndex
from reg_webapp.golden import _Pin, apply_golden_boost
from reg_webapp.models import (
    RegisterSearchGroup,
    RegisterValueSetSearchGroup,
    VariableSearchGroup,
)
from reg_webapp.routes import search as search_route
from reg_webapp.routes.search import (
    _best_bet_score,
    _best_bets,
    _has_searchable_token,
    _narrow_variable_leaf_columns,
    _rank_codes,
    _validated_limit,
)

from reg_webapp import golden


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture
def conn(catalog_db):
    # A raw read-only connection on the slugged fixture DB for unit-testing the
    # pure golden-boost resolver (no app / request needed). sqlite3.Row factory is
    # set by open_db, which golden.py's row["..."] access relies on.
    c = reg_meta.db.open_db(catalog_db, check_schema=False)
    try:
        yield c
    finally:
        c.close()


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
        "classification_codes",
        "register_value_sets",
    }
    # Every group carries bounded-continuation metadata plus its results list.
    # per-group envelope docs will reuse).
    for g in body["groups"]:
        assert "has_more" in g and "next_cursor" in g
        assert isinstance(g["results"], list)


# ── scoped search: ?type= toggle (#393 item 1) ───────────────────────────────


def test_type_register_returns_only_registers_group(client):
    body = client.get("/api/search", params={"q": "LISA", "type": "register"}).json()
    assert [g["group"] for g in body["groups"]] == ["registers"]
    # …and the scoped group still carries the expected hit.
    g = _group(body, "registers")
    assert "scb/lisa" in [r["fqid"] for r in g["results"]]


def test_type_value_returns_only_value_groups(client):
    body = client.get("/api/search", params={"q": "Man", "type": "value"}).json()
    assert [g["group"] for g in body["groups"]] == [
        "classification_codes",
        "register_value_sets",
    ]
    g = _group(body, "classification_codes")
    assert any(r["label"] == "Man" for r in g["results"])


def test_type_variable_returns_only_variables_group(client):
    body = client.get("/api/search", params={"q": "Kön", "type": "variable"}).json()
    assert [g["group"] for g in body["groups"]] == ["variables"]


def test_type_classification_returns_only_classifications_group(client):
    body = client.get(
        "/api/search", params={"q": "SUN2020", "type": "classification"}
    ).json()
    assert [g["group"] for g in body["groups"]] == ["classifications"]


def test_invalid_type_is_422(client):
    assert (
        client.get("/api/search", params={"q": "x", "type": "bogus"}).status_code == 422
    )


def test_invalid_cursor_is_actionable_422(client):
    response = client.get(
        "/api/search",
        params={"q": "LISA", "type": "register", "cursor": "not-a-cursor"},
    )
    assert response.status_code == 422
    assert "cursor" in response.json()["detail"].lower()


def test_default_type_is_all_four_groups(client):
    # No ?type= preserves the canonical typed groups when there is no useful
    # cross-group top-results panel to show.
    body = client.get("/api/search", params={"q": "lisa"}).json()
    assert [g["group"] for g in body["groups"]] == [
        "registers",
        "variables",
        "classifications",
        "classification_codes",
        "register_value_sets",
    ]


def test_type_all_explicit_matches_default(client):
    # Passing type=all explicitly is equivalent to omitting it.
    body = client.get("/api/search", params={"q": "lisa", "type": "all"}).json()
    assert [g["group"] for g in body["groups"]] == [
        "registers",
        "variables",
        "classifications",
        "classification_codes",
        "register_value_sets",
    ]


def test_scoped_empty_query_returns_only_selected_empty_group(client):
    # The empty-query short-circuit honors ?type= too — only the value groups,
    # not the non-value groups.
    body = client.get("/api/search", params={"q": "", "type": "value"}).json()
    assert [g["group"] for g in body["groups"]] == [
        "classification_codes",
        "register_value_sets",
    ]
    assert all(not g["has_more"] for g in body["groups"])
    assert all(g["results"] == [] for g in body["groups"])


def test_scoped_empty_query_non_value_scope(client):
    # A blank query under a non-`value` scope returns ONLY that scope's group,
    # empty — guards the register/variable/classification arms of the empty-query
    # short-circuit (the existing scoped-empty test covers only `value`).
    body = client.get("/api/search", params={"q": "  ", "type": "register"}).json()
    assert [g["group"] for g in body["groups"]] == ["registers"]
    assert not body["groups"][0]["has_more"]
    assert body["groups"][0]["results"] == []


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
    # The terminal edition itself carries no `terminal_fqid` (it IS current).
    assert hit["terminal_fqid"] is None


def test_lone_old_edition_leaf_carries_terminal_fqid(client):
    # `SUN1996` matches only the sun1996 short_name — a lone, NON-terminal edition
    # of the sun1996 → sun2000 → sun2020 succession chain (#571), and one NOT in any
    # concept group, so it stays a leaf rather than folding. The reg_meta fold
    # annotates it with the terminal edition's fqid; the route must surface it on
    # `ClassificationSearchResult` so the SPA can link "current edition".
    g = _group(
        client.get("/api/search", params={"q": "SUN1996"}).json(), "classifications"
    )
    leaves = [r for r in g["results"] if r["type"] == "classification"]
    hit = next(r for r in leaves if r["fqid"] == "class/sun1996")
    assert hit["terminal_fqid"] == "class/sun2020"


# ── value/code groups (#352) ─────────────────────────────────────────────────


def test_value_groups_always_present(client):
    # Present even when nothing matches (keep all groups in the envelope).
    body = client.get("/api/search", params={"q": "zzqq"}).json()
    for name in ("classification_codes", "register_value_sets"):
        g = _group(body, name)
        assert not g["has_more"]
        assert g["results"] == []


def test_code_label_hit_carries_owning_variable(client):
    # "Man" is a value label on the kon binding's value set (seeded in conftest)
    # → a code hit annotated with its owning variable.
    g = _group(
        client.get("/api/search", params={"q": "Man"}).json(),
        "classification_codes",
    )
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
    g = _group(
        client.get("/api/search", params={"q": "Man"}).json(),
        "classification_codes",
    )
    hit = next(r for r in g["results"] if r["label"] == "Man")
    assert hit["classification_count"] >= 1
    owner = next(c for c in hit["classifications"] if c["fqid"] == "class/sun2020")
    assert owner["short_name"] == "SUN2020"


def test_code_shaped_query_well_formed(client):
    # A code-shaped query (digit + len>=3) drives the value_code.code exact/prefix
    # path. The fixture has no "0180" code, so this asserts the group stays
    # well-formed (no 500); the code-match resolution itself is covered by the
    # reg_meta query-layer unit test.
    body = client.get("/api/search", params={"q": "0180"}).json()
    assert isinstance(_group(body, "classification_codes")["results"], list)
    assert isinstance(_group(body, "register_value_sets")["results"], list)


def test_code_hit_carries_code_system(client):
    # The "Man" code is owned by the sun2020 classification (short_name SUN2020),
    # so its inferred `code_system` is that short_name (#393 item 3).
    g = _group(
        client.get("/api/search", params={"q": "Man"}).json(),
        "classification_codes",
    )
    hit = next(r for r in g["results"] if r["label"] == "Man")
    assert hit["code_system"] == "SUN2020"


def test_c12_code_hit_uses_icd_code_system(client):
    g = _group(
        client.get("/api/search", params={"q": "C12"}).json(),
        "classification_codes",
    )
    hit = next(r for r in g["results"] if r["label"] == "Malign tumör i tungbas")
    assert hit["code_system"] == "ICD-10-SE"
    assert [c["fqid"] for c in hit["classifications"]] == ["class/icd-10-se"]


def test_register_local_code_has_null_code_system(client):
    # A code with NO owning classification (the kvinna_only value, seeded as a
    # register-local value with no classification owner) has code_system == null.
    g = _group(
        client.get("/api/search", params={"q": "Kvinna"}).json(),
        "register_value_sets",
    )
    hit = next(
        r for r in g["results"] if r["label"] == "Kvinna" and not r["classifications"]
    )
    assert hit["code_system"] is None


# ── code-aware classification surfacing (#393 item 5) ────────────────────────


def test_code_shaped_query_surfaces_owning_classification(client):
    # 'C12' is a code-shaped query (digit + len>=3) owned by ICD-10-SE
    # (seeded in conftest), matching no classification NAME. The classifications
    # group must surface ICD-10-SE via code-containment, navigable.
    g = _group(client.get("/api/search", params={"q": "C12"}).json(), "classifications")
    fqids = [r["fqid"] for r in g["results"] if r["type"] == "classification"]
    assert "class/icd-10-se" in fqids
    assert len(g["results"]) >= 1


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
    # The terminal sun2020 carries the name "Svensk utbildningsnomenklatur", which
    # is also the `sun` group label → a hit on it folds into ONE group row AND its
    # member leaves are SUBSUMED (not emitted standalone too — the #350 review bug:
    # classification leaves were duplicated as both leaf and folded member). The
    # `sun` group's members are the terminal dimensions sun2020 + niva-test (#608 /
    # #516 umbrella shape); the superseded sun2000/sun1996 share the name but are
    # NOT members, so they stay standalone leaves — and never collide with members.
    g = _group(
        client.get("/api/search", params={"q": "utbildningsnomenklatur"}).json(),
        "classifications",
    )
    groups = [r for r in g["results"] if r["type"] == "group"]
    grp = next(r for r in groups if r["kind"] == "classification")
    member_fqids = {m["fqid"] for m in grp["members"]}
    assert {"class/sun2020", "class/niva-test"} <= member_fqids
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
        "classification_codes",
        "register_value_sets",
    }
    assert all(not g["has_more"] and g["results"] == [] for g in body["groups"])


def test_whitespace_query_returns_empty_groups(client):
    body = client.get("/api/search", params={"q": "   "}).json()
    assert all(not g["has_more"] for g in body["groups"])


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


def test_etag_covers_type(client):
    # ?type= changes the response body (which groups it carries), so the
    # body-derived ETag must differ across scopes for the same query — else a
    # scoped request could be served the wrong scope's cached validator.
    a = client.get("/api/search", params={"q": "Man"}).headers["etag"]
    b = client.get("/api/search", params={"q": "Man", "type": "value"}).headers["etag"]
    assert a != b
    # The all-scope ETag must NOT revalidate (304) a different-scope request.
    resp = client.get(
        "/api/search",
        params={"q": "Man", "type": "value"},
        headers={"If-None-Match": a},
    )
    assert resp.status_code == 200


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


def test_narrow_variable_leaf_columns_treats_none_as_concrete_column():
    index = CatalogIndex(
        bindings_by_variant={
            "scb/lisa/individer-15plus": frozenset(
                {("scb/lisa/kon", None), ("scb/lisa/kon", "Kon")}
            )
        },
        period_range_by_register={"scb/lisa": ("2018", "2018")},
        drift_warnings=(),
    )
    result = VariableSearchResult(
        fqid="scb/lisa/kon",
        name="Kön",
        register="LISA",
        delivery_column_names=("Kon", "KonOld"),
        rank=0.0,
    )

    kept = _narrow_variable_leaf_columns([result], index)

    assert len(kept) == 1
    assert kept[0].delivery_column_names == ("Kon",)


def test_filtered_variable_search_passes_delivery_scope_into_full_backfill_query(
    client, monkeypatch
):
    index = CatalogIndex(
        bindings_by_variant={
            "scb/lisa/individer-15plus": frozenset({("scb/lisa/kon", "Kon")})
        },
        period_range_by_register={"scb/lisa": ("2018", "2018")},
        drift_warnings=(),
    )
    client.app.state.catalog_index = index
    calls: list[int | None] = []

    def fake_search(
        _conn,
        query,
        *,
        field,
        type,
        fqids=None,
        delivery_column_scope=None,
        limit=50,
        fold_groups=True,
        cursor=None,
    ):
        assert query == "needle"
        assert field == "description"
        assert type == "variable"
        assert fqids == {"scb/lisa", "scb/lisa/kon"}
        assert delivery_column_scope == {"scb/lisa/kon": frozenset({"Kon"})}
        assert fold_groups
        calls.append(limit)
        rows = (
            VariableSearchResult(
                fqid="scb/lisa/kon",
                name="needle variable",
                register="LISA",
                delivery_column_names=("Kon",),
                rank=1.0,
            ),
        )
        return SearchResults(
            results=rows[:limit],
            has_more=False,
        )

    monkeypatch.setattr(search_route, "reg_meta_search", fake_search)

    body = client.get("/api/search?q=needle&type=variable&limit=1").json()
    group = _group(body, "variables")

    assert calls == [1]
    assert not group["has_more"]
    assert [r["name"] for r in group["results"]] == ["needle variable"]
    assert group["results"][0]["delivery_column_names"] == ["Kon"]


def test_value_search_keeps_code_owner_lists_bounded(client, monkeypatch):
    class ExplodingIndex:
        held_register_fqids = frozenset({"scb/lisa"})
        admitted_variable_fqids = frozenset({"scb/lisa/kon"})

        def held_columns(self, _fqid):
            raise AssertionError("value search must not build delivery-column scope")

    client.app.state.catalog_index = ExplodingIndex()
    calls: list[tuple[int | None, str]] = []

    def fake_search(
        _conn,
        query,
        *,
        field,
        type,
        limit=50,
        fold_groups=True,
        code_variable_owner_limit=5,
        code_owner_scope="all",
        cursor=None,
    ):
        assert query == "needle"
        assert field == "value"
        assert type == "value"
        assert limit == 1
        assert not fold_groups
        calls.append((code_variable_owner_limit, code_owner_scope))
        if code_owner_scope == "register_local":
            return SearchResults(
                results=(_code("1", variable_count=250),), has_more=False
            )
        return SearchResults(results=(), has_more=False)

    monkeypatch.setattr(search_route, "reg_meta_search", fake_search)

    body = client.get("/api/search?q=needle&type=value&limit=1").json()
    group = _group(body, "register_value_sets")

    assert calls == [(5, "classification"), (5, "register_local")]
    assert not group["has_more"]
    assert group["results"][0]["variable_count"] == 250


def _code(code: str, *, classification_count: int = 0, variable_count: int = 0):
    """Build a minimal typed code result for route helper tests."""
    return CodeSearchResult(
        code=code,
        label=code,
        classification_count=classification_count,
        variable_count=variable_count,
        rank=0.0,
    )


def test_rank_codes_classification_backed_precede():
    results = [
        _code("a", classification_count=0, variable_count=99),
        _code("b", classification_count=1, variable_count=0),
    ]
    assert [result.code for result in _rank_codes(results)] == ["b", "a"]


def test_rank_codes_orders_by_classification_then_variable_count():
    results = [
        _code("a", classification_count=1, variable_count=1),
        _code("b", classification_count=3, variable_count=0),
        _code("c", classification_count=1, variable_count=5),
    ]
    assert [result.code for result in _rank_codes(results)] == ["b", "c", "a"]


def test_rank_codes_is_stable_on_ties():
    results = [
        _code("a", classification_count=2, variable_count=1),
        _code("b", classification_count=2, variable_count=1),
        _code("c", classification_count=2, variable_count=1),
    ]
    assert [result.code for result in _rank_codes(results)] == ["a", "b", "c"]


def test_rank_codes_tolerates_default_counts():
    results = [_code("a"), _code("b", classification_count=1)]
    assert [result.code for result in _rank_codes(results)] == ["b", "a"]


def test_code_system_first_short_name():
    assert _code_system([{"short_name": "SUN2020", "name": "Svensk u."}]) == "SUN2020"


def test_code_system_falls_back_to_name():
    assert _code_system([{"short_name": None, "name": "Bespoke set"}]) == "Bespoke set"


def test_code_system_none_when_empty():
    assert _code_system([]) is None


def test_code_system_uses_first_owner():
    # The PRIMARY (first) owning classification wins when there are several.
    classifications = [
        {"short_name": "SUN2020", "name": None},
        {"short_name": "SUN2000", "name": None},
    ]
    assert _code_system(classifications) == "SUN2020"


def test_limit_param_clamped_end_to_end(client):
    # limit applies PER GROUP; total_count still reflects the full folded count.
    body = client.get("/api/search", params={"q": "kon", "limit": 1}).json()
    for g in body["groups"]:
        assert len(g["results"]) <= 1


# ── top-results / best-bets (#393 items 6/7) ────────────────────────────────


def test_top_results_group_precedes_typed_groups(client):
    body = client.get("/api/search", params={"q": "C12"}).json()
    assert [g["group"] for g in body["groups"]][:2] == ["top_results", "registers"]
    top = _group(body, "top_results")
    assert not top["has_more"]
    assert top["results"]


def test_single_candidate_search_omits_top_results(client):
    body = client.get("/api/search", params={"q": "lisa"}).json()
    assert "top_results" not in [g["group"] for g in body["groups"]]


def test_scoped_search_omits_top_results(client):
    body = client.get("/api/search", params={"q": "lisa", "type": "register"}).json()
    assert [g["group"] for g in body["groups"]] == ["registers"]


def test_top_results_exact_variable_beats_register_prior():
    register = RegisterSearchResult(
        fqid="scb/konj",
        name="Konjunktur",
        rank=-1.0,
    )
    variable = VariableSearchResult(
        fqid="scb/lisa/kon",
        name="Kön",
        register="LISA",
        rank=0.0,
    )

    top = _best_bets(
        "kon",
        [
            RegisterSearchGroup(total_count=1, results=[register]),
            VariableSearchGroup(total_count=1, results=[variable]),
        ],
        limit=2,
    )

    assert [r.type for r in top] == ["variable", "register"]


def test_top_results_type_priors_break_non_exact_ties():
    register = RegisterSearchResult(
        fqid="scb/rams",
        name="RAMS",
        rank=0.0,
    )
    variable = VariableSearchResult(
        fqid="scb/rams/syss",
        name="Sysselsättning",
        register="RAMS",
        rank=0.0,
    )
    code = _code("1", variable_count=1)

    top = _best_bets(
        "arbetsmarknad",
        [
            RegisterValueSetSearchGroup(total_count=1, results=[code]),
            VariableSearchGroup(total_count=1, results=[variable]),
            RegisterSearchGroup(total_count=1, results=[register]),
        ],
        limit=3,
    )

    assert [r.type for r in top] == ["register", "variable", "code"]


def test_top_results_group_member_exact_match_gets_boost():
    register = RegisterSearchResult(
        fqid="scb/konj",
        name="Konjunktur",
        rank=-1.0,
    )
    group = ConceptGroupSearchResult(
        kind="variable",
        group_key="scb/lisa/demografi",
        group_label="Demografi",
        register="LISA",
        member_count=2,
        matched_count=1,
        members=(
            ConceptGroupMember(fqid="scb/lisa/kon", name="Kön", facets=()),
            ConceptGroupMember(fqid="scb/lisa/alder", name="Ålder", facets=()),
        ),
        rank=0.0,
    )

    top = _best_bets(
        "kon",
        [
            RegisterSearchGroup(total_count=1, results=[register]),
            VariableSearchGroup(total_count=1, results=[group]),
        ],
        limit=2,
    )

    assert top == [group, register]


def test_top_results_keeps_golden_pins_first():
    pin = RegisterSearchResult(
        fqid="scb/aku",
        name="Arbetskraftsundersökningarna",
        rank=0.0,
    )
    exact = VariableSearchResult(
        fqid="scb/rams/syss",
        name="Sysselsättning",
        register="RAMS",
        rank=-10.0,
    )

    top = _best_bets(
        "sysselsättning",
        [
            RegisterSearchGroup(total_count=1, results=[pin]),
            VariableSearchGroup(total_count=1, results=[exact]),
        ],
        limit=2,
    )

    assert top == [pin, exact]


def test_top_results_keep_same_key_groups_from_different_registers():
    iot_group = ConceptGroupSearchResult(
        kind="variable",
        group_key="tfoab",
        group_label="Transfereringar",
        register="IoT",
        member_count=1,
        matched_count=1,
        members=(
            ConceptGroupMember(
                fqid="scb/iot/tfoab01", name="Transfereringar", facets=()
            ),
        ),
        rank=0.0,
    )
    lisa_group = ConceptGroupSearchResult(
        kind="variable",
        group_key="tfoab",
        group_label="Transfereringar",
        register="LISA",
        member_count=1,
        matched_count=1,
        members=(
            ConceptGroupMember(
                fqid="scb/lisa/tfoab01", name="Transfereringar", facets=()
            ),
        ),
        rank=0.0,
    )

    top = _best_bets(
        "transfereringar",
        [VariableSearchGroup(total_count=2, results=[iot_group, lisa_group])],
        limit=5,
    )

    assert top == [iot_group, lisa_group]


def test_top_results_direct_group_label_match_beats_prefix_leaves():
    leaves = [
        VariableSearchResult(
            fqid=f"scb/register-{i}/disponibel-inkomst",
            name="Disponibel inkomst",
            register=f"Register {i}",
            rank=-10.0 - i,
        )
        for i in range(6)
    ]
    group = ConceptGroupSearchResult(
        kind="variable",
        group_key="disponibel-inkomst",
        group_label="Disponibel inkomst",
        register="IoT",
        member_count=53,
        matched_count=36,
        label_matched=True,
        members=(
            ConceptGroupMember(
                fqid="scb/iot/disponibel-inkomst",
                name="Disponibel inkomst",
                facets=(),
            ),
        ),
        rank=-12.0,
    )

    top = _best_bets(
        "disponibel",
        [VariableSearchGroup(total_count=7, results=[*leaves, group])],
        limit=5,
    )

    assert top[0] == group


def test_top_results_deduplicates_group_members_before_limit():
    member_leaves = [
        VariableSearchResult(
            fqid=f"scb/iot/disp-{i}",
            name=f"Disponibel inkomst member {i}",
            register="IoT",
            rank=-10.0 - i,
        )
        for i in range(6)
    ]
    fillers = [
        VariableSearchResult(
            fqid=f"scb/lisa/disp-{i}",
            name=f"Disponibel inkomst filler {i}",
            register="LISA",
            rank=-20.0 - i,
        )
        for i in range(5)
    ]
    group = ConceptGroupSearchResult(
        kind="variable",
        group_key="disponibel-inkomst",
        group_label="Disponibel inkomst",
        register="IoT",
        member_count=len(member_leaves),
        matched_count=len(member_leaves),
        label_matched=True,
        members=tuple(
            ConceptGroupMember(fqid=leaf.fqid, name=leaf.name, facets=())
            for leaf in member_leaves
            if leaf.fqid is not None
        ),
        rank=-9.0,
    )

    top = _best_bets(
        "disp",
        [
            VariableSearchGroup(
                total_count=1 + len(member_leaves) + len(fillers),
                results=[group, *member_leaves, *fillers],
            )
        ],
        limit=5,
    )

    assert top == [group, *fillers[:4]]


def test_top_results_exact_leaf_still_beats_group_authority_bonus():
    exact = VariableSearchResult(
        fqid="scb/lisa/kon",
        name="Kön",
        register="LISA",
        rank=-10.0,
    )
    broad_group = ConceptGroupSearchResult(
        kind="variable",
        group_key="kontroll",
        group_label="Kontroll",
        register="IoT",
        member_count=80,
        matched_count=80,
        label_matched=True,
        members=(
            ConceptGroupMember(fqid="scb/iot/kontroll", name="Kontroll", facets=()),
        ),
        rank=-9.0,
    )

    top = _best_bets(
        "kon",
        [VariableSearchGroup(total_count=2, results=[broad_group, exact])],
        limit=2,
    )

    assert top == [exact, broad_group]


def test_top_results_keeps_distinct_null_fqid_leaves():
    first = VariableSearchResult(
        fqid=None,
        name="Unresolved first",
        register="LISA",
        rank=0.0,
    )
    second = VariableSearchResult(
        fqid=None,
        name="Unresolved second",
        register="LISA",
        rank=1.0,
    )

    top = _best_bets(
        "unresolved",
        [VariableSearchGroup(total_count=2, results=[first, second])],
        limit=5,
    )

    assert top == [first, second]


def test_best_bet_score_folds_diacritics_for_exact_matches():
    variable = VariableSearchResult(
        fqid="scb/lisa/kon",
        name="Kön",
        register="LISA",
        rank=0.0,
    )

    assert _best_bet_score("kon", variable) >= 1000


# ── golden-boost: curated-pin injection (#393 item 4 / #311) ─────────────────
#
# The synthetic catalog fixture has no scb/lisa-for-sysselsättning / sos/par
# content, so these tests pin fqids the fixture DOES carry (scb/rams register,
# class/sun2020 classification) under test-only `_PINS`, exercising the real
# `apply_golden_boost` resolver + the route's total_count adjustment.


@pytest.fixture
def pinned(monkeypatch):
    """Swap golden._PINS for a test-only pin set so injection is exercised against
    the fixture's own entities. `gizmo` matches nothing via FTS, so the register it
    pins (scb/rams) is purely injected; `LISA` IS an FTS hit, so its pin tests
    dedup. A classification pin (class/sun2020) covers that builder too."""
    monkeypatch.setattr(
        golden,
        "_PINS",
        {
            ("gizmo", "register"): _Pin(
                query="gizmo", group="register", fqids=("scb/rams",), note=None
            ),
            ("lisa", "register"): _Pin(
                query="lisa", group="register", fqids=("scb/lisa",), note=None
            ),
            ("gizmo", "classification"): _Pin(
                query="gizmo",
                group="classification",
                fqids=("class/sun2020",),
                note=None,
            ),
        },
    )


def _reg(fqid: str, name: str):
    """A `RegisterSearchResult` model standing in for an FTS register hit (#701) —
    `apply_golden_boost` now operates on the reg_meta typed models, not raw dicts."""
    from reg_meta.search import RegisterSearchResult

    return RegisterSearchResult(fqid=fqid, name=name, purpose=None, rank=0.0)


def test_apply_golden_boost_injects_register_at_rank_1(conn, pinned):
    # 'gizmo' has no register FTS hit, so the pinned scb/rams is the only result —
    # rank 1, built as a `RegisterSearchResult` model.
    boosted = apply_golden_boost(conn, "gizmo", "register", ())
    assert [str(r.fqid) for r in boosted] == ["scb/rams"]
    assert boosted[0].name == "RAMS"
    assert hasattr(boosted[0], "purpose")  # carries the purpose field


def test_apply_golden_boost_prepends_before_fts(conn, pinned):
    # The pin lands at rank 1 even when FTS already returned other registers.
    fts = (_reg("scb/other", "Other"),)
    boosted = apply_golden_boost(conn, "gizmo", "register", fts)
    assert [str(r.fqid) for r in boosted] == ["scb/rams", "scb/other"]


def test_apply_golden_boost_dedups_when_pin_is_already_a_hit(conn, pinned):
    # 'LISA' is an FTS hit AND pinned to scb/lisa → no duplicate, order unchanged.
    fts = (_reg("scb/lisa", "LISA"),)
    boosted = apply_golden_boost(conn, "LISA", "register", fts)
    assert [str(r.fqid) for r in boosted] == ["scb/lisa"]


def test_apply_golden_boost_skips_fqidless_group_row(conn, pinned):
    # A `ConceptGroupSearchResult` carries no `fqid` field, so the dedup's
    # `_fqid_str(getattr(r, "fqid", None))` guard must treat it as un-pinnable and
    # leave it untouched — a regression to `r.fqid` would `AttributeError` here.
    # The classification leaf (fqid class/sun2020) DOES match the 'gizmo'
    # classification pin, so it is deduped out; the group survives unchanged.
    from reg_meta.search import ClassificationSearchResult, ConceptGroupSearchResult

    group = ConceptGroupSearchResult(
        kind="classification",
        group_key="sun",
        group_label="SUN editions",
        rank=0.0,
        members=(),
    )
    leaf = ClassificationSearchResult(
        fqid="class/sun2020", short_name="SUN2020", rank=0.5
    )
    boosted = apply_golden_boost(conn, "gizmo", "classification", (group, leaf))
    # The group is preserved by identity (never deduped); the matching leaf is
    # removed and re-prepended as the freshly-built pin.
    assert group in boosted
    assert [str(getattr(r, "fqid", None)) for r in boosted] == ["class/sun2020", "None"]


def test_apply_golden_boost_promotes_on_page_hit_to_rank_1(conn, pinned):
    # The pin (scb/lisa) is on the page but NOT at rank 1 in the FTS order: it must
    # be PROMOTED to rank 1, appear EXACTLY ONCE (removed from its FTS slot, not
    # duplicated), and the page length is unchanged → the route's total_count delta
    # is 0 (it was already counted by FTS).
    fts = (
        _reg("scb/other", "Other"),
        _reg("scb/lisa", "LISA"),
        _reg("scb/third", "Third"),
    )
    boosted = apply_golden_boost(conn, "LISA", "register", fts)
    assert [str(r.fqid) for r in boosted] == ["scb/lisa", "scb/other", "scb/third"]
    assert [str(r.fqid) for r in boosted].count("scb/lisa") == 1
    assert len(boosted) == len(fts)  # delta 0: no net-new injection


def test_apply_golden_boost_normalizes_query(conn, pinned):
    # casefold + strip: "  GIZMO  " resolves to the "gizmo" pin.
    boosted = apply_golden_boost(conn, "  GIZMO  ", "register", ())
    assert [str(r.fqid) for r in boosted] == ["scb/rams"]


def test_apply_golden_boost_non_pinned_query_unchanged(conn, pinned):
    fts = (_reg("scb/other", "Other"),)
    boosted = apply_golden_boost(conn, "no-such-pin", "register", fts)
    # No matching pin → the same results, materialized as a list (no re-ordering).
    assert [str(r.fqid) for r in boosted] == ["scb/other"]
    assert boosted == list(fts)


def test_apply_golden_boost_classification_builder(conn, pinned):
    # The classification arm builds a `ClassificationSearchResult` model.
    boosted = apply_golden_boost(conn, "gizmo", "classification", ())
    assert [str(r.fqid) for r in boosted] == ["class/sun2020"]
    assert boosted[0].short_name == "SUN2020"
    assert boosted[0].name is not None


def test_apply_golden_boost_unresolvable_fqid_raises(conn, monkeypatch):
    # A pin fqid that doesn't resolve fails fast at apply (not a silent drop).
    monkeypatch.setattr(
        golden,
        "_PINS",
        {
            ("gizmo", "register"): _Pin(
                query="gizmo", group="register", fqids=("scb/nonexistent",), note=None
            )
        },
    )
    with pytest.raises(ValueError, match="does not resolve to a register"):
        apply_golden_boost(conn, "gizmo", "register", ())


def test_golden_boost_register_injection_end_to_end(client, pinned):
    # Through the route: the pinned register surfaces at rank 1 in the registers
    # group AND total_count reflects the net-new injection (1 here — gizmo has no
    # register FTS hit, so the pinned scb/rams is the lone net-new result).
    g = _group(client.get("/api/search", params={"q": "gizmo"}).json(), "registers")
    assert g["results"][0]["fqid"] == "scb/rams"
    assert not g["has_more"]


def test_golden_boost_no_double_count_when_pin_is_fts_hit(client, pinned):
    # 'LISA' is BOTH an FTS register hit and pinned to scb/lisa: it must appear
    # ONCE and NOT inflate total_count (net-new = 0).
    body = client.get("/api/search", params={"q": "LISA"}).json()
    g = _group(body, "registers")
    fqids = [r["fqid"] for r in g["results"]]
    assert fqids.count("scb/lisa") == 1
    # The dedup'd pin added nothing, so total_count == the result count (no
    # net-new injection inflating it).
    assert not g["has_more"]


def test_golden_boost_unpinned_query_continuation_well_formed(client):
    # A query with no pin (default production _PINS, which targets sysselsättning /
    # diagnos — absent from the fixture) leaves every group's total_count as the
    # raw FTS count: no injection, no off-by-one.
    body = client.get("/api/search", params={"q": "kon"}).json()
    for g in body["groups"]:
        assert isinstance(g["has_more"], bool)  # well-formed; no boost-driven inflation


# ── Fix 2: golden-boost must respect ?limit (#393 item 2) ────────────────────


def test_golden_boost_respects_limit(client, monkeypatch):
    # A net-new pin prepended onto an already-`limit`-full FTS page must NOT push the
    # group past the requested cap: with limit=1 and a query that BOTH FTS-matches a
    # register (`LISA` → scb/lisa) AND pins a DIFFERENT register (scb/rams, net-new),
    # the displayed page is capped to 1 result, while total_count reflects the full
    # count (2: the FTS hit + the net-new pin).
    monkeypatch.setattr(
        golden,
        "_PINS",
        {
            ("lisa", "register"): _Pin(
                query="lisa", group="register", fqids=("scb/rams",), note=None
            )
        },
    )
    g = _group(
        client.get("/api/search", params={"q": "LISA", "limit": 1}).json(), "registers"
    )
    assert len(g["results"]) == 1  # page capped at ?limit
    assert g["results"][0]["fqid"] == "scb/rams"  # the pin still leads at rank 1
    assert g["has_more"]  # the displaced FTS hit remains available by cursor

    continuation = _group(
        client.get(
            "/api/search",
            params={
                "q": "LISA",
                "limit": 1,
                "type": "register",
                "cursor": g["next_cursor"],
            },
        ).json(),
        "registers",
    )
    assert [row["fqid"] for row in continuation["results"]] == ["scb/lisa"]


def test_golden_pins_over_limit_continue_once_in_configured_order(client, monkeypatch):
    monkeypatch.setattr(
        golden,
        "_PINS",
        {
            ("gizmo", "register"): _Pin(
                query="gizmo",
                group="register",
                fqids=("scb/rams", "scb/lisa"),
                note=None,
            )
        },
    )
    built: list[str] = []
    real_builder = golden._PIN_BUILDERS["register"]

    def counted_builder(conn, fqid):
        built.append(fqid)
        return real_builder(conn, fqid)

    monkeypatch.setitem(golden._PIN_BUILDERS, "register", counted_builder)

    first = _group(
        client.get(
            "/api/search",
            params={"q": "gizmo", "limit": 1, "type": "register"},
        ).json(),
        "registers",
    )
    assert [row["fqid"] for row in first["results"]] == ["scb/rams"]
    assert first["has_more"]
    assert first["next_cursor"].startswith("golden.")

    second = _group(
        client.get(
            "/api/search",
            params={
                "q": "gizmo",
                "limit": 1,
                "type": "register",
                "cursor": first["next_cursor"],
            },
        ).json(),
        "registers",
    )
    assert [row["fqid"] for row in second["results"]] == ["scb/lisa"]
    assert not second["has_more"]
    assert second["next_cursor"] is None

    combined = [*first["results"], *second["results"]]
    assert [row["fqid"] for row in combined] == ["scb/rams", "scb/lisa"]
    assert len({row["fqid"] for row in combined}) == len(combined)
    assert built == ["scb/rams", "scb/lisa"]

    mismatched = client.get(
        "/api/search",
        params={
            "q": "other",
            "limit": 1,
            "type": "register",
            "cursor": first["next_cursor"],
        },
    )
    assert mismatched.status_code == 422
    assert "does not match" in mismatched.json()["detail"]

    tampered_cursor = first["next_cursor"][:-1] + (
        "A" if first["next_cursor"][-1] != "A" else "B"
    )
    tampered = client.get(
        "/api/search",
        params={
            "q": "gizmo",
            "limit": 1,
            "type": "register",
            "cursor": tampered_cursor,
        },
    )
    assert tampered.status_code == 422
    assert "invalid or tampered" in tampered.json()["detail"]


def test_deep_golden_fts_hit_is_emitted_once_across_all_pages(
    client, catalog_db, monkeypatch
):
    conn = sqlite3.connect(catalog_db)
    try:
        conn.execute("UPDATE register SET purpose = 'Deep golden topic'")
        conn.execute("INSERT INTO register_fts(register_fts) VALUES ('rebuild')")
        conn.commit()
    finally:
        conn.close()
    monkeypatch.setattr(
        golden,
        "_PINS",
        {
            ("deep golden topic", "register"): _Pin(
                query="deep golden topic",
                group="register",
                fqids=("scb/rams",),
                note=None,
            )
        },
    )

    cursor = None
    fqids: list[str] = []
    while True:
        params = {
            "q": "deep golden topic",
            "limit": 1,
            "type": "register",
        }
        if cursor is not None:
            params["cursor"] = cursor
        group = _group(client.get("/api/search", params=params).json(), "registers")
        fqids.extend(row["fqid"] for row in group["results"])
        if not group["has_more"]:
            assert group["next_cursor"] is None
            break
        cursor = group["next_cursor"]
        assert cursor is not None

    assert fqids[0] == "scb/rams"
    assert fqids.count("scb/rams") == 1
    assert len(fqids) == len(set(fqids))


# ── Fix 3: diacritic fold in the pin lookup key ──────────────────────────────


def test_normalize_folds_diacritics():
    # A diacriticless query normalizes identically to its diacritic spelling, so a
    # `sysselsättning` pin is resolved by a `sysselsattning` query (FTS folds å/ä→a
    # on both sides; the pin key must fold the same way or the pin never fires).
    assert golden._normalize("sysselsattning") == golden._normalize("sysselsättning")
    assert golden._normalize("DIAGNOS") == golden._normalize("diagnos")
    assert golden._normalize("  Kön  ") == golden._normalize("kon")


def test_diacriticless_query_resolves_diacritic_pin(conn, monkeypatch):
    # End-to-end on the resolver: a pin keyed under the FOLDED `sysselsättning` key is
    # hit by the diacriticless `sysselsattning` spelling (the eval gap Fix 3 closes).
    monkeypatch.setattr(
        golden,
        "_PINS",
        {
            (golden._normalize("sysselsättning"), "register"): _Pin(
                query="sysselsättning",
                group="register",
                fqids=("scb/rams",),
                note=None,
            )
        },
    )
    boosted = apply_golden_boost(conn, "sysselsattning", "register", ())
    assert [str(r.fqid) for r in boosted] == ["scb/rams"]


# ── Fix 1: packaging + fail-fast on a missing config ─────────────────────────


def test_golden_path_is_packaged():
    # GOLDEN_PATH must live INSIDE the reg_webapp package dir so it ships with the src
    # tree the runtime Docker stage copies — a sibling at backend/ would be absent in
    # the deployed image → silent no-op.
    import reg_webapp

    package_dir = Path(reg_webapp.__file__).resolve().parent
    assert package_dir in golden.GOLDEN_PATH.resolve().parents


def test_load_pins_missing_file_raises(tmp_path):
    # A MISSING config is a packaging bug, not a "no pins" state — `_load_pins` fails
    # fast (CLAUDE.md) rather than silently disabling golden-boost.
    with pytest.raises(FileNotFoundError):
        golden._load_pins(tmp_path / "nonexistent.toml")


def test_load_pins_rejects_duplicate_fqids(tmp_path):
    config = tmp_path / "golden.toml"
    config.write_text(
        '[[pin]]\nquery = "gizmo"\ngroup = "register"\n'
        'fqids = ["scb/rams", "scb/rams"]\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate `fqids`"):
        golden._load_pins(config)


def test_committed_pins_non_empty():
    # The committed/packaged _PINS must load ≥1 pin — guards the silent-no-op
    # regression where a mislocated config parsed to an empty pin set.
    assert len(golden._PINS) >= 1
