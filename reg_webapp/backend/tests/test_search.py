"""`GET /api/search` — typed FTS result groups (#350).

Runs against the slugged ``catalog_db`` fixture (its FTS indexes are rebuilt in
conftest). Covers: the typed-groups shape + extension contract, register /
variable / classification hits, concept-group folding (#322), the diacritic
parity (å→a) with the SPA filter, the input gates (too-long / NUL / FTS-operator
neutralization), the empty-query degradation, and the ETag round-trip.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


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


def test_empty_cursor_is_actionable_422_without_golden_pin(client):
    response = client.get("/api/search?q=unconfigured&type=register&cursor=")
    assert response.status_code == 422
    assert response.json()["detail"] == (
        "Search cursor must not be empty. Restart the search without cursor."
    )


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


# ── topical ranking vs incidental code labels (Y-18) ─────────────────────────
#
# Driven through the route against the `topical_catalog_db` fixture: the topic
# sits in the scb/rams PURPOSE and in a variable NAME + DEFINITION, and six value
# codes merely BEGIN their labels with it. The text is invented for this fixture,
# not a claim about any real register's terms.


_TOPICAL_REGISTER = "register:scb/rams"
_TOPICAL_VARIABLE = "variable:scb/rams/covidanalys"


@pytest.fixture
def topical_client(topical_catalog_db):
    with TestClient(create_app()) as c:
        yield c


def _row_ids(body: dict, name: str) -> list[str]:
    """One group's rows as ``type:identifier``, in wire order."""
    return [
        f"code:{r['code']}" if r["type"] == "code" else f"{r['type']}:{r.get('fqid')}"
        for r in _group(body, name)["results"]
    ]


@pytest.mark.parametrize(
    ("query", "expected_top"),
    [
        # `covidanalys` is an identifier PREFIX of the variable, so it leads; the
        # register's purpose match follows. Both precede the incidental codes.
        ("covid", [_TOPICAL_VARIABLE, _TOPICAL_REGISTER, "code:C900"]),
        ("covid test", [_TOPICAL_REGISTER, _TOPICAL_VARIABLE, "code:C900"]),
        # Non-prefix label matches — these already led before the label prefix
        # signal was withdrawn, and must keep doing so.
        ("testing", [_TOPICAL_REGISTER, _TOPICAL_VARIABLE, "code:C900"]),
        ("provtagning", [_TOPICAL_REGISTER, _TOPICAL_VARIABLE, "code:C900"]),
    ],
)
def test_topical_top_results_lead_with_register_and_variable(
    topical_client, query, expected_top
):
    body = topical_client.get("/api/search", params={"q": query}).json()
    # The precondition the ranking assertion rests on: ranking cannot promote what
    # retrieval never returned, so a failure HERE is a match gap, not a ranking
    # one. It doubles as the reorder-only check — the codes keep their own pages.
    assert _row_ids(body, "registers") == [_TOPICAL_REGISTER]
    assert _row_ids(body, "variables") == [_TOPICAL_VARIABLE]
    assert _row_ids(body, "classification_codes") == [
        "code:C900",
        "code:C901",
        "code:C902",
    ]
    assert _row_ids(body, "register_value_sets") == [
        "code:L900",
        "code:L901",
        "code:L902",
    ]

    assert _row_ids(body, "top_results") == expected_top


def test_topical_query_keeps_bounded_continuation(topical_client):
    first = topical_client.get(
        "/api/search", params={"q": "covid test", "limit": 1}
    ).json()
    codes = _group(first, "classification_codes")
    assert _row_ids(first, "classification_codes") == ["code:C900"]
    assert codes["has_more"] and codes["next_cursor"]

    second = topical_client.get(
        "/api/search",
        params={
            "q": "covid test",
            "limit": 1,
            "type": "classification_code",
            "cursor": codes["next_cursor"],
        },
    ).json()
    assert _row_ids(second, "classification_codes") == ["code:C901"]


@pytest.mark.parametrize(
    ("query", "expected_first"),
    # The exact code IDENTIFIER and the exact whole LABEL both keep their lead
    # over the objects they pull in — only the label PREFIX signal was withdrawn.
    [("C900", "code:C900"), ("Man", "code:1")],
)
def test_exact_code_match_still_leads_top_results(
    topical_client, query, expected_first
):
    body = topical_client.get("/api/search", params={"q": query}).json()
    assert _row_ids(body, "top_results")[0] == expected_first


def test_exact_variable_identifier_still_resolves_its_variable(topical_client):
    # `CovidAnalys04` is the variable's delivery column: it resolves the variable
    # and nothing else — no incidental code rides along on an identifier query.
    body = topical_client.get("/api/search", params={"q": "CovidAnalys04"}).json()
    assert _row_ids(body, "variables") == [_TOPICAL_VARIABLE]
    assert _row_ids(body, "classification_codes") == []
    assert _row_ids(body, "register_value_sets") == []


@pytest.mark.parametrize("client_fixture", ["client", "topical_client"])
def test_unaffected_query_matches_the_unseeded_catalog(request, client_fixture):
    # The control the topical rows must not disturb: same wire order either way.
    body = (
        request.getfixturevalue(client_fixture)
        .get("/api/search", params={"q": "Kön"})
        .json()
    )
    assert {g["group"]: _row_ids(body, g["group"]) for g in body["groups"]} == {
        "registers": [],
        "variables": ["variable:scb/lisa/kon"],
        "classifications": [],
        "classification_codes": [],
        "register_value_sets": [],
    }
