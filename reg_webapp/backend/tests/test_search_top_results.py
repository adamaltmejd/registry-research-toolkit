"""`GET /api/search` top-results group against ``catalog_db`` and ``topical_catalog_db``.

Covers when the top-results group appears and its ranking of registers,
variables and codes for topical queries (Y-18).
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
