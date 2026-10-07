"""`GET /api/search` request contract against the slugged ``catalog_db`` fixture.

Covers the ``?type=`` scope toggle's edges, the empty-cursor refusal and the
input gates (scoped blank / too-long / NUL / FTS-operator neutralization). The
typed-group shape, per-type scoping, cursor tampering and the limit clamp are
pinned by ``conformance/cases/http_search``; the ETag round-trip by
``test_etag_middleware.py``.
"""

from __future__ import annotations

# ── scoped search: ?type= toggle (#393 item 1) ───────────────────────────────


def test_invalid_type_is_422(client):
    assert (
        client.get("/api/search", params={"q": "x", "type": "bogus"}).status_code == 422
    )


def test_empty_cursor_is_actionable_422_without_golden_pin(client):
    response = client.get("/api/search?q=unconfigured&type=register&cursor=")
    assert response.status_code == 422
    assert response.json()["detail"] == (
        "Search cursor must not be empty. Restart the search without cursor."
    )


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


# ── input gates + degradation ────────────────────────────────────────────────


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
