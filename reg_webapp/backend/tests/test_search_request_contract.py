"""`GET /api/search` request contract against the slugged ``catalog_db`` fixture.

Covers the typed-groups shape, the ``?type=`` scope toggle, cursor and limit
validation, the input gates (empty / whitespace / too-long / NUL / FTS-operator
neutralization) and the ETag round-trip.
"""

from __future__ import annotations

from backend_test_support import search_group as _group

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
