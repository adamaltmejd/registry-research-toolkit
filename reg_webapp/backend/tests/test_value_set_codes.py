"""`GET /api/value-sets/{value_set_id}/codes` — the bounded code-list read (Y-46).

The binding leaf carries each state's coding IDENTITY and a cardinality-independent
summary instead of its members, so this is the one route that returns members —
one bounded page at a time, filtered across the COMPLETE set. Asserted here: the
paging/filter composition order, the clamping the read endpoints share, the
mismatch-list read keyed by state, the not-found mapping, and the property the
ticket is about — an initial binding payload whose size does not follow its
codings' cardinality.

The fixture's `scb/lisa/forsamling` is the synthetic many-state binding: 73 states
over five shared codings (400 / 400 / 600 members, one empty, one absent) plus a
dense integer coding and two stored conformance verdicts.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app

# The seeded parish codings (see `scripts/fixture_db.py`): id → (count, prefix).
_HISTORIC = 900
_DISTRICT = 902
_EMPTY = 903
_AGES = 904


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def _codes(client, value_set_id, **params):
    resp = client.get(f"/api/value-sets/{value_set_id}/codes", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_default_page_is_bounded_and_ordered(client):
    body = _codes(client, _HISTORIC)
    assert body["value_set_id"] == _HISTORIC
    assert body["state_id"] is None
    assert body["q"] == ""
    # 400 members, 200 to a page: the total is the SET's, the page is the window.
    assert body["total"] == 400
    assert body["offset"] == 0
    assert body["limit"] == 200
    assert len(body["codes"]) == 200
    codes = [c["code"] for c in body["codes"]]
    assert codes == sorted(codes)
    assert codes[0] == "9000001"


def test_paging_walks_the_whole_set_without_gaps_or_repeats(client):
    seen = []
    for offset in (0, 200, 400):
        page = _codes(client, _HISTORIC, offset=offset)
        assert page["total"] == 400
        seen.extend(c["code"] for c in page["codes"])
    assert len(seen) == 400
    assert len(set(seen)) == 400
    assert seen == sorted(seen)
    # Past the end is an empty page, not an error — the caller stops on the total.
    assert _codes(client, _HISTORIC, offset=400)["codes"] == []


def test_filter_applies_to_the_complete_set_before_the_page(client):
    # "0033" matches exactly one member of 600; a filter applied AFTER paging
    # would find it only for a caller who had already walked to its page.
    body = _codes(client, _DISTRICT, q="0033", offset=0)
    assert body["total"] == 1
    assert [c["code"] for c in body["codes"]] == ["9020033"]
    # And the total is the FILTERED total, so "1 of 600" is true.
    wide = _codes(client, _DISTRICT, q="Distrikt 000")
    assert wide["total"] == 9
    assert len(wide["codes"]) == 9


def test_filter_folds_case_and_diacritics_like_the_browser(client):
    # `Församling 0001` — the SPA's `matchesFilter` folds NFD combining marks and
    # lowercases; a SQL LIKE would match neither spelling.
    assert _codes(client, _HISTORIC, q="församling 0001")["total"] == 1
    assert _codes(client, _HISTORIC, q="forsamling 0001")["total"] == 1
    assert _codes(client, _HISTORIC, q="FÖRSAMLING 0001")["total"] == 1


def test_wildcards_are_literal_characters_not_like_patterns(client):
    # A substring test, not LIKE: `%` matches nothing here rather than everything.
    assert _codes(client, _HISTORIC, q="%")["total"] == 0
    assert _codes(client, _HISTORIC, q="_")["total"] == 0
    # A blank query is the unfiltered set (the SPA's empty-needle semantics).
    assert _codes(client, _HISTORIC, q="   ")["total"] == 400


def test_limit_clamps_and_offset_floors(client):
    # `?limit` is display intent: clamped to [1, 1000], never a 422.
    assert _codes(client, _HISTORIC, limit=0)["limit"] == 1
    assert _codes(client, _HISTORIC, limit=-5)["limit"] == 1
    assert _codes(client, _HISTORIC, limit=9999)["limit"] == 1000
    assert len(_codes(client, _HISTORIC, limit=9999)["codes"]) == 400
    floored = _codes(client, _HISTORIC, offset=-10)
    assert floored["offset"] == 0
    assert floored["codes"][0]["code"] == "9000001"


def test_an_empty_coding_is_empty_not_missing(client):
    body = _codes(client, _EMPTY)
    assert body["total"] == 0
    assert body["codes"] == []


def test_a_dense_integer_coding_reads_like_any_other(client):
    # The leaf renders it as a range, but the members are still readable.
    body = _codes(client, _AGES, q="110")
    assert body["total"] == 1
    assert body["codes"] == [{"code": "110", "label": "110 år"}]


def test_unknown_value_set_is_an_actionable_404(client):
    resp = client.get("/api/value-sets/424242/codes")
    assert resp.status_code == 404
    assert "424242" in resp.json()["detail"]


def test_over_long_query_is_rejected(client):
    resp = client.get(f"/api/value-sets/{_HISTORIC}/codes", params={"q": "x" * 500})
    assert resp.status_code == 422


def _states(client):
    body = client.get("/api/catalog/scb/lisa/forsamling").json()
    assert body["kind"] == "binding"
    return body["states"]


def test_the_mismatch_list_is_read_through_its_own_state(client):
    severed = next(
        s
        for s in _states(client)
        if (s["classification_conformance"] or {}).get("status") == "severed"
    )
    value_set_id = severed["value_set_id"]
    body = _codes(client, value_set_id, state=severed["state_id"])
    assert body["state_id"] == severed["state_id"]
    assert (
        body["total"]
        == severed["classification_conformance"]["nonconforming_code_count"]
    )
    assert [c["code"] for c in body["codes"]] == sorted(
        c["code"] for c in body["codes"]
    )
    # The filter and the page apply to the mismatch list too.
    assert (
        _codes(client, value_set_id, state=severed["state_id"], limit=2)["codes"]
        == body["codes"][:2]
    )


def test_a_state_cannot_read_a_coding_it_does_not_carry(client):
    severed = next(
        s
        for s in _states(client)
        if (s["classification_conformance"] or {}).get("status") == "severed"
    )
    other = next(
        s["value_set_id"]
        for s in _states(client)
        if s["value_set_id"] not in (None, severed["value_set_id"])
    )
    resp = client.get(
        f"/api/value-sets/{other}/codes", params={"state": severed["state_id"]}
    )
    assert resp.status_code == 404
    assert str(other) in resp.json()["detail"]


class TestBindingPayloadIndependence:
    """The acceptance property: the initial payload keeps every historical state
    reference and its coding identity, and carries no code membership."""

    def test_every_state_survives_with_its_identity_and_summary(self, client):
        states = _states(client)
        # The seeded history: 73 states over five codings.
        assert len(states) == 73
        assert {s["value_set_id"] for s in states} == {900, 901, 902, 903, 904, None}
        for state in states:
            assert state["value_set"] is None
            if state["value_set_id"] is None:
                assert state["value_set_summary"] is None
            else:
                assert state["value_set_summary"]["code_count"] >= 0
        by_id = {s["value_set_id"]: s["value_set_summary"] for s in states}
        assert by_id[900]["code_count"] == 400
        assert by_id[902]["code_count"] == 600
        assert by_id[903] == {"code_count": 0, "integer_range": None}
        # The dense integer coding reports its span instead of 111 rows.
        assert by_id[904] == {
            "code_count": 111,
            "integer_range": {"min": 0, "max": 110},
        }

    def test_the_conformance_verdict_stays_but_its_code_list_does_not(self, client):
        verdicts = [
            s["classification_conformance"]
            for s in _states(client)
            if s["classification_conformance"]
        ]
        assert {v["status"] for v in verdicts} == {"kept", "severed"}
        assert all(v["nonconforming_codes"] == [] for v in verdicts)
        assert sorted(v["nonconforming_code_count"] for v in verdicts) == [3, 5]

    def test_the_payload_does_not_grow_with_code_cardinality(self, client):
        # 1511 members across the history's codings; embedding them per state
        # would be ~73× that. The payload is smaller than ONE copy of them.
        raw = client.get("/api/catalog/scb/lisa/forsamling").content
        assert len(raw) < 60_000

    def test_a_period_narrowed_read_keeps_the_same_shape(self, client):
        body = client.get(
            "/api/catalog/scb/lisa/forsamling", params={"period": 2010}
        ).json()
        [state] = body["states"]
        assert state["value_set_id"] == 902
        assert state["value_set"] is None
        assert state["value_set_summary"]["code_count"] == 600
        assert state["classification_conformance"]["status"] == "kept"
        assert state["classification_conformance"]["nonconforming_codes"] == []

    def test_the_complete_export_still_embeds_everything(self, client):
        # `/states` is the explicit full read and keeps its semantics.
        states = client.get("/api/catalog/scb/lisa/forsamling/states").json()["states"]
        coded = next(s for s in states if s["value_set_id"] == 900)
        assert len(coded["value_set"]) == 400
        assert coded["value_set_summary"] is None
        severed = next(
            s
            for s in states
            if (s["classification_conformance"] or {}).get("status") == "severed"
        )
        assert len(severed["classification_conformance"]["nonconforming_codes"]) == 5
