"""`/api/catalog` concept-group surfaces against the slugged ``catalog_db`` fixture.

Covers the register node's derived ``groups``, the register-scoped and
classification group subject routes, and the binding leaf's ``group`` ref.
"""

from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def test_register_node_carries_concept_groups(client):
    """#303: the register response carries derived concept `groups`; grouped
    members ALSO stay in `children` (the flat list is complete — the SPA folds)."""
    resp = client.get("/api/catalog/scb/rams")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["groups"]) == 1
    group = body["groups"][0]
    assert group["key"] == "ink"
    assert group["source"] == "token"
    # #819: axes carry the stable name + authored label ('månad'), not a bare key.
    assert group["axes"] == [{"name": "month", "label": "månad"}]
    assert [m["fqid"] for m in group["members"]] == [
        "scb/rams/inkjan",
        "scb/rams/inkfeb",
    ]
    assert group["members"][0]["facets"] == [
        {"axis": "month", "value": "01", "label": "januari"}
    ]
    # The grouped members are still in the flat children list.
    child_fqids = {c["fqid"] for c in body["children"] if c["kind"] == "binding"}
    assert {"scb/rams/inkjan", "scb/rams/inkfeb"} <= child_fqids


def test_register_groups_list_serializes(client):
    # scb/lisa carries the `lonefink-rep` representation group (#819, two members on
    # one FQID distinguished by delivery_column) — added for the steward column-grain
    # test. The register's `groups` list serializes it (empty-groups branch is covered
    # by the steward suite's drop-to-empty assertion).
    body = client.get("/api/catalog/scb/lisa").json()
    keys = {g["key"] for g in body["groups"]}
    assert keys == {"lonefink-rep"}


# ── Concept-group SUBJECT route (#617) ──────────────────────────────────────
# `/catalog/group/<provider>/<register>/<key>` exposes a group as a browsable
# subject. The fixture seeds a token month group `ink` on scb/rams (members
# inkjan/inkfeb) — see conftest `_seed_concept_groups`.


def test_group_route_returns_concept_group_node(client):
    """#617: the group route resolves a real group by key to a ConceptGroupNode —
    NOT a catch-all FQID parse. The `kind` is `concept-group`; identity +
    members + facets are carried."""
    resp = client.get("/api/catalog/group/scb/rams/ink")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "concept-group"
    assert body["provider"] == "scb"
    # The wire key is `register` (the BaseModel.register-shadow alias), not
    # `register_name`.
    assert body["register"] == "rams"
    assert body["key"] == "ink"
    assert body["source"] == "token"
    assert body["axes"] == [{"name": "month", "label": "månad"}]
    assert [m["fqid"] for m in body["members"]] == [
        "scb/rams/inkjan",
        "scb/rams/inkfeb",
    ]
    assert body["members"][0]["facets"] == [
        {"axis": "month", "value": "01", "label": "januari"}
    ]
    # No focus hint requested → `member` is null.
    assert body["member"] is None


def test_group_route_accepts_slash_bearing_key(catalog_db):
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) "
            "VALUES (98, 'variable', 2, 'slash/key', 'Slash key', 'edge')"
        )
        conn.execute(
            "INSERT INTO concept_group_variable (group_id, variable_id, "
            "delivery_column_name) "
            "SELECT 98, variable_id, NULL FROM variable "
            "WHERE register_id = 2 AND slug = 'syss'"
        )
    with TestClient(create_app()) as client:
        resp = client.get("/api/catalog/group/scb/rams/slash%2Fkey")
    assert resp.status_code == 200
    assert resp.json()["key"] == "slash/key"


def test_group_route_carries_per_member_coverage(client):
    """#617: each member carries its per-variable study-window `coverage` (#351),
    zipped on from `register_variable_coverage` — present as a key on every member
    (None for a stateless member; the fixture's inkjan/inkfeb have no states, so
    coverage is None — the FIELD must still be present per the additive shape)."""
    body = client.get("/api/catalog/group/scb/rams/ink").json()
    for member in body["members"]:
        assert "coverage" in member


def test_group_route_serializes_aggregated_member_tags(catalog_db):
    """#982: group subjects carry thematic tags aggregated from member bindings."""
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (960, 1, '960', 'Civilstånd', 'civilstand')"
        )
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (960, 'variable', 1, 'tagged-demo', "
            "'Tagged demo', 'curated')"
        )
        conn.execute(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) "
            "SELECT 960, variable_id, NULL FROM variable "
            "WHERE register_id = 1 AND slug IN ('kon', 'civilstand')"
        )

    with TestClient(create_app()) as client:
        body = client.get("/api/catalog/group/scb/lisa/tagged-demo").json()

    assert body["tags"] == [
        {
            "slug": "income",
            "label": "Income & earnings",
            "rank": 0,
            "starred": True,
            "note": "fixture recommendation",
        }
    ]


def test_group_route_representation_members_per_column_coverage(catalog_db):
    """#819: two representation members sharing ONE variable (e.g. CDISP 1968– vs
    CDISP5 2020–) must show DIFFERENT coverage — each its per-column window, not the
    variable's union span. Seeds a `disp` variable on scb/rams with two delivery
    columns (CDISP wide, CDISP5 narrow), then a representation group whose members
    both point at that variable but carry distinct `delivery_column_name`s."""
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (950, 2, '950', 'Disponibel inkomst', 'disp')"
        )
        # Two per-column windows on the SAME variable: CDISP 1968–2024 (wide),
        # CDISP5 2020–2024 (narrow). register_variant 20 ('standard') exists on rams.
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
            "valid_to, data_type, delivery_column_name) "
            "VALUES (950, 20, '1968-01-01', '2024-12-31', 'int', 'CDISP')"
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
            "valid_to, data_type, delivery_column_name) "
            "VALUES (950, 20, '2020-01-01', '2024-12-31', 'int', 'CDISP5')"
        )
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (97, 'variable', 2, 'disprep', 'Disp', 'edge')"
        )
        # Every faceted axis is declared in concept_group_axis (#819, build invariant).
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (97, 'rep', 0, 'Representation')"
        )
        for col in ("CDISP", "CDISP5"):
            cur = conn.execute(
                "INSERT INTO concept_group_variable "
                "(group_id, variable_id, delivery_column_name) VALUES (97, 950, ?)",
                (col,),
            )
            conn.execute(
                "INSERT INTO concept_group_variable_facet "
                "(member_id, axis, value, label) VALUES (?, 'rep', ?, ?)",
                (cur.lastrowid, col, col),
            )
    with TestClient(create_app()) as client:
        body = client.get("/api/catalog/group/scb/rams/disprep").json()
    by_col = {m["delivery_column"]: m["coverage"] for m in body["members"]}
    # CDISP keeps the wide window; CDISP5 its OWN narrow 2020– window — NOT the
    # variable's 1968– union (the bug this fix closes).
    assert by_col["CDISP"]["coverage_from"] == "1968-01-01"
    assert by_col["CDISP5"]["coverage_from"] == "2020-01-01"
    assert by_col["CDISP5"]["coverage_to"] == "2024-12-31"


def test_group_route_representation_member_without_state_row_has_zero_coverage(
    catalog_db,
):
    """#819: a representation member whose delivery column has NO `variable_state`
    row gets a zero-state coverage object, NOT `coverage = None` and NOT the variable's
    union — SCB keeps the full `variable_alias` set apart from `variable_state`, so an
    alias-only column must NOT borrow its sibling column's years.
    Seeds one variable with a single CDISP state row (1968–2024), then a group whose
    members are CDISP (delivered) and CDISP5 (alias-only, no state row)."""
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (951, 2, '951', 'Disponibel inkomst', 'disp')"
        )
        # Only CDISP has a state row; CDISP5 exists as a member but no per-column
        # coverage window (the alias-without-state case the fix guards against).
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
            "valid_to, data_type, delivery_column_name) "
            "VALUES (951, 20, '1968-01-01', '2024-12-31', 'int', 'CDISP')"
        )
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (96, 'variable', 2, 'disprep2', 'Disp', 'edge')"
        )
        # Every faceted axis is declared in concept_group_axis (#819, build invariant).
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (96, 'rep', 0, 'Representation')"
        )
        for col in ("CDISP", "CDISP5"):
            cur = conn.execute(
                "INSERT INTO concept_group_variable "
                "(group_id, variable_id, delivery_column_name) VALUES (96, 951, ?)",
                (col,),
            )
            conn.execute(
                "INSERT INTO concept_group_variable_facet "
                "(member_id, axis, value, label) VALUES (?, 'rep', ?, ?)",
                (cur.lastrowid, col, col),
            )
    with TestClient(create_app()) as client:
        body = client.get("/api/catalog/group/scb/rams/disprep2").json()
    by_col = {m["delivery_column"]: m["coverage"] for m in body["members"]}
    # CDISP shows its window; CDISP5 (no state row) is known empty, not unknown, and
    # not the variable union (which would falsely deliver CDISP5 across 1968–2024).
    assert by_col["CDISP"]["coverage_from"] == "1968-01-01"
    assert by_col["CDISP5"] == {
        "coverage_from": None,
        "coverage_to": None,
        "open_ended": False,
        "state_count": 0,
    }


def test_group_route_unknown_key_404(client):
    resp = client.get("/api/catalog/group/scb/rams/nosuchkey")
    assert resp.status_code == 404


def test_group_route_unknown_register_404(client):
    # `concept_group` returns None for a pair that names no register, too.
    resp = client.get("/api/catalog/group/scb/nope/ink")
    assert resp.status_code == 404


def test_group_route_member_focus_hint_echoed(client):
    """#617: a `?member=<slug>` that names a real member is echoed on the node so
    the SPA can highlight it."""
    body = client.get("/api/catalog/group/scb/rams/ink?member=inkjan").json()
    assert body["member"] == "inkjan"


def test_group_route_unknown_member_hint_ignored(client):
    """#617: a `?member=` that is NOT a member of this group is IGNORED (None), not
    a 404 — the group page stays first-class (a bad focus hint mustn't break it)."""
    resp = client.get("/api/catalog/group/scb/rams/ink?member=notamember")
    assert resp.status_code == 200
    assert resp.json()["member"] is None


def test_group_route_matched_before_catch_all(client):
    """#617 (the load-bearing route-ordering guard): a `/catalog/group/p/r/key`
    path must be matched by the FIXED group route, NOT greedy-consumed by the
    `{fqid:path}` catch-all and mis-parsed as an FQID. If the catch-all won, this
    4-seg path would 422 at the FQID arity guard (or 404 as a bogus FQID) — the
    `concept-group` kind proves the fixed route fired first."""
    body = client.get("/api/catalog/group/scb/rams/ink").json()
    assert body["kind"] == "concept-group"


# ── Classification-group SUBJECT route (#756) ───────────────────────────────
# `/catalog/group/class/<key>` exposes a classification umbrella group as a
# browsable subject (the classification sibling of the register-scoped group
# route). The fixture seeds the `sun` umbrella (group_id 11, kind classification,
# register_id NULL, AXIS-LESS — facet_axis NULL) over the terminal `sun2020`
# edition (facet niva) + the standalone `niva-test` aggregate (facet aggregat) —
# see conftest `_seed_concept_groups`.


def test_classification_group_route_returns_node(client):
    """#756: the route resolves a classification umbrella by key to a
    ClassificationGroupNode — NOT a catch-all FQID parse. `kind` is
    `classification-group`; identity + members + facets are carried, with members'
    `class/<slug>` FQIDs passed straight through (no provider/register/coverage).
    The umbrella is AXIS-LESS (`axes == []`); members still carry their short
    facet value/label with `axis: null`."""
    resp = client.get("/api/catalog/group/class/sun")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "classification-group"
    assert body["key"] == "sun"
    assert body["label"] == "Svensk utbildningsnomenklatur"
    assert body["source"] == "curated"
    assert body["axes"] == []
    # Members are ordered by facet value, then slug (list_classification_groups'
    # ORDER BY m.facet_value, c.slug): aggregat (niva-test) before niva (sun2020).
    assert [m["fqid"] for m in body["members"]] == [
        "class/niva-test",
        "class/sun2020",
    ]
    facets_by_fqid = {m["fqid"]: m["facets"] for m in body["members"]}
    assert facets_by_fqid["class/sun2020"] == [
        {"axis": None, "value": "niva", "label": "Utbildningsnivå"}
    ]
    assert facets_by_fqid["class/niva-test"] == [
        {"axis": None, "value": "aggregat", "label": "Aggregat"}
    ]
    # No coverage / member focus-hint surface — classification members carry
    # neither (distinct from the register-scoped ConceptGroupNode).
    assert all("coverage" not in m for m in body["members"])
    assert "member" not in body


def test_classification_group_route_unknown_key_404(client):
    resp = client.get("/api/catalog/group/class/nosuchkey")
    assert resp.status_code == 404


def test_classification_group_route_matched_before_catch_all(client):
    """#756 (the load-bearing route-ordering guard): `/catalog/group/class/sun`
    must be matched by the FIXED classification-group route, NOT mis-parsed as a
    register group with provider=`class` (the register route's `{provider}` slot
    would otherwise capture the literal `class`), and NOT greedy-consumed by the
    `{fqid:path}` catch-all. The `classification-group` kind proves the literal
    `class` route fired first."""
    body = client.get("/api/catalog/group/class/sun").json()
    assert body["kind"] == "classification-group"


def test_classification_group_route_accepts_slash_bearing_key(catalog_db):
    """#756: a slash in a classification umbrella key survives the `{key:path}` route
    (mirrors `test_group_route_accepts_slash_bearing_key` for the register-scoped
    group). Seed a fresh classification + a classification umbrella with a slash-bearing
    key over it (each classification can belong to only one group — the seeded
    members are already taken), then request it `%2F`-encoded."""
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "INSERT INTO classification (id, short_name, name, slug) "
            "VALUES (60, 'SLASH', 'Slash member', 'slash-member')"
        )
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) "
            "VALUES (99, 'classification', NULL, 'a/b', 'Slash key', 'token')"
        )
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (99, 'dimension', 0, 'dimension')"
        )
        conn.execute(
            "INSERT INTO concept_group_classification "
            "(classification_id, group_id, facet_value, facet_label) "
            "VALUES (60, 99, 'aggregat', 'Aggregat')"
        )
    with TestClient(create_app()) as client:
        resp = client.get("/api/catalog/group/class/a%2Fb")
    assert resp.status_code == 200
    assert resp.json()["key"] == "a/b"


def test_grouped_binding_leaf_carries_group_ref(client):
    """#616/#617: a grouped binding's leaf carries its owning group as a
    `(provider, register, key)` ref, so a member page knows its home group without
    a second fetch. inkjan is a member of the `ink` group on scb/rams."""
    body = client.get("/api/catalog/scb/rams/inkjan").json()
    assert body["kind"] == "binding"
    assert body["group"] == {"provider": "scb", "register": "rams", "key": "ink"}


def test_ungrouped_binding_leaf_group_ref_is_none(client):
    """#616/#617: an ungrouped binding's leaf carries `group: None`. kon is not a
    concept-group member."""
    body = client.get("/api/catalog/scb/lisa/kon").json()
    assert body["group"] is None


def test_same_as_alias_binding_leaf_reports_target_group(client):
    """#616/#617: a same_as alias's leaf reports its TARGET's group, since the
    ref is keyed on the RESOLVED variable's triple. `scb/lisa/inkjan-alias` is a
    phantom slug resolving via same_as to the grouped `scb/rams/inkjan`, so its
    leaf carries inkjan's `ink` group on scb/rams (not the alias's own register)."""
    body = client.get("/api/catalog/scb/lisa/inkjan-alias").json()
    assert body["kind"] == "binding"
    assert body["group"] == {"provider": "scb", "register": "rams", "key": "ink"}
