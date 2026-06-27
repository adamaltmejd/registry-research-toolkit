"""`/api/catalog` browse against the slugged ``catalog_db`` fixture.

See DESIGN.md → Catalog router structure. Covers the root, each catch-all node
kind (provider / register / binding leaf /
classification-root / classification), the discriminated-union ``kind`` tags, the
binding leaf's embedded longitudinal record, the register's ``variants`` ref
stub, and the 404-on-not-found mapping. The path-traversal guard lives in
``test_fqid_validation.py``.
"""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from reg_webapp.app import create_app


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c


def test_concurrent_browse_no_cross_thread_error(client):
    """Codex P1 (#168): a generator-dependency-opened sqlite connection was used
    cross-thread under FastAPI's sync threadpool → `sqlite3.ProgrammingError`
    (reproduced 72/80 fail before the fix). The per-request connection now opens
    inside the sync handler body (one thread), so concurrent browse requests must
    all succeed."""
    with ThreadPoolExecutor(max_workers=8) as pool:
        codes = list(
            pool.map(
                lambda _: client.get("/api/catalog/scb/lisa/kon").status_code, range(60)
            )
        )
    failures = [c for c in codes if c != 200]
    assert not failures, f"cross-thread failures under concurrency: {failures}"


def test_root_lists_providers_and_classification_root(client):
    resp = client.get("/api/catalog")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "root"
    kinds = [child["kind"] for child in body["children"]]
    # Providers come first (slug-ordered), classification-root last.
    assert "provider" in kinds
    assert kinds[-1] == "classification-root"
    providers = [c for c in body["children"] if c["kind"] == "provider"]
    # `fohm` + `fk` (#422) and `lakemedelsverket` / `pliktverket` / `riksarkivet` /
    # `umu` (#443) are seeded providers, so the catalog root lists them alongside
    # scb/sos (list_providers enumerates every seeded provider).
    assert {p["fqid"] for p in providers} == {
        "scb",
        "sos",
        "fohm",
        "fk",
        "lakemedelsverket",
        "pliktverket",
        "riksarkivet",
        "umu",
    }
    class_root = next(c for c in body["children"] if c["kind"] == "classification-root")
    assert class_root["fqid"] == "class"


def test_provider_node_lists_registers(client):
    resp = client.get("/api/catalog/scb")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "provider"
    assert body["fqid"] == "scb"
    child_fqids = {c["fqid"] for c in body["children"]}
    assert child_fqids == {"scb/lisa", "scb/rams"}
    assert all(c["kind"] == "register" for c in body["children"])


def test_register_node_lists_bindings_and_variants_ref(client):
    resp = client.get("/api/catalog/scb/lisa")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "register"
    assert body["fqid"] == "scb/lisa"
    bindings = [c for c in body["children"] if c["kind"] == "binding"]
    # `lonfink` is the merged monthly-family binding (#319) seeded alongside `kon`.
    assert {b["fqid"] for b in bindings} == {"scb/lisa/kon", "scb/lisa/lonfink"}
    # The variant-browser slot (A5.2a, wired): carries the navigable register_fqid.
    variants_refs = [c for c in body["children"] if c["kind"] == "variants-ref"]
    assert len(variants_refs) == 1
    assert variants_refs[0]["register_fqid"] == "scb/lisa"


def test_binding_leaf_embeds_full_record(client):
    resp = client.get("/api/catalog/scb/lisa/kon")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "binding"
    assert body["fqid"] == "scb/lisa/kon"
    # The leaf embeds the full longitudinal record from ONE resolve call.
    assert len(body["states"]) == 1
    state = body["states"][0]
    assert state["variant"] == "individer-15plus"
    # The variable-grain `is_identifier` is serialized as a required field
    # (the `_state_model` passthrough); the fixture seeds is_identifier=0.
    assert state["is_identifier"] is False
    # The per-state classification slug serializes too; the fixture kon state has
    # classification_id NULL → None.
    assert state["classification_slug"] is None
    # #321: an OPEN-ENDED state (valid_to = the 9999-12-31 sentinel) has no
    # finite period token — the field is None (the SPA renders "since
    # valid_from").
    assert state["period_token"] is None
    # The value set is hydrated as (code, label) objects.
    assert state["value_set"] == [
        {"code": "1", "label": "Man"},
        {"code": "2", "label": "Kvinna"},
    ]
    # The same_as edge (kon → rams/syss) is embedded, fqid serialized as a string.
    assert any(ref["fqid"] == "scb/rams/syss" for ref in body["same_as"])
    # Edge collections are present (possibly empty); #582 replaced the immediate
    # `replaced_by` embed with the full `succession_chain` (asserted in detail in
    # test_binding_leaf_embeds_full_succession_chain).
    for field in ("succession_chain", "related_to", "lineage"):
        assert field in body


def test_binding_leaf_embeds_full_succession_chain(client):
    # #582/#588: the binding leaf embeds the FULL variable succession timeline
    # (oldest first, terminal last) for the QUERIED node's own path, superseding the
    # immediate `replaced_by` embed. The fixture wires kon (2019, "kon→syss") →
    # rams/syss (the live terminal), and SEPARATELY the redirect-test dead
    # predecessors renamed-head → renamed-mid → syss. syss is therefore a MERGE
    # (two inbound branches). #588 anchors the chain on the QUERIED node's path, so
    # querying kon returns ONLY kon's branch [kon, syss] — the renamed-* branch is a
    # different inbound path and is NOT polluted in (the pre-#588 collect-all-from-
    # terminal walk wrongly rendered all four).
    resp = client.get("/api/catalog/scb/lisa/kon")
    assert resp.status_code == 200
    chain = resp.json()["succession_chain"]
    assert [(e["register"], e["variable"]) for e in chain] == [
        ("lisa", "kon"),
        ("rams", "syss"),
    ]
    by_var = {e["variable"]: e for e in chain}
    # The queried edition: is_self, dated, carries its edge's reason (beskrivning).
    assert by_var["kon"]["is_self"] is True
    assert by_var["kon"]["is_current"] is False
    assert by_var["kon"]["effective_year"] == 2019
    assert by_var["kon"]["reason"] == "kon→syss"
    assert by_var["kon"]["fqid"] == "scb/lisa/kon"
    assert by_var["kon"]["name"] == "Kön"
    # The terminal (live) edition: is_current, no successor-side year/reason.
    assert by_var["syss"]["is_current"] is True
    assert by_var["syss"]["is_self"] is False
    assert by_var["syss"]["effective_year"] is None
    assert by_var["syss"]["reason"] is None
    assert by_var["syss"]["fqid"] == "scb/rams/syss"
    assert by_var["syss"]["name"] == "Sysselsättning"
    assert sum(e["is_self"] for e in chain) == 1
    assert sum(e["is_current"] for e in chain) == 1
    # The sibling merge branch (renamed-head/renamed-mid) is on a DIFFERENT inbound
    # path to syss, so querying kon never reaches it (#588 — anchored on kon's path).
    assert "renamed-head" not in by_var
    assert "renamed-mid" not in by_var


def test_binding_leaf_omits_lineage_warnings(client):
    """ResolvedVariable doesn't carry lineage_warnings, so the leaf must NOT
    expose them (they arrive via A5.2's /lineage_warnings)."""
    resp = client.get("/api/catalog/scb/lisa/kon")
    assert "lineage_warnings" not in resp.json()


def test_classification_root_lists_classifications(client):
    resp = client.get("/api/catalog/class")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "classification-root"
    assert body["fqid"] == "class"
    slugs = {c["fqid"] for c in body["children"]}
    assert "class/sun2020" in slugs
    assert all(c["kind"] == "classification" for c in body["children"])


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


def test_group_route_representation_member_without_state_row_has_no_coverage(
    catalog_db,
):
    """#819: a representation member whose delivery column has NO `variable_state`
    row gets `coverage = None`, NOT the variable's union — SCB keeps the full
    `variable_alias` set apart from `variable_state`, so an alias-only column must
    NOT borrow its sibling column's years (which would grey the slider wrongly).
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
    # CDISP shows its window; CDISP5 (no state row) greys — coverage None, NOT the
    # variable union (which would falsely deliver CDISP5 across 1968–2024).
    assert by_col["CDISP"]["coverage_from"] == "1968-01-01"
    assert by_col["CDISP5"] is None


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


def test_classification_root_drops_superseded_and_folds_dimension_group(client):
    """#608: the classification root surfaces only TERMINAL editions as children
    (a row whose `superseded_by` is truthy — a successor exists — is dropped) and
    carries the #516 umbrella `sun` group over its members. The fixture seeds
    sun1996 → sun2000 → sun2020 with `supersedes_id` projected from those edges
    (so the superseded-by filter is genuinely exercised, not a no-op on NULLs);
    the `sun` group members are the terminal sun2020 + the non-succession
    `niva-test` aggregate, which therefore stay in `children` and fold. The
    umbrella is AXIS-LESS (`axes == []`)."""
    body = client.get("/api/catalog/class").json()
    assert len(body["groups"]) == 1
    group = body["groups"][0]
    assert group["key"] == "sun"
    assert group["axes"] == []
    assert {m["fqid"] for m in group["members"]} == {
        "class/sun2020",
        "class/niva-test",
    }
    child_fqids = {c["fqid"] for c in body["children"]}
    # Terminal editions + non-succession aggregates survive and fold under the group.
    assert "class/sun2020" in child_fqids
    assert "class/niva-test" in child_fqids
    # The superseded editions are dropped from children (the regression lock — this
    # fails on the pre-#608 code, which surfaced every classification as a child).
    assert "class/sun1996" not in child_fqids
    assert "class/sun2000" not in child_fqids


def test_classification_leaf_resolves(client):
    resp = client.get("/api/catalog/class/sun2020")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "classification"
    assert body["fqid"] == "class/sun2020"
    assert body["short_name"] == "SUN2020"


def test_classification_leaf_embeds_full_edition_chain(client):
    # #571: the classification leaf embeds the FULL succession timeline (oldest
    # first, terminal last) so the browse panel renders every edition synchronously.
    # The fixture seeds sun1996 → sun2000 → sun2020 (live terminal) — all LIVE rows,
    # matching the build validator's invariant (succession edges resolve to live
    # classification slugs; validate.py, the classification_replaced_by check).
    resp = client.get("/api/catalog/class/sun2020")
    assert resp.status_code == 200
    chain = resp.json()["edition_chain"]
    assert [e["slug"] for e in chain] == ["sun1996", "sun2000", "sun2020"]
    by_slug = {e["slug"]: e for e in chain}
    # Every edition is a live row → each carries a fqid (no dead-edition shape).
    assert all(e["fqid"] == f"class/{e['slug']}" for e in chain)
    assert by_slug["sun1996"]["name"] == "Svensk utbildningsnomenklatur"
    assert by_slug["sun1996"]["effective_year"] == 2000
    # Live terminal == the queried edition: is_current AND is_self.
    assert by_slug["sun2020"]["fqid"] == "class/sun2020"
    assert by_slug["sun2020"]["is_current"] is True
    assert by_slug["sun2020"]["is_self"] is True
    assert by_slug["sun2020"]["effective_year"] is None
    assert by_slug["sun2000"]["is_current"] is False
    assert by_slug["sun2000"]["is_self"] is False


def test_classification_leaf_embeds_value_set_codes(client):
    # #609: the classification leaf embeds the RESOLVED edition's value-set codes
    # (code-ordered) so the SPA's code viewer renders synchronously. The fixture
    # links sun2020 to a canonical "Man" code (is_valid=1) plus two observed-only
    # codes (X0 / C12, is_valid=0) — observed codes are SURFACED, not filtered, with
    # the validity flag passed through.
    resp = client.get("/api/catalog/class/sun2020")
    assert resp.status_code == 200
    codes = resp.json()["codes"]
    by_label = {c["label"]: c for c in codes}
    assert "Man" in by_label
    assert by_label["Man"]["is_valid"] is True
    # Observed-only codes are present with is_valid False (not dropped).
    assert by_label["Icke-kanonisk kod"]["is_valid"] is False
    # Code-ordered (the SQL ORDER BY vc.code, vc.label).
    assert [c["code"] for c in codes] == sorted(c["code"] for c in codes)


def test_classification_leaf_embeds_dimension_cross_reference(client):
    # #609: the leaf embeds the curated umbrella group(s) it belongs to (the niva ↔
    # aggregate granularity cross-reference). The fixture's `group:sun` umbrella
    # (AXIS-LESS) has sun2020 + the niva-test aggregate as members.
    resp = client.get("/api/catalog/class/sun2020")
    assert resp.status_code == 200
    dimensions = resp.json()["dimensions"]
    assert [g["key"] for g in dimensions] == ["sun"]
    assert dimensions[0]["axes"] == []
    member_fqids = {m["fqid"] for m in dimensions[0]["members"]}
    assert {"class/sun2020", "class/niva-test"} <= member_fqids


def test_classification_leaf_without_codes_or_dimensions_is_empty(client):
    # A classification in no umbrella group and with no codes carries empty lists
    # (the SPA omits both sections). sun1996 is a superseded edition with neither.
    resp = client.get("/api/catalog/class/sun1996")
    assert resp.status_code == 200
    body = resp.json()
    assert body["codes"] == []
    assert body["dimensions"] == []


def test_classification_split_root_edition_chain_fans_out(client):
    # #605 / #579: browsing the SPLIT root embeds ALL downstream branches in
    # edition_chain (the forward closure), not just the deterministic-first one.
    # The fixture seeds sni-root1996 → {sni-grp2000, sni-ink2000, sni-niv2000},
    # each → its 2020 tip. The closure is DFS in ORDER BY successor_slug (grp < ink
    # < niv), each branch's 2000→2020 subtree before the next.
    resp = client.get("/api/catalog/class/sni-root1996")
    assert resp.status_code == 200
    chain = resp.json()["edition_chain"]
    assert [e["slug"] for e in chain] == [
        "sni-root1996",
        "sni-grp2000",
        "sni-grp2020",
        "sni-ink2000",
        "sni-ink2020",
        "sni-niv2000",
        "sni-niv2020",
    ]
    # All three 2020 branch tips are current → MULTIPLE is_current editions.
    currents = {e["slug"] for e in chain if e["is_current"]}
    assert currents == {"sni-grp2020", "sni-ink2020", "sni-niv2020"}
    assert sum(e["is_current"] for e in chain) == 3
    # The split root is the queried (self) edition; its year is its det-first edge's.
    self_editions = [e["slug"] for e in chain if e["is_self"]]
    assert self_editions == ["sni-root1996"]
    by_slug = {e["slug"]: e for e in chain}
    assert by_slug["sni-root1996"]["effective_year"] == 2000


def test_classification_split_branch_leaf_scopes_to_own_path(client):
    # #605: querying a LEAF of the split (sni-niv2020) returns ONLY its own path back
    # to the root — the inriktning/grupp sibling branches are NOT included.
    resp = client.get("/api/catalog/class/sni-niv2020")
    assert resp.status_code == 200
    chain = resp.json()["edition_chain"]
    assert [e["slug"] for e in chain] == [
        "sni-root1996",
        "sni-niv2000",
        "sni-niv2020",
    ]
    assert [e["slug"] for e in chain if e["is_current"]] == ["sni-niv2020"]
    assert [e["slug"] for e in chain if e["is_self"]] == ["sni-niv2020"]


def test_missing_provider_returns_404(client):
    resp = client.get("/api/catalog/nope")
    assert resp.status_code == 404


def test_missing_binding_returns_404(client):
    resp = client.get("/api/catalog/scb/lisa/doesnotexist")
    assert resp.status_code == 404


def test_renamed_binding_redirects_301_to_terminal(client):
    """#355 PART 2: a citation of a renamed/dead binding slug 301-redirects to its
    TERMINAL successor. The fixture seeds the chain
    `scb/lisa/renamed-head → scb/lisa/renamed-mid → scb/rams/syss` (head + mid
    have no `variable` row); a GET on the head must land at the terminal.

    `follow_redirects=False` is REQUIRED — TestClient follows 301s by default, so
    without it we'd see the followed 200 from the terminal, not the redirect."""
    resp = client.get("/api/catalog/scb/lisa/renamed-head", follow_redirects=False)
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/rams/syss"


def test_renamed_register_redirects_301_to_terminal(client):
    """#412: a citation of a renamed/dead REGISTER slug 301-redirects to its
    terminal successor, the same way a renamed binding does. The fixture seeds
    `scb/oldreg → scb/lisa` (`oldreg` has no `register` row); a GET on the dead
    register must land at the live `scb/lisa` register node.

    `follow_redirects=False` is REQUIRED — TestClient follows 301s by default, so
    without it we'd see the followed 200 from the terminal, not the redirect."""
    resp = client.get("/api/catalog/scb/oldreg", follow_redirects=False)
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/lisa"


def test_unknown_dead_register_still_404(client):
    """#412: a truly-unknown dead REGISTER slug with NO successor edge stays 404
    (not a redirect) — `resolve_terminal_successor` returns None, so the original
    404 re-raises unchanged (mirrors `test_unknown_dead_binding_still_404`)."""
    resp = client.get("/api/catalog/scb/never-existed-reg", follow_redirects=False)
    assert resp.status_code == 404


def test_renamed_binding_redirect_walks_to_absolute_chain_end(client):
    """The redirect always resolves to the ABSOLUTE chain end, never one hop: a
    GET on the MIDDLE dead slug also lands at the terminal `scb/rams/syss`."""
    resp = client.get("/api/catalog/scb/lisa/renamed-mid", follow_redirects=False)
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/rams/syss"


def test_unknown_dead_binding_still_404(client):
    """A truly-unknown dead slug with NO successor edge stays 404 (not a
    redirect) — `resolve_terminal_successor` returns None, so the original 404
    re-raises unchanged."""
    resp = client.get("/api/catalog/scb/lisa/never-existed", follow_redirects=False)
    assert resp.status_code == 404


def test_dead_binding_with_period_redirects_301_preserving_query(client):
    """#411: a dead/renamed binding cited WITH `?period` now 301s to its terminal
    successor (previously a deferred 404). The query string rides along, so the
    Location keeps `?period=2019`. The chain head `renamed-head` → terminal
    `scb/rams/syss`."""
    resp = client.get(
        "/api/catalog/scb/lisa/renamed-head?period=2019", follow_redirects=False
    )
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/rams/syss?period=2019"


def test_live_binding_inverted_period_range_stays_422(client):
    """#411: a syntactically-valid but lo>hi `?period` range on a LIVE binding must
    stay 422 — `_redirect_or_4xx` redirects ONLY on `fqid_not_found`; the inverted
    range passes the syntactic `_validated_period` dependency (reg_meta is the
    semantic backstop) and is rejected inside `resolve_at` as an EXIT_USAGE
    `invalid_period` error, which falls back to `_http_4xx_from_regmeta` (422),
    never a 301."""
    resp = client.get(
        "/api/catalog/scb/lisa/kon?period=2020..2019", follow_redirects=False
    )
    assert resp.status_code == 422


# The 6 suffixed binding sub-endpoints — the redirect must preserve the suffix.
_SUB_ENDPOINTS = [
    "states",
    "predecessors",
    "successors",
    "related",
    "lineage",
    "lineage_warnings",
]


@pytest.mark.parametrize("suffix", _SUB_ENDPOINTS)
def test_dead_binding_subendpoint_redirects_301_to_same_suffix(client, suffix):
    """#411: a dead/renamed binding cited on any of the 6 suffixed sub-endpoints
    301s to the SAME suffix on its terminal successor. The chain head
    `renamed-head/<suffix>` → `scb/rams/syss/<suffix>`."""
    resp = client.get(
        f"/api/catalog/scb/lisa/renamed-head/{suffix}", follow_redirects=False
    )
    assert resp.status_code == 301
    assert resp.headers["location"] == f"/api/catalog/scb/rams/syss/{suffix}"


def test_dead_binding_with_period_redirect_walks_to_absolute_chain_end(client):
    """#411: the `?period` redirect resolves to the ABSOLUTE chain end, never one
    hop — a GET on the MIDDLE dead slug also lands at the terminal, query
    preserved."""
    resp = client.get(
        "/api/catalog/scb/lisa/renamed-mid?period=2019", follow_redirects=False
    )
    assert resp.status_code == 301
    assert resp.headers["location"] == "/api/catalog/scb/rams/syss?period=2019"


def test_unknown_dead_binding_with_period_still_404(client):
    """#411: an UNKNOWN dead binding (no successor edge) WITH `?period` still 404s —
    `resolve_terminal_successor` returns None, so `_redirect_or_4xx` falls back to
    the 404 (a 422 usage / 500 build-invariant error would NEVER redirect either)."""
    resp = client.get(
        "/api/catalog/scb/lisa/never-existed?period=2019", follow_redirects=False
    )
    assert resp.status_code == 404


@pytest.mark.parametrize("suffix", _SUB_ENDPOINTS)
def test_unknown_dead_binding_subendpoint_still_404(client, suffix):
    """#411: an UNKNOWN dead binding (no successor edge) on a sub-endpoint still
    404s — no terminal to redirect to."""
    resp = client.get(
        f"/api/catalog/scb/lisa/never-existed/{suffix}", follow_redirects=False
    )
    assert resp.status_code == 404


def test_too_many_segments_returns_422(client):
    # Every segment is a valid slug, so the per-segment guard admits it; the
    # >3-segment arity is rejected by `reg_meta.fqid.parse` → 422 (a structural
    # grammar error, not a 404).
    resp = client.get("/api/catalog/scb/lisa/kon/extra/more")
    assert resp.status_code == 422


def test_register_node_register_field_alias_on_wire(client):
    """The binding leaf's edge refs serialize the triple under the wire key
    `register` (alias), not the Python attr `register_name`."""
    resp = client.get("/api/catalog/scb/lisa/kon")
    same_as = resp.json()["same_as"]
    assert same_as, "fixture seeds a same_as edge"
    ref = same_as[0]
    assert ref["register"] == "rams"
    assert "register_name" not in ref


def test_state_period_token_passes_through_reg_meta():
    """#321/#681: the leaf no longer maps states through a webapp wrapper — it
    embeds reg_meta's `VariableState` directly, so the `period_token` reg_meta
    populates (see `reg_meta.catalog.Catalog`'s window logic, covered by
    test_fqid.py's `period_token_for_bounds` round-trips and the resolve-path
    coverage in test_catalog.py) is exactly what serializes to the wire. This
    asserts the pass-through on the embedded model itself for every token shape
    (the open-ended → None case is also covered at the response level in
    test_binding_leaf_embeds_full_record)."""
    from reg_meta.catalog import VariableState

    def state(period_token: str | None) -> VariableState:
        return VariableState(
            state_id=1,
            variant="v",
            variant_label=None,
            register_variant_id=1,
            valid_from="2018-01-01",
            valid_to="2018-12-31",
            data_type=None,
            data_length=None,
            delivery_column_name=None,
            value_set_version_label="",
            value_set_id=None,
            value_set=None,
            is_identifier=False,
            classification_slug=None,
            period_token=period_token,
        )

    for token in ("2018", "VT2009", "1992-01-01..2009-12-31", None):
        assert state(token).model_dump()["period_token"] == token
