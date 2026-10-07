"""`/api/catalog` tree browse against the slugged ``catalog_db`` fixture.

See DESIGN.md → Catalog router structure. Covers the provider node, the
register's delivery columns and ``variants`` ref stub, the binding leaf's
open-ended period and summary-only coding, its succession chain, and the 422 for
an over-long path. The root, register children and 404s are pinned by
``conformance/cases/http_catalog``; the path-traversal guard lives in
``test_fqid_validation.py``.
"""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient
from reg_webapp.app import create_app


def test_provider_node_lists_registers(client):
    resp = client.get("/api/catalog/scb")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "provider"
    assert body["fqid"] == "scb"
    child_fqids = {c["fqid"] for c in body["children"]}
    assert child_fqids == {"scb/lisa", "scb/rams"}
    assert all(c["kind"] == "register" for c in body["children"])
    by_fqid = {c["fqid"]: c for c in body["children"]}
    assert [tag["slug"] for tag in by_fqid["scb/lisa"]["tags"]] == ["income"]
    assert [tag["slug"] for tag in by_fqid["scb/rams"]["tags"]] == ["employment"]


def test_register_node_carries_the_variants_ref(client):
    # The variant-browser slot (A5.2a, wired): carries the navigable register_fqid.
    body = client.get("/api/catalog/scb/lisa").json()
    variants_refs = [c for c in body["children"] if c["kind"] == "variants-ref"]
    assert len(variants_refs) == 1
    assert variants_refs[0]["register_fqid"] == "scb/lisa"


def test_register_children_carry_every_delivering_variant(catalog_db):
    """Y-82: a variable delivered by TWO variants carries both, each with the
    column and window of that delivery — the register page's chip source. Seeds a
    second lisa variant delivering a renamed `disp` column."""
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, slug, "
            "name) VALUES (11, 1, 'individer-16plus', 'Individer 16+')"
        )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (940, 1, '940', 'Disponibel inkomst', 'disp')"
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
            "valid_to, data_type, delivery_column_name) "
            "VALUES (940, 10, '1968-01-01', '2019-12-31', 'int', 'CDISP')"
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
            "valid_to, data_type, delivery_column_name) "
            "VALUES (940, 11, '2020-01-01', '9999-12-31', 'int', 'CDISP5')"
        )
        # Every state column is a `variable_alias` row too (the shipped DB's
        # invariant), so this also holds the Y-93 alias pass to ONE delivery per
        # state column rather than a duplicate beside it.
        conn.executemany(
            "INSERT INTO variable_alias (variable_id, register_variant_id, "
            "delivery_column_name) VALUES (940, ?, ?)",
            [(10, "CDISP"), (11, "CDISP5")],
        )

    with TestClient(create_app()) as client:
        body = client.get("/api/catalog/scb/lisa").json()

    disp = next(c for c in body["children"] if c.get("fqid") == "scb/lisa/disp")
    assert [(d["variant"], d["column"]) for d in disp["deliveries"]] == [
        ("individer-15plus", "CDISP"),
        ("individer-16plus", "CDISP5"),
    ]
    # Each delivery keeps its own window, not the variable's 1968– union.
    assert disp["deliveries"][0]["coverage"]["coverage_to"] == "2019-12-31"
    assert disp["deliveries"][1]["coverage"]["open_ended"] is True


def test_register_children_carry_alias_backed_delivery_columns(client):
    """Y-93: a column carried as `variable_alias` + `variable_alias_window` and
    never as a state is a delivery too. The fixture's merged monthly family
    (#319) `lonfink` holds ONE annual state on `LonFinkJan`; the register page
    names all three month columns with their month windows, so a researcher
    typing `LonFinkMars` in the filter finds the variable."""
    body = client.get("/api/catalog/scb/lisa").json()
    by_fqid = {c["fqid"]: c for c in body["children"] if c["kind"] == "binding"}
    lonfink = by_fqid["scb/lisa/lonfink"]["deliveries"]
    # Each month column carries its OWN window, not the annual state's — the
    # state's own column included (its windows replace the base claim, as the
    # binding leaf's `_expand_state_windows` reads it).
    assert [
        (
            d["variant"],
            d["column"],
            d["coverage"]["coverage_from"],
            d["coverage"]["coverage_to"],
        )
        for d in lonfink
    ] == [
        ("individer-15plus", "LonFinkFeb", "2018-02-01", "2018-02-28"),
        ("individer-15plus", "LonFinkJan", "2018-01-01", "2018-01-31"),
        ("individer-15plus", "LonFinkMars", "2018-03-01", "2018-03-31"),
    ]


def test_binding_leaf_carries_open_end_and_summary_only_coding(client):
    state = client.get("/api/catalog/scb/lisa/kon").json()["states"][0]
    # #321: an OPEN-ENDED state (valid_to = the 9999-12-31 sentinel) has no
    # finite period token — the field is None (the SPA renders "since
    # valid_from").
    assert state["period_token"] is None
    # Y-46: the leaf carries the coding's IDENTITY and a cardinality-independent
    # summary, never its members — those come from
    # `/api/value-sets/{id}/codes` when the code panel is opened.
    assert state["value_set_id"] == "1"
    assert state["value_set"] is None
    assert state["value_set_summary"] == {"code_count": 2, "integer_range": None}


def test_states_and_binding_leaf_return_literal_delivery_text(client, catalog_db):
    provenance = (
        "errata:scoped-attributions\n"
        '[{"class":"omitted-column-in-version",'
        '"evidence":"The steward holds this delivery, which SCB omits.",'
        '"source_editions":["Höstterminen 2020"]}]'
    )
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "UPDATE variable_state SET provenance = ?, name = ?, description = ? "
            "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')",
            (provenance, "Supplied delivery name", "Supplied delivery description"),
        )

    resp = client.get("/api/catalog/scb/lisa/kon/states")

    assert resp.status_code == 200
    state = resp.json()["states"][0]
    assert state["provenance"] == provenance
    assert state["name"] == "Supplied delivery name"
    assert state["description"] == "Supplied delivery description"
    leaf = client.get("/api/catalog/scb/lisa/kon")
    assert leaf.status_code == 200
    assert leaf.json()["states"][0]["name"] == state["name"]
    assert leaf.json()["states"][0]["description"] == state["description"]


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


def test_too_many_segments_returns_422(client):
    # Every segment is a valid slug, so the per-segment guard admits it; the
    # >3-segment arity is rejected by `reg_meta.fqid.parse` → 422 (a structural
    # grammar error, not a 404).
    resp = client.get("/api/catalog/scb/lisa/kon/extra/more")
    assert resp.status_code == 422
