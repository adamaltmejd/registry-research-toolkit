"""`/api/catalog` tree browse against the slugged ``catalog_db`` fixture.

See DESIGN.md → Catalog router structure. Covers the root, provider and register
nodes, the binding leaf's embedded longitudinal record, the register's
``variants`` ref stub, and the 404/422 mapping for missing or over-long paths.
The path-traversal guard lives in ``test_fqid_validation.py``.
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
    by_fqid = {c["fqid"]: c for c in body["children"]}
    assert [tag["slug"] for tag in by_fqid["scb/lisa"]["tags"]] == ["income"]
    assert [tag["slug"] for tag in by_fqid["scb/rams"]["tags"]] == ["employment"]


def test_register_node_lists_bindings_and_variants_ref(client):
    resp = client.get("/api/catalog/scb/lisa")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "register"
    assert body["fqid"] == "scb/lisa"
    bindings = [c for c in body["children"] if c["kind"] == "binding"]
    # `lonfink` is the merged monthly-family binding (#319) seeded alongside `kon`;
    # `forsamling` is the many-state / shared-coding binding (Y-46); `lan` is the
    # interrupted-delivery binding and `forvink-ers` the rename-chain one (Y-109).
    assert {b["fqid"] for b in bindings} == {
        "scb/lisa/kon",
        "scb/lisa/lonfink",
        "scb/lisa/forsamling",
        "scb/lisa/lan",
        "scb/lisa/forvink-ers",
    }
    # The variant-browser slot (A5.2a, wired): carries the navigable register_fqid.
    variants_refs = [c for c in body["children"] if c["kind"] == "variants-ref"]
    assert len(variants_refs) == 1
    assert variants_refs[0]["register_fqid"] == "scb/lisa"


def test_register_children_carry_their_delivery_columns(client):
    """Y-82: each binding child names the `(variant, column)` pairs it is
    delivered under, with each pair's OWN window — the columns the register page
    shows beside the variable (and filters on) and the variants its chips narrow
    by. `kon` is delivered as `Kon` by the single `individer-15plus` variant.

    Y-104: and with that pair's DISJOINT `windows` beside the span, which is what
    the register list shows and matches on."""
    body = client.get("/api/catalog/scb/lisa").json()
    by_fqid = {c["fqid"]: c for c in body["children"] if c["kind"] == "binding"}
    assert by_fqid["scb/lisa/kon"]["deliveries"] == [
        {
            "variant": "individer-15plus",
            "column": "Kon",
            "period_scope": "intervals",
            "coverage": {
                "coverage_from": "2018-01-01",
                "coverage_to": None,
                "open_ended": True,
                "state_count": 1,
            },
            "windows": [{"valid_from": "2018-01-01", "valid_to": "9999-12-31"}],
        }
    ]


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
    assert state["classifications"] == []
    assert state["provenance"] is None
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
    # #892/#932: the per-(split-)variable distinguishing text is serialized on the
    # binding leaf. The fixture kon row leaves it empty → None; the field is always
    # present (proves the resolve→node wiring).
    assert "operational_definition" in body
    assert body["operational_definition"] is None
    # The same_as edge (kon → rams/syss) is embedded, fqid serialized as a string.
    assert any(ref["fqid"] == "scb/rams/syss" for ref in body["same_as"])
    # Edge collections are present (possibly empty); #582 replaced the immediate
    # `replaced_by` embed with the full `succession_chain` (asserted in detail in
    # test_binding_leaf_embeds_full_succession_chain).
    for field in ("succession_chain", "lineage"):
        assert field in body


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


def test_binding_leaf_omits_lineage_warnings(client):
    """ResolvedVariable doesn't carry lineage_warnings, so the leaf must NOT
    expose them (they arrive via A5.2's /lineage_warnings)."""
    resp = client.get("/api/catalog/scb/lisa/kon")
    assert "lineage_warnings" not in resp.json()


def test_missing_provider_returns_404(client):
    resp = client.get("/api/catalog/nope")
    assert resp.status_code == 404


def test_missing_binding_returns_404(client):
    resp = client.get("/api/catalog/scb/lisa/doesnotexist")
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
