"""`/api/catalog` browse against the slugged ``catalog_db`` fixture (§9.5).

Covers the root, each catch-all node kind (provider / register / binding leaf /
classification-root / classification), the discriminated-union ``kind`` tags, the
binding leaf's embedded longitudinal record, the register's ``variants`` ref
stub, and the 404-on-not-found mapping. The §16 path-traversal guard lives in
``test_fqid_validation.py``.
"""

from __future__ import annotations

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
    assert {p["fqid"] for p in providers} == {"scb", "sos"}
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
    assert {b["fqid"] for b in bindings} == {"scb/lisa/kon"}
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
    # The value set is hydrated as (code, label) objects.
    assert state["value_set"] == [
        {"code": "1", "label": "Man"},
        {"code": "2", "label": "Kvinna"},
    ]
    # The same_as edge (kon → rams/syss) is embedded, fqid serialized as a string.
    assert any(ref["fqid"] == "scb/rams/syss" for ref in body["same_as"])
    # Edge collections are present (possibly empty) — the leaf carries all four.
    for field in ("replaced_by", "related_to", "lineage"):
        assert field in body


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


def test_classification_leaf_resolves(client):
    resp = client.get("/api/catalog/class/sun2020")
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "classification"
    assert body["fqid"] == "class/sun2020"
    assert body["short_name"] == "SUN2020"


def test_missing_provider_returns_404(client):
    resp = client.get("/api/catalog/nope")
    assert resp.status_code == 404


def test_missing_binding_returns_404(client):
    resp = client.get("/api/catalog/scb/lisa/doesnotexist")
    assert resp.status_code == 404


def test_too_many_segments_returns_422(client):
    # Every segment is a valid slug, so the §16 per-segment guard admits it; the
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
