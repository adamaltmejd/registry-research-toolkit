"""`/api/catalog` browse against the slugged ``catalog_db`` fixture.

See DESIGN.md → Catalog router structure. Covers the root, each catch-all node
kind (provider / register / binding leaf /
classification-root / classification), the discriminated-union ``kind`` tags, the
binding leaf's embedded longitudinal record, the register's ``variants`` ref
stub, and the 404-on-not-found mapping. The path-traversal guard lives in
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
    assert group["axes"] == ["month"]
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


def test_register_without_groups_has_empty_list(client):
    body = client.get("/api/catalog/scb/lisa").json()
    assert body["groups"] == []


def test_classification_root_carries_vintage_groups(client):
    """#303: the classification root carries the derived vintage groups; the
    grouped classifications ALSO stay in `children`."""
    body = client.get("/api/catalog/class").json()
    assert len(body["groups"]) == 1
    group = body["groups"][0]
    assert group["key"] == "sun"
    assert group["axes"] == ["vintage"]
    assert [m["fqid"] for m in group["members"]] == [
        "class/sun2000",
        "class/sun2020",
    ]
    assert group["members"][1]["facets"] == [
        {"axis": "vintage", "value": "2020", "label": "2020"}
    ]
    assert {"class/sun2000", "class/sun2020"} <= {c["fqid"] for c in body["children"]}


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


def test_dead_binding_with_period_still_404_no_redirect(client):
    """The redirect lives ONLY on the no-period node path; the `?period` branch
    resolves via `resolve_at` and stays 404. This guards the intentional
    deferral — a dead slug WITH `?period` (here the chain head `renamed-head`,
    which redirects 301 without a period) must NOT silently turn into a
    redirect."""
    resp = client.get(
        "/api/catalog/scb/lisa/renamed-head?period=2019", follow_redirects=False
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


def test_state_model_period_token_is_the_coarsest_exact_token():
    """#321: `_state_model` exposes `period_token_for_bounds(valid_from,
    valid_to)` for a CLOSED window — the coarsest token that expands back to
    exactly the window (term/quarter/month/year), or the explicit `lo..hi`
    range for a non-grammar window — and None for the open-ended sentinel."""
    from types import SimpleNamespace

    from reg_webapp.routes.catalog import _state_model

    def stub(valid_from: str, valid_to: str):
        return SimpleNamespace(
            state_id=1,
            variant="v",
            register_variant_id=1,
            valid_from=valid_from,
            valid_to=valid_to,
            data_type=None,
            data_length=None,
            delivery_column_name=None,
            value_set_version_label="",
            value_set_id=None,
            value_set=None,
            is_identifier=False,
            classification_slug=None,
        )

    cases = [
        ("2018-01-01", "2018-12-31", "2018"),  # year-grain → bare year
        ("2009-01-01", "2009-06-30", "VT2009"),  # spring term
        ("2020-07-01", "2020-09-30", "2020-Q3"),  # quarter
        ("2020-02-01", "2020-02-29", "2020-02"),  # month (leap)
        ("1992-01-01", "2009-12-31", "1992-01-01..2009-12-31"),  # multi-year
        ("2018-01-01", "9999-12-31", None),  # open-ended sentinel
    ]
    for valid_from, valid_to, expected in cases:
        assert _state_model(stub(valid_from, valid_to)).period_token == expected
