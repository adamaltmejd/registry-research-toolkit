"""Filtered-steward scoping of the catalog BROWSE and SEARCH surfaces (#859).

A steward holding ONLY ``scb/lisa/kon`` (one binding under
``scb/lisa/individer-15plus``) is booted against the shared catalog fixture DB
(``scb/lisa`` with bindings kon/lonfink + ``scb/rams`` with syss/inkjan/inkfeb,
the ``ink`` concept group, the sun classification family). Every catalog
discovery surface must scope to that single holding:

- BROWSE: root keeps only the held provider (+ the always-on classification
  root); the provider lists only the held register; the register lists only the
  held binding and drops the unheld concept group; the binding leaf narrows its
  states to held columns; an unheld binding / register / variant 404s; a
  dead-slug citation 301s only when its terminal successor is HELD;
  classifications pass through unchanged (decision 2).
- SEARCH: the register/variable groups are scoped to held FQIDs with an EXACT
  ``total_count``; the classification group passes through.

The ``global`` deployment (no index) is covered by the existing
``test_catalog_browse`` / ``test_catalog_subendpoints`` / ``test_search`` suites
— this file asserts ONLY the filtered delta, so a regression that drops the
``index is None`` guard surfaces there, and a regression that drops the filter
surfaces here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _steward_helpers import write_steward as _write_steward
from fastapi.testclient import TestClient
from reg_webapp.app import create_app

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# The steward holds ONLY scb/lisa/kon — so scb/rams (and lisa's other bindings)
# are out of catalog, and the sun classification family stays catalog-global.
_HELD_SOURCES = [
    {
        "name": "lisa",
        "register_variant": "scb/lisa/individer-15plus",
        "period": 2018,
        "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
    }
]


@pytest.fixture
def steward_client(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """A booted app filtered to the ``ifau`` steward holding only scb/lisa/kon,
    pointed at the shared ``catalog_db`` fixture."""
    stewards = tmp_path / "stewards"
    _write_steward(stewards, "ifau", _HELD_SOURCES)
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "ifau")
    with TestClient(create_app()) as client:
        # Sanity: the index actually built around the single holding.
        assert client.app.state.catalog_index is not None
        yield client


# ── BROWSE ──────────────────────────────────────────────────────────────────


def test_root_keeps_held_provider_and_classification_root(steward_client):
    body = steward_client.get("/api/catalog").json()
    kinds = {c["kind"] for c in body["children"]}
    providers = [c["fqid"] for c in body["children"] if c["kind"] == "provider"]
    assert providers == ["scb"]  # the only held provider
    assert "classification-root" in kinds  # ALWAYS appended (pass-through)


def test_provider_lists_only_held_register(steward_client):
    body = steward_client.get("/api/catalog/scb").json()
    registers = {c["fqid"] for c in body["children"]}
    assert registers == {"scb/lisa"}  # scb/rams is not held


def test_register_lists_only_held_binding_and_drops_unheld_group(steward_client):
    body = steward_client.get("/api/catalog/scb/lisa").json()
    bindings = {c["fqid"] for c in body["children"] if c["kind"] == "binding"}
    assert bindings == {"scb/lisa/kon"}  # lonfink (unheld) is dropped
    # The `ink` group lives under scb/rams (unheld) — and lisa has no held group —
    # so the register's groups list is empty for this steward.
    assert body["groups"] == []


def test_held_binding_leaf_narrows_states_to_held_columns(steward_client):
    body = steward_client.get("/api/catalog/scb/lisa/kon").json()
    assert body["kind"] == "binding"
    columns = {s["delivery_column_name"] for s in body["states"]}
    assert columns == {"Kon"}  # the held column; nothing else leaks


def test_unheld_binding_leaf_404s(steward_client):
    # lonfink is a live lisa binding the steward does not hold.
    resp = steward_client.get("/api/catalog/scb/lisa/lonfink")
    assert resp.status_code == 404
    assert "catalog" in resp.json()["detail"].lower()


def test_unheld_register_binding_404s(steward_client):
    resp = steward_client.get("/api/catalog/scb/rams/syss")
    assert resp.status_code == 404


def test_unheld_register_variants_404(steward_client):
    resp = steward_client.get("/api/catalog/scb/rams/variants")
    assert resp.status_code == 404


def test_held_register_variants_filtered_to_held_coords(steward_client):
    body = steward_client.get("/api/catalog/scb/lisa/variants").json()
    slugs = {v["slug"] for v in body["variants"]}
    # Only the variant the steward holds a binding under.
    assert slugs == {"individer-15plus"}


def test_states_subendpoint_narrows_and_gates(steward_client):
    held = steward_client.get("/api/catalog/scb/lisa/kon/states").json()
    assert {s["delivery_column_name"] for s in held["states"]} == {"Kon"}
    # An unheld binding's /states 404s.
    assert steward_client.get("/api/catalog/scb/rams/syss/states").status_code == 404


def test_successors_subendpoint_gated_and_narrowed(steward_client):
    # kon → rams/syss is a succession edge, but rams/syss is UNHELD, so the held
    # subject (kon) returns an empty (narrowed) successor list — not the syss ref.
    body = steward_client.get("/api/catalog/scb/lisa/kon/successors").json()
    assert body["successors"] == []


def test_related_subendpoint_gated_and_narrowed(steward_client):
    body = steward_client.get("/api/catalog/scb/lisa/kon/related").json()
    assert body["related"] == []  # the only related (rams/syss) is unheld


def test_period_query_gates_and_narrows(steward_client):
    held = steward_client.get("/api/catalog/scb/lisa/kon?period=2018").json()
    assert {s["delivery_column_name"] for s in held["states"]} == {"Kon"}
    assert (
        steward_client.get("/api/catalog/scb/rams/syss?period=2019").status_code == 404
    )


def test_dead_slug_redirects_only_to_held_successor(steward_client):
    # renamed-head → renamed-mid → scb/rams/syss (UNHELD terminal) → 404, no 301.
    resp = steward_client.get(
        "/api/catalog/scb/lisa/renamed-head", follow_redirects=False
    )
    assert resp.status_code == 404


def test_classification_passthrough(steward_client):
    # Decision 2: classifications + the classification root pass through unchanged
    # for a filtered steward (the index has no classification holdings).
    assert steward_client.get("/api/catalog/class/sun2020").status_code == 200
    root = steward_client.get("/api/catalog/class").json()
    assert any(c["fqid"] == "class/sun2020" for c in root["children"])


# ── SEARCH ────────────────────────────────────────────────────────────────────


def test_search_scopes_register_and_variable_with_exact_count(steward_client):
    # "kon" matches the held lisa/kon variable; rams/syss etc. are filtered out.
    body = steward_client.get("/api/search?q=kon").json()
    groups = {g["group"]: g for g in body["groups"]}
    var_fqids = {r.get("fqid") for r in groups["variables"]["results"]}
    assert var_fqids <= {"scb/lisa/kon"}
    # total_count is query-time-exact (no unheld variable inflates it).
    assert groups["variables"]["total_count"] == len(groups["variables"]["results"])


def test_search_classification_group_passes_through(steward_client):
    # A classification query is catalog-global — the sun family still surfaces.
    body = steward_client.get("/api/search?q=utbildning&type=classification").json()
    groups = {g["group"]: g for g in body["groups"]}
    assert "classifications" in groups
