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
from reg_webapp.catalog_index import CatalogIndex
from reg_webapp.routes.catalog import _narrow_refs_to_held

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


def _booted(stewards: Path, steward_id: str, sources: list[dict]) -> TestClient:
    """Boot a filtered-steward app for `steward_id` holding `sources` (the env seam
    + index-built sanity). Caller wraps in a `with`."""
    _write_steward(stewards, steward_id, sources)
    return TestClient(create_app())


@pytest.fixture
def syss_client(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """A steward holding ONLY scb/rams/syss — the HELD terminal successor of the
    kon→syss / renamed-chain edges. Exercises the live-unheld-binding 404 (Fix 2)
    and the dead-slug→held-successor 301 (gap 1), which the kon-only steward can't
    (its successor is unheld)."""
    stewards = tmp_path / "stewards"
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "ifau")
    with _booted(
        stewards,
        "ifau",
        [
            {
                "name": "rams",
                "register_variant": "scb/rams/standard",
                "period": 2019,
                "bindings": [{"variable": "scb/rams/syss", "type": "numeric"}],
            }
        ],
    ) as client:
        assert client.app.state.catalog_index is not None
        yield client


@pytest.fixture
def both_client(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """A steward holding BOTH scb/lisa/kon and scb/rams/syss — so a held subject's
    predecessor/successor edge (kon→syss) lands on another HELD binding. Backs the
    `/predecessors` narrow-to-held test (gap 7)."""
    stewards = tmp_path / "stewards"
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "ifau")
    with _booted(
        stewards,
        "ifau",
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
            },
            {
                "name": "rams",
                "register_variant": "scb/rams/standard",
                "period": 2019,
                "bindings": [{"variable": "scb/rams/syss", "type": "numeric"}],
            },
        ],
    ) as client:
        assert client.app.state.catalog_index is not None
        yield client


@pytest.fixture
def lonfink_jan_client(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """A steward holding ONLY the `LonFinkJan` column of the multi-column `lonfink`
    variable (#319 merged monthly family — 3 columns Jan/Feb/Mars). Backs the
    partial-column-hold test (gap 7 nice-to-have): the leaf `states` show ONLY the
    held column."""
    stewards = tmp_path / "stewards"
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "ifau")
    with _booted(
        stewards,
        "ifau",
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [
                    {
                        "variable": "scb/lisa/lonfink",
                        "type": "numeric",
                        "representation": "LonFinkJan",
                    }
                ],
            }
        ],
    ) as client:
        assert client.app.state.catalog_index is not None
        yield client


@pytest.fixture
def global_client(catalog_db: Path) -> Iterator[TestClient]:
    """The `global` deployment (no steward env → no index): the full universe, so
    the unheld-provider / unheld-register paths still serve 200. Asserts the Fix 1
    gate is index-gated, not unconditional."""
    with TestClient(create_app()) as client:
        assert client.app.state.catalog_index is None
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


def test_search_register_arm_scoped_to_held(steward_client):
    # gap 5: the REGISTER search arm is scoped to held registers. The steward holds
    # scb/lisa; scb/rams is unheld.
    held = steward_client.get("/api/search?q=lisa&type=register").json()
    held_grp = {g["group"]: g for g in held["groups"]}["registers"]
    assert {r["fqid"] for r in held_grp["results"]} == {"scb/lisa"}
    # Query-time-exact: no unheld register inflates the count.
    assert held_grp["total_count"] == len(held_grp["results"])

    unheld = steward_client.get("/api/search?q=rams&type=register").json()
    unheld_grp = {g["group"]: g for g in unheld["groups"]}["registers"]
    assert unheld_grp["results"] == []  # scb/rams filtered out
    assert unheld_grp["total_count"] == 0  # exact, not the pre-filter universe count


# ── NEW-BEHAVIOR regressions (Fix 1 / Fix 2) ─────────────────────────────────


def test_unheld_live_register_404s(steward_client):
    # Fix 1: scb/rams resolves LIVE but the steward (holds only scb/lisa) does not
    # hold it — so the bare REGISTER node 404s, not 200-with-a-dead-end-variants-ref.
    resp = steward_client.get("/api/catalog/scb/rams")
    assert resp.status_code == 404
    assert "catalog" in resp.json()["detail"].lower()


def test_unheld_live_provider_404s(steward_client):
    # Fix 1: `sos` is a seeded provider (resolves LIVE) the steward does not hold.
    resp = steward_client.get("/api/catalog/sos")
    assert resp.status_code == 404


def test_global_serves_unheld_provider_and_register_200(global_client):
    # Fix 1 is index-gated: the global deployment (no index) still serves the live
    # provider/register nodes 200 — the gate must not fire unconditionally.
    assert global_client.get("/api/catalog/scb").status_code == 200
    assert global_client.get("/api/catalog/scb/rams").status_code == 200


def test_live_unheld_binding_with_held_successor_404s_not_301(syss_client):
    # Fix 2: scb/lisa/kon is a LIVE binding whose terminal successor (scb/rams/syss)
    # IS held by this steward. A live unheld binding must 404 — NOT 301 to the held
    # successor (succession edges exist between live bindings; only a DEAD slug
    # redirects).
    resp = syss_client.get("/api/catalog/scb/lisa/kon", follow_redirects=False)
    assert resp.status_code == 404
    assert "catalog" in resp.json()["detail"].lower()


def test_dead_slug_301s_to_held_successor_query_preserved(syss_client):
    # gap 1: a DEAD/renamed slug whose terminal successor IS held 301s to the
    # successor URL, query string preserved. renamed-head → renamed-mid →
    # scb/rams/syss (HELD here), so the dead-slug citation stays alive.
    resp = syss_client.get(
        "/api/catalog/scb/lisa/renamed-head?period=2018", follow_redirects=False
    )
    assert resp.status_code == 301
    location = resp.headers["location"]
    assert location.endswith("/api/catalog/scb/rams/syss?period=2018")


def test_predecessors_narrows_to_held_and_gates(both_client):
    # gap 7: kon → syss is a succession edge, so syss's predecessor is kon. With BOTH
    # held, the held subject (syss) narrows its predecessors to the held kon ref.
    body = both_client.get("/api/catalog/scb/rams/syss/predecessors").json()
    assert {r["fqid"] for r in body["predecessors"]} == {"scb/lisa/kon"}
    # An unheld binding's /predecessors 404s (no such binding for this steward).
    assert (
        both_client.get("/api/catalog/scb/lisa/lonfink/predecessors").status_code == 404
    )


def test_partial_column_hold_narrows_leaf_states(lonfink_jan_client):
    # gap 7 (nice-to-have): the steward holds ONLY the LonFinkJan column of the
    # 3-column `lonfink` variable — the leaf states show ONLY that column.
    leaf = lonfink_jan_client.get("/api/catalog/scb/lisa/lonfink").json()
    assert leaf["kind"] == "binding"
    assert {s["delivery_column_name"] for s in leaf["states"]} == {"LonFinkJan"}
    # The ?period view (resolve_at subset) narrows to the held column too.
    period = lonfink_jan_client.get("/api/catalog/scb/lisa/lonfink?period=2018").json()
    assert {s["delivery_column_name"] for s in period["states"]} == {"LonFinkJan"}


def test_all_unheld_concept_group_404s(steward_client):
    # gap 6 (nice-to-have): the `ink` group lives on scb/rams (members inkjan/inkfeb),
    # none held by the kon-only steward → the group subject 404s.
    resp = steward_client.get("/api/catalog/group/scb/rams/ink")
    assert resp.status_code == 404
    assert "catalog" in resp.json()["detail"].lower()


# ── Unit: edge-ref narrowing ─────────────────────────────────────────────────


class _StubRef:
    """A minimal variable-grain edge ref (predecessors/successors/related carry a
    `fqid` field, str-able or None)."""

    def __init__(self, fqid: str | None) -> None:
        self.fqid = fqid


def test_narrow_refs_drops_fqid_none_and_unheld():
    # gap 6: `_narrow_refs_to_held` drops a ref with `fqid is None` (unaddressable —
    # in no steward catalog) and an unheld FQID; keeps the held one.
    index = CatalogIndex(
        bindings_by_variant={
            "scb/lisa/individer-15plus": frozenset({("scb/lisa/kon", "Kon")})
        },
        period_range_by_register={"scb/lisa": ("2018", "2018")},
        drift_warnings=(),
    )
    refs = [_StubRef("scb/lisa/kon"), _StubRef(None), _StubRef("scb/rams/syss")]
    kept = _narrow_refs_to_held(refs, index)
    assert [r.fqid for r in kept] == ["scb/lisa/kon"]
