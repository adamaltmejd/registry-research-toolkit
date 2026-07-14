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

import sqlite3
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
def lonfink_rep_both_client(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """A steward holding BOTH the LonFinkJan and LonFinkFeb columns of
    `scb/lisa/lonfink` — i.e. BOTH representation members of the `lonefink-rep`
    concept group (#819). Backs the Fix-1 test: a label-matched variable search folds
    into the HELD group, whose (fqid-less) group row must SURVIVE `_scope_to_fqids`
    (not be dropped) and be counted in `total_count`, with both held members intact."""
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
                    },
                    {
                        "variable": "scb/lisa/lonfink",
                        "type": "numeric",
                        "representation": "LonFinkFeb",
                    },
                ],
            }
        ],
    ) as client:
        assert client.app.state.catalog_index is not None
        yield client


@pytest.fixture
def lonfink_rep_jan_client(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """A steward holding ONLY the `LonFinkJan` column of `scb/lisa/lonfink` — one of
    the TWO representation members (LonFinkJan / LonFinkFeb, same FQID) of the
    `lonefink-rep` concept group (#819). Backs the Fix-2 test: reg_meta narrows group
    members at FQID grain (so it keeps BOTH representations — the FQID is held), and
    only the webapp's column-grain refinement drops the unheld LonFinkFeb."""
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


def _set_steward_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, steward_id: str = "ifau"
) -> Path:
    stewards = tmp_path / "stewards"
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", steward_id)
    return stewards


def _seed_partial_column_lineage(catalog_db: Path) -> dict[str, int]:
    """Seed one two-column variable whose lineage/warnings attach to both states."""
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (9901, 1, '9901', 'Disponibel inkomst steward', 'disp')"
        )
        for col in ("CDISP", "CDISP5"):
            conn.execute(
                "INSERT INTO variable_alias "
                "(variable_id, register_variant_id, delivery_column_name) "
                "VALUES (9901, 10, ?)",
                (col,),
            )
        cdisp_state = conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, data_type, "
            "delivery_column_name) VALUES "
            "(9901, 10, '1968-01-01', '2024-12-31', 'int', 'CDISP')"
        ).lastrowid
        cdisp5_state = conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, data_type, "
            "delivery_column_name) VALUES "
            "(9901, 10, '2020-01-01', '2024-12-31', 'int', 'CDISP5')"
        ).lastrowid
        syss_state = conn.execute(
            "SELECT state_id FROM variable_state WHERE variable_id = "
            "(SELECT variable_id FROM variable WHERE register_id = 2 AND slug = 'syss')"
        ).fetchone()[0]
        for state_id, col in ((cdisp_state, "CDISP"), (cdisp5_state, "CDISP5")):
            conn.execute(
                "INSERT INTO variable_state_lineage "
                "(consumer_state_id, source_state_id, valid_from, valid_to) "
                "VALUES (?, ?, '2020-01-01', '2024-12-31')",
                (state_id, syss_state),
            )
            conn.execute(
                "INSERT INTO variable_state_lineage_warning "
                "(consumer_state_id, warning_kind, message) VALUES "
                "(?, 'no_source_state', ?)",
                (state_id, f"missing source for {col}"),
            )
        conn.commit()
    return {"CDISP": cdisp_state, "CDISP5": cdisp5_state}


def _seed_search_backfill_groups(catalog_db: Path) -> None:
    """Seed one dropped and one kept representation group for the same search label."""
    with sqlite3.connect(catalog_db) as conn:
        lonfink_id = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = 'lonfink'"
        ).fetchone()[0]
        for group_id, key, col, value in (
            (9902, "a-backfill-drop", "LonFinkFeb", "drop-a"),
            (9903, "b-backfill-drop", "LonFinkFeb", "drop-b"),
            (9904, "z-backfill-keep", "LonFinkJan", "keep"),
        ):
            conn.execute(
                "INSERT INTO concept_group "
                "(group_id, kind, register_id, group_key, label, source) "
                "VALUES (?, 'variable', 1, ?, 'Backfill family', 'curated')",
                (group_id, key),
            )
            conn.execute(
                "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
                "VALUES (?, 'rep', 0, 'Representation')",
                (group_id,),
            )
            cur = conn.execute(
                "INSERT INTO concept_group_variable "
                "(group_id, variable_id, delivery_column_name) VALUES (?, ?, ?)",
                (group_id, lonfink_id, col),
            )
            conn.execute(
                "INSERT INTO concept_group_variable_facet "
                "(member_id, axis, value, label) VALUES (?, 'rep', ?, ?)",
                (cur.lastrowid, value, value),
            )
        conn.commit()


def _seed_kon_same_register_alias(catalog_db: Path) -> None:
    """Seed a same-register alias that resolves to the live `kon` variable."""
    with sqlite3.connect(catalog_db) as conn:
        for a, b in (
            (("scb", "lisa", "kon-alias"), ("scb", "lisa", "kon")),
            (("scb", "lisa", "kon"), ("scb", "lisa", "kon-alias")),
        ):
            conn.execute(
                "INSERT INTO variable_same_as "
                "(a_provider, a_register, a_variable, "
                "b_provider, b_register, b_variable) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (*a, *b),
            )
        conn.commit()


def _seed_unnamed_column_variable(catalog_db: Path) -> None:
    """Seed a held variable whose only state has no delivery column name."""
    with sqlite3.connect(catalog_db) as conn:
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (9905, 1, '9905', 'Unnamed steward column', 'unnamed')"
        )
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, data_type) "
            "VALUES (9905, 10, '2017-01-01', '2019-12-31', 'int')"
        )
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, data_type, "
            "delivery_column_name) "
            "VALUES (9905, 10, '2020-01-01', '2024-12-31', 'int', 'UNNAMED_NAMED')"
        )
        conn.commit()


def _seed_steward_tag_scope_group(catalog_db: Path) -> None:
    """Seed a group where only the unheld sibling carries a thematic tag."""
    with sqlite3.connect(catalog_db) as conn:
        held_id = conn.execute(
            "INSERT INTO variable "
            "(variable_id, register_id, provider_key, name, slug) "
            "VALUES (9906, 1, '9906', 'Held untagged', 'helduntagged')"
        ).lastrowid
        tagged_id = conn.execute(
            "INSERT INTO variable "
            "(variable_id, register_id, provider_key, name, slug) "
            "VALUES (9907, 1, '9907', 'Unheld tagged', 'unheldtagged')"
        ).lastrowid
        conn.execute(
            "INSERT INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "VALUES (?, 10, 'HeldUntagged')",
            (held_id,),
        )
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, data_type, "
            "delivery_column_name) "
            "VALUES (?, 10, '2018-01-01', '9999-12-31', 'int', 'HeldUntagged')",
            (held_id,),
        )
        conn.execute(
            "INSERT INTO concept_group "
            "(group_id, kind, register_id, group_key, label, source) "
            "VALUES (9910, 'variable', 1, 'steward-tags', "
            "'Steward tag scope', 'curated')"
        )
        conn.executemany(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) VALUES (9910, ?, NULL)",
            [(held_id,), (tagged_id,)],
        )
        tag_id = conn.execute(
            "INSERT INTO tag (slug, label, description) VALUES (?, ?, ?)",
            ("assets", "Assets", "Fixture tag held only by the unheld sibling."),
        ).lastrowid
        conn.execute(
            "INSERT INTO tag_member "
            "(tag_id, register_id, variable_id, rank, starred, note) "
            "VALUES (?, NULL, ?, 0, 1, 'unheld sibling only')",
            (tag_id, tagged_id),
        )
        conn.commit()


def _seed_representation_member_tag_scope_group(catalog_db: Path) -> None:
    """Seed a representation group where the leaf is held through another column."""
    with sqlite3.connect(catalog_db) as conn:
        target_id = conn.execute(
            "INSERT INTO variable "
            "(variable_id, register_id, provider_key, name, slug) "
            "VALUES (9908, 1, '9908', 'Held through another representation', "
            "'targetrep')"
        ).lastrowid
        tagged_id = conn.execute(
            "INSERT INTO variable "
            "(variable_id, register_id, provider_key, name, slug) "
            "VALUES (9909, 1, '9909', 'Held tagged sibling', 'heldtaggedsibling')"
        ).lastrowid
        for column in ("TargetGroupColumn", "TargetHeldColumn"):
            conn.execute(
                "INSERT INTO variable_alias "
                "(variable_id, register_variant_id, delivery_column_name) "
                "VALUES (?, 10, ?)",
                (target_id, column),
            )
            conn.execute(
                "INSERT INTO variable_alias_window "
                "(variable_id, register_variant_id, delivery_column_name, "
                "valid_from, valid_to) VALUES (?, 10, ?, '2018-01-01', "
                "'2018-12-31')",
                (target_id, column),
            )
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, data_type, "
            "delivery_column_name) "
            "VALUES (?, 10, '2018-01-01', '2018-12-31', 'int', "
            "'TargetGroupColumn')",
            (target_id,),
        )
        conn.execute(
            "INSERT INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "VALUES (?, 10, 'HeldTaggedSibling')",
            (tagged_id,),
        )
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, data_type, "
            "delivery_column_name) "
            "VALUES (?, 10, '2018-01-01', '2018-12-31', 'int', "
            "'HeldTaggedSibling')",
            (tagged_id,),
        )
        conn.execute(
            "INSERT INTO concept_group "
            "(group_id, kind, register_id, group_key, label, source) "
            "VALUES (9912, 'variable', 1, 'rep-steward-tags', "
            "'Representation steward tag scope', 'curated')"
        )
        conn.execute(
            "INSERT INTO concept_group_axis (group_id, axis, ordinal, label) "
            "VALUES (9912, 'rep', 0, 'Representation')"
        )
        for variable_id, column, value in (
            (target_id, "TargetGroupColumn", "target-dropped"),
            (tagged_id, "HeldTaggedSibling", "tagged-kept"),
        ):
            cur = conn.execute(
                "INSERT INTO concept_group_variable "
                "(group_id, variable_id, delivery_column_name) "
                "VALUES (9912, ?, ?)",
                (variable_id, column),
            )
            conn.execute(
                "INSERT INTO concept_group_variable_facet "
                "(member_id, axis, value, label) VALUES (?, 'rep', ?, ?)",
                (cur.lastrowid, value, value),
            )
        tag_id = conn.execute(
            "INSERT INTO tag (slug, label, description) VALUES (?, ?, ?)",
            (
                "rep-scope",
                "Representation scope",
                "Fixture tag held only by the kept representation sibling.",
            ),
        ).lastrowid
        conn.execute(
            "INSERT INTO tag_member "
            "(tag_id, register_id, variable_id, rank, starred, note) "
            "VALUES (?, NULL, ?, 0, 1, 'kept representation sibling only')",
            (tag_id, tagged_id),
        )
        conn.commit()


def _seed_kon_single_member_group(catalog_db: Path) -> None:
    """Put canonical `kon` in a group so alias leaf tags use the scoped path."""
    with sqlite3.connect(catalog_db) as conn:
        kon_id = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = 'kon'"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO concept_group "
            "(group_id, kind, register_id, group_key, label, source) "
            "VALUES (9911, 'variable', 1, 'alias-tags', 'Alias tags', 'curated')"
        )
        conn.execute(
            "INSERT INTO concept_group_variable "
            "(group_id, variable_id, delivery_column_name) VALUES (9911, ?, NULL)",
            (kon_id,),
        )
        conn.commit()


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
    assert not groups["variables"]["has_more"]


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
    assert not held_grp["has_more"]

    unheld = steward_client.get("/api/search?q=rams&type=register").json()
    unheld_grp = {g["group"]: g for g in unheld["groups"]}["registers"]
    assert unheld_grp["results"] == []  # scb/rams filtered out
    assert not unheld_grp["has_more"]


def test_search_held_concept_group_row_survives_scope(lonfink_rep_both_client):
    # Fix 1: a variable search that FOLDS (by label match) into the held `lonefink-rep`
    # group must return the GROUP row — not drop it. The group row has NO `fqid`, so the
    # old golden-boost re-filter (`... and str(f) in fqids`) wrongly dropped it and
    # shrank total_count. The fqid-less pass-through keeps it. The steward holds BOTH
    # representation members (LonFinkJan + LonFinkFeb), so both survive.
    body = lonfink_rep_both_client.get("/api/search?q=Lönefink&type=variable").json()
    grp = {g["group"]: g for g in body["groups"]}["variables"]
    group_rows = [r for r in grp["results"] if r["type"] == "group"]
    rep = next(r for r in group_rows if r["group_key"] == "lonefink-rep")
    # Both held representation columns survive the column-grain narrow.
    assert {m["delivery_column"] for m in rep["members"]} == {
        "LonFinkJan",
        "LonFinkFeb",
    }
    # total_count counts the surviving group row (it is NOT dropped).
    assert not grp["has_more"]
    assert len(grp["results"]) >= 1


def test_search_group_members_narrowed_to_held_column(lonfink_rep_jan_client):
    # Fix 2: the `lonefink-rep` group has two REPRESENTATION members sharing one FQID
    # (scb/lisa/lonfink) but different delivery columns (LonFinkJan / LonFinkFeb). The
    # steward holds ONLY LonFinkJan — reg_meta's FQID-grain narrow keeps BOTH (the FQID
    # is held), so the webapp must refine at COLUMN grain and drop the unheld LonFinkFeb.
    body = lonfink_rep_jan_client.get("/api/search?q=Lönefink&type=variable").json()
    grp = {g["group"]: g for g in body["groups"]}["variables"]
    rep = next(
        r
        for r in grp["results"]
        if r["type"] == "group" and r["group_key"] == "lonefink-rep"
    )
    cols = {m["delivery_column"] for m in rep["members"]}
    assert cols == {"LonFinkJan"}  # the unheld LonFinkFeb representation is excluded
    assert rep["member_count"] == 1  # member_count reset to the narrowed length
    # The group is KEPT (it has a surviving member), so total_count stays exact.
    assert not grp["has_more"]


def test_search_variable_leaf_column_chips_narrowed_to_held_column(
    lonfink_jan_client,
):
    body = lonfink_jan_client.get("/api/search?q=LonFink&type=variable").json()
    grp = {g["group"]: g for g in body["groups"]}["variables"]
    leaf = next(
        r
        for r in grp["results"]
        if r["type"] == "variable" and r["fqid"] == "scb/lisa/lonfink"
    )
    assert leaf["delivery_column_names"] == ["LonFinkJan"]


def test_search_variable_leaf_drops_unheld_delivery_alias_hit(lonfink_jan_client):
    body = lonfink_jan_client.get("/api/search?q=LonFinkFeb&type=variable").json()
    grp = {g["group"]: g for g in body["groups"]}["variables"]
    assert grp["results"] == []
    assert not grp["has_more"]


def test_search_backfills_after_all_unheld_group_drop(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _seed_search_backfill_groups(catalog_db)
    stewards = _set_steward_env(tmp_path, monkeypatch)
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
        body = client.get("/api/search?q=Backfill&type=variable&limit=1").json()
    grp = {g["group"]: g for g in body["groups"]}["variables"]
    assert [r["group_key"] for r in grp["results"]] == ["z-backfill-keep"]
    assert not grp["has_more"]


def test_search_group_column_grain_passthrough_global(global_client):
    # Fix 2 is index-gated: the global deployment (no index) shows BOTH representation
    # members of the `lonefink-rep` group — the column-grain narrow must not fire.
    body = global_client.get("/api/search?q=Lönefink&type=variable").json()
    grp = {g["group"]: g for g in body["groups"]}["variables"]
    rep = next(
        r
        for r in grp["results"]
        if r["type"] == "group" and r["group_key"] == "lonefink-rep"
    )
    assert {m["delivery_column"] for m in rep["members"]} == {
        "LonFinkJan",
        "LonFinkFeb",
    }


# ── NEW-BEHAVIOR regressions (Fix 1 / Fix 2) ─────────────────────────────────


def test_unheld_live_register_404s(steward_client):
    # Fix 1: scb/rams resolves LIVE but the steward (holds only scb/lisa) does not
    # hold it — so the bare REGISTER node 404s, not 200-with-a-dead-end-variants-ref.
    resp = steward_client.get("/api/catalog/scb/rams")
    assert resp.status_code == 404
    assert "catalog" in resp.json()["detail"].lower()


def test_live_unheld_register_with_held_successor_404s_not_301(steward_client):
    # CHANGE-1 (register-grain mirror of the binding-grain
    # test_live_unheld_binding_with_held_successor_404s_not_301): scb/rams is a LIVE
    # register the steward (holds only scb/lisa) does NOT hold, AND it carries a
    # `register_replaced_by` edge to the HELD scb/lisa. It must 404 — NOT 301 to the
    # held successor. This pins the closed leak: the old post-resolve admits_register
    # 404 inside _resolve_to_node was caught by get_catalog_node's generic
    # `except HTTPException → resolve_terminal_successor → 301` branch (no
    # live-vs-dead, no held check), wrongly redirecting a live unheld register. The
    # pre-resolve _require_admitted gate now 404s it before that branch is reached.
    resp = steward_client.get("/api/catalog/scb/rams", follow_redirects=False)
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


def test_partial_column_hold_coverage_uses_held_column(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _seed_partial_column_lineage(catalog_db)
    stewards = _set_steward_env(tmp_path, monkeypatch)
    with _booted(
        stewards,
        "ifau",
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2020,
                "bindings": [
                    {
                        "variable": "scb/lisa/disp",
                        "type": "numeric",
                        "representation": "CDISP5",
                    }
                ],
            }
        ],
    ) as client:
        provider = client.get("/api/catalog/scb").json()
        register = client.get("/api/catalog/scb/lisa").json()

    lisa = next(c for c in provider["children"] if c["fqid"] == "scb/lisa")
    assert lisa["coverage"] == {
        "variable_count": 1,
        "coverage_from": "2020-01-01",
        "coverage_to": "2024-12-31",
        "open_ended": False,
    }

    disp = next(c for c in register["children"] if c.get("fqid") == "scb/lisa/disp")
    assert disp["coverage"] == {
        "coverage_from": "2020-01-01",
        "coverage_to": "2024-12-31",
        "open_ended": False,
        "state_count": 1,
    }


def test_unnamed_column_hold_coverage_uses_variable_fallback(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _seed_unnamed_column_variable(catalog_db)
    stewards = _set_steward_env(tmp_path, monkeypatch)
    with _booted(
        stewards,
        "ifau",
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/unnamed", "type": "numeric"}],
            }
        ],
    ) as client:
        provider = client.get("/api/catalog/scb").json()
        register = client.get("/api/catalog/scb/lisa").json()

    lisa = next(c for c in provider["children"] if c["fqid"] == "scb/lisa")
    assert lisa["coverage"] == {
        "variable_count": 1,
        "coverage_from": "2017-01-01",
        "coverage_to": "2019-12-31",
        "open_ended": False,
    }

    unnamed = next(
        c for c in register["children"] if c.get("fqid") == "scb/lisa/unnamed"
    )
    assert unnamed["coverage"] == {
        "coverage_from": "2017-01-01",
        "coverage_to": "2019-12-31",
        "open_ended": False,
        "state_count": 1,
    }


def test_all_unheld_concept_group_404s(steward_client):
    # gap 6 (nice-to-have): the `ink` group lives on scb/rams (members inkjan/inkfeb),
    # none held by the kon-only steward → the group subject 404s.
    resp = steward_client.get("/api/catalog/group/scb/rams/ink")
    assert resp.status_code == 404
    assert "catalog" in resp.json()["detail"].lower()


def test_indexed_steward_tags_ignore_unheld_group_siblings(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _seed_steward_tag_scope_group(catalog_db)

    with TestClient(create_app()) as client:
        assert client.app.state.catalog_index is None
        global_group = client.get("/api/catalog/group/scb/lisa/steward-tags").json()
        global_leaf = client.get("/api/catalog/scb/lisa/helduntagged").json()

    assert [tag["slug"] for tag in global_group["tags"]] == ["assets"]
    assert [tag["slug"] for tag in global_leaf["tags"]] == ["assets"]
    assert global_leaf["tags"][0]["starred"] is False
    assert global_leaf["tags"][0]["note"] is None

    stewards = _set_steward_env(tmp_path, monkeypatch)
    with _booted(
        stewards,
        "ifau",
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/helduntagged", "type": "numeric"}],
            }
        ],
    ) as client:
        assert client.app.state.catalog_index is not None
        steward_group = client.get("/api/catalog/group/scb/lisa/steward-tags").json()
        steward_leaf = client.get("/api/catalog/scb/lisa/helduntagged").json()

    assert [m["fqid"] for m in steward_group["members"]] == ["scb/lisa/helduntagged"]
    assert steward_group["tags"] == []
    assert steward_leaf["tags"] == []


def test_indexed_steward_leaf_tags_require_target_representation_member(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _seed_representation_member_tag_scope_group(catalog_db)

    with TestClient(create_app()) as client:
        assert client.app.state.catalog_index is None
        global_group = client.get("/api/catalog/group/scb/lisa/rep-steward-tags").json()
        global_leaf = client.get("/api/catalog/scb/lisa/targetrep").json()

    assert [tag["slug"] for tag in global_group["tags"]] == ["rep-scope"]
    assert [tag["slug"] for tag in global_leaf["tags"]] == ["rep-scope"]

    stewards = _set_steward_env(tmp_path, monkeypatch)
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
                        "variable": "scb/lisa/targetrep",
                        "type": "numeric",
                        "representation": "TargetHeldColumn",
                    },
                    {
                        "variable": "scb/lisa/heldtaggedsibling",
                        "type": "numeric",
                        "representation": "HeldTaggedSibling",
                    },
                ],
            }
        ],
    ) as client:
        assert client.app.state.catalog_index is not None
        steward_group = client.get(
            "/api/catalog/group/scb/lisa/rep-steward-tags"
        ).json()
        steward_leaf = client.get("/api/catalog/scb/lisa/targetrep").json()

    assert [m["fqid"] for m in steward_group["members"]] == [
        "scb/lisa/heldtaggedsibling"
    ]
    assert [tag["slug"] for tag in steward_group["tags"]] == ["rep-scope"]
    assert steward_leaf["tags"] == []


def test_indexed_same_as_alias_leaf_keeps_canonical_direct_tags(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _seed_kon_same_register_alias(catalog_db)
    _seed_kon_single_member_group(catalog_db)
    stewards = _set_steward_env(tmp_path, monkeypatch)

    with _booted(
        stewards,
        "ifau",
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/kon-alias", "type": "numeric"}],
            }
        ],
    ) as client:
        assert client.app.state.catalog_index is not None
        body = client.get("/api/catalog/scb/lisa/kon-alias").json()

    assert body["kind"] == "binding"
    assert body["fqid"] == "scb/lisa/kon-alias"
    assert [tag["slug"] for tag in body["tags"]] == ["income"]
    assert body["tags"][0]["starred"] is True
    assert body["tags"][0]["note"] == "fixture recommendation"


# ── Fix 3: held binding leaf narrows embedded edges ──────────────────────────


def test_held_binding_leaf_narrows_embedded_edges(steward_client):
    # Fix 3: the kon-only steward holds scb/lisa/kon; its embedded same_as /
    # succession_chain / lineage all point at scb/rams/syss (UNHELD). The leaf must
    # narrow them to held — consistent with the already-narrowed sub-endpoints — so it
    # does NOT leak the unheld neighbor.
    body = steward_client.get("/api/catalog/scb/lisa/kon").json()
    assert body["kind"] == "binding"
    # same_as / lineage point only at the unheld syss → empty.
    assert body["same_as"] == []
    assert body["lineage"] == []
    # succession_chain keeps the held self-edition (kon), drops the unheld successor.
    assert {e["fqid"] for e in body["succession_chain"]} == {"scb/lisa/kon"}


def test_global_binding_leaf_shows_full_embedded_edges(global_client):
    # Fix 3 is index-gated: the global deployment (no index) still embeds the full
    # neighbor set on the kon leaf — the narrowing must not fire unconditionally.
    body = global_client.get("/api/catalog/scb/lisa/kon").json()
    assert {r["fqid"] for r in body["same_as"]} == {"scb/rams/syss"}
    assert {e["source_fqid"] for e in body["lineage"]} == {"scb/rams/syss"}
    assert {e["fqid"] for e in body["succession_chain"]} == {
        "scb/lisa/kon",
        "scb/rams/syss",
    }


def test_binding_graph_narrows_neighbor_nodes_to_held(steward_client):
    body = steward_client.get("/api/catalog/scb/lisa/kon/graph").json()
    node_ids = {n["id"] for n in body["nodes"]}
    assert node_ids == {"scb/lisa/kon"}
    assert body["edges"] == []
    kon_node = next(n for n in body["nodes"] if n["id"] == "scb/lisa/kon")
    assert kon_node["same_as"] == []


def test_binding_graph_keeps_held_same_as_alias_focus(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    _seed_kon_same_register_alias(catalog_db)
    stewards = _set_steward_env(tmp_path, monkeypatch)
    with _booted(
        stewards,
        "ifau",
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [{"variable": "scb/lisa/kon-alias", "type": "numeric"}],
            }
        ],
    ) as client:
        body = client.get("/api/catalog/scb/lisa/kon-alias/graph").json()

    assert body["focus_id"] == "scb/lisa/kon"
    assert {n["id"] for n in body["nodes"]} == {"scb/lisa/kon"}
    kon_node = body["nodes"][0]
    assert {s["delivery_column_name"] for s in kon_node["states"]} == {"Kon"}
    assert {r["fqid"] for r in kon_node["same_as"]} == {"scb/lisa/kon-alias"}


# ── Codex round 2 — Fix A: /lineage narrows source edges to held ─────────────


def test_lineage_subendpoint_narrows_source_edges_to_held(steward_client):
    # Fix A: kon's lineage consumes scb/rams/syss's state (a `source_fqid` the
    # kon-only steward does NOT hold). The standalone `/lineage` must narrow that
    # edge out — consistent with the leaf embed (Fix 3) and the other ref
    # sub-endpoints — so it does not leak the unheld source.
    body = steward_client.get("/api/catalog/scb/lisa/kon/lineage").json()
    assert body["lineage_edges"] == []


def test_global_lineage_subendpoint_shows_source_edge(global_client):
    # Fix A is index-gated: the global deployment (no index) still returns kon's
    # lineage edge with its real scb/rams/syss source — the narrow must not fire
    # unconditionally.
    body = global_client.get("/api/catalog/scb/lisa/kon/lineage").json()
    assert {e["source_fqid"] for e in body["lineage_edges"]} == {"scb/rams/syss"}


def test_lineage_and_warnings_narrow_to_held_consumer_column(
    catalog_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    states = _seed_partial_column_lineage(catalog_db)
    stewards = _set_steward_env(tmp_path, monkeypatch)
    with _booted(
        stewards,
        "ifau",
        [
            {
                "name": "lisa",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2020,
                "bindings": [
                    {
                        "variable": "scb/lisa/disp",
                        "type": "numeric",
                        "representation": "CDISP5",
                    }
                ],
            },
            {
                "name": "rams",
                "register_variant": "scb/rams/standard",
                "period": 2020,
                "bindings": [{"variable": "scb/rams/syss", "type": "numeric"}],
            },
        ],
    ) as client:
        lineage = client.get("/api/catalog/scb/lisa/disp/lineage").json()
        warnings = client.get("/api/catalog/scb/lisa/disp/lineage_warnings").json()

    assert [e["consumer_state_id"] for e in lineage["lineage_edges"]] == [
        states["CDISP5"]
    ]
    assert [w["consumer_state_id"] for w in warnings["lineage_warnings"]] == [
        states["CDISP5"]
    ]


# ── Codex round 2 — Fix B: provider-page coverage scoped to held variables ────


def test_provider_register_coverage_scoped_to_held_variables(steward_client):
    # Fix B: the kon-only steward sees scb/lisa with a per-register `coverage` that
    # counts ONLY the held kon variable — NOT the full-register aggregate (which
    # would also count the unheld lonfink → variable_count 2). The span is held-only
    # too: kon's single state is 2018-01-01 → open-ended.
    body = steward_client.get("/api/catalog/scb").json()
    lisa = next(c for c in body["children"] if c["fqid"] == "scb/lisa")
    cov = lisa["coverage"]
    assert cov["variable_count"] == 1  # kon only — lonfink (unheld) excluded
    assert cov["coverage_from"] == "2018-01-01"
    assert cov["open_ended"] is True  # kon's state is open-ended
    assert cov["coverage_to"] is None


def test_global_provider_register_coverage_counts_full_register(global_client):
    # Fix B is index-gated: the global deployment (no index) keeps the full-register
    # aggregate — scb/lisa counts BOTH kon and lonfink (and any other slugged var).
    body = global_client.get("/api/catalog/scb").json()
    lisa = next(c for c in body["children"] if c["fqid"] == "scb/lisa")
    # The full register holds more variables than the kon-only steward's 1.
    assert lisa["coverage"]["variable_count"] >= 2


# ── Fix 4: concept-group graph route is gated ────────────────────────────────


def test_unheld_concept_group_graph_404s(steward_client):
    # Fix 4: the `ink` group lives on scb/rams (members inkjan/inkfeb), none held by
    # the kon-only steward → the group's /graph route 404s (mirrors the subject route
    # `get_concept_group`), not a leaked graph.
    resp = steward_client.get("/api/catalog/group/scb/rams/ink/graph")
    assert resp.status_code == 404
    assert "catalog" in resp.json()["detail"].lower()


def test_unheld_register_concept_group_graph_404s(steward_client):
    # Fix 4: an unheld REGISTER's group graph 404s too (scb/rams is unheld).
    resp = steward_client.get("/api/catalog/group/scb/rams/nonexistent/graph")
    assert resp.status_code == 404


def test_held_concept_group_graph_200s(lonfink_rep_both_client):
    # Fix 4: a steward holding the `lonefink-rep` group's members reaches the group's
    # /graph (200) — the gate only blocks UNHELD groups/registers, mirroring the
    # subject route.
    resp = lonfink_rep_both_client.get("/api/catalog/group/scb/lisa/lonefink-rep/graph")
    assert resp.status_code == 200


def test_global_concept_group_graph_200s(global_client):
    # Fix 4 is index-gated: the global deployment serves the group graph unconditionally.
    resp = global_client.get("/api/catalog/group/scb/rams/ink/graph")
    assert resp.status_code == 200


def _graph_columns(graph: dict, fqid: str) -> set[str | None]:
    """The distinct `delivery_column_name`s a variable node's states carry."""
    node = next(
        n for n in graph["nodes"] if n["kind"] == "variable" and n.get("fqid") == fqid
    )
    return {s["delivery_column_name"] for s in node["states"]}


def test_held_concept_group_graph_narrows_states_to_held_columns(
    lonfink_rep_jan_client,
):
    # #678 finding 2: a steward holding ONLY LonFinkJan of the multi-column
    # `scb/lisa/lonfink` reaches the `lonefink-rep` group graph (the FQID is held), but
    # the graph node's states must be NARROWED to the held column — otherwise the
    # picker would surface the unheld LonFinkFeb as an addable column (the leaf path
    # already narrows; this mirrors it on the graph the group picker consumes).
    body = lonfink_rep_jan_client.get(
        "/api/catalog/group/scb/lisa/lonefink-rep/graph"
    ).json()
    assert _graph_columns(body, "scb/lisa/lonfink") == {"LonFinkJan"}


def test_global_concept_group_graph_keeps_all_columns(global_client):
    # #678 finding 2 is index-gated: the global deployment narrows nothing, so the
    # group graph node still carries EVERY delivery column of the variable — all three
    # month columns of `lonfink` (Jan/Feb/Mars), even though the `lonefink-rep` group
    # only has Jan/Feb members (member-scoping is the frontend picker's job, #678
    # round-2 finding 1; the variable-grain graph itself is un-narrowed when global).
    body = global_client.get("/api/catalog/group/scb/lisa/lonefink-rep/graph").json()
    assert _graph_columns(body, "scb/lisa/lonfink") == {
        "LonFinkJan",
        "LonFinkFeb",
        "LonFinkMars",
    }


def test_classification_group_graph_passthrough(steward_client):
    # Fix 4: classification group graphs are catalog-global (decision 2) — they pass
    # through unchanged for a filtered steward (the `sun` umbrella graph still serves).
    resp = steward_client.get("/api/catalog/group/class/sun/graph")
    assert resp.status_code == 200


# ── Unit: edge-ref narrowing ─────────────────────────────────────────────────


class _StubRef:
    """A minimal variable-grain edge ref (predecessors/successors carry a
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
