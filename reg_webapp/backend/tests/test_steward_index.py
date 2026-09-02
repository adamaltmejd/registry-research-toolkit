"""Steward catalog index build + boot behaviour.

See DESIGN.md → Steward layering and the in-memory catalog index (stewards.py +
catalog_index.py).

Covers:

(a) the committed ``inventory.toml`` filters the universe (register_variant coord
    → admitted ``(FQID, resolved delivery column)`` pairs, the per-coordinate
    union of edition intervals and its coarse per-register projection, unmapped
    columns admitting nothing);
(b) the membership probes ``fqid_outside_steward_catalog`` (A5.2b-ii) consults —
    variant-scoped for admission, variant-blind for discovery;
(c) **boot-survives-drift**: an inventory mapping reg_meta has drifted out from
    under still BOOTS — the coordinate drops from the index, a drift warning is
    recorded, ``app.state.catalog_index`` is populated, and ``/api/context``
    exposes the warning — NOT a crash. Both drift arms are pinned as UNIT tests;
    at the boot seam only the EDITION arm survives, because §12's consistency
    gate (``stewards.check_delivery_inventory``) is period-agnostic and fails
    startup on a coordinate the catalog does not name at all;
(d) the ``global`` deployment has no inventory and no index.

A filtered steward is selected at boot via ``REG_WEBAPP_STEWARD`` +
``REG_WEBAPP_STEWARDS_DIR`` (the static per-deployment selection seam),
both pointed at a tmp stewards dir holding a minimal ``ifau`` inventory.
"""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING

import pytest
import reg_meta.db
from _steward_helpers import (
    CLEAN_HOLDINGS as _CLEAN_HOLDINGS,
    catalog_index as _catalog_index,
    write_global as _write_global,
    write_steward as _write_steward,
)
from fastapi.testclient import TestClient
from reg_meta.catalog import Catalog, CatalogSizes
from reg_meta.inventory import DeliveryInventory
from reg_webapp.app import create_app
from reg_webapp.catalog_index import CatalogIndex, build_catalog_index

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def catalog(catalog_db):
    conn = reg_meta.db.open_db(catalog_db, check_schema=False)
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def _inventory(toml: str) -> DeliveryInventory:
    return DeliveryInventory.model_validate(tomllib.loads(toml))


# ── (a) the inventory filters the universe ─────────────────────────────────


def test_index_maps_variant_coord_to_bindings(catalog):
    index = _catalog_index(_CLEAN_HOLDINGS, catalog)

    assert set(index.bindings_by_variant) == {
        "scb/lisa/individer-15plus",
        "scb/rams/standard",
    }
    # #206: values are (FQID, resolved delivery column) pairs. Each mapping
    # states an explicit `representation`, which IS the resolved
    # `delivery_column_name` — never the steward's physical column spelling
    # (`P0_kon` / `P1_syss` in this fixture).
    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {("scb/lisa/kon", "Kon")}
    )
    assert index.bindings_by_variant["scb/rams/standard"] == frozenset(
        {("scb/rams/syss", "Syss")}
    )
    # Period-range map keyed by register FQID, over the mapping tables' EDITION
    # bounds — the edition-aware replacement for the pseudo-project's whole-history
    # `_default` admission.
    assert index.period_range_by_register == {
        "scb/lisa": ("2018-01-01", "2018-12-31"),
        "scb/rams": ("2019-01-01", "2019-12-31"),
    }
    assert index.drift_warnings == ()
    assert index.catalog_period_span == (2018, 2019)


def test_explicit_representation_is_admitted_without_touching_the_db():
    """The committed swecov inventory states an explicit `representation` on
    EVERY mapping, and that token IS reg_meta's canonical
    `delivery_column_name` — so boot admits it verbatim and performs ZERO
    resolution. Pinned with a catalog that raises on any access."""

    class ExplodingCatalog:
        def __getattr__(self, name: str):
            raise AssertionError(f"index build touched the catalog: {name}")

    inventory = _inventory("""
version = 1
steward = "ifau"

[[table]]
id = "LISA_Individ_2018.csv"
edition = 2018

[[table.column]]
name = "P1105_Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"
""")

    index = build_catalog_index(inventory, ExplodingCatalog())

    assert index.admits("scb/lisa/kon", "Kon")
    # The PHYSICAL column name is not the admitted token.
    assert not index.admits("scb/lisa/kon", "P1105_Kon")


def test_null_representation_resolves_to_the_states_columns(catalog):
    """`representation = None` means "the concept's SINGLE representation" (§12),
    never a wildcard — so it resolves against the catalog over the table's
    edition bounds, giving the token the researcher side compares against."""
    index = _catalog_index(
        [("scb/lisa/individer-15plus", "scb/lisa/kon", None, "2018")], catalog
    )

    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {("scb/lisa/kon", "Kon")}
    )
    assert index.drift_warnings == ()


def test_unmapped_column_admits_nothing(catalog):
    """An inventoried column with no mappings is coverage denominator ONLY (§12):
    it is delivered, but nothing about it is authorable or orderable."""
    index = build_catalog_index(
        _inventory("""
version = 1
steward = "ifau"

[[table]]
id = "LISA_Individ_2018.csv"
edition = 2018

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
representation = "Kon"

[[table.column]]
name = "SomeUnresolvedColumn"
"""),
        catalog,
    )

    assert index.admitted_variable_fqids == frozenset({"scb/lisa/kon"})
    assert index.drift_warnings == ()


def test_register_span_unions_every_contributing_edition(catalog):
    """The register's span is the union of the EDITION bounds of every table
    contributing an admitted mapping — an annual series spans its whole run."""
    index = _catalog_index(
        [
            ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon", "2018"),
            ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon", "2020-Q3"),
        ],
        catalog,
    )

    assert index.period_range_by_register == {"scb/lisa": ("2018-01-01", "2020-09-30")}
    assert index.catalog_period_span == (2018, 2020)


def test_coordinate_periods_merge_abutting_editions(catalog):
    """The per-coordinate map retains the UNION of its tables' edition bounds,
    not just an outer span. Consecutive annual editions really are one continuous
    run, so they collapse into a single interval."""
    index = _catalog_index(
        [
            ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon", "2018"),
            ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon", "2019"),
        ],
        catalog,
    )

    assert index.periods_by_coordinate == {
        ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon"): (
            ("2018-01-01", "2019-12-31"),
        )
    }


def test_coordinate_periods_keep_a_real_gap(catalog):
    """A coordinate delivered in 2018 and again in 2020 was NOT delivered in
    2019. The register-wide span cannot express that hole; the per-coordinate
    union must, because it is what the order lane slices against."""
    index = _catalog_index(
        [
            ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon", "2018"),
            ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon", "2020"),
        ],
        catalog,
    )

    assert index.periods_by_coordinate[
        ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon")
    ] == (("2018-01-01", "2018-12-31"), ("2020-01-01", "2020-12-31"))
    # The coarse projection flattens the hole — which is exactly why it is a UI
    # hint and the per-coordinate intervals are the retained truth.
    assert index.period_range_by_register == {"scb/lisa": ("2018-01-01", "2020-12-31")}


def test_coordinate_periods_are_keyed_per_coordinate_not_per_register(catalog):
    """Two variants of ONE register can be delivered over different editions —
    the reason the intervals are keyed by the whole §12 coordinate."""
    index = _catalog_index(
        [
            ("scb/rams/standard", "scb/rams/syss", "Syss", "2018"),
            ("scb/rams/quarterly", "scb/rams/syss", "Syss", "2020"),
        ],
        catalog,
    )

    assert index.periods_by_coordinate == {
        ("scb/rams/standard", "scb/rams/syss", "Syss"): (("2018-01-01", "2018-12-31"),),
        ("scb/rams/quarterly", "scb/rams/syss", "Syss"): (
            ("2020-01-01", "2020-12-31"),
        ),
    }
    assert index.period_range_by_register == {"scb/rams": ("2018-01-01", "2020-12-31")}


def test_catalog_period_span_is_null_when_nothing_is_admitted():
    index = CatalogIndex(
        bindings_by_variant={},
        periods_by_coordinate={},
        period_range_by_register={},
        drift_warnings=(),
    )

    assert index.catalog_period_span is None


# ── (b) membership probe (fqid_outside_steward_catalog backing) ────────────


def test_held_columns_for_variant_never_unions_across_variants(catalog):
    """Admission grain: a mapping states a `(register_variant, variable,
    representation)` coordinate, so a concept held under one variant is held
    under THAT variant only. `held_columns` stays a cross-variant union on
    purpose — it backs the browse/search listings, which carry their own variant
    axis."""
    index = _catalog_index(_CLEAN_HOLDINGS, catalog)

    assert index.held_columns_for_variant(
        "scb/lisa/kon", "scb/lisa/individer-15plus"
    ) == frozenset({"Kon"})
    assert (
        index.held_columns_for_variant("scb/lisa/kon", "scb/lisa/individer-16plus")
        == frozenset()
    )
    assert index.held_columns("scb/lisa/kon") == frozenset({"Kon"})


def test_index_admits_known_and_rejects_unknown(catalog):
    index = _catalog_index(_CLEAN_HOLDINGS, catalog)
    assert index.admits("scb/lisa/kon", "Kon")
    assert index.admits("scb/rams/syss", "Syss")
    # In the universe but NOT in this steward's inventory → not admitted.
    assert not index.admits("scb/rams/nosuchbinding", "Whatever")
    # #206: the right FQID at a column the steward does not hold → not admitted,
    # but `held_columns` still names the concept (the representation-level case).
    assert not index.admits("scb/lisa/kon", "KonDetailed")
    assert index.held_columns("scb/lisa/kon") == frozenset({"Kon"})
    assert index.held_columns("scb/rams/nosuchbinding") == frozenset()


def test_catalog_sizes_de_dupes_binding_columns():
    index = CatalogIndex(
        bindings_by_variant={
            "scb/lisa/individer-15plus": frozenset(
                {
                    ("scb/lisa/kon", "Kon"),
                    ("scb/lisa/kon", "KonDetaljerad"),
                }
            ),
            "sos/patient/_default": frozenset({("sos/patient/diagnos", None)}),
        },
        periods_by_coordinate={},
        period_range_by_register={
            "scb/lisa": ("2018-01-01", "2018-12-31"),
            "sos/patient": ("2020-01-01", "2020-12-31"),
        },
        drift_warnings=(),
    )

    assert index.catalog_sizes() == CatalogSizes(providers=2, registers=2, variables=2)


def test_held_variant_coords_excludes_empty_slot():
    # gap 4: a variant slot admitting nothing must NOT count as a held coord —
    # the variants endpoint lists only variants the steward actually holds data
    # under.
    index = CatalogIndex(
        bindings_by_variant={
            "scb/lisa/individer-15plus": frozenset({("scb/lisa/kon", "Kon")}),
            "scb/lisa/empty": frozenset(),
        },
        periods_by_coordinate={},
        period_range_by_register={"scb/lisa": ("2018-01-01", "2018-12-31")},
        drift_warnings=(),
    )
    assert index.held_variant_coords_for_register("scb/lisa") == frozenset(
        {"scb/lisa/individer-15plus"}
    )


# ── (c) drift drops a mapping from the index (unit) ────────────────────────


def test_unresolvable_null_representation_drops_and_warns(catalog):
    """A `representation = None` mapping whose FQID reg_meta no longer resolves
    is DROPPED (it can't be authored against), the rest of the inventory is
    unaffected, and the miss is recorded for `/api/context`'s drift banner."""
    index = _catalog_index(
        [
            ("scb/lisa/individer-15plus", "scb/lisa/kon", "Kon", "2018"),
            ("scb/lisa/individer-15plus", "scb/lisa/ghostvar", None, "2018"),
        ],
        catalog,
    )

    assert index.bindings_by_variant["scb/lisa/individer-15plus"] == frozenset(
        {("scb/lisa/kon", "Kon")}
    )
    (warning,) = index.drift_warnings
    assert warning.code == "fqid_unresolved"
    assert warning.path == "table['T1_2018.csv'].column['P1_ghostvar']"
    assert "ghostvar" in warning.message


def test_null_representation_outside_state_validity_drops_and_warns(catalog):
    """The other drift arm: the FQID resolves, but reg_meta delivers no state for
    it over the table's edition (kon's only state starts 2018)."""
    index = _catalog_index(
        [("scb/lisa/individer-15plus", "scb/lisa/kon", None, "2015")], catalog
    )

    assert index.bindings_by_variant == {}
    assert index.period_range_by_register == {}
    (warning,) = index.drift_warnings
    assert warning.code == "period_outside_state_validity"
    assert "scb/lisa/kon" in warning.message


# ── (d) the global deployment has no index ─────────────────────────────────


def test_global_steward_has_no_inventory():
    from reg_webapp.stewards import load_delivery_inventory, load_steward

    steward = load_steward("global")
    assert load_delivery_inventory(steward) is None


# ── boot-path integration: filtered steward + drift survival ───────────────


@pytest.fixture
def _filtered_steward_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the app at a tmp stewards dir and select the ``ifau`` steward."""
    stewards = tmp_path / "stewards"
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "ifau")
    return stewards


def test_boot_with_filtered_steward_populates_index(catalog_db, _filtered_steward_dir):
    _write_steward(_filtered_steward_dir, "ifau", _CLEAN_HOLDINGS)
    app = create_app()
    with TestClient(app) as client:
        index = app.state.catalog_index
        assert index is not None
        assert "scb/lisa/individer-15plus" in index.bindings_by_variant
        # The SAME parsed inventory backs the order materializer.
        assert app.state.inventory is not None
        resp = client.get("/api/context")
    assert resp.status_code == 200
    body = resp.json()
    assert body["steward"]["id"] == "ifau"
    assert body["steward"]["catalog_period_span"] == {"from": 2018, "to": 2019}
    assert body["catalog_drift_warnings"] == []


def test_boot_survives_catalog_drift(catalog_db, _filtered_steward_dir):
    """⚠️ Boot-availability: an inventory mapping reg_meta no longer delivers
    over its table's edition must BOOT — the coordinate drops from the index,
    startup does NOT crash, and the drift is surfaced on /api/context.

    The EDITION arm is the drift that reaches the index at boot. §12's
    consistency gate (``stewards.check_delivery_inventory``) runs on the same
    boot connection and is period-agnostic, so a coordinate the catalog does not
    name at all fails startup instead of drifting through — pinned by
    ``test_project_order`` → the unresolvable-inventory boot guard.
    """
    _write_steward(
        _filtered_steward_dir,
        "ifau",
        [
            ("scb/rams/standard", "scb/rams/syss", "Syss", "2019"),
            # kon's only state starts 2018, so this table's edition holds none.
            ("scb/lisa/individer-15plus", "scb/lisa/kon", None, "2015"),
        ],
    )

    app = create_app()
    # Entering the context runs the lifespan — it must NOT raise on the drift.
    with TestClient(app) as client:
        index = app.state.catalog_index
        assert index is not None
        # The out-of-edition mapping dropped; the resolvable one survives.
        assert index.bindings_by_variant == {
            "scb/rams/standard": frozenset({("scb/rams/syss", "Syss")})
        }
        resp = client.get("/api/context")

    assert resp.status_code == 200
    warnings = resp.json()["catalog_drift_warnings"]
    assert len(warnings) == 1
    assert warnings[0]["code"] == "period_outside_state_validity"
    assert "scb/lisa/kon" in warnings[0]["message"]


def test_boot_global_has_no_index(catalog_db, tmp_path, monkeypatch):
    """The global deployment (no inventory.toml) boots with a None index and an
    empty drift list."""
    stewards = tmp_path / "stewards"
    _write_global(stewards)
    monkeypatch.setenv("REG_WEBAPP_STEWARDS_DIR", str(stewards))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", "global")

    app = create_app()
    with TestClient(app) as client:
        assert app.state.catalog_index is None
        assert app.state.inventory is None
        resp = client.get("/api/context")
    assert resp.status_code == 200
    body = resp.json()
    assert body["steward"]["catalog_period_span"] is None
    assert body["catalog_drift_warnings"] == []
