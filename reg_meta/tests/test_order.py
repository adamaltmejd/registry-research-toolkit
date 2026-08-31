"""Order materializer + JSON order-manifest contract (REFACTOR_SPEC.md §12 lane 2).

Everything here is synthetic: a hand-built catalog DB (`_slugged_db`, the same
builder the Catalog tests use) plus a fixture inventory parsed through lane 1's
real `load_inventory`. No steward's real holdings are committed.

The catalog fixture is one LISA variant delivering three concepts over
2018–2020, each shaped to exercise one §12 rule:

- `kon`      — `Kon` across all three years (the plain full-coverage case);
- `disponibel-inkomst` — `DispInk09` from 2019 only (the availability clip);
- `yrke`     — `Ssyk3` in 2018, `Ssyk4` from 2019 (the representation change
               that fans into two slices).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_state, add_variable, build_slugged_db
from reg_meta.inventory import load_inventory
from reg_meta.order import (
    ORDER_MANIFEST_VERSION,
    OrderManifest,
    extraction_filenames,
    materialize_order,
)
from reg_schema.project_data import Binding, PeriodRange, ProjectData, Source

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

    from reg_meta.inventory import DeliveryInventory

_VARIANT = "scb/lisa/individer-15plus"

FIXTURE_INVENTORY = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ_2018.csv"
edition = 2018

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"

[[table.column]]
name = "Ssyk3"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/yrke"
representation = "Ssyk3"

[[table]]
id = "LISA_Individ_2019-2020.csv"
edition = { from = 2019, to = 2020 }

[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"

[[table.column]]
name = "DispInk09"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/disponibel-inkomst"
representation = "DispInk09"

[[table.column]]
name = "Ssyk4"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/yrke"
representation = "Ssyk4"
"""

# Same steward, but `Kon` is delivered for 2018 and 2020 only — an in-availability
# hole at 2019 that the coverage gate must fail the WHOLE order on.
GAPPED_INVENTORY = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ_2018.csv"
edition = 2018
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"

[[table]]
id = "LISA_Individ_2020.csv"
edition = 2020
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
"""


def _inventory(tmp_path: Path, text: str) -> DeliveryInventory:
    path = tmp_path / "inventory.toml"
    path.write_text(text, encoding="utf-8")
    return load_inventory(path)


@pytest.fixture
def inventory(tmp_path: Path) -> DeliveryInventory:
    return _inventory(tmp_path, FIXTURE_INVENTORY)


@pytest.fixture
def conn() -> sqlite3.Connection:
    """The synthetic catalog: one variant, three concepts, 2018–2020."""
    db = build_slugged_db()
    # Replace the builder's open-ended seed state with explicit yearly windows,
    # so availability is a real (clippable) interval rather than "since 2018".
    db.execute(
        "DELETE FROM variable_state WHERE variable_id = "
        "(SELECT variable_id FROM variable WHERE register_id = 1 AND slug = 'kon')"
    )
    for year in (2018, 2019, 2020):
        add_state(
            db,
            register_id=1,
            variable_slug="kon",
            register_variant_id=10,
            valid_from=f"{year}-01-01",
            valid_to=f"{year}-12-31",
            delivery_column_name="Kon",
        )
    add_variable(
        db,
        register_id=1,
        var_id=45,
        name="Disponibel inkomst",
        slug="disponibel-inkomst",
    )
    for year in (2019, 2020):
        add_state(
            db,
            register_id=1,
            variable_slug="disponibel-inkomst",
            register_variant_id=10,
            valid_from=f"{year}-01-01",
            valid_to=f"{year}-12-31",
            delivery_column_name="DispInk09",
        )
    add_variable(db, register_id=1, var_id=46, name="Yrke", slug="yrke")
    add_state(
        db,
        register_id=1,
        variable_slug="yrke",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="2018-12-31",
        delivery_column_name="Ssyk3",
    )
    add_state(
        db,
        register_id=1,
        variable_slug="yrke",
        register_variant_id=10,
        valid_from="2019-01-01",
        valid_to="2020-12-31",
        delivery_column_name="Ssyk4",
    )
    for key, value in (
        ("schema_version", "6.7.0"),
        ("import_date", "2026-08-01T00:00:00Z"),
    ):
        db.execute("INSERT INTO import_manifest VALUES (?, ?)", (key, value))
    db.commit()
    return db


def _project(
    *variables: str,
    steward: str = "swecov",
    period: object = None,
    representation: str | None = None,
) -> ProjectData:
    return ProjectData(
        schema_version="1.0.0",
        steward=steward,
        reg_meta_version="0.39.1",
        name="Synthetic order",
        sources=(
            Source(
                name="lisa",
                register_variant=_VARIANT,
                period=period
                if period is not None
                else PeriodRange(from_=2018, to=2020),
                bindings=tuple(
                    Binding(
                        variable=variable,
                        type="categorical",
                        representation=representation,
                    )
                    for variable in variables
                ),
            ),
        ),
    )


def _codes(result) -> list[str]:
    return [finding.code for finding in result.findings]


class TestMaterializedOrder:
    def test_full_order_fans_out_deterministically(self, conn, inventory) -> None:
        project = _project(
            "scb/lisa/kon", "scb/lisa/disponibel-inkomst", "scb/lisa/yrke"
        )

        result = materialize_order(project, inventory, conn)

        assert result.findings == ()
        assert result.ok
        manifest = result.manifest
        assert manifest is not None
        assert manifest.version == ORDER_MANIFEST_VERSION
        # Project binding order is preserved; the per-binding fan-out sorts by
        # table, then edition, then physical column.
        assert [
            (
                e.logical.variable,
                e.requested_period,
                e.physical.table,
                e.physical.column,
            )
            for e in manifest.entries
        ] == [
            ("scb/lisa/kon", "2018", "LISA_Individ_2018.csv", "Kon"),
            ("scb/lisa/kon", "2019..2020", "LISA_Individ_2019-2020.csv", "Kon"),
            (
                "scb/lisa/disponibel-inkomst",
                "2019..2020",
                "LISA_Individ_2019-2020.csv",
                "DispInk09",
            ),
            ("scb/lisa/yrke", "2018", "LISA_Individ_2018.csv", "Ssyk3"),
            ("scb/lisa/yrke", "2019..2020", "LISA_Individ_2019-2020.csv", "Ssyk4"),
        ]

    def test_availability_clip_is_reported_not_widened(self, conn, inventory) -> None:
        # `DispInk09` exists only from 2019: the 2018–2020 source period is
        # clipped to the column's availability, and the clip is reported.
        project = _project("scb/lisa/kon", "scb/lisa/disponibel-inkomst")

        manifest = materialize_order(project, inventory, conn).manifest

        assert manifest is not None
        assert [
            (c.variable, c.requested_period, c.ordered_period) for c in manifest.clips
        ] == [("scb/lisa/disponibel-inkomst", "2018..2020", "2019..2020")]
        # No widened cross-product: the clipped binding orders the 2019–2020
        # table only, never the 2018 one.
        assert [
            e.physical.table
            for e in manifest.entries
            if e.logical.variable == "scb/lisa/disponibel-inkomst"
        ] == ["LISA_Individ_2019-2020.csv"]

    def test_disjoint_request_orders_every_segment(self, conn, inventory) -> None:
        # An interrupted series (#307): each segment is served on its own, and
        # the hole between them is not a coverage gap.
        project = _project(
            "scb/lisa/kon", period=(PeriodRange(from_=2018, to=2018), 2020)
        )

        result = materialize_order(project, inventory, conn)

        assert result.findings == ()
        assert result.manifest is not None
        assert [
            (e.requested_period, e.physical.table) for e in result.manifest.entries
        ] == [("2018", "LISA_Individ_2018.csv"), ("2020", "LISA_Individ_2019-2020.csv")]
        assert result.manifest.clips == ()

    def test_adjacent_period_segments_are_one_request(self, conn, inventory) -> None:
        # `[2018, 2019]` is day-adjacent, so it is ONE continuous request — not
        # a clip and not two entries against the same table.
        project = _project("scb/lisa/kon", period=(2018, 2019))

        manifest = materialize_order(project, inventory, conn).manifest

        assert manifest is not None
        assert manifest.clips == ()
        assert [(e.requested_period, e.physical.table) for e in manifest.entries] == [
            ("2018", "LISA_Individ_2018.csv"),
            ("2019", "LISA_Individ_2019-2020.csv"),
        ]

    def test_representation_change_fans_into_two_slices(self, conn, inventory) -> None:
        project = _project("scb/lisa/yrke")

        manifest = materialize_order(project, inventory, conn).manifest

        assert manifest is not None
        assert [
            (e.logical.representation, e.requested_period, e.physical.column)
            for e in manifest.entries
        ] == [("Ssyk3", "2018", "Ssyk3"), ("Ssyk4", "2019..2020", "Ssyk4")]
        # A representation change is not a clip: the full request is covered.
        assert manifest.clips == ()

    def test_provenance_carries_project_and_catalog_identity(
        self, conn, inventory
    ) -> None:
        project = _project("scb/lisa/kon")

        manifest = materialize_order(project, inventory, conn).manifest

        assert manifest is not None
        provenance = manifest.provenance
        assert provenance.steward == "swecov"
        assert provenance.project_name == "Synthetic order"
        assert provenance.catalog_schema_version == "6.7.0"
        assert provenance.catalog_import_date == "2026-08-01T00:00:00Z"
        assert len(provenance.project_hash) == 64
        # The hash is the project's identity: a different project, a different hash.
        other = materialize_order(_project("scb/lisa/yrke"), inventory, conn).manifest
        assert other is not None
        assert other.provenance.project_hash != provenance.project_hash


class TestManifestContract:
    def test_repeat_materialization_is_byte_identical(self, conn, inventory) -> None:
        project = _project(
            "scb/lisa/kon", "scb/lisa/disponibel-inkomst", "scb/lisa/yrke"
        )

        first = materialize_order(project, inventory, conn).manifest
        second = materialize_order(project, inventory, conn).manifest

        assert first is not None
        assert second is not None
        assert first.to_json() == second.to_json()
        # Canonical serialization: sorted keys, trailing newline.
        text = first.to_json()
        assert text.endswith("}\n")
        assert text.index('"clips"') < text.index('"entries"') < text.index('"version"')

    def test_manifest_round_trips_through_the_contract(self, conn, inventory) -> None:
        manifest = materialize_order(_project("scb/lisa/kon"), inventory, conn).manifest

        assert manifest is not None
        assert OrderManifest.model_validate_json(manifest.to_json()) == manifest

    def test_unknown_key_is_rejected_at_the_read_boundary(
        self, conn, inventory
    ) -> None:
        manifest = materialize_order(_project("scb/lisa/kon"), inventory, conn).manifest

        assert manifest is not None
        payload = manifest.model_dump(mode="json") | {"population": "all"}
        with pytest.raises(ValueError, match="population"):
            OrderManifest.model_validate(payload)

    def test_extraction_filename_is_pinned_per_variant_and_period_unit(
        self, conn, inventory
    ) -> None:
        manifest = materialize_order(_project("scb/lisa/kon"), inventory, conn).manifest

        assert manifest is not None
        assert extraction_filenames(manifest.entries[0]) == (
            "lisa_individer-15plus_2018.csv",
        )
        assert extraction_filenames(manifest.entries[1]) == (
            "lisa_individer-15plus_2019..2020.csv",
        )


class TestBlockingFindings:
    def test_in_availability_gap_blocks_the_whole_order(self, conn, tmp_path) -> None:
        project = _project("scb/lisa/kon")

        result = materialize_order(
            project, _inventory(tmp_path, GAPPED_INVENTORY), conn
        )

        assert result.manifest is None
        assert _codes(result) == ["coverage_gap"]
        # The exact uncovered subperiod, not just "incomplete".
        assert result.findings[0].period == "2019"
        assert result.findings[0].variable == "scb/lisa/kon"

    def test_steward_mismatch_blocks_before_anything_resolves(
        self, conn, inventory
    ) -> None:
        project = _project("scb/lisa/kon", steward="ifau")

        result = materialize_order(project, inventory, conn)

        assert result.manifest is None
        assert _codes(result) == ["steward_mismatch"]

    def test_empty_project_produces_no_header_only_manifest(
        self, conn, inventory
    ) -> None:
        result = materialize_order(_project(), inventory, conn)

        assert result.manifest is None
        assert _codes(result) == ["project_empty"]

    def test_every_gap_is_reported_in_one_pass(self, conn, tmp_path) -> None:
        # Two bindings the gapped inventory cannot serve: the researcher sees
        # both at once rather than one per round trip.
        project = _project("scb/lisa/kon", "scb/lisa/yrke")

        result = materialize_order(
            project, _inventory(tmp_path, GAPPED_INVENTORY), conn
        )

        assert _codes(result) == ["coverage_gap", "mapping_missing", "mapping_missing"]
        assert [f.period for f in result.findings] == ["2019", "2018", "2019..2020"]

    def test_default_period_is_not_orderable(self, conn, inventory) -> None:
        result = materialize_order(
            _project("scb/lisa/kon", period="_default"), inventory, conn
        )

        assert result.manifest is None
        assert _codes(result) == ["period_not_orderable"]

    def test_unresolvable_binding_blocks(self, conn, inventory) -> None:
        result = materialize_order(_project("scb/lisa/nonexistent"), inventory, conn)

        assert result.manifest is None
        assert _codes(result) == ["variable_unresolved"]

    def test_binding_outside_availability_blocks(self, conn, inventory) -> None:
        result = materialize_order(
            _project("scb/lisa/kon", period=2015), inventory, conn
        )

        assert result.manifest is None
        assert _codes(result) == ["binding_unavailable"]

    def test_unknown_pinned_representation_blocks(self, conn, inventory) -> None:
        result = materialize_order(
            _project("scb/lisa/yrke", representation="Ssyk5"), inventory, conn
        )

        assert result.manifest is None
        assert _codes(result) == ["representation_unknown"]

    def test_coexisting_representations_without_a_pin_block(
        self, conn, inventory
    ) -> None:
        # A second column valid at the same instant: genuine parallel
        # representations, which the manifest never guesses between.
        add_state(
            conn,
            register_id=1,
            variable_slug="yrke",
            register_variant_id=10,
            valid_from="2019-01-01",
            valid_to="2020-12-31",
            delivery_column_name="Ssyk5",
            # A parallel representation shares the window; the label is the
            # DB-level overlap discriminator that lets it co-exist.
            value_set_version_label="ssyk5",
        )
        conn.commit()

        result = materialize_order(_project("scb/lisa/yrke"), inventory, conn)

        assert result.manifest is None
        assert _codes(result) == ["representation_ambiguous"]

    def test_pinned_representation_narrows_the_order(self, conn, inventory) -> None:
        # Same co-existing shape, but pinned: the pin selects one slice and the
        # order materializes against it.
        add_state(
            conn,
            register_id=1,
            variable_slug="yrke",
            register_variant_id=10,
            valid_from="2019-01-01",
            valid_to="2020-12-31",
            delivery_column_name="Ssyk5",
            # A parallel representation shares the window; the label is the
            # DB-level overlap discriminator that lets it co-exist.
            value_set_version_label="ssyk5",
        )
        conn.commit()

        result = materialize_order(
            _project("scb/lisa/yrke", representation="Ssyk4"), inventory, conn
        )

        assert result.manifest is not None
        assert [e.physical.column for e in result.manifest.entries] == ["Ssyk4"]
