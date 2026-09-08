"""Order materializer + JSON order-manifest contract (REFACTOR_SPEC.md §12).

Everything here is synthetic: a hand-built catalog DB (`_slugged_db`, the same
builder the Catalog tests use) plus a fixture inventory parsed through lane 1's
real `load_inventory`. No steward's real holdings are committed.

The catalog fixture is one LISA variant delivering three concepts over
2018–2020, each shaped to exercise one §12 rule:

- `kon`      — `Kon` across all three years (the plain full-coverage case);
- `disponibel-inkomst` — `DispInk09` from 2019 only (the availability clip);
- `yrke`     — `Ssyk3` in 2018, `Ssyk4` from 2019 (the representation change
               that fans into two slices).

`TestGlobalFallback` runs the same catalog with NO inventory (`materialize_order(
project, None, conn)`) — §12's global-deployment fallback, which grounds the
order on canonical resolution alone.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_state, add_variable, build_slugged_db
from reg_meta.catalog import Catalog
from reg_meta.cli import run
from reg_meta.errors import EXIT_CONFIG, EXIT_NO_MATCH, RegMetaError
from reg_meta.inventory import load_inventory
from reg_meta.order import (
    ORDER_MANIFEST_VERSION,
    SUPPORTED_SCHEMA_VERSION,
    OrderManifest,
    extraction_filenames,
    load_project,
    materialize_order,
    project_from_raw,
    requested_intervals,
    resolve_binding,
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


# `yrke` mapped by a single column with NO `representation`, in a table spanning
# both of its representation slices (`Ssyk3` 2018, `Ssyk4` 2019–2020). The
# mapping cannot say which slice its column is, so it must block rather than
# claim one physical column carries both canonical representations.
UNQUALIFIED_INVENTORY = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Individ_2018-2020.csv"
edition = { from = 2018, to = 2020 }
[[table.column]]
name = "Yrke"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/yrke"
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"
"""


# The qualified fixture plus a 2021 table whose single `yrke` column is
# unqualified. It lies outside every request below, so it can never contribute a
# column — and therefore must never block a representation-changing binding.
INERT_UNQUALIFIED_INVENTORY = (
    FIXTURE_INVENTORY
    + """
[[table]]
id = "LISA_Individ_2021.csv"
edition = 2021
[[table.column]]
name = "Yrke"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/yrke"
"""
)


# §12's disjoint-partition arm: one edition delivered as two sub-population
# shards (the survey-strata shape), unified as ONE user-facing variant. The
# distinct `partition` labels are what make the overlapping mappings legal, and
# what keeps their extraction files apart.
PARTITIONED_INVENTORY = """
version = 1
steward = "swecov"

[[table]]
id = "LISA_Mikro_2018-2020.csv"
edition = { from = 2018, to = 2020 }
partition = "mikro"
[[table.column]]
name = "Kon"
[[table.column.mapping]]
register_variant = "scb/lisa/individer-15plus"
variable = "scb/lisa/kon"

[[table]]
id = "LISA_Stora_2018-2020.csv"
edition = { from = 2018, to = 2020 }
partition = "stora"
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
        schema_version=SUPPORTED_SCHEMA_VERSION,
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


def _raw_project(**over) -> dict:
    """`_project` as the RAW dict the adapter door actually takes
    (`project_from_raw`, and `reg-meta order`'s file). `**over` replaces
    top-level keys — `schema_version` is the one the gate tests vary."""
    return {
        "schema_version": SUPPORTED_SCHEMA_VERSION,
        "steward": "global",
        "reg_meta_version": "0.39.1",
        "name": "Synthetic order",
        "sources": [
            {
                "name": "lisa",
                "register_variant": _VARIANT,
                "period": {"from": 2018, "to": 2020},
                "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
            }
        ],
    } | over


def _codes(result) -> list[str]:
    return [finding.code for finding in result.findings]


def _add_window(
    conn: sqlite3.Connection, *, variable_slug: str, column: str, lo: str, hi: str
) -> None:
    """One raw `variable_alias_window` row (#319) on the fixture's variant — a
    monthly family's month column inside its annual claim."""
    conn.execute(
        "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
        "delivery_column_name, valid_from, valid_to) "
        "SELECT variable_id, 10, ?, ?, ? FROM variable "
        "WHERE register_id = 1 AND slug = ?",
        (column, lo, hi, variable_slug),
    )
    conn.commit()


def _resolve(conn, variable: str, period: object, representation: str | None = None):
    """Steps 1+2 alone, for the one binding of a single-source project."""
    source = _project(variable, period=period, representation=representation).sources[0]
    return resolve_binding(
        Catalog(conn), source, source.bindings[0], requested_intervals(source.period)
    )


class TestSharedResolution:
    """`resolve_binding` — §12 steps 1+2, the ONE availability/slicing answer
    the materializer and the webapp's `/api/project/validate` both read.

    §12 is intersection semantics: a binding is requested wherever it IS
    available inside the source window, so narrower availability clips (and is
    reported) rather than blocking.
    """

    def test_a_disjoint_request_is_clipped_to_what_exists(self, conn) -> None:
        # `kon` starts in 2018, so `[2010, 2018]` keeps its available half.
        resolution = _resolve(conn, "scb/lisa/kon", (2010, 2018))
        assert resolution.finding is None
        assert resolution.slices == (("2018-01-01", "2018-12-31", "Kon"),)
        assert resolution.clip is not None
        assert resolution.clip.requested_period == "2010,2018"
        assert resolution.clip.ordered_period == "2018"

    def test_a_list_and_its_enclosing_range_agree_on_availability(self, conn) -> None:
        # The consistency this seam exists for: neither spelling of the same span
        # blocks where the other clips. It is availability they agree on, not
        # every verdict — the enclosing range also asks for the hole, which can
        # add co-existence the list never sees (see the hole test below).
        listed = _resolve(conn, "scb/lisa/kon", (2010, 2018))
        ranged = _resolve(conn, "scb/lisa/kon", PeriodRange(from_=2010, to=2018))
        assert ranged.finding is None
        assert ranged.slices == listed.slices
        assert ranged.availability == listed.availability
        assert ranged.clip is not None
        assert ranged.clip.ordered_period == listed.clip.ordered_period

    def test_an_entirely_unavailable_binding_still_blocks(self, conn) -> None:
        # Clipping to nothing is not an order — this stays an error.
        resolution = _resolve(conn, "scb/lisa/kon", 2015)
        assert resolution.slices == ()
        assert resolution.clip is None
        assert resolution.finding is not None
        assert resolution.finding.code == "binding_unavailable"

    def test_a_hole_in_the_request_is_not_a_coexisting_instant(self, conn) -> None:
        # `Ssyk5` co-exists with `Ssyk4` in 2019 only. A request whose HOLE is
        # 2019 never asks for both at one instant, so it fans into slices; the
        # continuous range THROUGH the overlap is genuinely ambiguous.
        add_state(
            conn,
            register_id=1,
            variable_slug="yrke",
            register_variant_id=10,
            valid_from="2019-01-01",
            valid_to="2019-12-31",
            delivery_column_name="Ssyk5",
            value_set_version_label="ssyk5",
        )
        conn.commit()

        disjoint = _resolve(conn, "scb/lisa/yrke", (2018, 2020))
        assert disjoint.finding is None
        assert disjoint.slices == (
            ("2018-01-01", "2018-12-31", "Ssyk3"),
            ("2020-01-01", "2020-12-31", "Ssyk4"),
        )

        through = _resolve(conn, "scb/lisa/yrke", PeriodRange(from_=2018, to=2020))
        assert through.finding is not None
        assert through.finding.code == "representation_ambiguous"

    def test_a_pin_delivered_only_in_a_hole_is_unavailable(self, conn) -> None:
        # `KonHole` exists in 2018, which the request SKIPS. Nothing the binding
        # pins is delivered at a requested instant, so the binding is simply
        # unavailable — not "you pinned an unknown column", which would name
        # `KonHole` as both unknown and available in one message.
        add_variable(conn, register_id=1, var_id=47, name="Kon (hål)", slug="kon-hal")
        add_state(
            conn,
            register_id=1,
            variable_slug="kon-hal",
            register_variant_id=10,
            valid_from="2018-01-01",
            valid_to="2018-12-31",
            delivery_column_name="KonHole",
        )
        conn.commit()

        resolution = _resolve(conn, "scb/lisa/kon-hal", (2010, 2020), "KonHole")
        assert resolution.finding is not None
        assert resolution.finding.code == "binding_unavailable"
        assert resolution.slices == ()

    def test_the_columns_offered_are_the_ones_actually_requested(self, conn) -> None:
        # An unknown pin must be answered with what the binding delivers WHERE IT
        # WAS ASKED FOR: `KonHole` lives only in the hole, so offering it would
        # send the researcher to a column no requested instant can extract.
        add_variable(conn, register_id=1, var_id=48, name="Kon (kant)", slug="kon-kant")
        for valid_from, valid_to, column in (
            ("2010-01-01", "2010-12-31", "KonRequested"),
            ("2018-01-01", "2018-12-31", "KonHole"),
            ("2020-01-01", "2020-12-31", "KonRequested"),
        ):
            add_state(
                conn,
                register_id=1,
                variable_slug="kon-kant",
                register_variant_id=10,
                valid_from=valid_from,
                valid_to=valid_to,
                delivery_column_name=column,
            )
        conn.commit()

        resolution = _resolve(conn, "scb/lisa/kon-kant", (2010, 2020), "Missing")
        assert resolution.finding is not None
        assert resolution.finding.code == "representation_unknown"
        assert "available: ['KonRequested']" in resolution.finding.message
        assert "KonHole" not in resolution.finding.message

    def test_a_month_with_no_alias_window_keeps_the_annual_claim(self, conn) -> None:
        # #319: `resolve_at` falls back to the annual claim for a month the
        # family delivered no column window for (`test_period_family_resolve.
        # test_gap_year_month_falls_back_to_annual_state`) — March here. That
        # fallback is per QUERY, so the request is resolved per segment: one
        # resolve over 2018-01..2018-03 would find January's window, skip the
        # fallback, and drop a March the steward does deliver.
        add_variable(
            conn, register_id=1, var_id=49, name="Kon per månad", slug="kon-manad"
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="kon-manad",
            register_variant_id=10,
            valid_from="2018-01-01",
            valid_to="2018-12-31",
            delivery_column_name="KonJan",
        )
        for column, lo, hi in (
            ("KonJan", "2018-01-01", "2018-01-31"),
            ("KonFeb", "2018-02-01", "2018-02-28"),
        ):
            _add_window(conn, variable_slug="kon-manad", column=column, lo=lo, hi=hi)

        # March alone: the annual claim serves it.
        march = _resolve(conn, "scb/lisa/kon-manad", "2018-03", "KonJan")
        assert march.finding is None
        assert march.clip is None
        assert march.slices == (("2018-03-01", "2018-03-31", "KonJan"),)

        # And asking for January TOO must not cost March its coverage.
        both = _resolve(conn, "scb/lisa/kon-manad", ("2018-01", "2018-03"), "KonJan")
        assert both.finding is None
        assert both.clip is None
        assert both.slices == (
            ("2018-01-01", "2018-01-31", "KonJan"),
            ("2018-03-01", "2018-03-31", "KonJan"),
        )

    def test_a_state_reaching_two_segments_is_kept_once(self, conn) -> None:
        # `Ssyk4` spans 2019–2020, so it reaches both segments of an interrupted
        # request: ONE kept state carrying both requested windows. Two would
        # read downstream as a state transition inside the period.
        resolution = _resolve(conn, "scb/lisa/yrke", ("2019-Q1", "2020-Q1"))
        assert len(resolution.states) == 1
        assert resolution.states[0].intervals == (
            ("2019-01-01", "2019-03-31"),
            ("2020-01-01", "2020-03-31"),
        )
        assert resolution.clip is None


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

    def test_an_unpartitioned_manifest_carries_no_partition_key(
        self, conn, inventory
    ) -> None:
        """§12's partition arm must be invisible to an inventory that uses no
        partitions: the absent key is the None spelling, so these bytes are the
        ones the contract had before the arm existed."""
        project = _project(
            "scb/lisa/kon", "scb/lisa/disponibel-inkomst", "scb/lisa/yrke"
        )

        manifest = materialize_order(project, inventory, conn).manifest

        assert manifest is not None
        assert "partition" not in manifest.to_json()
        # Absent restores the default on the way back in, so the round-trip and
        # the extraction names are unchanged too.
        assert OrderManifest.model_validate_json(manifest.to_json()) == manifest
        assert extraction_filenames(manifest.entries[0]) == (
            "lisa_individer-15plus_2018.csv",
        )

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


class TestDisjointPartitions:
    """§12's disjoint-partition arm: every partition of a matched cell is
    emitted, and extraction preserves delivery topology — what goes in as two
    tables comes out as two files."""

    def test_every_partition_of_a_cell_is_emitted_with_its_label(
        self, conn, tmp_path
    ) -> None:
        # Both shards match the same cell over the same edition; coverage unions
        # them (they are just more matching tables), so nothing blocks and
        # neither is chosen over the other.
        result = materialize_order(
            _project("scb/lisa/kon"),
            _inventory(tmp_path, PARTITIONED_INVENTORY),
            conn,
        )

        assert result.findings == ()
        assert result.manifest is not None
        assert [
            (e.physical.table, e.physical.partition, e.requested_period)
            for e in result.manifest.entries
        ] == [
            ("LISA_Mikro_2018-2020.csv", "mikro", "2018..2020"),
            ("LISA_Stora_2018-2020.csv", "stora", "2018..2020"),
        ]

    def test_partition_token_separates_the_extraction_files(
        self, conn, tmp_path
    ) -> None:
        # Same variant, same edition segment: without the token both shards
        # would render the one filename `lisa_individer-15plus_2018..2020.csv`.
        manifest = materialize_order(
            _project("scb/lisa/kon"),
            _inventory(tmp_path, PARTITIONED_INVENTORY),
            conn,
        ).manifest

        assert manifest is not None
        assert [
            name for entry in manifest.entries for name in extraction_filenames(entry)
        ] == [
            "lisa_individer-15plus_mikro_2018..2020.csv",
            "lisa_individer-15plus_stora_2018..2020.csv",
        ]

    def test_the_partition_reaches_the_manifest_json(self, conn, tmp_path) -> None:
        # The extractor reads the manifest offline, so the label has to be ON
        # the entry — not re-derived from the table identifier.
        manifest = materialize_order(
            _project("scb/lisa/kon"),
            _inventory(tmp_path, PARTITIONED_INVENTORY),
            conn,
        ).manifest

        assert manifest is not None
        assert '"partition": "mikro"' in manifest.to_json()
        assert OrderManifest.model_validate_json(manifest.to_json()) == manifest


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

    def test_unqualified_mapping_blocks_a_representation_change(
        self, conn, tmp_path
    ) -> None:
        # One unqualified column cannot serve both `Ssyk3` and `Ssyk4`: matching
        # it against every slice would emit a manifest claiming that column is
        # two different canonical representations.
        result = materialize_order(
            _project("scb/lisa/yrke"),
            _inventory(tmp_path, UNQUALIFIED_INVENTORY),
            conn,
        )

        assert result.manifest is None
        assert _codes(result) == ["mapping_ambiguous"]
        assert "Ssyk3" in result.findings[0].message
        assert "Ssyk4" in result.findings[0].message

    def test_out_of_period_unqualified_mapping_never_blocks(
        self, conn, tmp_path
    ) -> None:
        # The unqualified `yrke` column sits in a 2021 table, outside the
        # 2018–2020 request: it cannot match any slice, so it cannot make the
        # representation change ambiguous either.
        result = materialize_order(
            _project("scb/lisa/yrke"),
            _inventory(tmp_path, INERT_UNQUALIFIED_INVENTORY),
            conn,
        )

        assert result.findings == ()
        assert result.manifest is not None
        assert [
            (e.logical.representation, e.physical.table)
            for e in result.manifest.entries
        ] == [
            ("Ssyk3", "LISA_Individ_2018.csv"),
            ("Ssyk4", "LISA_Individ_2019-2020.csv"),
        ]

    def test_unqualified_mapping_serves_a_single_representation(
        self, conn, tmp_path
    ) -> None:
        # Same inventory, but `kon` resolves to ONE canonical representation
        # across the request — §12's single-representation arm still matches.
        result = materialize_order(
            _project("scb/lisa/kon"),
            _inventory(tmp_path, UNQUALIFIED_INVENTORY),
            conn,
        )

        assert result.findings == ()
        assert result.manifest is not None
        assert [
            (e.logical.representation, e.physical.column)
            for e in result.manifest.entries
        ] == [("Kon", "Kon")]

    def test_steward_mismatch_blocks_before_anything_resolves(
        self, conn, inventory
    ) -> None:
        """The message is the researcher's whole correction — the steward this
        project records, the one this deployment serves, where to order it
        instead, and that its provenance is theirs to change, not the
        application's — with no internal design document to look up. It is
        pinned verbatim HERE, once: the adapters render it and the SPA's
        fixtures mimic it, but this is where the wording lives."""
        project = _project("scb/lisa/kon", steward="ifau")

        result = materialize_order(project, inventory, conn)

        assert result.manifest is None
        assert _codes(result) == ["steward_mismatch"]
        (finding,) = result.findings
        # Coordinates stay separate from the prose — a steward mismatch is a
        # whole-project finding, so it names none.
        assert (finding.source, finding.variable, finding.period) == (None, None, None)
        assert finding.message == (
            "this project's steward is 'ifau' and this deployment serves the "
            "'swecov' steward; order the project from the deployment that "
            "serves 'ifau'. Its steward is provenance — no deployment rewrites "
            "it to match its own"
        )

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

    def test_blocked_result_still_reports_its_clips(self, conn, inventory) -> None:
        # The clipped binding materializes, a later binding blocks: the clip is
        # still surfaced, so the researcher sees the window the rest of the
        # order was stated against and fixes everything in one pass.
        project = _project("scb/lisa/disponibel-inkomst", "scb/lisa/nonexistent")

        result = materialize_order(project, inventory, conn)

        assert result.manifest is None
        assert _codes(result) == ["variable_unresolved"]
        assert [
            (c.variable, c.requested_period, c.ordered_period) for c in result.clips
        ] == [("scb/lisa/disponibel-inkomst", "2018..2020", "2019..2020")]

    @pytest.mark.parametrize(
        "period",
        [
            pytest.param(PeriodRange(from_=2020, to=2018), id="inverted-range"),
            pytest.param("nope", id="malformed-token"),
        ],
    )
    def test_a_period_that_cannot_be_expanded_is_not_orderable(
        self, conn, inventory, period
    ) -> None:
        # Fail-closed backstop for a period the structural gate does not reject
        # (a SCALAR inverted range) or that reaches the materializer unvalidated:
        # no orderable interval means no manifest, never a guessed window.
        result = materialize_order(
            _project("scb/lisa/kon", period=period), inventory, conn
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


class TestGlobalFallback:
    """§12's global-deployment fallback: no inventory, so canonical resolution
    alone grounds the order — same entry shape, blank `table`, the resolved
    canonical column in `column`, `edition` = that slice's requested period."""

    def test_clean_project_materializes_against_canonical_resolution(
        self, conn
    ) -> None:
        project = _project(
            "scb/lisa/kon",
            "scb/lisa/disponibel-inkomst",
            "scb/lisa/yrke",
            steward="global",
        )

        result = materialize_order(project, None, conn)

        assert result.findings == ()
        manifest = result.manifest
        assert manifest is not None
        assert manifest.provenance.mode == "global_fallback"
        assert manifest.provenance.steward == "global"
        assert [
            (
                e.logical.variable,
                e.requested_period,
                e.physical.table,
                e.physical.column,
                e.physical.edition,
            )
            for e in manifest.entries
        ] == [
            ("scb/lisa/kon", "2018..2020", "", "Kon", "2018..2020"),
            (
                "scb/lisa/disponibel-inkomst",
                "2019..2020",
                "",
                "DispInk09",
                "2019..2020",
            ),
            ("scb/lisa/yrke", "2018", "", "Ssyk3", "2018"),
            ("scb/lisa/yrke", "2019..2020", "", "Ssyk4", "2019..2020"),
        ]
        # The availability clip is informational here too.
        assert [c.variable for c in manifest.clips] == ["scb/lisa/disponibel-inkomst"]

    def test_repeat_materialization_is_byte_identical(self, conn) -> None:
        project = _project("scb/lisa/kon", "scb/lisa/yrke", steward="global")

        first = materialize_order(project, None, conn).manifest
        second = materialize_order(project, None, conn).manifest

        assert first is not None
        assert second is not None
        assert first.to_json() == second.to_json()

    def test_representation_change_fans_into_two_entries(self, conn) -> None:
        result = materialize_order(
            _project("scb/lisa/yrke", steward="global"), None, conn
        )

        assert result.manifest is not None
        assert [
            (e.logical.representation, e.physical.column, e.physical.edition)
            for e in result.manifest.entries
        ] == [("Ssyk3", "Ssyk3", "2018"), ("Ssyk4", "Ssyk4", "2019..2020")]

    def test_unresolvable_binding_blocks(self, conn) -> None:
        result = materialize_order(
            _project("scb/lisa/nonexistent", steward="global"), None, conn
        )

        assert result.manifest is None
        assert _codes(result) == ["variable_unresolved"]

    def test_binding_outside_availability_blocks(self, conn) -> None:
        result = materialize_order(
            _project("scb/lisa/kon", steward="global", period=2015), None, conn
        )

        assert result.manifest is None
        assert _codes(result) == ["binding_unavailable"]

    def test_ambiguous_and_clipped_binding_reports_both(self, conn) -> None:
        # The clip is appended BEFORE the ambiguity gate returns: a binding that
        # is both clipped (2017 is outside availability) and ambiguous (`Ssyk4`
        # and `Ssyk5` co-exist from 2019) surfaces both, so the researcher sees
        # the window the finding is stated against.
        add_state(
            conn,
            register_id=1,
            variable_slug="yrke",
            register_variant_id=10,
            valid_from="2019-01-01",
            valid_to="2020-12-31",
            delivery_column_name="Ssyk5",
            value_set_version_label="ssyk5",
        )
        conn.commit()

        result = materialize_order(
            _project(
                "scb/lisa/yrke",
                steward="global",
                period=PeriodRange(from_=2017, to=2020),
            ),
            None,
            conn,
        )

        assert result.manifest is None
        assert _codes(result) == ["representation_ambiguous"]
        assert [
            (c.variable, c.requested_period, c.ordered_period) for c in result.clips
        ] == [("scb/lisa/yrke", "2017..2020", "2018..2020")]

    def test_steward_mismatch_blocks_a_steward_project(self, conn) -> None:
        # The global deployment is a deployment like any other: a project whose
        # provenance names a steward is not orderable against it.
        result = materialize_order(_project("scb/lisa/kon"), None, conn)

        assert result.manifest is None
        assert _codes(result) == ["steward_mismatch"]

    def test_extraction_filename_is_one_file_per_period_segment(self, conn) -> None:
        # `edition = requested_period`, so §12's naming rule gives an
        # interrupted request one file per segment without a special case.
        result = materialize_order(
            _project("scb/lisa/kon", steward="global", period=(2018, 2020)),
            None,
            conn,
        )

        assert result.manifest is not None
        assert extraction_filenames(result.manifest.entries[0]) == (
            "lisa_individer-15plus_2018.csv",
            "lisa_individer-15plus_2020.csv",
        )


class TestIntervalAlgebra:
    def test_coverage_reaching_the_open_ended_sentinel_leaves_no_gap(self) -> None:
        # `_next_day` saturates at the open-ended sentinel, so the coverage gate
        # must recognise "reached the upper bound" directly rather than by
        # stepping past it — otherwise a fully covered open-ended interval
        # reports a phantom zero-width gap.
        from reg_meta.order import _gaps

        whole = (("2018-01-01", "9999-12-31"),)

        assert _gaps(whole, [("2018-01-01", "9999-12-31")]) == ()
        assert _gaps(whole, [("2018-01-01", "2019-12-31")]) == (
            ("2020-01-01", "9999-12-31"),
        )
        assert _gaps(whole, [("2019-01-01", "9999-12-31")]) == (
            ("2018-01-01", "2018-12-31"),
        )


class TestSupportedSchemaVersion:
    """`project_from_raw`'s supported-version decision — the ONE place any
    consumer decides whether a raw project claims the contract this build reads.

    Synthetic fixtures only: `_raw_project`, the same well-shaped finite-period
    project the CLI cases order, with `schema_version` as the sole variable. The
    webapp's `/validate` and `/order` reuse this decision
    (`order.schema_version_issue`), so the fixtures that reject here reject there
    — pinned in `reg_webapp/backend/tests/test_project_order.py`.
    """

    def test_supported_version_is_reg_schemas_own_declaration(self) -> None:
        """Never a second spelling of the contract version: the gate reads the
        schema package's declaration, so the two cannot drift."""
        import reg_schema

        assert SUPPORTED_SCHEMA_VERSION == reg_schema.__version__ == "3.0.0"

    def test_the_current_contract_is_accepted(self) -> None:
        """The ONE accepted claim: exact equality, nothing around it."""
        project = project_from_raw(
            _raw_project(schema_version=SUPPORTED_SCHEMA_VERSION)
        )

        assert project.schema_version == SUPPORTED_SCHEMA_VERSION

    @pytest.mark.parametrize(
        "version",
        ["1.0.0", "2.0.0", "3.0.1", "3.1.0", "99.0.0", "not-a-version", "3.0"],
    )
    def test_every_other_version_is_rejected(self, version) -> None:
        """Old majors, a neighbouring patch, an unreleased minor, an unknown major
        and an unparseable version alike — nothing but the current contract is
        read: one stable code, and a message naming both the rejected claim and
        the contract this build reads."""
        with pytest.raises(RegMetaError) as exc_info:
            project_from_raw(_raw_project(schema_version=version))

        error = exc_info.value
        assert error.code == "unsupported_schema_version"
        assert error.exit_code == EXIT_CONFIG
        assert version in error.message
        assert SUPPORTED_SCHEMA_VERSION in error.message

    def test_the_version_decision_precedes_structural_interpretation(self) -> None:
        """A project written for another contract is named as such, not reported
        as a pile of structural noise from reading it as the current one."""
        broken = _raw_project(schema_version="1.0.0")
        broken["sources"][0]["period"] = "notaperiod"

        with pytest.raises(RegMetaError) as exc_info:
            project_from_raw(broken)

        assert exc_info.value.code == "unsupported_schema_version"
        assert "invalid_period" not in exc_info.value.message

    @pytest.mark.parametrize("value", [None, 3])
    def test_an_absent_or_mistyped_version_keeps_its_structural_error(
        self, value
    ) -> None:
        """Not this layer's call: no `schema_version` string is a malformed
        DOCUMENT, and the structural layer already says so precisely."""
        raw = _raw_project()
        if value is None:
            del raw["schema_version"]
        else:
            raw["schema_version"] = value

        with pytest.raises(RegMetaError) as exc_info:
            project_from_raw(raw)

        assert exc_info.value.code == "project_invalid"
        assert "@/schema_version" in exc_info.value.message


class TestCliAdapter:
    """`reg-meta order` — the CLI adapter over `materialize_order`.

    A thin adapter (§12), so what is pinned here is the adapter contract only:
    the manifest's own canonical bytes reach stdout/`--output` UNCHANGED (never
    the CLI envelope, never `--format`), and each failure gets a stable exit
    code from the existing error classes. The materializer's rules are pinned by
    the classes above; the byte-identity with the FastAPI adapter is pinned in
    `reg_webapp/backend/tests/test_project_order.py` (only there do both
    adapters exist).
    """

    @staticmethod
    def _db_dir(conn: sqlite3.Connection, tmp_path: Path) -> str:
        """The in-memory synthetic catalog, on disk where `--db` can open it."""
        import sqlite3 as _sqlite3

        from reg_meta.db import DB_FILENAME

        db_dir = tmp_path / "db"
        db_dir.mkdir()
        target = _sqlite3.connect(db_dir / DB_FILENAME)
        try:
            conn.backup(target)
        finally:
            target.close()
        return str(db_dir)

    @staticmethod
    def _project_file(tmp_path: Path, path_name: str = "project_data.json", **over):
        import json

        path = tmp_path / path_name
        path.write_text(json.dumps(_raw_project(**over)), encoding="utf-8")
        return path

    def test_stdout_carries_the_manifest_bytes_verbatim(
        self, conn, tmp_path, capsys
    ) -> None:
        project = self._project_file(tmp_path)
        expected = materialize_order(load_project(project), None, conn)

        code = run(["order", str(project), "--db", self._db_dir(conn, tmp_path)])

        assert code == 0
        assert expected.manifest is not None
        assert capsys.readouterr().out == expected.manifest.to_json()

    def test_output_flag_writes_the_manifest_to_a_file(
        self, conn, tmp_path, capsys
    ) -> None:
        project = self._project_file(tmp_path)
        out = tmp_path / "order.json"

        code = run(
            [
                "order",
                str(project),
                "--db",
                self._db_dir(conn, tmp_path),
                "--output",
                str(out),
            ]
        )

        assert code == 0
        assert capsys.readouterr().out == ""
        assert OrderManifest.model_validate_json(out.read_text(encoding="utf-8"))

    def test_inventory_flag_grounds_the_order_on_the_steward_topology(
        self, conn, tmp_path, capsys
    ) -> None:
        """`--inventory` is the steward deployment's arm: entries carry the
        inventory's literal table, not the global fallback's blank one."""
        import json

        inventory_path = tmp_path / "inventory.toml"
        inventory_path.write_text(FIXTURE_INVENTORY, encoding="utf-8")
        project = self._project_file(tmp_path, steward="swecov")

        code = run(
            [
                "order",
                str(project),
                "--inventory",
                str(inventory_path),
                "--db",
                self._db_dir(conn, tmp_path),
            ]
        )

        assert code == 0
        manifest = json.loads(capsys.readouterr().out)
        assert manifest["provenance"]["mode"] == "steward_inventory"
        assert [entry["physical"]["table"] for entry in manifest["entries"]] == [
            "LISA_Individ_2018.csv",
            "LISA_Individ_2019-2020.csv",
        ]

    def test_blocked_order_exits_no_match_naming_every_finding(
        self, conn, tmp_path, capsys
    ) -> None:
        """Fail-closed: a blocked order is the error envelope + exit 17
        (`EXIT_NO_MATCH`), never a partial manifest on stdout."""
        import json

        project = self._project_file(tmp_path, steward="swecov")

        code = run(["order", str(project), "--db", self._db_dir(conn, tmp_path)])

        assert code == EXIT_NO_MATCH
        error = json.loads(capsys.readouterr().out)["error"]
        assert error["code"] == "order_blocked"
        assert "steward_mismatch" in error["message"]

    def test_unreadable_project_exits_config(self, conn, tmp_path, capsys) -> None:
        import json

        code = run(
            ["order", str(tmp_path / "nope.json"), "--db", self._db_dir(conn, tmp_path)]
        )

        assert code == EXIT_CONFIG
        assert json.loads(capsys.readouterr().out)["error"]["code"] == (
            "project_unreadable"
        )

    @pytest.mark.parametrize("period", ["notaperiod", "_default"])
    def test_structurally_invalid_project_exits_config(
        self, conn, tmp_path, capsys, period
    ) -> None:
        """The shared gate runs before the DB is even opened: a model-valid but
        structurally invalid spec (a bad period token — the retired
        whole-history sentinel is now one of those) never materializes."""
        import json

        project = self._project_file(tmp_path)
        spec = json.loads(project.read_text(encoding="utf-8"))
        spec["sources"][0]["period"] = period
        project.write_text(json.dumps(spec), encoding="utf-8")

        code = run(["order", str(project), "--db", self._db_dir(conn, tmp_path)])

        assert code == EXIT_CONFIG
        payload = json.loads(capsys.readouterr().out)
        assert payload["error"]["code"] == "project_invalid"
        assert "entries" not in payload

    def test_unsupported_schema_version_exits_config_writing_no_manifest(
        self, conn, tmp_path, capsys
    ) -> None:
        """A project written for another schema contract is rejected at the same
        shared door, before the DB is opened: the error envelope, never a
        manifest for a spec this build cannot read."""
        import json

        project = self._project_file(tmp_path, schema_version="1.0.0")

        code = run(["order", str(project), "--db", self._db_dir(conn, tmp_path)])

        assert code == EXIT_CONFIG
        payload = json.loads(capsys.readouterr().out)
        assert payload["error"]["code"] == "unsupported_schema_version"
        assert "entries" not in payload
