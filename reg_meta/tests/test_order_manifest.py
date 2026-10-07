"""Order behavior at the library return and manifest-byte boundaries."""

from __future__ import annotations

import pytest
from order_fixture import (
    PARTITIONED_INVENTORY,
    conn as conn,  # noqa: PLC0414 - pytest fixture registration
    install_test_holdings,
    inventory as inventory,  # noqa: PLC0414 - pytest fixture registration
    order_inventory,
    order_project,
)
from reg_meta.db import SCHEMA_VERSION
from reg_meta.order import (
    ORDER_MANIFEST_VERSION,
    OrderManifest,
    extraction_filenames,
    materialize_order,
)
from reg_schema.project_data import PeriodRange


class TestMaterializedOrder:
    def test_full_order_fans_out_deterministically(self, conn, inventory) -> None:
        project = order_project(
            "scb/lisa/kon", "scb/lisa/disponibel-inkomst", "scb/lisa/yrke"
        )

        result = materialize_order(project, install_test_holdings(conn, inventory))

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

    def test_disjoint_request_orders_every_segment(self, conn, inventory) -> None:
        # An interrupted series (#307): each segment is served on its own, and
        # the hole between them is not a coverage gap.
        project = order_project(
            "scb/lisa/kon", period=(PeriodRange(from_=2018, to=2018), 2020)
        )

        result = materialize_order(project, install_test_holdings(conn, inventory))

        assert result.findings == ()
        assert result.manifest is not None
        assert [
            (e.requested_period, e.physical.table) for e in result.manifest.entries
        ] == [("2018", "LISA_Individ_2018.csv"), ("2020", "LISA_Individ_2019-2020.csv")]
        assert result.manifest.clips == ()

    def test_adjacent_period_segments_are_one_request(self, conn, inventory) -> None:
        # `[2018, 2019]` is day-adjacent, so it is ONE continuous request — not
        # a clip and not two entries against the same table.
        project = order_project("scb/lisa/kon", period=(2018, 2019))

        manifest = materialize_order(
            project, install_test_holdings(conn, inventory)
        ).manifest

        assert manifest is not None
        assert manifest.clips == ()
        assert [(e.requested_period, e.physical.table) for e in manifest.entries] == [
            ("2018", "LISA_Individ_2018.csv"),
            ("2019", "LISA_Individ_2019-2020.csv"),
        ]

    def test_provenance_carries_project_and_catalog_identity(
        self, conn, inventory
    ) -> None:
        project = order_project("scb/lisa/kon")

        manifest = materialize_order(
            project, install_test_holdings(conn, inventory)
        ).manifest

        assert manifest is not None
        provenance = manifest.provenance
        assert provenance.steward == "swecov"
        assert provenance.project_name == "Synthetic order"
        assert provenance.catalog_schema_version == SCHEMA_VERSION
        assert (
            provenance.catalog_generation_id
            == dict(conn.execute("SELECT key, value FROM import_manifest"))[
                "generation_id"
            ]
        )
        assert len(provenance.project_hash) == 64
        # The hash is the project's identity: a different project, a different hash.
        other = materialize_order(order_project("scb/lisa/yrke"), conn).manifest
        assert other is not None
        assert other.provenance.project_hash != provenance.project_hash


class TestManifestContract:
    def test_repeat_materialization_is_byte_identical(self, conn, inventory) -> None:
        project = order_project(
            "scb/lisa/kon", "scb/lisa/disponibel-inkomst", "scb/lisa/yrke"
        )

        first = materialize_order(
            project, install_test_holdings(conn, inventory)
        ).manifest
        second = materialize_order(project, conn).manifest

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
        """The disjoint-partition arm must be invisible to an inventory that uses no
        partitions: the absent key is the None spelling, so these bytes are the
        ones the contract had before the arm existed."""
        project = order_project(
            "scb/lisa/kon", "scb/lisa/disponibel-inkomst", "scb/lisa/yrke"
        )

        manifest = materialize_order(
            project, install_test_holdings(conn, inventory)
        ).manifest

        assert manifest is not None
        assert "partition" not in manifest.to_json()
        # Absent restores the default on the way back in, so the round-trip and
        # the extraction names are unchanged too.
        assert OrderManifest.model_validate_json(manifest.to_json()) == manifest
        assert extraction_filenames(manifest.entries[0]) == (
            "lisa_individer-15plus_2018.csv",
        )

    def test_unknown_key_is_rejected_at_the_read_boundary(
        self, conn, inventory
    ) -> None:
        manifest = materialize_order(
            order_project("scb/lisa/kon"), install_test_holdings(conn, inventory)
        ).manifest

        assert manifest is not None
        payload = manifest.model_dump(mode="json") | {"population": "all"}
        with pytest.raises(ValueError, match="population"):
            OrderManifest.model_validate(payload)


class TestDisjointPartitions:
    """The disjoint-partition arm: every partition of a matched cell is
    emitted, and extraction preserves delivery topology — what goes in as two
    tables comes out as two files."""

    def test_partition_token_separates_the_extraction_files(
        self, conn, tmp_path
    ) -> None:
        # Same variant, same edition segment: without the token both shards
        # would render the one filename `lisa_individer-15plus_2018..2020.csv`.
        manifest = materialize_order(
            order_project("scb/lisa/kon"),
            install_test_holdings(
                conn, order_inventory(tmp_path, PARTITIONED_INVENTORY)
            ),
        ).manifest

        assert manifest is not None
        assert [
            name for entry in manifest.entries for name in extraction_filenames(entry)
        ] == [
            "lisa_individer-15plus_mikro_2018..2020.csv",
            "lisa_individer-15plus_stora_2018..2020.csv",
        ]
