"""Order behavior at the library return and manifest-byte boundaries."""

from __future__ import annotations

import pytest
from _slugged_db import add_state
from order_fixture import (
    GAPPED_INVENTORY,
    conn as conn,  # noqa: PLC0414 - pytest fixture registration
    finding_codes,
    install_test_holdings,
    inventory as inventory,  # noqa: PLC0414 - pytest fixture registration
    order_inventory,
    order_project,
)
from reg_meta.order import (
    extraction_filenames,
    materialize_order,
)
from reg_schema.project_data import PeriodRange


class TestBlockingFindings:
    def test_in_availability_gap_blocks_the_whole_order(self, conn, tmp_path) -> None:
        project = order_project("scb/lisa/kon")

        result = materialize_order(
            project,
            install_test_holdings(conn, order_inventory(tmp_path, GAPPED_INVENTORY)),
        )

        assert result.manifest is None
        assert finding_codes(result) == ["coverage_gap"]
        # The exact uncovered subperiod, not just "incomplete".
        assert result.findings[0].period == "2019"
        assert result.findings[0].variable == "scb/lisa/kon"

    def test_steward_mismatch_blocks_before_anything_resolves(
        self, conn, inventory
    ) -> None:
        """The message is the researcher's whole correction — the steward this
        project records, the one this deployment serves, where to order it
        instead, and that its provenance is theirs to change, not the
        application's — with no internal design document to look up. It is
        pinned verbatim HERE, once: the adapters render it and the SPA's
        fixtures mimic it, but this is where the wording lives."""
        project = order_project("scb/lisa/kon", steward="ifau")

        result = materialize_order(project, install_test_holdings(conn, inventory))

        assert result.manifest is None
        assert finding_codes(result) == ["steward_mismatch"]
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
        result = materialize_order(
            order_project(), install_test_holdings(conn, inventory)
        )

        assert result.manifest is None
        assert finding_codes(result) == ["project_empty"]

    def test_every_gap_is_reported_in_one_pass(self, conn, tmp_path) -> None:
        # Two bindings the gapped inventory cannot serve: the researcher sees
        # both at once rather than one per round trip.
        project = order_project("scb/lisa/kon", "scb/lisa/yrke")

        result = materialize_order(
            project,
            install_test_holdings(conn, order_inventory(tmp_path, GAPPED_INVENTORY)),
        )

        assert finding_codes(result) == [
            "coverage_gap",
            "mapping_missing",
            "mapping_missing",
        ]
        assert [f.period for f in result.findings] == ["2019", "2018", "2019..2020"]

    def test_blocked_result_still_reports_its_clips(self, conn, inventory) -> None:
        # The clipped binding materializes, a later binding blocks: the clip is
        # still surfaced, so the researcher sees the window the rest of the
        # order was stated against and fixes everything in one pass.
        project = order_project("scb/lisa/disponibel-inkomst", "scb/lisa/nonexistent")

        result = materialize_order(project, install_test_holdings(conn, inventory))

        assert result.manifest is None
        assert finding_codes(result) == ["variable_unresolved"]
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
            order_project("scb/lisa/kon", period=period),
            install_test_holdings(conn, inventory),
        )

        assert result.manifest is None
        assert finding_codes(result) == ["period_not_orderable"]

    def test_unresolvable_binding_blocks(self, conn, inventory) -> None:
        result = materialize_order(
            order_project("scb/lisa/nonexistent"),
            install_test_holdings(conn, inventory),
        )

        assert result.manifest is None
        assert finding_codes(result) == ["variable_unresolved"]

    def test_binding_outside_availability_blocks(self, conn, inventory) -> None:
        result = materialize_order(
            order_project("scb/lisa/kon", period=2015),
            install_test_holdings(conn, inventory),
        )

        assert result.manifest is None
        assert finding_codes(result) == ["binding_unavailable"]

    def test_unknown_pinned_representation_blocks(self, conn, inventory) -> None:
        result = materialize_order(
            order_project("scb/lisa/yrke", representation="Ssyk5"),
            install_test_holdings(conn, inventory),
        )

        assert result.manifest is None
        assert finding_codes(result) == ["representation_unknown"]

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

        result = materialize_order(
            order_project("scb/lisa/yrke"), install_test_holdings(conn, inventory)
        )

        assert result.manifest is None
        assert finding_codes(result) == ["representation_ambiguous"]

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
            order_project("scb/lisa/yrke", representation="Ssyk4"),
            install_test_holdings(conn, inventory),
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
        project = order_project(
            "scb/lisa/kon",
            "scb/lisa/disponibel-inkomst",
            "scb/lisa/yrke",
            steward="global",
        )

        result = materialize_order(project, conn)

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
        project = order_project("scb/lisa/kon", "scb/lisa/yrke", steward="global")

        first = materialize_order(project, conn).manifest
        second = materialize_order(project, conn).manifest

        assert first is not None
        assert second is not None
        assert first.to_json() == second.to_json()

    def test_representation_change_fans_into_two_entries(self, conn) -> None:
        result = materialize_order(
            order_project("scb/lisa/yrke", steward="global"), conn
        )

        assert result.manifest is not None
        assert [
            (e.logical.representation, e.physical.column, e.physical.edition)
            for e in result.manifest.entries
        ] == [("Ssyk3", "Ssyk3", "2018"), ("Ssyk4", "Ssyk4", "2019..2020")]

    def test_unresolvable_binding_blocks(self, conn) -> None:
        result = materialize_order(
            order_project("scb/lisa/nonexistent", steward="global"), conn
        )

        assert result.manifest is None
        assert finding_codes(result) == ["variable_unresolved"]

    def test_binding_outside_availability_blocks(self, conn) -> None:
        result = materialize_order(
            order_project("scb/lisa/kon", steward="global", period=2015), conn
        )

        assert result.manifest is None
        assert finding_codes(result) == ["binding_unavailable"]

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
            order_project(
                "scb/lisa/yrke",
                steward="global",
                period=PeriodRange(from_=2017, to=2020),
            ),
            conn,
        )

        assert result.manifest is None
        assert finding_codes(result) == ["representation_ambiguous"]
        assert [
            (c.variable, c.requested_period, c.ordered_period) for c in result.clips
        ] == [("scb/lisa/yrke", "2017..2020", "2018..2020")]

    def test_steward_mismatch_blocks_a_steward_project(self, conn) -> None:
        # The global deployment is a deployment like any other: a project whose
        # provenance names a steward is not orderable against it.
        result = materialize_order(order_project("scb/lisa/kon"), conn)

        assert result.manifest is None
        assert finding_codes(result) == ["steward_mismatch"]

    def test_extraction_filename_is_one_file_per_period_segment(self, conn) -> None:
        # `edition = requested_period`, so §12's naming rule gives an
        # interrupted request one file per segment without a special case.
        result = materialize_order(
            order_project("scb/lisa/kon", steward="global", period=(2018, 2020)), conn
        )

        assert result.manifest is not None
        assert extraction_filenames(result.manifest.entries[0]) == (
            "lisa_individer-15plus_2018.csv",
            "lisa_individer-15plus_2020.csv",
        )
