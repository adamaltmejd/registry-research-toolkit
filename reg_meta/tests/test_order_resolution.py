"""Order behavior at the library return and manifest-byte boundaries."""

from __future__ import annotations

from _slugged_db import add_state, add_variable
from order_fixture import (
    add_window,
    conn as conn,  # noqa: PLC0414 - pytest fixture registration
    resolve_project_binding,
)
from reg_schema.project_data import PeriodRange


class TestSharedResolution:
    """`resolve_binding` — §12 steps 1+2, the ONE availability/slicing answer
    the materializer and the webapp's `/api/project/validate` both read.

    §12 is intersection semantics: a binding is requested wherever it IS
    available inside the source window, so narrower availability clips (and is
    reported) rather than blocking.
    """

    def test_a_disjoint_request_is_clipped_to_what_exists(self, conn) -> None:
        # `kon` starts in 2018, so `[2010, 2018]` keeps its available half.
        resolution = resolve_project_binding(conn, "scb/lisa/kon", (2010, 2018))
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
        listed = resolve_project_binding(conn, "scb/lisa/kon", (2010, 2018))
        ranged = resolve_project_binding(
            conn, "scb/lisa/kon", PeriodRange(from_=2010, to=2018)
        )
        assert ranged.finding is None
        assert ranged.slices == listed.slices
        assert ranged.availability == listed.availability
        assert ranged.clip is not None
        assert ranged.clip.ordered_period == listed.clip.ordered_period

    def test_an_entirely_unavailable_binding_still_blocks(self, conn) -> None:
        # Clipping to nothing is not an order — this stays an error.
        resolution = resolve_project_binding(conn, "scb/lisa/kon", 2015)
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

        disjoint = resolve_project_binding(conn, "scb/lisa/yrke", (2018, 2020))
        assert disjoint.finding is None
        assert disjoint.slices == (
            ("2018-01-01", "2018-12-31", "Ssyk3"),
            ("2020-01-01", "2020-12-31", "Ssyk4"),
        )

        through = resolve_project_binding(
            conn, "scb/lisa/yrke", PeriodRange(from_=2018, to=2020)
        )
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

        resolution = resolve_project_binding(
            conn, "scb/lisa/kon-hal", (2010, 2020), "KonHole"
        )
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

        resolution = resolve_project_binding(
            conn, "scb/lisa/kon-kant", (2010, 2020), "Missing"
        )
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
            add_window(conn, variable_slug="kon-manad", column=column, lo=lo, hi=hi)

        # March alone: the annual claim serves it.
        march = resolve_project_binding(conn, "scb/lisa/kon-manad", "2018-03", "KonJan")
        assert march.finding is None
        assert march.clip is None
        assert march.slices == (("2018-03-01", "2018-03-31", "KonJan"),)

        # And asking for January TOO must not cost March its coverage.
        both = resolve_project_binding(
            conn, "scb/lisa/kon-manad", ("2018-01", "2018-03"), "KonJan"
        )
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
        resolution = resolve_project_binding(
            conn, "scb/lisa/yrke", ("2019-Q1", "2020-Q1")
        )
        assert len(resolution.states) == 1
        assert resolution.states[0].intervals == (
            ("2019-01-01", "2019-03-31"),
            ("2020-01-01", "2020-03-31"),
        )
        assert resolution.clip is None
