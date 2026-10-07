"""Order behavior at the library return and manifest-byte boundaries."""

from __future__ import annotations

from _slugged_db import add_state, add_variable
from order_fixture import (
    conn as conn,  # noqa: PLC0414 - pytest fixture registration
    resolve_project_binding,
)


class TestSharedResolution:
    """`resolve_binding` — materializer steps 1+2, the ONE availability/slicing answer
    the materializer and the webapp's `/api/project/validate` both read.

    The order contract is intersection semantics: a binding is requested wherever it IS
    available inside the source window, so narrower availability clips (and is
    reported) rather than blocking.
    """

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
