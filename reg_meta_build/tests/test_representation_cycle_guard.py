"""Representation succession cycles are allowed only as single time-monotone round-trips."""

from __future__ import annotations

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.relations import (
    reject_nonmonotone_representation_cycles,
)


class TestRejectNonmonotoneRepresentationCycles:
    """#846: the pure, DB-free year-aware cycle checker. A representation cycle is
    permitted iff it is a single time-monotone round-trip (distinct, present years,
    one wrap); a non-cycle graph passes; a missing-year / same-year / multi-wrap
    cycle is rejected (EXIT_CONFIG, `replaced_by_cycle`)."""

    def test_empty_and_acyclic_pass(self) -> None:
        # No edges, and a plain chain A→B→C, both pass.
        reject_nonmonotone_representation_cycles([])
        reject_nonmonotone_representation_cycles([("A", "B", 2010), ("B", "C", 2014)])

    def test_distinct_year_two_cycle_allowed(self) -> None:
        reject_nonmonotone_representation_cycles([("A", "B", 2014), ("B", "A", 2018)])

    def test_missing_year_cycle_rejected(self) -> None:
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [("A", "B", None), ("B", "A", 2018)]
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_cycle"

    def test_same_year_cycle_rejected(self) -> None:
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [("A", "B", 2014), ("B", "A", 2014)]
            )
        assert exc.value.code == "replaced_by_cycle"

    def test_monotone_three_cycle_allowed(self) -> None:
        # A→B→C→A with strictly increasing years (one wrap at the close) is a
        # faithful round-trip.
        reject_nonmonotone_representation_cycles(
            [("A", "B", 2010), ("B", "C", 2014), ("C", "A", 2018)]
        )

    def test_nonmonotone_three_cycle_rejected(self) -> None:
        # Distinct years that do NOT form a single monotone wrap (rotating to the
        # min year still has a mid-cycle descent) → an impossible multi-wrap order.
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [("A", "B", 2010), ("B", "C", 2018), ("C", "A", 2014)]
            )
        assert exc.value.code == "replaced_by_cycle"

    def test_self_loop_rejected(self) -> None:
        # A column can't succeed itself: a self-loop A→A is a cycle and must be
        # rejected even with a present year.
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles([("A", "A", 2010)])
        assert exc.value.code == "replaced_by_cycle"

    def test_cycle_into_finished_node_rejected(self) -> None:
        # Codex's regression (#846): the white/gray/black DFS this replaced only
        # validated a cycle on a GRAY (on-stack) back-edge, so a NON-monotone cycle
        # reaching an already-FINISHED node fell through unchecked. Here a permitted
        # monotone 2-cycle A↔C (2010/2020) and a non-monotone 3-cycle A→B→C→A share
        # the node C; once the short cycle finishes C, the DFS would never validate
        # the long one. SCC validation sees ONE tangled component {A,B,C} and
        # rejects it (more than a single elementary loop), so the gap is closed.
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [
                    ("A", "C", 2010),
                    ("C", "A", 2020),
                    ("A", "B", 2010),
                    ("B", "C", 2005),
                    ("C", "A", 2020),
                ]
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_cycle"

    def test_two_interleaved_cycles_rejected(self) -> None:
        # Two simple monotone cycles sharing nodes form one SCC that is NOT a single
        # elementary loop (a node has two in-component successors) → rejected, even
        # though each cycle in isolation would be a clean round-trip.
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [
                    ("A", "B", 2010),
                    ("B", "A", 2012),
                    ("B", "C", 2014),
                    ("C", "B", 2016),
                ]
            )
        assert exc.value.code == "replaced_by_cycle"

    def test_two_disjoint_monotone_cycles_allowed(self) -> None:
        # Two INDEPENDENT variant-scoped round-trips (disjoint node sets) are each a
        # single elementary monotone loop → both permitted. Confirms SCC validation
        # is per-component, not global.
        reject_nonmonotone_representation_cycles(
            [
                ("A", "B", 2010),
                ("B", "A", 2014),
                ("C", "D", 2011),
                ("D", "C", 2015),
            ]
        )
