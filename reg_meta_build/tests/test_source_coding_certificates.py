"""A documented period block intersects its anchor's supplied period with the known delivery."""

from __future__ import annotations

from dataclasses import replace

import pytest
from reg_meta_build.source_coding import CodeListClaim, CodeMembershipClaim
from reg_meta_build.source_records import ScopeInterval, TemporalScope


@pytest.mark.parametrize("end", ["2020-12-31", None])
def test_documented_period_block_intersects_known_delivery_scope(end):
    """Interval algebra: an anchor supplied as 2019-2020 (or open 2019-) over a delivery
    known from 2020-01-01 (to 2020-12-31, or open) yields the delivery-scoped block, which
    covers 2020 and not 2019.

    The build case `coding-documented-period-block-states-only-its-block` (periodopen)
    shows the allowed side only: a build refuses a 2019 entry first because VJ has no
    2019 delivery ("period has no column occurrence"), so the intersection's lower bound
    is never observable there. Fails if `documented_period_block` stops intersecting the
    anchor's supplied window with the claim's delivery scope, or
    `documented_period_block_matches` accepts a window outside that intersection.
    """
    from reg_meta_build.source_coding_choices import (
        documented_period_block,
        documented_period_block_matches,
    )
    from reg_meta_build.source_values import SourceValueAssociation, SourceValueWindow

    effective = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020-01-01", end=end),)
    )
    association = SourceValueAssociation(
        6,
        "book",
        "6",
        "book.xlsx",
        "codes",
        supplied_period="2019-2020" if end else "2019-",
        supplied_window=SourceValueWindow("known", "2019-01-01", end),
    )
    claim = CodeListClaim(
        "Source",
        effective,
        (
            CodeMembershipClaim(
                "00", "Original", effective, associations=(association,)
            ),
            CodeMembershipClaim(
                "blank",
                "Missing",
                TemporalScope(kind="year_independent"),
                associations=(
                    replace(
                        association,
                        row_number=7,
                        value_key="7",
                        supplied_period=None,
                        supplied_window=None,
                    ),
                ),
            ),
        ),
        version_label="Source",
    )
    pairs = (("00", "Original"), ("blank", "Missing"))
    assert documented_period_block((claim,), association.locator) == (effective, pairs)
    assert documented_period_block_matches(
        (claim,), association.locator, pairs, "2020-01-01", "2020-12-31", effective
    )
    assert not documented_period_block_matches(
        (claim,), association.locator, pairs, "2019-01-01", "2019-12-31", effective
    )
