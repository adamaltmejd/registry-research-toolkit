"""Semantic validator period coverage against the slugged ``catalog_db`` fixture.

See DESIGN.md → Semantic validation (semantic.py). Covers range and list
periods against state windows: partial coverage, internal gaps, representation
clips, drift across states, and endpoint snapping.
"""

from __future__ import annotations

import pytest
from backend_test_support import project as _project
from reg_meta.catalog import Catalog
from reg_webapp.semantic import validate_semantic


@pytest.fixture
def uneven_representation_catalog():
    """Two co-existing columns with UNEVEN spans: `kon` 2010-9999 and `kon_detalj`
    only 2018-9999. Picking the shorter column over a range that predates it
    under-covers the requested period."""
    from _slugged_db import add_state, add_value_set, build_slugged_db

    conn = build_slugged_db()
    add_value_set(conn, value_set_id=702, codes=[("1", "M"), ("2", "K"), ("3", "X")])
    conn.execute(
        "UPDATE variable_state SET delivery_column_name = 'kon', "
        "valid_from = '2010-01-01', valid_to = '9999-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        delivery_column_name="kon_detalj",
        value_set_version_label="detalj",
        value_set_id=702,
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_representation_under_covering_range_is_clipped(uneven_representation_catalog):
    # Range 2010-2020 picking `kon_detalj` (only 2018+) is the §12 availability
    # clip: the pin is requested where it is available, so validation reports the
    # window the order will carry — informationally, never as an error.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2010, "to": 2020},
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon_detalj",
            }
        ],
    }
    result = validate_semantic(_project([source]), uneven_representation_catalog)
    clip = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert clip.level == "info"
    assert "ordered for 2018..2020" in clip.message, clip.message
    assert "binding_value_set_version_ambiguous" not in {i.code for i in result.issues}
    assert result.ok


@pytest.fixture
def representation_internal_gap_catalog():
    """The PINNED column `kon` is delivered in TWO disjoint windows (2010-2012 and
    2018-9999); a SIBLING column `kon_detalj` fills the middle (2013-2017). All
    three windows are non-overlapping, so no column co-exists with another (no
    `binding_value_set_version_ambiguous`). Over a 2010..2020 range the pinned
    column has the same outer coverage as the full state set once clamped to the
    requested range, so the old outer-bounds check stays silent — yet the pinned
    column's 2013-2017 extract is empty because only the sibling delivers it. This
    is the #342/#465 internal-gap case the gap-based check must catch."""
    from _slugged_db import add_state, add_value_set, build_slugged_db

    conn = build_slugged_db()
    # Re-window the seeded `kon` state to the FIRST pinned-column era.
    conn.execute(
        "UPDATE variable_state SET delivery_column_name = 'kon', "
        "valid_from = '2010-01-01', valid_to = '2012-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    # Second pinned-column era, leaving a 2013-2017 hole in `kon`.
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2018-01-01",
        valid_to="9999-12-31",
        delivery_column_name="kon",
    )
    # Sibling column filling the gap between the two pinned eras.
    add_value_set(conn, value_set_id=702, codes=[("1", "M"), ("2", "K"), ("3", "X")])
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2013-01-01",
        valid_to="2017-12-31",
        delivery_column_name="kon_detalj",
        value_set_version_label="detalj",
        value_set_id=702,
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_representation_internal_gap_in_range_is_clipped(
    representation_internal_gap_catalog,
):
    # #342: the pinned `kon` column is gapped 2013-2017 (a sibling fills it), so
    # the clip is INTERNAL — outer bounds alone would miss it. The ordered period
    # names both eras, which is exactly what the order manifest carries.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2010, "to": 2020},
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon",
            }
        ],
    }
    result = validate_semantic(_project([source]), representation_internal_gap_catalog)
    clip = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert clip.level == "info"
    assert "ordered for 2010..2012,2018..2020" in clip.message, clip.message
    assert "binding_value_set_version_ambiguous" not in {i.code for i in result.issues}
    assert result.ok


def test_representation_internal_gap_in_list_range_segment_is_clipped(
    representation_internal_gap_catalog,
):
    # #465: a list segment can be a range, and the clip spans the whole request
    # rather than being reported per segment — one clip for one request, which is
    # the point of the shared pass.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": [{"from": 2010, "to": 2020}, 2022],
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon",
            }
        ],
    }
    result = validate_semantic(_project([source]), representation_internal_gap_catalog)
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    assert "requested period 2010..2020,2022" in clip[0].message, clip[0].message
    assert "ordered for 2010..2012,2018..2020,2022" in clip[0].message, clip[0].message
    assert result.ok


def test_representation_full_coverage_range_is_no_drift(
    representation_internal_gap_catalog,
):
    # Control: pinning the SIBLING column over the exact window it fully covers
    # leaves no gap vs the full state set → no drift info.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2013, "to": 2017},
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon_detalj",
            }
        ],
    }
    result = validate_semantic(_project([source]), representation_internal_gap_catalog)
    codes = {i.code for i in result.issues}
    assert "binding_state_drifts_within_period" not in codes
    assert result.ok


def test_representation_full_coverage_to_open_end_is_no_drift(
    uneven_representation_catalog,
):
    # Pinning the long-lived column covers the requested range inside an
    # open-ended (`9999-12-31`) state. This guards both overflow and a spurious
    # one-day trailing gap at the open-end sentinel.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2010, "to": 2020},
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon",
            }
        ],
    }
    result = validate_semantic(_project([source]), uneven_representation_catalog)
    assert result.issues == ()
    assert result.ok


@pytest.fixture
def list_year_segment_gap_catalog():
    """The pinned column `kon` starts midway through the 2020 list segment while
    sibling `kon_detalj` fills the leading half. The pin is present in the
    segment, so the per-segment presence loop is not enough."""
    from _slugged_db import add_state, build_slugged_db

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET delivery_column_name = 'kon', "
        "valid_from = '2020-07-01', valid_to = '9999-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2020-01-01",
        valid_to="2020-06-30",
        delivery_column_name="kon_detalj",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_representation_gap_in_list_year_segment_is_clipped(
    list_year_segment_gap_catalog,
):
    # A year member of a list is an INTERVAL, not an instant: pinning `kon`
    # (delivered from July 2020) clips away Jan-Jun 2020, and the ordered period
    # says so to the day rather than rounding the year in.
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": [2020, 2021],
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon",
            }
        ],
    }
    result = validate_semantic(_project([source]), list_year_segment_gap_catalog)
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    assert "ordered for 2020-07-01..2021" in clip[0].message, clip[0].message
    assert result.ok


@pytest.fixture
def february_catalog():
    """A valid February-only delivery under-covers a requested calendar year."""
    from _slugged_db import build_slugged_db

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET delivery_column_name = 'kon', "
        "valid_from = '2019-02-01', valid_to = '2019-02-28' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_february_representation_has_partial_annual_coverage(
    february_catalog,
):
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": 2019, "to": 2019},
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "kon",
            }
        ],
    }
    result = validate_semantic(_project([source]), february_catalog)
    # February delivery does not establish coverage of the rest of the year.
    assert [(i.code, i.level) for i in result.issues] == [
        ("range_period_partially_covered", "info")
    ]
    assert result.ok


def test_representation_merged_family_uses_expanded_windows(catalog):
    # #319/#465: lonfink is one annual state expanded into Jan/Feb/Mars windows
    # sharing the same state_id. A range spanning them must compare the expanded
    # windows, not collapse them by state_id before checking a pinned
    # representation (collapsed, the annual state would look fully covering).
    source = {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": {"from": "2018-01", "to": "2018-03"},
        "bindings": [
            {
                "variable": "scb/lisa/lonfink",
                "type": "numeric",
                "representation": "LonFinkMars",
            }
        ],
    }
    result = validate_semantic(_project([source]), catalog)
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    # The request renders through the shared grammar, so it canonicalizes to
    # the coarsest exact token (`2018-01..2018-03` IS `2018-Q1`) — the same
    # spelling the order manifest carries.
    assert "requested period 2018-Q1" in clip[0].message, clip[0].message
    assert "ordered for 2018-03" in clip[0].message, clip[0].message
    assert result.ok


# ── #207: an explicit range PARTIALLY covered by the concept's states.
# `resolve_at` returns the states INTERSECTING the requested `[from, to]`; if their
# union leaves a gap NO column delivers, the binding silently drops that sub-range.
# `range_period_partially_covered` (info) surfaces it. This is the WHOLE-CONCEPT
# under-coverage case — distinct from #204's `binding_state_drifts_within_period`,
# which is the CHOSEN representation under-covering vs a sibling column that DOES
# deliver the gap. Zero coverage stays `period_outside_state_validity`.


def _kon_source(period) -> dict:
    return {
        "name": "s",
        "register_variant": "scb/lisa/individer-15plus",
        "period": period,
        "bindings": [{"variable": "scb/lisa/kon", "type": "categorical"}],
    }


def test_range_fully_covered_has_no_partial_finding(catalog):
    # kon spans 2018-01-01..9999-12-31; a range fully inside it has no gap.
    result = validate_semantic(
        _project([_kon_source({"from": 2019, "to": 2021})]),
        catalog,
    )
    codes = {i.code for i in result.issues}
    assert "range_period_partially_covered" not in codes
    assert result.ok


def test_range_partially_covered_is_flagged(catalog):
    # kon first delivered 2018; a 2010-2020 binding is clipped to 2018-2020.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2020})]),
        catalog,
    )
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert issue.level == "info"
    assert issue.path == "/sources/0/bindings/0/variable"
    # The report names the period the ORDER will carry, in the manifest's own
    # spelling — not a gap the researcher has to subtract themselves.
    assert "requested period 2010..2020" in issue.message, issue.message
    assert "ordered for 2018..2020" in issue.message, issue.message
    # Info is non-blocking: the available sub-range still orders.
    assert result.ok


def test_zero_coverage_is_only_period_outside_state_validity(catalog):
    # A range entirely BEFORE kon's first state is zero coverage, not partial —
    # `resolve_at` returns no states, so only the existing code fires.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2015})]),
        catalog,
    )
    codes = {i.code for i in result.issues}
    assert "period_outside_state_validity" in codes
    assert "range_period_partially_covered" not in codes


def test_point_period_has_no_partial_finding(catalog):
    # A point period is a single instant — no requested span to under-cover.
    result = validate_semantic(_project([_kon_source(2018)]), catalog)
    assert "range_period_partially_covered" not in {i.code for i in result.issues}


@pytest.fixture
def internal_gap_catalog():
    """`scb/lisa/kon` delivered in TWO non-adjacent windows under one variant:
    2010-2012, then 2016-9999 — an INTERNAL gap (2013-2015 has no state at all).
    The seeded state is re-windowed to the later era; the earlier era is added as a
    second state, leaving the 2013-2015 hole."""
    from _slugged_db import add_state, build_slugged_db

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET valid_from = '2016-01-01', valid_to = '9999-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2010-01-01",
        valid_to="2012-12-31",
        delivery_column_name="Kon",  # the same column, delivered in two eras
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_internal_gap_in_range_is_flagged(internal_gap_catalog):
    # Range 2010-2018 over a concept with a 2013-2015 hole → the ordered period
    # is the disjoint pair, so the hole is named by what it interrupts.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2018})]),
        internal_gap_catalog,
    )
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert "ordered for 2010..2012,2016..2018" in issue.message, issue.message
    assert result.ok


def test_day_adjacent_windows_leave_no_gap(internal_gap_catalog):
    # A range covering only the populated tail (2016+) is fully covered. Guards the
    # adjacency math: string `valid_from > valid_to` would false-positive a gap.
    result = validate_semantic(
        _project([_kon_source({"from": 2016, "to": 2018})]),
        internal_gap_catalog,
    )
    assert "range_period_partially_covered" not in {i.code for i in result.issues}


@pytest.mark.parametrize(
    "good_endpoint",
    [
        "2019-02",  # YYYY-02 in a NON-leap year: synthesized hi = 2019-02-29 (an
        # over-counted, non-real day) — the crash case. Must snap to 2019-02-28.
        "2020-02",  # leap-year Feb, for symmetry (synthesized hi IS a real day)
        "2019-12",  # plain month token
        "2019-Q1",  # quarter (hi = 2019-03-31)
        "2019-H1",  # half (hi = 2019-06-30)
        "HT2019",  # autumn term
        "VT2019",  # spring term
        "2019-02-28",  # a real Feb day token
    ],
)
def test_valid_period_token_to_endpoint_is_accepted(catalog, good_endpoint):
    # Regression for the synthesized-`hi` over-count CRASH (#239 follow-up): the
    # token is the `to` endpoint, so its synthesized UPPER bound is what the gap
    # math runs real `date` arithmetic on. A non-leap `2019-02` expands to a
    # 2019-02-29 hi that `date.fromisoformat` rejects — `_requested_range_bounds`
    # snaps it to 2019-02-28 so the call must NOT raise. `from: 2019` is ≤ every
    # endpoint and intersects kon's 2018-01-01..9999-12-31 state, so the range is
    # FULLY covered: no `invalid_period`, no spurious phantom Feb-29 gap, usable.
    source = _kon_source({"from": 2019, "to": good_endpoint})
    result = validate_semantic(_project([source]), catalog)
    codes = {i.code for i in result.issues}
    assert "invalid_period" not in codes, codes
    assert "range_period_partially_covered" not in codes, codes
    assert "period_outside_state_validity" not in codes, codes
    assert result.ok, result.issues


def test_non_leap_feb_to_endpoint_gap_is_snapped_not_phantom(catalog):
    # The snapped synthesized hi must produce CORRECT gap math, not a phantom
    # Feb-29 span. kon starts 2018-01-01; a `{2017, "2019-02"}` range has exactly
    # ONE real gap — the leading 2017 (uncovered before kon's first state) — and
    # the covered tail ends at the snapped 2019-02-28, never an impossible -02-29.
    result = validate_semantic(
        _project([_kon_source({"from": 2017, "to": "2019-02"})]),
        catalog,
    )
    issue = next(i for i in result.issues if i.code == "range_period_partially_covered")
    assert "ordered for 2018..2019-02-28" in issue.message, issue.message
    # No impossible Feb-29 leaked into either endpoint.
    assert "2019-02-29" not in issue.message, issue.message
    assert result.ok


# ── #307: the period LIST form (interrupted series) ──────────────────────────
# Structural validation guarantees the list is non-empty, sorted, and disjoint
# before this layer runs. The shared pass then treats the list as ONE request
# with holes, not a series of independent ones: it reports one
# `range_period_partially_covered` for the whole request rather than one per
# segment. The holes stay real, though — the request is NOT its enclosing range:
# a window overlapping only inside a HOLE is never co-existence, and segments are
# resolved on their own terms so one cannot suppress another's canonical
# fallback.


def test_list_period_clean_resolves_with_replacement_hint(catalog):
    # Both segments inside kon's single 2018+ state: the state intersecting both
    # segments counts ONCE (no phantom drift info). The fixture's kon→syss
    # succession is effective for the second segment, so the only issue is the
    # non-blocking replacement hint.
    result = validate_semantic(
        _project([_kon_source([2018, {"from": 2019, "to": 2020}])]),
        catalog,
    )
    assert [i.code for i in result.issues] == ["variable_replaced"]
    assert result.ok


def test_list_period_uncovered_segment_is_clipped_not_an_error(catalog):
    # Y-45: kon's state starts 2018, so the 2010..2012 segment simply is not
    # available — under §12 intersection semantics the request is CLIPPED to
    # where the binding exists and the order carries 2018. A segment the
    # materializer drops must not be an error the SPA blocks the order on.
    result = validate_semantic(
        _project([_kon_source([{"from": 2010, "to": 2012}, 2018])]),
        catalog,
    )
    assert "period_outside_state_validity" not in {i.code for i in result.issues}
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    assert "requested period 2010..2012,2018" in clip[0].message, clip[0].message
    assert "ordered for 2018" in clip[0].message, clip[0].message
    assert result.ok


def test_list_period_reports_one_clip_for_the_whole_request(catalog):
    # Several unavailable segments are ONE clip stating the whole ordered
    # period, not one finding per segment: the researcher fixes the period
    # against what the order will actually carry.
    result = validate_semantic(
        _project([_kon_source([2015, 2016, 2018])]),
        catalog,
    )
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    # 2015 and 2016 are day-adjacent, so the request is the merged
    # `2015..2016,2018` — two windows, not three.
    assert "requested period 2015..2016,2018" in clip[0].message, clip[0].message
    assert "ordered for 2018" in clip[0].message, clip[0].message
    assert result.ok


def test_list_period_partial_coverage_names_the_ordered_period(catalog):
    # kon starts 2018: the 2017..2019 segment is partly available and the
    # 2021..2022 segment fully. One info, naming the disjoint window that
    # survives the clip.
    result = validate_semantic(
        _project(
            [_kon_source([{"from": 2017, "to": 2019}, {"from": 2021, "to": 2022}])]
        ),
        catalog,
    )
    partial = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(partial) == 1
    assert "requested period 2017..2019,2021..2022" in partial[0].message
    assert "ordered for 2018..2019,2021..2022" in partial[0].message
    assert result.ok


def test_list_period_segments_on_distinct_states_report_drift(internal_gap_catalog):
    # The gap fixture delivers kon in TWO windows (2010-2012, 2016+). A list
    # period with one segment in each window unions TWO distinct states →
    # the sequential-drift info fires (and the union is what admission sees).
    result = validate_semantic(
        _project(
            [_kon_source([{"from": 2010, "to": 2012}, {"from": 2016, "to": 2018}])]
        ),
        internal_gap_catalog,
    )
    drift = [i for i in result.issues if i.code == "binding_state_drifts_within_period"]
    assert len(drift) == 1
    assert "spans 2 states" in drift[0].message
    assert result.ok


@pytest.fixture
def gap_overlap_catalog():
    """Two DISTINCT columns whose validity windows overlap only BETWEEN the
    requested segments of an interrupted series: `Kon` 2005-2014 and `KonNy`
    2012-2025 (mutual overlap 2012-2014). A researcher binding segments
    [2010, 2020] touches one column per segment — never both at one requested
    instant — while the scalar range 2010..2020 includes the overlap window."""
    from _slugged_db import add_state, build_slugged_db

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET valid_from = '2005-01-01', "
        "valid_to = '2014-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2012-01-01",
        valid_to="2025-12-31",
        delivery_column_name="KonNy",
        value_set_version_label="ny",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_list_period_no_false_ambiguity_across_segment_gap(gap_overlap_catalog):
    # Codex P2 (#334): the two columns' windows overlap only in 2012-2014 —
    # BETWEEN the requested segments — so no requested instant extracts both.
    # The per-segment probe must NOT raise the blocking ambiguity error; the
    # series resolves to one column per segment (a drift info, like a rename).
    result = validate_semantic(
        _project([_kon_source([2010, 2020])]),
        gap_overlap_catalog,
    )
    codes = {i.code for i in result.issues}
    assert "binding_value_set_version_ambiguous" not in codes
    assert "binding_state_drifts_within_period" in codes
    assert result.ok


def test_scalar_range_through_the_overlap_is_still_ambiguous(gap_overlap_catalog):
    # Control: the scalar range 2010..2020 INCLUDES the 2012-2014 overlap
    # window, so the same catalog genuinely is ambiguous there — the
    # per-segment probe must not have weakened the scalar behavior.
    result = validate_semantic(
        _project([_kon_source({"from": 2010, "to": 2020})]),
        gap_overlap_catalog,
    )
    by_code = {i.code: i for i in result.issues}
    assert "binding_value_set_version_ambiguous" in by_code
    assert not result.ok


@pytest.fixture
def middle_segment_sibling_catalog():
    """The pinned column `Kon` delivers 2009-2011 and 2019-2025 (two states);
    only the sibling `KonAlt` delivers 2014-2016. Segments [2010, 2015, 2020]
    have matching OUTER bounds for both the pin and the union — the middle
    segment's hole is invisible to the outer-bounds drift check."""
    from _slugged_db import add_state, build_slugged_db

    conn = build_slugged_db()
    conn.execute(
        "UPDATE variable_state SET valid_from = '2009-01-01', "
        "valid_to = '2011-12-31' "
        "WHERE variable_id = (SELECT variable_id FROM variable WHERE slug = 'kon')"
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2019-01-01",
        valid_to="2025-12-31",
        delivery_column_name="Kon",
    )
    add_state(
        conn,
        register_id=1,
        variable_slug="kon",
        register_variant_id=10,
        valid_from="2014-01-01",
        valid_to="2016-12-31",
        delivery_column_name="KonAlt",
    )
    conn.commit()
    try:
        yield Catalog(conn)
    finally:
        conn.close()


def test_pinned_representation_missing_middle_segment_is_clipped(
    middle_segment_sibling_catalog,
):
    # The pin exists at the outer segments only, so the middle segment's 2015
    # extract would be empty for that column. The ordered period drops it, which
    # is both the report and what the manifest will say.
    source = {
        **_kon_source([2010, 2015, 2020]),
        "bindings": [
            {
                "variable": "scb/lisa/kon",
                "type": "categorical",
                "representation": "Kon",
            }
        ],
    }
    result = validate_semantic(_project([source]), middle_segment_sibling_catalog)
    clip = [i for i in result.issues if i.code == "range_period_partially_covered"]
    assert len(clip) == 1
    assert "requested period 2010,2015,2020" in clip[0].message, clip[0].message
    assert "ordered for 2010,2020" in clip[0].message, clip[0].message
    # Non-blocking (info): the available segments still order.
    assert result.ok
