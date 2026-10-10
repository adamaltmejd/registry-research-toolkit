"""Occurrence resolution as interval algebra: the fold over one variable's occurrences.

What a build shows of it (cuts at exact bounds, reconciled facts, unplaced and
columnless members, pooled editions) is the `occurrence-` build cases. These tests pin
what no build case reaches: that the fold does not depend on the order its occurrences
arrive in, the fail-fast guard on its input, and the year-independent scope only the
LISA reader supplies, which the build cases have no source for.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _source_intervals_support import interval_record
from hypothesis import given, settings, strategies as st
from reg_meta_build.source_intervals import resolve_occurrence_intervals
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceCoordinate,
    SourceRecord,
    TemporalScope,
    value_field,
)

if TYPE_CHECKING:
    from collections.abc import Callable

_BOUNDS = ("2021-01-01", "2021-02-01", "2021-03-01", "2021-06-30", "2021-12-31")


@st.composite
def _occurrence(draw: Callable, row: int) -> SourceRecord:
    start, end = sorted(
        draw(st.lists(st.sampled_from(_BOUNDS), min_size=2, max_size=2))
    )
    kind = draw(
        st.sampled_from(("intervals", "intervals", "intervals", "pooled", "unknown"))
    )
    scope = (
        None
        if kind == "intervals"
        else TemporalScope.model_validate(
            {"kind": kind, "label": f"{start}/{end}"}
            | ({"pooled_start": start, "pooled_end": end} if kind == "pooled" else {})
        )
    )
    column = draw(
        st.sampled_from(("Column", "Column", "Column", "Column", "Parallel", None, ""))
    )
    record = interval_record(
        row,
        start,
        end,
        column=column or None,
        column_negative=column == "",
        data_type=draw(st.sampled_from(("integer", "text", "float", "date", None))),
        source_attribution=draw(st.sampled_from((None, "Fråga 1", "Fråga 2"))),
        scope=scope,
    )
    unit = draw(st.sampled_from((None, "MWh", "Megawattimmar")))
    if unit is None:
        return record
    return record.model_copy(
        update={
            "fields": record.fields.model_copy(
                update={"measurement_unit": value_field(unit)}
            )
        }
    )


@st.composite
def _occurrences(draw: Callable) -> tuple[SourceRecord, ...]:
    size = draw(st.integers(min_value=1, max_value=6))
    return tuple(draw(_occurrence(row)) for row in range(1, size + 1))


def _rows(records: tuple[SourceRecord, ...]) -> tuple[str, ...]:
    return tuple(sorted(record.locators[0].physical_record for record in records))


def _canonical(records: tuple[SourceRecord, ...]) -> tuple:
    """The resolution with every input-ordered tuple read as a multiset."""
    result = resolve_occurrence_intervals(records)

    def segments(found) -> list:
        return sorted(
            (
                segment.delivery_column_name,
                segment.valid_from,
                segment.valid_to,
                segment.pooled,
                segment.fields.model_dump_json(),
                _rows(segment.occurrences),
            )
            for segment in found
        )

    issues = sorted(
        (
            issue.code,
            issue.fields,
            issue.valid_from or "",
            issue.valid_to or "",
            issue.withheld,
            _rows(issue.occurrences),
        )
        for issue in result.issues
    )
    return (
        segments(result.segments),
        issues,
        _rows(result.unsupported_occurrences),
    )


# Pydantic construction per example overruns the default deadline on a loaded machine.
@settings(deadline=None)
@given(_occurrences(), st.data())
def test_occurrence_resolution_ignores_input_order(
    records: tuple[SourceRecord, ...], data: st.DataObject
) -> None:
    """Any order of the same occurrences resolves to the same segments, facts, issues
    and unplaced occurrences. Only the order inside each tuple follows the input.

    Fails if a reconciled fact keeps the first-seen value (a unit pair resolving to
    whichever spelling came first, or the documented-type provenance joined unsorted),
    or if a cut, a pooled merge or a withheld segment depends on arrival order.
    """
    shuffled = tuple(data.draw(st.permutations(records)))
    assert _canonical(shuffled) == _canonical(records)


def test_unrelated_native_subjects_cannot_enter_one_ordinary_resolution() -> None:
    """The fold refuses occurrences of two source variables rather than merging them.

    Formation groups occurrences by variable and variant before it calls the fold, so
    no build reaches this fail-fast guard. Fails if the subject check in
    `resolve_occurrence_intervals` is removed.
    """
    first = interval_record(1)
    unrelated = interval_record(2).model_copy(
        update={
            "subject": first.subject.model_copy(
                update={"variable": SourceCoordinate(status="value", native_id=99)}
            )
        }
    )
    with pytest.raises(ValueError, match="one source variable and variant"):
        resolve_occurrence_intervals((first, unrelated))


def test_year_independent_occurrences_form_one_undated_state() -> None:
    """Year-independent occurrences of one column form one state with no dates.

    Only the LISA reader produces a year-independent scope. Fails if the fold dates
    the state, marks it pooled, or reports a year-independent occurrence as unplaced.
    """
    scope = TemporalScope(kind="year_independent")
    records = (interval_record(1, scope=scope), interval_record(2, scope=scope))
    result = resolve_occurrence_intervals(records)
    assert result.issues == result.unsupported_occurrences == ()
    (segment,) = result.segments
    assert segment.period_scope == "year_independent"
    assert segment.valid_from is segment.valid_to is None
    assert segment.pooled is False
    assert segment.occurrences == records


@pytest.mark.parametrize(
    "scope",
    [
        TemporalScope(kind="unknown", label="unresolved source"),
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2021", end="2021"),)
        ),
    ],
    ids=["unknown", "dated"],
)
def test_year_independent_occurrence_cannot_absorb_other_temporal_claims(
    scope: TemporalScope,
) -> None:
    """A column claimed both year-independent and otherwise forms no state.

    Only the LISA reader produces a year-independent scope. Fails if
    `resolve_occurrence_intervals` stops reporting `conflicting_occurrence_scope` for a
    column that also has a dated or unknown claim, or forms a state for it.
    """
    records = (
        interval_record(1, scope=TemporalScope(kind="year_independent")),
        interval_record(2, scope=scope),
    )
    result = resolve_occurrence_intervals(records)
    assert result.segments == ()
    assert "conflicting_occurrence_scope" in {issue.code for issue in result.issues}
