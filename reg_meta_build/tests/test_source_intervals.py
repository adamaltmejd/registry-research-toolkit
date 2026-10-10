"""Occurrence resolution as interval algebra: the fold over one variable's occurrences.

What a build shows of it (cuts at exact bounds, reconciled facts, unplaced and
columnless members) is the `occurrence-` build case. These two tests pin what no build
reaches: that the fold does not depend on the order its occurrences arrive in, and the
fail-fast guard on its input.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _source_intervals_support import interval_record
from hypothesis import given, settings, strategies as st
from reg_meta_build.source_intervals import resolve_occurrence_intervals
from reg_meta_build.source_records import (
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
        negative=draw(st.sampled_from((False, False, False, False, False, True))),
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
        segments(result.negative_segments),
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
