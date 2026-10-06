"""Common occurrence resolution: population conflicts, pooled editions and independent occurrences keep their exact periods."""

from __future__ import annotations

import pytest
from _source_intervals_support import interval_record as _record
from reg_meta.source_evidence import SourceField
from reg_meta_build.source_intervals import resolve_occurrence_intervals
from reg_meta_build.source_occurrences import effective_occurrence
from reg_meta_build.source_periods import source_scopes
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceCoordinate,
    SourceRecord,
    TemporalScope,
    value_field,
)


@pytest.mark.parametrize("negative", [False, True])
def test_population_conflict_withholds_only_its_exact_intersection(
    negative: bool,
) -> None:
    first = _record(
        1,
        population=SourceCoordinate(status="value", name="Adults"),
        negative=negative,
    )
    rival = _record(
        2,
        "2021-04-01",
        "2021-06-30",
        population=SourceCoordinate(status="value", name="All residents"),
        negative=negative,
    )
    result = resolve_occurrence_intervals((first, rival))
    segments = result.negative_segments if negative else result.segments
    assert [(s.valid_from, s.valid_to) for s in segments] == [
        ("2021-01-01", "2021-03-31"),
        ("2021-07-01", "2021-12-31"),
    ]
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert issue.fields == ("subject.population",)
    assert issue.withheld == ("column_segment",)
    assert (issue.valid_from, issue.valid_to) == ("2021-04-01", "2021-06-30")
    assert issue.occurrences == (first, rival)


def test_population_changes_on_disjoint_periods_do_not_change_ordinary_identity() -> (
    None
):
    first = _record(
        1,
        "2021-01-01",
        "2021-03-31",
        population=SourceCoordinate(status="value", name="Adults"),
    )
    second = _record(
        2,
        "2021-04-01",
        "2021-12-31",
        population=SourceCoordinate(status="value", name="All residents"),
    )
    result = resolve_occurrence_intervals((first, second))
    assert result.issues == result.unsupported_occurrences == ()
    assert tuple(s.occurrences for s in result.segments) == ((first,), (second,))


def test_unknown_population_does_not_contradict_one_concrete_population() -> None:
    known = _record(1, population=SourceCoordinate(status="value", name="Adults"))
    unknown = _record(2)
    result = resolve_occurrence_intervals((known, unknown))
    assert result.issues == result.unsupported_occurrences == ()
    assert len(result.segments) == 1
    assert result.segments[0].occurrences == (known, unknown)


def _pooled_record(row: int, label: str, **kwargs) -> SourceRecord:
    """A record whose scopes come from a real pooled edition label (Y-202)."""
    edition, period, issue = source_scopes(label)
    assert issue == "pooled_period"
    return _record(row, scope=period, **kwargs).model_copy(
        update={"edition_scope": edition}
    )


def test_pooled_scope_forms_one_marked_state_over_the_whole_range() -> None:
    result = resolve_occurrence_intervals((_pooled_record(1, "2012 - 2014"),))

    assert result.issues == result.unsupported_occurrences == ()
    assert [(s.valid_from, s.valid_to, s.pooled) for s in result.segments] == [
        ("2012-01-01", "2014-12-31", True)
    ]


def test_pooled_scope_without_a_carried_range_stays_unsupported() -> None:
    scope = TemporalScope.model_validate({"kind": "pooled", "label": "2012 - 2014"})
    record = _record(1, scope=scope)
    result = resolve_occurrence_intervals((record,))

    assert result.segments == ()
    assert result.unsupported_occurrences == (record,)
    assert result.issues[0].fields == ("period",)


def test_lasaren_school_year_hull_forms_one_marked_pooled_state() -> None:
    # Y-208: a Läsåren hull over whole school years resolves exactly like a
    # whole-year pooled range — one marked state, no inferred annual states.
    result = resolve_occurrence_intervals(
        (_pooled_record(1, "Läsåren 1977/1978 - 2024/2025"),)
    )

    assert result.issues == result.unsupported_occurrences == ()
    assert [(s.valid_from, s.valid_to, s.pooled) for s in result.segments] == [
        ("1977-07-01", "2025-06-30", True)
    ]


def test_term_structured_pooled_label_stays_unsupported() -> None:
    # Y-202 P1: term/school-year edges are not whole-year delivery evidence —
    # a real pooled label with that structure resolves nothing.
    record = _pooled_record(1, "Komvux HT 1988 - VT 2024")
    result = resolve_occurrence_intervals((record,))

    assert result.segments == ()
    assert result.unsupported_occurrences == (record,)
    assert result.issues[0].fields == ("period",)


def test_explicit_annual_coverage_wins_over_pooled() -> None:
    pooled = _pooled_record(1, "2012 - 2014")
    annual = _record(
        2,
        scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2013-01-01", end="2013-12-31"),),
        ),
    )
    result = resolve_occurrence_intervals((pooled, annual))

    assert result.issues == result.unsupported_occurrences == ()
    assert [(s.valid_from, s.valid_to, s.pooled) for s in result.segments] == [
        ("2012-01-01", "2012-12-31", True),
        ("2013-01-01", "2013-12-31", False),
        ("2014-01-01", "2014-12-31", True),
    ]


def test_explicit_annual_facts_override_pooled_facts_without_conflict() -> None:
    # Y-202 P1: where an explicit occurrence covers the span, pooled evidence
    # is filtered out BEFORE reconciliation — a differing pooled fact must not
    # dispute the annual state.
    pooled = _pooled_record(1, "2012 - 2014")
    annual = _record(
        2,
        data_type="text",
        scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2013-01-01", end="2013-12-31"),),
        ),
    )
    result = resolve_occurrence_intervals((pooled, annual))

    assert result.issues == result.unsupported_occurrences == ()
    assert [(s.valid_from, s.valid_to, s.pooled) for s in result.segments] == [
        ("2012-01-01", "2012-12-31", True),
        ("2013-01-01", "2013-12-31", False),
        ("2014-01-01", "2014-12-31", True),
    ]
    middle = result.segments[1]
    assert middle.fields.data_type == SourceField(status="value", value="text")
    assert middle.occurrences == (annual,)
    assert middle.effective_occurrences == (effective_occurrence(annual),)


def test_fully_annual_covered_pooled_range_forms_no_pooled_state() -> None:
    pooled = _pooled_record(1, "2012 - 2014")
    annuals = tuple(
        _record(
            row,
            scope=TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start=f"{year}-01-01", end=f"{year}-12-31"),),
            ),
        )
        for row, year in ((2, 2012), (3, 2013), (4, 2014))
    )
    result = resolve_occurrence_intervals((pooled, *annuals))

    assert result.issues == result.unsupported_occurrences == ()
    assert [(s.valid_from, s.valid_to) for s in result.segments] == [
        ("2012-01-01", "2012-12-31"),
        ("2013-01-01", "2013-12-31"),
        ("2014-01-01", "2014-12-31"),
    ]
    assert all(not segment.pooled for segment in result.segments)


def test_overlapping_pooled_editions_with_identical_fields_merge_into_one_state() -> (
    None
):
    # Y-209: overlapping pooled ranges cut at their boundaries into adjacent
    # pooled cuts; identical facts re-join into one state over the hull, with
    # each contributing edition's evidence exactly once.
    first = _pooled_record(1, "2016 - 2018")
    second = _pooled_record(2, "2018 - 2020")
    result = resolve_occurrence_intervals((first, second))

    assert result.issues == result.unsupported_occurrences == ()
    assert [(s.valid_from, s.valid_to, s.pooled) for s in result.segments] == [
        ("2016-01-01", "2020-12-31", True)
    ]
    (segment,) = result.segments
    assert segment.occurrences == (first, second)
    assert [item.source_records for item in segment.effective_occurrences] == [
        (first,),
        (second,),
    ]


def test_overlapping_pooled_editions_with_differing_facts_stay_split() -> None:
    # Y-209: differing reconciled facts never merge — the three cuts stay
    # three pooled states, as before.
    result = resolve_occurrence_intervals(
        (
            _pooled_record(1, "2016 - 2018"),
            _pooled_record(2, "2018 - 2020", data_type="text"),
        )
    )

    assert [(s.valid_from, s.valid_to, s.pooled) for s in result.segments] == [
        ("2016-01-01", "2017-12-31", True),
        ("2018-01-01", "2018-12-31", True),
        ("2019-01-01", "2020-12-31", True),
    ]
    assert [s.fields.data_type.value for s in result.segments] == [
        "integer",
        "text",
        "text",
    ]


def test_pooled_segment_adjacent_to_explicit_does_not_merge() -> None:
    # Y-209: a pooled segment never merges with an explicit neighbor, even
    # where the windows are adjacent.
    pooled = _pooled_record(1, "2012 - 2014")
    annual = _record(
        2,
        scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2015-01-01", end="2015-12-31"),),
        ),
    )
    result = resolve_occurrence_intervals((pooled, annual))

    assert result.issues == result.unsupported_occurrences == ()
    assert [(s.valid_from, s.valid_to, s.pooled) for s in result.segments] == [
        ("2012-01-01", "2014-12-31", True),
        ("2015-01-01", "2015-12-31", False),
    ]


def test_explicit_independent_occurrences_keep_raw_scopes_without_dates():
    scope = TemporalScope(kind="year_independent")
    records = (_record(1, scope=scope), _record(2, scope=scope))
    resolution = resolve_occurrence_intervals(records)
    assert resolution.issues == ()
    (segment,) = resolution.segments
    assert segment.period_scope == "year_independent"
    assert segment.valid_from is segment.valid_to is None
    assert segment.pooled is False
    assert segment.occurrences == records
    assert resolution.unsupported_occurrences == ()


@pytest.mark.parametrize(
    "scope",
    [
        TemporalScope(kind="unknown", label="unresolved source"),
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2021", end="2021"),)
        ),
    ],
)
def test_independent_occurrence_cannot_absorb_other_temporal_claims(scope):
    records = (
        _record(1, scope=TemporalScope(kind="year_independent")),
        _record(2, scope=scope),
    )
    resolution = resolve_occurrence_intervals(records)
    assert resolution.segments == ()
    assert "conflicting_occurrence_scope" in {issue.code for issue in resolution.issues}


def test_same_column_definition_conflict_is_not_absorbed() -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"definition": value_field(definition)}
                )
            }
        )
        for record, definition in zip(
            (_record(1), _record(2)), ("January income", "February income"), strict=True
        )
    )
    result = resolve_occurrence_intervals(records)
    assert result.segments[0].fields.definition == SourceField(status="unknown")
    assert [(issue.code, issue.fields) for issue in result.issues] == [
        ("conflicting_occurrence_facts", ("definition",))
    ]
