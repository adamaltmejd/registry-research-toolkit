"""Common occurrence resolution keeps precise coverage and conflicting evidence."""

from __future__ import annotations

import pytest
from reg_meta_build.source_intervals import resolve_occurrence_intervals
from reg_meta_build.source_occurrences import effective_occurrence
from reg_meta_build.source_periods import source_scopes
from reg_meta_build.source_records import (
    NativeCoordinates,
    RecordLocator,
    ScopeInterval,
    SourceCoordinate,
    SourceField,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)


def _record(
    row: int,
    start: str = "2021-01-01",
    end: str = "2021-12-31",
    *,
    column: str | None = "Column",
    column_negative: bool = False,
    data_type: str | None = "integer",
    operational_definition: str | None = None,
    source_attribution: str | None = None,
    negative: bool = False,
    scope: TemporalScope | None = None,
    population: SourceCoordinate | None = None,
) -> SourceRecord:
    return SourceRecord.create(
        revision=SourceRevision.create(
            dataset="fixture",
            publisher="fixture",
            purpose="interval test",
            upstream_revision="1",
            artifact_path="input.csv",
            artifact_size=1,
            artifact_sha256="a" * 64,
        ),
        locators=(
            RecordLocator(
                semantic_record_key=(f"member:{row}",),
                physical_file="input.csv",
                physical_table="input.csv",
                physical_record=f"row:{row}",
                physical_cells=(f"input.csv:row:{row}:column",),
            ),
        ),
        subject=SourceSubject(
            provider="fixture",
            register=SourceCoordinate(status="value", native_id=1, name="Register"),
            variant=SourceCoordinate(status="value", native_id=2, name="Variant"),
            population=population or SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(status="value", native_id=3, name="Variable"),
            member=SourceCoordinate(status="value", native_id=row),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2021", end="2021"),)
        ),
        edition_period_scope=scope
        or TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start=start, end=end),)
        ),
        fields=SourceFields(
            availability=SourceField(status="negative")
            if negative
            else value_field(True),
            column_name=SourceField(status="negative", raw_value="")
            if column_negative
            else (value_field(column) if column else SourceField(status="unknown")),
            data_type=value_field(data_type)
            if data_type
            else SourceField(status="unknown"),
            definition=value_field("Common definition"),
            operational_definition=value_field(operational_definition)
            if operational_definition
            else None,
            source_attribution=value_field(source_attribution)
            if source_attribution
            else None,
        ),
    )


def test_agreement_retains_physical_duplicates_and_optional_unknowns() -> None:
    first, second = _record(1), _record(2, data_type=None)
    result = resolve_occurrence_intervals((first, first, second))
    assert result.issues == result.unsupported_occurrences == ()
    assert len(result.segments) == 1
    segment = result.segments[0]
    assert segment.occurrences == (first, first, second)
    assert segment.fields.data_type == SourceField(status="value", value="integer")
    assert segment.fields.data_length is None


def test_conflicting_field_is_unknown_only_on_exact_intersection() -> None:
    first = _record(1, "2021-02-02", "2021-03-14")
    rival = _record(2, "2021-02-23", "2021-02-28", data_type="text")
    result = resolve_occurrence_intervals((first, rival))
    assert [
        (s.valid_from, s.valid_to, s.fields.data_type) for s in result.segments
    ] == [
        ("2021-02-02", "2021-02-22", SourceField(status="value", value="integer")),
        ("2021-02-23", "2021-02-28", SourceField(status="unknown")),
        ("2021-03-01", "2021-03-14", SourceField(status="value", value="integer")),
    ]
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert issue.fields == issue.withheld == ("data_type",)
    assert issue.occurrences == (first, rival)
    assert all(
        s.fields.definition == SourceField(status="value", value="Common definition")
        for s in result.segments
    )


@pytest.mark.parametrize(
    "field", ["source_attribution", "operational_definition", "reference_period"]
)
def test_descriptive_text_disagreement_is_unknown_without_occurrence_issue(
    field: str,
) -> None:
    first = _record(1)
    second = _record(2)
    first = first.model_copy(
        update={
            "fields": first.fields.model_copy(update={field: value_field("First text")})
        }
    )
    second = second.model_copy(
        update={
            "fields": second.fields.model_copy(
                update={field: value_field("Second text")}
            )
        }
    )
    result = resolve_occurrence_intervals((first, second))
    assert result.issues == result.unsupported_occurrences == ()
    assert len(result.segments) == 1
    segment = result.segments[0]
    assert getattr(segment.fields, field) == SourceField(status="unknown")
    assert segment.occurrences == (first, second)


@pytest.mark.parametrize(
    ("published", "other"),
    [
        ("Kronor", "kronor"),
        ("Kronor (SEK)", "kronor"),
        ("Antal månader", "Månader"),
        ("Antal veckor", "Veckor"),
        ("Antal minuter", "Minuter"),
        ("Antal barn", "Antal"),
        ("Dagar", "Antal"),
        ("Årtal", "År"),
    ],
)
def test_exact_unit_pair_resolves_at_occurrence_grain_in_either_order(
    published: str, other: str
) -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"measurement_unit": value_field(unit)}
                )
            }
        )
        for record, unit in ((_record(1), published), (_record(2), other))
    )
    for ordered in (records, records[::-1]):
        result = resolve_occurrence_intervals(ordered)
        assert result.issues == ()
        assert result.segments[0].fields.measurement_unit == SourceField(
            status="value", value=published
        )


@pytest.mark.parametrize(
    "units",
    [
        ("Kronor", "Kronor (SEK)"),
        ("Procent", "Andel"),
        ("Veckor", "Antal", "Antal veckor"),
    ],
)
def test_unlisted_unit_sets_remain_occurrence_conflicts(units: tuple[str, ...]) -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"measurement_unit": value_field(unit)}
                )
            }
        )
        for record, unit in zip((_record(i) for i in range(len(units))), units)
    )
    result = resolve_occurrence_intervals(records)
    assert result.segments[0].fields.measurement_unit == SourceField(status="unknown")
    assert [(issue.code, issue.fields) for issue in result.issues] == [
        ("conflicting_occurrence_facts", ("measurement_unit",))
    ]


@pytest.mark.parametrize(
    ("types", "lengths", "expected_length", "expected_fields"),
    [
        (("varchar", "varchar"), ("5", "6"), "6", ()),
        (("varchar", "float"), ("18", "53"), None, ("data_length", "data_type")),
        (("varchar", "varchar"), ("5", "8,2"), None, ("data_length",)),
        (("varchar", None), ("5", "6"), None, ("data_length",)),
        (("varchar", "varchar"), ("05", "6"), None, ("data_length",)),
    ],
)
def test_length_maximum_requires_one_declared_type_and_canonical_integers(
    types: tuple[str | None, str | None],
    lengths: tuple[str, str],
    expected_length: str | None,
    expected_fields: tuple[str, ...],
) -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={
                        "data_type": value_field(kind)
                        if kind is not None
                        else SourceField(status="unknown"),
                        "data_length": value_field(length),
                    }
                )
            }
        )
        for record, kind, length in zip((_record(1), _record(2)), types, lengths)
    )
    for ordered in (records, records[::-1]):
        result = resolve_occurrence_intervals(ordered)
        assert result.segments[0].fields.data_length == (
            SourceField(status="value", value=expected_length)
            if expected_length is not None
            else SourceField(status="unknown")
        )
        assert [issue.fields for issue in result.issues] == (
            [expected_fields] if expected_fields else []
        )


def test_data_type_conflict_still_reports_when_source_attribution_also_differs() -> (
    None
):
    first = _record(1, source_attribution="Fråga 1")
    second = _record(2, data_type="text", source_attribution="Fråga 2")
    result = resolve_occurrence_intervals((first, second))
    assert len(result.segments) == 1
    segment = result.segments[0]
    assert segment.fields.data_type == SourceField(status="unknown")
    assert segment.fields.source_attribution == SourceField(status="unknown")
    assert [(issue.code, issue.fields, issue.withheld) for issue in result.issues] == [
        ("conflicting_occurrence_facts", ("data_type",), ("data_type",))
    ]


def test_unanimous_source_attribution_resolves_normally() -> None:
    first = _record(1, source_attribution="Fråga 1")
    second = _record(2, source_attribution="Fråga 1")
    result = resolve_occurrence_intervals((first, second))
    assert result.issues == result.unsupported_occurrences == ()
    assert result.segments[0].fields.source_attribution == SourceField(
        status="value", value="Fråga 1"
    )


def test_real_gaps_and_parallel_column_periods_survive() -> None:
    result = resolve_occurrence_intervals(
        (
            _record(1, "2020-02-28", "2020-02-29"),
            _record(2, "2020-03-02", "2020-03-03"),
            _record(3, "2020-02-29", "2020-03-02", column="Parallel"),
        )
    )
    assert result.issues == ()
    assert [
        (s.delivery_column_name, s.valid_from, s.valid_to) for s in result.segments
    ] == [
        ("Column", "2020-02-28", "2020-02-29"),
        ("Column", "2020-03-02", "2020-03-03"),
        ("Parallel", "2020-02-29", "2020-03-02"),
    ]


def test_negative_availability_withholds_only_contested_segment() -> None:
    positive = _record(1)
    negative = _record(2, "2021-04-01", "2021-06-30", negative=True)
    result = resolve_occurrence_intervals((positive, negative))
    assert [(s.valid_from, s.valid_to) for s in result.segments] == [
        ("2021-01-01", "2021-03-31"),
        ("2021-07-01", "2021-12-31"),
    ]
    assert result.issues[0].fields == ("availability",)
    assert result.issues[0].withheld == ("column_segment",)
    negative_result = resolve_occurrence_intervals((negative, negative))
    assert negative_result.segments == negative_result.issues == ()
    assert len(negative_result.negative_segments) == 1
    segment = negative_result.negative_segments[0]
    assert (segment.valid_from, segment.valid_to) == ("2021-04-01", "2021-06-30")
    assert segment.occurrences == (negative, negative)
    assert segment.fields.availability == SourceField(status="negative")


@pytest.mark.parametrize("kind", ["pooled", "unknown"])
def test_annual_edition_cannot_override_unsupported_precise_period(kind: str) -> None:
    scope = TemporalScope.model_validate({"kind": kind, "label": "2020-2022"})
    record = _record(1, scope=scope)
    result = resolve_occurrence_intervals((record,))
    assert result.segments == ()
    assert result.unsupported_occurrences == (record,)
    assert result.issues[0].fields == ("period",)


@pytest.mark.parametrize(
    ("start", "end"), [("2021-02-29", "2021-03-31"), ("9999-01-01", "9999-12-31")]
)
def test_invalid_or_unbounded_dates_are_not_materialized(start: str, end: str) -> None:
    record = _record(1, start, end)
    result = resolve_occurrence_intervals((record,))
    assert result.segments == ()
    assert result.unsupported_occurrences == (record,)


def test_lasaret_occurrence_forms_a_state_on_the_school_year_interval() -> None:
    _, period_scope, issue = source_scopes("Läsåret 2014/2015")

    assert issue is None
    result = resolve_occurrence_intervals((_record(1, scope=period_scope),))

    assert result.unsupported_occurrences == result.issues == ()
    assert [(s.valid_from, s.valid_to) for s in result.segments] == [
        ("2014-07-01", "2015-06-30")
    ]


def test_unplaced_occurrence_does_not_erase_independently_supported_content() -> None:
    supported, unnamed = _record(1), _record(2, column=None)
    result = resolve_occurrence_intervals((supported, unnamed))
    assert result.segments[0].occurrences == (supported,)
    assert result.unsupported_occurrences == (unnamed,)
    assert result.issues[0].fields == ("column_name",)


def test_negative_column_omits_the_occurrence_with_a_warning() -> None:
    supported, columnless = _record(1), _record(2, column_negative=True)
    result = resolve_occurrence_intervals((supported, columnless))
    assert [segment.occurrences for segment in result.segments] == [(supported,)]
    assert result.unsupported_occurrences == ()
    (issue,) = result.issues
    assert issue.code == "omitted_columnless_occurrence"
    assert issue.fields == ("column_name",)
    assert issue.occurrences == (columnless,)
    assert (issue.valid_from, issue.valid_to) == (None, None)
    assert issue.withheld == ("occurrence",)


def test_all_columnless_occurrences_form_no_state_and_no_error() -> None:
    result = resolve_occurrence_intervals(
        (_record(1, column_negative=True), _record(2, column_negative=True))
    )
    assert result.segments == ()
    assert result.unsupported_occurrences == ()
    (issue,) = result.issues
    assert issue.code == "omitted_columnless_occurrence"
    assert len(issue.occurrences) == 2


def test_unknown_column_still_errors() -> None:
    record = _record(1, column=None)
    result = resolve_occurrence_intervals((record,))
    assert result.segments == ()
    assert result.unsupported_occurrences == (record,)
    (issue,) = result.issues
    assert issue.code == "unsupported_occurrence"
    assert issue.fields == ("column_name",)


def test_unrelated_native_subjects_cannot_enter_one_ordinary_resolution() -> None:
    first = _record(1)
    unrelated = _record(2).model_copy(
        update={
            "subject": first.subject.model_copy(
                update={"variable": SourceCoordinate(status="value", native_id=99)}
            )
        }
    )
    with pytest.raises(ValueError, match="one source variable and variant"):
        resolve_occurrence_intervals((first, unrelated))


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
        None,
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
