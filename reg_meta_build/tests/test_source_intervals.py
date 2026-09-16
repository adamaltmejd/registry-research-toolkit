"""Common occurrence resolution keeps precise coverage and conflicting evidence."""

from __future__ import annotations

import pytest
from reg_meta_build.source_intervals import resolve_occurrence_intervals
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
    data_type: str | None = "integer",
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
            column_name=value_field(column)
            if column
            else SourceField(status="unknown"),
            data_type=value_field(data_type)
            if data_type
            else SourceField(status="unknown"),
            definition=value_field("Common definition"),
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


def test_unplaced_occurrence_does_not_erase_independently_supported_content() -> None:
    supported, unnamed = _record(1), _record(2, column=None)
    result = resolve_occurrence_intervals((supported, unnamed))
    assert result.segments[0].occurrences == (supported,)
    assert result.unsupported_occurrences == (unnamed,)
    assert result.issues[0].fields == ("column_name",)


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
