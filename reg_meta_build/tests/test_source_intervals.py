"""Common occurrence resolution keeps precise coverage and conflicting evidence: field agreement, type widening, units, storage caps and gaps."""

from __future__ import annotations

import pytest
from _source_intervals_support import interval_record as _record
from reg_meta.source_evidence import SourceField
from reg_meta_build.source_intervals import resolve_occurrence_intervals
from reg_meta_build.source_periods import source_scopes
from reg_meta_build.source_records import (
    SourceCoordinate,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.swecov_column_types import StewardColumnStorage


def test_agreement_retains_physical_duplicates_and_optional_unknowns() -> None:
    first, second = _record(1), _record(2, data_type=None)
    result = resolve_occurrence_intervals((first, first, second))
    assert result.issues == result.unsupported_occurrences == ()
    assert len(result.segments) == 1
    segment = result.segments[0]
    assert segment.occurrences == (first, first, second)
    assert segment.fields.data_type == SourceField(status="value", value="integer")
    assert segment.fields.data_length is None


def test_conflicting_type_widens_only_on_exact_intersection() -> None:
    first = _record(1, "2021-02-02", "2021-03-14")
    rival = _record(2, "2021-02-23", "2021-02-28", data_type="text")
    result = resolve_occurrence_intervals((first, rival))
    assert [
        (s.valid_from, s.valid_to, s.fields.data_type.value) for s in result.segments
    ] == [
        ("2021-02-02", "2021-02-22", "integer"),
        ("2021-02-23", "2021-02-28", "text"),
        ("2021-03-01", "2021-03-14", "integer"),
    ]
    assert result.issues == ()
    assert result.segments[1].occurrences == (first, rival)
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
        ("MWh", "Megawattimmar"),
        ("Kronor", "kronor"),
        ("Kronor (SEK)", "kronor"),
        ("Antal månader", "Månader"),
        ("Månader", "Månad"),
        ("Antal veckor", "Veckor"),
        ("Antal minuter", "Minuter"),
        ("Antal barn", "Antal"),
        ("Antal", "Antal elever"),
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
        ("Antal månader", "Månader", "Månad"),
        ("År", "Datum"),
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
        (("varchar", "float"), ("18", "53"), None, ()),
        (("float", "numeric"), ("53", "18"), "53", ()),
        (("float", "numeric"), ("05", "18"), None, ("data_length",)),
        (("date", "integer"), ("18", "53"), None, ("data_length", "data_type")),
        (("varchar", "varchar"), ("5", "8,2"), None, ("data_length",)),
        (("varchar", None), ("5", "6"), None, ("data_length",)),
        (("varchar", "varchar"), ("05", "6"), None, ("data_length",)),
    ],
)
def test_length_maximum_with_type_widening_and_canonical_integers(
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


@pytest.mark.parametrize(
    ("types", "expected", "conflict"),
    [
        (("float", "integer"), "decimal", False),
        (("integer", "text"), "text", False),
        (("date", "integer"), None, True),
        (("uniqueidentifier", "integer"), None, True),
        (("numerisk", "numeric"), "decimal", False),
        (("alfanumerisk", "Character"), "text", False),
    ],
)
def test_declared_type_widening(
    types: tuple[str, str], expected: str | None, conflict: bool
) -> None:
    records = (_record(1, data_type=types[0]), _record(2, data_type=types[1]))
    for ordered in (records, records[::-1]):
        result = resolve_occurrence_intervals(ordered)
        field = result.segments[0].fields.data_type
        assert field is not None
        assert field.value == expected
        assert field.raw_value == "Documented Datatyp: " + ", ".join(sorted(types))
        assert [issue.fields for issue in result.issues] == (
            [("data_type",)] if conflict else []
        )


@pytest.mark.parametrize(
    ("literal", "expected"),
    [("numerisk", "decimal"), ("alfanumerisk", "text"), ("Character", "text")],
)
def test_single_source_type_spelling_is_classified_without_changing_original(
    literal: str, expected: str
) -> None:
    original = _record(1, data_type=literal)
    result = resolve_occurrence_intervals((original,))
    field = result.segments[0].fields.data_type
    assert field is not None
    assert field.value == expected
    assert field.raw_value == f"Documented Datatyp: {literal}"
    assert original.fields.data_type == value_field(literal)
    assert not result.issues


@pytest.mark.parametrize(
    ("classes", "expected"),
    [
        (frozenset({"integer", "decimal"}), "decimal"),
        (frozenset({"integer", "text"}), "text"),
        (frozenset({"integer", None}), "text"),
        (frozenset(), "text"),
    ],
)
def test_numeric_storage_caps_only_all_numeric_waves(
    classes: frozenset[str | None], expected: str
) -> None:
    records = (_record(1, data_type="varchar"), _record(2, data_type="float"))
    storage = {
        "column": StewardColumnStorage(
            classes=classes,
            provenance="SWECOV storage csv: CIS2018=int, CIS2020=float",
        )
    }
    for ordered in (records, records[::-1]):
        result = resolve_occurrence_intervals(ordered, storage=storage)
        field = result.segments[0].fields.data_type
        assert field is not None and field.value == expected
        assert result.issues == ()
        assert "Documented Datatyp: float, varchar" in field.raw_value
        assert "CIS2018=int" in field.raw_value
    assert (
        resolve_occurrence_intervals(records).segments[0].fields.data_type.value
        == "text"
    )


@pytest.mark.parametrize("lengths", [("18", "18"), ("18", "53")])
def test_storage_cap_discards_length_within_one_documented_class(
    lengths: tuple[str, str],
) -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"data_length": value_field(length)}
                )
            }
        )
        for record, length in zip(
            (_record(1, data_type="varchar"), _record(2, data_type="text")),
            lengths,
        )
    )
    storage = {
        "column": StewardColumnStorage(
            classes=frozenset({"integer", "decimal"}),
            provenance="SWECOV storage csv: CIS2018=int, CIS2020=float",
        )
    }
    for ordered in (records, records[::-1]):
        result = resolve_occurrence_intervals(ordered, storage=storage)
        (segment,) = result.segments
        assert segment.fields.data_type is not None
        assert segment.fields.data_type.value == "decimal"
        assert segment.fields.data_length == SourceField(status="unknown")
        assert result.issues == ()


def test_single_declared_type_is_not_rewritten_by_storage() -> None:
    storage = {
        "column": StewardColumnStorage(
            classes=frozenset({"integer"}), provenance="SWECOV storage: CIS=int"
        )
    }
    field = (
        resolve_occurrence_intervals((_record(1, data_type="float"),), storage=storage)
        .segments[0]
        .fields.data_type
    )
    assert field == SourceField(status="value", value="float")


def test_data_type_widening_absorbs_source_attribution_disagreement() -> None:
    first = _record(1, source_attribution="Fråga 1")
    second = _record(2, data_type="text", source_attribution="Fråga 2")
    result = resolve_occurrence_intervals((first, second))
    assert len(result.segments) == 1
    segment = result.segments[0]
    assert segment.fields.data_type is not None
    assert segment.fields.data_type.value == "text"
    assert segment.fields.source_attribution == SourceField(status="unknown")
    assert result.issues == ()


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
