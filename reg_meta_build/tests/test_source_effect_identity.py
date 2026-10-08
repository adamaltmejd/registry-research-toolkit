"""Checked corrections: field conditions, identity partitions and column ownership keep physical evidence."""

from __future__ import annotations

import pytest
from _source_effects_support import (
    effect_case as _case,
    effect_field as _field,
    effect_record as _record,
)
from reg_meta_build.curation_compile import convert_column_partitions
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    value_field,
)


@pytest.mark.parametrize("conflicting", [False, True])
def test_field_conditions_select_original_alternative_without_touching_its_peer(
    conflicting: bool,
) -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(update={"name": value_field(label)})
            }
        )
        for record, label in (
            (_record(row=1, column="INVARN8"), "Date 8"),
            (_record(row=2, column="INVARN8"), "Date 9"),
        )
    )
    first = records[0]
    condition = FieldExpectation(name="name", status="value", value="Date 9")
    correction = CheckedFieldChange(
        ref=record_ref(first),
        replacement=FieldExpectation(
            name="column_name", status="value", value="INVARN9"
        ),
        when=(condition,),
    )
    case = _case(first, correction).model_copy(
        update={
            "targets": capture_expectations(records, fields=("name", "column_name"))
        }
    )
    # Conditions inspect source fields even when another case changes that field.
    rename = case.model_copy(
        update={
            "case_id": "change-label",
            "decision": case.decision.model_copy(
                update={"effects": (_field(first, "name", "Changed label"),)}
            ),
        }
    )
    cases = (case, rename)
    if conflicting:
        cases += (
            case.model_copy(
                update={
                    "case_id": "competing-column",
                    "decision": case.decision.model_copy(
                        update={
                            "effects": (
                                correction.model_copy(
                                    update={
                                        "replacement": FieldExpectation(
                                            name="column_name",
                                            status="value",
                                            value="OTHER",
                                        )
                                    }
                                ),
                            )
                        }
                    ),
                }
            ),
        )
    result = apply_occurrence_cases(records, cases)
    left, right = result.occurrences
    assert left.fields.column_name == first.fields.column_name
    assert left.fields.name is not None and right.fields.name is not None
    assert right.fields.column_name is not None
    assert left.fields.name.value == right.fields.name.value == "Changed label"
    assert right.fields.column_name.value == (None if conflicting else "INVARN9")
    assert right.withheld_fields == (("column_name",) if conflicting else ())
    assert left.withheld_fields == ()
    assert bool(result.diagnostics) == conflicting
    assert apply_occurrence_cases(records, cases[::-1]) == result


def test_existing_column_discriminators_convert_into_exact_guarded_partitions() -> None:
    first = _record(column="ANSWER_A")
    # The full source contains distinct column assertions under the same CVID.
    second = _record(row=2, column="ANSWER_B")
    blank = _record(cvid=22, column="")
    converted = convert_column_partitions(
        (first, second, blank),
        source_id="1.5",
        split_ids=("1.5.answer-a", "1.5.answer-b"),
    )
    assert converted.diagnostics == () and converted.case is not None
    assert len(converted.bindings) == 2
    assert {item.ref for item in converted.case.targets} == {
        record_ref(first),
        record_ref(second),
    }
    assert tuple(item.ref for item in converted.case.support) == (record_ref(blank),)
    result = apply_occurrence_cases((first, second, blank), (converted.case,))
    expected_keys = {
        binding.source_id: binding.target.source_key for binding in converted.bindings
    }
    assert result.occurrences[0].variable_key == expected_keys["1.5.answer-a"]
    assert result.occurrences[1].variable_key == expected_keys["1.5.answer-b"]
    assert result.occurrences[2].variable_key == source_occurrence(blank).variable_key
    rewrite = converted.case.model_copy(
        update={
            "case_id": "separate-column-correction",
            "decision": OccurrenceCorrectionDecision(
                reviewed=True,
                effects=(_field(first, "column_name", "ANSWER_B"),),
                reason="Independent checked field correction",
                provenance="fixture",
            ),
        }
    )
    combined = apply_occurrence_cases((first, second, blank), (converted.case, rewrite))
    assert combined.diagnostics == ()
    assert combined.occurrences[0].fields.column_name is not None
    assert combined.occurrences[0].fields.column_name.value == "ANSWER_B"
    assert combined.occurrences[0].variable_key == expected_keys["1.5.answer-a"]
    assert combined.occurrences[1].variable_key == expected_keys["1.5.answer-b"]
    changed = _record(cvid=23, column="ANSWER_C")
    assert (
        apply_occurrence_cases((first, second, blank, changed), (converted.case,))
        .accounting[0]
        .disposition
        == "stale"
    )


def test_unique_suffix_binds_while_unmatched_siblings_stay_unresolved() -> None:
    record = _record(column="ANSWER")
    converted = convert_column_partitions(
        (record,), source_id="1.5", split_ids=("1.5.answer", "1.5.answer-1")
    )
    assert converted.case is not None
    assert tuple(item.source_id for item in converted.bindings) == ("1.5.answer",)
    assert converted.diagnostics[0].code == "split_identity_conversion_pending"
    assert "answer-1" in converted.diagnostics[0].detail
    renamed = _record(cvid=21, column="ANSWER_NEW")
    converted = convert_column_partitions(
        (record, renamed), source_id="1.5", split_ids=("1.5.answer",)
    )
    assert converted.case is not None
    assert tuple(item.source_id for item in converted.bindings) == ("1.5.answer",)
    assert tuple(item.ref for item in converted.case.support) == (record_ref(renamed),)


@pytest.mark.parametrize("twin_year", ("2020", "2021"))
def test_ambiguous_split_withholds_only_its_own_partition(twin_year: str) -> None:
    records = (
        _record(column="ANSWER", year="2020"),
        _record(cvid=21, column="Answer", year=twin_year),
        _record(cvid=22, column="OTHER", year="2021"),
    )
    converted = convert_column_partitions(
        records, source_id="1.5", split_ids=("1.5.answer", "1.5.other")
    )
    assert converted.case is not None
    assert tuple(b.source_id for b in converted.bindings) == (
        ("1.5.other",) if twin_year == "2020" else ("1.5.answer", "1.5.other")
    )
    assert tuple(d.withheld_output for d in converted.diagnostics) == (
        (("1.5.answer",),) if twin_year == "2020" else ()
    )
    result = apply_occurrence_cases(records, (converted.case,))
    assert not result.diagnostics
    answer_key = (
        source_occurrence(records[0]).variable_key
        if twin_year == "2020"
        else converted.bindings[0].target.source_key
    )
    assert result.occurrences[0].variable_key == answer_key
    assert result.occurrences[1].variable_key == answer_key
    assert (
        result.occurrences[2].variable_key == converted.bindings[-1].target.source_key
    )
    assert all(
        o.fields == r.fields for o, r in zip(result.occurrences, records, strict=True)
    )
    # Every original peer is still guarded, including the ambiguous partition.
    added = _record(cvid=23, column="Other", year="2022")
    assert (
        apply_occurrence_cases((*records, added), (converted.case,))
        .accounting[0]
        .disposition
        == "stale"
    )


@pytest.mark.parametrize(
    ("first", "second", "year", "suffix"),
    [
        ("ANSWER", "Answer", "2020", "answer"),
        ("ANSWER", "Answer", "2021", "answer"),
        ("Kön", "Kon", "2021", "kon"),
        ("ANSWER_A", "ANSWER-A", "2021", "answer-a"),
    ],
)
def test_naming_pin_does_not_establish_identity_for_distinct_column_spellings(
    first: str, second: str, year: str, suffix: str
) -> None:
    converted = convert_column_partitions(
        (_record(column=first), _record(cvid=21, column=second, year=year)),
        source_id="1.5",
        split_ids=(f"1.5.{suffix}",),
    )
    binds = (first, year) in {("ANSWER", "2021"), ("Kön", "2021")}
    assert (converted.case is not None) == binds
    assert tuple(d.code for d in converted.diagnostics) == (
        () if binds else ("split_identity_conversion_pending",)
    )
    assert tuple(b.source_id for b in converted.bindings) == (
        (f"1.5.{suffix}",) if binds else ()
    )
