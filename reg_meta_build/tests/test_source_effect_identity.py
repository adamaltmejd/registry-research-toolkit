"""Checked corrections: field conditions, identity partitions and column ownership keep physical evidence."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from catalog_manifest import synthetic_manifest
from reg_meta_build.curation_compile import convert_column_partitions
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.source_coding import resolve_code_membership
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CheckedIdentityChange,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    SourceFields,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path

from _source_effects_support import (
    effect_case as _case,
    effect_field as _field,
    effect_record as _record,
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


def test_checked_identity_partition_keeps_physical_evidence_and_rejects_new_peers() -> (
    None
):
    record = _record(column="ANSWER_A")
    native = source_occurrence(record).variable_key
    assert native is not None
    partition = (*native, "accepted-partition", "answer-a")
    case = _case(
        record, CheckedIdentityChange(ref=record_ref(record), variable_key=partition)
    )
    case = CurationCase.model_validate_json(case.model_dump_json())
    result = apply_occurrence_cases((record, record), (case,))
    assert result.diagnostics == ()
    assert len(result.occurrences) == 2
    assert all(item.variable_key == partition for item in result.occurrences)
    assert all(item.source_records == (record,) for item in result.occurrences)
    assert source_occurrence(record).variable_key == native
    new_peer = _record(cvid=21, column="ANSWER_B")
    stale = apply_occurrence_cases((record, new_peer), (case,))
    assert stale.accounting[0].disposition == "stale"
    assert all(item.variable_key == native for item in stale.occurrences)


def test_conflicting_identity_assignments_withhold_only_identity() -> None:
    record = _record(column="ANSWER")
    first = _case(
        record,
        CheckedIdentityChange(ref=record_ref(record), variable_key=("accepted", "a")),
        name="first",
    )
    second = _case(
        record,
        CheckedIdentityChange(ref=record_ref(record), variable_key=("accepted", "b")),
        name="second",
    )
    result = apply_occurrence_cases((record,), (first, second))
    assert result.occurrences[0].variable_key is None
    assert result.occurrences[0].fields == record.fields
    assert result.occurrences[0].variant_key == source_occurrence(record).variant_key
    assert result.occurrences[0].withheld_fields == ("identity",)
    assert {item.disposition for item in result.accounting} == {"conflicted"}
    assert all(
        issue.withheld_output == ("occurrence.identity",)
        for issue in result.diagnostics
    )
    with pytest.raises(ValueError, match="guarded source membership"):
        apply_occurrence_cases(
            (record,), (first.model_copy(update={"peer_guards": ()}),)
        )


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


def test_existing_literal_ownership_converts_renames_without_changing_source_facts() -> (
    None
):
    records = (
        _record(column="ANSWER", year="2020"),
        _record(cvid=21, column="Answer", year="2021"),
        _record(cvid=22, column="OTHER", year="2021"),
    )
    converted = convert_column_partitions(
        records,
        source_id="1.5",
        split_ids=("1.5.answer", "1.5.other"),
        declared_columns={
            "ANSWER": "1.5.answer",
            "Answer": "1.5.answer",
            "OTHER": "1.5.other",
        },
        declaration_reference="accepted concept group, literal members 0-2",
    )
    assert converted.case is not None and not converted.diagnostics
    result = apply_occurrence_cases(records, (converted.case,))
    assert not result.diagnostics
    assert result.occurrences[0].variable_key == result.occurrences[1].variable_key
    assert result.occurrences[2].variable_key != result.occurrences[1].variable_key
    assert all(
        o.fields == r.fields for o, r in zip(result.occurrences, records, strict=True)
    )
    assert converted.case.decision.kind == "correct_occurrences"
    assert "literal members 0-2" in converted.case.decision.provenance
    added = _record(cvid=23, column="ANSWER_NEW", year="2022")
    assert (
        apply_occurrence_cases((*records, added), (converted.case,))
        .accounting[0]
        .disposition
        == "stale"
    )
    moved = (records[0], _record(cvid=21, column="Answer", year="2022"), records[2])
    assert (
        apply_occurrence_cases(moved, (converted.case,)).accounting[0].disposition
        == "stale"
    )


@pytest.mark.parametrize(
    "ownership",
    [
        {"ANSWER": "1.5.answer"},
        {"ANSWER": "1.5.answer", "Answer": "1.5.unknown"},
        {"ANSWER": "1.5.answer", "Answer": "1.5.answer", "ADDED": "1.5.answer"},
    ],
)
def test_declared_ownership_cannot_leave_or_invent_columns_or_split_keys(
    ownership: dict[str, str],
) -> None:
    with pytest.raises(ValueError, match="complete columns and split keys"):
        convert_column_partitions(
            (_record(column="ANSWER"), _record(cvid=21, column="Answer")),
            source_id="1.5",
            split_ids=("1.5.answer",),
            declared_columns=ownership,
            declaration_reference="accepted literal members",
        )


def test_declared_column_ownership_requires_original_declaration_reference() -> None:
    with pytest.raises(ValueError, match="declaration reference"):
        convert_column_partitions(
            (_record(column="ANSWER"),),
            source_id="1.5",
            split_ids=("1.5.answer",),
            declared_columns={"ANSWER": "1.5.answer"},
        )


def test_explicit_unassigned_columns_do_not_block_independent_reviewed_ownership() -> (
    None
):
    records = (
        _record(column="ANSWER"),
        _record(cvid=21, column="Answer"),
        _record(cvid=22, column="UNASSIGNED"),
    )
    converted = convert_column_partitions(
        records,
        source_id="1.5",
        split_ids=("1.5.answer",),
        declared_columns={
            "ANSWER": "1.5.answer",
            "Answer": "1.5.answer",
            "UNASSIGNED": None,
        },
        declaration_reference="Existing exact ownership of ANSWER and Answer",
    )
    assert converted.case is not None and len(converted.bindings) == 1
    assert [(d.code, d.severity, d.refs) for d in converted.diagnostics] == [
        ("unassigned_original_columns", "error", (record_ref(records[2]),))
    ]
    result = apply_occurrence_cases(records, (converted.case,))
    assert not result.diagnostics
    assert (
        result.occurrences[0].variable_key
        == result.occurrences[1].variable_key
        == converted.bindings[0].target.source_key
    )
    assert result.occurrences[2] == source_occurrence(records[2])
    assert all(
        o.fields == r.fields for o, r in zip(result.occurrences, records, strict=True)
    )
    # The unassigned part is evidence too; adding a member invalidates the case.
    added = _record(cvid=23, column="UNASSIGNED", year="2021")
    assert (
        apply_occurrence_cases((*records, added), (converted.case,))
        .accounting[0]
        .disposition
        == "stale"
    )


def test_explicit_identity_decision_keeps_exact_columns_and_periods(
    tmp_path: Path,
) -> None:
    records = (
        _record(column="ANSWER", year="2020"),
        _record(cvid=21, column="Answer", year="2021"),
    )
    expected = capture_expectations(records, fields=("column_name",))
    case = CurationCase(
        case_id="reviewed-column-identity",
        targets=expected,
        peer_guards=(
            PeerGuard(
                guard_id="complete-family",
                source=records[0].source,
                native=NativeCoordinates(register_id=1, variable_id=5),
                expected_members=tuple(item.ref for item in expected),
            ),
        ),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(
                CheckedIdentityChange(
                    ref=record_ref(record), variable_key=("reviewed",)
                )
                for record in records
            ),
            reason="Fixture explicitly establishes the same variable in these two editions.",
            provenance="Independent reviewed identity evidence in the fixture",
        ),
    )
    result = apply_occurrence_cases(records, (case,))
    assert all(record.identity_checked for record in result.occurrences)
    assert [record.fields for record in result.occurrences] == [
        record.fields for record in records
    ]
    variant = ResolvedVariant(slug="people", name="People")
    variable = form_native_variable(
        result.occurrences,
        register=ResolvedRegister(provider="scb", slug="fixture", name="Fixture"),
        variants={
            record.variant_key: variant
            for record in result.occurrences
            if record.variant_key is not None
        },
        slug="answer",
        provider_key="5.answer",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding={
            record.column_key: resolve_code_membership(())
            for record in result.occurrences
            if record.column_key is not None
        },
    )
    assert variable.variable is not None and variable.diagnostics == ()
    output = tmp_path / "catalog.db"
    write_resolved_catalog((variable.variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT delivery_column_name, valid_from, valid_to FROM variable_state ORDER BY valid_from"
            )
        ] == [
            ("ANSWER", "2020-01-01", "2020-12-31"),
            ("Answer", "2021-01-01", "2021-12-31"),
        ]
    changed = (records[0], _record(cvid=21, column="OTHER", year="2021"))
    stale = apply_occurrence_cases(changed, (case,))
    assert stale.accounting[0].disposition == "stale"
    assert not any(record.identity_checked for record in stale.occurrences)


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
