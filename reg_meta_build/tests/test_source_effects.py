"""Checked corrections compose without turning curated output into source evidence."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta.db import open_db
from reg_meta_build.catalog_resolution import resolve_parents
from reg_meta_build.convert_errata import (
    ErrataEditionBinding,
    capture_expectations,
    convert_column_entry,
    convert_delivered_entry,
)
from reg_meta_build.convert_identity import convert_column_partitions
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.scb_errata import ErrataColumn, ErrataDelivered
from reg_meta_build.source_annotations import apply_alias_cases
from reg_meta_build.source_coding import resolve_code_membership
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CheckedIdentityChange,
    CheckedPeriodChange,
    CheckedSourceUse,
    CheckedVariantAssignment,
    CuratedOccurrenceAddition,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    RecordExpectation,
    RecordProjection,
    SearchAliasDecision,
)
from reg_meta_build.source_effects import apply_occurrence_cases, record_ref
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_intervals import resolve_occurrence_intervals
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceField,
    SourceFields,
    SourceRecord,
    SourceRevision,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_curation import OccurrenceEffect


_REVISION = SourceRevision.create(
    dataset="scb-fixture",
    publisher="SCB",
    purpose="checked occurrence fixture",
    upstream_revision="1",
    artifact_path="Registerinformation.csv",
    artifact_size=1,
    artifact_sha256="b" * 64,
)


def _scope(year: str) -> TemporalScope:
    return TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start=year, end=year),)
    )


def _record(
    *,
    row: int = 1,
    cvid: int = 20,
    column: str = "",
    year: str = "2020",
    variable: int = 5,
    data_type: str = "int",
) -> SourceRecord:
    values = _var_row(
        cvid=cvid,
        var_id=variable,
        colname=column,
        register=("TESTREG", 1, 2),
        regver_id=int(year),
        year=year,
        data_type=data_type,
    ).split("|")
    header = REGISTERINFORMATION_HEADER.split("|")
    cells: dict[str, tuple[bool, str | None, str]] = {
        name: (True, value, value) for name, value in zip(header, values, strict=True)
    }
    return clean_scb_row(header, row, cells, _REVISION).record


def _expect(record: SourceRecord) -> RecordExpectation:
    return RecordExpectation(
        ref=record_ref(record),
        alternatives=(
            RecordProjection(
                fields=tuple(
                    FieldExpectation(name=name, status="absent")
                    if field is None
                    else FieldExpectation(
                        name=name, status=field.status, value=field.value
                    )
                    for name in SourceFields.model_fields
                    for field in (getattr(record.fields, name),)
                ),
                subject=record.subject,
                edition_scope=record.edition_scope,
                edition_period_scope=record.edition_period_scope,
            ),
        ),
    )


def _case(
    record: SourceRecord,
    *effects: OccurrenceEffect,
    name: str = "accepted",
    decision: SearchAliasDecision | None = None,
) -> CurationCase:
    return CurationCase(
        case_id=name,
        targets=(_expect(record),),
        peer_guards=(
            PeerGuard(
                guard_id=f"{name}-family",
                source=record.source,
                native=NativeCoordinates(variable_id=record.subject.native.variable_id),
                expected_members=(record_ref(record),),
            ),
        ),
        decision=decision
        or OccurrenceCorrectionDecision(
            reviewed=True,
            effects=effects,
            reason="Existing accepted delivery correction",
            provenance=f"errata:fixture\n{name}",
        ),
    )


def _search_alias_fixture():
    record = _record(column="VALUE")
    occurrence = source_occurrence(record)
    assert occurrence.variable_key is not None and occurrence.variant_key is not None
    assert occurrence.column_key is not None
    variant = ResolvedVariant(slug="people", name="People")
    result = form_native_variable(
        (record,),
        register=ResolvedRegister(provider="scb", slug="fixture", name="Fixture"),
        variants={occurrence.variant_key: variant},
        slug="value",
        provider_key="5",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding={occurrence.column_key: resolve_code_membership(())},
    )
    assert result.variable is not None
    case = _case(
        record,
        decision=SearchAliasDecision(
            reviewed=True,
            variable_key=occurrence.variable_key,
            variant_keys=(occurrence.variant_key,),
            column="ALTERNATIVE",
            reason="Existing delivery-list search alias",
            provenance="fixture",
        ),
    )
    return (
        record,
        case,
        result.variable,
        occurrence.variable_key,
        {occurrence.variant_key: variant},
    )


def test_search_alias_is_guarded_metadata_without_added_availability(
    tmp_path: Path,
) -> None:
    record, case, variable, key, variants = _search_alias_fixture()
    result = apply_alias_cases(
        (record,), (case,), variables={key: variable}, variants=variants
    )
    assert result.diagnostics == ()
    updated = result.variables[key]
    assert updated is not None and updated.states == variable.states
    assert [
        (alias.delivery_column_name, alias.windows) for alias in updated.aliases
    ] == [("ALTERNATIVE", ())]
    output = tmp_path / "aliases.db"
    write_resolved_catalog((updated,), output, manifest={})
    with closing(open_db(output)) as conn:
        assert (
            conn.execute(
                "SELECT delivery_column_name FROM variable_alias WHERE delivery_column_name='ALTERNATIVE'"
            ).fetchone()[0]
            == "ALTERNATIVE"
        )
        assert (
            conn.execute("SELECT count(*) FROM variable_alias_window").fetchone()[0]
            == 0
        )
        assert conn.execute("SELECT count(*) FROM variable_state").fetchone()[0] == 1
    same = apply_alias_cases(
        (_record(column=" VALUE "),),
        (case,),
        variables={key: variable},
        variants=variants,
    )
    assert same.diagnostics == () and same.variables == result.variables
    stale = apply_alias_cases(
        (record, _record(cvid=21, column="VALUE", year="2021")),
        (case,),
        variables={key: variable},
        variants=variants,
    )
    assert stale.variables[key] == variable
    assert stale.evaluations[0].status == "stale"


def test_search_alias_preserves_existing_precise_windows() -> None:
    record, case, variable, key, variants = _search_alias_fixture()
    alias = ResolvedAlias(
        variant=next(iter(variants.values())),
        delivery_column_name="ALTERNATIVE",
        windows=(ResolvedAliasWindow(valid_from="2020-03-01", valid_to="2020-04-30"),),
    )
    variable = variable.model_copy(update={"aliases": (alias,)})
    result = apply_alias_cases(
        (record,), (case,), variables={key: variable}, variants=variants
    )
    assert result.diagnostics == () and result.variables[key] == variable


def test_search_alias_distinguishes_withheld_dependencies_from_missing_conversion() -> (
    None
):
    record, case, variable, key, variants = _search_alias_fixture()
    withheld = apply_alias_cases(
        (record,), (case,), variables={key: None}, variants=variants
    )
    assert withheld.variables[key] is None
    assert [issue.code for issue in withheld.diagnostics] == [
        "withheld_alias_dependency"
    ]
    assert withheld.evaluations[0].status == "applicable"
    for variables, parents in (({}, variants), ({key: variable}, {})):
        with pytest.raises(ValueError, match="unconverted"):
            apply_alias_cases((record,), (case,), variables=variables, variants=parents)


def _field(record: SourceRecord, name: str, value: str) -> CheckedFieldChange:
    return CheckedFieldChange(
        ref=record_ref(record),
        replacement=FieldExpectation(name=name, status="value", value=value),
    )


def _addition(
    record: SourceRecord, *, key: str = "delivery-2019"
) -> CuratedOccurrenceAddition:
    original = source_occurrence(record)
    assert original.variable_key and original.variant_key
    return CuratedOccurrenceAddition(
        occurrence_key=key,
        provider="scb",
        variable_key=original.variable_key,
        variant_key=original.variant_key,
        edition_key=("curated-edition", "2019"),
        fields=record.fields,
        edition_scope=_scope("2019"),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        evidence=(record_ref(record),),
        donor=record_ref(record),
        copied_fields=tuple(
            name
            for name in SourceFields.model_fields
            if getattr(record.fields, name) is not None
        ),
    )


def test_authored_coverage_does_not_require_a_fabricated_delivery_edition() -> None:
    record = _record(column="VALUE")
    addition = CuratedOccurrenceAddition.model_validate_json(
        _addition(record).model_copy(update={"edition_key": None}).model_dump_json()
    )
    result = apply_occurrence_cases((record,), (_case(record, addition),))
    assert result.diagnostics == ()
    declared = next(
        occurrence for occurrence in result.occurrences if occurrence.occurrence_key
    )
    assert declared.edition_key is None
    assert declared.source_records == ()
    assert declared.support_records == (record,)


def test_added_occurrence_copies_coding_only_with_explicit_checked_declaration() -> (
    None
):
    record = _record(column="VALUE")
    addition = _addition(record)
    metadata_only = apply_occurrence_cases((record,), (_case(record, addition),))
    assert metadata_only.occurrences[-1].coding_records == ()

    copied = addition.model_copy(update={"copy_coding": True})
    case = _case(record, copied)
    with pytest.raises(ValueError, match="checked code-set references"):
        apply_occurrence_cases((record,), (case,))
    case = case.model_copy(
        update={
            "targets": capture_expectations(
                (record,), fields=tuple(SourceFields.model_fields), coding=True
            )
        }
    )
    result = apply_occurrence_cases((record,), (case,))
    assert result.diagnostics == ()
    assert result.occurrences[-1].coding_records == (record,)


def test_addition_cannot_omit_coverage_without_marking_it_unresolved() -> None:
    record = _record(column="VALUE")
    addition = _addition(record).model_copy(
        update={
            "edition_key": None,
            "edition_scope": TemporalScope(kind="not_applicable"),
            "edition_period_scope": TemporalScope(kind="not_applicable"),
        }
    )
    with pytest.raises(ValueError, match="explicitly unresolved coverage"):
        apply_occurrence_cases((record,), (_case(record, addition),))


def test_declared_pooled_edition_is_retained_without_inferred_annual_states() -> None:
    record = _record(column="VALUE")
    pooled = TemporalScope(kind="pooled", label="2014 - 2016")
    addition = _addition(record).model_copy(
        update={"edition_scope": pooled, "edition_period_scope": pooled}
    )
    result = apply_occurrence_cases((record,), (_case(record, addition),))
    assert result.diagnostics == ()
    declared = next(item for item in result.occurrences if item.occurrence_key)
    assert declared.edition_key == addition.edition_key
    assert declared.edition_scope == pooled
    assert declared.support_records == (record,)
    resolved = resolve_occurrence_intervals((declared,))
    assert resolved.segments == ()
    assert {issue.code for issue in resolved.issues} == {"unsupported_occurrence"}


@pytest.mark.parametrize("versions", [None, ("2020",)])
@pytest.mark.parametrize("classification", [None, "FIX"])
def test_column_declaration_preserves_only_supplied_flags_and_periods(
    versions, classification
) -> None:
    record = _record(column="OTHER")
    original = source_occurrence(record)
    assert original.edition_key is not None
    entry = ErrataColumn(
        register_id=1,
        register_variant_id=2,
        column="MISSING",
        name="Authored name",
        definition="Authored description",
        data_type=None,
        classification=classification,
        is_identifier=False,
        is_sensitive=True,
        versions=versions,
        source="steward",
        provenance="Existing accepted column declaration",
    )
    binding = ErrataEditionBinding(
        key=original.edition_key,
        name="2020",
        edition_scope=record.edition_scope,
        edition_period_scope=record.edition_period_scope,
        support=(record_ref(record),),
        native_id=2020,
    )
    converted = convert_column_entry(
        entry,
        case_id="column-1",
        records=(record,),
        editions=(binding,),
        declared_flags=frozenset({"is_sensitive"}),
    )
    assert converted.case is not None and converted.blockers == ()
    later = _record(column="UNRELATED", year="2021", cvid=21)
    result = apply_occurrence_cases((record, later), (converted.case,))
    assert result.diagnostics == ()
    originals = tuple(item for item in result.occurrences if item.source_records)
    assert tuple(item.source_records for item in originals) == ((record,), (later,))
    assert tuple(item.fields for item in originals) == (record.fields, later.fields)
    assert not any(item.corrections for item in originals)
    additions = tuple(item for item in result.occurrences if item.occurrence_key)
    assert len(additions) == 1
    addition = additions[0]
    if versions is None:
        assert addition.edition_key is None
        assert addition.edition_scope.kind == "unknown"
        intervals = resolve_occurrence_intervals((addition,))
        assert intervals.segments == ()
        assert {issue.code for issue in intervals.issues} == {"unsupported_occurrence"}
    else:
        assert addition.edition_key == binding.key
        assert addition.edition_scope == record.edition_scope
    assert addition.fields.name == value_field("Authored name")
    assert addition.fields.description == value_field("Authored description")
    assert addition.fields.data_type is None
    assert addition.fields.classification_declared == (
        value_field(classification) if classification is not None else None
    )
    assert addition.fields.identifier is None
    assert addition.fields.sensitivity == value_field(True)
    assert addition.source_records == () and addition.support_records == (record,)
    documented = _record(column="missing", year="2021", cvid=21)
    stale = apply_occurrence_cases((record, documented), (converted.case,))
    assert stale.accounting[0].disposition == "stale"
    assert not any(item.occurrence_key for item in stale.occurrences)
    blocked = convert_column_entry(
        entry,
        case_id="column-1",
        records=(record, documented),
        editions=(binding,),
        declared_flags=frozenset({"is_sensitive"}),
    )
    assert blocked.case is None and blocked.blockers == ("column_now_documented",)


def test_checked_variant_routing_retains_unknown_scope_and_physical_evidence() -> None:
    record = _record(column="VALUE").model_copy(
        update={
            "parent_facts": (),
            "edition_scope": TemporalScope(kind="unknown", label="not supplied"),
        }
    )
    case = _case(
        record,
        CheckedVariantAssignment(
            ref=record_ref(record), variant_keys=(("second",), ("first",))
        ),
    )
    result = apply_occurrence_cases((record,), (case,))
    assert result.diagnostics == ()
    assert result.accounting[0].disposition == "applied"
    assert [item.variant_key for item in result.occurrences] == [
        ("first",),
        ("second",),
    ]
    for item in result.occurrences:
        assert item.source_records == (record,)
        assert item.fields == record.fields
        assert item.edition_scope == record.edition_scope
        assert item.edition_key is None
        assert item.corrections[0].case_id == case.case_id
    changed = record.model_copy(
        update={"edition_scope": TemporalScope(kind="not_applicable")}
    )
    stale = apply_occurrence_cases((changed,), (case,))
    assert stale.accounting[0].disposition == "stale"
    assert len(stale.occurrences) == 1
    assert stale.occurrences[0].variant_key == source_occurrence(changed).variant_key


def test_variant_routing_disagreements_withhold_the_route_not_source_fields() -> None:
    record = _record(column="VALUE").model_copy(update={"parent_facts": ()})
    cases = tuple(
        _case(
            record,
            CheckedVariantAssignment(ref=record_ref(record), variant_keys=keys),
            name=name,
        )
        for name, keys in (("one", (("first",),)), ("two", (("second",),)))
    )
    result = apply_occurrence_cases((record,), cases)
    assert all(item.disposition == "conflicted" for item in result.accounting)
    assert {item.code for item in result.diagnostics} == {
        "conflicting_curation_effects"
    }
    assert len(result.occurrences) == 1
    assert result.occurrences[0].variant_key is None
    assert result.occurrences[0].fields == record.fields
    assert result.occurrences[0].withheld_fields == ("variant",)
    assert apply_occurrence_cases((record,), cases[::-1]) == result


def test_variant_routing_cannot_move_a_native_edition_implicitly() -> None:
    record = _record(column="VALUE")
    case = _case(
        record,
        CheckedVariantAssignment(ref=record_ref(record), variant_keys=(("other",),)),
    )
    with pytest.raises(ValueError, match="cannot implicitly reparent"):
        apply_occurrence_cases((record,), (case,))


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


def test_existing_shape_split_or_rename_cluster_is_not_guessed_from_discriminator() -> (
    None
):
    record = _record(column="ANSWER")
    converted = convert_column_partitions(
        (record,), source_id="1.5", split_ids=("1.5.answer", "1.5.answer-1")
    )
    assert converted.case is None and converted.bindings == ()
    assert converted.diagnostics[0].code == "split_identity_conversion_pending"
    assert "answer-1" in converted.diagnostics[0].detail
    renamed = _record(cvid=21, column="ANSWER_NEW")
    converted = convert_column_partitions(
        (record, renamed), source_id="1.5", split_ids=("1.5.answer",)
    )
    assert converted.case is None


def test_ambiguous_split_withholds_only_its_own_partition() -> None:
    records = (
        _record(column="ANSWER", year="2020"),
        _record(cvid=21, column="Answer", year="2021"),
        _record(cvid=22, column="OTHER", year="2021"),
    )
    converted = convert_column_partitions(
        records, source_id="1.5", split_ids=("1.5.answer", "1.5.other")
    )
    assert converted.case is not None
    assert tuple(b.source_id for b in converted.bindings) == ("1.5.other",)
    assert converted.diagnostics[0].withheld_output == ("1.5.answer",)
    result = apply_occurrence_cases(records, (converted.case,))
    assert not result.diagnostics
    assert (
        result.occurrences[0].variable_key == source_occurrence(records[0]).variable_key
    )
    assert (
        result.occurrences[1].variable_key == source_occurrence(records[1]).variable_key
    )
    assert result.occurrences[2].variable_key == converted.bindings[0].target.source_key
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
    write_resolved_catalog((variable.variable,), output, manifest={})
    with closing(open_db(output)) as conn:
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
    assert converted.case is None
    assert converted.diagnostics[0].code == "split_identity_conversion_pending"


def test_checked_lookup_role_keeps_evidence_and_does_not_materialize_parents() -> None:
    lookup = _record(column="CODE")
    other = _record(cvid=21, column="DATA", variable=6)
    case = _case(lookup, CheckedSourceUse(ref=record_ref(lookup)))
    result = apply_occurrence_cases((lookup, lookup, other, other), (case,))
    assert [item.use for item in result.occurrences] == [
        "support",
        "support",
        "catalog",
        "catalog",
    ]
    assert len(result.occurrences) == 4 and result.diagnostics == ()
    assert result.occurrences[0].source_records == (lookup,)
    parents = resolve_parents(result.occurrences[:2], ())
    assert parents.registers == parents.variants == parents.editions == {}
    assert parents.support_only_refs == (record_ref(lookup),)
    with pytest.raises(ValueError, match="support-only"):
        form_native_variable(
            result.occurrences[:2],
            register=ResolvedRegister(provider="scb", slug="fixture", name="Fixture"),
            variants={},
            slug="code",
            provider_key="5",
            flags=SourceFields(),
            coding={},
        )


def test_disjoint_and_equal_effects_compose_against_original_evidence() -> None:
    record = _record()
    column = _field(record, "column_name", "VALUE")
    cases = (
        _case(record, column, name="name-column"),
        _case(
            record, column, _field(record, "description", "Corrected"), name="describe"
        ),
    )
    result = apply_occurrence_cases((record, record), cases)
    assert result.diagnostics == ()
    assert all(item.disposition == "applied" for item in result.accounting)
    assert len(result.occurrences) == 2
    for occurrence in result.occurrences:
        assert occurrence.source_records == (record,)
        assert occurrence.fields.column_name == SourceField(
            status="value", value="VALUE"
        )
        assert occurrence.fields.description == SourceField(
            status="value", value="Corrected"
        )
        assert len(occurrence.corrections) == 3
    assert record.fields.column_name is not None
    assert record.fields.column_name.status == "unknown"
    assert result == apply_occurrence_cases((record, record), tuple(reversed(cases)))


def test_field_conflict_withholds_only_that_field_and_retains_all_claims() -> None:
    record = _record(column="VALUE")
    cases = (
        _case(record, _field(record, "data_type", "text"), name="text"),
        _case(record, _field(record, "data_type", "integer"), name="integer"),
    )
    result = apply_occurrence_cases((record,), cases)
    occurrence = result.occurrences[0]
    assert occurrence.fields.data_type == SourceField(status="unknown")
    assert occurrence.withheld_fields == ("data_type",)
    assert {d.case_id for d in result.diagnostics} == {"text", "integer"}
    assert all(d.fields == ("data_type",) for d in result.diagnostics)
    # Another source row cannot turn a disputed correction into an apparent consensus.
    intervals = resolve_occurrence_intervals((occurrence, record))
    assert len(intervals.segments) == 1
    assert intervals.segments[0].fields.data_type == SourceField(status="unknown")
    assert intervals.segments[0].delivery_column_name == "VALUE"


def test_later_effect_cannot_use_an_earlier_effect_as_source_evidence() -> None:
    record = _record()
    first = _case(record, _field(record, "column_name", "VALUE"), name="first")
    rewritten = _record(column="VALUE")
    second = _case(
        rewritten, _field(rewritten, "description", "Changed"), name="second"
    )
    result = apply_occurrence_cases((record,), (first, second))
    assert [item.disposition for item in result.accounting] == ["applied", "stale"]
    assert result.occurrences[0].fields.description == record.fields.description
    assert result.diagnostics[0].code == "target_projection_changed"


def test_period_effect_is_exact_and_conflicts_do_not_restore_old_dates() -> None:
    record = _record(column="VALUE")
    first = CheckedPeriodChange(
        ref=record_ref(record),
        edition_scope=_scope("2019"),
        edition_period_scope=TemporalScope(kind="not_applicable"),
    )
    result = apply_occurrence_cases((record,), (_case(record, first),))
    assert [
        (s.valid_from, s.valid_to)
        for s in resolve_occurrence_intervals(result.occurrences).segments
    ] == [("2019-01-01", "2019-12-31")]
    other = first.model_copy(update={"edition_scope": _scope("2021")})
    conflicted = apply_occurrence_cases(
        (record,), (_case(record, first), _case(record, other, name="other"))
    )
    assert resolve_occurrence_intervals(conflicted.occurrences).segments == ()
    assert all(d.withheld_output == ("occurrence",) for d in conflicted.diagnostics)


def test_addition_is_a_bounded_claim_without_fabricated_source_row() -> None:
    record = _record(column="VALUE")
    case = _case(record, _addition(record))
    result = apply_occurrence_cases((record,), (case,))
    added = result.occurrences[1]
    assert added.source_records == ()
    assert added.support_records == (record,)
    assert added.occurrence_key == "delivery-2019"
    assert added.variable_key == result.occurrences[0].variable_key
    assert [
        (s.valid_from, s.valid_to)
        for s in resolve_occurrence_intervals(result.occurrences).segments
    ] == [("2019-01-01", "2019-12-31"), ("2020-01-01", "2020-12-31")]
    # Reappearing evidence invalidates the finite membership pin.
    changed = apply_occurrence_cases(
        (record, _record(cvid=21, year="2019", column="VALUE")), (case,)
    )
    assert changed.accounting[0].disposition == "stale"
    assert all(item.occurrence_key is None for item in changed.occurrences)
    assert "peer_membership_changed" in {d.code for d in changed.diagnostics}


def test_conflicting_additions_withhold_the_declared_occurrence() -> None:
    record = _record(column="VALUE")
    addition = _addition(record)
    other = addition.model_copy(update={"edition_scope": _scope("2018")})
    result = apply_occurrence_cases(
        (record,), (_case(record, addition), _case(record, other, name="other"))
    )
    assert len(result.occurrences) == 1
    assert all(item.disposition == "conflicted" for item in result.accounting)


def test_contract_rejects_unchecked_fields_and_misrepresented_donor() -> None:
    record = _record(column="VALUE")
    case = _case(record, _field(record, "column_name", "OTHER"))
    projection = case.targets[0].alternatives[0].model_copy(update={"fields": ()})
    target = case.targets[0].model_copy(update={"alternatives": (projection,)})
    with pytest.raises(ValueError, match="every changed/copied field"):
        apply_occurrence_cases(
            (record,), (case.model_copy(update={"targets": (target,)}),)
        )
    addition = _addition(record).model_copy(
        update={"fields": SourceFields(column_name=value_field("OTHER"))}
    )
    with pytest.raises(ValueError, match="every checked donor alternative"):
        apply_occurrence_cases((record,), (_case(record, addition),))


def test_layout_changes_preserve_applicability_but_relevant_changes_do_not() -> None:
    record = _record(column="VALUE")
    case = _case(record, _field(record, "description", "Accepted description"))
    assert (
        apply_occurrence_cases((_record(row=99, column="VALUE"),), (case,)).diagnostics
        == ()
    )
    changed = _record(column="RENAMED")
    assert (
        apply_occurrence_cases((changed,), (case,)).accounting[0].disposition == "stale"
    )


def test_corrected_facts_and_provenance_reach_direct_materialization(
    tmp_path: Path,
) -> None:
    record = _record()
    result = apply_occurrence_cases(
        (record,), (_case(record, _field(record, "column_name", "VALUE")),)
    )
    occurrence = result.occurrences[0]
    assert occurrence.variant_key and occurrence.column_key
    formed = form_native_variable(
        result.occurrences,
        register=ResolvedRegister(provider="scb", slug="fixture", name="Fixture"),
        variants={
            occurrence.variant_key: ResolvedVariant(slug="people", name="People")
        },
        slug="value",
        provider_key="5",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding={occurrence.column_key: resolve_code_membership(())},
    )
    assert formed.variable is not None
    assert formed.diagnostics == ()
    output = tmp_path / "catalog.db"
    write_resolved_catalog(
        (formed.variable,), output, manifest={"fixture": "checked-effects"}
    )
    with closing(open_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT delivery_column_name, valid_from, valid_to, provenance FROM variable_state"
            ).fetchone()
        ) == ("VALUE", "2020-01-01", "2020-12-31", "errata:fixture\naccepted")
    assert replace(occurrence, corrections=()).source_records[0] is record


def test_cases_roundtrip_through_the_shared_checked_contract() -> None:
    record = _record(column="VALUE")
    case = _case(record, _field(record, "description", "Known"), _addition(record))
    assert CurationCase.model_validate_json(case.model_dump_json()) == case


def _convert_delivered(records: tuple[SourceRecord, ...], target_edition: SourceRecord):
    assert target_edition.original_period_text is not None
    target = source_occurrence(target_edition)
    assert target.edition_key is not None
    return convert_delivered_entry(
        ErrataDelivered(
            register_id=1,
            register_variant_id=2,
            column="VALUE",
            versions=(target_edition.original_period_text,),
            provenance="errata:accepted\nExisting evidence",
        ),
        case_id="accepted/delivered/0",
        records=records,
        editions=(
            ErrataEditionBinding(
                key=target.edition_key,
                name=target_edition.original_period_text,
                edition_scope=target.edition_scope,
                edition_period_scope=target.edition_period_scope,
                support=(record_ref(target_edition),),
                native_id=target_edition.subject.native.edition_id,
            ),
        ),
    )


def test_converted_blank_target_changes_only_column_and_stays_source_guarded() -> None:
    donor = _record(column="Value", year="2022")
    blank = _record(cvid=21, year="2020", data_type="varchar")
    result = _convert_delivered((donor, blank), blank)
    assert result.case is not None and result.blockers == ()
    applied = apply_occurrence_cases((donor, blank), (result.case,))
    assert applied.diagnostics == ()
    assert applied.occurrences[1].fields.column_name == SourceField(
        status="value", value="VALUE"
    )
    assert applied.occurrences[1].fields.data_type == blank.fields.data_type
    assert applied.occurrences[1].source_records == (blank,)
    assert all(item.occurrence_key is None for item in applied.occurrences)
    # Unrelated variables and unused adjacent-edition metadata are not dependencies.
    unrelated = _record(variable=99, cvid=30, column="UNRELATED")
    assert (
        apply_occurrence_cases((donor, blank, unrelated), (result.case,)).diagnostics
        == ()
    )
    changed_donor = _record(column="Value", year="2022", data_type="varchar")
    assert (
        apply_occurrence_cases((changed_donor, blank), (result.case,))
        .accounting[0]
        .disposition
        == "applied"
    )


def test_delivery_statement_does_not_inherit_nearest_editions_metadata() -> None:
    before = _record(cvid=10, column="VALUE", year="2018", data_type="varchar")
    after = _record(cvid=20, column="VALUE", year="2022")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    records = before, after, edition
    result = _convert_delivered(records, edition)
    assert result.case is not None and result.blockers == ()
    assert set(result.identity_refs) == {record_ref(before), record_ref(after)}
    applied = apply_occurrence_cases(records, (result.case,))
    assert applied.diagnostics == ()
    added = applied.occurrences[-1]
    assert added.source_records == ()
    assert added.fields == SourceFields(
        availability=value_field(True), column_name=value_field("VALUE")
    )
    addition = result.case.decision.effects[0]
    assert isinstance(addition, CuratedOccurrenceAddition)
    assert addition.donor is None and addition.copied_fields == ()
    assert added.edition_scope == _scope("2020")
    # New source members require review; they never become automatic donors.
    nearer = _record(cvid=40, column="vAlUe", year="2021")
    changed = apply_occurrence_cases((*records, nearer), (result.case,))
    assert changed.accounting[0].disposition == "stale"
    assert all(item.occurrence_key is None for item in changed.occurrences)


def test_delivery_statement_cannot_choose_between_reused_column_identities() -> None:
    before = _record(cvid=10, column="VALUE", year="2018")
    after = _record(cvid=20, variable=6, column="VALUE", year="2022")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    result = _convert_delivered((before, after, edition), edition)
    assert result.case is None
    assert result.blockers == ("ambiguous_documented_column_identity",)
    assert set(result.identity_refs) == {record_ref(before), record_ref(after)}


@pytest.mark.parametrize(
    "column, extra, blocker",
    [
        ("VALUE", False, "now_present"),
        ("OTHER", False, "target_under_other_column"),
        ("", True, "ambiguous_target"),
    ],
)
def test_conversion_reports_original_evidence_conflicts(
    column: str, extra: bool, blocker: str
) -> None:
    donor = _record(column="VALUE", year="2022")
    target = _record(cvid=21, column=column)
    records = (donor, target, _record(cvid=22)) if extra else (donor, target)
    result = _convert_delivered(records, target)
    assert result.case is None
    assert any(item.startswith(blocker + ":") for item in result.blockers)


def _window_case(
    case, key, variant_key, *, start="2020-03-01", end="2020-04-30", name="window"
):
    from reg_meta_build.source_curation import AliasWindowDecision

    return case.model_copy(
        update={
            "case_id": name,
            "decision": AliasWindowDecision(
                reviewed=True,
                variable_key=key,
                variant_key=variant_key,
                column="ALTERNATIVE",
                valid_from=start,
                valid_to=end,
                reason="Existing accepted omitted representation",
                provenance="accepted alias_windows entry",
            ),
        }
    )


def test_alias_window_needs_owned_alias_and_does_not_expand_states(
    tmp_path: Path,
) -> None:
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    case = _window_case(search, key, variant_key)
    result = apply_alias_cases(
        (record,), (case,), variables={key: owned}, variants=variants
    )
    assert result.diagnostics == ()
    updated = result.variables[key]
    assert updated is not None and updated.states == variable.states
    assert [(w.valid_from, w.valid_to) for w in updated.aliases[0].windows] == [
        ("2020-03-01", "2020-04-30"),
    ]
    assert updated.aliases[0].windows[0].provenance is not None
    output = tmp_path / "window.db"
    write_resolved_catalog((updated,), output, manifest={})
    with closing(open_db(output)) as conn:
        assert conn.execute("SELECT count(*) FROM variable_state").fetchone()[0] == 1
        assert tuple(
            conn.execute(
                "SELECT delivery_column_name, valid_from, valid_to FROM variable_alias_window"
            ).fetchone()
        ) == ("ALTERNATIVE", "2020-03-01", "2020-04-30")
    unowned = apply_alias_cases(
        (record,), (case,), variables={key: variable}, variants=variants
    )
    assert unowned.variables[key] == variable
    assert [d.code for d in unowned.diagnostics] == ["unowned_alias_window"]
    future = _window_case(
        search, key, variant_key, start="2021-01-01", end="2021-12-31"
    )
    outside = apply_alias_cases(
        (record,), (future,), variables={key: owned}, variants=variants
    )
    assert outside.variables[key] == owned
    assert [d.code for d in outside.diagnostics] == ["unsupported_alias_window"]


def test_alias_window_checks_original_ownership_and_complete_period_coverage() -> None:
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    window = _window_case(search, key, variant_key)
    result = apply_alias_cases(
        (record,), (search, window), variables={key: variable}, variants=variants
    )
    assert result == apply_alias_cases(
        (record,), (window, search), variables={key: variable}, variants=variants
    )
    assert [d.code for d in result.diagnostics] == ["unowned_alias_window"]
    annotated = result.variables[key]
    assert annotated is not None
    assert annotated.aliases[0].windows == ()
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    split = owned.model_copy(
        update={
            "states": (
                owned.states[0].model_copy(update={"valid_to": "2020-03-31"}),
                owned.states[0].model_copy(update={"valid_from": "2020-04-01"}),
            )
        }
    )
    assert (
        apply_alias_cases(
            (record,), (window,), variables={key: split}, variants=variants
        ).diagnostics
        == ()
    )
    gap = split.model_copy(
        update={
            "states": (
                split.states[0],
                split.states[1].model_copy(update={"valid_from": "2020-04-02"}),
            )
        }
    )
    result = apply_alias_cases(
        (record,), (window,), variables={key: gap}, variants=variants
    )
    assert [d.code for d in result.diagnostics] == ["unsupported_alias_window"]
    assert result.variables[key] == gap


def test_overlapping_alias_windows_compose_with_scoped_provenance_and_stale_guards() -> (
    None
):
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    first = _window_case(search, key, variant_key, name="first")
    second = _window_case(
        search, key, variant_key, start="2020-04-01", end="2020-05-31", name="second"
    )
    result = apply_alias_cases(
        (record,), (first, second), variables={key: owned}, variants=variants
    )
    assert result == apply_alias_cases(
        (record,), (second, first), variables={key: owned}, variants=variants
    )
    updated = result.variables[key]
    assert updated is not None and result.diagnostics == ()
    windows = updated.aliases[0].windows
    assert [(w.valid_from, w.valid_to) for w in windows] == [
        ("2020-03-01", "2020-03-31"),
        ("2020-04-01", "2020-04-30"),
        ("2020-05-01", "2020-05-31"),
    ]
    assert windows[1].provenance is not None
    assert "first:" in windows[1].provenance and "second:" in windows[1].provenance
    stale = apply_alias_cases(
        (_record(column="OTHER"),), (first,), variables={key: owned}, variants=variants
    )
    assert stale.variables[key] == owned and stale.evaluations[0].status == "stale"


def test_alias_window_rejects_another_supported_owner_in_the_same_period() -> None:
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    other = variable.model_copy(
        update={
            "slug": "other",
            "states": (
                variable.states[0].model_copy(
                    update={"delivery_column_name": "ALTERNATIVE"}
                ),
            ),
        }
    )
    window = _window_case(search, key, variant_key)
    result = apply_alias_cases(
        (record,),
        (window,),
        variables={key: owned, (*key, "other"): other},
        variants=variants,
    )
    assert [d.code for d in result.diagnostics] == ["conflicting_alias_window_owner"]
    assert result.variables[key] == owned
    future = other.model_copy(
        update={
            "states": (
                other.states[0].model_copy(
                    update={"valid_from": "2021-01-01", "valid_to": "2021-12-31"}
                ),
            )
        }
    )
    assert (
        apply_alias_cases(
            (record,),
            (window,),
            variables={key: owned, (*key, "other"): future},
            variants=variants,
        ).diagnostics
        == ()
    )


def test_competing_alias_window_decisions_withhold_only_their_overlap() -> None:
    record, search, variable, key, variants = _search_alias_fixture()
    variant_key, variant = next(iter(variants.items()))
    owned = variable.model_copy(
        update={
            "aliases": (
                ResolvedAlias(variant=variant, delivery_column_name="ALTERNATIVE"),
            )
        }
    )
    other_key = (*key, "other")
    other = owned.model_copy(update={"slug": "other"})
    first = _window_case(search, key, variant_key, name="first")
    second = _window_case(
        search,
        other_key,
        variant_key,
        start="2020-04-01",
        end="2020-05-31",
        name="second",
    )
    result = apply_alias_cases(
        (record,),
        (first, second),
        variables={key: owned, other_key: other},
        variants=variants,
    )
    assert result == apply_alias_cases(
        (record,),
        (second, first),
        variables={key: owned, other_key: other},
        variants=variants,
    )
    assert len(result.diagnostics) == 2
    assert {(d.code, d.valid_from, d.valid_to) for d in result.diagnostics} == {
        ("conflicting_alias_window_decisions", "2020-04-01", "2020-04-30"),
    }
    assert [
        [(w.valid_from, w.valid_to) for w in v.aliases[0].windows]
        for v in (result.variables[key], result.variables[other_key])
        if v is not None
    ] == [[("2020-03-01", "2020-03-31")], [("2020-05-01", "2020-05-31")]]
