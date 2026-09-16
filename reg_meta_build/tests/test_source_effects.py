"""Checked corrections compose without turning curated output into source evidence."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta.db import open_db
from reg_meta_build.convert_errata import ErrataEditionBinding, convert_delivered_entry
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.scb_errata import ErrataDelivered
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CheckedPeriodChange,
    CuratedOccurrenceAddition,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    RecordExpectation,
    RecordProjection,
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
    record: SourceRecord, *effects: OccurrenceEffect, name: str = "accepted"
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
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=effects,
            reason="Existing accepted delivery correction",
            provenance=f"errata:fixture\n{name}",
        ),
    )


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
        coding={occurrence.column_key: ()},
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
        status="value", value="Value"
    )
    assert applied.occurrences[1].fields.data_type == blank.fields.data_type
    assert applied.occurrences[1].source_records == (blank,)
    assert all(item.occurrence_key is None for item in applied.occurrences)
    # Existing scope and values matter; adding an unrelated variable does not.
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
        == "stale"
    )


def test_converted_absent_delivery_freezes_legacy_donor_choice() -> None:
    before = _record(cvid=10, column="VALUE", year="2018", data_type="varchar")
    after = _record(cvid=20, column="VALUE", year="2022")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    records = before, after, edition
    result = _convert_delivered(records, edition)
    assert result.case is not None and result.blockers == ()
    assert result.donor_refs == (record_ref(after),)
    applied = apply_occurrence_cases(records, (result.case,))
    assert applied.diagnostics == ()
    added = applied.occurrences[-1]
    assert added.source_records == ()
    assert added.fields.data_type == after.fields.data_type
    assert added.edition_scope == _scope("2020")
    # A nearer donor is an applicability error, never an automatic new choice.
    nearer = _record(cvid=40, column="vAlUe", year="2021")
    changed = apply_occurrence_cases((*records, nearer), (result.case,))
    assert changed.accounting[0].disposition == "stale"
    assert all(item.occurrence_key is None for item in changed.occurrences)


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
