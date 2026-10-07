"""Shared synthetic records, cases and storage columns for the checked-correction (source effects) tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _csv_fixtures import SCB_REVISION, scb_record
from reg_meta.source_evidence import RecordLocator, SourceField
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    RecordExpectation,
    RecordProjection,
    SearchAliasDecision,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceFields,
    SourceRecord,
    TemporalScope,
    value_field,
)
from reg_meta_build.source_reference_records import SourceColumnTypeDeclaration
from reg_meta_build.sources.swecov_column_types import index_swecov_column_types

if TYPE_CHECKING:
    from reg_meta_build.source_curation import OccurrenceEffect


def effect_scope(year: str) -> TemporalScope:
    return TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start=year, end=year),)
    )


def effect_record(
    *,
    row: int = 1,
    cvid: int = 20,
    column: str = "",
    year: str = "2020",
    variable: int = 5,
    data_type: str = "int",
) -> SourceRecord:
    return scb_record(
        row,
        cvid=cvid,
        var_id=variable,
        colname=column,
        register=("TESTREG", 1, 2),
        regver_id=int(year),
        year=year,
        data_type=data_type,
    )


def effect_expectation(record: SourceRecord) -> RecordExpectation:
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


def effect_case(
    record: SourceRecord,
    *effects: OccurrenceEffect,
    name: str = "accepted",
    decision: SearchAliasDecision | None = None,
) -> CurationCase:
    return CurationCase(
        case_id=name,
        targets=(effect_expectation(record),),
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


def effect_field(record: SourceRecord, name: str, value: str) -> CheckedFieldChange:
    return CheckedFieldChange(
        ref=record_ref(record),
        replacement=FieldExpectation(name=name, status="value", value=value),
    )


def storage_columns(
    *pairs: tuple[str, str],
    column: str = "MISSING",
) -> dict[tuple[str, str], SourceColumnTypeDeclaration]:
    return index_swecov_column_types(
        SourceColumnTypeDeclaration(
            revision=SCB_REVISION,
            locator=RecordLocator(
                semantic_record_key=("storage", table, column),
                physical_file="fixture.csv",
                physical_table=table,
                physical_record=f"row:{index}",
                physical_cells=(),
            ),
            delivered_cells=(),
            table_name=value_field(table),
            column_name=value_field(column),
            data_type=value_field(sql_type),
            declared_width=SourceField(status="unknown"),
            nullable=SourceField(status="unknown"),
        )
        for index, (table, sql_type) in enumerate(pairs, start=1)
    )
