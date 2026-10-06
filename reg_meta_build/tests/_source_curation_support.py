"""Shared synthetic records, expectations and decisions for the curation-applicability tests."""

from __future__ import annotations

import hashlib
from typing import Literal

from reg_meta.source_evidence import (
    DeliveredCell,
    RecordLocator,
    SourceRevision,
)
from reg_meta_build.source_curation import (
    CodingDecision,
    FieldExpectation,
    RecordExpectation,
    RecordProjection,
    SourceRecordRef,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
)


def curation_revision(source: str, marker: str = "accepted") -> SourceRevision:
    digest = hashlib.sha256(marker.encode()).hexdigest()
    return SourceRevision.create(
        dataset=source,
        publisher="fixture publisher",
        purpose="illustrative source-curation test only",
        upstream_revision=marker,
        artifact_path=f"{marker}.source",
        artifact_size=len(marker),
        artifact_sha256=digest,
    )


def scope_interval(start: str, end: str) -> TemporalScope:
    return TemporalScope(
        kind="intervals",
        intervals=(ScopeInterval(start=start, end=end),),
    )


def curation_record(
    *,
    source: str,
    key: tuple[str, ...],
    register_name: str,
    member_name: str,
    fields: SourceFields,
    edition_scope: TemporalScope,
    native: NativeCoordinates | None = None,
    variant: SourceCoordinate | None = None,
    population: SourceCoordinate | None = None,
    marker: str = "accepted",
    row: int = 1,
    context: tuple[str, ...] = (),
    raw_note: str = "original",
) -> SourceRecord:
    revision = curation_revision(source, marker)
    return SourceRecord.create(
        revision=revision,
        locators=(
            RecordLocator(
                semantic_record_key=key,
                physical_file=revision.artifact_path,
                physical_table="fixture",
                physical_record=f"row:{row}",
                physical_cells=(f"fixture!A{row}",),
            ),
        ),
        subject=SourceSubject(
            provider=source.split("-", maxsplit=1)[0],
            register=SourceCoordinate(status="value", name=register_name),
            variant=variant or SourceCoordinate(status="unknown"),
            population=population or SourceCoordinate(status="unknown"),
            member=SourceCoordinate(status="value", name=member_name),
            native=native or NativeCoordinates(),
        ),
        edition_scope=edition_scope,
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=fields,
        context=context,
        delivered_cells=(
            DeliveredCell(
                name="fixture_note",
                present=True,
                raw_value=raw_note,
                interpreted_value=raw_note,
            ),
        ),
    )


def source_ref(record: SourceRecord) -> SourceRecordRef:
    return SourceRecordRef(
        source=record.source,
        semantic_record_key=record.locators[0].semantic_record_key,
    )


def field_value(
    name: str,
    status: Literal["absent", "value", "unknown", "negative"],
    value: str | None = None,
) -> FieldExpectation:
    return FieldExpectation(name=name, status=status, value=value)


def expected_field(record: SourceRecord, name: str) -> FieldExpectation:
    field = getattr(record.fields, name)
    assert field is not None
    return FieldExpectation(name=name, status=field.status, value=field.value)


def projection(
    *fields: FieldExpectation,
    edition_scope: TemporalScope,
    subject: SourceSubject | None = None,
) -> RecordProjection:
    return RecordProjection(
        fields=fields,
        edition_scope=edition_scope,
        subject=subject,
    )


def expectation(
    record: SourceRecord,
    *fields: FieldExpectation,
) -> RecordExpectation:
    return RecordExpectation(
        ref=source_ref(record),
        alternatives=(
            projection(
                *fields,
                edition_scope=record.edition_scope,
                subject=record.subject,
            ),
        ),
    )


def curation_decision() -> CodingDecision:
    """Any decision: these cases check applicability, never application."""
    return CodingDecision(
        reviewed=True,
        column_key=("fixture-column",),
        valid_from="2020-01-01",
        valid_to="2020-12-31",
        expected_codings=(),
        selection="omit_state",
        reason="Retained source evidence does not justify a semantic winner.",
        provenance="illustrative fixture",
    )
