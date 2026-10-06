"""Shared synthetic occurrence record builder for the common occurrence-resolution tests."""

from __future__ import annotations

from reg_meta.source_evidence import RecordLocator, SourceField, SourceRevision
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    value_field,
)


def interval_record(
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
