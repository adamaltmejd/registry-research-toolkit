"""Shared synthetic records, prepared value sources and the join entry for the source value-binding tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from _prepared_fixtures import accept_prepared
from reg_meta.source_evidence import RecordLocator, SourceField, SourceRevision
from reg_meta_build.prepared_values import (
    open_prepared_source_values,
    prepare_source_values,
)
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
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
    SourceValueValidity,
)

if TYPE_CHECKING:
    from pathlib import Path


def value_revision(source: str) -> SourceRevision:
    return SourceRevision.create(
        dataset=source,
        publisher="Fixture",
        purpose="test",
        upstream_revision="1",
        artifact_path=source,
        artifact_size=0,
        artifact_sha256="0" * 64,
    )


def value_record(
    *,
    source: str = "records",
    member: int | str = 1001,
    declared: str | None = None,
    description: str = "first",
    identifier: SourceField | None = None,
    provider: str = "test",
) -> SourceRecord:
    return SourceRecord.create(
        revision=value_revision(source),
        locators=(
            RecordLocator(
                semantic_record_key=(str(member),),
                physical_file=source,
                physical_table="records",
                physical_record="row:2",
                physical_cells=("A2",),
            ),
        ),
        subject=SourceSubject(
            provider=provider,
            register=SourceCoordinate(status="value", native_id=1),
            variant=SourceCoordinate(status="value", native_id=2),
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(status="value", native_id=3),
            member=SourceCoordinate(
                status="value",
                native_id=member if type(member) is int else None,
                name=member if isinstance(member, str) else None,
            ),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020", end="2020"),)
        ),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            column_name=value_field("column"),
            description=value_field(description),
            value_set_declared=value_field(declared) if declared else None,
            identifier=identifier,
        ),
    )


def join_bindings(
    kind: Literal["native_member", "member_name", "declared_list"] = "native_member",
    *,
    source: str = "records",
    missing: Literal["unknown", "unrestricted"] = "unrestricted",
) -> SourceValueJoin:
    return SourceValueJoin(
        record_sources=(source,),
        member_target=kind,
        member_format="integer" if kind == "native_member" else "none",
        validity_target="item" if kind == "native_member" else "row",
        missing_validity=missing,
        rule="Fixture explicit structural relation",
        provenance=("fixture format",),
    )


def prepare_value_sources(
    path: Path,
    *,
    join: SourceValueJoin,
    descriptors: tuple[SourceValueDescriptor, ...] = (
        SourceValueDescriptor("list", version="v1"),
    ),
    values: tuple[SourceValue, ...] = (
        SourceValue("a", "01", "One"),
        SourceValue("b", "", "Blank"),
    ),
    rows: tuple[SourceValueAssociation, ...] | None = None,
    validity: tuple[SourceValueValidity, ...] = (),
    validity_present: bool = True,
):
    if rows is None:
        rows = (
            SourceValueAssociation(
                2, "list", "a", "values", member_id="1001", item_id="1"
            ),
        )
    manifest = prepare_source_values(
        path,
        revision=value_revision("values"),
        descriptors=descriptors,
        values=values,
        associations=rows,
        validity=validity,
        validity_revision=value_revision("validity") if validity_present else None,
        join=join,
    )
    commit = accept_prepared(path)
    return open_prepared_source_values(
        path, expected_sha256=manifest.sha256, input_commit=commit
    )
