"""Shared synthetic native-family records and the formation entry used by the source formation tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from reg_meta.source_evidence import RecordLocator, SourceRevision
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
)
from reg_meta_build.source_coding import (
    CodeListClaim,
    resolve_code_membership,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import (
    EffectiveOccurrence,
    effective_occurrence,
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

if TYPE_CHECKING:
    from reg_meta_build.sources.swecov_column_types import StewardColumnStorage

REVISION = SourceRevision.create(
    dataset="fixture",
    publisher="SCB",
    purpose="Formation fixture",
    upstream_revision="1",
    artifact_path="input.csv",
    artifact_size=1,
    artifact_sha256="a" * 64,
)


REGISTER = ResolvedRegister(provider="scb", slug="example", name="Example")


# Keyed by the fixture's native variant id, the last native variant key element.
VARIANTS: dict[str | int, ResolvedVariant] = {
    2: ResolvedVariant(slug="people", name="People"),
    3: ResolvedVariant(slug="households", name="Households"),
}


VARIANT = VARIANTS[2]


FLAGS = SourceFields(sensitivity=value_field(False), identifier=value_field(False))


def formation_record(
    year: int,
    *,
    column: str = "VALUE",
    native_id: int | str = 4,
    definition: str = "Source definition",
    variant: int = 2,
    operational_definition: str | None = None,
    source_attribution: str | None = None,
    row: str = "",
) -> SourceRecord:
    return SourceRecord.create(
        revision=REVISION,
        locators=(
            RecordLocator(
                semantic_record_key=("variable:4", f"year:{year}{row}"),
                physical_file="input.csv",
                physical_table="input.csv",
                physical_record=f"row:{year}{row}",
                physical_cells=(),
            ),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(status="value", native_id=1, name="Example"),
            variant=SourceCoordinate(status="value", native_id=variant, name="People"),
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(
                status="value", native_id=native_id, name="Source variable"
            ),
            member=SourceCoordinate(status="value", native_id=year),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start=str(year), end=str(year)),)
        ),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            availability=value_field(True),
            name=value_field("Source variable"),
            definition=value_field(definition),
            operational_definition=value_field(operational_definition)
            if operational_definition
            else None,
            source_attribution=value_field(source_attribution)
            if source_attribution
            else None,
            column_name=value_field(column),
            data_type=value_field("integer"),
        ),
    )


def form_family(
    records: tuple[SourceRecord | EffectiveOccurrence, ...],
    *,
    flags: SourceFields = FLAGS,
    claims: tuple[CodeListClaim, ...] = (),
    storage: dict[str, StewardColumnStorage] | None = None,
):
    variants = {}
    coding = {}
    for record in records:
        occurrence = effective_occurrence(record)
        if (key := occurrence.variant_key) is not None:
            variants[key] = VARIANTS[key[-1]]
        if (key := occurrence.column_key) is not None:
            coding[key] = resolve_code_membership(claims)
    return form_native_variable(
        records,
        register=REGISTER,
        variants=variants,
        slug="value",
        provider_key="4",
        flags=flags,
        coding=coding,
        storage=storage,
    )
