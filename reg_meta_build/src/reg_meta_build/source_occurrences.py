"""Effective occurrence facts with a separate, immutable source-evidence trail."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from reg_meta_build.source_coordinates import (
    NativeKey,
    _coordinate_key,
    native_variable_key,
    native_variant_key,
)

if TYPE_CHECKING:
    from reg_meta_build.source_records import SourceFields, SourceRecord, TemporalScope


@dataclass(frozen=True)
class AppliedCorrection:
    case_id: str
    effect_index: int
    provenance: str


@dataclass(frozen=True)
class EffectiveOccurrence:
    provider: str
    variable_key: NativeKey | None
    variant_key: NativeKey | None
    edition_key: NativeKey | None
    population_key: NativeKey | None
    fields: SourceFields
    edition_scope: TemporalScope
    edition_period_scope: TemporalScope
    source_records: tuple[SourceRecord, ...]
    support_records: tuple[SourceRecord, ...] = ()
    occurrence_key: str | None = None
    corrections: tuple[AppliedCorrection, ...] = ()
    withheld_fields: tuple[str, ...] = ()
    use: Literal["catalog", "support"] = "catalog"

    @property
    def evidence(self) -> tuple[SourceRecord, ...]:
        return (*self.source_records, *self.support_records)

    @property
    def column_key(self) -> NativeKey | None:
        column = self.fields.column_name
        if (
            self.variable_key is None
            or self.variant_key is None
            or column is None
            or column.status != "value"
            or not isinstance(column.value, str)
            or not column.value
        ):
            return None
        return (
            *self.variable_key,
            "variant-key",
            *self.variant_key,
            "column",
            column.value,
        )


def source_occurrence(record: SourceRecord) -> EffectiveOccurrence:
    """Expose source facts directly; never manufacture physical source evidence."""
    editions = {
        key
        for parent in record.parent_facts
        if parent.kind == "edition"
        and (key := _coordinate_key(parent.coordinate)) is not None
    }
    variant_key = native_variant_key(record)
    edition_key = (
        (*variant_key, "edition", *next(iter(editions)))
        if variant_key is not None and len(editions) == 1
        else None
    )
    return EffectiveOccurrence(
        provider=record.subject.provider,
        variable_key=native_variable_key(record),
        variant_key=variant_key,
        edition_key=edition_key,
        population_key=_coordinate_key(record.subject.population),
        fields=record.fields,
        edition_scope=record.edition_scope,
        edition_period_scope=record.edition_period_scope,
        source_records=(record,),
    )


def effective_occurrence(
    record: SourceRecord | EffectiveOccurrence,
) -> EffectiveOccurrence:
    return (
        record if isinstance(record, EffectiveOccurrence) else source_occurrence(record)
    )
