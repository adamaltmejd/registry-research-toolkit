"""Compact normalized records emitted before source reconciliation."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")


class _SourceModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        populate_by_name=True,
        serialize_by_alias=True,
    )


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class SourceRevision(_SourceModel):
    revision_id: str
    dataset: str
    publisher: str
    purpose: str
    upstream_revision: str
    artifact_path: str
    artifact_size: int
    artifact_sha256: str

    @staticmethod
    def _revision_id(dataset: str, upstream_revision: str, artifact_sha256: str) -> str:
        digest = canonical_sha256(
            {
                "dataset": dataset,
                "upstream_revision": upstream_revision,
                "artifact_sha256": artifact_sha256,
            }
        )
        return f"{dataset}@sha256:{digest}"

    @classmethod
    def create(
        cls,
        *,
        dataset: str,
        publisher: str,
        purpose: str,
        upstream_revision: str,
        artifact_path: str,
        artifact_size: int,
        artifact_sha256: str,
    ) -> SourceRevision:
        return cls(
            revision_id=cls._revision_id(dataset, upstream_revision, artifact_sha256),
            dataset=dataset,
            publisher=publisher,
            purpose=purpose,
            upstream_revision=upstream_revision,
            artifact_path=artifact_path,
            artifact_size=artifact_size,
            artifact_sha256=artifact_sha256,
        )

    @model_validator(mode="after")
    def _valid_revision(self) -> Self:
        if not all(
            value.strip()
            for value in (
                self.dataset,
                self.publisher,
                self.purpose,
                self.upstream_revision,
                self.artifact_path,
            )
        ):
            raise ValueError("source revision text fields must be non-empty")
        if self.artifact_size < 0:
            raise ValueError("artifact_size cannot be negative")
        if not _HASH_RE.fullmatch(self.artifact_sha256):
            raise ValueError("artifact_sha256 must be a lowercase SHA-256 value")
        expected = self._revision_id(
            self.dataset, self.upstream_revision, self.artifact_sha256
        )
        if self.revision_id != expected:
            raise ValueError("source revision identity does not match its fields")
        return self


class SourceCoordinate(_SourceModel):
    status: Literal["value", "unknown", "not_applicable"]
    native_id: int | str | None = None
    name: str | None = None

    @model_validator(mode="after")
    def _valid_coordinate(self) -> Self:
        if self.status == "value":
            if self.native_id is None and not self.name:
                raise ValueError("a value coordinate needs a native id or name")
        elif self.native_id is not None or self.name is not None:
            raise ValueError("unknown/not-applicable coordinates cannot carry values")
        return self


class NativeCoordinates(_SourceModel):
    register_id: int | None = None
    register_variant_id: int | None = None
    edition_id: int | None = None
    variable_id: int | None = None
    member_id: int | None = None


class SourceSubject(_SourceModel):
    provider: str
    register_name: SourceCoordinate = Field(alias="register")
    variant: SourceCoordinate
    population: SourceCoordinate
    member: SourceCoordinate
    native: NativeCoordinates

    @field_validator("provider")
    @classmethod
    def _non_empty_provider(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("source provider must be non-empty")
        return value


class ScopeInterval(_SourceModel):
    start: str
    end: str

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if not self.start or not self.end or self.end < self.start:
            raise ValueError("scope interval must have non-empty ordered bounds")
        return self


class TemporalScope(_SourceModel):
    kind: Literal[
        "not_applicable", "unknown", "year_independent", "intervals", "pooled"
    ]
    intervals: tuple[ScopeInterval, ...] = ()
    label: str | None = None

    @model_validator(mode="after")
    def _valid_scope(self) -> Self:
        if self.kind == "intervals":
            if not self.intervals or self.label is not None:
                raise ValueError("interval scope requires intervals and no label")
            for previous, current in zip(self.intervals, self.intervals[1:]):
                if current.start <= previous.end:
                    raise ValueError("scope intervals must be ordered and disjoint")
        elif self.kind in {"unknown", "pooled"}:
            if self.intervals or not self.label:
                raise ValueError(f"{self.kind} scope requires a label")
        elif self.intervals or self.label is not None:
            raise ValueError(f"{self.kind} scope cannot carry intervals or a label")
        return self


FieldState = Literal["value", "unknown", "negative"]
FieldScalar = bool | int | str


class SourceField(_SourceModel):
    status: FieldState
    value: FieldScalar | None = None
    raw_value: FieldScalar | None = None

    @model_validator(mode="after")
    def _valid_field(self) -> Self:
        if self.status == "value" and self.value is None:
            raise ValueError("a value field must carry a value")
        if self.status != "value" and self.value is not None:
            raise ValueError("unknown/negative fields cannot carry a normalized value")
        return self


class SourceFields(_SourceModel):
    availability: SourceField | None = None
    column_name: SourceField | None = None
    name: SourceField | None = None
    definition: SourceField | None = None
    description: SourceField | None = None
    operational_definition: SourceField | None = None
    data_type: SourceField | None = None
    data_length: SourceField | None = None
    representation: SourceField | None = None
    source_attribution: SourceField | None = None
    measurement_unit: SourceField | None = None
    reference_period: SourceField | None = None
    sensitivity: SourceField | None = None
    base_register: SourceField | None = None
    population_definition: SourceField | None = None
    population_comment: SourceField | None = None
    population_date: SourceField | None = None

    @model_validator(mode="after")
    def _typed_fields(self) -> Self:
        if (
            self.availability is not None
            and self.availability.status == "value"
            and self.availability.value is not True
        ):
            raise ValueError(
                "availability values must be true; use negative status for "
                "explicit nonavailability"
            )
        if self.sensitivity is not None and self.sensitivity.status == "value":
            value = self.sensitivity.value
            if type(value) is not bool and value != "conditional":
                raise ValueError(
                    "sensitivity must carry a boolean or the conditional marker"
                )
        for field_name in type(self).model_fields.keys() - {
            "availability",
            "sensitivity",
        }:
            observation = getattr(self, field_name)
            if (
                observation is not None
                and observation.status == "value"
                and not isinstance(observation.value, str)
            ):
                raise ValueError(f"{field_name} must carry a string value")
        for field_name in type(self).model_fields:
            observation = getattr(self, field_name)
            if (
                observation is not None
                and observation.status == "negative"
                and field_name != "availability"
            ):
                raise ValueError("explicit negative is supported only for availability")
        return self


class CodeSetReference(_SourceModel):
    reference_id: str
    content_sha256: str
    physical_locator: str

    @model_validator(mode="after")
    def _non_empty(self) -> Self:
        if not self.reference_id or not self.physical_locator:
            raise ValueError("code-set reference coordinates must be non-empty")
        return self

    @field_validator("content_sha256")
    @classmethod
    def _valid_hash(cls, value: str) -> str:
        if not _HASH_RE.fullmatch(value):
            raise ValueError("code-set content identity must be a SHA-256 value")
        return value


class RecordLocator(_SourceModel):
    semantic_record_key: tuple[str, ...]
    physical_file: str
    physical_table: str
    physical_record: str
    physical_cells: tuple[str, ...]

    @model_validator(mode="after")
    def _non_empty(self) -> Self:
        values = (
            *self.semantic_record_key,
            self.physical_file,
            self.physical_table,
            self.physical_record,
            *self.physical_cells,
        )
        if not self.semantic_record_key or any(not value for value in values):
            raise ValueError("source record locators must be non-empty")
        return self


class DeliveredCell(_SourceModel):
    name: str
    present: bool
    raw_value: str | None = None
    interpreted_value: str
    raw_type: str | None = None
    storage_type: str | None = None
    number_format: str | None = None
    hyperlink_target: str | None = None
    hyperlink_location: str | None = None

    @model_validator(mode="after")
    def _coherent(self) -> Self:
        if (
            not self.name
            or (self.present and self.raw_value is None)
            or (not self.present and self.raw_value is not None)
        ):
            raise ValueError("delivered cell presence and raw value disagree")
        return self


class SourceRecord(_SourceModel):
    record_id: str
    source: str
    source_revision_id: str
    locators: tuple[RecordLocator, ...]
    subject: SourceSubject
    edition_scope: TemporalScope
    edition_period_scope: TemporalScope
    fields: SourceFields
    code_set_references: tuple[CodeSetReference, ...] = ()
    original_period_text: str | None = None
    context: tuple[str, ...] = ()
    delivered_cells: tuple[DeliveredCell, ...] = ()

    @staticmethod
    def _record_id(
        *,
        source: str,
        semantic_record_key: tuple[str, ...],
        subject: SourceSubject,
        edition_scope: TemporalScope,
        edition_period_scope: TemporalScope,
        fields: SourceFields,
        code_set_references: tuple[CodeSetReference, ...],
        original_period_text: str | None,
        context: tuple[str, ...],
        delivered_cells: tuple[DeliveredCell, ...],
    ) -> str:
        digest = canonical_sha256(
            {
                "source": source,
                "semantic_record_key": semantic_record_key,
                "subject": subject.model_dump(mode="json"),
                "edition_scope": edition_scope.model_dump(mode="json"),
                "edition_period_scope": edition_period_scope.model_dump(mode="json"),
                "fields": fields.model_dump(mode="json"),
                "code_set_references": [
                    {
                        "reference_id": reference.reference_id,
                        "content_sha256": reference.content_sha256,
                    }
                    for reference in code_set_references
                ],
                "original_period_text": original_period_text,
                "context": context,
                "delivered_cells": [
                    cell.model_dump(mode="json") for cell in delivered_cells
                ],
            }
        )
        return f"{source}:record:sha256:{digest}"

    @classmethod
    def create(
        cls,
        *,
        revision: SourceRevision,
        locators: tuple[RecordLocator, ...],
        subject: SourceSubject,
        edition_scope: TemporalScope,
        edition_period_scope: TemporalScope,
        fields: SourceFields,
        code_set_references: tuple[CodeSetReference, ...] = (),
        original_period_text: str | None = None,
        context: tuple[str, ...] = (),
        delivered_cells: tuple[DeliveredCell, ...] = (),
    ) -> SourceRecord:
        if not locators:
            raise ValueError("a source record needs at least one physical locator")
        record_id = cls._record_id(
            source=revision.dataset,
            semantic_record_key=locators[0].semantic_record_key,
            subject=subject,
            edition_scope=edition_scope,
            edition_period_scope=edition_period_scope,
            fields=fields,
            code_set_references=code_set_references,
            original_period_text=original_period_text,
            context=context,
            delivered_cells=delivered_cells,
        )
        return cls(
            record_id=record_id,
            source=revision.dataset,
            source_revision_id=revision.revision_id,
            locators=locators,
            subject=subject,
            edition_scope=edition_scope,
            edition_period_scope=edition_period_scope,
            fields=fields,
            code_set_references=code_set_references,
            original_period_text=original_period_text,
            context=context,
            delivered_cells=delivered_cells,
        )

    @model_validator(mode="after")
    def _valid_identity(self) -> Self:
        if not self.locators:
            raise ValueError("a source record needs at least one physical locator")
        semantic_record_key = self.locators[0].semantic_record_key
        if any(
            locator.semantic_record_key != semantic_record_key
            for locator in self.locators[1:]
        ):
            raise ValueError("one semantic record cannot mix semantic locator keys")
        physical = [
            (
                locator.physical_file,
                locator.physical_table,
                locator.physical_record,
                locator.physical_cells,
            )
            for locator in self.locators
        ]
        if len(physical) != len(set(physical)):
            raise ValueError("source record physical locators must be unique")
        expected = self._record_id(
            source=self.source,
            semantic_record_key=semantic_record_key,
            subject=self.subject,
            edition_scope=self.edition_scope,
            edition_period_scope=self.edition_period_scope,
            fields=self.fields,
            code_set_references=self.code_set_references,
            original_period_text=self.original_period_text,
            context=self.context,
            delivered_cells=self.delivered_cells,
        )
        if self.record_id != expected:
            raise ValueError("source record identity does not match its semantic facts")
        if not self.source_revision_id.startswith(f"{self.source}@sha256:"):
            raise ValueError("source record revision belongs to another logical source")
        return self

    def with_additional_locators(
        self, locators: tuple[RecordLocator, ...]
    ) -> SourceRecord:
        payload = self.model_dump(mode="python")
        payload["locators"] = (*self.locators, *locators)
        return type(self).model_validate(payload)


def value_field(value: FieldScalar, *, raw: FieldScalar | None = None) -> SourceField:
    return SourceField(
        status="value", value=value, raw_value=value if raw is None else raw
    )


__all__ = [
    "CodeSetReference",
    "DeliveredCell",
    "NativeCoordinates",
    "RecordLocator",
    "ScopeInterval",
    "SourceCoordinate",
    "SourceField",
    "SourceFields",
    "SourceRecord",
    "SourceRevision",
    "SourceSubject",
    "TemporalScope",
    "canonical_sha256",
    "value_field",
]
