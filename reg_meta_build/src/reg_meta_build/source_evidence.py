"""Strict source evidence values shared by build ingestion and catalog metadata."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
FieldState = Literal["value", "unknown", "negative"]
FieldScalar = bool | int | str


class _SourceModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        populate_by_name=True,
        serialize_by_alias=True,
    )


class SourceRecordRef(_SourceModel):
    """Revision- and layout-independent reference to one semantic source member."""

    source: str
    semantic_record_key: tuple[str, ...]

    @model_validator(mode="after")
    def _non_empty(self) -> Self:
        if not self.source.strip() or not self.semantic_record_key:
            raise ValueError("source record references must be non-empty")
        if any(not part.strip() for part in self.semantic_record_key):
            raise ValueError("semantic record key parts must be non-empty")
        return self


def canonical_json(value: Any) -> str:
    """Serialize shared deterministic JSON for digests and artifact fields."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


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
    cached_raw_value: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    cached_raw_type: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def _coherent(self) -> Self:
        if (
            not self.name
            or (self.present and self.raw_value is None)
            or (not self.present and self.raw_value is not None)
        ):
            raise ValueError("delivered cell presence and raw value disagree")
        if (self.cached_raw_value is None) != (self.cached_raw_type is None):
            raise ValueError("cached source value and type must be supplied together")
        return self
