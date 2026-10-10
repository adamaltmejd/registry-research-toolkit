"""Strict source evidence values shared by build ingestion and catalog metadata."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

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


_EVIDENCE_JSON = TypeAdapter(object, config=ConfigDict(ser_json_inf_nan="constants"))
# Where and in which delivery a fact arrived: a new delivery or a row re-sort
# changes these without changing what a record says. Record and claim ids are
# derived: a record id hashes facts its record repeats, a claim id names its list.
# Semantic keys, file and table names, content-addressed value and descriptor
# keys, and delivered cells stay in the digest.
_DELIVERY_POSITION = frozenset(
    {
        "source_revision_id",
        "value_revision_id",
        "row_number",
        "physical_record",
        "physical_cells",
        "record_id",
        "claim_id",
    }
)


def evidence_sha256(value: Any) -> str:
    """Pin delivered evidence by its own content, not by the delivery around it.

    Curation guards hash the records (or code-list claims, declarations, tables) they
    rely on. The whole-delivery revision, row positions and the identities derived
    from them are dropped, and a nested source revision collapses to its dataset, so
    a new delivery or a row re-sort leaves every guard whose own records are unchanged
    fresh. Repeated objects form a multiset: their order is immaterial, their
    multiplicity is not.
    """
    return canonical_sha256(
        _evidence_content(
            _EVIDENCE_JSON.dump_python(value, mode="json", warnings="error")
        )
    )


def _evidence_content(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: item["dataset"]
            if key == "revision" and isinstance(item, dict)
            else _evidence_content(item)
            for key, item in value.items()
            if key not in _DELIVERY_POSITION
        }
    if isinstance(value, list):
        items = [_evidence_content(item) for item in value]
        if all(isinstance(item, dict) for item in items):
            items.sort(key=canonical_json)
        return items
    return value


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
