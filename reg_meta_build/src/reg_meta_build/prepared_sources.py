"""Deterministic artifacts for bounded, provider-neutral source records."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationError,
    field_validator,
    model_validator,
)

from reg_meta_build.source_records import SourceRecord, SourceRevision

if TYPE_CHECKING:
    from collections.abc import Iterable

_FORMAT = "reg-meta-prepared-source-records"
_SCHEMA_VERSION = 1


class PreparedSourceError(ValueError):
    """A prepared source artifact is invalid or does not match its pin."""


class _PreparedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class PreparedSourceManifest(_PreparedModel):
    """Metadata and exact file digest for one bounded prepared source artifact."""

    format: Literal["reg-meta-prepared-source-records"] = _FORMAT
    schema_version: Literal[1] = _SCHEMA_VERSION
    scope: str
    partial: Literal[True] = True
    record_count: int
    revisions: tuple[SourceRevision, ...]
    artifact_sha256: str

    @field_validator("scope")
    @classmethod
    def _non_empty_scope(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("prepared source scope must be non-empty")
        return value

    @field_validator("record_count")
    @classmethod
    def _non_negative_record_count(cls, value: int) -> int:
        if value < 0:
            raise ValueError("prepared source record_count cannot be negative")
        return value

    @field_validator("artifact_sha256")
    @classmethod
    def _valid_artifact_sha256(cls, value: str) -> str:
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("artifact_sha256 must be a lowercase SHA-256 value")
        return value

    @property
    def sha256(self) -> str:
        """Return the exact serialized-artifact digest used for warm-read pinning."""
        return self.artifact_sha256


@dataclass(frozen=True, slots=True)
class PreparedSourceRecords:
    """A verified prepared-source manifest and its decoded records."""

    manifest: PreparedSourceManifest
    records: tuple[SourceRecord, ...]


class _PreparedSourceArtifact(_PreparedModel):
    # simplify: in-memory JSON serves finite source slices; use the existing
    # compact dictionaries/streams before extending preparation to the full corpus.
    format: Literal["reg-meta-prepared-source-records"] = _FORMAT
    schema_version: Literal[1] = _SCHEMA_VERSION
    scope: str
    partial: Literal[True] = True
    record_count: int
    revisions: tuple[SourceRevision, ...]
    records: tuple[SourceRecord, ...]

    @model_validator(mode="after")
    def _valid_contract(self) -> Self:
        if not self.scope.strip():
            raise ValueError("prepared source scope must be non-empty")
        if self.record_count != len(self.records):
            raise ValueError(
                "prepared source record_count does not match the record collection"
            )

        revisions_by_id: dict[str, SourceRevision] = {}
        for revision in self.revisions:
            if revision.revision_id in revisions_by_id:
                raise ValueError(
                    f"duplicate source revision in prepared artifact: {revision.revision_id}"
                )
            revisions_by_id[revision.revision_id] = revision

        for position, record in enumerate(self.records, start=1):
            revision = revisions_by_id.get(record.source_revision_id)
            if revision is None:
                raise ValueError(
                    "prepared source record revision absent from artifact: "
                    f"record {position} ({record.record_id}) references "
                    f"{record.source_revision_id}"
                )
            if revision.dataset != record.source:
                raise ValueError(
                    "prepared source record and revision name different sources: "
                    f"record {position} has {record.source}, revision has "
                    f"{revision.dataset}"
                )
        return self


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _model_sort_key(value: SourceRecord | SourceRevision) -> bytes:
    return _canonical_json_bytes(value.model_dump(mode="json"))


def _validated_revisions(
    revisions: tuple[SourceRevision, ...],
) -> tuple[SourceRevision, ...]:
    validated: list[SourceRevision] = []
    for position, revision in enumerate(revisions, start=1):
        try:
            validated.append(
                SourceRevision.model_validate_json(revision.model_dump_json())
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise PreparedSourceError(
                f"source revision {position} violates the SourceRevision contract: {exc}"
            ) from exc
    return tuple(sorted(validated, key=_model_sort_key))


def _validated_records(records: Iterable[SourceRecord]) -> tuple[SourceRecord, ...]:
    validated: list[SourceRecord] = []
    for position, record in enumerate(records, start=1):
        try:
            validated.append(SourceRecord.model_validate_json(record.model_dump_json()))
        except (AttributeError, TypeError, ValueError) as exc:
            raise PreparedSourceError(
                f"source record {position} violates the SourceRecord contract: {exc}"
            ) from exc
    return tuple(sorted(validated, key=_model_sort_key))


def _write_atomic(output: Path, payload: bytes) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output.name}.", suffix=".tmp", dir=output.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(output)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def prepare_source_records(
    output: Path,
    *,
    records: Iterable[SourceRecord],
    revisions: tuple[SourceRevision, ...],
    scope: str,
) -> PreparedSourceManifest:
    """Validate and atomically write one finite prepared-source slice."""
    validated_revisions = _validated_revisions(revisions)
    validated_records = _validated_records(records)
    try:
        artifact = _PreparedSourceArtifact(
            scope=scope,
            record_count=len(validated_records),
            revisions=validated_revisions,
            records=validated_records,
        )
    except ValidationError as exc:
        raise PreparedSourceError(f"invalid prepared source contract: {exc}") from exc

    payload = _canonical_json_bytes(artifact.model_dump(mode="json"))
    artifact_sha256 = hashlib.sha256(payload).hexdigest()
    _write_atomic(output, payload)
    return PreparedSourceManifest(
        scope=artifact.scope,
        record_count=artifact.record_count,
        revisions=artifact.revisions,
        artifact_sha256=artifact_sha256,
    )


def open_prepared_source_records(
    path: Path, *, expected_sha256: str
) -> PreparedSourceRecords:
    """Open one exact prepared artifact without consulting original source inputs."""
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise PreparedSourceError(
            f"cannot read prepared source artifact {path}: {exc}"
        ) from exc

    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if actual_sha256 != expected_sha256:
        raise PreparedSourceError(
            f"prepared source artifact hash mismatch for {path}: "
            f"expected {expected_sha256}, got {actual_sha256}"
        )

    try:
        selection = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PreparedSourceError(
            f"invalid prepared source artifact JSON {path}: {exc}"
        ) from exc
    if not isinstance(selection, dict):
        raise PreparedSourceError(
            f"invalid prepared source artifact {path}: top level must be an object"
        )
    if selection.get("format") != _FORMAT:
        raise PreparedSourceError(
            f"unsupported prepared source format: {selection.get('format')!r}"
        )
    schema_version = selection.get("schema_version")
    if type(schema_version) is not int or schema_version != _SCHEMA_VERSION:
        raise PreparedSourceError(
            f"unsupported prepared source schema version {schema_version!r}"
        )

    try:
        artifact = _PreparedSourceArtifact.model_validate_json(payload)
    except ValidationError as exc:
        raise PreparedSourceError(
            f"invalid prepared source artifact contract {path}: {exc}"
        ) from exc
    manifest = PreparedSourceManifest(
        scope=artifact.scope,
        record_count=artifact.record_count,
        revisions=artifact.revisions,
        artifact_sha256=actual_sha256,
    )
    return PreparedSourceRecords(manifest=manifest, records=artifact.records)


__all__ = [
    "PreparedSourceError",
    "PreparedSourceManifest",
    "PreparedSourceRecords",
    "open_prepared_source_records",
    "prepare_source_records",
]
