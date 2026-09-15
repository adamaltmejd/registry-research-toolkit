"""Versioned catalog-input bundles and lossless SCB CSV snapshots.

This module selects every ordinary catalog-build input at one accepted local Git
revision. It also condenses a coherent SCB delivery into deterministic, reviewable text
files and can reconstruct the same ordered logical records without consulting the
retained raw archive. Provider interpretation (encoding repair, filtering, projection,
coalescing, and curation) stays in the existing loaders and adapters.

The archive contract is logical-record losslessness, not original-CSV byte identity.
The independently retained archive remains the authority for delimiters, quoting, and
line endings.  Snapshot cells retain their exact bytes, however, including undefined
cp1252 bytes and the distinction between quoted and unquoted empty fields.
"""

from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import io
import json
import os
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self, cast
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from .db import _CURATED_PROVIDERS, DDL, _file_sha256
from .dbdiff import TableIgnore, diff_db_content, format_report

if TYPE_CHECKING:
    from collections.abc import Collection, Iterator, Mapping, Sequence
    from typing import IO

SNAPSHOT_SCHEMA_VERSION = 1
CONVERTER_VERSION = 1
SERIALIZATION = "escaped-tsv-v1"
CHUNK_RECORDS = 100_000
MANIFEST_NAME = "manifest.json"
BUNDLE_MANIFEST_NAME = "catalog-bundle.json"
BUNDLE_SCHEMA_VERSION = 2

LISA_DATASET_ID = "scb-lisa-variable-availability"
LISA_BUNDLE_PATH = "supplemental/scb-lisa-variable-availability/source.xlsx"
LISA_PUBLISHER = "SCB"
LISA_PURPOSE = (
    "LISA physical-column declarations by documented table and temporal scope"
)
LISA_READER = "lisa-variable-workbook-v1"
LISA_LAYOUT = "lisa-variable-list-2024-2025-v1"

CATALOG_CURATION_FILES = (
    "alias_windows.toml",
    "cis2014-matrix-meaning-evidence.json",
    "cis2016-matrix-meaning-evidence.json",
    "classifications.toml",
    "codeless_overlap.toml",
    "codelivery.toml",
    "concept_groups.auto.toml",
    "concept_groups.toml",
    "delivery_enrichment.generated.toml",
    "lineage.toml",
    "period_family_merges.toml",
    "relations.toml",
    "scb_errata.toml",
    "tags.toml",
)
CATALOG_SLUG_PROVIDERS = (
    "scb",
    "sos",
    *(provider for provider, _directory in _CURATED_PROVIDERS),
)
_BUNDLE_INVENTORY_ROOTS = ("catalog", "curation", "fqid_slugs", "supplemental")

_GIT_PACK_SETTINGS = (
    ("core.compression", "9"),
    ("pack.compression", "9"),
    ("pack.depth", "50"),
    ("pack.threads", "1"),
    ("pack.useSparse", "true"),
    ("pack.window", "10"),
    ("pack.windowMemory", "0"),
    ("repack.useDeltaBaseOffset", "true"),
    ("repack.writeBitmaps", "false"),
)

_REPLAY_DB_IGNORE = {
    "import_manifest": TableIgnore(skip_where="key IN ('import_date', 'input_dir')")
}

SCB_CSV_FILES = (
    "Registerinformation.csv",
    "UnikaRegisterOchVariabler.csv",
    "Identifierare.csv",
    "Timeseries.csv",
    "Vardemangder.csv",
    "VardemangderValidDates.csv",
)

_KEY_RE = re.compile(r"[A-Za-z0-9_-]{22}\Z")
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_GIT_COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")
_UNSET = object()

# These groups are compression hints only.  A group is enabled only when all of its
# named columns occur exactly once.  A missing, duplicated, or oddly encoded heading
# falls back to inline storage, so source drift cannot lose a column.
_GROUP_RULES: dict[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "Registerinformation.csv": (
        ("register", ("Registernamn", "Registerrubrik", "Registersyfte")),
        (
            "variant",
            (
                "Registervariantrubrik",
                "Registervariantnamn",
                "Registervariantbeskrivning",
                "RegistervariantSekretess",
            ),
        ),
        (
            "version",
            (
                "Registerversionnamn",
                "Registerversionbeskrivning",
                "Registerversionmätinformation",
                "Registerversion_DocStaus",
                "Registerversion_ForstaGodkannandeDatum",
                "Registerversion_SenastGodkandDatum",
            ),
        ),
        (
            "context",
            (
                "Populationnamn",
                "Populationdefinition",
                "Populationkommentar",
                "Populationdatum",
                "Objekttypnamn",
                "Objekttypdefinition",
            ),
        ),
        (
            "variable",
            (
                "Variabelnamn",
                "Variabeldefinition",
                "Variabelbeskrivning",
                "VariabelOperationell_definition",
                "VariabelReferenstid",
                "VariabelHämtadFrån",
                "VariabelRegister_Källa",
                "VariabelExtern_kommentar",
                "Mattenhet",
            ),
        ),
        ("delivery", ("Kolumnnamn", "Datatyp", "Datalängd")),
    ),
    "Vardemangder.csv": (
        ("value_set", ("Värdemängdsversion", "Värdemängdsnivå")),
        ("value", ("Värdekod", "Värdebenämning")),
    ),
}

type RawCell = str | None
type EncodedCell = tuple[str, ...]


class SnapshotError(ValueError):
    """The delivery or snapshot violates the lossless snapshot contract."""


class SnapshotMaterializationError(SnapshotError):
    """A complete pinned source role must be materialized before it can be read."""

    def __init__(self, repository: Path, commit: str, role_path: str) -> None:
        self.repository = repository
        self.commit = commit
        self.role_path = role_path
        self.hydration_action = (
            "run `git sparse-checkout add --stdin` in repository "
            f"{repository} and provide `{role_path}` on stdin"
        )
        super().__init__(
            "SCB prepared values are not materialized in the pinned input "
            f"repository {repository} at commit {commit}; {self.hydration_action}"
        )


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _NamedCsvModel(_Model):
    name: str

    @field_validator("name")
    @classmethod
    def _safe_csv_name(cls, value: str) -> str:
        if Path(value).name != value or not value.lower().endswith(".csv"):
            raise ValueError("name must be a plain .csv filename")
        return value


class InventoryFile(_NamedCsvModel):
    required: bool


class ArchiveInput(_Model):
    path: Path
    locator: str
    members: tuple[str, ...]
    sha256: str

    @field_validator("locator")
    @classmethod
    def _stable_locator(cls, value: str) -> str:
        if not value or Path(value).is_absolute() or value.startswith("file:"):
            raise ValueError(
                "locator must be stable and must not be an absolute host path"
            )
        return value

    @field_validator("sha256")
    @classmethod
    def _valid_hash(cls, value: str) -> str:
        if not _HASH_RE.fullmatch(value):
            raise ValueError("sha256 must be 64 lowercase hexadecimal characters")
        return value


class DeliveryInventory(_Model):
    bundle_id: str
    edition: str
    source_dir: Path
    files: tuple[InventoryFile, ...]
    archives: tuple[ArchiveInput, ...]

    @model_validator(mode="after")
    def _coherent_inventory(self) -> Self:
        names = [item.name for item in self.files]
        if len(names) != len(set(names)):
            raise ValueError("inventory file names must be unique")
        missing = set(SCB_CSV_FILES) - set(names)
        if missing:
            raise ValueError(
                f"inventory must declare every known SCB CSV: {sorted(missing)}"
            )
        if not self.archives:
            raise ValueError("at least one independently retained archive is required")
        unknown_members = {
            member
            for archive in self.archives
            for member in archive.members
            if member not in set(names)
        }
        if unknown_members:
            raise ValueError(
                f"archive members are not declared files: {sorted(unknown_members)}"
            )
        return self


class ArchiveManifest(_Model):
    locator: str
    size: int
    sha256: str
    members: tuple[str, ...]

    @model_validator(mode="after")
    def _valid_archive(self) -> Self:
        if (
            not self.locator
            or Path(self.locator).is_absolute()
            or self.locator.startswith("file:")
        ):
            raise ValueError("archive locator must not be an absolute host path")
        if self.size < 0 or not _HASH_RE.fullmatch(self.sha256):
            raise ValueError("archive size/hash is invalid")
        return self


class NormalizedFile(_Model):
    path: str
    size: int
    sha256: str
    lines: int

    @field_validator("path")
    @classmethod
    def _safe_relative_path(cls, value: str) -> str:
        path = Path(value)
        if path.is_absolute() or ".." in path.parts or not value.startswith("files/"):
            raise ValueError("normalized file path must stay under files/")
        return value

    @field_validator("sha256")
    @classmethod
    def _valid_hash(cls, value: str) -> str:
        if not _HASH_RE.fullmatch(value):
            raise ValueError("sha256 must be 64 lowercase hexadecimal characters")
        return value

    @model_validator(mode="after")
    def _nonnegative_counts(self) -> Self:
        if self.size < 0 or self.lines < 0:
            raise ValueError("normalized size and line count cannot be negative")
        return self


class PayloadGroup(_Model):
    name: str
    positions: tuple[int, ...]
    dictionary: NormalizedFile
    entries: int


class SnapshotFile(_NamedCsvModel):
    required: bool
    present: bool
    raw_size: int | None = None
    raw_sha256: str | None = None
    header: tuple[EncodedCell, ...] = ()
    column_count: int = 0
    record_count: int = 0
    ordered_records_sha256: str | None = None
    logical_sha256: str | None = None
    groups: tuple[PayloadGroup, ...] = ()
    inline_positions: tuple[int, ...] = ()
    records: tuple[NormalizedFile, ...] = ()

    @model_validator(mode="after")
    def _valid_shape(self) -> Self:
        if not self.present:
            if self.required:
                raise ValueError("a required file cannot be absent")
            populated = (
                self.raw_size is not None
                or self.raw_sha256 is not None
                or self.header
                or self.column_count
                or self.record_count
                or self.ordered_records_sha256 is not None
                or self.logical_sha256 is not None
                or self.groups
                or self.inline_positions
                or self.records
            )
            if populated:
                raise ValueError(
                    "an absent file cannot carry source or normalized data"
                )
            return self

        if self.raw_size is None or self.raw_sha256 is None:
            raise ValueError("a present file requires raw size and hash")
        if self.raw_size < 0 or not _HASH_RE.fullmatch(self.raw_sha256):
            raise ValueError("raw file size/hash is invalid")
        if self.ordered_records_sha256 is None or self.logical_sha256 is None:
            raise ValueError("a present file requires logical record hashes")
        if self.column_count != len(self.header):
            raise ValueError("column_count does not match the retained header")
        for cell in self.header:
            _validate_cell(cell)
        positions = [position for group in self.groups for position in group.positions]
        positions.extend(self.inline_positions)
        if sorted(positions) != list(range(self.column_count)):
            raise ValueError(
                "group and inline positions must partition every column once"
            )
        if len({group.name for group in self.groups}) != len(self.groups):
            raise ValueError("payload group names must be unique within a file")
        return self


class SnapshotManifest(_Model):
    format: Literal["reg-meta-build-input-snapshot"]
    schema_version: int
    converter_version: int
    converter_commit: str
    serialization: Literal["escaped-tsv-v1"]
    byte_codec: Literal["latin-1-passthrough/cp1252-or-base64"]
    csv_dialect: Literal["pipe-doublequote-quote-notnull"]
    null_policy: Literal["unquoted-empty-is-null;quoted-empty-is-empty-text"]
    dictionary_key: Literal["blake2b-128-base64url"]
    chunk_records: int
    bundle_id: str
    edition: str
    archives: tuple[ArchiveManifest, ...]
    files: tuple[SnapshotFile, ...]

    @field_validator("converter_commit")
    @classmethod
    def _valid_converter_commit(cls, value: str) -> str:
        if not value or any(char.isspace() for char in value):
            raise ValueError("converter_commit must be one non-empty revision token")
        return value

    @model_validator(mode="after")
    def _supported_and_coherent(self) -> Self:
        if self.schema_version != SNAPSHOT_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported snapshot schema version {self.schema_version}"
            )
        if self.converter_version != CONVERTER_VERSION:
            raise ValueError(f"unsupported converter version {self.converter_version}")
        if self.chunk_records != CHUNK_RECORDS:
            raise ValueError(f"unsupported chunk size {self.chunk_records}")
        names = [item.name for item in self.files]
        if len(names) != len(set(names)):
            raise ValueError("snapshot file names must be unique")
        covered = {member for archive in self.archives for member in archive.members}
        uncovered = {item.name for item in self.files if item.present} - covered
        if uncovered:
            raise ValueError(
                f"present files lack an archive locator: {sorted(uncovered)}"
            )
        normalized = [
            data_file.path
            for item in self.files
            for data_file in _normalized_files(item)
        ]
        if len(normalized) != len(set(normalized)):
            raise ValueError("normalized file paths must be unique")
        return self


class AuxiliaryPin(_Model):
    name: str
    present: bool
    size: int | None = None
    sha256: str | None = None

    @model_validator(mode="after")
    def _valid_presence(self) -> Self:
        if self.present:
            if self.size is None or self.size < 0 or self.sha256 is None:
                raise ValueError("present auxiliary input requires size and sha256")
            if not _HASH_RE.fullmatch(self.sha256):
                raise ValueError("auxiliary sha256 must be lowercase hexadecimal")
        elif self.size is not None or self.sha256 is not None:
            raise ValueError("absent auxiliary input cannot carry size or sha256")
        return self


class BuildLock(_Model):
    format: Literal["reg-meta-build-replay-lock"]
    schema_version: Literal[1]
    input_repository_commit: str
    snapshot_path: str
    snapshot_manifest_sha256: str
    snapshot_schema_version: int
    snapshot_converter_version: int
    builder_commit: str
    uv_lock_sha256: str
    python_runtime: str
    providers: tuple[str, ...]
    build_options: dict[str, bool | int | str]
    auxiliary_inputs: tuple[AuxiliaryPin, ...]
    result_db_sha256: str

    @model_validator(mode="after")
    def _valid_lock(self) -> Self:
        hashes = (
            self.snapshot_manifest_sha256,
            self.uv_lock_sha256,
            self.result_db_sha256,
        )
        if not all(_HASH_RE.fullmatch(value) for value in hashes):
            raise ValueError("build-lock hashes must be lowercase SHA256 values")
        if not self.providers or len(set(self.providers)) != len(self.providers):
            raise ValueError("providers must be non-empty and unique")
        names = [item.name for item in self.auxiliary_inputs]
        if len(names) != len(set(names)):
            raise ValueError("auxiliary input names must be unique")
        return self


class BundleFile(_Model):
    path: str
    present: bool
    size: int | None = None
    sha256: str | None = None

    @field_validator("path")
    @classmethod
    def _safe_path(cls, value: str) -> str:
        path = Path(value)
        if (
            path.is_absolute()
            or ".." in path.parts
            or len(path.parts) < 2
            or path.parts[0] not in _BUNDLE_INVENTORY_ROOTS
        ):
            raise ValueError(
                "bundle file path must stay under catalog/, curation/, fqid_slugs/, "
                "or supplemental/"
            )
        return path.as_posix()

    @model_validator(mode="after")
    def _valid_presence(self) -> Self:
        if self.present:
            if self.size is None or self.size < 0 or self.sha256 is None:
                raise ValueError("present bundle file requires size and sha256")
            if not _HASH_RE.fullmatch(self.sha256):
                raise ValueError("bundle file sha256 must be lowercase hexadecimal")
        elif self.size is not None or self.sha256 is not None:
            raise ValueError("absent bundle file cannot carry size or sha256")
        return self


class SupplementalDataset(_Model):
    dataset: Literal["scb-lisa-variable-availability"]
    publisher: Literal["SCB"]
    purpose: str
    upstream_revision: str | None
    reader: Literal["lisa-variable-workbook-v1"]
    layout: Literal["lisa-variable-list-2024-2025-v1"]
    selected: bool
    required: bool
    artifact_path: str
    exclusion_reason: str | None = None

    @field_validator("purpose")
    @classmethod
    def _supported_purpose(cls, value: str) -> str:
        if value != LISA_PURPOSE:
            raise ValueError(f"unsupported LISA dataset purpose {value!r}")
        return value

    @field_validator("upstream_revision")
    @classmethod
    def _clean_revision(cls, value: str | None) -> str | None:
        if value is not None and (not value.strip() or value != value.strip()):
            raise ValueError("LISA upstream revision must be non-empty and trimmed")
        return value

    @field_validator("artifact_path")
    @classmethod
    def _safe_artifact_path(cls, value: str) -> str:
        if value != LISA_BUNDLE_PATH:
            raise ValueError(
                f"LISA supplemental artifact path must be {LISA_BUNDLE_PATH!r}"
            )
        return value

    @model_validator(mode="after")
    def _coherent_selection(self) -> Self:
        if self.selected:
            if not self.required or not self.upstream_revision:
                raise ValueError(
                    "a selected supplemental dataset must be required and revisioned"
                )
            if self.exclusion_reason is not None:
                raise ValueError("a selected supplemental dataset cannot be excluded")
        elif (
            self.required
            or self.upstream_revision is not None
            or not self.exclusion_reason
        ):
            raise ValueError(
                "an unselected supplemental dataset needs an exclusion reason only"
            )
        return self


class CatalogBundleManifest(_Model):
    format: Literal["reg-meta-build-catalog-input-bundle"]
    schema_version: int
    bundle_id: str
    edition: str
    scb_snapshot_path: str
    scb_manifest_sha256: str
    files: tuple[BundleFile, ...]
    supplemental_datasets: tuple[SupplementalDataset, ...]

    @field_validator("scb_snapshot_path")
    @classmethod
    def _safe_snapshot_path(cls, value: str) -> str:
        path = Path(value)
        if path.is_absolute() or ".." in path.parts or value in {"", "."}:
            raise ValueError(
                "scb_snapshot_path must be a non-empty repository-relative path"
            )
        return path.as_posix()

    @model_validator(mode="after")
    def _supported_and_coherent(self) -> Self:
        if self.schema_version != BUNDLE_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported catalog bundle schema version {self.schema_version}"
            )
        if not _HASH_RE.fullmatch(self.scb_manifest_sha256):
            raise ValueError("scb_manifest_sha256 must be a lowercase SHA256 value")
        paths = [item.path for item in self.files]
        if len(paths) != len(set(paths)):
            raise ValueError("catalog bundle file paths must be unique")
        if paths != sorted(paths):
            raise ValueError("catalog bundle file paths must be sorted")
        datasets = [item.dataset for item in self.supplemental_datasets]
        if datasets != [LISA_DATASET_ID]:
            raise ValueError(
                "catalog bundle must declare the one supported supplemental dataset"
            )
        inventory = {item.path: item for item in self.files}
        dataset = self.supplemental_datasets[0]
        artifact = inventory.get(dataset.artifact_path)
        if artifact is None or artifact.present != dataset.selected:
            raise ValueError(
                "supplemental dataset selection must match its bundle inventory entry"
            )
        return self


@dataclass(frozen=True)
class SnapshotStats:
    raw_bytes: int
    normalized_bytes: int
    records: int
    elapsed_seconds: float
    codec_sample: dict[str, dict[str, int]]


@dataclass(frozen=True)
class ScbSnapshotSelection:
    """An explicitly pinned normalized SCB input selected for ``build_db``."""

    path: Path
    input_commit: str
    manifest_sha256: str


@dataclass(frozen=True)
class CatalogBundleSelection:
    """An explicitly pinned complete catalog input selected for ``build_db``."""

    path: Path
    input_commit: str
    manifest_sha256: str


@dataclass(frozen=True)
class LisaWorkbookSelection:
    """Explicit source identity for the optional LISA supplemental dataset."""

    path: Path
    upstream_revision: str
    sha256: str


@dataclass(frozen=True)
class CatalogBundleReader:
    """Resolved read-only paths from one accepted catalog input bundle."""

    root: Path
    repository: Path
    manifest: CatalogBundleManifest
    snapshot: ScbSnapshotReader
    provenance: dict[str, str]

    @property
    def input_dir(self) -> Path:
        return self.root / "catalog"

    @property
    def curation_dir(self) -> Path:
        return self.root / "curation"

    @property
    def slug_dir(self) -> Path:
        return self.root / "fqid_slugs"

    def require_supplemental_dataset(
        self, dataset_id: str
    ) -> tuple[SupplementalDataset, BundleFile, Path]:
        dataset = next(
            (
                item
                for item in self.manifest.supplemental_datasets
                if item.dataset == dataset_id
            ),
            None,
        )
        if dataset is None:
            raise SnapshotError(
                f"catalog bundle does not declare supplemental dataset {dataset_id!r}"
            )
        if not dataset.selected:
            raise SnapshotError(
                f"supplemental dataset {dataset_id!r} was not selected when this "
                "bundle was prepared; prepare and accept a bundle with the explicit "
                "LISA workbook selection"
            )
        artifact = next(
            item for item in self.manifest.files if item.path == dataset.artifact_path
        )
        if not artifact.present:
            raise SnapshotError(
                f"selected supplemental dataset {dataset_id!r} is missing its artifact"
            )
        return dataset, artifact, self.root / artifact.path


@dataclass(frozen=True)
class CatalogBundleStats:
    files: int
    bytes: int
    manifest_sha256: str


@dataclass
class _CodecSample:
    limit: int
    by_kind: dict[str, dict[str, int]] = field(default_factory=dict)

    def add(self, kind: str, logical: Sequence[str | EncodedCell], tsv: bytes) -> None:
        counts = self.by_kind.setdefault(
            kind, {"lines": 0, "escaped_tsv_bytes": 0, "jsonl_bytes": 0}
        )
        if counts["lines"] >= self.limit:
            return
        counts["lines"] += 1
        counts["escaped_tsv_bytes"] += len(tsv)
        counts["jsonl_bytes"] += len(_json_line(logical))


def _json_line(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode()


def _manifest_bytes(
    manifest: SnapshotManifest | CatalogBundleManifest | BuildLock,
) -> bytes:
    return (
        json.dumps(
            manifest.model_dump(mode="json"),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode()


def _encode_cell(value: RawCell) -> EncodedCell:
    if value is None:
        return ("n",)
    raw = value.encode("latin-1")
    if b"\0" in raw:
        return ("b", base64.b64encode(raw).decode("ascii"))
    try:
        decoded = raw.decode("cp1252")
    except UnicodeDecodeError:
        return ("b", base64.b64encode(raw).decode("ascii"))
    return ("t", decoded)


def _validate_cell(value: Sequence[str]) -> EncodedCell:
    token = tuple(value)
    if token == ("n",):
        return token
    if len(token) != 2 or token[0] not in {"t", "b"}:
        raise SnapshotError(f"invalid encoded cell {token!r}")
    if token[0] == "t":
        try:
            raw = token[1].encode("cp1252")
        except UnicodeEncodeError as exc:
            raise SnapshotError(
                "text cell is not reversibly encodable as cp1252"
            ) from exc
        if b"\0" in raw:
            raise SnapshotError("NUL-bearing cells must use canonical base64 encoding")
        return token
    try:
        raw = base64.b64decode(token[1], validate=True)
    except (ValueError, binascii.Error) as exc:
        raise SnapshotError("binary cell is not valid base64") from exc
    if base64.b64encode(raw).decode("ascii") != token[1]:
        raise SnapshotError("binary cell is not canonical base64")
    if b"\0" not in raw:
        try:
            raw.decode("cp1252")
        except UnicodeDecodeError:
            pass
        else:
            raise SnapshotError("cp1252 cells must use canonical text encoding")
    return token


def _decode_cell(value: Sequence[str]) -> RawCell:
    token = _validate_cell(value)
    if token[0] == "n":
        return None
    if token[0] == "t":
        return token[1].encode("cp1252").decode("latin-1")
    return base64.b64decode(token[1], validate=True).decode("latin-1")


def _escape_text(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
    )


def _unescape_text(value: str) -> str:
    output: list[str] = []
    index = 0
    while index < len(value):
        char = value[index]
        if char != "\\":
            output.append(char)
            index += 1
            continue
        index += 1
        if index == len(value) or value[index] not in {"\\", "t", "r", "n"}:
            raise SnapshotError("invalid escaped-TSV text escape")
        output.append({"\\": "\\", "t": "\t", "r": "\r", "n": "\n"}[value[index]])
        index += 1
    return "".join(output)


def _cell_field(value: EncodedCell) -> str:
    token = _validate_cell(value)
    if token[0] == "n":
        return "n"
    if token[0] == "t":
        return "t" + _escape_text(token[1])
    return "b" + token[1]


def _field_cell(value: str) -> EncodedCell:
    if value == "n":
        return ("n",)
    if not value:
        raise SnapshotError("empty escaped-TSV field")
    if value[0] == "t":
        return _validate_cell(("t", _unescape_text(value[1:])))
    if value[0] == "b":
        return _validate_cell(("b", value[1:]))
    raise SnapshotError(f"unknown escaped-TSV cell tag {value[0]!r}")


def _decode_field(value: str) -> RawCell:
    if value == "n":
        return None
    if (
        value.startswith("t")
        and "\\" not in value
        and "\0" not in value
        and value[1:].isascii()
    ):
        return value[1:]
    return _decode_cell(_field_cell(value))


def _tsv_line(fields: Sequence[str]) -> bytes:
    if any("\t" in value or "\r" in value or "\n" in value for value in fields):
        raise AssertionError("escaped TSV fields must occupy one physical line")
    return ("\t".join(fields) + "\n").encode()


def _iter_tsv(
    path: Path, expected: NormalizedFile | None = None
) -> Iterator[tuple[int, list[str]]]:
    digest = hashlib.sha256() if expected is not None else None
    size = 0
    lines = 0
    try:
        handle = path.open("rb")
    except OSError as exc:
        raise SnapshotError(f"cannot open normalized artifact {path}: {exc}") from exc
    try:
        with handle:
            for line_number, raw in enumerate(handle, start=1):
                if digest is not None:
                    digest.update(raw)
                    size += len(raw)
                lines += 1
                if not raw.endswith(b"\n"):
                    raise SnapshotError(
                        f"{path}: line {line_number} lacks a newline terminator"
                    )
                try:
                    text = raw[:-1].decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise SnapshotError(
                        f"{path}: line {line_number} is not UTF-8"
                    ) from exc
                if "\r" in text:
                    raise SnapshotError(
                        f"{path}: line {line_number} contains an unescaped CR"
                    )
                yield line_number, text.split("\t")
    except OSError as exc:
        raise SnapshotError(f"cannot read normalized artifact {path}: {exc}") from exc
    if expected is not None:
        assert digest is not None
        if (
            size != expected.size
            or lines != expected.lines
            or digest.hexdigest() != expected.sha256
        ):
            raise SnapshotError(
                f"normalized file hash/size/line mismatch: {expected.path}"
            )


@contextmanager
def open_lossless_csv(
    path: Path,
) -> Iterator[tuple[list[RawCell], Iterator[list[RawCell]]]]:
    """Yield exact latin-1-pass-through fields from an SCB-style CSV.

    Python 3.14's ``QUOTE_NOTNULL`` is load-bearing: it reads an unquoted empty
    field as ``None`` and a quoted empty field as ``""`` while leaving all other
    lexical values as strings.
    """
    try:
        raw_handle = path.open("rb")
    except OSError as exc:
        raise SnapshotError(f"cannot open source CSV {path}: {exc}") from exc
    with raw_handle:
        text_handle = io.TextIOWrapper(raw_handle, encoding="latin-1", newline="")
        reader = csv.reader(
            text_handle,
            delimiter="|",
            quotechar='"',
            quoting=csv.QUOTE_NOTNULL,
            strict=True,
        )
        try:
            header = cast("list[RawCell]", next(reader))
        except StopIteration as exc:
            raise SnapshotError(f"source CSV is empty: {path.name}") from exc
        except csv.Error as exc:
            raise SnapshotError(f"malformed header in {path.name}: {exc}") from exc
        width = len(header)

        def rows() -> Iterator[list[RawCell]]:
            try:
                for row_number, raw_row in enumerate(reader, start=2):
                    row = cast("list[RawCell]", raw_row)
                    if len(row) != width:
                        raise SnapshotError(
                            f"{path.name}: row {row_number} has {len(row)} fields; expected {width}"
                        )
                    yield row
            except csv.Error as exc:
                raise SnapshotError(f"malformed CSV in {path.name}: {exc}") from exc

        yield header, rows()


def _update_record_hash(digest: Any, cells: Sequence[RawCell]) -> None:
    digest.update(len(cells).to_bytes(4, "big"))
    for value in cells:
        if value is None:
            digest.update(b"n")
            continue
        raw = value.encode("latin-1")
        digest.update(b"b")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)


def _payload_key(name: str, positions: Sequence[int], cells: Sequence[RawCell]) -> str:
    digest = hashlib.blake2b(digest_size=16, person=b"regmeta-input-v1")
    name_bytes = name.encode()
    digest.update(len(name_bytes).to_bytes(2, "big"))
    digest.update(name_bytes)
    digest.update(len(positions).to_bytes(2, "big"))
    for position in positions:
        digest.update(position.to_bytes(4, "big"))
    _update_record_hash(digest, cells)
    return base64.urlsafe_b64encode(digest.digest()).rstrip(b"=").decode("ascii")


def _header_text(value: RawCell) -> str | None:
    if value is None:
        return None
    try:
        return value.encode("latin-1").decode("cp1252")
    except UnicodeDecodeError:
        return None


def _group_layout(
    name: str, header: Sequence[RawCell]
) -> tuple[list[tuple[str, tuple[int, ...]]], tuple[int, ...]]:
    decoded = [_header_text(value) for value in header]
    groups: list[tuple[str, tuple[int, ...]]] = []
    claimed: set[int] = set()
    for group_name, column_names in _GROUP_RULES.get(name, ()):
        positions: list[int] = []
        for column_name in column_names:
            matches = [
                index for index, value in enumerate(decoded) if value == column_name
            ]
            if len(matches) != 1:
                positions = []
                break
            positions.append(matches[0])
        if positions and not claimed.intersection(positions):
            group_positions = tuple(positions)
            groups.append((group_name, group_positions))
            claimed.update(group_positions)
    inline = tuple(index for index in range(len(header)) if index not in claimed)
    return groups, inline


def _safe_file_dir(name: str) -> str:
    return quote(name, safe="._-")


def _normalized_file(root: Path, path: Path, lines: int) -> NormalizedFile:
    relative = path.relative_to(root).as_posix()
    return NormalizedFile(
        path=relative,
        size=path.stat().st_size,
        sha256=_file_sha256(path),
        lines=lines,
    )


def _prepare_file(
    source: Path,
    root: Path,
    required: bool,
    codec_sample: _CodecSample,
) -> SnapshotFile:
    source_stat = source.stat()
    source_identity = _file_identity(source_stat)
    file_root = root / "files" / _safe_file_dir(source.name)
    records_root = file_root / "records"
    dictionaries_root = file_root / "dictionaries"
    records_root.mkdir(parents=True)
    dictionaries_root.mkdir()

    database_fd, database_name = tempfile.mkstemp(
        prefix="regmeta-input-payloads-", suffix=".sqlite"
    )
    os.close(database_fd)
    database_path = Path(database_name)
    conn = sqlite3.connect(database_path)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute(
        "CREATE TABLE payload ("
        "group_name TEXT NOT NULL, key TEXT NOT NULL, payload_sha256 TEXT NOT NULL, "
        "payload TEXT NOT NULL, PRIMARY KEY (group_name, key, payload_sha256)"
        ") WITHOUT ROWID"
    )
    record_files: list[NormalizedFile] = []
    record_handle: Any = None
    record_path: Path | None = None
    record_lines = 0
    record_count = 0
    records_digest = hashlib.sha256()
    logical_digest = hashlib.sha256()
    try:
        with open_lossless_csv(source) as (header, rows):
            groups, inline = _group_layout(source.name, header)
            logical_digest.update(b"header\0")
            _update_record_hash(logical_digest, header)
            for row in rows:
                if record_count % CHUNK_RECORDS == 0:
                    if record_handle is not None and record_path is not None:
                        record_handle.close()
                        record_files.append(
                            _normalized_file(root, record_path, record_lines)
                        )
                    record_path = (
                        records_root / f"{record_count // CHUNK_RECORDS:06d}.tsv"
                    )
                    record_handle = record_path.open("wb")
                    record_lines = 0

                refs: list[str] = []
                logical_fields: list[str | EncodedCell] = []
                for group_name, positions in groups:
                    values = [row[position] for position in positions]
                    key = _payload_key(group_name, positions, values)
                    payload_tokens = [_encode_cell(value) for value in values]
                    payload_json = json.dumps(
                        payload_tokens, ensure_ascii=False, separators=(",", ":")
                    )
                    conn.execute(
                        "INSERT OR IGNORE INTO payload VALUES (?, ?, ?, ?)",
                        (
                            group_name,
                            key,
                            hashlib.sha256(payload_json.encode()).hexdigest(),
                            payload_json,
                        ),
                    )
                    refs.append(key)
                    logical_fields.append(key)
                inline_tokens = [_encode_cell(row[position]) for position in inline]
                logical_fields.extend(inline_tokens)
                encoded = _tsv_line(
                    [*refs, *(_cell_field(value) for value in inline_tokens)]
                )
                record_handle.write(encoded)
                codec_sample.add(f"{source.name}:records", logical_fields, encoded)
                record_lines += 1
                record_count += 1
                _update_record_hash(records_digest, row)
                logical_digest.update(b"record\0")
                _update_record_hash(logical_digest, row)

        if record_handle is not None and record_path is not None:
            record_handle.close()
            record_handle = None
            record_files.append(_normalized_file(root, record_path, record_lines))
        conn.commit()
        collision = conn.execute(
            "SELECT group_name, key, COUNT(*) FROM payload "
            "GROUP BY group_name, key HAVING COUNT(*) > 1 LIMIT 1"
        ).fetchone()
        if collision is not None:
            raise SnapshotError(
                f"content-key collision for {source.name}/{collision[0]} key {collision[1]}"
            )

        group_manifests: list[PayloadGroup] = []
        for group_name, positions in groups:
            dictionary_path = dictionaries_root / f"{group_name}.tsv"
            entries = 0
            with dictionary_path.open("wb") as handle:
                for key, payload_json in conn.execute(
                    "SELECT key, payload FROM payload WHERE group_name = ? ORDER BY key",
                    (group_name,),
                ):
                    tokens = tuple(tuple(cell) for cell in json.loads(payload_json))
                    encoded = _tsv_line([key, *(_cell_field(cell) for cell in tokens)])
                    handle.write(encoded)
                    codec_sample.add(
                        f"{source.name}:{group_name}", [key, *tokens], encoded
                    )
                    entries += 1
            group_manifests.append(
                PayloadGroup(
                    name=group_name,
                    positions=positions,
                    dictionary=_normalized_file(root, dictionary_path, entries),
                    entries=entries,
                )
            )

        raw_sha256 = _file_sha256(source)
        final_identity = _file_identity(source.stat())
        if final_identity != source_identity:
            raise SnapshotError(f"source CSV changed during conversion: {source}")
        return SnapshotFile(
            name=source.name,
            required=required,
            present=True,
            raw_size=source_stat.st_size,
            raw_sha256=raw_sha256,
            header=tuple(_encode_cell(value) for value in header),
            column_count=len(header),
            record_count=record_count,
            ordered_records_sha256=records_digest.hexdigest(),
            logical_sha256=logical_digest.hexdigest(),
            groups=tuple(group_manifests),
            inline_positions=inline,
            records=tuple(record_files),
        )
    finally:
        if record_handle is not None:
            record_handle.close()
        conn.close()
        database_path.unlink(missing_ok=True)


def _resolve_from_inventory(inventory_path: Path, value: Path) -> Path:
    if value.is_absolute():
        return value.resolve()
    return (inventory_path.parent / value).resolve()


def load_inventory(path: Path) -> DeliveryInventory:
    try:
        return DeliveryInventory.model_validate_json(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise SnapshotError(f"invalid delivery inventory {path}: {exc}") from exc


def _source_inventory(inventory_path: Path, inventory: DeliveryInventory) -> Path:
    source_dir = _resolve_from_inventory(inventory_path, inventory.source_dir)
    if not source_dir.is_dir():
        raise SnapshotError(f"source_dir is not a directory: {source_dir}")
    declared = {item.name: item for item in inventory.files}
    actual = {
        path.name
        for path in source_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv"
    }
    if unlisted := actual - set(declared):
        raise SnapshotError(
            f"source_dir contains unlisted CSV files: {sorted(unlisted)}"
        )
    if missing := {
        name for name, item in declared.items() if item.required and name not in actual
    }:
        raise SnapshotError(f"required CSV files are missing: {sorted(missing)}")
    value_pair = {"Vardemangder.csv", "VardemangderValidDates.csv"}
    if len(actual.intersection(value_pair)) == 1:
        raise SnapshotError(
            "Vardemangder.csv and VardemangderValidDates.csv must be present or absent together"
        )
    archive_coverage = {
        member for archive in inventory.archives for member in archive.members
    }
    if uncovered := actual - archive_coverage:
        raise SnapshotError(
            f"present CSV files are not covered by a retained archive: {sorted(uncovered)}"
        )
    return source_dir


def _source_bundle_identities(
    source_dir: Path, inventory: DeliveryInventory
) -> dict[str, tuple[int, int, int, int] | None]:
    return {
        item.name: (
            _file_identity(source.stat())
            if (source := source_dir / item.name).is_file()
            else None
        )
        for item in inventory.files
    }


def _verify_source_bundle_unchanged(
    inventory_path: Path,
    inventory: DeliveryInventory,
    source_dir: Path,
    expected: Mapping[str, tuple[int, int, int, int] | None],
) -> None:
    current_source_dir = _source_inventory(inventory_path, inventory)
    current = _source_bundle_identities(current_source_dir, inventory)
    if current_source_dir != source_dir or current != expected:
        changed = sorted(name for name in expected if current[name] != expected[name])
        raise SnapshotError(f"source bundle changed during conversion: {changed}")


def prepare_snapshot(
    inventory_path: Path,
    output: Path,
    *,
    converter_commit: str,
    codec_sample_lines: int = 100_000,
) -> SnapshotStats:
    """Create a new candidate snapshot without overwriting any existing path."""
    started = time.perf_counter()
    inventory_path = inventory_path.resolve()
    output = output.resolve()
    if output.exists():
        raise SnapshotError(
            f"candidate path already exists and will not be overwritten: {output}"
        )
    if not converter_commit or any(char.isspace() for char in converter_commit):
        raise SnapshotError("converter_commit must be one non-empty revision token")
    inventory = load_inventory(inventory_path)
    source_dir = _source_inventory(inventory_path, inventory)
    source_identities = _source_bundle_identities(source_dir, inventory)

    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.candidate-", dir=output.parent)
    )
    codec_sample = _CodecSample(codec_sample_lines)
    try:
        archives: list[ArchiveManifest] = []
        for archive in inventory.archives:
            archive_path = _resolve_from_inventory(inventory_path, archive.path)
            if not archive_path.is_file():
                raise SnapshotError(f"retained archive is missing: {archive_path}")
            actual_hash = _file_sha256(archive_path)
            if archive.sha256 != actual_hash:
                raise SnapshotError(
                    f"retained archive hash mismatch for {archive.locator}: expected {archive.sha256}, got {actual_hash}"
                )
            archives.append(
                ArchiveManifest(
                    locator=archive.locator,
                    size=archive_path.stat().st_size,
                    sha256=actual_hash,
                    members=archive.members,
                )
            )

        files: list[SnapshotFile] = []
        for item in inventory.files:
            source = source_dir / item.name
            if source.is_file():
                files.append(
                    _prepare_file(source, staging, item.required, codec_sample)
                )
            else:
                files.append(
                    SnapshotFile(name=item.name, required=item.required, present=False)
                )

        manifest = SnapshotManifest(
            format="reg-meta-build-input-snapshot",
            schema_version=SNAPSHOT_SCHEMA_VERSION,
            converter_version=CONVERTER_VERSION,
            converter_commit=converter_commit,
            serialization=SERIALIZATION,
            byte_codec="latin-1-passthrough/cp1252-or-base64",
            csv_dialect="pipe-doublequote-quote-notnull",
            null_policy="unquoted-empty-is-null;quoted-empty-is-empty-text",
            dictionary_key="blake2b-128-base64url",
            chunk_records=CHUNK_RECORDS,
            bundle_id=inventory.bundle_id,
            edition=inventory.edition,
            archives=tuple(archives),
            files=tuple(files),
        )
        (staging / MANIFEST_NAME).write_bytes(_manifest_bytes(manifest))
        verify_snapshot(staging)
        _verify_source_bundle_unchanged(
            inventory_path,
            inventory,
            source_dir,
            source_identities,
        )
        normalized_bytes = _snapshot_size(staging, manifest)
        raw_bytes = sum(item.raw_size or 0 for item in files)
        records = sum(item.record_count for item in files)
        staging.replace(output)
        return SnapshotStats(
            raw_bytes=raw_bytes,
            normalized_bytes=normalized_bytes,
            records=records,
            elapsed_seconds=time.perf_counter() - started,
            codec_sample=codec_sample.by_kind,
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def load_manifest(root: Path) -> SnapshotManifest:
    path = root / MANIFEST_NAME
    try:
        return SnapshotManifest.model_validate_json(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise SnapshotError(f"invalid snapshot manifest {path}: {exc}") from exc


def _normalized_files(item: SnapshotFile) -> Iterator[NormalizedFile]:
    yield from (group.dictionary for group in item.groups)
    yield from item.records


def _snapshot_size(root: Path, manifest: SnapshotManifest) -> int:
    return (root / MANIFEST_NAME).stat().st_size + sum(
        normalized.size
        for item in manifest.files
        for normalized in _normalized_files(item)
    )


def _declared_normalized_files(manifest: SnapshotManifest) -> set[str]:
    return {
        normalized.path
        for item in manifest.files
        for normalized in _normalized_files(item)
    }


def _vardemangder_normalized_files(manifest: SnapshotManifest) -> set[str]:
    item = next(
        (item for item in manifest.files if item.name == "Vardemangder.csv"), None
    )
    if item is None or not item.present:
        return set()
    return {normalized.path for normalized in _normalized_files(item)}


def _verify_normalized_inventory(
    root: Path,
    manifest: SnapshotManifest,
    *,
    omitted: Collection[str] = (),
) -> None:
    declared = _declared_normalized_files(manifest)
    omitted_set = set(omitted)
    if not omitted_set <= declared:
        raise SnapshotError(
            f"undeclared normalized artifacts cannot be omitted: {sorted(omitted_set - declared)}"
        )
    expected = declared - omitted_set
    actual = _actual_normalized_files(root)
    if loose_replacements := sorted(omitted_set & actual.keys()):
        raise SnapshotError(
            "normalized artifacts marked omitted by the accepted Git index are "
            "physically present; restore the saved sparse layout or hydrate the "
            f"complete role through Git sparse-checkout: {loose_replacements}"
        )
    if actual.keys() != expected:
        raise SnapshotError(
            "normalized file inventory mismatch; "
            f"missing={sorted(expected - actual.keys())}, "
            f"extra={sorted(actual.keys() - expected)}"
        )

    for item in manifest.files:
        for normalized in _normalized_files(item):
            if normalized.path in omitted_set:
                continue
            if actual[normalized.path] != normalized.size:
                raise SnapshotError(
                    "normalized artifact size mismatch: "
                    f"{normalized.path} "
                    f"(expected {normalized.size}, got {actual[normalized.path]})"
                )


def _actual_normalized_files(root: Path) -> dict[str, int]:
    actual_root = root / "files"
    if not actual_root.exists():
        return {}
    return {
        path.relative_to(root).as_posix(): path.stat().st_size
        for path in actual_root.rglob("*")
        if path.is_file()
    }


def _iter_group_payloads(
    root: Path, group: PayloadGroup, *, exhaustive: bool
) -> Iterator[tuple[str, tuple[RawCell, ...]]]:
    last_key = ""
    lines = 0
    expected = group.dictionary if exhaustive else None
    for line_number, fields in _iter_tsv(root / group.dictionary.path, expected):
        if len(fields) != len(group.positions) + 1:
            raise SnapshotError(
                f"{group.dictionary.path}: line {line_number} has the wrong field count"
            )
        key = fields[0]
        if not _KEY_RE.fullmatch(key) or key <= last_key:
            raise SnapshotError(
                f"{group.dictionary.path}: keys are invalid or not strictly sorted"
            )
        cells = tuple(_decode_cell(_field_cell(value)) for value in fields[1:])
        if exhaustive and _payload_key(group.name, group.positions, cells) != key:
            raise SnapshotError(
                f"{group.dictionary.path}: line {line_number} has a false content key"
            )
        yield key, cells
        last_key = key
        lines += 1
    if lines != group.entries or lines != group.dictionary.lines:
        raise SnapshotError(f"dictionary line count mismatch: {group.dictionary.path}")


def _load_groups(
    root: Path, item: SnapshotFile, *, exhaustive: bool
) -> list[dict[str, tuple[RawCell, ...]]]:
    return [
        dict(_iter_group_payloads(root, group, exhaustive=exhaustive))
        for group in item.groups
    ]


def _iter_snapshot_record_fields(
    root: Path, item: SnapshotFile, *, exhaustive: bool
) -> Iterator[tuple[str, int, list[str]]]:
    expected_fields = len(item.groups) + len(item.inline_positions)
    seen = 0
    for record_file in item.records:
        lines = 0
        expected = record_file if exhaustive else None
        for line_number, fields in _iter_tsv(root / record_file.path, expected):
            if len(fields) != expected_fields:
                raise SnapshotError(
                    f"{record_file.path}: line {line_number} has the wrong field count"
                )
            yield record_file.path, line_number, fields
            lines += 1
            seen += 1
        if lines != record_file.lines:
            raise SnapshotError(f"record line count mismatch: {record_file.path}")
    if seen != item.record_count:
        raise SnapshotError(
            f"record count mismatch for {item.name}: expected {item.record_count}, got {seen}"
        )


def _iter_snapshot_rows(
    root: Path,
    item: SnapshotFile,
    dictionaries: Sequence[Mapping[str, tuple[RawCell, ...]]],
    *,
    exhaustive: bool,
) -> Iterator[list[RawCell]]:
    for record_path, line_number, fields in _iter_snapshot_record_fields(
        root, item, exhaustive=exhaustive
    ):
        row: list[RawCell | object] = [_UNSET] * item.column_count
        for index, group in enumerate(item.groups):
            key = fields[index]
            if not _KEY_RE.fullmatch(key):
                raise SnapshotError(
                    f"{record_path}: line {line_number} has an invalid group key"
                )
            try:
                values = dictionaries[index][key]
            except KeyError as exc:
                raise SnapshotError(
                    f"{record_path}: line {line_number} references missing {group.name} payload {key}"
                ) from exc
            for position, value in zip(group.positions, values, strict=True):
                row[position] = value
        offset = len(item.groups)
        for position, value in zip(item.inline_positions, fields[offset:], strict=True):
            row[position] = _decode_field(value)
        if any(value is _UNSET for value in row):
            raise SnapshotError(
                f"{record_path}: line {line_number} did not fill every column"
            )
        yield cast("list[RawCell]", row)


def _logical_hashes(
    header: Sequence[RawCell], rows: Iterator[Sequence[RawCell]]
) -> tuple[str, str, int]:
    records_digest = hashlib.sha256()
    logical_digest = hashlib.sha256()
    logical_digest.update(b"header\0")
    _update_record_hash(logical_digest, header)
    count = 0
    for row in rows:
        _update_record_hash(records_digest, row)
        logical_digest.update(b"record\0")
        _update_record_hash(logical_digest, row)
        count += 1
    return records_digest.hexdigest(), logical_digest.hexdigest(), count


@contextmanager
def _open_snapshot_rows(
    root: Path, item: SnapshotFile
) -> Iterator[tuple[list[RawCell], Iterator[list[RawCell]]]]:
    """Open one normalized CSV and validate its bytes and logical stream at EOF."""
    dictionaries = _load_groups(root, item, exhaustive=True)
    header = [_decode_cell(cell) for cell in item.header]
    records_digest = hashlib.sha256()
    logical_digest = hashlib.sha256()
    logical_digest.update(b"header\0")
    _update_record_hash(logical_digest, header)
    count = 0
    complete = False

    def checked_rows() -> Iterator[list[RawCell]]:
        nonlocal complete, count
        for row in _iter_snapshot_rows(root, item, dictionaries, exhaustive=True):
            _update_record_hash(records_digest, row)
            logical_digest.update(b"record\0")
            _update_record_hash(logical_digest, row)
            count += 1
            yield row
        if count != item.record_count:
            raise SnapshotError(f"record count mismatch for {item.name}")
        if (
            records_digest.hexdigest() != item.ordered_records_sha256
            or logical_digest.hexdigest() != item.logical_sha256
        ):
            raise SnapshotError(f"logical record round-trip mismatch for {item.name}")
        complete = True

    yield header, checked_rows()
    if not complete:
        raise SnapshotError(f"snapshot row stream was not fully consumed: {item.name}")


@contextmanager
def _open_prepared_rows(
    root: Path, item: SnapshotFile
) -> Iterator[tuple[list[RawCell], Iterator[list[RawCell]]]]:
    """Open accepted rows with structural checks but no exhaustive re-hashing."""
    dictionaries = _load_groups(root, item, exhaustive=False)
    header = [_decode_cell(cell) for cell in item.header]
    complete = False

    def checked_rows() -> Iterator[list[RawCell]]:
        nonlocal complete
        yield from _iter_snapshot_rows(root, item, dictionaries, exhaustive=False)
        complete = True

    yield header, checked_rows()
    if not complete:
        raise SnapshotError(f"snapshot row stream was not fully consumed: {item.name}")


@dataclass(frozen=True)
class PreparedVardemangder:
    """Prepared value payloads plus ordered CVID/ItemId occurrence references."""

    header: tuple[RawCell, ...]
    _root: Path
    _item: SnapshotFile
    _value_set_group: PayloadGroup
    _value_group: PayloadGroup
    _value_set_index: int
    _value_index: int

    def value_sets(self) -> Iterator[tuple[str, tuple[RawCell, ...]]]:
        yield from _iter_group_payloads(
            self._root, self._value_set_group, exhaustive=False
        )

    def values(self) -> Iterator[tuple[str, tuple[RawCell, ...]]]:
        yield from _iter_group_payloads(self._root, self._value_group, exhaustive=False)

    def occurrences(
        self,
        *,
        value_set_keys: Collection[str],
        value_keys: Collection[str],
    ) -> Iterator[tuple[RawCell, RawCell, str, str]]:
        """Yield ``(CVID, ItemId, value-set key, value key)`` in source order."""
        for record_path, line_number, fields in _iter_snapshot_record_fields(
            self._root, self._item, exhaustive=False
        ):
            value_set_key = fields[self._value_set_index]
            value_key = fields[self._value_index]
            for group_name, key, payloads in (
                ("value_set", value_set_key, value_set_keys),
                ("value", value_key, value_keys),
            ):
                if key not in payloads:
                    if not _KEY_RE.fullmatch(key):
                        raise SnapshotError(
                            f"{record_path}: line {line_number} has an invalid {group_name} key"
                        )
                    raise SnapshotError(
                        f"{record_path}: line {line_number} references missing {group_name} payload {key}"
                    )
            offset = len(self._item.groups)
            yield (
                _decode_field(fields[offset]),
                _decode_field(fields[offset + 1]),
                value_set_key,
                value_key,
            )


class ScbSnapshotReader:
    """Quick, streamed access to one accepted SCB input snapshot."""

    def __init__(
        self,
        root: Path,
        manifest: SnapshotManifest,
        *,
        repository: Path,
        input_commit: str,
        snapshot_path: str,
        manifest_sha256: str,
        omitted: Collection[str] = (),
    ) -> None:
        self.root = root
        self.repository = repository
        self.manifest = manifest
        self._files = {item.name: item for item in manifest.files}
        self.provenance = {
            "input_repository_commit": input_commit,
            "snapshot_path": snapshot_path,
            "manifest_sha256": manifest_sha256,
        }
        self._omitted = frozenset(omitted)
        _verify_normalized_inventory(root, manifest, omitted=self._omitted)

    @property
    def vardemangder_materialized(self) -> bool:
        return not self._omitted

    def materialization_error(self) -> SnapshotMaterializationError:
        return SnapshotMaterializationError(
            self.repository,
            self.provenance["input_repository_commit"],
            _snapshot_repo_path(
                self.provenance["snapshot_path"], "files/Vardemangder.csv"
            ),
        )

    def require_vardemangder_materialized(self) -> None:
        if not self.vardemangder_materialized:
            raise self.materialization_error()

    def has_file(self, name: str) -> bool:
        item = self._files.get(name)
        return item is not None and item.present

    def raw_sha256(self, name: str) -> str:
        item = self._files.get(name)
        if item is None or not item.present or item.raw_sha256 is None:
            raise SnapshotError(f"snapshot source file is absent: {name}")
        return item.raw_sha256

    @contextmanager
    def open_csv(
        self, name: str
    ) -> Iterator[tuple[list[RawCell], Iterator[list[RawCell]]]]:
        item = self._files.get(name)
        if item is None or not item.present:
            raise SnapshotError(f"snapshot source file is absent: {name}")
        with _open_prepared_rows(self.root, item) as opened:
            yield opened

    def open_vardemangder(self) -> PreparedVardemangder:
        """Load prepared dictionaries without expanding value occurrences."""
        self.require_vardemangder_materialized()
        item = self._files.get("Vardemangder.csv")
        if item is None or not item.present:
            raise SnapshotError("snapshot source file is absent: Vardemangder.csv")
        layout = {group.name: group.positions for group in item.groups}
        if (
            layout != {"value_set": (0, 1), "value": (2, 3)}
            or item.inline_positions != (4, 5)
            or item.column_count != 6
        ):
            raise SnapshotError(
                "Vardemangder.csv has an unsupported prepared layout; "
                "prepare, verify, and accept a snapshot with the current converter"
            )
        indexes = {group.name: index for index, group in enumerate(item.groups)}
        return PreparedVardemangder(
            header=tuple(_decode_cell(cell) for cell in item.header),
            _root=self.root,
            _item=item,
            _value_set_group=item.groups[indexes["value_set"]],
            _value_group=item.groups[indexes["value"]],
            _value_set_index=indexes["value_set"],
            _value_index=indexes["value"],
        )


def _write_csv(
    path: Path, header: Sequence[RawCell], rows: Iterator[Sequence[RawCell]]
) -> None:
    with path.open("wb") as raw_handle:
        text_handle = io.TextIOWrapper(raw_handle, encoding="latin-1", newline="")
        writer = csv.writer(
            text_handle,
            delimiter="|",
            quotechar='"',
            quoting=csv.QUOTE_NOTNULL,
            lineterminator="\r\n",
        )
        writer.writerow(header)
        writer.writerows(rows)
        text_handle.flush()


def _inspect_snapshot(
    root: Path,
    restore_dir: Path | None = None,
    *,
    manifest: SnapshotManifest | None = None,
) -> SnapshotManifest:
    if manifest is None:
        manifest = load_manifest(root)
    _verify_normalized_inventory(root, manifest)
    for item in manifest.files:
        if not item.present:
            continue
        with _open_snapshot_rows(root, item) as (header, rows):
            if restore_dir is None:
                for _row in rows:
                    pass
            else:
                restored = restore_dir / item.name
                _write_csv(restored, header, rows)
                with open_lossless_csv(restored) as (
                    restored_header,
                    restored_rows,
                ):
                    records_hash, logical_hash, count = _logical_hashes(
                        restored_header, restored_rows
                    )
                if count != item.record_count:
                    raise SnapshotError(f"record count mismatch for {item.name}")
                if (
                    records_hash != item.ordered_records_sha256
                    or logical_hash != item.logical_sha256
                ):
                    raise SnapshotError(
                        f"logical record round-trip mismatch for {item.name}"
                    )
    return manifest


def _accepted_snapshot_materialization(
    root: Path, manifest: SnapshotManifest
) -> tuple[Path, str, str, set[str]] | None:
    """Inspect accepted Git state before status; return None for untracked candidates."""
    try:
        repo = Path(_git(root, "rev-parse", "--show-toplevel")).resolve()
        snapshot_path = root.relative_to(repo).as_posix()
        commit = _git(repo, "rev-parse", "HEAD")
    except (
        SnapshotError,
        ValueError,
    ):
        return None
    manifest_repo_path = _snapshot_repo_path(snapshot_path, MANIFEST_NAME)
    if not _git_bytes(repo, "ls-tree", "-z", commit, "--", manifest_repo_path):
        return None
    committed_manifest, _manifest_sha256 = _read_committed_manifest(
        repo, commit, snapshot_path
    )
    _verify_committed_inventory(repo, commit, snapshot_path, committed_manifest)
    omitted = _verify_git_snapshot_materialization(
        repo, commit, snapshot_path, committed_manifest
    )
    clean_commit = clean_git_commit(repo)
    if clean_commit != commit:
        raise SnapshotError(
            "accepted input commit changed while its materialization was checked: "
            f"expected {commit}, got {clean_commit}"
        )
    if committed_manifest != manifest:
        raise SnapshotError("worktree snapshot manifest differs from pinned commit")
    return repo, commit, snapshot_path, omitted


def _require_complete_normalized_inventory(
    root: Path, manifest: SnapshotManifest
) -> None:
    accepted = _accepted_snapshot_materialization(root, manifest)
    if accepted is None:
        _verify_normalized_inventory(root, manifest)
        return
    repo, commit, snapshot_path, omitted = accepted
    _verify_normalized_inventory(root, manifest, omitted=omitted)
    if omitted:
        raise SnapshotMaterializationError(
            repo,
            commit,
            _snapshot_repo_path(snapshot_path, "files/Vardemangder.csv"),
        )


def verify_snapshot(root: Path) -> SnapshotManifest:
    """Verify normalized hashes, dictionary closure, and ordered logical records."""
    root = root.resolve()
    manifest = load_manifest(root)
    _require_complete_normalized_inventory(root, manifest)
    return _inspect_snapshot(root, manifest=manifest)


def restore_snapshot(root: Path, output: Path) -> SnapshotStats:
    """Reconstruct CSVs into a new directory, atomically on complete success."""
    started = time.perf_counter()
    root = root.resolve()
    output = output.resolve()
    if output == root or root in output.parents:
        raise SnapshotError("restore target must be outside the snapshot root")
    manifest = load_manifest(root)
    _require_complete_normalized_inventory(root, manifest)
    if output.exists():
        raise SnapshotError(
            f"restore target already exists and will not be overwritten: {output}"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.restore-", dir=output.parent)
    )
    try:
        manifest = _inspect_snapshot(root, staging, manifest=manifest)
        staging.replace(output)
        return SnapshotStats(
            raw_bytes=sum(item.raw_size or 0 for item in manifest.files),
            normalized_bytes=_snapshot_size(root, manifest),
            records=sum(item.record_count for item in manifest.files),
            elapsed_seconds=time.perf_counter() - started,
            codec_sample={},
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _file_identity(stat: os.stat_result) -> tuple[int, int, int, int]:
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)


def _sample_ordinals(total_records: int, limit: int) -> Iterator[int]:
    """Yield fixed source ordinals spanning the whole stream, including its ends."""
    if total_records <= limit:
        yield from range(total_records)
    elif limit == 1:
        yield total_records // 2
    else:
        for sample_index in range(limit):
            yield sample_index * (total_records - 1) // (limit - 1)


def _sample_rows(
    rows: Iterator[list[RawCell]], total_records: int, limit: int
) -> Iterator[list[RawCell]]:
    targets = iter(_sample_ordinals(total_records, limit))
    target = next(targets, None)
    for ordinal, row in enumerate(rows):
        if target is not None and ordinal == target:
            yield row
            target = next(targets, None)


def _ordered_normalized_rows(
    root: Path,
    normalized_files: Sequence[NormalizedFile],
    field_count: int,
) -> Iterator[tuple[int | str, ...]]:
    ordinal = 0
    for normalized in normalized_files:
        for line_number, fields in _iter_tsv(root / normalized.path):
            if len(fields) != field_count:
                raise SnapshotError(
                    f"{normalized.path}: line {line_number} has the wrong field count"
                )
            yield (ordinal, *fields)
            ordinal += 1


def _sqlite_normalized_tables_control(
    root: Path,
    files: Sequence[SnapshotFile],
    database: Path,
) -> dict[str, Any]:
    logical_tables: list[tuple[str, tuple[NormalizedFile, ...], int]] = []
    for item in files:
        for group in item.groups:
            logical_tables.append(
                (
                    f"{item.name}:{group.name}",
                    (group.dictionary,),
                    len(group.positions) + 1,
                )
            )
        logical_tables.append(
            (
                f"{item.name}:records",
                item.records,
                len(item.groups) + len(item.inline_positions),
            )
        )

    table_rows: dict[str, int] = {}
    table_field_counts: dict[str, int] = {}
    conn = sqlite3.connect(database)
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        for table_index, (logical_name, normalized_files, field_count) in enumerate(
            logical_tables
        ):
            table_name = f"normalized_{table_index:04d}"
            columns = ", ".join(
                f"field_{field_index:04d} TEXT NOT NULL"
                for field_index in range(field_count)
            )
            conn.execute(
                f"CREATE TABLE {table_name} "
                f"(occurrence_ordinal INTEGER PRIMARY KEY, {columns})"
            )
            placeholders = ", ".join("?" for _index in range(field_count + 1))
            conn.executemany(
                f"INSERT INTO {table_name} VALUES ({placeholders})",
                _ordered_normalized_rows(root, normalized_files, field_count),
            )
            table_rows[logical_name] = conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
            table_field_counts[logical_name] = field_count
        conn.commit()
    finally:
        conn.close()
    return {
        "bytes": database.stat().st_size,
        "logical_tables": len(table_rows),
        "logical_rows": sum(table_rows.values()),
        "table_rows": table_rows,
        "table_field_counts": table_field_counts,
        "ordering": "zero-based occurrence ordinal per logical table",
    }


def measure_codec_sample(
    inventory_path: Path,
    *,
    limits: Mapping[str, int] | None = None,
    codec_sample_lines: int = 100_000,
) -> dict[str, Any]:
    """Compare codecs on bounded, evenly spaced records across each source stream."""
    started = time.perf_counter()
    inventory_path = inventory_path.resolve()
    inventory = load_inventory(inventory_path)
    source_dir = _source_inventory(inventory_path, inventory)
    default_limits = {
        "Registerinformation.csv": 100_000,
        "Vardemangder.csv": 1_000_000,
    }
    selected_limits = {**default_limits, **(limits or {})}
    if any(limit < 1 for limit in selected_limits.values()):
        raise SnapshotError("codec sample limits must be positive")
    codec_sample = _CodecSample(codec_sample_lines)
    with tempfile.TemporaryDirectory(prefix="regmeta-input-codec-") as temporary:
        workspace = Path(temporary)
        source_root = workspace / "source"
        normalized_root = workspace / "normalized"
        source_root.mkdir()
        population_records: dict[str, int] = {}
        sample_records: dict[str, int] = {}
        prepared_files: list[SnapshotFile] = []
        file_limits: dict[str, int] = {}
        raw_bytes = 0
        for item in inventory.files:
            source = source_dir / item.name
            if not source.is_file():
                continue
            limit = selected_limits.get(item.name, 100_000)
            file_limits[item.name] = limit
            source_identity = _file_identity(source.stat())
            with open_lossless_csv(source) as (header, rows):
                population_records[item.name] = sum(1 for _row in rows)
            sample = source_root / item.name
            with open_lossless_csv(source) as (header, rows):
                _write_csv(
                    sample,
                    header,
                    _sample_rows(rows, population_records[item.name], limit),
                )
            if _file_identity(source.stat()) != source_identity:
                raise SnapshotError(
                    f"source CSV changed during codec sampling: {source}"
                )
            prepared = _prepare_file(
                sample, normalized_root, item.required, codec_sample
            )
            prepared_files.append(prepared)
            sample_records[item.name] = prepared.record_count
            raw_bytes += sample.stat().st_size
        sqlite_control = _sqlite_normalized_tables_control(
            normalized_root,
            prepared_files,
            workspace / "normalized-tables-control.sqlite",
        )
        return {
            "sampling": {
                "method": "evenly-spaced-source-ordinals-v1",
                "source_passes": 2,
            },
            "limits": file_limits,
            "population_records": population_records,
            "sample_records": sample_records,
            "logical_sample_csv_bytes": raw_bytes,
            "normalized_sample_bytes": tree_size(normalized_root),
            "sqlite_normalized_tables_control": sqlite_control,
            "codec_sample": codec_sample.by_kind,
            "elapsed_seconds": time.perf_counter() - started,
        }


def _git_bytes(repo: Path, *args: str, input_bytes: bytes | None = None) -> bytes:
    try:
        # Git read commands may otherwise refresh accepted index evidence on disk.
        process = subprocess.run(
            ["git", "-C", str(repo), *args],
            input=input_bytes,
            check=True,
            capture_output=True,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = (
            exc.stderr.decode("utf-8", errors="replace").strip()
            if isinstance(exc, subprocess.CalledProcessError)
            else str(exc)
        )
        raise SnapshotError(f"git {' '.join(args)} failed in {repo}: {detail}") from exc
    return process.stdout


def _git(repo: Path, *args: str) -> str:
    return _git_bytes(repo, *args).decode("utf-8").strip()


def _git_config_bool(repo: Path, name: str) -> bool:
    return (
        _git(
            repo,
            "config",
            "--type=bool",
            "--default=false",
            "--get",
            name,
        )
        == "true"
    )


def clean_git_commit(repo: Path) -> str:
    repo = repo.resolve()
    status = _git(repo, "status", "--porcelain", "--untracked-files=all")
    if status:
        raise SnapshotError(f"Git repository must be clean for an exact pin: {repo}")
    return _git(repo, "rev-parse", "HEAD")


def input_bundle_repository(path: Path) -> Path:
    """Resolve the Git repository containing a selected catalog bundle path."""
    root = path.expanduser().resolve()
    if not root.is_dir():
        raise SnapshotError(f"catalog input bundle directory not found: {root}")
    return Path(_git(root, "rev-parse", "--show-toplevel")).resolve()


def _tracked_source_commit(
    source_paths: Sequence[Path], *, identity: str
) -> tuple[Path, str]:
    sources = tuple(source.resolve() for source in source_paths)
    if any(not source.is_file() for source in sources):
        raise SnapshotError(f"{identity} source files must exist")
    repositories = tuple(
        Path(_git(source.parent, "rev-parse", "--show-toplevel")).resolve()
        for source in sources
    )
    if len(set(repositories)) != 1:
        raise SnapshotError(f"{identity} sources must come from the same Git checkout")
    repo = repositories[0]
    commit = clean_git_commit(repo)
    for source in sources:
        relative = source.relative_to(repo).as_posix()
        try:
            committed_blob = _git(repo, "rev-parse", "--verify", f"{commit}:{relative}")
        except SnapshotError as exc:
            raise SnapshotError(
                f"{identity} source is not a tracked HEAD blob: {relative}"
            ) from exc
        if _git(repo, "cat-file", "-t", committed_blob) != "blob":
            raise SnapshotError(f"{identity} source is not a Git blob: {relative}")
        worktree_blob = _git(repo, "hash-object", "--no-filters", relative)
        if worktree_blob != committed_blob:
            raise SnapshotError(
                f"{identity} source differs from its HEAD blob: {relative}"
            )
    return repo, commit


def converter_source_commit(cli_path: Path) -> str:
    """Return the commit containing the clean CLI and imported converter sources."""
    _repo, commit = _tracked_source_commit(
        (cli_path, Path(__file__)), identity="converter"
    )
    return commit


def _builder_source_identity() -> tuple[Path, str]:
    module_path = Path(__file__).resolve()
    repo = Path(_git(module_path.parent, "rev-parse", "--show-toplevel")).resolve()
    return _tracked_source_commit(
        (
            module_path,
            module_path.with_name("cli.py"),
            repo / "scripts" / "prototype_scb_inputs.py",
            repo / "scripts" / "build_db_watch.py",
            repo / "uv.lock",
        ),
        identity="builder",
    )


def _snapshot_repo_path(snapshot_path: str, relative_path: str) -> str:
    if snapshot_path == ".":
        return relative_path
    return f"{snapshot_path}/{relative_path}"


def _read_committed_manifest(
    repo: Path, commit: str, snapshot_path: str
) -> tuple[SnapshotManifest, str]:
    manifest_repo_path = _snapshot_repo_path(snapshot_path, MANIFEST_NAME)
    manifest_bytes = _git_bytes(
        repo, "cat-file", "blob", f"{commit}:{manifest_repo_path}"
    )
    try:
        manifest = SnapshotManifest.model_validate_json(manifest_bytes)
    except ValueError as exc:
        raise SnapshotError(
            f"invalid snapshot manifest in pinned commit: {exc}"
        ) from exc
    return manifest, hashlib.sha256(manifest_bytes).hexdigest()


def _committed_inventory_sizes(
    repo: Path,
    commit: str,
    base_path: str,
    roots: Sequence[str],
    *,
    context: str,
) -> dict[str, int]:
    tree = _git_bytes(repo, "ls-tree", "-r", "-l", "-z", commit, "--", *roots)
    actual: dict[str, int] = {}
    prefix = f"{base_path}/" if base_path != "." else ""
    for entry in tree.split(b"\0"):
        if not entry:
            continue
        try:
            metadata, raw_path = entry.split(b"\t", 1)
            _mode, object_type, _object_id, raw_size = metadata.split(b" ", 3)
            path = raw_path.decode("utf-8")
            size = int(raw_size)
        except (UnicodeDecodeError, ValueError) as exc:
            raise SnapshotError(f"invalid Git tree entry in {context}") from exc
        if object_type != b"blob" or not path.startswith(prefix):
            raise SnapshotError(f"invalid Git object in {context}: {path}")
        actual[path.removeprefix(prefix)] = size
    return actual


def _verify_committed_inventory(
    repo: Path, commit: str, snapshot_path: str, manifest: SnapshotManifest
) -> None:
    actual = _committed_inventory_sizes(
        repo,
        commit,
        snapshot_path,
        (_snapshot_repo_path(snapshot_path, "files"),),
        context="snapshot inventory",
    )

    expected = {
        normalized.path: normalized.size
        for item in manifest.files
        for normalized in _normalized_files(item)
    }
    if actual.keys() != expected.keys():
        missing = sorted(expected.keys() - actual.keys())
        extra = sorted(actual.keys() - expected.keys())
        details = []
        if missing:
            details.append(f"missing from pinned commit={missing}")
        if extra:
            details.append(f"extra in pinned commit={extra}")
        raise SnapshotError(
            "normalized file inventory mismatch in pinned commit; " + ", ".join(details)
        )
    if mismatched := sorted(
        path for path, size in expected.items() if actual[path] != size
    ):
        raise SnapshotError(
            f"normalized artifact size mismatch in pinned commit: {mismatched}"
        )


def _committed_tree_paths(repo: Path, commit: str) -> set[str]:
    tree = _git_bytes(repo, "ls-tree", "-r", "-z", commit)
    paths: set[str] = set()
    for entry in tree.split(b"\0"):
        if not entry:
            continue
        try:
            metadata, raw_path = entry.split(b"\t", 1)
            _mode, object_type, _object_id = metadata.split(b" ", 2)
            path = raw_path.decode("utf-8")
        except (UnicodeDecodeError, ValueError) as exc:
            raise SnapshotError("invalid Git tree entry in accepted input") from exc
        if object_type != b"blob":
            raise SnapshotError(f"non-file object in accepted input: {path}")
        paths.add(path)
    return paths


def _index_tags(repo: Path) -> dict[str, str]:
    # Active sparse handling reports a stored S bit as H when its path is present.
    # The command-local override exposes the raw bit without changing checkout config.
    tagged = _git_bytes(
        repo,
        "-c",
        "core.sparseCheckout=false",
        "ls-files",
        "--sparse",
        "-v",
        "--stage",
        "-z",
    )
    result: dict[str, str] = {}
    for entry in tagged.split(b"\0"):
        if not entry:
            continue
        tagged_metadata, separator, raw_path = entry.partition(b"\t")
        if not separator or len(tagged_metadata) < 3 or tagged_metadata[1:2] != b" ":
            raise SnapshotError("invalid Git index entry in accepted input")
        raw_tag = tagged_metadata[:1]
        metadata = tagged_metadata[2:]
        fields = metadata.split(b" ")
        if len(fields) != 3:
            raise SnapshotError("invalid staged Git index entry in accepted input")
        mode, _object_id, stage = fields
        if mode == b"040000":
            path = raw_path.decode("utf-8", errors="replace")
            raise SnapshotError(
                "accepted input uses a sparse Git index entry at "
                f"{path}; reapply the operator selection with --no-sparse-index"
            )
        if stage != b"0":
            path = raw_path.decode("utf-8", errors="replace")
            raise SnapshotError(
                f"accepted input has an unmerged Git index entry: {path}"
            )
        try:
            tag = raw_tag.decode("ascii")
            path = raw_path.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SnapshotError("invalid Git index path in accepted input") from exc
        if path in result:
            raise SnapshotError(f"duplicate Git index entry in accepted input: {path}")
        result[path] = tag
    return result


def _verify_git_snapshot_materialization(
    repo: Path,
    commit: str,
    snapshot_path: str,
    manifest: SnapshotManifest,
) -> set[str]:
    """Return the one cold role omitted by a valid full-index cone checkout."""
    committed_paths = _committed_tree_paths(repo, commit)
    index_tags = _index_tags(repo)
    if index_tags.keys() != committed_paths:
        raise SnapshotError(
            "accepted input Git index differs from the pinned commit; "
            f"missing={sorted(committed_paths - index_tags.keys())}, "
            f"extra={sorted(index_tags.keys() - committed_paths)}"
        )
    if invalid := sorted(
        path for path, tag in index_tags.items() if tag not in {"H", "S"}
    ):
        raise SnapshotError(
            "accepted input has assume-unchanged or unsupported Git index flags: "
            f"{invalid}"
        )

    sparse_checkout = _git_config_bool(repo, "core.sparseCheckout")
    if sparse_checkout and (
        not _git_config_bool(repo, "core.sparseCheckoutCone")
        or _git_config_bool(repo, "index.sparse")
    ):
        raise SnapshotError(
            "accepted input sparse selection requires cone mode and a normal full "
            "Git index (--no-sparse-index)"
        )

    cold = _vardemangder_normalized_files(manifest)
    cold_repo_paths = {
        _snapshot_repo_path(snapshot_path, relative) for relative in cold
    }
    unexpected_skips = sorted(
        path
        for path, tag in index_tags.items()
        if tag == "S" and path not in cold_repo_paths
    )
    if unexpected_skips:
        raise SnapshotError(
            "accepted input sparse selection omits files outside the SCB "
            f"Vardemangder role: {unexpected_skips}"
        )
    cold_tags = {index_tags[path] for path in cold_repo_paths}
    if not cold_tags or cold_tags == {"H"}:
        omitted: set[str] = set()
    elif cold_tags == {"S"}:
        omitted = cold
    else:
        raise SnapshotError(
            "accepted input must materialize or omit the complete SCB "
            "Vardemangder role; partial Git index selection is unsupported"
        )

    if omitted and not sparse_checkout:
        raise SnapshotError(
            "the SCB Vardemangder role may be omitted only by cone-mode Git "
            "sparse-checkout with --no-sparse-index"
        )
    if sparse_checkout:
        rule_input = b"\0".join(
            path.encode("utf-8") for path in sorted(committed_paths)
        )
        if rule_input:
            rule_input += b"\0"
        selected = {
            path.decode("utf-8")
            for path in _git_bytes(
                repo,
                "sparse-checkout",
                "check-rules",
                "-z",
                input_bytes=rule_input,
            ).split(b"\0")
            if path
        }
        expected_selected = committed_paths - (cold_repo_paths if omitted else set())
        if selected != expected_selected:
            raise SnapshotError(
                "accepted input sparse rules must exclude exactly the complete SCB "
                "Vardemangder role; "
                f"unexpectedly omitted={sorted(expected_selected - selected)}, "
                f"unexpectedly selected={sorted(selected - expected_selected)}"
            )
    return omitted


def _read_git_blob(
    stdin: IO[bytes], stdout: IO[bytes], object_spec: str, *, retain: bool = False
) -> tuple[int, str, bytes | None]:
    stdin.write(object_spec.encode("utf-8") + b"\n")
    stdin.flush()
    response = stdout.readline()
    if response.endswith(b" missing\n"):
        path = object_spec.partition(":")[2]
        raise SnapshotError(f"snapshot artifact is missing from pinned commit: {path}")
    parts = response.rstrip(b"\n").split()
    if len(parts) != 3 or parts[1] != b"blob":
        raise SnapshotError(f"invalid Git blob response for {object_spec!r}")
    try:
        size = int(parts[2])
    except ValueError as exc:
        raise SnapshotError(f"invalid Git blob size for {object_spec!r}") from exc
    remaining = size
    digest = hashlib.sha256()
    payload = bytearray() if retain else None
    while remaining:
        chunk = stdout.read(min(remaining, 1024 * 1024))
        if not chunk:
            raise SnapshotError(f"truncated Git blob for {object_spec!r}")
        digest.update(chunk)
        if payload is not None:
            payload.extend(chunk)
        remaining -= len(chunk)
    if stdout.read(1) != b"\n":
        raise SnapshotError(f"invalid Git blob terminator for {object_spec!r}")
    return size, digest.hexdigest(), bytes(payload) if payload is not None else None


def _verify_committed_snapshot(
    repo: Path, commit: str, snapshot_path: str
) -> tuple[SnapshotManifest, str]:
    try:
        with subprocess.Popen(
            ["git", "-C", str(repo), "cat-file", "--batch"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as process:
            assert process.stdin is not None
            assert process.stdout is not None
            assert process.stderr is not None
            manifest_repo_path = _snapshot_repo_path(snapshot_path, MANIFEST_NAME)
            _, manifest_sha256, manifest_bytes = _read_git_blob(
                process.stdin,
                process.stdout,
                f"{commit}:{manifest_repo_path}",
                retain=True,
            )
            assert manifest_bytes is not None
            try:
                manifest = SnapshotManifest.model_validate_json(manifest_bytes)
            except ValueError as exc:
                raise SnapshotError(
                    f"invalid snapshot manifest in pinned commit: {exc}"
                ) from exc
            for item in manifest.files:
                for normalized in _normalized_files(item):
                    repo_path = _snapshot_repo_path(snapshot_path, normalized.path)
                    size, sha256, _ = _read_git_blob(
                        process.stdin, process.stdout, f"{commit}:{repo_path}"
                    )
                    if size != normalized.size or sha256 != normalized.sha256:
                        raise SnapshotError(
                            "normalized artifact hash/size mismatch in pinned commit: "
                            f"{normalized.path}"
                        )
            process.stdin.close()
            stderr = process.stderr.read().decode("utf-8", errors="replace").strip()
            return_code = process.wait()
            if return_code:
                raise SnapshotError(
                    f"git cat-file --batch failed in {repo}: {stderr or return_code}"
                )
            return manifest, manifest_sha256
    except OSError as exc:
        raise SnapshotError(f"cannot inspect pinned input commit: {exc}") from exc


def open_scb_snapshot(selection: ScbSnapshotSelection) -> ScbSnapshotReader:
    """Quick-check a pinned accepted snapshot and return its streamed reader.

    The commit and manifest pins are the maintainer's acceptance declaration. This
    ordinary-use boundary checks identity, supported versions, clean checkout, and
    declared inventory/size. ``verify_snapshot`` remains the explicit exhaustive
    artifact, dictionary, and logical-record proof.
    """
    if not _GIT_COMMIT_RE.fullmatch(selection.input_commit):
        raise SnapshotError(
            "SCB input commit must be a full 40-character lowercase Git commit"
        )
    if not _HASH_RE.fullmatch(selection.manifest_sha256):
        raise SnapshotError(
            "SCB manifest SHA-256 must be 64 lowercase hexadecimal characters"
        )
    root = selection.path.expanduser().resolve()
    if not root.is_dir():
        raise SnapshotError(f"SCB snapshot directory not found: {root}")
    repo = Path(_git(root, "rev-parse", "--show-toplevel")).resolve()
    try:
        snapshot_path = root.relative_to(repo).as_posix()
    except ValueError as exc:
        raise SnapshotError(
            f"SCB snapshot is outside its Git repository: {root}"
        ) from exc
    actual_commit = _git(repo, "rev-parse", "HEAD")
    if actual_commit != selection.input_commit:
        raise SnapshotError(
            "SCB input commit pin mismatch: expected "
            f"{selection.input_commit}, got {actual_commit}"
        )
    committed_manifest, committed_manifest_sha256 = _read_committed_manifest(
        repo, selection.input_commit, snapshot_path
    )
    if committed_manifest_sha256 != selection.manifest_sha256:
        raise SnapshotError(
            "SCB snapshot manifest pin mismatch: expected "
            f"{selection.manifest_sha256}, got {committed_manifest_sha256}"
        )
    _verify_committed_inventory(
        repo, selection.input_commit, snapshot_path, committed_manifest
    )
    omitted = _verify_git_snapshot_materialization(
        repo, selection.input_commit, snapshot_path, committed_manifest
    )
    actual_commit = clean_git_commit(repo)
    if actual_commit != selection.input_commit:
        raise SnapshotError(
            "SCB input commit changed while its materialization was checked: expected "
            f"{selection.input_commit}, got {actual_commit}"
        )
    manifest_path = root / MANIFEST_NAME
    try:
        worktree_manifest_bytes = manifest_path.read_bytes()
    except OSError as exc:
        raise SnapshotError(
            f"cannot read snapshot manifest {manifest_path}: {exc}"
        ) from exc
    worktree_manifest_sha256 = hashlib.sha256(worktree_manifest_bytes).hexdigest()
    if worktree_manifest_sha256 != selection.manifest_sha256:
        raise SnapshotError(
            "worktree snapshot manifest differs from its pin: expected "
            f"{selection.manifest_sha256}, got {worktree_manifest_sha256}"
        )
    try:
        manifest = SnapshotManifest.model_validate_json(worktree_manifest_bytes)
    except ValueError as exc:
        raise SnapshotError(
            f"invalid snapshot manifest {manifest_path}: {exc}"
        ) from exc
    if manifest != committed_manifest:
        raise SnapshotError("worktree snapshot manifest differs from pinned commit")
    files = {item.name: item for item in manifest.files}
    if missing := set(SCB_CSV_FILES) - set(files):
        raise SnapshotError(
            f"snapshot manifest is missing known SCB files: {sorted(missing)}"
        )
    if not files["Registerinformation.csv"].present:
        raise SnapshotError(
            "snapshot is missing required SCB backbone Registerinformation.csv"
        )
    if (
        files["Vardemangder.csv"].present
        and not files["VardemangderValidDates.csv"].present
    ):
        raise SnapshotError("Vardemangder.csv requires VardemangderValidDates.csv")
    return ScbSnapshotReader(
        root,
        manifest,
        repository=repo,
        input_commit=selection.input_commit,
        snapshot_path=snapshot_path,
        manifest_sha256=selection.manifest_sha256,
        omitted=omitted,
    )


def _fixed_bundle_paths() -> tuple[str, ...]:
    source_paths = (
        "catalog/SCB/Tabelldefinitioner.sql",
        "catalog/SCB/ID-kolumner.xlsx",
        "catalog/scb_canonical/scb_canonical.toml",
        *(
            f"catalog/{directory}/{provider}.toml"
            for provider, directory in _CURATED_PROVIDERS
        ),
    )
    curation_paths = (f"curation/{name}" for name in CATALOG_CURATION_FILES)
    slug_paths = (
        "fqid_slugs/classifications.toml",
        "fqid_slugs/freeze.toml",
        "fqid_slugs/.snapshot.json",
        *(
            path
            for provider in CATALOG_SLUG_PROVIDERS
            for path in (
                f"fqid_slugs/{provider}.toml",
                f"fqid_slugs/{provider}.auto.toml",
            )
        ),
    )
    return (*source_paths, *curation_paths, *slug_paths)


def _bundle_source_files(
    input_dir: Path,
    curation_dir: Path,
    slug_dir: Path,
    lisa_workbook: LisaWorkbookSelection | None = None,
) -> dict[str, Path | None]:
    """Resolve exactly the files an ordinary catalog build can read."""
    roots = {
        "input directory": input_dir,
        "curation directory": curation_dir,
        "slug directory": slug_dir,
    }
    for label, root in roots.items():
        if not root.is_dir():
            raise SnapshotError(f"{label} not found: {root}")

    resolved: dict[str, Path | None] = {}
    for relative in _fixed_bundle_paths():
        if relative.startswith("catalog/"):
            source = input_dir / Path(relative).relative_to("catalog")
        elif relative.startswith("curation/"):
            source = curation_dir / Path(relative).relative_to("curation")
        else:
            source = slug_dir / Path(relative).relative_to("fqid_slugs")
        resolved[relative] = source if source.is_file() else None

    if lisa_workbook is None:
        resolved[LISA_BUNDLE_PATH] = None
    else:
        if not lisa_workbook.upstream_revision.strip():
            raise SnapshotError("selected LISA workbook revision must be non-empty")
        if not _HASH_RE.fullmatch(lisa_workbook.sha256):
            raise SnapshotError(
                "selected LISA workbook SHA-256 must be 64 lowercase hexadecimal characters"
            )
        source = lisa_workbook.path.expanduser().resolve()
        if not source.is_file():
            raise SnapshotError(f"selected LISA workbook not found: {source}")
        if _file_sha256(source) != lisa_workbook.sha256:
            raise SnapshotError(
                "selected LISA workbook content does not match --lisa-workbook-sha256"
            )
        resolved[LISA_BUNDLE_PATH] = source

    sos_dir = input_dir / "Socialstyrelsen"
    if sos_dir.is_dir():
        for source in sorted(sos_dir.iterdir()):
            if (
                source.is_file()
                and source.suffix.lower() == ".xlsx"
                and not source.name.startswith("~$")
            ):
                resolved[f"catalog/Socialstyrelsen/{source.name}"] = source

    classification_seed = curation_dir / "classifications.toml"
    if classification_seed.is_file():
        from .classifications import _resolve_valid_codes_paths, load_seed

        entries = load_seed(classification_seed)
        classification_dir = input_dir / "classifications"
        classification_paths = _resolve_valid_codes_paths(entries, classification_dir)
        for entry in entries:
            name = entry["valid_codes_file"]
            relative = f"catalog/classifications/{name}"
            resolved[relative] = classification_paths[entry["short_name"]]

    canonical_toml = input_dir / "scb_canonical" / "scb_canonical.toml"
    if canonical_toml.is_file():
        from .sources.curated import CanonicalScbAdapter

        conn = sqlite3.connect(":memory:")
        try:
            adapter = CanonicalScbAdapter(
                conn, classification_seed_path=classification_seed
            )
            for name in adapter.referenced_value_sets(canonical_toml.parent):
                relative = f"catalog/scb_canonical/{name}.csv"
                source = canonical_toml.parent / f"{name}.csv"
                resolved[relative] = source if source.is_file() else None
        finally:
            conn.close()

    # Slug loading intentionally globs every top-level TOML. Include future
    # provider files automatically while retaining explicit absence for today's
    # fixed seed/freeze/auto paths above.
    for source in sorted(slug_dir.glob("*.toml")):
        resolved[f"fqid_slugs/{source.name}"] = source

    source_roots = {
        "catalog": input_dir.resolve(),
        "curation": curation_dir.resolve(),
        "fqid_slugs": slug_dir.resolve(),
    }
    validated: dict[str, Path | None] = {}
    for relative, source in sorted(resolved.items()):
        try:
            safe_relative = BundleFile(path=relative, present=False).path
        except ValueError as exc:
            raise SnapshotError(
                f"invalid catalog bundle destination {relative!r}: {exc}"
            ) from exc
        if safe_relative != relative:
            raise SnapshotError(
                f"catalog bundle destination must be normalized: {relative!r}"
            )
        if source is not None:
            source = source.resolve()
            role = Path(relative).parts[0]
            if role == "supplemental":
                if lisa_workbook is None:
                    raise SnapshotError(
                        "supplemental bundle source has no explicit LISA selection"
                    )
                selected_source = lisa_workbook.path.expanduser().resolve()
                if source != selected_source:
                    raise SnapshotError(
                        "LISA bundle source differs from its explicit selection: "
                        f"{source}"
                    )
            elif not source.is_relative_to(source_roots[role]):
                raise SnapshotError(
                    f"catalog bundle source escapes its selected root: {source}"
                )
        validated[relative] = source
    return validated


def _actual_bundle_files(root: Path) -> dict[str, int]:
    actual: dict[str, int] = {}
    for name in _BUNDLE_INVENTORY_ROOTS:
        directory = root / name
        if not directory.exists():
            continue
        for path in directory.rglob("*"):
            if path.is_file():
                actual[path.relative_to(root).as_posix()] = path.stat().st_size
    return actual


def _verify_bundle_inventory(
    root: Path, manifest: CatalogBundleManifest, *, hashes: bool
) -> None:
    expected = {item.path: item for item in manifest.files if item.present}
    actual = _actual_bundle_files(root)
    if actual.keys() != expected.keys():
        raise SnapshotError(
            "catalog bundle file inventory mismatch; "
            f"missing={sorted(expected.keys() - actual.keys())}, "
            f"extra={sorted(actual.keys() - expected.keys())}"
        )
    mismatched_sizes = sorted(
        path for path, item in expected.items() if actual[path] != item.size
    )
    if mismatched_sizes:
        raise SnapshotError(f"catalog bundle file size mismatch: {mismatched_sizes}")
    if hashes:
        mismatched_hashes = sorted(
            path
            for path, item in expected.items()
            if _file_sha256(root / path) != item.sha256
        )
        if mismatched_hashes:
            raise SnapshotError(
                f"catalog bundle file hash mismatch: {mismatched_hashes}"
            )


def _validate_bundle_contract(root: Path) -> None:
    """Run the existing small-input parsers at preparation/verification time."""
    from .alias_windows import load_alias_windows
    from .cis2016_matrix import load_cis2014_matrix, load_cis2016_matrix
    from .classification_links import load_classification_links
    from .classifications import load_seed, load_valid_codes
    from .codeless_overlap import load_codeless_overlap
    from .codelivery import load_codelivery
    from .concept_groups import (
        load_classification_groups,
        load_code_label_pairs,
        load_concept_group_accepts,
        load_concept_groups,
    )
    from .delivery_enrichment import load_delivery_enrichment
    from .fqid_slugs import load_lineage_config, load_slug_dir, read_snapshot
    from .period_family_merges import load_period_family_merges
    from .relations import load_relations
    from .scb_errata import load_scb_errata
    from .sources.curated import CanonicalScbAdapter, CuratedAdapter
    from .sources.scb import _import_id_kolumner, _import_tabelldefinitioner
    from .sources.sos import parse_directory
    from .tags import load_tags

    catalog = root / "catalog"
    curation = root / "curation"
    slugs = root / "fqid_slugs"
    seed = curation / "classifications.toml"
    # Keep the explicit bundle path even when absent: loaders that encounter a
    # classification reference must fail on that recorded absence, never fall
    # back to the builder checkout's curation.
    seed_path = seed

    if seed.is_file():
        for entry in load_seed(seed_path):
            load_valid_codes(catalog / "classifications" / entry["valid_codes_file"])

    sos = catalog / "Socialstyrelsen"
    if sos.is_dir():
        parse_directory(sos)

    for provider, directory in _CURATED_PROVIDERS:
        source_dir = catalog / directory
        if (source_dir / f"{provider}.toml").is_file():
            list(
                CuratedAdapter(provider, classification_seed_path=seed_path).emit(
                    source_dir
                )
            )

    canonical_dir = catalog / "scb_canonical"
    canonical_toml = canonical_dir / "scb_canonical.toml"
    if canonical_toml.is_file():
        conn = sqlite3.connect(":memory:")
        try:
            adapter = CanonicalScbAdapter(conn, classification_seed_path=seed_path)
            adapter.validate_source_files(canonical_dir)
        finally:
            conn.close()

    scb = catalog / "SCB"
    auxiliary_conn = sqlite3.connect(":memory:")
    try:
        auxiliary_conn.executescript(DDL)
        sql_path = scb / "Tabelldefinitioner.sql"
        if sql_path.is_file():
            _import_tabelldefinitioner(auxiliary_conn, sql_path)
        xlsx_path = scb / "ID-kolumner.xlsx"
        if xlsx_path.is_file():
            _import_id_kolumner(auxiliary_conn, xlsx_path)
    finally:
        auxiliary_conn.close()

    if slugs.is_dir():
        load_slug_dir(slugs)
        snapshot_path = slugs / ".snapshot.json"
        if snapshot_path.is_file():
            read_snapshot(snapshot_path)

    def curation_path(name: str) -> Path | None:
        path = curation / name
        return path if path.is_file() else None

    classifications = curation_path("classifications.toml")
    load_alias_windows(curation_path("alias_windows.toml"))
    load_classification_links(classifications)
    load_cis2014_matrix(curation_path("cis2014-matrix-meaning-evidence.json"), slugs)
    load_cis2016_matrix(curation_path("cis2016-matrix-meaning-evidence.json"), slugs)
    load_codeless_overlap(curation_path("codeless_overlap.toml"))
    load_codelivery(curation_path("codelivery.toml"))
    concept_groups = curation_path("concept_groups.toml")
    load_concept_groups(concept_groups)
    load_concept_group_accepts(concept_groups)
    load_classification_groups(concept_groups)
    load_code_label_pairs(concept_groups)
    load_concept_groups(curation_path("concept_groups.auto.toml"))
    load_delivery_enrichment(curation_path("delivery_enrichment.generated.toml"))
    load_lineage_config(curation_path("lineage.toml"))
    load_period_family_merges(curation_path("period_family_merges.toml"))
    load_relations(curation_path("relations.toml"))
    load_scb_errata(
        curation_path("scb_errata.toml"),
        slugs,
        classification_seed_path=seed_path,
    )
    load_tags(curation_path("tags.toml"))

    manifest_path = root / BUNDLE_MANIFEST_NAME
    if manifest_path.is_file():
        manifest_bytes = manifest_path.read_bytes()
        manifest = CatalogBundleManifest.model_validate_json(manifest_bytes)
        dataset = manifest.supplemental_datasets[0]
        if dataset.selected:
            from .source_records import SourceRevision
            from .sources.lisa import read_lisa_source

            artifact = next(
                item for item in manifest.files if item.path == dataset.artifact_path
            )
            assert artifact.size is not None
            assert artifact.sha256 is not None
            assert dataset.upstream_revision is not None
            read_lisa_source(
                root / dataset.artifact_path,
                SourceRevision.create(
                    dataset=dataset.dataset,
                    publisher=dataset.publisher,
                    purpose=dataset.purpose,
                    upstream_revision=dataset.upstream_revision,
                    artifact_path=dataset.artifact_path,
                    artifact_size=artifact.size,
                    artifact_sha256=artifact.sha256,
                ),
            )


def prepare_input_bundle(
    input_dir: Path,
    curation_dir: Path,
    slug_dir: Path,
    scb_snapshot: ScbSnapshotSelection,
    output: Path,
    *,
    lisa_workbook: LisaWorkbookSelection | None = None,
) -> CatalogBundleStats:
    """Capture a new byte-preserved catalog-input candidate beside an accepted snapshot."""
    input_dir = input_dir.expanduser().resolve()
    curation_dir = curation_dir.expanduser().resolve()
    slug_dir = slug_dir.expanduser().resolve()
    output = output.expanduser().resolve()
    if output.exists():
        raise SnapshotError(
            f"candidate path already exists and will not be overwritten: {output}"
        )
    if any((parent / BUNDLE_MANIFEST_NAME).is_file() for parent in output.parents):
        raise SnapshotError(
            "catalog bundle candidate must stay outside every existing bundle"
        )

    snapshot = open_scb_snapshot(scb_snapshot)
    repository = Path(_git(snapshot.root, "rev-parse", "--show-toplevel")).resolve()
    if not output.is_relative_to(repository) or output == repository:
        raise SnapshotError(
            "catalog bundle candidate must be a new directory inside the accepted input repository"
        )
    if output == snapshot.root or output.is_relative_to(snapshot.root):
        raise SnapshotError(
            "catalog bundle candidate must stay outside the SCB snapshot"
        )
    sources = _bundle_source_files(
        input_dir, curation_dir, slug_dir, lisa_workbook=lisa_workbook
    )
    missing_references = sorted(
        path for path, source in sources.items() if source is None
    )
    # Fixed optional paths intentionally remain absent. Dynamic classification and
    # canonical code references, however, were introduced only by a manifest that
    # requires them and therefore must exist.
    required_prefixes = ("catalog/classifications/", "catalog/scb_canonical/")
    required_missing = [
        path
        for path in missing_references
        if path.startswith(required_prefixes)
        and path != "catalog/scb_canonical/scb_canonical.toml"
    ]
    if required_missing:
        raise SnapshotError(
            f"referenced catalog input files are missing: {required_missing}"
        )

    source_state = {
        path: None if source is None else (source, _file_identity(source.stat()))
        for path, source in sources.items()
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.candidate-", dir=output.parent)
    )
    try:
        for name in _BUNDLE_INVENTORY_ROOTS:
            (staging / name).mkdir()
        files: list[BundleFile] = []
        for relative, source in sources.items():
            if source is None:
                files.append(BundleFile(path=relative, present=False))
                continue
            destination = staging / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            files.append(
                BundleFile(
                    path=relative,
                    present=True,
                    size=destination.stat().st_size,
                    sha256=_file_sha256(destination),
                )
            )

        snapshot_path = snapshot.root.relative_to(repository).as_posix()
        manifest = CatalogBundleManifest(
            format="reg-meta-build-catalog-input-bundle",
            schema_version=BUNDLE_SCHEMA_VERSION,
            bundle_id=snapshot.manifest.bundle_id,
            edition=snapshot.manifest.edition,
            scb_snapshot_path=snapshot_path,
            scb_manifest_sha256=scb_snapshot.manifest_sha256,
            files=tuple(files),
            supplemental_datasets=(
                SupplementalDataset(
                    dataset=LISA_DATASET_ID,
                    publisher=LISA_PUBLISHER,
                    purpose=LISA_PURPOSE,
                    upstream_revision=(
                        lisa_workbook.upstream_revision
                        if lisa_workbook is not None
                        else None
                    ),
                    reader=LISA_READER,
                    layout=LISA_LAYOUT,
                    selected=lisa_workbook is not None,
                    required=lisa_workbook is not None,
                    artifact_path=LISA_BUNDLE_PATH,
                    exclusion_reason=(
                        None
                        if lisa_workbook is not None
                        else "not selected during bundle preparation"
                    ),
                ),
            ),
        )
        manifest_bytes = _manifest_bytes(manifest)
        (staging / BUNDLE_MANIFEST_NAME).write_bytes(manifest_bytes)
        _verify_bundle_inventory(staging, manifest, hashes=True)
        _validate_bundle_contract(staging)
        current_sources = _bundle_source_files(
            input_dir, curation_dir, slug_dir, lisa_workbook=lisa_workbook
        )
        current_state = {
            path: None if source is None else (source, _file_identity(source.stat()))
            for path, source in current_sources.items()
        }
        changed = sorted(
            path
            for path in source_state.keys() | current_state.keys()
            if source_state.get(path, _UNSET) != current_state.get(path, _UNSET)
        )
        if changed:
            raise SnapshotError(
                f"catalog input files changed during bundle preparation: {changed}"
            )
        # The candidate itself makes the repository dirty until acceptance, so
        # recheck only the already-accepted snapshot subtree here.
        snapshot_path = snapshot.root.relative_to(repository).as_posix()
        if _git(repository, "rev-parse", "HEAD") != scb_snapshot.input_commit:
            raise SnapshotError("input repository commit changed during preparation")
        if _git(
            repository,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--",
            snapshot_path,
        ):
            raise SnapshotError("accepted SCB snapshot changed during preparation")
        staging.replace(output)
        return CatalogBundleStats(
            files=sum(item.present for item in files),
            bytes=sum(item.size or 0 for item in files) + len(manifest_bytes),
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _read_committed_bundle_manifest(
    repo: Path, commit: str, bundle_path: str
) -> tuple[CatalogBundleManifest, bytes, str]:
    path = _snapshot_repo_path(bundle_path, BUNDLE_MANIFEST_NAME)
    payload = _git_bytes(repo, "cat-file", "blob", f"{commit}:{path}")
    try:
        manifest = CatalogBundleManifest.model_validate_json(payload)
    except ValueError as exc:
        raise SnapshotError(
            f"invalid catalog bundle manifest in pinned commit: {exc}"
        ) from exc
    return manifest, payload, hashlib.sha256(payload).hexdigest()


def _verify_committed_bundle_inventory(
    repo: Path,
    commit: str,
    bundle_path: str,
    manifest: CatalogBundleManifest,
) -> None:
    roots = [_snapshot_repo_path(bundle_path, name) for name in _BUNDLE_INVENTORY_ROOTS]
    actual = _committed_inventory_sizes(
        repo,
        commit,
        bundle_path,
        roots,
        context="catalog bundle",
    )
    expected = {item.path: item.size for item in manifest.files if item.present}
    if actual.keys() != expected.keys():
        raise SnapshotError(
            "catalog bundle file inventory mismatch in pinned commit; "
            f"missing={sorted(expected.keys() - actual.keys())}, "
            f"extra={sorted(actual.keys() - expected.keys())}"
        )
    mismatched = sorted(path for path, size in expected.items() if actual[path] != size)
    if mismatched:
        raise SnapshotError(
            f"catalog bundle file size mismatch in pinned commit: {mismatched}"
        )


def open_input_bundle(selection: CatalogBundleSelection) -> CatalogBundleReader:
    """Quick-check one accepted complete bundle without hashing its payloads."""
    if not _GIT_COMMIT_RE.fullmatch(selection.input_commit):
        raise SnapshotError(
            "catalog input commit must be a full 40-character lowercase Git commit"
        )
    if not _HASH_RE.fullmatch(selection.manifest_sha256):
        raise SnapshotError(
            "catalog bundle manifest SHA-256 must be 64 lowercase hexadecimal characters"
        )
    root = selection.path.expanduser().resolve()
    repo = input_bundle_repository(root)
    try:
        bundle_path = root.relative_to(repo).as_posix()
    except ValueError as exc:
        raise SnapshotError(
            f"catalog bundle is outside its Git repository: {root}"
        ) from exc
    actual_commit = _git(repo, "rev-parse", "HEAD")
    if actual_commit != selection.input_commit:
        raise SnapshotError(
            "catalog input commit pin mismatch: expected "
            f"{selection.input_commit}, got {actual_commit}"
        )
    committed, committed_bytes, committed_sha256 = _read_committed_bundle_manifest(
        repo, selection.input_commit, bundle_path
    )
    if committed_sha256 != selection.manifest_sha256:
        raise SnapshotError(
            "catalog bundle manifest pin mismatch: expected "
            f"{selection.manifest_sha256}, got {committed_sha256}"
        )
    _verify_committed_bundle_inventory(
        repo, selection.input_commit, bundle_path, committed
    )
    snapshot = open_scb_snapshot(
        ScbSnapshotSelection(
            path=repo / committed.scb_snapshot_path,
            input_commit=selection.input_commit,
            manifest_sha256=committed.scb_manifest_sha256,
        )
    )
    manifest_path = root / BUNDLE_MANIFEST_NAME
    try:
        worktree_bytes = manifest_path.read_bytes()
    except OSError as exc:
        raise SnapshotError(
            f"cannot read catalog bundle manifest {manifest_path}: {exc}"
        ) from exc
    if hashlib.sha256(worktree_bytes).hexdigest() != selection.manifest_sha256:
        raise SnapshotError("worktree catalog bundle manifest differs from its pin")
    try:
        manifest = CatalogBundleManifest.model_validate_json(worktree_bytes)
    except ValueError as exc:
        raise SnapshotError(
            f"invalid catalog bundle manifest {manifest_path}: {exc}"
        ) from exc
    if manifest != committed or worktree_bytes != committed_bytes:
        raise SnapshotError(
            "worktree catalog bundle manifest differs from pinned commit"
        )
    _verify_bundle_inventory(root, manifest, hashes=False)
    return CatalogBundleReader(
        root=root,
        repository=repo,
        manifest=manifest,
        snapshot=snapshot,
        provenance={
            "input_repository_commit": selection.input_commit,
            "bundle_manifest_path": _snapshot_repo_path(
                bundle_path, BUNDLE_MANIFEST_NAME
            ),
            "bundle_manifest_sha256": selection.manifest_sha256,
        },
    )


def verify_input_bundle(
    selection: CatalogBundleSelection,
) -> CatalogBundleManifest:
    """Exhaustively verify accepted small inputs and the referenced SCB snapshot."""
    bundle = open_input_bundle(selection)
    bundle.snapshot.require_vardemangder_materialized()
    _verify_bundle_inventory(bundle.root, bundle.manifest, hashes=True)
    _validate_bundle_contract(bundle.root)
    verified_snapshot = verify_snapshot(bundle.snapshot.root)
    if verified_snapshot != bundle.snapshot.manifest:
        raise SnapshotError("verified SCB snapshot differs from the selected manifest")
    return bundle.manifest


def create_slug_workspace(bundle: CatalogBundleReader, parent: Path) -> Path:
    """Copy only mutable slug TOMLs outside the accepted input repository."""
    parent = parent.resolve()
    workspace = Path(tempfile.mkdtemp(prefix="regmeta-slugs-", dir=parent))
    for item in bundle.manifest.files:
        if not item.present or not item.path.startswith("fqid_slugs/"):
            continue
        source = bundle.root / item.path
        if source.suffix != ".toml":
            continue
        shutil.copy2(source, workspace / source.name)
    return workspace


def slug_workspace_changes(
    bundle: CatalogBundleReader, workspace: Path
) -> dict[str, list[str]]:
    """Describe generated slug changes without mutating accepted inputs."""
    accepted = {
        Path(item.path).name: item.sha256
        for item in bundle.manifest.files
        if item.present
        and item.path.startswith("fqid_slugs/")
        and item.path.endswith(".toml")
    }
    actual = {
        path.name: _file_sha256(path)
        for path in workspace.glob("*.toml")
        if path.is_file()
    }
    return {
        "added": sorted(actual.keys() - accepted.keys()),
        "changed": sorted(
            name
            for name in actual.keys() & accepted.keys()
            if actual[name] != accepted[name]
        ),
        "removed": sorted(accepted.keys() - actual.keys()),
    }


def create_build_lock(
    snapshot: Path,
    recorded_db: Path,
    output: Path,
    *,
    providers: Sequence[str],
    build_options: Mapping[str, bool | int | str],
    auxiliary_inputs: Mapping[str, Path | None],
) -> BuildLock:
    """Bind exact inputs/code and authenticate one retained recorded database."""
    snapshot = snapshot.resolve()
    recorded_db = recorded_db.resolve()
    output = output.resolve()
    if output.exists():
        raise SnapshotError(
            f"build lock already exists and will not be overwritten: {output}"
        )
    input_repo = Path(_git(snapshot, "rev-parse", "--show-toplevel"))
    builder_repo, builder_commit = _builder_source_identity()
    snapshot_path = snapshot.relative_to(input_repo).as_posix()
    worktree_manifest = load_manifest(snapshot)
    _require_complete_normalized_inventory(snapshot, worktree_manifest)
    input_commit = clean_git_commit(input_repo)
    committed_manifest, committed_manifest_sha256 = _verify_committed_snapshot(
        input_repo, input_commit, snapshot_path
    )
    manifest = verify_snapshot(snapshot)
    if manifest != committed_manifest:
        raise SnapshotError("worktree snapshot manifest differs from pinned commit")
    uv_lock = builder_repo / "uv.lock"
    if not recorded_db.is_file():
        raise SnapshotError(f"recorded result DB is missing: {recorded_db}")
    auxiliary: list[AuxiliaryPin] = []
    for name, path in sorted(auxiliary_inputs.items()):
        if path is None:
            auxiliary.append(AuxiliaryPin(name=name, present=False))
        else:
            resolved = path.resolve()
            if not resolved.is_file():
                raise SnapshotError(f"auxiliary input is missing: {name}={resolved}")
            auxiliary.append(
                AuxiliaryPin(
                    name=name,
                    present=True,
                    size=resolved.stat().st_size,
                    sha256=_file_sha256(resolved),
                )
            )
    lock = BuildLock(
        format="reg-meta-build-replay-lock",
        schema_version=1,
        input_repository_commit=input_commit,
        snapshot_path=snapshot_path,
        snapshot_manifest_sha256=committed_manifest_sha256,
        snapshot_schema_version=committed_manifest.schema_version,
        snapshot_converter_version=committed_manifest.converter_version,
        builder_commit=builder_commit,
        uv_lock_sha256=_file_sha256(uv_lock),
        python_runtime=f"{sys.implementation.name}-{platform.python_version()}",
        providers=tuple(providers),
        build_options=dict(sorted(build_options.items())),
        auxiliary_inputs=tuple(auxiliary),
        result_db_sha256=_file_sha256(recorded_db),
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_fd, temporary_name = tempfile.mkstemp(
        prefix=f".{output.name}.", suffix=".tmp", dir=output.parent
    )
    os.close(temporary_fd)
    temporary = Path(temporary_name)
    try:
        temporary.write_bytes(_manifest_bytes(lock))
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    return lock


def verify_build_lock(
    lock_path: Path,
    snapshot: Path,
    *,
    auxiliary_inputs: Mapping[str, Path | None],
    recorded_db: Path | None = None,
    replay_db: Path | None = None,
) -> BuildLock:
    """Verify pins, authenticate a recorded DB, and optionally compare a replay."""
    try:
        lock = BuildLock.model_validate_json(lock_path.read_bytes())
    except (OSError, ValueError) as exc:
        raise SnapshotError(f"invalid build lock {lock_path}: {exc}") from exc
    snapshot = snapshot.resolve()
    recorded_db = recorded_db.resolve() if recorded_db is not None else None
    replay_db = replay_db.resolve() if replay_db is not None else None
    if replay_db is not None and recorded_db is None:
        raise SnapshotError("replay comparison requires the recorded result DB")
    input_repo = Path(_git(snapshot, "rev-parse", "--show-toplevel"))
    builder_repo, builder_commit = _builder_source_identity()
    uv_lock = builder_repo / "uv.lock"
    if recorded_db is not None and not recorded_db.is_file():
        raise SnapshotError(f"recorded result DB is missing: {recorded_db}")
    if replay_db is not None and not replay_db.is_file():
        raise SnapshotError(f"replay result DB is missing: {replay_db}")
    snapshot_path = snapshot.relative_to(input_repo).as_posix()
    worktree_manifest = load_manifest(snapshot)
    _require_complete_normalized_inventory(snapshot, worktree_manifest)
    input_commit = clean_git_commit(input_repo)
    checks = {
        "input repository commit": (
            input_commit,
            lock.input_repository_commit,
        ),
        "builder commit": (builder_commit, lock.builder_commit),
        "snapshot path": (
            snapshot_path,
            lock.snapshot_path,
        ),
        "uv.lock": (_file_sha256(uv_lock), lock.uv_lock_sha256),
        "Python runtime": (
            f"{sys.implementation.name}-{platform.python_version()}",
            lock.python_runtime,
        ),
    }
    for label, (actual, expected) in checks.items():
        if actual != expected:
            raise SnapshotError(
                f"{label} pin mismatch: expected {expected}, got {actual}"
            )
    committed_manifest, committed_manifest_sha256 = _verify_committed_snapshot(
        input_repo, input_commit, snapshot_path
    )
    if committed_manifest_sha256 != lock.snapshot_manifest_sha256:
        raise SnapshotError(
            "snapshot manifest pin mismatch: expected "
            f"{lock.snapshot_manifest_sha256}, got {committed_manifest_sha256}"
        )
    manifest = verify_snapshot(snapshot)
    if manifest != committed_manifest:
        raise SnapshotError("worktree snapshot manifest differs from pinned commit")
    version_checks = {
        "snapshot schema version": (
            manifest.schema_version,
            lock.snapshot_schema_version,
        ),
        "snapshot converter version": (
            manifest.converter_version,
            lock.snapshot_converter_version,
        ),
    }
    for label, (actual, expected) in version_checks.items():
        if actual != expected:
            raise SnapshotError(
                f"{label} pin mismatch: expected {expected}, got {actual}"
            )
    expected_aux = {item.name: item for item in lock.auxiliary_inputs}
    if set(auxiliary_inputs) != set(expected_aux):
        raise SnapshotError("auxiliary input names do not match the build lock")
    for name, path in auxiliary_inputs.items():
        expected = expected_aux[name]
        if path is None:
            if expected.present:
                raise SnapshotError(f"auxiliary input {name} is unexpectedly absent")
            continue
        if not expected.present or not path.is_file():
            raise SnapshotError(
                f"auxiliary input {name} presence does not match the lock"
            )
        if (
            path.stat().st_size != expected.size
            or _file_sha256(path) != expected.sha256
        ):
            raise SnapshotError(f"auxiliary input {name} hash/size mismatch")
    if recorded_db is not None and _file_sha256(recorded_db) != lock.result_db_sha256:
        raise SnapshotError("recorded result DB hash does not match the build lock")
    if replay_db is not None:
        assert recorded_db is not None
        try:
            report = diff_db_content(
                recorded_db,
                replay_db,
                ignore=_REPLAY_DB_IGNORE,
            )
        except (OSError, sqlite3.Error) as exc:
            raise SnapshotError(f"cannot compare replay result DB: {exc}") from exc
        if not report.identical:
            raise SnapshotError(
                "replay result DB differs from the authenticated recorded DB:\n"
                f"{format_report(report)}"
            )
    return lock


def tree_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _git_object_sizes(repo: Path) -> tuple[int, int]:
    values = {
        key: int(value)
        for line in _git(repo, "count-objects", "-v").splitlines()
        for key, separator, value in (line.partition(":"),)
        if separator and value.strip().isdigit()
    }
    return values.get("size", 0) * 1024, values.get("size-pack", 0) * 1024


def measure_git_history(initial: Path, update: Path) -> dict[str, Any]:
    """Measure initial and one-update Git growth in an isolated temporary repo."""
    initial = initial.resolve()
    update = update.resolve()
    with tempfile.TemporaryDirectory(prefix="regmeta-input-git-") as temporary:
        repo = Path(temporary)
        _git(repo, "init", "-q")
        _git(repo, "config", "user.email", "snapshot@example.invalid")
        _git(repo, "config", "user.name", "Snapshot measurement")
        for name, value in _GIT_PACK_SETTINGS:
            _git(repo, "config", "--local", name, value)

        def install(source: Path) -> None:
            for child in repo.iterdir():
                if child.name == ".git":
                    continue
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            for child in source.iterdir():
                target = repo / child.name
                if child.is_dir():
                    shutil.copytree(child, target)
                else:
                    shutil.copy2(child, target)

        install(initial)
        _git(repo, "add", "-A")
        _git(repo, "commit", "-q", "-m", "initial")
        initial_loose, _ = _git_object_sizes(repo)
        effective_pack_settings = {
            name: _git(repo, "config", "--get", name)
            for name, _value in _GIT_PACK_SETTINGS
        }
        _git(repo, "gc", "--prune=now")
        _, initial_pack = _git_object_sizes(repo)
        install(update)
        _git(repo, "add", "-N", ".")
        numstat = _git(repo, "diff", "--numstat")
        changed_files = len([line for line in numstat.splitlines() if line])
        inserted = deleted = 0
        for line in numstat.splitlines():
            add, remove, _name = line.split("\t", 2)
            if add.isdigit():
                inserted += int(add)
            if remove.isdigit():
                deleted += int(remove)
        if numstat:
            _git(repo, "add", "-A")
            _git(repo, "commit", "-q", "-m", "update")
            update_loose, _ = _git_object_sizes(repo)
            _git(repo, "gc", "--prune=now")
            _, final_pack = _git_object_sizes(repo)
        else:
            _git(repo, "reset", "-q")
            update_loose = 0
            final_pack = initial_pack
        return {
            "git_version": _git(repo, "--version"),
            "git_gc": "git gc --prune=now",
            "git_pack_settings": effective_pack_settings,
            "initial_working_tree_bytes": tree_size(initial),
            "update_working_tree_bytes": tree_size(update),
            "initial_loose_object_bytes": initial_loose,
            "initial_packed_object_bytes": initial_pack,
            "update_loose_object_bytes": update_loose,
            "two_commit_packed_object_bytes": final_pack,
            "incremental_packed_object_bytes": final_pack - initial_pack,
            "changed_files": changed_files,
            "inserted_lines": inserted,
            "deleted_lines": deleted,
        }


__all__ = [
    "BuildLock",
    "CatalogBundleManifest",
    "CatalogBundleReader",
    "CatalogBundleSelection",
    "CatalogBundleStats",
    "DeliveryInventory",
    "LisaWorkbookSelection",
    "ScbSnapshotReader",
    "ScbSnapshotSelection",
    "SnapshotError",
    "SnapshotManifest",
    "SnapshotMaterializationError",
    "SnapshotStats",
    "SupplementalDataset",
    "clean_git_commit",
    "converter_source_commit",
    "create_build_lock",
    "create_slug_workspace",
    "input_bundle_repository",
    "load_inventory",
    "load_manifest",
    "measure_codec_sample",
    "measure_git_history",
    "open_input_bundle",
    "open_lossless_csv",
    "open_scb_snapshot",
    "prepare_input_bundle",
    "prepare_snapshot",
    "restore_snapshot",
    "slug_workspace_changes",
    "verify_build_lock",
    "verify_input_bundle",
    "verify_snapshot",
]
