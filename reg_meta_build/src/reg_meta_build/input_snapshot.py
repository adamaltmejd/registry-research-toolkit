"""Lossless, versioned snapshots of SCB machine-readable CSV deliveries.

This module is deliberately outside the normal build path.  It condenses a coherent
CSV delivery into deterministic, reviewable text files and can reconstruct the same
ordered logical records without consulting the retained raw archive.  Provider
interpretation (encoding repair, filtering, projection, coalescing, and curation) stays
in :mod:`reg_meta_build.sources.scb`.

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

from .db import _file_sha256

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from typing import IO

SNAPSHOT_SCHEMA_VERSION = 1
CONVERTER_VERSION = 1
SERIALIZATION = "escaped-tsv-v1"
CHUNK_RECORDS = 100_000
MANIFEST_NAME = "manifest.json"

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


@dataclass(frozen=True)
class SnapshotStats:
    raw_bytes: int
    normalized_bytes: int
    records: int
    elapsed_seconds: float
    codec_sample: dict[str, dict[str, int]]


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


def _manifest_bytes(manifest: SnapshotManifest | BuildLock) -> bytes:
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


def _tsv_line(fields: Sequence[str]) -> bytes:
    if any("\t" in value or "\r" in value or "\n" in value for value in fields):
        raise AssertionError("escaped TSV fields must occupy one physical line")
    return ("\t".join(fields) + "\n").encode()


def _iter_tsv(path: Path) -> Iterator[tuple[int, list[str]]]:
    with path.open("rb") as handle:
        for line_number, raw in enumerate(handle, start=1):
            if not raw.endswith(b"\n"):
                raise SnapshotError(
                    f"{path}: line {line_number} lacks a newline terminator"
                )
            try:
                text = raw[:-1].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise SnapshotError(f"{path}: line {line_number} is not UTF-8") from exc
            if "\r" in text:
                raise SnapshotError(
                    f"{path}: line {line_number} contains an unescaped CR"
                )
            yield line_number, text.split("\t")


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


def _verify_normalized_files(root: Path, manifest: SnapshotManifest) -> None:
    declared = _declared_normalized_files(manifest)
    actual_root = root / "files"
    actual = (
        {
            path.relative_to(root).as_posix()
            for path in actual_root.rglob("*")
            if path.is_file()
        }
        if actual_root.exists()
        else set()
    )
    if actual != declared:
        raise SnapshotError(
            f"normalized file inventory mismatch; missing={sorted(declared - actual)}, extra={sorted(actual - declared)}"
        )
    for item in manifest.files:
        for normalized in _normalized_files(item):
            path = root / normalized.path
            if (
                path.stat().st_size != normalized.size
                or _file_sha256(path) != normalized.sha256
            ):
                raise SnapshotError(
                    f"normalized file hash/size mismatch: {normalized.path}"
                )


def _load_groups(
    root: Path, item: SnapshotFile
) -> list[dict[str, tuple[RawCell, ...]]]:
    dictionaries: list[dict[str, tuple[RawCell, ...]]] = []
    for group in item.groups:
        payloads: dict[str, tuple[RawCell, ...]] = {}
        last_key = ""
        lines = 0
        for line_number, fields in _iter_tsv(root / group.dictionary.path):
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
            if _payload_key(group.name, group.positions, cells) != key:
                raise SnapshotError(
                    f"{group.dictionary.path}: line {line_number} has a false content key"
                )
            payloads[key] = cells
            last_key = key
            lines += 1
        if lines != group.entries or lines != group.dictionary.lines:
            raise SnapshotError(
                f"dictionary line count mismatch: {group.dictionary.path}"
            )
        dictionaries.append(payloads)
    return dictionaries


def _iter_snapshot_rows(
    root: Path,
    item: SnapshotFile,
    dictionaries: Sequence[Mapping[str, tuple[RawCell, ...]]],
) -> Iterator[list[RawCell]]:
    expected_fields = len(item.groups) + len(item.inline_positions)
    seen = 0
    for record_file in item.records:
        lines = 0
        for line_number, fields in _iter_tsv(root / record_file.path):
            if len(fields) != expected_fields:
                raise SnapshotError(
                    f"{record_file.path}: line {line_number} has the wrong field count"
                )
            row: list[RawCell | object] = [_UNSET] * item.column_count
            for index, group in enumerate(item.groups):
                key = fields[index]
                if not _KEY_RE.fullmatch(key):
                    raise SnapshotError(
                        f"{record_file.path}: line {line_number} has an invalid group key"
                    )
                try:
                    values = dictionaries[index][key]
                except KeyError as exc:
                    raise SnapshotError(
                        f"{record_file.path}: line {line_number} references missing {group.name} payload {key}"
                    ) from exc
                for position, value in zip(group.positions, values, strict=True):
                    row[position] = value
            offset = len(item.groups)
            for position, value in zip(
                item.inline_positions, fields[offset:], strict=True
            ):
                row[position] = _decode_cell(_field_cell(value))
            if any(value is _UNSET for value in row):
                raise SnapshotError(
                    f"{record_file.path}: line {line_number} did not fill every column"
                )
            yield cast("list[RawCell]", row)
            lines += 1
            seen += 1
        if lines != record_file.lines:
            raise SnapshotError(f"record line count mismatch: {record_file.path}")
    if seen != item.record_count:
        raise SnapshotError(
            f"record count mismatch for {item.name}: expected {item.record_count}, got {seen}"
        )


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
    _verify_normalized_files(root, manifest)
    for item in manifest.files:
        if not item.present:
            continue
        dictionaries = _load_groups(root, item)
        header = [_decode_cell(cell) for cell in item.header]
        rows = _iter_snapshot_rows(root, item, dictionaries)
        if restore_dir is None:
            records_hash, logical_hash, count = _logical_hashes(header, rows)
        else:
            restored = restore_dir / item.name
            _write_csv(restored, header, rows)
            with open_lossless_csv(restored) as (restored_header, restored_rows):
                records_hash, logical_hash, count = _logical_hashes(
                    restored_header, restored_rows
                )
        if count != item.record_count:
            raise SnapshotError(f"record count mismatch for {item.name}")
        if (
            records_hash != item.ordered_records_sha256
            or logical_hash != item.logical_sha256
        ):
            raise SnapshotError(f"logical record round-trip mismatch for {item.name}")
    return manifest


def verify_snapshot(root: Path) -> SnapshotManifest:
    """Verify normalized hashes, dictionary closure, and ordered logical records."""
    return _inspect_snapshot(root.resolve())


def restore_snapshot(root: Path, output: Path) -> SnapshotStats:
    """Reconstruct CSVs into a new directory, atomically on complete success."""
    started = time.perf_counter()
    root = root.resolve()
    output = output.resolve()
    manifest = load_manifest(root)
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
            sample_records[item.name] = prepared.record_count
            raw_bytes += sample.stat().st_size
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
            "codec_sample": codec_sample.by_kind,
            "elapsed_seconds": time.perf_counter() - started,
        }


def _git(repo: Path, *args: str) -> str:
    try:
        process = subprocess.run(
            ["git", "-C", str(repo), *args], check=True, text=True, capture_output=True
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = (
            exc.stderr.strip()
            if isinstance(exc, subprocess.CalledProcessError)
            else str(exc)
        )
        raise SnapshotError(f"git {' '.join(args)} failed in {repo}: {detail}") from exc
    return process.stdout.strip()


def clean_git_commit(repo: Path) -> str:
    repo = repo.resolve()
    status = _git(repo, "status", "--porcelain", "--untracked-files=all")
    if status:
        raise SnapshotError(f"Git repository must be clean for an exact pin: {repo}")
    return _git(repo, "rev-parse", "HEAD")


def converter_source_commit(cli_path: Path) -> str:
    """Return the commit containing the clean CLI and imported converter sources."""
    sources = (cli_path.resolve(), Path(__file__).resolve())
    if any(not source.is_file() for source in sources):
        raise SnapshotError("CLI and imported converter source files must exist")
    repositories = tuple(
        Path(_git(source.parent, "rev-parse", "--show-toplevel")).resolve()
        for source in sources
    )
    if repositories[0] != repositories[1]:
        raise SnapshotError(
            "CLI and imported converter must come from the same Git checkout"
        )
    repo = repositories[0]
    commit = clean_git_commit(repo)
    for source in sources:
        relative = source.relative_to(repo).as_posix()
        try:
            committed_blob = _git(repo, "rev-parse", "--verify", f"{commit}:{relative}")
        except SnapshotError as exc:
            raise SnapshotError(
                f"converter source is not a tracked HEAD blob: {relative}"
            ) from exc
        if _git(repo, "cat-file", "-t", committed_blob) != "blob":
            raise SnapshotError(f"converter source is not a Git blob: {relative}")
        worktree_blob = _git(repo, "hash-object", "--no-filters", relative)
        if worktree_blob != committed_blob:
            raise SnapshotError(
                f"converter source differs from its HEAD blob: {relative}"
            )
    return commit


def _snapshot_repo_path(snapshot_path: str, relative_path: str) -> str:
    if snapshot_path == ".":
        return relative_path
    return f"{snapshot_path}/{relative_path}"


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


def create_build_lock(
    snapshot: Path,
    builder_repo: Path,
    result_db: Path,
    output: Path,
    *,
    providers: Sequence[str],
    build_options: Mapping[str, bool | int | str],
    auxiliary_inputs: Mapping[str, Path | None],
) -> BuildLock:
    """Bind exact input/builder commits, auxiliary inputs, and a built DB hash."""
    snapshot = snapshot.resolve()
    builder_repo = builder_repo.resolve()
    output = output.resolve()
    if output.exists():
        raise SnapshotError(
            f"build lock already exists and will not be overwritten: {output}"
        )
    input_repo = Path(_git(snapshot, "rev-parse", "--show-toplevel"))
    input_commit = clean_git_commit(input_repo)
    builder_commit = clean_git_commit(builder_repo)
    snapshot_path = snapshot.relative_to(input_repo).as_posix()
    committed_manifest, committed_manifest_sha256 = _verify_committed_snapshot(
        input_repo, input_commit, snapshot_path
    )
    manifest = verify_snapshot(snapshot)
    if manifest != committed_manifest:
        raise SnapshotError("worktree snapshot manifest differs from pinned commit")
    uv_lock = builder_repo / "uv.lock"
    if not uv_lock.is_file() or not result_db.is_file():
        raise SnapshotError("builder uv.lock and result DB must both exist")
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
        result_db_sha256=_file_sha256(result_db),
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
    builder_repo: Path,
    *,
    auxiliary_inputs: Mapping[str, Path | None],
    result_db: Path | None = None,
) -> BuildLock:
    """Verify replay pins before a build and, when supplied, its result afterward."""
    try:
        lock = BuildLock.model_validate_json(lock_path.read_bytes())
    except (OSError, ValueError) as exc:
        raise SnapshotError(f"invalid build lock {lock_path}: {exc}") from exc
    snapshot = snapshot.resolve()
    builder_repo = builder_repo.resolve()
    input_repo = Path(_git(snapshot, "rev-parse", "--show-toplevel"))
    uv_lock = builder_repo / "uv.lock"
    if not uv_lock.is_file():
        raise SnapshotError(f"builder uv.lock is missing: {uv_lock}")
    if result_db is not None and not result_db.is_file():
        raise SnapshotError(f"result DB is missing: {result_db}")
    input_commit = clean_git_commit(input_repo)
    builder_commit = clean_git_commit(builder_repo)
    snapshot_path = snapshot.relative_to(input_repo).as_posix()
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
    if result_db is not None and _file_sha256(result_db) != lock.result_db_sha256:
        raise SnapshotError("result DB hash does not match the build lock")
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
            "git_pack_settings": dict(_GIT_PACK_SETTINGS),
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
    "DeliveryInventory",
    "SnapshotError",
    "SnapshotManifest",
    "SnapshotStats",
    "clean_git_commit",
    "converter_source_commit",
    "create_build_lock",
    "load_inventory",
    "load_manifest",
    "measure_codec_sample",
    "measure_git_history",
    "open_lossless_csv",
    "prepare_snapshot",
    "restore_snapshot",
    "verify_build_lock",
    "verify_snapshot",
]
