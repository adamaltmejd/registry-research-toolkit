"""Lossless prepared source values with compact ordered occurrences and member lookup.

The binary stream contains four little-endian uint32 dictionary references per
occurrence. Source order supplies its default row number; exceptional locators and
other evidence live in sparse payloads. No catalog membership decision is stored.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import mmap
import os
import re
import shutil
import sqlite3
import struct
import tempfile
from array import array
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from functools import cached_property, lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    TypeAdapter,
    field_validator,
    model_validator,
)

from reg_meta_build._accepted_prepared import (
    check_accepted_files,
    read_accepted_manifest,
)
from reg_meta_build.db import _file_sha256
from reg_meta_build.input_snapshot import SnapshotError, _git
from reg_meta_build.prepared_sources import (
    _COMMIT_RE,
    _HASH_RE,
    _json,
    _PayloadWriter,
    _readonly,
)
from reg_meta_build.source_records import DeliveredCell, RecordLocator, SourceRevision
from reg_meta_build.source_values import (
    SourceMemberHint,
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueJoin,
    SourceValueValidity,
    SourceValueWindow,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from typing import Self

_MANIFEST = "manifest.json"
_DATABASE = "files/values.sqlite"
_ASSOCIATIONS = "files/associations.bin"
_POSTINGS = "files/member-positions.bin"
_MEMBERS = (_DATABASE, _ASSOCIATIONS, _POSTINGS)
_VERSION = 2
_UINT32_MAX = 2**32 - 1
_ROW = struct.Struct("<IIII")
_POSITION = struct.Struct("<I")


class PreparedValueError(ValueError):
    """Prepared values violate their contract or accepted selection."""


def _adapter[T](kind: type[T]) -> TypeAdapter[T]:
    adapter = TypeAdapter(kind)
    adapter.rebuild(
        _types_namespace={
            "DeliveredCell": DeliveredCell,
            "RecordLocator": RecordLocator,
            "SourceMemberHint": SourceMemberHint,
            "SourceValueWindow": SourceValueWindow,
        }
    )
    return adapter


_DESCRIPTOR = _adapter(SourceValueDescriptor)
_VALUE = _adapter(SourceValue)
_ASSOCIATION = _adapter(SourceValueAssociation)
_VALIDITY = _adapter(SourceValueValidity)


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _FileProof(_Model):
    path: Literal[
        "files/values.sqlite", "files/associations.bin", "files/member-positions.bin"
    ]
    size: int
    sha256: str
    git_blob: str

    @model_validator(mode="after")
    def _valid(self) -> Self:
        if self.size < 0 or not _HASH_RE.fullmatch(self.sha256):
            raise ValueError("invalid prepared value file size or SHA-256")
        if not _COMMIT_RE.fullmatch(self.git_blob):
            raise ValueError("invalid prepared value Git blob ID")
        return self


class _AssociationDefaults(_Model):
    first_row_number: int
    source_file: str
    source_table: str | None
    member_id_field: str | None

    @model_validator(mode="after")
    def _valid(self) -> Self:
        if self.first_row_number < 1 or not self.source_file:
            raise ValueError("invalid association row number or source file")
        return self


class _ManifestDocument(_Model):
    format: Literal["reg-meta-prepared-source-values"] = (
        "reg-meta-prepared-source-values"
    )
    schema_version: Literal[2] = _VERSION
    revision: SourceRevision
    join: SourceValueJoin | None = None
    validity_revision: SourceRevision | None
    descriptor_count: int
    value_count: int
    member_count: int
    item_count: int
    association_count: int
    auxiliary_count: int
    validity_count: int
    association_defaults: _AssociationDefaults | None
    files: tuple[_FileProof, ...]

    @field_validator(
        "descriptor_count",
        "value_count",
        "member_count",
        "item_count",
        "association_count",
        "auxiliary_count",
        "validity_count",
    )
    @classmethod
    def _count(cls, value: int) -> int:
        _limit(value)
        return value

    @model_validator(mode="after")
    def _layout(self) -> Self:
        if tuple(item.path for item in self.files) != _MEMBERS:
            raise ValueError("prepared value file inventory differs from format")
        if self.files[1].size != self.association_count * _ROW.size:
            raise ValueError("association stream size differs from count")
        if self.files[2].size != self.association_count * _POSITION.size:
            raise ValueError("member posting size differs from count")
        if bool(self.association_count) != (self.association_defaults is not None):
            raise ValueError("association defaults differ from stream presence")
        if self.auxiliary_count > self.association_count:
            raise ValueError("more auxiliary records than associations")
        if self.validity_count and self.validity_revision is None:
            raise ValueError("validity records require an explicit source revision")
        if (
            self.join is not None
            and self.join.validity_target == "item"
            and self.validity_revision is None
            and self.join.missing_validity != "unknown"
        ):
            raise ValueError(
                "absent item-validity source cannot imply unrestricted membership"
            )
        return self


class PreparedValueManifest(_ManifestDocument):
    artifact_sha256: str

    @property
    def sha256(self) -> str:
        return self.artifact_sha256


def _manifest(data: bytes) -> PreparedValueManifest:
    document = _ManifestDocument.model_validate_json(data)
    return PreparedValueManifest(
        **{name: getattr(document, name) for name in type(document).model_fields},
        artifact_sha256=hashlib.sha256(data).hexdigest(),
    )


def _limit(count: int) -> None:
    # simplify: uint32 bounds the current 102m-row source; revise the format if exceeded.
    if count < 0 or count > _UINT32_MAX:
        raise PreparedValueError("prepared value count exceeds the uint32 format limit")


def _checked[T](value: T, adapter: TypeAdapter[T]) -> T:
    checked = adapter.validate_json(
        adapter.dump_json(value, warnings="error"), strict=True
    )
    if checked != value:
        raise PreparedValueError("source value evidence changes on contract validation")
    return checked


_DDL = """
PRAGMA user_version=2;
CREATE TABLE payload (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    digest BLOB NOT NULL,
    body TEXT NOT NULL,
    UNIQUE(kind, digest)
);
CREATE TABLE dictionary (
    kind TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    key TEXT NOT NULL,
    payload INTEGER NOT NULL REFERENCES payload(id),
    PRIMARY KEY(kind, ordinal),
    UNIQUE(kind, key)
) WITHOUT ROWID;
CREATE TABLE token (
    kind TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    value TEXT,
    coordinate,
    posting_offset INTEGER,
    posting_count INTEGER,
    PRIMARY KEY(kind, ordinal)
) WITHOUT ROWID;
CREATE INDEX token_value ON token(kind, value);
CREATE INDEX token_coordinate ON token(kind, coordinate);
CREATE TABLE auxiliary (
    ordinal INTEGER PRIMARY KEY,
    payload INTEGER NOT NULL REFERENCES payload(id)
);
CREATE TABLE descriptor_occurrence (
    descriptor INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY(descriptor, ordinal)
) WITHOUT ROWID;
CREATE TABLE validity (
    ordinal INTEGER PRIMARY KEY,
    coordinate,
    locator TEXT NOT NULL,
    payload INTEGER NOT NULL REFERENCES payload(id)
);
CREATE INDEX validity_coordinate ON validity(coordinate);
CREATE INDEX validity_locator ON validity(locator);
"""


def _integer_coordinate(raw: str | None) -> int | None:
    # Unknown/malformed tokens remain in the raw dictionary; never coerce them to 0.
    if raw is None or not re.fullmatch(r"[+]?\d+", raw.strip(), flags=re.ASCII):
        return None
    value = int(raw)
    return value if value <= 2**63 - 1 else None


class _Tokens:
    def __init__(
        self, conn: sqlite3.Connection, kind: str, *, integer: bool = False
    ) -> None:
        self.conn = conn
        self.kind = kind
        self.integer = integer
        self.keys: dict[str | None, int] = {}
        self.counts = array("I")
        if self.counts.itemsize != _POSITION.size:
            raise PreparedValueError(
                "this platform does not provide 32-bit array integers"
            )

    def intern(self, value: str | None) -> int:
        ordinal = self.keys.get(value)
        if ordinal is None:
            ordinal = len(self.keys)
            _limit(ordinal + 1)
            self.keys[value] = ordinal
            self.counts.append(0)
            self.conn.execute(
                "INSERT INTO token(kind, ordinal, value, coordinate) VALUES (?, ?, ?, ?)",
                (
                    self.kind,
                    ordinal,
                    value,
                    _integer_coordinate(value) if self.integer else None,
                ),
            )
        self.counts[ordinal] += 1
        return ordinal


def _write_dictionary[T: (SourceValueDescriptor, SourceValue)](
    conn: sqlite3.Connection,
    payloads: _PayloadWriter,
    kind: str,
    values: Iterable[T],
    adapter: TypeAdapter[T],
) -> dict[str, int]:
    keys: dict[str, int] = {}
    for ordinal, value in enumerate(values):
        _limit(ordinal + 1)
        checked = _checked(value, adapter)
        key = checked.payload_key
        if not key or key in keys:
            raise PreparedValueError(f"empty or duplicate {kind} payload key: {key!r}")
        keys[key] = ordinal
        payload = payloads.put(kind, adapter.dump_python(checked, mode="json"))
        conn.execute(
            "INSERT INTO dictionary VALUES (?, ?, ?, ?)", (kind, ordinal, key, payload)
        )
    return keys


def _check_association(value: SourceValueAssociation) -> None:
    if not isinstance(value, SourceValueAssociation):
        raise PreparedValueError("association must be a SourceValueAssociation")
    if type(value.row_number) is not int or value.row_number < 1:
        raise PreparedValueError("association row_number must be a positive integer")
    for name in ("descriptor_key", "value_key", "source_file"):
        if not isinstance(getattr(value, name), str) or not getattr(value, name):
            raise PreparedValueError(f"association {name} must be a nonempty string")
    for name in (
        "member_id",
        "item_id",
        "source_table",
        "member_id_field",
        "supplied_period",
        "section_period",
    ):
        item = getattr(value, name)
        if item is not None and not isinstance(item, str):
            raise PreparedValueError(f"association {name} must be a string or None")
    if (
        type(value.member_references) is not tuple
        or type(value.member_hints) is not tuple
        or type(value.delivered_cells) is not tuple
    ):
        raise PreparedValueError("association evidence collections must be tuples")
    if (
        value.member_hints
        or value.member_references
        or value.supplied_window is not None
        or value.section_window is not None
        or value.section_locator is not None
        or value.delivered_cells
    ):
        _checked(value, _ASSOCIATION)


def _extra(
    value: SourceValueAssociation, defaults: _AssociationDefaults, ordinal: int
) -> dict[str, Any]:
    baseline = {
        "row_number": defaults.first_row_number + ordinal,
        "source_file": defaults.source_file,
        "source_table": defaults.source_table,
        "member_id_field": defaults.member_id_field,
        "member_hints": (),
        "member_references": (),
        "supplied_window": None,
        "section_window": None,
        "supplied_period": None,
        "section_period": None,
        "section_locator": None,
        "delivered_cells": (),
    }
    changed = {key for key, value_ in baseline.items() if getattr(value, key) != value_}
    if not changed:
        return {}
    return _ASSOCIATION.dump_python(value, mode="json", include=changed)


def _write_postings(
    root: Path, conn: sqlite3.Connection, counts: array, association_count: int
) -> None:
    offsets = array("Q")
    next_offset = 0
    for member, count in enumerate(counts):
        offsets.append(next_offset)
        conn.execute(
            "UPDATE token SET posting_offset=?, posting_count=? WHERE kind='member' AND ordinal=?",
            (next_offset, count, member),
        )
        next_offset += count
    if next_offset != association_count:
        raise PreparedValueError("member counts differ from association count")
    with (root / _POSTINGS).open("w+b") as target:
        target.truncate(association_count * _POSITION.size)
        if not association_count:
            return
        with mmap.mmap(target.fileno(), 0) as positions:
            ordinal = 0
            with (root / _ASSOCIATIONS).open("rb") as source:
                while block := source.read(_ROW.size * 65_536):
                    for member, _, _, _ in _ROW.iter_unpack(block):
                        _POSITION.pack_into(
                            positions, offsets[member] * _POSITION.size, ordinal
                        )
                        offsets[member] += 1
                        ordinal += 1
            if ordinal != association_count:
                raise PreparedValueError("association count changed while indexing")
            positions.flush()


def prepare_source_values(
    output: Path,
    *,
    revision: SourceRevision,
    descriptors: Iterable[SourceValueDescriptor],
    values: Iterable[SourceValue],
    associations: Iterable[SourceValueAssociation],
    validity: Iterable[SourceValueValidity] = (),
    validity_revision: SourceRevision | None = None,
    join: SourceValueJoin | None = None,
) -> PreparedValueManifest:
    """Validate once and atomically publish a new, unaccepted source-value candidate."""
    if output.exists() or output.is_symlink():
        raise PreparedValueError(f"prepared value output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        revision = SourceRevision.model_validate_json(revision.model_dump_json())
        if join is not None:
            join = _checked(join, TypeAdapter(SourceValueJoin))
        if validity_revision is not None:
            validity_revision = SourceRevision.model_validate_json(
                validity_revision.model_dump_json()
            )
        (staging / "files").mkdir()
        with closing(sqlite3.connect(staging / _DATABASE)) as conn:
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(_DDL)
            payloads = _PayloadWriter(conn)
            descriptor_keys = _write_dictionary(
                conn, payloads, "descriptor", descriptors, _DESCRIPTOR
            )
            value_keys = _write_dictionary(conn, payloads, "value", values, _VALUE)
            members = _Tokens(
                conn,
                "member",
                integer=join is not None and join.member_format == "integer",
            )
            items = _Tokens(
                conn,
                "item",
                integer=join is not None and join.validity_target == "item",
            )
            defaults = None
            association_count = auxiliary_count = validity_count = 0
            with (staging / _ASSOCIATIONS).open("wb", buffering=1 << 20) as stream:
                for ordinal, value in enumerate(associations):
                    _limit(ordinal + 1)
                    _check_association(value)
                    if defaults is None:
                        defaults = _AssociationDefaults(
                            first_row_number=value.row_number,
                            source_file=value.source_file,
                            source_table=value.source_table,
                            member_id_field=value.member_id_field,
                        )
                    try:
                        descriptor, code = (
                            descriptor_keys[value.descriptor_key],
                            value_keys[value.value_key],
                        )
                    except KeyError as exc:
                        raise PreparedValueError(
                            f"association references missing dictionary key {exc}"
                        ) from exc
                    stream.write(
                        _ROW.pack(
                            members.intern(value.member_id),
                            items.intern(value.item_id),
                            descriptor,
                            code,
                        )
                    )
                    if join is not None and join.member_target != "native_member":
                        conn.execute(
                            "INSERT INTO descriptor_occurrence VALUES (?, ?)",
                            (descriptor, ordinal),
                        )
                    if extra := _extra(value, defaults, ordinal):
                        conn.execute(
                            "INSERT INTO auxiliary VALUES (?, ?)",
                            (ordinal, payloads.put("association", extra)),
                        )
                        auxiliary_count += 1
                    association_count += 1
            for validity_count, record in enumerate(validity, start=1):
                _limit(validity_count)
                if validity_revision is None:
                    raise PreparedValueError(
                        "validity records require an explicit validity_revision"
                    )
                checked = _checked(record, _VALIDITY)
                if checked.row_number < 1 or not checked.source_file:
                    raise PreparedValueError(
                        "invalid validity row number or source file"
                    )
                conn.execute(
                    "INSERT INTO validity VALUES (?, ?, ?, ?)",
                    (
                        validity_count,
                        _integer_coordinate(checked.item_id)
                        if join is not None and join.validity_target == "item"
                        else None,
                        checked.locator,
                        payloads.put(
                            "validity", _VALIDITY.dump_python(checked, mode="json")
                        ),
                    ),
                )
            _write_postings(staging, conn, members.counts, association_count)
            if conn.execute(
                "PRAGMA foreign_key_check"
            ).fetchone() is not None or conn.execute(
                "PRAGMA integrity_check"
            ).fetchone() != ("ok",):
                raise PreparedValueError(
                    "prepared value database failed integrity validation"
                )
            conn.commit()
        files = []
        for name in _MEMBERS:
            path = staging / name
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
            files.append(
                _FileProof(
                    path=name,
                    size=path.stat().st_size,
                    sha256=_file_sha256(path),
                    git_blob=_git(staging, "hash-object", "--no-filters", name),
                )
            )
        document = _ManifestDocument(
            revision=revision,
            join=join,
            validity_revision=validity_revision,
            descriptor_count=len(descriptor_keys),
            value_count=len(value_keys),
            member_count=len(members.keys),
            item_count=len(items.keys),
            association_count=association_count,
            auxiliary_count=auxiliary_count,
            validity_count=validity_count,
            association_defaults=defaults,
            files=tuple(files),
        )
        data = (_json(document.model_dump(mode="json")) + "\n").encode("utf-8")
        with (staging / _MANIFEST).open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        manifest = _manifest(data)
        staging.replace(output)
        return manifest
    except (AttributeError, OSError, TypeError, ValueError, sqlite3.Error) as exc:
        if isinstance(exc, PreparedValueError):
            raise
        raise PreparedValueError(f"cannot prepare source values: {exc}") from exc
    finally:
        if staging.exists():
            shutil.rmtree(staging)


@dataclass(frozen=True)
class PreparedSourceValues:
    root: Path
    manifest: PreparedValueManifest
    input_commit: str

    def descriptors(self) -> Iterator[SourceValueDescriptor]:
        yield from self._dictionary("descriptor", _DESCRIPTOR)

    def values(self) -> Iterator[SourceValue]:
        yield from self._dictionary("value", _VALUE)

    def _dictionary[T](self, kind: str, adapter: TypeAdapter[T]) -> Iterator[T]:
        with closing(_readonly(self.root / _DATABASE)) as conn:
            for (body,) in conn.execute(
                "SELECT p.body FROM dictionary d JOIN payload p ON p.id=d.payload WHERE d.kind=? ORDER BY d.ordinal",
                (kind,),
            ):
                yield adapter.validate_json(body, strict=True)

    def validity(self) -> Iterator[SourceValueValidity]:
        with closing(_readonly(self.root / _DATABASE)) as conn:
            for (body,) in conn.execute(
                "SELECT p.body FROM validity v JOIN payload p ON p.id=v.payload ORDER BY v.ordinal"
            ):
                yield _VALIDITY.validate_json(body, strict=True)

    def associations(self) -> Iterator[SourceValueAssociation]:
        yield from self._associations(None)

    def lookup_member(self, member_id: str | None) -> Iterator[SourceValueAssociation]:
        """Return every matching native-token occurrence in original source order."""
        if member_id is not None and not isinstance(member_id, str):
            raise PreparedValueError("member lookup requires a string or None")
        yield from self._associations((member_id,))

    @cached_property
    def _references(
        self,
    ) -> tuple[dict[str, tuple[str, ...]], dict[str, tuple[str | None, ...]]]:
        with closing(_readonly(self.root / _DATABASE)) as conn:
            keys = {
                kind: tuple(
                    value
                    for (value,) in conn.execute(
                        "SELECT key FROM dictionary WHERE kind=? ORDER BY ordinal",
                        (kind,),
                    )
                )
                for kind in ("descriptor", "value")
            }
            tokens = {
                kind: tuple(
                    value
                    for (value,) in conn.execute(
                        "SELECT value FROM token WHERE kind=? ORDER BY ordinal", (kind,)
                    )
                )
                for kind in ("member", "item")
            }
        return keys, tokens

    @contextmanager
    def session(self) -> Iterator[PreparedValueSession]:
        """Reuse one connection and mapped streams for a whole source-resolution pass."""
        with ExitStack() as stack:
            conn = stack.enter_context(closing(_readonly(self.root / _DATABASE)))
            streams = []
            if self.manifest.association_count:
                for name in (_ASSOCIATIONS, _POSTINGS):
                    handle = stack.enter_context((self.root / name).open("rb"))
                    streams.append(
                        stack.enter_context(
                            mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
                        )
                    )
            yield PreparedValueSession(self, conn, *(streams or (None, None)))

    def _associations(
        self, member: tuple[str | None] | None
    ) -> Iterator[SourceValueAssociation]:
        with self.session() as session:
            yield from (
                session.associations()
                if member is None
                else session.lookup_member(member[0])
            )


class PreparedValueSession:
    """Bounded cached dictionary reads and indexed source-coordinate lookups."""

    def __init__(
        self,
        source: PreparedSourceValues,
        conn: sqlite3.Connection,
        rows: mmap.mmap | None,
        postings: mmap.mmap | None,
    ) -> None:
        self.source, self.conn, self.rows, self.postings = source, conn, rows, postings
        self._cached_dictionary = lru_cache(maxsize=8192)(self._dictionary)
        self._cached_validity = lru_cache(maxsize=8192)(self._validity_for)
        self._cached_item_coordinate = lru_cache(maxsize=8192)(
            self._native_item_coordinate
        )

    def _dictionary(self, kind: str, key: str) -> Any:
        row = self.conn.execute(
            "SELECT p.body FROM dictionary d JOIN payload p ON p.id=d.payload WHERE d.kind=? AND d.key=?",
            (kind, key),
        ).fetchone()
        if row is None:
            raise PreparedValueError(f"missing prepared {kind} key {key!r}")
        return (_DESCRIPTOR if kind == "descriptor" else _VALUE).validate_json(
            row[0], strict=True
        )

    def descriptor(self, key: str) -> SourceValueDescriptor:
        return self._cached_dictionary("descriptor", key)

    def value(self, key: str) -> SourceValue:
        return self._cached_dictionary("value", key)

    def _decode(self, ordinal: int) -> SourceValueAssociation:
        assert self.rows is not None
        defaults = self.source.manifest.association_defaults
        assert defaults is not None
        keys, tokens = self.source._references
        member, item, descriptor, value = _ROW.unpack_from(
            self.rows, ordinal * _ROW.size
        )
        base: dict[str, Any] = {
            "row_number": defaults.first_row_number + ordinal,
            "source_file": defaults.source_file,
            "source_table": defaults.source_table,
            "member_id_field": defaults.member_id_field,
            "member_id": tokens["member"][member],
            "item_id": tokens["item"][item],
            "descriptor_key": keys["descriptor"][descriptor],
            "value_key": keys["value"][value],
        }
        extra = None
        if self.source.manifest.auxiliary_count:
            extra = self.conn.execute(
                "SELECT p.body FROM auxiliary a JOIN payload p ON p.id=a.payload WHERE a.ordinal=?",
                (ordinal,),
            ).fetchone()
        if extra is None:
            return SourceValueAssociation(**base)
        base.update(json.loads(extra[0]))
        return _ASSOCIATION.validate_json(_json(base), strict=True)

    def associations(self) -> Iterator[SourceValueAssociation]:
        for ordinal in range(self.source.manifest.association_count):
            yield self._decode(ordinal)

    def _lookup(
        self, column: Literal["value", "coordinate"], key: str | int | None
    ) -> Iterator[SourceValueAssociation]:
        if self.postings is None:
            return
        # SQLite otherwise favors the (kind, ordinal) primary key and scans every
        # token of this kind on the full corpus. These indexes belong to this format.
        entries = self.conn.execute(
            f"SELECT posting_offset, posting_count FROM token INDEXED BY token_{column} WHERE kind='member' AND {column} IS ? ORDER BY ordinal",
            (key,),
        )

        def ordinals(offset: int, count: int) -> Iterator[int]:
            assert self.postings is not None
            for position in range(offset, offset + count):
                yield _POSITION.unpack_from(self.postings, position * _POSITION.size)[0]

        for ordinal in heapq.merge(
            *(ordinals(offset, count) for offset, count in entries)
        ):
            yield self._decode(ordinal)

    def lookup_member(self, raw: str | None) -> Iterator[SourceValueAssociation]:
        if raw is not None and not isinstance(raw, str):
            raise PreparedValueError("member lookup requires a string or None")
        yield from self._lookup("value", raw)

    def lookup_native_member(self, coordinate: int) -> Iterator[SourceValueAssociation]:
        if type(coordinate) is not int:
            raise PreparedValueError(
                "native member lookup requires an integer coordinate"
            )
        yield from self._lookup("coordinate", coordinate)

    def member_coordinates(self) -> Iterator[tuple[str | None, int | None, int]]:
        """Expose every raw token and occurrence count, including malformed tokens."""
        yield from self.conn.execute(
            "SELECT value, coordinate, posting_count FROM token WHERE kind='member' ORDER BY ordinal"
        )

    def lookup_descriptor(self, key: str) -> Iterator[SourceValueAssociation]:
        join = self.source.manifest.join
        if join is None or join.member_target == "native_member":
            raise PreparedValueError(
                "descriptor lookup requires a prepared named-list index"
            )
        for (ordinal,) in self.conn.execute(
            "SELECT o.ordinal FROM descriptor_occurrence o JOIN dictionary d ON d.kind='descriptor' AND d.ordinal=o.descriptor WHERE d.key=? ORDER BY o.ordinal",
            (key,),
        ):
            yield self._decode(ordinal)

    def native_item_coordinate(self, raw: str | None) -> int | None:
        return self._cached_item_coordinate(raw)

    def _native_item_coordinate(self, raw: str | None) -> int | None:
        row = self.conn.execute(
            "SELECT coordinate FROM token INDEXED BY token_value WHERE kind='item' AND value IS ?",
            (raw,),
        ).fetchone()
        return row[0] if row else None

    def validity_for(
        self, *, item_id: str | None = None, locator: str | None = None
    ) -> tuple[SourceValueValidity, ...]:
        join = self.source.manifest.join
        return (
            self._cached_validity(item_id, None)
            if join is not None and join.validity_target == "item"
            else self._cached_validity(None, locator)
        )

    def _validity_for(
        self, item_id: str | None, locator: str | None
    ) -> tuple[SourceValueValidity, ...]:
        join = self.source.manifest.join
        if join is None or join.validity_target == "none":
            return ()
        if join.validity_target == "item":
            coordinate = self.native_item_coordinate(item_id)
            if coordinate is None:
                return ()
            column, key = "coordinate", coordinate
        else:
            column, key = "locator", locator
        return tuple(
            _VALIDITY.validate_json(body, strict=True)
            for (body,) in self.conn.execute(
                f"SELECT p.body FROM validity v JOIN payload p ON p.id=v.payload WHERE v.{column} IS ? ORDER BY v.ordinal",
                (key,),
            )
        )


def prepared_value_paths(root: Path) -> tuple[Path, ...]:
    return tuple(root / name for name in (_MANIFEST, *_MEMBERS))


def open_prepared_source_values(
    path: Path, *, expected_sha256: str, input_commit: str
) -> PreparedSourceValues:
    """Open accepted values without hashing or semantically revalidating their data."""
    try:
        selection = read_accepted_manifest(
            path, expected_sha256=expected_sha256, input_commit=input_commit
        )
        manifest = _manifest(selection.manifest_bytes)
        check_accepted_files(
            selection,
            {file.path: (file.size, file.git_blob) for file in manifest.files},
        )
        with closing(_readonly(selection.root / _DATABASE)) as conn:
            if conn.execute("PRAGMA user_version").fetchone() != (_VERSION,):
                raise PreparedValueError("unsupported prepared value database version")
            conn.execute("SELECT kind, ordinal, key, payload FROM dictionary LIMIT 0")
            conn.execute(
                "SELECT coordinate, posting_offset, posting_count FROM token LIMIT 0"
            )
        return PreparedSourceValues(
            root=selection.root, manifest=manifest, input_commit=input_commit
        )
    except (SnapshotError, OSError, ValueError, sqlite3.Error) as exc:
        if isinstance(exc, PreparedValueError):
            raise
        raise PreparedValueError(
            f"cannot open accepted prepared values: {exc}"
        ) from exc


__all__ = [
    "PreparedSourceValues",
    "PreparedValueError",
    "PreparedValueManifest",
    "PreparedValueSession",
    "open_prepared_source_values",
    "prepare_source_values",
    "prepared_value_paths",
]
