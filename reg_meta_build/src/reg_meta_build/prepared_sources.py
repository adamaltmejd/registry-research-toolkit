"""Accepted, indexed source observations prepared once for deterministic curation.

SQLite stores ordered occurrences and interned evidence payloads. It is the source
record representation, not a catalog or a cache of resolved value-set memberships.
Large value-association streams belong to their existing pinned compact storage;
this module deliberately does not expand them into database rows.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from contextlib import closing, contextmanager
from dataclasses import dataclass
from functools import lru_cache
from itertools import groupby
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from reg_meta_build._accepted_prepared import (
    check_accepted_files,
    read_accepted_manifest,
)
from reg_meta_build.db import _file_sha256
from reg_meta_build.input_snapshot import SnapshotError, _git
from reg_meta_build.source_coordinates import _coordinate_key, native_variable_key
from reg_meta_build.source_records import (
    CodeSetReference,
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    SourceCoordinate,
    SourceEvidenceRow,
    SourceEvidenceTable,
    SourceField,
    SourceFieldCells,
    SourceFields,
    SourceParentObservation,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
)
from reg_meta_build.source_support import SupportTarget, support_join_key

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_support import SourceSupportJoin

_FORMAT = "reg-meta-prepared-source-records"
_SCHEMA_VERSION = 6
_MANIFEST = "manifest.json"
_DATABASE = "files/records.sqlite"
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT_RE = re.compile(r"[0-9a-f]{40}\Z")


class PreparedSourceError(ValueError):
    """Prepared source records violate their contract or accepted selection."""


class _PreparedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _ManifestDocument(_PreparedModel):
    format: Literal["reg-meta-prepared-source-records"] = _FORMAT
    schema_version: Literal[6] = _SCHEMA_VERSION
    scope: str
    partial: Literal[True] = True
    record_count: int
    table_count: int
    table_row_count: int
    revisions: tuple[SourceRevision, ...]
    database_path: Literal["files/records.sqlite"] = _DATABASE
    database_size: int
    database_sha256: str
    database_git_blob: str

    @field_validator("scope")
    @classmethod
    def _non_empty_scope(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("prepared source scope must be non-empty")
        return value

    @field_validator("record_count", "table_count", "table_row_count", "database_size")
    @classmethod
    def _nonnegative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("prepared source counts and sizes cannot be negative")
        return value

    @field_validator("database_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        if not _HASH_RE.fullmatch(value):
            raise ValueError("database_sha256 must be a lowercase SHA-256 value")
        return value

    @field_validator("database_git_blob")
    @classmethod
    def _blob(cls, value: str) -> str:
        if not _COMMIT_RE.fullmatch(value):
            raise ValueError(
                "database_git_blob must be a lowercase Git SHA-1 object ID"
            )
        return value


class PreparedSourceManifest(_ManifestDocument):
    """Small manifest and its exact digest; payload hashes are preparation proofs."""

    artifact_sha256: str

    @field_validator("artifact_sha256")
    @classmethod
    def _valid_artifact_sha256(cls, value: str) -> str:
        if not _HASH_RE.fullmatch(value):
            raise ValueError("artifact_sha256 must be a lowercase SHA-256 value")
        return value

    @property
    def sha256(self) -> str:
        return self.artifact_sha256


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _manifest(payload: bytes) -> PreparedSourceManifest:
    try:
        document = _ManifestDocument.model_validate_json(payload)
    except ValueError as exc:
        raise PreparedSourceError(f"invalid prepared source manifest: {exc}") from exc
    return PreparedSourceManifest(
        **document.model_dump(mode="python"),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


_DDL = """
PRAGMA user_version=6;
CREATE TABLE payload (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    digest BLOB NOT NULL,
    body TEXT NOT NULL,
    UNIQUE(kind, digest)
);
CREATE TABLE occurrence (
    ordinal INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    semantic_key TEXT NOT NULL,
    record_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    register_payload INTEGER NOT NULL REFERENCES payload(id),
    variant_payload INTEGER NOT NULL REFERENCES payload(id),
    variant_references_payload INTEGER NOT NULL REFERENCES payload(id),
    population_payload INTEGER NOT NULL REFERENCES payload(id),
    variable_payload INTEGER NOT NULL REFERENCES payload(id),
    member_payload INTEGER NOT NULL REFERENCES payload(id),
    native_payload INTEGER NOT NULL REFERENCES payload(id),
    scope_payload INTEGER NOT NULL REFERENCES payload(id),
    period_payload INTEGER NOT NULL REFERENCES payload(id),
    fields_payload INTEGER NOT NULL REFERENCES payload(id),
    language TEXT,
    original_period_text TEXT,
    context_payload INTEGER NOT NULL REFERENCES payload(id),
    codes_payload INTEGER NOT NULL REFERENCES payload(id),
    parents_payload INTEGER NOT NULL REFERENCES payload(id),
    family_payload INTEGER REFERENCES payload(id)
);
CREATE INDEX occurrence_source_key ON occurrence(source, semantic_key);
CREATE INDEX occurrence_native_family ON occurrence(source, family_payload, ordinal);
CREATE TABLE locator (
    occurrence INTEGER NOT NULL REFERENCES occurrence(ordinal),
    position INTEGER NOT NULL,
    physical_file TEXT NOT NULL,
    physical_table TEXT NOT NULL,
    physical_record TEXT NOT NULL,
    cells_prefix TEXT NOT NULL,
    cells_payload INTEGER NOT NULL REFERENCES payload(id),
    PRIMARY KEY(occurrence, position)
) WITHOUT ROWID;
CREATE TABLE delivered_cell (
    occurrence INTEGER NOT NULL REFERENCES occurrence(ordinal),
    position INTEGER NOT NULL,
    payload INTEGER NOT NULL REFERENCES payload(id),
    PRIMARY KEY(occurrence, position)
) WITHOUT ROWID;
CREATE TABLE evidence_table (
    ordinal INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    name TEXT NOT NULL,
    row_count INTEGER NOT NULL
);
CREATE INDEX evidence_table_source ON evidence_table(source);
CREATE TABLE evidence_row (
    ordinal INTEGER PRIMARY KEY,
    table_ordinal INTEGER NOT NULL REFERENCES evidence_table(ordinal),
    position INTEGER NOT NULL,
    role TEXT NOT NULL,
    semantic_key TEXT NOT NULL,
    physical_file TEXT NOT NULL,
    physical_table TEXT NOT NULL,
    physical_record TEXT NOT NULL,
    cells_prefix TEXT NOT NULL,
    cells_payload INTEGER NOT NULL REFERENCES payload(id),
    UNIQUE(table_ordinal, position)
);
CREATE TABLE evidence_cell (
    row_ordinal INTEGER NOT NULL REFERENCES evidence_row(ordinal),
    position INTEGER NOT NULL,
    payload INTEGER NOT NULL REFERENCES payload(id),
    PRIMARY KEY(row_ordinal, position)
) WITHOUT ROWID;
"""


class _PayloadWriter:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        # A bounded cache speeds repeated common cells without retaining the corpus.
        self.intern = lru_cache(maxsize=8192)(self._intern)

    def _intern(self, kind: str, body: str) -> int:
        digest = hashlib.sha256(body.encode("utf-8")).digest()
        existing = self.conn.execute(
            "SELECT id, body FROM payload WHERE kind = ? AND digest = ?", (kind, digest)
        ).fetchone()
        if existing is not None:
            if existing[1] != body:
                raise PreparedSourceError("prepared payload content-key collision")
            return existing[0]
        cursor = self.conn.execute(
            "INSERT INTO payload(kind, digest, body) VALUES (?, ?, ?)",
            (kind, digest, body),
        )
        assert cursor.lastrowid is not None
        return cursor.lastrowid

    def put(self, kind: str, value: Any) -> int:
        if isinstance(value, BaseModel):
            value = value.model_dump(mode="json", exclude_defaults=True)
        return self.intern(kind, _json(value))

    def fields(self, fields: SourceFields) -> int:
        # Edition-specific combinations share the individual field observations.
        # Whole-field-set JSON duplicated over a gigabyte in the maintained corpus.
        return self.put(
            "fields",
            {
                name: self.put("field", value)
                for name in SourceFields.model_fields
                if (value := getattr(fields, name)) is not None
            },
        )

    def parents(self, parents: tuple[SourceParentObservation, ...]) -> int:
        return self.put(
            "parents",
            [
                self.put(
                    "parent",
                    {
                        "kind": parent.kind,
                        "coordinate": self.put("coordinate", parent.coordinate),
                        "register": self.put("coordinate", parent.register_name),
                        "variant": self.put("coordinate", parent.variant)
                        if parent.variant is not None
                        else None,
                        "edition": self.put("coordinate", parent.edition)
                        if parent.edition is not None
                        else None,
                        "fields": self.fields(parent.fields),
                        "field_cells": [
                            mapping.model_dump(mode="json")
                            for mapping in parent.field_cells
                        ],
                    },
                )
                for parent in parents
            ],
        )


def _write_record(
    conn: sqlite3.Connection,
    payloads: _PayloadWriter,
    ordinal: int,
    record: SourceRecord,
) -> None:
    subject = record.subject
    key = _json(record.locators[0].semantic_record_key)
    conn.execute(
        "INSERT INTO occurrence VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            ordinal,
            record.source,
            key,
            record.record_id,
            record.source_revision_id,
            subject.provider,
            payloads.put("coordinate", subject.register_name),
            payloads.put("coordinate", subject.variant),
            payloads.put(
                "coordinates",
                [value.model_dump(mode="json") for value in subject.variant_references],
            ),
            payloads.put("coordinate", subject.population),
            payloads.put("coordinate", subject.variable),
            payloads.put("coordinate", subject.member),
            payloads.put("native", subject.native),
            payloads.put("scope", record.edition_scope),
            payloads.put("scope", record.edition_period_scope),
            payloads.fields(record.fields),
            record.language,
            record.original_period_text,
            payloads.put("strings", record.context),
            payloads.put(
                "codes",
                [code.model_dump(mode="json") for code in record.code_set_references],
            ),
            payloads.parents(record.parent_facts),
            payloads.put("native_family", family)
            if (family := native_variable_key(record)) is not None
            else None,
        ),
    )
    conn.executemany(
        "INSERT INTO locator VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            (ordinal, position, *_locator_values(payloads, locator))
            for position, locator in enumerate(record.locators)
        ),
    )
    _write_cells(
        conn,
        payloads,
        "INSERT INTO delivered_cell VALUES (?, ?, ?)",
        ordinal,
        record.delivered_cells,
    )


def _locator_values(
    payloads: _PayloadWriter, locator: RecordLocator
) -> tuple[str, str, str, str, int]:
    prefix = os.path.commonprefix(locator.physical_cells)
    return (
        locator.physical_file,
        locator.physical_table,
        locator.physical_record,
        prefix,
        payloads.put(
            "strings", tuple(cell[len(prefix) :] for cell in locator.physical_cells)
        ),
    )


def _write_cells(
    conn: sqlite3.Connection,
    payloads: _PayloadWriter,
    statement: str,
    owner: int,
    cells: tuple[DeliveredCell, ...],
) -> None:
    conn.executemany(
        statement,
        (
            (owner, position, payloads.put("cell", cell))
            for position, cell in enumerate(cells)
        ),
    )


def _write_table(
    conn: sqlite3.Connection,
    payloads: _PayloadWriter,
    ordinal: int,
    table: SourceEvidenceTable,
) -> None:
    conn.execute(
        "INSERT INTO evidence_table VALUES (?, ?, ?, ?, ?)",
        (ordinal, table.source, table.source_revision_id, table.name, len(table.rows)),
    )
    for position, row in enumerate(table.rows):
        cursor = conn.execute(
            "INSERT INTO evidence_row(table_ordinal, position, role, semantic_key, physical_file, physical_table, physical_record, cells_prefix, cells_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                ordinal,
                position,
                row.role,
                _json(row.locator.semantic_record_key),
                *_locator_values(payloads, row.locator),
            ),
        )
        assert cursor.lastrowid is not None
        _write_cells(
            conn,
            payloads,
            "INSERT INTO evidence_cell VALUES (?, ?, ?)",
            cursor.lastrowid,
            row.cells,
        )


def _require_revision(
    source: str,
    revision_id: str,
    revisions: dict[str, SourceRevision],
    *,
    kind: str,
) -> None:
    revision = revisions.get(revision_id)
    if revision is None:
        raise PreparedSourceError(
            f"{kind} revision absent from artifact: {revision_id}"
        )
    if revision.dataset != source:
        raise PreparedSourceError(f"{kind} and revision name different sources")


def prepare_source_records(
    output: Path,
    *,
    records: Iterable[SourceRecord],
    revisions: tuple[SourceRevision, ...],
    scope: str,
    tables: Iterable[SourceEvidenceTable] = (),
) -> PreparedSourceManifest:
    """Validate a streamed selection and atomically publish a new candidate directory.

    Acceptance is a later Git commit, as with input snapshots. Existing candidates
    are never overwritten, so failed preparation cannot disturb an accepted input.
    """
    if output.exists() or output.is_symlink():
        raise PreparedSourceError(f"prepared source output already exists: {output}")
    if not scope.strip():
        raise PreparedSourceError("prepared source scope must be non-empty")
    try:
        validated_revisions = tuple(
            sorted(
                (
                    SourceRevision.model_validate_json(revision.model_dump_json())
                    for revision in revisions
                ),
                key=lambda revision: revision.revision_id,
            )
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise PreparedSourceError(f"invalid SourceRevision contract: {exc}") from exc
    by_revision = {revision.revision_id: revision for revision in validated_revisions}
    if len(by_revision) != len(validated_revisions):
        raise PreparedSourceError("duplicate source revision in prepared artifact")

    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        database = staging / _DATABASE
        database.parent.mkdir()
        count = table_count = table_row_count = 0
        with closing(sqlite3.connect(database)) as conn:
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(_DDL)
            payloads = _PayloadWriter(conn)
            for count, record in enumerate(records, start=1):
                try:
                    checked = SourceRecord.model_validate_json(record.model_dump_json())
                except (AttributeError, TypeError, ValueError) as exc:
                    raise PreparedSourceError(
                        f"source record {count} violates the SourceRecord contract: {exc}"
                    ) from exc
                _require_revision(
                    checked.source,
                    checked.source_revision_id,
                    by_revision,
                    kind="source record",
                )
                _write_record(conn, payloads, count, checked)
            for table_count, table in enumerate(tables, start=1):
                try:
                    checked_table = SourceEvidenceTable.model_validate_json(
                        table.model_dump_json()
                    )
                except (AttributeError, TypeError, ValueError) as exc:
                    raise PreparedSourceError(
                        f"source table {table_count} violates the SourceEvidenceTable contract: {exc}"
                    ) from exc
                _require_revision(
                    checked_table.source,
                    checked_table.source_revision_id,
                    by_revision,
                    kind="source table",
                )
                _write_table(conn, payloads, table_count, checked_table)
                table_row_count += len(checked_table.rows)
            if conn.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise PreparedSourceError(
                    "prepared records contain an invalid payload reference"
                )
            if conn.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                raise PreparedSourceError(
                    "prepared records database failed integrity validation"
                )
            conn.commit()
        with database.open("rb") as handle:
            os.fsync(handle.fileno())
        document = _ManifestDocument(
            scope=scope,
            record_count=count,
            table_count=table_count,
            table_row_count=table_row_count,
            revisions=validated_revisions,
            database_size=database.stat().st_size,
            database_sha256=_file_sha256(database),
            database_git_blob=_git(staging, "hash-object", "--no-filters", _DATABASE),
        )
        manifest_bytes = (_json(document.model_dump(mode="json")) + "\n").encode(
            "utf-8"
        )
        with (staging / _MANIFEST).open("wb") as handle:
            handle.write(manifest_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        staging.replace(output)
        return _manifest(manifest_bytes)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


_PAYLOAD_MODELS: dict[str, type[BaseModel]] = {
    "coordinate": SourceCoordinate,
    "native": NativeCoordinates,
    "scope": TemporalScope,
    "field": SourceField,
    "cell": DeliveredCell,
}


def _readonly(database: Path) -> sqlite3.Connection:
    return sqlite3.connect(database.as_uri() + "?mode=ro&immutable=1", uri=True)


def _payload_reader(conn: sqlite3.Connection) -> Callable[[int, str], Any]:
    @lru_cache(maxsize=8192)
    def payload(key: int, kind: str) -> Any:
        row = conn.execute(
            "SELECT kind, body FROM payload WHERE id = ?", (key,)
        ).fetchone()
        if row is None or row["kind"] != kind:
            raise PreparedSourceError(f"missing or wrong-kind prepared payload {key}")
        if kind in _PAYLOAD_MODELS:
            return _PAYLOAD_MODELS[kind].model_validate_json(row["body"])
        decoded = json.loads(row["body"])
        if kind == "native_family":
            if not isinstance(decoded, list) or any(
                type(value) not in {str, int} for value in decoded
            ):
                raise PreparedSourceError("invalid prepared native-family key")
            return tuple(decoded)
        if kind == "parents":
            if not isinstance(decoded, list) or any(
                type(value) is not int for value in decoded
            ):
                raise PreparedSourceError("invalid prepared parent references")
            return tuple(payload(value, "parent") for value in decoded)
        if kind == "parent":
            if not isinstance(decoded, dict) or set(decoded) != {
                "kind",
                "coordinate",
                "register",
                "variant",
                "edition",
                "fields",
                "field_cells",
            }:
                raise PreparedSourceError("invalid prepared parent observation")
            if any(
                type(decoded[name]) is not int
                for name in ("coordinate", "register", "fields")
            ) or any(
                decoded[name] is not None and type(decoded[name]) is not int
                for name in ("variant", "edition")
            ):
                raise PreparedSourceError("invalid prepared parent fragment references")
            if not isinstance(decoded["field_cells"], list):
                raise PreparedSourceError("invalid prepared parent field cells")
            return SourceParentObservation(
                kind=decoded["kind"],
                coordinate=payload(decoded["coordinate"], "coordinate"),
                register=payload(decoded["register"], "coordinate"),
                variant=payload(decoded["variant"], "coordinate")
                if decoded["variant"] is not None
                else None,
                edition=payload(decoded["edition"], "coordinate")
                if decoded["edition"] is not None
                else None,
                fields=payload(decoded["fields"], "fields"),
                field_cells=tuple(
                    SourceFieldCells.model_validate_json(_json(mapping))
                    for mapping in decoded["field_cells"]
                ),
            )
        if kind == "fields":
            if not isinstance(decoded, dict) or any(
                name not in SourceFields.model_fields or type(value) is not int
                for name, value in decoded.items()
            ):
                raise PreparedSourceError("invalid prepared field references")
            return SourceFields.model_validate(
                {name: payload(value, "field") for name, value in decoded.items()}
            )
        if kind == "strings":
            if not isinstance(decoded, list) or any(
                not isinstance(value, str) for value in decoded
            ):
                raise PreparedSourceError("invalid prepared string tuple")
            return tuple(decoded)
        if kind == "codes":
            return tuple(
                CodeSetReference.model_validate_json(_json(value)) for value in decoded
            )
        if kind == "coordinates":
            return tuple(
                SourceCoordinate.model_validate_json(_json(value)) for value in decoded
            )
        raise PreparedSourceError(f"unknown prepared payload kind {kind}")

    return payload


@contextmanager
def _decoded_database(
    root: Path, manifest: PreparedSourceManifest
) -> Iterator[tuple[sqlite3.Connection, Callable[[int, str], Any]]]:
    try:
        with closing(_readonly(root / manifest.database_path)) as conn:
            conn.row_factory = sqlite3.Row
            yield conn, _payload_reader(conn)
    except (sqlite3.Error, ValueError, KeyError, TypeError) as exc:
        if isinstance(exc, PreparedSourceError):
            raise
        raise PreparedSourceError(
            f"cannot decode prepared source evidence: {exc}"
        ) from exc


def _read_locator(
    row: sqlite3.Row, semantic_key: tuple[str, ...], payload: Callable[[int, str], Any]
) -> RecordLocator:
    return RecordLocator(
        semantic_record_key=semantic_key,
        physical_file=row["physical_file"],
        physical_table=row["physical_table"],
        physical_record=row["physical_record"],
        physical_cells=tuple(
            row["cells_prefix"] + suffix
            for suffix in payload(row["cells_payload"], "strings")
        ),
    )


def _read_cells(
    conn: sqlite3.Connection,
    payload: Callable[[int, str], Any],
    statement: str,
    owner: int,
) -> tuple[DeliveredCell, ...]:
    return tuple(payload(cell[0], "cell") for cell in conn.execute(statement, (owner,)))


def _read_record(
    conn: sqlite3.Connection,
    payload: Callable[[int, str], Any],
    row: sqlite3.Row,
) -> SourceRecord:
    semantic_key = tuple(json.loads(row["semantic_key"]))
    locators = tuple(
        _read_locator(locator, semantic_key, payload)
        for locator in conn.execute(
            "SELECT * FROM locator WHERE occurrence = ? ORDER BY position",
            (row["ordinal"],),
        )
    )
    subject = SourceSubject.model_construct(
        provider=row["provider"],
        register_name=payload(row["register_payload"], "coordinate"),
        variant=payload(row["variant_payload"], "coordinate"),
        variant_references=payload(row["variant_references_payload"], "coordinates"),
        population=payload(row["population_payload"], "coordinate"),
        variable=payload(row["variable_payload"], "coordinate"),
        member=payload(row["member_payload"], "coordinate"),
        native=payload(row["native_payload"], "native"),
    )
    # Identity and revision membership were exhaustively proved during
    # preparation. The accepted Git/manifest boundary permits decoding
    # this exact immutable representation without repeating those hashes.
    return SourceRecord.model_construct(
        record_id=row["record_id"],
        source=row["source"],
        source_revision_id=row["revision_id"],
        locators=locators,
        subject=subject,
        edition_scope=payload(row["scope_payload"], "scope"),
        edition_period_scope=payload(row["period_payload"], "scope"),
        fields=payload(row["fields_payload"], "fields"),
        parent_facts=payload(row["parents_payload"], "parents"),
        language=row["language"],
        code_set_references=payload(row["codes_payload"], "codes"),
        original_period_text=row["original_period_text"],
        context=payload(row["context_payload"], "strings"),
        delivered_cells=_read_cells(
            conn,
            payload,
            "SELECT payload FROM delivered_cell WHERE occurrence = ? ORDER BY position",
            row["ordinal"],
        ),
    )


@dataclass(frozen=True)
class PreparedSourceRecords:
    """A reusable accepted reader; records are decoded only when iterated or selected."""

    root: Path
    manifest: PreparedSourceManifest
    input_commit: str

    @property
    def records(self) -> Iterator[SourceRecord]:
        return self.iter_records()

    def __len__(self) -> int:
        return self.manifest.record_count

    def lookup(
        self, source: str, semantic_record_key: tuple[str, ...]
    ) -> Iterator[SourceRecord]:
        """Select all exact-key occurrences in original order, including duplicates."""
        return self._iter(
            "WHERE source = ? AND semantic_key = ?",
            (source, _json(semantic_record_key)),
        )

    def iter_records(self, *, source: str | None = None) -> Iterator[SourceRecord]:
        return self._iter(
            "WHERE source = ?" if source is not None else "",
            (source,) if source is not None else (),
        )

    def iter_support_targets(
        self,
        joins: tuple[SourceSupportJoin, ...],
    ) -> Iterator[SupportTarget]:
        """Read complete join cardinalities without hydrating physical source records.

        DISTINCT removes only repetitions of the same original native identity and
        required join fields. Unknown identities remain None candidates. This is a
        read projection of the accepted store, not source cleaning or curation.
        Ordinary resolution still visits every physical occurrence separately.
        """
        columns = {
            "register_name": "register_payload",
            "variant_name": "variant_payload",
            "variable_name": "variable_payload",
            "variable_id": "variable_payload",
            "column_name": "fields_payload",
        }
        with _decoded_database(self.root, self.manifest) as (conn, payload):
            for join in joins:
                selected = tuple(sorted({columns[name] for name in join.keys}))
                projection = ", ".join(("family_payload", *selected))
                for source in join.target_sources:
                    rows = conn.execute(
                        f"SELECT DISTINCT {projection} FROM occurrence "
                        f"WHERE source=? ORDER BY {projection}",
                        (source,),
                    )
                    for row in rows:
                        key = support_join_key(
                            join,
                            coordinates={
                                name: payload(row[columns[name]], "coordinate")
                                for name in join.keys
                                if name != "column_name"
                            },
                            column=payload(row["fields_payload"], "fields").column_name
                            if "column_name" in join.keys
                            else None,
                        )
                        yield SupportTarget(
                            join.source,
                            source,
                            payload(row["family_payload"], "native_family")
                            if row["family_payload"] is not None
                            else None,
                            key,
                        )

    def iter_native_families(
        self,
        source: str,
    ) -> Iterator[tuple[NativeKey, tuple[SourceRecord, ...]]]:
        """Decode one source-native family at a time using the cold-prepared index.

        Families exclude the variant so callers see the complete native variable.
        Native IDs take precedence over supplied names; name-only source identities
        remain scoped to their exact source/register. This is grouping of evidence,
        not a declaration that each group is one catalog variable. Duplicate rows
        keep source order. Records without a complete family are available separately.
        """
        with _decoded_database(self.root, self.manifest) as (conn, payload):
            rows = conn.execute(
                "SELECT * FROM occurrence WHERE source=? AND family_payload IS NOT NULL "
                "ORDER BY family_payload, ordinal",
                (source,),
            )
            for family, members in groupby(rows, key=lambda row: row["family_payload"]):
                yield (
                    payload(family, "native_family"),
                    tuple(_read_record(conn, payload, row) for row in members),
                )

    def iter_register_slices(
        self, source: str
    ) -> Iterator[tuple[NativeKey | None, tuple[SourceRecord, ...]]]:
        """Read complete source-native registers for overlapping occurrence decisions.

        Register labels may differ under one native identity. Include every physical
        row, including parent-only and unknown-variable rows. Unknown registers have
        a separate slice; no source evidence is assigned to a fabricated register.
        The small temporary relation orders the read without changing accepted data.
        """
        with _decoded_database(self.root, self.manifest) as (conn, payload):
            partitions: dict[tuple[str, NativeKey | None], int] = {}
            mappings = []
            for row in conn.execute(
                "SELECT DISTINCT provider, register_payload FROM occurrence "
                "WHERE source=? ORDER BY provider, register_payload",
                (source,),
            ):
                coordinate = _coordinate_key(
                    payload(row["register_payload"], "coordinate")
                )
                register = (
                    (source, row["provider"], "register", *coordinate)
                    if coordinate is not None
                    else None
                )
                partition = partitions.setdefault(
                    (row["provider"], register), len(partitions)
                )
                mappings.append((row["provider"], row["register_payload"], partition))
            conn.execute(
                "CREATE TEMP TABLE register_partition (provider TEXT, register_payload INTEGER, "
                "partition INTEGER, PRIMARY KEY (provider, register_payload)) WITHOUT ROWID"
            )
            conn.executemany(
                "INSERT INTO register_partition VALUES (?, ?, ?)", mappings
            )
            keys = [register for _provider, register in partitions]
            rows = conn.execute(
                "SELECT occurrence.*, register_partition.partition FROM occurrence "
                "JOIN register_partition USING (provider, register_payload) "
                "WHERE source=? ORDER BY register_partition.partition, ordinal",
                (source,),
            )
            for partition, members in groupby(rows, key=lambda row: row["partition"]):
                yield (
                    keys[partition],
                    tuple(_read_record(conn, payload, row) for row in members),
                )

    def iter_without_native_family(self, source: str) -> Iterator[SourceRecord]:
        """Retain declarations and incomplete identities outside ordinary families."""
        return self._iter("WHERE source=? AND family_payload IS NULL", (source,))

    def _iter(self, where: str, parameters: tuple[str, ...]) -> Iterator[SourceRecord]:
        with _decoded_database(self.root, self.manifest) as (conn, payload):
            seen = 0
            for row in conn.execute(
                f"SELECT * FROM occurrence {where} ORDER BY ordinal", parameters
            ):
                yield _read_record(conn, payload, row)
                seen += 1
            if not where and seen != self.manifest.record_count:
                raise PreparedSourceError(
                    "prepared source record count differs from manifest"
                )

    def iter_tables(
        self, *, source: str | None = None
    ) -> Iterator[SourceEvidenceTable]:
        """Decode one table at a time, preserving table and row occurrence order."""
        where = "WHERE source = ?" if source is not None else ""
        parameters = (source,) if source is not None else ()
        with _decoded_database(self.root, self.manifest) as (conn, payload):
            table_count = row_count = 0
            for table in conn.execute(
                f"SELECT * FROM evidence_table {where} ORDER BY ordinal", parameters
            ):
                rows = tuple(
                    SourceEvidenceRow(
                        locator=_read_locator(
                            row, tuple(json.loads(row["semantic_key"])), payload
                        ),
                        role=row["role"],
                        cells=_read_cells(
                            conn,
                            payload,
                            "SELECT payload FROM evidence_cell WHERE row_ordinal = ? ORDER BY position",
                            row["ordinal"],
                        ),
                    )
                    for row in conn.execute(
                        "SELECT * FROM evidence_row WHERE table_ordinal = ? ORDER BY position",
                        (table["ordinal"],),
                    )
                )
                if len(rows) != table["row_count"]:
                    raise PreparedSourceError(
                        "prepared evidence table row count differs from preparation"
                    )
                yield SourceEvidenceTable.model_construct(
                    source=table["source"],
                    source_revision_id=table["revision_id"],
                    name=table["name"],
                    rows=rows,
                )
                table_count += 1
                row_count += len(rows)
            if source is None and (
                table_count != self.manifest.table_count
                or row_count != self.manifest.table_row_count
            ):
                raise PreparedSourceError(
                    "prepared evidence table/row counts differ from manifest"
                )


def prepared_source_paths(root: Path) -> tuple[Path, ...]:
    """Return immutable prepared members for output-collision checks before opening."""
    return root / _MANIFEST, root / _DATABASE


def open_prepared_source_records(
    path: Path, *, expected_sha256: str, input_commit: str
) -> PreparedSourceRecords:
    """Quick-check an accepted Git selection, without hashing or scanning its data."""
    try:
        selection = read_accepted_manifest(
            path, expected_sha256=expected_sha256, input_commit=input_commit
        )
        manifest = _manifest(selection.manifest_bytes)
        check_accepted_files(
            selection,
            {_DATABASE: (manifest.database_size, manifest.database_git_blob)},
        )
        root = selection.root
        database = root / _DATABASE
        with closing(_readonly(database)) as conn:
            if conn.execute("PRAGMA user_version").fetchone() != (_SCHEMA_VERSION,):
                raise PreparedSourceError(
                    "unsupported prepared records database version"
                )
            conn.execute("SELECT ordinal, source, semantic_key FROM occurrence LIMIT 0")
            conn.execute(
                "SELECT ordinal, source, name, row_count FROM evidence_table LIMIT 0"
            )
    except (SnapshotError, OSError, sqlite3.Error, ValueError) as exc:
        if isinstance(exc, PreparedSourceError):
            raise
        raise PreparedSourceError(
            f"cannot open accepted prepared sources: {exc}"
        ) from exc
    return PreparedSourceRecords(
        root=root, manifest=manifest, input_commit=input_commit
    )


__all__ = [
    "PreparedSourceError",
    "PreparedSourceManifest",
    "PreparedSourceRecords",
    "open_prepared_source_records",
    "prepare_source_records",
    "prepared_source_paths",
]
