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
from contextlib import closing
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from reg_meta_build.db import _file_sha256
from reg_meta_build.input_snapshot import (
    SnapshotError,
    _committed_inventory_sizes,
    _git,
    _git_bytes,
    _index_tags,
    _snapshot_repo_path,
    clean_git_commit,
    input_bundle_repository,
)
from reg_meta_build.source_records import (
    CodeSetReference,
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

_FORMAT = "reg-meta-prepared-source-records"
_SCHEMA_VERSION = 2
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
    schema_version: Literal[2] = _SCHEMA_VERSION
    scope: str
    partial: Literal[True] = True
    record_count: int
    revisions: tuple[SourceRevision, ...]
    database_path: Literal["files/records.sqlite"] = _DATABASE
    database_size: int
    database_sha256: str

    @field_validator("scope")
    @classmethod
    def _non_empty_scope(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("prepared source scope must be non-empty")
        return value

    @field_validator("record_count", "database_size")
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
PRAGMA user_version=2;
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
    codes_payload INTEGER NOT NULL REFERENCES payload(id)
);
CREATE INDEX occurrence_source_key ON occurrence(source, semantic_key);
CREATE TABLE locator (
    occurrence INTEGER NOT NULL REFERENCES occurrence(ordinal),
    position INTEGER NOT NULL,
    physical_file TEXT NOT NULL,
    physical_table TEXT NOT NULL,
    physical_record TEXT NOT NULL,
    cells_payload INTEGER NOT NULL REFERENCES payload(id),
    PRIMARY KEY(occurrence, position)
) WITHOUT ROWID;
CREATE TABLE delivered_cell (
    occurrence INTEGER NOT NULL REFERENCES occurrence(ordinal),
    position INTEGER NOT NULL,
    payload INTEGER NOT NULL REFERENCES payload(id),
    PRIMARY KEY(occurrence, position)
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


def _write_record(
    conn: sqlite3.Connection,
    payloads: _PayloadWriter,
    ordinal: int,
    record: SourceRecord,
) -> None:
    subject = record.subject
    key = _json(record.locators[0].semantic_record_key)
    conn.execute(
        "INSERT INTO occurrence VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            ordinal,
            record.source,
            key,
            record.record_id,
            record.source_revision_id,
            subject.provider,
            payloads.put("coordinate", subject.register_name),
            payloads.put("coordinate", subject.variant),
            payloads.put("coordinate", subject.population),
            payloads.put("coordinate", subject.variable),
            payloads.put("coordinate", subject.member),
            payloads.put("native", subject.native),
            payloads.put("scope", record.edition_scope),
            payloads.put("scope", record.edition_period_scope),
            payloads.put("fields", record.fields),
            record.language,
            record.original_period_text,
            payloads.put("strings", record.context),
            payloads.put(
                "codes",
                [code.model_dump(mode="json") for code in record.code_set_references],
            ),
        ),
    )
    conn.executemany(
        "INSERT INTO locator VALUES (?, ?, ?, ?, ?, ?)",
        (
            (
                ordinal,
                position,
                locator.physical_file,
                locator.physical_table,
                locator.physical_record,
                payloads.put("strings", locator.physical_cells),
            )
            for position, locator in enumerate(record.locators)
        ),
    )
    conn.executemany(
        "INSERT INTO delivered_cell VALUES (?, ?, ?)",
        (
            (ordinal, position, payloads.put("cell", cell))
            for position, cell in enumerate(record.delivered_cells)
        ),
    )


def prepare_source_records(
    output: Path,
    *,
    records: Iterable[SourceRecord],
    revisions: tuple[SourceRevision, ...],
    scope: str,
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
        count = 0
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
                revision = by_revision.get(checked.source_revision_id)
                if revision is None:
                    raise PreparedSourceError(
                        f"source record revision absent from artifact: {checked.source_revision_id}"
                    )
                if revision.dataset != checked.source:
                    raise PreparedSourceError(
                        "source record and revision name different sources"
                    )
                _write_record(conn, payloads, count, checked)
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
            revisions=validated_revisions,
            database_size=database.stat().st_size,
            database_sha256=_file_sha256(database),
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
    "fields": SourceFields,
    "cell": DeliveredCell,
}


def _readonly(database: Path) -> sqlite3.Connection:
    return sqlite3.connect(database.as_uri() + "?mode=ro&immutable=1", uri=True)


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

    def _iter(self, where: str, parameters: tuple[str, ...]) -> Iterator[SourceRecord]:
        conn: sqlite3.Connection | None = None
        try:
            conn = _readonly(self.root / self.manifest.database_path)
            conn.row_factory = sqlite3.Row

            @lru_cache(maxsize=8192)
            def payload(key: int, kind: str) -> Any:
                row = conn.execute(
                    "SELECT kind, body FROM payload WHERE id = ?", (key,)
                ).fetchone()
                if row is None or row["kind"] != kind:
                    raise PreparedSourceError(
                        f"missing or wrong-kind prepared payload {key}"
                    )
                if kind in _PAYLOAD_MODELS:
                    return _PAYLOAD_MODELS[kind].model_validate_json(row["body"])
                decoded = json.loads(row["body"])
                if kind == "strings":
                    if not isinstance(decoded, list) or any(
                        not isinstance(value, str) for value in decoded
                    ):
                        raise PreparedSourceError("invalid prepared string tuple")
                    return tuple(decoded)
                if kind == "codes":
                    return tuple(
                        CodeSetReference.model_validate_json(_json(value))
                        for value in decoded
                    )
                raise PreparedSourceError(f"unknown prepared payload kind {kind}")

            seen = 0
            for row in conn.execute(
                f"SELECT * FROM occurrence {where} ORDER BY ordinal", parameters
            ):
                semantic_key = tuple(json.loads(row["semantic_key"]))
                locators = tuple(
                    RecordLocator(
                        semantic_record_key=semantic_key,
                        physical_file=locator["physical_file"],
                        physical_table=locator["physical_table"],
                        physical_record=locator["physical_record"],
                        physical_cells=payload(locator["cells_payload"], "strings"),
                    )
                    for locator in conn.execute(
                        "SELECT * FROM locator WHERE occurrence = ? ORDER BY position",
                        (row["ordinal"],),
                    )
                )
                subject = SourceSubject.model_construct(
                    provider=row["provider"],
                    register_name=payload(row["register_payload"], "coordinate"),
                    variant=payload(row["variant_payload"], "coordinate"),
                    population=payload(row["population_payload"], "coordinate"),
                    variable=payload(row["variable_payload"], "coordinate"),
                    member=payload(row["member_payload"], "coordinate"),
                    native=payload(row["native_payload"], "native"),
                )
                # Identity and revision membership were exhaustively proved during
                # preparation. The accepted Git/manifest boundary permits decoding
                # this exact immutable representation without repeating those hashes.
                yield SourceRecord.model_construct(
                    record_id=row["record_id"],
                    source=row["source"],
                    source_revision_id=row["revision_id"],
                    locators=locators,
                    subject=subject,
                    edition_scope=payload(row["scope_payload"], "scope"),
                    edition_period_scope=payload(row["period_payload"], "scope"),
                    fields=payload(row["fields_payload"], "fields"),
                    language=row["language"],
                    code_set_references=payload(row["codes_payload"], "codes"),
                    original_period_text=row["original_period_text"],
                    context=payload(row["context_payload"], "strings"),
                    delivered_cells=tuple(
                        payload(cell[0], "cell")
                        for cell in conn.execute(
                            "SELECT payload FROM delivered_cell WHERE occurrence = ? ORDER BY position",
                            (row["ordinal"],),
                        )
                    ),
                )
                seen += 1
            if not where and seen != self.manifest.record_count:
                raise PreparedSourceError(
                    "prepared source record count differs from manifest"
                )
        except (sqlite3.Error, ValueError, KeyError, TypeError) as exc:
            if isinstance(exc, PreparedSourceError):
                raise
            raise PreparedSourceError(
                f"cannot decode prepared source records: {exc}"
            ) from exc
        finally:
            if conn is not None:
                conn.close()


def prepared_source_paths(root: Path) -> tuple[Path, ...]:
    """Return immutable prepared members for output-collision checks before opening."""
    return root / _MANIFEST, root / _DATABASE


def open_prepared_source_records(
    path: Path, *, expected_sha256: str, input_commit: str
) -> PreparedSourceRecords:
    """Quick-check an accepted Git selection, without hashing or scanning its data."""
    if not _HASH_RE.fullmatch(expected_sha256) or not _COMMIT_RE.fullmatch(
        input_commit
    ):
        raise PreparedSourceError(
            "prepared selection requires a full input_commit and manifest SHA-256"
        )
    root = path.expanduser().resolve()
    try:
        repo = input_bundle_repository(root)
        relative = root.relative_to(repo).as_posix()
        if clean_git_commit(repo) != input_commit:
            raise PreparedSourceError("prepared input commit pin mismatch")
        manifest_path = _snapshot_repo_path(relative, _MANIFEST)
        committed_bytes = _git_bytes(
            repo, "cat-file", "blob", f"{input_commit}:{manifest_path}"
        )
        if hashlib.sha256(committed_bytes).hexdigest() != expected_sha256:
            raise PreparedSourceError("prepared source manifest hash mismatch")
        if (root / _MANIFEST).is_symlink() or (
            root / _MANIFEST
        ).read_bytes() != committed_bytes:
            raise PreparedSourceError(
                "prepared source manifest differs from pinned commit"
            )
        manifest = _manifest(committed_bytes)
        inventory = _committed_inventory_sizes(
            repo,
            input_commit,
            relative,
            (_snapshot_repo_path(relative, "files"),),
            context="prepared sources",
        )
        if inventory != {_DATABASE: manifest.database_size}:
            raise PreparedSourceError(
                "prepared source committed inventory differs from manifest"
            )
        tags = _index_tags(repo)
        for name in (_MANIFEST, _DATABASE):
            if tags.get(_snapshot_repo_path(relative, name)) != "H":
                raise PreparedSourceError(
                    "prepared input files must be fully materialized with ordinary Git index flags"
                )
        database = root / _DATABASE
        actual = {
            item.relative_to(root).as_posix(): item.stat().st_size
            for item in (root / "files").rglob("*")
            if item.is_file()
        }
        if (
            (root / "files").is_symlink()
            or database.is_symlink()
            or actual != inventory
        ):
            raise PreparedSourceError(
                "prepared source worktree inventory differs from manifest"
            )
        with closing(_readonly(database)) as conn:
            if conn.execute("PRAGMA user_version").fetchone() != (_SCHEMA_VERSION,):
                raise PreparedSourceError(
                    "unsupported prepared records database version"
                )
            conn.execute("SELECT ordinal, source, semantic_key FROM occurrence LIMIT 0")
        if _git(repo, "rev-parse", "HEAD") != input_commit:
            raise PreparedSourceError("prepared input commit changed during selection")
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
