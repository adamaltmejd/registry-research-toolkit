"""Register helpers backed by the reg_meta SQLite database.

Two functions exposed: ``list_registers`` (enumerate all registers) and
``resolve_register`` (name-or-id → ``Register`` lookup). Both degrade
gracefully when the reg_meta DB is absent or unreadable — they return
empty/None rather than raising — so the editor can operate on a project
without reg_meta installed (every group becomes ``register=None`` and
the user assigns types by hand).

The graceful-degradation pattern is in one place here so editor code
doesn't need to wrap every reg_meta call in try/except.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True, slots=True)
class Register:
    """Minimal register identity. Add fields if a UI use case demands."""

    id: int
    name: str


def _open_or_none(db_path: Path | None):
    """Open the reg_meta DB returning the connection, or None on any failure
    that means "reg_meta isn't available." Callers must close the connection
    when done with it."""
    from reg_meta.db import db_path_from_args
    from reg_meta.errors import RegMetaError

    from reg_meta import open_db

    try:
        resolved = db_path_from_args(str(db_path) if db_path else None)
        return open_db(resolved)
    except (FileNotFoundError, OSError, sqlite3.OperationalError, RegMetaError):
        return None


def list_registers(*, db_path: Path | None = None) -> list[Register]:
    """All registers in the reg_meta DB, ordered by name. ``[]`` if the DB
    is missing or unreadable — the editor can still operate without reg_meta."""
    conn = _open_or_none(db_path)
    if conn is None:
        return []
    try:
        rows = conn.execute(
            "SELECT register_id, registernamn FROM register ORDER BY registernamn"
        ).fetchall()
        return [Register(id=r["register_id"], name=r["registernamn"]) for r in rows]
    finally:
        conn.close()


def resolve_register(
    name_or_id: str, *, db_path: Path | None = None
) -> Register | None:
    """Resolve a register name or numeric id to a ``Register``.

    Returns ``None`` when the DB is missing/unreadable, the input doesn't
    match anything, or it matches multiple registers ambiguously (the
    caller can recover by re-prompting with a more specific name).
    """
    from reg_meta.errors import RegMetaError

    from reg_meta import resolve_register_ids

    conn = _open_or_none(db_path)
    if conn is None:
        return None
    try:
        try:
            ids = resolve_register_ids(conn, name_or_id)
        except RegMetaError:
            return None
        if not ids or len(ids) > 1:
            return None
        row = conn.execute(
            "SELECT register_id, registernamn FROM register WHERE register_id = ?",
            (ids[0],),
        ).fetchone()
        if row is None:
            return None
        return Register(id=row["register_id"], name=row["registernamn"])
    finally:
        conn.close()
