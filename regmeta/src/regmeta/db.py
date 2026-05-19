"""Database connection management and schema-compat checks for regmeta.

The build pipeline (DDL, CSV import, ``build_db``) lives in
``regmeta_build.db``; this module exposes only the query-side surface:
DB-path resolution, read-only ``open_db``, manifest reads, and the
shared ``SCHEMA_VERSION`` / ``DB_FILENAME`` constants that the build
side imports back.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from .errors import EXIT_CONFIG, RegmetaError

SCHEMA_VERSION = "3.3.0"
DB_FILENAME = "regmeta.db"


def default_db_dir() -> Path:
    """Default directory for the regmeta database.

    Resolution: $REGMETA_DB > $XDG_DATA_HOME/regmeta > platform default.
    """
    if env := os.environ.get("REGMETA_DB"):
        return Path(env).expanduser()
    if xdg := os.environ.get("XDG_DATA_HOME"):
        return Path(xdg) / "regmeta"
    if sys.platform == "win32":
        return Path(os.environ.get("LOCALAPPDATA", "~/AppData/Local")) / "regmeta"
    return Path.home() / ".local" / "share" / "regmeta"


def db_path_from_args(db_arg: str | None, filename: str = DB_FILENAME) -> Path:
    if db_arg:
        return Path(db_arg).expanduser().resolve() / filename
    return default_db_dir().resolve() / filename


def _check_schema_compat(conn: sqlite3.Connection, db_path: Path) -> None:
    """Raise if the database schema is incompatible with the installed code.

    Code with ``SCHEMA_VERSION = M.m.p`` requires a DB whose manifest records a
    schema version with the same major M and minor >= m. A lower minor means
    the code may reference columns that don't exist in the DB; different majors
    are hard breaks. Patch differences are ignored.

    Missing or unparseable ``schema_version`` is treated as incompatible — the
    ``check_schema=False`` escape hatch exists for legitimate bypasses
    (e.g. ``regmeta info``, doc DB).
    """
    fix = "Run `regmeta update` to get a compatible database."

    try:
        manifest = get_manifest(conn)
    except sqlite3.OperationalError as exc:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="schema_incompatible",
            error_class="configuration",
            message=(
                f"Database manifest is missing or unreadable in {db_path}. "
                f"Expected schema v{SCHEMA_VERSION} metadata."
            ),
            remediation=fix,
        ) from exc

    db_ver = manifest.get("schema_version")
    try:
        if not db_ver:
            raise ValueError("missing schema_version")
        db_parts = db_ver.split(".")
        db_major, db_minor = int(db_parts[0]), int(db_parts[1])
        code_parts = SCHEMA_VERSION.split(".")
        code_major, code_minor = int(code_parts[0]), int(code_parts[1])
    except (ValueError, IndexError) as exc:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="schema_incompatible",
            error_class="configuration",
            message=(
                f"Database schema version is missing or invalid in {db_path}: "
                f"{db_ver!r}. This version of regmeta expects schema v{SCHEMA_VERSION}."
            ),
            remediation=fix,
        ) from exc

    if db_major != code_major or db_minor < code_minor:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="schema_incompatible",
            error_class="configuration",
            message=(
                f"Database schema v{db_ver} ({db_path}) is incompatible "
                f"with this version of regmeta (expects schema v{SCHEMA_VERSION})."
            ),
            remediation=fix,
        )


def open_db(
    db_path: Path,
    *,
    check_schema: bool = True,
    error_code: str = "db_not_found",
    remediation: str = (
        "Run `regmeta update` to fetch the pre-built DB, "
        "or `regmeta-build build-db --input-dir <path>` to build from CSV exports."
    ),
) -> sqlite3.Connection:
    if not db_path.exists():
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code=error_code,
            error_class="configuration",
            message=f"Database not found: {db_path}",
            remediation=remediation,
        )
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    if check_schema:
        try:
            _check_schema_compat(conn, db_path)
        except RegmetaError:
            conn.close()
            raise
    return conn


def get_manifest(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT key, value FROM import_manifest").fetchall()
    return {row["key"]: row["value"] for row in rows}


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
