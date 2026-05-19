"""Doc index: read-only access to the prebuilt FTS5 search index.

The build pipeline (parse markdown, populate FTS) lives in
``regmeta_build.doc_db``; this module exposes the query-side surface:
path resolution, schema-compat check, and ``open_doc_db`` /
``ensure_doc_db``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from .errors import EXIT_CONFIG, RegmetaError

DOC_DB_FILENAME = "regmeta_docs.db"
DOC_DB_ASSET_NAME = "regmeta_docs.db.zst"
DOCS_SOURCE_FILE = ".docs_source"

# Versioning parallels the main-DB SCHEMA_VERSION. Bump the minor when the
# code starts reading a new column / meta key, major when tables or columns
# are renamed or removed. Patch differences are ignored.
DOC_SCHEMA_VERSION = "1.0.0"


def doc_db_path(db_arg: str | None) -> Path:
    """Resolve path to the doc index DB."""
    from .db import db_path_from_args

    return db_path_from_args(db_arg, filename=DOC_DB_FILENAME)


def _check_doc_schema_compat(conn: sqlite3.Connection, db_path: Path) -> None:
    """Raise if the doc DB schema is incompatible with the installed code.

    Mirrors ``_check_schema_compat`` in ``db.py``: same-major / minor>=code
    rule against ``DOC_SCHEMA_VERSION``. Missing/unparseable metadata is
    treated as incompatible so stale pre-versioning DBs get replaced.
    """
    fix = (
        "Run `regmeta maintain update` to replace it with a compatible asset. "
        "(Doc DBs built by pre-0.7 regmeta lack schema_version and are always "
        "reported as incompatible — the update will overwrite them.)"
    )

    try:
        row = conn.execute(
            "SELECT value FROM doc_meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.OperationalError as exc:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="doc_schema_incompatible",
            error_class="configuration",
            message=(
                f"Doc DB metadata is missing or unreadable in {db_path}. "
                f"Expected doc schema v{DOC_SCHEMA_VERSION}."
            ),
            remediation=fix,
        ) from exc

    db_ver = row["value"] if row else None
    try:
        if not db_ver:
            raise ValueError("missing schema_version")
        db_parts = db_ver.split(".")
        db_major, db_minor = int(db_parts[0]), int(db_parts[1])
        code_parts = DOC_SCHEMA_VERSION.split(".")
        code_major, code_minor = int(code_parts[0]), int(code_parts[1])
    except (ValueError, IndexError) as exc:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="doc_schema_incompatible",
            error_class="configuration",
            message=(
                f"Doc DB schema version is missing or invalid in {db_path}: "
                f"{db_ver!r}. This version of regmeta expects doc schema v{DOC_SCHEMA_VERSION}."
            ),
            remediation=fix,
        ) from exc

    if db_major != code_major or db_minor < code_minor:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="doc_schema_incompatible",
            error_class="configuration",
            message=(
                f"Doc DB schema v{db_ver} ({db_path}) is incompatible with this "
                f"version of regmeta (expects doc schema v{DOC_SCHEMA_VERSION})."
            ),
            remediation=fix,
        )


def open_doc_db(db_path: Path, *, check_schema: bool = True) -> sqlite3.Connection:
    """Open the doc index DB read-only and verify schema compatibility."""
    if not db_path.exists():
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="doc_db_not_found",
            error_class="configuration",
            message=f"Doc DB not found: {db_path}",
            remediation="Run `regmeta maintain update` to fetch the doc DB.",
        )
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    if check_schema:
        try:
            _check_doc_schema_compat(conn, db_path)
        except RegmetaError:
            conn.close()
            raise
    return conn


def ensure_doc_db(db_arg: str | None) -> sqlite3.Connection:
    """Open the doc DB, failing with an actionable error if missing.

    Unlike the pre-0.7 behaviour, this no longer auto-builds from bundled
    markdown — the doc DB is distributed as a release asset and installed
    via ``maintain update`` alongside the main DB.
    """
    path = doc_db_path(db_arg)
    return open_doc_db(path)
