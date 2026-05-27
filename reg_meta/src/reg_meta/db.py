"""Database connection management and schema-compat checks for reg_meta.

The build pipeline (DDL, CSV import, ``build_db``) lives in
``reg_meta_build.db``; this module exposes only the query-side surface:
DB-path resolution, read-only ``open_db``, manifest reads, and the
shared ``SCHEMA_VERSION`` / ``DB_FILENAME`` constants that the build
side imports back.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

from .errors import EXIT_CONFIG, RegMetaError

# SCHEMA_VERSION bump history (Model A migration). Each entry pins the
# refactor stage that justified the bump and what becomes
# incompatible. `_check_schema_compat` rejects DBs with a different
# major OR an older minor — additive table/column changes are minor
# bumps, drops or renames are major.
#
# - 4.0.0 (A1.1): §5.11 renamed ~21 columns across the universal schema
#   and dropped the SCB-Swedish names. Pre-4.x DBs reference columns
#   that no longer exist — hard break. A1.2's additive sensitivity
#   columns (`is_sensitive`, `is_identifier`) rode on the same major
#   bump and didn't require their own version step.
# - 4.1.0 (A2.1): added `variable_state` and dropped the
#   build-only `unika_summary` table. The drop is benign for the query
#   layer (nothing in `reg_meta` reads `unika_summary`); the addition
#   matters because the upcoming A2.5 resolver flip needs
#   `variable_state` to be populated. Old 4.0.0 DBs without that table
#   are rejected via the minor-version gate.
# - 4.2.0 (A2.2, current): added `variable_related_to` table — A2.2's
#   build-time triage emits symmetric edges between sibling variables
#   split from one source (kolumnnamn / vardemangdsniva / datalangd
#   discriminators per §5.7) and the same table is the curation slot
#   for cross-register relationships in slug TOMLs. Additive new table,
#   so a minor bump. Note: A2.2 ships in parallel with A2.3 (the
#   `*_replaced_by` family); whichever PR lands second appends its DDL
#   alongside and bumps to 4.3.0.
# - 5.0.0 (A2.7, planned): drops `variable_instance` once the resolver
#   moves to `variable_state` for good — that's the next breaking
#   change.
SCHEMA_VERSION = "4.2.0"
DB_FILENAME = "reg_meta.db"


def default_db_dir() -> Path:
    """Default directory for the reg_meta database.

    Resolution: $REG_META_DB > $XDG_DATA_HOME/reg_meta > platform default.
    """
    if env := os.environ.get("REG_META_DB"):
        return Path(env).expanduser()
    if xdg := os.environ.get("XDG_DATA_HOME"):
        return Path(xdg) / "reg_meta"
    if sys.platform == "win32":
        return Path(os.environ.get("LOCALAPPDATA", "~/AppData/Local")) / "reg_meta"
    return Path.home() / ".local" / "share" / "reg_meta"


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
    (e.g. ``reg-meta info``, doc DB).
    """
    fix = "Run `reg-meta update` to get a compatible database."

    try:
        manifest = get_manifest(conn)
    except sqlite3.OperationalError as exc:
        raise RegMetaError(
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
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="schema_incompatible",
            error_class="configuration",
            message=(
                f"Database schema version is missing or invalid in {db_path}: "
                f"{db_ver!r}. This version of reg_meta expects schema v{SCHEMA_VERSION}."
            ),
            remediation=fix,
        ) from exc

    if db_major != code_major or db_minor < code_minor:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="schema_incompatible",
            error_class="configuration",
            message=(
                f"Database schema v{db_ver} ({db_path}) is incompatible "
                f"with this version of reg_meta (expects schema v{SCHEMA_VERSION})."
            ),
            remediation=fix,
        )


def open_db(
    db_path: Path,
    *,
    check_schema: bool = True,
    error_code: str = "db_not_found",
    remediation: str = (
        "Run `reg-meta update` to fetch the pre-built DB, "
        "or `reg-meta-build build-db --input-dir <path>` to build from CSV exports."
    ),
) -> sqlite3.Connection:
    if not db_path.exists():
        raise RegMetaError(
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
        except RegMetaError:
            conn.close()
            raise
    return conn


def get_manifest(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT key, value FROM import_manifest").fetchall()
    return {row["key"]: row["value"] for row in rows}


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
