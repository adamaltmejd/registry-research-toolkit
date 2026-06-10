"""Read-only opens of WAL-mode DBs must work from non-writable dirs (#283).

Published DB assets ship in WAL journal mode. A plain `?mode=ro` open of a
WAL DB still tries to create the `-wal`/`-shm` sidecars, which crashes on a
read-only catalog dir. `open_db`/`open_doc_db` pass `immutable=1` to skip
sidecar creation. These tests build a WAL DB, strip its sidecars, drop it in a
chmod 0o555 dir, and assert the read-only open still reads — they fail without
the `immutable=1` fix.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import stat
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from reg_meta.db import DB_FILENAME, SCHEMA_VERSION, open_db
from reg_meta.doc_db import DOC_DB_FILENAME, DOC_SCHEMA_VERSION, open_doc_db

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


def _make_wal_db(path: Path, *, setup_sql: str) -> None:
    """Create a WAL-mode DB at `path`, then close it removing sidecars.

    A clean close after `wal_checkpoint(TRUNCATE)` leaves only the base file,
    reproducing how a freshly-decompressed asset sits on disk before any reader
    touches it (no `-wal`/`-shm` present yet, so the read-only open is the thing
    that would otherwise have to create them).
    """
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(setup_sql)
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    # Defensive: ensure no sidecars remain so the read-only open is forced to
    # create them (which is exactly what fails without immutable=1).
    for suffix in ("-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)


def _readonly_dir(path: Path) -> None:
    """chmod a dir to 0o555 (no write) — the trigger condition for #283."""
    path.chmod(0o555)


@pytest.fixture
def restore_perms() -> Iterator[Callable[[Path], None]]:
    """Restore directory perms in teardown so tmp_path cleanup can unlink."""
    touched: list[Path] = []
    yield touched.append
    for p in touched:
        with contextlib.suppress(OSError):
            p.chmod(stat.S_IRWXU)


def test_open_db_reads_wal_db_in_readonly_dir(
    tmp_path: Path, restore_perms: Callable[[Path], None]
) -> None:
    ro_dir = tmp_path / "catalog"
    ro_dir.mkdir()
    db_file = ro_dir / DB_FILENAME
    _make_wal_db(
        db_file,
        setup_sql=(
            "CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT);"
            f"INSERT INTO import_manifest VALUES ('schema_version', '{SCHEMA_VERSION}');"
            "CREATE TABLE probe (n INTEGER);"
            "INSERT INTO probe VALUES (42);"
        ),
    )
    _readonly_dir(ro_dir)
    restore_perms(ro_dir)

    # Sanity: the dir really is non-writable for this process (skip if running
    # as root, where DAC permission checks are bypassed and the test is moot).
    if os.access(ro_dir, os.W_OK):
        pytest.skip("catalog dir is writable (likely running as root)")

    conn = open_db(db_file)
    try:
        assert conn.execute("SELECT n FROM probe").fetchone()[0] == 42
    finally:
        conn.close()


def test_open_doc_db_reads_wal_db_in_readonly_dir(
    tmp_path: Path, restore_perms: object
) -> None:
    ro_dir = tmp_path / "catalog"
    ro_dir.mkdir()
    db_file = ro_dir / DOC_DB_FILENAME
    _make_wal_db(
        db_file,
        setup_sql=(
            "CREATE TABLE doc_meta (key TEXT PRIMARY KEY, value TEXT);"
            f"INSERT INTO doc_meta VALUES ('schema_version', '{DOC_SCHEMA_VERSION}');"
            "CREATE TABLE probe (n INTEGER);"
            "INSERT INTO probe VALUES (7);"
        ),
    )
    _readonly_dir(ro_dir)
    restore_perms(ro_dir)

    if os.access(ro_dir, os.W_OK):
        pytest.skip("catalog dir is writable (likely running as root)")

    conn = open_doc_db(db_file)
    try:
        assert conn.execute("SELECT n FROM probe").fetchone()[0] == 7
    finally:
        conn.close()
