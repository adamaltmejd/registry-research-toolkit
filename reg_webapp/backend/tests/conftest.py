"""Test fixtures: a manifest-only reg_meta DB pointed at via REG_META_DB.

CI has no real reg_meta asset (5.1.0 is unpublished), so the backend tests
build a tiny fixture DB and point the app at it via the highest-precedence
``REG_META_DB`` override (``reg_meta.db.default_db_dir``). ``/api/context``
reads ONLY ``import_manifest``, so the fixture needs nothing but that table —
no reg_meta_build DDL. The fixture's ``schema_version`` matches the installed
``reg_meta.SCHEMA_VERSION`` so ``open_db``'s compat check passes.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
import reg_meta.db

if TYPE_CHECKING:
    from pathlib import Path

FIXTURE_IMPORT_DATE = "2026-06-01T00:00:00Z"


def _write_manifest_db(db_path: Path, schema_version: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE import_manifest(key TEXT PRIMARY KEY, value TEXT)")
        conn.executemany(
            "INSERT INTO import_manifest(key, value) VALUES (?, ?)",
            [
                ("schema_version", schema_version),
                ("import_date", FIXTURE_IMPORT_DATE),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _point_app_at(monkeypatch: pytest.MonkeyPatch, db_dir: Path) -> None:
    # REG_META_DB is the highest-precedence dir in reg_meta.db.default_db_dir.
    monkeypatch.setenv("REG_META_DB", str(db_dir))


@pytest.fixture
def fixture_import_date() -> str:
    return FIXTURE_IMPORT_DATE


@pytest.fixture
def compatible_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A fixture DB whose manifest matches the installed SCHEMA_VERSION."""
    db_path = tmp_path / reg_meta.db.DB_FILENAME
    _write_manifest_db(db_path, reg_meta.db.SCHEMA_VERSION)
    _point_app_at(monkeypatch, tmp_path)
    return db_path


@pytest.fixture
def mismatched_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A fixture DB one MAJOR ahead — startup must reject it."""
    db_path = tmp_path / reg_meta.db.DB_FILENAME
    major = int(reg_meta.db.SCHEMA_VERSION.split(".")[0])
    _write_manifest_db(db_path, f"{major + 1}.0.0")
    _point_app_at(monkeypatch, tmp_path)
    return db_path
