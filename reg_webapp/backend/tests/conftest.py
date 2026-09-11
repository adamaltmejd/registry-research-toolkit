"""Test fixtures: reg_meta fixture DBs pointed at via REG_META_DB.

CI has no real reg_meta asset (5.1.0 is unpublished), so the backend tests build
fixture DBs and point the app at them via the highest-precedence ``REG_META_DB``
override (``reg_meta.db.default_db_dir``).

- ``/api/context`` reads ONLY ``import_manifest`` → the manifest-only fixture
  (``compatible_db`` / ``mismatched_db``) needs nothing but that table.
- ``/api/catalog`` resolves/lists against the full reg_meta schema → the
  ``catalog_db`` / ``docs_db`` fixtures delegate to ``scripts/fixture_db.py``,
  the builder ``dev.sh --fixture-db`` also runs, so the DB pair the tests assert
  against and the one the dev servers render are the same bytes.
"""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest
import reg_meta.db
import reg_meta.doc_db

# Load the sibling builder script directly, without mutating sys.path (mirrors
# test_openapi_snapshot.py), so its bare-name imports don't leak.
_FIXTURE_DB_PATH = Path(__file__).resolve().parents[1] / "scripts" / "fixture_db.py"
_spec = importlib.util.spec_from_file_location(
    "reg_webapp_fixture_db", _FIXTURE_DB_PATH
)
assert _spec and _spec.loader
fixture_db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fixture_db)

# `test_steward_index` builds its own slugged DBs from `reg_meta_build`'s bare-name
# `_slugged_db` helper, so make that dir importable for the whole session (the
# builder above does the same on its own behalf; the call is idempotent).
fixture_db.ensure_slugged_db_importable()

FIXTURE_IMPORT_DATE = fixture_db.FIXTURE_IMPORT_DATE
FIXTURE_SCHEMA_VERSION = fixture_db.FIXTURE_SCHEMA_VERSION


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
def fixture_schema_version() -> str:
    return FIXTURE_SCHEMA_VERSION


@pytest.fixture
def compatible_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A fixture DB whose manifest is gate-compatible with the installed code."""
    db_path = tmp_path / reg_meta.db.DB_FILENAME
    _write_manifest_db(db_path, FIXTURE_SCHEMA_VERSION)
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


@pytest.fixture
def catalog_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A slugged reg_meta DB for the catalog browse tests, pointed at via
    REG_META_DB. Resolves: ``scb`` (provider, 2 registers), ``scb/lisa``
    (register, binding ``kon`` + variants-ref), ``scb/lisa/kon`` (binding leaf,
    1 state w/ value set + a same_as edge), ``class`` (classification-root, 1
    classification), ``class/sun2020`` (classification leaf)."""
    db_path = tmp_path / reg_meta.db.DB_FILENAME
    fixture_db.build_catalog_fixture_db(db_path)
    _point_app_at(monkeypatch, tmp_path)
    return db_path


@pytest.fixture
def docs_db(catalog_db: Path) -> Path:
    """The catalog DB PLUS a `reg_meta_docs.db` in the same REG_META_DB dir, so
    the app boots the main catalog AND opens the optional docs index (#354).
    Returns the docs DB path. Tests that want the docs-ABSENT degradation use the
    plain ``catalog_db`` fixture (no docs DB written)."""
    docs_path = catalog_db.parent / reg_meta.doc_db.DOC_DB_FILENAME
    fixture_db.build_docs_fixture_db(docs_path)
    return docs_path


@pytest.fixture
def case_twin_db(catalog_db: Path) -> Path:
    """``catalog_db`` plus the Y-107 case-twin scenario (``fixture_db``): one
    `scb/lisa/idve` variable whose column is spelled `Idh` by the era a steward's
    inventory was generated over and `IdH` by the era after it (plus an unheld
    `Taxvarde` rename). Seeded here rather than in ``build_catalog_fixture_db`` for
    the same reason ``topical_catalog_db`` is."""
    conn = sqlite3.connect(catalog_db)
    try:
        fixture_db.seed_case_twin_column(conn)
        conn.commit()
    finally:
        conn.close()
    return catalog_db


@pytest.fixture
def topical_catalog_db(catalog_db: Path) -> Path:
    """``catalog_db`` plus the topical ranking scenario (``fixture_db``): one
    register purpose and one variable name/definition carrying a topic, and six
    value codes whose labels merely BEGIN with it. Seeded here rather than in
    ``build_catalog_fixture_db`` so the pair ``dev.sh --fixture-db`` builds — and
    the UI gates screenshot — stays byte-identical."""
    conn = sqlite3.connect(catalog_db)
    try:
        fixture_db.seed_topical_rows(conn)
        conn.commit()
    finally:
        conn.close()
    return catalog_db
