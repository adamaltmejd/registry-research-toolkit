"""Test fixtures: reg_meta fixture DBs pointed at via REG_META_DB.

CI has no real reg_meta asset, so the backend tests build fixture DBs and point the
app at them via the highest-precedence ``REG_META_DB`` override
(``reg_meta.db.default_db_dir``). The ``catalog_db`` fixture delegates
to ``scripts/fixture_db.py``, the builder ``dev.sh --fixture-db`` also runs, so the
DB the tests assert against and the one the dev servers render are the same
bytes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import reg_meta.db
from fastapi.testclient import TestClient
from reg_webapp.app import create_app
from webapp_fixture_support import fixture_db

if TYPE_CHECKING:
    from pathlib import Path


def _point_app_at(monkeypatch: pytest.MonkeyPatch, db_dir: Path) -> None:
    # REG_META_DB is the highest-precedence dir in reg_meta.db.default_db_dir.
    monkeypatch.setenv("REG_META_DB", str(db_dir))


@pytest.fixture
def catalog_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The slugged reg_meta fixture DB (``scripts/fixture_db.py``), pointed at via
    REG_META_DB. The project tests validate and order against its ``scb/lisa``
    bindings."""
    db_path = tmp_path / reg_meta.db.DB_FILENAME
    fixture_db.build_catalog_fixture_db(db_path)
    _point_app_at(monkeypatch, tmp_path)
    return db_path


@pytest.fixture
def client(catalog_db):
    with TestClient(create_app()) as c:
        yield c
