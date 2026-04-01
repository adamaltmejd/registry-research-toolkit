"""Shared fixtures for regmeta tests.

Builds a small but realistic SQLite database from synthetic CSV fixtures
that exercise the key edge cases: multiple registers, alias anomalies,
cross-register var_id reuse, cp1252 encoding, and value sets.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from regmeta.db import build_db

from _csv_fixtures import (
    IDENTIFIERARE_HEADER,
    IDENTIFIERARE_ROWS,
    REGISTERINFORMATION_HEADER,
    REGISTERINFORMATION_ROWS,
    TIMESERIES_HEADER,
    TIMESERIES_ROWS,
    UNIKA_HEADER,
    UNIKA_ROWS,
    VALID_DATES_HEADER,
    VALID_DATES_ROWS,
    VARDEMANGDER_HEADER,
    VARDEMANGDER_ROWS,
    write_csv,
)


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a test database from synthetic fixtures. Shared across all tests."""
    csv_dir = tmp_path_factory.mktemp("csv")
    db_dir = tmp_path_factory.mktemp("db")

    write_csv(
        csv_dir / "Registerinformation.csv",
        REGISTERINFORMATION_HEADER,
        REGISTERINFORMATION_ROWS,
    )
    write_csv(csv_dir / "UnikaRegisterOchVariabler.csv", UNIKA_HEADER, UNIKA_ROWS)
    write_csv(csv_dir / "Identifierare.csv", IDENTIFIERARE_HEADER, IDENTIFIERARE_ROWS)
    write_csv(csv_dir / "Timeseries.csv", TIMESERIES_HEADER, TIMESERIES_ROWS)
    write_csv(csv_dir / "Vardemangder.csv", VARDEMANGDER_HEADER, VARDEMANGDER_ROWS)
    write_csv(
        csv_dir / "VardemangderValidDates.csv", VALID_DATES_HEADER, VALID_DATES_ROWS
    )

    build_db(csv_dir=csv_dir, db_dir=db_dir)

    return db_dir / "regmeta.db"


@pytest.fixture()
def db_conn(fixture_db: Path) -> Iterator[sqlite3.Connection]:
    """Open a read-only connection to the fixture database."""
    from regmeta.db import open_db

    conn = open_db(fixture_db)
    yield conn
    conn.close()


@pytest.fixture()
def db_path(fixture_db: Path) -> str:
    """Return --db arg pointing to the fixture database directory."""
    return str(fixture_db.parent)
