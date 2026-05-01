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

from _csv_fixtures import write_scb_input


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a test database from synthetic fixtures. Shared across all tests.

    Also builds a minimal doc DB in the same directory because query
    commands (search/get/resolve) now require both artifacts — the CLI
    refuses to run queries without docs installed.
    """
    input_dir = tmp_path_factory.mktemp("input")
    db_dir = tmp_path_factory.mktemp("db")

    write_scb_input(input_dir)

    build_db(input_dir=input_dir, db_dir=db_dir, skip_classifications=True)
    _build_stub_doc_db(db_dir, tmp_path_factory)

    return db_dir / "regmeta.db"


def _build_stub_doc_db(db_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Write a minimally valid doc DB alongside the main DB.

    Query-command tests don't exercise doc-search behaviour — they just
    need *a* schema-compatible doc DB present so the presence guard lets
    them through. Doc-specific behaviour is tested in test_doc_commands.py.
    """
    from regmeta.doc_db import build_doc_db

    docs_src = tmp_path_factory.mktemp("stub_docs")
    reg_dir = docs_src / "stub"
    reg_dir.mkdir()
    (reg_dir / "Stub.md").write_text(
        "---\nvariable: Stub\ndisplay_name: Stub\ntags:\n  - type/variable\n---\n\nStub body.\n",
        encoding="utf-8",
    )
    build_doc_db(docs_src, db_dir)


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
