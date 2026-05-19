"""Test scaffolding for regmeta_build.

Adds this directory to ``sys.path`` so the bare-name helper modules
(``_csv_fixtures``, ``_slugged_db``) can be imported by individual tests.
Also defines the same fixture DB used by regmeta's query-side tests.

This directory deliberately has no ``__init__.py``: pytest's rootdir-relative
module discovery breaks when ``regmeta/tests/`` and ``regmeta_build/tests/``
both register as proper packages. Keep it that way.
"""

from __future__ import annotations

import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from regmeta_build.db import build_db  # noqa: E402

from _csv_fixtures import write_scb_input  # noqa: E402


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a small SQLite DB from synthetic CSV fixtures.

    Same shape as the query-side fixture in `regmeta/tests/conftest.py` —
    builds *with* slugs (the strict `link_consumer_side_bindings` path
    requires non-NULL `register_version.slug`) and seeds a minimal doc DB
    so tests that touch the query CLI don't trip on the "docs not
    installed" guard.
    """
    input_dir = tmp_path_factory.mktemp("input")
    db_dir = tmp_path_factory.mktemp("db")
    slug_dir = tmp_path_factory.mktemp("slugs")

    write_scb_input(input_dir)
    _write_fixture_slug_dir(slug_dir)

    build_db(
        input_dir=input_dir,
        db_dir=db_dir,
        skip_classifications=True,
        slug_dir=slug_dir,
    )
    _build_stub_doc_db(db_dir, tmp_path_factory)

    return db_dir / "regmeta.db"


def _write_fixture_slug_dir(slug_dir: Path) -> None:
    (slug_dir / "scb.toml").write_text(
        '[register."1"]\nslug = "testreg"\n'
        '[register."2"]\nslug = "otherreg"\n'
        '[register_variant."1.10"]\nslug = "individer"\n'
        '[register_variant."2.20"]\nslug = "foretag"\n',
        encoding="utf-8",
    )
    (slug_dir / "classifications.toml").write_text("", encoding="utf-8")


def _build_stub_doc_db(db_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> None:
    from regmeta_build.doc_db import build_doc_db

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
    from regmeta.db import open_db

    conn = open_db(fixture_db)
    yield conn
    conn.close()


@pytest.fixture()
def db_path(fixture_db: Path) -> str:
    return str(fixture_db.parent)
