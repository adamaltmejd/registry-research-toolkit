"""Shared pytest fixtures used by both `reg_meta` and `reg_meta_build` test
suites. Both conftests import these via the on-`sys.path` bare-name path
(see each conftest's `sys.path.insert`)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from reg_meta.db import register_py_lower

if TYPE_CHECKING:
    from collections.abc import Iterator


def connect_built_db(db: Path | str) -> sqlite3.Connection:
    """Open a writable connection to a built fixture DB the way production
    validation does: a raw connect plus the `py_lower` UDF that
    `reg_meta.db.open_db` (and thus `validate_built_db`) registers. Tests that
    drive `_check_*` validators or `queries.*` against a built DB and then mutate
    it can't use the read-only `open_db` path, so they go through here to get the
    same UDF surface (refs #853)."""
    conn = sqlite3.connect(str(db))
    register_py_lower(conn)
    return conn


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Seed a small explicit catalog, independent of source-resolution rules.

    Reader and structural-validator tests need stable graph identities, not a
    source build. Pipeline tests exercise preparation and the resolved writer.
    """
    from contextlib import closing

    from catalog_manifest import synthetic_manifest
    from reg_meta_build.artifact_identity import generation_id
    from reg_meta_build.db import DDL, SCHEMA_VERSION, seed_providers
    from reg_meta_build.derive import derive

    db_dir = tmp_path_factory.mktemp("db")
    output = db_dir / "reg_meta.db"
    with closing(sqlite3.connect(output)) as conn:
        conn.executescript(DDL)
        seed_providers(conn)
        conn.executescript(
            Path(__file__).with_name("_catalog_fixture.sql").read_text(encoding="utf-8")
        )
        identity = synthetic_manifest() | {
            "schema_version": SCHEMA_VERSION,
            "catalog_artifact_kind": "catalog",
        }
        identity["generation_id"] = generation_id(identity)
        conn.executemany(
            "INSERT INTO import_manifest (key, value) VALUES (?, ?)",
            (
                ("schema_version", SCHEMA_VERSION),
                ("catalog_artifact_kind", "catalog"),
                ("catalog_publishable", "true"),
                ("catalog_completeness", "complete"),
                *tuple(
                    (key, value)
                    for key, value in identity.items()
                    if key not in {"schema_version", "catalog_artifact_kind"}
                ),
                ("import_date", "2020-01-01T00:00:00Z"),
                ("row_counts", json.dumps({"variables": 8, "states": 9})),
            ),
        )
        derive(conn)
        conn.execute("ANALYZE")
        conn.commit()
        conn.execute("VACUUM")
    _build_stub_doc_db(db_dir, tmp_path_factory)

    return output


def _build_stub_doc_db(db_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Write a minimally valid doc DB alongside the main DB.

    Query-command tests don't exercise doc-search behaviour — they just
    need *a* schema-compatible doc DB present so the presence guard lets
    them through. Doc-specific behaviour is tested in test_doc_commands.py.
    """
    from reg_meta_build.doc_db import build_doc_db

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
    """Read-only connection to the fixture database."""
    from reg_meta_build.db import open_built_db

    conn = open_built_db(fixture_db)
    yield conn
    conn.close()


@pytest.fixture()
def db_path(fixture_db: Path) -> str:
    """`--db` arg pointing to the fixture database directory."""
    return str(fixture_db.parent)
