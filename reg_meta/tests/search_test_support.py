"""Shared support for the search test modules.

`reader_search_conn` opens a readable source under `conformance/cases/reader/`
built through the real resolved-catalog writer; it is the source form new search
boundary tests use. The `_slugged_db` builders and `rebuild_fts` serve the older
search tests that seed explicit rows.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from reader_artifacts import build_reader_artifact
from reg_meta.db import open_db

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "reg_meta_build" / "tests")
)

from _slugged_db import (
    add_binding,
    add_register,
    add_state,
    add_value_set,
    add_variable,
    build_slugged_db,
)

if TYPE_CHECKING:
    import sqlite3

    import pytest

__all__ = [
    "add_binding",
    "add_register",
    "add_state",
    "add_value_set",
    "add_variable",
    "build_slugged_db",
    "reader_search_conn",
    "rebuild_fts",
]


def reader_search_conn(
    tmp_path_factory: pytest.TempPathFactory, source: str
) -> sqlite3.Connection:
    """Open the catalog artifact built from `conformance/cases/reader/<source>`."""
    path = build_reader_artifact(
        tmp_path_factory.mktemp(source), f"reader/{source}", "catalog"
    )
    return open_db(path)


def rebuild_fts(conn: sqlite3.Connection) -> None:
    """Repopulate the external-content FTS5 indexes from their content tables."""
    for index in ("register_fts", "variable_fts", "classification_fts"):
        conn.execute(f"INSERT INTO {index}({index}) VALUES('rebuild')")
