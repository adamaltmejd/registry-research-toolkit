"""Shared support for the search test modules.

`reader_search_conn` opens a readable source under `conformance/cases/reader/`
built through the real resolved-catalog writer; it is the source form new search
boundary tests use. The `_slugged_db` builders and `rebuild_fts` serve the older
search tests that seed explicit rows.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from reader_artifacts import CASES, build_reader_artifact, replicate_filler
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
    tmp_path_factory: pytest.TempPathFactory, source: str, kind: str = "catalog"
) -> sqlite3.Connection:
    """Open the artifact built from `conformance/cases/reader/<source>`.

    A case whose `request.json` names a `filler` is expanded first
    (`replicate_filler`); other reader cases use `request.json` differently.
    """
    directory = tmp_path_factory.mktemp(source)
    case = CASES / "reader" / source
    request = case / "request.json"
    fixture = (
        replicate_filler(case, directory / "source")
        if request.exists() and "filler" in json.loads(request.read_text())
        else case
    )
    return open_db(build_reader_artifact(directory / "artifact", fixture, kind))


def rebuild_fts(conn: sqlite3.Connection) -> None:
    """Repopulate the external-content FTS5 indexes from their content tables."""
    for index in ("register_fts", "variable_fts", "classification_fts"):
        conn.execute(f"INSERT INTO {index}({index}) VALUES('rebuild')")
