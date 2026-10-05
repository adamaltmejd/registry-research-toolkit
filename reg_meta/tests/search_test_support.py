"""Shared support for the search test modules.

`reader_search_conn` opens a readable source under `conformance/cases/reader/`
built through the real resolved-catalog writer; it is the source form new search
boundary tests use.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from reader_artifacts import build_reader_artifact
from reg_meta.db import open_db

if TYPE_CHECKING:
    import sqlite3

    import pytest


def reader_search_conn(
    tmp_path_factory: pytest.TempPathFactory, source: str
) -> sqlite3.Connection:
    """Open the catalog artifact built from `conformance/cases/reader/<source>`."""
    path = build_reader_artifact(
        tmp_path_factory.mktemp(source), f"reader/{source}", "catalog"
    )
    return open_db(path)
