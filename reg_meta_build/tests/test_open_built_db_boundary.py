"""The builder's catalog open, at its located refusal.

The case asserts on the located ``RegMetaError`` of ``open_built_db``.
"""

from __future__ import annotations

import gc
import sqlite3
import warnings
from typing import TYPE_CHECKING

from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.db import open_built_db

if TYPE_CHECKING:
    from pathlib import Path


def _refusal(path: Path) -> tuple[str, int, str]:
    """Open ``path`` and return the refusal's code, exit code and message only, so
    no traceback keeps the builder's frames (and their connection) alive."""
    try:
        open_built_db(path)
    except RegMetaError as error:
        return error.code, error.exit_code, error.message
    raise AssertionError("open_built_db accepted a catalog without a manifest")


def test_builder_refuses_a_catalog_without_a_manifest(tmp_path: Path) -> None:
    """A SQLite file without ``import_manifest`` is a located configuration error,
    and the refused connection is closed (Python's sqlite3 emits a
    ``ResourceWarning`` when an unclosed connection is garbage-collected)."""
    path = tmp_path / "missing-manifest.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE dummy (x TEXT)")
    connection.close()
    # Collect first so connections leaked by earlier tests in this worker are not
    # attributed to this refusal.
    gc.collect()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        code, exit_code, message = _refusal(path)
        gc.collect()
    assert code == "schema_incompatible"
    assert exit_code == EXIT_CONFIG
    assert "manifest is missing or unreadable" in message
    assert not [w for w in caught if issubclass(w.category, ResourceWarning)]
