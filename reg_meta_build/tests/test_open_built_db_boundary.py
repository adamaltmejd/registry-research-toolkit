"""The builder's catalog open, at its located refusal.

The case asserts on the located ``RegMetaError`` of ``open_built_db``.
"""

from __future__ import annotations

import gc
import sqlite3
import warnings
from typing import TYPE_CHECKING

from reg_meta_build.db import SCHEMA_VERSION, open_built_db
from reg_meta_build.errors import EXIT_CONFIG, RegMetaError

if TYPE_CHECKING:
    from pathlib import Path


def _refusal(path: Path) -> tuple[str, int, str]:
    """Open ``path`` and return the refusal's code, exit code and message only, so
    no traceback keeps the builder's frames (and their connection) alive."""
    try:
        open_built_db(path)
    except RegMetaError as error:
        return error.code, error.exit_code, error.message
    raise AssertionError(f"open_built_db accepted {path.name}")


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


def test_builder_refuses_a_catalog_of_another_schema_version(tmp_path: Path) -> None:
    """A catalog whose manifest names another schema version (6.18.0) is refused as
    `schema_incompatible` (exit 10), and the message names both that version and the
    builder's.

    Fails if `open_built_db` stops comparing the manifest's `schema_version` with
    `SCHEMA_VERSION` (opening an older catalog as current), or drops either version
    from the refusal.
    """
    path = tmp_path / "old-schema.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT)"
        )
        connection.execute(
            "INSERT INTO import_manifest VALUES ('schema_version', '6.18.0')"
        )
    connection.close()
    code, exit_code, message = _refusal(path)
    assert code == "schema_incompatible"
    assert exit_code == EXIT_CONFIG
    assert "'6.18.0'" in message and SCHEMA_VERSION in message
