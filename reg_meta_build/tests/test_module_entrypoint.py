"""The builder module propagates the CLI's stable process exit codes."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, EXIT_USAGE

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("exit_code", [0, EXIT_USAGE, EXIT_CONFIG])
def test_module_propagates_cli_exit_code(tmp_path: Path, exit_code: int):
    args = ["--help"] if exit_code == 0 else []
    if exit_code == EXIT_CONFIG:
        database = tmp_path / "reg_meta.db"
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT)"
            )
            connection.execute(
                "INSERT INTO import_manifest VALUES ('schema_version', '0.0.0')"
            )
        args = [
            "--db",
            str(tmp_path),
            "seed-slugs",
            "--out-dir",
            str(tmp_path / "slugs"),
        ]
    result = subprocess.run(
        [sys.executable, "-m", "reg_meta_build", *args],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == exit_code
    if exit_code == EXIT_CONFIG:
        assert json.loads(result.stdout)["error"]["code"] == "schema_incompatible"
