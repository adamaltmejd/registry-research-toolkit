"""The frozen `reg-meta validate` CLI fails fast on a missing catalog.

The CLI and FastAPI project adapters' agreement over `cases/validate` went with the
FastAPI routes (3e.4): the Rust server answers `/api/project/*`, its `api` twins pin
the answers, and G1 compares it with the CLI baseline.
"""

from __future__ import annotations

import json

from reg_meta.cli import run
from reg_meta.errors import EXIT_CONFIG


def test_cli_validate_fails_fast_on_a_missing_catalog(tmp_path, capsys):
    """The catalog is required before the project is read, so a missing catalog
    is reported as such (exit 10, `db_not_found`) even for a project the CLI
    could not have validated anyway."""
    code = run(
        ["--db", str(tmp_path / "absent"), "validate", str(tmp_path / "nope.json")]
    )
    error = json.loads(capsys.readouterr().out)["error"]
    assert (code, error["code"]) == (EXIT_CONFIG, "db_not_found")
