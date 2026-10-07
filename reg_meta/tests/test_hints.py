"""Tests for contextual stderr hints."""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING

from cli_test_support import build_cli_source
from reg_meta.cli import run

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _run_capture(argv: list[str]) -> tuple[str, str, int]:
    """Run CLI, capturing stdout and stderr. Returns (stdout, stderr, exit_code)."""
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout = out_buf = io.StringIO()
    sys.stderr = err_buf = io.StringIO()
    try:
        code = run(argv)
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr
    return out_buf.getvalue(), err_buf.getvalue(), code


class TestQuiet:
    def test_quiet_flag_suppresses_hints(self, db_path: str):
        _, err, _ = _run_capture(
            ["-q", "--db", db_path, "search", "--query", "Kommun"],
        )
        assert "hint:" not in err

    def test_env_var_suppresses_hints(self, db_path: str, monkeypatch: object):
        monkeypatch.setenv("REG_META_QUIET", "1")  # type: ignore[attr-defined]
        _, err, _ = _run_capture(["--db", db_path, "search", "--query", "Kommun"])
        assert "hint:" not in err


def test_hints_capped_at_three_with_truncation_hints_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Four hints apply (no value set, several registers, long values, row cap).
    The two that say the table omits data lead, so the cap drops the last
    advisory hint instead of the row-truncation notice."""
    monkeypatch.setenv("COLUMNS", "80")
    db = build_cli_source(tmp_path, "cli-display-limits")
    _, err, code = _run_capture(
        ["--db", db, "--format", "table", "get", "values", "Category"]
    )
    assert code == 0
    assert err == (
        "\n"
        "  hint: Table view truncated 1 rows (--format json for full output)\n"
        "  hint: Long values truncated (--format list for full text)\n"
        "  hint: 1/2 instance(s) had no value set "
        "(elided from table; see JSON for full picture).\n"
    )


class TestJsonClean:
    def test_json_suppresses_hints(self, db_path: str):
        """JSON output should never produce hints, even on stderr."""
        _, err, code = _run_capture(
            [
                "--format",
                "json",
                "--db",
                db_path,
                "get",
                "schema",
                "--register",
                "TESTREG",
            ],
        )
        assert code == 0
        assert "hint:" not in err
