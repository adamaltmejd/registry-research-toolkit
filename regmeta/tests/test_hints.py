"""Tests for contextual stderr hints."""

from __future__ import annotations

import io
import sys

from regmeta.cli import run


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


class TestSearchHints:
    def test_all_fields_hint(self, db_path: str):
        _, err, code = _run_capture(["--db", db_path, "search", "--query", "Kommun"])
        assert code == 0
        assert "Searching all fields" in err

    def test_no_all_fields_hint_when_field_specified(self, db_path: str):
        _, err, code = _run_capture(
            ["--db", db_path, "search", "--query", "Kommun", "--field", "varname"],
        )
        assert code == 0
        assert "Searching all fields" not in err

    def test_no_results_hint(self, db_path: str):
        _, err, code = _run_capture(
            ["--db", db_path, "search", "--query", "xyznonexistent99"],
        )
        assert code == 0
        assert "No results" in err


class TestSchemaHints:
    def test_full_schema_hint(self, db_path: str):
        _, err, code = _run_capture(
            ["--db", db_path, "get", "schema", "--register", "TESTREG"],
        )
        assert code == 0
        assert "--summary" in err

    def test_no_hint_with_summary(self, db_path: str):
        _, err, code = _run_capture(
            ["--db", db_path, "get", "schema", "--register", "TESTREG", "--summary"],
        )
        assert code == 0
        assert "--summary" not in err

    def test_no_hint_with_flat(self, db_path: str):
        _, err, code = _run_capture(
            ["--db", db_path, "get", "schema", "--register", "TESTREG", "--flat"],
        )
        assert code == 0
        assert "--summary" not in err


class TestQuiet:
    def test_quiet_flag_suppresses_hints(self, db_path: str):
        _, err, _ = _run_capture(
            ["-q", "--db", db_path, "search", "--query", "Kommun"],
        )
        assert "hint:" not in err

    def test_env_var_suppresses_hints(self, db_path: str, monkeypatch: object):
        monkeypatch.setenv("REGMETA_QUIET", "1")  # type: ignore[attr-defined]
        _, err, _ = _run_capture(["--db", db_path, "search", "--query", "Kommun"])
        assert "hint:" not in err


class TestHintCap:
    def test_max_three_hints(self, db_path: str):
        """Even when many hints could fire, at most 3 are shown."""
        import regmeta.cli

        old_max = regmeta.cli._MAX_DISPLAY_ROWS
        try:
            regmeta.cli._MAX_DISPLAY_ROWS = 1
            _, err, _ = _run_capture(
                ["--db", db_path, "search", "--query", "Kommun"],
            )
            hint_count = err.count("hint:")
            assert 0 < hint_count <= 3
        finally:
            regmeta.cli._MAX_DISPLAY_ROWS = old_max


class TestJsonClean:
    def test_json_data_has_no_hint_keys(self, db_path: str):
        import json

        out, err, code = _run_capture(
            ["--format", "json", "--db", db_path, "search", "--query", "Kommun"],
        )
        assert code == 0
        data = json.loads(out)
        assert "doc_hint" not in data

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
