"""CLI output formats: envelope, human-readable rendering and display limits."""

from __future__ import annotations

import sqlite3

import pytest
from cli_test_support import build_cli_source, run_json as _run_json
from reader_artifacts import stamp_catalog_identity
from reg_meta.cli import run


@pytest.fixture(scope="module")
def limits_db(tmp_path_factory: pytest.TempPathFactory) -> str:
    return build_cli_source(tmp_path_factory.mktemp("limits"), "cli-display-limits")


def test_default_format_lists_few_rows_and_tabulates_many(
    limits_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    get = ["--db", limits_db, "get"]
    assert run([*get, "varinfo", "Category", "--register", "Wide"]) == 0
    one_row = capsys.readouterr().out
    assert "  register_id  " in one_row
    assert "---" not in one_row
    assert (
        run([*get, "values", "Category", "--register", "Wide", "--year", "2020"]) == 0
    )
    many_rows = capsys.readouterr().out.splitlines()
    assert many_rows[0].split() == ["code", "label"]
    assert set(many_rows[1]) <= {"-", " "}


# ---------------------------------------------------------------------------
# Envelope and error model
# ---------------------------------------------------------------------------


class TestOutputFormats:
    def test_json_envelope(self, db_path: str):
        data, _ = _run_json(["--db", db_path, "search", "--query", "test"])
        assert data["contract_version"] == "3.0.0"
        assert "generated_at" in data
        assert "request" in data
        assert "database" in data
        assert "data" in data
        assert "duration_ms" in data["run"]

    def test_json_no_verbose_is_data_only(self, db_path: str):
        data, code = _run_json(
            ["--db", db_path, "search", "--query", "Kön"], verbose=False
        )
        assert code == 0
        assert "contract_version" not in data
        assert "run" not in data
        assert "has_more" in data

    def test_repeated_flag_errors(self, db_path: str):
        """Repeated optional flags should error, not silently overwrite."""
        _, code = _run_json(
            ["--db", db_path, "--db", db_path, "search", "--query", "test"]
        )
        assert code == 2

    def test_diff_output_file_has_all_sections(self, db_path: str, tmp_path):
        """--output with get diff must include all sections, not just the last."""
        out = tmp_path / "diff.txt"
        code = run(
            [
                "--db",
                db_path,
                "--output",
                str(out),
                "get",
                "diff",
                "--register",
                "TESTREG",
                "--from",
                "2020",
                "--to",
                "2022",
                "--variable",
                "Kön",
                "TestVar",
            ]
        )
        assert code == 0
        content = out.read_text(encoding="utf-8")
        # Multi-section: resolved variables header + diff table + unchanged footer
        assert "Kön" in content
        assert "TestVar" in content
        assert "Unchanged" in content

    def test_no_command(self):
        _, code = _run_json([])
        assert code == 2


# ---------------------------------------------------------------------------
# Schema version gate (A2.7: 4.x → 5.0.0 break)
# ---------------------------------------------------------------------------


class TestSchemaCompat:
    """A2.7 bumped SCHEMA_VERSION to 5.0.0 (major break). A v4.x DB — which
    still carries `variable_instance` + a cvid-keyed `variable_alias` and no
    `variable_state.classification_id` — must be rejected with an actionable
    'rebuild' error via the major-version gate (4 != 5)."""

    @staticmethod
    def _db_with_manifest_version(tmp_path, version: str):

        from reg_meta_build.db import DDL

        db = tmp_path / "reg_meta.db"
        conn = sqlite3.connect(str(db))
        conn.executescript(DDL)
        stamp_catalog_identity(conn)
        conn.execute(
            "UPDATE import_manifest SET value=? WHERE key='schema_version'", (version,)
        )
        conn.commit()
        conn.close()
        return db

    def test_v4_db_rejected(self, tmp_path):
        from reg_meta.db import open_db
        from reg_meta.errors import EXIT_CONFIG, RegMetaError

        db = self._db_with_manifest_version(tmp_path, "4.9.0")
        with pytest.raises(RegMetaError) as exc:
            open_db(db, check_schema=True)
        assert exc.value.code == "schema_incompatible"
        assert exc.value.exit_code == EXIT_CONFIG
        assert "update" in exc.value.remediation.lower()
