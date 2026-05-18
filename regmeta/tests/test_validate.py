"""Tests for the value-set dedup validator (issue #92).

Exercises the module-level entry point (`validate_built_db`) and the
argparse wiring for `regmeta maintain build-db --validate`. The CLI
handler itself is two lines of glue around `validate_built_db` and
`RegmetaError`; the validator module is the part with logic worth
testing in depth.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from regmeta.validate import validate_built_db


class TestValidateModule:
    def test_passes_on_fresh_fixture_db(self, fixture_db: Path):
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[schema]" in report
        assert "[OK] value_set present" in report

    def test_missing_db_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            validate_built_db(tmp_path / "no_such.db")

    def test_dropped_value_set_table_surfaces_failures(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A DB missing `value_set` must fail the schema-shape check
        without crashing the dependent projection queries."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute("DROP TABLE value_set_member")
        conn.execute("DROP TABLE value_set")
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("value_set missing" in f for f in result.failures)
        assert any("value_set_member missing" in f for f in result.failures)

    def test_legacy_table_resurrection_is_failure(
        self, fixture_db: Path, tmp_path: Path
    ):
        """The schema invariant requires `cvid_value_code` / `value_item` /
        `value_item_validity` to be absent post-rebuild."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute("CREATE TABLE cvid_value_code (cvid INTEGER)")
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "cvid_value_code should have been dropped" in f for f in result.failures
        )


class TestBuildDbValidateFlag:
    def test_argparse_exposes_validate(self):
        """The `--validate` flag is wired into `maintain build-db`'s
        argparse subparser; default is False so existing callers that
        omit it are unaffected."""
        from regmeta.cli import _build_parser

        parser = _build_parser()
        ns = parser.parse_args(
            ["maintain", "build-db", "--input-dir", "x", "--validate"]
        )
        assert ns.validate is True
        ns = parser.parse_args(["maintain", "build-db", "--input-dir", "x"])
        assert ns.validate is False

    def test_failed_validation_does_not_replace_installed_db(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Regression for Copilot review on PR #99: a failing `--validate`
        run must not leave the staging DB installed at `<db_dir>/regmeta.db`.
        Pre-populates the install path with a sentinel, builds with a hook
        that always fails, and asserts the sentinel is preserved."""
        import sys as _sys

        _sys.path.insert(0, str(Path(__file__).parent))
        from _csv_fixtures import write_scb_input

        from regmeta import validate as validate_mod
        from regmeta.db import DB_FILENAME, build_db
        from regmeta.errors import RegmetaError

        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        input_dir.mkdir()
        db_dir.mkdir()
        write_scb_input(input_dir)

        sentinel = db_dir / DB_FILENAME
        sentinel_bytes = b"SENTINEL-PREVIOUS-DB-MUST-SURVIVE"
        sentinel.write_bytes(sentinel_bytes)

        def always_fail(_db_path: Path) -> validate_mod.ValidationResult:
            r = validate_mod.ValidationResult()
            r.fail("synthetic invariant breach")
            return r

        monkeypatch.setattr(validate_mod, "validate_built_db", always_fail)
        # Also patch the re-export in cli so the handler sees the fake.
        from regmeta import cli as cli_mod

        monkeypatch.setattr(cli_mod, "validate_built_db", always_fail)

        with pytest.raises(RegmetaError) as exc_info:
            build_db(
                input_dir=input_dir,
                db_dir=db_dir,
                skip_classifications=True,
                skip_slugs=True,
                pre_rename_hook=cli_mod._build_validate_hook(),
            )
        assert exc_info.value.code == "validation_failed"
        # The prior DB is untouched and the failed staging file is gone.
        assert sentinel.read_bytes() == sentinel_bytes
        tmp_file = sentinel.with_suffix(".db.tmp")
        assert not tmp_file.exists()
