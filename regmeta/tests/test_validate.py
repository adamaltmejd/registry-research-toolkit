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
