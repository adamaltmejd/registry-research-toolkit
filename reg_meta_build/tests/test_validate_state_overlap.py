"""State-overlap guards (code-less vs code-bearing, pooled) at the validate_built_db boundary."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from _shared_fixtures import connect_built_db
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path


class TestValidateModule:
    def test_codeless_codebearing_overlap_passes_on_fixture(self, fixture_db: Path):
        """Clean build: the backstop emits its section and reports OK — the
        synthetic fixture (like a real #867/#868-resolved build) has no code-less
        window overlapping a code-bearing window on one column."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[invariant: no code-less ↔ code-bearing window overlap]" in report, (
            report
        )
        assert "no code-less state overlaps a code-bearing state" in report, report

    @staticmethod
    def _inject_codeless_overlap(src: Path, dst: Path) -> None:
        """Clone ``src`` and add a code-less (``value_set_id IS NULL``) state
        whose window overlaps an existing code-bearing state on the SAME
        ``(variable_id, register_variant_id, delivery_column_name)`` — the exact
        regression the backstop guards. Returns nothing; mutates ``dst``."""
        dst.write_bytes(src.read_bytes())
        conn = sqlite3.connect(dst)
        # An existing code-bearing state to overlap (named column, real value
        # set). Its window is the one the injected code-less state will straddle.
        vid, rvid, col, vf, vt = conn.execute(
            "SELECT variable_id, register_variant_id, delivery_column_name, "
            "       valid_from, valid_to "
            "FROM variable_state "
            "WHERE value_set_id IS NOT NULL AND delivery_column_name IS NOT NULL "
            "LIMIT 1"
        ).fetchone()
        # Identical window (true overlap) but a distinct value_set_version_label
        # so the injected row clears the (variable, variant, valid_from, label)
        # uniqueness index — the test targets the overlap guard, not that index.
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, "
            " delivery_column_name, value_set_id, value_set_version_label) "
            "VALUES (?, ?, ?, ?, ?, NULL, 'codeless-inject')",
            (vid, rvid, vf, vt, col),
        )
        conn.commit()
        conn.close()

    def test_codeless_codebearing_overlap_fails(self, fixture_db: Path, tmp_path: Path):
        """The regression backstop: a code-less state overlapping a code-bearing
        state on one ``(variable, variant, column)`` must FAIL the build — this
        is the future-regression guard for the #867/#868 resolvers."""
        broken = tmp_path / "broken.db"
        self._inject_codeless_overlap(fixture_db, broken)
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "code-less ↔ code-bearing overlapping state pair" in f
            for f in result.failures
        ), result.failures

    def test_codeless_codebearing_overlap_folds_column_casing(
        self, fixture_db: Path, tmp_path: Path
    ):
        """Y-131: a code-less twin spelled in a different case (``kON`` vs the
        code-bearing ``Kon``) must still FAIL — the consumer matches columns
        folded (``py_lower``), so the final guard must too. Full
        ``validate_built_db`` path, proving the registered UDF covers the
        folded predicate (same-case is the pre-existing
        ``test_codeless_codebearing_overlap_fails``)."""
        mixed = tmp_path / "mixed.db"
        mixed.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(mixed)
        vid, rvid, col, vf, vt = conn.execute(
            "SELECT variable_id, register_variant_id, delivery_column_name, "
            "       valid_from, valid_to "
            "FROM variable_state "
            "WHERE value_set_id IS NOT NULL AND delivery_column_name IS NOT NULL "
            "LIMIT 1"
        ).fetchone()
        twin = col.swapcase()
        assert twin != col and twin.lower() == col.lower()
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, "
            " delivery_column_name, value_set_id, value_set_version_label) "
            "VALUES (?, ?, ?, ?, ?, NULL, 'codeless-fold-inject')",
            (vid, rvid, vf, vt, twin),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(mixed)
        assert not result.passed
        assert any(
            "code-less ↔ code-bearing overlapping state pair" in f
            for f in result.failures
        ), result.failures

    def test_pooled_column_present_on_fixture(self, fixture_db: Path):
        """Y-202: the schema-shape gate sees the pooled marker column."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        assert "[OK] variable_state.pooled present" in result.format_report()

    def test_pooled_explicit_overlap_passes_on_fixture(self, fixture_db: Path):
        """Y-202: clean build — no pooled state overlaps an explicit state on
        one (variable, variant, column), so the guard reports OK."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[invariant: no pooled ↔ explicit window overlap]" in report, report
        assert "no pooled state overlaps an explicit state on one column" in report

    @staticmethod
    def _inject_pooled_overlap(src: Path, dst: Path) -> None:
        """Clone ``src`` and add a pooled (``pooled = 1``) state on the SAME
        window, (variable, variant, column) — and the SAME value set — as an
        existing explicit state: the exact pooled-loses-to-annual regression
        the backstop guards. Same value set keeps the one-value-set and
        code-less guards silent, so only the pooled guard fires."""
        dst.write_bytes(src.read_bytes())
        conn = connect_built_db(dst)
        vid, rvid, col, vf, vt, vsid, dt, dl = conn.execute(
            "SELECT variable_id, register_variant_id, delivery_column_name, "
            "       valid_from, valid_to, value_set_id, data_type, data_length "
            "FROM variable_state "
            "WHERE delivery_column_name IS NOT NULL "
            "LIMIT 1"
        ).fetchone()
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, "
            " delivery_column_name, data_type, data_length, value_set_id, "
            " value_set_version_label, pooled) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pooled-inject', 1)",
            (vid, rvid, vf, vt, col, dt, dl, vsid),
        )
        conn.commit()
        conn.close()

    def test_pooled_explicit_overlap_fails(self, fixture_db: Path, tmp_path: Path):
        """Y-202: a pooled state overlapping an explicit state on one
        (variable, variant, column) must FAIL the build."""
        broken = tmp_path / "broken-pooled.db"
        self._inject_pooled_overlap(fixture_db, broken)
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "pooled ↔ explicit overlapping state pair" in f for f in result.failures
        ), result.failures
