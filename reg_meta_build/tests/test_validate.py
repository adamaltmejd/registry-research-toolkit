"""Tests for the value-set dedup validator.

Exercises the module-level entry point (`validate_built_db`) and the
argparse wiring for `reg-meta-build build-db` (validates by default,
opt out with `--no-validate`). The CLI
handler itself is two lines of glue around `validate_built_db` and
`RegMetaError`; the validator module is the part with logic worth
testing in depth.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from reg_meta_build.validate import validate_built_db


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

    def test_state_value_set_with_no_codes_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7: the projection-integrity check FAILs when a `variable_state`
        names a `value_set` that yields zero codes (a dangling year-projection
        link), not a legitimately code-less state (NULL value_set_id).

        Mint an empty value_set and point an existing state at it."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # An empty value_set (a row with no value_set_member children).
        conn.execute("INSERT INTO value_set (member_hash) VALUES (?)", (b"\xee" * 32,))
        empty_vs = conn.execute("SELECT MAX(value_set_id) FROM value_set").fetchone()[0]
        # Point one state at it → projection yields zero codes for that state.
        state_id = conn.execute("SELECT MIN(state_id) FROM variable_state").fetchone()[
            0
        ]
        conn.execute(
            "UPDATE variable_state SET value_set_id = ? WHERE state_id = ?",
            (empty_vs, state_id),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "yield" in f and "no projected codes" in f for f in result.failures
        ), result.failures

    def test_var_year_codes_anchor_self_skips_on_fixture(self, fixture_db: Path):
        """A2.7: the var_id-24193 code-membership anchor self-skips cleanly when
        the var_id is absent (the synthetic fixture has no var_id 24193), so it
        never falses on a corpus that legitimately lacks the anchor variable.

        It still EMITS its section + an [OK] skip line so a future fixture that
        happens to grow the var_id can't silently drop the check."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[projection: var_id 24193 codes anchor]" in report
        assert "var_id 24193 not present" in report

    @staticmethod
    def _anchor_value_set(conn: sqlite3.Connection, codes: list[str]) -> int:
        """Mint a fresh value_set stocked with ``codes`` and return its id.
        ``value_code`` is content-addressed (UNIQUE code), so reuse-or-insert."""
        conn.execute("INSERT INTO value_set (member_hash) VALUES (?)", (b"\xab" * 32,))
        vs_id = conn.execute("SELECT MAX(value_set_id) FROM value_set").fetchone()[0]
        for code in codes:
            row = conn.execute(
                "SELECT code_id FROM value_code WHERE code = ?", (code,)
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO value_code (code, label) VALUES (?, ?)", (code, code)
                )
                code_id = conn.execute(
                    "SELECT code_id FROM value_code WHERE code = ?", (code,)
                ).fetchone()[0]
            else:
                code_id = row[0]
            conn.execute(
                "INSERT INTO value_set_member (value_set_id, code_id) VALUES (?, ?)",
                (vs_id, code_id),
            )
        return vs_id

    def _plant_anchor(self, conn: sqlite3.Connection, codes: list[str]) -> None:
        """Repoint variable_id=1 to provider_key 24193 and give its state a
        2010-overlapping window linked to a value_set carrying ``codes`` — so the
        anchor resolves the var_id → state → codes path exactly as it does on the
        real corpus."""
        # Match the anchor's (register 34, provider_key 24193) pin. validate runs
        # PRAGMA foreign_key_check (reports dangling FKs regardless of the
        # pragma), so register 34 must exist — insert it borrowing variable_id=1's
        # provider.
        prov = conn.execute(
            "SELECT r.provider_id FROM variable v "
            "JOIN register r ON v.register_id = r.register_id WHERE v.variable_id = 1"
        ).fetchone()[0]
        conn.execute(
            "INSERT OR IGNORE INTO register (register_id, provider_id, name, slug) "
            "VALUES (34, ?, 'Anchor register', 'anchor-reg')",
            (prov,),
        )
        conn.execute(
            "UPDATE variable SET provider_key = '24193', register_id = 34 "
            "WHERE variable_id = 1"
        )
        vs_id = self._anchor_value_set(conn, codes)
        conn.execute(
            "UPDATE variable_state SET valid_from = '2010-01-01', "
            "valid_to = '2010-12-31', value_set_id = ? WHERE variable_id = 1",
            (vs_id,),
        )

    def test_var_year_codes_anchor_passes_on_correct_codes(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7: the anchor PASSES when var_id 24193's 2010 state projects exactly
        the expected codes (01-04) and none of the forbidden ones (00/05)."""
        ok_db = tmp_path / "ok.db"
        ok_db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(ok_db)
        self._plant_anchor(conn, ["01", "02", "03", "04"])
        conn.commit()
        conn.close()
        result = validate_built_db(ok_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "var_id 24193 year 2010 contains ['01', '02', '03', '04']" in report
        assert "var_id 24193 year 2010 excludes 00/05" in report

    def test_var_year_codes_anchor_fails_on_forbidden_code(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7: the anchor FAILs when the 2010 year-projection wrongly INCLUDES a
        forbidden code (05) — the wrong-code-membership bug class the corpus-wide
        >= 1-code check cannot catch (it would pass any non-empty projection)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        self._plant_anchor(conn, ["01", "02", "03", "04", "05"])
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("forbidden codes ['05']" in f for f in result.failures), (
            result.failures
        )

    def test_var_year_codes_anchor_fails_on_missing_code(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7: the anchor FAILs when an expected code (04) is dropped from the
        2010 projection — guards a year-projection that under-includes."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        self._plant_anchor(conn, ["01", "02", "03"])
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("missing codes ['04']" in f for f in result.failures), (
            result.failures
        )

    def test_var_year_codes_anchor_fails_when_present_but_no_year_overlap(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7 (Codex P2 #149): when var_id 24193 is PRESENT (register 34) but no
        `variable_state` overlaps the anchor year, that is a year-window/coalescing
        regression — a FAIL — not the 'variable absent' skip. Distinguishing the
        two is the whole point: a broken validity window must not masquerade as a
        legitimate skip on a corpus that does carry the anchor variable."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        self._plant_anchor(conn, ["01", "02", "03", "04"])
        # Shove the planted state's window off 2010 entirely: the variable
        # (register 34, provider_key 24193) still exists, but nothing overlaps.
        conn.execute(
            "UPDATE variable_state SET valid_from = '2015-01-01', "
            "valid_to = '2015-12-31' WHERE variable_id = 1"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "present but no state overlaps 2010" in f for f in result.failures
        ), result.failures

    def test_variable_alias_missing_state_column_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A2.7 (Codex P2 #149): the invariant FAILs when a `variable_state`
        carries a delivery column absent from `variable_alias` — i.e. the reparent
        regressed and the catalog API would miss a column the data actively
        uses."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        sid = conn.execute("SELECT MIN(state_id) FROM variable_state").fetchone()[0]
        conn.execute(
            "UPDATE variable_state SET delivery_column_name = 'GHOSTCOL_NO_ALIAS' "
            "WHERE state_id = ?",
            (sid,),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("missing from variable_alias" in f for f in result.failures), (
            result.failures
        )


class TestBuildDbProvidersDefault:
    def test_cli_default_is_combined_scb_sos(self):
        """A4.5: the CLI `--providers` default is the combined `scb,sos` build.
        `--providers scb` still selects the SCB-only DB (the A4.3b byte-identical
        gate). Only the CLI surface flipped — `build_db()`'s function default
        stays `('scb',)` so synthetic SCB-only fixtures need no SOS workbooks."""
        from reg_meta_build.cli import _build_parser

        parser = _build_parser()
        ns = parser.parse_args(["build-db", "--input-dir", "x"])
        assert ns.providers == "scb,sos"
        ns = parser.parse_args(["build-db", "--input-dir", "x", "--providers", "scb"])
        assert ns.providers == "scb"


class TestBuildDbValidateFlag:
    def test_argparse_exposes_no_validate(self):
        """Validation is on by default; `--no-validate` is the opt-out wired
        into `reg-meta-build build-db`'s argparse subparser."""
        from reg_meta_build.cli import _build_parser

        parser = _build_parser()
        ns = parser.parse_args(["build-db", "--input-dir", "x"])
        assert ns.no_validate is False
        ns = parser.parse_args(["build-db", "--input-dir", "x", "--no-validate"])
        assert ns.no_validate is True

    def test_failed_validation_does_not_replace_installed_db(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Regression for Copilot review on PR #99: a failing validation
        run must not leave the staging DB installed at `<db_dir>/reg_meta.db`.
        Pre-populates the install path with a sentinel, builds with a hook
        that always fails, and asserts the sentinel is preserved."""
        import sys as _sys

        _sys.path.insert(0, str(Path(__file__).parent))
        from _csv_fixtures import write_scb_input
        from reg_meta.db import DB_FILENAME
        from reg_meta.errors import RegMetaError
        from reg_meta_build.db import build_db

        from reg_meta_build import validate as validate_mod

        input_dir = tmp_path / "input"
        db_dir = tmp_path / "db"
        input_dir.mkdir()
        db_dir.mkdir()
        write_scb_input(input_dir)

        sentinel = db_dir / DB_FILENAME
        sentinel_bytes = b"SENTINEL-PREVIOUS-DB-MUST-SURVIVE"
        sentinel.write_bytes(sentinel_bytes)

        def always_fail(
            _db_path: Path, *, corpus: bool = False
        ) -> validate_mod.ValidationResult:
            r = validate_mod.ValidationResult()
            r.fail("synthetic invariant breach")
            return r

        monkeypatch.setattr(validate_mod, "validate_built_db", always_fail)
        # Also patch the re-export in the build CLI module so the handler
        # closure sees the fake.
        from reg_meta_build import cli as cli_mod

        monkeypatch.setattr(cli_mod, "validate_built_db", always_fail)

        with pytest.raises(RegMetaError) as exc_info:
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
