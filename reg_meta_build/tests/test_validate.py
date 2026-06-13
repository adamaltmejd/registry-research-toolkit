"""Tests for the value-set dedup validator.

Exercises the module-level entry point (`validate_built_db`) and the
argparse wiring for `reg-meta-build build-db` (validates by default,
opt out with `--no-validate`). The CLI
handler itself is two lines of glue around `validate_built_db` and
`RegMetaError`; the validator module is the part with logic worth
testing in depth.
"""

from __future__ import annotations

import json
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

    def test_value_code_search_checks_pass(self, fixture_db: Path):
        """#352: the schema-shape + value-code-search sections recognize
        value_code.mapping_count and value_code_fts on a fresh build."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[OK] value_code.mapping_count present" in report
        assert "[OK] value_code_fts present" in report
        assert "[value-code search]" in report

    def test_missing_value_code_fts_surfaces_failure(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#352: dropping value_code_fts must fail the schema-shape check."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute("DROP TABLE value_code_fts")
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("value_code_fts missing" in f for f in result.failures)

    def test_empty_fts_index_fails_corpus_check(self, fixture_db: Path, tmp_path: Path):
        """#352: a populated-schema build whose value_code_fts INDEX is empty (table
        present, content cleared) must FAIL the corpus value-code-search check. Uses
        the FTS5 'delete-all' command so the table stays but the `_docsize` shadow
        count drops to 0 — the honest indexed-row count the check reads (COUNT(*)
        would still read the content table and miss this). Calls the check directly
        so the synthetic fixture's SOS-volume corpus gate doesn't muddy the result."""
        from reg_meta_build.validate import ValidationResult, _check_value_code_search

        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.row_factory = sqlite3.Row
        # Precondition: the index is non-empty before we clear it.
        assert (
            conn.execute("SELECT COUNT(*) FROM value_code_fts_docsize").fetchone()[0]
            > 0
        )
        conn.execute("INSERT INTO value_code_fts(value_code_fts) VALUES('delete-all')")
        conn.commit()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        result = ValidationResult()
        _check_value_code_search(conn, result, tables, corpus=True)
        conn.close()
        assert not result.passed
        assert any("EMPTY" in f for f in result.failures), result.failures

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

    def test_open_ended_sentinel_passes_and_reports_on_fixture(self, fixture_db: Path):
        """The sentinel-exactness check runs on the fixture and emits its
        section, so a regression can't silently drop it from the suite."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        assert "[window: open-ended valid_to sentinel]" in result.format_report()

    def test_near_sentinel_state_valid_to_fails(self, fixture_db: Path, tmp_path: Path):
        """A 9999-prefixed `variable_state.valid_to` that is not exactly
        '9999-12-31' must FAIL: downstream display (reg_webapp catalog routes
        + the SPA's formatWindow) branches on the exact literal, so a
        near-sentinel like '9999-06-30' would render as a garbage period
        token instead of an open window."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        state_id = conn.execute("SELECT MIN(state_id) FROM variable_state").fetchone()[
            0
        ]
        conn.execute(
            "UPDATE variable_state SET valid_to = '9999-06-30' WHERE state_id = ?",
            (state_id,),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "variable_state" in f and "9999-06-30" in f for f in result.failures
        ), result.failures

    def test_near_sentinel_lineage_valid_to_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """Same exactness invariant on `variable_state_lineage.valid_to` (the
        lineage edge windows carry the same '9999-12-31' open-end sentinel).

        Plants '9999-00-00': malformed, sorts BELOW '9999-01-01', and lineage
        has no date CHECK at all — so this locks the prefix match (a
        `>= '9999-01-01'` range predicate would miss it)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        state_id = conn.execute("SELECT MIN(state_id) FROM variable_state").fetchone()[
            0
        ]
        conn.execute(
            "INSERT INTO variable_state_lineage "
            "(consumer_state_id, source_state_id, valid_from, valid_to) "
            "VALUES (?, ?, '2000-01-01', '9999-00-00')",
            (state_id, state_id),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "variable_state_lineage" in f and "9999-00-00" in f for f in result.failures
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

    def test_delivery_column_whitespace_fails(self, fixture_db: Path, tmp_path: Path):
        """Hygiene invariant: a delivery_column_name with surrounding
        whitespace (on a state OR an alias) fails — the SCB read boundary
        trims, so any padded value in a shipped DB is a build regression.
        Tab-padded deliberately: the check must match `str.strip()` semantics
        (all whitespace), not SQLite TRIM() (ASCII space only)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        # Pad a matched alias+state pair in lockstep so only the hygiene
        # check (not the alias-covers-state-columns projection) fires.
        sid, col = conn.execute(
            "SELECT state_id, delivery_column_name FROM variable_state "
            "WHERE delivery_column_name IS NOT NULL LIMIT 1"
        ).fetchone()
        conn.execute(
            "UPDATE variable_state SET delivery_column_name = ? WHERE state_id = ?",
            (col + "\t", sid),
        )
        conn.execute(
            "UPDATE variable_alias SET delivery_column_name = ? "
            "WHERE delivery_column_name = ?",
            (col + "\t", col),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("surrounding whitespace" in f for f in result.failures), (
            result.failures
        )

    def test_empty_delivery_column_alias_fails(self, fixture_db: Path, tmp_path: Path):
        """Hygiene invariant: '' is not a delivery header — a no-header
        variable is a NULL state column + alias-row absence, never an
        empty-string alias row."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        vid, rvid = conn.execute(
            "SELECT variable_id, register_variant_id FROM variable_alias LIMIT 1"
        ).fetchone()
        conn.execute(
            "INSERT INTO variable_alias "
            "(variable_id, register_variant_id, delivery_column_name) "
            "VALUES (?, ?, '')",
            (vid, rvid),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any("empty-string delivery_column_name" in f for f in result.failures), (
            result.failures
        )

    def test_name_field_whitespace_fails(self, fixture_db: Path, tmp_path: Path):
        """Hygiene invariant (#366): a variable/register/register_variant
        `name` with surrounding whitespace fails — the SCB read boundary
        trims, so any padded name in a shipped DB is a build regression.
        Tab-padded deliberately: the check must match `str.strip()` semantics
        (all whitespace), not SQLite TRIM() (ASCII space only)."""
        for table, column in (
            ("variable", "name"),
            ("register", "name"),
            ("register_variant", "name"),
        ):
            broken = tmp_path / f"broken_{table}.db"
            broken.write_bytes(fixture_db.read_bytes())
            conn = sqlite3.connect(broken)
            rowid, val = conn.execute(
                f"SELECT rowid, {column} FROM {table} "
                f"WHERE {column} IS NOT NULL LIMIT 1"
            ).fetchone()
            conn.execute(
                f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
                (val + "\t", rowid),
            )
            conn.commit()
            conn.close()
            result = validate_built_db(broken)
            assert not result.passed, (table, column)
            assert any(
                f"{table}.{column} value(s) with surrounding whitespace" in f
                for f in result.failures
            ), (table, result.failures)

    def test_panel_refs_skip_when_no_variant_carries_them(self, fixture_db: Path):
        """A4.4c: the unmodified fixture carries NO panel refs (all variants have
        NULL panel keys), so the resolution check self-passes but still EMITS its
        section + an [OK] line so a future fixture that grows panel data can't
        silently drop the gate."""
        result = validate_built_db(fixture_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[panel: refs resolve to register-scoped variable slugs]" in report
        assert "no variant carries panel refs" in report

    def test_panel_ref_good_and_period_exempt_pass(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A4.4c: a `panel_entity_key` naming a real variable slug in the variant's
        register resolves, and the literal "period" `panel_time_key` sentinel is
        exempt (it's delivery-aligned time, not a variable slug). Variant 10 lives
        in register 1 (TESTREG), whose variables include slug `kon`."""
        ok_db = tmp_path / "ok.db"
        ok_db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(ok_db)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = 'kon', "
            "panel_time_key = 'period' WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(ok_db)
        assert result.passed, result.failures
        assert "all 1 panel ref(s)" in result.format_report()

    def test_panel_ref_dangling_fails(self, fixture_db: Path, tmp_path: Path):
        """A4.4c: a `panel_time_key` naming a slug that exists in NO variable for
        the variant's register is a dangling reference — a FAIL."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute(
            "UPDATE register_variant SET panel_time_key = 'nonexistent_slug' "
            "WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_time_key 'nonexistent_slug' resolves to no variable.slug" in f
            for f in result.failures
        ), result.failures

    def test_panel_ref_wrong_register_fails(self, fixture_db: Path, tmp_path: Path):
        """A4.4c: slug is only register-unique. `parencol` exists under register 2
        (OTHERREG) but NOT register 1 (TESTREG). A panel ref on variant 10
        (register 1) pointing at `parencol` must FAIL — resolution is scoped to the
        variant's own register, so a cross-register slug is dangling."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = 'parencol' "
            "WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_entity_key 'parencol' resolves to no variable.slug" in f
            for f in result.failures
        ), result.failures

    def test_panel_ref_composite_array_element_miss_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """A4.4c: a composite `panel_entity_key` is a json-array string; it resolves
        element-wise. ANY element that fails to resolve is a finding. Here `kon`
        resolves (register 1) but `ghost` does not — the array must FAIL on the
        bad element while leaving the good one alone."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = ? "
            "WHERE register_variant_id = 10",
            (json.dumps(["kon", "ghost"]),),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_entity_key 'ghost' resolves to no variable.slug" in f
            for f in result.failures
        ), result.failures
        # The resolving element must NOT produce a finding.
        assert not any("'kon' resolves to no" in f for f in result.failures), (
            result.failures
        )

    def test_panel_ref_resolves_but_stateless_in_variant_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#287: the strict check's signature mis-point — a key that RESOLVES
        (the variable exists in the variant's register) but whose states all
        live in a SIBLING variant. Variant 999 is added next to variant 10 in
        register 1; `kon`'s states are in variant 10 only, so the resolution
        check passes and the states check must FAIL."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        reg = conn.execute(
            "SELECT register_id FROM register_variant WHERE register_variant_id = 10"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, "
            "slug, name, panel_entity_key, panel_time_key, panel_time_grain) "
            "VALUES (999, ?, 'sibling', 'Sibling', 'kon', 'period', 'delivery')",
            (reg,),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_entity_key 'kon' has no variable_state rows in that variant" in f
            for f in result.failures
        ), result.failures
        # The resolution check must NOT have fired — the slug resolves fine.
        assert not any("resolves to no variable.slug" in f for f in result.failures), (
            result.failures
        )

    def test_panel_ref_with_states_in_variant_passes_strict(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#287: `kon` carries states in variant 10 itself, so the strict check
        emits its [OK] line (and the period time-key sentinel stays exempt)."""
        ok_db = tmp_path / "ok.db"
        ok_db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(ok_db)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = 'kon', "
            "panel_time_key = 'period' WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        result = validate_built_db(ok_db)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[panel: entity key has states in the variant]" in report
        assert "have states in their variant" in report

    def test_panel_ref_composite_stateless_element_fails_strict(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#287: composite keys check element-wise in the strict pass too. The
        json-array key decodes and its `kon` element resolves in register 1,
        but carries no states in the fresh sibling variant — the element fails
        the states check while the resolution check stays clean."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        reg = conn.execute(
            "SELECT register_id FROM register_variant WHERE register_variant_id = 10"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, "
            "slug, name, panel_entity_key) VALUES (998, ?, 'sib2', 'Sib2', ?)",
            (reg, json.dumps(["kon"])),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_entity_key 'kon' has no variable_state rows" in f
            for f in result.failures
        ), result.failures


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


class TestConceptGroupChecks:
    """#303 `_check_concept_groups` — exercised against NON-EMPTY group tables
    so CI covers the invariants (the e2e fixture build derives zero groups:
    its synthetic corpus has no sibling edges / month families, and the
    curated TOML is nulled by `_no_repo_curation`). Each test corrupts one
    invariant on a hand-built slugged DB and asserts the gate bites."""

    @staticmethod
    def _grouped_db():
        from _slugged_db import add_variable, build_slugged_db

        conn = build_slugged_db(classification=None)  # scb/lisa (register 1)
        add_variable(conn, register_id=1, var_id=901, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=901, name="A", slug="varb")
        for a, b in (("vara", "varb"), ("varb", "vara")):
            conn.execute(
                "INSERT INTO variable_related_to (a_provider, a_register, "
                "a_variable, b_provider, b_register, b_variable, relation_kind, "
                "note) VALUES ('scb', 'lisa', ?, 'scb', 'lisa', ?, "
                "'same_definition_different_column', 'auto:triage')",
                (a, b),
            )
        conn.execute(
            "INSERT INTO concept_group (group_id, kind, register_id, group_key, "
            "label, source) VALUES (10, 'variable', 1, 'vara', 'A', 'edge')"
        )
        conn.executemany(
            "INSERT INTO concept_group_variable (variable_id, group_id) "
            "SELECT variable_id, 10 FROM variable WHERE slug = ?",
            [("vara",), ("varb",)],
        )
        return conn

    @staticmethod
    def _run(conn):
        from reg_meta_build.validate import ValidationResult, _check_concept_groups

        result = ValidationResult()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        _check_concept_groups(conn, result, tables, corpus=False)
        return result

    def test_passes_on_coherent_groups(self):
        result = self._run(self._grouped_db())
        assert result.passed, result.failures
        assert any("parity" in ln.text for ln in result.lines if ln.kind == "ok")

    def test_undersized_group_fails(self):
        conn = self._grouped_db()
        conn.execute(
            "DELETE FROM concept_group_variable WHERE variable_id IN "
            "(SELECT variable_id FROM variable WHERE slug = 'varb')"
        )
        result = self._run(conn)
        assert any("< 2 members" in f for f in result.failures)

    def test_cross_register_member_fails(self):
        conn = self._grouped_db()
        conn.execute("UPDATE concept_group SET register_id = 99 WHERE group_id = 10")
        result = self._run(conn)
        assert any("outside their group's register" in f for f in result.failures)

    def test_wrong_kind_wiring_fails(self):
        conn = self._grouped_db()
        conn.execute(
            "UPDATE concept_group SET kind = 'classification', register_id = NULL "
            "WHERE group_id = 10"
        )
        result = self._run(conn)
        assert any("wrong-kind group" in f for f in result.failures)

    def test_lost_edge_component_breaks_parity(self):
        conn = self._grouped_db()
        # Simulate the edge pass losing the component: drop the group while the
        # sibling edges remain.
        conn.execute("DELETE FROM concept_group_variable")
        conn.execute("DELETE FROM concept_group")
        result = self._run(conn)
        assert any("parity" in f or "components" in f for f in result.failures)


class TestTagChecks:
    """#311 `_check_tags` closure — exercised against NON-EMPTY tag tables (the
    e2e fixture build ships zero tags, so CI needs hand-built data). Each test
    corrupts one invariant with FKs OFF (so the corrupting DELETE can leave a
    dangling reference the check is meant to catch) and asserts the gate bites."""

    @staticmethod
    def _tagged_db():
        from _slugged_db import add_variable, build_slugged_db
        from reg_meta_build.tags import CuratedTag, TagMember, materialize_tags

        conn = build_slugged_db(classification=None)  # scb/lisa (register 1), `kon`
        add_variable(conn, register_id=1, var_id=90, name="Income", slug="dispink")
        materialize_tags(
            conn,
            (
                CuratedTag(
                    slug="income",
                    label="Income",
                    description=None,
                    members=(
                        TagMember("scb", "lisa", "dispink", 0, True, "primary"),
                        TagMember("scb", "lisa", None, 1, False, None),
                    ),
                ),
            ),
            providers=frozenset({"scb"}),
        )
        return conn

    @staticmethod
    def _run(conn):
        from reg_meta_build.validate import ValidationResult, _check_tags

        result = ValidationResult()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        _check_tags(conn, result, tables)
        return result

    def test_passes_on_coherent_tags(self):
        result = self._run(self._tagged_db())
        assert result.passed, result.failures

    def test_missing_tag_row_fails(self):
        conn = self._tagged_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DELETE FROM tag")
        result = self._run(conn)
        assert any("missing tag" in f for f in result.failures), result.failures

    def test_dangling_register_member_fails(self):
        conn = self._tagged_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        # Drop the register a register-grain member points at.
        conn.execute(
            "DELETE FROM register WHERE register_id IN "
            "(SELECT register_id FROM tag_member WHERE register_id IS NOT NULL)"
        )
        result = self._run(conn)
        assert any("dangling register_id" in f for f in result.failures), (
            result.failures
        )

    def test_dangling_variable_member_fails(self):
        conn = self._tagged_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        # Drop the variable a variable-grain member points at.
        conn.execute(
            "DELETE FROM variable WHERE variable_id IN "
            "(SELECT variable_id FROM tag_member WHERE variable_id IS NOT NULL)"
        )
        result = self._run(conn)
        assert any("dangling" in f and "variable_id" in f for f in result.failures), (
            result.failures
        )


def test_tags_section_present_in_report(fixture_db: Path):
    """#311: the `[tags]` section + its empty info line must appear in the full
    report so `_check_tags` can't be silently de-registered from
    `validate_built_db` (the fixture ships empty tags)."""
    report = validate_built_db(fixture_db).format_report()
    assert "[tags]" in report
    assert "0 tags / 0 tag members" in report


class TestVariableAliasWindowChecks:
    """#319 `_check_variable_alias_window` — exercised against NON-EMPTY window
    rows (the e2e fixture ships none). Each test corrupts one invariant with FKs
    OFF (so the corrupting DELETE can leave the dangling row the check catches)."""

    @staticmethod
    def _windowed_db():
        from _slugged_db import build_slugged_db

        conn = build_slugged_db(classification=None)  # scb/lisa, variable `kon`
        # kon has a variable_alias row under variant 10 (its delivery column). Seed
        # a window row reusing that exact (variable_id, variant, column).
        vid, rvid, col = conn.execute(
            "SELECT variable_id, register_variant_id, delivery_column_name "
            "FROM variable_alias LIMIT 1"
        ).fetchone()
        conn.execute(
            "INSERT INTO variable_alias_window (variable_id, register_variant_id, "
            "delivery_column_name, valid_from, valid_to) "
            "VALUES (?, ?, ?, '2018-01-01', '2018-01-31')",
            (vid, rvid, col),
        )
        return conn

    @staticmethod
    def _run(conn):
        from reg_meta_build.validate import (
            ValidationResult,
            _check_variable_alias_window,
        )

        result = ValidationResult()
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        _check_variable_alias_window(conn, result, tables, corpus=False)
        return result

    def test_passes_on_coherent_window(self):
        assert self._run(self._windowed_db()).passed

    def test_orphan_variable_id_fails(self):
        conn = self._windowed_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            "DELETE FROM variable WHERE variable_id IN "
            "(SELECT variable_id FROM variable_alias_window)"
        )
        result = self._run(conn)
        assert any("dangling variable/variant" in f for f in result.failures), (
            result.failures
        )

    def test_orphan_register_variant_id_fails(self):
        conn = self._windowed_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute(
            "DELETE FROM register_variant WHERE register_variant_id IN "
            "(SELECT register_variant_id FROM variable_alias_window)"
        )
        result = self._run(conn)
        assert any("dangling variable/variant" in f for f in result.failures), (
            result.failures
        )

    def test_window_column_not_in_alias_fails(self):
        conn = self._windowed_db()
        conn.execute("PRAGMA foreign_keys=OFF")
        # Make the window's column absent from variable_alias (rename the alias).
        conn.execute("UPDATE variable_alias SET delivery_column_name = 'OtherCol'")
        result = self._run(conn)
        assert any("missing from variable_alias" in f for f in result.failures), (
            result.failures
        )


def test_variable_alias_window_section_present_in_report(fixture_db: Path):
    """#319: the `[monthly-family windows]` section must appear so the check can't
    be silently de-registered (the fixture ships zero windows)."""
    report = validate_built_db(fixture_db).format_report()
    assert "[monthly-family windows]" in report
    assert "0 alias windows" in report
