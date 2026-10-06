"""Tests for structural and corpus validation at the catalog write boundary."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path


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

    # ── code-less ↔ code-bearing overlap backstop (epic #858) ─────────────────

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

    # ── entity-key curation gate (#546) ───────────────────────────────────────

    # ── flavored (steward-scoped) gate (#559) ─────────────────────────────────


class TestConceptGroupChecks:
    """#303 `_check_concept_groups` — exercised against NON-EMPTY group tables
    so CI covers the invariants (the shared synthetic catalog has no groups).
    Each test corrupts one
    invariant on a hand-built slugged DB and asserts the gate bites."""

    @staticmethod
    def _grouped_db():
        # The edge fold reads the in-build sibling pairs (`edge_siblings`), never a
        # shipped table, so the check recomputes nothing from sibling rows — a
        # hand-built `edge` group row is the whole fixture (no companion edges).
        from _slugged_db import add_variable, build_slugged_db

        conn = build_slugged_db(classification=None)  # scb/lisa (register 1)
        add_variable(conn, register_id=1, var_id=901, name="A", slug="vara")
        add_variable(conn, register_id=1, var_id=901, name="A", slug="varb")
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

    # #819 multi-axis facet invariants. `_grouped_db` seeds an EDGE group (group 10,
    # zero axes) whose members carry no facets — the valid edge shape. Each test
    # corrupts the new tables one way and asserts the gate bites.

    def test_duplicate_whole_variable_member_rejected_by_index(self):
        # The COALESCE unique index closes the NULL-distinctness footgun: a second
        # whole-variable (NULL delivery_column) member of `vara` in group 10 — which
        # a bare composite UNIQUE would silently admit — is rejected at insert time.
        conn = self._grouped_db()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO concept_group_variable (group_id, variable_id, "
                "delivery_column_name) SELECT 10, variable_id, NULL FROM variable "
                "WHERE slug = 'vara'"
            )


def test_tags_section_present_in_report(fixture_db: Path):
    """#311: the `[tags]` section + its empty info line must appear in the full
    report so `_check_tags` can't be silently de-registered from
    `validate_built_db` (the fixture ships empty tags)."""
    report = validate_built_db(fixture_db).format_report()
    assert "[tags]" in report
    assert "0 tags / 0 tag members" in report


def test_variable_alias_window_section_present_in_report(fixture_db: Path):
    """#319/#945: the `[alias windows]` section must appear so the check can't
    be silently de-registered."""
    report = validate_built_db(fixture_db).format_report()
    assert "[alias windows]" in report
    assert "every window has valid_from <= valid_to" in report
