"""Panel entity/time key references and the entity-key curation gate at the validate_built_db boundary."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path


class TestValidateModule:
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

    def test_panel_time_key_composite_resolves(self, fixture_db: Path, tmp_path: Path):
        """#567: a composite `panel_time_key` is a json-array string; it resolves
        element-wise like the composite entity key. Both `kon` and `testcol` exist
        under register 1, so the array fully resolves."""
        ok_db = tmp_path / "ok.db"
        ok_db.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(ok_db)
        conn.execute(
            "UPDATE register_variant SET panel_time_key = ? "
            "WHERE register_variant_id = 10",
            (json.dumps(["kon", "testcol"]),),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(ok_db)
        assert result.passed, result.failures

    def test_panel_time_key_composite_element_miss_fails(
        self, fixture_db: Path, tmp_path: Path
    ):
        """#567: a composite `panel_time_key` fails on ANY dangling element while
        leaving the resolving one alone (mirrors the composite entity-key check)."""
        broken = tmp_path / "broken.db"
        broken.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(broken)
        conn.execute(
            "UPDATE register_variant SET panel_time_key = ? "
            "WHERE register_variant_id = 10",
            (json.dumps(["kon", "ghost"]),),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(broken)
        assert not result.passed
        assert any(
            "panel_time_key 'ghost' resolves to no variable.slug" in f
            for f in result.failures
        ), result.failures
        assert not any(
            "panel_time_key 'kon' resolves to no" in f for f in result.failures
        ), result.failures

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

    @staticmethod
    def _db_with_kon_entity_key(fixture_db: Path, dst: Path) -> Path:
        """Copy the fixture DB and key variant 10's panel on `kon` (register 1's
        variable, source_id `1.44`). Returns the DB path."""
        dst.write_bytes(fixture_db.read_bytes())
        conn = sqlite3.connect(dst)
        conn.execute(
            "UPDATE register_variant SET panel_entity_key = 'kon' "
            "WHERE register_variant_id = 10"
        )
        conn.commit()
        conn.close()
        return dst

    @staticmethod
    def _slug_dir(tmp_path: Path, scb_body: str) -> Path:
        d = tmp_path / "egk_slugs"
        d.mkdir()
        (d / "scb.toml").write_text(scb_body, encoding="utf-8")
        return d

    def test_entity_key_gate_skipped_without_slug_dir(self, fixture_db: Path):
        """`slug_dir=None` (synthetic CI / `validate_built_db(corpus=False)`'s
        existing call sites) SKIPS the gate — even with an entity key present, no
        false failure, but the section + skip line still emit."""
        result = validate_built_db(fixture_db, slug_dir=None)
        assert result.passed, result.failures
        report = result.format_report()
        assert "[panel: entity-key variables are curated]" in report
        assert "entity-key curation gate skipped (no slug_dir)" in report

    def test_entity_key_gate_no_keys_passes(self, fixture_db: Path, tmp_path: Path):
        """A slug_dir is present but the fixture carries no entity key → the gate
        emits an [OK] (nothing to curate), not a skip."""
        slug_dir = self._slug_dir(tmp_path, "")
        result = validate_built_db(fixture_db, slug_dir=slug_dir)
        assert result.passed, result.failures
        assert "nothing to curate" in result.format_report()

    def test_entity_key_gate_fails_when_unpinned(
        self, fixture_db: Path, tmp_path: Path
    ):
        """An entity-key variable with NO curated `[variable]` pin FAILS the gate,
        with the source_id + a remediation pointing at `entity-key-pins`."""
        db = self._db_with_kon_entity_key(fixture_db, tmp_path / "unpinned.db")
        slug_dir = self._slug_dir(tmp_path, "")
        result = validate_built_db(db, slug_dir=slug_dir)
        assert not result.passed
        assert any(
            "source_id 1.44" in f and "no curated [variable] slug pin" in f
            for f in result.failures
        ), result.failures
        assert "reg-meta-build entity-key-pins" in result.format_report()

    def test_entity_key_gate_passes_when_pinned(self, fixture_db: Path, tmp_path: Path):
        """The same DB passes once the entity-key variable carries a curated pin
        binding its source_id (`1.44`) to its slug (`kon`)."""
        db = self._db_with_kon_entity_key(fixture_db, tmp_path / "pinned.db")
        slug_dir = self._slug_dir(tmp_path, '[variable."1.44"]\nslug = "kon"\n')
        result = validate_built_db(db, slug_dir=slug_dir)
        assert result.passed, result.failures
        assert "all 1 entity-key var(s) are curated" in result.format_report()
