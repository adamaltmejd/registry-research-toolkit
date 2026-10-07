"""precheck_slugs findings and the slug snapshot (write, read, diff, frozen-zone blocking)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import pytest
from _fqid_slug_support import write_text_file as _write
from _slugged_db import (
    build_slugged_db,
)
from reg_meta.errors import RegMetaError

from reg_meta_build.fqid_slugs import (
    SlugEntry,
    diff_snapshot,
    populate_variable_slugs,
    precheck_slugs,
    read_snapshot,
    snapshot_payload,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestPrecheckSlugs:
    def test_drifting_variables_advisory(self, tmp_path: Path):
        # #143: precheck lists drifting-column variables (advisory — it must
        # NOT affect `ok`). The drift var is reported with its stored slug + the
        # distinct columns in edition order; a constant-column var is omitted.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        conn = build_slugged_db()  # var 44, constant column "Kon" → not a drifter
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '65', 'Utbildningsinriktning')"
        ).lastrowid
        for yr, col in (
            ("2000", "SunInr"),
            ("2016", "sun2000inr1"),
            ("2020", "sun2020inr1"),
        ):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)
        # provider_key stays raw TEXT (never int-coerced — a non-numeric SOS key
        # must not crash this advisory); for SCB it's the numeric var_id as text.
        by_pk = {row[2]: row for row in result.drifting_variables}
        assert "65" in by_pk
        assert "44" not in by_pk  # constant column is not a drifter
        prov, reg_id, provider_key, slug, name, cols = by_pk["65"]
        assert (prov, reg_id, provider_key) == ("scb", 1, "65")
        assert slug == "utbildningsinriktning"
        assert name == "Utbildningsinriktning"
        assert cols == ("SunInr", "sun2000inr1", "sun2020inr1")

    def test_wobble_variable_absent_from_drifting_advisory(self, tmp_path: Path):
        # #539: the advisory measures drift in the SAME slug-space the basis
        # selection uses (both call `_register_slug_fn` →
        # COUNT(DISTINCT variable_slug(...))). A pure case/diacritic wobble var
        # (`Kön`→`Kon`, one distinct slug) must be ABSENT from the advisory, while
        # a genuine drifter is still present with its RAW column variants. This
        # locks the lockstep contract: if the advisory dropped the slug-fn
        # registration it would count raw columns and wrongly flag the wobble var.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        conn = build_slugged_db()  # var 44, constant column "Kon" → not a drifter
        wobble = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '90', 'Kommun')"
        ).lastrowid
        for yr, col in (("2000", "Kön"), ("2016", "Kon")):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (wobble, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        genuine = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, '91', 'Utbildningsinriktning')"
        ).lastrowid
        for yr, col in (
            ("2000", "SunInr"),
            ("2016", "sun2000inr1"),
            ("2020", "sun2020inr1"),
        ):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (genuine, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)
        by_pk = {row[2]: row for row in result.drifting_variables}
        assert "90" not in by_pk  # slug-space wobble: one distinct slug → not drift
        assert "91" in by_pk  # genuine rename still flagged
        # The genuine drifter still lists its RAW column variants (slug-space gates,
        # but the listing shows the actual SCB columns for the curator).
        assert by_pk["91"][5] == ("SunInr", "sun2000inr1", "sun2020inr1")

    def test_drifting_advisory_tolerates_nonnumeric_provider_key(self, tmp_path: Path):
        # `variable.provider_key` is TEXT — SOS ships a merged variable name, not
        # a numeric var_id. The advisory must report it raw, not `int()` it (that
        # would crash the otherwise non-fatal precheck on a non-SCB provider).
        d = tmp_path / "slugs"
        d.mkdir()
        _write(d / "scb.toml", "")
        conn = build_slugged_db()
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, 'BefolkningPerKommun', 'Befolkning')"
        ).lastrowid
        for yr, col in (("2000", "BefKom"), ("2010", "BefKommun")):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)  # must not raise on the non-numeric key
        hit = [r for r in result.drifting_variables if r[2] == "BefolkningPerKommun"]
        assert len(hit) == 1
        assert hit[0][3] == "befolkning"  # name basis (cols collide-free, drift)

    def test_stale_register_id_reported(self, tmp_path: Path):
        # TOML entry for register 999 has no live row; `populate_slugs` would
        # raise `slug_unknown_source_id` at build time. Precheck surfaces it
        # earlier so maintainers don't ship a TOML that breaks the next build.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n[register."999"]\nslug = "ghost"\n',
        )
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        result = precheck_slugs(conn, d)
        assert not result.ok
        assert ("scb", "999") in result.stale_registers
        assert ("scb", "1") not in result.stale_registers

    def test_stale_variant_id_reported(self, tmp_path: Path):
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer"\n'
            '[register_variant."1.999"]\nslug = "ghost"\n',
        )
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        result = precheck_slugs(conn, d)
        assert ("scb", "1.999") in result.stale_variants
        assert ("scb", "1.10") not in result.stale_variants

    def test_deprecated_entries_excluded_from_stale(self, tmp_path: Path):
        # Deprecated rows are allowed to outlive their DB row — that's the
        # whole point of `deprecated=true`. Stale-check must not flag them.
        d = tmp_path / "slugs"
        d.mkdir()
        _write(
            d / "scb.toml",
            '[register."1"]\nslug = "lisa"\n'
            '[register."999"]\nslug = "old-lisa"\ndeprecated = true\n',
        )
        conn = build_slugged_db()
        conn.execute("UPDATE register SET slug = NULL")
        conn.execute("UPDATE register_variant SET slug = NULL")
        result = precheck_slugs(conn, d)
        assert ("scb", "999") not in result.stale_registers


class TestSnapshot:
    def test_snapshot_missing_file_returns_empty(self, tmp_path: Path):
        loaded = read_snapshot(tmp_path / "nope.json")
        assert loaded == {
            "register": {},
            "register_variant": {},
            "variable": {},
        }

    def test_snapshot_payload_skips_slugless_entries(self):
        entries = [
            SlugEntry(
                kind="variable",
                source_id="34.99",
                slug=None,
                provider="scb",
                deprecated=True,
            ),
            SlugEntry(
                kind="variable",
                source_id="34.4",
                slug="kon",
                provider="scb",
            ),
        ]
        payload = snapshot_payload(entries)
        assert payload["variable"] == {"scb/34.4": "kon"}


class TestSnapshotCorrupt:
    """A snapshot file that fails to parse raises `slug_snapshot_unreadable`
    so the maintainer can't silently progress against garbage."""

    def test_corrupt_json_raises(self, tmp_path: Path):
        path = tmp_path / "snap.json"
        path.write_text("{not json", encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            read_snapshot(path)
        assert exc.value.code == "slug_snapshot_unreadable"


class TestDiffSnapshotFrozenZones:
    """`diff_snapshot`'s `blocked` list scopes rename/removal refusal per zone."""

    _PREV: ClassVar[dict[str, dict[str, str]]] = {
        "register": {"scb/1": "lisa", "sos/9": "deaths"},
        "register_variant": {},
        "variable": {},
    }

    def test_only_frozen_zone_violations_block(self) -> None:
        cur = {
            "register": {"scb/1": "lisa-renamed"},  # scb rename + sos/9 removed
            "register_variant": {},
            "variable": {},
        }
        # Freeze only `scb`; the sos removal is reported but not blocked.
        diff = diff_snapshot(self._PREV, cur, frozen_zones=frozenset({"scb"}))
        assert diff["blocked"] == ["register/scb/1: 'lisa' -> 'lisa-renamed'"]
        assert "register/sos/9 (was 'deaths')" in diff["removed"]
