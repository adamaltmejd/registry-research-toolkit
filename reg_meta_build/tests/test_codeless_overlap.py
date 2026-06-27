"""Coverage for the curated residual code-less ↔ code-bearing overlap mechanism
(#868): the `codeless_overlap.py` loader and the `db.py` materialization pass
`_resolve_curated_codeless_overlaps` + its mandatory-curation gate.

Two halves:
  - Loader validation — mirrors `TestLoadCodelivery` in `test_triage.py`: bad
    directive, missing/forbidden `extend`, blank key parts, duplicate key, malformed
    TOML → EXIT_CONFIG.
  - Materialization — in-memory DDL with FKs off (no provider/register parents
    needed) and states inserted directly, mirroring `TestDropFullcoverCodelessStates`
    in `test_triage.py`. Exercises cap-edge, cap-interior split, drop, extend, the
    unresolvable-extend failure, and the post-resolution mandatory-curation gate —
    both buckets: uncurated keys and a curated entry whose resolution leaves a
    residual overlap.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.codeless_overlap import load_codeless_overlap
from reg_meta_build.db import _resolve_curated_codeless_overlaps

if TYPE_CHECKING:
    from pathlib import Path


# ── loader validation ───────────────────────────────────────────────────────


class TestLoadCodelessOverlap:
    def test_parses_each_resolution(self, tmp_path: Path) -> None:
        toml = tmp_path / "codeless_overlap.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="naringsgren"\nvariable="sni"\n'
            'column="SNI"\nresolution="cap"\n\n'
            '[[resolve]]\nprovider="scb"\nregister="skollan"\nvariable="skollan"\n'
            'column="SkolLan"\nresolution="drop"\n\n'
            '[[resolve]]\nprovider="scb"\nregister="bef"\nvariable="fodelseland"\n'
            'column="FodelseLand"\nresolution="extend"\nextend="2010 coding"\n',
            encoding="utf-8",
        )
        cmap = load_codeless_overlap(toml)
        # Column folded to the rule-2 connectivity key (#196) — casing is cosmetic.
        # Key is provider-scoped: (provider, register, variable, folded-column).
        assert cmap[("scb", "naringsgren", "sni", "sni")] == ("cap", None)
        assert cmap[("scb", "skollan", "skollan", "skollan")] == ("drop", None)
        assert cmap[("scb", "bef", "fodelseland", "fodelseland")] == (
            "extend",
            "2010 coding",
        )

    def test_omitted_column_is_none_sentinel(self, tmp_path: Path) -> None:
        # A state with `delivery_column_name IS NULL` is curated by OMITTING the
        # `column` field → the key's column component is the `None` sentinel (NOT
        # `""`), so it matches the NULL row at materialization time.
        toml = tmp_path / "codeless_overlap.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="skyddad-natur"\n'
            'variable="naturtyp-skyddade-omraden-areal"\nresolution="cap"\n',
            encoding="utf-8",
        )
        cmap = load_codeless_overlap(toml)
        assert cmap[
            ("scb", "skyddad-natur", "naturtyp-skyddade-omraden-areal", None)
        ] == (
            "cap",
            None,
        )

    def test_missing_provider_rejected(self, tmp_path: Path) -> None:
        # `provider` is a required key part (the register slug is unique only per
        # provider) — a missing one is curation drift, not a silent default.
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nregister="r"\nvariable="v"\ncolumn="c"\nresolution="drop"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_invalid"
        assert "`provider`" in exc.value.message

    def test_empty_column_rejected(self, tmp_path: Path) -> None:
        # A present-but-empty `column = ""` is ambiguous (absent already means NULL)
        # → rejected, not folded to "".
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn=""\n'
            'resolution="drop"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_invalid"
        assert "`column`" in exc.value.message

    def test_unknown_resolution_rejected(self, tmp_path: Path) -> None:
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="c"\n'
            'resolution="trim"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_invalid"
        assert "unknown resolution" in exc.value.message

    def test_extend_missing_label_rejected(self, tmp_path: Path) -> None:
        # An `extend` entry with NO `extend` key at all stays an error (the typo
        # guard) — distinct from a present-but-empty `extend = ""`, which is valid.
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="c"\n'
            'resolution="extend"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "no `extend` key" in exc.value.message

    def test_extend_empty_label_loads(self, tmp_path: Path) -> None:
        # `extend = ""` is the empty-label target: the KEY is present (so the typo
        # guard passes) and the stored `extend_label` is `""` (NOT None — None means
        # "no extend", reserved for cap/drop). It names the unique empty/whitespace-
        # labelled coded vintage on the key (HDIA/ATCO shape).
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="par"\nvariable="typ-av-diagnos"\n'
            'column="HDIA"\nresolution="extend"\nextend=""\n',
            encoding="utf-8",
        )
        cmap = load_codeless_overlap(toml)
        assert cmap[("scb", "par", "typ-av-diagnos", "hdia")] == ("extend", "")

    def test_extend_empty_forbidden_on_cap(self, tmp_path: Path) -> None:
        # `extend = ""` on a `cap` entry is still a forbidden stray `extend` — the
        # present-empty key is detected by membership, not truthiness.
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="c"\n'
            'resolution="cap"\nextend=""\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "sets `extend`" in exc.value.message

    def test_extend_forbidden_on_cap(self, tmp_path: Path) -> None:
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="c"\n'
            'resolution="cap"\nextend="2010"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "sets `extend`" in exc.value.message

    def test_blank_key_part_rejected(self, tmp_path: Path) -> None:
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable=""\ncolumn="c"\n'
            'resolution="drop"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "`variable`" in exc.value.message

    def test_duplicate_key_rejected(self, tmp_path: Path) -> None:
        # Two entries that fold to the same (register, variable, column) key.
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="Col"\n'
            'resolution="drop"\n'
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="COL"\n'
            'resolution="cap"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "duplicate" in exc.value.message

    def test_unknown_top_level_key_rejected(self, tmp_path: Path) -> None:
        # A misspelled `[[resolves]]` must be a loud error, not a silent no-op that
        # disables ALL curation (shared scaffold guarantee).
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolves]]\nregister="r"\nvariable="v"\ncolumn="c"\nresolution="drop"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_invalid"

    def test_malformed_toml_is_config_error(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.toml"
        bad.write_text('[[resolve]]\nregister = = "r"\n', encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(bad)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_toml_unreadable"

    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        assert load_codeless_overlap(tmp_path / "nope.toml") == {}
        assert load_codeless_overlap(None) == {}


# ── materialization pass ─────────────────────────────────────────────────────


class TestResolveCuratedCodelessOverlaps:
    """`_resolve_curated_codeless_overlaps` (#868): cap / drop / extend the residual
    partial-coverage code-less states, with a mandatory-curation gate. In-memory DDL
    (FKs off), with the `register` + `variable` parents present so the slug JOIN in
    the pass resolves the curated `(register_slug, variable_slug, column)` key."""

    PROVIDER_SLUG = "p"
    REG_SLUG = "reg"
    VAR_SLUG = "var"
    COLUMN = "Col"

    @classmethod
    def _conn(cls) -> sqlite3.Connection:
        from reg_meta_build.db import DDL

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(DDL)  # FKs off → minimal parents
        # provider/register/variable parents carry the slugs the pass joins on; the
        # provider slug is part of the curated key (provider-scoped register slugs).
        conn.execute(
            "INSERT INTO provider (provider_id, slug, name) VALUES (1, ?, 'P')",
            (cls.PROVIDER_SLUG,),
        )
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'R', ?)",
            (cls.REG_SLUG,),
        )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (920, 1, '920', 'Var', ?)",
            (cls.VAR_SLUG,),
        )
        return conn

    @staticmethod
    def _state(
        conn: sqlite3.Connection,
        *,
        valid_from: str,
        valid_to: str,
        value_set_id: int | None,
        column: str | None = "Col",
        variant: int = 10,
        variable_id: int = 920,
        label: str = "",
    ) -> int:
        cur = conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, delivery_column_name, value_set_id, "
            "value_set_version_label) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (variable_id, variant, valid_from, valid_to, column, value_set_id, label),
        )
        assert cur.lastrowid is not None
        return cur.lastrowid

    @classmethod
    def _key(cls) -> tuple[str, str, str, str | None]:
        # (provider, register, variable, folded-column)
        return (cls.PROVIDER_SLUG, cls.REG_SLUG, cls.VAR_SLUG, "col")

    @staticmethod
    def _live(conn: sqlite3.Connection) -> set[int]:
        return {r[0] for r in conn.execute("SELECT state_id FROM variable_state")}

    @staticmethod
    def _row(conn: sqlite3.Connection, state_id: int) -> sqlite3.Row:
        return conn.execute(
            "SELECT valid_from, valid_to, value_set_id, value_set_version_label "
            "FROM variable_state WHERE state_id = ?",
            (state_id,),
        ).fetchone()

    def test_cap_edge_updates_single_residual(self) -> None:
        # Code-less [1997..2018] OVERLAPS coded [2015..2020] and extends BEFORE its
        # start → an edge residual: cap leaves ONE span [1997..2014] (the day before
        # the coded window) → update valid_from/valid_to in place.
        conn = self._conn()
        codeless = self._state(
            conn, valid_from="1997-01-01", valid_to="2018-12-31", value_set_id=None
        )
        coded = self._state(
            conn,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="v7",
        )
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("cap", None)})
        assert self._live(conn) == {codeless, coded}
        row = self._row(conn, codeless)
        # Trimmed to the day before the coded window's start (2014-12-31 here).
        assert (row["valid_from"], row["valid_to"]) == ("1997-01-01", "2014-12-31")
        assert row["value_set_id"] is None
        conn.close()

    def test_cap_interior_splits_into_twin(self) -> None:
        # Coded [2008..2010] strictly interior to code-less [2005..2015] leaves TWO
        # residual gap spans [2005..2007] + [2011..2015] → first stays on the
        # existing state_id, the second mints a code-less twin with a DISTINCT
        # valid_from (no idx_variable_state_unique collision). The code-less state
        # carries a NON-EMPTY label (the coalescer propagates the column label onto
        # code-less states); the twin must COPY it (still code-less via NULL value
        # set), not hard-code ''.
        conn = self._conn()
        cl_label = "Civilstånd i folkpensionshänseende"
        codeless = self._state(
            conn,
            valid_from="2005-01-01",
            valid_to="2015-12-31",
            value_set_id=None,
            label=cl_label,
        )
        coded = self._state(
            conn,
            valid_from="2008-01-01",
            valid_to="2010-12-31",
            value_set_id=7,
            label="v7",
        )
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("cap", None)})
        live = self._live(conn)
        # Original + coded + one minted twin = 3 rows.
        assert len(live) == 3
        assert codeless in live and coded in live
        twin_id = (live - {codeless, coded}).pop()
        first = self._row(conn, codeless)
        twin = self._row(conn, twin_id)
        spans = sorted(
            [
                (first["valid_from"], first["valid_to"]),
                (twin["valid_from"], twin["valid_to"]),
            ]
        )
        assert spans == [
            ("2005-01-01", "2007-12-31"),
            ("2011-01-01", "2015-12-31"),
        ]
        # Both stay code-less (NULL value set); both faithfully carry the ORIGINAL
        # code-less label (not a hard-coded ''). Uniqueness holds via distinct
        # valid_from — the INSERT would have raised IntegrityError otherwise.
        assert first["value_set_id"] is None and twin["value_set_id"] is None
        assert first["value_set_version_label"] == cl_label
        assert twin["value_set_version_label"] == cl_label
        conn.close()

    def test_cap_interior_twin_minted_in_low_band_despite_high_band_max(self) -> None:
        # Band-pinned twin minting (#868 follow-up): the original code-less state is
        # SCB-provider (LOW band, a small AUTOINCREMENT id < 2^62), but the table also
        # holds a high-band state (>= 2^62 — a graft / canonical-attach / SOS row), so
        # the GLOBAL MAX(state_id) is high-band. A naive global-MAX+1 mint would land
        # the SCB twin in the minted band and fail validate._check_minted_id_bands
        # ("SCB id overflows the minted band"). The twin must be minted in the SAME
        # band as its low-band original → state_id < 2^62.
        mint_bit = 1 << 62
        conn = self._conn()
        # Explicit-id inserts (not the AUTOINCREMENT `_state` helper): AUTOINCREMENT
        # tracks the table-wide max, so a high-band row would push every subsequent
        # auto id into the high band — defeating the "low-band original" setup. Pin
        # the SCB original to a small low-band id directly.
        codeless = 100
        conn.execute(
            "INSERT INTO variable_state (state_id, variable_id, register_variant_id, "
            "valid_from, valid_to, delivery_column_name, value_set_id, "
            "value_set_version_label) VALUES (?, 920, 10, '2005-01-01', '2015-12-31', "
            "?, NULL, 'lbl')",
            (codeless, self.COLUMN),
        )
        assert codeless < mint_bit  # the original is genuinely low-band
        coded = 101
        conn.execute(
            "INSERT INTO variable_state (state_id, variable_id, register_variant_id, "
            "valid_from, valid_to, delivery_column_name, value_set_id, "
            "value_set_version_label) VALUES (?, 920, 10, '2008-01-01', '2010-12-31', "
            "?, 7, 'v7')",
            (coded, self.COLUMN),
        )
        # A high-band decoy on a DIFFERENT variant so it is not itself a residual
        # overlap needing curation — it only inflates the global MAX(state_id) into
        # the minted band, the condition that broke the old global-MAX+1 mint.
        decoy = mint_bit + 5
        conn.execute(
            "INSERT INTO variable_state (state_id, variable_id, register_variant_id, "
            "valid_from, valid_to, delivery_column_name, value_set_id, "
            "value_set_version_label) VALUES (?, 920, 99, '2000-01-01', '2000-12-31', "
            "'Other', NULL, '')",
            (decoy,),
        )
        # Interior coded window splits the low-band code-less state into ≥2 residual
        # spans → one twin minted.
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("cap", None)})
        live = self._live(conn)
        # Original + coded + minted twin + the untouched high-band decoy = 4 rows.
        twin_ids = live - {codeless, coded, decoy}
        assert len(twin_ids) == 1
        twin_id = twin_ids.pop()
        # The twin lands in the LOW band (matching the SCB original), NOT at
        # global-MAX+1 (which would be decoy + 1, the minted band).
        assert twin_id < mint_bit
        assert twin_id != decoy + 1
        conn.close()

    def test_cap_interior_twin_collision_raises_actionable(self) -> None:
        # Contrived multi-state messy key (the cap analog of the `extend` collision
        # wrap): a code-less twin INSERT collides with a pre-existing code-less state
        # at the same (variable, variant, valid_from, label) → the raw
        # sqlite3.IntegrityError is re-raised as an ACTIONABLE
        # `codeless_overlap_cap_collision` (EXIT_CONFIG), symmetric with the extend
        # grow's wrap.
        #
        # Shape: code-less A [2005..2015] (label L) with coded interior [2008..2010]
        # caps into [2005..2007] (stays) + [2011..2015] (twin minted at
        # valid_from=2011-01-01, copying label L). A second code-less state B already
        # sits at valid_from=2011-01-01 with the SAME label L but does NOT overlap the
        # coded window ([2011..2011] starts after [2008..2010] ends) → B is not a
        # residual, never resolved, and survives to occupy the (valid_from, label)
        # tuple the twin INSERT targets → idx_variable_state_unique collision.
        conn = self._conn()
        label = "Civilstånd i folkpensionshänseende"
        self._state(
            conn,
            valid_from="2005-01-01",
            valid_to="2015-12-31",
            value_set_id=None,
            label=label,
        )
        self._state(
            conn,
            valid_from="2008-01-01",
            valid_to="2010-12-31",
            value_set_id=7,
            label="v7",
        )
        # B: pre-existing code-less state at the twin's target valid_from + label, NOT
        # overlapping the coded window → not a residual, left in place to collide.
        self._state(
            conn,
            valid_from="2011-01-01",
            valid_to="2011-12-31",
            value_set_id=None,
            label=label,
        )
        with pytest.raises(RegMetaError) as exc:
            _resolve_curated_codeless_overlaps(conn, {self._key(): ("cap", None)})
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_cap_collision"
        assert self.VAR_SLUG in exc.value.message
        conn.close()

    def test_drop_deletes_codeless(self) -> None:
        conn = self._conn()
        self._state(
            conn, valid_from="1997-01-01", valid_to="2018-12-31", value_set_id=None
        )
        coded = self._state(
            conn,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="v7",
        )
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("drop", None)})
        assert self._live(conn) == {coded}
        conn.close()

    def test_extend_absorbs_codeless_into_coded_window(self) -> None:
        # Code-less [1997..2014] overlaps coded [2010..2020] (label "2010 coding").
        # extend ABSORBS the code-less span into the coded vintage's window: the
        # coded state grows to cover the code-less span (valid_from = min, valid_to
        # = max) and the code-less state is DELETED.
        conn = self._conn()
        self._state(
            conn, valid_from="1997-01-01", valid_to="2014-12-31", value_set_id=None
        )
        coded = self._state(
            conn,
            valid_from="2010-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="2010 coding",
        )
        _resolve_curated_codeless_overlaps(
            conn, {self._key(): ("extend", "2010 coding")}
        )
        # The code-less state is gone; only the grown coded state remains.
        assert self._live(conn) == {coded}
        row = self._row(conn, coded)
        assert row["value_set_id"] == 7
        assert row["value_set_version_label"] == "2010 coding"
        # The coded window grew to cover the former code-less span: valid_from
        # lowered to the code-less start, valid_to is the later end.
        assert (row["valid_from"], row["valid_to"]) == ("1997-01-01", "2020-12-31")
        conn.close()

    def test_extend_absorbs_shared_valid_from_no_collision(self) -> None:
        # The ha0162 regression shape: the code-less state and the target coded
        # state SHARE a valid_from (both start 1989-01-01). The old re-point mechanic
        # would create a duplicate (valid_from, label) tuple and raise
        # `UNIQUE constraint failed`. Absorb instead grows the coded state to cover
        # the code-less span and deletes the code-less one — one coded state spanning
        # both windows, no IntegrityError, no duplicate.
        conn = self._conn()
        codeless = self._state(
            conn, valid_from="1989-01-01", valid_to="2010-12-31", value_set_id=None
        )
        coded = self._state(
            conn,
            valid_from="1989-01-01",
            valid_to="1995-12-31",
            value_set_id=7,
            label="Påverkar arbetsförmåga",
        )
        _resolve_curated_codeless_overlaps(
            conn, {self._key(): ("extend", "Påverkar arbetsförmåga")}
        )
        assert self._live(conn) == {coded}
        assert codeless not in self._live(conn)
        row = self._row(conn, coded)
        assert row["value_set_id"] == 7
        # valid_from stays 1989-01-01 (shared); valid_to grows to the later end.
        assert (row["valid_from"], row["valid_to"]) == ("1989-01-01", "2010-12-31")
        conn.close()

    def test_extend_codeless_label_equals_target_lower_from_no_collision(self) -> None:
        # The fasit/bcivpen regression shape: the code-less state carries a NON-EMPTY
        # value_set_version_label EQUAL to the target coded state's label (the
        # coalescer propagates the column label onto code-less states), and
        # codeless.valid_from < target.valid_from. Growing the target's valid_from
        # DOWN to the code-less start lands it on the (valid_from, label) tuple the
        # code-less state still occupies → idx_variable_state_unique collision unless
        # the code-less state is DELETED first. With the delete-before-grow reorder
        # the absorb resolves into one coded state [codeless_from..max], no
        # IntegrityError.
        conn = self._conn()
        label = "Civilstånd i folkpensionshänseende"
        codeless = self._state(
            conn,
            valid_from="2006-01-01",
            valid_to="2023-12-31",
            value_set_id=None,
            label=label,
        )
        coded = self._state(
            conn,
            valid_from="2015-01-01",
            valid_to="2015-12-31",
            value_set_id=6710,
            label=label,
        )
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("extend", label)})
        # No IntegrityError; the code-less state is gone, the coded state grew to
        # cover the whole former code-less span.
        assert self._live(conn) == {coded}
        assert codeless not in self._live(conn)
        row = self._row(conn, coded)
        assert row["value_set_id"] == 6710
        assert row["value_set_version_label"] == label
        assert (row["valid_from"], row["valid_to"]) == ("2006-01-01", "2023-12-31")
        conn.close()

    def test_extend_multi_window_grows_earliest(self) -> None:
        # One vintage (same label, same value_set_id) delivered in TWO overlapping
        # windows. extend grows the EARLIEST-valid_from overlapping one to absorb the
        # code-less span; the later window keeps its bounds (same value set, no
        # conflict). The code-less state is deleted.
        conn = self._conn()
        codeless = self._state(
            conn, valid_from="2000-01-01", valid_to="2018-12-31", value_set_id=None
        )
        early = self._state(
            conn,
            valid_from="2005-01-01",
            valid_to="2008-12-31",
            value_set_id=7,
            label="HDI",
        )
        late = self._state(
            conn,
            valid_from="2012-01-01",
            valid_to="2015-12-31",
            value_set_id=7,
            label="HDI",
        )
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("extend", "HDI")})
        assert self._live(conn) == {early, late}
        assert codeless not in self._live(conn)
        early_row = self._row(conn, early)
        late_row = self._row(conn, late)
        # Earliest window grew to cover the whole code-less span.
        assert (early_row["valid_from"], early_row["valid_to"]) == (
            "2000-01-01",
            "2018-12-31",
        )
        # Later window untouched.
        assert (late_row["valid_from"], late_row["valid_to"]) == (
            "2012-01-01",
            "2015-12-31",
        )
        conn.close()

    def test_extend_matches_label_modulo_surrounding_whitespace(self) -> None:
        # SCB labels carry surrounding whitespace ('Flag  '); the curated `extend`
        # value is stripped by the loader ('Flag'). The lookup must match modulo
        # surrounding whitespace, absorbing the code-less span into that coded state.
        conn = self._conn()
        self._state(
            conn, valid_from="1997-01-01", valid_to="2014-12-31", value_set_id=None
        )
        coded = self._state(
            conn,
            valid_from="2010-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="Flag  ",  # raw coded label with trailing whitespace
        )
        _resolve_curated_codeless_overlaps(
            conn,
            {self._key(): ("extend", "Flag")},  # stripped TOML value
        )
        # The code-less state was absorbed; the coded state grew to cover it.
        assert self._live(conn) == {coded}
        row = self._row(conn, coded)
        assert row["value_set_id"] == 7
        assert (row["valid_from"], row["valid_to"]) == ("1997-01-01", "2020-12-31")
        # A genuinely absent label still fails fast — whitespace tolerance does not
        # widen the match beyond surrounding whitespace.
        conn2 = self._conn()
        self._state(
            conn2, valid_from="1997-01-01", valid_to="2014-12-31", value_set_id=None
        )
        self._state(
            conn2,
            valid_from="2010-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="Flag  ",
        )
        with pytest.raises(RegMetaError) as exc:
            _resolve_curated_codeless_overlaps(
                conn2, {self._key(): ("extend", "Other")}
            )
        assert exc.value.code == "codeless_overlap_extend_unresolved"
        conn.close()
        conn2.close()

    def test_extend_unresolvable_label_fails(self) -> None:
        conn = self._conn()
        self._state(
            conn, valid_from="1997-01-01", valid_to="2014-12-31", value_set_id=None
        )
        self._state(
            conn,
            valid_from="2010-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="2010 coding",
        )
        with pytest.raises(RegMetaError) as exc:
            _resolve_curated_codeless_overlaps(
                conn, {self._key(): ("extend", "no such label")}
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_extend_unresolved"
        conn.close()

    def test_extend_empty_label_absorbs_single_empty_vintage(self) -> None:
        # HDIA/ATCO shape: a single coded vintage with an EMPTY value_set_version_label
        # (`''`) overlaps the code-less span. `extend = ""` (stored extend_label "")
        # keys to the stripped-empty `""` bucket with no special-casing and absorbs
        # the code-less span into that coded vintage's window; the code-less state is
        # deleted, the coded vintage grown.
        conn = self._conn()
        codeless = self._state(
            conn, valid_from="1997-01-01", valid_to="2014-12-31", value_set_id=None
        )
        coded = self._state(
            conn,
            valid_from="2010-01-01",
            valid_to="2020-12-31",
            value_set_id=13569,
            label="",  # empty label — the unnamed binary flag
        )
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("extend", "")})
        assert self._live(conn) == {coded}
        assert codeless not in self._live(conn)
        row = self._row(conn, coded)
        assert row["value_set_id"] == 13569
        assert (row["valid_from"], row["valid_to"]) == ("1997-01-01", "2020-12-31")
        conn.close()

    def test_extend_empty_label_whitespace_coded_label_absorbs(self) -> None:
        # The coded vintage's label is WHITESPACE-only ('  '); it strips to `""` and is
        # still reached by `extend = ""` (the loader stores `""`, the matcher keys on
        # the stripped label). Confirms the empty-label match is whitespace-tolerant,
        # not exact-empty-only.
        conn = self._conn()
        codeless = self._state(
            conn, valid_from="1997-01-01", valid_to="2014-12-31", value_set_id=None
        )
        coded = self._state(
            conn,
            valid_from="2010-01-01",
            valid_to="2020-12-31",
            value_set_id=13565,
            label="  ",  # whitespace-only → strips to ""
        )
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("extend", "")})
        assert self._live(conn) == {coded}
        assert codeless not in self._live(conn)
        conn.close()

    def test_extend_empty_label_ambiguous_distinct_value_sets_fails(self) -> None:
        # Two DISTINCT empty-label value sets overlap the code-less span. An empty
        # label is not a stable vintage identity, so `extend = ""` cannot pick one →
        # fail fast (EXIT_CONFIG), pointing the maintainer at `cap`. (Contrast the
        # single empty-label vintage above, which is unambiguous.)
        conn = self._conn()
        self._state(
            conn, valid_from="2000-01-01", valid_to="2018-12-31", value_set_id=None
        )
        # Two coded windows, SAME empty label, DIFFERENT value sets.
        self._state(
            conn,
            valid_from="2005-01-01",
            valid_to="2008-12-31",
            value_set_id=100,
            label="",
        )
        self._state(
            conn,
            valid_from="2012-01-01",
            valid_to="2015-12-31",
            value_set_id=200,
            label="",
        )
        with pytest.raises(RegMetaError) as exc:
            _resolve_curated_codeless_overlaps(conn, {self._key(): ("extend", "")})
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_extend_ambiguous_empty_label"
        assert self.VAR_SLUG in exc.value.message
        conn.close()

    def test_extend_empty_label_one_value_set_two_windows_absorbs(self) -> None:
        # A SINGLE empty-label value set delivered in TWO overlapping windows is NOT
        # ambiguous (one distinct value_set_id) — the earliest-valid_from window grows
        # to absorb the span, the later one keeps its bounds (multi-window deterministic
        # tie-break, unchanged for the empty-label case).
        conn = self._conn()
        codeless = self._state(
            conn, valid_from="2000-01-01", valid_to="2018-12-31", value_set_id=None
        )
        early = self._state(
            conn,
            valid_from="2005-01-01",
            valid_to="2008-12-31",
            value_set_id=13569,
            label="",
        )
        late = self._state(
            conn,
            valid_from="2012-01-01",
            valid_to="2015-12-31",
            value_set_id=13569,
            label="",
        )
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("extend", "")})
        assert self._live(conn) == {early, late}
        assert codeless not in self._live(conn)
        early_row = self._row(conn, early)
        assert (early_row["valid_from"], early_row["valid_to"]) == (
            "2000-01-01",
            "2018-12-31",
        )
        late_row = self._row(conn, late)
        assert (late_row["valid_from"], late_row["valid_to"]) == (
            "2012-01-01",
            "2015-12-31",
        )
        conn.close()

    def test_uncurated_residual_fails_build(self) -> None:
        # A residual overlap with NO curated entry → the mandatory-curation gate
        # fails the build, listing the uncurated key.
        conn = self._conn()
        self._state(
            conn, valid_from="1997-01-01", valid_to="2018-12-31", value_set_id=None
        )
        self._state(
            conn,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="v7",
        )
        with pytest.raises(RegMetaError) as exc:
            _resolve_curated_codeless_overlaps(conn, {})  # empty curation
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_uncurated_residual"
        assert self.VAR_SLUG in exc.value.message
        conn.close()

    def test_curated_extend_leaving_residual_fails_gate(self) -> None:
        # REGRESSION (the silent-pass bug): a CURATED `extend` entry is applied, but
        # its resolution LEAVES a residual code-less ↔ code-bearing overlap, which the
        # old pre-resolution `seen_keys` gate missed (the key HAD an entry, so it was
        # not in `seen_keys - curated`). The post-resolution re-query gate must catch
        # it and bucket it as the CURATED-but-unresolved case.
        #
        # Shape: a messy multi-state key. Coded "X" [2010..2012]; code-less A
        # [2008..2011] overlaps X (so A is resolved by the `extend "X"` entry — X
        # grows to cover A, A is deleted); code-less B [2005..2009] does NOT overlap
        # any CODED state initially (B ends 2009, before X's 2010 start), so the entry
        # never touches B. After `extend` grows X DOWN to [2008..2012], the grown X now
        # overlaps the untouched code-less B (2008 <= 2009) → a residual the single
        # `extend` entry left dangling.
        conn = self._conn()
        # A: code-less, overlaps coded X → absorbed by extend.
        self._state(
            conn, valid_from="2008-01-01", valid_to="2011-12-31", value_set_id=None
        )
        # B: code-less, no coded overlap initially → not in the per-state resolution.
        codeless_b = self._state(
            conn, valid_from="2005-01-01", valid_to="2009-12-31", value_set_id=None
        )
        coded_x = self._state(
            conn,
            valid_from="2010-01-01",
            valid_to="2012-12-31",
            value_set_id=7,
            label="X",
        )
        with pytest.raises(RegMetaError) as exc:
            _resolve_curated_codeless_overlaps(conn, {self._key(): ("extend", "X")})
        assert exc.value.exit_code == EXIT_CONFIG
        # Distinct code for the curated-but-unresolved bucket.
        assert exc.value.code == "codeless_overlap_unresolved_residual"
        # The message names the curated-but-unresolved cause, distinguishably.
        assert "residual overlap" in exc.value.message
        assert self.VAR_SLUG in exc.value.message
        # The grow happened (X absorbed A) before the gate fired; B still dangles —
        # the residual is real, not a pre-grow artifact.
        assert codeless_b in self._live(conn)
        x_row = self._row(conn, coded_x)
        assert (x_row["valid_from"], x_row["valid_to"]) == ("2008-01-01", "2012-12-31")
        conn.close()

    def test_fully_curated_set_passes(self) -> None:
        # Two residual keys, both curated → no gate failure.
        conn = self._conn()
        # Second variable so there are two distinct keys.
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (921, 1, '921', 'Var2', 'var2')"
        )
        self._state(
            conn, valid_from="1997-01-01", valid_to="2018-12-31", value_set_id=None
        )
        self._state(
            conn,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="v7",
        )
        self._state(
            conn,
            valid_from="1997-01-01",
            valid_to="2018-12-31",
            value_set_id=None,
            variable_id=921,
            column="Col2",
        )
        self._state(
            conn,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
            value_set_id=8,
            label="v8",
            variable_id=921,
            column="Col2",
        )
        _resolve_curated_codeless_overlaps(
            conn,
            {
                self._key(): ("drop", None),
                (self.PROVIDER_SLUG, self.REG_SLUG, "var2", "col2"): ("drop", None),
            },
        )
        # Both code-less states dropped; both coded survive.
        assert len(self._live(conn)) == 2
        conn.close()

    def test_null_column_residual_matched_by_omit_column_entry(self) -> None:
        # A residual code-less state whose `delivery_column_name IS NULL` (no
        # delivery alias) is curated by an entry that OMITS `column` → its key
        # column component is the `None` sentinel. The overlap is matched + capped,
        # and the mandatory-curation gate does NOT fire for it.
        conn = self._conn()
        codeless = self._state(
            conn,
            valid_from="1997-01-01",
            valid_to="2018-12-31",
            value_set_id=None,
            column=None,
        )
        coded = self._state(
            conn,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="v7",
            column=None,
        )
        # Key with the `None` column sentinel — the omit-`column` curated entry.
        null_key = (self.PROVIDER_SLUG, self.REG_SLUG, self.VAR_SLUG, None)
        _resolve_curated_codeless_overlaps(conn, {null_key: ("cap", None)})
        assert self._live(conn) == {codeless, coded}
        row = self._row(conn, codeless)
        assert (row["valid_from"], row["valid_to"]) == ("1997-01-01", "2014-12-31")
        assert row["value_set_id"] is None
        conn.close()

    def test_null_column_residual_uncurated_fails_gate(self) -> None:
        # The same NULL-column residual with NO curated entry (and a column-bearing
        # entry does NOT match it) → the mandatory-curation gate fails.
        conn = self._conn()
        self._state(
            conn,
            valid_from="1997-01-01",
            valid_to="2018-12-31",
            value_set_id=None,
            column=None,
        )
        self._state(
            conn,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="v7",
            column=None,
        )
        # A folded-column entry on the same (register, variable) must NOT resolve
        # the NULL-column residual.
        with pytest.raises(RegMetaError) as exc:
            _resolve_curated_codeless_overlaps(conn, {self._key(): ("cap", None)})
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_uncurated_residual"
        conn.close()

    def test_no_overlap_no_curation_is_noop(self) -> None:
        # A lone code-less state with no code-bearing overlap → no residual, no
        # curation needed, no gate failure.
        conn = self._conn()
        lone = self._state(
            conn, valid_from="2010-01-01", valid_to="2010-12-31", value_set_id=None
        )
        _resolve_curated_codeless_overlaps(conn, {})
        assert self._live(conn) == {lone}
        conn.close()

    def test_extend_two_codeless_states_grow_to_union(self) -> None:
        # Fix 1 (#878 review): the HDIA/ATCO shape — the SAME coded vintage absorbs
        # TWO code-less states on one key. Each grow must re-read the target's CURRENT
        # bounds so the window accumulates the UNION of both spans; a stale pre-loop
        # snapshot would recompute from the original window on the second absorb and
        # could shrink the first growth (silent, since both code-less rows are
        # deleted).
        #
        # Coded "X" [2005..2012]; code-less A [1997..2006] (overlaps X's start, extends
        # BEFORE it) and code-less B [2011..2018] (overlaps X's end, extends AFTER it)
        # BOTH overlap X and are extended into it. Each must accumulate onto the live
        # window: final X must span [1997..2018] — the union — not just one absorb.
        conn = self._conn()
        # A: overlaps X's start, extends BEFORE the coded window.
        self._state(
            conn, valid_from="1997-01-01", valid_to="2006-12-31", value_set_id=None
        )
        # B: overlaps X's end, extends AFTER it. Distinct valid_from from A.
        self._state(
            conn, valid_from="2011-01-01", valid_to="2018-12-31", value_set_id=None
        )
        coded = self._state(
            conn,
            valid_from="2005-01-01",
            valid_to="2012-12-31",
            value_set_id=7,
            label="X",
        )
        _resolve_curated_codeless_overlaps(conn, {self._key(): ("extend", "X")})
        # Both code-less states absorbed; only the grown coded state remains.
        assert self._live(conn) == {coded}
        row = self._row(conn, coded)
        # The window grew to the UNION of both code-less spans — neither growth lost.
        assert (row["valid_from"], row["valid_to"]) == ("1997-01-01", "2018-12-31")
        assert row["value_set_id"] == 7
        conn.close()

    def test_extend_swallowing_different_vintage_fails(self) -> None:
        # Fix 2 (#878 review): an `extend` whose grown span would CONTAIN a coded
        # window with a DIFFERENT value_set_id → fail fast (the grow would fabricate a
        # distinct-value_set coded↔coded overlap on one column, which only validate
        # catches — `--no-validate` would ship it).
        #
        # Code-less [1997..2014] overlaps target "X" [2010..2020] (vs 7); a DIFFERENT
        # vintage "Y" [2000..2005] (vs 9) sits inside the [1997..2020] grown span.
        conn = self._conn()
        self._state(
            conn, valid_from="1997-01-01", valid_to="2014-12-31", value_set_id=None
        )
        self._state(
            conn,
            valid_from="2010-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="X",
        )
        # Different-value_set coded window inside the grow span.
        self._state(
            conn,
            valid_from="2000-01-01",
            valid_to="2005-12-31",
            value_set_id=9,
            label="Y",
        )
        with pytest.raises(RegMetaError) as exc:
            _resolve_curated_codeless_overlaps(conn, {self._key(): ("extend", "X")})
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_extend_swallows_vintage"
        assert self.VAR_SLUG in exc.value.message
        conn.close()

    def test_provider_mismatch_treated_as_uncurated(self) -> None:
        # Fix 3 (#878 review): the curated key includes the PROVIDER. An entry under
        # the WRONG provider must NOT match the residual (so the mandatory-curation
        # gate still fires); the SAME entry under the right provider DOES match.
        conn = self._conn()
        self._state(
            conn, valid_from="1997-01-01", valid_to="2018-12-31", value_set_id=None
        )
        self._state(
            conn,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="v7",
        )
        # Same register/variable/column slug, but a DIFFERENT provider slug → the
        # entry does not cross providers, so the residual stays uncurated.
        wrong_provider_key = ("other", self.REG_SLUG, self.VAR_SLUG, "col")
        with pytest.raises(RegMetaError) as exc:
            _resolve_curated_codeless_overlaps(
                conn, {wrong_provider_key: ("drop", None)}
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_uncurated_residual"
        conn.close()

        # The SAME entry under the CORRECT provider resolves it (no gate failure).
        conn2 = self._conn()
        self._state(
            conn2, valid_from="1997-01-01", valid_to="2018-12-31", value_set_id=None
        )
        coded = self._state(
            conn2,
            valid_from="2015-01-01",
            valid_to="2020-12-31",
            value_set_id=7,
            label="v7",
        )
        _resolve_curated_codeless_overlaps(conn2, {self._key(): ("drop", None)})
        assert self._live(conn2) == {coded}
        conn2.close()
