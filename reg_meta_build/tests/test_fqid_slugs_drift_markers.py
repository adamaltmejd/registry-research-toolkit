"""populate_variable_slugs drift-stable slug basis, and the auto.toml derivation marker and name-fallback worklist."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _fqid_slug_support import PopulateVariableSlugsHelpers
from _slugged_db import (
    add_state,
    add_variable,
    build_slugged_db,
)
from reg_meta.errors import RegMetaError
from reg_meta.fqid import derive_variable_slug

from reg_meta_build.fqid_slugs import (
    AUTO_FILE_SUFFIX,
    FREEZE_STATE_FILE,
    load_provider_toml,
    load_slug_dir,
    populate_variable_slugs,
    precheck_slugs,
    read_auto_derivations,
    snapshot_payload,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


class TestPopulateVariableSlugs(PopulateVariableSlugsHelpers):
    """Stored `variable.slug` population — kolumnnamn-derived
    where register-unique, name-fallback otherwise, never-failing fallback
    chain, curated overrides, .auto.toml generation."""

    # --- #143: drift-stable slug basis (+ doable-now part of #141) --------

    @staticmethod
    def _add_drift_variable(
        conn: sqlite3.Connection,
        *,
        var_id: int,
        name: str,
        cols: list[str],
        register_id: int = 1,
    ) -> int:
        """Insert a variable + one variable_state era per column so its
        `delivery_column_name` drifts across editions. `cols` is earliest→latest
        (era N spans year 2000+N), so `cols[0]` is the earliest delivery column.
        Repeating a column (`["Syss", "Syss"]`) yields a constant, NON-drifting
        variable (COUNT(DISTINCT)=1)."""
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) VALUES (?, ?, ?)",
            (register_id, str(var_id), name),
        ).lastrowid
        assert vid is not None
        for i, col in enumerate(cols):
            yr = 2000 + i
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        return vid

    def _slug_of_vid(self, conn: sqlite3.Connection, vid: int) -> str | None:
        return conn.execute(
            "SELECT slug FROM variable WHERE variable_id = ?", (vid,)
        ).fetchone()[0]

    def test_drifting_column_slugs_from_name(self, tmp_path: Path) -> None:
        # #143: a 1:1 variable whose single delivery column drifts across
        # editions (SunInr→sun2000inr1→sun2020inr1) must NOT slug from its latest
        # column (`sun2020inr1` — misleading + version-coupled). The
        # register-unique NAME wins: a version-neutral `utbildningsinriktning`.
        conn = self._db(kol="Kon")  # var 44 (kon) stays an unrelated live var
        vid = self._add_drift_variable(
            conn,
            var_id=65,
            name="Utbildningsinriktning",
            cols=["SunInr", "sun2000inr1", "sun2020inr1"],
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._slug_of_vid(conn, vid) == "utbildningsinriktning"

    def test_drifting_name_collision_falls_back_to_earliest_column(
        self, tmp_path: Path
    ) -> None:
        # The watch-out: two drifting variables in one register sharing a name.
        # The name slug collides → each routes to its EARLIEST delivery column
        # (the stable, disambiguating basis), NOT an arbitrary `name`/`name-2`.
        conn = self._db(kol="Kon")
        a = self._add_drift_variable(
            conn, var_id=70, name="Inriktning", cols=["AlfaKod", "alfa_ny"]
        )
        b = self._add_drift_variable(
            conn, var_id=71, name="Inriktning", cols=["BetaKod", "beta_ny"]
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        sa, sb = self._slug_of_vid(conn, a), self._slug_of_vid(conn, b)
        assert sa == "alfakod"  # earliest column, not "inriktning"
        assert sb == "betakod"

    def test_split_siblings_drift_slug_from_earliest_column(
        self, tmp_path: Path
    ) -> None:
        # #141 (doable-now part): split siblings share one provider_key AND the
        # generic name, so a drifting sibling's name collides → it slugs from its
        # own EARLIEST column (#139's discriminator basis), staying rebuild-stable
        # and distinct per sibling. Frozen-build rename-immutability across a
        # rename of that earliest column is deferred post-slug-freeze.
        conn = self._db(kol="Kon")
        a = self._add_drift_variable(
            conn, var_id=99, name="Imputerat", cols=["BoareaImp", "boarea_imp"]
        )
        b = self._add_drift_variable(
            conn, var_id=99, name="Imputerat", cols=["BantalrumImp", "bantalrum_imp"]
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        sa, sb = self._slug_of_vid(conn, a), self._slug_of_vid(conn, b)
        assert sa == "boareaimp"
        assert sb == "bantalrumimp"

    def test_curated_split_pin_uses_current_same_date_discriminator(
        self, tmp_path: Path
    ) -> None:
        # Y-128/RPU 34373: claimed-year ordering puts the lowercase `astsni`
        # state after `Astsni2` at the same earliest date, so the split source
        # key is now `.astsni2`. Curate that exact key to retain the established
        # `astsni` slug while the 43145 `.astsnig` sibling remains distinct.
        conn = self._db(kol="Kon")
        add_variable(
            conn,
            register_id=1,
            var_id=17509,
            name="Arbetsställets näringsgren",
            slug="target-temp",
        )
        add_variable(
            conn,
            register_id=1,
            var_id=17509,
            name="Arbetsställets näringsgren",
            slug="sibling-temp",
        )
        target = conn.execute(
            "SELECT variable_id FROM variable WHERE slug = 'target-temp'"
        ).fetchone()[0]
        sibling = conn.execute(
            "SELECT variable_id FROM variable WHERE slug = 'sibling-temp'"
        ).fetchone()[0]
        add_state(
            conn,
            register_id=1,
            variable_slug="target-temp",
            register_variant_id=10,
            valid_from="2000-01-01",
            valid_to="2000-12-31",
            delivery_column_name="astsni",
            value_set_version_label="legacy",
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="target-temp",
            register_variant_id=10,
            valid_from="2000-01-01",
            valid_to="2000-12-31",
            delivery_column_name="Astsni2",
            value_set_version_label="current",
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sibling-temp",
            register_variant_id=10,
            valid_from="2000-01-01",
            valid_to="9999-12-31",
            delivery_column_name="astsnig",
        )
        conn.execute(
            "UPDATE variable SET slug = NULL WHERE variable_id IN (?, ?)",
            (target, sibling),
        )
        conn.commit()

        d = self._slug_dir(
            tmp_path,
            '[variable."1.17509.astsni2"]\nslug = "astsni"\n',
            scb_freeze="curating",
        )
        (d / f"scb{AUTO_FILE_SUFFIX}").write_text(
            '[variable."1.17509.astsnig"]\nslug = "astsnig"\n', encoding="utf-8"
        )
        counts = populate_variable_slugs(conn, d)

        assert self._slug_of_vid(conn, target) == "astsni"
        assert self._slug_of_vid(conn, sibling) == "astsnig"
        assert counts["curated"] == 1
        assert counts["auto_existing"] == 1

    def test_constant_column_is_not_drift(self, tmp_path: Path) -> None:
        # Regression: two eras carrying the SAME column is not drift
        # (COUNT(DISTINCT)=1) — it keeps the register-unique column slug.
        conn = self._db(kol="Kon")
        vid = self._add_drift_variable(
            conn, var_id=80, name="Sysselsattning", cols=["Syss", "Syss"]
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._slug_of_vid(conn, vid) == "syss"

    def test_case_diacritic_wobble_is_not_drift(self, tmp_path: Path) -> None:
        # #539: drift is counted in SLUG-space — COUNT(DISTINCT
        # variable_slug(delivery_column_name)) — so a column that only wobbles in
        # case/diacritics across editions (`Kön`→`Kon`, both fold to `kon`) is a
        # SINGLE distinct slug → NOT a drifter. It must take the non-drift,
        # latest-column basis (`kon`), NOT the name/earliest-column drift basis.
        #
        # This is a true OLD-vs-NEW regression lock: the NAME slug
        # (`Könstillhörighet`→`konstillhorighet`) is chosen to DIFFER from the
        # column slug (`kon`) precisely so the drift-name basis and the
        # latest-column basis produce DIFFERENT values. New slug-space count sees
        # 1 distinct slug → non-drift → latest-column → `kon` (asserted). Old
        # raw-string count saw 2 columns → falsely drift → name basis
        # (name_freq==1) → `konstillhorighet`, which would FAIL this assertion.
        # Isolate via the default var 44 carrying a non-`kon` column so `kon` is
        # register-unique for the wobble var and the assertion is exact (no `-2`).
        conn = self._db(kol="Alder", name="Ålder")
        vid = self._add_drift_variable(
            conn, var_id=65, name="Könstillhörighet", cols=["Kön", "Kon"]
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._slug_of_vid(conn, vid) == "kon"  # latest-column, not drift
        assert self._stored_slug(conn, 44) == "alder"  # isolation holds

    def test_mixed_wobble_and_rename_still_drifts(self, tmp_path: Path) -> None:
        # #539: a partial-wobble set with ≥2 distinct slugs still drifts. The
        # case/diacritic pair `PersonNr`/`personnr` collapses in slug-space, but
        # `PNR` does NOT — so the distinct slug set is exactly {`personnr`, `pnr`}
        # (size 2 > 1 → drifter). This test can't discriminate old-vs-new via the
        # final slug (it drifts to `personnummer` under both regimes), so it locks
        # the slug-space PREMISE directly: the assert below documents that the
        # count is 2, not 3 (the `PersonNr`/`personnr` collapse) and not 1.
        assert {derive_variable_slug(c) for c in ["PersonNr", "personnr", "PNR"]} == {
            "personnr",
            "pnr",
        }
        conn = self._db(kol="Alder", name="Ålder")
        vid = self._add_drift_variable(
            conn,
            var_id=66,
            name="Personnummer",
            cols=["PersonNr", "personnr", "PNR"],
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        # name basis (register-unique among drifters), not the latest column `pnr`.
        assert self._slug_of_vid(conn, vid) == "personnummer"

    def test_latest_sluggable_column_wins_over_nonsluggable_latest(
        self, tmp_path: Path
    ) -> None:
        # #547: the `kol` basis is the latest *sluggable* column, not the raw
        # latest one. A NON-drift variable (1 distinct slug in slug-space) whose
        # latest era carries a reserved token (`states` → NULL) but whose earlier
        # era carries a real column (`Yrke`) must slug from the earlier sluggable
        # column (`yrke`), NOT fall through to the name basis.
        #
        # True OLD-vs-NEW regression lock: the NAME (`Sysselsattning` →
        # `sysselsattning`) is chosen to DIFFER from the column slug (`yrke`) so
        # the two bases produce DIFFERENT values. New latest-sluggable basis sees
        # `Yrke` (skips `states`) → `yrke` (asserted). Old raw-latest basis saw
        # `states` → NULL → name basis (name_freq==1) → `sysselsattning`, which
        # would FAIL this assertion. `variable_slug("states")` is None, so
        # slug-space n_cols == 1 → non-drift either way (`yrke` is register-unique
        # since the default var 44 carries `Kon`).
        conn = self._db(kol="Kon")
        vid = self._add_drift_variable(
            conn, var_id=65, name="Sysselsattning", cols=["Yrke", "states"]
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._slug_of_vid(conn, vid) == "yrke"  # earlier sluggable, not name

    def test_all_nonsluggable_columns_fall_to_name_basis(self, tmp_path: Path) -> None:
        # #547: boundary the latest-*sluggable*-`kol` filter newly reaches. When
        # EVERY delivery column rejects to NULL (here both `states` and `variants`
        # are reserved-slot tokens → `variable_slug(...)` is NULL), the filtered
        # `kol` subquery (`AND variable_slug(vs.delivery_column_name) IS NOT NULL`)
        # returns no row → `kol IS NULL`, and the slug-space `n_cols`
        # (COUNT(DISTINCT variable_slug(...))) == 0 → non-drift. With no sluggable
        # column to anchor on, the variable correctly falls to the NAME basis
        # (Pass 3 `else` arm) → `sysselsattning`.
        #
        # NOTE: this is NOT an old-vs-new discriminating lock. Before the #547
        # filter, `kol` was the raw latest column (`variants`), and
        # derive_variable_slug("variants") is None → the column basis was None too
        # → name basis under both regimes. This is a forward-lock on the new
        # NULL-`kol`-subquery path, not a regression of prior behavior.
        conn = self._db(kol="Kon")  # default var 44 → `kon`, name-slug register-unique
        vid = self._add_drift_variable(
            conn, var_id=66, name="Sysselsattning", cols=["states", "variants"]
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._slug_of_vid(conn, vid) == "sysselsattning"  # name basis


class TestAutoDerivationMarker:
    """A4.4a: the `# source:` derivation marker written into `scb.auto.toml` and
    the name-fallback worklist surfaced via `precheck-slugs`. The marker is a
    TOML COMMENT (provenance), so it must be invisible to tomllib/SlugEntry/
    snapshot while remaining readable by `read_auto_derivations`."""

    @staticmethod
    def _slug_dir(tmp_path: Path, scb_body: str = "", *, scb_freeze: str = "") -> Path:
        d = tmp_path / "slugs"
        d.mkdir()
        (d / "scb.toml").write_text(scb_body, encoding="utf-8")
        # #470: default churning. A rebuild test that needs the prior auto.toml
        # markers carried forward pins the `scb` zone via `scb_freeze=`.
        if scb_freeze:
            (d / FREEZE_STATE_FILE).write_text(
                f'scb = "{scb_freeze}"\n', encoding="utf-8"
            )
        return d

    @staticmethod
    def _add_variable(
        conn: sqlite3.Connection,
        *,
        var_id: int,
        name: str,
        cols: list[str],
        register_id: int = 1,
    ) -> int:
        """Variable + one variable_state era per column. A single col → constant
        (no drift); repeated/distinct cols exercise the drift arm."""
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) VALUES (?, ?, ?)",
            (register_id, str(var_id), name),
        ).lastrowid
        assert vid is not None
        for i, col in enumerate(cols):
            yr = 2000 + i
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        return vid

    def _auto_path(self, d: Path) -> Path:
        return d / f"scb{AUTO_FILE_SUFFIX}"

    def test_marker_round_trips_invisibly_to_tomllib(self, tmp_path: Path) -> None:
        # The `# source:` comment must NOT become a parsed key — re-parsing the
        # written auto.toml yields the same slug values and only the `slug` field
        # (no `source`/derivation key leaks into SlugEntry / snapshot).
        conn = build_slugged_db(variable=("Kön", 44, 1001, "Kon"))
        conn.execute("UPDATE variable SET slug = NULL")
        conn.commit()
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        auto = self._auto_path(d)
        # The raw file carries the comment ...
        assert "# source:" in auto.read_text(encoding="utf-8")
        # ... but tomllib (via load_provider_toml) sees only the slug.
        entries = [e for e in load_provider_toml(auto) if e.kind == "variable"]
        assert len(entries) == 1
        e = entries[0]
        assert e.source_id == "1.44"
        assert e.slug == "kon"
        # No stray attribute carries the derivation — SlugEntry has a fixed
        # field set; the snapshot payload is unaffected. Pin scb after the build
        # so load_slug_dir loads the auto file (#775: churning auto.toml is
        # skipped); the direct load_provider_toml read above is state-agnostic.
        (d / FREEZE_STATE_FILE).write_text('scb = "curating"\n', encoding="utf-8")
        payload = snapshot_payload(load_slug_dir(d))
        assert payload["variable"] == {"scb/1.44": "kon"}

    def test_marker_class_per_derivation_arm(self, tmp_path: Path) -> None:
        # Each fallback arm stamps its own class. One register, distinct vars:
        #   - kolumnnamn-unique → `kolumnnamn`
        #   - name fallback (shared generic column) → `name-fallback`
        #   - drift (column changes across eras), unique name → `drift-name`
        #   - underivable kol+name (leading digit) → `v-provider-key`
        conn = build_slugged_db(variable=None)  # bare register/variant, no var
        self._add_variable(conn, var_id=10, name="Kön", cols=["Kon"])
        # 20 & 21 share generic OBS_VALUE → kol collides → both name-fall back.
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        self._add_variable(
            conn,
            var_id=30,
            name="Utbildningsinriktning",
            cols=["SunInr", "sun2020inr1"],
        )
        self._add_variable(conn, var_id=40, name="3D-område", cols=["3DOMR"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        deriv = read_auto_derivations(self._auto_path(d))
        assert deriv["1.10"] == "kolumnnamn"
        assert deriv["1.20"] == "name-fallback"
        assert deriv["1.21"] == "name-fallback"
        assert deriv["1.30"] == "drift-name"
        assert deriv["1.40"] == "v-provider-key"

    def test_disambiguator_marked_and_in_worklist(self, tmp_path: Path) -> None:
        # Two distinct vars whose NAMES fold to the same slug → the second is
        # `_uniquify`-suffixed. The marker carries `+disambiguated` and the row
        # lands in the worklist regardless of its base class.
        conn = build_slugged_db(variable=None)
        # Shared generic column forces the name fallback for both; identical
        # names collide so the second gets `-2`.
        self._add_variable(conn, var_id=50, name="Belopp", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=51, name="Belopp", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        deriv = read_auto_derivations(self._auto_path(d))
        # One is plain name-fallback, the other carries +disambiguated.
        kinds = {deriv["1.50"], deriv["1.51"]}
        assert kinds == {"name-fallback", "name-fallback+disambiguated"}
        result = precheck_slugs(conn, d)
        worklist = {(sid, kind) for _p, sid, _s, kind in result.name_fallback_variables}
        assert ("1.50", "name-fallback") in worklist
        assert ("1.51", "name-fallback+disambiguated") in worklist

    def test_worklist_excludes_column_derived(self, tmp_path: Path) -> None:
        # The worklist is the curation BACKLOG: name / `-N` / `v<key>` only.
        # kolumnnamn, fold, and drift-earliest-column (column-derived) bases are
        # EXCLUDED — they have a stable, canonical basis already.
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=10, name="Kön", cols=["Kon"])  # kolumnnamn
        # Two drifting vars sharing a name → each routes to earliest column
        # (drift-earliest-column), which must NOT be in the worklist.
        self._add_variable(
            conn, var_id=70, name="Inriktning", cols=["AlfaKod", "alfa_ny"]
        )
        self._add_variable(
            conn, var_id=71, name="Inriktning", cols=["BetaKod", "beta_ny"]
        )
        # A name-fallback var that IS in the worklist (control).
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)
        by_sid = {sid: kind for _p, sid, _s, kind in result.name_fallback_variables}
        assert "1.10" not in by_sid  # kolumnnamn
        assert "1.70" not in by_sid  # drift-earliest-column
        assert "1.71" not in by_sid  # drift-earliest-column
        assert by_sid.get("1.20") == "name-fallback"
        assert by_sid.get("1.21") == "name-fallback"

    def test_worklist_carries_slug_and_provider(self, tmp_path: Path) -> None:
        # Each worklist row is (provider, source_id, slug, derivation); the slug
        # is read from the auto file (no DB join needed).
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)
        rows = {
            sid: (prov, slug, kind)
            for prov, sid, slug, kind in result.name_fallback_variables
        }
        assert rows["1.20"] == ("scb", "inkomst", "name-fallback")
        assert rows["1.21"] == ("scb", "utgift", "name-fallback")

    def test_worklist_is_advisory_only(self, tmp_path: Path) -> None:
        # A populated worklist must NOT flip `ok` or the precheck exit. Curate the
        # register/variant/classification slugs so the gating checks are all
        # clean, then confirm `ok` is True DESPITE a non-empty name-fallback
        # worklist (variables auto-slug, so they never feed the missing/stale
        # gates anyway).
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(
            tmp_path,
            '[register."1"]\nslug = "lisa"\n'
            '[register_variant."1.10"]\nslug = "individer-15plus"\n',
        )
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)
        assert result.name_fallback_variables  # worklist non-empty
        assert result.ok  # ... yet the gating checks are all clean

    def test_read_auto_derivations_tolerates_missing_and_unmarked(
        self, tmp_path: Path
    ) -> None:
        # Robustness: a missing file → {}; an entry without a `# source:` comment
        # (legacy pre-A4.4a row) → simply absent (never crashes).
        d = self._slug_dir(tmp_path)
        auto = self._auto_path(d)
        assert read_auto_derivations(auto) == {}  # no file yet
        auto.write_text(
            '[variable."1.44"]\nslug = "kon"\n\n'
            '[variable."1.55"]\nslug = " inkomst "  # source: name-fallback\n',
            encoding="utf-8",
        )
        deriv = read_auto_derivations(auto)
        assert "1.44" not in deriv  # unmarked legacy row
        assert deriv["1.55"] == "name-fallback"

    def test_worklist_tolerates_nonnumeric_provider_key(self, tmp_path: Path) -> None:
        # `variable.provider_key` is TEXT — a SOS key is a merged variable name,
        # not a numeric var_id, so the auto.toml key is `1.BefolkningPerKommun`.
        # The validating loader rejects that grammar, so the worklist must read
        # the auto file RAW (like `_drifting_variables` tolerates the TEXT key)
        # — otherwise this otherwise-non-fatal precheck would crash.
        conn = build_slugged_db(variable=None)
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, 'BefolkningPerKommun', 'Befolkning')"
        ).lastrowid
        assert vid is not None
        for yr, col in (("2000", "BefKom"), ("2010", "BefKommun")):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, ?, ?, 'int', ?)",
                (vid, f"{yr}-01-01", f"{yr}-12-31", col),
            )
        conn.commit()
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        result = precheck_slugs(conn, d)  # must not raise on the non-numeric key
        hit = [
            r for r in result.name_fallback_variables if r[1] == "1.BefolkningPerKommun"
        ]
        assert len(hit) == 1
        # drift + name-unique-among-drifters → drift-name (a worklist class).
        assert hit[0] == ("scb", "1.BefolkningPerKommun", "befolkning", "drift-name")

    def test_existing_markers_carried_forward_on_rebuild(self, tmp_path: Path) -> None:
        # An incremental rebuild (prior auto.toml + a NEW variable → auto_dirty)
        # must PRESERVE the pre-existing rows' `# source:` markers, not strip them
        # — else the worklist shrinks over time. populate_variable_slugs seeds
        # auto_derivation from the prior file before Pass 3 re-derives only the new.
        # This carry-forward is the pinned-auto behavior — `curating` (#470); a
        # churning zone re-derives all three vars from scratch each build.
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        # Generate build (churning) writes the baseline .auto.toml; pinning requires
        # that committed file (#471), so flip to curating only AFTER it exists.
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        first = read_auto_derivations(self._auto_path(d))
        assert first["1.20"] == "name-fallback"
        assert first["1.21"] == "name-fallback"
        (d / FREEZE_STATE_FILE).write_text('scb = "curating"\n', encoding="utf-8")
        # Add a NEW variable and rebuild — the full file is rewritten. (30 is now
        # the only pending var, so its OBS_VALUE column is unique among pending →
        # it takes the kolumnnamn slug; the class differs from 20/21, which is
        # exactly why a re-derivation can't reconstruct their original markers.)
        self._add_variable(conn, var_id=30, name="Sparande", cols=["OBS_VALUE"])
        populate_variable_slugs(conn, d)
        second = read_auto_derivations(self._auto_path(d))
        assert second["1.30"] == "kolumnnamn"  # the new row is freshly classed ...
        assert second["1.20"] == "name-fallback"  # ... and the prior markers
        assert second["1.21"] == "name-fallback"  # survived the rewrite.

    def test_read_auto_derivations_tolerates_undecodable_file(
        self, tmp_path: Path
    ) -> None:
        # "Tolerant by design" (advisory): invalid UTF-8 must yield {}, not raise.
        d = self._slug_dir(tmp_path)
        auto = self._auto_path(d)
        auto.write_bytes(
            b'[variable."1.44"]\nslug = "\xff\xfe"  # source: name-fallback\n'
        )
        assert read_auto_derivations(auto) == {}

    def test_advisory_worklist_survives_malformed_auto_toml(
        self, tmp_path: Path
    ) -> None:
        # A malformed `<provider>.auto.toml` is precheck's job to report via
        # parse_errors; the advisory worklist must NOT turn it into a crash —
        # it skips the unparseable provider.
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        # Unterminated string → tomllib raises → _parse_toml → RegMetaError.
        self._auto_path(d).write_text(
            '[variable."1.20"]\nslug = "inkomst\n', encoding="utf-8"
        )
        result = precheck_slugs(conn, d)  # must not raise
        assert all(r[0] != "scb" for r in result.name_fallback_variables)

    def test_worklist_excludes_curated_override(self, tmp_path: Path) -> None:
        # A variable a curator FIXED via a [variable] override in <provider>.toml
        # is no longer backlog, even though its frozen auto entry + marker linger
        # in the auto file across the rebuild. The lingering auto entry across the
        # rebuild is the pinned-auto behavior — `curating` (#470); churning would
        # re-derive 1.21 (now the only pending var) to a non-fallback slug.
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        # Generate build (churning) writes the baseline .auto.toml; pinning requires
        # that committed file (#471), so flip to curating only AFTER it exists.
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        (d / FREEZE_STATE_FILE).write_text('scb = "curating"\n', encoding="utf-8")
        before = {r[1] for r in precheck_slugs(conn, d).name_fallback_variables}
        assert {"1.20", "1.21"} <= before
        # Curate 1.20 — its auto entry/marker persist, but it drops from the list.
        (d / "scb.toml").write_text(
            '[variable."1.20"]\nslug = "hushalls-inkomst"\n', encoding="utf-8"
        )
        populate_variable_slugs(conn, d)
        after = {r[1] for r in precheck_slugs(conn, d).name_fallback_variables}
        assert "1.20" not in after  # curated → excluded from the backlog
        assert "1.21" in after  # still backlog

    def test_advisory_worklist_survives_nontable_variable(self, tmp_path: Path) -> None:
        # A syntactically-valid auto.toml whose `variable` is a non-table value
        # must not crash the worklist (precheck reports it via parse_errors).
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        self._auto_path(d).write_text('variable = "bad"\n', encoding="utf-8")
        result = precheck_slugs(conn, d)  # must not raise on the odd shape
        assert all(r[0] != "scb" for r in result.name_fallback_variables)

    def test_worklist_keeps_metadata_only_override(self, tmp_path: Path) -> None:
        # A [variable] entry WITHOUT a string slug leaves the auto slug unchanged,
        # so it stays backlog regardless of other metadata: a `replaced_by`
        # within-file rename pointer and `deprecated` (still slugged so old
        # references resolve) both keep their variable in the list. Only a string
        # `slug` override drops it (Codex: don't treat every [variable] row as a
        # slug fix).
        conn = build_slugged_db(variable=None)
        self._add_variable(conn, var_id=20, name="Inkomst", cols=["OBS_VALUE"])
        self._add_variable(conn, var_id=21, name="Utgift", cols=["OBS_VALUE"])
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        (d / "scb.toml").write_text(
            '[variable."1.20"]\nreplaced_by = "1.21"\n'
            '[variable."1.21"]\ndeprecated = true\n',
            encoding="utf-8",
        )
        populate_variable_slugs(conn, d)
        worklist = {r[1] for r in precheck_slugs(conn, d).name_fallback_variables}
        assert "1.20" in worklist  # replaced_by only → slug unfixed → stays
        assert "1.21" in worklist  # deprecated-only → slug still ships → stays

    def test_dotted_provider_key_fails_fast(self, tmp_path: Path) -> None:
        # A provider_key containing '.' would mis-parse the variable source-ID as
        # a split-sibling 3-part key (silent slug mis-attribution). Fail fast at
        # source-ID construction instead. (Inert for SCB — its keys are ints — but
        # a non-SCB provider_key is an arbitrary name, so guard it. A4.4b review.)
        conn = build_slugged_db(variable=None)
        vid = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (1, 'FOO.BAR', 'Foo')"
        ).lastrowid
        assert vid is not None
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (?, 10, '2000-01-01', '2000-12-31', 'int', 'FooBar')",
            (vid,),
        )
        conn.commit()
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, self._slug_dir(tmp_path))
        assert "contains '.'" in exc.value.message
