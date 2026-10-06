"""populate_variable_slugs: column-derived and name-fallback variable slugs, curated overrides, era handling, auto.toml pins, and the frozen-zone new-variable gate."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _fqid_slug_support import PopulateVariableSlugsHelpers
from _slugged_db import (
    add_state,
    add_variable,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError

from reg_meta_build.fqid_slugs import (
    AUTO_FILE_SUFFIX,
    FREEZE_STATE_FILE,
    diff_snapshot,
    load_provider_toml,
    load_slug_dir,
    populate_variable_slugs,
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

    @staticmethod
    def _add_variable(
        conn: sqlite3.Connection,
        *,
        var_id: int,
        name: str,
        kol: str,
        eras: tuple[tuple[str, str], ...] = (("2018-01-01", "9999-12-31"),),
    ):
        """Variable + one `kol` state per era. The default era matches the fixture
        variable's, so two variables sharing a column overlap unless a test says
        otherwise."""
        add_variable(conn, register_id=1, var_id=var_id, name=name)
        for valid_from, valid_to in eras:
            add_state(
                conn,
                register_id=1,
                var_id=var_id,
                register_variant_id=10,
                valid_from=valid_from,
                valid_to=valid_to,
                delivery_column_name=kol,
            )
        conn.commit()

    def test_auto_derives_and_persists(self, tmp_path: Path) -> None:
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        counts = populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "kon"
        assert counts["auto_new"] == 1
        # .auto.toml written, keyed register.var.
        auto = d / f"scb{AUTO_FILE_SUFFIX}"
        assert auto.is_file()
        entries = load_provider_toml(auto)
        assert any(
            e.kind == "variable" and e.source_id == "1.44" and e.slug == "kon"
            for e in entries
        )

    def test_curated_override_wins(self, tmp_path: Path) -> None:
        conn = self._db(kol="Kon")
        d = self._slug_dir(
            tmp_path,
            '[register."1"]\nslug = "lisa"\n[variable."1.44"]\nslug = "kon-curated"\n',
        )
        counts = populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "kon-curated"
        assert counts["curated"] == 1
        assert counts["auto_new"] == 0

    def test_deprecated_metadata_persists_without_slug_override(
        self, tmp_path: Path
    ) -> None:
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path, '[variable."1.44"]\ndeprecated = true\n')
        populate_variable_slugs(conn, d)
        row = conn.execute(
            "SELECT slug, deprecated FROM variable WHERE provider_key = '44'"
        ).fetchone()
        assert row["slug"] == "kon"
        assert row["deprecated"] == 1

    def test_stale_curated_override_rejected(self, tmp_path: Path) -> None:
        # A non-deprecated [variable] override for a (register, var) with no live
        # variable is a typo — fail rather than silently auto-slug the variable.
        conn = self._db(kol="Kon")  # only live variable is 1.44
        d = self._slug_dir(tmp_path, '[variable."1.999"]\nslug = "ghost"\n')
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_variable_override_stale"
        assert "1.999" in exc.value.message

    def test_deprecated_curated_override_may_be_stale(self, tmp_path: Path) -> None:
        # A deprecated override may outlive its (retired) variable — no error.
        conn = self._db(kol="Kon")
        d = self._slug_dir(
            tmp_path, '[variable."1.999"]\nslug = "ghost"\ndeprecated = true\n'
        )
        populate_variable_slugs(conn, d)  # must not raise
        assert self._stored_slug(conn, 44) == "kon"

    def test_deprecated_curated_slug_reserved(self, tmp_path: Path) -> None:
        # A deprecated curated [variable] slug (retired variable kept in
        # the snapshot) is reserved — a new live variable can't auto-derive it
        # and recreate the published FQID.
        conn = self._db(kol="Kon")  # live var 44 → would derive "kon"
        d = self._slug_dir(
            tmp_path, '[variable."1.999"]\nslug = "kon"\ndeprecated = true\n'
        )
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "kon-2"  # not the retired "kon"

    def test_override_for_unbuilt_provider_is_skipped(self, tmp_path: Path) -> None:
        # #563: a curated [variable] override for a provider NOT built this run (a
        # --providers-restricted build: a thin provider's entity-key pin) has no live
        # variable to match. It must be SKIPPED, not flagged stale — that provider was
        # never built. The typo guard still fails for a BUILT provider
        # (test_stale_curated_override_rejected).
        conn = self._db(kol="Kon")  # only scb (register 1) is live
        d = self._slug_dir(tmp_path)
        (d / "fk.toml").write_text(
            '[variable."99.personnummer"]\nslug = "personnummer"\n', encoding="utf-8"
        )
        populate_variable_slugs(conn, d)  # must NOT raise on the un-built fk override
        assert self._stored_slug(conn, 44) == "kon"  # scb still slugs normally

    def test_override_for_unknown_provider_still_fails(self, tmp_path: Path) -> None:
        # #563 (Codex): a curated [variable] override under an UNKNOWN provider stem
        # (a misfiled TOML — `fkk.toml` typo for `fk.toml`) is a real typo and must
        # still raise, unlike a known-but-not-built provider (which is skipped).
        conn = self._db(kol="Kon")  # seeds all 8 providers; only scb is live
        d = self._slug_dir(tmp_path)
        (d / "fkk.toml").write_text(  # `fkk` is not a seeded provider
            '[variable."99.personnummer"]\nslug = "personnummer"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_variable_override_stale"
        assert "fkk" in exc.value.message

    def test_curated_override_reusing_auto_slug_rejected(self, tmp_path: Path) -> None:
        # A curated override must not reuse a slug frozen for a DIFFERENT source
        # in auto.toml — it would duplicate a published FQID (or hit UNIQUE).
        # The auto.toml is only read back (pinned) under curating/frozen (#470).
        conn = self._db(kol="Kon")  # live var 44
        d = self._slug_dir(
            tmp_path, '[variable."1.44"]\nslug = "ghost"\n', scb_freeze="curating"
        )
        # "ghost" is already frozen for a different (pruned) source 1.999.
        (d / f"scb{AUTO_FILE_SUFFIX}").write_text(
            '[variable."1.999"]\nslug = "ghost"\n', encoding="utf-8"
        )
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_variable_override_conflict"

    def test_existing_auto_not_recomputed_on_rename(self, tmp_path: Path) -> None:
        # The pinned-auto behavior is curating/frozen (#470): a churning zone
        # would re-derive `konkod` on the rebuild instead.
        # Generate build (churning): auto-derives `kon` from `Kon`, persists the
        # .auto.toml. Pinning REQUIRES that committed file (#471), so the maintainer
        # commits it (`git add -f`) and only THEN flips the zone to curating.
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        (d / FREEZE_STATE_FILE).write_text('scb = "curating"\n', encoding="utf-8")
        # SCB renames the delivery column; rebuild reads the existing
        # .auto.toml and keeps the original slug (immutability).
        conn2 = self._db(kol="Konkod")  # would derive `konkod`
        counts = populate_variable_slugs(conn2, d)
        assert self._stored_slug(conn2, 44) == "kon"
        assert counts["auto_existing"] == 1
        assert counts["auto_new"] == 0

    def test_retired_auto_slug_not_reused(self, tmp_path: Path) -> None:
        # Immutability: a frozen auto slug whose variable was pruned from
        # the delivery stays reserved — a live/new variable can't be assigned it,
        # which would duplicate the slug in the rewritten auto.toml. The auto.toml
        # is only pinned (read back) under curating/frozen (#470).
        conn = self._db(kol="Kon")  # live var 44, kolumnnamn → "kon"
        d = self._slug_dir(tmp_path, scb_freeze="curating")
        # Retired var 1.999 (absent from the DB) froze "kon" in auto.toml.
        (d / f"scb{AUTO_FILE_SUFFIX}").write_text(
            '[variable."1.999"]\nslug = "kon"\n', encoding="utf-8"
        )
        populate_variable_slugs(conn, d)
        # var 44 wanted "kon" but it's frozen to the retired entry → uniquified.
        assert self._stored_slug(conn, 44) == "kon-2"
        # The rewritten auto.toml keeps the retired entry and adds no duplicate.
        slugs = [
            e.slug
            for e in load_provider_toml(d / f"scb{AUTO_FILE_SUFFIX}")
            if e.kind == "variable"
        ]
        assert sorted(slugs) == ["kon", "kon-2"]

    def test_collision_falls_back_to_name(self, tmp_path: Path) -> None:
        # Two distinct variables under one register whose kolumnnamn fold to the
        # SAME slug ('kon') no longer fail the build — both fall back to their
        # (distinct) names, yielding distinct register-unique slugs.
        conn = self._db(kol="Kon", name="Kön")  # var 44
        self._add_variable(conn, var_id=88, name="Civilstånd", kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        s44, s88 = self._stored_slug(conn, 44), self._stored_slug(conn, 88)
        assert s44 and s88 and s44 != s88
        assert s88 == "civilstand"  # name-derived, since 'kon' collided

    def test_unique_kolumnnamn_keeps_short_slug(self, tmp_path: Path) -> None:
        # When the kolumnnamn slug is register-unique it wins over the (longer)
        # name even if the name differs — keeps the short common-case leaf.
        conn = self._db(kol="Sysselsattning", name="Sysselsättningsstatus i november")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "sysselsattning"

    def test_disjoint_eras_share_the_column_slug(self, tmp_path: Path) -> None:
        # LISA's `ForvErs`: var 31395 delivers it 1990–2021, var 47670 2022–2023
        # (SCB re-minted the definition). Two eras of ONE column are not a naming
        # conflict, so the column arm still applies — the earlier era takes the
        # bare `forvers`, the later one carries its start year. The register-unique
        # `ForvErsNetto` keeps its own short slug.
        conn = self._db(kol="ForvErsNetto", name="Förvärvsinkomst netto")
        self._add_variable(
            conn,
            var_id=31395,
            name="Förvärvsinkomst",
            kol="ForvErs",
            # Two states that overlap EACH OTHER (a multi-vintage era): only
            # cross-variable windows are compared, and the earliest start wins.
            eras=(("1990-01-01", "2005-12-31"), ("2000-01-01", "2021-12-31")),
        )
        self._add_variable(
            conn,
            var_id=47670,
            name="Förvärvsinkomst",  # identical name: only the era arm can split these
            kol="ForvErs",
            eras=(("2022-01-01", "2023-12-31"),),
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 31395) == "forvers"
        assert self._stored_slug(conn, 47670) == "forvers-2022"
        assert self._stored_slug(conn, 44) == "forversnetto"
        # Both are kolumnnamn-class: not name-fallback, not `+disambiguated`.
        deriv = read_auto_derivations(d / f"scb{AUTO_FILE_SUFFIX}")
        assert deriv["1.31395"] == "kolumnnamn"
        assert deriv["1.47670"] == "kolumnnamn"

    def test_overlapping_eras_still_fall_back_to_name(self, tmp_path: Path) -> None:
        # Same column delivered at the SAME time by two variables is a genuine
        # conflict — unchanged behavior, both fall to the name arm.
        conn = self._db(kol="ForvErsNetto", name="Förvärvsinkomst netto")
        self._add_variable(
            conn,
            var_id=31395,
            name="Förvärvsinkomst",
            kol="ForvErs",
            eras=(("1990-01-01", "2021-12-31"),),
        )
        self._add_variable(
            conn,
            var_id=47670,
            name="Förvärvsinkomst brutto",
            kol="ForvErs",
            eras=(("2010-01-01", "2023-12-31"),),  # overlaps 2010–2021
        )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 31395) == "forvarvsinkomst"
        assert self._stored_slug(conn, 47670) == "forvarvsinkomst-brutto"
        deriv = read_auto_derivations(d / f"scb{AUTO_FILE_SUFFIX}")
        assert deriv["1.31395"] == "name-fallback"
        assert deriv["1.47670"] == "name-fallback"

    def test_era_chain_ordered_by_earliest_start(self, tmp_path: Path) -> None:
        # Three eras of one column, inserted OUT of chronological order: the
        # bare slug follows the earliest `valid_from`, not the insertion order.
        conn = self._db(kol="ForvErsNetto", name="Förvärvsinkomst netto")
        for var_id, era in (
            (3, ("2018-01-01", "9999-12-31")),
            (1, ("1990-01-01", "2003-12-31")),
            (2, ("2004-01-01", "2017-12-31")),
        ):
            self._add_variable(
                conn, var_id=var_id, name="Förvärvsinkomst", kol="ForvErs", eras=(era,)
            )
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 1) == "forvers"
        assert self._stored_slug(conn, 2) == "forvers-2004"
        assert self._stored_slug(conn, 3) == "forvers-2018"

    def test_multiple_eras_single_slug(self, tmp_path: Path) -> None:
        # variable.slug is register-scoped (one row per variable), so multiple
        # variable_state eras for one variable can't multiply or fork the slug.
        conn = self._db(kol="Kon")
        vid = conn.execute(
            "SELECT variable_id FROM variable WHERE provider_key = '44'"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO variable_state "
            "(variable_id, register_variant_id, valid_from, valid_to, "
            "data_type, delivery_column_name) "
            "VALUES (?, 10, '2019-01-01', '9999-12-31', 'int', 'Kon')",
            (vid,),
        )
        conn.commit()
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        slugs = conn.execute(
            "SELECT slug FROM variable WHERE provider_key = '44'"
        ).fetchall()
        assert [r[0] for r in slugs] == ["kon"]

    def test_underivable_kol_falls_back_to_name(self, tmp_path: Path) -> None:
        # A delivery column that folds to empty ('...') no longer fails — the
        # slug derives from the variable name instead.
        conn = self._db(kol="...", name="Kön")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "kon"

    def test_ultimate_fallback_when_name_underivable(self, tmp_path: Path) -> None:
        # Neither kolumnnamn nor name yields a slug (both lead with a digit) →
        # last-resort v<provider_key>.
        conn = self._db(kol="3DOMR", name="3D-område")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        assert self._stored_slug(conn, 44) == "v44"

    def test_long_name_fallback_is_length_capped(self, tmp_path: Path) -> None:
        # A generic kolumnnamn forces the name fallback; a very long name is
        # truncated on a hyphen boundary to a readable leaf.
        long_name = (
            "Utgifter för egen FoU efter finansieringskälla EU ramprogram forskning"
        )
        conn = self._db(kol="OBS_VALUE", name=long_name)
        # Second variable sharing OBS_VALUE so the kolumnnamn slug collides.
        self._add_variable(conn, var_id=88, name="Annat värde", kol="OBS_VALUE")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        slug = self._stored_slug(conn, 44)
        assert slug is not None and len(slug) <= 60
        assert slug.startswith("utgifter-for-egen-fou")

    def test_auto_toml_parses_as_provider_scb(self, tmp_path: Path) -> None:
        # The generated scb.auto.toml must load via load_slug_dir as provider
        # `scb` (the `.auto` suffix must not break provider-slug grammar).
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        # load_slug_dir skips a churning zone's auto.toml (#775); pin scb AFTER
        # the build (not via scb_freeze=, which would trip the #471
        # slug_freeze_auto_missing guard with no committed auto file yet) so the
        # generated auto slugs flow through the pinned-load path.
        (d / FREEZE_STATE_FILE).write_text('scb = "curating"\n', encoding="utf-8")
        entries = load_slug_dir(d)
        var_entries = [
            e for e in entries if e.kind == "variable" and e.provider == "scb"
        ]
        assert any(e.source_id == "1.44" and e.slug == "kon" for e in var_entries)

    def test_auto_slugs_flow_into_snapshot(self, tmp_path: Path) -> None:
        # A PINNED provider's auto-derived variable slugs (in its committed
        # .auto.toml) must land in the snapshot payload's "variable" kind so the
        # grow-only guard covers them. (A churning zone's auto.toml is skipped by
        # load_slug_dir — #775 — so pin scb after the build.)
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)
        (d / FREEZE_STATE_FILE).write_text('scb = "curating"\n', encoding="utf-8")
        payload = snapshot_payload(load_slug_dir(d))
        assert payload["variable"].get("scb/1.44") == "kon"
        # A rename of the auto slug is flagged by diff_snapshot.
        previous = {k: dict(v) for k, v in payload.items()}
        previous["variable"]["scb/1.44"] = "kon-old"
        diff = diff_snapshot(previous, payload)
        assert any("1.44" in r for r in diff["renamed"])

    # --- #786: frozen-provider new-variable fragile-basis gate ------------

    def _frozen_with_new_var(
        self, tmp_path: Path, *, new_name: str, new_kol: str
    ) -> tuple[sqlite3.Connection, Path]:
        # Mirror test_existing_auto_not_recomputed_on_rename's setup: build once
        # churning to generate (and commit) the auto.toml, then add a NEW variable
        # and flip the zone to frozen. The existing var 44 (kol "Kon" → "kon") is
        # pinned from the committed auto.toml; only the new variable is first-sight.
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)  # churning: writes scb.auto.toml
        self._add_variable(conn, var_id=88, name=new_name, kol=new_kol)
        (d / FREEZE_STATE_FILE).write_text('scb = "frozen"\n', encoding="utf-8")
        return conn, d

    def test_frozen_new_fallback_basis_rejected(self, tmp_path: Path) -> None:
        # A NEW variable on a FROZEN provider whose slug derives from a fragile
        # (name-fallback / last-resort) basis would lock that artifact in as an
        # immutable slug — the gate fails the build so a curator pins it instead.
        # kol "3DOMR" and name "3D-område" both lead with a digit ⇒ neither
        # slugifies ⇒ last-resort v<key> arm (_DERIVATION_FALLBACK).
        conn, d = self._frozen_with_new_var(
            tmp_path, new_name="3D-område", new_kol="3DOMR"
        )
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_freeze_new_fallback"
        assert exc.value.exit_code == EXIT_CONFIG
        assert "1.88" in exc.value.message

    def test_frozen_new_clean_kolumnnamn_accepted(self, tmp_path: Path) -> None:
        # A NEW variable on a frozen provider with a clean, register-unique
        # kolumnnamn derives _DERIVATION_KOLUMNNAMN (not fragile) ⇒ no gate.
        conn, d = self._frozen_with_new_var(
            tmp_path, new_name="Civilstånd", new_kol="Civilstand"
        )
        populate_variable_slugs(conn, d)  # must not raise
        assert self._stored_slug(conn, 88) == "civilstand"

    def test_new_fallback_basis_allowed_when_curating(self, tmp_path: Path) -> None:
        # The SAME fragile new-variable scenario as the FAIL case is dormant on a
        # non-frozen provider: curating still derives a fresh slug for the new
        # variable, and the gate is gated strictly on state == "frozen".
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)  # churning: writes scb.auto.toml
        self._add_variable(conn, var_id=88, name="3D-område", kol="3DOMR")
        (d / FREEZE_STATE_FILE).write_text('scb = "curating"\n', encoding="utf-8")
        populate_variable_slugs(conn, d)  # must not raise
        assert self._stored_slug(conn, 88) == "v88"

    def test_frozen_new_fallback_cleared_by_pin(self, tmp_path: Path) -> None:
        # A curated [variable] pin for the offending new variable resolves it as
        # `curated` (the deliberate review) ⇒ the fragile-basis gate doesn't fire.
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)  # churning: writes scb.auto.toml
        self._add_variable(conn, var_id=88, name="3D-område", kol="3DOMR")
        (d / FREEZE_STATE_FILE).write_text('scb = "frozen"\n', encoding="utf-8")
        # Pin the new variable — the curated override wins over the fragile arm.
        (d / "scb.toml").write_text(
            '[variable."1.88"]\nslug = "tredimensionellt-omrade"\n', encoding="utf-8"
        )
        counts = populate_variable_slugs(conn, d)  # must not raise
        assert self._stored_slug(conn, 88) == "tredimensionellt-omrade"
        assert counts["curated"] == 1

    def test_frozen_new_disambiguated_basis_rejected(self, tmp_path: Path) -> None:
        # The OTHER way the gate fires (vs test_frozen_new_fallback_basis_rejected's
        # pure last-resort arm): a NEW frozen-provider variable whose base is an
        # otherwise CLEAN kolumnnamn but COLLIDES with an already-pinned slug, so
        # `_uniquify` appends `-N` and the kind carries `+disambiguated`. The gate's
        # `_is_name_fallback_derivation` is true via its `endswith("+disambiguated")`
        # branch, not its name-fallback-class branch. The new var (kol "Kon") shares
        # the existing pinned var 44's clean kolumnnamn → derives "kon", which is
        # reserved → the kolumnnamn arm is collision-gated, so the name basis wins
        # and gets `kon-2` tagged `name-fallback+disambiguated` (the `-N` suffix is
        # what trips the gate, regardless of base class).
        conn, d = self._frozen_with_new_var(tmp_path, new_name="Kon", new_kol="Kon")
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_freeze_new_fallback"
        assert exc.value.exit_code == EXIT_CONFIG
        assert "1.88" in exc.value.message
        # The trigger is specifically the disambiguator suffix, not a name/last-resort
        # base class — assert the offending slug/kind surfaced in the message.
        assert "kon-2" in exc.value.message
        assert "+disambiguated" in exc.value.message

    def test_frozen_new_fallback_aggregates_offenders(self, tmp_path: Path) -> None:
        # Two NEW fragile-basis variables on the SAME frozen provider raise ONCE,
        # listing both (mirrors the stale-override aggregation) so a curator pins
        # them in a single pass. Both kol+name lead with a digit ⇒ neither
        # slugifies ⇒ the `v<key>` last-resort arm for each.
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)  # churning: writes scb.auto.toml
        self._add_variable(conn, var_id=88, name="3D-område", kol="3DOMR")
        self._add_variable(conn, var_id=99, name="4K-vy", kol="4KSKM")
        (d / FREEZE_STATE_FILE).write_text('scb = "frozen"\n', encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_freeze_new_fallback"
        assert exc.value.exit_code == EXIT_CONFIG
        # One raise, both offenders, count of 2.
        assert exc.value.message.startswith("2 NEW")
        assert "1.88" in exc.value.message
        assert "1.99" in exc.value.message

    def test_frozen_new_fallback_not_persisted_refires_on_rerun(
        self, tmp_path: Path
    ) -> None:
        # Codex P2: a frozen+fragile offender must NOT be written into the
        # committed `<provider>.auto.toml` before the post-loop raise. If it were,
        # a bare rerun (no curated pin added) would read it back as `auto_existing`
        # in Pass 1, making the variable no longer first-sight ⇒ the gate would
        # never re-fire and the fragile slug would silently ship — defeating the
        # gate via a simple rerun. Prove the real property two ways: (1) the
        # offending source_id is absent from the on-disk auto.toml, and (2) a
        # SECOND populate on the same conn/slug_dir STILL raises.
        conn, d = self._frozen_with_new_var(
            tmp_path, new_name="3D-område", new_kol="3DOMR"
        )
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_freeze_new_fallback"

        # (1) The offender was not persisted; the existing pinned slug still is.
        # (Frozen first-sight is decided by auto.toml membership, NOT the DB
        # `variable.slug` — Pass 1 keys off `auto.get(source_id)` — so no NULL
        # reset is needed for the read-back path to be exercised.)
        var_slugs = {
            e.source_id: e.slug
            for e in load_provider_toml(d / f"scb{AUTO_FILE_SUFFIX}")
            if e.kind == "variable"
        }
        assert "1.88" not in var_slugs
        assert var_slugs.get("1.44") == "kon"

        # (2) Rerun re-derives the still-first-sight variable and re-fires.
        with pytest.raises(RegMetaError) as exc2:
            populate_variable_slugs(conn, d)
        assert exc2.value.code == "slug_freeze_new_fallback"
        assert "1.88" in exc2.value.message

    def test_frozen_rejected_offender_does_not_overreport_clean_sibling(
        self, tmp_path: Path
    ) -> None:
        # Codex P2: a REJECTED frozen offender must not reserve its slug — a
        # rejected slug is never persisted, so it must not influence any sibling's
        # derivation. Two NEW first-sight variables on the same frozen register
        # whose bases COLLIDE on "vinst":
        #   A (var 88): kol "3DOMR" (no slugify) → name-fallback "vinst" (fragile
        #     ⇒ a real offender).
        #   B (var 99): clean kolumnnamn "Vinst" → "vinst" — register-unique among
        #     pending, so the non-fragile kolumnnamn arm wins.
        # A is processed first (lower variable_id). If A reserved its slug before
        # the `continue` (the pre-fix bug), "vinst" would be in `used`, the clean
        # kolumnnamn arm would fail for B, and B would fall to the kolumnnamn-
        # residual arm (also fragile) ⇒ B would be falsely flagged. B's name
        # "4K-vy" doesn't name-slug, so the kolumnnamn arm is the ONLY thing
        # keeping B clean — proving the over-report once A's phantom slug leaks.
        # Post-fix A reserves nothing, so B derives clean "vinst" and is NOT
        # reported. Only the real offender A (1.88) appears.
        conn = self._db(kol="Kon")
        d = self._slug_dir(tmp_path)
        populate_variable_slugs(conn, d)  # churning: writes scb.auto.toml
        self._add_variable(conn, var_id=88, name="Vinst", kol="3DOMR")
        self._add_variable(conn, var_id=99, name="4K-vy", kol="Vinst")
        (d / FREEZE_STATE_FILE).write_text('scb = "frozen"\n', encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            populate_variable_slugs(conn, d)
        assert exc.value.code == "slug_freeze_new_fallback"
        # Only the genuine offender A is reported; the clean sibling B is not.
        assert "1.88" in exc.value.message
        assert "1.99" not in exc.value.message
        assert exc.value.message.startswith("1 NEW")
