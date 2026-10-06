"""Concept-group candidate worklists: regeneration under accepted scopes and the
rendered TOML round-trip through the worklist loader (#496).

Fully synthetic: in-memory `_slugged_db` helpers, never the shipped
`concept_groups.toml` or a real built DB."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _concept_group_families import add_family as _add_family, base_db as _base_db
from _slugged_db import add_register, add_variable
from reg_meta_build.concept_group_candidates import (
    infer_concept_group_candidates,
    render_candidates_toml,
)
from reg_meta_build.concept_groups import load_worklist_concept_groups

if TYPE_CHECKING:
    from pathlib import Path


class TestGenerator:
    def test_render_escapes_control_chars_and_roundtrips(self, tmp_path: Path) -> None:
        # A family name carrying an embedded newline (and quotes/backslash) must not
        # break the generated `label = "..."` line or the provenance comment: the
        # shared _toml_str escapes control chars and _toml_comment collapses newlines,
        # so the worklist still re-parses through load_worklist_concept_groups.
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="diag",
            suffixes=[1, 2],
            name='Diagnos\n"kod"\\rad',  # newline + quotes + backslash
            var_id_base=1500,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert len(result.candidates) == 1
        toml = render_candidates_toml(
            result, min_siblings=2, min_label_prefix=8, min_agreement=0.5
        )
        # The newline in the label must have been collapsed into the single
        # provenance comment line, not split it into a second (would-be-TOML) line:
        # exactly one `# axis=` line, and the fragment after it ("kod"...) must NOT
        # have leaked onto its own bare line.
        comment_lines = [ln for ln in toml.splitlines() if ln.startswith("# axis=")]
        assert len(comment_lines) == 1
        assert not any(ln.startswith('"kod"') for ln in toml.splitlines())

        path = tmp_path / "candidates.toml"
        path.write_text(toml, encoding="utf-8")
        groups = load_worklist_concept_groups(path)
        assert {g.key for g in groups} == {"diag"}

    def test_accepted_family_reemitted_when_scope_passed(self) -> None:
        # Idempotent regeneration: simulate an accepted auto family by materializing
        # it as a `curated` concept group keyed on its own stem ('morsak') and claiming
        # all three members. Every member is grouped AND the (register, key) names a
        # group, so a naive rescan would drop the family twice over.
        #
        # WITHOUT accepted_scopes the family is excluded (grouped members) — the bug.
        # WITH the family's (provider, register, key) in accepted_scopes it re-emits
        # as a candidate (members re-included, own key exempt from the collision
        # guard), so the literal group can be regenerated without changing membership.
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="morsak",
            suffixes=[1, 2, 3],
            name="ICD-kod underliggande dödsorsak",
            var_id_base=1600,
        )
        cur = conn.execute(
            "INSERT INTO concept_group (kind, register_id, group_key, label, source) "
            "VALUES ('variable', 1, 'morsak', 'ICD-kod', 'curated')"
        )
        group_id = cur.lastrowid
        for slug in ("morsak1", "morsak2", "morsak3"):
            vid = conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
                (slug,),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO concept_group_variable (variable_id, group_id) "
                "VALUES (?, ?)",
                (vid, group_id),
            )
        conn.commit()

        # Default (empty accepted_scopes): the materialized group hides the family.
        bare = infer_concept_group_candidates(conn)
        assert bare.candidates == []
        assert bare.skipped_existing_key == 0  # no ungrouped members → no family seen

        # With its group scope: the family re-emits, and its key is exempted.
        scope = frozenset({("scb", "lisa", "morsak")})
        aware = infer_concept_group_candidates(conn, accepted_scopes=scope)
        assert [c.key for c in aware.candidates] == ["morsak"]
        assert aware.skipped_existing_key == 0
        assert aware.excluded_batteries == 0
        c = aware.candidates[0]
        assert c.register_fqid == "scb/lisa"
        assert [m.suffix for m in c.members] == [1, 2, 3]

    def test_accepted_family_preserved_under_trim_collision(self) -> None:
        # Idempotent-regen + trim-collision interaction (Codex P2 #646): an
        # Materialized `[[group]]` family `artal-person-1/2/3` (raw stem `artal-person-`,
        # materialized as a curated group keyed on the trimmed `artal-person`) shares
        # its trimmed key with a SECOND independently-folding raw stem `artal-person4/5/6`
        # (raw stem `artal-person`) that appeared in a later build. The blanket
        # trim-collision skip would drop the WHOLE bucket — including the materialized
        # family — so candidate regeneration could no longer reproduce its membership.
        # With the group scope passed, that subgroup is preserved and only the peer is
        # rejected; the collision is NOT counted.
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="artal-person-",  # slugs artal-person-1/2/3, raw stem `artal-person-`
            suffixes=[1, 2, 3],
            name="Antal år som person",
            var_id_base=2700,
        )
        _add_family(
            conn,
            register_id=1,
            stem="artal-person",  # slugs artal-person4/5/6, raw stem `artal-person`
            suffixes=[4, 5, 6],
            name="Annat antal år personräkning",
            var_id_base=2710,
        )
        # Materialize the group keyed on the trimmed `artal-person`
        # claiming the THREE accepted members (the `artal-person-` family).
        cur = conn.execute(
            "INSERT INTO concept_group (kind, register_id, group_key, label, source) "
            "VALUES ('variable', 1, 'artal-person', 'Antal år', 'curated')"
        )
        group_id = cur.lastrowid
        for slug in ("artal-person-1", "artal-person-2", "artal-person-3"):
            vid = conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
                (slug,),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO concept_group_variable (variable_id, group_id) "
                "VALUES (?, ?)",
                (vid, group_id),
            )
        conn.commit()

        # WITHOUT the group scope: its members are grouped → excluded, so only the peer
        # `artal-person4/5/6` is ungrouped. Its trimmed key collides with the
        # materialized group key, so the family is missing.
        bare = infer_concept_group_candidates(conn)
        assert bare.candidates == []
        assert bare.skipped_existing_key == 1

        # WITH the group scope: its family is preserved and re-emitted; the peer is
        # rejected and NOT counted as a trim-collision.
        scope = frozenset({("scb", "lisa", "artal-person")})
        aware = infer_concept_group_candidates(conn, accepted_scopes=scope)
        assert aware.skipped_trim_collision == 0
        assert [c.key for c in aware.candidates] == ["artal-person"]
        c = aware.candidates[0]
        assert c.register_fqid == "scb/lisa"
        # The re-emitted candidate is the materialized family (`artal-person-` slugs),
        # NOT the colliding peer.
        assert [m.slug for m in c.members] == [
            "artal-person-1",
            "artal-person-2",
            "artal-person-3",
        ]

    def test_accepted_subgroup_degraded_peer_not_retargeted(self) -> None:
        # Preserve-or-fail (#651): a materialized `[[group]]` family whose raw-stem
        # subgroup NO LONGER folds (its labels degraded to NULL after corpus drift)
        # shares its trimmed key with a DIFFERENT raw stem that DOES fold. Only ONE
        # stem folds, so the old preserve block was SKIPPED — the peer was selected as
        # winner and, because the materialized key is exempt, emitted UNDER that key
        # with the WRONG variables. The fix selects the materialized subgroup as the
        # sole winner regardless of `fold_qual` count; the degraded subgroup is then
        # dropped by the emit path's NULL gate, and the folding peer is NEVER emitted
        # under the materialized key.
        conn = _base_db()
        # Materialized family `morsak-1/2/3` (raw stem `morsak-`), labels degraded to
        # NULL so it no longer folds; its members are in the register group.
        for i, suffix in enumerate([1, 2, 3]):
            add_variable(
                conn,
                register_id=1,
                var_id=2900 + i,
                name=None,  # degraded — no labels to agree on
                slug=f"morsak-{suffix}",
            )
        # A DIFFERENT raw stem `morsak` that DOES fold (`morsak4/5/6`, agreeing names).
        _add_family(
            conn,
            register_id=1,
            stem="morsak",  # slugs morsak4/5/6, raw stem `morsak`
            suffixes=[4, 5, 6],
            name="Annan dödsorsak kod",
            var_id_base=2910,
        )
        # Materialize the accept: a curated group keyed on the trimmed `morsak`
        # claiming the THREE accepted (degraded) members (the `morsak-` family).
        cur = conn.execute(
            "INSERT INTO concept_group (kind, register_id, group_key, label, source) "
            "VALUES ('variable', 1, 'morsak', 'Dödsorsak', 'curated')"
        )
        group_id = cur.lastrowid
        for slug in ("morsak-1", "morsak-2", "morsak-3"):
            vid = conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
                (slug,),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO concept_group_variable (variable_id, group_id) "
                "VALUES (?, ?)",
                (vid, group_id),
            )
        conn.commit()

        scope = frozenset({("scb", "lisa", "morsak")})
        result = infer_concept_group_candidates(conn, accepted_scopes=scope)
        # The folding peer (`morsak4/5/6`) is NOT emitted under the materialized key —
        # the degraded subgroup is selected and dropped by the NULL gate, so `morsak`
        # yields no candidate. The peer is not a symmetric collision drop, so no
        # trim-collision is counted.
        assert result.skipped_trim_collision == 0
        assert [c.key for c in result.candidates] == []
        # In particular the peer's slugs never surface under the materialized key.
        assert all(
            {m.slug for m in c.members}.isdisjoint({"morsak4", "morsak5", "morsak6"})
            for c in result.candidates
        )

    def test_non_accepted_trim_collision_still_skips_with_other_scope(self) -> None:
        # A genuine two-family trim-collision whose key is NOT accepted still
        # skips-and-counts, even when an UNRELATED scope is accepted: the accepted-
        # preserve path only fires for the colliding key's own accept (Codex P2 #646).
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="artal-person-",
            suffixes=[1, 2, 3],
            name="Antal år som person",
            var_id_base=2800,
        )
        _add_family(
            conn,
            register_id=1,
            stem="artal-person",
            suffixes=[4, 5, 6],
            name="Annat antal år personräkning",
            var_id_base=2810,
        )
        conn.commit()
        # Include a DIFFERENT group scope — `artal-person` is not materialized.
        scope = frozenset({("scb", "lisa", "morsak")})
        result = infer_concept_group_candidates(conn, accepted_scopes=scope)
        assert result.skipped_trim_collision == 1
        assert result.candidates == []

    def test_non_accepted_group_stays_excluded_with_scopes(self) -> None:
        # A custom `[[variable_group]]` / edge group is NOT a candidate even when
        # OTHER scopes are accepted: only the named scope is re-included. The 'custom'
        # group's members stay grouped, so its family is never emitted.
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="custom",
            suffixes=[1, 2],
            name="Hand authored family namn",
            var_id_base=1700,
        )
        cur = conn.execute(
            "INSERT INTO concept_group (kind, register_id, group_key, label, source) "
            "VALUES ('variable', 1, 'custom', 'Custom', 'curated')"
        )
        group_id = cur.lastrowid
        for slug in ("custom1", "custom2"):
            vid = conn.execute(
                "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
                (slug,),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO concept_group_variable (variable_id, group_id) "
                "VALUES (?, ?)",
                (vid, group_id),
            )
        conn.commit()

        # Include a DIFFERENT, non-existent group scope — custom stays excluded.
        scope = frozenset({("scb", "lisa", "morsak")})
        result = infer_concept_group_candidates(conn, accepted_scopes=scope)
        assert result.candidates == []

    def test_render_roundtrips_through_loader(self, tmp_path: Path) -> None:
        conn = _base_db()
        add_register(conn, register_id=2, slug="par", name="PAR")
        _add_family(
            conn,
            register_id=1,
            stem="morsak",
            suffixes=[1, 2, 3],
            name='ICD-kod "underliggande" dödsorsak',  # embedded quotes → escaping
            var_id_base=1000,
        )
        _add_family(
            conn,
            register_id=2,
            stem="sun-niva",
            suffixes=[2000, 2010],
            name="Utbildningsnivå enligt SUN",
            var_id_base=1100,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        toml = render_candidates_toml(
            result, min_siblings=2, min_label_prefix=8, min_agreement=0.5
        )
        path = tmp_path / "candidates.toml"
        path.write_text(toml, encoding="utf-8")

        groups = load_worklist_concept_groups(path)
        # Every emitted candidate re-parses as a curated group, same key/register.
        emitted = {(c.register_fqid, c.key) for c in result.candidates}
        parsed = {(f"{g.provider}/{g.register}", g.key) for g in groups}
        assert emitted == parsed
        assert len(groups) == len(result.candidates)
        # Members carry the variable-leaf reference + facet.
        by_key = {g.key: g for g in groups}
        morsak = by_key["morsak"]
        assert all(m.variable is not None for m in morsak.members)
        # The generator emits the legacy single-axis shape; the loader maps it to
        # whole-variable members (delivery_column None) with one coord each (#819).
        assert morsak.axes == (("ordinal", "ordinal"),)
        assert all(m.delivery_column is None for m in morsak.members)
        assert [m.coords[0][1] for m in morsak.members] == ["1", "2", "3"]
