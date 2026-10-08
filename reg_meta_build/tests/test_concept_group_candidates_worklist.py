"""Concept-group candidate regeneration under accepted scopes (#496).

The candidate catalog itself (families, the label gate, collisions, ranking and the
round-trip through the worklist loader) is pinned at the CLI boundary
(`cases/cli/concept-group-candidates/`). Accepted scopes stay here: the
`concept-group-candidates` command reads them from the checkout's curation tree, not
from a directory a case can supply, so a case cannot reach them.

Fully synthetic: in-memory `_slugged_db` helpers, never the shipped
`concept_groups.toml` or a real built DB."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_variable, build_slugged_db
from reg_meta_build.concept_group_candidates import infer_concept_group_candidates

if TYPE_CHECKING:
    import sqlite3


def _base_db() -> sqlite3.Connection:
    """An scb/lisa register with no variables and no curated classification — the
    blank canvas each test seeds with `add_variable`."""
    return build_slugged_db(variable=None, version=None, classification=None)


def _add_family(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    stem: str,
    suffixes: list[int],
    name: str,
    var_id_base: int,
) -> None:
    """Add a digit-suffixed slug family (`<stem><suffix>`) all sharing one `name`
    (a strong, foldable family). `var_id` is unique per member."""
    for i, suffix in enumerate(suffixes):
        add_variable(
            conn,
            register_id=register_id,
            var_id=var_id_base + i,
            name=name,
            slug=f"{stem}{suffix}",
        )


class TestGenerator:
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
