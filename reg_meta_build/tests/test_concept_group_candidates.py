"""Tests for the concept-group fold-candidate generator (#496;
`concept_group_candidates.py`).

Mirrors `test_variable_same_as.py::TestGenerator`: `infer_concept_group_candidates`
over a synthetic DB exercises the foldable/battery split, the sibling floor, the
proposed-axis classifier, and the already-grouped exclusion;
`render_candidates_toml` round-trips through `concept_groups.load_concept_groups`.

Fully synthetic (CLAUDE.md): builds its own DBs (in-memory `_slugged_db` helpers)
and never reads the shipped `concept_groups.toml` or a real built DB."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _slugged_db import add_register, add_variable, build_slugged_db
from reg_meta_build.concept_group_candidates import (
    _strip_slot_number,
    infer_concept_group_candidates,
    render_candidates_toml,
)
from reg_meta_build.concept_groups import load_concept_groups

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path


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
    def test_strong_family_emitted_as_ordinal(self) -> None:
        # morsak1/2/3, all named "ICD-kod underliggande dödsorsak" → foldable,
        # contiguous run from 1 → axis=ordinal.
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="morsak",
            suffixes=[1, 2, 3],
            name="ICD-kod underliggande dödsorsak",
            var_id_base=100,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.excluded_batteries == 0
        assert len(result.candidates) == 1
        c = result.candidates[0]
        assert c.key == "morsak"
        assert c.register_fqid == "scb/lisa"
        assert c.axis == "ordinal"
        assert [m.suffix for m in c.members] == [1, 2, 3]
        # Single-digit suffixes → width 1, value == bare suffix; label placeholder.
        assert [m.value for m in c.members] == ["1", "2", "3"]
        assert [m.label for m in c.members] == ["1", "2", "3"]

    def test_mid_label_number_family_emitted(self) -> None:
        # The slot number sits MID-label ("Kod 1, x" … "Kod 3, x"): under raw
        # common-prefix scoring the prefix stops at "Kod " (the digit breaks it) and
        # the family is wrongly excluded as a battery. Number-invariant scoring strips
        # each member's own slot number first ("Kod , x"), so the family agrees ~1.0
        # and EMITS. The display label still derives from the RAW names → "Kod" (the
        # raw common prefix), NOT the stripped/garbled form.
        conn = _base_db()
        for i, suffix in enumerate([1, 2, 3]):
            add_variable(
                conn,
                register_id=1,
                var_id=900 + i,
                name=f"Åtgärdskod {suffix}, den förlösta",
                slug=f"flop{suffix}",
            )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.excluded_batteries == 0
        assert len(result.candidates) == 1
        c = result.candidates[0]
        assert c.key == "flop"
        assert [m.suffix for m in c.members] == [1, 2, 3]
        # Number-stripped names agree on all but the digit → very high agreement.
        assert c.agreement > 0.9
        # Display label is the RAW common prefix, not the stripped form.
        assert c.group_label == "Åtgärdskod"

    def test_battery_excluded_and_counted(self) -> None:
        # Same stem `f`, genuinely different label TEXT (not just the number) →
        # number-stripping doesn't make them agree → weak agreement → battery,
        # excluded from candidates and counted.
        conn = _base_db()
        add_variable(conn, register_id=1, var_id=200, name="Apples", slug="f1")
        add_variable(conn, register_id=1, var_id=201, name="Oranges", slug="f2")
        add_variable(conn, register_id=1, var_id=202, name="Cars", slug="f3")
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.candidates == []
        assert result.excluded_batteries == 1

    def test_below_min_siblings_not_emitted(self) -> None:
        # Only one distinct suffix → not a family at all (neither foldable nor
        # battery).
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="solo",
            suffixes=[1],
            name="Inkomst av tjänst",
            var_id_base=300,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.candidates == []
        assert result.excluded_batteries == 0

    def test_vintage_family_axis(self) -> None:
        # 4-digit year suffixes on a non-digit stem → axis=vintage; padded values
        # already 4-wide.
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="sun-niva",
            suffixes=[2000, 2010],
            name="Utbildningsnivå enligt SUN",
            var_id_base=400,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert len(result.candidates) == 1
        c = result.candidates[0]
        assert c.axis == "vintage"
        assert [m.value for m in c.members] == ["2000", "2010"]

    def test_numeric_axis_for_sparse_suffixes(self) -> None:
        # Non-year, non-contiguous numeric suffixes → axis=numeric.
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="kod",
            suffixes=[3, 7, 11],
            name="Standardkod för bransch",
            var_id_base=500,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert len(result.candidates) == 1
        assert result.candidates[0].axis == "numeric"
        # Width follows the max suffix (11 → 2 digits): zero-padded so members sort.
        assert [m.value for m in result.candidates[0].members] == ["03", "07", "11"]

    def test_already_grouped_variable_excluded(self) -> None:
        # A variable already in a concept_group_variable row is NOT a candidate —
        # the edge/month/curated passes already claimed it. The claiming group is
        # keyed OFF the family stem ('claimed', not 'morsak') so this exercises only
        # member-exclusion, not the key-collision skip (covered separately).
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="morsak",
            suffixes=[1, 2, 3],
            name="ICD-kod underliggande dödsorsak",
            var_id_base=600,
        )
        # Materialize a stub group (non-colliding key) and claim morsak1 as a member.
        cur = conn.execute(
            "INSERT INTO concept_group (kind, register_id, group_key, label, source) "
            "VALUES ('variable', 1, 'claimed', 'x', 'edge')"
        )
        group_id = cur.lastrowid
        claimed = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = 'morsak1'"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, ?)",
            (claimed, group_id),
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        # Only morsak2/morsak3 remain ungrouped → one distinct-suffix shortfall is
        # avoided (2 siblings >= default min 2), but morsak1 is gone.
        assert len(result.candidates) == 1
        c = result.candidates[0]
        assert [m.suffix for m in c.members] == [2, 3]

    def test_existing_group_key_collision_skipped(self) -> None:
        # An edge/token group already owns group_key 'morsak' in register 1. A
        # foldable family keyed on the SAME (register, stem) would collide on
        # idx_concept_group_key if curated verbatim, so it is NOT emitted and is
        # counted into skipped_existing_key (mirrors _derive_month_groups' guard).
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="morsak",
            suffixes=[1, 2, 3],
            name="ICD-kod underliggande dödsorsak",
            var_id_base=1300,
        )
        # An existing group on the colliding key, with an unrelated member (a
        # different register's variable so it doesn't suppress the family itself).
        add_register(conn, register_id=2, slug="par", name="PAR")
        add_variable(conn, register_id=2, var_id=1399, name="Annan", slug="annan1")
        cur = conn.execute(
            "INSERT INTO concept_group (kind, register_id, group_key, label, source) "
            "VALUES ('variable', 1, 'morsak', 'x', 'edge')"
        )
        group_id = cur.lastrowid
        other = conn.execute(
            "SELECT variable_id FROM variable WHERE register_id = 2 AND slug = 'annan1'"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO concept_group_variable (variable_id, group_id) VALUES (?, ?)",
            (other, group_id),
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        # morsak1/2/3 are all ungrouped (the existing group's member is in par), but
        # the key collides → skipped, not emitted, not counted as a battery.
        assert result.skipped_existing_key == 1
        assert [c.key for c in result.candidates] == []
        assert result.excluded_batteries == 0

    def test_existing_key_collision_counted_once_not_as_battery(self) -> None:
        # A colliding family whose names would ALSO fail the battery gate is counted
        # once — as a key-collision skip (the check runs first), not a battery.
        conn = _base_db()
        add_variable(conn, register_id=1, var_id=1400, name="Ålder", slug="f1")
        add_variable(conn, register_id=1, var_id=1401, name="Kön", slug="f2")
        add_variable(conn, register_id=1, var_id=1402, name="Civilstånd", slug="f3")
        conn.execute(
            "INSERT INTO concept_group (kind, register_id, group_key, label, source) "
            "VALUES ('variable', 1, 'f', 'x', 'edge')"
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.skipped_existing_key == 1
        assert result.excluded_batteries == 0
        assert result.candidates == []

    def test_render_escapes_control_chars_and_roundtrips(self, tmp_path: Path) -> None:
        # A family name carrying an embedded newline (and quotes/backslash) must not
        # break the generated `label = "..."` line or the provenance comment: the
        # shared _toml_str escapes control chars and _toml_comment collapses newlines,
        # so the worklist still re-parses through load_concept_groups.
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
        groups = load_concept_groups(path)
        assert {g.key for g in groups} == {"diag"}

    def test_accepted_family_reemitted_when_scope_passed(self) -> None:
        # Idempotent regeneration: simulate an `[[accept]]`-ed auto family by
        # materializing it as a `curated` concept group keyed on its own stem
        # ('morsak') and claiming all three members. This reproduces a normal built
        # DB AFTER the accept landed: every member is grouped AND the (register, key)
        # names a group, so a naive rescan would drop the family twice over.
        #
        # WITHOUT accepted_scopes the family is excluded (grouped members) — the bug.
        # WITH the family's (provider, register, key) in accepted_scopes it re-emits
        # as a candidate (members re-included, own key exempt from the collision
        # guard), so the accept stays resolvable on the next build.
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

        # Default (empty accepted_scopes): the materialized accept hides the family.
        bare = infer_concept_group_candidates(conn)
        assert bare.candidates == []
        assert bare.skipped_existing_key == 0  # no ungrouped members → no family seen

        # Accept-aware: the family re-emits, key-collision guard exempts its own key.
        scope = frozenset({("scb", "lisa", "morsak")})
        aware = infer_concept_group_candidates(conn, accepted_scopes=scope)
        assert [c.key for c in aware.candidates] == ["morsak"]
        assert aware.skipped_existing_key == 0
        assert aware.excluded_batteries == 0
        c = aware.candidates[0]
        assert c.register_fqid == "scb/lisa"
        assert [m.suffix for m in c.members] == [1, 2, 3]

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

        # Accept a DIFFERENT, non-existent scope — the custom family stays excluded.
        scope = frozenset({("scb", "lisa", "morsak")})
        result = infer_concept_group_candidates(conn, accepted_scopes=scope)
        assert result.candidates == []

    def test_null_name_family_skipped(self) -> None:
        # A family with a NULL member name has no labels to agree on → conservative
        # skip (neither foldable nor counted as a battery).
        conn = _base_db()
        add_variable(conn, register_id=1, var_id=700, name="Diagnos A", slug="diag1")
        add_variable(conn, register_id=1, var_id=701, name=None, slug="diag2")
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.candidates == []
        assert result.excluded_batteries == 0

    def test_ranking_is_deterministic(self) -> None:
        # Two foldable families: the higher-agreement one ranks first.
        conn = _base_db()
        add_register(conn, register_id=2, slug="par", name="PAR")
        # Family A: long shared prefix → high agreement.
        _add_family(
            conn,
            register_id=1,
            stem="morsak",
            suffixes=[1, 2, 3],
            name="ICD-kod underliggande dödsorsak",
            var_id_base=800,
        )
        # Family B: shorter shared prefix relative to name length → lower agreement,
        # but still foldable (>= min_label_prefix=8).
        _add_family(
            conn,
            register_id=2,
            stem="substans",
            suffixes=[1, 2],
            name="Substanskod ATC behandling läkemedel långt namn",
            var_id_base=900,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert len(result.candidates) == 2
        assert result.candidates[0].agreement >= result.candidates[1].agreement
        assert result.candidates[0].key == "morsak"

    def test_group_label_preserves_original_case(self) -> None:
        # Names share a mixed-case prefix — the display label must carry the ORIGINAL
        # casing (derived from the original-case common prefix), not the casefolded
        # form used to score agreement.
        conn = _base_db()
        add_variable(
            conn, register_id=1, var_id=1200, name="Förvärvsinkomst total", slug="ink1"
        )
        add_variable(
            conn, register_id=1, var_id=1201, name="Förvärvsinkomst netto", slug="ink2"
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert len(result.candidates) == 1
        # Original-case shared prefix "Förvärvsinkomst " (trimmed), NOT "förvärvs…".
        assert result.candidates[0].group_label == "Förvärvsinkomst"

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

        groups = load_concept_groups(path)
        # Every emitted candidate re-parses as a curated group, same key/register.
        emitted = {(c.register_fqid, c.key) for c in result.candidates}
        parsed = {(f"{g.provider}/{g.register}", g.key) for g in groups}
        assert emitted == parsed
        assert len(groups) == len(result.candidates)
        # Members carry the variable-leaf reference (not a group ref) + facet.
        by_key = {g.key: g for g in groups}
        morsak = by_key["morsak"]
        assert all(m.variable is not None and m.group is None for m in morsak.members)
        assert [m.value for m in morsak.members] == ["1", "2", "3"]


class TestStripSlotNumber:
    def test_mid_label_number_removed_once(self) -> None:
        # The slot number sitting mid-label is removed (first whole-number
        # occurrence), so number-stripped names of a multi-instance family agree.
        assert _strip_slot_number("Åtgärdskod 1, den förlösta", 1) == (
            "Åtgärdskod , den förlösta"
        )
        assert _strip_slot_number("Åtgärdskod 12, den förlösta", 12) == (
            "Åtgärdskod , den förlösta"
        )

    def test_substring_of_longer_number_not_clipped(self) -> None:
        # Digit-boundary aware: a slot number that's a substring of a longer number
        # in the label is NOT clipped (the "1" inside "ICD-10" / "10" survives).
        assert _strip_slot_number("ICD-10 kod 1", 1) == "ICD-10 kod "
        assert _strip_slot_number("Variabel 10", 1) == "Variabel 10"

    def test_only_first_occurrence_removed(self) -> None:
        # count=1: a second occurrence of the same number is left intact.
        assert _strip_slot_number("Kod 1 av 1", 1) == "Kod  av 1"

    def test_name_without_number_unchanged(self) -> None:
        # When the slot number isn't present in the name (e.g. morsak1 named without
        # a "1"), the name is returned unchanged.
        assert _strip_slot_number("ICD-kod för multipel dödsorsak", 1) == (
            "ICD-kod för multipel dödsorsak"
        )
