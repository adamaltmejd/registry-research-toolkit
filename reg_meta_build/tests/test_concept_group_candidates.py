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
    _split_stem_suffix,
    _strip_digits,
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

    def test_fixed_qualifier_family_emitted(self) -> None:
        # The shared label carries a FIXED number ("Tillsyn 1 skolbarn …") that equals
        # one member's slot suffix. Under per-suffix stripping only the suffix-1 member
        # lost the constant ("Tillsyn  skolbarn …") while the others kept "Tillsyn 1 …",
        # breaking the common prefix → the family was dropped as a battery. Stripping
        # ALL digit runs uniformly normalizes every member to "Tillsyn  skolbarn ", so
        # the family agrees ~1.0 and EMITS. The display label is still the RAW common
        # prefix ("Tillsyn 1 skolbarn", trimmed).
        conn = _base_db()
        for i, suffix in enumerate([1, 2, 3]):
            add_variable(
                conn,
                register_id=1,
                var_id=950 + i,
                name=f"Tillsyn 1 skolbarn {suffix}",
                slug=f"tillsyn-1-skolbarn-{suffix}",
            )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.excluded_batteries == 0
        assert len(result.candidates) == 1
        c = result.candidates[0]
        # The digit strip on `tillsyn-1-skolbarn-1` leaves stem `tillsyn-1-skolbarn-`;
        # the trailing hyphen is trimmed off the URL key (#645). The display label
        # below still derives from the RAW names, so the fixed "1" is untouched.
        assert c.key == "tillsyn-1-skolbarn"
        assert [m.suffix for m in c.members] == [1, 2, 3]
        assert c.agreement > 0.9
        # Display label is the RAW common prefix, keeping the fixed "1".
        assert c.group_label == "Tillsyn 1 skolbarn"

    def test_trailing_hyphen_trimmed_from_key(self) -> None:
        # The digit strip on `artal-person-1` leaves stem `artal-person-`; the URL
        # key is the trailing-hyphen-TRIMMED slug `artal-person` (#645), not the
        # dangling-hyphen form. Members are unchanged (the slugs keep their hyphen).
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="artal-person-",  # slugs `artal-person-1/2/3`
            suffixes=[1, 2, 3],
            name="Antal år som person",
            var_id_base=2000,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.skipped_trim_collision == 0
        assert len(result.candidates) == 1
        c = result.candidates[0]
        assert c.key == "artal-person"
        assert [m.slug for m in c.members] == [
            "artal-person-1",
            "artal-person-2",
            "artal-person-3",
        ]

    def test_trim_collision_skipped_not_merged(self) -> None:
        # Two genuinely-distinct families whose stems TRIM to the same key:
        # `artal-person-1/2/3` (raw stem `artal-person-`) and `artal-person4/5/6`
        # (raw stem `artal-person`). Both → key `artal-person`. Folding them into one
        # group would silently merge unrelated families, so the bucket is skipped and
        # counted (never merged, never crashed).
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="artal-person-",
            suffixes=[1, 2, 3],
            name="Antal år som person",
            var_id_base=2100,
        )
        _add_family(
            conn,
            register_id=1,
            stem="artal-person",
            suffixes=[4, 5, 6],
            name="Annat antal år",
            var_id_base=2200,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.skipped_trim_collision == 1
        assert result.candidates == []
        assert result.excluded_batteries == 0

    def test_noise_singleton_does_not_poison_valid_family(self) -> None:
        # A real digit family `foo-1/2/3` (raw stem `foo-`) shares the trimmed key
        # `foo` with an unrelated SINGLETON `foo4` (raw stem `foo`, one member, not
        # itself foldable). The coarse "> 1 raw stem" check would skip the whole
        # bucket and drop the valid family; the refined check counts only raw stems
        # that independently qualify (>= min_siblings distinct suffixes), so only the
        # `foo-` family qualifies → it is EMITTED and the singleton is ignored, no
        # trim-collision counted (#645).
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="foo-",  # slugs `foo-1/2/3`, raw stem `foo-`
            suffixes=[1, 2, 3],
            name="Antal år",
            var_id_base=2400,
        )
        add_variable(
            conn, register_id=1, var_id=2410, name="Annat", slug="foo4"
        )  # raw stem `foo`, lone member
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.skipped_trim_collision == 0
        assert len(result.candidates) == 1
        c = result.candidates[0]
        assert c.key == "foo"
        assert [m.slug for m in c.members] == ["foo-1", "foo-2", "foo-3"]

    def test_battery_peer_does_not_suppress_valid_family(self) -> None:
        # A valid family `foo-1/2/3` (raw stem `foo-`, agreeing names) shares the
        # trimmed key `foo` with a count-qualifying BATTERY peer `foo4/5/6` (raw stem
        # `foo`, 3 distinct suffixes but DISAGREEING names → would never fold). The
        # buggy collision predicate counted the battery as a competing family on the
        # SUFFIX FLOOR alone and skipped the bucket, silently dropping the valid
        # family. The qualification now mirrors the full fold predicate, so the
        # battery doesn't count → the `foo` family IS emitted, not skipped as a
        # trim-collision (Codex P2 #646). The battery is not separately counted: it
        # never headlines its own bucket.
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="foo-",  # slugs `foo-1/2/3`, raw stem `foo-`
            suffixes=[1, 2, 3],
            name="Förvärvsinkomst total",  # agreeing, prefix >= 8
            var_id_base=2500,
        )
        # Battery peer: raw stem `foo`, 3 distinct suffixes, but genuinely different
        # label TEXT → weak agreement, never folds.
        add_variable(conn, register_id=1, var_id=2510, name="Apples", slug="foo4")
        add_variable(conn, register_id=1, var_id=2511, name="Oranges", slug="foo5")
        add_variable(conn, register_id=1, var_id=2512, name="Cars", slug="foo6")
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.skipped_trim_collision == 0
        assert len(result.candidates) == 1
        c = result.candidates[0]
        assert c.key == "foo"
        assert [m.slug for m in c.members] == ["foo-1", "foo-2", "foo-3"]

    def test_null_name_peer_does_not_suppress_valid_family(self) -> None:
        # Same shape as the battery-peer case, but the count-qualifying peer
        # `foo4/5/6` (raw stem `foo`) has a NULL member name → no labels to agree on,
        # never folds. It must NOT count as a competing family: the valid `foo-1/2/3`
        # family is still emitted, no trim-collision (Codex P2 #646).
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="foo-",
            suffixes=[1, 2, 3],
            name="Förvärvsinkomst total",
            var_id_base=2600,
        )
        add_variable(conn, register_id=1, var_id=2610, name="Inkomst år", slug="foo4")
        add_variable(conn, register_id=1, var_id=2611, name=None, slug="foo5")
        add_variable(conn, register_id=1, var_id=2612, name="Inkomst år", slug="foo6")
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.skipped_trim_collision == 0
        assert len(result.candidates) == 1
        c = result.candidates[0]
        assert c.key == "foo"
        assert [m.slug for m in c.members] == ["foo-1", "foo-2", "foo-3"]

    def test_hyphen_only_stem_not_emitted(self) -> None:
        # A slug whose only non-digit prefix is hyphens (`--1`/`--2`) trims to an
        # EMPTY stem — `_split_stem_suffix` returns None, so no empty/invalid key is
        # minted (mirrors the bare-number-slug drop).
        conn = _base_db()
        add_variable(conn, register_id=1, var_id=2300, name="Kod", slug="--1")
        add_variable(conn, register_id=1, var_id=2301, name="Kod", slug="--2")
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert result.candidates == []
        assert result.skipped_trim_collision == 0

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

    def test_vintage_axis_uses_raw_stem_not_trimmed_key(self) -> None:
        # `foo1-2000`/`foo1-2010`: the raw stem is `foo1-` (ends in `-`, proving the
        # 4-digit tail is a YEAR, not part of a longer number), but the trimmed key
        # `foo1` ends in a digit. Axis inference must use the RAW stem, so the family
        # is classified `vintage`; passing the trimmed `foo1` would mis-classify it
        # `numeric` (#645). The emitted key stays the trimmed `foo1`.
        conn = _base_db()
        _add_family(
            conn,
            register_id=1,
            stem="foo1-",  # slugs `foo1-2000`, `foo1-2010`; raw stem `foo1-`
            suffixes=[2000, 2010],
            name="Inkomst per år",
            var_id_base=450,
        )
        conn.commit()
        result = infer_concept_group_candidates(conn)
        assert len(result.candidates) == 1
        c = result.candidates[0]
        assert c.key == "foo1"
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

    def test_accepted_family_preserved_under_trim_collision(self) -> None:
        # Idempotent-regen + trim-collision interaction (Codex P2 #646): an
        # `[[accept]]`-ed family `artal-person-1/2/3` (raw stem `artal-person-`,
        # materialized as a curated group keyed on the trimmed `artal-person`) shares
        # its trimmed key with a SECOND independently-folding raw stem `artal-person4/5/6`
        # (raw stem `artal-person`) that appeared in a later build. The blanket
        # trim-collision skip would drop the WHOLE bucket — including the accepted
        # family — so the next build's `resolve_accept` would fail on the now-missing
        # candidate. With the accepted scope passed, the accepted subgroup is preserved
        # (re-emitted) and only the non-accepted peer is rejected; the collision is NOT
        # counted (the accepted family survives, it is not a dropped collision).
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
        # Materialize the accept: a curated group keyed on the trimmed `artal-person`
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

        # WITHOUT the accept scope: the accepted members are grouped → excluded, so
        # only the peer `artal-person4/5/6` is ungrouped (no collision seen). It is
        # then dropped anyway — its trimmed key `artal-person` collides with the
        # materialized curated group's key. Either way the accepted family is missing.
        bare = infer_concept_group_candidates(conn)
        assert bare.candidates == []
        assert bare.skipped_existing_key == 1

        # WITH the accept scope: the accepted family is preserved and re-emitted; the
        # non-accepted peer is rejected and NOT counted as a trim-collision.
        scope = frozenset({("scb", "lisa", "artal-person")})
        aware = infer_concept_group_candidates(conn, accepted_scopes=scope)
        assert aware.skipped_trim_collision == 0
        assert [c.key for c in aware.candidates] == ["artal-person"]
        c = aware.candidates[0]
        assert c.register_fqid == "scb/lisa"
        # The re-emitted candidate is the accepted family (the `artal-person-` slugs),
        # NOT the colliding peer.
        assert [m.slug for m in c.members] == [
            "artal-person-1",
            "artal-person-2",
            "artal-person-3",
        ]

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
        # Accept a DIFFERENT scope — the colliding `artal-person` key is not accepted.
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
        # Members carry the variable-leaf reference + facet.
        by_key = {g.key: g for g in groups}
        morsak = by_key["morsak"]
        assert all(m.variable is not None for m in morsak.members)
        assert [m.value for m in morsak.members] == ["1", "2", "3"]


class TestSplitStemSuffix:
    def test_plain_family_keeps_stem(self) -> None:
        # No trailing hyphen: key stem == raw stem, suffix parsed.
        assert _split_stem_suffix("sun-niva2000") == ("sun-niva", "sun-niva", 2000)

    def test_trailing_hyphen_trimmed_in_key_stem(self) -> None:
        # The digit strip leaves a trailing hyphen on the raw stem; the KEY stem is
        # trimmed (#645), the raw stem is returned untrimmed for collision detection.
        assert _split_stem_suffix("artal-person-1") == (
            "artal-person",
            "artal-person-",
            1,
        )

    def test_multiple_trailing_hyphens_all_trimmed(self) -> None:
        assert _split_stem_suffix("kod--3") == ("kod", "kod--", 3)

    def test_no_trailing_digits_is_none(self) -> None:
        assert _split_stem_suffix("agi1lonfink") is None

    def test_bare_number_is_none(self) -> None:
        assert _split_stem_suffix("2000") is None

    def test_hyphen_only_stem_is_none(self) -> None:
        # The stem is all hyphens → trims to empty → not a usable key, return None.
        assert _split_stem_suffix("--1") is None


class TestStripDigits:
    def test_all_digit_runs_removed(self) -> None:
        # Every maximal digit run is removed (mid-label slot number and any other
        # numeral), so number-invariant names of a multi-instance family agree.
        assert _strip_digits("Åtgärdskod 12, den förlösta") == (
            "Åtgärdskod , den förlösta"
        )
        # A fixed numeric qualifier AND the varying slot number both go in one pass —
        # the constant "1" can't survive on the member whose slot equals it.
        assert _strip_digits("Tillsyn 1 skolbarn 5") == "Tillsyn  skolbarn "

    def test_name_without_digits_unchanged(self) -> None:
        # A name carrying no digits is returned unchanged.
        assert _strip_digits("ICD-kod för multipel dödsorsak") == (
            "ICD-kod för multipel dödsorsak"
        )
