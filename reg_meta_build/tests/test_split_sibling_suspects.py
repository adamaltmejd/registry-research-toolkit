"""Tests for the read-only split-sibling SUSPECT diagnostic (#918;
`split_sibling_suspects.py`).

Mirrors the other read-only diagnostics' test shape (`test_doc_coverage.py`,
`test_concept_group_candidates.py`): `infer_split_sibling_suspects` over a
hand-built synthetic DB exercises the type_flip / length_disagree / same-shape /
co-grouped classification, and `render_suspects_toml` round-trips through a TOML
parse. A separate test proves the diagnostic never mutates the DB.

Fully synthetic (CLAUDE.md): builds its own in-memory DB via the `_slugged_db`
helpers; never reads a real built DB.
"""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING

from _slugged_db import add_variable, build_slugged_db
from reg_meta_build.split_sibling_suspects import (
    _split_relation_reason,
    infer_split_sibling_suspects,
    render_suspects_toml,
)

if TYPE_CHECKING:
    import sqlite3

# The default fixture register is scb/lisa (provider 1), variant 10. Reuse it for
# every family so the suspects share a register_fqid; provider_key (the source
# var_id) distinguishes the families.
_REGISTER_ID = 1
_VARIANT_ID = 10


def _base_db() -> sqlite3.Connection:
    """A blank scb/lisa register (no default variable / classification) — the
    canvas each test seeds with sibling families."""
    return build_slugged_db(variable=None, version=None, classification=None)


def _add_sibling(
    conn: sqlite3.Connection,
    *,
    var_id: int,
    slug: str,
    name: str,
    data_type: str,
    data_length: str | None,
    value_set_id: int | None = None,
    delivery_column: str | None = None,
    valid_to: str = "9999-12-31",
) -> None:
    """Add one split sibling: a `variable` (sharing `var_id`/provider_key with its
    family) plus its representative `variable_state` (the latest-era shape)."""
    add_variable(conn, register_id=_REGISTER_ID, var_id=var_id, name=name, slug=slug)
    _add_state(
        conn,
        variable_slug=slug,
        valid_to=valid_to,
        data_type=data_type,
        data_length=data_length,
        value_set_id=value_set_id,
        delivery_column_name=delivery_column,
    )


def _variable_id(conn: sqlite3.Connection, slug: str) -> int:
    return conn.execute(
        "SELECT variable_id FROM variable WHERE register_id = ? AND slug = ?",
        (_REGISTER_ID, slug),
    ).fetchone()[0]


def _add_state(
    conn: sqlite3.Connection,
    *,
    variable_slug: str,
    valid_from: str = "2018-01-01",
    valid_to: str = "9999-12-31",
    data_type: str,
    data_length: str | None,
    value_set_id: int | None = None,
    delivery_column_name: str | None = None,
) -> None:
    """Insert one `variable_state`. Unlike `_slugged_db.add_state`, this writes
    `data_length` (the shape field the suspect classifier reads alongside
    `data_type`)."""
    conn.execute(
        "INSERT INTO variable_state (variable_id, register_variant_id, valid_from, "
        "valid_to, data_type, data_length, value_set_id, delivery_column_name) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            _variable_id(conn, variable_slug),
            _VARIANT_ID,
            valid_from,
            valid_to,
            data_type,
            data_length,
            value_set_id,
            delivery_column_name,
        ),
    )


def _co_group(conn: sqlite3.Connection, *slugs: str) -> None:
    """Put the named variables into one shared `concept_group` (so the pair reads
    as already-folded / co_grouped)."""
    cur = conn.execute(
        "INSERT INTO concept_group (kind, register_id, group_key, label, source) "
        "VALUES ('variable', ?, 'grp', 'Group', 'curated')",
        (_REGISTER_ID,),
    )
    group_id = cur.lastrowid
    for slug in slugs:
        conn.execute(
            "INSERT INTO concept_group_variable (group_id, variable_id) VALUES (?, ?)",
            (group_id, _variable_id(conn, slug)),
        )


def _seed_corpus(conn: sqlite3.Connection) -> None:
    """Seed four split families exercising every branch:

    - var_id 100 — type_flip (numeric `int` vs text `varchar`) → suspect;
    - var_id 200 — length_disagree (both `Datum`/other, lengths 8 vs 10) → suspect;
    - var_id 300 — same shape (both `int`, length 4) → NOT a suspect;
    - var_id 400 — type_flip AND co-grouped → suspect flagged co_grouped.
    """
    # type_flip family.
    _add_sibling(
        conn,
        var_id=100,
        slug="flip-num",
        name="Belopp",
        data_type="int",
        data_length="8",
        delivery_column="Belopp",
    )
    _add_sibling(
        conn,
        var_id=100,
        slug="flip-text",
        name="Belopp",
        data_type="varchar",
        data_length="20",
        delivery_column="BeloppTxt",
    )
    # length_disagree family: both "other" (Datum), present-on-both differing lengths.
    _add_sibling(
        conn,
        var_id=200,
        slug="len-a",
        name="Datum",
        data_type="Datum",
        data_length="8",
    )
    _add_sibling(
        conn,
        var_id=200,
        slug="len-b",
        name="Datum",
        data_type="Datum",
        data_length="10",
    )
    # same-shape family: not a suspect (same class, same length).
    _add_sibling(
        conn,
        var_id=300,
        slug="same-a",
        name="Kommun",
        data_type="int",
        data_length="4",
    )
    _add_sibling(
        conn,
        var_id=300,
        slug="same-b",
        name="Kommun",
        data_type="int",
        data_length="4",
    )
    # co-grouped type_flip family.
    _add_sibling(
        conn,
        var_id=400,
        slug="cog-num",
        name="Kod",
        data_type="int",
        data_length="4",
    )
    _add_sibling(
        conn,
        var_id=400,
        slug="cog-text",
        name="Kod",
        data_type="text",
        data_length="10",
    )
    _co_group(conn, "cog-num", "cog-text")
    conn.commit()


class TestRelationReason:
    """Unit coverage of the shape classifier (mirrors `_import_bug_suspect`)."""

    def _shape(self, data_type, data_length):
        from reg_meta_build.split_sibling_suspects import SiblingShape

        return SiblingShape(
            variable_id=0,
            fqid="scb/lisa/x",
            name=None,
            data_type=data_type,
            data_length=data_length,
            has_value_set=False,
            delivery_column=None,
        )

    def test_type_flip(self) -> None:
        assert (
            _split_relation_reason(
                self._shape("int", "8"), self._shape("varchar", "20")
            )
            == "type_flip"
        )

    def test_length_disagree_on_other(self) -> None:
        # Both unclassifiable (Datum → other), present-on-both differing lengths.
        assert (
            _split_relation_reason(
                self._shape("Datum", "8"), self._shape("Datum", "10")
            )
            == "length_disagree"
        )

    def test_same_class_length_diff_not_suspect(self) -> None:
        # Same numeric class, differing widths → NOT flagged (normal for splits).
        assert (
            _split_relation_reason(self._shape("int", "4"), self._shape("int", "8"))
            is None
        )

    def test_other_with_missing_length_not_suspect(self) -> None:
        # An "other" pair with a missing length on one side has no shape evidence.
        assert (
            _split_relation_reason(
                self._shape("Datum", None), self._shape("Datum", "10")
            )
            is None
        )


class TestInfer:
    def test_family_and_pair_counts(self) -> None:
        conn = _base_db()
        _seed_corpus(conn)
        result = infer_split_sibling_suspects(conn)
        # 4 families, 8 variables total (the #805-shape sanity check, in miniature).
        assert result.family_count == 4
        assert result.family_variable_count == 8
        # Three of the four families are suspects (same-shape family is not).
        assert result.total_pairs == 3
        assert len(result.suspects) == 3
        assert result.co_grouped_count == 1

    def test_each_pair_classified(self) -> None:
        conn = _base_db()
        _seed_corpus(conn)
        result = infer_split_sibling_suspects(conn)
        by_key = {(s.provider_key, s.reason): s for s in result.suspects}
        # type_flip family 100.
        assert ("100", "type_flip") in by_key
        # length_disagree family 200.
        assert ("200", "length_disagree") in by_key
        # co-grouped type_flip family 400.
        cog = by_key[("400", "type_flip")]
        assert cog.co_grouped is True
        # The non-co-grouped families are NOT flagged.
        assert by_key[("100", "type_flip")].co_grouped is False
        # The same-shape family (300) produced no suspect.
        assert all(s.provider_key != "300" for s in result.suspects)

    def test_register_fqid_and_shape_evidence(self) -> None:
        conn = _base_db()
        _seed_corpus(conn)
        result = infer_split_sibling_suspects(conn)
        flip = next(s for s in result.suspects if s.provider_key == "100")
        assert flip.register_fqid == "scb/lisa"
        # Ordered by variable_id; both sides carry their representative shape.
        assert {flip.a.data_type, flip.b.data_type} == {"int", "varchar"}
        assert flip.a.variable_id < flip.b.variable_id
        assert flip.a.fqid.startswith("scb/lisa/")
        assert flip.a.delivery_column in {"Belopp", "BeloppTxt"}

    def test_representative_is_latest_era(self) -> None:
        # A sibling with two states must classify off the LATEST-valid_to one.
        conn = _base_db()
        # var_id 500: one sibling stays numeric across eras; its partner is text in
        # its latest era but numeric in an earlier era. The latest (text) era must
        # drive the type_flip.
        _add_sibling(
            conn,
            var_id=500,
            slug="era-num",
            name="X",
            data_type="int",
            data_length="4",
        )
        add_variable(
            conn, register_id=_REGISTER_ID, var_id=500, name="X", slug="era-text"
        )
        # Earlier era: numeric (would NOT flip against the int sibling).
        _add_state(
            conn,
            variable_slug="era-text",
            valid_from="2010-01-01",
            valid_to="2014-12-31",
            data_type="int",
            data_length="4",
        )
        # Latest era: text (flips).
        _add_state(
            conn,
            variable_slug="era-text",
            valid_from="2015-01-01",
            valid_to="9999-12-31",
            data_type="varchar",
            data_length="20",
        )
        conn.commit()
        result = infer_split_sibling_suspects(conn)
        suspect = next(s for s in result.suspects if s.provider_key == "500")
        assert suspect.reason == "type_flip"

    def test_singleton_family_ignored(self) -> None:
        # A var_id with a single variable is not a split family.
        conn = _base_db()
        _add_sibling(
            conn,
            var_id=900,
            slug="solo",
            name="Solo",
            data_type="int",
            data_length="4",
        )
        conn.commit()
        result = infer_split_sibling_suspects(conn)
        assert result.family_count == 0
        assert result.total_pairs == 0

    def test_empty_db(self) -> None:
        conn = _base_db()
        result = infer_split_sibling_suspects(conn)
        assert result.total_pairs == 0
        assert result.family_count == 0
        assert render_suspects_toml(result).strip().endswith("(no suspect pairs)")


class TestRender:
    def test_toml_parses_and_carries_pairs(self) -> None:
        conn = _base_db()
        _seed_corpus(conn)
        result = infer_split_sibling_suspects(conn)
        toml_text = render_suspects_toml(result)
        parsed = tomllib.loads(toml_text)
        pairs = parsed["pair"]
        assert len(pairs) == 3
        # Every pair carries the curation contract: register, the two variable
        # FQIDs, the reason, co_grouped, and the empty disposition placeholder.
        for p in pairs:
            assert p["register"] == "scb/lisa"
            assert p["variable_a"].startswith("scb/lisa/")
            assert p["variable_b"].startswith("scb/lisa/")
            assert p["reason"] in {"type_flip", "length_disagree"}
            assert p["disposition"] == ""
            assert isinstance(p["co_grouped"], bool)
        # The co-grouped pair round-trips its flag.
        cog = next(p for p in pairs if p["provider_key"] == "400")
        assert cog["co_grouped"] is True

    def test_high_value_first_grouping(self) -> None:
        # A family with MORE suspect pairs sorts before a smaller one. Build a
        # 3-member type_flip family (3 pairs) and a 2-member one (1 pair); the
        # bigger family's header must appear first.
        conn = _base_db()
        _add_sibling(
            conn,
            var_id=10,
            slug="big-a",
            name="B",
            data_type="int",
            data_length="4",
        )
        _add_sibling(
            conn,
            var_id=10,
            slug="big-b",
            name="B",
            data_type="text",
            data_length="8",
        )
        _add_sibling(
            conn,
            var_id=10,
            slug="big-c",
            name="B",
            data_type="char",
            data_length="8",
        )
        _add_sibling(
            conn,
            var_id=20,
            slug="small-a",
            name="S",
            data_type="int",
            data_length="4",
        )
        _add_sibling(
            conn,
            var_id=20,
            slug="small-b",
            name="S",
            data_type="varchar",
            data_length="8",
        )
        conn.commit()
        toml_text = render_suspects_toml(infer_split_sibling_suspects(conn))
        big_pos = toml_text.index("var_id 10")
        small_pos = toml_text.index("var_id 20")
        assert big_pos < small_pos


def test_diagnostic_does_not_mutate() -> None:
    """The diagnostic is READ-ONLY: row counts of every table it reads are
    unchanged after a run (no temp tables leak into the schema either)."""
    conn = _base_db()
    _seed_corpus(conn)
    tables = [
        "variable",
        "variable_state",
        "concept_group",
        "concept_group_variable",
        "register",
        "provider",
    ]

    def _snapshot() -> dict[str, int]:
        return {
            t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables
        }

    before = _snapshot()
    schema_before = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    result = infer_split_sibling_suspects(conn)
    render_suspects_toml(result)
    after = _snapshot()
    schema_after = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    assert before == after
    assert schema_before == schema_after
