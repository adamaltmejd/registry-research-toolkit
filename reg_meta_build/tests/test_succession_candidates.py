"""Tests for the read-only succession-candidate diagnostic
(`succession_candidates.py`).

`infer_succession_candidates` over a hand-built synthetic DB exercises the
candidate shape plus every gate (already-edged, co-delivered, non-adjacent), and
`render_succession_toml` is round-tripped BOTH through a plain TOML parse and
through `relations.py`'s real loader —
the emitted worklist must be in the exact `[[edge]]` grammar
`curation/relations.toml` accepts. A separate test proves the diagnostic never
mutates the DB.

Fully synthetic (CLAUDE.md): builds its own in-memory DB via the `_slugged_db`
helpers; never reads a real built DB.
"""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_state, add_variable, add_variant, build_slugged_db
from reg_meta_build.relations import (
    load_relations,
)
from reg_meta_build.succession_candidates import (
    infer_succession_candidates,
    render_succession_toml,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

# The default fixture register is scb/lisa (provider 1), variant 10
# (`individer-15plus`). Variant 20 (`individer-16plus`) is added by `_base_db` so a
# test can deliver eras in DIFFERENT variants.
_REGISTER_ID = 1
_VARIANT_ID = 10
_OTHER_VARIANT_ID = 20


def _base_db() -> sqlite3.Connection:
    """A blank scb/lisa register with two variants (no default variable /
    classification) — the canvas each test seeds with eras."""
    conn = build_slugged_db(variable=None, version=None, classification=None)
    add_variant(
        conn,
        register_variant_id=_OTHER_VARIANT_ID,
        register_id=_REGISTER_ID,
        slug="individer-16plus",
        name="Individer 16+",
    )
    return conn


def _add_era(
    conn: sqlite3.Connection,
    *,
    var_id: int,
    slug: str,
    name: str,
    column: str,
    valid_from: str,
    valid_to: str,
    variant_id: int = _VARIANT_ID,
) -> None:
    """Add one delivered era: the `variable` (minted on first use), its
    `variable_state` window, and the `variable_alias` row that makes the
    `(variable, column)` representation LIVE."""
    exists = conn.execute(
        "SELECT 1 FROM variable WHERE register_id = ? AND slug = ?",
        (_REGISTER_ID, slug),
    ).fetchone()
    if exists is None:
        add_variable(
            conn, register_id=_REGISTER_ID, var_id=var_id, name=name, slug=slug
        )
    add_state(
        conn,
        register_id=_REGISTER_ID,
        variable_slug=slug,
        register_variant_id=variant_id,
        valid_from=valid_from,
        valid_to=valid_to,
        delivery_column_name=column,
    )
    conn.execute(
        "INSERT OR IGNORE INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) "
        "SELECT variable_id, ?, ? FROM variable WHERE register_id = ? AND slug = ?",
        (variant_id, column, _REGISTER_ID, slug),
    )


def _add_variable_edge(conn: sqlite3.Connection, pred: str, succ: str) -> None:
    """Curate away a pair: one `variable_replaced_by` row between two scb/lisa
    variable slugs."""
    conn.execute(
        "INSERT INTO variable_replaced_by (predecessor_provider, "
        "predecessor_register, predecessor_variable, successor_provider, "
        "successor_register, successor_variable) "
        "VALUES ('scb', 'lisa', ?, 'scb', 'lisa', ?)",
        (pred, succ),
    )


def _seed_corpus(conn: sqlite3.Connection) -> None:
    """Seed the four families the diagnostic must separate:

    - var 31395 / 47670 `ForvErs` — the SAME column re-minted under a new var_id,
      adjacent windows (2021-12-31 → 2022-01-01) → candidate;
    - var 41660 / 37046 `AnnInk` — the cross-var_id shape, but the pair is ALREADY
      joined by a `variable_replaced_by` edge → skipped;
    - var 700 / 701 `Kod` — the cross-var_id shape over adjacent windows, but the
      predecessor also ships a LATER era co-delivered with the successor in the
      same variant → skipped (co-delivered);
    - var 800 / 801 `Ort` — the cross-var_id shape but with a YEAR-WIDE GAP between
      the windows → skipped (not adjacent, so not one column's era chain).
    """
    _add_era(
        conn,
        var_id=31395,
        slug="forvink-ers-aktiv",
        name="Förvärvsinkomst, aktiv",
        column="ForvErs",
        valid_from="2010-01-01",
        valid_to="2021-12-31",
    )
    _add_era(
        conn,
        var_id=47670,
        slug="forvink-ers",
        name="Förvärvsinkomst",
        column="ForvErs",
        valid_from="2022-01-01",
        valid_to="2023-12-31",
    )
    _add_era(
        conn,
        var_id=41660,
        slug="anninkf",
        name="Summa annan inkomst",
        column="AnnInk",
        valid_from="2010-01-01",
        valid_to="2015-12-31",
    )
    _add_era(
        conn,
        var_id=37046,
        slug="anninkf04",
        name="Summa annan inkomst, 2004 års definition",
        column="AnnInk",
        valid_from="2016-01-01",
        valid_to="2023-12-31",
    )
    _add_variable_edge(conn, "anninkf", "anninkf04")
    _add_era(
        conn,
        var_id=700,
        slug="kod-gammal",
        name="Kod",
        column="Kod",
        valid_from="2010-01-01",
        valid_to="2015-12-31",
    )
    _add_era(
        conn,
        var_id=701,
        slug="kod-ny",
        name="Kod",
        column="Kod",
        valid_from="2016-01-01",
        valid_to="2023-12-31",
    )
    # The predecessor kept shipping alongside the successor: same variant,
    # overlapping windows → the pair was CO-DELIVERED.
    _add_era(
        conn,
        var_id=700,
        slug="kod-gammal",
        name="Kod",
        column="Kod",
        valid_from="2016-01-01",
        valid_to="2018-12-31",
    )
    _add_era(
        conn,
        var_id=800,
        slug="ort-gammal",
        name="Ort",
        column="Ort",
        valid_from="2010-01-01",
        valid_to="2015-12-31",
    )
    _add_era(
        conn,
        var_id=801,
        slug="ort-ny",
        name="Ort",
        column="Ort",
        valid_from="2018-01-01",
        valid_to="2023-12-31",
    )
    conn.commit()


class TestInfer:
    def test_only_the_real_succession(self) -> None:
        conn = _base_db()
        _seed_corpus(conn)
        result = infer_succession_candidates(conn)
        assert [(c.predecessor.fqid, c.successor.fqid) for c in result.candidates] == [
            ("scb/lisa/forvink-ers-aktiv", "scb/lisa/forvink-ers"),
        ]
        assert result.total == 1
        assert result.per_register_counts == {"scb/lisa": 1}

    def test_cross_var_id_evidence(self) -> None:
        conn = _base_db()
        _seed_corpus(conn)
        (cross,) = infer_succession_candidates(conn).candidates
        assert cross.predecessor.provider_key == "31395"
        assert cross.successor.provider_key == "47670"
        assert cross.predecessor.column == cross.successor.column == "ForvErs"
        assert (cross.predecessor.valid_from, cross.predecessor.valid_to) == (
            "2010-01-01",
            "2021-12-31",
        )
        assert cross.effective_year == 2022
        assert cross.register_fqid == "scb/lisa"

    def test_the_curated_edge_is_what_suppresses_the_anninkf_pair(self) -> None:
        # `test_only_the_real_succession` proves the AnnInk pair is absent;
        # this proves the EDGE is why — drop it and the pair is a candidate.
        conn = _base_db()
        _seed_corpus(conn)
        conn.execute("DELETE FROM variable_replaced_by")
        conn.commit()
        assert ("scb/lisa/anninkf", "scb/lisa/anninkf04") in [
            (c.predecessor.fqid, c.successor.fqid)
            for c in infer_succession_candidates(conn).candidates
        ]

    def test_edge_in_the_reverse_direction_also_skips(self) -> None:
        # The gate is direction-blind: an edge either way means the pair is curated.
        conn = _base_db()
        _seed_corpus(conn)
        conn.execute("DELETE FROM variable_replaced_by")
        _add_variable_edge(conn, "anninkf04", "anninkf")
        conn.commit()
        assert all(
            "anninkf" not in c.predecessor.fqid
            for c in infer_succession_candidates(conn).candidates
        )

    def test_representation_edge_also_skips(self) -> None:
        # A curated column-grain edge between the two variables settles the pair
        # whatever columns it names. Fails if the edge gate reads only
        # `variable_replaced_by`.
        conn = _base_db()
        _seed_corpus(conn)
        conn.execute(
            "INSERT INTO representation_replaced_by (predecessor_provider, "
            "predecessor_register, predecessor_variable, predecessor_column, "
            "successor_provider, successor_register, successor_variable, "
            "successor_column) VALUES ('scb', 'lisa', 'forvink-ers-aktiv', "
            "'ForvErs', 'scb', 'lisa', 'forvink-ers', 'ForvErs')"
        )
        conn.commit()
        assert infer_succession_candidates(conn).total == 0

    def test_the_overlapping_era_is_what_suppresses_the_kod_pair(self) -> None:
        # var 700/701's windows are adjacent; the pair is absent only because var
        # 700 also ships an era overlapping the successor in the same variant.
        # Drop that era and the succession is a candidate.
        conn = _base_db()
        _seed_corpus(conn)
        conn.execute("DELETE FROM variable_state WHERE valid_to = '2018-12-31'")
        conn.commit()
        assert ("scb/lisa/kod-gammal", "scb/lisa/kod-ny") in [
            (c.predecessor.fqid, c.successor.fqid)
            for c in infer_succession_candidates(conn).candidates
        ]

    def test_the_gap_is_what_suppresses_the_ort_pair(self) -> None:
        # var 800/801 are 2010-2015 and 2018-2023 — two years apart, so not one
        # column's era chain. Close the gap and the same pair is a candidate.
        conn = _base_db()
        _seed_corpus(conn)
        conn.execute(
            "UPDATE variable_state SET valid_from = '2016-01-01' "
            "WHERE valid_from = '2018-01-01'"
        )
        conn.commit()
        assert ("scb/lisa/ort-gammal", "scb/lisa/ort-ny") in [
            (c.predecessor.fqid, c.successor.fqid)
            for c in infer_succession_candidates(conn).candidates
        ]

    def test_two_shared_columns_are_one_variable_grain_candidate(self) -> None:
        # A variable delivers its own column per variant. When two var_ids succeed
        # each other in SEVERAL variants, the emitted edge is variable-grain and
        # identical for each — so it is ONE candidate, evidenced by the earliest
        # transition, not one per era pair.
        conn = _base_db()
        for variant_id, column in (
            (_VARIANT_ID, "Belopp"),
            (_OTHER_VARIANT_ID, "Antal"),
        ):
            _add_era(
                conn,
                var_id=900,
                slug="matt-gammal",
                name="Mått",
                column=column,
                valid_from="2010-01-01",
                valid_to="2015-12-31",
                variant_id=variant_id,
            )
            _add_era(
                conn,
                var_id=901,
                slug="matt-ny",
                name="Mått",
                column=column,
                valid_from="2016-01-01",
                valid_to="2023-12-31",
                variant_id=variant_id,
            )
        conn.commit()
        result = infer_succession_candidates(conn)
        assert result.total == 1
        assert result.candidates[0].predecessor.column == "Belopp"

    @pytest.mark.parametrize(
        ("predecessor_to", "successor_from", "successor_to", "emitted"),
        [
            # A mid-year end followed by the next delivery year is still succession.
            ("2015-06-30", "2016-01-01", "2023-12-31", True),
            # Inside one year, the day after succeeds.
            ("2015-06-30", "2015-07-01", "2023-12-31", True),
            # Inside one year, a gap does not.
            ("2015-06-30", "2015-09-01", "2023-12-31", False),
            # An open-ended (`9999-12-31`) successor is also tried as a
            # predecessor of the earlier era; that overlap is rejected before any
            # date arithmetic past the sentinel.
            ("2015-12-31", "2016-01-01", "9999-12-31", True),
        ],
        ids=[
            "year-after-clipped-era",
            "same-year-day-after",
            "same-year-gap",
            "open-ended-successor",
        ],
    )
    def test_adjacency_gate(
        self,
        predecessor_to: str,
        successor_from: str,
        successor_to: str,
        emitted: bool,
    ) -> None:
        conn = _base_db()
        _add_era(
            conn,
            var_id=950,
            slug="wert-gammal",
            name="Wert",
            column="Wert",
            valid_from="2010-01-01",
            valid_to=predecessor_to,
        )
        _add_era(
            conn,
            var_id=951,
            slug="wert-ny",
            name="Wert",
            column="Wert",
            valid_from=successor_from,
            valid_to=successor_to,
        )
        conn.commit()
        pairs = [
            (c.predecessor.fqid, c.successor.fqid)
            for c in infer_succession_candidates(conn).candidates
        ]
        expected = [("scb/lisa/wert-gammal", "scb/lisa/wert-ny")]
        assert pairs == (expected if emitted else [])

    def test_empty_db(self) -> None:
        conn = _base_db()
        result = infer_succession_candidates(conn)
        assert result.total == 0
        assert (
            render_succession_toml(result)
            .strip()
            .endswith("(no succession candidates)")
        )


class TestRender:
    def test_toml_parses_as_relations_edges(self) -> None:
        conn = _base_db()
        _seed_corpus(conn)
        parsed = tomllib.loads(
            render_succession_toml(infer_succession_candidates(conn))
        )
        assert parsed["edge"] == [
            {
                "type": "replaced_by",
                "from": "scb/lisa/forvink-ers-aktiv",
                "to": "scb/lisa/forvink-ers",
                "effective_year": 2022,
            },
        ]

    def test_evidence_comments_carry_the_columns_and_windows(self) -> None:
        conn = _base_db()
        _seed_corpus(conn)
        toml_text = render_succession_toml(infer_succession_candidates(conn))
        assert "# === register scb/lisa — 1 candidate(s) ===" in toml_text
        assert (
            "#   from: scb/lisa/forvink-ers-aktiv (Förvärvsinkomst, aktiv) "
            "var_id 31395, column ForvErs, variant individer-15plus, "
            "2010-01-01..2021-12-31" in toml_text
        )

    def test_round_trips_through_the_relations_loader(self, tmp_path: Path) -> None:
        # The worklist's whole point: a confirmed candidate copies into
        # curation/relations.toml verbatim. Parse the emitted text with the REAL
        # loader; persistence is exercised by the common metadata writer tests.
        conn = _base_db()
        _seed_corpus(conn)
        path = tmp_path / "worklist.toml"
        path.write_text(
            render_succession_toml(infer_succession_candidates(conn)), encoding="utf-8"
        )
        relations = load_relations(path)
        (variable,) = relations.replaced_by
        assert (
            variable.predecessor.variable,
            variable.successor.variable,
            variable.effective_year,
        ) == ("forvink-ers-aktiv", "forvink-ers", 2022)

    def test_emitted_candidates_are_no_longer_candidates(self, tmp_path: Path) -> None:
        # A catalog containing the accepted edge retires the worklist: the
        # already-edged gate drops the pair.
        conn = _base_db()
        _seed_corpus(conn)
        path = tmp_path / "worklist.toml"
        path.write_text(
            render_succession_toml(infer_succession_candidates(conn)), encoding="utf-8"
        )
        assert len(load_relations(path).replaced_by) == 1
        _add_variable_edge(conn, "forvink-ers-aktiv", "forvink-ers")
        assert infer_succession_candidates(conn).total == 0


def test_diagnostic_does_not_mutate() -> None:
    """The diagnostic is READ-ONLY: row counts of every table it reads are
    unchanged after a run (no temp tables leak into the schema either)."""
    conn = _base_db()
    _seed_corpus(conn)
    tables = [
        "variable",
        "variable_state",
        "variable_alias",
        "variable_replaced_by",
        "representation_replaced_by",
        "register",
        "register_variant",
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
    render_succession_toml(infer_succession_candidates(conn))
    assert _snapshot() == before
    assert {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    } == schema_before
