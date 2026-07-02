"""Tests for the unified curated relation surface (#522; `relations.py`).

The single typed `[[edge]]` surface consolidates the `same_as` / `replaced_by`
loaders. Layers:
  - `TestLoaderDispatch` — the `type` discriminator: unknown/missing type, and
    a field legal for one type rejected as foreign on another (a mis-typed edge).
  - `TestSameAsLoad` / `TestSameAsMaterialize` — same_as parse + materialize
    (both directions, endpoint resolution fail-fast, provider gate, cycle rejection,
    the component-size guard, classification grain).
  - `TestReplacedByLoad` — replaced_by parse (grain, self-loop, effective_year).
  - `TestMovedEdges` — the real edges moved into the file load through the
    repo `relations.toml`.

Fully synthetic (CLAUDE.md): builds its own TOMLs/DBs (tmp_path) and (except
`TestMovedEdges`) never reads the shipped `curation/relations.toml`."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _slugged_db import (
    add_register,
    add_state,
    add_variable,
    add_variant,
    build_slugged_db,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import FqidKind, parse as parse_fqid
from reg_meta_build.relations import (
    _SAME_AS_MAX_COMPONENT,
    CuratedReplacedBy,
    CuratedSameAs,
    derive_variable_vintage_succession,
    load_relations,
    materialize_curated_replaced_by,
    materialize_same_as,
    reject_nonmonotone_representation_cycles,
)

if TYPE_CHECKING:
    import sqlite3

_SCB = frozenset({"scb"})
# reg_meta_build/ package root (tests/ sits beside the curation/ dir).
_ROOT = Path(__file__).resolve().parent.parent


def _load(tmp_path: Path, text: str):  # noqa: ANN202 - returns CuratedRelations
    path = tmp_path / "relations.toml"
    path.write_text(text, encoding="utf-8")
    return load_relations(path)


# ---------------------------------------------------------------------------
# Dispatch: the `type` discriminator + per-type foreign-field rejection
# ---------------------------------------------------------------------------


class TestLoaderDispatch:
    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        rel = load_relations(None)
        assert rel.same_as == () and rel.replaced_by == ()
        assert load_relations(tmp_path / "absent.toml").same_as == ()

    def test_present_but_empty_file_is_empty(self, tmp_path: Path) -> None:
        rel = _load(tmp_path, "# no edges yet\n")
        assert rel.same_as == () and rel.replaced_by == ()

    def test_missing_type_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(tmp_path, '[[edge]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n')
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_invalid"

    def test_unknown_type_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "is_a"\na = "scb/lisa/x"\nb = "scb/rams/y"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_invalid"
        # The legal types are listed so a typo is self-correcting.
        for legal in ("same_as", "replaced_by"):
            assert legal in exc.value.remediation

    def test_misspelled_toplevel_key_rejected(self, tmp_path: Path) -> None:
        # `[[edges]]` (typo) would silently disable ALL curation → loud error.
        with pytest.raises(RegMetaError) as exc:
            _load(tmp_path, '[[edges]]\ntype = "same_as"\na = "scb/lisa/x"\n')
        assert exc.value.code == "relations_invalid"
        assert "edges" in exc.value.message

    def test_foreign_field_effective_year_on_same_as_rejected(
        self, tmp_path: Path
    ) -> None:
        # `effective_year` belongs to replaced_by; on a same_as edge it's the tell
        # of a mis-typed edge (right field, wrong type) → reject at load.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "same_as"\na = "scb/lisa/x"\nb = "scb/rams/y"\n'
                "effective_year = 2019\n",
            )
        assert exc.value.code == "relations_invalid"
        assert "effective_year" in exc.value.message

    def test_foreign_field_relation_kind_on_same_as_rejected(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "same_as"\na = "scb/lisa/x"\nb = "scb/rams/y"\n'
                'relation_kind = "similar_concept"\n',
            )
        assert exc.value.code == "relations_invalid"
        assert "relation_kind" in exc.value.message

    def test_foreign_field_a_on_replaced_by_rejected(self, tmp_path: Path) -> None:
        # `a`/`b` belong to same_as; replaced_by uses `from`/`to`.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
                'to = "scb/lisa/y"\na = "scb/lisa/x"\n',
            )
        assert exc.value.code == "relations_invalid"


# ---------------------------------------------------------------------------
# same_as — load
# ---------------------------------------------------------------------------


class TestSameAsLoad:
    def test_parses_variable_grain_edge(self, tmp_path: Path) -> None:
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "same_as"\na = "scb/lisa/inkomst"\n'
            'b = "scb/rams/inkomst"\nnote = "candidate:tier1"\n',
        )
        assert len(rel.same_as) == 1
        e = rel.same_as[0]
        assert e.grain is FqidKind.VARIABLE_BINDING
        assert (e.a_provider, e.a_register, e.a_variable) == ("scb", "lisa", "inkomst")
        assert (e.b_provider, e.b_register, e.b_variable) == ("scb", "rams", "inkomst")
        assert e.note == "candidate:tier1"

    def test_parses_classification_grain_edge(self, tmp_path: Path) -> None:
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "same_as"\na = "scb/sun2000"\nb = "scb/sun2020"\n',
        )
        assert len(rel.same_as) == 1
        e = rel.same_as[0]
        assert e.grain is FqidKind.CLASSIFICATION
        assert (e.a_provider, e.a_register, e.a_variable) == ("scb", "sun2000", None)

    def test_mismatched_grain_rejected(self, tmp_path: Path) -> None:
        # variable (3-seg) vs classification (2-seg) → not the same grain.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "same_as"\na = "scb/lisa/x"\nb = "scb/sun2020"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_note_is_optional(self, tmp_path: Path) -> None:
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "same_as"\na = "scb/lisa/x"\nb = "scb/rams/y"\n',
        )
        assert rel.same_as[0].note is None

    @pytest.mark.parametrize("fqid", ["scb", "scb/lisa/x/y", "scb//x", ""])
    def test_bad_fqid_arity_rejected(self, tmp_path: Path, fqid: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                f'[[edge]]\ntype = "same_as"\na = "{fqid}"\nb = "scb/rams/y"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_self_edge_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "same_as"\na = "scb/lisa/x"\nb = "scb/lisa/x"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_duplicate_unordered_pair_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "same_as"\na = "scb/lisa/x"\nb = "scb/rams/y"\n\n'
                '[[edge]]\ntype = "same_as"\na = "scb/rams/y"\nb = "scb/lisa/x"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_empty_note_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "same_as"\na = "scb/lisa/x"\nb = "scb/rams/y"\n'
                'note = ""\n',
            )
        assert exc.value.code == "relations_invalid"


# ---------------------------------------------------------------------------
# same_as — materialize
# ---------------------------------------------------------------------------


def _cross_register_db() -> sqlite3.Connection:
    """scb/lisa/<v1> + scb/rams/<v2>, both resolvable, no edges yet."""
    conn = build_slugged_db(classification=None)  # scb/lisa with `kon`
    add_register(conn, register_id=2, slug="rams", name="RAMS")
    add_variable(conn, register_id=1, var_id=900, name="Inkomst", slug="inkomst")
    add_variable(conn, register_id=2, var_id=901, name="Inkomst", slug="rinkomst")
    conn.commit()
    return conn


def _same_as_edge(
    a: str = "scb/lisa/inkomst",
    b: str = "scb/rams/rinkomst",
    *,
    note: str | None = "candidate:tier1",
) -> CuratedSameAs:
    pa, pb = a.split("/"), b.split("/")
    return CuratedSameAs(
        grain=FqidKind.VARIABLE_BINDING,
        a_provider=pa[0],
        a_register=pa[1],
        a_variable=pa[2],
        b_provider=pb[0],
        b_register=pb[1],
        b_variable=pb[2],
        note=note,
    )


def _same_as_rows(conn: sqlite3.Connection) -> list[tuple]:
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT a_provider, a_register, a_variable, b_provider, b_register, "
            "b_variable FROM variable_same_as ORDER BY a_register, b_register"
        )
    ]


class TestSameAsMaterialize:
    def test_cross_register_edge_writes_both_directions(self) -> None:
        conn = _cross_register_db()
        counts = materialize_same_as(conn, (_same_as_edge(),), providers=_SCB)
        assert counts == {"variable": 1, "classification": 0}
        assert _same_as_rows(conn) == [
            ("scb", "lisa", "inkomst", "scb", "rams", "rinkomst"),
            ("scb", "rams", "rinkomst", "scb", "lisa", "inkomst"),
        ]

    def test_unknown_register_fails_fast(self) -> None:
        conn = _cross_register_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as(
                conn, (_same_as_edge(b="scb/nonexistent/x"),), providers=_SCB
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_same_as_unknown_endpoint"
        assert "nonexistent" in exc.value.message
        assert _same_as_rows(conn) == []  # nothing written

    def test_unknown_a_register_fails_fast(self) -> None:
        conn = _cross_register_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as(conn, (_same_as_edge(a="scb/nope/x"),), providers=_SCB)
        assert exc.value.code == "relations_same_as_unknown_endpoint"
        assert "nope" in exc.value.message

    def test_unknown_variable_slug_fails_fast(self) -> None:
        conn = _cross_register_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as(
                conn, (_same_as_edge(b="scb/rams/renamed-tomorrow"),), providers=_SCB
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_same_as_unknown_endpoint"
        assert "scb/rams/renamed-tomorrow" in exc.value.message
        assert "variable" in exc.value.message
        assert _same_as_rows(conn) == []

    def test_out_of_build_provider_is_skipped(self) -> None:
        conn = _cross_register_db()
        counts = materialize_same_as(
            conn, (_same_as_edge(),), providers=frozenset({"sos"})
        )
        assert counts["variable"] == 0
        assert _same_as_rows(conn) == []

    def test_one_out_of_build_provider_is_skipped_before_variable_resolution(
        self,
    ) -> None:
        conn = _cross_register_db()
        counts = materialize_same_as(
            conn,
            (_same_as_edge(b="sos/rams/renamed-tomorrow"),),
            providers=_SCB,
        )
        assert counts["variable"] == 0
        assert _same_as_rows(conn) == []

    def test_reciprocal_edges_rejected_as_cycle(self) -> None:
        # Two curated edges for the same unordered pair, opposite directions, form
        # a 2-cycle in the as-declared graph → rejected before any INSERT.
        conn = _cross_register_db()
        edges = (
            _same_as_edge(a="scb/lisa/inkomst", b="scb/rams/rinkomst"),
            _same_as_edge(a="scb/rams/rinkomst", b="scb/lisa/inkomst"),
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as(conn, edges, providers=_SCB)
        assert exc.value.code == "relations_same_as_cycle"
        assert _same_as_rows(conn) == []

    def test_classification_grain_edge_writes(self) -> None:
        # Default fixture seeds the SUN2020 classification (publisher → scb). Add
        # a second so the edge has two resolvable endpoints.
        conn = build_slugged_db()  # ships SUN2020 / slug "sun2020"
        conn.execute(
            "INSERT INTO classification (short_name, name, slug) "
            "VALUES ('SUN2000', 'SUN 2000', 'sun2000')"
        )
        conn.commit()
        edge = CuratedSameAs(
            grain=FqidKind.CLASSIFICATION,
            a_provider="scb",
            a_register="sun2000",
            a_variable=None,
            b_provider="scb",
            b_register="sun2020",
            b_variable=None,
            note=None,
        )
        counts = materialize_same_as(conn, (edge,), providers=_SCB)
        assert counts == {"variable": 0, "classification": 1}
        rows = conn.execute(
            "SELECT a_provider, a_classification_slug, b_provider, "
            "b_classification_slug FROM classification_same_as "
            "ORDER BY a_classification_slug"
        ).fetchall()
        assert {tuple(r) for r in rows} == {
            ("scb", "sun2000", "scb", "sun2020"),
            ("scb", "sun2020", "scb", "sun2000"),
        }

    def test_component_size_guard_refuses_runaway_cluster(self) -> None:
        # A chain a0-a1-a2-…-aN that would merge into one identity component
        # larger than the cap is refused before any INSERT (#522). Build a register
        # with cap+1 variables and a chain of cap same_as edges linking them.
        conn = build_slugged_db(classification=None)
        add_register(conn, register_id=2, slug="rams", name="RAMS")
        n = _SAME_AS_MAX_COMPONENT + 1
        for i in range(n):
            register_id = 1 if i % 2 == 0 else 2
            add_variable(
                conn,
                register_id=register_id,
                var_id=900 + i,
                name=f"Variable {i}",
                slug=f"v{i}",
            )
        conn.commit()

        # Cross-register chain: even nodes in lisa, odd nodes in rams (so each
        # edge is cross-register, the only kind same_as allows).
        def fqid(i: int) -> str:
            return f"scb/lisa/v{i}" if i % 2 == 0 else f"scb/rams/v{i}"

        edges = tuple(_same_as_edge(a=fqid(i), b=fqid(i + 1)) for i in range(n - 1))
        with pytest.raises(RegMetaError) as exc:
            materialize_same_as(conn, edges, providers=_SCB)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_same_as_component_too_large"
        assert _same_as_rows(conn) == []  # nothing written


# ---------------------------------------------------------------------------
# replaced_by — load (DB-free shape validation)
# ---------------------------------------------------------------------------


class TestReplacedByLoad:
    def test_parses_variable_grain_edge(self, tmp_path: Path) -> None:
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/old"\n'
            'to = "scb/lisa/new"\neffective_year = 2019\nnote = "recut"\n',
        )
        assert len(rel.replaced_by) == 1
        e = rel.replaced_by[0]
        assert str(e.predecessor) == "scb/lisa/old"
        assert str(e.successor) == "scb/lisa/new"
        assert e.effective_year == 2019
        assert e.note == "recut"

    def test_parses_register_grain_edge(self, tmp_path: Path) -> None:
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "replaced_by"\nfrom = "sos/old"\nto = "scb/new"\n',
        )
        e = rel.replaced_by[0]
        assert e.predecessor.kind is FqidKind.REGISTER
        assert e.effective_year is None and e.note is None

    def test_mismatched_grain_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa"\n'
                'to = "scb/lisa/new"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_self_loop_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
                'to = "scb/lisa/x"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_bool_effective_year_rejected(self, tmp_path: Path) -> None:
        # `isinstance(True, int)` is True — a bare bool must not pass as a year.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
                'to = "scb/lisa/y"\neffective_year = true\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_variant_grain_rejected(self, tmp_path: Path) -> None:
        # The variant grain (4-segment) is out of scope for succession.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/v/2020"\n'
                'to = "scb/lisa/v/2021"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_parses_classification_grain_edge(self, tmp_path: Path) -> None:
        # #579: the `class/<slug>` form parses as a CLASSIFICATION-grain edge.
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "replaced_by"\nfrom = "class/sun1996"\n'
            'to = "class/sun-niva2000"\neffective_year = 2000\n',
        )
        assert len(rel.replaced_by) == 1
        e = rel.replaced_by[0]
        assert e.predecessor.kind is FqidKind.CLASSIFICATION
        assert e.successor.kind is FqidKind.CLASSIFICATION
        assert str(e.predecessor) == "class/sun1996"
        assert str(e.successor) == "class/sun-niva2000"
        assert e.effective_year == 2000
        assert e.note is None

    def test_classification_note_rejected(self, tmp_path: Path) -> None:
        # #579: `note` is provenance-only on a classification edge (the table has
        # no `beskrivning`, the build stamps `curated:slug_toml`), so a `note`
        # field is rejected at load rather than silently dropped — the human reason
        # belongs in a TOML `#` comment.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "class/sun1996"\n'
                'to = "class/sun-niva2000"\neffective_year = 2000\nnote = "split"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_invalid"
        assert "note" in exc.value.message

    def test_classification_mixed_with_register_rejected(self, tmp_path: Path) -> None:
        # A class↔register mix is a different grain on each side → rejected. The
        # `class/` form disambiguates from the 2-segment register grain.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "class/sun1996"\n'
                'to = "scb/lisa"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_classification_self_loop_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "class/sun1996"\n'
                'to = "class/sun1996"\n',
            )
        assert exc.value.code == "relations_invalid"

    # #843 representation grain: variable-grain edge + `from_column`/`to_column`.

    def test_parses_representation_edge(self, tmp_path: Path) -> None:
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/dispink"\n'
            'to = "scb/lisa/dispink"\nfrom_column = "DispInk04"\n'
            'to_column = "DispInk10"\neffective_year = 2010\nnote = "recut"\n',
        )
        assert len(rel.replaced_by) == 1
        e = rel.replaced_by[0]
        # Same variable FQID, two columns — LEGAL for a representation edge.
        assert str(e.predecessor) == "scb/lisa/dispink"
        assert str(e.successor) == "scb/lisa/dispink"
        assert e.predecessor_column == "DispInk04"
        assert e.successor_column == "DispInk10"
        assert e.effective_year == 2010
        assert e.note == "recut"

    def test_both_or_neither_column_required(self, tmp_path: Path) -> None:
        # Exactly one of from_column/to_column → rejected (a representation edge
        # names BOTH endpoints' columns).
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/a"\n'
                'to = "scb/lisa/b"\nfrom_column = "A"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_invalid"

    def test_column_on_register_grain_rejected(self, tmp_path: Path) -> None:
        # Columns require both endpoints variable-grain (column-within-variable).
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "sos/old"\n'
                'to = "scb/new"\nfrom_column = "A"\nto_column = "B"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_representation_self_loop_same_column_rejected(
        self, tmp_path: Path
    ) -> None:
        # Same variable AND same column = a genuine self-loop → rejected.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
                'to = "scb/lisa/x"\nfrom_column = "Col"\nto_column = "Col"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_representation_case_only_self_loop_rejected(self, tmp_path: Path) -> None:
        # Same variable, columns differing ONLY in case (`Col` / `col`) — the build
        # matches columns case-insensitively, so this is a case-only self-loop and
        # is rejected (not a legitimate column rename).
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
                'to = "scb/lisa/x"\nfrom_column = "Col"\nto_column = "col"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_invalid"

    def test_representation_same_variable_different_column_allowed(
        self, tmp_path: Path
    ) -> None:
        # Same variable FQID, DIFFERENT columns — the common column rename, LEGAL.
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
            'to = "scb/lisa/x"\nfrom_column = "Old"\nto_column = "New"\n',
        )
        assert len(rel.replaced_by) == 1

    def test_empty_column_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
                'to = "scb/lisa/y"\nfrom_column = ""\nto_column = "New"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_non_string_column_rejected(self, tmp_path: Path) -> None:
        # A non-string `from_column` (here a TOML integer) is not a delivery column
        # name → rejected at load.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
                'to = "scb/lisa/y"\nfrom_column = 123\nto_column = "New"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_from_column_on_same_as_rejected(self, tmp_path: Path) -> None:
        # `from_column` is foreign to a same_as edge — rejected by the per-type
        # field map (a mis-typed edge: representation field, wrong `type`).
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "same_as"\na = "scb/lisa/x"\n'
                'b = "scb/lisa/y"\nfrom_column = "A"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_representation_cross_register_rejected(self, tmp_path: Path) -> None:
        # A representation (column-rename) edge is INTRA-register; endpoints in
        # different registers (same provider) are rejected. Cross-register
        # succession uses the entity (variable) grain, not column fields.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
                'to = "scb/rams/x"\nfrom_column = "A"\nto_column = "B"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_invalid"

    def test_representation_same_register_different_variable_allowed(
        self, tmp_path: Path
    ) -> None:
        # Two sibling variables of ONE register, column moved across the variable
        # boundary — LEGAL (intra-register). End-to-end coverage is in the
        # cross-variable materialize test; this asserts the loader accepts it.
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/a"\n'
            'to = "scb/lisa/b"\nfrom_column = "A"\nto_column = "B"\n',
        )
        assert len(rel.replaced_by) == 1
        e = rel.replaced_by[0]
        assert str(e.predecessor) == "scb/lisa/a"
        assert str(e.successor) == "scb/lisa/b"
        assert e.predecessor_column == "A"
        assert e.successor_column == "B"

    # #846 variant scope: an optional `variant` register_variant slug on a
    # representation edge, defaulting to `''` (variable-level).

    def test_representation_no_variant_defaults_empty(self, tmp_path: Path) -> None:
        # A #843 representation edge with no `variant` parses to the `''` default.
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
            'to = "scb/lisa/x"\nfrom_column = "Old"\nto_column = "New"\n',
        )
        assert rel.replaced_by[0].variant == ""

    def test_representation_variant_parses(self, tmp_path: Path) -> None:
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "replaced_by"\nfrom = "scb/frida/firmkey"\n'
            'to = "scb/frida/firmkey"\nfrom_column = "borgnr"\n'
            'to_column = "persorgnr"\neffective_year = 2014\n'
            'variant = "punktskatter-for-energi"\n',
        )
        assert len(rel.replaced_by) == 1
        e = rel.replaced_by[0]
        assert e.variant == "punktskatter-for-energi"
        assert e.effective_year == 2014

    def test_variant_on_non_representation_edge_rejected(self, tmp_path: Path) -> None:
        # `variant` scopes a column-level rename; on a plain variable-grain edge
        # (no columns) it is a mis-modeled succession → rejected.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/old"\n'
                'to = "scb/lisa/new"\neffective_year = 2019\n'
                'variant = "individer"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_invalid"
        assert "variant" in exc.value.message

    def test_variant_requires_effective_year(self, tmp_path: Path) -> None:
        # A variant-scoped succession may be a time-monotone cycle; the cycle check
        # orders it by year, so `effective_year` is mandatory.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/frida/firmkey"\n'
                'to = "scb/frida/firmkey"\nfrom_column = "borgnr"\n'
                'to_column = "persorgnr"\nvariant = "punktskatter-for-energi"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_invalid"
        assert "effective_year" in exc.value.message

    def test_empty_variant_rejected(self, tmp_path: Path) -> None:
        # An explicit empty `variant = ""` is rejected — drop the field for the
        # variable-level default rather than spell the sentinel.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/x"\n'
                'to = "scb/lisa/x"\nfrom_column = "Old"\nto_column = "New"\n'
                'effective_year = 2010\nvariant = ""\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_variant_on_same_as_rejected(self, tmp_path: Path) -> None:
        # `variant` is foreign to a same_as edge — the per-type field map rejects it.
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "same_as"\na = "scb/lisa/x"\n'
                'b = "scb/lisa/y"\nvariant = "individer"\n',
            )
        assert exc.value.code == "relations_invalid"


# ---------------------------------------------------------------------------
# replaced_by — classification-grain materialize (#579)
# ---------------------------------------------------------------------------


def _replaced_by_edge(
    frm: str, to: str, *, year: int | None = None, note: str | None = None
) -> CuratedReplacedBy:
    """A `CuratedReplacedBy` built straight from FQID strings (the loader's
    output shape), so the materialize tests bypass the TOML round-trip."""
    return CuratedReplacedBy(
        predecessor=parse_fqid(frm),
        successor=parse_fqid(to),
        note=note,
        effective_year=year,
    )


def _class_succession_db() -> sqlite3.Connection:
    """A slugged scb/lisa DB carrying three live classifications — sun1996 plus
    the two 2000-split successors — and NO succession edges yet (the curated pass
    inserts them). `progress` is a no-op sink."""
    conn = build_slugged_db(classification=None)
    for short, slug in (
        ("SUN1996", "sun1996"),
        ("SUN-NIVA2000", "sun-niva2000"),
        ("SUN-INRIKTNING2000", "sun-inriktning2000"),
    ):
        conn.execute(
            "INSERT INTO classification (short_name, name, slug) VALUES (?, ?, ?)",
            (short, short, slug),
        )
    conn.commit()
    return conn


def _class_succession_rows(conn: sqlite3.Connection) -> list[tuple]:
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT predecessor_slug, successor_slug, effective_year, note "
            "FROM classification_replaced_by "
            "ORDER BY predecessor_slug, successor_slug"
        )
    ]


def _noop(_msg: str) -> None:
    pass


class TestClassificationReplacedByMaterialize:
    """#579: curated classification-grain `replaced_by` edges land in
    `classification_replaced_by` (alongside the #571 auto edges) — the sun1996 →
    2000-split 1→many dual the auto same-stem rule can't produce. BOTH endpoints
    must resolve to a live classification (unlike register/variable, where a dead
    predecessor is allowed) — classification succession is all-live by design."""

    def test_one_edge_writes_with_provenance_marker(self) -> None:
        conn = _class_succession_db()
        out = materialize_curated_replaced_by(
            conn,
            [_replaced_by_edge("class/sun1996", "class/sun-niva2000", year=2000)],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["classification"] == 1
        # `note` is provenance-only: the fixed `curated:slug_toml` marker (the
        # table has no `beskrivning`; the human reason lives in a TOML `#` comment).
        assert _class_succession_rows(conn) == [
            ("sun1996", "sun-niva2000", 2000, "curated:slug_toml"),
        ]

    def test_one_to_many_split_both_allowed(self) -> None:
        # The branching split is intentional: one predecessor, two successors.
        conn = _class_succession_db()
        out = materialize_curated_replaced_by(
            conn,
            [
                _replaced_by_edge("class/sun1996", "class/sun-niva2000", year=2000),
                _replaced_by_edge(
                    "class/sun1996", "class/sun-inriktning2000", year=2000
                ),
            ],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["classification"] == 2
        # Both rows stamp the provenance marker (note is provenance-only).
        assert _class_succession_rows(conn) == [
            ("sun1996", "sun-inriktning2000", 2000, "curated:slug_toml"),
            ("sun1996", "sun-niva2000", 2000, "curated:slug_toml"),
        ]

    def test_not_provider_gated(self) -> None:
        # Classifications are GLOBAL (`class/<slug>` has no provider) — an edge is
        # NOT skipped just because `scb` isn't the only/active provider; even a
        # disjoint provider set still materializes the classification edge.
        conn = _class_succession_db()
        out = materialize_curated_replaced_by(
            conn,
            [_replaced_by_edge("class/sun1996", "class/sun-niva2000", year=2000)],
            set(),
            set(),
            providers=frozenset({"sos"}),  # scb not present
            progress=_noop,
        )
        assert out["classification"] == 1
        assert out["skipped_inactive_provider"] == 0
        assert len(_class_succession_rows(conn)) == 1

    def test_unresolved_successor_fails_fast(self) -> None:
        conn = _class_succession_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [_replaced_by_edge("class/sun1996", "class/ghost2000")],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_unresolved_successor"
        assert _class_succession_rows(conn) == []  # nothing written

    def test_dead_predecessor_fails_fast(self) -> None:
        # UNLIKE the register/variable grain (where a dead predecessor is inserted
        # verbatim), the classification grain requires BOTH endpoints live: the read
        # side (`classification_chain`) and the validator's `dangling` check both
        # depend on classification succession being all-live, so a dead-predecessor
        # edge must FAIL FAST here (EXIT_CONFIG) rather than fail late at validation
        # (CLI) or ship a dangling row (`--no-validate`).
        conn = _class_succession_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [_replaced_by_edge("class/retired1990", "class/sun-niva2000")],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_unresolved_predecessor"
        assert _class_succession_rows(conn) == []  # nothing written

    def test_dedups_against_existing_auto_edge(self) -> None:
        # An auto #571 edge already in the table on the same (pred, succ) pair →
        # the curated row dedups against it (counted as a skip, no second row).
        conn = _class_succession_db()
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES ('sun1996', 'sun-niva2000', 2000, 'derived:vintage_chain')"
        )
        conn.commit()
        out = materialize_curated_replaced_by(
            conn,
            [_replaced_by_edge("class/sun1996", "class/sun-niva2000", year=2000)],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["classification"] == 0
        assert out["skipped_duplicate"] == 1
        # The pre-existing auto edge survives untouched (its note kept).
        assert _class_succession_rows(conn) == [
            ("sun1996", "sun-niva2000", 2000, "derived:vintage_chain"),
        ]

    def test_curated_edge_closing_cycle_with_auto_edge_fails(self) -> None:
        # An auto edge A→B plus a curated edge B→A close a 2-cycle in the COMBINED
        # classification graph — caught before any INSERT (both endpoints resolve,
        # so it's the cycle check, not the unresolved-successor guard).
        conn = _class_succession_db()
        conn.execute(
            "INSERT INTO classification_replaced_by "
            "(predecessor_slug, successor_slug, effective_year, note) "
            "VALUES ('sun-niva2000', 'sun1996', 1996, 'derived:vintage_chain')"
        )
        conn.commit()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [_replaced_by_edge("class/sun1996", "class/sun-niva2000")],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_cycle"
        # The cycle aborts before INSERT: only the pre-seeded auto edge remains.
        assert _class_succession_rows(conn) == [
            ("sun-niva2000", "sun1996", 1996, "derived:vintage_chain"),
        ]


# ---------------------------------------------------------------------------
# replaced_by — representation-grain materialize (#843)
# ---------------------------------------------------------------------------


def _representation_edge(
    frm: str,
    to: str,
    from_column: str,
    to_column: str,
    *,
    year: int | None = None,
    note: str | None = None,
    variant: str = "",
) -> CuratedReplacedBy:
    """A representation `CuratedReplacedBy` (variable-grain FQID + column pair),
    bypassing the TOML round-trip — the loader's output shape. `variant` (#846)
    scopes the succession to one register_variant; `''` = variable-level."""
    return CuratedReplacedBy(
        predecessor=parse_fqid(frm),
        successor=parse_fqid(to),
        note=note,
        effective_year=year,
        predecessor_column=from_column,
        successor_column=to_column,
        variant=variant,
    )


def _add_alias(
    conn: sqlite3.Connection, variable_slug: str, delivery_column_name: str
) -> None:
    """Add an extra observed delivery column to a live variable (so one variable
    can carry several representations). Resolves the variable by its register-unique
    slug under register_id=1 + the default variant (10) from `build_slugged_db`."""
    vid = conn.execute(
        "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
        (variable_slug,),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, 10, ?)",
        (vid, delivery_column_name),
    )
    conn.commit()


def _add_alias_in_variant(
    conn: sqlite3.Connection,
    variable_slug: str,
    delivery_column_name: str,
    register_variant_id: int,
) -> None:
    """Like `_add_alias` but binds the observed column to an EXPLICIT delivering
    variant (#846) — so a variable can observe different columns in different
    variants and the variant-aware endpoint-liveness check can be exercised."""
    vid = conn.execute(
        "SELECT variable_id FROM variable WHERE register_id = 1 AND slug = ?",
        (variable_slug,),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, ?, ?)",
        (vid, register_variant_id, delivery_column_name),
    )
    conn.commit()


def _representation_db_two_variants() -> sqlite3.Connection:
    """`_representation_db()` (variant 10 `individer-15plus` observing
    `DispInk04`/`DispInk10`) plus a SIBLING variant 20 `foretag` of the SAME
    register where `dispink` observes a DIFFERENT column `OrgInk20`. Lets a
    variant-scoped edge's endpoint liveness be checked PER variant: a `foretag`-
    scoped edge on `OrgInk20` is live, but one on `DispInk04` (live only in the
    sibling `individer-15plus`) is not."""
    conn = _representation_db()
    add_variant(
        conn, register_variant_id=20, register_id=1, slug="foretag", name="Företag"
    )
    _add_alias_in_variant(conn, "dispink", "OrgInk20", 20)
    return conn


def _representation_rows(conn: sqlite3.Connection) -> list[tuple]:
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT predecessor_variable, predecessor_column, successor_variable, "
            "successor_column, effective_year, note, beskrivning "
            "FROM representation_replaced_by "
            "ORDER BY predecessor_column, successor_column"
        )
    ]


def _representation_variant_rows(conn: sqlite3.Connection) -> list[tuple]:
    """#846: like `_representation_rows` but surfacing the `variant` scope +
    `effective_year`, for the variant-scoped succession tests."""
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT predecessor_column, successor_column, variant, effective_year "
            "FROM representation_replaced_by "
            "ORDER BY effective_year, predecessor_column"
        )
    ]


def _representation_db() -> sqlite3.Connection:
    """scb/lisa with one variable `dispink` (delivery column `Kon` from the
    default fixture, renamed to a `dispink`-shaped slug) carrying TWO observed
    delivery columns `DispInk04` + `DispInk10` — the within-variable column rename
    a representation edge expresses."""
    conn = build_slugged_db(
        classification=None,
        variable=("Disponibel inkomst", 70, 1070, "DispInk04"),
        variable_slug="dispink",
    )
    _add_alias(conn, "dispink", "DispInk10")
    return conn


def _representation_db_two_variables() -> sqlite3.Connection:
    """`_representation_db()` plus a SECOND live scb/lisa variable `sysink`
    carrying its own observed delivery column `SysInk10` — so a representation
    edge can succeed BETWEEN two different variables (a column moved across the
    variable boundary), not only within one."""
    conn = _representation_db()
    add_variable(conn, register_id=1, var_id=71, name="System inkomst", slug="sysink")
    _add_alias(conn, "sysink", "SysInk10")
    return conn


class TestRejectNonmonotoneRepresentationCycles:
    """#846: the pure, DB-free year-aware cycle checker. A representation cycle is
    permitted iff it is a single time-monotone round-trip (distinct, present years,
    one wrap); a non-cycle graph passes; a missing-year / same-year / multi-wrap
    cycle is rejected (EXIT_CONFIG, `replaced_by_cycle`)."""

    def test_empty_and_acyclic_pass(self) -> None:
        # No edges, and a plain chain A→B→C, both pass.
        reject_nonmonotone_representation_cycles([])
        reject_nonmonotone_representation_cycles([("A", "B", 2010), ("B", "C", 2014)])

    def test_distinct_year_two_cycle_allowed(self) -> None:
        reject_nonmonotone_representation_cycles([("A", "B", 2014), ("B", "A", 2018)])

    def test_missing_year_cycle_rejected(self) -> None:
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [("A", "B", None), ("B", "A", 2018)]
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_cycle"

    def test_same_year_cycle_rejected(self) -> None:
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [("A", "B", 2014), ("B", "A", 2014)]
            )
        assert exc.value.code == "replaced_by_cycle"

    def test_monotone_three_cycle_allowed(self) -> None:
        # A→B→C→A with strictly increasing years (one wrap at the close) is a
        # faithful round-trip.
        reject_nonmonotone_representation_cycles(
            [("A", "B", 2010), ("B", "C", 2014), ("C", "A", 2018)]
        )

    def test_nonmonotone_three_cycle_rejected(self) -> None:
        # Distinct years that do NOT form a single monotone wrap (rotating to the
        # min year still has a mid-cycle descent) → an impossible multi-wrap order.
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [("A", "B", 2010), ("B", "C", 2018), ("C", "A", 2014)]
            )
        assert exc.value.code == "replaced_by_cycle"

    def test_self_loop_rejected(self) -> None:
        # A column can't succeed itself: a self-loop A→A is a cycle and must be
        # rejected even with a present year.
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles([("A", "A", 2010)])
        assert exc.value.code == "replaced_by_cycle"

    def test_cycle_into_finished_node_rejected(self) -> None:
        # Codex's regression (#846): the white/gray/black DFS this replaced only
        # validated a cycle on a GRAY (on-stack) back-edge, so a NON-monotone cycle
        # reaching an already-FINISHED node fell through unchecked. Here a permitted
        # monotone 2-cycle A↔C (2010/2020) and a non-monotone 3-cycle A→B→C→A share
        # the node C; once the short cycle finishes C, the DFS would never validate
        # the long one. SCC validation sees ONE tangled component {A,B,C} and
        # rejects it (more than a single elementary loop), so the gap is closed.
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [
                    ("A", "C", 2010),
                    ("C", "A", 2020),
                    ("A", "B", 2010),
                    ("B", "C", 2005),
                    ("C", "A", 2020),
                ]
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_cycle"

    def test_two_interleaved_cycles_rejected(self) -> None:
        # Two simple monotone cycles sharing nodes form one SCC that is NOT a single
        # elementary loop (a node has two in-component successors) → rejected, even
        # though each cycle in isolation would be a clean round-trip.
        with pytest.raises(RegMetaError) as exc:
            reject_nonmonotone_representation_cycles(
                [
                    ("A", "B", 2010),
                    ("B", "A", 2012),
                    ("B", "C", 2014),
                    ("C", "B", 2016),
                ]
            )
        assert exc.value.code == "replaced_by_cycle"

    def test_two_disjoint_monotone_cycles_allowed(self) -> None:
        # Two INDEPENDENT variant-scoped round-trips (disjoint node sets) are each a
        # single elementary monotone loop → both permitted. Confirms SCC validation
        # is per-component, not global.
        reject_nonmonotone_representation_cycles(
            [
                ("A", "B", 2010),
                ("B", "A", 2014),
                ("C", "D", 2011),
                ("D", "C", 2015),
            ]
        )


class TestRepresentationReplacedByMaterialize:
    """#843: curated representation-grain `replaced_by` edges land in
    `representation_replaced_by` — a `(variable, delivery_column)`-pair succession
    (a column-level era rename the variable grain can't express). Curated-only,
    ALL-LIVE (both endpoints' observed columns must exist)."""

    def test_one_edge_writes_with_provenance(self) -> None:
        conn = _representation_db()
        out = materialize_curated_replaced_by(
            conn,
            [
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "DispInk04",
                    "DispInk10",
                    year=2010,
                    note="recut to net-of-tax",
                )
            ],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["representation"] == 1
        # The row's own `note` lands in `beskrivning`; `note` carries the fixed
        # provenance marker (mirrors the register/variable arms).
        assert _representation_rows(conn) == [
            (
                "dispink",
                "DispInk04",
                "dispink",
                "DispInk10",
                2010,
                "curated:slug_toml",
                "recut to net-of-tax",
            ),
        ]

    def test_empty_returns_full_dict_shape(self) -> None:
        # No edges → the early-return dict carries every key (incl. representation).
        out = materialize_curated_replaced_by(
            _representation_db(),
            [],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out == {
            "register": 0,
            "variable": 0,
            "classification": 0,
            "representation": 0,
            "skipped_duplicate": 0,
            "skipped_inactive_provider": 0,
        }

    def test_unknown_column_fails_fast(self) -> None:
        conn = _representation_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [
                    _representation_edge(
                        "scb/lisa/dispink",
                        "scb/lisa/dispink",
                        "DispInk04",
                        "Ghost",  # not an observed delivery column
                    )
                ],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_unresolved_representation"
        assert _representation_rows(conn) == []  # nothing written

    def test_unknown_variable_fails_fast(self) -> None:
        conn = _representation_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [
                    _representation_edge(
                        "scb/lisa/ghostvar",
                        "scb/lisa/dispink",
                        "DispInk04",
                        "DispInk10",
                    )
                ],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.code == "replaced_by_unresolved_representation"
        assert _representation_rows(conn) == []

    def test_inactive_provider_skipped(self) -> None:
        # Successor provider not in the (partial) build → SKIPPED, not failed.
        conn = _representation_db()
        out = materialize_curated_replaced_by(
            conn,
            [
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "DispInk04",
                    "DispInk10",
                )
            ],
            set(),
            set(),
            providers=frozenset({"sos"}),  # scb not present
            progress=_noop,
        )
        assert out["representation"] == 0
        assert out["skipped_inactive_provider"] == 1
        assert _representation_rows(conn) == []

    def test_missing_year_cycle_rejected(self) -> None:
        # #846: A→B plus B→A on a VARIABLE-level (variant='') representation graph
        # close a 2-cycle. With NO `effective_year` the round-trip ordering is
        # undefined, so the time-aware checker rejects it (both endpoints resolve,
        # so it's the cycle check, not the unresolved guard).
        conn = _representation_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [
                    _representation_edge(
                        "scb/lisa/dispink",
                        "scb/lisa/dispink",
                        "DispInk04",
                        "DispInk10",
                    ),
                    _representation_edge(
                        "scb/lisa/dispink",
                        "scb/lisa/dispink",
                        "DispInk10",
                        "DispInk04",
                    ),
                ],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_cycle"
        assert _representation_rows(conn) == []  # aborts before INSERT

    def test_same_year_cycle_rejected(self) -> None:
        # #846: a variant-scoped 2-cycle whose edges SHARE an effective_year is an
        # ambiguous / impossible round-trip → rejected even with the year-aware
        # checker.
        conn = _representation_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [
                    _representation_edge(
                        "scb/lisa/dispink",
                        "scb/lisa/dispink",
                        "DispInk04",
                        "DispInk10",
                        year=2014,
                        variant="individer-15plus",
                    ),
                    _representation_edge(
                        "scb/lisa/dispink",
                        "scb/lisa/dispink",
                        "DispInk10",
                        "DispInk04",
                        year=2014,
                        variant="individer-15plus",
                    ),
                ],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_cycle"
        assert _representation_rows(conn) == []

    def test_variant_distinct_year_round_trip_allowed(self) -> None:
        # #846: the FRIDA shape — a variant-scoped 2-cycle with DISTINCT years is a
        # faithful time-monotone round-trip (a column left and later returned within
        # one variant). It is PERMITTED and both edges land.
        conn = _representation_db()
        out = materialize_curated_replaced_by(
            conn,
            [
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "DispInk04",
                    "DispInk10",
                    year=2014,
                    variant="individer-15plus",
                ),
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "DispInk10",
                    "DispInk04",
                    year=2018,
                    variant="individer-15plus",
                ),
            ],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["representation"] == 2
        assert _representation_variant_rows(conn) == [
            ("DispInk04", "DispInk10", "individer-15plus", 2014),
            ("DispInk10", "DispInk04", "individer-15plus", 2018),
        ]

    def test_variant_scope_distinguishes_node_from_variable_level(self) -> None:
        # #846: a variant-scoped node `(...,'individer-15plus')` is DISTINCT from the
        # variable-level node `(...,'')`, so a variable-level A→B PLUS a
        # variant-scoped B→A do NOT close a cycle (different node spaces). Both land.
        conn = _representation_db()
        out = materialize_curated_replaced_by(
            conn,
            [
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "DispInk04",
                    "DispInk10",
                ),  # variable-level (variant='')
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "DispInk10",
                    "DispInk04",
                    year=2018,
                    variant="individer-15plus",
                ),
            ],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["representation"] == 2

    def test_variant_unresolved_fails_fast(self) -> None:
        # #846: a `variant` slug that names no live register_variant of the edge's
        # register fails fast (EXIT_CONFIG), mirroring an unresolved endpoint.
        conn = _representation_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [
                    _representation_edge(
                        "scb/lisa/dispink",
                        "scb/lisa/dispink",
                        "DispInk04",
                        "DispInk10",
                        year=2014,
                        variant="ghost-variant",
                    )
                ],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_unresolved_variant"
        assert _representation_rows(conn) == []

    def test_variant_endpoint_live_in_scoped_variant_passes(self) -> None:
        # #846 FIX 1: a `foretag`-scoped 2-cycle whose endpoints (`OrgInk20`) ARE
        # observed in that variant is the genuine shape — it resolves and lands. The
        # variant slug also resolves (a live register_variant). Distinct years keep
        # it a permitted round-trip.
        conn = _representation_db_two_variants()
        _add_alias_in_variant(conn, "dispink", "OrgInk25", 20)
        out = materialize_curated_replaced_by(
            conn,
            [
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "OrgInk20",
                    "OrgInk25",
                    year=2020,
                    variant="foretag",
                ),
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "OrgInk25",
                    "OrgInk20",
                    year=2025,
                    variant="foretag",
                ),
            ],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["representation"] == 2

    def test_variant_endpoint_live_only_in_sibling_variant_fails(self) -> None:
        # #846 FIX 1: a `foretag`-scoped edge whose endpoint column `DispInk04` is
        # live ONLY in the SIBLING variant `individer-15plus` (not in `foretag`)
        # fails fast. Register-wide liveness alone would WRONGLY pass it — the
        # variant-aware check is what catches the mistyped variant.
        conn = _representation_db_two_variants()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [
                    _representation_edge(
                        "scb/lisa/dispink",
                        "scb/lisa/dispink",
                        "DispInk04",  # observed only in individer-15plus, not foretag
                        "OrgInk20",
                        year=2020,
                        variant="foretag",
                    )
                ],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_unresolved_representation"
        assert "foretag" in exc.value.message  # the message names the scoped variant
        assert _representation_rows(conn) == []

    def test_unscoped_round_trip_rejected(self) -> None:
        # #846 FIX 2: an UNSCOPED (variant='') A→B (2014) + B→A (2018) round-trip is
        # the VARIABLE-level grain, which must be strictly ACYCLIC — distinct years
        # do NOT make it a permitted round-trip (only a variant-scoped cycle may be).
        # Both endpoints resolve, so this is the cycle partition, not the unresolved
        # guard.
        conn = _representation_db()
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_replaced_by(
                conn,
                [
                    _representation_edge(
                        "scb/lisa/dispink",
                        "scb/lisa/dispink",
                        "DispInk04",
                        "DispInk10",
                        year=2014,  # distinct years, but variable-level
                    ),
                    _representation_edge(
                        "scb/lisa/dispink",
                        "scb/lisa/dispink",
                        "DispInk10",
                        "DispInk04",
                        year=2018,
                    ),
                ],
                set(),
                set(),
                providers=_SCB,
                progress=_noop,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "replaced_by_cycle"
        assert _representation_rows(conn) == []  # aborts before INSERT

    def test_duplicate_pending_deduped(self) -> None:
        # Two identical representation edges → one row, one skip.
        conn = _representation_db()
        edge = _representation_edge(
            "scb/lisa/dispink", "scb/lisa/dispink", "DispInk04", "DispInk10"
        )
        out = materialize_curated_replaced_by(
            conn, [edge, edge], set(), set(), providers=_SCB, progress=_noop
        )
        assert out["representation"] == 1
        assert out["skipped_duplicate"] == 1
        assert len(_representation_rows(conn)) == 1

    def test_cross_variable_edge_writes(self) -> None:
        # A column moved across the VARIABLE boundary: both endpoints are distinct
        # live variables each observing the named delivery column. The row records
        # different predecessor/successor variables.
        conn = _representation_db_two_variables()
        out = materialize_curated_replaced_by(
            conn,
            [
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/sysink",
                    "DispInk10",
                    "SysInk10",
                    year=2010,
                )
            ],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["representation"] == 1
        rows = _representation_rows(conn)
        assert len(rows) == 1
        pred_var, _pred_col, succ_var, _succ_col = rows[0][:4]
        assert pred_var == "dispink"
        assert succ_var == "sysink"
        assert pred_var != succ_var

    def test_representation_column_case_insensitive_resolves(self) -> None:
        # The observed headers are `DispInk04`/`DispInk10`, but the edge is authored
        # with lowercased columns `dispink04`/`dispink10` (SCB headers drift in
        # case). Matching is case-INSENSITIVE so the edge still resolves; the STORED
        # columns are the curator's VERBATIM TOML values (match-lower, store-verbatim).
        conn = _representation_db()
        out = materialize_curated_replaced_by(
            conn,
            [
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "dispink04",
                    "dispink10",
                )
            ],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["representation"] == 1
        rows = _representation_rows(conn)
        assert len(rows) == 1
        _pred_var, pred_col, _succ_var, succ_col = rows[0][:4]
        assert pred_col == "dispink04"  # verbatim TOML value, not the header case
        assert succ_col == "dispink10"

    def test_representation_non_ascii_column_resolves(self) -> None:
        # #843 regression: a Swedish-header column with an uppercase åäö (e.g.
        # `Ägare`, common in SCB headers). The live set must fold the observed
        # column the SAME way as the materializer's edge keys — Python `str.lower`
        # (Unicode-aware), NOT SQLite `LOWER()` (ASCII-only, leaves `Ä` unchanged).
        # With the ASCII fold the edge column `ägare` would never match the observed
        # `Ägare` and the edge would falsely raise unresolved_representation.
        conn = _representation_db()
        _add_alias(conn, "dispink", "Ägare")
        out = materialize_curated_replaced_by(
            conn,
            [
                _representation_edge(
                    "scb/lisa/dispink",
                    "scb/lisa/dispink",
                    "ägare",  # Unicode-lowercased; observed header is `Ägare`
                    "DispInk10",
                )
            ],
            set(),
            set(),
            providers=_SCB,
            progress=_noop,
        )
        assert out["representation"] == 1
        rows = _representation_rows(conn)
        assert len(rows) == 1
        assert rows[0][1] == "ägare"  # verbatim TOML value


# ---------------------------------------------------------------------------
# The three real edges moved into the repo file
# ---------------------------------------------------------------------------


class TestMovedEdges:
    def test_repo_file_carries_the_moved_edges(self) -> None:
        rel = load_relations(_ROOT / "curation" / "relations.toml")
        # 11 variable replaced_by (the #375 LISA succession chain) + 21 #931
        # LISA SNI-coding succession edges + 2 variable replaced_by (the #400
        # SSYK 96 → SSYK 2012 J16 succession) + 3
        # classification replaced_by (the #579 sun1996 → niva/inriktning/grupp
        # split) + 3 #770/#768 ICD/KS disease-classification succession edges + 7
        # #814 iot disponibel-inkomst 2004-års-definition succession edges + 1
        # #875 KSju lgrp → NgGr1 representation-grain succession edge + 1
        # #846 RTB PNR → PersonNr representation-grain rename edge + 2 #846 FRIDA
        # firm-key variant-scoped gap-fill round-trip edges.
        assert len(rel.replaced_by) == 51
        # #508 (615) + #737 (232) = 847 curated same_as identity edges; all
        # variable-grain with a non-empty note; max connected component stays
        # ≤32 FQIDs.
        assert len(rel.same_as) == 847
        assert all(
            e.grain is FqidKind.VARIABLE_BINDING
            and e.a_variable
            and e.b_variable
            and e.note
            for e in rel.same_as
        )
        # Spot-check one moved edge of each type.
        assert ("scb/lisa/anninkf", "scb/lisa/anninkf04") in {
            (str(e.predecessor), str(e.successor)) for e in rel.replaced_by
        }
        # The #579 1→many classification split: one predecessor, three successors
        # (all three SUN 2000 dimensions), parsed as `class/<slug>` (CLASSIFICATION
        # grain).
        sun_succ = {
            str(e.successor)
            for e in rel.replaced_by
            if str(e.predecessor) == "class/sun1996"
        }
        assert sun_succ == {
            "class/sun2000-niva",
            "class/sun2000-inriktning",
            "class/sun2000-grupp",
        }
        assert all(
            e.predecessor.kind is FqidKind.CLASSIFICATION
            for e in rel.replaced_by
            if str(e.predecessor) == "class/sun1996"
        )
        icd_edge = next(
            e
            for e in rel.replaced_by
            if (str(e.predecessor), str(e.successor))
            == ("class/icd-10-se", "class/icd-11-se")
        )
        assert icd_edge.effective_year == 2027
        # The #846 RTB representation-grain rename: a variable-grain edge carrying
        # BOTH `from_column`/`to_column`, parsed onto `predecessor_column` /
        # `successor_column` (the representation arm of `replaced_by`).
        rtb = next(
            e
            for e in rel.replaced_by
            if (str(e.predecessor), str(e.successor))
            == ("scb/rtb/pnr", "scb/rtb/personnr")
        )
        assert (rtb.predecessor_column, rtb.successor_column) == ("PNR", "PersonNr")
        # The #875 KSju grouped-SNI handoff is also representation-grain, but
        # crosses sibling variables inside one register rather than columns inside
        # one variable.
        ksju = next(
            e
            for e in rel.replaced_by
            if str(e.predecessor) == "scb/ksju/naringsgren-grupperad-2009"
        )
        assert str(ksju.successor) == "scb/ksju/naringsgren"
        assert (ksju.predecessor_column, ksju.successor_column) == ("lgrp", "NgGr1")
        # The #846 FRIDA firm-key gap-fill: a variant-SCOPED representation
        # round-trip (`borgnr` → `PERSORGNR` → `borgnr`) on the
        # punktskatter-for-energi variant, time-ordered 2014 < 2018. Verifies the
        # variant arm of `replaced_by` parses end-to-end from the repo file.
        frida = [
            e
            for e in rel.replaced_by
            if e.predecessor.register == "frida" and e.variant
        ]
        assert {(e.predecessor_column, e.successor_column) for e in frida} == {
            ("borgnr", "PERSORGNR"),
            ("PERSORGNR", "borgnr"),
        }
        assert all(e.variant == "punktskatter-for-energi" for e in frida)
        assert {e.effective_year for e in frida} == {2014, 2018}


# ---------------------------------------------------------------------------
# Variable vintage succession (#584) — lift classification editions to variables
# ---------------------------------------------------------------------------


def _add_classification(conn: sqlite3.Connection, short: str, slug: str) -> int:
    """Insert a classification and return its `id`."""
    cur = conn.execute(
        "INSERT INTO classification (short_name, name, slug) VALUES (?, ?, ?)",
        (short, short, slug),
    )
    return cur.lastrowid


def _cid(conn: sqlite3.Connection, slug: str) -> int:
    """The `classification.id` for a slug (sqlite3.Connection can't carry test
    attrs, so resolve on demand)."""
    return conn.execute(
        "SELECT id FROM classification WHERE slug = ?", (slug,)
    ).fetchone()[0]


def _add_edition_edge(
    conn: sqlite3.Connection, pred: str, succ: str, year: int | None
) -> None:
    """Insert one `classification_replaced_by` edition edge (the #571 chain the
    lift consumes). `year=None` inserts a NULL `effective_year` (a curated/auto
    edition edge may carry no year — the lift passes it through verbatim)."""
    conn.execute(
        "INSERT INTO classification_replaced_by "
        "(predecessor_slug, successor_slug, effective_year, note) "
        "VALUES (?, ?, ?, 'derived:vintage_chain')",
        (pred, succ, year),
    )


def _lift_rows(conn: sqlite3.Connection) -> list[tuple]:
    """Derived vintage-lift edges, ordered, as (pred_var, succ_var, year)."""
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT predecessor_variable, successor_variable, effective_year "
            "FROM variable_replaced_by "
            "WHERE note = 'derived:classification_vintage_lift' "
            "ORDER BY predecessor_variable, successor_variable"
        )
    ]


def _vintage_db() -> sqlite3.Connection:
    """scb/lisa with a single variant + two chained classification editions
    (sni2002 → sni2007, effective 2007). No variables/states yet — each test
    seeds its own family shape. `classification=None` so the only classifications
    are the two editions (the default fixture's SUN2020 would just be inert, but
    keeping the table minimal makes the chain explicit)."""
    conn = build_slugged_db(
        register=None, variant=None, version=None, variable=None, classification=None
    )
    add_register(conn, register_id=1, slug="lisa", name="LISA")
    add_variant(
        conn, register_variant_id=10, register_id=1, slug="ind", name="Individer"
    )
    _add_classification(conn, "SNI2002", "sni2002")
    _add_classification(conn, "SNI2007", "sni2007")
    _add_edition_edge(conn, "sni2002", "sni2007", 2007)
    conn.commit()
    return conn


class TestVariableVintageSuccession:
    def test_clean_pair_mints_one_edge(self) -> None:
        # Two DISTINCT variables, same name, one per edition → a bijection.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-2002")
        add_variable(conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-2007")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 1
        assert _lift_rows(conn) == [("sni-2002", "sni-2007", 2007)]

    def test_three_edition_chain_mints_adjacent_edges(self) -> None:
        # Adjacent-chain (NOT predecessor→latest): a 3-edition family → 2 edges.
        conn = _vintage_db()
        _add_classification(conn, "SNI2012", "sni2012")
        _add_edition_edge(conn, "sni2007", "sni2012", 2012)
        for vid, slug, cls in (
            (1, "sni-2002", "sni2002"),
            (2, "sni-2007", "sni2007"),
            (3, "sni-2012", "sni2012"),
        ):
            cid = _cid(conn, cls)
            add_variable(conn, register_id=1, var_id=vid, name="Näringsgren", slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=cid,
            )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 2
        # Adjacent hops only — no 2002→2012 star edge.
        assert _lift_rows(conn) == [
            ("sni-2002", "sni-2007", 2007),
            ("sni-2007", "sni-2012", 2012),
        ]

    def test_entangled_parent_streams_mint_parallel_edges(self) -> None:
        # Two variables BOTH bind sni2002 (and two more bind sni2007): an edition
        # bound by >1 variable in the family. #592 partitions the same-name
        # family by slug stream after stripping the classification vintage token,
        # so fars-* links only to fars-* and mors-* only to mors-*.
        conn = _vintage_db()
        for vid, slug, cls in (
            (1, "fars-sni-2002", "sni2002"),
            (2, "mors-sni-2002", "sni2002"),
            (3, "fars-sni-2007", "sni2007"),
            (4, "mors-sni-2007", "sni2007"),
        ):
            add_variable(
                conn,
                register_id=1,
                var_id=vid,
                name="Föräldrars näringsgren",
                slug=slug,
            )
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=_cid(conn, cls),
            )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 2
        assert _lift_rows(conn) == [
            ("fars-sni-2002", "fars-sni-2007", 2007),
            ("mors-sni-2002", "mors-sni-2007", 2007),
        ]

    def test_entangled_population_streams_do_not_cross_link(self) -> None:
        # Same classification edge, same variable.name, parallel population
        # streams. Stripping only the vintage years leaves the population token in
        # the stream key, so no individ→foretag cross-link is possible.
        conn = _vintage_db()
        for vid, slug, cls in (
            (1, "individ-sni-2002", "sni2002"),
            (2, "foretag-sni-2002", "sni2002"),
            (3, "individ-sni-2007", "sni2007"),
            (4, "foretag-sni-2007", "sni2007"),
        ):
            add_variable(conn, register_id=1, var_id=vid, name="Näringsgren", slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=_cid(conn, cls),
            )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 2
        assert _lift_rows(conn) == [
            ("foretag-sni-2002", "foretag-sni-2007", 2007),
            ("individ-sni-2002", "individ-sni-2007", 2007),
        ]

    def test_entangled_ambiguous_stream_is_skipped(self) -> None:
        # Two predecessor variables collapse to the same non-vintage stream key.
        # The lift refuses to choose one and skips that stream rather than minting
        # a false cross-product edge.
        conn = _vintage_db()
        for vid, slug, cls in (
            (1, "fars-sni-2002", "sni2002"),
            (2, "fars-sni-2002-2002", "sni2002"),
            (3, "fars-sni-2007", "sni2007"),
        ):
            add_variable(
                conn,
                register_id=1,
                var_id=vid,
                name="Föräldrars näringsgren",
                slug=slug,
            )
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=_cid(conn, cls),
            )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 0
        assert _lift_rows(conn) == []

    def test_interval_native_variable_mints_nothing(self) -> None:
        # ONE variable spanning BOTH editions across its own two states (the #271
        # interval-native case) already carries the lineage in one variable_id →
        # no lift. The variable appears under >1 edition, breaking the bijection.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni",
            register_variant_id=10,
            valid_from="2002-01-01",
            valid_to="2006-12-31",
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni",
            register_variant_id=10,
            valid_from="2007-01-01",
            valid_to="9999-12-31",
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 0
        assert _lift_rows(conn) == []

    def test_existing_curated_edge_wins_no_duplicate(self) -> None:
        # A pre-existing edge on the same PK (curated #375/#440 or auto
        # timeseries_event) WINS — the lift's INSERT OR IGNORE leaves it untouched
        # and mints no derived duplicate. Same clean family as the first test.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-2002")
        add_variable(conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-2007")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        # Pre-seed the SAME PK with a curated row (richer provenance).
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, "
            "effective_year, note, beskrivning) "
            "VALUES ('scb','lisa','sni-2002','scb','lisa','sni-2007', "
            "2007, 'curated:slug_toml', 'hand reason')"
        )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 0  # the derived row collapsed onto the curated PK
        # Exactly ONE row on that PK, and it kept the curated note + beskrivning.
        rows = conn.execute(
            "SELECT note, beskrivning FROM variable_replaced_by "
            "WHERE predecessor_variable = 'sni-2002' "
            "AND successor_variable = 'sni-2007'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "curated:slug_toml"
        assert rows[0][1] == "hand reason"

    def test_reversed_pre_existing_edge_closes_cycle_raises(self) -> None:
        # The lift is the one writer that inserts AFTER curated/auto, so it must
        # re-check the COMBINED graph: a pre-existing REVERSED edge sni-2007 ->
        # sni-2002 plus the lift's chain-direction sni-2002 -> sni-2007 closes a
        # 2-cycle. The earlier passes couldn't see it (the lift edge didn't exist
        # yet), so the lift's own post-insert full-graph cycle check must fail the
        # build loudly. Same clean family as the first test.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-2002")
        add_variable(conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-2007")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        # Pre-seed the REVERSED edge (successor -> predecessor of the lift edge).
        conn.execute(
            "INSERT INTO variable_replaced_by ("
            "predecessor_provider, predecessor_register, predecessor_variable, "
            "successor_provider, successor_register, successor_variable, "
            "effective_year, note) "
            "VALUES ('scb','lisa','sni-2007','scb','lisa','sni-2002', "
            "2002, 'curated:slug_toml')"
        )
        conn.commit()
        with pytest.raises(RegMetaError) as exc:
            derive_variable_vintage_succession(conn)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_distinct_levels_do_not_cross_link(self) -> None:
        # Two LEVELS of the same series each bind their own classification lineage
        # (sni2007-grov ≠ sni2007-utokad). The lift over distinct slugs isolates
        # each level's chain — grov never links into utokad — with NO special
        # level handling. Two clean families → two independent edges.
        conn = _vintage_db()  # has sni2002→sni2007 (treat as the "grov" lineage)
        cid_ug_2002 = _add_classification(conn, "SNI2002-UTOKAD", "sni2002-utokad")
        cid_ug_2007 = _add_classification(conn, "SNI2007-UTOKAD", "sni2007-utokad")
        _add_edition_edge(conn, "sni2002-utokad", "sni2007-utokad", 2007)
        # grov family
        add_variable(
            conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-grov-2002"
        )
        add_variable(
            conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-grov-2007"
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-grov-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-grov-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        # utokad family — DIFFERENT name so it's a distinct family key (a real
        # level split carries a distinct name/slug; the slug-chain isolation here
        # is what the test asserts: utokad rides its own classification slugs).
        add_variable(
            conn, register_id=1, var_id=3, name="Näringsgren utökad", slug="sni-ut-2002"
        )
        add_variable(
            conn, register_id=1, var_id=4, name="Näringsgren utökad", slug="sni-ut-2007"
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-ut-2002",
            register_variant_id=10,
            classification_id=cid_ug_2002,
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-ut-2007",
            register_variant_id=10,
            classification_id=cid_ug_2007,
        )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 2
        assert _lift_rows(conn) == [
            ("sni-grov-2002", "sni-grov-2007", 2007),
            ("sni-ut-2002", "sni-ut-2007", 2007),
        ]

    def test_same_name_levels_isolate_by_slug_chain(self) -> None:
        # The STRONG isolation case: two levels share the IDENTICAL family key
        # (register=1, name="Näringsgren"), differing ONLY by classification slug
        # — grov rides sni2002/sni2007, utokad rides sni2002-utokad/sni2007-utokad.
        # Each edition still binds exactly ONE variable, so the bijection spans all
        # FOUR editions cleanly and the slug-chain isolation mints grov→grov and
        # utokad→utokad with NO grov↔utokad cross-link, even with no name signal.
        conn = _vintage_db()  # has sni2002→sni2007 (the "grov" lineage)
        cid_ug_2002 = _add_classification(conn, "SNI2002-UTOKAD", "sni2002-utokad")
        cid_ug_2007 = _add_classification(conn, "SNI2007-UTOKAD", "sni2007-utokad")
        _add_edition_edge(conn, "sni2002-utokad", "sni2007-utokad", 2007)
        for vid, slug, cid in (
            (1, "sni-grov-2002", _cid(conn, "sni2002")),
            (2, "sni-grov-2007", _cid(conn, "sni2007")),
            (3, "sni-ut-2002", cid_ug_2002),
            (4, "sni-ut-2007", cid_ug_2007),
        ):
            add_variable(conn, register_id=1, var_id=vid, name="Näringsgren", slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=cid,
            )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 2
        assert _lift_rows(conn) == [
            ("sni-grov-2002", "sni-grov-2007", 2007),
            ("sni-ut-2002", "sni-ut-2007", 2007),
        ]

    def test_gapped_chain_mints_nothing_across_gap(self) -> None:
        # Documents the no-transitive-link guarantee: a 3-edition chain
        # sni2002→sni2007→sni2012 with variables seeded ONLY for the two ENDS
        # (no variable binds the intermediate sni2007). Each adjacent edge needs
        # BOTH endpoints bound, so the missing middle breaks both hops and no
        # transitive sni2002→sni2012 edge is invented across the gap.
        conn = _vintage_db()
        _add_classification(conn, "SNI2012", "sni2012")
        _add_edition_edge(conn, "sni2007", "sni2012", 2012)
        for vid, slug, cls in (
            (1, "sni-2002", "sni2002"),
            (3, "sni-2012", "sni2012"),
        ):
            add_variable(conn, register_id=1, var_id=vid, name="Näringsgren", slug=slug)
            add_state(
                conn,
                register_id=1,
                variable_slug=slug,
                register_variant_id=10,
                classification_id=_cid(conn, cls),
            )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 0
        assert _lift_rows(conn) == []

    def test_null_effective_year_passes_through(self) -> None:
        # NULL pass-through is intended: a curated/auto edition edge may carry no
        # year, and the lift writes the edge's `effective_year` verbatim — so a
        # NULL-year edition mints a NULL-year variable edge (not a dropped one).
        conn = build_slugged_db(
            register=None,
            variant=None,
            version=None,
            variable=None,
            classification=None,
        )
        add_register(conn, register_id=1, slug="lisa", name="LISA")
        add_variant(
            conn, register_variant_id=10, register_id=1, slug="ind", name="Individer"
        )
        _add_classification(conn, "SNI2002", "sni2002")
        _add_classification(conn, "SNI2007", "sni2007")
        _add_edition_edge(conn, "sni2002", "sni2007", None)  # NULL effective_year
        add_variable(conn, register_id=1, var_id=1, name="Näringsgren", slug="sni-2002")
        add_variable(conn, register_id=1, var_id=2, name="Näringsgren", slug="sni-2007")
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2002",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="sni-2007",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = derive_variable_vintage_succession(conn)
        assert n == 1
        row = conn.execute(
            "SELECT predecessor_variable, successor_variable, effective_year "
            "FROM variable_replaced_by "
            "WHERE note = 'derived:classification_vintage_lift'"
        ).fetchone()
        assert (row[0], row[1]) == ("sni-2002", "sni-2007")
        assert row[2] is None
