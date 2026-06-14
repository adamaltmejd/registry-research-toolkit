"""Tests for the curated cross-register "see also" loader (#353;
`variable_related_to.py`).

Two layers, mirroring `test_concept_groups.py`:
  - `TestLoader` — the loader's good parse + every load-time failure mode (FQID
    arity, the curated relation-kind vocabulary incl. the explicit rejection of
    the auto:triage kind, self-edges, duplicate unordered pairs, note shape).
  - `TestMaterialize` — DB-backed (`_slugged_db`): a curated cross-register edge
    materializes BOTH directions with the curated note; a dangling endpoint
    fails fast; an inactive-provider edge is skipped; a collision with an
    existing edge fails loud; and the curated kind is NOT folded by the
    concept-group edge pass (it is non-foldable by construction).

Fully synthetic (CLAUDE.md): the shipped `variable_related_to.toml` is empty, so
it never touches these fixtures."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import add_register, add_variable, build_slugged_db
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.concept_groups import materialize_concept_groups
from reg_meta_build.variable_related_to import (
    CURATED_RELATION_KINDS,
    CuratedRelatedTo,
    load_related_to,
    materialize_curated_related_to,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

_SCB = frozenset({"scb"})
_AUTO_KIND = "same_definition_different_column"


def test_auto_kind_is_not_a_curated_kind() -> None:
    """Vocabulary distinctness: the foldable auto:triage kind must never be a
    curated kind (a curated 'see also' must never fold)."""
    assert _AUTO_KIND not in CURATED_RELATION_KINDS
    assert "similar_concept" in CURATED_RELATION_KINDS


class TestLoader:
    @staticmethod
    def _load(tmp_path: Path, text: str) -> tuple[CuratedRelatedTo, ...]:
        path = tmp_path / "variable_related_to.toml"
        path.write_text(text, encoding="utf-8")
        return load_related_to(path)

    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        assert load_related_to(None) == ()
        assert load_related_to(tmp_path / "absent.toml") == ()

    def test_present_but_empty_file_is_empty(self, tmp_path: Path) -> None:
        assert self._load(tmp_path, "# no edges yet\n") == ()

    def test_parses_valid_edge(self, tmp_path: Path) -> None:
        edges = self._load(
            tmp_path,
            """
            [[related]]
            a = "scb/lisa/inkomst"
            b = "scb/rams/inkomst"
            relation_kind = "similar_concept"
            note = "curated:cross_register"
            """,
        )
        assert len(edges) == 1
        e = edges[0]
        assert (e.a_provider, e.a_register, e.a_variable) == ("scb", "lisa", "inkomst")
        assert (e.b_provider, e.b_register, e.b_variable) == ("scb", "rams", "inkomst")
        assert e.relation_kind == "similar_concept"
        assert e.note == "curated:cross_register"

    def test_note_is_optional(self, tmp_path: Path) -> None:
        edges = self._load(
            tmp_path,
            '[[related]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n'
            'relation_kind = "similar_concept"\n',
        )
        assert edges[0].note is None

    def test_malformed_toml_is_config_error(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "[[related]]\na = = 1\n")
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_related_to_toml_unreadable"

    def test_misspelled_toplevel_key_rejected(self, tmp_path: Path) -> None:
        # `[[relateds]]` (typo) would silently disable ALL curation → loud error.
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[relateds]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n'
                'relation_kind = "similar_concept"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_related_to_invalid"
        assert "relateds" in exc.value.message

    def test_scalar_related_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "related = 5\n")
        assert exc.value.code == "variable_related_to_invalid"

    def test_non_table_entry_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, "related = [1, 2]\n")
        assert exc.value.code == "variable_related_to_invalid"

    @pytest.mark.parametrize(
        "fqid",
        [
            "scb",  # 1-segment
            "scb/lisa",  # 2-segment
            "scb/lisa/x/y",  # 4-segment
            "scb//x",  # empty middle segment
            "",  # empty
        ],
    )
    def test_bad_fqid_arity_rejected(self, tmp_path: Path, fqid: str) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                f'[[related]]\na = "{fqid}"\nb = "scb/rams/y"\n'
                'relation_kind = "similar_concept"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_related_to_invalid"

    def test_missing_relation_kind_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(tmp_path, '[[related]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n')
        assert exc.value.code == "variable_related_to_invalid"

    def test_unknown_relation_kind_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[related]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n'
                'relation_kind = "made_up_kind"\n',
            )
        assert exc.value.code == "variable_related_to_invalid"

    def test_auto_foldable_kind_rejected(self, tmp_path: Path) -> None:
        # The explicit distinctness guard: the auto:triage kind is foldable, so a
        # curated edge naming it is rejected at load (proves curated ⊥ auto).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[related]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n'
                f'relation_kind = "{_AUTO_KIND}"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_related_to_invalid"

    def test_self_edge_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[related]]\na = "scb/lisa/x"\nb = "scb/lisa/x"\n'
                'relation_kind = "similar_concept"\n',
            )
        assert exc.value.code == "variable_related_to_invalid"

    def test_duplicate_unordered_pair_rejected(self, tmp_path: Path) -> None:
        # The same pair in REVERSED a/b order is still a duplicate (symmetric).
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[related]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n'
                'relation_kind = "similar_concept"\n\n'
                '[[related]]\na = "scb/rams/y"\nb = "scb/lisa/x"\n'
                'relation_kind = "similar_concept"\n',
            )
        assert exc.value.code == "variable_related_to_invalid"

    def test_empty_note_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            self._load(
                tmp_path,
                '[[related]]\na = "scb/lisa/x"\nb = "scb/rams/y"\n'
                'relation_kind = "similar_concept"\nnote = ""\n',
            )
        assert exc.value.code == "variable_related_to_invalid"


def _cross_register_db() -> sqlite3.Connection:
    """scb/lisa/<v1> + scb/rams/<v2>, both resolvable, no edges yet."""
    conn = build_slugged_db(classification=None)  # scb/lisa with `kon`
    add_register(conn, register_id=2, slug="rams", name="RAMS")
    add_variable(conn, register_id=1, var_id=900, name="Inkomst", slug="inkomst")
    add_variable(conn, register_id=2, var_id=901, name="Inkomst", slug="rinkomst")
    return conn


def _edge(
    a: str = "scb/lisa/inkomst",
    b: str = "scb/rams/rinkomst",
    *,
    relation_kind: str = "similar_concept",
    note: str | None = "curated:cross_register",
) -> CuratedRelatedTo:
    pa = a.split("/")
    pb = b.split("/")
    return CuratedRelatedTo(
        a_provider=pa[0],
        a_register=pa[1],
        a_variable=pa[2],
        b_provider=pb[0],
        b_register=pb[1],
        b_variable=pb[2],
        relation_kind=relation_kind,
        note=note,
    )


def _related_rows(conn: sqlite3.Connection) -> list[tuple]:
    return [
        tuple(r)
        for r in conn.execute(
            "SELECT a_provider, a_register, a_variable, b_provider, b_register, "
            "b_variable, relation_kind, note FROM variable_related_to "
            "ORDER BY a_register, b_register"
        )
    ]


class TestMaterialize:
    def test_cross_register_edge_writes_both_directions(self) -> None:
        conn = _cross_register_db()
        n = materialize_curated_related_to(conn, (_edge(),), providers=_SCB)
        assert n == 2
        assert _related_rows(conn) == [
            (
                "scb",
                "lisa",
                "inkomst",
                "scb",
                "rams",
                "rinkomst",
                "similar_concept",
                "curated:cross_register",
            ),
            (
                "scb",
                "rams",
                "rinkomst",
                "scb",
                "lisa",
                "inkomst",
                "similar_concept",
                "curated:cross_register",
            ),
        ]

    def test_default_note_when_absent(self) -> None:
        conn = _cross_register_db()
        materialize_curated_related_to(conn, (_edge(note=None),), providers=_SCB)
        notes = {r[7] for r in _related_rows(conn)}
        assert notes == {"curated"}

    def test_dangling_endpoint_fails_fast(self) -> None:
        conn = _cross_register_db()
        edge = _edge(b="scb/rams/does-not-exist")
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_related_to(conn, (edge,), providers=_SCB)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_related_to_unresolved"
        assert "does-not-exist" in exc.value.message
        assert _related_rows(conn) == []  # nothing written

    def test_dangling_a_endpoint_fails_fast(self) -> None:
        # Symmetric to the b-endpoint case: the `a` FQID dangles. Guards against a
        # copy-paste bug that drops the `a` resolution check.
        conn = _cross_register_db()
        edge = _edge(a="scb/lisa/does-not-exist")
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_related_to(conn, (edge,), providers=_SCB)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_related_to_unresolved"
        assert "does-not-exist" in exc.value.message
        assert _related_rows(conn) == []  # nothing written

    def test_inactive_provider_edge_is_skipped(self) -> None:
        conn = _cross_register_db()
        # scb endpoints, but this build only carries sos → skip, don't fail.
        n = materialize_curated_related_to(
            conn, (_edge(),), providers=frozenset({"sos"})
        )
        assert n == 0
        assert _related_rows(conn) == []

    def test_collision_with_existing_edge_fails_loud(self) -> None:
        conn = _cross_register_db()
        # Pre-seed the exact a→b row the curated edge would write (e.g. an
        # auto:triage sibling) → the plain INSERT collides on the PK.
        conn.execute(
            "INSERT INTO variable_related_to (a_provider, a_register, a_variable, "
            "b_provider, b_register, b_variable, relation_kind, note) VALUES "
            "('scb', 'lisa', 'inkomst', 'scb', 'rams', 'rinkomst', "
            "'same_definition_different_column', 'auto:triage')"
        )
        with pytest.raises(RegMetaError) as exc:
            materialize_curated_related_to(conn, (_edge(),), providers=_SCB)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "variable_related_to_collision"

    def test_curated_edge_is_not_folded_by_concept_groups(self) -> None:
        # The curated kind ≠ the foldable auto kind, so the concept-group edge
        # pass must not turn a curated edge into a browse group.
        conn = _cross_register_db()
        materialize_curated_related_to(conn, (_edge(),), providers=_SCB)
        counts = materialize_concept_groups(conn, (), providers=_SCB)
        assert counts["edge_groups"] == 0
        assert counts["grouped_variables"] == 0
