"""Relation declaration loading, graph guards and common vintage succession.

Retired SQL overlay tests were removed with their implementation. Common resolved
metadata and catalog dependency tests cover endpoint validation and persistence.
"""

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
from reg_meta.fqid import FqidKind
from reg_meta_build.catalog_dependencies import resolve_variable_successions
from reg_meta_build.relations import (
    load_relations,
    reject_nonmonotone_representation_cycles,
)
from reg_meta_build.resolved_catalog import (
    ResolvedClassificationSuccession,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
)
from reg_meta_build.resolved_metadata import ResolvedMetadata, ResolvedSuccession

if TYPE_CHECKING:
    import sqlite3

_SCB = frozenset({"scb"})
# reg_meta_build/ package root (tests/ sits beside the curation/ dir).
_ROOT = Path(__file__).resolve().parent.parent


def _load(tmp_path: Path, text: str):
    path = tmp_path / "relations.toml"
    path.write_text(text, encoding="utf-8")
    return load_relations(path)


# ---------------------------------------------------------------------------
# Dispatch: the `type` discriminator + per-type foreign-field rejection
# ---------------------------------------------------------------------------


class TestLoaderDispatch:
    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        rel = load_relations(None)
        assert rel.same_as == () and rel.replaced_by == () and rel.derived_from == ()
        absent = load_relations(tmp_path / "absent.toml")
        assert absent.same_as == () and absent.derived_from == ()

    def test_present_but_empty_file_is_empty(self, tmp_path: Path) -> None:
        rel = _load(tmp_path, "# no edges yet\n")
        assert rel.same_as == () and rel.replaced_by == () and rel.derived_from == ()

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
        for legal in ("same_as", "replaced_by", "derived_from"):
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
# derived_from — load
# ---------------------------------------------------------------------------


class TestDerivedFromLoad:
    def test_parses_classification_edge(self, tmp_path: Path) -> None:
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "derived_from"\n'
            'derived = "class/ks87-p"\n'
            'source = "class/icd-9-ks87"\n'
            'note = "primary-care variant"\n',
        )
        assert len(rel.derived_from) == 1
        edge = rel.derived_from[0]
        assert edge.derived.kind is FqidKind.CLASSIFICATION
        assert edge.source.kind is FqidKind.CLASSIFICATION
        assert edge.derived.classification == "ks87-p"
        assert edge.source.classification == "icd-9-ks87"
        assert edge.note == "primary-care variant"

    def test_rejects_non_classification_endpoint(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "derived_from"\n'
                'derived = "scb/lisa/kon"\n'
                'source = "class/icd-9-ks87"\n',
            )
        assert exc.value.code == "relations_invalid"
        assert "only classification" in exc.value.message

    def test_self_link_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "derived_from"\n'
                'derived = "class/ks87-p"\n'
                'source = "class/ks87-p"\n',
            )
        assert exc.value.code == "relations_invalid"

    def test_duplicate_directional_pair_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "derived_from"\n'
                'derived = "class/ks87-p"\n'
                'source = "class/icd-9-ks87"\n\n'
                '[[edge]]\ntype = "derived_from"\n'
                'derived = "class/ks87-p"\n'
                'source = "class/icd-9-ks87"\n',
            )
        assert exc.value.code == "relations_invalid"
        assert "duplicate derived_from" in exc.value.message

    def test_foreign_field_effective_year_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "derived_from"\n'
                'derived = "class/ks87-p"\n'
                'source = "class/icd-9-ks87"\n'
                "effective_year = 1987\n",
            )
        assert exc.value.code == "relations_invalid"
        assert "effective_year" in exc.value.message


# ---------------------------------------------------------------------------
# same_as — materialize
# ---------------------------------------------------------------------------


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

    def test_parses_register_variant_endpoint_edge(self, tmp_path: Path) -> None:
        rel = _load(
            tmp_path,
            '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa"\n'
            'from_variant = "individer-16plus"\nto = "scb/lisa"\n'
            'to_variant = "individer-15plus"\neffective_year = 2010\n',
        )
        e = rel.replaced_by[0]
        assert e.predecessor.kind is FqidKind.REGISTER
        assert e.predecessor_variant == "individer-16plus"
        assert e.successor_variant == "individer-15plus"
        assert e.effective_year == 2010

    def test_variant_endpoints_are_both_or_neither(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa"\n'
                'from_variant = "individer-16plus"\nto = "scb/lisa"\n',
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "relations_invalid"

    def test_variant_endpoints_require_register_grain(self, tmp_path: Path) -> None:
        with pytest.raises(RegMetaError) as exc:
            _load(
                tmp_path,
                '[[edge]]\ntype = "replaced_by"\nfrom = "scb/lisa/a"\n'
                'from_variant = "individer-16plus"\nto = "scb/lisa/b"\n'
                'to_variant = "individer-15plus"\n',
            )
        assert exc.value.code == "relations_invalid"

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


# ---------------------------------------------------------------------------
# replaced_by — representation-grain materialize (#843)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# The real curated edges shipped in the repo file
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
        # firm-key variant-scoped gap-fill round-trip edges + 1 #376 LISA
        # register_variant succession edge + 3 #1122 LISA FÅMANS KU→AGI source
        # succession edges + 8 Y-88 curated LISA succession edges.
        assert len(rel.replaced_by) == 63
        # #508 (615) + #737 (232) = 847 curated same_as identity edges; all
        # variable-grain with a non-empty note; max connected component stays
        # ≤32 FQIDs.
        assert len(rel.same_as) == 847
        assert len(rel.derived_from) == 1
        assert (str(rel.derived_from[0].derived), str(rel.derived_from[0].source)) == (
            "class/ks87-p",
            "class/icd-9-ks87",
        )
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
        faman_edges = {
            (str(e.predecessor), str(e.successor), e.effective_year)
            for e in rel.replaced_by
            if e.predecessor.register == "lisa"
            and (e.predecessor.variable or "").endswith("faman")
        }
        assert faman_edges == {
            ("scb/lisa/ku1faman", "scb/lisa/agi1faman", 2019),
            ("scb/lisa/ku2faman", "scb/lisa/agi2faman", 2019),
            ("scb/lisa/ku3faman", "scb/lisa/agi3faman", 2019),
        }
        # Y-88's curated LISA round, read off the succession-candidates worklist
        # as `(predecessor, successor, from_column, to_column, variant, year)` —
        # a tuple that also carries the GRAIN each edge landed at. Four
        # `cross_var_id` re-mints are VARIABLE-grain (one measure recut under a
        # new SCB VarId: no columns, hence no variant scope); four
        # never-co-delivered renames are REPRESENTATION-grain, scoped to the
        # individ variant, since LISA's other variants keep delivering the
        # predecessor column.
        y88_expected = {
            (
                "scb/lisa/antal-anstallda-ku",
                "scb/lisa/antal-anstallda",
                None,
                None,
                "",
                2022,
            ),
            ("scb/lisa/forvink-aktiv", "scb/lisa/forvink", None, None, "", 2022),
            (
                "scb/lisa/forvink-ers-aktiv",
                "scb/lisa/forvink-ers",
                None,
                None,
                "",
                2022,
            ),
            ("scb/lisa/yrkesbaserad-seg", "scb/lisa/eseg", None, None, "", 2016),
            (
                "scb/lisa/raks-huvinkkalla",
                "scb/lisa/huvudsaklig-inkomstkalla",
                "Raks_HuvInkKalla",
                "HuvInkKalla",
                "individer-15plus",
                2022,
            ),
            (
                "scb/lisa/sysselsattningsstatus-november",
                "scb/lisa/syssstat19",
                "SyssStat11",
                "SyssStat19",
                "individer-15plus",
                2019,
            ),
            (
                "scb/lisa/cfar-nummer",
                "scb/lisa/cfar-nummer-2",
                "CfarNr",
                "CfarNr_LISA",
                "individer-15plus",
                2016,
            ),
            (
                "scb/lisa/person-orgnr",
                "scb/lisa/person-orgnr-2",
                "PeOrgNr",
                "PeOrgNr_LISA",
                "individer-15plus",
                2016,
            ),
        }
        y88_predecessors = {pred for pred, *_ in y88_expected}
        y88 = [e for e in rel.replaced_by if str(e.predecessor) in y88_predecessors]
        assert {
            (
                str(e.predecessor),
                str(e.successor),
                e.predecessor_column,
                e.successor_column,
                e.variant,
                e.effective_year,
            )
            for e in y88
        } == y88_expected
        # `note` is optional on a replaced_by edge; a curation round owes one.
        assert all(e.note for e in y88)
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


def _resolve_vintage_fixture(conn: sqlite3.Connection) -> int:
    """Project the existing relation fixtures into the common resolver.

    Keep their original assertions and table-shaped setup. The fixture's omitted
    column names are irrelevant to edition succession; use the variable slug.
    """
    variables = []
    for row in conn.execute(
        "SELECT v.*, p.slug AS provider, r.slug AS register_slug "
        "FROM variable v JOIN register r USING (register_id) "
        "JOIN provider p USING (provider_id)"
    ):
        states = tuple(
            ResolvedState(
                variant=ResolvedVariant(
                    slug=state["variant_slug"], name=state["variant_name"]
                ),
                valid_from=state["valid_from"],
                valid_to=state["valid_to"],
                delivery_column_name=state["delivery_column_name"] or row["slug"],
                value_set_version_label=state["value_set_version_label"],
                data_type=state["data_type"],
                data_length=None,
                operational_definition=None,
                provenance=None,
                classification=state["classification_slug"],
            )
            for state in conn.execute(
                "SELECT s.*, rv.slug AS variant_slug, rv.name AS variant_name, "
                "c.slug AS classification_slug FROM variable_state s "
                "JOIN register_variant rv USING (register_variant_id) "
                "LEFT JOIN classification c ON c.id = s.classification_id "
                "WHERE s.variable_id = ?",
                (row["variable_id"],),
            )
        )
        if states:
            variables.append(
                ResolvedVariable(
                    register=ResolvedRegister(
                        provider=row["provider"],
                        slug=row["register_slug"],
                        name=row["register_slug"],
                    ),
                    slug=row["slug"],
                    provider_key=row["provider_key"],
                    name=row["name"],
                    definition=None,
                    description=None,
                    operational_definition=None,
                    measurement_unit=None,
                    is_sensitive=False,
                    is_identifier=False,
                    states=states,
                )
            )
    metadata = ResolvedMetadata(
        successions=tuple(
            ResolvedSuccession(
                predecessor="/".join(row[:3]),
                successor="/".join(row[3:6]),
                effective_year=row[6],
                note=row[7],
                description=row[8],
            )
            for row in conn.execute("SELECT * FROM variable_replaced_by")
        )
    )
    classifications = tuple(
        ResolvedClassificationSuccession(
            predecessor=a, successor=b, effective_year=year
        )
        for a, b, year in conn.execute(
            "SELECT predecessor_slug, successor_slug, effective_year FROM classification_replaced_by"
        )
    )
    result = resolve_variable_successions(metadata, tuple(variables), classifications)
    added = result.successions[len(metadata.successions) :]
    conn.executemany(
        "INSERT INTO variable_replaced_by VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                *e.predecessor.split("/"),
                *e.successor.split("/"),
                e.effective_year,
                e.note,
                e.description,
            )
            for e in added
        ],
    )
    return len(added)


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
        n = _resolve_vintage_fixture(conn)
        assert n == 1
        assert _lift_rows(conn) == [("sni-2002", "sni-2007", 2007)]

    def test_clean_pair_with_different_streams_mints_nothing(self) -> None:
        # Same-name and 1:1 by edition is not enough after #592: non-vintage
        # slug tokens still define separate streams, so this clean-looking pair
        # would fail the real-corpus stream guard and must be skipped at source.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Kommun", slug="u-ukom")
        add_variable(conn, register_id=1, var_id=2, name="Kommun", slug="u-hkom")
        add_state(
            conn,
            register_id=1,
            variable_slug="u-ukom",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="u-hkom",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
        assert n == 0
        assert _lift_rows(conn) == []

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
        n = _resolve_vintage_fixture(conn)
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
        n = _resolve_vintage_fixture(conn)
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
        n = _resolve_vintage_fixture(conn)
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
        n = _resolve_vintage_fixture(conn)
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
        n = _resolve_vintage_fixture(conn)
        assert n == 0
        assert _lift_rows(conn) == []

    def test_interval_native_variable_is_not_paired_with_neighbor(self) -> None:
        # A chain-spanning variable is interval-native even when the same-name
        # family has another variable on one edition. It must not be paired with
        # that neighbor; the real corpus has municipality columns in this shape,
        # where a false lift would close a reversed source edge into a cycle.
        conn = _vintage_db()
        add_variable(conn, register_id=1, var_id=1, name="Kommun", slug="u-ukom")
        add_state(
            conn,
            register_id=1,
            variable_slug="u-ukom",
            register_variant_id=10,
            valid_from="2002-01-01",
            valid_to="2006-12-31",
            classification_id=_cid(conn, "sni2002"),
        )
        add_state(
            conn,
            register_id=1,
            variable_slug="u-ukom",
            register_variant_id=10,
            valid_from="2007-01-01",
            valid_to="9999-12-31",
            classification_id=_cid(conn, "sni2007"),
        )
        add_variable(conn, register_id=1, var_id=2, name="Kommun", slug="u-hkom")
        add_state(
            conn,
            register_id=1,
            variable_slug="u-hkom",
            register_variant_id=10,
            classification_id=_cid(conn, "sni2007"),
        )
        conn.commit()
        n = _resolve_vintage_fixture(conn)
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
        n = _resolve_vintage_fixture(conn)
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
            _resolve_vintage_fixture(conn)
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
        n = _resolve_vintage_fixture(conn)
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
        n = _resolve_vintage_fixture(conn)
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
        n = _resolve_vintage_fixture(conn)
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
        n = _resolve_vintage_fixture(conn)
        assert n == 1
        row = conn.execute(
            "SELECT predecessor_variable, successor_variable, effective_year "
            "FROM variable_replaced_by "
            "WHERE note = 'derived:classification_vintage_lift'"
        ).fetchone()
        assert (row[0], row[1]) == ("sni-2002", "sni-2007")
        assert row[2] is None
