"""Relation declaration loading: type dispatch, foreign-field rejection and per-type edge shape validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import FqidKind
from reg_meta_build.relations import (
    load_relations,
)

if TYPE_CHECKING:
    from pathlib import Path


def _load(tmp_path: Path, text: str):
    path = tmp_path / "relations.toml"
    path.write_text(text, encoding="utf-8")
    return load_relations(path)


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
