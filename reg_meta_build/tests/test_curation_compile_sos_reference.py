"""Thin native naming captures its complete family guard per selected scope."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from _curation_compile_support import (
    case_record as _case_record,
    make_naming_reader as _naming_reader,
    make_tree as _tree,
)
from reg_meta_build.curation_compile import compile_native_naming
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.id import mint
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.source_coordinates import (
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
)

from reg_meta_build.fqid_slugs import SlugEntry


def test_thin_native_naming_captures_complete_family_guard(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers/fk/r.toml"
    path.parent.mkdir()
    register_id = mint("fk", "r")
    path.write_text(
        f'[register]\nprovider = "fk"\nslug = "r"\nnative_id = "{register_id}"\n'
        f'[[variant]]\nnative_id = "{register_id}.{mint("fk", "r", "_default")}"\nslug = "_default"\n'
        f'[[variable]]\nnative_id = "{register_id}.col"\nslug = "col"\n'
    )
    parent = _case_record(provider="fk", register="r", parent="register")
    variable = _case_record(provider="fk", register="r", variable="col")
    records = (parent, variable)

    class Reader:
        def iter_native_families(self, source, registers=None):
            return iter(((native_variable_key(variable), (variable,)),))

        def iter_records(self, *, source):
            return iter(records)

    register_key = source_register_key(variable)
    assert register_key is not None
    scope = CompiledScope(
        source=variable.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register", provider="fk", source_key=register_key
                ),
                naming=SlugEntry(
                    kind="register", provider="fk", source_id=str(register_id), slug="r"
                ),
                contributors=(),
            ),
        ),
    )
    names, _, _, diagnostics, _ = compile_native_naming(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=_naming_reader(Reader()))),
        (scope,),
        subset=True,
    )
    assert diagnostics == ()
    target = next(
        item.target
        for item in names[(variable.source, None)]
        if item.target.kind == "variable"
    )
    assert len(target.expectations) == 1
    assert target.peer_guards[0].expected_members == (target.expectations[0].ref,)
