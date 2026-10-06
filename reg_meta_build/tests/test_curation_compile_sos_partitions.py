"""SOS subdataset splits, partition determinism and classification bindings compile from source coordinates."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _curation_compile_support import (
    compile_partition_fixture as _compile_partition_fixture,
    compiled_bytes as _bytes,
    make_naming_reader as _naming_reader,
    make_prepared as _prepared,
    make_scope as _scope,
    make_tree as _tree,
    scb_partition_records as _scb_partition_records,
    scb_partition_tree as _scb_partition_tree,
    sos_partition_records as _sos_partition_records,
)
from reg_meta_build.curation_compile import (
    CompiledCuration,
    compile_curation,
    finalize_classification_bindings,
)
from reg_meta_build.curation_tree import (
    load_curation_tree,
)
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.source_coordinates import (
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_sos_subdataset_split_routes_same_type_occurrences(tmp_path: Path):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers/sos/par.toml"
    path.parent.mkdir(parents=True)
    base = (
        '[register]\nprovider = "sos"\nslug = "par"\n'
        'native_id = "5891427617861710725"\nname = "Patientregistret"\n'
        '[[identity.split]]\nvariable = "ATC"\nby = "deldatamangd"\n'
    )
    parts = (
        '{ deldatamangd = "PAR_OV", owner = "5891427617861710725.ATC.outpatient" }',
        '{ deldatamangd = "PAR_SV", owner = "5891427617861710725.ATC.inpatient" }',
    )
    names = (
        '[[variable]]\nnative_id = "5891427617861710725.ATC.outpatient"\n'
        'slug = "outpatient"\n'
        '[[variable]]\nnative_id = "5891427617861710725.ATC.inpatient"\n'
        'slug = "inpatient"\n'
    )
    records = _sos_partition_records(subsets=("PAR_OV", "PAR_SV"))
    expected = {
        "PAR_OV": "outpatient",
        "PAR_SV": "inpatient",
    }
    first = None
    for ordered_parts in (parts, parts[::-1]):
        path.write_text(
            base + f"parts = [{', '.join(ordered_parts)}]\n" + names,
            encoding="utf-8",
        )
        compiled, key, native = _compile_partition_fixture(root, records)
        if first is None:
            first = compiled
        else:
            assert compiled == first
        cases, naming, keys, _, bases, _, issues = compiled
        assert not issues
        assert len(cases[key]) == 1
        assert len(naming[key]) == 2
        assert native in bases[key]
        corrected = apply_occurrence_cases(records, cases[key]).occurrences
        assert {
            record.subject.variant.name: occurrence.variable_key[-1]
            for record, occurrence in zip(records, corrected, strict=True)
        } == {subset: subset for subset in expected}
        assert {
            item.target.source_key[-1]: item.naming.slug for item in naming[key]
        } == expected
        assert all(value is not None for _, value in keys[key])


@pytest.mark.parametrize(
    "subsets, declared",
    [
        (("PAR_OV",), ("PAR_OV", "PAR_SV")),
        (("PAR_OV", "PAR_TV"), ("PAR_OV", "PAR_SV")),
    ],
)
def test_sos_subdataset_split_requires_exact_partition(
    tmp_path: Path, subsets: tuple[str, ...], declared: tuple[str, ...]
):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers/sos/par.toml"
    path.parent.mkdir(parents=True)
    slugs = {"PAR_OV": "outpatient", "PAR_SV": "inpatient"}
    path.write_text(
        '[register]\nprovider = "sos"\nslug = "par"\n'
        'native_id = "5891427617861710725"\nname = "Patientregistret"\n'
        '[[identity.split]]\nvariable = "ATC"\nby = "deldatamangd"\n'
        "parts = ["
        + ", ".join(
            f'{{ deldatamangd = "{subset}", owner = "5891427617861710725.ATC.{slugs[subset]}" }}'
            for subset in declared
        )
        + "]\n"
        + "".join(
            f'[[variable]]\nnative_id = "5891427617861710725.ATC.{slugs[subset]}"\n'
            f'slug = "{slugs[subset]}"\n'
            for subset in declared
        ),
        encoding="utf-8",
    )
    records = _sos_partition_records(subsets=subsets)
    compiled, key, native = _compile_partition_fixture(root, records)
    cases, _, _, _, bases, _, issues = compiled
    assert not cases.get(key)
    assert native in bases[key]
    assert [issue.code for issue in issues] == ["stale_curation_entry"]
    assert all(
        occurrence.variable_key == native
        for occurrence in apply_occurrence_cases(
            records, cases.get(key, ())
        ).occurrences
    )


def test_partition_compile_is_byte_identical_on_rerun(tmp_path: Path):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root, '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
    )
    records = _scb_partition_records(("ANSWER", "LEFT"))
    first = _compile_partition_fixture(root, records)[0]
    second = _compile_partition_fixture(root, records)[0]
    shuffled = _compile_partition_fixture(root, records, shuffled=True)[0]

    def encode(value):
        cases, naming, keys, ambiguities, bases, retained, diagnostics = value
        compiled = CompiledCuration(
            fields={},
            cases=cases,
            report={},
            naming=naming,
            provider_keys=keys,
            naming_ambiguities=ambiguities,
            diagnostics=diagnostics,
            source_diagnostics=retained,
        )
        return _bytes(compiled), bases

    assert encode(first) == encode(second) == encode(shuffled)


def test_selected_classification_variable_binding_has_exact_status(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "classifications" / "BETA.toml"
    path.write_text(path.read_text().replace("scb/other/one", "scb/sample/one"))
    tree = load_curation_tree(root)
    scope = _scope()
    ref = "classifications/BETA.toml#/binding/variable/1"
    stale = compile_curation(tree, _prepared(), (scope,), subset=True)
    assert stale.report["_classifications"]["stale"] == [ref]
    assert [issue.code for issue in stale.diagnostics] == ["stale_curation_entry"]


@pytest.mark.parametrize("owner", ["1.5", "1.5.answer"])
def test_classification_binding_matches_partition_produced_name(
    tmp_path: Path, owner: str
) -> None:
    root = tmp_path / "curation"
    _tree(root)
    register_file = root / "registers/scb/sample.toml"
    register_file.write_text(
        register_file.read_text(encoding="utf-8")
        + '[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
        + '[[identity.partition]]\nvariable = "1.5"\n'
        + 'columns = { ANSWER = "1.5.answer" }\ncolumns_ref = "fixture"\n',
        encoding="utf-8",
    )
    if owner == "1.5":
        register_file.write_text(register_file.read_text().replace("1.5.answer", owner))
    (root / "classifications/GAMMA.toml").write_text(
        '[classification]\nshort_name = "GAMMA"\nslug = "gamma"\n'
        'name = "Gamma"\ncodes_file = "gamma.csv"\n'
        '[[binding.variable]]\nvariable = "scb/sample/answer"\n',
        encoding="utf-8",
    )
    records = _scb_partition_records(("ANSWER",))
    native = native_variable_key(records[0])
    register = source_register_key(records[0])
    assert native is not None and register is not None

    class Records:
        def iter_native_families(self, source, registers=None):
            return iter(((native, records),))

        def iter_records(self, *, source):
            return iter(records)

        def iter_register_slices(self, source, registers):
            return iter(((register, records),))

    prepared = _prepared()
    prepared.records = _naming_reader(Records())
    scope = CompiledScope(source=records[0].source, register_key=register)
    compiled = compile_curation(
        load_curation_tree(root), prepared, (scope,), subset=True
    )
    ref = "classifications/GAMMA.toml#/binding/variable/1"
    assert ref in compiled.report["_classifications"]["entries_matched"]
    assert ref not in compiled.report["_classifications"]["stale"]
    assert any(
        item.target.kind == "variable" and item.naming.slug == "answer"
        for item in (compiled.naming or {})[scope.source, scope.register_key]
    )


def test_classification_label_staleness_is_deferred_until_scope_resolution(tmp_path):
    tree = _tree(tmp_path / "curation")
    full = compile_curation(tree, _prepared(), (_scope(),))
    issues = finalize_classification_bindings(
        full,
        tree,
        matched_labels={"Selected provider label"},
        duplicate_overrides=set(),
        subset=False,
    )
    ref = "classifications/BETA.toml#/binding/value_set_labels/1"
    assert ref in full.report["_classifications"]["stale"]
    assert [(issue.code, issue.subject) for issue in issues] == [
        ("stale_curation_entry", ref)
    ]
    subset = compile_curation(tree, _prepared(), (_scope(),), subset=True)
    assert not finalize_classification_bindings(
        subset,
        tree,
        matched_labels=set(),
        duplicate_overrides=set(),
        subset=True,
    )
    assert set(subset.report["_classifications"]["not_evaluated_in_subset"]) >= {
        ref,
        "classifications/ALPHA.toml#/binding/value_set_labels/1",
    }


def test_unmatched_global_entry_is_stale_only_in_complete_build(tmp_path):
    tree = _tree(tmp_path / "curation")
    complete = compile_curation(tree, _prepared(), (), subset=False)
    assert (
        "curation/registers/scb/sample.toml#/group/1"
        in complete.report["scb/sample"]["stale"]
    )
    assert any(item.code == "stale_curation_entry" for item in complete.diagnostics)
    subset = compile_curation(tree, _prepared(), (), subset=True)
    assert not subset.diagnostics
    assert (
        "curation/registers/scb/sample.toml#/group/1"
        in subset.report["scb/sample"]["not_evaluated_in_subset"]
    )
