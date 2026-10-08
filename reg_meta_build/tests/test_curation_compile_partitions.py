"""Split descriptions, unassigned families, tracked partition maps and implicit partitions compile from source coordinates."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from _curation_compile_support import (
    ALIAS as _ALIAS,
    DESCRIPTION as _DESCRIPTION,
    compile_partition_fixture as _compile_partition_fixture,
    enrichment_fixture as _enrichment_fixture,
    errata_record as _errata_record,
    partition_scope as _partition_scope,
    scb_partition_records as _scb_partition_records,
    scb_partition_tree as _scb_partition_tree,
)
from reg_meta_build.curation_compile import (
    compile_deferred_partitions,
    compile_enrichment,
    compile_partitions,
    convert_column_partitions,
)
from reg_meta_build.curation_tree import (
    load_curation_tree,
)
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import (
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import (
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    SearchAliasDecision,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    SourceFields,
    value_field,
)

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path


def test_compiled_split_descriptions_condition_shared_native_member(tmp_path: Path):
    first = _errata_record(column="A", year="2020")
    second = _errata_record(column="B", year="2020")
    assert record_ref(first) == record_ref(second)
    converted = convert_column_partitions(
        (first, second),
        source_id="1.5",
        split_ids=("1.5.a", "1.5.b"),
        declared_columns={"A": "1.5.a", "B": "1.5.b"},
        declaration_reference="fixture",
    )
    assert converted.case is not None
    targets = {item.source_id: item.target for item in converted.bindings}
    second_description = _DESCRIPTION.replace(
        'variable = "a"', 'variable = "b"'
    ).replace("Accepted prose", "Other prose")
    tree, prepared, scope, naming = _enrichment_fixture(
        tmp_path,
        (first, second),
        _DESCRIPTION + second_description + _ALIAS,
        target=targets["1.5.a"],
    )
    scope_key = (scope.source, scope.register_key)
    naming[scope_key] = (
        *naming[scope_key],
        NamingDeclaration(
            target=targets["1.5.b"],
            naming=SlugEntry(
                kind="variable", provider="scb", source_id="1.5.b", slug="b"
            ),
            contributors=(),
        ),
    )
    cases, diagnostics, _ = compile_enrichment(
        tree,
        prepared,
        (scope,),
        naming,
        {scope_key: (converted.case,)},
        {},
        subset=False,
    )
    assert diagnostics == ()
    descriptions = cases[scope_key][:2]
    assert [case.decision.effects[0].when[0].value for case in descriptions] == [
        "A",
        "B",
    ]
    assert all(len(case.targets[0].alternatives) == 2 for case in descriptions)
    result = apply_occurrence_cases((first, second), (converted.case, *descriptions))
    assert not [
        issue
        for issue in result.diagnostics
        if issue.code == "conflicting_curation_effects"
    ]
    assert {
        occurrence.variable_key[-1]: occurrence.fields.description.value
        for occurrence in result.occurrences
    } == {"1.5.a": "Accepted prose", "1.5.b": "Other prose"}
    for split, description in (
        ("1.5.a", "Accepted prose"),
        ("1.5.b", "Other prose"),
    ):
        (occurrence,) = tuple(
            item for item in result.occurrences if item.variable_key[-1] == split
        )
        assert occurrence.variant_key is not None
        assert occurrence.column_key is not None
        column = occurrence.fields.column_name
        assert column is not None and isinstance(column.value, str)
        formed = form_native_variable(
            (occurrence,),
            register=ResolvedRegister(provider="scb", slug="sample", name="Sample"),
            variants={
                occurrence.variant_key: ResolvedVariant(slug="people", name="People")
            },
            slug=split.rsplit(".", 1)[1],
            provider_key=column.value,
            flags=SourceFields(
                sensitivity=value_field(False), identifier=value_field(False)
            ),
            coding={occurrence.column_key: resolve_code_membership(())},
        )
        assert formed.variable is not None
        assert formed.variable.description == description
    alias = cases[scope_key][2]
    assert isinstance(alias.decision, SearchAliasDecision)
    assert alias.decision.variable_key == targets["1.5.a"].source_key
    assert len(alias.targets[0].alternatives) == 2

    described_second = second.model_copy(
        update={
            "fields": second.fields.model_copy(
                update={"description": value_field("Sibling description")}
            )
        }
    )
    tree, prepared, scope, sibling_naming = _enrichment_fixture(
        tmp_path / "sibling",
        (first, described_second),
        _DESCRIPTION,
        target=targets["1.5.a"],
    )
    sibling_cases, diagnostics, _ = compile_enrichment(
        tree,
        prepared,
        (scope,),
        sibling_naming,
        {scope_key: (converted.case,)},
        {},
        subset=False,
    )
    assert diagnostics == () and len(sibling_cases[scope_key]) == 1

    base = native_variable_key(first)
    register = source_register_key(first)
    assert base is not None and register is not None
    tree, prepared, scope, base_naming = _enrichment_fixture(
        tmp_path / "base",
        (first, second),
        _DESCRIPTION,
        target=NativeNamingTarget(
            kind="variable", provider="scb", source_key=base, register_key=register
        ),
    )
    broad, diagnostics, _ = compile_enrichment(
        tree,
        prepared,
        (scope,),
        base_naming,
        {scope_key: (converted.case,)},
        {},
        subset=False,
    )
    assert broad == {}
    assert [item.code for item in diagnostics] == ["overbroad_curation_entry"]


@pytest.mark.parametrize("drift", [None, "definition", "missing", "added"])
def test_unassigned_family_withholds_naming_and_checks_complete_source(tmp_path, drift):
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256

    root = tmp_path / "curation"
    originals = _scb_partition_records(("ANSWER", "OTHER"))
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5"\nslug = "quantity"\n'
        '[[identity.unassigned]]\nvariable = "1.5"\n'
        f'expected_evidence_sha256 = "{acknowledgement_evidence_sha256(originals)}"\n'
        'evidence = "complete source review: no supported owner"\n',
    )
    records = originals
    if drift == "definition":
        records = (
            originals[0].model_copy(
                update={
                    "fields": originals[0].fields.model_copy(
                        update={"definition": value_field("changed source meaning")}
                    )
                }
            ),
            originals[1],
        )
    elif drift == "missing":
        records = originals[:1]
    elif drift == "added":
        records = (*originals, _scb_partition_records(("ANSWER", "OTHER", "NEW"))[-1])
    native = native_variable_key(records[0])
    reader = SimpleNamespace(
        iter_partition_families=lambda *a, **k: iter(((native, records),)),
        iter_native_families=lambda *a, **k: iter(((native, records),)),
    )
    scope = _partition_scope(records)
    tree = load_curation_tree(root)
    prepared = cast("Any", SimpleNamespace(records=reader))
    cases, names, keys, ambiguities, bases, _, issues = compile_partitions(
        tree, prepared, (scope,)
    )
    key = scope.source, None
    assert not cases.get(key) and not names[key] and not ambiguities.get(key)
    assert keys[key] == ((native, None),)
    assert bases[key] == {native}
    assert {d.code for d in issues} == ({"stale_curation_entry"} if drift else set())
    # No identity correction or synthetic owner changes any original fact.
    applied = apply_occurrence_cases(records, cases.get(key, ()))
    assert tuple(o.source_records[0] for o in applied.occurrences) == records
    deferred = compile_deferred_partitions(tree, prepared, (scope,))
    assert deferred[2][key] == {native} and not deferred[0][key]


@pytest.mark.parametrize(
    "extra",
    [
        '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n',
        '\n[[identity.partition]]\nvariable = "1.5"\ncolumns = { ANSWER = "1.5" }\ncolumns_ref = "competing owner"\n',
    ],
)
def test_unassigned_family_rejects_competing_identity(tmp_path, extra):
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256

    root = tmp_path / "curation"
    records = _scb_partition_records(("ANSWER",))
    _scb_partition_tree(
        root,
        '[[identity.unassigned]]\nvariable = "1.5"\n'
        f'expected_evidence_sha256 = "{acknowledgement_evidence_sha256(records)}"\n'
        'evidence = "complete source review"\n' + extra,
    )
    with pytest.raises(ValueError, match="competing ownership"):
        _compile_partition_fixture(root, records)


def test_unassigned_and_partial_suffix_keep_base_identity(tmp_path: Path):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
        '[[identity.partition]]\nvariable = "1.5"\n'
        'columns = { ANSWER = "1.5.answer" }\n'
        'unassigned_columns = ["LEFT"]\ncolumns_ref = "fixture map"\n',
    )
    records = _scb_partition_records(("ANSWER", "LEFT"))
    compiled, key, native = _compile_partition_fixture(root, records)
    cases, _, keys, _, _, _, _ = compiled
    assert (native, None) in keys[key]
    corrected = apply_occurrence_cases(records, cases[key]).occurrences
    assert corrected[0].variable_key != native
    assert corrected[1].variable_key == native

    path = root / "registers" / "scb" / "sample.toml"
    path.write_text(
        path.read_text().split("[[identity.partition]]")[0]
        + '[[variable]]\nnative_id = "1.5.unknown"\nslug = "unknown"\n',
        encoding="utf-8",
    )
    compiled, key, native = _compile_partition_fixture(root, records)
    cases, naming, keys, ambiguities, _, _, _ = compiled
    assert {item.naming.source_id for item in naming[key]} == {"1.5.answer"}
    assert (native, None) in keys[key]
    assert len(ambiguities[key]) == 1
    assert ambiguities[key][0].candidate_columns == ()
    assert (
        apply_occurrence_cases(records, cases[key]).occurrences[1].variable_key
        == native
    )


def test_partition_map_without_split_name_is_stale(tmp_path: Path):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[identity.partition]]\nvariable = "1.5"\n'
        'columns = { ANSWER = "1.5.answer" }\n'
        'columns_ref = "fixture map"\n',
    )
    compiled, key, _ = _compile_partition_fixture(
        root, _scb_partition_records(("ANSWER",))
    )
    assert not compiled[0].get(key)
    assert [issue.code for issue in compiled[-1]] == ["stale_curation_entry"]
    assert "#/identity.partition/1" in compiled[-1][0].detail


@pytest.mark.parametrize("editions", ['["2020", "absent"]', '["2021"]'])
def test_column_owner_stale_or_competing_editions_fail_closed(tmp_path: Path, editions):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.old"\nslug = "old"\n'
        '[[variable]]\nnative_id = "1.5.new"\nslug = "new"\n'
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.2"\n'
        'column = "ANSWER"\nowner = "1.5.old"\nref = "old basis"\n'
        f"source_editions = {editions}\n"
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.2"\n'
        'column = "ANSWER"\nowner = "1.5.new"\nref = "new basis"\n'
        'source_editions = ["2021"]\n',
    )
    records = tuple(
        _errata_record(column="ANSWER", year=year, member=20 + index)
        for index, year in enumerate(("2020", "2021"))
    )
    compiled, key, native = _compile_partition_fixture(root, records)
    assert compiled[-1]
    corrected = apply_occurrence_cases(records, compiled[0].get(key, ()))
    assert {o.variable_key for o in corrected.occurrences} == {native}
