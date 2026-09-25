"""The global hybrid families compile from tracked coordinates, without pins."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal, cast

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta.errors import RegMetaError
from reg_meta_build.curation_compile import (
    COMPILED,
    FAMILIES,
    CompiledCuration,
    _case_family,
    _compile_sos_register,
    _compile_thin_register,
    _gap_family,
    _naming_family,
    compile_curation,
    compile_native_naming,
    compile_partitions,
    finalize_classification_bindings,
    merge_scope,
    merge_selection,
    tree_sha256,
)
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.id import mint
from reg_meta_build.pipeline import PipelineSelection, ScopeDeclarations
from reg_meta_build.resolved_catalog import ResolvedVariant
from reg_meta_build.source_coding import CodeListClaim, copied_coding_fingerprints
from reg_meta_build.source_coordinates import native_variable_key, source_register_key
from reg_meta_build.source_curation import (
    AcknowledgeDecision,
    CheckedVariantAssignment,
    CuratedOccurrenceAddition,
    CurationCase,
    OccurrenceCorrectionDecision,
)
from reg_meta_build.source_effects import apply_occurrence_cases
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import (
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    SourceCoordinate,
    SourceFieldCells,
    SourceFields,
    SourceParentObservation,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path


def _revision(
    dataset: str, *, artifact_path: str | None = None, upstream_revision: str = "v1"
) -> SourceRevision:
    return SourceRevision.create(
        dataset=dataset,
        publisher="fixture",
        purpose="fixture",
        upstream_revision=upstream_revision,
        artifact_path=artifact_path or f"{dataset}.csv",
        artifact_size=1,
        artifact_sha256="0" * 64,
    )


def _tree(root: Path):
    classes = root / "classifications"
    classes.mkdir(parents=True)
    for short, slug in (("ALPHA", "alpha"), ("BETA", "beta")):
        (classes / f"{short}.toml").write_text(
            f'[classification]\nshort_name = "{short}"\nslug = "{slug}"\n'
            f'name = "{short}"\ncodes_file = "{slug}.csv"\n'
            + (
                '[binding]\nlabel_source = "sos"\n'
                'value_set_labels = ["Unmatched label"]\n'
                '[[binding.variable]]\nvariable = "scb/other/one"\n'
                if short == "BETA"
                else '[binding]\nvalue_set_labels = ["Selected provider label"]\n'
            ),
            encoding="utf-8",
        )
    registers = root / "registers" / "scb"
    registers.mkdir(parents=True)
    (registers / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[group]]\nregister = "scb/sample"\nkey = "pair"\nlabel = "Pair"\n'
        'axis = "rank"\nmembers = [{variable = "one", value = "1", label = "First"}, '
        '{variable = "two", value = "2", label = "Second"}]\n'
        '[[code_label_pair]]\ncode = "scb/sample/one"\nlabel = "scb/sample/two"\n',
        encoding="utf-8",
    )
    (registers / "other.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "other"\nnative_id = "2"\n',
        encoding="utf-8",
    )
    (root / "classification_groups.toml").write_text(
        '[[classification_group]]\nkey = "umbrella"\nlabel = "Umbrella"\n'
        'members = [{classification = "alpha", value = "a", label = "A"}, '
        '{classification = "beta", value = "b", label = "B"}]\n',
        encoding="utf-8",
    )
    (root / "relations.toml").write_text(
        '[[edge]]\ntype = "replaced_by"\nfrom = "class/alpha"\nto = "class/beta"\n'
        '[[edge]]\ntype = "derived_from"\nderived = "class/beta"\nsource = "class/alpha"\n'
        '[[edge]]\ntype = "replaced_by"\nfrom = "scb/sample/one"\n'
        'to = "scb/sample/two"\neffective_year = 2020\n'
        '[[edge]]\ntype = "same_as"\na = "scb/sample/one"\nb = "scb/other/one"\n',
        encoding="utf-8",
    )
    (root / "tags.toml").write_text(
        '[[tag]]\nslug = "theme"\nlabel = "Theme"\n'
        '[[tag.member]]\nvariable = "scb/sample/one"\n'
        '[[tag.member]]\nvariable = "scb/other/one"\n',
        encoding="utf-8",
    )
    (root / "lineage.toml").write_text(
        '[lineage_defaults]\n"scb/sample" = "people"\n"scb/other" = "people"\n',
        encoding="utf-8",
    )
    return load_curation_tree(root)


def _scope() -> ScopeDeclarations:
    return ScopeDeclarations(
        source="scb-registerinformation",
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register",
                    provider="scb",
                    source_key=("scb", "register", 1),
                    identity_revision=_revision("scb-registerinformation"),
                ),
                naming=SlugEntry(
                    kind="register", provider="scb", source_id="1", slug="sample"
                ),
                contributors=(),
            ),
        ),
    )


def _prepared():
    class Records:
        def iter_native_families(self, source):
            return iter(())

        def iter_records(self, *, source):
            return iter((self._record(source),))

        def iter_register_slices(self, source, registers):
            return iter(((None, (self._record(source),)),))

        @staticmethod
        def _record(source):
            register = SourceCoordinate(status="value", native_id=1)
            return SimpleNamespace(
                source=source,
                subject=SimpleNamespace(
                    provider="scb",
                    register_name=register,
                    variant=SourceCoordinate(status="unknown"),
                ),
                parent_facts=(
                    SimpleNamespace(
                        kind="register", register_name=register, variant=None
                    ),
                ),
            )

    inputs = tuple(
        SimpleNamespace(
            origin="snapshot",
            path=path,
            revision=_revision(dataset, artifact_path=f"snapshot-a:source/{path}"),
        )
        for path, dataset in (
            ("Identifierare.csv", "scb-identifierare"),
            ("Timeseries.csv", "scb-timeseries"),
            ("Registerinformation.csv", "scb-registerinformation"),
        )
    )
    return SimpleNamespace(
        manifest=SimpleNamespace(inputs=inputs),
        iter_evidence=lambda: iter(()),
        records=Records(),
        value_sources=(),
    )


def _bytes(compiled) -> bytes:
    return json.dumps(
        {
            "fields": compiled.fields,
            "cases": {repr(key): value for key, value in compiled.cases.items()},
            "naming": {
                repr(key): value for key, value in (compiled.naming or {}).items()
            },
            "provider_keys": {
                repr(key): value
                for key, value in (compiled.provider_keys or {}).items()
            },
            "naming_ambiguities": {
                repr(key): value
                for key, value in (compiled.naming_ambiguities or {}).items()
            },
            "variants": {
                repr(key): value for key, value in (compiled.variants or {}).items()
            },
            "report": compiled.report,
        },
        default=lambda value: (
            value.model_dump(mode="json")
            if hasattr(value, "model_dump")
            else value.__dict__
        ),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def test_global_families_and_manifest_wiring_compile_for_subset(tmp_path):
    tree = _tree(tmp_path / "curation")
    result = compile_curation(tree, _prepared(), (_scope(),), subset=True)
    assert FAMILIES.keys() >= COMPILED
    assert [book.metadata["slug"] for book in result.fields["classifications"]] == [
        "alpha",
        "beta",
    ]
    assert [book.source for book in result.fields["classifications"]] == [
        "classifications/alpha.csv",
        "classifications/beta.csv",
    ]
    assert len(result.fields["classification_successions"]) == 1
    metadata = result.fields["metadata"]
    assert len(metadata.variable_groups) == 1
    assert metadata.variable_groups[0].members[0].variable == "scb/sample/one"
    assert len(metadata.classification_groups) == 1
    assert len(metadata.classification_derivations) == 1
    assert len(metadata.successions) == 1
    assert len(metadata.variable_same_as) == 1
    assert [member.target for member in metadata.tags[0].members] == [
        "scb/sample/one",
        "scb/other/one",
    ]
    assert len(result.fields["code_label_pairs"]) == 1
    assert result.fields["lineage_defaults"] == (
        ("scb/other", "people"),
        ("scb/sample", "people"),
    )
    assert result.fields["identifier_sources"] == ("scb-identifierare",)
    assert result.fields["event_sources"] == (
        ("scb-timeseries", "scb-registerinformation"),
    )
    assert result.report["_subset"]["dropped"] == []
    assert len(result.report["_classifications"]["not_evaluated_in_subset"]) == 1
    assert (
        "classifications/ALPHA.toml#/binding/value_set_labels/1"
        not in result.report["_classifications"]["not_evaluated_in_subset"]
    )
    full = compile_curation(tree, _prepared(), (_scope(),))
    assert full.report["_classifications"]["not_evaluated_in_subset"] == []
    stored = PipelineSelection(
        prepared_path="prepared",
        prepared_commit="0" * 40,
        prepared_sha256="0" * 64,
        scopes=(),
    )
    merged = merge_selection(stored, result)
    assert merged.code_label_pairs == result.fields["code_label_pairs"]
    assert merged.metadata == metadata


def test_compilation_is_byte_identical_with_shuffled_register_order(tmp_path):
    tree = _tree(tmp_path / "curation")
    first = compile_curation(tree, _prepared(), (_scope(),))
    second = compile_curation(tree, _prepared(), (_scope(),))
    shuffled = compile_curation(
        replace(tree, registers=tuple(reversed(tree.registers))),
        _prepared(),
        (_scope(),),
    )
    assert _bytes(first) == _bytes(second) == _bytes(shuffled)


def test_coding_register_tables_load_with_finite_periods(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers" / "scb" / "sample.toml"
    path.write_text(
        path.read_text()
        + '\n[[coding.choice]]\nvariable = "1.5"\nvariant = "people"\n'
        + 'column = "VALUE"\nperiods = [["2020-01-01", "2020-06-30"], '
        + '["2020-07-01", "2020-12-31"]]\n'
        + 'keep = "kept"\nkeep_members = [["01", "Label"]]\n'
        + 'over = ["other"]\nreason = "Reviewed"\nsource = "fixture"\n'
        + '\n[[coding.uncoded]]\nvariable = "1.5"\nvariant = "people"\n'
        + 'column = "VALUE"\nperiods = [["2019-01-01", "2019-12-31"]]\n'
        + 'reason = "Reviewed"\nsource = "fixture"\n'
        + '\n[[coding.omit]]\nvariable = "1.5"\nvariant = "people"\n'
        + 'column = "VALUE"\nperiods = [["2018-01-01", "2018-12-31"]]\n'
        + 'reason = "Reviewed"\nsource = "fixture"\n'
        + '\n[[coding.extend]]\nvariable = "1.5"\nvariant = "people"\n'
        + 'column = "VALUE"\nperiods = [["2017-01-01", "2017-12-31"]]\n'
        + 'list = "kept"\nlist_members = [["01", "Label"]]\n'
        + 'witness = ["2020-01-01", "2020-06-30"]\n'
        + 'reason = "Reviewed"\nsource = "fixture"\n',
        encoding="utf-8",
    )
    register = next(
        entry
        for entry in load_curation_tree(root).registers
        if entry.register_info.slug == "sample"
    )
    assert len(register.coding.choice[0].periods) == 2
    assert (
        len(register.coding.uncoded)
        == len(register.coding.omit)
        == len(register.coding.extend)
        == 1
    )


@pytest.mark.parametrize(
    "extra",
    [
        "unexpected = true\n",
        'periods = [["2020-13-01", "2020-12-31"]]\n',
        'periods = [["2020-12-31", "2020-01-01"]]\n',
    ],
)
def test_coding_register_invalid_entry_names_file_and_index(tmp_path, extra):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers" / "scb" / "sample.toml"
    path.write_text(
        path.read_text()
        + '\n[[coding.choice]]\nvariable = "1.5"\nvariant = "people"\n'
        + 'column = "VALUE"\nkeep = "kept"\nover = ["other"]\n'
        + 'reason = "Reviewed"\nsource = "fixture"\n'
        + (
            'periods = [["2020-01-01", "2020-12-31"]]\n'
            if "periods" not in extra
            else ""
        )
        + extra,
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        load_curation_tree(root)
    assert "curation/registers/scb/sample.toml [[coding.choice." in exc.value.message
    assert "entry 1" in exc.value.message


def test_coding_register_rejects_duplicate_entry(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers" / "scb" / "sample.toml"
    block = (
        '\n[[coding.uncoded]]\nvariable = "1.5"\nvariant = "people"\n'
        'column = "VALUE"\nperiods = [["2020-01-01", "2020-12-31"]]\n'
        'reason = "Reviewed"\nsource = "fixture"\n'
    )
    path.write_text(path.read_text() + block + block, encoding="utf-8")
    with pytest.raises(RegMetaError) as exc:
        load_curation_tree(root)
    assert "[[coding.uncoded]] entry 2: duplicate entry" in exc.value.message


def test_source_event_without_selected_target_reaches_scoped_resolver(tmp_path):
    result = compile_curation(_tree(tmp_path / "curation"), _prepared(), ())
    assert result.fields["event_sources"] == (
        ("scb-timeseries", "scb-registerinformation"),
    )
    assert result.report["_subset"]["dropped"] == []


def test_event_sources_pair_within_same_snapshot_revision(tmp_path):
    inputs = tuple(
        SimpleNamespace(
            origin="snapshot",
            path=path,
            revision=_revision(
                dataset,
                artifact_path=f"{snapshot}:source/{path}",
                upstream_revision=edition,
            ),
        )
        for snapshot, edition, path, dataset in (
            ("snapshot-a", "v1", "Timeseries.csv", "timeseries-a"),
            ("snapshot-b", "v2", "Timeseries.csv", "timeseries-b"),
            ("snapshot-b", "v2", "Registerinformation.csv", "register-b"),
            ("snapshot-a", "v1", "Registerinformation.csv", "register-a"),
        )
    )
    prepared = SimpleNamespace(
        manifest=SimpleNamespace(inputs=inputs),
        iter_evidence=lambda: iter(()),
        records=_prepared().records,
    )
    scopes = tuple(
        _scope().model_copy(update={"source": source})
        for source in ("register-a", "register-b")
    )
    result = compile_curation(
        _tree(tmp_path / "curation"), cast("Any", prepared), scopes, subset=True
    )
    assert result.fields["event_sources"] == (
        ("timeseries-a", "register-a"),
        ("timeseries-b", "register-b"),
    )


def test_compiled_partition_preserves_null_for_unvisited_family(tmp_path):
    partition_base = (
        "scb-registerinformation",
        "scb",
        "register",
        "native-int",
        25,
        "variable",
        "native-int",
        1075,
    )
    scope = _scope().model_copy(update={"provider_keys": ((partition_base, None),)})
    compiled = compile_curation(
        _tree(tmp_path / "curation"), _prepared(), (scope,), subset=True
    )
    assert merge_scope(scope, compiled).provider_keys == ((partition_base, None),)


def test_native_names_overlay_and_unnamed_provider_keys_compile(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    register_file = root / "registers" / "scb" / "sample.toml"
    register_file.write_text(
        register_file.read_text()
        + '\n[[variable]]\nnative_id = "1.5"\nslug = "curated"\n'
        + '\n[[variable]]\nnative_id = "1.6"\nslug = "stale"\n'
    )
    register_file.with_name("sample.auto.toml").write_text(
        '[[variable]]\nnative_id = "1.5"\nslug = "generated"\n'
    )
    coordinate = SourceCoordinate(status="value", native_id=1)

    def record(variable_id):
        return SimpleNamespace(
            source="scb-registerinformation",
            subject=SimpleNamespace(
                provider="scb",
                register_name=coordinate,
                variable=SourceCoordinate(status="value", native_id=variable_id),
                variant=SourceCoordinate(status="unknown"),
            ),
            parent_facts=(
                SimpleNamespace(
                    kind="register", register_name=coordinate, variant=None
                ),
            ),
        )

    records = (record(5), record(7))

    class Reader:
        def iter_native_families(self, source):
            return ((native_variable_key(item), (item,)) for item in records)

        def iter_records(self, *, source):
            return iter(records)

        def iter_register_slices(self, source, registers):
            return iter(((None, records),))

    key = ("scb-registerinformation", None)
    naming, variants, provider_keys, diagnostics, _ = compile_native_naming(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=Reader())),
        (_scope(),),
        subset=True,
    )
    assert variants[key] == ()
    assert [(name.target.kind, name.naming.slug) for name in naming[key]] == [
        ("register", "sample"),
        ("variable", "curated"),
    ]
    assert all(not name.target.expectations for name in naming[key])
    assert dict(provider_keys[key]) == {
        native_variable_key(records[0]): "5",
        native_variable_key(records[1]): None,
    }
    assert [(issue.code, issue.subject) for issue in diagnostics] == [
        ("stale_curation_entry", "1.6")
    ]


@pytest.mark.parametrize("auto_file", [False, True])
def test_declared_column_pin_is_not_evaluated_by_native_naming(tmp_path, auto_file):
    root = tmp_path / "curation"
    _tree(root)
    register_file = root / "registers" / "scb" / "sample.toml"
    if auto_file:
        register_file = register_file.with_name("sample.auto.toml")
    register_file.write_text(
        (register_file.read_text() if not auto_file else "")
        + '\n[[variable]]\nnative_id = "1.ColumnName"\nslug = "column-name"\n'
    )
    scope = _scope()
    declared_column = NamingDeclaration(
        target=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=(
                "scb-registerinformation",
                "scb",
                "register",
                "native-int",
                1,
                "declared-column",
                "ColumnName",
            ),
            register_key=(
                "scb-registerinformation",
                "scb",
                "register",
                "native-int",
                1,
            ),
            identity_revision=_revision("scb-registerinformation"),
        ),
        naming=SlugEntry(
            kind="variable",
            provider="scb",
            source_id="1.ColumnName",
            slug="column-name",
        ),
        contributors=(),
    )
    scope = scope.model_copy(update={"naming": (*scope.naming, declared_column)})
    tree = load_curation_tree(root)
    compiled = compile_curation(tree, _prepared(), (scope,), subset=True)
    ref = f"curation/registers/scb/{register_file.name} [[variable]] entry 1"
    assert not [
        issue for issue in compiled.diagnostics if issue.code == "stale_curation_entry"
    ]
    assert compiled.report["scb/sample"]["not_evaluated"] == [ref]
    assert compiled.report["scb/sample"]["stale"] == []
    assert declared_column in merge_scope(scope, compiled).naming

    register_file.write_text(
        register_file.read_text()
        + '\n[[variable]]\nnative_id = "1.Missing"\nslug = "missing"\n'
    )
    compiled = compile_curation(
        load_curation_tree(root), _prepared(), (scope,), subset=True
    )
    assert compiled.report["scb/sample"]["not_evaluated"] == [ref]
    assert [(issue.code, issue.subject) for issue in compiled.diagnostics] == [
        ("stale_curation_entry", "1.Missing")
    ]


def test_thin_default_variant_carries_panel_fields(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    register_id = mint("fk", "activity")
    variant_id = mint("fk", "activity", "_default")
    path = root / "registers" / "fk" / "activity.toml"
    path.parent.mkdir(parents=True)
    path.write_text(
        '[register]\nprovider = "fk"\nslug = "activity"\n'
        f'native_id = "{register_id}"\n'
        "[[variant]]\n"
        f'native_id = "{register_id}.{variant_id}"\n'
        'slug = "_default"\ndisplay_group = "Activity"\n'
        'panel_entity_key = "person"\npanel_time_key = "period"\n'
        'panel_time_grain = "delivery"\n'
    )
    coordinate = SourceCoordinate(status="value", native_id="activity")
    record = SimpleNamespace(
        source="fk-source",
        subject=SimpleNamespace(
            provider="fk",
            register_name=coordinate,
            variant=SourceCoordinate(status="not_applicable"),
        ),
        parent_facts=(
            SimpleNamespace(kind="register", register_name=coordinate, variant=None),
        ),
    )

    class Reader:
        def iter_native_families(self, source):
            return iter(())

        def iter_records(self, *, source):
            return iter((record,))

        def iter_register_slices(self, source, registers):
            return iter(((None, (record,)),))

    native_register = source_register_key(cast("Any", record))
    assert native_register is not None
    scope = ScopeDeclarations(
        source=record.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register",
                    provider="fk",
                    source_key=native_register,
                ),
                naming=SlugEntry(
                    kind="register",
                    provider="fk",
                    source_id=str(register_id),
                    slug="activity",
                ),
                contributors=(),
            ),
        ),
    )
    names, variants, _, diagnostics, _ = compile_native_naming(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=Reader())),
        (scope,),
        subset=True,
    )
    assert not diagnostics
    default_key = (*native_register, "variant", "not-applicable")
    assert names[(record.source, None)][1].target.source_key == default_key
    assert variants[(record.source, None)][0][1] == ResolvedVariant(
        slug="_default",
        name="_default",
        display_group="Activity",
        panel_entity_key="person",
        panel_time_key="period",
        panel_time_grain="delivery",
    )
    record = SimpleNamespace(
        source=record.source,
        subject=SimpleNamespace(
            provider="fk",
            register_name=coordinate,
            variant=SourceCoordinate(status="value", native_id="subset"),
        ),
        parent_facts=(
            *record.parent_facts,
            SimpleNamespace(
                kind="variant",
                register_name=coordinate,
                variant=SourceCoordinate(status="value", native_id="subset"),
            ),
        ),
    )
    _, _, _, stale, _ = compile_native_naming(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=Reader())),
        (scope,),
        subset=True,
    )
    assert any(
        issue.code == "stale_curation_entry"
        and issue.subject == f"{register_id}.{variant_id}"
        for issue in stale
    )
    path.write_text(path.read_text().split("[[variant]]")[0])
    record = SimpleNamespace(
        source=record.source,
        subject=SimpleNamespace(
            provider="fk",
            register_name=coordinate,
            variant=SourceCoordinate(status="not_applicable"),
        ),
        parent_facts=record.parent_facts[:1],
    )
    _, _, _, missing, _ = compile_native_naming(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=Reader())),
        (scope,),
        subset=True,
    )
    assert any(
        issue.code == "stale_curation_entry"
        and "no tracked default slug" in issue.detail
        for issue in missing
    )


def test_stored_split_naming_is_replaced_by_compiled_partition():
    parent = _scope().naming[0]
    native = (
        "scb-registerinformation",
        "scb",
        "register",
        "native-int",
        1,
        "variable",
        "native-int",
        5,
    )
    split_key = (*native, "accepted-partition", "first")
    split = NamingDeclaration(
        target=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=split_key,
            register_key=native[:5],
            identity_revision=_revision("scb-registerinformation"),
        ),
        naming=SlugEntry(
            kind="variable", provider="scb", source_id="1.5.first", slug="first"
        ),
        contributors=(),
    )
    scope = _scope().model_copy(
        update={
            "naming": (parent, split),
            "provider_keys": ((split_key, "5.first"),),
        }
    )
    compiled = CompiledCuration(
        fields={},
        cases={},
        report={},
        naming={(scope.source, scope.register_key): (parent,)},
        variants={(scope.source, scope.register_key): ()},
    )
    merged = merge_scope(scope, compiled)
    assert merged.naming == (parent,)
    assert merged.provider_keys == ()


def test_stored_period_family_provider_key_is_removed_by_compiled_merge():
    parent = _scope().naming[0]
    native_key = (*parent.target.source_key, "variable", "native-int", 5)
    native = NamingDeclaration(
        target=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=native_key,
            register_key=parent.target.source_key,
            identity_revision=_revision("scb-registerinformation"),
        ),
        naming=SlugEntry(
            kind="variable", provider="scb", source_id="1.5", slug="value"
        ),
        contributors=(),
    )
    period_key = ("curation", "period-family", "scb", 1, "value")
    period = NamingDeclaration(
        target=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=period_key,
            register_key=parent.target.source_key,
            identity_revision=_revision("scb-registerinformation"),
        ),
        naming=SlugEntry(
            kind="variable",
            provider="scb",
            source_id="accepted-period-family:5",
            slug="period-value",
        ),
        contributors=(),
    )
    scope = _scope().model_copy(
        update={
            "naming": (parent, native, period),
            "provider_keys": ((native_key, "5"), (period_key, "period-value")),
        }
    )
    compiled = CompiledCuration(
        fields={},
        cases={},
        report={},
        naming={(scope.source, scope.register_key): (parent, native)},
        variants={(scope.source, scope.register_key): ()},
    )
    merged = merge_scope(scope, compiled)
    assert merged.naming == (parent, native)
    assert merged.provider_keys == ()


def test_tree_hash_covers_curation_and_transitional_slug_files(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    first = tree_sha256(root)
    tags = root / "tags.toml"
    tags.write_text(tags.read_text() + "# editorial change\n")
    second = tree_sha256(root)
    assert second != first
    slugs = tmp_path / "fqid_slugs"
    slugs.mkdir()
    (slugs / "scb.toml").write_text('[register."1"]\nslug = "sample"\n')
    assert tree_sha256(root) != second


def test_stored_case_with_unowned_prefix_fails(tmp_path):
    compiled = compile_curation(_tree(tmp_path / "curation"), _prepared(), (_scope(),))
    scope = _scope().model_copy(
        update={
            "cases": (
                CurationCase(
                    case_id="unowned",
                    targets=(),
                    decision=AcknowledgeDecision(
                        code="error",
                        subject="scb/sample/one",
                        refs=(),
                        register_key=("scb-source", "scb", "register", "native-int", 1),
                        reason="Reviewed",
                        evidence="Ledger",
                    ),
                ),
            )
        }
    )
    with pytest.raises(ValueError, match="unowned or ambiguous prefix"):
        merge_scope(scope, compiled)


def test_stored_v18b_case_and_gap_families_are_owned():
    assert "classification_bindings" in COMPILED
    assert "sos_thin" in COMPILED
    assert "matrix_repr" in COMPILED
    assert (
        _naming_family(
            NativeNamingTarget(
                kind="variable",
                provider="fohm",
                source_key=(
                    "Folkhalsomyndigheten/fohm.toml",
                    "fohm",
                    "register",
                    "native-str",
                    "nvr",
                    "variable",
                    "native-str",
                    "dosnummer",
                ),
                register_key=(
                    "Folkhalsomyndigheten/fohm.toml",
                    "fohm",
                    "register",
                    "native-str",
                    "nvr",
                ),
            )
        )
        == "sos_thin"
    )
    cases = {
        "accepted-column-partitions:scb-registerinformation:1.2": "partition",
        "accepted-coding:46:0": "coding",
        "accepted-alias-window:5:identity": "matrix_repr",
        f"accepted-classification-seed:{'a' * 64}": "classification_bindings",
        "scb_errata.toml/column/1": "errata",
        "accepted-errata:sos-declared-flags": "errata",
        "accepted-authored:Folkhalsomyndigheten/fohm.toml:nvr:dosnummer": "sos_thin",
        "delivery_enrichment.generated.toml/description/1": "annotations",
        "accepted-sos-routes:lss:ALDER": "sos_thin",
        "scb_errata.toml/delivered/1": "errata",
        "accepted-classification-override:1:0": "classification_bindings",
        "accepted-codeless:46:0": "coding",
        "delivery_enrichment.generated.toml/alias/1": "annotations",
        "accepted-period-family:5:identity": "matrix_repr",
        "accepted-sos-identity:bu:FOD_DATUMN": "partition",
        "accepted-cis2014-answers": "matrix_repr",
        "accepted-cis2016-answers": "matrix_repr",
        "existing-source-use:Socialstyrelsen/Metadata_Förteckning legitimerade": "sos_thin",
    }
    assert {case_id: _case_family(case_id) for case_id in cases} == cases
    assert (
        _naming_family(
            NativeNamingTarget(
                kind="variable",
                provider="scb",
                source_key=(
                    "scb-registerinformation",
                    "scb",
                    "register",
                    "native-int",
                    257,
                    "accepted-matrix",
                    "case",
                    "answer",
                ),
                register_key=(
                    "scb-registerinformation",
                    "scb",
                    "register",
                    "native-int",
                    257,
                ),
            )
        )
        == "matrix_repr"
    )
    assert (
        _naming_family(
            NativeNamingTarget(
                kind="variable",
                provider="scb",
                source_key=("curation", "period-family", "scb", "lisa", "lonfink"),
                register_key=(
                    "scb-registerinformation",
                    "scb",
                    "register",
                    "native-int",
                    34,
                ),
            )
        )
        == "matrix_repr"
    )
    gaps = {
        "curation/delivery_enrichment.generated.toml": "annotations",
        "curation/classifications.toml": "classification_bindings",
        "curation/codeless_overlap.toml": "coding",
    }
    assert {
        dataset: _gap_family(
            SimpleNamespace(revision=SimpleNamespace(dataset=dataset), pointer="/1")
        )
        for dataset in gaps
    } == gaps
    with pytest.raises(ValueError, match="unowned or ambiguous prefix"):
        _case_family("accepted-cis2014-answers-extra")


def _case_record(
    *,
    provider: str,
    register: str,
    variant: str | None = None,
    variable: str | None = None,
    parent: Literal["register", "variant"] | None = None,
    fields: SourceFields | None = None,
    references: tuple[str, ...] = (),
) -> SourceRecord:
    source = "Socialstyrelsen/test.xlsx" if provider == "sos" else "Agency/test.toml"
    reg = SourceCoordinate(
        status="value",
        name=register if provider == "sos" else None,
        native_id=register if provider != "sos" else None,
    )
    var = (
        SourceCoordinate(
            status="value",
            name=variant if provider == "sos" else None,
            native_id=variant if provider != "sos" else None,
        )
        if variant is not None
        else SourceCoordinate(status="not_applicable")
    )
    col = (
        SourceCoordinate(status="value", native_id=variable)
        if variable
        else SourceCoordinate(status="not_applicable")
    )
    subject = SourceSubject(
        provider=provider,
        register=reg,
        variant=var,
        variant_references=tuple(
            SourceCoordinate(status="value", native_id=item) for item in references
        ),
        population=SourceCoordinate(status="unknown"),
        variable=col,
        member=col,
        native=NativeCoordinates(),
    )
    facts = ()
    cells = ()
    if parent is not None:
        coordinate = reg if parent == "register" else var
        parent_fields = fields or SourceFields(name=value_field(variant or register))
        names = tuple(
            name
            for name in SourceFields.model_fields
            if getattr(parent_fields, name) is not None
        )
        cells = tuple(
            DeliveredCell(
                name=name,
                present=True,
                raw_value=str(getattr(parent_fields, name).value),
                interpreted_value=str(getattr(parent_fields, name).value),
            )
            for name in names
        )
        facts = (
            SourceParentObservation(
                kind=parent,
                coordinate=coordinate,
                register=reg,
                variant=var if parent == "variant" else None,
                fields=parent_fields,
                field_cells=tuple(
                    SourceFieldCells(field=name, positions=(index,))
                    for index, name in enumerate(names)
                ),
            ),
        )
    semantic = (
        f"register:{register}",
        f"variant:{variant}",
        f"variable:{variable}",
        f"parent:{parent}",
    )
    return SourceRecord.create(
        revision=_revision(source, artifact_path=source),
        locators=(
            RecordLocator(
                semantic_record_key=semantic,
                physical_file=source,
                physical_table="fixture",
                physical_record=repr(semantic),
                physical_cells=tuple(
                    f"fixture.{name}" for name in (cell.name for cell in cells)
                ),
            ),
        ),
        subject=subject,
        edition_scope=TemporalScope(kind="not_applicable"),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=fields or SourceFields(),
        parent_facts=facts,
        delivered_cells=cells,
    )


def _route_register(*routes):
    return SimpleNamespace(
        register_info=SimpleNamespace(slug="sample"),
        source_file="curation/registers/sos/sample.toml",
        identity=SimpleNamespace(
            route=tuple(
                SimpleNamespace(deldatamangd=token, variants=names)
                for token, names in routes
            )
        ),
    )


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
        def iter_native_families(self, source):
            return iter(((native_variable_key(variable), (variable,)),))

        def iter_records(self, *, source):
            return iter(records)

    register_key = source_register_key(variable)
    assert register_key is not None
    scope = ScopeDeclarations(
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
        cast("Any", SimpleNamespace(records=Reader())),
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


def test_sos_variantless_workbook_routes_to_default():
    record = _case_record(
        provider="sos", register="Book", variant="TOKEN", variable="COL"
    )
    cases, diagnostics, _ = _compile_sos_register(_route_register(), (record,))
    assert diagnostics == ()
    assert len(cases) == 1
    assert isinstance(cases[0].decision, OccurrenceCorrectionDecision)
    effect = cases[0].decision.effects[0]
    assert isinstance(effect, CheckedVariantAssignment)
    assert effect.variant_keys == (
        (
            "Socialstyrelsen/test.xlsx",
            "sos",
            "register",
            "name",
            "Book",
            "variant",
            "not-applicable",
        ),
    )


def test_sos_token_routes_to_two_native_parents_and_unmatched_route_is_stale():
    record = _case_record(
        provider="sos", register="Book", variant="TOKEN", variable="COL"
    )
    parents = tuple(
        _case_record(provider="sos", register="Book", variant=name, parent="variant")
        for name in ("A", "B")
    )
    register = _route_register(("TOKEN", ("A", "B")), ("MISSING", ("A",)))
    cases, diagnostics, statuses = _compile_sos_register(register, (*parents, record))
    assert len(cases) == 1
    assert isinstance(cases[0].decision, OccurrenceCorrectionDecision)
    effect = cases[0].decision.effects[0]
    assert isinstance(effect, CheckedVariantAssignment)
    assert {key[-1] for key in effect.variant_keys} == {"A", "B"}
    assert [issue.code for issue in diagnostics] == ["stale_curation_entry"]
    assert statuses["stale"] == ["curation/registers/sos/sample.toml#/identity.route/2"]


def test_sos_styrtabell_requires_both_lookup_signals():
    parent = _case_record(
        provider="sos",
        register="Book",
        variant="A",
        parent="variant",
        fields=SourceFields(name=value_field("Styrtabell A")),
    )
    _, diagnostics, _ = _compile_sos_register(_route_register(), (parent,))
    assert [issue.code for issue in diagnostics] == ["invalid_sos_lookup_signals"]


def test_sos_lookup_parent_and_routed_variable_become_source_uses():
    parent = _case_record(
        provider="sos",
        register="Book",
        variant="A",
        parent="variant",
        fields=SourceFields(
            name=value_field("Styrtabell A"),
            aggregation_level=value_field("Ej relevant"),
        ),
    )
    variable = _case_record(
        provider="sos", register="Book", variant="TOKEN", variable="COL"
    )
    cases, diagnostics, _ = _compile_sos_register(
        _route_register(("TOKEN", ("A",))), (parent, variable)
    )
    assert diagnostics == ()
    assert len(cases) == 1
    assert cases[0].case_id.startswith("existing-source-use:")
    assert len(cases[0].targets) == 2
    assert isinstance(cases[0].decision, OccurrenceCorrectionDecision)
    assert {effect.kind for effect in cases[0].decision.effects} == {"source_use"}


def test_thin_intersects_raw_windows_and_rejects_inversion():
    register = _case_record(
        provider="fk",
        register="r",
        parent="register",
        fields=SourceFields(
            coverage_from=value_field("2000-01-01"),
            coverage_to=value_field("2020-12-31"),
        ),
    )
    variant = _case_record(
        provider="fk",
        register="r",
        variant="v",
        parent="variant",
        fields=SourceFields(
            coverage_from=value_field("2010-01-01"),
            coverage_to=value_field("2018-12-31"),
        ),
    )
    variable = _case_record(
        provider="fk",
        register="r",
        variable="col",
        references=("v",),
        fields=SourceFields(
            coverage_from=value_field("2005-01-01"),
            coverage_to=value_field("2019-12-31"),
        ),
    )
    (case,) = _compile_thin_register((register, variant, variable), ())
    assert isinstance(case.decision, OccurrenceCorrectionDecision)
    addition = case.decision.effects[1]
    assert isinstance(addition, CuratedOccurrenceAddition)
    assert [
        (part.start, part.end) for part in addition.edition_period_scope.intervals
    ] == [("2010-01-01", "2018-12-31")]
    inverted = variant.model_copy(
        update={
            "parent_facts": (
                variant.parent_facts[0].model_copy(
                    update={
                        "fields": SourceFields(coverage_from=value_field("2021-01-01"))
                    }
                ),
            )
        }
    )
    with pytest.raises(ValueError, match="inverted thin coverage"):
        _compile_thin_register((register, inverted, variable), ())


def test_thin_copies_coding_from_its_own_declared_list(monkeypatch):
    register = _case_record(
        provider="fk",
        register="r",
        parent="register",
        fields=SourceFields(coverage_from=value_field("2010-01-01")),
    )
    variable = _case_record(
        provider="fk",
        register="r",
        variable="col",
        fields=SourceFields(value_set_declared=value_field("own-list")),
    )
    claim = CodeListClaim(
        claim_id="own-list", scope=TemporalScope(kind="year_independent"), members=()
    )

    def own_list(record, sessions, *, scope):
        assert record is variable
        assert scope.intervals[0].start == "2010-01-01"
        return SimpleNamespace(claims=(claim,))

    monkeypatch.setattr("reg_meta_build.curation_compile.bind_code_lists", own_list)
    (case,) = _compile_thin_register((register, variable), ())
    assert isinstance(case.decision, OccurrenceCorrectionDecision)
    addition = case.decision.effects[1]
    assert isinstance(addition, CuratedOccurrenceAddition)
    assert addition.copy_coding
    assert addition.expected_codings == copied_coding_fingerprints((claim,))
    assert addition.fields.value_set_declared is not None
    assert addition.fields.value_set_declared.value == "own-list"


def _partition_scope(records: tuple[SourceRecord, ...]) -> ScopeDeclarations:
    first = records[0]
    register = source_register_key(first)
    assert register is not None
    provider = first.subject.provider
    register_id = str(register[-1]) if provider == "scb" else "5891427617861710725"
    slug = "sample" if provider == "scb" else "par"
    return ScopeDeclarations(
        source=first.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register",
                    provider=provider,
                    source_key=register,
                ),
                naming=SlugEntry(
                    kind="register",
                    provider=provider,
                    source_id=register_id,
                    slug=slug,
                ),
                contributors=(),
            ),
        ),
    )


def _compile_partition_fixture(
    root: Path,
    records: tuple[SourceRecord, ...],
    *,
    shuffled: bool = False,
):
    native = native_variable_key(records[0])
    assert native is not None
    reader = SimpleNamespace(
        iter_native_families=lambda source: iter(((native, records),))
    )
    scope = _partition_scope(records)
    tree = load_curation_tree(root)
    if shuffled:
        tree = replace(tree, registers=tuple(reversed(tree.registers)))
    return (
        compile_partitions(
            tree,
            cast("Any", SimpleNamespace(records=reader)),
            (scope,),
        ),
        (scope.source, None),
        native,
    )


def _scb_partition_records(
    columns: tuple[str, ...],
    *,
    variants: tuple[int, ...] | None = None,
    register_id: int = 1,
):
    header = REGISTERINFORMATION_HEADER.split("|")
    revision = _revision("scb-registerinformation")
    records = []
    for index, column in enumerate(columns, 1):
        row = _var_row(
            cvid=20 + index,
            var_id=5,
            colname=column,
            register=(
                "TEST",
                register_id,
                (variants or (2,) * len(columns))[index - 1],
            ),
        ).split("|")
        cells = {
            name: (True, value, value) for name, value in zip(header, row, strict=True)
        }
        records.append(clean_scb_row(header, index, cells, revision).record)
    return tuple(records)


def _scb_partition_tree(root: Path, extra: str):
    tree = _tree(root)
    path = root / "registers" / "scb" / "sample.toml"
    path.write_text(path.read_text() + extra, encoding="utf-8")
    return tree


@pytest.mark.parametrize(
    "columns,expected_stale",
    [
        (("ANSWER", "OTHER"), False),
        (("ANSWER", "OTHER", "MISSING"), True),
        (("ANSWER",), True),
    ],
)
def test_tracked_partition_map_checks_every_literal(
    tmp_path: Path,
    columns: tuple[str, ...],
    expected_stale: bool,
):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
        '[[variable]]\nnative_id = "1.5.other"\nslug = "other"\n'
        '[[identity.partition]]\nvariable = "1.5"\n'
        'columns = { ANSWER = "1.5.answer", OTHER = "1.5.other" }\n'
        'columns_ref = "fixture literal map"\n',
    )
    records = _scb_partition_records(columns)
    compiled, key, native = _compile_partition_fixture(root, records)
    cases, naming, keys, ambiguities, bases, issues = compiled
    assert native in bases[key]
    assert (
        any(issue.code == "stale_curation_entry" for issue in issues) == expected_stale
    )
    assert bool(cases.get(key)) != expected_stale
    assert bool(naming[key]) != expected_stale
    assert (
        (native, None) in keys[key]
        if expected_stale
        else (native, None) not in keys[key]
    )
    assert bool(ambiguities.get(key)) == expected_stale


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
    cases, _, keys, _, _, _ = compiled
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
    cases, naming, keys, ambiguities, _, _ = compiled
    assert {item.naming.source_id for item in naming[key]} == {"1.5.answer"}
    assert (native, None) in keys[key]
    assert len(ambiguities[key]) == 1
    assert ambiguities[key][0].candidate_columns == (("1.5.answer", "ANSWER"),)
    assert (
        apply_occurrence_cases(records, cases[key]).occurrences[1].variable_key
        == native
    )


@pytest.mark.parametrize("register_id", [1, 258])
def test_tracked_partition_compiles_without_stored_case(
    tmp_path: Path, register_id: int
):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        f'\n[[variable]]\nnative_id = "{register_id}.5.answer"\nslug = "answer"\n'
        f'[[identity.partition]]\nvariable = "{register_id}.5"\n'
        f'columns = {{ ANSWER = "{register_id}.5.answer" }}\n'
        'unassigned_columns = ["LEFT"]\ncolumns_ref = "fixture map"\n',
    )
    if register_id == 258:
        path = root / "registers" / "scb" / "sample.toml"
        path.write_text(
            path.read_text().replace('native_id = "1"', 'native_id = "258"'),
            encoding="utf-8",
        )
    records = _scb_partition_records(("ANSWER", "LEFT"), register_id=register_id)
    compiled, key, native = _compile_partition_fixture(root, records)
    cases, naming, keys, ambiguities, bases, issues = compiled
    assert len(cases[key]) == 1
    assert cases[key][0].case_id == (
        f"accepted-column-partitions:scb-registerinformation:{register_id}.5"
    )
    assert {item.naming.source_id for item in naming[key]} == {
        f"{register_id}.5.answer"
    }
    assert (native, None) in keys[key]
    assert not ambiguities.get(key)
    assert native in bases[key]
    assert [issue.code for issue in issues] == ["unassigned_original_columns"]


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
    assert [issue.code for issue in compiled[5]] == ["stale_curation_entry"]
    assert "#/identity.partition/1" in compiled[5][0].detail


def test_partition_ambiguity_does_not_depend_on_stored_inventory(tmp_path: Path):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
        '[[variable]]\nnative_id = "1.5.unknown"\nslug = "unknown"\n',
    )
    records = _scb_partition_records(("ANSWER", "LEFT"))
    generated, key, native = _compile_partition_fixture(root, records)
    scope = _partition_scope(records).model_copy(
        update={"naming_ambiguities": generated[3][key]}
    )
    reader = SimpleNamespace(
        iter_native_families=lambda source: iter(((native, records),))
    )
    compiled = compile_partitions(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=reader)),
        (scope,),
    )
    assert compiled == generated
    assert generated[0][key]
    assert generated[3][key][0].candidate_columns == (("1.5.answer", "ANSWER"),)
    assert native in generated[4][key]


def test_merge_keeps_untouched_ambiguity_and_unresolved_key(tmp_path: Path):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
        '[[variable]]\nnative_id = "1.5.unknown"\nslug = "unknown"\n',
    )
    records = _scb_partition_records(("ANSWER", "LEFT"))
    generated, key, native = _compile_partition_fixture(root, records)
    ambiguity = generated[3][key][0]
    scope = _partition_scope(records).model_copy(
        update={
            "provider_keys": ((native, None),),
            "naming_ambiguities": (ambiguity,),
        }
    )
    compiled = CompiledCuration(
        fields={},
        cases={},
        report={},
        provider_keys={key: ((native, "5"),)},
        naming_ambiguities={key: ()},
        partition_bases={},
    )
    merged = merge_scope(scope, compiled)
    assert merged.provider_keys == ((native, None),)
    assert merged.naming_ambiguities == (ambiguity,)

    replaced = merge_scope(
        scope,
        replace(compiled, partition_bases={key: frozenset((native,))}),
    )
    assert replaced.provider_keys == ((native, "5"),)
    assert replaced.naming_ambiguities == ()


def test_variant_scoped_column_owner_only_binds_its_variant(tmp_path: Path):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.2"\n'
        'column = "ANSWER"\nowner = "1.5.answer"\nref = "fixture variant"\n',
    )
    records = _scb_partition_records(("ANSWER", "ANSWER"), variants=(2, 3))
    compiled, key, native = _compile_partition_fixture(root, records)
    cases, _, keys, _, _, _ = compiled
    assert (native, None) in keys[key]
    corrected = apply_occurrence_cases(records, cases[key]).occurrences
    assert corrected[0].variable_key != native
    assert corrected[1].variable_key == native


def _sos_partition_records(*, rename: bool = False) -> tuple[SourceRecord, ...]:
    source = "Socialstyrelsen/Metadata_Patientregistret (PAR)_webb.xlsx"
    revision = _revision(source)
    register = SourceCoordinate(status="value", name="Patientregistret")
    variant = SourceCoordinate(status="value", name="PAR_OV")
    variable = SourceCoordinate(
        status="value", native_id="INVARN8" if rename else "ATC"
    )
    types = ("integer",) if rename else ("integer", "text")
    records = []
    for index, data_type in enumerate(types, 1):
        subject = SourceSubject(
            provider="sos",
            register=register,
            variant=variant,
            population=SourceCoordinate(status="not_applicable"),
            variable=variable,
            member=SourceCoordinate(status="value", native_id=str(index)),
            native=NativeCoordinates(),
        )
        locator = RecordLocator(
            semantic_record_key=(f"member:{index}",),
            physical_file="fixture.xlsx",
            physical_table="PAR_OV",
            physical_record=str(index),
            physical_cells=(),
        )
        records.append(
            SourceRecord.create(
                revision=revision,
                locators=(locator,),
                subject=subject,
                edition_scope=TemporalScope(kind="not_applicable"),
                edition_period_scope=TemporalScope(kind="not_applicable"),
                fields=SourceFields(
                    column_name=value_field("INVARN8" if rename else "ATC"),
                    name=value_field("target" if rename else "ATC"),
                    data_type=value_field(data_type),
                ),
            )
        )
    return tuple(records)


@pytest.mark.parametrize("rename", [False, True])
def test_sos_type_split_and_name_rename(tmp_path: Path, rename: bool):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers" / "sos" / "par.toml"
    path.parent.mkdir(parents=True)
    base = '[register]\nprovider = "sos"\nslug = "par"\nnative_id = "5891427617861710725"\nname = "Patientregistret"\n'
    if rename:
        extra = (
            '[[identity.rename]]\ndeldatamangd = "PAR_OV"\nvariable = "INVARN8"\n'
            'name = "target"\ncolumn = "INVARN9"\n'
            '[[variable]]\nnative_id = "5891427617861710725.INVARN9"\nslug = "new-name"\n'
        )
    else:
        extra = (
            '[[identity.split]]\nvariable = "ATC"\nby = "data_type"\n'
            'parts = [{ data_type = "integer", owner = "5891427617861710725.ATC.atc" }, '
            '{ data_type = "text", owner = "5891427617861710725.ATC.atc-1" }]\n'
            '[[variable]]\nnative_id = "5891427617861710725.ATC.atc"\nslug = "atc"\n'
            '[[variable]]\nnative_id = "5891427617861710725.ATC.atc-1"\nslug = "atc-1"\n'
        )
    path.write_text(base + extra, encoding="utf-8")
    records = _sos_partition_records(rename=rename)
    compiled, key, native = _compile_partition_fixture(root, records)
    cases, naming, keys, _, bases, issues = compiled
    assert not issues
    assert len(cases[key]) == 1
    assert len(naming[key]) == (1 if rename else 2)
    assert native not in bases[key] if rename else native in bases[key]
    corrected = apply_occurrence_cases(records, cases[key]).occurrences
    assert all(item.variable_key != native for item in corrected)
    if rename:
        assert corrected[0].fields.column_name.value == "INVARN9"
        reader = SimpleNamespace(
            iter_native_families=lambda source: iter(((native, records),)),
            iter_records=lambda **kwargs: iter(records),
            iter_register_slices=lambda source, wanted: iter(((None, records),)),
        )
        names = compile_native_naming(
            load_curation_tree(root),
            cast("Any", SimpleNamespace(records=reader)),
            (_partition_scope(records),),
            subset=True,
        )
        assert not any(
            issue.code == "stale_curation_entry" and "INVARN9" in issue.detail
            for issue in names[3]
        )
    else:
        assert corrected[0].variable_key != corrected[1].variable_key
    assert all(value is not None for _, value in keys[key])


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
        cases, naming, keys, ambiguities, bases, diagnostics = value
        compiled = CompiledCuration(
            fields={},
            cases=cases,
            report={},
            naming=naming,
            provider_keys=keys,
            naming_ambiguities=ambiguities,
            diagnostics=diagnostics,
        )
        return _bytes(compiled), bases

    assert encode(first) == encode(second) == encode(shuffled)


def test_partition_replaces_stored_scope_declarations_in_compiled_selection(
    tmp_path: Path,
):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root, '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
    )
    records = _scb_partition_records(("ANSWER", "LEFT"))
    register = source_register_key(records[0])
    assert register is not None
    scope = _partition_scope(records)
    scope = scope.model_copy(
        update={
            "cases": (
                CurationCase(
                    case_id="accepted-column-partitions:scb-registerinformation:1.5",
                    targets=(),
                    decision=AcknowledgeDecision(
                        code="fixture",
                        subject="scb/sample/answer",
                        refs=(),
                        register_key=register,
                        reason="Stored fixture",
                        evidence="fixture",
                    ),
                ),
            )
        }
    )
    native = native_variable_key(records[0])
    assert native is not None
    prepared = _prepared()

    class Reader:
        def iter_native_families(self, source):
            return iter(((native, records),))

        def iter_records(self, *, source):
            return iter(records)

        def iter_register_slices(self, source, registers):
            return iter(((None, records),))

    prepared.records = Reader()
    compiled = compile_curation(
        load_curation_tree(root), prepared, (scope,), subset=True
    )
    merged = merge_scope(scope, compiled)
    assert len(merged.cases) == 1
    assert {
        item.naming.source_id
        for item in merged.naming
        if item.target.kind == "variable"
    } == {"1.5.answer"}
    assert (native, None) in merged.provider_keys
    assert len(merged.naming_ambiguities) == 0


def test_compiled_classification_family_drops_both_stored_case_prefixes(tmp_path):
    compiled = compile_curation(_tree(tmp_path / "curation"), _prepared(), (_scope(),))
    register_key = ("scb-source", "scb", "register", "native-int", 1)
    cases = tuple(
        CurationCase(
            case_id=case_id,
            targets=(),
            decision=AcknowledgeDecision(
                code="fixture",
                subject="scb/sample/one",
                refs=(),
                register_key=register_key,
                reason="Reviewed",
                evidence="Ledger",
            ),
        )
        for case_id in (
            f"accepted-classification-seed:{'a' * 64}",
            "accepted-classification-override:1:0",
        )
    )
    scope = _scope().model_copy(update={"cases": cases})
    assert merge_scope(scope, compiled).cases == ()


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
    register = scope.naming[0].target.source_key
    variable = NamingDeclaration(
        target=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=(*register, "variable", 1),
            register_key=register,
            identity_revision=_revision(scope.source),
        ),
        naming=SlugEntry(kind="variable", provider="scb", source_id="1.1", slug="one"),
        contributors=(),
    )
    named = scope.model_copy(update={"naming": (*scope.naming, variable)})
    matched = compile_curation(tree, _prepared(), (named,), subset=True)
    assert matched.report["_classifications"]["entries_matched"][-1] == ref
    assert not matched.diagnostics
    duplicate = named.model_copy(update={"source": "other"})
    overbroad = compile_curation(tree, _prepared(), (named, duplicate), subset=True)
    assert overbroad.report["_classifications"]["stale"] == [ref]
    assert [issue.code for issue in overbroad.diagnostics] == [
        "stale_curation_entry",
        "overbroad_curation_entry",
    ]


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


def test_acknowledgement_owning_two_scopes_is_overbroad(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    register = root / "registers" / "scb" / "sample.toml"
    register.write_text(
        register.read_text() + '\n[[acknowledge]]\ncode = "unresolved"\n'
        'subject = "exact source key"\nrefs = []\nreason = "Reviewed"\n'
        'evidence = "Ledger"\n'
    )
    other = _scope().model_copy(update={"source": "other"})
    result = compile_curation(
        load_curation_tree(root), _prepared(), (_scope(), other), subset=True
    )
    assert result.report["scb/sample"]["over_broad"] == [
        "curation/registers/scb/sample.toml#/acknowledge/1",
        "curation/registers/scb/sample.toml [register]",
    ]
    assert [item.code for item in result.diagnostics] == [
        "overbroad_curation_entry",
        "overbroad_curation_entry",
    ]
