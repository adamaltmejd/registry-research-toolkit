"""The global hybrid families compile from tracked coordinates, without pins."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from reg_meta.errors import RegMetaError
from reg_meta_build.curation_compile import (
    COMPILED,
    FAMILIES,
    CompiledCuration,
    _case_family,
    _gap_family,
    compile_curation,
    compile_native_naming,
    finalize_classification_bindings,
    merge_scope,
    merge_selection,
    tree_sha256,
)
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.id import mint
from reg_meta_build.pipeline import PipelineSelection, ScopeDeclarations
from reg_meta_build.resolved_catalog import ResolvedVariant
from reg_meta_build.source_coordinates import native_variable_key, source_register_key
from reg_meta_build.source_curation import AcknowledgeDecision, CurationCase
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import SourceCoordinate, SourceRevision

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
        _tree(tmp_path / "curation"), prepared, scopes, subset=True
    )
    assert result.fields["event_sources"] == (
        ("timeseries-a", "register-a"),
        ("timeseries-b", "register-b"),
    )


def test_null_partition_base_provider_key_survives_hybrid_merge(tmp_path):
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


def test_stored_split_naming_survives_compiled_native_merge():
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
    assert merged.naming == (split, parent)
    assert merged.provider_keys == ((split_key, "5.first"),)


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
    cases = {
        "accepted-column-partitions:scb-registerinformation:1.2": "identity",
        "accepted-coding:46:0": "coding",
        "accepted-alias-window:5:identity": "representation",
        f"accepted-classification-seed:{'a' * 64}": "classification_bindings",
        "scb_errata.toml/column/1": "errata",
        "accepted-errata:sos-declared-flags": "errata",
        "accepted-authored:Folkhalsomyndigheten/fohm.toml:nvr:dosnummer": "thin_provider",
        "delivery_enrichment.generated.toml/description/1": "annotations",
        "accepted-sos-routes:lss:ALDER": "identity",
        "scb_errata.toml/delivered/1": "errata",
        "accepted-classification-override:1:0": "classification_bindings",
        "accepted-codeless:46:0": "coding",
        "delivery_enrichment.generated.toml/alias/1": "annotations",
        "accepted-period-family:5:identity": "representation",
        "accepted-sos-identity:bu:FOD_DATUMN": "identity",
        "accepted-cis2014-answers": "matrix",
        "accepted-cis2016-answers": "matrix",
        "existing-source-use:Socialstyrelsen/Metadata_Förteckning legitimerade": "identity",
    }
    assert {case_id: _case_family(case_id) for case_id in cases} == cases
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
