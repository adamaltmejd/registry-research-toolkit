"""The global hybrid families compile from tracked coordinates, without pins."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from reg_meta_build.curation_compile import (
    COMPILED,
    FAMILIES,
    _case_family,
    _gap_family,
    compile_curation,
    finalize_classification_bindings,
    merge_scope,
    merge_selection,
    tree_sha256,
)
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.pipeline import PipelineSelection, ScopeDeclarations
from reg_meta_build.source_curation import AcknowledgeDecision, CurationCase
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_records import SourceRevision

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
        manifest=SimpleNamespace(inputs=inputs), iter_evidence=lambda: iter(())
    )


def _bytes(compiled) -> bytes:
    return json.dumps(
        {
            "fields": compiled.fields,
            "cases": {repr(key): value for key, value in compiled.cases.items()},
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
        manifest=SimpleNamespace(inputs=inputs), iter_evidence=lambda: iter(())
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
    assert [issue.code for issue in overbroad.diagnostics] == ["stale_curation_entry"]


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
        "curation/registers/scb/sample.toml#/acknowledge/1"
    ]
    assert [item.code for item in result.diagnostics] == ["overbroad_curation_entry"]
