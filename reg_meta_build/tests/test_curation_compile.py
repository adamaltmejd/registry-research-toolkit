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
    compile_curation,
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


def _revision(dataset: str) -> SourceRevision:
    return SourceRevision.create(
        dataset=dataset,
        publisher="fixture",
        purpose="fixture",
        upstream_revision="v1",
        artifact_path=f"{dataset}.csv",
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
                '[binding]\nvalue_set_labels = ["Unmatched label"]\n'
                '[[binding.variable]]\nvariable = "scb/other/one"\n'
                if short == "BETA"
                else ""
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
        SimpleNamespace(origin="snapshot", path=path, revision=_revision(dataset))
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
    result = compile_curation(tree, _prepared(), (_scope(),))
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
    assert metadata.variable_same_as == ()
    assert [member.target for member in metadata.tags[0].members] == ["scb/sample/one"]
    assert len(result.fields["code_label_pairs"]) == 1
    assert result.fields["lineage_defaults"] == (("scb/sample", "people"),)
    assert result.fields["identifier_sources"] == ("scb-identifierare",)
    assert result.fields["event_sources"] == (
        ("scb-timeseries", "scb-registerinformation"),
    )
    assert any("same_as" in item for item in result.report["_subset"]["dropped"])
    assert len(result.report["_classifications"]["not_evaluated_in_subset"]) == 2
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


def test_source_event_without_selected_target_is_reported(tmp_path):
    result = compile_curation(_tree(tmp_path / "curation"), _prepared(), ())
    assert result.fields["event_sources"] == ()
    assert result.report["_subset"]["dropped"] == sorted(
        result.report["_subset"]["dropped"]
    )
    assert (
        "prepared://scb-timeseries->scb-registerinformation"
        in result.report["_subset"]["dropped"]
    )


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
