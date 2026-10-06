"""Tracked curation tree, global families, coding registers, events and native naming compile for a selected scope."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any, cast

import pytest
from _curation_compile_support import (
    compiled_bytes as _bytes,
    make_naming_reader as _naming_reader,
    make_prepared as _prepared,
    make_revision as _revision,
    make_scope as _scope,
    make_tree as _tree,
)
from reg_meta.errors import RegMetaError
from reg_meta_build.curation_compile import (
    compile_curation,
    compile_native_naming,
    tree_sha256,
)
from reg_meta_build.curation_tree import (
    load_curation_tree,
)
from reg_meta_build.id import mint
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.resolved_catalog import ResolvedVariant
from reg_meta_build.source_coordinates import (
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    SourceCoordinate,
)

from reg_meta_build.fqid_slugs import SlugEntry


def test_acknowledgement_compiler_preserves_optional_evidence_guard(tmp_path):
    from reg_meta.source_evidence import SourceRecordRef
    from reg_meta_build.curation_tree import AcknowledgeEntry

    tree = _tree(tmp_path / "curation")
    register = next(r for r in tree.registers if r.register_info.slug == "sample")
    entry = AcknowledgeEntry(
        code="unresolved_native_identity",
        subject="scb/sample/one",
        refs=[
            SourceRecordRef(
                source="fixture", semantic_record_key=("one",)
            ).model_dump_json()
        ],
        reason="The source omits the physical matrix coordinates.",
        evidence="Complete original source family.",
        expected_evidence_sha256="a" * 64,
        expected_diagnostic_sha256="b" * 64,
    )
    tree = replace(
        tree,
        registers=tuple(
            r.model_copy(update={"acknowledge": [entry]}) if r is register else r
            for r in tree.registers
        ),
    )
    compiled = compile_curation(tree, _prepared(), (_scope(),), subset=True)
    (case,) = (
        case
        for cases in compiled.cases.values()
        for case in cases
        if case.decision.kind == "acknowledge"
    )
    assert case.decision.expected_evidence_sha256 == entry.expected_evidence_sha256
    assert case.decision.expected_diagnostic_sha256 == entry.expected_diagnostic_sha256
    with pytest.raises(ValueError):
        AcknowledgeEntry.model_validate(
            {**entry.model_dump(), "expected_evidence_sha256": "not-a-sha256"}
        )
    with pytest.raises(ValueError):
        AcknowledgeEntry.model_validate(
            {**entry.model_dump(), "expected_diagnostic_sha256": "not-a-sha256"}
        )


def test_global_families_and_manifest_wiring_compile_for_subset(tmp_path):
    tree = _tree(tmp_path / "curation")
    result = compile_curation(tree, _prepared(), (_scope(),), subset=True)
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
            role="scb_events" if path == "Timeseries.csv" else "scb_records",
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
        def iter_native_families(self, source, registers=None):
            return ((native_variable_key(item), (item,)) for item in records)

        def iter_records(self, *, source):
            return iter(records)

        def iter_register_slices(self, source, registers):
            return iter(((None, records),))

    key = ("scb-registerinformation", None)
    naming, variants, provider_keys, diagnostics, _ = compile_native_naming(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=_naming_reader(Reader()))),
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
        def iter_native_families(self, source, registers=None):
            return iter(())

        def iter_records(self, *, source):
            return iter((record,))

        def iter_register_slices(self, source, registers):
            return iter(((None, (record,)),))

    native_register = source_register_key(cast("Any", record))
    assert native_register is not None
    scope = CompiledScope(
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
        cast("Any", SimpleNamespace(records=_naming_reader(Reader()))),
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
        cast("Any", SimpleNamespace(records=_naming_reader(Reader()))),
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
        cast("Any", SimpleNamespace(records=_naming_reader(Reader()))),
        (scope,),
        subset=True,
    )
    assert any(
        issue.code == "stale_curation_entry"
        and "no tracked default slug" in issue.detail
        for issue in missing
    )


def test_tree_hash_covers_curation_and_source_slug_files(tmp_path):
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
