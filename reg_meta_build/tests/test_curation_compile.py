"""Compile tracked curation families from source coordinates."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal, cast

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row as _var_row
from reg_meta.errors import RegMetaError
from reg_meta.source_evidence import (
    DeliveredCell,
    RecordLocator,
    SourceRevision,
)
from reg_meta_build.catalog_resolution import resolve_parents
from reg_meta_build.curation_compile import (
    CompiledCuration,
    compile_coding_register,
    compile_curation,
    compile_deferred_partitions,
    compile_edition_splits,
    compile_enrichment,
    compile_errata,
    compile_native_naming,
    compile_occurrence_corrections,
    compile_partitions,
    compile_provider_declarations,
    compile_scb_preliminary,
    convert_column_partitions,
    finalize_classification_bindings,
    tree_sha256,
)
from reg_meta_build.curation_tree import (
    ErrataDataTypeEntry,
    ErrataFieldEntry,
    ErrataOccurrencePeriodEntry,
    load_curation_tree,
    load_register_files,
)
from reg_meta_build.id import mint
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.prepared_sources import (
    PreparedPartitionRecord,
)
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.scb_errata import ErrataVersion, edition_bindings
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_coordinates import (
    column_identity,
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CheckedIdentityChange,
    CuratedOccurrenceAddition,
    OccurrenceCorrectionDecision,
    SearchAliasDecision,
    SourceEvidence,
    capture_expectations,
    evaluate_cases,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_intervals import resolve_occurrence_intervals
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
    check_naming_target,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    CodeSetReference,
    NativeCoordinates,
    ScopeInterval,
    SourceCoordinate,
    SourceFieldCells,
    SourceFields,
    SourceParentObservation,
    SourceRecord,
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


def _scope() -> CompiledScope:
    return CompiledScope(
        source="scb-registerinformation",
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register",
                    provider="scb",
                    source_key=("scb", "register", 1),
                ),
                naming=SlugEntry(
                    kind="register", provider="scb", source_id="1", slug="sample"
                ),
                contributors=(),
            ),
        ),
    )


def _naming_reader(reader: Any) -> Any:
    reader.iter_naming_families = reader.iter_native_families

    def partitions(source, registers=None, select_family=None):
        return (
            (key, members)
            for key, members in reader.iter_native_families(source, registers)
            if select_family is None or select_family(key)
        )

    reader.iter_partition_families = partitions

    def slices(source, registers):
        if None in registers:
            return iter(((None, tuple(reader.iter_records(source=source))),))
        return reader.iter_register_slices(source, registers)

    reader.iter_naming_register_slices = slices
    return reader


def _prepared():
    class Records:
        def iter_native_families(self, source, registers=None):
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
                context=(),
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
            role=role,
            path=path,
            revision=_revision(dataset, artifact_path=f"snapshot-a:source/{path}"),
        )
        for path, dataset, role in (
            ("Identifierare.csv", "scb-identifierare", "scb_auxiliary"),
            ("Timeseries.csv", "scb-timeseries", "scb_events"),
            ("Registerinformation.csv", "scb-registerinformation", "scb_records"),
        )
    )
    return SimpleNamespace(
        manifest=SimpleNamespace(inputs=inputs),
        iter_evidence=lambda: iter(()),
        records=_naming_reader(Records()),
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


def _case_record(
    *,
    provider: str,
    register: str,
    variant: str | None = None,
    variable: str | None = None,
    parent: Literal["register", "variant"] | None = None,
    fields: SourceFields | None = None,
    references: tuple[str, ...] = (),
    source: str | None = None,
    code_set_references: tuple[CodeSetReference, ...] = (),
    delivered_cells: tuple[DeliveredCell, ...] = (),
) -> SourceRecord:
    source = source or (
        "Socialstyrelsen/test.xlsx" if provider == "sos" else "Agency/test.toml"
    )
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
    cells = delivered_cells
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
        code_set_references=code_set_references,
    )


def _route_register(*routes):
    return SimpleNamespace(
        register_info=SimpleNamespace(provider="sos", slug="sample"),
        source_file="curation/registers/sos/sample.toml",
        variant=[],
        identity=SimpleNamespace(
            route=tuple(
                SimpleNamespace(deldatamangd=token, variants=names)
                for token, names in routes
            )
        ),
        errata=SimpleNamespace(data_type=(), classification_reference=()),
    )


def _sos_type_register():
    register = _route_register()
    register.errata.data_type = (
        ErrataDataTypeEntry(
            deldatamangd="A_LOVA_HOSP",
            variable="DESLEG_DATUM",
            column="DESLEG_DATUM",
            expected_type="Decimal",
            expected_representation="YYYY-MM-DD",
            data_type="date",
            evidence="Workbook row 28 declares a date.",
            noted="2026-09-29",
        ),
    )
    return register


def _sos_type_record(
    *,
    column: str = "DESLEG_DATUM",
    data_type: str = "Decimal",
    representation: str = "YYYY-MM-DD",
) -> SourceRecord:
    return _case_record(
        provider="sos",
        register="LOVA",
        variant="A_LOVA_HOSP",
        variable="DESLEG_DATUM",
        fields=SourceFields(
            column_name=value_field(column),
            data_type=value_field(data_type),
            representation=value_field(representation),
        ),
    )


def test_sos_data_type_skipped_register_is_accounted_in_subset():
    register = _sos_type_register()
    tree = SimpleNamespace(registers=(register,))
    prepared = SimpleNamespace(
        value_sources=(), records=SimpleNamespace(), manifest=SimpleNamespace(inputs=())
    )
    case_id = "curation/registers/sos/sample.toml#/errata.data_type/1"
    cases, diagnostics, report = compile_provider_declarations(
        tree, prepared, (), subset=True
    )
    assert cases == {}
    assert diagnostics == ()
    assert report["sos/sample"]["entries_read"] == [case_id]
    assert report["sos/sample"]["not_evaluated_in_subset"] == [case_id]
    _, diagnostics, report = compile_provider_declarations(
        tree, prepared, (), subset=False
    )
    assert [issue.code for issue in diagnostics] == ["stale_curation_entry"]
    assert report["sos/sample"]["stale"] == [case_id]


def test_sos_data_type_compiles_only_selected_native_source_scope():
    record = _sos_type_record()
    register_key = source_register_key(record)
    assert register_key is not None
    register = _sos_type_register()
    tree = SimpleNamespace(registers=(register,))
    scope = CompiledScope(
        source=record.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register", provider="sos", source_key=register_key
                ),
                naming=SlugEntry(
                    kind="register", provider="sos", source_id="1", slug="sample"
                ),
                contributors=(),
            ),
        ),
    )
    other = record.model_copy(update={"source": "Socialstyrelsen/other.xlsx"})
    prepared = SimpleNamespace(
        value_sources=(),
        manifest=SimpleNamespace(inputs=()),
        records=SimpleNamespace(
            iter_records=lambda *, source: iter(
                item for item in (record, other) if item.source == source
            )
        ),
    )
    cases, diagnostics, report = compile_provider_declarations(
        tree, prepared, (scope,), subset=True
    )
    assert diagnostics == ()
    case = next(
        case
        for case in cases[record.source, None]
        if "/errata.data_type/" in case.case_id
    )
    assert case.peer_guards[0].source == record.source
    assert case.peer_guards[0].expected_members == (record_ref(record),)
    assert report["sos/sample"]["entries_matched"] == [case.case_id]


def _mfr_reference_register(variable: str):
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "curation"
    register = next(
        item
        for item in load_register_files(root)
        if item.register_info.provider == "sos" and item.register_info.slug == "mfr"
    )
    entry = next(
        item
        for item in register.errata.classification_reference
        if item.variable == variable
    )
    return register.model_copy(
        update={
            "errata": register.errata.model_copy(
                update={"classification_reference": (entry,)}
            )
        }
    ), entry


def test_mfr_reference_entries_exclude_missing_sheet_and_bdiag_consumers():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "curation"
    register = next(
        item
        for item in load_register_files(root)
        if item.register_info.provider == "sos" and item.register_info.slug == "mfr"
    )
    assert {
        (item.deldatamangd, item.variable)
        for item in register.errata.classification_reference
    } == {
        ("MFR_IVF", name)
        for name in ("BPNR", "BPSEUDO", "EMBRYON", "ETDATUM", "HINNSACK")
    } | {("MFR", name) for name in ("SECMARK", "SUGMARK", "TANGMARK")} | {
        (variant, "BPNRQ") for variant in ("MFR", "MFR_FOK", "MFR_IVF")
    } | {(variant, "MPNRQ") for variant in ("MFR", "MFR_FOK", "MFR_IVF", "MFR_LMED")}
    assert all(
        item.expected_reference == "Kodlista_förlossningssätt!A1"
        and item.expected_representation == "1 = ja" + " " * 538 + "0 = nej"
        for item in register.errata.classification_reference
        if item.deldatamangd == "MFR"
        and item.variable in {"SECMARK", "SUGMARK", "TANGMARK"}
    )


def test_mfr_reference_absent_full_source_vs_skipped_subset():
    register, _ = _mfr_reference_register("BPNR")
    tree = SimpleNamespace(registers=(register,))
    prepared = SimpleNamespace(
        value_sources=(), records=SimpleNamespace(), manifest=SimpleNamespace(inputs=())
    )
    case_id = f"{register.source_file}#/errata.classification_reference/1"
    _, diagnostics, report = compile_provider_declarations(
        tree, prepared, (), subset=True
    )
    assert diagnostics == ()
    assert report["sos/mfr"]["entries_read"] == [case_id]
    assert report["sos/mfr"]["not_evaluated_in_subset"] == [case_id]
    _, diagnostics, report = compile_provider_declarations(
        tree, prepared, (), subset=False
    )
    assert [issue.code for issue in diagnostics] == ["stale_curation_entry"]
    assert report["sos/mfr"]["stale"] == [case_id]


_LOVA_SSYK = (
    "https://www.scb.se/dokumentation/klassifikationer-och-standarder/"
    "standard-for-svensk-yrkesklassificering-ssyk/"
)
_LOVA_SUN = (
    "https://www.scb.se/dokumentation/klassifikationer-och-standarder/"
    "svensk-utbildningsnomenklatur-sun/"
)
_LOVA_REFERENCE_ROWS = (
    ("A_LOVA_HOSP", "EU_EES", "Fritext, land eller område", _LOVA_SSYK),
    ("A_LOVA", "EXAMAR", "YYYY", _LOVA_SSYK),
    ("A_LOVA_EXAMEN", "EXAMAR", "YYYY", _LOVA_SSYK),
    ("A_LOVA_HOSP", "EXAMAR", "YYYY", _LOVA_SSYK),
    ("A_LOVA_EXAMEN", "EXAMEN", "fritext", _LOVA_SSYK),
    ("A_LOVA_HOSP", "TEMPBEHORIGHETFRAN", "YYYY-MM-DD", _LOVA_SUN),
    ("A_LOVA_HOSP", "TEMPBEHORIGHETTILL", "YYYY-MM-DD", _LOVA_SUN),
    (
        "A_LOVA",
        "CFARNR",
        "Åttaställigt nummer för arbetsställe",
        "CfarNrSok - SCB - Sökning efter arbetsställen",
    ),
    (
        "A_LOVA_LISA",
        "CFARNR",
        "Åttaställigt nummer för arbetsställe",
        "CfarNrSok - SCB - Sökning efter arbetsställen",
    ),
    (
        "A_LOVA",
        "SSYKSTATUS",
        "1 vid överenstämmelse",
        "fel i SCB dokumentaion försök igen",
    ),
    (
        "A_LOVA_LISA",
        "SSYKSTATUS",
        "1 vid överenstämmelse",
        "fel i SCB dokumentaion försök igen",
    ),
    (
        "A_LOVA",
        "SSYKSTATUS_J16",
        "1 vid överenstämmelse",
        "fel i SCB dokumentaion försök igen",
    ),
    (
        "A_LOVA_LISA",
        "SSYKSTATUS_J16",
        "1 vid överenstämmelse",
        "fel i SCB dokumentaion försök igen",
    ),
    ("A_LOVA_HOSP", "EJ_PNR", "0, 1", "1 = personnumer saknas"),
    ("A_LOVA_HOSP", "FORSKRIVNINGSRATT", "J;N", "J = ja, N=nej"),
    (
        "A_LOVA_HOSP",
        "KALLA",
        "hosp; DESL, sk_spec",
        "hosp = HOSP, DESL=uppgifter om deslegitimation, sk_spec = uppgifter om specialistsjuksköterskor",
    ),
    ("A_LOVA_LISA", "SEKTORKOD", "Se A_LOVA_STYR_SEKTORKOD", "Administrativ"),
)


def test_lova_reference_inventory_is_exact():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "curation"
    register = next(
        item
        for item in load_register_files(root)
        if item.register_info.provider == "sos" and item.register_info.slug == "lova"
    )
    assert {
        (
            item.deldatamangd,
            item.variable,
            item.expected_representation,
            item.expected_reference,
        )
        for item in register.errata.classification_reference
    } == set(_LOVA_REFERENCE_ROWS)
    assert len(register.errata.classification_reference) == len(_LOVA_REFERENCE_ROWS)


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


def _partition_scope(records: tuple[SourceRecord, ...]) -> CompiledScope:
    first = records[0]
    register = source_register_key(first)
    assert register is not None
    provider = first.subject.provider
    register_id = str(register[-1]) if provider == "scb" else "5891427617861710725"
    slug = "sample" if provider == "scb" else "par"
    return CompiledScope(
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
        iter_partition_families=lambda source, registers=None, select_family=None: iter(
            ((native, records),)
        )
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
    variable_id: int = 5,
):
    header = REGISTERINFORMATION_HEADER.split("|")
    revision = _revision("scb-registerinformation")
    records = []
    for index, column in enumerate(columns, 1):
        row = _var_row(
            cvid=20 + index,
            var_id=variable_id,
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


def test_partition_reads_only_registers_with_partition_work(tmp_path: Path) -> None:
    root = tmp_path / "curation"
    tree = _tree(root)
    records = _scb_partition_records(("ANSWER",))
    scope = _partition_scope(records)
    register = source_register_key(records[0])
    assert register is not None
    requested = []

    class Reader:
        def iter_partition_families(self, source, registers=None, select_family=None):
            requested.append(set(registers))
            return iter(())

    prepared = cast("Any", SimpleNamespace(records=Reader()))
    compile_partitions(tree, prepared, (scope,))
    assert requested == [set()]

    path = root / "registers/scb/sample.toml"
    path.write_text(
        path.read_text() + '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
    )
    compile_partitions(load_curation_tree(root), prepared, (scope,))
    assert requested[-1] == {register}


def _errata_record(
    *,
    column: str,
    year: str,
    variable: int = 5,
    member: int = 20,
    variant: int = 2,
    edition_name: str | None = None,
    edition_id: int | None = None,
    data_type: str = "int",
) -> SourceRecord:
    header = REGISTERINFORMATION_HEADER.split("|")
    row = _var_row(
        colname=column,
        cvid=member,
        var_id=variable,
        year=year,
        versionname=edition_name,
        regver_id=edition_id if edition_id is not None else int(year),
        register=("TEST", 1, variant),
        data_type=data_type,
    ).split("|")
    cells = {
        name: (True, value, value) for name, value in zip(header, row, strict=True)
    }
    return clean_scb_row(
        header, member, cells, _revision("scb-registerinformation")
    ).record


def _edition_record(
    *,
    name: str,
    edition_id: int,
    member: int,
    variable: int = 5,
    variant: int = 2,
    column: str = "VALUE",
) -> SourceRecord:
    header = REGISTERINFORMATION_HEADER.split("|")
    row = _var_row(
        colname=column,
        cvid=member,
        var_id=variable,
        year="2020",
        versionname=name,
        regver_id=edition_id,
        register=("TEST", 1, variant),
    ).split("|")
    return clean_scb_row(
        header,
        member,
        {field: (True, value, value) for field, value in zip(header, row, strict=True)},
        _revision("scb-registerinformation"),
    ).record


def _preliminary_cases(records: tuple[SourceRecord, ...]):
    first = records[0]
    prepared = SimpleNamespace(
        records=SimpleNamespace(iter_records=lambda *, source: iter(records))
    )
    return compile_scb_preliminary(cast("Any", prepared), (_partition_scope(records),))[
        first.source, None
    ]


def test_scb_final_supersedes_only_shared_native_variables():
    preliminary = _edition_record(
        name=" 2020, preliminär version ", edition_id=10, member=1
    )
    preliminary_only = _edition_record(
        name="2020, preliminär version", edition_id=10, member=2, variable=6
    )
    final = _edition_record(name="2020, slutlig version", edition_id=11, member=3)
    records = (preliminary, preliminary_only, final)
    assert preliminary.context[2] == "2020, preliminär version"
    assert preliminary.original_period_text == " 2020, preliminär version "
    (case,) = _preliminary_cases(records)
    assert case.case_id == "superseded-preliminary:1:2:2020"
    assert {target.ref for target in case.targets} == {record_ref(preliminary)}
    assert set(case.peer_guards[0].expected_members) == {
        record_ref(record) for record in records
    }
    corrected = apply_occurrence_cases(records, (case,))
    assert corrected.accounting[0].disposition == "applied"
    assert [item.use for item in corrected.occurrences] == [
        "support",
        "catalog",
        "catalog",
    ]
    added = _edition_record(
        name="2020, slutlig version", edition_id=11, member=4, variable=6
    )
    next_records = (*records, added)
    (next_case,) = _preliminary_cases(next_records)
    assert {target.ref for target in next_case.targets} == {
        record_ref(preliminary),
        record_ref(preliminary_only),
    }
    rederived = apply_occurrence_cases(next_records, (next_case,))
    assert rederived.accounting[0].disposition == "applied"
    assert [item.use for item in rederived.occurrences] == [
        "support",
        "support",
        "catalog",
        "catalog",
    ]
    assert _preliminary_cases(tuple(reversed(records))) == (case,)


@pytest.mark.parametrize(
    "preliminary_name,final_name,final_variant",
    [
        ("2010 preliminär", "2010, slutlig version", 2),
        ("2020, preliminär version", "2020, slutlig version", 3),
        ("2020, preliminär version", "2019, slutlig version", 2),
        ("2020, Preliminär version", "2020, slutlig version", 2),
        ("2020,  preliminär version", "2020, slutlig version", 2),
    ],
)
def test_scb_preliminary_requires_exact_same_variant_final_partner(
    preliminary_name: str, final_name: str, final_variant: int
):
    preliminary = _edition_record(name=preliminary_name, edition_id=10, member=1)
    other_variant = _edition_record(
        name=final_name, edition_id=11, member=2, variant=final_variant
    )
    prepared = SimpleNamespace(
        records=SimpleNamespace(
            iter_records=lambda *, source: iter((preliminary, other_variant))
        )
    )
    assert (
        compile_scb_preliminary(
            cast("Any", prepared), (_partition_scope((preliminary,)),)
        )
        == {}
    )


def test_scb_unpaired_preliminary_is_untouched():
    preliminary = _edition_record(
        name="2020, preliminär version", edition_id=10, member=1
    )
    prepared = SimpleNamespace(
        records=SimpleNamespace(iter_records=lambda *, source: iter((preliminary,)))
    )
    assert (
        compile_scb_preliminary(
            cast("Any", prepared), (_partition_scope((preliminary,)),)
        )
        == {}
    )


def test_scb_pair_without_shared_native_variable_needs_no_case():
    preliminary = _edition_record(
        name="2020, preliminär version", edition_id=10, member=1
    )
    final = _edition_record(
        name="2020, slutlig version", edition_id=11, member=2, variable=6
    )
    prepared = SimpleNamespace(
        records=SimpleNamespace(
            iter_records=lambda *, source: iter((preliminary, final))
        )
    )
    assert (
        compile_scb_preliminary(
            cast("Any", prepared), (_partition_scope((preliminary,)),)
        )
        == {}
    )


def _errata_fixture(tmp_path: Path, records: tuple[SourceRecord, ...], fragment: str):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers/scb/sample.toml"
    path.write_text(
        path.read_text()
        + '\n[[variant]]\nnative_id = "1.2"\nslug = "people"\n'
        + fragment
        + (
            '\n[[variable]]\nnative_id = "1.NewCol"\nslug = "new-col"\n'
            if "[[errata.column]]" in fragment
            else ""
        ),
        encoding="utf-8",
    )
    scope = _partition_scope(records)
    native = source_register_key(records[0])
    assert native is not None
    reader = SimpleNamespace(
        iter_register_slices=lambda source, registers: iter(((native, records),))
    )
    return (
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=reader, iter_evidence=lambda: iter(()))),
        scope,
    )


_DELIVERED = (
    '\n[[errata.delivered]]\nvariant = "people"\ncolumn = "A"\n'
    'versions = ["2021"]\nevidence = "accepted delivery"\nnoted = "2026-09-25"\n'
)

_EDITION_PERIOD = (
    '\n[[errata.edition_period]]\nvariant = "people"\nname = "Födelseland"\n'
    'valid_from = "2018-02-01"\nvalid_to = "2018-11-30"\n'
    'evidence = "Source documentation"\nnoted = "2026-09-26"\n'
)


def _scope_with_coding_names(
    scope: CompiledScope, record: SourceRecord
) -> CompiledScope:
    occurrence = source_occurrence(record)
    assert occurrence.variable_key is not None and occurrence.variant_key is not None
    register = source_register_key(record)
    assert register is not None
    return scope.model_copy(
        update={
            "naming": (
                *scope.naming,
                NamingDeclaration(
                    target=NativeNamingTarget(
                        kind="register_variant",
                        provider="scb",
                        source_key=occurrence.variant_key,
                        register_key=register,
                    ),
                    naming=SlugEntry("register_variant", "1.2", "people", "scb"),
                    contributors=(),
                ),
                NamingDeclaration(
                    target=NativeNamingTarget(
                        kind="variable",
                        provider="scb",
                        source_key=occurrence.variable_key,
                        register_key=register,
                    ),
                    naming=SlugEntry("variable", "1.5", "value", "scb"),
                    contributors=(),
                ),
            )
        }
    )


@pytest.mark.parametrize("warning", [None, "Range interpretation is unavailable"])
def test_named_edition_split_rebinds_parents_and_is_order_independent(
    tmp_path: Path,
    warning: str | None,
) -> None:
    root = tmp_path / "curation"
    path = root / "registers/scb/sample.toml"
    path.parent.mkdir(parents=True)
    (root / "classifications").mkdir()
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.2"\nslug = "flow"\n'
        '[[variant]]\nnative_id = "1.2.stock"\nslug = "stock"\n'
        '[[identity.edition_split]]\nvariant = "1.2"\n'
        'split = "1.2.stock"\neditions = ["2007-12-31"]\n'
        'source_editions = ["2007"]\n'
        'evidence = "SCB population text distinguishes stock"\n'
        'noted = "2026-09-27"\n',
        encoding="utf-8",
    )
    tree = load_curation_tree(root)
    if warning is not None:
        path.write_text(path.read_text() + f"data_warning = {json.dumps(warning)}\n")
        tree = load_curation_tree(root)
    records = (
        _errata_record(column="SHARED", year="2007", edition_name="2007", edition_id=1),
        _errata_record(
            column="SHARED",
            year="2007",
            edition_name="2007-12-31",
            edition_id=2,
            member=21,
            data_type="str",
        ),
    )
    assert "conflicting_occurrence_facts" in {
        issue.code for issue in resolve_occurrence_intervals(records).issues
    }
    scope = _partition_scope(records)
    native_register = source_register_key(records[0])
    native_variable = native_variable_key(records[0])
    assert native_register is not None and native_variable is not None

    def prepared(rows):
        reader = SimpleNamespace(
            iter_records=lambda **kwargs: iter(rows),
            iter_register_slices=lambda source, wanted: iter(
                ((native_register, rows),)
            ),
            iter_native_families=lambda source, registers=None: iter(
                ((native_variable, rows),)
            ),
        )
        return cast("Any", SimpleNamespace(records=_naming_reader(reader)))

    first, issues, statuses = compile_edition_splits(
        tree, prepared(records), (scope,), subset=False
    )
    second, _, _ = compile_edition_splits(
        tree, prepared(records[::-1]), (scope,), subset=False
    )
    assert not issues and statuses["scb/sample"]["entries_matched"]
    assert first == second
    (case,) = first[scope.source, scope.register_key]
    assert isinstance(case.decision, OccurrenceCorrectionDecision)
    assert case.decision.data_warning == warning
    assert case.decision.data_warning_refs == (
        (record_ref(records[1]),) if warning is not None else ()
    )
    assert case.decision.data_warning_fields == (
        ("availability",) if warning is not None else ()
    )
    result = apply_occurrence_cases(records, (case,))
    assert result.diagnostics == ()
    flow, stock = result.occurrences
    assert flow.variant_key is not None
    assert stock.variant_key is not None and stock.edition_key is not None
    assert flow.variant_key == native_variant_key(records[0])
    assert stock.variant_key == (*flow.variant_key, "edition-split", "stock")
    assert stock.edition_key[: len(stock.variant_key)] == stock.variant_key
    if stock.population_key is not None:
        assert stock.population_key[: len(stock.variant_key)] == stock.variant_key
    assert all(
        "conflicting_occurrence_facts"
        not in {
            issue.code for issue in resolve_occurrence_intervals((occurrence,)).issues
        }
        for occurrence in result.occurrences
    )

    naming, _, _, name_issues, _ = compile_native_naming(
        tree, prepared(records), (scope,), subset=False
    )
    assert not name_issues
    parents = resolve_parents(
        result.occurrences,
        naming[scope.source, scope.register_key],
        rebinds={record_ref(records[1]): stock.variant_key},
    )
    assert set(parents.variants) == {flow.variant_key, stock.variant_key}
    assert stock.edition_key in parents.editions
    assert any(
        key[: len(stock.variant_key)] == stock.variant_key for key in parents.fields
    )

    selected = records[1]
    changed_parent = selected.parent_facts[0].model_copy(
        update={
            "fields": selected.parent_facts[0].fields.model_copy(
                update={"description": value_field("Changed parent prose")}
            )
        }
    )
    drifted = (
        selected.model_copy(
            update={
                "fields": selected.fields.model_copy(
                    update={"operational_definition": value_field("Changed operation")}
                )
            }
        ),
        selected.model_copy(
            update={"parent_facts": (changed_parent, *selected.parent_facts[1:])}
        ),
        selected.model_copy(
            update={
                "code_set_references": (
                    CodeSetReference(
                        reference_id="new-codes",
                        content_sha256="a" * 64,
                        physical_locator="codes.csv:1",
                    ),
                )
            }
        ),
    )
    for changed in drifted:
        replay = apply_occurrence_cases((records[0], changed), (case,))
        assert replay.diagnostics
        assert all(
            item.variant_key == native_variant_key(records[0])
            for item in replay.occurrences
        )

    missing = path.read_text().replace('2007-12-31"]', '2008-12-31"]')
    path.write_text(missing, encoding="utf-8")
    stale, stale_issues, _ = compile_edition_splits(
        load_curation_tree(root), prepared(records), (scope,), subset=False
    )
    assert not stale
    assert [issue.code for issue in stale_issues] == ["stale_curation_entry"]


def test_split_variant_naming_accepts_source_parent_and_keeps_states(
    tmp_path: Path,
) -> None:
    root = tmp_path / "curation"
    path = root / "registers/scb/sample.toml"
    path.parent.mkdir(parents=True)
    (root / "classifications").mkdir()
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "2"\n'
        '[[variant]]\nnative_id = "2.66"\nslug = "flow"\n'
        '[[variant]]\nnative_id = "2.66.stock"\nslug = "stock"\n'
        '[[identity.edition_split]]\nvariant = "2.66"\nsplit = "2.66.stock"\n'
        'editions = ["2007-12-31"]\nsource_editions = ["2007"]\n'
        'evidence = "SCB distinguishes flow and stock"\nnoted = "2026-09-27"\n',
        encoding="utf-8",
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    records = tuple(
        clean_scb_row(
            header,
            member,
            {
                name: (True, value, value)
                for name, value in zip(
                    header,
                    _var_row(
                        colname="SHARED",
                        cvid=member,
                        var_id=5,
                        year="2007",
                        versionname=edition_name,
                        regver_id=edition_id,
                        register=("RTB", 2, 66),
                    ).split("|"),
                    strict=True,
                )
            },
            _revision("scb-registerinformation"),
        ).record
        for member, edition_name, edition_id in (
            (20, "2007", 1),
            (21, "2007-12-31", 2),
        )
    )
    source_variant = native_variant_key(records[0])
    register = source_register_key(records[0])
    variable = native_variable_key(records[0])
    assert source_variant is not None and register is not None and variable is not None
    assert {native_variant_key(record) for record in records} == {source_variant}
    reader = SimpleNamespace(
        iter_native_families=lambda source, registers=None: iter(
            ((variable, records),)
        ),
        iter_records=lambda **kwargs: iter(records),
        iter_register_slices=lambda source, wanted: iter(((register, records),)),
    )
    prepared = cast("Any", SimpleNamespace(records=_naming_reader(reader)))
    scope = _partition_scope(records)
    scope_key = scope.source, scope.register_key
    tree = load_curation_tree(root)
    split_cases, split_issues, _ = compile_edition_splits(
        tree, prepared, (scope,), subset=False
    )
    naming, _, _, naming_issues, _ = compile_native_naming(
        tree, prepared, (scope,), subset=False
    )
    assert split_issues == () and naming_issues == ()
    names = naming[scope_key]
    split_key = (*source_variant, "edition-split", "stock")
    split_target = next(
        name.target for name in names if name.target.source_key == split_key
    )
    evidence = SourceEvidence(records)
    checks = tuple(
        issue for name in names for issue in check_naming_target(name.target, evidence)
    )
    assert checks == ()
    assert (
        check_naming_target(
            split_target.model_copy(
                update={
                    "source_key": (*source_variant[:-1], 67, "edition-split", "stock")
                }
            ),
            evidence,
        )[0].code
        == "naming_native_identity_missing"
    )
    assert (
        check_naming_target(
            split_target.model_copy(update={"register_key": (*register[:-1], 3)}),
            evidence,
        )[0].code
        == "naming_native_identity_missing"
    )
    (case,) = split_cases[scope_key]
    corrected = apply_occurrence_cases(records, (case,))
    assert corrected.diagnostics == ()
    parents = resolve_parents(
        corrected.occurrences,
        names,
        rebinds={record_ref(records[1]): split_key},
    )
    assert split_key in parents.variants
    assert not [
        issue for issue in parents.diagnostics if issue.code == "withheld_parent_naming"
    ]
    formed = form_native_variable(
        corrected.occurrences,
        register=parents.registers[register],
        variants=parents.variants,
        slug="shared",
        provider_key="SHARED",
        flags=SourceFields(
            identifier=value_field(False), sensitivity=value_field(False)
        ),
        coding={
            occurrence.column_key: resolve_code_membership(())
            for occurrence in corrected.occurrences
            if occurrence.column_key is not None
        },
    )
    assert formed.variable is not None
    assert {state.variant.slug for state in formed.variable.states} == {"flow", "stock"}
    assert not formed.withheld_variant_states


@pytest.mark.parametrize(
    ("change", "detail"),
    [
        ("added", "unlisted native editions ['2008']"),
        ("removed", "listed names without exactly one native edition ['2007']"),
    ],
)
def test_named_edition_split_withholds_when_native_inventory_changes(
    tmp_path: Path, change: str, detail: str
) -> None:
    flow = _errata_record(column="A", year="2007", edition_name="2007", edition_id=1)
    stock = _errata_record(
        column="B", year="2007", edition_name="2007-12-31", edition_id=2, member=21
    )
    records = (
        (
            flow,
            stock,
            _errata_record(
                column="C", year="2008", edition_name="2008", edition_id=3, member=22
            ),
        )
        if change == "added"
        else (stock,)
    )
    fragment = (
        '\n[[variant]]\nnative_id = "1.2.stock"\nslug = "stock"\n'
        '[[identity.edition_split]]\nvariant = "1.2"\nsplit = "1.2.stock"\n'
        'editions = ["2007-12-31"]\nsource_editions = ["2007"]\n'
        'evidence = "SCB population text"\nnoted = "2026-09-27"\n'
    )
    tree, prepared, scope = _errata_fixture(tmp_path, records, fragment)
    cases, issues, report = compile_edition_splits(
        tree, prepared, (scope,), subset=False
    )
    assert cases == {}
    assert [issue.code for issue in issues] == ["stale_curation_entry"]
    assert detail in issues[0].detail
    assert len(report["scb/sample"]["stale"]) == 1


def test_compiled_edition_period_places_only_named_unparseable_edition(
    tmp_path: Path,
) -> None:
    topic = _errata_record(column="A", year="2020", edition_name="Födelseland")
    another_topic = _errata_record(
        column="C",
        year="2020",
        variable=6,
        member=22,
        edition_name="Födelseland",
    )
    sibling = _errata_record(column="B", year="2021", member=21, variant=3)
    tree, prepared, scope = _errata_fixture(
        tmp_path, (topic, another_topic, sibling), _EDITION_PERIOD
    )
    cases, _, _, diagnostics, report = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert diagnostics == ()
    assert len(report["scb/sample"]["entries_matched"]) == 1
    corrected = apply_occurrence_cases(
        (topic, another_topic, sibling), cases[(scope.source, scope.register_key)]
    )
    assert corrected.diagnostics == ()
    assert [account.disposition for account in corrected.accounting] == ["applied"]
    by_member = {
        occurrence.source_records[0].subject.native.member_id: occurrence
        for occurrence in corrected.occurrences
    }
    for member in (20, 22):
        result = resolve_occurrence_intervals((by_member[member],))
        assert result.issues == ()
        assert [
            (segment.valid_from, segment.valid_to) for segment in result.segments
        ] == [("2018-02-01", "2018-11-30")]
    assert (
        by_member[21].edition_period_scope
        == source_occurrence(sibling).edition_period_scope
    )


@pytest.mark.parametrize("edition_name", ["Renamed", "2020"])
def test_compiled_edition_period_stale_when_missing_or_parseable(
    tmp_path: Path, edition_name: str
) -> None:
    record = _errata_record(column="A", year="2020", edition_name=edition_name)
    fragment = (
        _EDITION_PERIOD.replace('name = "Födelseland"', 'name = "2020"')
        if edition_name == "2020"
        else _EDITION_PERIOD
    )
    tree, prepared, scope = _errata_fixture(tmp_path, (record,), fragment)
    cases, _, _, diagnostics, report = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert cases == {}
    assert [diagnostic.code for diagnostic in diagnostics] == ["stale_curation_entry"]
    assert len(report["scb/sample"]["stale"]) == 1
    unresolved = resolve_occurrence_intervals((record,))
    if edition_name == "Renamed":
        assert unresolved.segments == ()
        assert unresolved.issues[0].fields == ("period",)


def test_unmatched_topic_edition_remains_unsupported(tmp_path: Path) -> None:
    record = _errata_record(column="A", year="2020", edition_name="Födelseland")
    tree, prepared, scope = _errata_fixture(tmp_path, (record,), "")
    cases, _, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert cases == {} and diagnostics == ()
    result = resolve_occurrence_intervals((record,))
    assert result.segments == ()
    assert result.issues[0].fields == ("period",)


def test_compiled_errata_delivered_addition_and_blank_target(tmp_path: Path):
    donor = _errata_record(column="A", year="2020")
    other = _errata_record(column="B", year="2021", variable=6, member=21)
    tree, prepared, scope = _errata_fixture(tmp_path, (donor, other), _DELIVERED)
    cases, _, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert diagnostics == ()
    case = cases[(scope.source, scope.register_key)][0]
    assert isinstance(case.decision.effects[0], CuratedOccurrenceAddition)
    assert case.decision.effects[0].edition_key == source_occurrence(other).edition_key
    assert (
        apply_occurrence_cases((donor, other), (case,)).accounting[0].disposition
        == "applied"
    )

    blank = _errata_record(column="", year="2021", member=22)
    tree, prepared, scope = _errata_fixture(
        tmp_path / "blank", (donor, blank), _DELIVERED
    )
    cases, _, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert diagnostics == ()
    case = cases[(scope.source, scope.register_key)][0]
    assert isinstance(case.decision.effects[0], CheckedFieldChange)
    assert (
        apply_occurrence_cases((donor, blank), (case,)).accounting[0].disposition
        == "applied"
    )


def test_compiled_errata_uses_loaded_tree_when_authored_files_change(tmp_path: Path):
    donor = _errata_record(column="A", year="2020")
    other = _errata_record(column="B", year="2021", variable=6, member=21)
    tree, prepared, scope = _errata_fixture(tmp_path, (donor, other), _DELIVERED)
    expected = compile_errata(tree, prepared, (scope,), {}, subset=False)
    assert expected[0]
    for path in tree.root.rglob("*.toml"):
        path.write_text("invalid TOML [", encoding="utf-8")
    assert compile_errata(tree, prepared, (scope,), {}, subset=False) == expected


def test_same_column_delivery_entries_preserve_distinct_blank_and_missing_editions(
    tmp_path: Path,
) -> None:
    donor = _errata_record(column="A", year="2020")
    blank = _errata_record(column="", year="2021", member=22)
    other = _errata_record(column="B", year="2022", variable=6, member=23)
    originals = (donor, blank, other)
    fragment = _DELIVERED + _DELIVERED.replace(
        'versions = ["2021"]', 'versions = ["2022"]'
    ).replace("accepted delivery", "accepted missing-edition delivery")
    tree, prepared, scope = _errata_fixture(tmp_path, originals, fragment)
    cases, _, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert not diagnostics
    first, second = cases[scope.source, scope.register_key]
    assert isinstance(first.decision.effects[0], CheckedFieldChange)
    assert first.decision.effects[0].ref == record_ref(blank)
    assert isinstance(second.decision.effects[0], CuratedOccurrenceAddition)
    assert (
        second.decision.effects[0].edition_key == source_occurrence(other).edition_key
    )
    changed = apply_occurrence_cases(originals, (first, second))
    assert all(item.disposition == "applied" for item in changed.accounting)
    corrected = next(o for o in changed.occurrences if blank in o.source_records)
    assert corrected.fields.column_name.value == "A"
    added = next(o for o in changed.occurrences if o.occurrence_key)
    assert added.edition_key == source_occurrence(other).edition_key
    assert added.variable_key == source_occurrence(donor).variable_key
    assert not added.coding_records


def test_coding_choice_on_errata_delivered_pooled_columns_keeps_exact_peers(
    tmp_path: Path,
) -> None:
    earlier = _errata_record(
        column="", year="2008", edition_name="2008 - 2010", member=20
    )
    later = _errata_record(
        column="", year="2010", edition_name="2010 - 2012", member=21
    )
    donor = _errata_record(column="VALUE", year="2013", member=22)
    originals = (earlier, later, donor)
    fragment = (
        '\n[[errata.delivered]]\nvariant = "people"\ncolumn = "VALUE"\n'
        'versions = ["2008 - 2010", "2010 - 2012"]\n'
        'evidence = "accepted delivery"\nnoted = "2026-09-28"\n'
        '\n[[coding.choice]]\nvariable = "1.5"\nvariant = "people"\n'
        'column = "VALUE"\nperiods = [["2010-01-01", "2010-12-31"]]\n'
        'keep = "later"\nover = ["earlier"]\n'
        'reason = "Reviewed overlap"\nsource = "fixture"\n'
    )
    tree, prepared, scope = _errata_fixture(tmp_path, originals, fragment)
    errata_cases, _, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert not diagnostics
    corrected = apply_occurrence_cases(
        originals, errata_cases[(scope.source, scope.register_key)]
    )
    assert corrected.accounting[0].disposition == "applied"
    occurrence = source_occurrence(donor)
    assert occurrence.variable_key is not None and occurrence.variant_key is not None
    column = column_identity(occurrence.variable_key, occurrence.variant_key, "VALUE")
    members = tuple(
        record
        for item in corrected.occurrences
        if item.column_key == column
        for record in item.evidence
    )
    assert {record_ref(record) for record in members} == {
        record_ref(record) for record in originals
    }
    scope = _scope_with_coding_names(scope, donor)
    independent = TemporalScope(kind="year_independent")
    claims = (
        CodeListClaim(
            "earlier",
            earlier.edition_scope,
            (CodeMembershipClaim("0", "Nej", independent),),
            version_label="earlier",
        ),
        CodeListClaim(
            "later",
            later.edition_scope,
            (CodeMembershipClaim("1", "Ja", independent),),
            version_label="later",
        ),
    )
    assert "conflicting_code_memberships" in {
        issue.code for issue in resolve_code_membership(claims).issues
    }
    register = next(
        entry for entry in tree.registers if entry.register_info.slug == "sample"
    )
    choices, diagnostics = compile_coding_register(
        register,
        scope,
        originals=originals,
        columns={column: members},
        column_scopes=SourceEvidence(
            originals, effective_occurrences=corrected.occurrences
        ).effective_scopes
        or {},
        coding={column: claims},
    )
    assert not diagnostics and len(choices) == 1
    applied = apply_coding_choices(
        SourceEvidence(originals, effective_occurrences=corrected.occurrences),
        choices,
        coding={column: claims},
    )
    assert applied.accounting[0].status == "applied"
    assert not applied.diagnostics
    assert "conflicting_code_memberships" not in {
        issue.code for issue in applied.coding[column].issues
    }

    added_peer = _errata_record(column="VALUE", year="2014", member=23)
    stale = apply_coding_choices(
        SourceEvidence(
            (*originals, added_peer),
            effective_occurrences=(
                *corrected.occurrences,
                source_occurrence(added_peer),
            ),
        ),
        choices,
        coding={column: claims},
    )
    assert stale.accounting[0].status == "stale"
    assert "peer_membership_changed" in {issue.code for issue in stale.diagnostics}

    removed = apply_coding_choices(
        SourceEvidence((later, donor), effective_occurrences=corrected.occurrences),
        choices,
        coding={column: claims},
    )
    assert removed.accounting[0].status == "stale"
    assert "peer_membership_changed" in {issue.code for issue in removed.diagnostics}


def test_two_errata_delivered_blank_columns_have_separate_coding_peers(
    tmp_path: Path,
) -> None:
    a_blank = _errata_record(
        column="", year="2008", edition_name="2008 - 2010", member=20
    )
    b_blank = _errata_record(
        column="", year="2009", edition_name="2009 - 2010", member=21
    )
    a_donor = _errata_record(
        column="A", year="2010", edition_name="2010 - 2012", member=22
    )
    b_donor = _errata_record(
        column="B", year="2010", edition_name="2010 - 2012", member=23
    )
    originals = (a_blank, b_blank, a_donor, b_donor)
    fragment = (
        '\n[[errata.delivered]]\nvariant = "people"\ncolumn = "A"\n'
        'versions = ["2008 - 2010"]\n'
        'evidence = "accepted A delivery"\nnoted = "2026-09-28"\n'
        '\n[[errata.delivered]]\nvariant = "people"\ncolumn = "B"\n'
        'versions = ["2009 - 2010"]\n'
        'evidence = "accepted B delivery"\nnoted = "2026-09-28"\n'
        '\n[[coding.choice]]\nvariable = "1.5"\nvariant = "people"\n'
        'column = "A"\nperiods = [["2010-01-01", "2010-12-31"]]\n'
        'keep = "A later"\nover = ["A earlier"]\n'
        'reason = "Reviewed A overlap"\nsource = "fixture"\n'
        '\n[[coding.choice]]\nvariable = "1.5"\nvariant = "people"\n'
        'column = "B"\nperiods = [["2010-01-01", "2010-12-31"]]\n'
        'keep = "B later"\nover = ["B earlier"]\n'
        'reason = "Reviewed B overlap"\nsource = "fixture"\n'
    )
    tree, prepared, scope = _errata_fixture(tmp_path, originals, fragment)
    errata_cases, _, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert not diagnostics
    corrected = apply_occurrence_cases(
        originals, errata_cases[(scope.source, scope.register_key)]
    )
    assert all(item.disposition == "applied" for item in corrected.accounting)
    scope = _scope_with_coding_names(scope, a_donor)
    occurrence = source_occurrence(a_donor)
    assert occurrence.variable_key is not None and occurrence.variant_key is not None
    columns = {}
    for name in ("A", "B"):
        key = column_identity(occurrence.variable_key, occurrence.variant_key, name)
        columns[key] = tuple(
            record
            for item in corrected.occurrences
            if item.column_key == key
            for record in item.evidence
        )
    assert all(len(members) == 2 for members in columns.values())
    independent = TemporalScope(kind="year_independent")
    coding = {}
    for name, blank, donor in (("A", a_blank, a_donor), ("B", b_blank, b_donor)):
        column = column_identity(occurrence.variable_key, occurrence.variant_key, name)
        coding[column] = (
            CodeListClaim(
                f"{name} earlier",
                blank.edition_scope,
                (CodeMembershipClaim("0", "Nej", independent),),
                version_label=f"{name} earlier",
            ),
            CodeListClaim(
                f"{name} later",
                donor.edition_scope,
                (CodeMembershipClaim("1", "Ja", independent),),
                version_label=f"{name} later",
            ),
        )
        assert "conflicting_code_memberships" in {
            issue.code for issue in resolve_code_membership(coding[column]).issues
        }
    register = next(
        entry for entry in tree.registers if entry.register_info.slug == "sample"
    )
    choices, diagnostics = compile_coding_register(
        register,
        scope,
        originals=originals,
        columns=columns,
        column_scopes=SourceEvidence(
            originals, effective_occurrences=corrected.occurrences
        ).effective_scopes
        or {},
        coding=coding,
    )
    assert not diagnostics and len(choices) == 2
    applied = apply_coding_choices(
        SourceEvidence(originals, effective_occurrences=corrected.occurrences),
        choices,
        coding=coding,
    )
    assert [entry.status for entry in applied.accounting] == ["applied", "applied"]
    assert not applied.diagnostics
    assert all(
        "conflicting_code_memberships" not in {issue.code for issue in resolved.issues}
        for resolved in applied.coding.values()
    )

    added_a = _errata_record(column="A", year="2014", member=24)
    changed = apply_coding_choices(
        SourceEvidence(
            (*originals, added_a),
            effective_occurrences=(*corrected.occurrences, source_occurrence(added_a)),
        ),
        choices,
        coding=coding,
    )
    assert [entry.status for entry in changed.accounting] == ["stale", "applied"]
    assert [issue.code for issue in changed.diagnostics] == ["peer_membership_changed"]


def test_compiled_errata_with_declared_split_uses_native_variant(tmp_path: Path):
    donor = _errata_record(column="A", year="2020")
    flow = _errata_record(
        column="B", year="2021", variable=6, member=21, edition_name="2021"
    )
    stock = _errata_record(
        column="C",
        year="2021",
        variable=7,
        member=22,
        edition_name="2021-12-31",
        edition_id=2,
    )
    fragment = (
        '\n[[variant]]\nnative_id = "1.2.stock"\nslug = "stock"\n'
        '[[identity.edition_split]]\nvariant = "1.2"\nsplit = "1.2.stock"\n'
        'editions = ["2021-12-31"]\nevidence = "SCB stock population"\n'
        'source_editions = ["2020", "2021"]\n'
        'noted = "2026-09-27"\n' + _DELIVERED
    )
    tree, prepared, scope = _errata_fixture(tmp_path, (donor, flow, stock), fragment)
    cases, _, _, diagnostics, report = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert diagnostics == ()
    assert len(report["scb/sample"]["entries_matched"]) == 1
    case = cases[(scope.source, scope.register_key)][0]
    assert isinstance(case.decision.effects[0], CuratedOccurrenceAddition)
    assert case.decision.effects[0].edition_key == source_occurrence(flow).edition_key
    assert (
        apply_occurrence_cases((donor, flow, stock), (case,)).accounting[0].disposition
        == "applied"
    )


@pytest.mark.parametrize(
    ("table", "erratum"),
    [
        (
            "delivered",
            '\n[[errata.delivered]]\nvariant = "people"\ncolumn = "A"\n'
            'versions = ["2021-12-31"]\nevidence = "accepted delivery"\n'
            'noted = "2026-09-27"\n',
        ),
        (
            "column",
            '\n[[errata.column]]\nvariant = "people"\ncolumn = "NewCol"\n'
            'name = "New column"\ndefinition = "Documented"\n'
            'source = "scb-docs"\nversions = ["2021-12-31"]\n'
            'evidence = "accepted column"\nnoted = "2026-09-27"\n',
        ),
    ],
)
def test_compiled_errata_addition_to_moved_edition_is_stale(
    tmp_path: Path, table: str, erratum: str
) -> None:
    donor = _errata_record(column="A", year="2020")
    stock = _errata_record(
        column="B", year="2021", variable=6, member=21, edition_name="2021-12-31"
    )
    fragment = (
        '\n[[variant]]\nnative_id = "1.2.stock"\nslug = "stock"\n'
        '[[identity.edition_split]]\nvariant = "1.2"\nsplit = "1.2.stock"\n'
        'editions = ["2021-12-31"]\nsource_editions = ["2020"]\n'
        'evidence = "SCB stock population"\nnoted = "2026-09-27"\n' + erratum
    )
    tree, prepared, scope = _errata_fixture(tmp_path, (donor, stock), fragment)
    cases, naming, keys, diagnostics, report = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert cases == {} and naming == {} and keys == {}
    assert [issue.code for issue in diagnostics] == ["stale_curation_entry"]
    assert f"#/errata.{table}/1" in diagnostics[0].detail
    assert "('2021-12-31', '1.2.stock')" in diagnostics[0].detail
    assert len(report["scb/sample"]["stale"]) == 1


@pytest.mark.parametrize(
    "records,code",
    [
        ((("B", "2020", 5, 20), ("B", "2021", 6, 21)), "stale_curation_entry"),
        (
            (("A", "2020", 5, 20), ("A", "2020", 6, 21), ("B", "2021", 7, 22)),
            "overbroad_curation_entry",
        ),
        ((("A", "2020", 5, 20), ("A", "2021", 5, 21)), "stale_curation_entry"),
        (
            (("A", "2020", 5, 20), ("", "2021", 5, 21), ("", "2021", 5, 22)),
            "overbroad_curation_entry",
        ),
        ((("A", "2020", 5, 20), ("B", "2021", 5, 21)), "stale_curation_entry"),
    ],
)
def test_compiled_errata_blockers_are_errors(tmp_path: Path, records, code):
    members = tuple(
        _errata_record(column=c, year=y, variable=v, member=m) for c, y, v, m in records
    )
    tree, prepared, scope = _errata_fixture(tmp_path, members, _DELIVERED)
    cases, _, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert cases == {}
    assert [item.code for item in diagnostics] == [code]
    assert (
        diagnostics[0].subject
        == "curation/registers/scb/sample.toml#/errata.delivered/1"
    )


def test_compiled_errata_missing_edition_is_stale(tmp_path: Path):
    donor = _errata_record(column="A", year="2020")
    tree, prepared, scope = _errata_fixture(tmp_path, (donor,), _DELIVERED)
    cases, _, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert cases == {}
    assert diagnostics[0].code == "stale_curation_entry"
    assert "2021" in diagnostics[0].detail


@pytest.mark.parametrize("drift", ["parent", "coding"])
@pytest.mark.parametrize("owner", ["1.5", "1.5.a"])
def test_guarded_column_partition_preserves_parent_and_coding_guards(
    drift: str, owner: str
):
    original = _errata_record(column="A", year="2020")
    conversion = convert_column_partitions(
        (original,),
        source_id="1.5",
        split_ids=(owner,),
        declared_columns={"A": owner},
        declaration_reference="exact source",
        guard_fields=tuple(SourceFields.model_fields),
    )
    assert conversion.case is not None
    if drift == "parent":
        parent = original.parent_facts[0]
        changed = original.model_copy(
            update={
                "parent_facts": (
                    parent.model_copy(
                        update={
                            "fields": parent.fields.model_copy(
                                update={"name": value_field("changed parent")}
                            )
                        }
                    ),
                    *original.parent_facts[1:],
                )
            }
        )
    else:
        changed = original.model_copy(
            update={
                "code_set_references": (
                    CodeSetReference(
                        reference_id="changed-codes",
                        content_sha256="a" * 64,
                        physical_locator="codes.csv:1",
                    ),
                )
            }
        )
    result = apply_occurrence_cases((changed,), (conversion.case,))
    assert result.accounting[0].disposition == "stale"
    assert result.occurrences[0].variable_key == native_variable_key(original)
    default = convert_column_partitions(
        (original,),
        source_id="1.5",
        split_ids=(owner,),
        declared_columns={"A": owner},
        declaration_reference="exact source",
    )
    assert default.case is not None
    assert (default.case.targets[0].alternatives[0].parent_facts is None) == (
        owner != "1.5"
    )
    assert (default.case.targets[0].alternatives[0].code_set_references is None) == (
        owner != "1.5"
    )


@pytest.mark.parametrize("mode", ["ambiguous", "new_literal"])
def test_delivered_blank_partition_rejects_unproved_owner(tmp_path: Path, mode: str):
    donor = _errata_record(column="A", year="2020")
    blank = _errata_record(column="", year="2021", member=22)
    tree, prepared, scope = _errata_fixture(tmp_path, (donor, blank), _DELIVERED)
    native = native_variable_key(donor)
    assert native is not None
    key = scope.source, scope.register_key
    members = (
        frozenset(((record_ref(donor), "A"),))
        if mode == "ambiguous"
        else frozenset(((record_ref(donor), "OTHER"),))
    )
    owners = {(*native, "accepted-partition", "1.5.a"): members}
    if mode == "ambiguous":
        owners[(*native, "accepted-partition", "1.5.b")] = members
    compiled = compile_errata(tree, prepared, (scope,), {key: owners}, subset=False)
    assert compiled[0] == {}
    assert compiled[3][0].code in {"stale_curation_entry", "overbroad_curation_entry"}


def test_errata_and_enrichment_compile_is_order_independent(tmp_path: Path):
    donor = _errata_record(column="A", year="2020")
    other = _errata_record(column="B", year="2021", variable=6, member=21)
    tree, prepared, scope = _errata_fixture(tmp_path, (donor, other), _DELIVERED)

    def errata_bytes(candidate):
        return repr(
            compile_errata(candidate, prepared, (scope,), {}, subset=False)
        ).encode()

    assert (
        errata_bytes(tree)
        == errata_bytes(tree)
        == errata_bytes(replace(tree, registers=tuple(reversed(tree.registers))))
    )

    record = _errata_record(column="A", year="2020")
    tree, prepared, scope, naming = _enrichment_fixture(
        tmp_path / "enrichment", (record,), _DESCRIPTION + _ALIAS
    )

    def enrichment_bytes(candidate):
        return repr(
            compile_enrichment(
                candidate, prepared, (scope,), naming, {}, {}, subset=False
            )
        ).encode()

    assert (
        enrichment_bytes(tree)
        == enrichment_bytes(tree)
        == enrichment_bytes(replace(tree, registers=tuple(reversed(tree.registers))))
    )


@pytest.mark.parametrize(
    "placement,kind,edition_name",
    [
        ('versions = ["2020"]', "intervals", "2020"),
        ("all_versions = true", "unknown", None),
        ('holdings_period = "2019-2021"', "pooled", None),
    ],
)
def test_compiled_errata_column_placements_and_declared_flags(
    tmp_path: Path, placement: str, kind: str, edition_name: str | None
):
    record = _errata_record(column="A", year="2020")
    fragment = (
        '\n[[errata.column]]\nvariant = "people"\ncolumn = "NewCol"\n'
        'name = "New column"\ndefinition = "Documented"\n'
        'source = "steward-holdings"\nevidence = "held"\n'
        'noted = "2026-09-25"\n' + placement + "\n"
    )
    tree, prepared, scope = _errata_fixture(tmp_path, (record,), fragment)
    cases, naming, keys, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert diagnostics == ()
    key = (scope.source, scope.register_key)
    addition = cases[key][0].decision.effects[0]
    assert isinstance(addition, CuratedOccurrenceAddition)
    assert addition.edition_scope.kind == kind
    assert (addition.edition_key is not None) == (edition_name is not None)
    assert addition.fields.identifier == value_field(False)
    assert addition.fields.sensitivity == value_field(False)
    assert keys[key][0][1] == "NewCol"
    assert naming[key][0].naming.source_id == "1.NewCol"
    assert (
        apply_occurrence_cases((record,), (cases[key][0],)).accounting[0].disposition
        == "applied"
    )


def test_compiled_errata_declared_edition_uses_variant_support(tmp_path: Path):
    record = _errata_record(column="A", year="2020")
    fragment = (
        '\n[[errata.version]]\nvariant = "people"\nname = "2019"\n'
        'evidence = "omitted edition"\nnoted = "2026-09-25"\n'
        '\n[[errata.column]]\nvariant = "people"\ncolumn = "NewCol"\n'
        'name = "New column"\ndefinition = "Documented"\n'
        'source = "scb-docs"\nevidence = "held"\nnoted = "2026-09-25"\n'
        'versions = ["2019"]\nis_identifier = false\n'
    )
    tree, prepared, scope = _errata_fixture(tmp_path, (record,), fragment)
    cases, _, _, diagnostics, report = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert diagnostics == ()
    addition = cases[(scope.source, scope.register_key)][0].decision.effects[0]
    assert addition.edition_key[-2:] == ("accepted-edition", "2019")
    assert addition.fields.identifier.value is False
    assert addition.fields.sensitivity is None
    assert len(addition.evidence) == 1
    assert any(
        "errata.version" in item for item in report["scb/sample"]["entries_matched"]
    )


def test_errata_edition_bindings_refuse_conflicting_native_interpretation():
    first = _errata_record(column="A", year="2020")
    conflicting = _errata_record(column="B", year="2020", member=21).model_copy(
        update={"original_period_text": "2020 revised"}
    )
    with pytest.raises(ValueError, match="conflicting interpretations"):
        edition_bindings((first, conflicting), ())
    with pytest.raises(ValueError, match="already native"):
        edition_bindings((first,), (ErrataVersion(2, "2020"),))


def _enrichment_fixture(
    tmp_path: Path,
    records: tuple[SourceRecord, ...],
    fragment: str,
    *,
    target: NativeNamingTarget | None = None,
):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers/scb/sample.toml"
    path.write_text(path.read_text() + fragment, encoding="utf-8")
    scope = _partition_scope(records)
    native = source_register_key(records[0])
    variable = native_variable_key(records[0])
    assert native is not None and variable is not None
    declaration = NamingDeclaration(
        target=target
        or NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=variable,
            register_key=native,
        ),
        naming=SlugEntry(kind="variable", provider="scb", source_id="1.5", slug="a"),
        contributors=(),
    )
    reader = SimpleNamespace(
        iter_register_slices=lambda source, registers: iter(((native, records),))
    )
    key = (scope.source, scope.register_key)
    return (
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=reader)),
        scope,
        {key: (declaration,)},
    )


_DESCRIPTION = (
    '\n[[enrichment.description]]\nregister = "scb/sample"\n'
    'variable = "a"\ndescription = "Accepted prose"\n'
    'provenance = "delivery list"\n'
)
_ALIAS = (
    '\n[[enrichment.alias]]\nregister = "scb/sample"\n'
    'variable = "a"\ndelivery_column = "FormerA"\n'
    'provenance = "delivery list"\n'
)


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


def _scb_partition_tree(root: Path, extra: str):
    tree = _tree(root)
    path = root / "registers" / "scb" / "sample.toml"
    path.write_text(path.read_text() + extra, encoding="utf-8")
    return tree


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
    cases, naming, keys, ambiguities, bases, retained, issues = compiled
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
    assert not issues
    assert [issue.code for _, issue in retained[key]] == ["unassigned_original_columns"]
    assert retained[key][0][0] == source_register_key(records[0])
    assert retained[key][0][1].refs == (record_ref(records[1]),)


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
        iter_partition_families=lambda source, registers=None, select_family=None: iter(
            ((native, records),)
        )
    )
    compiled = compile_partitions(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=reader)),
        (scope,),
    )
    assert compiled == generated
    assert generated[0][key]
    assert generated[3][key][0].candidate_columns == ()
    assert native in generated[4][key]


def test_implicit_partition_binds_non_co_delivered_case_twins(tmp_path: Path):
    records = (
        _errata_record(column="Kon", year="2020", member=20),
        _errata_record(column="KON", year="2021", member=21),
    )
    forward = convert_column_partitions(
        records, source_id="1.5", split_ids=("1.5.kon",)
    )
    reverse = convert_column_partitions(
        records[::-1], source_id="1.5", split_ids=("1.5.kon",)
    )
    assert forward == reverse
    assert forward.diagnostics == ()
    assert forward.case is not None
    assert {effect.ref for effect in forward.case.decision.effects} == {
        record_ref(record) for record in records
    }
    assert {effect.when[0].value for effect in forward.case.decision.effects} == {
        "Kon",
        "KON",
    }

    root = tmp_path / "curation"
    _scb_partition_tree(root, '\n[[variable]]\nnative_id = "1.5.kon"\nslug = "kon"\n')
    compiled, key, _ = _compile_partition_fixture(root, records)
    assert len(compiled[0][key]) == 1
    assert not compiled[3].get(key)


def test_bound_twins_are_not_ambiguous_when_another_split_is_pending(tmp_path: Path):
    records = (
        _errata_record(column="Kon", year="2020", member=20),
        _errata_record(column="KON", year="2021", member=21),
        _errata_record(column="LEFT", year="2021", member=22),
    )
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.kon"\nslug = "kon"\n'
        '[[variable]]\nnative_id = "1.5.other"\nslug = "other"\n',
    )
    compiled, key, _ = _compile_partition_fixture(root, records)
    assert compiled[3][key][0].candidate_columns == ()
    assert {
        binding.source_id
        for binding in convert_column_partitions(
            records,
            source_id="1.5",
            split_ids=("1.5.kon", "1.5.other"),
        ).bindings
    } == {"1.5.kon"}


@pytest.mark.parametrize(
    "columns,years",
    [
        (("Kon", "KON"), ("2020", "2020")),
        (("A_B", "A-B"), ("2020", "2021")),
    ],
)
def test_implicit_partition_refuses_unsafe_slug_twins(columns, years):
    records = tuple(
        _errata_record(column=column, year=year, member=20 + index)
        for index, (column, year) in enumerate(zip(columns, years, strict=True))
    )
    suffix = "kon" if columns[0] == "Kon" else "a-b"
    converted = convert_column_partitions(
        records, source_id="1.5", split_ids=(f"1.5.{suffix}",)
    )
    assert converted == convert_column_partitions(
        records[::-1], source_id="1.5", split_ids=(f"1.5.{suffix}",)
    )
    assert converted.case is None
    assert [issue.code for issue in converted.diagnostics] == [
        "split_identity_conversion_pending"
    ]


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


def _sos_partition_records(
    *, rename: bool = False, subsets: tuple[str, ...] | None = None
) -> tuple[SourceRecord, ...]:
    source = "Socialstyrelsen/Metadata_Patientregistret (PAR)_webb.xlsx"
    revision = _revision(source)
    register = SourceCoordinate(status="value", name="Patientregistret")
    variable = SourceCoordinate(
        status="value", native_id="INVARN8" if rename else "ATC"
    )
    types = (
        ("text",) * len(subsets)
        if subsets is not None
        else (("integer",) if rename else ("integer", "text"))
    )
    records = []
    for index, data_type in enumerate(types, 1):
        subset = subsets[index - 1] if subsets is not None else "PAR_OV"
        subject = SourceSubject(
            provider="sos",
            register=register,
            variant=SourceCoordinate(status="value", name=subset),
            population=SourceCoordinate(status="not_applicable"),
            variable=variable,
            member=SourceCoordinate(status="value", native_id=str(index)),
            native=NativeCoordinates(),
        )
        locator = RecordLocator(
            semantic_record_key=(f"member:{index}",),
            physical_file="fixture.xlsx",
            physical_table=subset,
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


def _pooled_parallel_fixture(tmp_path, *, co_delivered=False):
    from reg_meta_build.source_curation import PeerGuard, capture_expectations

    header = REGISTERINFORMATION_HEADER.split("|")
    records = []
    for i, (column, edition) in enumerate(
        (("First", "2022"), ("Second", "2022"))
        if co_delivered
        else (("First", "2020-2022"), ("Second", "2022-2024"))
    ):
        values = _var_row(
            colname=column,
            cvid=100 if co_delivered else 100 + i,
            var_id=1,
            varname="Income",
            year="2022" if co_delivered else "2020",
            versionname=edition,
            regver_id=110 if co_delivered else 110 + i,
        ).split("|")
        records.append(
            clean_scb_row(
                header,
                i + 1,
                {
                    name: (True, value, value)
                    for name, value in zip(header, values, strict=True)
                },
                _revision("fixture"),
            ).record
        )
    records = tuple(records)
    register_key = source_register_key(records[0])
    variant_key = native_variant_key(records[0])
    assert register_key is not None and variant_key is not None
    owner = ("accepted", "fixture", "income")
    guard = PeerGuard(
        guard_id="fixture-parallel-family",
        source=records[0].source,
        native=NativeCoordinates(register_id=1, register_variant_id=10, variable_id=1),
        expected_members=tuple(dict.fromkeys(record_ref(r) for r in records)),
    )
    naming = tuple(
        NamingDeclaration(
            target=NativeNamingTarget(
                kind=kind,
                provider="scb",
                source_key=key,
                register_key=None if kind == "register" else register_key,
                expectations=(
                    capture_expectations(records, fields=("column_name",), coding=True)
                    if kind == "variable"
                    else ()
                ),
                peer_guards=(guard,) if kind == "variable" else (),
            ),
            naming=SlugEntry(kind=kind, provider="scb", source_id=source_id, slug=slug),
            contributors=(),
        )
        for kind, key, source_id, slug in (
            ("register", register_key, "1", "sample"),
            ("register_variant", variant_key, "1.10", "people"),
            ("variable", owner, "1.1.income", "income"),
        )
    )
    root = tmp_path / "curation"
    path = root / "registers/scb/sample.toml"
    path.parent.mkdir(parents=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[representation.parallel]]\nvariable = "1.1.income"\n'
        'variant = "1.10"\nvalid_from = "2022-01-01"\n'
        'valid_to = "2022-12-31"\nevidence = "Reviewed original boundary"\n'
        'noted = "2026-09-29"\n'
        'columns = [{column = "First", valid_from = "2020-01-01", '
        'valid_to = "2022-12-31", source_editions = ["2020-2022"]}, '
        '{column = "Second", valid_from = "2022-01-01", '
        'valid_to = "2024-12-31", source_editions = ["2022-2024"]}]\n'
    )
    if co_delivered:
        path.write_text(
            path.read_text()
            .replace("2020-01-01", "2022-01-01")
            .replace("2024-12-31", "2022-12-31")
            .replace("2020-2022", "2022")
            .replace("2022-2024", "2022")
            + 'co_delivered = true\ncolumn_metadata = "per_column"\n'
        )
    return path, records, naming


def _compile_pooled_parallel(path, records, naming):
    from reg_meta_build.curation_compile import compile_parallel_representations

    (register,) = load_register_files(path.parents[2])
    return compile_parallel_representations(register, records, naming)


def _parallel_coding(case):
    from reg_meta_build.source_curation import RepresentationDecision

    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    return {
        column_identity(
            decision.variable_key, decision.variant_key, column.column
        ): resolve_code_membership(())
        for column in decision.columns
    }


@pytest.mark.parametrize("defect", [None, "definition", "missing", "new_member"])
def test_case_aliases_keep_distinct_members_and_refuse_source_drift(tmp_path, defect):
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path, co_delivered=True)
    path.write_text(
        path.read_text()
        .replace('column = "Second"', 'column = "first"')
        .replace("co_delivered = true", "case_aliases = true")
    )
    peer = records[1].model_copy(
        update={
            "fields": records[1].fields.model_copy(
                update={"column_name": value_field("first")}
            ),
            "locators": tuple(
                locator.model_copy(
                    update={
                        "semantic_record_key": (
                            *locator.semantic_record_key,
                            "independent-member",
                        )
                    }
                )
                for locator in records[1].locators
            ),
        }
    )
    originals = (records[0], peer)
    # Naming membership covers both distinct source members, without claiming paired delivery.
    naming = tuple(
        name.model_copy(
            update={
                "target": name.target.model_copy(
                    update={
                        "expectations": capture_expectations(
                            originals, fields=("column_name",), coding=True
                        )
                    }
                )
            }
        )
        if name.target.kind == "variable"
        else name
        for name in naming
    )
    cases, diagnostics = _compile_pooled_parallel(path, originals, naming)
    assert not diagnostics and len(cases) == 1
    proof = resolve_representation_cases(
        originals, cases, coding=_parallel_coding(cases[0])
    )
    assert not proof.diagnostics
    changed = originals
    if defect == "definition":
        changed = (
            originals[0],
            peer.model_copy(
                update={
                    "fields": peer.fields.model_copy(
                        update={"definition": value_field("different quantity")}
                    )
                }
            ),
        )
    elif defect == "missing":
        changed = originals[:1]
    elif defect == "new_member":
        changed = (
            *originals,
            peer.model_copy(
                update={
                    "locators": tuple(
                        locator.model_copy(
                            update={
                                "semantic_record_key": (
                                    *locator.semantic_record_key,
                                    "extra",
                                )
                            }
                        )
                        for locator in peer.locators
                    )
                }
            ),
        )
    if defect:
        assert resolve_representation_cases(
            changed, cases, coding=_parallel_coding(cases[0])
        ).diagnostics
        if defect != "new_member":
            assert _compile_pooled_parallel(path, changed, naming)[1]
    path.write_text(
        path.read_text().replace("case_aliases = true", "co_delivered = true")
    )
    assert _compile_pooled_parallel(path, originals, naming)[1]


def test_co_delivered_parallel_requires_explicit_per_column_opt_in(tmp_path):
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path, co_delivered=True)
    records = (
        records[0],
        records[1].model_copy(
            update={
                "fields": records[1].fields.model_copy(
                    update={"data_type": value_field("text")}
                )
            }
        ),
    )
    cases, diagnostics = _compile_pooled_parallel(path, records, naming)
    assert diagnostics == () and len(cases) == 1
    proof = resolve_representation_cases(
        records, cases, coding=_parallel_coding(cases[0])
    )
    assert proof.diagnostics == ()
    path.write_text(
        path.read_text().replace("co_delivered = true", "co_delivered = false")
    )
    assert _compile_pooled_parallel(path, records, naming)[0] == ()
    path.write_text(
        path.read_text()
        .replace('column_metadata = "per_column"', 'column_metadata = "shared"')
        .replace("co_delivered = false", "co_delivered = true")
    )
    with pytest.raises(RegMetaError) as failure:
        _compile_pooled_parallel(path, records, naming)
    assert "per-column metadata" in failure.value.message


@pytest.mark.parametrize(
    "defect", ["missing", "definition", "foreign_member", "identifier"]
)
def test_co_delivered_parallel_refuses_unsupported_common_quantity(tmp_path, defect):
    path, records, naming = _pooled_parallel_fixture(tmp_path, co_delivered=True)
    if defect == "missing":
        records = records[:1]
    elif defect == "definition":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={"definition": value_field("Different quantity")}
                    )
                }
            ),
        )
    elif defect == "foreign_member":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "locators": tuple(
                        locator.model_copy(
                            update={
                                "semantic_record_key": (
                                    *locator.semantic_record_key[:-1],
                                    "member:999",
                                )
                            }
                        )
                        for locator in records[1].locators
                    )
                }
            ),
        )
    else:
        records = tuple(
            record.model_copy(
                update={
                    "fields": record.fields.model_copy(
                        update={"identifier": value_field(index == 1)}
                    )
                }
            )
            for index, record in enumerate(records)
        )
    cases, diagnostics = _compile_pooled_parallel(path, records, naming)
    assert cases == ()
    assert diagnostics and all(d.severity == "error" for d in diagnostics)


@pytest.mark.parametrize("column_metadata", ["shared", "per_column"])
def test_pooled_parallel_compile_keeps_original_bounds_and_exact_intersection(
    tmp_path,
    column_metadata,
):
    from reg_meta_build.source_curation import RepresentationDecision
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path)
    if column_metadata == "per_column":
        path.write_text(
            path.read_text().replace(
                'variant = "1.10"', 'variant = "1.10"\ncolumn_metadata = "per_column"'
            )
        )
    before = tuple(r.model_dump(mode="json") for r in records)
    cases, diagnostics = _compile_pooled_parallel(path, records, naming)
    assert diagnostics == () and len(cases) == 1
    case = cases[0]
    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    assert (decision.valid_from, decision.valid_to) == ("2022-01-01", "2022-12-31")
    assert [(c.column, c.valid_from, c.valid_to) for c in decision.columns] == [
        ("First", "2022-01-01", "2022-12-31"),
        ("Second", "2022-01-01", "2022-12-31"),
    ]
    assert decision.column_metadata == column_metadata
    proof = resolve_representation_cases(records, cases, coding=_parallel_coding(case))
    assert proof.cases == cases and proof.diagnostics == ()
    assert tuple(r.model_dump(mode="json") for r in records) == before
    assert _compile_pooled_parallel(path, records[::-1], naming) == (cases, diagnostics)


@pytest.mark.parametrize(
    "original,replacement",
    [
        ('valid_from = "2022-01-01"', 'valid_from = "2022-07-01"'),
        ('valid_to = "2022-12-31"', 'valid_to = "2022-06-30"'),
    ],
)
def test_pooled_parallel_partial_intersection_is_rejected(
    tmp_path, original, replacement
):
    path, records, naming = _pooled_parallel_fixture(tmp_path)
    # Only the shared window changes; both authored original windows stay exact.
    path.write_text(path.read_text().replace(original, replacement, 1))
    with pytest.raises(RegMetaError) as failure:
        _compile_pooled_parallel(path, records, naming)
    assert "source-window intersection" in failure.value.message


@pytest.mark.parametrize(
    "original,replacement",
    [
        ('variable = "1.1.income"', 'variable = "1.2.income"'),
        ('variant = "1.10"', 'variant = "1.11"'),
        ('column = "First"', 'column = "Absent"'),
        ('source_editions = ["2020-2022"]', 'source_editions = ["2020-2023"]'),
        ('valid_from = "2020-01-01"', 'valid_from = "2021-01-01"'),
        ('valid_to = "2024-12-31"', 'valid_to = "2025-12-31"'),
    ],
)
def test_pooled_parallel_authored_source_mismatch_withholds_case(
    tmp_path, original, replacement
):
    path, records, naming = _pooled_parallel_fixture(tmp_path)
    path.write_text(path.read_text().replace(original, replacement))
    cases, diagnostics = _compile_pooled_parallel(path, records, naming)
    assert cases == ()
    assert diagnostics and all(d.severity == "error" for d in diagnostics)


@pytest.mark.parametrize(
    "defect", ["missing", "new", "facts", "source_bounds", "coding"]
)
def test_pooled_parallel_original_membership_and_facts_remain_guarded(tmp_path, defect):
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path)
    (case,), diagnostics = _compile_pooled_parallel(path, records, naming)
    assert diagnostics == ()
    changed = records
    if defect == "missing":
        changed = records[:1]
    elif defect == "new":
        peer = records[0].model_copy(
            update={
                "fields": records[0].fields.model_copy(
                    update={"column_name": value_field("Third")}
                ),
                "subject": records[0].subject.model_copy(
                    update={"member": SourceCoordinate(status="value", native_id=999)}
                ),
                "record_id": "new-peer",
            }
        )
        changed = (*records, peer)
    elif defect == "facts":
        changed = (
            records[0].model_copy(
                update={
                    "fields": records[0].fields.model_copy(
                        update={"operational_definition": value_field("Changed")}
                    )
                }
            ),
            records[1],
        )
    elif defect == "coding":
        changed = (
            records[0].model_copy(
                update={
                    "code_set_references": (
                        CodeSetReference(
                            reference_id="changed",
                            content_sha256="a" * 64,
                            physical_locator="codes.csv:1",
                        ),
                    )
                }
            ),
            records[1],
        )
    else:
        changed = (
            records[0].model_copy(
                update={
                    "edition_period_scope": TemporalScope(
                        kind="pooled",
                        label="2020-2023",
                        pooled_start="2020-01-01",
                        pooled_end="2023-12-31",
                    )
                }
            ),
            records[1],
        )
    proof = resolve_representation_cases(
        changed, (case,), coding=_parallel_coding(case)
    )
    assert proof.cases == ()
    assert proof.evaluations[0].status == "stale" and proof.diagnostics


@pytest.mark.parametrize("conflict", [None, "fact", "coding", "explicit_annual"])
def test_pooled_parallel_reconciliation_preserves_outer_windows_and_conflicts(
    tmp_path, conflict
):
    from reg_meta_build.catalog_dependencies import check_delivery_coverage
    from reg_meta_build.source_curation import RepresentationDecision
    from reg_meta_build.source_records import ScopeInterval
    from reg_meta_build.source_representations import resolve_representation_cases

    path, records, naming = _pooled_parallel_fixture(tmp_path)
    if conflict == "explicit_annual":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "edition_scope": TemporalScope(
                        kind="intervals",
                        intervals=(ScopeInterval(start="2022", end="2024"),),
                    ),
                    "edition_period_scope": TemporalScope(
                        kind="intervals",
                        intervals=(
                            ScopeInterval(start="2022-01-01", end="2024-12-31"),
                        ),
                    ),
                }
            ),
        )
    elif conflict == "fact":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={"data_type": value_field("text")}
                    )
                }
            ),
        )
    (case,), diagnostics = _compile_pooled_parallel(path, records, naming)
    assert diagnostics == ()
    decision = case.decision
    assert isinstance(decision, RepresentationDecision)
    coding = _parallel_coding(case)
    if conflict == "coding":
        for column, code in (("First", "01"), ("Second", "02")):
            coding[
                column_identity(decision.variable_key, decision.variant_key, column)
            ] = resolve_code_membership(
                (
                    CodeListClaim(
                        column,
                        TemporalScope(
                            kind="intervals",
                            intervals=(
                                ScopeInterval(start="2020-01-01", end="2024-12-31"),
                            ),
                        ),
                        (
                            CodeMembershipClaim(
                                code,
                                "Label",
                                TemporalScope(kind="year_independent"),
                            ),
                        ),
                    ),
                )
            )
    proof = resolve_representation_cases(records, (case,), coding=coding)
    assert proof.cases == (case,) and proof.diagnostics == ()
    occurrences = tuple(
        replace(
            source_occurrence(record),
            variable_key=decision.variable_key,
            identity_checked=True,
        )
        for record in records
    )
    formed = form_native_variable(
        occurrences,
        register=ResolvedRegister(provider="scb", slug="sample", name="Sample"),
        variants={decision.variant_key: ResolvedVariant(slug="people", name="People")},
        slug="income",
        provider_key="family",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding=coding,
        representations=proof.cases,
    )
    assert formed.variable is not None
    assert [(s.valid_from, s.valid_to) for s in formed.variable.states] == [
        ("2020-01-01", "2021-12-31"),
        ("2022-01-01", "2022-12-31"),
        ("2023-01-01", "2024-12-31"),
    ]
    assert formed.variable.states[0].delivery_column_name == "First"
    assert formed.variable.states[2].delivery_column_name == "Second"
    check_delivery_coverage((formed.variable,), formed.coverage, withheld={})
    shared = formed.variable.states[1]
    assert [state.pooled for state in formed.variable.states] == (
        [True, False, False] if conflict == "explicit_annual" else [True, True, True]
    )
    assert shared.provenance is not None and case.case_id in shared.provenance
    witnesses = tuple(
        original
        for occurrence in formed.occurrences
        for original in occurrence.source_records
    )
    assert {r.record_id for r in witnesses} == {r.record_id for r in records}
    assert {r.record_id: r.edition_period_scope for r in witnesses} == {
        r.record_id: r.edition_period_scope for r in records
    }
    if conflict == "fact":
        assert shared.data_type is None
        assert ("conflicting_representation_fact", ("data_type",)) in {
            (d.code, d.fields) for d in formed.diagnostics
        }
        assert formed.variable.states[0].data_type == "integer"
        assert formed.variable.states[2].data_type == "text"
    elif conflict == "coding":
        assert shared.value_set is None
        assert any(
            d.code == "conflicting_representation_coding" for d in formed.diagnostics
        )
        assert [
            state.value_set.members
            for state in (formed.variable.states[0], formed.variable.states[2])
        ] == [(("01", "Label"),), (("02", "Label"),)]
        assert all(
            claim.coding_claim is None
            for claim in formed.coverage
            if claim.valid_from == "2022-01-01"
        )
        changed_outer = formed.variable.model_copy(
            update={
                "states": (
                    formed.variable.states[0].model_copy(update={"value_set": None}),
                    *formed.variable.states[1:],
                )
            }
        )
        with pytest.raises(ValueError, match="claimed coding"):
            check_delivery_coverage((changed_outer,), formed.coverage, withheld={})
    else:
        assert formed.diagnostics == ()
    assert [
        (r.edition_period_scope.pooled_start, r.edition_period_scope.pooled_end)
        for r in records
    ] == [
        ("2020-01-01", "2022-12-31"),
        (None, None) if conflict == "explicit_annual" else ("2022-01-01", "2024-12-31"),
    ]
    assert {
        (a.delivery_column_name, w.valid_from, w.valid_to)
        for a in formed.variable.aliases
        for w in a.windows
    } == {
        ("First", "2022-01-01", "2022-12-31"),
        ("Second", "2022-01-01", "2022-12-31"),
    }


def _checked_correction_fixture(tmp_path, *, period=False):
    root = tmp_path / "curation"
    _scb_partition_tree(root, "")
    selected = _errata_record(column="ANSWER", year="2009", edition_id=99)
    negative = _errata_record(column="ANSWER", year="2017", edition_id=100, member=21)
    tree = load_curation_tree(root)
    scope = _partition_scope((selected, negative))
    base = {
        "variable": "1.5",
        "variant": "1.2",
        "column": "ANSWER",
        "edition": "99",
        "expected_fields": list(
            capture_expectations(
                (selected,),
                fields=("name", "definition", "description", "operational_definition"),
            )[0]
            .alternatives[0]
            .fields
        ),
        "expected_period_text": selected.original_period_text,
        "expected_scope": selected.edition_scope,
        "expected_period": selected.edition_period_scope,
        "evidence": "Exact supplied definition establishes the reviewed correction.",
        "noted": "2026-09-30",
    }
    if period:
        entry = ErrataOccurrencePeriodEntry(
            **base,
            edition_scope=TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start="2016-01-01", end=None),),
            ),
            edition_period_scope=TemporalScope(
                kind="unknown", label="Original unknown period retained"
            ),
        )
        field = "occurrence_period"
    else:
        entry = ErrataFieldEntry(**base, field="name", value="Reviewed label")
        field = "field"
    reg = next(r for r in tree.registers if r.register_info.slug == "sample")
    reg = reg.model_copy(
        update={"errata": reg.errata.model_copy(update={field: [entry]})}
    )
    return replace(tree, registers=(reg,)), scope, selected, negative, entry


def _run_checked_correction(tree, scope, records):
    native = native_variable_key(records[0])
    reader = SimpleNamespace(
        iter_native_families=lambda source, registers=None, select_family=None: iter(
            ((native, records),)
        )
    )
    return compile_occurrence_corrections(
        tree, cast("Any", SimpleNamespace(records=reader)), (scope,), subset=False
    )


@pytest.mark.parametrize("period", [False, True])
def test_checked_occurrence_corrections_preserve_originals_and_unselected_editions(
    tmp_path, period
):
    tree, scope, original, negative, entry = _checked_correction_fixture(
        tmp_path, period=period
    )
    cases, issues, report = _run_checked_correction(tree, scope, (original, negative))
    assert not issues
    assert len(report["scb/sample"]["entries_matched"]) == 1
    result = apply_occurrence_cases((original, negative), cases[scope.source, None])
    assert not result.diagnostics
    changed, untouched = result.occurrences
    assert changed.source_records == (original,)
    assert untouched == source_occurrence(negative)
    if period:
        assert changed.edition_scope == entry.edition_scope
        assert changed.edition_period_scope == entry.edition_period_scope
        assert changed.edition_scope.intervals[0].end is None
        assert changed.fields == original.fields
    else:
        assert changed.fields.name.value == entry.value
        assert changed.edition_scope == original.edition_scope
        assert changed.edition_period_scope == original.edition_period_scope
    assert changed.variable_key == native_variable_key(original)


@pytest.mark.parametrize(
    "change", ["prose", "period", "period-text", "missing", "new-peer", "new-match"]
)
def test_checked_occurrence_corrections_fail_closed_on_source_changes(tmp_path, change):
    tree, scope, original, negative, _ = _checked_correction_fixture(tmp_path)
    cases, _, _ = _run_checked_correction(tree, scope, (original, negative))
    records = (original, negative)
    if change == "prose":
        records = (
            original.model_copy(
                update={
                    "fields": original.fields.model_copy(
                        update={"description": value_field("Changed source meaning")}
                    )
                }
            ),
            negative,
        )
    elif change == "period":
        records = (
            original.model_copy(
                update={
                    "edition_scope": TemporalScope(
                        kind="pooled",
                        label="Unknown annual assignment",
                        pooled_start="2009-01-01",
                        pooled_end="2010-12-31",
                    )
                }
            ),
            negative,
        )
    elif change == "period-text":
        records = (
            original.model_copy(
                update={"original_period_text": "Changed original edition text"}
            ),
            negative,
        )
    elif change == "missing":
        records = (negative,)
    elif change == "new-match":
        records = (
            *records,
            _errata_record(column="ANSWER", year="2009", member=22, edition_id=99),
        )
    else:
        records = (
            *records,
            _errata_record(column="ANSWER", year="2018", member=22, edition_id=101),
        )
    result = apply_occurrence_cases(records, cases[scope.source, None])
    if change != "period-text":
        assert result.diagnostics
        assert result.occurrences == tuple(source_occurrence(r) for r in records)
    if change != "new-peer":
        refreshed, issues, _ = _run_checked_correction(tree, scope, records)
        assert issues and not refreshed


def test_checked_text_corrections_compose_and_withhold_conflicting_assignments(
    tmp_path,
):
    tree, scope, original, negative, entry = _checked_correction_fixture(tmp_path)
    reg = tree.registers[0]
    conflicting = entry.model_copy(update={"value": "Other reviewed label"})
    reg = reg.model_copy(
        update={"errata": reg.errata.model_copy(update={"field": [entry, conflicting]})}
    )
    cases, issues, _ = _run_checked_correction(
        replace(tree, registers=(reg,)), scope, (original, negative)
    )
    assert not issues
    result = apply_occurrence_cases((original, negative), cases[scope.source, None])
    assert result.diagnostics
    assert result.occurrences[0].fields.name.status == "unknown"
    assert result.occurrences[0].source_records == (original,)
    assert result.occurrences[1] == source_occurrence(negative)


def _shared_ref_column_fixture(tmp_path):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.first"\nslug = "first"\n'
        '[[variable]]\nnative_id = "1.5.second"\nslug = "second"\n'
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.2"\n'
        'column = "FIRST"\nowner = "1.5.first"\nref = "first construct"\n'
        'source_editions = ["2020"]\n'
        '[[identity.column_owner]]\nvariable = "1.5"\nvariant = "1.2"\n'
        'column = "SECOND"\nowner = "1.5.second"\nref = "second construct"\n'
        'source_editions = ["2020"]\n',
    )
    records = tuple(
        _errata_record(column=column, year="2020", member=20)
        for column in ("FIRST", "SECOND")
    )
    assert record_ref(records[0]) == record_ref(records[1])
    return root, records


def _shared_ref_field_fixture(tmp_path):
    root, records = _shared_ref_column_fixture(tmp_path)
    tree = load_curation_tree(root)
    first = records[0]
    entry = ErrataFieldEntry(
        variable="1.5",
        variant="1.2",
        column="FIRST",
        edition="2020",
        expected_fields=list(
            capture_expectations(
                (first,),
                fields=("name", "definition", "description", "operational_definition"),
            )[0]
            .alternatives[0]
            .fields
        ),
        expected_period_text=first.original_period_text,
        expected_scope=first.edition_scope,
        expected_period=first.edition_period_scope,
        field="name",
        value="Reviewed first construct",
        evidence="Reviewed exact literal",
        noted="2026-09-30",
    )
    reg = next(r for r in tree.registers if r.register_info.slug == "sample")
    tree = replace(
        tree,
        registers=(
            reg.model_copy(
                update={"errata": reg.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    scope = _partition_scope(records)
    cases, issues, _ = _run_checked_correction(tree, scope, records)
    assert not issues
    return tree, scope, records, cases[scope.source, None]


def test_checked_field_shared_ref_changes_only_target_literal(tmp_path):
    _, _, records, cases = _shared_ref_field_fixture(tmp_path)
    result = apply_occurrence_cases(records, cases)
    assert not result.diagnostics
    assert result.occurrences[0].fields.name.value == "Reviewed first construct"
    assert result.occurrences[1] == source_occurrence(records[1])
    assert result.occurrences[0].source_records == (records[0],)
    assert len(cases[0].targets[0].alternatives) == 2


@pytest.mark.parametrize("change", ["changed", "missing", "new"])
@pytest.mark.parametrize("field_correction", [False, True])
def test_shared_ref_literal_effects_reject_changed_physical_peers(
    tmp_path, change, field_correction
):
    if field_correction:
        _, _, records, cases = _shared_ref_field_fixture(tmp_path)
    else:
        root, records = _shared_ref_column_fixture(tmp_path)
        compiled, key, _ = _compile_partition_fixture(root, records)
        cases = compiled[0][key]
    if change == "missing":
        altered = records[:1]
    elif change == "new":
        altered = (*records, _errata_record(column="THIRD", year="2020", member=20))
    else:
        altered = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={
                            (
                                "name" if field_correction else "column_name"
                            ): value_field("Changed second construct")
                        }
                    )
                }
            ),
        )
    result = apply_occurrence_cases(altered, cases)
    assert result.diagnostics
    assert result.occurrences == tuple(source_occurrence(r) for r in altered)


def test_checked_period_correction_rejects_shared_ref_physical_columns(tmp_path):
    tree, scope, selected, negative, _ = _checked_correction_fixture(
        tmp_path, period=True
    )
    twin = _errata_record(column="OTHER", year="2009", edition_id=99)
    assert record_ref(twin) == record_ref(selected)
    cases, issues, report = _run_checked_correction(
        tree, scope, (selected, twin, negative)
    )
    assert issues and not cases
    assert len(report["scb/sample"]["over_broad"]) == 1


@pytest.mark.parametrize("label", ["Unknown", "Substantive industry"])
def test_compile_scoped_sentinel_requires_exact_members_and_preserves_guards(
    tmp_path, label
):
    from reg_meta_build.resolved_catalog import (
        ResolvedClassification,
        ResolvedClassificationCode,
    )
    from reg_meta_build.source_classification_bindings import apply_classification_cases
    from reg_meta_build.source_curation import ClassificationDecision

    record = _errata_record(column="VALUE", year="2021", member=20)
    fragment = (
        '\n[[coding.sentinel]]\nvariable = "1.5"\nvariant = "people"\n'
        'column = "VALUE"\nclassification = "fixture"\n'
        'periods = [["2021-01-01", "2021-12-31"]]\n'
        'members = [["99", "Unknown"]]\n'
        'reason = "Exact supplied unknown marker"\nsource = "Reviewed complete source list"\n'
    )
    tree, _, scope = _errata_fixture(tmp_path, (record,), fragment)
    scope = _scope_with_coding_names(scope, record)
    register = next(r for r in tree.registers if r.register_info.slug == "sample")
    occurrence = source_occurrence(record)
    column = occurrence.column_key
    assert column is not None
    claims = (
        CodeListClaim(
            "list",
            record.edition_period_scope,
            (
                CodeMembershipClaim(
                    "01", "Category", TemporalScope(kind="year_independent")
                ),
                CodeMembershipClaim(
                    "99", label, TemporalScope(kind="year_independent")
                ),
            ),
        ),
    )
    book = ResolvedClassification(
        slug="fixture",
        short_name="FIX",
        name="Fixture",
        codes=(ResolvedClassificationCode(code="01", label="Canonical"),),
    )
    evidence = SourceEvidence((record,), effective_occurrences=(occurrence,))
    cases, issues = compile_coding_register(
        register,
        scope,
        originals=(record,),
        columns={column: (record,)},
        column_scopes=evidence.effective_scopes or {},
        coding={column: claims},
        classifications={"fixture": book},
    )
    if label != "Unknown":
        assert not cases and [d.code for d in issues] == ["stale_curation_entry"]
        return
    assert len(cases) == 1 and not issues
    assert isinstance(cases[0].decision, ClassificationDecision)
    assert cases[0].decision.sentinel_members == (("99", "Unknown"),)
    result = apply_classification_cases(
        evidence,
        cases,
        coding={column: resolve_code_membership(claims)},
        classifications={"fixture": book},
    )
    assert result.coding[column].claims == claims
    assert tuple(
        link.classification
        for link in result.coding[column].segments[0].classification_links
    ) == ("fixture",)
    assert result.coding[column].segments[0].classification_links[
        0
    ].conformance.sentinel_members == (("99", "Unknown"),)
    changed = record.model_copy(
        update={
            "fields": record.fields.model_copy(
                update={"description": value_field("Changed description")}
            )
        }
    )
    stale = apply_classification_cases(
        SourceEvidence((changed,), effective_occurrences=(source_occurrence(changed),)),
        cases,
        coding={column: resolve_code_membership(claims)},
        classifications={"fixture": book},
    )
    assert stale.evaluations[0].status != "applicable"
    assert all(
        not segment.classification_links for segment in stale.coding[column].segments
    )


@pytest.mark.parametrize("change", [None, "coverage", "missing-coverage-guard"])
def test_sos_period_correction_without_native_edition_preserves_disjoint_years(
    tmp_path: Path, change
):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers/sos/par.toml"
    path.parent.mkdir(parents=True)
    path.write_text(
        '[register]\nprovider = "sos"\nslug = "par"\nnative_id = "5891427617861710725"\n'
    )
    original, negative = _sos_partition_records(subsets=("PAR_OV", "PAR_SV"))
    original = original.model_copy(
        update={
            "fields": original.fields.model_copy(
                update={
                    "availability": value_field(True),
                    "coverage_from": value_field("2011 och 2013"),
                    "coverage_to": value_field("2011 och 2013"),
                }
            ),
            "edition_scope": TemporalScope(kind="unknown", label="2011 och 2013"),
        }
    )
    intervals = TemporalScope(
        kind="intervals",
        intervals=(
            ScopeInterval(start="2011", end="2011"),
            ScopeInterval(start="2013", end="2013"),
        ),
    )
    entry = ErrataOccurrencePeriodEntry(
        variable="5891427617861710725.ATC",
        variant="PAR_OV",
        column="ATC",
        expected_fields=list(
            capture_expectations(
                (original,),
                fields=(
                    "name",
                    "definition",
                    "description",
                    "operational_definition",
                    "coverage_from",
                    "coverage_to",
                ),
            )[0]
            .alternatives[0]
            .fields
        ),
        expected_scope=original.edition_scope,
        expected_period=original.edition_period_scope,
        edition_scope=intervals,
        edition_period_scope=original.edition_period_scope,
        evidence="Both supplied coverage bounds name exactly 2011 and 2013.",
        noted="2026-09-30",
    )
    tree = load_curation_tree(root)
    reg = next(r for r in tree.registers if r.register_info.slug == "par")
    reg = reg.model_copy(
        update={"errata": reg.errata.model_copy(update={"occurrence_period": [entry]})}
    )
    tree = replace(tree, registers=(reg,))
    scope = _partition_scope((original, negative))
    cases, issues, _ = _run_checked_correction(tree, scope, (original, negative))
    assert not issues
    if change == "coverage":
        changed = original.model_copy(
            update={
                "fields": original.fields.model_copy(
                    update={"coverage_to": value_field("2011-2013")}
                )
            }
        )
        refused = apply_occurrence_cases((changed, negative), cases[scope.source, None])
        assert all(
            o == source_occurrence(r)
            for o, r in zip(refused.occurrences, (changed, negative), strict=True)
        )
        original = changed
    elif change == "missing-coverage-guard":
        bad = entry.model_copy(
            update={
                "expected_fields": [
                    f for f in entry.expected_fields if f.name != "coverage_from"
                ]
            }
        )
        reg = reg.model_copy(
            update={
                "errata": reg.errata.model_copy(update={"occurrence_period": [bad]})
            }
        )
        tree = replace(tree, registers=(reg,))
    if change is not None:
        fresh, diagnostics, _ = _run_checked_correction(
            tree, scope, (original, negative)
        )
        assert diagnostics and not fresh
        return
    result = apply_occurrence_cases((original, negative), cases[scope.source, None])
    assert not result.diagnostics
    changed, untouched = result.occurrences
    assert changed.edition_scope == intervals
    assert changed.edition_period_scope == original.edition_period_scope
    assert changed.source_records == (original,)
    assert untouched == source_occurrence(negative)
    assert replace(
        changed, edition_scope=original.edition_scope, corrections=()
    ) == source_occurrence(original)

    formed = form_native_variable(
        (changed,),
        register=ResolvedRegister(provider="sos", slug="par", name="Patientregistret"),
        variants={
            native_variant_key(original): ResolvedVariant(slug="par-ov", name="PAR_OV")
        },
        slug="sequence",
        provider_key="ATC",
        coding={changed.column_key: resolve_code_membership(())},
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
    )
    assert formed.variable is not None
    assert [(state.valid_from, state.valid_to) for state in formed.variable.states] == [
        ("2011-01-01", "2011-12-31"),
        ("2013-01-01", "2013-12-31"),
    ]


def test_scb_period_correction_still_requires_native_edition(tmp_path: Path):
    tree, scope, original, negative, entry = _checked_correction_fixture(
        tmp_path, period=True
    )
    reg = tree.registers[0]
    bad = entry.model_copy(update={"edition": None})
    reg = reg.model_copy(
        update={"errata": reg.errata.model_copy(update={"occurrence_period": [bad]})}
    )
    cases, issues, _ = _run_checked_correction(
        replace(tree, registers=(reg,)), scope, (original, negative)
    )
    assert issues and not cases


def test_classification_reference_corrections_preserve_shared_ref_other_windows(
    tmp_path,
):
    tree, scope, original, _, base = _checked_correction_fixture(tmp_path)
    original = original.model_copy(
        update={
            "fields": original.fields.model_copy(
                update={
                    "classification_declared": value_field("Incorrect municipal list"),
                    "representation": value_field("Six-digit district code"),
                    "coverage_from": value_field("2009"),
                    "coverage_to": value_field("2009"),
                }
            )
        }
    )
    negative = original.model_copy(
        update={
            "record_id": original.record_id + "-other-window",
            "fields": original.fields.model_copy(
                update={
                    "classification_declared": value_field("District reference"),
                    "coverage_from": value_field("2017"),
                    "coverage_to": value_field("2017"),
                }
            ),
            "edition_scope": TemporalScope(
                kind="intervals", intervals=(ScopeInterval(start="2017", end="2017"),)
            ),
        }
    )
    assert record_ref(original) == record_ref(negative)
    entry = ErrataFieldEntry(
        **{
            **base.model_dump(exclude={"field", "value", "expected_fields"}),
            "expected_scope": original.edition_scope,
            "expected_fields": list(
                capture_expectations(
                    (original,),
                    fields=(
                        "name",
                        "definition",
                        "description",
                        "operational_definition",
                        "classification_declared",
                        "representation",
                        "data_type",
                        "coverage_from",
                        "coverage_to",
                    ),
                )[0]
                .alternatives[0]
                .fields
            ),
        },
        field="classification_declared",
        value="District reference",
    )
    register = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={"errata": register.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (original, negative))
    assert not issues
    result = apply_occurrence_cases((original, negative), cases[scope.source, None])
    assert not result.diagnostics
    changed, untouched = result.occurrences
    assert changed.source_records == (original,)
    assert changed.fields.classification_declared.value == "District reference"
    assert changed.edition_scope == original.edition_scope
    assert untouched == source_occurrence(negative)
    for field in ("classification_declared", "representation", "coverage_from"):
        changed_source = original.model_copy(
            update={
                "fields": original.fields.model_copy(
                    update={field: value_field("Changed source assertion")}
                )
            }
        )
        _, stale, _ = _run_checked_correction(tree, scope, (changed_source, negative))
        assert stale and stale[0].code == "stale_curation_entry"


def test_classification_reference_corrections_require_complete_metadata_guards(
    tmp_path,
):
    _, _, _, _, base = _checked_correction_fixture(tmp_path)
    with pytest.raises(ValueError, match="classification corrections require"):
        ErrataFieldEntry(
            **base.model_dump(exclude={"field", "value"}),
            field="classification_declared",
            value="District reference",
        )


def test_measurement_unit_correction_guards_shared_ref_physical_assertions(tmp_path):
    tree, scope, source, _, base = _checked_correction_fixture(tmp_path)
    original = source.model_copy(
        update={
            "fields": source.fields.model_copy(
                update={"measurement_unit": value_field("Antal")}
            )
        }
    )
    negative = original.model_copy(
        update={
            "record_id": original.record_id + "-other-unit",
            "fields": original.fields.model_copy(
                update={"measurement_unit": value_field("Kronor (SEK)")}
            ),
        }
    )
    fields = (
        "name",
        "definition",
        "description",
        "operational_definition",
        "classification_declared",
        "representation",
        "data_type",
        "coverage_from",
        "coverage_to",
        "measurement_unit",
    )
    guards = list(
        capture_expectations((original,), fields=fields)[0].alternatives[0].fields
    )
    entry = ErrataFieldEntry(
        **{
            **base.model_dump(exclude={"field", "value", "expected_fields"}),
            "expected_fields": guards,
        },
        field="measurement_unit",
        value="Antal personer",
    )
    register = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={"errata": register.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (original, negative))
    assert not issues
    result = apply_occurrence_cases((original, negative), cases[scope.source, None])
    assert not result.diagnostics
    changed, untouched = result.occurrences
    assert changed.fields.measurement_unit.value == "Antal personer"
    assert changed.source_records == (original,)
    assert changed.edition_scope == original.edition_scope
    assert untouched == source_occurrence(negative)
    for field in ("measurement_unit", "definition", "representation"):
        altered = original.model_copy(
            update={
                "fields": original.fields.model_copy(
                    update={field: value_field("Changed supplied assertion")}
                )
            }
        )
        _, stale, _ = _run_checked_correction(tree, scope, (altered, negative))
        assert stale and stale[0].code == "stale_curation_entry"
    for missing in ("measurement_unit", "representation", "coverage_from"):
        with pytest.raises(ValueError):
            entry.model_validate(
                {
                    **entry.model_dump(),
                    "expected_fields": [
                        g.model_dump() for g in guards if g.name != missing
                    ],
                }
            )


def test_checked_name_correction_keeps_same_ref_other_source_scope(tmp_path):
    tree, scope, original, _, _ = _checked_correction_fixture(tmp_path)
    other = original.model_copy(
        update={
            "record_id": original.record_id + "-monthly",
            "edition_scope": TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start="2008-01-01", end="2008-12-31"),),
            ),
            "fields": original.fields.model_copy(
                update={"name": value_field("Monthly observation")}
            ),
        }
    )
    assert record_ref(original) == record_ref(other)
    cases, issues, _ = _run_checked_correction(tree, scope, (original, other))
    assert not issues
    result = apply_occurrence_cases((original, other), cases[scope.source, None])
    assert not result.diagnostics
    assert result.occurrences[0].fields.name.value == "Reviewed label"
    assert result.occurrences[1] == source_occurrence(other)
    drifted = original.model_copy(update={"edition_scope": other.edition_scope})
    _, stale, _ = _run_checked_correction(tree, scope, (drifted, other))
    assert stale and stale[0].code == "stale_curation_entry"
    replay = apply_occurrence_cases((drifted, other), cases[scope.source, None])
    assert replay.diagnostics


@pytest.mark.parametrize(
    "guards,editions",
    [
        ('[{ name = "measurement_unit", status = "value", value = "SEK" }]', "[]"),
        (
            '[{ name = "measurement_unit", status = "value", value = "SEK" }, { name = "measurement_unit", status = "value", value = "SEK" }]',
            '["2020"]',
        ),
    ],
)
def test_guarded_column_owner_requires_finite_unique_fields(guards, editions):
    from pydantic import ValidationError
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry

    with pytest.raises(ValidationError):
        IdentityColumnOwnerEntry.model_validate(
            {
                "variable": "1.5",
                "variant": "1.2",
                "column": "ANSWER",
                "owner": "1.5.amount",
                "ref": "role",
                "source_editions": json.loads(editions),
                "expected_fields": json.loads(
                    guards.replace("name = ", '"name": ')
                    .replace("status = ", '"status": ')
                    .replace("value = ", '"value": ')
                ),
            }
        )


def test_guarded_column_owner_allows_operation_only_guard():
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry

    entry = IdentityColumnOwnerEntry.model_validate(
        {
            "variable": "1.5",
            "variant": "1.2",
            "column": "ANSWER",
            "owner": "1.5.amount",
            "ref": "role",
            "source_editions": ["2020"],
            "expected_fields": [
                {"name": "operational_definition", "status": "value", "value": "Amount"}
            ],
        }
    )
    assert entry.expected_fields[0].value == "Amount"


@pytest.mark.parametrize("end", ["2020-12-31", None])
def test_documented_source_scope_has_reportable_window_when_source_missing(
    tmp_path: Path, end
):
    from reg_meta_build.curation_compile import coding_entry_windows
    from reg_meta_build.curation_tree import (
        CodingDocumentedEntry,
        PreparedCodingAuthority,
    )

    root = tmp_path / "curation"
    _scb_partition_tree(root, "")
    record = _errata_record(column="ANSWER", year="2020", member=20)
    revision = SourceRevision.create(
        dataset=record.source,
        publisher="SCB",
        purpose="coding fixture",
        upstream_revision="1",
        artifact_path="records.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    authority = PreparedCodingAuthority(
        revision=revision,
        source_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020-01-01", end=end),)
        ),
        locators=list(record.locators),
        records=list(
            capture_expectations(
                (record,),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        codings=["b" * 64],
    )
    entry = CodingDocumentedEntry(
        variable="1.999",
        variant="1.2",
        column="MISSING",
        reason="Exact supplied source domain",
        source="fixture",
        members=(("J", "Yes"), ("N", "No")),
        version_label="Source list",
        source_authority=authority,
    )
    assert entry.periods == []
    assert coding_entry_windows(entry) == (("2020-01-01", end or "9999-12-31"),)
    tree = load_curation_tree(root)
    register = tree.registers[0]
    register = register.model_copy(
        update={"coding": register.coding.model_copy(update={"documented": [entry]})}
    )
    cases, diagnostics = compile_coding_register(
        register,
        _partition_scope((record,)),
        originals=(record,),
        columns={},
        column_scopes={},
        coding={},
    )
    assert not cases
    assert len(diagnostics) == 1
    assert diagnostics[0].code == "stale_curation_entry"
    assert diagnostics[0].case_id.endswith("coding.documented/1/period/1")
    assert (diagnostics[0].valid_from, diagnostics[0].valid_to) == coding_entry_windows(
        entry
    )[0]


@pytest.mark.parametrize("unlisted_peer", [False, True])
def test_finite_scoped_owner_closes_only_fully_covered_unassigned_literal(
    unlisted_peer,
):
    records = tuple(
        _errata_record(column="ANSWER", year=year, member=20 + i)
        for i, year in enumerate(("2020", "2021"))
    )
    owners = {(record_ref(record), "ANSWER"): "1.5.answer" for record in records}
    records += (_errata_record(column="KNOWN", year="1999", member=19),)
    if unlisted_peer:
        records += (_errata_record(column="ANSWER", year="2022", member=22),)
    result = convert_column_partitions(
        records,
        source_id="1.5",
        split_ids=("1.5.answer",),
        declared_columns={"ANSWER": None, "KNOWN": "1.5.answer"},
        declaration_reference="Explicitly withheld except finite reviewed owners",
        scoped_owners=owners,
    )
    assert bool(result.diagnostics) is unlisted_peer
    assert result.case is not None
    after = apply_occurrence_cases(records, (result.case,))
    assert not after.diagnostics
    for occurrence in after.occurrences:
        record = occurrence.source_records[0]
        if (
            record.fields.column_name.value == "KNOWN"
            or (record_ref(record), "ANSWER") in owners
        ):
            assert occurrence.variable_key[-1] == "1.5.answer"
        else:
            assert occurrence.variable_key == source_occurrence(record).variable_key


@pytest.mark.parametrize("drift", [None, "unit", "missing", "new", "partial", "window"])
def test_delivery_metadata_compiler_keeps_complete_literal_source_guards(
    tmp_path, drift
):
    from reg_meta_build.curation_compile import compile_delivery_metadata
    from reg_meta_build.curation_tree import DeliveryMetadataEntry
    from reg_meta_build.source_curation import (
        DeliveryMetadataColumn,
        capture_expectations,
    )
    from reg_meta_build.source_records import SourceFields, value_field

    path, raw, naming = _pooled_parallel_fixture(tmp_path)
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={
                        "measurement_unit": value_field(unit),
                        "definition": value_field("Source income definition"),
                    }
                )
            }
        )
        for record, unit in zip(raw, ("100-tal kronor", "Kronor (SEK)"), strict=True)
    )
    entry = DeliveryMetadataEntry(
        fields=["name"],
        variable="1.1.income",
        records=list(
            capture_expectations(
                records,
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        columns=[
            DeliveryMetadataColumn(
                variant_key=native_variant_key(record),
                column=record.fields.column_name.value,
                valid_from=start,
                valid_to=end,
                expected_codings=(),
            )
            for record, start, end in zip(
                records,
                ("2020-01-01", "2022-01-01"),
                ("2022-12-31", "2024-12-31"),
                strict=True,
            )
        ],
        evidence="Retain source encoding without value conversion",
        noted="2026-09-30",
    )
    (register,) = load_register_files(path.parents[2])
    register = register.model_copy(
        update={
            "representation": register.representation.model_copy(
                update={"parallel": [], "delivery_metadata": [entry]}
            )
        }
    )
    if drift == "unit":
        records = (
            records[0],
            records[1].model_copy(
                update={
                    "fields": records[1].fields.model_copy(
                        update={"measurement_unit": value_field("changed")}
                    )
                }
            ),
        )
    elif drift == "missing":
        records = records[:-1]
    elif drift == "new":
        new = records[0].model_copy(
            update={
                "locators": (
                    records[0]
                    .locators[0]
                    .model_copy(
                        update={
                            "semantic_record_key": (
                                *records[0].locators[0].semantic_record_key,
                                "new-peer",
                            )
                        }
                    ),
                )
            }
        )
        records = (*records, new)
    elif drift == "partial":
        register = register.model_copy(
            update={
                "representation": register.representation.model_copy(
                    update={
                        "delivery_metadata": [
                            entry.model_copy(update={"records": entry.records[:-1]})
                        ]
                    }
                )
            }
        )
    elif drift == "window":
        register = register.model_copy(
            update={
                "representation": register.representation.model_copy(
                    update={
                        "delivery_metadata": [
                            entry.model_copy(
                                update={
                                    "columns": [
                                        entry.columns[0].model_copy(
                                            update={"valid_to": "2021-12-31"}
                                        ),
                                        entry.columns[1],
                                    ]
                                }
                            )
                        ]
                    }
                )
            }
        )
    from reg_meta_build.source_curation import (
        CurationCase,
        OccurrenceCorrectionDecision,
        PeerGuard,
    )

    original_targets = tuple(entry.records)
    identity = CurationCase(
        case_id="unit-owner",
        targets=original_targets,
        peer_guards=(
            PeerGuard(
                guard_id="unit-identity-peers",
                source=original_targets[0].ref.source,
                native=NativeCoordinates(
                    register_id=1, register_variant_id=10, variable_id=1
                ),
                expected_members=tuple(target.ref for target in original_targets),
            ),
        ),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(
                CheckedIdentityChange(
                    ref=target.ref, variable_key=naming[-1].target.source_key
                )
                for target in original_targets
            ),
            reason="Checked owner",
            provenance="Exact family",
        ),
    )
    cases, diagnostics = compile_delivery_metadata(
        register, records, naming, ownership_cases=(identity,)
    )
    assert bool(cases) == (drift is None)
    assert bool(diagnostics) == (drift is not None)


def test_disjoint_edition_splits_share_native_variant_but_not_target_edition(
    tmp_path: Path,
) -> None:
    root = tmp_path / "curation"
    path = root / "registers/scb/sample.toml"
    path.parent.mkdir(parents=True)
    (root / "classifications").mkdir()
    prefix = (
        '[register]\nprovider="scb"\nslug="sample"\nnative_id="1"\n'
        '[[variant]]\nnative_id="1.2"\nslug="flow"\n'
        '[[variant]]\nnative_id="1.2.marriage"\nslug="marriage"\n'
        '[[variant]]\nnative_id="1.2.divorce"\nslug="divorce"\n'
    )
    entries = (
        '[[identity.edition_split]]\nvariant="1.2"\nsplit="1.2.marriage"\n'
        'editions=["Married"]\nsource_editions=["Divorced"]\n'
        'evidence="Explicit source event edition"\nnoted="2026-09-30"\n'
        '[[identity.edition_split]]\nvariant="1.2"\nsplit="1.2.divorce"\n'
        'editions=["Divorced"]\nsource_editions=["Married"]\n'
        'evidence="Explicit source event edition"\nnoted="2026-09-30"\n'
    )
    path.write_text(prefix + entries)
    assert len(load_curation_tree(root).registers[0].identity.edition_split) == 2
    path.write_text(
        prefix
        + entries.replace(
            'editions=["Divorced"]\nsource_editions=["Married"]',
            'editions=["Married"]\nsource_editions=["Divorced"]',
        )
    )
    with pytest.raises(RegMetaError) as exc:
        load_curation_tree(root)
    assert "editions assigned twice" in exc.value.message


@pytest.mark.parametrize(
    "change",
    [None, "scope", "parent-prose", "missing", "duplicate", "unrelated", "new-parent"],
)
def test_occurrence_period_parent_authority_guards_fresh_and_stored_cases(
    tmp_path, change
):
    tree, scope, original, negative, entry = _checked_correction_fixture(
        tmp_path, period=True
    )
    coordinate = original.subject.variant.model_copy(update={"name": entry.variant})
    authority = original.model_copy(
        update={
            "locators": (
                original.locators[0].model_copy(
                    update={"semantic_record_key": ("parent-authority",)}
                ),
            ),
            "subject": original.subject.model_copy(
                update={
                    "variable": SourceCoordinate(status="not_applicable"),
                    "variant": coordinate,
                }
            ),
            "fields": SourceFields(),
            "edition_scope": entry.edition_scope,
            "parent_facts": (
                original.parent_facts[0].model_copy(
                    update={
                        "kind": "variant",
                        "coordinate": coordinate,
                        "register_name": original.subject.register_name,
                        "variant": coordinate,
                        "fields": SourceFields(
                            coverage_from=value_field("2016"),
                            description=value_field("Annual delivery"),
                        ),
                    }
                ),
            ),
        }
    )
    entry = entry.model_copy(
        update={
            "authority": list(
                capture_expectations(
                    (authority,),
                    fields=tuple(SourceFields.model_fields),
                    parents=True,
                    coding=True,
                )
            )
        }
    )
    reg = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            reg.model_copy(
                update={
                    "errata": reg.errata.model_copy(
                        update={"occurrence_period": [entry]}
                    )
                }
            ),
        ),
    )

    def compile_with(parents):
        reader = SimpleNamespace(
            iter_without_native_family=lambda source: iter(parents),
            iter_native_families=lambda *args, **kwargs: iter(
                ((native_variable_key(original), (original, negative)),)
            ),
            lookup=lambda source, key: iter(
                r
                for r in parents
                if record_ref(r).source == source
                and record_ref(r).semantic_record_key == key
            ),
        )
        return compile_occurrence_corrections(
            tree, cast("Any", SimpleNamespace(records=reader)), (scope,), subset=False
        )

    cases, issues, _ = compile_with((authority,))
    assert not issues
    (case,) = cases[scope.source, None]
    assert entry.authority[0] in case.support
    parents = (authority,)
    if change == "scope":
        parents = (
            authority.model_copy(update={"edition_scope": original.edition_scope}),
        )
    elif change == "parent-prose":
        parent = authority.parent_facts[0]
        parents = (
            authority.model_copy(
                update={
                    "parent_facts": (
                        parent.model_copy(
                            update={
                                "fields": parent.fields.model_copy(
                                    update={
                                        "description": value_field("Changed authority")
                                    }
                                )
                            }
                        ),
                    )
                }
            ),
        )
    elif change == "missing":
        parents = ()
    elif change == "duplicate":
        parents = (authority, authority)
    elif change == "unrelated":
        parents = (
            authority.model_copy(
                update={
                    "subject": authority.subject.model_copy(
                        update={
                            "variant": coordinate.model_copy(update={"name": "OTHER"})
                        }
                    )
                }
            ),
        )
    if change == "new-parent":
        parents = (
            authority,
            authority.model_copy(
                update={
                    "locators": (
                        authority.locators[0].model_copy(
                            update={"semantic_record_key": ("new-parent",)}
                        ),
                    )
                }
            ),
        )
    fresh, diagnostics, _ = compile_with(parents)
    stored = evaluate_cases((case,), (original, negative, *parents))
    if change is None:
        assert fresh and not diagnostics and stored[0].status == "applicable"
    else:
        assert not fresh and diagnostics
        # Identical physical duplicates retain the existing semantic dedup policy.
        assert stored[0].status == ("applicable" if change == "duplicate" else "stale")


def test_period_correction_uses_exact_prose_to_distinguish_same_column_originals(
    tmp_path,
):
    tree, scope, original, negative, entry = _checked_correction_fixture(
        tmp_path, period=True
    )
    negative = negative.model_copy(
        update={
            "edition_scope": original.edition_scope,
            "edition_period_scope": original.edition_period_scope,
            "original_period_text": original.original_period_text,
            "fields": original.fields.model_copy(
                update={"name": value_field("Distinct ninth event")}
            ),
        }
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (original, negative))
    assert not issues
    (case,) = cases[scope.source, None]
    assert {target.ref for target in case.targets} == {record_ref(original)}
    assert {support.ref for support in case.support} == {record_ref(negative)}
    result = apply_occurrence_cases((original, negative), (case,))
    assert not result.diagnostics
    assert result.occurrences[0].edition_scope == entry.edition_scope
    assert result.occurrences[1] == source_occurrence(negative)
    drift = original.model_copy(
        update={
            "fields": original.fields.model_copy(
                update={"name": value_field("Changed event")}
            )
        }
    )
    assert _run_checked_correction(tree, scope, (drift, negative))[1]
    assert evaluate_cases((case,), (drift, negative))[0].status == "stale"
    extra = original.model_copy(
        update={
            "locators": (
                original.locators[0].model_copy(
                    update={"semantic_record_key": ("new-original",)}
                ),
            )
        }
    )
    assert _run_checked_correction(tree, scope, (original, negative, extra))[1]
    assert evaluate_cases((case,), (original, negative, extra))[0].status == "stale"


def test_parallel_representation_uses_checked_effective_literal_and_raw_guards(
    tmp_path,
):
    from reg_meta_build.curation_compile import compile_parallel_representations
    from reg_meta_build.source_curation import (
        CheckedFieldChange,
        CurationCase,
        FieldExpectation,
        OccurrenceCorrectionDecision,
        capture_expectations,
        evaluate_cases,
    )

    path, records, naming = _pooled_parallel_fixture(tmp_path)
    (register,) = load_register_files(path.parents[2])
    raw = (
        records[0],
        records[1].model_copy(
            update={
                "fields": records[1].fields.model_copy(
                    update={"column_name": value_field("OldSecond")}
                )
            }
        ),
    )
    owner = naming[-1].target.source_key
    targets = capture_expectations(
        raw, fields=tuple(SourceFields.model_fields), parents=True, coding=True
    )
    correction = CurationCase(
        case_id="reviewed-column",
        targets=targets,
        peer_guards=naming[-1].target.peer_guards,
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=(
                *(
                    CheckedIdentityChange(ref=record_ref(r), variable_key=owner)
                    for r in raw
                ),
                CheckedFieldChange(
                    ref=record_ref(raw[1]),
                    replacement=FieldExpectation(
                        name="column_name", status="value", value="Second"
                    ),
                ),
            ),
            reason="Exact supplied literal correction",
            provenance="fixture",
        ),
    )
    assert compile_parallel_representations(register, raw, naming)[0] == ()
    (case,), issues = compile_parallel_representations(
        register, raw, naming, ownership_cases=(correction,)
    )
    assert issues == ()
    assert evaluate_cases((case,), raw)[0].status == "applicable"
    assert any(
        field.value == "OldSecond"
        for target in case.targets
        for alternative in target.alternatives
        for field in alternative.fields
        if field.name == "column_name"
    )
    changed = (
        raw[0],
        raw[1].model_copy(
            update={
                "fields": raw[1].fields.model_copy(
                    update={"definition": value_field("Changed meaning")}
                )
            }
        ),
    )
    assert evaluate_cases((case,), changed)[0].status == "stale"
    assert (
        compile_parallel_representations(
            register, changed, naming, ownership_cases=(correction,)
        )[0]
        == ()
    )
    empty = register.model_copy(
        update={
            "representation": register.representation.model_copy(
                update={"parallel": []}
            )
        }
    )
    assert compile_parallel_representations(
        empty, changed, naming, ownership_cases=(correction,)
    ) == ((), ())
    before = compile_parallel_representations(register, records, naming)
    assert before == compile_parallel_representations(
        register, records, naming, ownership_cases=()
    )


def test_column_correction_requires_complete_fields_and_preserves_original(tmp_path):
    from reg_meta_build.source_records import SourceFields

    tree, scope, source, _, base = _checked_correction_fixture(tmp_path)
    guards = list(
        capture_expectations((source,), fields=tuple(SourceFields.model_fields))[0]
        .alternatives[0]
        .fields
    )
    entry = ErrataFieldEntry(
        **{
            **base.model_dump(exclude={"field", "value", "expected_fields"}),
            "expected_fields": guards,
        },
        field="column_name",
        value="PHYSICAL",
    )
    register = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={"errata": register.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (source,))
    assert not issues
    applied = apply_occurrence_cases((source,), cases[scope.source, None])
    assert not applied.diagnostics
    assert applied.occurrences[0].fields.column_name.value == "PHYSICAL"
    assert applied.occurrences[0].source_records == (source,)
    changed = source.model_copy(
        update={
            "fields": source.fields.model_copy(
                update={"data_length": value_field("17")}
            )
        }
    )
    assert apply_occurrence_cases((changed,), cases[scope.source, None]).diagnostics
    _, stale, _ = _run_checked_correction(tree, scope, (changed,))
    assert stale
    for updates in (
        {"expected_fields": guards[:-1]},
        {"edition": None},
        {"value": source.fields.column_name.value},
    ):
        with pytest.raises(ValueError):
            ErrataFieldEntry.model_validate({**entry.model_dump(), **updates})


def test_partition_data_warning_is_explicit_and_scoped_to_annotated_members(
    tmp_path: Path,
):
    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.answer"\nslug = "answer"\n'
        '[[identity.partition]]\nvariable = "1.5"\n'
        'columns = { ANSWER = "1.5.answer" }\n'
        'unassigned_columns = ["LEFT"]\ncolumns_ref = "fixture map"\n'
        'data_warning = "Identity follows the exported source question"\n',
    )
    records = _scb_partition_records(("ANSWER", "LEFT"))
    compiled, key, _ = _compile_partition_fixture(root, records)
    (case,) = compiled[0][key]
    assert case.decision.data_warning == "Identity follows the exported source question"
    assert set(case.decision.data_warning_refs) == {
        record_ref(record) for record in records
    }
    assert case.decision.data_warning_fields == ("identity",)


@pytest.mark.parametrize("provider", ["scb", "fk"])
@pytest.mark.parametrize("source_role", ["thin_provider", "scb_records"])
@pytest.mark.parametrize("declared_start", ["2019-01-01", None])
def test_maintained_provider_coverage_uses_input_role_not_provider_name(
    provider,
    source_role,
    declared_start,
):
    parent = _case_record(
        provider=provider,
        register="r",
        parent="register",
        fields=SourceFields(coverage_from=value_field(declared_start))
        if declared_start
        else SourceFields(name=value_field("Unknown coverage register")),
    )
    variable = _case_record(
        provider=provider,
        register="r",
        variable="col",
        fields=SourceFields(
            column_name=value_field("COL"), availability=value_field(True)
        ),
    )
    records = (parent, variable)
    register_key = source_register_key(variable)
    assert register_key is not None
    register = SimpleNamespace(
        register_info=SimpleNamespace(provider=provider, slug="r"),
        source_file=f"curation/registers/{provider}/r.toml",
    )
    scope = CompiledScope(
        source=variable.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register", provider=provider, source_key=register_key
                ),
                naming=SlugEntry(
                    kind="register", provider=provider, source_id="1", slug="r"
                ),
                contributors=(),
            ),
        ),
    )
    prepared = SimpleNamespace(
        value_sources=(),
        manifest=SimpleNamespace(
            inputs=(
                SimpleNamespace(
                    role=source_role, revision=SimpleNamespace(dataset=variable.source)
                ),
            )
        ),
        records=SimpleNamespace(iter_records=lambda *, source: iter(records)),
    )
    if source_role == "thin_provider" and declared_start is None:
        with pytest.raises(ValueError, match="empty or inverted thin coverage window"):
            compile_provider_declarations(
                SimpleNamespace(registers=(register,)), prepared, (scope,), subset=False
            )
        assert parent.parent_facts[0].fields.coverage_from is None
        return
    cases, diagnostics, _ = compile_provider_declarations(
        SimpleNamespace(registers=(register,)),
        prepared,
        (scope,),
        subset=False,
    )
    assert not diagnostics
    if source_role != "thin_provider":
        assert not cases
        return
    result = apply_occurrence_cases(records, cases[scope.source, None])
    addition = next(
        e
        for c in cases[scope.source, None]
        for e in c.decision.effects
        if isinstance(e, CuratedOccurrenceAddition)
    )
    assert addition.edition_period_scope.intervals == (
        ScopeInterval(start=declared_start, end=None),
    )
    assert addition.fields == variable.fields
    assert result.occurrences[1].source_records == (variable,)


@pytest.mark.parametrize(
    "columns, owners, reference",
    [
        ({"OLD": "1.5"}, ("1.5",), "review"),
        ({"OLD": "1.6", "NEW": "1.6"}, ("1.6",), "review"),
        (None, ("1.5",), None),
    ],
)
def test_native_partition_requires_complete_explicit_own_family(
    columns, owners, reference
):
    with pytest.raises(ValueError):
        convert_column_partitions(
            _scb_partition_records(("OLD", "NEW")),
            source_id="1.5",
            split_ids=owners,
            declared_columns=columns,
            declaration_reference=reference,
        )


@pytest.mark.parametrize(
    "columns, unassigned",
    [
        ({"OLD": "1.6"}, []),
        ({"OLD": "1.5", "NEW": "1.5.new"}, []),
        ({"OLD": "1.5"}, ["NEW"]),
    ],
)
def test_authored_native_partition_rejects_foreign_or_partial_owners(
    columns, unassigned
):
    from reg_meta_build.curation_tree import IdentityPartitionEntry

    with pytest.raises(ValueError):
        IdentityPartitionEntry(
            variable="1.5",
            columns=columns,
            unassigned_columns=unassigned,
            columns_ref="reviewed quantity",
        )


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "field",
        "parent",
        "coding",
        "missing_duplicate",
        "partial_digest",
        "added_duplicate",
    ],
)
def test_authored_native_partition_pins_complete_originals_on_fresh_compile(
    tmp_path: Path, drift: str | None
):
    from reg_meta_build.curation_tree import IdentityPartitionEntry
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256
    from reg_meta_build.source_records import CodeSetReference

    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5"\nslug = "quantity"\n'
        '[[identity.partition]]\nvariable = "1.5"\n'
        'columns = { OLD = "1.5", NEW = "1.5" }\n'
        'columns_ref = "complete source-native quantity review"\n',
    )
    original = _scb_partition_records(("OLD", "NEW"))
    duplicate = original[0].model_copy(
        update={
            "record_id": original[0].record_id + "-duplicate",
            "locators": (
                original[0].locators[0].model_copy(update={"physical_record": "99"}),
            ),
        }
    )
    original = (*original, duplicate)
    tree = load_curation_tree(root)
    register = next(r for r in tree.registers if r.identity.partition)
    declaration = register.identity.partition[0]
    guarded = IdentityPartitionEntry.model_validate_json(
        json.dumps(
            {
                **declaration.model_dump(mode="json", exclude_none=True),
                "expected_evidence_sha256": acknowledgement_evidence_sha256(
                    original[:1] if drift == "partial_digest" else original
                ),
            }
        )
    )
    register = register.model_copy(
        update={
            "identity": register.identity.model_copy(update={"partition": [guarded]})
        }
    )
    tree = replace(tree, registers=(register,))
    records = original
    if drift == "missing_duplicate":
        records = original[:-1]
    elif drift == "added_duplicate":
        records = (
            *original,
            duplicate.model_copy(update={"record_id": "new-duplicate"}),
        )
    elif drift in {"field", "parent", "coding"}:
        first = original[0]
        if drift == "field":
            change = {
                "fields": first.fields.model_copy(
                    update={"definition": value_field("different source quantity")}
                )
            }
        elif drift == "parent":
            parent = first.parent_facts[0]
            change = {
                "parent_facts": (
                    parent.model_copy(
                        update={
                            "fields": parent.fields.model_copy(
                                update={"name": value_field("different source parent")}
                            )
                        }
                    ),
                    *first.parent_facts[1:],
                )
            }
        else:
            change = {
                "code_set_references": (
                    CodeSetReference(
                        reference_id="different-source-list",
                        content_sha256="0" * 64,
                        physical_locator="different source list cell",
                    ),
                )
            }
        records = (first.model_copy(update=change), *original[1:])
    native = native_variable_key(records[0])
    reader = SimpleNamespace(
        iter_partition_families=lambda *a, **k: iter(((native, records),))
    )
    scope = _partition_scope(records)
    compiled = compile_partitions(
        tree, cast("Any", SimpleNamespace(records=reader)), (scope,)
    )
    key = (scope.source, None)
    cases, naming, keys, _, _, _, issues = compiled
    if drift is not None:
        assert {d.code for d in issues} == {"stale_curation_entry"}
        assert not cases.get(key) and not naming.get(key)
        assert keys[key] == ((native, None),)
    else:
        assert not issues
        assert (
            cases[key][0].expected_evidence_sha256 == guarded.expected_evidence_sha256
        )
        result = apply_occurrence_cases(records, cases[key])
        assert not result.diagnostics and len(result.occurrences) == len(original)
        assert all(
            o.identity_checked and o.variable_key == native for o in result.occurrences
        )
        stale = apply_occurrence_cases(original[:-1], cases[key])
        assert stale.diagnostics and not any(
            o.identity_checked for o in stale.occurrences
        )


@pytest.mark.parametrize("digest", ["not-a-sha", "A" * 64, "a" * 63])
def test_authored_partition_requires_a_complete_digest(digest: str):
    from reg_meta_build.curation_tree import IdentityPartitionEntry

    with pytest.raises(ValueError):
        IdentityPartitionEntry(
            variable="1.5",
            columns={"OLD": "1.5", "NEW": "1.5"},
            columns_ref="reviewed complete family",
            expected_evidence_sha256=digest,
        )


def test_delivery_metadata_rejects_redundant_unit_permission():
    from reg_meta_build.curation_tree import DeliveryMetadataEntry
    from reg_meta_build.source_curation import DeliveryMetadataDecision

    with pytest.raises(ValueError):
        DeliveryMetadataEntry(
            fields=["measurement_unit"],
            variable="1.5",
            records=[],
            columns=[],
            evidence="source",
            noted="2026-10-01",
        )
    with pytest.raises(ValueError, match="name or description"):
        DeliveryMetadataDecision.require_fields(("measurement_unit",))


def test_field_correction_accepts_guarded_source_alternatives_without_losing_originals(
    tmp_path,
):
    tree, scope, source, _, base = _checked_correction_fixture(tmp_path)
    source = source.model_copy(
        update={
            "fields": source.fields.model_copy(
                update={"representation": value_field("Source reference")}
            )
        }
    )
    peer = source.model_copy(
        update={
            "record_id": source.record_id + ":peer",
            "fields": source.fields.model_copy(
                update={
                    "representation": value_field(
                        "Source reference https://example.org"
                    )
                }
            ),
        }
    )
    fields = (
        "name",
        "definition",
        "description",
        "operational_definition",
        "classification_declared",
        "representation",
        "data_type",
        "coverage_from",
        "coverage_to",
    )
    entry = ErrataFieldEntry(
        **base.model_dump(
            exclude={"field", "value", "expected_fields", "expected_records"}
        ),
        field="representation",
        value=peer.fields.representation.value,
        expected_fields=list(
            capture_expectations((source,), fields=fields)[0].alternatives[0].fields
        ),
        expected_records=list(
            capture_expectations(
                (source, peer),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
    )
    register = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={"errata": register.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (source, peer))
    assert not issues
    applied = apply_occurrence_cases((source, peer), cases[scope.source, None])
    assert not applied.diagnostics
    assert all(
        o.fields.representation.value == entry.value for o in applied.occurrences
    )
    assert {r for o in applied.occurrences for r in o.source_records} == {source, peer}
    _, issues, _ = _run_checked_correction(tree, scope, (source,))
    assert issues
    for changed_field in ("representation", "description", "identifier"):
        changed = peer.model_copy(
            update={
                "fields": peer.fields.model_copy(
                    update={
                        changed_field: value_field(False)
                        if changed_field == "identifier"
                        else value_field("CHANGED")
                    }
                )
            }
        )
        _, issues, _ = _run_checked_correction(tree, scope, (source, changed))
        assert issues
        assert apply_occurrence_cases(
            (source, changed), cases[scope.source, None]
        ).diagnostics
    changed = peer.model_copy(
        update={"edition_scope": TemporalScope(kind="year_independent")}
    )
    _, issues, _ = _run_checked_correction(tree, scope, (source, changed))
    assert issues
    for updates in (
        {"expected_records": entry.expected_records[:-1]},
        {"value": "Unsupplied replacement"},
        {"expected_fields": entry.expected_fields[:-1]},
    ):
        with pytest.raises(ValueError):
            ErrataFieldEntry.model_validate({**entry.model_dump(), **updates})


@pytest.mark.parametrize("native_base", [False, True])
@pytest.mark.parametrize("shared_ref", [False, True])
def test_parallel_representation_selects_checked_owner_without_literal_changes(
    tmp_path,
    native_base,
    shared_ref,
):
    from reg_meta_build.curation_compile import compile_parallel_representations
    from reg_meta_build.source_curation import (
        CurationCase,
        OccurrenceCorrectionDecision,
        capture_expectations,
        evaluate_cases,
    )

    path, selected, naming = _pooled_parallel_fixture(tmp_path)
    header = REGISTERINFORMATION_HEADER.split("|")
    values = _var_row(
        colname="Sibling",
        cvid=102,
        var_id=1,
        varname="Income",
        year="2022",
        versionname="2022",
        regver_id=112,
    ).split("|")
    sibling = clean_scb_row(
        header,
        3,
        {
            name: (True, value, value)
            for name, value in zip(header, values, strict=True)
        },
        _revision("fixture"),
    ).record
    if shared_ref:
        sibling = sibling.model_copy(update={"locators": selected[0].locators})
    records = (*selected, sibling)
    owner = (
        native_variable_key(selected[0])
        if native_base
        else naming[-1].target.source_key
    )
    assert owner is not None
    targets = capture_expectations(
        records, fields=tuple(SourceFields.model_fields), parents=True, coding=True
    )
    guards = tuple(
        guard.model_copy(
            update={
                "expected_members": tuple(dict.fromkeys(record_ref(r) for r in records))
            }
        )
        for guard in naming[-1].target.peer_guards
    )
    naming = (
        *naming[:-1],
        naming[-1].model_copy(
            update={
                "target": naming[-1].target.model_copy(
                    update={
                        "source_key": owner,
                        "expectations": targets,
                        "peer_guards": guards,
                    }
                ),
            }
        ),
    )
    correction = CurationCase(
        case_id="reviewed-complete-partition",
        targets=targets,
        peer_guards=guards,
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(
                CheckedIdentityChange(
                    ref=record_ref(r),
                    variable_key=owner if r in selected else (*owner, "sibling"),
                    when=capture_expectations((r,), fields=("column_name",))[0]
                    .alternatives[0]
                    .fields,
                )
                for r in records
            ),
            reason="Complete literal role partition",
            provenance="fixture",
        ),
    )
    (register,) = load_register_files(path.parents[2])
    if not shared_ref:
        assert compile_parallel_representations(register, records, naming)[0] == ()
    (case,), issues = compile_parallel_representations(
        register, records, naming, ownership_cases=(correction,)
    )
    assert issues == ()
    if shared_ref:
        assert (
            len(
                next(
                    target
                    for target in case.targets
                    if target.ref == record_ref(sibling)
                ).alternatives
            )
            == 2
        )
    assert {target.ref for target in case.targets} == {record_ref(r) for r in selected}
    assert record_ref(sibling) in {target.ref for target in case.support}
    assert evaluate_cases((case,), records)[0].status == "applicable"
    assert evaluate_cases((case,), selected)[0].status == "stale"
    assert (
        compile_parallel_representations(
            register, selected, naming, ownership_cases=(correction,)
        )[0]
        == ()
    )
    changed = (
        *selected,
        sibling.model_copy(
            update={
                "fields": sibling.fields.model_copy(
                    update={"definition": value_field("Changed role")}
                )
            }
        ),
    )
    assert evaluate_cases((case,), changed)[0].status == "stale"
    assert (
        compile_parallel_representations(
            register, changed, naming, ownership_cases=(correction,)
        )[0]
        == ()
    )


def test_source_attribution_correction_requires_complete_originals_and_preserves_raw(
    tmp_path,
):
    tree, scope, source, _, base = _checked_correction_fixture(tmp_path)
    source = source.model_copy(
        update={
            "fields": source.fields.model_copy(
                update={"source_attribution": value_field("Register : Variant (FE)")}
            )
        }
    )
    peer = source.model_copy(
        update={
            "record_id": source.record_id + ":peer",
            "subject": source.subject.model_copy(
                update={
                    "native": source.subject.native.model_copy(
                        update={"member_id": 999}
                    )
                }
            ),
        }
    )
    expectations = list(
        capture_expectations(
            (source, peer),
            fields=tuple(SourceFields.model_fields),
            parents=True,
            coding=True,
        )
    )
    entry = ErrataFieldEntry(
        **base.model_dump(
            exclude={"field", "value", "expected_fields", "expected_records"}
        ),
        field="source_attribution",
        value="Register : Variant (företagsenhet)",
        expected_fields=list(expectations[0].alternatives[0].fields),
        expected_records=expectations,
    )
    register = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={"errata": register.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    cases, issues, _ = _run_checked_correction(tree, scope, (source, peer))
    assert not issues
    applied = apply_occurrence_cases((source, peer), cases[scope.source, None])
    assert not applied.diagnostics
    assert all(
        o.fields.source_attribution.value == entry.value for o in applied.occurrences
    )
    assert {r for o in applied.occurrences for r in o.source_records} == {source, peer}
    for records in (
        (source,),
        (
            source,
            peer.model_copy(
                update={
                    "fields": peer.fields.model_copy(
                        update={"source_attribution": value_field("Different source")}
                    )
                }
            ),
        ),
    ):
        _, issues, _ = _run_checked_correction(tree, scope, records)
        assert issues
        assert apply_occurrence_cases(records, cases[scope.source, None]).diagnostics
    for updates in (
        {"expected_records": None},
        {"edition": None},
        {"expected_fields": entry.expected_fields[:-1]},
    ):
        with pytest.raises(ValueError):
            ErrataFieldEntry.model_validate({**entry.model_dump(), **updates})


@pytest.mark.parametrize("drift", [None, "description", "coding", "missing_duplicate"])
def test_guarded_sos_split_checks_complete_physical_family(tmp_path: Path, drift):
    from reg_meta_build.curation_tree import IdentitySplitEntry
    from reg_meta_build.source_curation import (
        acknowledgement_evidence_sha256,
        capture_expectations,
    )
    from reg_meta_build.source_records import CodeSetReference

    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers/sos/par.toml"
    path.parent.mkdir(parents=True)
    path.write_text(
        '[register]\nprovider = "sos"\nslug = "par"\n'
        'native_id = "5891427617861710725"\nname = "Patientregistret"\n'
        '[[identity.split]]\nvariable = "ATC"\nby = "deldatamangd"\n'
        'parts = [{ deldatamangd = "PAR_OV", owner = "5891427617861710725.ATC.outpatient" },'
        '{ deldatamangd = "PAR_SV", owner = "5891427617861710725.ATC.inpatient" }]\n'
        '[[variable]]\nnative_id = "5891427617861710725.ATC.outpatient"\nslug = "outpatient"\n'
        '[[variable]]\nnative_id = "5891427617861710725.ATC.inpatient"\nslug = "inpatient"\n'
    )
    original = _sos_partition_records(subsets=("PAR_OV", "PAR_SV"))
    duplicate = original[0].model_copy(
        update={
            "record_id": original[0].record_id + "-duplicate",
            "locators": (
                original[0].locators[0].model_copy(update={"physical_record": "99"}),
            ),
        }
    )
    original = (*original, duplicate)
    tree = load_curation_tree(root)
    register = next(r for r in tree.registers if r.register_info.provider == "sos")
    declaration = register.identity.split[0]
    guarded = IdentitySplitEntry.model_validate_json(
        json.dumps(
            {
                **declaration.model_dump(mode="json", exclude_none=True),
                "expected_records": [
                    e.model_dump(mode="json")
                    for e in capture_expectations(
                        original,
                        fields=tuple(SourceFields.model_fields),
                        parents=True,
                        coding=True,
                    )
                ],
                "expected_evidence_sha256": acknowledgement_evidence_sha256(original),
                "data_warning": "Separate source-defined clinical contexts; no equivalence inferred.",
            }
        )
    )
    register = register.model_copy(
        update={"identity": register.identity.model_copy(update={"split": [guarded]})}
    )
    tree = replace(tree, registers=(register,))
    records = original
    if drift == "missing_duplicate":
        records = original[:-1]
    elif drift is not None:
        changed = original[0].model_copy(
            update={
                "fields": original[0].fields.model_copy(
                    update={"description": value_field("different role")}
                )
            }
            if drift == "description"
            else {
                "code_set_references": (
                    CodeSetReference(
                        reference_id="changed",
                        content_sha256="0" * 64,
                        physical_locator="changed",
                    ),
                )
            }
        )
        records = (changed, *original[1:])
    native = native_variable_key(records[0])
    reader = SimpleNamespace(
        iter_partition_families=lambda *a, **kw: iter(((native, records),))
    )
    scope = _partition_scope(records)
    cases, names, _, _, _, _, diagnostics = compile_partitions(
        tree, cast("Any", SimpleNamespace(records=reader)), (scope,)
    )
    key = (scope.source, None)
    if drift is not None:
        assert [d.code for d in diagnostics] == ["stale_curation_entry"]
        assert not cases.get(key) and not names.get(key)
    else:
        assert not diagnostics
        assert cases[key][0].decision.data_warning == guarded.data_warning
        corrected = apply_occurrence_cases(records, cases[key])
        assert not corrected.diagnostics
        assert len(corrected.occurrences) == 3
        assert evaluate_cases(cases[key], original[:-1])[0].status == "stale"
        assert {o.variable_key[-1] for o in corrected.occurrences} == {
            "PAR_OV",
            "PAR_SV",
        }


def test_name_correction_uses_only_complete_same_family_witnesses(tmp_path):
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256

    tree, scope, target, witness, base = _checked_correction_fixture(tmp_path)
    witness = witness.model_copy(
        update={
            "fields": witness.fields.model_copy(
                update={"name": value_field("Canonical source label")}
            )
        }
    )
    duplicate = witness.model_copy(
        update={
            "record_id": witness.record_id + "-duplicate",
            "locators": (
                witness.locators[0].model_copy(update={"physical_record": "99"}),
            ),
        }
    )
    originals = (target, witness, duplicate)
    entry = ErrataFieldEntry(
        **base.model_dump(
            exclude={
                "field",
                "value",
                "expected_records",
                "authority_records",
                "expected_evidence_sha256",
            }
        ),
        field="name",
        value="Canonical source label",
        expected_records=list(
            capture_expectations(
                (target,),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        authority_records=list(
            capture_expectations(
                (witness, duplicate),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        expected_evidence_sha256=acknowledgement_evidence_sha256(originals),
    )
    register = tree.registers[0]
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={"errata": register.errata.model_copy(update={"field": [entry]})}
            ),
        ),
    )
    cases, issues, _ = _run_checked_correction(tree, scope, originals)
    assert not issues
    applied = apply_occurrence_cases(originals, cases[scope.source, None])
    assert not applied.diagnostics
    assert all(o.fields.name.value == entry.value for o in applied.occurrences)
    assert {r for o in applied.occurrences for r in o.source_records} == set(originals)
    for changed in (
        (target,),
        (target, witness),
        (
            target,
            witness.model_copy(
                update={
                    "fields": witness.fields.model_copy(
                        update={"description": value_field("Changed witness role")}
                    )
                }
            ),
            duplicate,
        ),
    ):
        assert _run_checked_correction(tree, scope, changed)[1]
        assert evaluate_cases(cases[scope.source, None], changed)[0].status == "stale"
    foreign = witness.model_copy(
        update={
            "subject": witness.subject.model_copy(
                update={
                    "variable": witness.subject.variable.model_copy(
                        update={"native_id": 999}
                    )
                }
            )
        }
    )
    with pytest.raises(ValueError, match="same native family"):
        ErrataFieldEntry.model_validate_json(
            json.dumps(
                {
                    **entry.model_dump(mode="json", exclude_none=True),
                    "authority_records": [
                        e.model_dump(mode="json")
                        for e in capture_expectations(
                            (foreign,),
                            fields=tuple(SourceFields.model_fields),
                            parents=True,
                            coding=True,
                        )
                    ],
                }
            )
        )


def _column_owner_alternatives_fixture(tmp_path, *, digest_only=False):
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256
    from reg_meta_build.source_records import CodeSetReference, SourceFields

    root = tmp_path / "curation"
    _scb_partition_tree(
        root,
        '\n[[variable]]\nnative_id = "1.5.amount"\nslug = "amount"\n',
    )
    rows = tuple(
        _errata_record(column="ANSWER", year="2020", member=member).model_copy(
            update={
                "fields": _errata_record(
                    column="ANSWER", year="2020", member=member
                ).fields.model_copy(
                    update={"operational_definition": value_field(text)}
                ),
                "code_set_references": (
                    CodeSetReference(
                        reference_id="source-list",
                        content_sha256="a" * 64,
                        physical_locator="source-list.csv",
                    ),
                ),
            }
        )
        for member, text in ((20, "Derived amount"), (21, "Sum of components"))
    )
    owner = IdentityColumnOwnerEntry(
        variable="1.5",
        variant="1.2",
        column="ANSWER",
        owner="1.5.amount",
        ref="Same physical amount, retain both source operations",
        source_editions=["2020"],
        expected_records=None
        if digest_only
        else list(
            capture_expectations(
                rows, fields=tuple(SourceFields.model_fields), parents=True, coding=True
            )
        ),
        expected_evidence_sha256=acknowledgement_evidence_sha256(rows),
    )
    tree = load_curation_tree(root)
    register = next(r for r in tree.registers if r.register_info.native_id == "1")
    tree = replace(
        tree,
        registers=(
            register.model_copy(
                update={
                    "identity": register.identity.model_copy(
                        update={"column_owner": [owner]}
                    )
                }
            ),
        ),
    )
    scope = _partition_scope(rows)

    def compile_rows(records, *, projected=False, deferred=False):
        partition_records = (
            tuple(
                PreparedPartitionRecord(
                    source=r.source,
                    subject=r.subject,
                    parent_facts=r.parent_facts,
                    edition_scope=r.edition_scope,
                    edition_period_scope=r.edition_period_scope,
                    locators=r.locators,
                    fields=r.fields.model_copy(update={"operational_definition": None}),
                )
                for r in records
            )
            if projected
            else records
        )
        reader = SimpleNamespace(
            iter_partition_families=lambda *args, **kwargs: iter(
                ((native_variable_key(rows[0]), partition_records),)
            ),
            iter_native_families=lambda *args, **kwargs: iter(
                ((native_variable_key(rows[0]), records),)
            ),
        )
        compiler = compile_deferred_partitions if deferred else compile_partitions
        return compiler(tree, SimpleNamespace(records=reader), (scope,))

    compiled = compile_rows(rows)
    assert not compiled[-1]
    return rows, owner, compiled[0][scope.source, None], compile_rows


@pytest.mark.parametrize(
    "change", ["operation", "remove", "add", "duplicate", "parent", "scope", "coding"]
)
@pytest.mark.parametrize("digest_only", [False, True])
def test_column_owner_alternatives_refuse_original_drift(tmp_path, change, digest_only):
    rows, _, cases, compile_rows = _column_owner_alternatives_fixture(
        tmp_path, digest_only=digest_only
    )
    first = rows[0]
    if change == "operation":
        changed = (
            first.model_copy(
                update={
                    "fields": first.fields.model_copy(
                        update={
                            "operational_definition": value_field("Changed formula")
                        }
                    )
                }
            ),
            rows[1],
        )
    elif change == "remove":
        changed = rows[1:]
    elif change == "add":
        changed = (*rows, _errata_record(column="ANSWER", year="2020", member=22))
    elif change == "duplicate":
        changed = (
            *rows,
            first.model_copy(update={"record_id": first.record_id + "-copy"}),
        )
    elif change == "parent":
        parent = first.parent_facts[0]
        changed = (
            first.model_copy(
                update={
                    "parent_facts": (
                        parent.model_copy(
                            update={
                                "fields": parent.fields.model_copy(
                                    update={"name": value_field("Changed parent name")}
                                )
                            }
                        ),
                        *first.parent_facts[1:],
                    )
                }
            ),
            rows[1],
        )
    elif change == "scope":
        changed = (
            first.model_copy(
                update={
                    "edition_scope": TemporalScope(
                        kind="intervals",
                        intervals=(ScopeInterval(start="2019", end="2019"),),
                    )
                }
            ),
            rows[1],
        )
    else:
        changed = (
            first.model_copy(
                update={
                    "code_set_references": (
                        first.code_set_references[0].model_copy(
                            update={"content_sha256": "b" * 64}
                        ),
                    )
                }
            ),
            rows[1],
        )
    refreshed = compile_rows(changed)
    assert any(d.code == "stale_curation_entry" for d in refreshed[-1])
    assert compile_rows(changed, projected=True) == refreshed
    assert not any(compile_rows(changed, projected=True, deferred=True)[0].values())
    assert apply_occurrence_cases(changed, cases).diagnostics


@pytest.mark.parametrize(
    "update",
    [
        {"source_editions": []},
        {"expected_evidence_sha256": "invalid"},
        {
            "expected_fields": [
                {"name": "column_name", "status": "value", "value": "ANSWER"}
            ]
        },
    ],
)
def test_digest_column_owner_requires_finite_exclusive_guards(tmp_path, update):
    from pydantic import ValidationError
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry

    _, owner, _, _ = _column_owner_alternatives_fixture(tmp_path, digest_only=True)
    with pytest.raises(ValidationError):
        IdentityColumnOwnerEntry.model_validate(owner.model_dump(mode="json") | update)


def test_column_owner_alternatives_require_complete_exclusive_guards(tmp_path):
    from pydantic import ValidationError
    from reg_meta_build.curation_tree import IdentityColumnOwnerEntry

    _, owner, _, _ = _column_owner_alternatives_fixture(tmp_path)
    supplied = owner.model_dump(mode="json")
    for update in (
        {"expected_evidence_sha256": None},
        {"source_editions": []},
        {
            "expected_fields": [
                {"name": "column_name", "status": "value", "value": "ANSWER"}
            ]
        },
        {
            "expected_records": [
                owner.expected_records[0].model_dump(mode="json")
                | {"alternatives": [{"fields": []}]}
            ]
        },
    ):
        with pytest.raises(ValidationError):
            IdentityColumnOwnerEntry.model_validate(supplied | update)


def test_partial_partition_finding_excludes_exact_scoped_owned_originals():
    records = _scb_partition_records(("ID", "ID", "ID"), variants=(2, 3, 2))
    supported = "1.5.supported"
    converted = convert_column_partitions(
        records,
        source_id="1.5",
        split_ids=(supported,),
        declared_columns={"ID": None},
        declaration_reference="Retain the contradictory delivery only as unresolved.",
        scoped_owners={(record_ref(records[0]), "ID"): supported},
    )
    assert len(converted.diagnostics) == 1
    assert converted.diagnostics[0].code == "unassigned_original_columns"
    assert converted.diagnostics[0].refs == tuple(
        sorted((record_ref(records[1]), record_ref(records[2])), key=str)
    )
    assert converted.case is not None
    applied = apply_occurrence_cases(records, (converted.case,))
    native = native_variable_key(records[0])
    assert applied.occurrences[0].variable_key != native
    assert [o.variable_key for o in applied.occurrences[1:]] == [native, native]
    assert tuple(o.source_records[0] for o in applied.occurrences) == records
    assert evaluate_cases((converted.case,), records[:-1])[0].status == "stale"


@pytest.mark.parametrize("stored_role", ["label", "free_text"])
def test_guarded_uncoded_text_role_preserves_attached_book_and_refuses_drift(
    tmp_path, stored_role
):
    from reg_meta_build.curation_tree import CodingUncodedEntry
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import acknowledgement_evidence_sha256

    original = _errata_record(column="ANSWER", year="2020", member=20)
    originals = (original,)
    tree, _, scope = _errata_fixture(tmp_path, originals, "")
    scope = _scope_with_coding_names(scope, original)
    occurrence = source_occurrence(original)
    column = occurrence.column_key
    assert column is not None
    claims = (
        CodeListClaim(
            "attached-book",
            original.edition_scope,
            (
                CodeMembershipClaim(
                    "1", "Description", TemporalScope(kind="year_independent")
                ),
            ),
            version_label="Reference classification",
        ),
    )
    register = next(r for r in tree.registers if r.register_info.slug == "sample")
    entry = CodingUncodedEntry(
        variable="1.5",
        variant="people",
        column="ANSWER",
        periods=[["2020-01-01", "2020-12-31"]],
        stored_role=stored_role,
        expected_evidence_sha256=acknowledgement_evidence_sha256(
            originals, tuple(coding_source_sha256(q) for q in claims)
        ),
        reason="The reviewed source documents a stored text component.",
        source="Exact original source and reference-book review",
        data_warning="The attached numeric book is reference evidence, not the text response domain.",
    )
    register = register.model_copy(
        update={"coding": register.coding.model_copy(update={"uncoded": [entry]})}
    )

    def compile_rows(rows, source_claims):
        return compile_coding_register(
            register,
            scope,
            originals=rows,
            columns={column: rows},
            column_scopes={column: frozenset((original.edition_scope,))},
            coding={column: source_claims},
        )

    cases, issues = compile_rows(originals, claims)
    assert not issues and len(cases) == 1
    applied = apply_coding_choices(originals, cases, coding={column: claims})
    assert not applied.diagnostics
    assert applied.coding[column].claims == claims
    assert all(segment.code_set is None for segment in applied.coding[column].segments)
    for rows, source_claims in (
        (
            (
                original.model_copy(
                    update={
                        "fields": original.fields.model_copy(
                            update={
                                "description": value_field("Changed source meaning")
                            }
                        )
                    }
                ),
            ),
            claims,
        ),
        ((), claims),
        (
            originals,
            (
                replace(
                    claims[0],
                    members=(
                        CodeMembershipClaim(
                            "1", "Changed label", TemporalScope(kind="year_independent")
                        ),
                    ),
                ),
            ),
        ),
    ):
        assert compile_rows(rows, source_claims)[0] == ()
        assert apply_coding_choices(
            rows, cases, coding={column: source_claims}
        ).diagnostics
    with pytest.raises(ValueError, match="complete source coding guards"):
        CodingUncodedEntry.model_validate(
            {**entry.model_dump(), "expected_evidence_sha256": None}
        )
    ordinary = CodingUncodedEntry.model_validate(
        {**entry.model_dump(), "stored_role": None, "expected_evidence_sha256": None}
    )
    register = register.model_copy(
        update={"coding": register.coding.model_copy(update={"uncoded": [ordinary]})}
    )
    assert compile_rows(originals, claims)[0] == ()


@pytest.mark.parametrize("guarded", [True, False])
@pytest.mark.parametrize(
    "code",
    ["unresolved_source_event_endpoint", "unresolved_lineage_ambiguous_source_variant"],
)
def test_global_event_acknowledgement_is_routed_out_of_local_scope(
    tmp_path, guarded, code
):
    root = tmp_path / "curation"
    _tree(root)
    source_ref = json.dumps(
        {"source": "scb-timeseries", "semantic_record_key": ["event", "exact"]}
    )
    fragment = (
        f'\n[[acknowledge]]\ncode = "{code}"\n'
        'subject = "exact"\nreason = "Source endpoint is absent"\n'
        'evidence = "Complete accepted evidence reviewed"\n'
        f"refs = [{json.dumps(source_ref)}]\n"
    )
    if guarded:
        fragment += (
            f'expected_evidence_sha256 = "{"0" * 64}"\n'
            f'expected_diagnostic_sha256 = "{"1" * 64}"\n'
        )
    with (root / "registers/scb/sample.toml").open("a") as handle:
        handle.write(fragment)
    tree = load_curation_tree(root)
    if not guarded:
        with pytest.raises(ValueError, match="full evidence and diagnostic guards"):
            compile_curation(tree, _prepared(), (_scope(),), subset=True)
        return
    compiled = compile_curation(tree, _prepared(), (_scope(),), subset=True)
    assert (
        len(
            compiled.event_acknowledgements
            if code == "unresolved_source_event_endpoint"
            else compiled.lineage_acknowledgements
        )
        == 1
    )
    assert not any(
        case.decision.kind == "acknowledge"
        for cases in compiled.cases.values()
        for case in cases
    )
