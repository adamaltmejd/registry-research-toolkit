"""Compile tracked curation families from source coordinates."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal, cast

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta.errors import RegMetaError
from reg_meta_build.catalog_resolution import resolve_parents
from reg_meta_build.curation_compile import (
    CompiledCuration,
    _compile_sos_register,
    _compile_thin_register,
    compile_coding_register,
    compile_curation,
    compile_edition_splits,
    compile_enrichment,
    compile_errata,
    compile_flags,
    compile_native_naming,
    compile_partitions,
    compile_scb_preliminary,
    convert_column_partitions,
    finalize_classification_bindings,
    tree_sha256,
)
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.id import mint
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.scb_errata import ErrataVersion, edition_bindings
from reg_meta_build.source_annotations import apply_alias_cases
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    copied_coding_fingerprints,
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
    CheckedVariantAssignment,
    CuratedOccurrenceAddition,
    OccurrenceCorrectionDecision,
    SearchAliasDecision,
    SourceEvidence,
)
from reg_meta_build.source_effects import (
    _require_checked,
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_intervals import resolve_occurrence_intervals
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
    authored_naming_id,
    check_naming_target,
)
from reg_meta_build.source_occurrences import source_occurrence
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


def test_lova_routes_choose_distinct_labeled_rows_and_stale() -> None:
    import tomllib
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "curation/registers/sos/lova.toml"
    curated = tomllib.loads(source.read_text(encoding="utf-8"))
    routes = curated["identity"]["route"]
    selected = {entry["deldatamangd"]: tuple(entry["variants"]) for entry in routes}
    row2 = "LOVA / Legitimerade omsorgs- och vårdyrkesgruppers ekonomi och arbetsmarknadssituation"
    row15 = "LOVA / Legitimerade omsorgs- och vårdyrkesgruppers arbetsmarknadsstatus"
    assert selected["A_LOVA"] == (row15,)
    assert selected["A_LOVA_LISA"] == (row2,)
    variant_ids = {entry["native_id"] for entry in curated["variant"]}
    assert {
        authored_naming_id(
            "register_variant", provider="sos", register_key="lova", member_key=name
        )
        for name in (row2, row15)
    } <= variant_ids

    parents = tuple(
        _case_record(
            provider="sos",
            register="LOVA",
            variant=name,
            parent="variant",
            fields=SourceFields(name=value_field(name.removeprefix("LOVA / "))),
        )
        for name in (row2, row15)
    )
    variables = tuple(
        _case_record(provider="sos", register="LOVA", variant=token, variable=token)
        for token in ("A_LOVA", "A_LOVA_LISA")
    )
    register = _route_register(
        *((token, selected[token]) for token in ("A_LOVA", "A_LOVA_LISA"))
    )
    cases, diagnostics, _ = _compile_sos_register(register, (*parents, *variables))
    assert diagnostics == ()
    assignments = {
        effect.ref.semantic_record_key[1]: effect.variant_keys[0][-1]
        for case in cases
        if isinstance(case.decision, OccurrenceCorrectionDecision)
        for effect in case.decision.effects
        if isinstance(effect, CheckedVariantAssignment)
    }
    assert assignments == {"variant:A_LOVA": row15, "variant:A_LOVA_LISA": row2}

    renamed = _case_record(
        provider="sos",
        register="LOVA",
        variant=f"{row2} renamed",
        parent="variant",
        fields=SourceFields(name=value_field("Renamed")),
    )
    for remaining in (parents[1:], (parents[0],), (renamed, parents[1])):
        _, stale, _ = _compile_sos_register(register, (*remaining, *variables))
        assert [issue.code for issue in stale] == ["stale_curation_entry"]


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


def test_flags_compile_exact_native_variable_and_report_missing_or_foreign(
    tmp_path: Path,
) -> None:
    item = _errata_record(column="A", year="2020")
    entries = (
        '\n[[flags]]\nvariable = "1.5"\nis_sensitive = true\n'
        'is_identifier = false\nevidence = "SCB documentation"\nnoted = "2026-09-28"\n'
        '[[flags]]\nvariable = "1.9"\nis_sensitive = true\n'
        'evidence = "Missing"\nnoted = "2026-09-28"\n'
        '[[flags]]\nvariable = "2.5"\nis_identifier = true\n'
        'evidence = "Foreign"\nnoted = "2026-09-28"\n'
    )
    tree, prepared, scope = _errata_fixture(tmp_path, (item,), entries)
    cases, issues, report = compile_flags(tree, prepared, (scope,), subset=False)
    (case,) = cases[scope.source, scope.register_key]
    assert case.decision.variable_key == native_variable_key(item)
    assert case.decision.provenance.endswith("#/flags/1.5")
    assert [issue.code for issue in issues] == [
        "stale_curation_entry",
        "stale_curation_entry",
    ]
    assert "another register" in issues[1].detail
    assert len(report["scb/sample"]["entries_matched"]) == 1


def test_flags_compile_is_independent_of_entry_order(tmp_path: Path) -> None:
    records = (
        _errata_record(column="A", year="2020"),
        _errata_record(column="B", year="2020", variable=6, member=21),
    )
    first = (
        '[[flags]]\nvariable = "1.5"\nis_sensitive = true\n'
        'evidence = "A"\nnoted = "2026-09-28"\n'
    )
    second = (
        '[[flags]]\nvariable = "1.6"\nis_identifier = false\n'
        'evidence = "B"\nnoted = "2026-09-28"\n'
    )
    tree, prepared, scope = _errata_fixture(tmp_path, records, first + second)
    original = compile_flags(tree, prepared, (scope,), subset=False)
    path = tmp_path / "curation/registers/scb/sample.toml"
    path.write_text(path.read_text().replace(first + second, second + first))
    reordered = compile_flags(
        load_curation_tree(tmp_path / "curation"),
        prepared,
        (scope,),
        subset=False,
    )
    assert original == reordered


def test_named_edition_split_rebinds_parents_and_is_order_independent(
    tmp_path: Path,
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
            iter_native_families=lambda source: iter(((native_variable, rows),)),
        )
        return cast("Any", SimpleNamespace(records=reader))

    first, issues, statuses = compile_edition_splits(
        tree, prepared(records), (scope,), subset=False
    )
    second, _, _ = compile_edition_splits(
        tree, prepared(records[::-1]), (scope,), subset=False
    )
    assert not issues and statuses["scb/sample"]["entries_matched"]
    assert first == second
    (case,) = first[scope.source, scope.register_key]
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
        iter_native_families=lambda source: iter(((variable, records),)),
        iter_records=lambda **kwargs: iter(records),
        iter_register_slices=lambda source, wanted: iter(((register, records),)),
    )
    prepared = cast("Any", SimpleNamespace(records=reader))
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
    scope = scope.model_copy(
        update={
            "naming": (
                *scope.naming,
                NamingDeclaration(
                    target=NativeNamingTarget(
                        kind="register_variant",
                        provider="scb",
                        source_key=occurrence.variant_key,
                        register_key=source_register_key(donor),
                    ),
                    naming=SlugEntry("register_variant", "1.2", "people", "scb"),
                    contributors=(),
                ),
                NamingDeclaration(
                    target=NativeNamingTarget(
                        kind="variable",
                        provider="scb",
                        source_key=occurrence.variable_key,
                        register_key=source_register_key(donor),
                    ),
                    naming=SlugEntry("variable", "1.5", "value", "scb"),
                    contributors=(),
                ),
            )
        }
    )
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
        coding={column: claims},
    )
    assert not diagnostics and len(choices) == 1
    applied = apply_coding_choices(originals, choices, coding={column: claims})
    assert applied.accounting[0].status == "applied"
    assert not applied.diagnostics
    assert "conflicting_code_memberships" not in {
        issue.code for issue in applied.coding[column].issues
    }

    added_peer = _errata_record(column="VALUE", year="2014", member=23)
    stale = apply_coding_choices(
        (*originals, added_peer), choices, coding={column: claims}
    )
    assert stale.accounting[0].status == "stale"
    assert "peer_membership_changed" in {issue.code for issue in stale.diagnostics}

    removed = apply_coding_choices((later, donor), choices, coding={column: claims})
    assert removed.accounting[0].status == "stale"
    assert "peer_membership_changed" in {issue.code for issue in removed.diagnostics}


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


def test_compiled_delivered_addition_uses_unique_literal_split(tmp_path: Path):
    donor = _errata_record(column="A", year="2020")
    sibling = _errata_record(column="B", year="2020")
    other = _errata_record(column="B", year="2021", variable=6, member=21)
    tree, prepared, scope = _errata_fixture(
        tmp_path, (donor, sibling, other), _DELIVERED
    )
    native = native_variable_key(donor)
    assert native is not None
    converted = convert_column_partitions(
        (donor, sibling),
        source_id="1.5",
        split_ids=("1.5.a", "1.5.b"),
        declared_columns={"A": "1.5.a", "B": "1.5.b"},
        declaration_reference="fixture",
    )
    assert converted.case is not None
    scope_key = (scope.source, scope.register_key)
    cases, _, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {scope_key: (converted.case,)}, subset=False
    )
    assert diagnostics == ()
    assert cases[scope_key][0].decision.effects[0].variable_key == (
        *native,
        "accepted-partition",
        "1.5.a",
    )


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


def test_compiled_enrichment_description_alias_and_staleness(tmp_path: Path):
    record = _errata_record(column="A", year="2020")
    second = _errata_record(column="A", year="2021", member=21, variant=3)
    tree, prepared, scope, naming = _enrichment_fixture(
        tmp_path, (record, second), _DESCRIPTION + _ALIAS
    )
    cases, diagnostics, _ = compile_enrichment(
        tree, prepared, (scope,), naming, {}, {}, subset=False
    )
    assert diagnostics == ()
    description, alias = cases[(scope.source, scope.register_key)]
    assert isinstance(description.decision.effects[0], CheckedFieldChange)
    assert description.decision.effects[0].replacement.value == "Accepted prose"
    assert isinstance(alias.decision, SearchAliasDecision)
    assert (
        apply_occurrence_cases((record, second), (description,))
        .accounting[0]
        .disposition
        == "applied"
    )
    for target in alias.targets:
        _require_checked(target, ("column_name",), case_id=alias.case_id)
        assert any(target.ref in guard.expected_members for guard in alias.peer_guards)
    apply_alias_cases(
        (record, second),
        (alias,),
        variables={alias.decision.variable_key: None},
        variants=dict.fromkeys(alias.decision.variant_keys),
    )
    assert set(alias.decision.variant_keys) == {
        source_occurrence(record).variant_key,
        source_occurrence(second).variant_key,
    }

    described = record.model_copy(
        update={
            "fields": record.fields.model_copy(
                update={"description": value_field("Already")}
            )
        }
    )
    tree, prepared, scope, naming = _enrichment_fixture(
        tmp_path / "described", (described,), _DESCRIPTION
    )
    cases, diagnostics, _ = compile_enrichment(
        tree, prepared, (scope,), naming, {}, {}, subset=False
    )
    assert cases == {}
    assert [item.code for item in diagnostics] == ["stale_curation_entry"]
    assert (
        diagnostics[0].subject
        == "curation/registers/scb/sample.toml#/enrichment.description/1"
    )


def test_compiled_enrichment_split_uses_partition_records(tmp_path: Path):
    first, second = _scb_partition_records(("A", "B"))
    converted = convert_column_partitions(
        (first, second),
        source_id="1.5",
        split_ids=("1.5.a", "1.5.b"),
        declared_columns={"A": "1.5.a", "B": "1.5.b"},
        declaration_reference="fixture",
    )
    assert converted.case is not None
    split = next(
        item.target for item in converted.bindings if item.source_id == "1.5.b"
    )
    tree, prepared, scope, naming = _enrichment_fixture(
        tmp_path, (first, second), _DESCRIPTION + _ALIAS, target=split
    )
    scope_key = (scope.source, scope.register_key)
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
    assert cases[(scope.source, scope.register_key)][0].targets[0].ref == record_ref(
        second
    )
    alias = cases[(scope.source, scope.register_key)][1]
    assert alias.targets[0].ref == record_ref(second)
    _require_checked(alias.targets[0], ("column_name",), case_id=alias.case_id)
    assert alias.targets[0].ref in alias.peer_guards[0].expected_members


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


def test_compiled_alias_for_declared_column_has_checked_anchor(tmp_path: Path):
    record = _errata_record(column="A", year="2020")
    fragment = (
        '\n[[errata.column]]\nvariant = "people"\ncolumn = "NewCol"\n'
        'name = "New column"\ndefinition = "Documented"\n'
        'source = "steward-holdings"\nevidence = "held"\n'
        'noted = "2026-09-25"\nall_versions = true\n'
        + _ALIAS.replace('variable = "a"', 'variable = "new-col"')
    )
    tree, prepared, scope = _errata_fixture(tmp_path, (record,), fragment)
    errata_cases, naming, _, diagnostics, _ = compile_errata(
        tree, prepared, (scope,), {}, subset=False
    )
    assert diagnostics == ()
    cases, diagnostics, _ = compile_enrichment(
        tree, prepared, (scope,), naming, {}, errata_cases, subset=False
    )
    assert diagnostics == ()
    alias = cases[(scope.source, scope.register_key)][0]
    assert isinstance(alias.decision, SearchAliasDecision)
    assert alias.decision.variant_keys == (
        errata_cases[(scope.source, scope.register_key)][0]
        .decision.effects[0]
        .variant_key,
    )
    assert alias.targets[0].ref == record_ref(record)
    _require_checked(alias.targets[0], ("column_name",), case_id=alias.case_id)
    assert alias.targets[0].ref in alias.peer_guards[0].expected_members


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
        cases, naming, keys, _, bases, issues = compiled
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
    cases, _, _, _, bases, issues = compiled
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


def test_classification_binding_matches_partition_produced_name(tmp_path: Path) -> None:
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
        def iter_native_families(self, source):
            return iter(((native, records),))

        def iter_records(self, *, source):
            return iter(records)

        def iter_register_slices(self, source, registers):
            return iter(((register, records),))

    prepared = _prepared()
    prepared.records = Records()
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
