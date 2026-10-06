"""SCB preliminary/final supersession, named edition splits and edition periods compile from source coordinates."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from _curation_compile_support import (
    errata_fixture as _errata_fixture,
    errata_record as _errata_record,
    make_naming_reader as _naming_reader,
    make_revision as _revision,
    make_tree as _tree,
    partition_scope as _partition_scope,
    scb_partition_records as _scb_partition_records,
)
from reg_meta_build.catalog_resolution import resolve_parents
from reg_meta_build.curation_compile import (
    compile_edition_splits,
    compile_errata,
    compile_native_naming,
    compile_partitions,
    compile_scb_preliminary,
)
from reg_meta_build.curation_tree import (
    load_curation_tree,
)
from reg_meta_build.source_coding import (
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    OccurrenceCorrectionDecision,
    SourceEvidence,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_intervals import resolve_occurrence_intervals
from reg_meta_build.source_naming import (
    check_naming_target,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    CodeSetReference,
    SourceFields,
    SourceRecord,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from pathlib import Path


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
    row = var_row(
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


_EDITION_PERIOD = (
    '\n[[errata.edition_period]]\nvariant = "people"\nname = "Födelseland"\n'
    'valid_from = "2018-02-01"\nvalid_to = "2018-11-30"\n'
    'evidence = "Source documentation"\nnoted = "2026-09-26"\n'
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
                    var_row(
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
