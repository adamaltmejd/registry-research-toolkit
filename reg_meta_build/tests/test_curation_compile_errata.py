"""SCB errata (delivered additions, blank targets, columns, blockers) and their coding peers compile from source coordinates."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _curation_compile_support import (
    ALIAS as _ALIAS,
    DESCRIPTION as _DESCRIPTION,
    enrichment_fixture as _enrichment_fixture,
    errata_fixture as _errata_fixture,
    errata_record as _errata_record,
    scope_with_coding_names as _scope_with_coding_names,
)
from reg_meta_build.curation_compile import (
    compile_coding_register,
    compile_enrichment,
    compile_errata,
    convert_column_partitions,
)
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
)
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CuratedOccurrenceAddition,
    SourceEvidence,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    CodeSetReference,
    SourceFields,
    TemporalScope,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path


_DELIVERED = (
    '\n[[errata.delivered]]\nvariant = "people"\ncolumn = "A"\n'
    'versions = ["2021"]\nevidence = "accepted delivery"\nnoted = "2026-09-25"\n'
)


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
