"""Checked occurrence, text, shared-record and period corrections compile with their source guards."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _curation_compile_support import (
    checked_correction_fixture as _checked_correction_fixture,
    compile_partition_fixture as _compile_partition_fixture,
    errata_fixture as _errata_fixture,
    errata_record as _errata_record,
    make_tree as _tree,
    partition_scope as _partition_scope,
    run_checked_correction as _run_checked_correction,
    scb_partition_tree as _scb_partition_tree,
    scope_with_coding_names as _scope_with_coding_names,
    sos_partition_records as _sos_partition_records,
)
from reg_meta_build.curation_compile import (
    compile_coding_register,
)
from reg_meta_build.curation_tree import (
    ErrataFieldEntry,
    ErrataOccurrencePeriodEntry,
    load_curation_tree,
)
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_curation import (
    SourceEvidence,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceFields,
    TemporalScope,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path


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
