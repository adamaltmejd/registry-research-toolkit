"""Documented coding lists keep literal provenance and original claims and refuse changed evidence."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _source_coding_choices_support import (
    apply_choices as _apply,
    choice_record as _record,
    compile_entry as _compile_entry,
    documented_values as _documented_values,
    list_claim as _claim,
)
from pydantic import ValidationError
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import (
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_curation import (
    CurationCase,
    SourceEvidence,
    capture_expectations,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceFields,
    TemporalScope,
    value_field,
)


def test_documented_coding_keeps_literal_blank_provenance_and_original_claims():
    record = _record()
    claims = (_claim("later", "2", "2021-01-01", "2021-12-31"),)
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented", _documented_values(), claims
    )
    assert not diagnostics and len(cases) == 1
    result = _apply(record, claims, *cases)
    assert result.accounting[0].status == "applied"
    assert result.coding[column].claims == claims
    permuted = cases[0].model_copy(
        update={
            "decision": cases[0].decision.model_copy(
                update={
                    "selection": cases[0].decision.selection.model_copy(
                        update={
                            "members": tuple(
                                reversed(cases[0].decision.selection.members)
                            )
                        }
                    )
                }
            )
        }
    )
    assert _apply(record, claims, permuted).coding == result.coding
    first, later = result.coding[column].segments
    assert (first.valid_from, first.valid_to) == ("2020-01-01", "2020-12-31")
    assert first.code_set is not None
    assert set(first.code_set.members) == {("1", "ja"), ("", "inte tillfrågad")}
    assert first.version_label == "Official 2020 questionnaire"
    assert "https://example.org/official.pdf" in first.provenance[0]
    assert "SHA256: " + "a" * 64 in first.provenance[0]
    assert "Pages: 12" in first.provenance[0]
    assert later == resolve_code_membership(claims).segments[0]
    assert CurationCase.model_validate_json(cases[0].model_dump_json()) == cases[0]
    occurrence = source_occurrence(record)
    assert occurrence.variant_key is not None
    formed = form_native_variable(
        (record,),
        register=ResolvedRegister(provider="scb", slug="test", name="Test"),
        variants={
            occurrence.variant_key: ResolvedVariant(slug="people", name="People")
        },
        slug="value",
        provider_key="5",
        coding=result.coding,
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
    )
    assert formed.variable is not None
    assert not any(d.code == "missing_coding_period" for d in formed.diagnostics)
    assert formed.variable.states[0].value_set == first.code_set
    assert formed.variable.states[0].provenance is not None
    assert "https://example.org/official.pdf" in formed.variable.states[0].provenance


@pytest.mark.parametrize(
    "members",
    [[["1", "yes"], ["1", "no"]], [["", "not asked"], ["", "not asked"]], [["1", ""]]],
)
def test_documented_coding_rejects_duplicate_codes_and_missing_labels(members):
    with pytest.raises(ValidationError):
        _compile_entry("documented", _documented_values(members), ())


@pytest.mark.parametrize(
    "claims",
    [(_claim("existing", "1"),), (_claim("partial", "1", "2020-06-01", "2020-07-01"),)],
)
def test_documented_coding_rejects_complete_supplied_membership_even_partial_period(
    claims,
):
    cases, diagnostics, *_ = _compile_entry("documented", _documented_values(), claims)
    assert not cases and diagnostics[0].code == "stale_curation_entry"
    assert "complete source list" in diagnostics[0].detail


@pytest.mark.parametrize("drift", ["field", "scope", "missing", "new_peer", "coding"])
def test_documented_coding_rejects_changed_original_evidence(drift):
    record = _record()
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented", _documented_values(), ()
    )
    assert not diagnostics
    records = (record,)
    claims = ()
    if drift == "field":
        records = (
            record.model_copy(
                update={
                    "fields": record.fields.model_copy(
                        update={"definition": value_field("Changed")}
                    )
                }
            ),
        )
    elif drift == "scope":
        records = (
            record.model_copy(
                update={
                    "edition_period_scope": TemporalScope(
                        kind="intervals",
                        intervals=(
                            ScopeInterval(start="2020-06-01", end="2020-12-31"),
                        ),
                    )
                }
            ),
        )
    elif drift == "missing":
        records = ()
    elif drift == "new_peer":
        records = (record, _record(2021))
    else:
        claims = (_claim("new export", "2"),)
    result = apply_coding_choices(records, cases, coding={column: claims})
    assert result.accounting[0].status == "stale"
    assert result.diagnostics
    assert result.coding[column] == resolve_code_membership(claims)


def test_conflicting_documented_lists_withhold_only_overlap():
    first, _, _, _, _, column = _compile_entry("documented", _documented_values(), ())
    second = first[0].model_copy(
        update={
            "case_id": "second",
            "decision": first[0].decision.model_copy(
                update={
                    "valid_from": "2020-06-01",
                    "selection": first[0].decision.selection.model_copy(
                        update={"members": (("2", "nej"),)}
                    ),
                }
            ),
        }
    )
    result = _apply(_record(), (), first[0], second)
    assert {a.status for a in result.accounting} == {"conflicted"}
    assert result.coding[column].issues[0].code == "conflicting_coding_choices"
    assert (
        result.coding[column].issues[0].valid_from,
        result.coding[column].issues[0].valid_to,
    ) == ("2020-06-01", "2020-12-31")
    assert result.coding[column].segments[0].code_set is not None
    assert result.coding[column].segments[1].code_set is None


def test_documented_coding_requires_an_existing_exact_column_window():
    values = {**_documented_values(), "periods": [["2019-01-01", "2019-12-31"]]}
    cases, diagnostics, *_ = _compile_entry("documented", values, ())
    assert not cases and diagnostics[0].code == "stale_curation_entry"
    assert "no column occurrence" in diagnostics[0].detail


@pytest.mark.parametrize(
    "scopes",
    [
        (),
        (TemporalScope(kind="unknown", label="unsupplied"),),
        (TemporalScope(kind="pooled", label="historical window"),),
        (
            TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start="2019-01-01", end="2019-12-31"),),
            ),
        ),
        (
            TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start="2020-02-01", end="2020-12-31"),),
            ),
        ),
    ],
    ids=["removed", "unknown", "unbounded-pooled", "disjoint", "incomplete"],
)
def test_documented_coding_uses_current_effective_delivery_without_rewriting_originals(
    scopes,
):
    original = _record(year=2024)
    _, _, register, scope, columns, column = _compile_entry(
        "documented", _documented_values(), (), record=original
    )
    historical = TemporalScope(
        kind="pooled", label="2020", pooled_start="2020-01-01", pooled_end="2020-12-31"
    )
    occurrence = replace(source_occurrence(original), edition_period_scope=historical)
    evidence = SourceEvidence((original,), effective_occurrences=(occurrence,))
    assert evidence.effective_scopes is not None
    cases, diagnostics = compile_coding_register(
        register,
        scope,
        originals=(original,),
        columns=columns,
        column_scopes=evidence.effective_scopes,
        coding={column: ()},
    )
    assert not diagnostics and len(cases) == 1
    result = apply_coding_choices(evidence, cases, coding={column: ()})
    assert result.accounting[0].status == "applied"
    assert result.coding[column].segments[0].code_set is not None
    assert original.edition_period_scope.intervals[0].start == "2024-01-01"
    assert (
        cases[0].targets[0].alternatives[0].edition_period_scope
        == original.edition_period_scope
    )

    invalid = SourceEvidence(
        (original,),
        effective_occurrences=tuple(
            replace(occurrence, edition_period_scope=value) for value in scopes
        ),
    )
    assert invalid.effective_scopes is not None
    absent, diagnostics = compile_coding_register(
        register,
        scope,
        originals=(original,),
        columns=columns,
        column_scopes=invalid.effective_scopes,
        coding={column: ()},
    )
    assert not absent and diagnostics[0].code == "stale_curation_entry"
    stale = apply_coding_choices(invalid, cases, coding={column: ()})
    assert stale.accounting[0].status == "stale"
    assert "coding_delivery_changed" in {d.code for d in stale.diagnostics}
    assert not stale.coding[column].segments


@pytest.mark.parametrize("drift", ["new-peer", "anchor-removed", "anchor-changed"])
def test_documented_historical_delivery_checks_cross_variable_anchor_membership(drift):
    original = _record(year=2024)
    anchor = _record(column="ANCHOR", variable=6)
    _, _, register, scope, _, column = _compile_entry(
        "documented", _documented_values(), (), record=original
    )
    historical = replace(
        source_occurrence(original),
        source_records=(original, anchor),
        edition_period_scope=anchor.edition_period_scope,
    )
    evidence = SourceEvidence((original, anchor), effective_occurrences=(historical,))
    assert evidence.effective_scopes is not None
    cases, diagnostics = compile_coding_register(
        register,
        scope,
        originals=(original, anchor),
        columns={column: (original, anchor)},
        column_scopes=evidence.effective_scopes,
        coding={column: ()},
    )
    assert not diagnostics and len(cases) == 1
    assert set(cases[0].peer_guards[0].expected_members) == {
        record_ref(original),
        record_ref(anchor),
    }
    applied = apply_coding_choices(evidence, cases, coding={column: ()})
    assert applied.accounting[0].status == "applied"

    if drift == "anchor-removed":
        records = (original,)
        historical = replace(historical, source_records=records)
    elif drift == "anchor-changed":
        changed = anchor.model_copy(
            update={
                "fields": anchor.fields.model_copy(
                    update={"definition": value_field("Changed")}
                )
            }
        )
        records = (original, changed)
        historical = replace(historical, source_records=records)
    else:
        added = _record(year=2023, column="OTHER", variable=7)
        records = (original, anchor, added)
        historical = replace(historical, source_records=records)
    stale = apply_coding_choices(
        SourceEvidence(records, effective_occurrences=(historical,)),
        cases,
        coding={column: ()},
    )
    assert stale.accounting[0].status == "stale"
    assert not stale.coding[column].segments


def test_support_occurrence_cannot_supply_catalog_coding_coverage():
    occurrence = replace(source_occurrence(_record()), use="support")
    evidence = SourceEvidence(
        occurrence.source_records, effective_occurrences=(occurrence,)
    )
    assert evidence.effective_scopes == {}


def test_documented_coding_requires_the_exact_partition_owner():
    cases, diagnostics, *_ = _compile_entry(
        "documented", _documented_values(), (), split=True
    )
    assert not cases and diagnostics[0].code == "stale_curation_entry"
    cases, diagnostics, *_ = _compile_entry(
        "documented", {**_documented_values(), "variable": "1.5.part"}, (), split=True
    )
    assert len(cases) == 1 and not diagnostics


@pytest.mark.parametrize(
    "invalid",
    [
        {"document_sha256": "unknown"},
        {"document_url": "local.pdf"},
        {"document_pages": [0]},
    ],
)
def test_documented_coding_requires_exact_document_attribution(invalid):
    with pytest.raises(ValidationError):
        _compile_entry("documented", {**_documented_values(), **invalid}, ())


def test_existing_extension_selector_still_rejects_blank_codes():
    with pytest.raises(ValidationError):
        _compile_entry(
            "extend",
            {
                "list": "list",
                "list_members": [["", "not asked"]],
                "witness": ["2020-01-01", "2020-12-31"],
            },
            (),
        )


def test_documented_application_requires_full_original_source_guards():
    cases, diagnostics, *_ = _compile_entry("documented", _documented_values(), ())
    assert not diagnostics
    weak = cases[0].model_copy(
        update={"targets": capture_expectations((_record(),), fields=("column_name",))}
    )
    with pytest.raises(ValueError, match="checked"):
        _apply(_record(), (), weak)


def test_documented_coding_rejects_an_open_ended_window():
    with pytest.raises(ValidationError):
        _compile_entry(
            "documented",
            {**_documented_values(), "periods": [["2020-01-01", "9999-12-31"]]},
            (),
        )


def _association_support_fixture():
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_values import SourceValueAssociation

    scope = TemporalScope(kind="year_independent")
    associations = tuple(
        SourceValueAssociation(i, "book", str(i), "book.xlsx", "codes")
        for i in (21, 22, 23)
    )
    claim = replace(
        _claim("complete", "16310"),
        members=(
            CodeMembershipClaim("16310", "Pachygyria", scope, (associations[0],)),
            CodeMembershipClaim("16320", "Microgyria", scope, (associations[1],)),
            CodeMembershipClaim(
                "16310", "Erroneous microgyria", scope, (associations[2],)
            ),
        ),
    )
    values = {
        "code": "16310",
        "label": "Erroneous microgyria",
        "association": associations[2].locator,
        "expected_association": coding_source_sha256(associations[2]),
        "authority_code": "16320",
        "authority_label": "Microgyria",
        "authority_association": associations[1].locator,
        "expected_authority_association": coding_source_sha256(associations[1]),
        "expected_source_codings": [coding_source_sha256(claim)],
    }
    return claim, values


def test_checked_association_support_preserves_raw_claim_and_distinct_constructs():
    claim, values = _association_support_fixture()
    record = _record()
    cases, diagnostics, _, _, _, column = _compile_entry(
        "support", values, (claim,), record=record
    )
    assert not diagnostics
    result = apply_coding_choices((record,), cases, coding={column: (claim,)})
    assert result.accounting[0].status == "applied"
    assert result.coding[column].claims == (claim,)
    code_set = result.coding[column].segments[0].code_set
    assert code_set is not None
    assert code_set.members == (
        ("16310", "Pachygyria"),
        ("16320", "Microgyria"),
    )
    assert result.coding[column].segments[0].provenance
    assert [d.code for d in result.diagnostics] == [
        "supported_erroneous_coding_association"
    ]


@pytest.mark.parametrize("change", ["missing", "new", "label", "association", "scope"])
def test_checked_association_support_rejects_changed_complete_evidence(change):
    claim, values = _association_support_fixture()
    record = _record()
    cases, _, _, _, _, column = _compile_entry(
        "support", values, (claim,), record=record
    )
    if change == "missing":
        changed = replace(claim, members=claim.members[:1] + claim.members[2:])
    elif change == "new":
        changed = replace(
            claim,
            members=claim.members
            + (
                CodeMembershipClaim("9", "New", TemporalScope(kind="year_independent")),
            ),
        )
    elif change == "label":
        changed = replace(
            claim,
            members=(replace(claim.members[0], label="Changed"), *claim.members[1:]),
        )
    elif change == "association":
        m = claim.members[0]
        changed = replace(
            claim,
            members=(
                replace(m, associations=(replace(m.associations[0], row_number=99),)),
                *claim.members[1:],
            ),
        )
    else:
        changed = replace(
            claim,
            scope=TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start="2020-02-01", end="2020-12-31"),),
            ),
        )
    _, diagnostics, *_ = _compile_entry("support", values, (changed,), record=record)
    assert diagnostics[0].code == "stale_curation_entry"
    result = apply_coding_choices((record,), cases, coding={column: (changed,)})
    assert result.accounting[0].status == "stale"
    assert result.coding[column].claims == (changed,)
