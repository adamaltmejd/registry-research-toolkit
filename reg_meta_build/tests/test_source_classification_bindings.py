"""Checked codebook bindings preserve original evidence and bounded ambiguity."""

from dataclasses import replace

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta_build.convert_errata import capture_expectations
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationCode,
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.source_classification_bindings import (
    apply_classification_cases,
    classification_content_sha256,
)
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coding_choices import coding_expectations
from reg_meta_build.source_curation import (
    ClassificationDecision,
    CurationCase,
    PeerGuard,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceFields,
    SourceRevision,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row


def _setup(*, code="01", inline=True):
    revision = SourceRevision.create(
        dataset="fixture",
        publisher="SCB",
        purpose="test",
        upstream_revision="1",
        artifact_path="rows.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    values = _var_row(
        colname="Column",
        var_id=1,
        cvid=100,
        varname="Variable",
        year="2020",
        data_type="int",
    ).split("|")
    record = clean_scb_row(
        header,
        1,
        {k: (True, v, v) for k, v in zip(header, values, strict=True)},
        revision,
    ).record
    occurrence = source_occurrence(record)
    assert occurrence.column_key is not None
    classification = ResolvedClassification(
        slug="fixture",
        short_name="FIX",
        name="Fixture",
        codes=(
            ResolvedClassificationCode(code="01", label="Canonical label"),
            ResolvedClassificationCode(code="02", label="Second label"),
        ),
    )
    claims = (
        (
            CodeListClaim(
                "list",
                TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="2020", end="2020"),),
                ),
                (
                    CodeMembershipClaim(
                        code, "Source label", TemporalScope(kind="year_independent")
                    ),
                ),
            ),
        )
        if inline
        else ()
    )
    coding = {occurrence.column_key: resolve_code_membership(claims)}
    expected = capture_expectations((record,), fields=("column_name",), coding=True)
    case = CurationCase(
        case_id="binding",
        targets=expected,
        peer_guards=(
            PeerGuard(
                guard_id="membership",
                source="fixture",
                native=NativeCoordinates(register_id=1, variable_id=1),
                edition_scopes=(record.edition_scope,),
                expected_members=tuple(e.ref for e in expected),
            ),
        ),
        decision=ClassificationDecision(
            reviewed=True,
            column_key=occurrence.column_key,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            expected_codings=coding_expectations(claims, "2020-01-01", "2020-12-31"),
            classification="fixture",
            expected_classification=classification_content_sha256(classification),
            binding_scope="declared",
            reason="Existing accepted classification declaration",
            provenance="accepted fixture",
        ),
    )
    return record, case, coding, {"fixture": classification}


def _apply(setup, cases=None, *, coding=None, classifications=None):
    record, case, original, books = setup
    return apply_classification_cases(
        (record,),
        (case,) if cases is None else cases,
        coding=original if coding is None else coding,
        classifications=books if classifications is None else classifications,
    )


def _form(setup, result):
    occurrence = source_occurrence(setup[0])
    assert occurrence.variant_key is not None
    formed = form_native_variable(
        (setup[0],),
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        variants={
            occurrence.variant_key: ResolvedVariant(slug="people", name="People")
        },
        slug="variable",
        provider_key="1",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding=result.coding,
    )
    assert formed.variable is not None
    return formed.variable


def test_checked_classification_forms_and_writes_without_copying_canonical_labels(
    tmp_path,
):
    setup = _setup()
    result = _apply(setup)
    assert result.diagnostics == ()
    variable = _form(setup, result)
    state = variable.states[0]
    assert state.classification == "fixture"
    assert state.value_set is not None and state.value_set.members == (
        ("01", "Source label"),
    )
    assert state.conformance is not None and state.conformance.status == "kept"
    assert state.provenance is not None and "binding:" in state.provenance
    write_resolved_catalog(
        (variable,),
        tmp_path / "reg_meta.db",
        manifest={},
        classifications=tuple(setup[3].values()),
    )
    with pytest.raises(ValueError, match="one application"):
        _apply(setup, coding=result.coding)


def test_original_coding_or_canonical_book_change_invalidates_binding():
    setup = _setup()
    changed = _setup(code="02")[2]
    assert (
        _apply(setup, coding=changed).diagnostics[0].code
        == "classification_evidence_changed"
    )
    book = setup[3]["fixture"]
    reordered = book.model_copy(update={"codes": tuple(reversed(book.codes))})
    assert classification_content_sha256(reordered) == classification_content_sha256(
        book
    )
    assert _apply(setup, classifications={"fixture": reordered}).diagnostics == ()
    changed_book = book.model_copy(update={"name": "Changed definition"})
    result = _apply(setup, classifications={"fixture": changed_book})
    assert result.coding == setup[2]
    assert result.diagnostics[0].code == "classification_evidence_changed"


def test_conflicting_bindings_withhold_only_overlap_and_preserve_inline_coding():
    setup = _setup()
    case = setup[1]
    assert isinstance(case.decision, ClassificationDecision)
    alternate = setup[3]["fixture"].model_copy(update={"slug": "alternate"})
    competing = case.model_copy(
        update={
            "case_id": "competing",
            "decision": case.decision.model_copy(
                update={
                    "classification": "alternate",
                    "expected_classification": classification_content_sha256(alternate),
                    "valid_from": "2020-07-01",
                    "expected_codings": coding_expectations(
                        setup[2][case.decision.column_key].claims,
                        "2020-07-01",
                        "2020-12-31",
                    ),
                }
            ),
        }
    )
    books = {**setup[3], "alternate": alternate}
    result = _apply(setup, (case, competing), classifications=books)
    states = _form(setup, result).states
    assert [(s.valid_from, s.valid_to, s.classification) for s in states] == [
        ("2020-01-01", "2020-06-30", "fixture"),
        ("2020-07-01", "2020-12-31", None),
    ]
    assert states[0].value_set == states[1].value_set
    assert result.diagnostics[0].code == "conflicting_classification_decisions"
    assert _apply(setup, (competing, case), classifications=books) == result


def test_declared_reference_can_exist_without_inline_codes_but_inline_override_cannot():
    setup = _setup(inline=False)
    declared = _form(setup, _apply(setup)).states[0]
    assert declared.classification == "fixture" and declared.value_set is None
    assert declared.conformance is None
    case = setup[1]
    inline_case = case.model_copy(
        update={
            "decision": case.decision.model_copy(
                update={"binding_scope": "inline_coding"}
            )
        }
    )
    inline = _apply(setup, (inline_case,))
    assert inline.coding == setup[2] and inline.diagnostics == ()


def test_noncanonical_codes_keep_source_members_and_declared_evidence(tmp_path):
    setup = _setup(code="99")
    result = _apply(setup)
    variable = _form(setup, result)
    state = variable.states[0]
    assert state.classification is None and state.value_set is not None
    assert state.value_set.members == (("99", "Source label"),)
    assert state.conformance is not None
    assert state.conformance.declared_classification == "fixture"
    assert state.conformance.status == "severed"
    assert result.diagnostics[0].code == "nonconforming_classification_codes"
    write_resolved_catalog(
        (variable,),
        tmp_path / "reg_meta.db",
        manifest={},
        diagnostic=True,
        classifications=tuple(setup[3].values()),
    )


def test_accepted_omission_survives_classification_application():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    omitted = replace(
        original,
        segments=tuple(replace(s, state_disposition="omit") for s in original.segments),
    )
    result = _apply(setup, coding={key: omitted})
    assert result.coding == {key: omitted} and result.diagnostics == ()


def test_missing_conversion_is_fatal_and_original_membership_change_is_stale():
    setup = _setup()
    with pytest.raises(ValueError, match="unconverted canonical"):
        _apply(setup, classifications={})
    with pytest.raises(ValueError, match="unconverted column"):
        _apply(setup, coding={})
    result = apply_classification_cases(
        (), (setup[1],), coding=setup[2], classifications=setup[3]
    )
    assert result.coding == setup[2]
    assert result.evaluations[0].status == "stale"
    assert result.diagnostics
