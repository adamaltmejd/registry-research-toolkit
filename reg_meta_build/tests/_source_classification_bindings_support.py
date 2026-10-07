"""Shared synthetic codebook setup, binding application and declarations for the classification-binding tests."""

from dataclasses import replace

from _csv_fixtures import scb_record
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationCode,
    ResolvedRegister,
    ResolvedVariant,
)
from reg_meta_build.source_classification_bindings import (
    apply_classification_cases,
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
    capture_expectations,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceFields,
    TemporalScope,
    value_field,
)


def sole_classification(state):
    assert len(state.classification_links) <= 1
    return (
        state.classification_links[0].classification
        if state.classification_links
        else None
    )


def sole_conformance(state):
    assert len(state.classification_links) <= 1
    return (
        state.classification_links[0].conformance
        if state.classification_links
        else None
    )


def binding_setup(*, code="01", inline=True, sentinels=()):
    record = scb_record(
        colname="Column",
        var_id=1,
        cvid=100,
        varname="Variable",
        year="2020",
        data_type="int",
    )
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
        sentinel_codes=sentinels,
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
                source=record.source,
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
            expected_classification="0" * 64,
            binding_scope="declared",
            reason="Existing accepted classification declaration",
            provenance="accepted fixture",
        ),
    )
    return record, case, coding, {"fixture": classification}


def apply_bindings(setup, cases=None, *, coding=None, classifications=None):
    record, case, original, books = setup
    return apply_classification_cases(
        (record,),
        (case,) if cases is None else cases,
        coding=original if coding is None else coding,
        classifications=books if classifications is None else classifications,
    )


def form_bindings(setup, result):
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


def binding_declaration(setup, field=None):
    occurrence = source_occurrence(setup[0])
    return replace(
        occurrence,
        fields=occurrence.fields.model_copy(
            update={"classification_declared": field or value_field("FIX")}
        ),
    )


def declared_bindings(setup, occurrences, **kwargs):
    return apply_classification_cases(
        (setup[0],),
        kwargs.pop("cases", ()),
        coding=kwargs.pop("coding", setup[2]),
        classifications=kwargs.pop("classifications", setup[3]),
        references=kwargs.pop("references", {"FIX": "fixture"}),
        occurrences=occurrences,
        **kwargs,
    )
