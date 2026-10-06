"""Checked codebook bindings preserve original evidence and bounded ambiguity."""

import sqlite3
from dataclasses import replace
from functools import cache
from pathlib import Path

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row as _var_row
from catalog_manifest import synthetic_manifest
from reg_meta.source_evidence import SourceField, SourceRevision
from reg_meta_build.classifications import load_valid_codes
from reg_meta_build.curation_compile import SentinelCode
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.db import SCHEMA_VERSION
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationCode,
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
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
from reg_meta_build.sources.scb_records import clean_scb_row


def _sole_classification(state):
    assert len(state.classification_links) <= 1
    return (
        state.classification_links[0].classification
        if state.classification_links
        else None
    )


def _sole_conformance(state):
    assert len(state.classification_links) <= 1
    return (
        state.classification_links[0].conformance
        if state.classification_links
        else None
    )


def _setup(*, code="01", inline=True, sentinels=()):
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
            expected_classification="0" * 64,
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
    assert _sole_classification(state) == "fixture"
    assert state.value_set is not None and state.value_set.members == (
        ("01", "Source label"),
    )
    assert (
        _sole_conformance(state) is not None
        and _sole_conformance(state).status == "conforming"
    )
    assert state.provenance is not None and "binding:" in state.provenance
    write_resolved_catalog(
        (variable,),
        tmp_path / "reg_meta.db",
        manifest=synthetic_manifest(),
        classifications=tuple(setup[3].values()),
    )
    with pytest.raises(ValueError, match="one application"):
        _apply(setup, coding=result.coding)


def test_codebook_change_is_checked_against_current_delivery():
    setup = _setup()
    changed = _setup(code="02")[2]
    assert _apply(setup, coding=changed).diagnostics == ()
    book = setup[3]["fixture"]
    reordered = book.model_copy(update={"codes": tuple(reversed(book.codes))})
    assert _apply(setup, classifications={"fixture": reordered}).diagnostics == ()
    changed_book = book.model_copy(update={"name": "Changed definition"})
    result = _apply(setup, classifications={"fixture": changed_book})
    assert result.diagnostics == ()
    assert (
        _sole_classification(next(iter(result.coding.values())).segments[0])
        == "fixture"
    )


def test_book_losing_the_observed_code_severs_current_binding():
    setup = _setup(code="02")
    assert _apply(setup).diagnostics == ()
    book = setup[3]["fixture"]
    shrunk = book.model_copy(
        update={"codes": tuple(c for c in book.codes if c.code != "02")}
    )
    result = _apply(setup, classifications={"fixture": shrunk})
    assert result.diagnostics[0].code == "nonconforming_classification_codes"
    state = _form(setup, result).states[0]
    assert _sole_classification(state) == "fixture"
    assert (
        _sole_conformance(state) is not None
        and _sole_conformance(state).status == "extended"
    )
    assert state.value_set is not None and state.value_set.members == (
        ("02", "Source label"),
    )


def test_multiple_books_retain_independent_overlap_links_and_inline_coding():
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
                    "expected_classification": "0" * 64,
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
    assert [
        (
            s.valid_from,
            s.valid_to,
            tuple(link.classification for link in s.classification_links),
        )
        for s in states
    ] == [
        ("2020-01-01", "2020-06-30", ("fixture",)),
        ("2020-07-01", "2020-12-31", ("alternate", "fixture")),
    ]
    assert states[0].value_set == states[1].value_set
    assert result.diagnostics[0].code == "multiple_classifications_declared"
    assert result.diagnostics[0].severity == "warning"
    assert _apply(setup, (competing, case), classifications=books) == result


def test_declared_reference_can_exist_without_inline_codes_but_inline_override_cannot():
    setup = _setup(inline=False)
    declared = _form(setup, _apply(setup)).states[0]
    assert _sole_classification(declared) == "fixture" and declared.value_set is None
    assert _sole_conformance(declared) is None
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
    assert _sole_classification(state) == "fixture" and state.value_set is not None
    assert state.value_set.members == (("99", "Source label"),)
    assert _sole_conformance(state) is not None
    assert _sole_conformance(state).declared_classification == "fixture"
    assert _sole_conformance(state).status == "extended"
    assert result.diagnostics[0].code == "nonconforming_classification_codes"
    write_resolved_catalog(
        (variable,),
        tmp_path / "reg_meta.db",
        manifest=synthetic_manifest(),
        diagnostic=True,
        classifications=tuple(setup[3].values()),
    )


def test_curated_sentinel_keeps_checked_binding_with_warning(tmp_path):
    setup = _setup(
        code="99",
        sentinels=(SentinelCode(code="99", meaning="not applicable"),),
    )
    sentinel_book = setup[3]["fixture"]
    result = _apply(setup)
    assert [d.code for d in result.diagnostics] == ["sentinel_classification_codes"]
    assert result.diagnostics[0].severity == "warning"
    variable = _form(setup, result)
    state = variable.states[0]
    assert _sole_classification(state) == "fixture"
    assert state.value_set is not None and state.value_set.members == (
        ("99", "Source label"),
    )
    assert _sole_conformance(state) is not None
    assert _sole_conformance(state).status == "extended"
    assert _sole_conformance(state).nonconforming_members == ()
    assert _sole_conformance(state).sentinel_members == (("99", "Source label"),)
    write_resolved_catalog(
        (variable,),
        tmp_path / "reg_meta.db",
        manifest=synthetic_manifest(),
        classifications=(sentinel_book,),
    )


def test_naming_a_sentinel_does_not_stale_the_binding():
    setup = _setup()
    book = setup[3]["fixture"]
    assert _apply(setup).diagnostics == ()
    sentinel_book = book.model_copy(
        update={"sentinel_codes": (SentinelCode(code="99", meaning="not applicable"),)}
    )
    assert _apply(setup, classifications={"fixture": sentinel_book}).diagnostics == ()


def test_accepted_omission_survives_classification_application():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    omitted = replace(
        original,
        segments=tuple(replace(s, state_disposition="omit") for s in original.segments),
    )
    result = _apply(setup, coding={key: omitted})
    assert result.coding == {key: omitted} and result.diagnostics == ()


def test_declared_classification_does_not_hide_an_uncovered_coding_period():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    claim = replace(
        original.claims[0],
        scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2020-01-01", end="2020-06-30"),),
        ),
    )
    case = setup[1].model_copy(
        update={
            "decision": setup[1].decision.model_copy(
                update={
                    "expected_codings": coding_expectations(
                        (claim,), "2020-01-01", "2020-12-31"
                    )
                }
            )
        }
    )
    result = _apply(setup, (case,), coding={key: resolve_code_membership((claim,))})
    issue = result.coding[key].issues[-1]
    assert (issue.code, issue.valid_from, issue.valid_to) == (
        "missing_coding_period",
        "2020-07-01",
        "2020-12-31",
    )
    assert result.coding[key].segments[-1].code_set is None
    assert result.coding[key].claims == (claim,)


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


def _declaration(setup, field=None):
    occurrence = source_occurrence(setup[0])
    return replace(
        occurrence,
        fields=occurrence.fields.model_copy(
            update={"classification_declared": field or value_field("FIX")}
        ),
    )


def _declared(setup, occurrences, **kwargs):
    return apply_classification_cases(
        (setup[0],),
        kwargs.pop("cases", ()),
        coding=kwargs.pop("coding", setup[2]),
        classifications=kwargs.pop("classifications", setup[3]),
        references=kwargs.pop("references", {"FIX": "fixture"}),
        occurrences=occurrences,
        **kwargs,
    )


def test_source_declaration_preserves_explicit_open_scope_without_adding_codes():
    setup = _setup(inline=False)
    occurrence = replace(
        _declaration(setup),
        edition_period_scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2020", end=None),)
        ),
    )
    result = _declared(setup, (occurrence,))
    assert result.evaluations == result.diagnostics == ()
    segment = next(iter(result.coding.values())).segments[0]
    assert (segment.valid_from, segment.valid_to, _sole_classification(segment)) == (
        "2020-01-01",
        "9999-12-31",
        "fixture",
    )
    assert segment.code_set is _sole_conformance(segment) is None
    assert "Source classification declaration" in segment.provenance[0]


def test_source_declaration_resolves_exact_alias():
    setup = _setup(inline=False)
    result = _declared(
        setup,
        (_declaration(setup, value_field("Source title")),),
        references={"FIX": "fixture", "Source title": "fixture"},
    )
    assert result.diagnostics == ()
    assert (
        _sole_classification(next(iter(result.coding.values())).segments[0])
        == "fixture"
    )


def test_family_reference_requires_one_edition_to_cover_whole_occurrence():
    setup = _setup(inline=False)
    first = setup[3]["fixture"].model_copy(
        update={"valid_from": 2000, "valid_to": 2020}
    )
    second = first.model_copy(
        update={
            "slug": "second",
            "short_name": "SECOND",
            "valid_from": 2021,
            "valid_to": None,
        }
    )
    options = {
        "classifications": {"fixture": first, "second": second},
        "family_references": {"Family": ("fixture", "second")},
    }

    def occurrence(start: str, end: str):
        return replace(
            _declaration(setup, value_field("Family")),
            edition_period_scope=TemporalScope(
                kind="intervals", intervals=(ScopeInterval(start=start, end=end),)
            ),
        )

    result = _declared(setup, (occurrence("2020", "2020"),), **options)
    assert result.diagnostics == ()
    assert (
        _sole_classification(next(iter(result.coding.values())).segments[0])
        == "fixture"
    )
    shifted = {
        **options,
        "classifications": {
            "fixture": first.model_copy(update={"valid_to": 2019}),
            "second": second.model_copy(update={"valid_from": 2020}),
        },
    }
    result = _declared(setup, (occurrence("2020", "2020"),), **shifted)
    assert result.diagnostics == ()
    assert (
        _sole_classification(next(iter(result.coding.values())).segments[0]) == "second"
    )
    result = _declared(setup, (occurrence("2019", "2021"),), **options)
    assert [d.code for d in result.diagnostics] == [
        "unresolved_classification_reference"
    ]
    overlapping = second.model_copy(update={"valid_from": 2020})
    result = _declared(
        setup,
        (occurrence("2020", "2020"),),
        **{**options, "classifications": {"fixture": first, "second": overlapping}},
    )
    assert [d.code for d in result.diagnostics] == [
        "unresolved_classification_reference"
    ]
    result = _declared(setup, (occurrence("1990", "1991"),), **options)
    assert [d.code for d in result.diagnostics] == [
        "unresolved_classification_reference"
    ]
    direct = _declared(setup, (_declaration(setup),), **options)
    assert direct.diagnostics == ()
    assert (
        _sole_classification(next(iter(direct.coding.values())).segments[0])
        == "fixture"
    )


def test_source_and_case_bindings_compose_together_and_check_conformance():
    setup = _setup(code="outside")
    result = _declared(setup, (_declaration(setup),), cases=(setup[1],))
    assert [d.code for d in result.diagnostics] == [
        "nonconforming_classification_codes"
    ]
    segment = next(iter(result.coding.values())).segments[0]
    assert _sole_classification(segment) == "fixture"
    assert _sole_conformance(segment).declared_classification == "fixture"
    assert segment.code_set.members == (("outside", "Source label"),)
    assert len(segment.provenance) == 2


def test_source_conflict_with_case_is_bounded_and_order_independent():
    setup = _setup()
    second = setup[3]["fixture"].model_copy(
        update={"slug": "second", "short_name": "TWO"}
    )
    competing = replace(
        _declaration(setup, value_field("TWO")),
        edition_period_scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2020-06-01", end="2020-08-31"),),
        ),
    )
    occurrences = (_declaration(setup), competing)
    options = {
        "cases": (setup[1],),
        "classifications": {**setup[3], "second": second},
        "references": {"FIX": "fixture", "TWO": "second"},
    }
    result = _declared(setup, occurrences, **options)
    assert result == _declared(setup, tuple(reversed(occurrences)), **options)
    assert [
        (
            s.valid_from,
            s.valid_to,
            tuple(link.classification for link in s.classification_links),
        )
        for s in next(iter(result.coding.values())).segments
    ] == [
        ("2020-01-01", "2020-05-31", ("fixture",)),
        ("2020-06-01", "2020-08-31", ("fixture", "second")),
        ("2020-09-01", "2020-12-31", ("fixture",)),
    ]
    assert result.diagnostics[0].code == "multiple_classifications_declared"
    assert result.diagnostics[0].severity == "warning"


@pytest.mark.parametrize("reference", ["FIX extra", "https://example.test/FIX"])
def test_unknown_reference_blocks_even_when_another_declaration_is_known(reference):
    setup = _setup()
    result = _declared(
        setup, (_declaration(setup), _declaration(setup, value_field(reference)))
    )
    assert _sole_classification(next(iter(result.coding.values())).segments[0]) is None
    assert [d.code for d in result.diagnostics] == [
        "unresolved_classification_reference"
    ]
    assert result.diagnostics[0].refs


def test_declared_binding_contract_errors_remain_fatal():
    setup = _setup()
    occurrence = _declaration(setup)
    with pytest.raises(ValueError, match="unconverted codebook"):
        _declared(setup, (occurrence,), references={"FIX": "missing"})
    with pytest.raises(ValueError, match="unconverted column"):
        _declared(setup, (occurrence,), coding={})
    with pytest.raises(ValueError, match="explicit reference dictionary"):
        _declared(setup, (occurrence,), references=None)
    with pytest.raises(ValueError, match="original evidence"):
        _declared(setup, (replace(occurrence, source_records=()),))
    with pytest.raises(ValueError, match="nonempty text"):
        _declared(setup, (_declaration(setup, value_field(True)),))
    result = _declared(setup, (occurrence,))
    with pytest.raises(ValueError, match="one application"):
        _declared(setup, (occurrence,), coding=result.coding)


def test_unknown_negative_and_absent_declarations_keep_distinct_meanings():
    setup = _setup()
    absent = source_occurrence(setup[0])
    assert _declared(setup, (absent,)).coding == setup[2]
    negative = _declaration(setup, SourceField(status="negative"))
    result = _declared(setup, (negative,))
    assert result.diagnostics == ()
    assert _sole_classification(next(iter(result.coding.values())).segments[0]) is None
    result = _declared(setup, (_declaration(setup), negative))
    assert result.diagnostics[0].code == "conflicting_classification_decisions"
    assert result.diagnostics[0].severity == "error"
    unknown = _declaration(setup, SourceField(status="unknown"))
    result = _declared(setup, (_declaration(setup), unknown))
    assert [d.code for d in result.diagnostics] == [
        "unknown_classification_declaration"
    ]
    assert result.diagnostics[0].severity == "warning"
    assert (
        _sole_classification(next(iter(result.coding.values())).segments[0])
        == "fixture"
    )
    withheld = replace(unknown, withheld_fields=("classification_declared",))
    result = _declared(setup, (_declaration(setup), withheld))
    assert result.diagnostics[0].severity == "error"
    assert _sole_classification(next(iter(result.coding.values())).segments[0]) is None
    assert _declared(setup, (replace(unknown, use="support"),)).diagnostics == ()


def test_source_scope_and_identity_do_not_supply_missing_classification_bounds():
    setup = _setup()
    occurrence = _declaration(setup)
    pooled = replace(
        occurrence,
        edition_period_scope=TemporalScope(kind="pooled", label="2019-2021"),
    )
    missing_column = replace(
        occurrence,
        fields=occurrence.fields.model_copy(update={"column_name": None}),
    )
    result = _declared(setup, (pooled, missing_column))
    assert result.coding == setup[2]
    assert {d.code for d in result.diagnostics} == {
        "unsupported_classification_scope",
        "unknown_classification_column",
    }


def test_label_rule_normalizes_claims_and_preserves_occurrence_evidence():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    claim = replace(original.claims[0], version_label="  LKF\u00a0 1998  ")
    coding = {key: resolve_code_membership((claim,))}
    matched: set[str] = set()
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications=setup[3],
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"LKF 1998": "fixture"},
        matched_labels=matched,
    )
    segment = result.coding[key].segments[0]
    assert _sole_classification(segment) == "fixture"
    assert (
        _sole_conformance(segment) is not None
        and _sole_conformance(segment).status == "conforming"
    )
    assert (
        "label rule: 'LKF 1998' -> fixture (classifications/FIX.toml)"
        in segment.provenance
    )
    assert matched == {"LKF 1998"}
    changed_book = setup[3]["fixture"].model_copy(
        update={
            "codes": (
                *setup[3]["fixture"].codes,
                ResolvedClassificationCode(code="03", label="New code"),
            )
        }
    )
    changed = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications={"fixture": changed_book},
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"LKF 1998": "fixture"},
    )
    assert changed.diagnostics == ()
    assert _sole_classification(changed.coding[key].segments[0]) == "fixture"


def test_label_rule_skips_segment_without_catalog_occurrence():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    claim = replace(original.claims[0], version_label="Listed")
    coding = {key: resolve_code_membership((claim,))}
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications=setup[3],
        occurrences=(),
        label_rules={"Listed": "fixture"},
    )
    assert result.coding == coding
    assert result.diagnostics == ()


def test_two_labels_for_one_book_make_one_rule_binding():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    first = replace(original.claims[0], version_label="First")
    second = replace(first, claim_id="other", version_label="Second")
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding={key: resolve_code_membership((first, second))},
        classifications=setup[3],
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"First": "fixture", "Second": "fixture"},
    )
    segment = result.coding[key].segments[0]
    assert _sole_classification(segment) == "fixture"
    assert segment.provenance == (
        "label rule: 'First' -> fixture (classifications/FIX.toml)",
    )
    assert result.diagnostics == ()


def test_label_rule_preserves_multiple_books_and_respects_omitted_state():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    first = replace(original.claims[0], version_label="First")
    second = replace(first, claim_id="other", version_label="Second")
    coding = {key: resolve_code_membership((first, second))}
    alternate = setup[3]["fixture"].model_copy(update={"slug": "alternate"})
    kwargs = {
        "coding": coding,
        "classifications": {**setup[3], "alternate": alternate},
        "occurrences": (source_occurrence(setup[0]),),
        "label_rules": {"First": "fixture", "Second": "alternate"},
    }
    result = apply_classification_cases((setup[0],), (), **kwargs)
    assert tuple(
        link.classification
        for link in result.coding[key].segments[0].classification_links
    ) == ("alternate", "fixture")
    assert [issue.code for issue in result.diagnostics] == [
        "multiple_classifications_declared"
    ]
    omitted = replace(
        coding[key],
        segments=tuple(
            replace(segment, state_disposition="omit")
            for segment in coding[key].segments
        ),
    )
    result = apply_classification_cases(
        (setup[0],), (), **{**kwargs, "coding": {key: omitted}}
    )
    assert result.coding == {key: omitted}
    assert result.diagnostics == ()


def test_override_wins_over_label_rule_on_its_window():
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    claim = replace(original.claims[0], version_label="Listed")
    alternate = setup[3]["fixture"].model_copy(update={"slug": "alternate"})
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding={key: resolve_code_membership((claim,))},
        classifications={**setup[3], "alternate": alternate},
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"Listed": "fixture"},
        override=("alternate", "classifications/ALT.toml#/binding/variable/1"),
    )
    assert result.diagnostics == ()
    assert _sole_classification(result.coding[key].segments[0]) == "alternate"
    duplicate_overrides: set[str] = set()
    apply_classification_cases(
        (setup[0],),
        (),
        coding={key: resolve_code_membership((claim,))},
        classifications=setup[3],
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"Listed": "fixture"},
        override=("fixture", "classifications/FIX.toml#/binding/variable/1"),
        duplicate_overrides=duplicate_overrides,
    )
    assert duplicate_overrides == {"classifications/FIX.toml#/binding/variable/1"}


_REPO_ROOT = Path(__file__).resolve().parent.parent
_CURATION = _REPO_ROOT / "curation"
_CLASSIFICATION_CODES = _REPO_ROOT / "input_data" / "classifications"


@cache
def _repo_label_rules() -> dict[str, str]:
    tree = load_curation_tree(_CURATION)
    return {
        label: entry.classification.slug
        for entry in tree.classifications
        for label in entry.binding.value_set_labels
    }


@cache
def _repo_book(short_name: str, codes_file: str) -> ResolvedClassification:
    return ResolvedClassification(
        slug=short_name.lower(),
        short_name=short_name,
        name=short_name,
        codes=tuple(
            ResolvedClassificationCode(code=code, label=label)
            for code, label in load_valid_codes(
                _CLASSIFICATION_CODES / codes_file
            ).items()
        ),
    )


def _synthetic_label_claim(setup, *, label: str, code: str):
    key, original = next(iter(setup[2].items()))
    claim = replace(
        original.claims[0],
        version_label=label,
        members=(replace(original.claims[0].members[0], code=code),),
    )
    return key, {key: resolve_code_membership((claim,))}


@pytest.mark.parametrize(
    ("label", "code"),
    [
        ("SNI 2002, begränsad nivå", "01"),
        ("SNI 2002, begränsad nivå", "42"),
        # SNI 92 reporting groups: limited (42 groups) and coarse (10 groups).
        ("SNI 92, begränsad nivå", "42"),
        ("SNI 92, begränsad nivå", "70"),  # RAMS 'Okänd värde' (missing)
        ("SNI 92, grov nivå", "03"),
        ("SNI 92, grov nivå", "00000"),  # coarse 'Uppgift saknas' (missing)
        # SNI 2002 coarse reporting groups (10 groups).
        ("SNI 2002, grov nivå", "02"),
        ('SNI 2007, grov nivå - "populärversion"', "G01"),
        ('SNI 2007, grov nivå - "populärversion"', "G99"),
        ('SNI 2007, utökad nivå - "populärversion"', "U01"),
        ('SNI 2007, utökad nivå - "populärversion"', "U99"),
    ],
)
def test_grouped_sni_reporting_labels_are_not_bound_to_detailed_books(label, code):
    setup = _setup()
    key, coding = _synthetic_label_claim(setup, label=label, code=code)
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications={
            "sni1992": _repo_book("SNI92", "sni92.csv").model_copy(
                update={"slug": "sni1992"}
            ),
            "sni2002": _repo_book("SNI2002", "sni2002.csv"),
            "sni2007": _repo_book("SNI2007", "sni2007.csv"),
        },
        occurrences=(source_occurrence(setup[0]),),
        label_rules=_repo_label_rules(),
    )
    assert result.diagnostics == ()
    segment = result.coding[key].segments[0]
    assert _sole_classification(segment) is None
    assert _sole_conformance(segment) is None
    assert segment.code_set is not None
    assert segment.code_set.members == ((code, "Source label"),)
    assert result.coding[key].claims[0].version_label == label
    assert (segment.valid_from, segment.valid_to) == ("2020-01-01", "2020-12-31")
    assert not any(p.startswith("label rule:") for p in segment.provenance)


def test_retained_detailed_sni_label_still_binds():
    setup = _setup()
    key, coding = _synthetic_label_claim(
        setup,
        label="Standard för svensk näringsgrensindelning, 2002 Branscher",
        code="01",
    )
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications={"sni2002": _repo_book("SNI2002", "sni2002.csv")},
        occurrences=(source_occurrence(setup[0]),),
        label_rules=_repo_label_rules(),
    )
    assert result.diagnostics == ()
    segment = result.coding[key].segments[0]
    assert _sole_classification(segment) == "sni2002"
    assert (
        _sole_conformance(segment) is not None
        and _sole_conformance(segment).status == "conforming"
    )
    assert any(p.startswith("label rule:") for p in segment.provenance)


def _scoped_sentinel_case(setup, *, start="2020-01-01", end="2020-12-31"):
    from reg_meta.source_evidence import canonical_sha256
    from reg_meta_build.source_coding import copied_coding_fingerprints

    record, case, coding, books = setup
    decision = case.decision
    claims = coding[decision.column_key].claims
    return case.model_copy(
        update={
            "case_id": "scoped-sentinel",
            "targets": capture_expectations(
                (record,), fields=tuple(SourceFields.model_fields), coding=True
            ),
            "decision": decision.model_copy(
                update={
                    "valid_from": start,
                    "valid_to": end,
                    "expected_codings": coding_expectations(claims, start, end),
                    "expected_source_codings": copied_coding_fingerprints(claims),
                    "expected_classification": canonical_sha256(
                        books["fixture"].model_dump(mode="json")
                    ),
                    "sentinel_members": (("99", "Source label"),),
                    "binding_scope": "inline_coding",
                }
            ),
        }
    )


def test_scoped_sentinel_preserves_source_list_and_only_affects_its_window():
    setup = _setup(code="99")
    case = _scoped_sentinel_case(setup, start="2020-07-01")
    result = _apply(setup, (setup[1], case))
    segments = result.coding[case.decision.column_key].segments
    assert [(s.valid_from, s.valid_to, _sole_classification(s)) for s in segments] == [
        ("2020-01-01", "2020-06-30", "fixture"),
        ("2020-07-01", "2020-12-31", "fixture"),
    ]
    assert segments[0].code_set == segments[1].code_set
    assert (
        result.coding[case.decision.column_key].claims
        == setup[2][case.decision.column_key].claims
    )
    assert _sole_conformance(segments[1]).sentinel_members == (("99", "Source label"),)
    certificate = _sole_conformance(segments[1]).scoped_sentinels[0]
    assert certificate.members == _sole_conformance(segments[1]).sentinel_members
    assert certificate.classification_sha256 == case.decision.expected_classification
    assert certificate.source_fingerprints == case.decision.expected_source_codings
    assert certificate.valid_from == "2020-07-01"
    assert certificate.delivery_column_name == case.decision.column_key[-1]
    assert "scoped-sentinel" in certificate.provenance
    assert setup[3]["fixture"].sentinel_codes == ()
    assert [d.code for d in result.diagnostics] == [
        "nonconforming_classification_codes",
        "sentinel_classification_codes",
    ]


@pytest.mark.parametrize("change", ["label", "member", "outside-window", "book"])
def test_scoped_sentinel_rejects_changed_coding_and_codebook(change):
    setup = _setup(code="99")
    case = _scoped_sentinel_case(setup, start="2020-07-01")
    key = case.decision.column_key
    claims = setup[2][key].claims
    books = setup[3]
    if change == "book":
        books = {"fixture": books["fixture"].model_copy(update={"name": "Changed"})}
    elif change == "outside-window":
        claims = (
            *claims,
            CodeListClaim(
                "additional",
                TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="2019", end="2019"),),
                ),
                (
                    CodeMembershipClaim(
                        "02", "Other", TemporalScope(kind="year_independent")
                    ),
                ),
            ),
        )
    else:
        members = claims[0].members
        members = (
            (replace(members[0], label="Substantive industry"),)
            if change == "label"
            else (*members, replace(members[0], code="02"))
        )
        claims = (replace(claims[0], members=members),)
    result = _apply(
        setup,
        (setup[1], case),
        coding={key: resolve_code_membership(claims)},
        classifications=books,
    )
    assert "classification_evidence_changed" in {d.code for d in result.diagnostics}
    assert all(
        _sole_classification(s)
        == (None if s.valid_from.startswith("2019") else "fixture")
        for s in result.coding[key].segments
    )
    assert all(
        not _sole_conformance(s).sentinel_members
        for s in result.coding[key].segments
        if _sole_conformance(s)
    )


def test_scoped_sentinel_guard_uses_accepted_owner_and_complete_effective_peers():
    from reg_meta_build.source_curation import SourceEvidence

    setup = _setup(code="99")
    record = setup[0]
    original = source_occurrence(record)
    accepted = replace(
        original,
        variable_key=(*original.variable_key, "accepted-partition", "1.1.accepted"),
    )
    key = accepted.column_key
    assert key is not None
    case = _scoped_sentinel_case(setup)
    case = case.model_copy(
        update={
            "decision": case.decision.model_copy(update={"column_key": key}),
            "peer_guards": (
                PeerGuard(
                    guard_id="accepted-members",
                    source=record.source,
                    effective_column=key,
                    expected_members=tuple(t.ref for t in case.targets),
                ),
            ),
        }
    )
    coding = {key: setup[2][original.column_key]}
    arguments = {"coding": coding, "classifications": setup[3]}
    evidence = SourceEvidence((record,), effective_occurrences=(accepted,))
    good = apply_classification_cases(evidence, (case,), **arguments)
    assert _sole_classification(good.coding[key].segments[0]) == "fixture"
    peer = record.model_copy(
        update={
            "record_id": "new-peer",
            "locators": tuple(
                loc.model_copy(
                    update={
                        "semantic_record_key": (
                            *loc.semantic_record_key[:-1],
                            "member:101",
                        )
                    }
                )
                for loc in record.locators
            ),
        }
    )
    extra = replace(accepted, source_records=(peer,))
    for changed in (
        SourceEvidence((record, peer), effective_occurrences=(accepted, extra)),
        SourceEvidence((record,), effective_occurrences=()),
        SourceEvidence(
            (record,),
            effective_occurrences=(
                replace(
                    accepted,
                    edition_period_scope=TemporalScope(
                        kind="intervals",
                        intervals=(
                            ScopeInterval(start="2020-07-01", end="2020-12-31"),
                        ),
                    ),
                ),
            ),
        ),
    ):
        result = apply_classification_cases(changed, (case,), **arguments)
        assert result.diagnostics and all(
            _sole_classification(s) is None for s in result.coding[key].segments
        )


def test_scoped_sentinel_leaves_substantive_same_literal_in_another_window():
    setup = _setup(code="99")
    key = setup[1].decision.column_key
    current = setup[2][key].claims[0]
    older = replace(
        current,
        claim_id="older",
        scope=TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2019", end="2019"),)
        ),
        members=(replace(current.members[0], label="Substantive industry"),),
    )
    coding = {key: resolve_code_membership((older, current))}
    setup = (*setup[:2], coding, setup[3])
    case = _scoped_sentinel_case(setup)
    declared = setup[1].model_copy(
        update={
            "decision": setup[1].decision.model_copy(
                update={"valid_from": "2019-01-01"}
            )
        }
    )
    result = _apply(setup, (declared, case))
    old, new = result.coding[key].segments
    assert (
        _sole_classification(old) == "fixture"
        and _sole_conformance(old).sentinel_members == ()
    )
    assert old.code_set.members == (("99", "Substantive industry"),)
    assert _sole_classification(new) == "fixture"
    assert _sole_conformance(new).sentinel_members == (("99", "Source label"),)
    assert result.coding[key].claims == (older, current)


def test_scoped_sentinel_requires_every_shared_ref_original_projection():
    from reg_meta_build.source_curation import SourceEvidence

    setup = _setup(code="99")
    record = setup[0]
    twin = record.model_copy(
        update={
            "record_id": "shared-ref-twin",
            "fields": record.fields.model_copy(
                update={"column_name": value_field("Sibling")}
            ),
        }
    )
    first = source_occurrence(record)
    second = replace(source_occurrence(twin), fields=first.fields)
    key = first.column_key
    assert key is not None
    case = _scoped_sentinel_case(setup)
    targets = capture_expectations(
        (record, twin), fields=tuple(SourceFields.model_fields), coding=True
    )
    assert len(targets) == 1 and len(targets[0].alternatives) == 2
    case = case.model_copy(
        update={
            "targets": targets,
            "peer_guards": (
                PeerGuard(
                    guard_id="complete-twins",
                    source=record.source,
                    effective_column=key,
                    expected_members=tuple(t.ref for t in targets),
                ),
            ),
        }
    )
    good = apply_classification_cases(
        SourceEvidence((record, twin), effective_occurrences=(first, second)),
        (case,),
        coding=setup[2],
        classifications=setup[3],
    )
    assert _sole_classification(good.coding[key].segments[0]) == "fixture"
    missing = apply_classification_cases(
        SourceEvidence((record,), effective_occurrences=(first,)),
        (case,),
        coding=setup[2],
        classifications=setup[3],
    )
    assert missing.evaluations[0].status != "applicable"
    assert _sole_classification(missing.coding[key].segments[0]) is None


def test_dated_classification_decision_does_not_date_independent_delivery():
    setup = _setup()
    coding = {
        key: replace(
            resolution,
            segments=tuple(
                replace(
                    segment,
                    period_scope="year_independent",
                    valid_from=None,
                    valid_to=None,
                )
                for segment in resolution.segments
            ),
        )
        for key, resolution in setup[2].items()
    }
    result = _apply(setup, coding=coding)
    assert any(
        issue.code == "unsupported_classification_scope" for issue in result.diagnostics
    )
    assert result.coding == coding


def test_two_book_conformance_and_extensions_are_stored_independently(tmp_path):
    setup = _setup()
    second = setup[3]["fixture"].model_copy(
        update={
            "slug": "second",
            "short_name": "TWO",
            "codes": (ResolvedClassificationCode(code="02", label="Two"),),
        }
    )
    result = _declared(
        setup,
        (_declaration(setup), _declaration(setup, value_field("TWO"))),
        references={"FIX": "fixture", "TWO": "second"},
        classifications={**setup[3], "second": second},
    )
    variable = _form(setup, result)
    state = variable.states[0]
    assert state.value_set.members == (("01", "Source label"),)
    assert [
        (link.classification, link.conformance.status)
        for link in state.classification_links
    ] == [("fixture", "conforming"), ("second", "extended")]
    assert all(link.provenance for link in state.classification_links)
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (variable,),
        output,
        manifest=synthetic_manifest(),
        classifications=(*setup[3].values(), second),
    )
    with sqlite3.connect(output) as connection:
        assert connection.execute(
            "SELECT c.slug, cc.status, cc.overlap FROM classification_conformance cc JOIN classification c ON c.id=cc.declared_classification_id ORDER BY c.slug"
        ).fetchall() == [("fixture", "conforming", 1.0), ("second", "extended", 0.0)]
        assert connection.execute(
            "SELECT c.slug, vc.code FROM classification_conformance_code cc JOIN classification c ON c.id=cc.declared_classification_id JOIN value_code vc USING(code_id)"
        ).fetchall() == [("second", "01")]
        assert (
            connection.execute("SELECT count(*) FROM state_classification").fetchone()[
                0
            ]
            == 2
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert (
            connection.execute(
                "SELECT value FROM import_manifest WHERE key='schema_version'"
            ).fetchone()[0]
            == SCHEMA_VERSION
        )
    from reg_meta.db import open_db
    from reg_meta.errors import RegMetaError

    with open_db(output) as conn:
        assert (
            conn.execute("SELECT count(*) FROM state_classification").fetchone()[0] == 2
        )
    conn.close()
    from reg_meta_build.db import open_built_db

    with open_built_db(output) as conn:
        assert (
            conn.execute("SELECT count(*) FROM state_classification").fetchone()[0] == 2
        )
    conn.close()
    with sqlite3.connect(output) as conn:
        conn.execute(
            "UPDATE import_manifest SET value='6.18.0' WHERE key='schema_version'"
        )
    for opener in (open_db, open_built_db):
        with pytest.raises(RegMetaError) as stale:
            opener(output)
        assert stale.value.code == "schema_incompatible"
        assert "6.18.0" in stale.value.message and SCHEMA_VERSION in stale.value.message


@pytest.mark.parametrize("difference", ["code", "label"])
def test_plural_books_do_not_union_contrary_source_domains(difference):
    setup = _setup()
    key, original = next(iter(setup[2].items()))
    first = replace(original.claims[0], version_label="First")
    member = first.members[0]
    changed = (
        replace(member, code="02")
        if difference == "code"
        else replace(member, label="Different meaning")
    )
    second = replace(
        first, claim_id="other", version_label="Second", members=(changed,)
    )
    coding = {key: resolve_code_membership((first, second))}
    alternate = setup[3]["fixture"].model_copy(update={"slug": "alternate"})
    result = apply_classification_cases(
        (setup[0],),
        (),
        coding=coding,
        classifications={**setup[3], "alternate": alternate},
        occurrences=(source_occurrence(setup[0]),),
        label_rules={"First": "fixture", "Second": "alternate"},
    )
    assert result.coding[key].issues == coding[key].issues
    assert coding[key].issues
    assert all(segment.code_set is None for segment in result.coding[key].segments)
    assert all(
        link.conformance is None
        for segment in result.coding[key].segments
        for link in segment.classification_links
    )
    assert tuple(
        link.classification
        for link in result.coding[key].segments[0].classification_links
    ) == ("alternate", "fixture")
