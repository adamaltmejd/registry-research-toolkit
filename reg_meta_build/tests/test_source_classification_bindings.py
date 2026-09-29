"""Checked codebook bindings preserve original evidence and bounded ambiguity."""

from dataclasses import replace
from functools import cache
from pathlib import Path

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from reg_meta_build._curation import SentinelCode
from reg_meta_build.classifications import load_valid_codes
from reg_meta_build.curation_tree import load_curation_tree
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
    SourceField,
    SourceFields,
    SourceRevision,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row


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
    assert next(iter(result.coding.values())).segments[0].classification == "fixture"


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
    assert state.classification is None
    assert state.conformance is not None and state.conformance.status == "severed"
    assert state.value_set is not None and state.value_set.members == (
        ("02", "Source label"),
    )


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
    assert state.classification == "fixture"
    assert state.value_set is not None and state.value_set.members == (
        ("99", "Source label"),
    )
    assert state.conformance is not None
    assert state.conformance.status == "kept"
    assert state.conformance.nonconforming_members == ()
    assert state.conformance.sentinel_members == (("99", "Source label"),)
    write_resolved_catalog(
        (variable,),
        tmp_path / "reg_meta.db",
        manifest={},
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
    assert (segment.valid_from, segment.valid_to, segment.classification) == (
        "2020-01-01",
        "9999-12-31",
        "fixture",
    )
    assert segment.code_set is segment.conformance is None
    assert "Source classification declaration" in segment.provenance[0]


def test_source_declaration_resolves_exact_alias():
    setup = _setup(inline=False)
    result = _declared(
        setup,
        (_declaration(setup, value_field("Source title")),),
        references={"FIX": "fixture", "Source title": "fixture"},
    )
    assert result.diagnostics == ()
    assert next(iter(result.coding.values())).segments[0].classification == "fixture"


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
    assert next(iter(result.coding.values())).segments[0].classification == "fixture"
    shifted = {
        **options,
        "classifications": {
            "fixture": first.model_copy(update={"valid_to": 2019}),
            "second": second.model_copy(update={"valid_from": 2020}),
        },
    }
    result = _declared(setup, (occurrence("2020", "2020"),), **shifted)
    assert result.diagnostics == ()
    assert next(iter(result.coding.values())).segments[0].classification == "second"
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
    assert next(iter(direct.coding.values())).segments[0].classification == "fixture"


def test_source_and_case_bindings_compose_together_and_check_conformance():
    setup = _setup(code="outside")
    result = _declared(setup, (_declaration(setup),), cases=(setup[1],))
    assert [d.code for d in result.diagnostics] == [
        "nonconforming_classification_codes"
    ]
    segment = next(iter(result.coding.values())).segments[0]
    assert segment.classification is None
    assert segment.conformance.declared_classification == "fixture"
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
        (s.valid_from, s.valid_to, s.classification)
        for s in next(iter(result.coding.values())).segments
    ] == [
        ("2020-01-01", "2020-05-31", "fixture"),
        ("2020-06-01", "2020-08-31", None),
        ("2020-09-01", "2020-12-31", "fixture"),
    ]
    assert result.diagnostics[0].code == "conflicting_classification_decisions"


@pytest.mark.parametrize("reference", ["FIX extra", "https://example.test/FIX"])
def test_unknown_reference_blocks_even_when_another_declaration_is_known(reference):
    setup = _setup()
    result = _declared(
        setup, (_declaration(setup), _declaration(setup, value_field(reference)))
    )
    assert next(iter(result.coding.values())).segments[0].classification is None
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
    assert next(iter(result.coding.values())).segments[0].classification is None
    result = _declared(setup, (_declaration(setup), negative))
    assert result.diagnostics[0].code == "conflicting_classification_decisions"
    unknown = _declaration(setup, SourceField(status="unknown"))
    result = _declared(setup, (_declaration(setup), unknown))
    assert [d.code for d in result.diagnostics] == [
        "unknown_classification_declaration"
    ]
    assert result.diagnostics[0].severity == "warning"
    assert next(iter(result.coding.values())).segments[0].classification == "fixture"
    withheld = replace(unknown, withheld_fields=("classification_declared",))
    result = _declared(setup, (_declaration(setup), withheld))
    assert result.diagnostics[0].severity == "error"
    assert next(iter(result.coding.values())).segments[0].classification is None
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
    assert segment.classification == "fixture"
    assert segment.conformance is not None and segment.conformance.status == "kept"
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
    assert changed.coding[key].segments[0].classification == "fixture"


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
    assert segment.classification == "fixture"
    assert segment.provenance == (
        "label rule: 'First' -> fixture (classifications/FIX.toml)",
    )
    assert result.diagnostics == ()


def test_label_rule_omits_state_and_conflicts_on_two_distinct_books():
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
    assert result.coding[key].segments[0].classification is None
    assert [issue.code for issue in result.diagnostics] == [
        "conflicting_classification_decisions"
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
    assert result.coding[key].segments[0].classification == "alternate"
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
            "sni92": _repo_book("SNI92", "sni92.csv"),
            "sni2002": _repo_book("SNI2002", "sni2002.csv"),
            "sni2007": _repo_book("SNI2007", "sni2007.csv"),
        },
        occurrences=(source_occurrence(setup[0]),),
        label_rules=_repo_label_rules(),
    )
    assert result.diagnostics == ()
    segment = result.coding[key].segments[0]
    assert segment.classification is None
    assert segment.conformance is None
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
    assert segment.classification == "sni2002"
    assert segment.conformance is not None and segment.conformance.status == "kept"
    assert any(p.startswith("label rule:") for p in segment.provenance)
