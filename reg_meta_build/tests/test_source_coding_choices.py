"""Accepted coding choices are finite, source-checked and order-independent."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from pydantic import ValidationError
from reg_meta_build.convert_errata import capture_expectations
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    coding_content_sha256,
    resolve_code_membership,
)
from reg_meta_build.source_coding_choices import apply_coding_choices, coding_for_period
from reg_meta_build.source_curation import CodingChoiceDecision, CurationCase, PeerGuard
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceFields,
    SourceRecord,
    SourceRevision,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row


def _record(year: int = 2020, column: str = "VALUE") -> SourceRecord:
    revision = SourceRevision.create(
        dataset="scb-fixture",
        publisher="SCB",
        purpose="coding fixture",
        upstream_revision="1",
        artifact_path="records.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    header = REGISTERINFORMATION_HEADER.split("|")
    values = _var_row(
        cvid=year,
        var_id=5,
        colname=column,
        register=("TEST", 1, 2),
        regver_id=year,
        year=str(year),
    ).split("|")
    return clean_scb_row(
        header,
        1,
        {
            name: (True, value, value)
            for name, value in zip(header, values, strict=True)
        },
        revision,
    ).record


def _claim(
    name: str, code: str, start: str = "2020-01-01", end: str = "2020-12-31"
) -> CodeListClaim:
    return CodeListClaim(
        name,
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start=start, end=end),)
        ),
        (CodeMembershipClaim(code, "Label", TemporalScope(kind="year_independent")),),
        version_label=name,
    )


def _case(
    record: SourceRecord,
    claims: tuple[CodeListClaim, ...],
    *,
    start: str = "2020-01-01",
    end: str = "2020-12-31",
    selected: int = 0,
    name: str = "accepted",
) -> CurationCase:
    projected = coding_for_period(claims, start, end)
    digests = []
    for claim in projected:
        digest = coding_content_sha256(claim)
        assert digest is not None
        digests.append(digest)
    assert digests
    column = source_occurrence(record).column_key
    assert column is not None
    return CurationCase(
        case_id=name,
        targets=capture_expectations((record,), fields=("column_name",)),
        peer_guards=(
            PeerGuard(
                guard_id=name,
                source=record.source,
                native=NativeCoordinates(register_id=1, variable_id=5),
                edition_scopes=(record.edition_scope,),
                expected_members=(record_ref(record),),
            ),
        ),
        decision=CodingChoiceDecision(
            reviewed=True,
            column_key=column,
            valid_from=start,
            valid_to=end,
            expected_codings=tuple(sorted(set(digests))),
            selected_coding=digests[selected],
            reason="Existing accepted list selection",
            provenance="accepted.toml entry 1",
        ),
    )


def _apply(
    record: SourceRecord, claims: tuple[CodeListClaim, ...], *cases: CurationCase
):
    key = source_occurrence(record).column_key
    assert key is not None
    return apply_coding_choices((record,), cases, coding={key: claims})


def test_choice_changes_only_checked_window_and_feeds_ordinary_formation() -> None:
    record = _record()
    claims = (_claim("first", "01"), _claim("second", "02"))
    case = _case(record, claims, start="2020-04-01", end="2020-06-30")
    assert CurationCase.model_validate_json(case.model_dump_json()) == case
    result = _apply(record, claims, case)
    assert result.accounting[0].status == "applied" and result.diagnostics == ()
    coding = next(iter(result.coding.values()))
    assert coding.claims == claims
    assert [(s.valid_from, s.valid_to, s.version_label) for s in coding.segments] == [
        ("2020-01-01", "2020-03-31", ""),
        ("2020-04-01", "2020-06-30", "first"),
        ("2020-07-01", "2020-12-31", ""),
    ]
    assert coding.segments[1].code_set is not None
    assert coding.segments[1].code_set.members == (("01", "Label"),)
    assert [(i.valid_from, i.valid_to) for i in coding.issues] == [
        ("2020-01-01", "2020-03-31"),
        ("2020-07-01", "2020-12-31"),
    ]
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
    assert formed.variable is not None and len(formed.variable.states) == 3
    chosen = next(
        state for state in formed.variable.states if state.valid_from == "2020-04-01"
    )
    assert chosen.value_set == coding.segments[1].code_set
    assert (
        chosen.provenance
        == "accepted: Existing accepted list selection\naccepted.toml entry 1"
    )
    assert formed.occurrences == (record,)


@pytest.mark.parametrize(
    "change", ["code", "label", "version", "period", "added", "removed"]
)
def test_relevant_coding_change_invalidates_choice_without_refresh(change: str) -> None:
    record = _record()
    claims = (_claim("first", "01"), _claim("second", "02"))
    case = _case(record, claims)
    changed = list(claims)
    if change in {"code", "label"}:
        changed[1] = replace(
            claims[1], members=(replace(claims[1].members[0], **{change: "different"}),)
        )
    elif change == "version":
        changed[1] = replace(claims[1], version_label="changed")
    elif change == "period":
        changed[1] = _claim("second", "02", end="2020-06-30")
    elif change == "added":
        changed.append(_claim("third", "03"))
    else:
        changed.pop()
    result = _apply(record, tuple(changed), case)
    assert result.accounting[0].status == "stale"
    assert result.diagnostics[0].code == "coding_evidence_changed"
    assert next(iter(result.coding.values())) == resolve_code_membership(tuple(changed))


def test_physical_duplicates_and_irrelevant_future_coding_do_not_expand_choice() -> (
    None
):
    record = _record()
    claims = (_claim("first", "01"), _claim("second", "02"))
    case = _case(record, claims)
    future = _claim("future", "03", "2021-01-01", "2021-12-31")
    changed = (
        future,
        replace(claims[1], claim_id="other-physical-id"),
        claims[0],
        claims[0],
    )
    key = source_occurrence(record).column_key
    assert key is not None
    result = apply_coding_choices(
        (record, _record(2021)), (case,), coding={key: changed}
    )
    assert result.accounting[0].status == "applied" and result.diagnostics == ()
    assert result.coding[key].claims == changed
    assert result.coding[key].segments[-1].version_label == "future"


def test_original_record_guard_is_checked_before_coding() -> None:
    record = _record()
    claims = (_claim("first", "01"), _claim("second", "02"))
    case = _case(record, claims)
    key = source_occurrence(record).column_key
    assert key is not None
    result = apply_coding_choices(
        (_record(column="OTHER"),), (case,), coding={key: claims}
    )
    assert result.accounting[0].status == "stale"
    assert result.diagnostics[0].code == "target_projection_changed"
    assert result.coding[key] == resolve_code_membership(claims)


def test_conflicting_choices_withhold_only_overlap_and_agreeing_choices_compose() -> (
    None
):
    record = _record()
    claims = (_claim("first", "01"), _claim("second", "02"))
    first = _case(record, claims, end="2020-08-31", name="first")
    second = _case(record, claims, start="2020-05-01", selected=1, name="second")
    result = _apply(record, claims, first, second)
    assert result == _apply(record, claims, second, first)
    assert [item.status for item in result.accounting] == ["conflicted", "conflicted"]
    coding = next(iter(result.coding.values()))
    assert [(s.valid_from, s.valid_to, s.version_label) for s in coding.segments] == [
        ("2020-01-01", "2020-04-30", "first"),
        ("2020-05-01", "2020-08-31", ""),
        ("2020-09-01", "2020-12-31", "second"),
    ]
    assert coding.issues[0].code == "conflicting_coding_choices"
    assert coding.issues[0].case_ids == ("first", "second")
    agreeing = _case(record, claims, start="2020-05-01", name="second")
    agreed = _apply(record, claims, first, agreeing)
    assert all(item.status == "applied" for item in agreed.accounting)
    assert next(iter(agreed.coding.values())).issues == ()


def test_incomplete_and_pooled_claims_remain_unresolved_without_annual_inference() -> (
    None
):
    record = _record()
    claims = (_claim("first", "01"), _claim("second", "02"))
    case = _case(record, claims)
    unknown = replace(claims[1], members=(replace(claims[1].members[0], label=None),))
    result = _apply(record, (claims[0], unknown), case)
    assert result.accounting[0].status == "stale"
    assert result.accounting[0].incomplete_claims == ("second",)
    pooled = replace(
        claims[1],
        claim_id="pooled",
        scope=TemporalScope(kind="pooled", label="2010-2020"),
    )
    result = _apply(record, (*claims, pooled), case)
    assert result.accounting[0].status == "applied"
    assert (
        next(iter(result.coding.values())).issues[0].code == "unsupported_coding_scope"
    )
    assert next(iter(result.coding.values())).claims[-1] == pooled


def test_selected_list_must_cover_whole_reviewed_period() -> None:
    record = _record()
    claims = (_claim("first", "01", end="2020-06-30"), _claim("second", "02"))
    result = _apply(record, claims, _case(record, claims))
    assert result.accounting[0].status == "stale"
    assert result.diagnostics[0].code == "incomplete_selected_coding"


def test_missing_binding_or_guard_is_fatal_not_a_content_waiver() -> None:
    record = _record()
    claims = (_claim("first", "01"),)
    case = _case(record, claims)
    with pytest.raises(ValueError, match="unconverted column"):
        apply_coding_choices((record,), (case,), coding={})
    with pytest.raises(ValueError, match="guarded original membership"):
        _apply(record, claims, case.model_copy(update={"peer_guards": ()}))


@pytest.mark.parametrize(
    "changes",
    [
        {"valid_from": "2020"},
        {"valid_to": "9999-12-31"},
        {"valid_to": "2019-12-31"},
        {"expected_codings": ()},
        {"selected_coding": "f" * 64},
        {"column_key": ()},
    ],
)
def test_choice_contract_requires_finite_scope_and_existing_selection(
    changes: dict,
) -> None:
    decision = _case(_record(), (_claim("first", "01"),)).decision.model_dump()
    with pytest.raises(ValidationError):
        CodingChoiceDecision.model_validate(decision | changes)
