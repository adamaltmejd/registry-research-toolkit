"""Accepted coding choices are finite, source-checked and order-independent."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from pydantic import ValidationError
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.curation_tree import RegisterCuration
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    coding_content_sha256,
    resolve_code_membership,
)
from reg_meta_build.source_coding_choices import apply_coding_choices, coding_for_period
from reg_meta_build.source_coordinates import column_identity, source_register_key
from reg_meta_build.source_curation import (
    CodingDecision,
    CodingSelection,
    CurationCase,
    PeerGuard,
    capture_expectations,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
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

from reg_meta_build.fqid_slugs import SlugEntry


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
        decision=CodingDecision(
            reviewed=True,
            column_key=column,
            valid_from=start,
            valid_to=end,
            expected_codings=tuple(sorted(set(digests))),
            selection=CodingSelection(
                valid_from=start,
                valid_to=end,
                expected_codings=tuple(sorted(set(digests))),
                selected_coding=digests[selected],
            ),
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


def _compile_entry(
    kind: str,
    values: dict,
    claims: tuple[CodeListClaim, ...],
    *,
    record: SourceRecord | None = None,
    split: bool = False,
):
    record = record or _record()
    register_key = source_register_key(record)
    occurrence = source_occurrence(record)
    assert register_key is not None
    assert occurrence.variable_key is not None and occurrence.variant_key is not None
    variable = (
        (*occurrence.variable_key, "accepted-partition", "1.5.part")
        if split
        else occurrence.variable_key
    )
    column = column_identity(variable, occurrence.variant_key, "VALUE")
    scope = CompiledScope(
        source=record.source,
        register_key=register_key,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register",
                    provider="scb",
                    source_key=register_key,
                ),
                naming=SlugEntry("register", "1", "sample", "scb"),
                contributors=(),
            ),
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register_variant",
                    provider="scb",
                    source_key=occurrence.variant_key,
                    register_key=register_key,
                ),
                naming=SlugEntry("register_variant", "1.2", "people", "scb"),
                contributors=(),
            ),
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="variable",
                    provider="scb",
                    source_key=variable,
                    register_key=register_key,
                ),
                naming=SlugEntry(
                    "variable", "1.5.part" if split else "1.5", "value", "scb"
                ),
                contributors=(),
            ),
        ),
    )
    register = RegisterCuration.model_validate(
        {
            "register": {"provider": "scb", "slug": "sample", "native_id": "1"},
            "coding": {
                kind: [
                    {
                        "variable": "1.5",
                        "variant": "people",
                        "column": "VALUE",
                        "periods": [["2020-01-01", "2020-12-31"]],
                        "reason": "Reviewed coding",
                        "source": "fixture",
                        **values,
                    }
                ]
            },
        }
    )
    register._source_file = "curation/registers/scb/sample.toml"
    columns = {column: (record,)}
    cases, diagnostics = compile_coding_register(
        register,
        scope,
        originals=(record,),
        columns=columns,
        coding={column: claims},
    )
    return cases, diagnostics, register, scope, columns, column


def test_pin_free_choice_compiles_and_applies_from_literal_labels() -> None:
    record = _record()
    claims = (_claim("keep", "01"), _claim("other", "02"))
    cases, diagnostics, _, _, _, _ = _compile_entry(
        "choice", {"keep": "keep", "over": ["other"]}, claims
    )
    assert len(cases) == 1 and not diagnostics
    assert cases[0].decision.expected_codings
    assert _apply(record, claims, *cases).accounting[0].status == "applied"
    assert "/coding.choice/1/period/1" in cases[0].case_id


def test_pin_free_uncoded_and_omit_compile_without_nonempty_lists() -> None:
    empty = replace(_claim("empty", "01"), members=())
    for kind, selection in (("uncoded", "uncoded"), ("omit", "omit_state")):
        cases, diagnostics, _, _, _, _ = _compile_entry(kind, {}, (empty,))
        assert not diagnostics and cases[0].decision.selection == selection
        assert _apply(_record(), (empty,), *cases).accounting[0].status == "applied"


def test_pin_free_extend_compiles_from_finite_witness() -> None:
    claims = (_claim("list", "01", "2020-05-01", "2020-06-30"),)
    cases, diagnostics, _, _, _, _ = _compile_entry(
        "extend",
        {
            "list": "list",
            "list_members": [["01", "Label"]],
            "witness": ["2020-05-01", "2020-06-30"],
            "periods": [["2020-01-01", "2020-02-29"]],
        },
        claims,
    )
    assert not diagnostics and len(cases) == 1
    assert _apply(_record(), claims, *cases).accounting[0].status == "applied"


def test_extend_list_only_complete_inside_short_witness() -> None:
    claim = _claim("list", "01")
    short_member = replace(
        claim.members[0],
        scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2020-05-01", end="2020-06-30"),),
        ),
    )
    cases, diagnostics, _, _, _, _ = _compile_entry(
        "extend",
        {
            "list": "list",
            "witness": ["2020-05-01", "2020-06-30"],
            "periods": [["2020-01-01", "2020-02-29"]],
        },
        (replace(claim, members=(short_member,)),),
    )
    assert not diagnostics and len(cases) == 1


def test_extend_ignores_incomplete_same_label_claim() -> None:
    witness = _claim("list", "01", "2020-05-01", "2020-06-30")
    incomplete = replace(
        _claim("list", "02", "2020-07-01", "2020-08-31"),
        members=(
            CodeMembershipClaim("02", "Label", TemporalScope(kind="year_independent")),
            CodeMembershipClaim("03", None, TemporalScope(kind="year_independent")),
        ),
    )
    cases, diagnostics, _, _, _, _ = _compile_entry(
        "extend",
        {
            "list": "list",
            "witness": ["2020-05-01", "2020-06-30"],
            "periods": [["2020-01-01", "2020-02-29"]],
        },
        (witness, incomplete),
    )
    assert not diagnostics and len(cases) == 1


@pytest.mark.parametrize(
    ("kind", "values", "claims", "code"),
    [
        (
            "choice",
            {"keep": "missing", "over": ["other"]},
            (_claim("keep", "01"), _claim("other", "02")),
            "stale_curation_entry",
        ),
        (
            "choice",
            {"keep": "keep", "over": ["gone"]},
            (_claim("keep", "01"), _claim("other", "02")),
            "stale_curation_entry",
        ),
        (
            "choice",
            {"keep": "keep", "over": ["other"]},
            (_claim("keep", "01"),),
            "stale_curation_entry",
        ),
        (
            "choice",
            {"keep": "keep", "keep_members": [["99", "Label"]], "over": ["other"]},
            (_claim("keep", "01"), _claim("other", "02")),
            "stale_curation_entry",
        ),
        (
            "choice",
            {"keep": "keep", "over": ["other"]},
            (_claim("keep", "01"), _claim("keep", "03"), _claim("other", "02")),
            "overbroad_curation_entry",
        ),
        ("uncoded", {}, (_claim("coded", "01"),), "stale_curation_entry"),
        ("omit", {}, (_claim("coded", "01"),), "stale_curation_entry"),
        (
            "extend",
            {"list": "gone", "witness": ["2020-05-01", "2020-06-30"]},
            (_claim("list", "01", "2020-05-01", "2020-06-30"),),
            "stale_curation_entry",
        ),
        (
            "extend",
            {"list": "list", "witness": ["2020-07-01", "2020-08-31"]},
            (_claim("list", "01", "2020-05-01", "2020-06-30"),),
            "stale_curation_entry",
        ),
        (
            "extend",
            {"list": "list", "witness": ["2020-05-01", "2020-06-30"]},
            (_claim("list", "01"),),
            "stale_curation_entry",
        ),
        (
            "extend",
            {
                "list": "list",
                "list_members": [["99", "Label"]],
                "witness": ["2020-05-01", "2020-06-30"],
            },
            (_claim("list", "01", "2020-05-01", "2020-06-30"),),
            "stale_curation_entry",
        ),
        (
            "extend",
            {"list": "list", "witness": ["2020-05-01", "2020-06-30"]},
            (
                _claim("list", "01", "2020-05-01", "2020-06-30"),
                _claim("list", "02", "2020-05-01", "2020-06-30"),
            ),
            "overbroad_curation_entry",
        ),
    ],
)
def test_pin_free_coding_staleness(kind, values, claims, code) -> None:
    cases, diagnostics, _, _, _, _ = _compile_entry(kind, values, claims)
    assert not cases and [item.code for item in diagnostics] == [code]


def test_pin_free_choice_member_disambiguation_and_split_key() -> None:
    claims = (_claim("keep", "01"), _claim("keep", "03"), _claim("other", "02"))
    cases, diagnostics, register, scope, _, column = _compile_entry(
        "choice",
        {"keep": "keep", "keep_members": [["01", "Label"]], "over": ["keep", "other"]},
        claims,
        split=True,
    )
    assert not diagnostics and cases[0].decision.column_key == column
    assert "accepted-partition" in column
    assert register.coding.choice[0].variable == "1.5"
    assert any(
        item.naming.source_id == "1.5.part"
        for item in scope.naming
        if item.target.kind == "variable"
    )


def test_coding_target_captures_sibling_projection_on_same_ref() -> None:
    record = _record()
    sibling = record.model_copy(
        update={
            "fields": record.fields.model_copy(
                update={"column_name": value_field("SIBLING")}
            )
        }
    )
    assert record_ref(record) == record_ref(sibling)
    _, _, register, scope, columns, column = _compile_entry(
        "uncoded", {}, (), record=record
    )
    cases, diagnostics = compile_coding_register(
        register,
        scope,
        originals=(record, sibling),
        columns=columns,
        coding={column: ()},
    )
    assert not diagnostics and len(cases) == 1
    assert len(cases[0].targets) == 1
    assert len(cases[0].targets[0].alternatives) == 2
    assert (
        apply_coding_choices((record, sibling), cases, coding={column: ()})
        .accounting[0]
        .status
        == "applied"
    )


def test_pin_free_coding_period_and_column_cardinality_are_checked() -> None:
    values = {"keep": "keep", "over": ["other"]}
    claims = (_claim("keep", "01"), _claim("other", "02"))
    cases, diagnostics, _, _, _, _ = _compile_entry(
        "choice", {**values, "periods": [["2021-01-01", "2021-12-31"]]}, claims
    )
    assert not cases and diagnostics[0].code == "stale_curation_entry"

    cases, diagnostics, _, _, _, _ = _compile_entry(
        "choice",
        {
            **values,
            "periods": [
                ["2020-01-01", "2020-12-31"],
                ["2021-01-01", "2021-12-31"],
            ],
        },
        claims,
    )
    assert len(cases) == 1 and len(diagnostics) == 1
    assert diagnostics[0].case_id.endswith("/period/2")
    cases, diagnostics, _, _, _, _ = _compile_entry(
        "choice", {**values, "column": "MISSING"}, claims
    )
    assert not cases and diagnostics[0].code == "stale_curation_entry"


def test_pin_free_coding_rejects_ambiguous_column_identity() -> None:
    record = _record()
    claims = (_claim("keep", "01"), _claim("other", "02"))
    _, _, register, scope, columns, column = _compile_entry(
        "choice", {"keep": "keep", "over": ["other"]}, claims
    )
    occurrence = source_occurrence(record)
    assert occurrence.variable_key is not None and occurrence.variant_key is not None
    split = (*occurrence.variable_key, "accepted-partition", "1.5.part")
    second = NamingDeclaration(
        target=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=split,
            register_key=source_register_key(record),
        ),
        naming=SlugEntry("variable", "1.5", "other", "scb"),
        contributors=(),
    )
    other_column = column_identity(split, occurrence.variant_key, "VALUE")
    ambiguous = scope.model_copy(update={"naming": (*scope.naming, second)})
    cases, diagnostics = compile_coding_register(
        register,
        ambiguous,
        originals=(record,),
        columns={other_column: (record,), column: columns[column]},
        coding={column: claims, other_column: claims},
    )
    assert not cases and diagnostics[0].code == "overbroad_curation_entry"


def test_pin_free_coding_compile_is_byte_identical() -> None:
    claims = (_claim("keep", "01"), _claim("other", "02"))
    args = ("choice", {"keep": "keep", "over": ["other"]}, claims)
    first = _compile_entry(*args)
    second = _compile_entry(*args)
    assert [case.model_dump_json() for case in first[0]] == [
        case.model_dump_json() for case in second[0]
    ]
    _, _, register, scope, columns, column = first
    unrelated = (*column[:-1], "OTHER")
    other_record = _record(year=2021, column="OTHER")
    shuffled, diagnostics = compile_coding_register(
        register,
        scope,
        originals=(*columns[column], other_record),
        columns={unrelated: (other_record,), column: columns[column]},
        coding={unrelated: (), column: claims},
    )
    assert not diagnostics
    assert [case.model_dump_json() for case in shuffled] == [
        case.model_dump_json() for case in first[0]
    ]


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
        {"expected_codings": ("bad-hash",)},
        {"selection": "guess"},
        {"column_key": ()},
    ],
)
def test_choice_contract_requires_finite_scope_and_existing_selection(
    changes: dict,
) -> None:
    decision = _case(_record(), (_claim("first", "01"),)).decision.model_dump()
    with pytest.raises(ValidationError):
        CodingDecision.model_validate(decision | changes)


def _assignment_case(
    record: SourceRecord,
    claims: tuple[CodeListClaim, ...],
    selection: CodingSelection | str,
    *,
    start: str = "2020-01-01",
    end: str = "2020-12-31",
    name: str = "assignment",
) -> CurationCase:
    from reg_meta_build.source_coding_choices import coding_expectations

    case = _case(record, (_claim("fixture", "0"),), name=name)
    assert isinstance(case.decision, CodingDecision)
    return case.model_copy(
        update={
            "decision": CodingDecision.model_validate(
                {
                    "reviewed": True,
                    "column_key": case.decision.column_key,
                    "valid_from": start,
                    "valid_to": end,
                    "expected_codings": coding_expectations(claims, start, end),
                    "selection": selection,
                    "reason": "Existing accepted finite coding correction",
                    "provenance": "accepted codeless decision",
                }
            )
        }
    )


def _witness(claims: tuple[CodeListClaim, ...]) -> CodingSelection:
    from reg_meta_build.source_coding_choices import coding_expectations

    start, end = "2020-05-01", "2020-06-30"
    digest = coding_content_sha256(coding_for_period(claims, start, end)[0])
    assert digest is not None
    return CodingSelection(
        valid_from=start,
        valid_to=end,
        expected_codings=coding_expectations(claims, start, end),
        selected_coding=digest,
    )


def test_extension_checks_separate_witness_and_changes_only_exact_target() -> None:
    record = _record()
    claims = (_claim("coding", "01", "2020-05-01", "2020-06-30"),)
    case = _assignment_case(record, claims, _witness(claims), end="2020-02-29")
    result = _apply(record, claims, case)
    assert result.accounting[0].status == "applied"
    resolved = next(iter(result.coding.values()))
    assert [(s.valid_from, s.valid_to) for s in resolved.segments] == [
        ("2020-01-01", "2020-02-29"),
        ("2020-05-01", "2020-06-30"),
    ]
    assert resolved.segments[0].code_set == resolved.segments[1].code_set
    assert resolved.segments[0].provenance and not resolved.segments[1].provenance
    assert resolved.claims == claims
    changed = (_claim("coding", "02", "2020-05-01", "2020-06-30"),)
    stale = _apply(record, changed, case)
    assert stale.accounting[0].status == "stale"
    assert stale.diagnostics[0].code == "coding_witness_changed"
    new_target_claim = _claim("new", "03", end="2020-02-29")
    stale = _apply(record, (*claims, new_target_claim), case)
    assert stale.diagnostics[0].code == "coding_evidence_changed"


def test_extension_rejects_varying_witness_but_same_period_selection_keeps_changes() -> (
    None
):
    record = _record()
    claim = _claim("coding", "01", "2020-05-01", "2020-06-30")
    claim = replace(
        claim,
        members=(
            replace(
                claim.members[0],
                scope=TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="2020-05-01", end="2020-05-31"),),
                ),
            ),
            CodeMembershipClaim(
                "02",
                "Label",
                TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="2020-06-01", end="2020-06-30"),),
                ),
            ),
        ),
    )
    claims = (claim,)
    witness = _witness(claims)
    extension = _assignment_case(record, claims, witness, end="2020-02-29")
    result = _apply(record, claims, extension)
    assert result.diagnostics[0].code == "nonconstant_coding_witness"
    same = _assignment_case(
        record, claims, witness, start="2020-05-01", end="2020-06-30"
    )
    result = _apply(record, claims, same)
    assert result.accounting[0].status == "applied"
    assert len(next(iter(result.coding.values())).segments) == 2


def test_exact_empty_evidence_can_be_accepted_as_uncoded_without_guessing_members() -> (
    None
):
    from reg_meta_build.source_coding_choices import coding_expectations

    record = _record()
    empty = replace(_claim("empty", "01"), members=())
    case = _assignment_case(record, (empty,), "uncoded", end="2020-06-30")
    result = _apply(record, (empty,), case)
    resolved = next(iter(result.coding.values()))
    assert result.accounting[0].status == "applied"
    assert resolved.segments[0].code_set is None and resolved.segments[0].provenance
    assert [(i.code, i.valid_from, i.valid_to) for i in resolved.issues] == [
        ("empty_active_coding", "2020-07-01", "2020-12-31"),
    ]
    unknown = replace(
        empty,
        members=(
            CodeMembershipClaim(
                None, "Label", TemporalScope(kind="unknown", label="unsupplied")
            ),
        ),
    )
    assert coding_expectations(
        (empty,), "2020-01-01", "2020-06-30"
    ) != coding_expectations((unknown,), "2020-01-01", "2020-06-30")
    assert _apply(record, (unknown,), case).accounting[0].status == "stale"
    assert _apply(record, (), case).accounting[0].status == "stale"


def test_cap_can_select_complete_coding_among_exact_known_empty_competitors() -> None:
    record = _record()
    claims = (_claim("coding", "01"), replace(_claim("empty", "0"), members=()))
    selection = _witness(claims)
    case = _assignment_case(
        record, claims, selection, start="2020-05-01", end="2020-06-30"
    )
    result = _apply(record, claims, case)
    resolved = next(iter(result.coding.values()))
    assert result.accounting[0].status == "applied"
    assert resolved.segments[1].code_set is not None
    assert [(i.valid_from, i.valid_to) for i in resolved.issues] == [
        ("2020-01-01", "2020-04-30"),
        ("2020-07-01", "2020-12-31"),
    ]


def test_incomplete_expectations_ignore_order_duplicates_and_inactive_members() -> None:
    from reg_meta_build.source_coding import coding_observation_sha256

    base = replace(
        _claim("unknown", "01"),
        members=(
            CodeMembershipClaim("01", None, TemporalScope(kind="year_independent")),
            CodeMembershipClaim(
                "02", "Label", TemporalScope(kind="unknown", label="unclear")
            ),
        ),
    )
    duplicate = replace(
        base,
        claim_id="new-layout",
        members=(base.members[1], base.members[0], base.members[1]),
    )
    assert coding_observation_sha256(base) == coding_observation_sha256(duplicate)
    changed = replace(
        base, members=(replace(base.members[0], code="1"), base.members[1])
    )
    assert coding_observation_sha256(base) != coding_observation_sha256(changed)
    future = CodeMembershipClaim(
        "future",
        "Label",
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start="2021", end="2021"),)
        ),
    )
    assert coding_observation_sha256(base) == coding_observation_sha256(
        replace(base, members=(*base.members, future))
    )


def test_unresolved_scope_fingerprint_ignores_new_unset_optional_field() -> None:
    from reg_meta_build.source_coding import coding_observation_sha256

    class _ExtendedScope(TemporalScope):
        extra_note: str | None = None

    base = CodeListClaim(
        "unknown",
        TemporalScope(kind="unknown", label="unclear"),
        (
            CodeMembershipClaim(
                "01", None, TemporalScope(kind="unknown", label="unclear")
            ),
        ),
        version_label="unknown",
    )
    extended = CodeListClaim(
        "unknown",
        _ExtendedScope(kind="unknown", label="unclear", extra_note=None),
        (
            CodeMembershipClaim(
                "01", None, _ExtendedScope(kind="unknown", label="unclear")
            ),
        ),
        version_label="unknown",
    )
    assert base.scope.model_dump(mode="json") != extended.scope.model_dump(mode="json")
    assert coding_observation_sha256(base) == coding_observation_sha256(extended)


def test_unresolved_scope_fingerprint_keeps_pooled_bounds() -> None:
    from reg_meta_build.source_coding import coding_observation_sha256

    def _pooled(start: str) -> CodeListClaim:
        return CodeListClaim(
            "pooled",
            TemporalScope(
                kind="pooled",
                label="2010-2020",
                pooled_start=start,
                pooled_end="2020-12-31",
            ),
            (
                CodeMembershipClaim(
                    "01", "Label", TemporalScope(kind="year_independent")
                ),
            ),
            version_label="pooled",
        )

    assert coding_observation_sha256(
        _pooled("2010-01-01")
    ) != coding_observation_sha256(_pooled("2011-01-01"))


def test_omission_retains_evidence_and_withholds_conflicting_overlap() -> None:
    record = _record()
    claims = (_claim("coding", "01"),)
    omit = _assignment_case(record, claims, "omit_state", end="2020-08-31", name="omit")
    include = _assignment_case(
        record, claims, "uncoded", start="2020-05-01", name="include"
    )
    result = _apply(record, claims, omit, include)
    assert result == _apply(record, claims, include, omit)
    resolved = next(iter(result.coding.values()))
    assert [s.state_disposition for s in resolved.segments] == [
        "omit",
        "withhold",
        "include",
    ]
    assert resolved.issues[0].withheld == "state"
    assert resolved.issues[0].valid_from == "2020-05-01"
    assert resolved.issues[0].valid_to == "2020-08-31"
    assert resolved.claims == claims


def test_omission_is_not_a_negative_availability_fact_or_a_new_formation_error() -> (
    None
):
    record = _record()
    claims = (_claim("coding", "01"),)
    omit = _assignment_case(record, claims, "omit_state")
    coding = _apply(record, claims, omit)
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
        coding=coding.coding,
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
    )
    assert formed.variable is None and formed.occurrences == (record,)
    assert all(d.severity == "warning" for d in formed.diagnostics)
    assert any(d.code == "curated_state_omission" for d in formed.diagnostics)
    assert formed.intervals[0].negative_segments == ()


def test_undated_coding_is_not_consumed_by_finite_uncoded_acceptance() -> None:
    record = _record()
    pooled = replace(
        _claim("pooled", "01"), scope=TemporalScope(kind="pooled", label="2010-2020")
    )
    case = _assignment_case(record, (pooled,), "uncoded")
    resolved = next(iter(_apply(record, (pooled,), case).coding.values()))
    assert [i.code for i in resolved.issues] == ["unsupported_coding_scope"]
    assert resolved.claims == (pooled,)


def _documented_values(members=None):
    return {
        "members": members
        if members is not None
        else [["1", "ja"], ["", "inte tillfrågad"]],
        "version_label": "Official 2020 questionnaire",
        "document_url": "https://example.org/official.pdf",
        "document_sha256": "a" * 64,
        "document_pages": [12],
    }


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
