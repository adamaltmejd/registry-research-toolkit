"""Accepted coding choices are finite, source-checked and order-independent."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import REGISTERINFORMATION_HEADER, _var_row
from pydantic import ValidationError
from reg_meta.source_evidence import SourceRevision
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
from reg_meta_build.source_coordinates import (
    NativeKey,
    column_identity,
    source_register_key,
)
from reg_meta_build.source_curation import (
    CodingDecision,
    CodingSelection,
    CurationCase,
    PeerGuard,
    SourceEvidence,
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
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from collections.abc import Mapping


def _column_scopes(
    columns: Mapping[NativeKey, tuple[SourceRecord, ...]],
) -> dict[NativeKey, frozenset[TemporalScope]]:
    return {
        key: frozenset(
            record.edition_period_scope
            if record.edition_period_scope.kind != "not_applicable"
            else record.edition_scope
            for record in records
        )
        for key, records in columns.items()
    }


def _record(
    year: int = 2020, column: str = "VALUE", *, variable: int = 5
) -> SourceRecord:
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
        var_id=variable,
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
        column_scopes=_column_scopes(columns),
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


def test_choice_preserves_documented_blank_sentinel_and_checks_complete_members():
    ordinary = _claim("same label", "1")
    superset = replace(
        ordinary,
        claim_id="superset",
        members=ordinary.members
        + (
            CodeMembershipClaim(
                "", "Not applicable", TemporalScope(kind="year_independent")
            ),
        ),
    )
    claims = (ordinary, superset)
    cases, diagnostics, *_ = _compile_entry(
        "choice",
        {
            "keep": "same label",
            "keep_members": [["1", "Label"], ["", "Not applicable"]],
            "over": ["same label"],
        },
        claims,
    )
    assert not diagnostics
    result = _apply(_record(), claims, *cases)
    assert result.accounting[0].status == "applied"
    assert set(
        result.coding[next(iter(result.coding))].segments[0].code_set.members
    ) == {
        ("1", "Label"),
        ("", "Not applicable"),
    }
    changed = replace(superset, members=superset.members + _claim("extra", "2").members)
    assert (
        _apply(_record(), (ordinary, changed), *cases).accounting[0].status == "stale"
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
        column_scopes=_column_scopes(columns),
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
        column_scopes=_column_scopes(
            {other_column: (record,), column: columns[column]}
        ),
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
        column_scopes=_column_scopes(
            {unrelated: (other_record,), column: columns[column]}
        ),
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


def test_effective_column_peer_index_matches_full_scan_and_preserves_exclusions():
    from reg_meta_build.source_curation import _peer_matches

    original = _record()
    anchor = _record(column="ANCHOR", variable=6)
    excluded = _record(year=2021)
    unmapped = _record(year=2022)
    foreign = _record(year=2023).model_copy(update={"source": "other-source"})
    occurrence = replace(
        source_occurrence(original), source_records=(original, anchor, foreign)
    )
    column = occurrence.column_key
    assert column is not None
    evidence = SourceEvidence(
        (original, original, anchor, excluded, unmapped, foreign),
        effective_occurrences=(
            occurrence,
            replace(source_occurrence(excluded), use="support"),
        ),
    )
    guard = PeerGuard(
        guard_id="indexed",
        source=original.source,
        effective_column=column,
        expected_members=(
            record_ref(original),
            record_ref(anchor),
            record_ref(unmapped),
        ),
    )
    assert "effective_column_records" not in evidence.__dict__
    expected = tuple(
        r
        for r in evidence.records
        if _peer_matches(r, guard) and evidence._effective_column_matches(r, column)
    )
    assert (
        tuple(evidence.peers(guard))
        == expected
        == (original, original, anchor, unmapped)
    )
    assert "effective_column_records" in evidence.__dict__
    assert tuple(evidence.peers(guard)) == expected
    # A new immutable slice with the anchor removed from the binding cannot
    # accidentally reuse the previous effective-column cache.
    removed = SourceEvidence(
        evidence.records,
        effective_occurrences=(
            replace(occurrence, source_records=(original, foreign)),
            replace(source_occurrence(anchor), use="support"),
            replace(source_occurrence(excluded), use="support"),
        ),
    )
    assert tuple(removed.peers(guard)) == (original, original, unmapped)


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


@pytest.mark.parametrize("accepted", [False, True])
def test_independent_coding_retains_base_and_rejects_dated_choices(accepted):
    record = _record()
    independent = replace(
        _claim("independent", "01"), scope=TemporalScope(kind="year_independent")
    )
    cases = (_assignment_case(record, (independent,), "uncoded"),) if accepted else ()
    result = _apply(record, (independent,), *cases)
    resolved = next(iter(result.coding.values()))
    assert resolved == resolve_code_membership((independent,))
    assert resolved.segments[0].period_scope == "year_independent"
    assert resolved.segments[0].valid_from is resolved.segments[0].valid_to is None
    if accepted:
        assert result.accounting[0].status == "stale"
        assert result.diagnostics[0].code == "coding_scope_changed"
    else:
        assert not result.diagnostics


def _row_authority(record, claims, source_scope=None):
    from reg_meta_build.curation_tree import PreparedCodingAuthority
    from reg_meta_build.source_coding import copied_coding_fingerprints

    revision = SourceRevision.create(
        dataset="scb-fixture",
        publisher="SCB",
        purpose="coding fixture",
        upstream_revision="1",
        artifact_path="records.csv",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )
    return PreparedCodingAuthority(
        revision=revision,
        source_scope=source_scope,
        locators=list(record.locators),
        records=list(
            capture_expectations(
                (record,),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        codings=list(copied_coding_fingerprints(claims)),
    )


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "prose",
        "scope",
        "revision",
        "coding",
        "label",
        "peer",
        "missing",
        "locator",
        "incomplete_authority",
        "mixed_authority",
        "pdf_open",
        "pdf_missing_page",
        "added_documented_code",
    ],
)
def test_documented_source_rows_guard_fresh_compile_and_replay(drift):
    record = _record()
    claims = (
        replace(
            _claim("source", "1"),
            members=(
                CodeMembershipClaim("1", None, TemporalScope(kind="year_independent")),
            ),
        ),
    )
    authority = _row_authority(record, claims)
    values = {
        "members": [["1", "Included"]],
        "version_label": "Supplied row",
        "source_authority": authority,
    }
    if drift in {
        "incomplete_authority",
        "mixed_authority",
        "pdf_open",
        "pdf_missing_page",
        "added_documented_code",
    }:
        if drift == "incomplete_authority":
            raw = authority.model_dump(mode="json")
            raw["records"][0]["alternatives"][0]["fields"].pop()
            values["source_authority"] = raw
        elif drift == "mixed_authority":
            values.update(_documented_values())
        else:
            values = _documented_values()
            if drift == "pdf_open":
                values["periods"] = [["2020-01-01", "9999-12-31"]]
            else:
                values.pop("document_pages")
        with pytest.raises(ValidationError):
            _compile_entry("documented", values, claims)
        return
    cases, issues, register, scope, _, column = _compile_entry(
        "documented", values, claims
    )
    assert len(cases) == 1 and not issues
    changed = record
    changed_claims = claims
    records = (record,)
    if drift == "prose":
        changed = record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"description": value_field("Changed")}
                )
            }
        )
    elif drift == "scope":
        changed = record.model_copy(
            update={"edition_scope": TemporalScope(kind="unknown", label="Changed")}
        )
    elif drift == "revision":
        changed = record.model_copy(update={"source_revision_id": "changed"})
    elif drift == "coding":
        changed_claims = (
            replace(
                claims[0],
                members=(
                    *claims[0].members,
                    CodeMembershipClaim(
                        "2", None, TemporalScope(kind="year_independent")
                    ),
                ),
            ),
        )
    elif drift == "label":
        changed_claims = (
            replace(
                claims[0],
                members=(replace(claims[0].members[0], label="Contradictory"),),
            ),
        )
    elif drift == "locator":
        changed = record.model_copy(
            update={
                "locators": (
                    record.locators[0].model_copy(
                        update={"physical_record": "changed"}
                    ),
                )
            }
        )
    elif drift == "missing":
        records = ()
    elif drift == "peer":
        records = (record, _record(2021))
    if drift not in {"peer", "missing"}:
        records = (changed,)
    if drift == "added_documented_code":
        entry = register.coding.documented[0].model_copy(
            update={"members": (("1", "Included"), ("0", "Absent invented token"))}
        )
        register = register.model_copy(
            update={
                "coding": register.coding.model_copy(update={"documented": [entry]})
            }
        )
    fresh, diagnostics = compile_coding_register(
        register,
        scope,
        originals=records,
        columns={column: records},
        column_scopes=_column_scopes({column: records}),
        coding={column: changed_claims},
    )
    if drift is None:
        assert fresh == cases and not diagnostics
    else:
        assert not fresh and diagnostics[0].code == "stale_curation_entry"
    replay = apply_coding_choices(records, cases, coding={column: changed_claims})
    if drift in {"prose", "scope", "coding", "label", "peer", "missing"}:
        assert replay.accounting[0].status == "stale"
    elif drift is None:
        assert replay.accounting[0].status == "applied"
        assert replay.coding[column].claims == claims
        code_set = replay.coding[column].segments[0].code_set
        assert code_set is not None
        assert code_set.members == (("1", "Included"),)
        assert "records.csv" in replay.coding[column].segments[0].provenance[0]


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "enlarge",
        "shorten",
        "close",
        "wrong_claim",
        "prose",
        "removed_delivery",
        "mixed_periods",
        "unknown",
        "literal_9999",
    ],
)
def test_source_row_coding_follows_exact_supplied_open_scope(drift):
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="1900", end=None),)
    )
    record = _record().model_copy(
        update={"edition_scope": scope, "edition_period_scope": scope}
    )
    claims = (
        replace(
            _claim("source", "J"),
            scope=scope,
            members=(
                CodeMembershipClaim("J", None, TemporalScope(kind="year_independent")),
                CodeMembershipClaim("N", None, TemporalScope(kind="year_independent")),
            ),
        ),
    )
    authority = _row_authority(record, claims, scope)
    values = {
        "members": [["J", "ja"], ["N", "nej"]],
        "version_label": "Supplied J/N",
        "periods": [],
        "source_authority": authority,
    }
    if drift in {"mixed_periods", "unknown", "literal_9999"}:
        if drift == "mixed_periods":
            values["periods"] = [["1900-01-01", "2000-12-31"]]
        else:
            raw = authority.model_dump(mode="json")
            raw["source_scope"] = (
                TemporalScope(kind="unknown", label="unknown").model_dump(mode="json")
                if drift == "unknown"
                else TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="1900", end="9999-12-31"),),
                ).model_dump(mode="json")
            )
            values["source_authority"] = raw
        with pytest.raises(ValidationError):
            _compile_entry("documented", values, claims, record=record)
        return
    cases, issues, register, compiled_scope, columns, column = _compile_entry(
        "documented", values, claims, record=record
    )
    assert len(cases) == 1 and not issues
    case = cases[0]
    assert case.decision.selection.source_scope == scope
    assert case.decision.selection.source_scope.intervals[0].end is None
    assert case.decision.valid_to == "9999-12-31"
    actual_record = record
    actual_claims = claims
    actual_scopes = _column_scopes(columns)
    if drift in {"enlarge", "shorten", "close"}:
        changed_scope = TemporalScope(
            kind="intervals",
            intervals=(
                ScopeInterval(
                    start="1899"
                    if drift == "enlarge"
                    else "1901"
                    if drift == "shorten"
                    else "1900",
                    end="2000" if drift == "close" else None,
                ),
            ),
        )
        actual_record = record.model_copy(
            update={
                "edition_scope": changed_scope,
                "edition_period_scope": changed_scope,
            }
        )
        actual_scopes = {column: frozenset((changed_scope,))}
    elif drift == "wrong_claim":
        actual_claims = (
            replace(
                claims[0],
                scope=TemporalScope(
                    kind="intervals", intervals=(ScopeInterval(start="1901", end=None),)
                ),
            ),
        )
    elif drift == "prose":
        actual_record = record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"description": value_field("changed")}
                )
            }
        )
    elif drift == "removed_delivery":
        actual_scopes = {column: frozenset()}
    fresh, diagnostics = compile_coding_register(
        register,
        compiled_scope,
        originals=(actual_record,),
        columns={column: (actual_record,)},
        column_scopes=actual_scopes,
        coding={column: actual_claims},
    )
    if drift is None:
        assert fresh == cases and not diagnostics
    else:
        assert not fresh and diagnostics
    evidence = SourceEvidence(
        (actual_record,),
        effective_occurrences=()
        if drift == "removed_delivery"
        else (source_occurrence(actual_record),),
    )
    result = apply_coding_choices(evidence, cases, coding={column: actual_claims})
    if drift is None:
        resolved = result.coding[column]
        assert resolved.claims == claims
        (segment,) = resolved.segments
        assert segment.code_set is not None
        assert segment.code_set.members == (("J", "ja"), ("N", "nej"))
        assert segment.valid_from == "1900-01-01" and segment.valid_to == "9999-12-31"
        assert resolved.claims[0].scope.intervals[0].end is None
    else:
        assert result.accounting[0].status == "stale"


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "prose",
        "partial",
        "binding",
        "missing_binding",
        "outside_labels",
        "finite",
        "peer",
        "missing",
        "scope",
    ],
)
def test_enumerated_source_meanings_require_complete_marker_certificate(drift):
    from reg_meta_build.source_value_bindings import (
        ValueListBinding,
        marker_binding_fingerprints,
    )
    from reg_meta_build.source_values import SourceValueAssociation

    record = _record().model_copy(
        update={
            "fields": _record().fields.model_copy(
                update={
                    "definition": value_field(
                        "Which amount?\n1. Less than 500\n2. At least 500\n8. Unknown"
                    )
                }
            )
        }
    )
    scope = record.edition_scope
    marker = SourceValueAssociation(1, "Tal", "marker", "codes.csv")
    binding = ValueListBinding(
        None, record.record_id, record.locators, "revision", "Tal", 1, (), (marker,)
    )
    bound = ((scope, binding),)
    fingerprints = marker_binding_fingerprints(bound, "2020-01-01", "2020-12-31")
    assert fingerprints
    authority = _row_authority(record, (_claim("irrelevant", "1"),)).model_dump(
        mode="json"
    )
    base_claims = (
        (_claim("later", "1", "2021-01-01", "2021-12-31"),)
        if drift == "outside_labels"
        else ()
    )
    from reg_meta_build.source_coding import copied_coding_fingerprints

    authority.update(
        codings=list(copied_coding_fingerprints(base_claims)),
        enumeration={
            "field": "definition",
            "syntax": "ascii-decimal-dot-space",
            "lines": ["1. Less than 500", "2. At least 500", "8. Unknown"],
        },
        marker_bindings=list(fingerprints),
    )
    values = {
        "members": [["1", "Less than 500"], ["2", "At least 500"], ["8", "Unknown"]],
        "version_label": "Exact enumerated source",
        "source_authority": authority,
    }
    if drift == "partial":
        values["members"].pop()
        authority["enumeration"]["lines"].pop()
    # Missing binding evidence must never act as a compatibility bypass.
    cases, issues, register, compiled_scope, _, column = _compile_entry(
        "documented", values, base_claims, record=record
    )
    assert not cases and issues
    cases, issues = compile_coding_register(
        register,
        compiled_scope,
        originals=(record,),
        columns={column: (record,)},
        column_scopes=_column_scopes({column: (record,)}),
        coding={column: base_claims},
        value_bindings={column: bound},
    )
    if drift == "partial":
        assert not cases and issues
        return
    assert len(cases) == 1 and not issues
    records, changed_bound, claims = (record,), bound, base_claims
    if drift == "prose":
        records = (
            record.model_copy(
                update={
                    "fields": record.fields.model_copy(
                        update={"definition": value_field("Changed")}
                    )
                }
            ),
        )
    elif drift == "binding":
        changed_bound = ((scope, replace(binding, descriptor_key="Changed")),)
    elif drift == "missing_binding":
        changed_bound = ()
    elif drift == "finite":
        changed_bound = ((scope, replace(binding, claim_id="new finite")),)
        claims = (_claim("new", "1"),)
    elif drift == "peer":
        records += (_record(2021),)
    elif drift == "missing":
        records = ()
    elif drift == "scope":
        changed_bound = ((TemporalScope(kind="unknown", label="changed"), binding),)
    fresh, issues = compile_coding_register(
        register,
        compiled_scope,
        originals=records,
        columns={column: records},
        column_scopes=_column_scopes({column: records}),
        coding={column: claims},
        value_bindings={column: changed_bound},
    )
    evidence = SourceEvidence(
        records,
        effective_occurrences=tuple(source_occurrence(r) for r in records),
        value_bindings={column: changed_bound},
    )
    replay = apply_coding_choices(evidence, cases, coding={column: claims})
    if drift in {None, "outside_labels"}:
        assert fresh == cases and not issues
        assert replay.accounting[0].status == "applied"
        assert replay.coding[column].claims == base_claims
        code_set = replay.coding[column].segments[0].code_set
        assert code_set is not None
        assert code_set.members == tuple(map(tuple, values["members"]))
    else:
        assert not fresh and issues
        assert replay.accounting[0].status == "stale"


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "prose",
        "parent",
        "multiplicity",
        "locator",
        "label",
        "token",
        "validity",
        "peer",
        "removed",
        "removed_assertion",
        "column",
        "scope",
    ],
)
@pytest.mark.parametrize("kind", ["choice", "extend"])
def test_guarded_selection_pins_complete_physical_source_authority(drift, kind):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_values import SourceValueAssociation

    record = _record()
    association = SourceValueAssociation(1, "binary", "1", "codes.csv")
    narrow = _claim("narrow", "1")
    keeper = replace(
        (
            _claim("keeper", "1")
            if kind == "choice"
            else _claim("keeper", "1", "2021-01-01", "2021-12-31")
        ),
        members=(
            replace(narrow.members[0], associations=(association,)),
            CodeMembershipClaim(
                ".",
                "Skip",
                TemporalScope(kind="year_independent"),
                associations=(replace(association, row_number=2, value_key="."),),
            ),
        ),
    )
    claims = (narrow, keeper) if kind == "choice" else (keeper,)
    authority = _row_authority(record, claims).model_copy(
        update={"raw_codings": sorted({coding_source_sha256(c) for c in claims})}
    )
    values = (
        {
            "keep": "keeper",
            "keep_members": [["1", "Label"], [".", "Skip"]],
            "over": ["narrow"],
        }
        if kind == "choice"
        else {
            "list": "keeper",
            "list_members": [["1", "Label"], [".", "Skip"]],
            "witness": ["2021-01-01", "2021-12-31"],
        }
    )
    values["source_authority"] = authority
    cases, issues, register, scope, _, column = _compile_entry(kind, values, claims)
    assert len(cases) == 1 and not issues
    assert cases[0].targets == tuple(authority.records)
    records, changed_claims = (record,), claims
    if drift == "prose":
        records = (
            record.model_copy(
                update={
                    "fields": record.fields.model_copy(
                        update={"definition": value_field("Changed")}
                    )
                }
            ),
        )
    elif drift == "parent":
        assert record.parent_facts
        records = (record.model_copy(update={"parent_facts": ()}),)
    elif drift == "peer":
        records += (_record(2021),)
    elif drift == "column":
        records = (
            record.model_copy(
                update={
                    "fields": record.fields.model_copy(
                        update={"column_name": value_field("Other")}
                    )
                }
            ),
        )
    elif drift == "scope":
        records = (
            record.model_copy(
                update={"edition_scope": TemporalScope(kind="unknown", label="Changed")}
            ),
        )
    elif drift == "removed":
        changed_claims = (keeper,) if kind == "choice" else ()
    elif drift is not None:
        member = keeper.members[0]
        if drift == "multiplicity":
            member = replace(member, associations=(association, association))
        elif drift == "removed_assertion":
            member = replace(member, associations=())
        elif drift == "locator":
            member = replace(member, associations=(replace(association, row_number=3),))
        elif drift == "label":
            member = replace(member, label="Changed")
        elif drift == "token":
            member = replace(member, code="2")
        elif drift == "validity":
            member = replace(
                member,
                scope=TemporalScope(
                    kind="intervals",
                    intervals=(ScopeInterval(start="2020-05-01", end="2020-12-31"),),
                ),
            )
        changed = replace(keeper, members=(member, keeper.members[1]))
        changed_claims = (narrow, changed) if kind == "choice" else (changed,)
    fresh, issues = compile_coding_register(
        register,
        scope,
        originals=records,
        columns={column: records},
        column_scopes=_column_scopes({column: records}),
        coding={column: changed_claims},
    )
    replay = apply_coding_choices(
        SourceEvidence(
            records, effective_occurrences=tuple(source_occurrence(r) for r in records)
        ),
        cases,
        coding={column: changed_claims},
    )
    if drift is None:
        assert fresh == cases and not issues
        assert replay.accounting[0].status == "applied"
        assert replay.coding[column].claims == claims
        code_set = replay.coding[column].segments[0].code_set
        assert code_set is not None
        assert set(code_set.members) == {
            ("1", "Label"),
            (".", "Skip"),
        }
    else:
        assert not fresh and issues
        assert replay.accounting[0].status == "stale"


@pytest.mark.parametrize("kind", ["choice", "documented"])
def test_prepared_coding_authority_materializes_all_original_projection_alternatives(
    kind,
):
    from reg_meta_build.source_coding import coding_source_sha256

    record = _record()
    alternative = record.model_copy(
        update={
            "fields": record.fields.model_copy(
                update={"data_length": value_field("10")}
            ),
            "locators": (
                record.locators[0].model_copy(update={"physical_record": "second"}),
            ),
        }
    )
    claims = (
        (_claim("keep", "1"), _claim("other", "2"))
        if kind == "choice"
        else (
            replace(
                _claim("source", "1"),
                members=(
                    CodeMembershipClaim(
                        "1", None, TemporalScope(kind="year_independent")
                    ),
                ),
            ),
        )
    )
    authority = _row_authority(record, claims).model_copy(
        update={
            "records": list(
                capture_expectations(
                    (record, alternative),
                    fields=tuple(SourceFields.model_fields),
                    parents=True,
                    coding=True,
                )
            ),
            "locators": [*record.locators, *alternative.locators],
            "raw_codings": sorted({coding_source_sha256(c) for c in claims})
            if kind == "choice"
            else None,
        }
    )
    values = (
        {"keep": "keep", "over": ["other"]}
        if kind == "choice"
        else {"members": [["1", "Included"]], "version_label": "Supplied row"}
    )
    _, _, register, scope, columns, column = _compile_entry(
        kind,
        {**values, "source_authority": authority},
        claims,
    )
    # Production column membership contains one representative per semantic ref.
    assert columns[column] == (record,)
    cases, issues = compile_coding_register(
        register,
        scope,
        originals=(record, alternative),
        columns=columns,
        column_scopes=_column_scopes(columns),
        coding={column: claims},
    )
    assert len(cases) == 1 and not issues
    assert cases[0].targets == tuple(authority.records)
    replay = apply_coding_choices(
        SourceEvidence(
            (record, alternative),
            effective_occurrences=(
                source_occurrence(record),
                source_occurrence(alternative),
            ),
        ),
        cases,
        coding={column: claims},
    )
    assert replay.accounting[0].status == "applied"
    changed = alternative.model_copy(
        update={
            "fields": alternative.fields.model_copy(
                update={"data_length": value_field("11")}
            )
        }
    )
    fresh, issues = compile_coding_register(
        register,
        scope,
        originals=(record, changed),
        columns=columns,
        column_scopes=_column_scopes(columns),
        coding={column: claims},
    )
    assert not fresh and issues
    replay = apply_coding_choices(
        SourceEvidence(
            (record, changed),
            effective_occurrences=(
                source_occurrence(record),
                source_occurrence(changed),
            ),
        ),
        cases,
        coding={column: claims},
    )
    assert replay.accounting[0].status == "stale"


@pytest.mark.parametrize("missing", [None, "authority", "members", "raw_codings"])
def test_unlabelled_extension_requires_complete_literal_source_authority(missing):
    from reg_meta_build.source_coding import coding_source_sha256

    record = _record()
    claim = replace(
        _claim("source", "1", "2021-01-01", "2021-12-31"), version_label=None
    )
    authority = _row_authority(record, (claim,)).model_copy(
        update={"raw_codings": [coding_source_sha256(claim)]}
    )
    values = {
        "list": "",
        "list_members": [["1", "Label"]],
        "witness": ["2021-01-01", "2021-12-31"],
        "source_authority": authority,
    }
    if missing == "authority":
        values.pop("source_authority")
    elif missing == "members":
        values.pop("list_members")
    elif missing == "raw_codings":
        values["source_authority"] = authority.model_copy(update={"raw_codings": None})
    if missing is not None:
        with pytest.raises(ValueError):
            _compile_entry("extend", values, (claim,))
        return
    cases, issues, _, _, _, column = _compile_entry("extend", values, (claim,))
    assert len(cases) == 1 and not issues
    result = apply_coding_choices((record,), cases, coding={column: (claim,)})
    assert not result.diagnostics
    assert result.coding[column].claims == (claim,)
    segment = result.coding[column].segments[0]
    assert segment.code_set is not None
    assert segment.code_set.members == (("1", "Label"),)
    assert segment.version_label == ""


def test_guarded_extend_uses_held_column_owner_without_anchor_coding():
    from reg_meta_build.source_coding import coding_source_sha256

    donor, anchor = _record(2021), _record(2020, "ANCHOR", variable=6)
    claim = _claim("binary", "1", "2021-01-01", "2021-12-31")
    _, _, register, scope, _, column = _compile_entry(
        "extend",
        {"list": "binary", "witness": ["2021-01-01", "2021-12-31"]},
        (claim,),
        record=donor,
    )
    authority = _row_authority(donor, (claim,)).model_copy(
        update={
            "records": list(
                capture_expectations(
                    (donor, anchor),
                    fields=tuple(SourceFields.model_fields),
                    parents=True,
                    coding=True,
                )
            ),
            "locators": [*donor.locators, *anchor.locators],
            "raw_codings": [coding_source_sha256(claim)],
        }
    )
    entry = register.coding.extend[0].model_copy(update={"source_authority": authority})
    register = register.model_copy(
        update={"coding": register.coding.model_copy(update={"extend": [entry]})}
    )
    held = replace(
        source_occurrence(anchor),
        variable_key=source_occurrence(donor).variable_key,
        fields=SourceFields(column_name=value_field("VALUE")),
        source_records=(),
        support_records=(anchor,),
        coding_records=(),
        occurrence_key="accepted-holding",
    )
    occurrences = (source_occurrence(donor), held)
    evidence = SourceEvidence((donor, anchor), effective_occurrences=occurrences)
    assert evidence.effective_scopes is not None
    cases, issues = compile_coding_register(
        register,
        scope,
        originals=(donor, anchor),
        columns={column: (donor, anchor)},
        column_scopes=evidence.effective_scopes,
        coding={column: (claim,)},
    )
    assert len(cases) == 1 and not issues
    result = apply_coding_choices(evidence, cases, coding={column: (claim,)})
    assert not result.diagnostics
    assert result.coding[column].claims == (claim,)
    code_set = result.coding[column].segments[0].code_set
    assert code_set is not None and code_set.members == (("1", "Label"),)
    assert held.variable_key == source_occurrence(donor).variable_key
    assert held.coding_records == ()
    changed = anchor.model_copy(update={"parent_facts": ()})
    assert apply_coding_choices(
        SourceEvidence((donor, changed), effective_occurrences=occurrences),
        cases,
        coding={column: (claim,)},
    ).diagnostics


@pytest.mark.parametrize(
    "drift", [None, "label", "missing", "code", "association", "book", "historical"]
)
def test_documented_exact_label_equivalence_retains_raw_claims(drift):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import CodeLabelEquivalence
    from reg_meta_build.source_values import SourceValueAssociation

    record = _record()
    association = SourceValueAssociation(1, "descriptor", "value", "values.csv")
    claim = replace(
        _claim("Sector", "14", "2019-01-01", "2019-12-31"),
        members=(
            CodeMembershipClaim(
                "14", "Landsting", TemporalScope(kind="year_independent")
            ),
            CodeMembershipClaim(
                "14",
                "Region",
                TemporalScope(kind="year_independent"),
                associations=(association,),
            ),
            CodeMembershipClaim("15", "Other", TemporalScope(kind="year_independent")),
        ),
    )
    historical = _claim("Old sector", "29", "1968-01-01", "1968-12-31")
    claims = (claim, historical)
    authority = _row_authority(record, claims).model_copy(
        update={
            "raw_codings": [coding_source_sha256(c) for c in claims],
            "label_equivalences": [
                CodeLabelEquivalence(
                    code="14",
                    labels=("Landsting", "Region"),
                    selected_label="Landsting",
                )
            ],
        }
    )
    values = {
        "members": [["14", "Landsting"], ["15", "Other"]],
        "version_label": "Sector",
        "source_authority": authority,
    }
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented", values, claims, record=record
    )
    assert not diagnostics
    changed = claim
    if drift == "label":
        changed = replace(
            claim,
            members=(
                claim.members[0],
                replace(claim.members[1], label="Different"),
                claim.members[2],
            ),
        )
    elif drift == "missing":
        changed = replace(claim, members=(claim.members[0], claim.members[2]))
    elif drift == "code":
        changed = replace(
            claim, members=(*claim.members, replace(claim.members[2], code="16"))
        )
    elif drift == "association":
        changed = replace(
            claim,
            members=(
                claim.members[0],
                replace(
                    claim.members[1], associations=(replace(association, row_number=2),)
                ),
                claim.members[2],
            ),
        )
    if drift == "book":
        changed = replace(claim, version_label="Different book")
    if drift == "historical":
        historical = replace(
            historical,
            members=(replace(historical.members[0], label="Changed old label"),),
        )
    current = (changed, historical)
    result = apply_coding_choices((record,), cases, coding={column: current})
    assert result.coding[column].claims == current
    if drift is None:
        assert not result.diagnostics
        selected = next(
            s for s in result.coding[column].segments if s.valid_from == "2020-01-01"
        )
        assert selected.code_set.members == (("14", "Landsting"), ("15", "Other"))
    else:
        assert result.accounting[0].status == "stale"
        assert _compile_entry("documented", values, current, record=record)[1]


@pytest.mark.parametrize(
    "labels,selected",
    [
        (["Region", "Region"], "Region"),
        (["Region"], "Region"),
        (["Landsting", "Region"], "Other"),
    ],
)
def test_label_equivalence_requires_exact_positive_source_alternatives(
    labels, selected
):
    from reg_meta_build.source_curation import CodeLabelEquivalence

    with pytest.raises(ValidationError):
        CodeLabelEquivalence(code="14", labels=labels, selected_label=selected)


@pytest.mark.parametrize("three_codes", [False, True])
def test_closed_alpha_own_name_certifies_bare_source_codes(three_codes):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import SourceEnumeration

    pairs = (("LEG", "legitimation"), ("SPEC", "specialitet"))
    if three_codes:
        pairs += (("EXAM", "examen"),)
    lines = tuple(f"{code} = {label}" for code, label in pairs)
    prose = "Kategori " + ", ".join(lines[:-1]) + " eller " + lines[-1]
    record = _record().model_copy(
        update={
            "fields": _record().fields.model_copy(update={"name": value_field(prose)})
        }
    )
    claims = (
        CodeListClaim(
            "bare",
            record.edition_scope,
            tuple(
                CodeMembershipClaim(code, None, TemporalScope(kind="year_independent"))
                for code, _ in pairs
            ),
        ),
    )
    authority = _row_authority(record, claims).model_dump(mode="json")
    authority.update(
        enumeration={
            "field": "name",
            "syntax": "kategori-alpha-equals",
            "lines": lines,
        },
        raw_codings=[coding_source_sha256(claims[0])],
    )
    cases, issues, register, scope, columns, column = _compile_entry(
        "documented",
        {
            "members": pairs,
            "version_label": "Own literal meanings",
            "source_authority": authority,
        },
        claims,
        record=record,
    )
    assert len(cases) == 1 and not issues
    evidence = SourceEvidence((record,), value_bindings={})
    applied = apply_coding_choices(evidence, cases, coding={column: claims})
    assert not applied.diagnostics
    assert set(applied.coding[column].segments[0].code_set.members) == set(pairs)
    assert applied.coding[column].claims == claims
    certificate = SourceEnumeration.model_validate(authority["enumeration"])
    for changed in (
        "Not " + prose,
        prose + " or OTHER = other",
        prose.replace("legitimation", "unknown"),
        prose.replace("SPEC", "OTHER"),
    ):
        fields = record.fields.model_copy(update={"name": value_field(changed)})
        assert not certificate.matches_fields(fields)
        altered = record.model_copy(update={"fields": fields})
        fresh, issues = compile_coding_register(
            register,
            scope,
            originals=(altered,),
            columns={column: (altered,)},
            column_scopes=_column_scopes(columns),
            coding={column: claims},
        )
        assert not fresh and issues
        assert apply_coding_choices(
            SourceEvidence((altered,), value_bindings={}),
            cases,
            coding={column: claims},
        ).diagnostics
    changed_claim = replace(
        claims[0],
        members=claims[0].members
        + (CodeMembershipClaim("OTHER", None, TemporalScope(kind="year_independent")),),
    )
    assert apply_coding_choices(
        evidence, cases, coding={column: (changed_claim,)}
    ).diagnostics


@pytest.mark.parametrize("drift", ["missing", "peer", "parent", "code_label"])
def test_closed_alpha_certificate_refuses_complete_source_evidence_drift(drift):
    from reg_meta_build.source_coding import coding_source_sha256

    record = _record().model_copy(
        update={
            "fields": _record().fields.model_copy(
                update={
                    "name": value_field(
                        "Kategori LEG = legitimation eller SPEC = specialitet"
                    )
                }
            )
        }
    )
    claims = (
        CodeListClaim(
            "bare",
            record.edition_scope,
            tuple(
                CodeMembershipClaim(code, None, TemporalScope(kind="year_independent"))
                for code in ("LEG", "SPEC")
            ),
        ),
    )
    authority = _row_authority(record, claims).model_dump(mode="json")
    authority.update(
        enumeration={
            "field": "name",
            "syntax": "kategori-alpha-equals",
            "lines": ["LEG = legitimation", "SPEC = specialitet"],
        },
        raw_codings=[coding_source_sha256(claims[0])],
    )
    cases, issues, register, scope, columns, column = _compile_entry(
        "documented",
        {
            "members": [["LEG", "legitimation"], ["SPEC", "specialitet"]],
            "version_label": "Own literal meanings",
            "source_authority": authority,
        },
        claims,
        record=record,
    )
    assert len(cases) == 1 and not issues
    rows = (record,)
    if drift == "missing":
        rows = ()
    elif drift == "peer":
        rows += (_record(2021),)
    elif drift == "parent":
        rows = (record.model_copy(update={"parent_facts": ()}),)
    else:
        claims = (
            replace(
                claims[0],
                members=(
                    replace(claims[0].members[0], label="Unreviewed"),
                    claims[0].members[1],
                ),
            ),
        )
    fresh, issues = compile_coding_register(
        register,
        scope,
        originals=rows,
        columns={column: rows},
        column_scopes=_column_scopes(columns),
        coding={column: claims},
    )
    assert not fresh and issues
    assert apply_coding_choices(
        SourceEvidence(rows, value_bindings={}), cases, coding={column: claims}
    ).diagnostics


@pytest.mark.parametrize(
    "drift", [None, "label", "code", "missing", "outside", "wrong", "sibling"]
)
def test_documented_label_witness_preserves_period_specific_missing_tokens(drift):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import CodeLabelEquivalence

    record = _record()
    base = replace(
        _claim("Status", "30", "2016-01-01", "2016-12-31"),
        members=tuple(
            CodeMembershipClaim(code, label, TemporalScope(kind="year_independent"))
            for code, label in (
                ("30", "Matrix imputation"),
                ("30", "Model imputation"),
                ("6", "Missing"),
            )
        ),
    )
    future = replace(
        base,
        claim_id="future",
        scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2017-01-01", end="2017-12-31"),),
        ),
        members=(
            *base.members,
            CodeMembershipClaim(
                "NULL", "Missing", TemporalScope(kind="year_independent")
            ),
        ),
    )
    claims = (base, future)
    authority = _row_authority(record, claims).model_copy(
        update={
            "raw_codings": [coding_source_sha256(c) for c in claims],
            "label_equivalences": [
                CodeLabelEquivalence(
                    code="30",
                    labels=("Matrix imputation", "Model imputation"),
                    selected_label="Model imputation",
                )
            ],
            "witness": ("2016-01-01", "2016-12-31"),
        }
    )
    values = {
        "members": [["30", "Model imputation"], ["6", "Missing"]],
        "version_label": "Status",
        "source_authority": authority,
    }
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented", values, claims, record=record
    )
    assert not diagnostics
    changed = claims
    if drift == "label":
        changed = (
            replace(
                base,
                members=(
                    replace(base.members[0], label="Other method"),
                    *base.members[1:],
                ),
            ),
            future,
        )
    elif drift == "code":
        changed = (
            replace(base, members=(*base.members, replace(base.members[2], code="7"))),
            future,
        )
    elif drift == "missing":
        changed = (future,)
    elif drift == "sibling":
        changed = (
            base,
            replace(
                future,
                members=(*future.members, replace(future.members[-1], code="NEW")),
            ),
        )
    elif drift == "wrong":
        values = {
            **values,
            "source_authority": authority.model_copy(
                update={"witness": ("2017-01-01", "2017-12-31")}
            ),
        }
        assert _compile_entry("documented", values, claims, record=record)[1]
        return
    elif drift == "outside":
        values = {
            **values,
            "source_authority": authority.model_copy(
                update={"witness": ("2015-01-01", "2015-12-31")}
            ),
        }
        assert _compile_entry("documented", values, claims, record=record)[1]
        return
    result = apply_coding_choices((record,), cases, coding={column: changed})
    assert result.coding[column].claims == changed
    if drift is None:
        selected = next(
            s for s in result.coding[column].segments if s.valid_from == "2020-01-01"
        )
        assert set(selected.code_set.members) == {
            ("30", "Model imputation"),
            ("6", "Missing"),
        }
        # A later witnessed domain keeps its actual missing token.
        later = {
            **values,
            "members": [*values["members"], ["NULL", "Missing"]],
            "source_authority": authority.model_copy(
                update={"witness": ("2017-01-01", "2017-12-31")}
            ),
        }
        assert not _compile_entry("documented", later, claims, record=record)[1]
    else:
        assert result.accounting[0].status == "stale"
        assert _compile_entry("documented", values, changed, record=record)[1]


@pytest.mark.parametrize("contrary", [None, "code", "label"])
def test_documented_witness_refuses_contrary_complete_target_domain(contrary):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_curation import CodeLabelEquivalence

    record = _record()
    members = tuple(
        CodeMembershipClaim(code, label, TemporalScope(kind="year_independent"))
        for code, label in (("30", "Matrix"), ("30", "Model"), ("6", "Missing"))
    )
    witness = replace(
        _claim("Status", "30", "2016-01-01", "2016-12-31"), members=members
    )
    target_members = (members[1], members[2])
    if contrary == "code":
        target_members = (*target_members, replace(members[2], code="7"))
    elif contrary == "label":
        target_members = (replace(members[1], label="Directly observed"), members[2])
    target = replace(_claim("Status", "30"), claim_id="target", members=target_members)
    claims = (witness, target)
    authority = _row_authority(record, claims).model_copy(
        update={
            "raw_codings": [coding_source_sha256(c) for c in claims],
            "witness": ("2016-01-01", "2016-12-31"),
            "label_equivalences": [
                CodeLabelEquivalence(
                    code="30", labels=("Matrix", "Model"), selected_label="Model"
                )
            ],
        }
    )
    values = {
        "members": [["30", "Model"], ["6", "Missing"]],
        "version_label": "Status",
        "source_authority": authority,
    }
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented", values, claims, record=record
    )
    if contrary:
        assert diagnostics and not cases
    else:
        assert not diagnostics
        result = apply_coding_choices((record,), cases, coding={column: claims})
        assert not result.diagnostics and result.coding[column].claims == claims
        # Ordinary documentary entries still cannot overwrite a complete source list.
        values["source_authority"] = authority.model_copy(update={"witness": None})
        assert _compile_entry("documented", values, claims, record=record)[1]


def test_reviewed_decimal_comma_certificate_preserves_exact_source_and_no_claims():
    from reg_meta_build.source_curation import SourceEnumeration
    from reg_meta_build.sources.sos import _classify_value_set_text

    pairs = (("0", "giltigt pnr"), ("8", "Ogiltigt pnr"))
    prose = "0=giltigt pnr,  8=Ogiltigt pnr"
    assert _classify_value_set_text(prose) == (None, True)
    record = _record().model_copy(
        update={
            "fields": _record().fields.model_copy(
                update={"representation": value_field(prose)}
            )
        }
    )
    placeholder = CodeListClaim("placeholder", record.edition_scope, ())
    authority = _row_authority(record, (placeholder,)).model_dump(mode="json")
    authority.update(
        codings=[],
        raw_codings=[],
        enumeration={
            "field": "representation",
            "syntax": "ascii-decimal-comma-equals",
            "lines": tuple(f"{code}={label}" for code, label in pairs),
        },
    )
    cases, issues, register, scope, columns, column = _compile_entry(
        "documented",
        {
            "members": pairs,
            "version_label": "Exact source decimal meanings",
            "source_authority": authority,
        },
        (),
        record=record,
    )
    assert len(cases) == 1 and not issues
    evidence = SourceEvidence((record,), value_bindings={})
    applied = apply_coding_choices(evidence, cases, coding={column: ()})
    assert not applied.diagnostics
    assert set(applied.coding[column].segments[0].code_set.members) == set(pairs)
    assert applied.coding[column].claims == ()
    certificate = SourceEnumeration.model_validate(authority["enumeration"])
    for changed in (
        prose + ", 4=samordningsnummer",
        prose.replace("8=", "9="),
        prose.replace("Ogiltigt", "Annat"),
        "Prefix " + prose,
    ):
        altered = record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"representation": value_field(changed)}
                )
            }
        )
        assert not certificate.matches_fields(altered.fields)
        fresh, issues = compile_coding_register(
            register,
            scope,
            originals=(altered,),
            columns={column: (altered,)},
            column_scopes=_column_scopes(columns),
            coding={column: ()},
        )
        assert not fresh and issues
        assert apply_coding_choices(
            SourceEvidence((altered,), value_bindings={}), cases, coding={column: ()}
        ).diagnostics
    assert apply_coding_choices(
        evidence, cases, coding={column: (placeholder,)}
    ).diagnostics
    for bad_lines in (
        ("0=giltigt pnr", "0=Ogiltigt pnr"),
        ("0=giltigt pnr", "x=Ogiltigt pnr"),
        ("0=giltigt pnr, annat", "8=Ogiltigt pnr"),
    ):
        with pytest.raises(ValidationError):
            SourceEnumeration(
                field="representation",
                syntax="ascii-decimal-comma-equals",
                lines=bad_lines,
            )


@pytest.mark.parametrize(
    "drift",
    [
        None,
        "new_anchor",
        "changed_period",
        "missing_row",
        "foreign_sheet",
        "multiplicity",
        "out_of_block",
        "outside",
        "multiple_claims",
    ],
)
def test_documented_period_block_retains_literal_codes_and_all_raw_claims(drift):
    from reg_meta_build.source_coding import coding_source_sha256
    from reg_meta_build.source_values import SourceValueAssociation, SourceValueWindow

    record = _record()
    early = TemporalScope(
        kind="intervals",
        intervals=(ScopeInterval(start="2019-01-01", end="2020-12-31"),),
    )
    later = TemporalScope(
        kind="intervals",
        intervals=(ScopeInterval(start="2021-01-01", end="2021-12-31"),),
    )
    unrestricted = TemporalScope(kind="year_independent")
    pairs = (
        ("00", "Original"),
        ("2-3", "Range token"),
        ("blank", "No type"),
        ("00", "Later meaning"),
        ("blank", "Later missing meaning"),
    )
    members = []
    for row, (code, label) in enumerate(pairs, 1):
        period = early if row == 1 else later if row == 4 else None
        association = SourceValueAssociation(
            row,
            "book",
            str(row),
            "book.xlsx",
            "codes",
            supplied_period="2019-2020" if row == 1 else "2021" if row == 4 else None,
            supplied_window=SourceValueWindow(
                "known",
                period.intervals[0].start,
                period.intervals[0].end,
            )
            if period is not None
            else None,
        )
        members.append(
            CodeMembershipClaim(
                code, label, period or unrestricted, associations=(association,)
            )
        )
    claim = replace(
        _claim("Source", "00", "2019-01-01", "2021-12-31"), members=tuple(members)
    )
    claims = (claim,)
    authority = _row_authority(record, claims).model_copy(
        update={
            "raw_codings": [coding_source_sha256(claim)],
            "period_block": members[0].associations[0].locator,
        }
    )
    values = {
        "members": list(pairs[:3]),
        "version_label": "Reviewed2019-2020block",
        "data_warning": "The source tokens' stored encoding is unverified.",
        "source_authority": authority,
    }
    cases, diagnostics, _, _, _, column = _compile_entry(
        "documented",
        values,
        claims,
        record=record,
    )
    assert not diagnostics and cases
    assert cases[0].decision.data_warning == values["data_warning"]
    changed = members.copy()
    if drift in {"new_anchor", "changed_period", "foreign_sheet"}:
        index = 0 if drift == "changed_period" else 1
        association = changed[index].associations[0]
        update = (
            {"source_table": "other"}
            if drift == "foreign_sheet"
            else {
                "supplied_period": "2018-2020",
                "supplied_window": SourceValueWindow(
                    "known", "2018-01-01", "2020-12-31"
                ),
            }
        )
        changed[index] = replace(
            changed[index], associations=(replace(association, **update),)
        )
    elif drift == "missing_row":
        changed.pop(2)
    elif drift == "multiplicity":
        changed[1] = replace(changed[1], associations=changed[1].associations * 2)
    elif drift == "out_of_block":
        changed[-1] = replace(changed[-1], label="Contrary later source meaning")
    elif drift == "outside":
        values = {**values, "periods": [["2021-01-01", "2021-12-31"]]}
        assert _compile_entry("documented", values, claims, record=record)[1]
        return
    fresh = (replace(claim, members=tuple(changed)),)
    if drift == "multiple_claims":
        fresh = (*fresh, replace(claim, claim_id="other"))
    result = apply_coding_choices((record,), cases, coding={column: fresh})
    assert result.coding[column].claims == fresh
    if drift is None:
        assert not result.diagnostics
        assert any(
            s.code_set is not None and set(s.code_set.members) == set(pairs[:3])
            for s in result.coding[column].segments
        )
    else:
        assert result.accounting[0].status == "stale"
        assert _compile_entry("documented", values, fresh, record=record)[1]


@pytest.mark.parametrize("end", ["2020-12-31", None])
def test_documented_period_block_intersects_known_delivery_scope(end):
    from reg_meta_build.source_coding_choices import (
        documented_period_block,
        documented_period_block_matches,
    )
    from reg_meta_build.source_values import SourceValueAssociation, SourceValueWindow

    effective = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2020-01-01", end=end),)
    )
    association = SourceValueAssociation(
        6,
        "book",
        "6",
        "book.xlsx",
        "codes",
        supplied_period="2019-2020" if end else "2019-",
        supplied_window=SourceValueWindow("known", "2019-01-01", end),
    )
    claim = replace(
        _claim("Source", "00", "2020-01-01", "2021-12-31"),
        scope=effective,
        members=(
            CodeMembershipClaim(
                "00", "Original", effective, associations=(association,)
            ),
            CodeMembershipClaim(
                "blank",
                "Missing",
                TemporalScope(kind="year_independent"),
                associations=(
                    replace(
                        association,
                        row_number=7,
                        value_key="7",
                        supplied_period=None,
                        supplied_window=None,
                    ),
                ),
            ),
        ),
    )
    pairs = (("00", "Original"), ("blank", "Missing"))
    assert documented_period_block((claim,), association.locator) == (effective, pairs)
    assert documented_period_block_matches(
        (claim,), association.locator, pairs, "2020-01-01", "2020-12-31", effective
    )
    assert not documented_period_block_matches(
        (claim,), association.locator, pairs, "2019-01-01", "2019-12-31", effective
    )
    assert claim.members[0].associations[0].supplied_period == (
        "2019-2020" if end else "2019-"
    )
