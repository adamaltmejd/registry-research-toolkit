"""Accepted coding choices are finite, source-checked and order-independent: pin-free choices compile from literal labels and finite witnesses."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _source_coding_choices_support import (
    apply_choices as _apply,
    choice_record as _record,
    column_scopes as _column_scopes,
    compile_entry as _compile_entry,
    list_claim as _claim,
)
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.source_coding import (
    CodeMembershipClaim,
)
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_coordinates import (
    column_identity,
    source_register_key,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    TemporalScope,
    value_field,
)

from reg_meta_build.fqid_slugs import SlugEntry


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
