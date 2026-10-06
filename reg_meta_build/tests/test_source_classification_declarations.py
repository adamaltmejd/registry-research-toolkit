"""Checked codebook bindings: source declarations, family references and declared-binding contracts."""

from dataclasses import replace

import pytest
from _source_classification_bindings_support import (
    binding_declaration as _declaration,
    binding_setup as _setup,
    declared_bindings as _declared,
    sole_classification as _sole_classification,
    sole_conformance as _sole_conformance,
)
from reg_meta.source_evidence import SourceField
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    TemporalScope,
    value_field,
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
