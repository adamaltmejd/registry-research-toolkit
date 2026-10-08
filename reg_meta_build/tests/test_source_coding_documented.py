"""Documented coding follows the current effective delivery and the exact partition owner."""

from __future__ import annotations

from dataclasses import replace

import pytest
from _source_coding_choices_support import (
    choice_record as _record,
    compile_entry as _compile_entry,
    documented_values as _documented_values,
)
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_curation import SourceEvidence
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    ScopeInterval,
    TemporalScope,
    value_field,
)


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


def test_documented_coding_requires_the_exact_partition_owner():
    cases, diagnostics, *_ = _compile_entry(
        "documented", _documented_values(), (), split=True
    )
    assert not cases and diagnostics[0].code == "stale_curation_entry"
    cases, diagnostics, *_ = _compile_entry(
        "documented", {**_documented_values(), "variable": "1.5.part"}, (), split=True
    )
    assert len(cases) == 1 and not diagnostics
