"""Checked corrections compose without turning curated output into source evidence: sensitivity, additions and effect composition."""

from __future__ import annotations

import pytest
from _source_effects_support import (
    effect_case as _case,
    effect_field as _field,
    effect_record as _record,
    effect_scope as _scope,
)
from reg_meta.source_evidence import SourceField
from reg_meta_build.source_curation import (
    CheckedPeriodChange,
    CuratedOccurrenceAddition,
    capture_expectations,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    copied_coding_key,
    record_ref,
)
from reg_meta_build.source_intervals import (
    resolve_occurrence_intervals,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    SourceFields,
    SourceRecord,
    TemporalScope,
)


def _addition(
    record: SourceRecord, *, key: str = "delivery-2019"
) -> CuratedOccurrenceAddition:
    original = source_occurrence(record)
    assert original.variable_key and original.variant_key
    return CuratedOccurrenceAddition(
        occurrence_key=key,
        provider="scb",
        variable_key=original.variable_key,
        variant_key=original.variant_key,
        edition_key=("curated-edition", "2019"),
        fields=record.fields,
        edition_scope=_scope("2019"),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        evidence=(record_ref(record),),
        donor=record_ref(record),
        copied_fields=tuple(
            name
            for name in SourceFields.model_fields
            if getattr(record.fields, name) is not None
        ),
    )


def test_added_occurrence_copies_coding_only_with_explicit_checked_declaration() -> (
    None
):
    record = _record(column="VALUE")
    addition = _addition(record)
    metadata_only = apply_occurrence_cases((record,), (_case(record, addition),))
    assert metadata_only.occurrences[-1].coding_records == ()

    copied = addition.model_copy(update={"copy_coding": True, "expected_codings": ()})
    case = _case(record, copied)
    with pytest.raises(ValueError, match="checked code-set references"):
        apply_occurrence_cases((record,), (case,))
    case = case.model_copy(
        update={
            "targets": capture_expectations(
                (record,), fields=tuple(SourceFields.model_fields), coding=True
            )
        }
    )
    with pytest.raises(ValueError, match="original donor binding evidence"):
        apply_occurrence_cases((record,), (case,))
    result = apply_occurrence_cases(
        (record,), (case,), coding={copied_coding_key(copied): ()}
    )
    assert result.diagnostics == ()
    assert result.occurrences[-1].coding_records == (record,)
    without_guard = copied.model_dump(exclude={"expected_codings"})
    with pytest.raises(ValueError, match="original coding fingerprints"):
        CuratedOccurrenceAddition.model_validate(without_guard)


def test_disjoint_and_equal_effects_compose_against_original_evidence() -> None:
    record = _record()
    column = _field(record, "column_name", "VALUE")
    cases = (
        _case(record, column, name="name-column"),
        _case(
            record, column, _field(record, "description", "Corrected"), name="describe"
        ),
    )
    result = apply_occurrence_cases((record, record), cases)
    assert result.diagnostics == ()
    assert all(item.disposition == "applied" for item in result.accounting)
    assert len(result.occurrences) == 2
    for occurrence in result.occurrences:
        assert occurrence.source_records == (record,)
        assert occurrence.fields.column_name == SourceField(
            status="value", value="VALUE"
        )
        assert occurrence.fields.description == SourceField(
            status="value", value="Corrected"
        )
        assert len(occurrence.corrections) == 3
    assert record.fields.column_name is not None
    # The fixture cells are all present, so the blank column is a delivered
    # blank: an explicit negative claim the applied cases leave untouched.
    assert record.fields.column_name.status == "negative"
    assert result == apply_occurrence_cases((record, record), tuple(reversed(cases)))


def test_field_conflict_withholds_only_that_field_and_retains_all_claims() -> None:
    record = _record(column="VALUE")
    cases = (
        _case(record, _field(record, "data_type", "text"), name="text"),
        _case(record, _field(record, "data_type", "integer"), name="integer"),
    )
    result = apply_occurrence_cases((record,), cases)
    occurrence = result.occurrences[0]
    assert occurrence.fields.data_type == SourceField(status="unknown")
    assert occurrence.withheld_fields == ("data_type",)
    assert {d.case_id for d in result.diagnostics} == {"text", "integer"}
    assert all(d.fields == ("data_type",) for d in result.diagnostics)
    # Another source row cannot turn a disputed correction into an apparent consensus.
    intervals = resolve_occurrence_intervals((occurrence, record))
    assert len(intervals.segments) == 1
    assert intervals.segments[0].fields.data_type == SourceField(status="unknown")
    assert intervals.segments[0].delivery_column_name == "VALUE"


def test_later_effect_cannot_use_an_earlier_effect_as_source_evidence() -> None:
    record = _record()
    first = _case(record, _field(record, "column_name", "VALUE"), name="first")
    rewritten = _record(column="VALUE")
    second = _case(
        rewritten, _field(rewritten, "description", "Changed"), name="second"
    )
    result = apply_occurrence_cases((record,), (first, second))
    assert [item.disposition for item in result.accounting] == ["applied", "stale"]
    assert result.occurrences[0].fields.description == record.fields.description
    assert result.diagnostics[0].code == "target_projection_changed"


def test_period_effect_is_exact_and_conflicts_do_not_restore_old_dates() -> None:
    record = _record(column="VALUE")
    first = CheckedPeriodChange(
        ref=record_ref(record),
        edition_scope=_scope("2019"),
        edition_period_scope=TemporalScope(kind="not_applicable"),
    )
    result = apply_occurrence_cases((record,), (_case(record, first),))
    assert [
        (s.valid_from, s.valid_to)
        for s in resolve_occurrence_intervals(result.occurrences).segments
    ] == [("2019-01-01", "2019-12-31")]
    other = first.model_copy(update={"edition_scope": _scope("2021")})
    conflicted = apply_occurrence_cases(
        (record,), (_case(record, first), _case(record, other, name="other"))
    )
    assert resolve_occurrence_intervals(conflicted.occurrences).segments == ()
    assert all(d.withheld_output == ("occurrence",) for d in conflicted.diagnostics)


def test_layout_changes_preserve_applicability_but_relevant_changes_do_not() -> None:
    record = _record(column="VALUE")
    case = _case(record, _field(record, "description", "Accepted description"))
    assert (
        apply_occurrence_cases((_record(row=99, column="VALUE"),), (case,)).diagnostics
        == ()
    )
    changed = _record(column="RENAMED")
    assert (
        apply_occurrence_cases((changed,), (case,)).accounting[0].disposition == "stale"
    )
