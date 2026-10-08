"""Checked corrections: column declarations, holdings periods, steward storage and variant routing."""

from __future__ import annotations

import pytest
from _source_effects_support import (
    effect_record as _record,
)
from reg_meta_build.scb_errata import (
    ErrataColumn,
    ErrataEditionBinding,
    ErrataVariantContext,
    convert_column_entry,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_intervals import (
    resolve_occurrence_intervals,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    value_field,
)


@pytest.mark.parametrize("versions", [None, ("2020",)])
@pytest.mark.parametrize("classification", ["FIX"])
def test_column_declaration_preserves_only_supplied_flags_and_periods(
    versions, classification
) -> None:
    record = _record(column="OTHER")
    original = source_occurrence(record)
    assert original.edition_key is not None
    entry = ErrataColumn(
        register_id=1,
        register_variant_id=2,
        column="MISSING",
        name="Authored name",
        definition="Authored description",
        data_type=None,
        classification=classification,
        is_identifier=False,
        is_sensitive=True,
        versions=versions,
        holdings_period=None,
        source="steward",
        provenance="Existing accepted column declaration",
    )
    binding = ErrataEditionBinding(
        key=original.edition_key,
        name="2020",
        edition_scope=record.edition_scope,
        edition_period_scope=record.edition_period_scope,
        support=(record_ref(record),),
        native_id=2020,
    )
    converted = convert_column_entry(
        entry,
        case_id="column-1",
        context=ErrataVariantContext((record,), 1, 2, (binding,)),
        declared_flags=frozenset({"is_sensitive"}),
    )
    assert converted.case is not None and converted.blockers == ()
    later = _record(column="UNRELATED", year="2021", cvid=21)
    result = apply_occurrence_cases((record, later), (converted.case,))
    assert result.diagnostics == ()
    originals = tuple(item for item in result.occurrences if item.source_records)
    assert tuple(item.source_records for item in originals) == ((record,), (later,))
    assert tuple(item.fields for item in originals) == (record.fields, later.fields)
    assert not any(item.corrections for item in originals)
    additions = tuple(item for item in result.occurrences if item.occurrence_key)
    assert len(additions) == 1
    addition = additions[0]
    if versions is None:
        assert addition.edition_key is None
        assert addition.edition_scope.kind == "unknown"
        intervals = resolve_occurrence_intervals((addition,))
        assert intervals.segments == ()
        assert {issue.code for issue in intervals.issues} == {"unsupported_occurrence"}
    else:
        assert addition.edition_key == binding.key
        assert addition.edition_scope == record.edition_scope
    assert addition.fields.name == value_field("Authored name")
    assert addition.fields.description == value_field("Authored description")
    assert addition.fields.data_type is None
    assert addition.fields.classification_declared == (
        value_field(classification) if classification is not None else None
    )
    assert addition.fields.identifier is None
    assert addition.fields.sensitivity == value_field(True)
    assert addition.source_records == () and addition.support_records == (record,)
    documented = _record(column="missing", year="2021", cvid=21)
    stale = apply_occurrence_cases((record, documented), (converted.case,))
    assert stale.accounting[0].disposition == "stale"
    assert not any(item.occurrence_key for item in stale.occurrences)
    blocked = convert_column_entry(
        entry,
        case_id="column-1",
        context=ErrataVariantContext((record, documented), 1, 2, (binding,)),
        declared_flags=frozenset({"is_sensitive"}),
    )
    assert blocked.case is None and blocked.blockers == ("column_now_documented",)
