"""Checked corrections: column declarations, holdings periods, steward storage and variant routing."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from catalog_manifest import synthetic_manifest
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.scb_errata import (
    ErrataColumn,
    ErrataEditionBinding,
    ErrataVariantContext,
    convert_column_entry,
)
from reg_meta_build.source_coding import resolve_code_membership
from reg_meta_build.source_curation import (
    CheckedEditionRebind,
    CheckedVariantAssignment,
    CuratedOccurrenceAddition,
    OccurrenceCorrectionDecision,
)
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_intervals import (
    resolve_occurrence_intervals,
)
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    SourceFields,
    TemporalScope,
    value_field,
)

if TYPE_CHECKING:
    from pathlib import Path

from _source_effects_support import (
    effect_case as _case,
    effect_record as _record,
    storage_columns as _storage_columns,
)


@pytest.mark.parametrize("versions", [None, ("2020",)])
@pytest.mark.parametrize("classification", [None, "FIX"])
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


def test_holdings_period_converts_to_one_pooled_range(tmp_path: Path) -> None:
    """Y-212: a dataset-grain steward holding is ONE pooled range, never
    per-edition claims — one `:holdings` addition resolving to one pooled
    segment that forms one `pooled = 1` state."""
    record = _record(column="OTHER")
    original = source_occurrence(record)
    assert original.edition_key is not None
    entry = ErrataColumn(
        register_id=1,
        register_variant_id=2,
        column="MISSING",
        name="Authored name",
        definition="Authored description",
        data_type="text",
        classification=None,
        is_identifier=False,
        is_sensitive=False,
        versions=None,
        holdings_period="2002-2020",
        source="steward-holdings",
        provenance="errata:steward-holdings\nThe steward holds it.",
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
        declared_flags=frozenset(),
    )
    assert converted.case is not None and converted.blockers == ()
    assert isinstance(converted.case.decision, OccurrenceCorrectionDecision)
    (effect,) = converted.case.decision.effects
    assert isinstance(effect, CuratedOccurrenceAddition)
    assert effect.occurrence_key == "column-1:holdings"
    assert effect.edition_key is None
    assert (
        effect.edition_scope
        == effect.edition_period_scope
        == TemporalScope(
            kind="pooled",
            label="2002-2020",
            pooled_start="2002-01-01",
            pooled_end="2020-12-31",
        )
    )
    assert effect.fields.data_type == value_field("text")
    result = apply_occurrence_cases((record,), (converted.case,))
    assert result.diagnostics == ()
    (addition,) = tuple(item for item in result.occurrences if item.occurrence_key)
    resolved = resolve_occurrence_intervals((addition,))
    assert resolved.issues == ()
    (segment,) = resolved.segments
    assert (segment.valid_from, segment.valid_to) == ("2002-01-01", "2020-12-31")
    assert segment.pooled is True
    assert addition.variant_key is not None and addition.column_key is not None
    formed = form_native_variable(
        (addition,),
        register=ResolvedRegister(provider="scb", slug="fixture", name="Fixture"),
        variants={addition.variant_key: ResolvedVariant(slug="people", name="People")},
        slug="missing",
        provider_key="2.missing",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(False)
        ),
        coding={addition.column_key: resolve_code_membership(())},
    )
    assert formed.variable is not None and formed.diagnostics == ()
    (state,) = formed.variable.states
    assert (state.valid_from, state.valid_to) == ("2002-01-01", "2020-12-31")
    assert state.pooled is True
    output = tmp_path / "catalog.db"
    write_resolved_catalog((formed.variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT valid_from, valid_to, pooled FROM variable_state"
            ).fetchone()
        ) == ("2002-01-01", "2020-12-31", 1)


def test_steward_storage_type_and_flags_preserve_curated_identifier() -> None:
    record = _record(column="OTHER")
    original = source_occurrence(record)
    assert original.edition_key is not None
    entry = ErrataColumn(
        register_id=1,
        register_variant_id=2,
        column="MISSING",
        name="Held column",
        definition="Held by SWECOV",
        data_type=None,
        classification=None,
        is_identifier=True,
        is_sensitive=False,
        versions=("2020",),
        holdings_period=None,
        source="steward-holdings",
        provenance="errata:steward-holdings\nHeld by SWECOV",
    )
    binding = ErrataEditionBinding(
        key=original.edition_key,
        name="2020",
        edition_scope=record.edition_scope,
        edition_period_scope=record.edition_period_scope,
        support=(record_ref(record),),
        native_id=2020,
    )
    columns = _storage_columns(("CIS2004", "smallint"), ("CIS2012", "float"))
    result = convert_column_entry(
        entry,
        case_id="column-storage",
        context=ErrataVariantContext((record,), 1, 2, (binding,)),
        declared_flags=frozenset({"is_identifier"}),
        steward_table_prefixes=("CIS",),
        storage_columns=columns,
    )
    assert result.case is not None
    assert isinstance(result.case.decision, OccurrenceCorrectionDecision)
    (effect,) = result.case.decision.effects
    assert isinstance(effect, CuratedOccurrenceAddition)
    assert effect.fields.data_type == value_field("decimal")
    assert effect.fields.identifier == value_field(True)
    assert effect.fields.sensitivity == value_field(False)
    assert "CIS2004=smallint, CIS2012=float" in result.case.decision.provenance

    missing = convert_column_entry(
        entry,
        case_id="column-storage",
        context=ErrataVariantContext((record,), 1, 2, (binding,)),
        declared_flags=frozenset({"is_identifier"}),
        steward_table_prefixes=("NONE",),
        storage_columns=columns,
    )
    assert missing.case is not None
    assert isinstance(missing.case.decision, OccurrenceCorrectionDecision)
    (effect,) = missing.case.decision.effects
    assert isinstance(effect, CuratedOccurrenceAddition)
    assert effect.fields.data_type is None

    curated = convert_column_entry(
        replace(entry, data_type="integer"),
        case_id="column-storage",
        context=ErrataVariantContext((record,), 1, 2, (binding,)),
        declared_flags=frozenset({"is_identifier"}),
        steward_table_prefixes=("CIS",),
        storage_columns=_storage_columns(("CIS2016", "varchar")),
    )
    assert curated.case is not None
    assert isinstance(curated.case.decision, OccurrenceCorrectionDecision)
    (effect,) = curated.case.decision.effects
    assert isinstance(effect, CuratedOccurrenceAddition)
    assert effect.fields.data_type == value_field("integer")

    mixed = convert_column_entry(
        entry,
        case_id="column-storage",
        context=ErrataVariantContext((record,), 1, 2, (binding,)),
        declared_flags=frozenset({"is_identifier"}),
        steward_table_prefixes=("CIS",),
        storage_columns=_storage_columns(("CIS2004", "date"), ("CIS2012", "int")),
    )
    assert mixed.case is not None
    applied = apply_occurrence_cases((record,), (mixed.case,))
    addition = next(item for item in applied.occurrences if item.occurrence_key)
    assert addition.fields.data_type is None
    intervals = resolve_occurrence_intervals((addition,))
    assert intervals.segments
    assert addition.variant_key is not None and addition.column_key is not None
    formed = form_native_variable(
        (addition,),
        register=ResolvedRegister(provider="scb", slug="fixture", name="Fixture"),
        variants={addition.variant_key: ResolvedVariant(slug="people", name="People")},
        slug="missing",
        provider_key="2.missing",
        flags=SourceFields(
            sensitivity=value_field(False), identifier=value_field(True)
        ),
        coding={addition.column_key: resolve_code_membership(())},
    )
    assert any(
        issue.code == "unknown_data_type"
        and "CIS2004=date, CIS2012=int" in issue.detail
        for issue in formed.diagnostics
    )


def test_checked_variant_routing_retains_unknown_scope_and_physical_evidence() -> None:
    record = _record(column="VALUE").model_copy(
        update={
            "parent_facts": (),
            "edition_scope": TemporalScope(kind="unknown", label="not supplied"),
        }
    )
    case = _case(
        record,
        CheckedVariantAssignment(
            ref=record_ref(record), variant_keys=(("second",), ("first",))
        ),
    )
    result = apply_occurrence_cases((record,), (case,))
    assert result.diagnostics == ()
    assert result.accounting[0].disposition == "applied"
    assert [item.variant_key for item in result.occurrences] == [
        ("first",),
        ("second",),
    ]
    for item in result.occurrences:
        assert item.source_records == (record,)
        assert item.fields == record.fields
        assert item.edition_scope == record.edition_scope
        assert item.edition_key is None
        assert item.corrections[0].case_id == case.case_id
    changed = record.model_copy(
        update={"edition_scope": TemporalScope(kind="not_applicable")}
    )
    stale = apply_occurrence_cases((changed,), (case,))
    assert stale.accounting[0].disposition == "stale"
    assert len(stale.occurrences) == 1
    assert stale.occurrences[0].variant_key == source_occurrence(changed).variant_key


def test_variant_routing_disagreements_withhold_the_route_not_source_fields() -> None:
    record = _record(column="VALUE").model_copy(update={"parent_facts": ()})
    cases = tuple(
        _case(
            record,
            CheckedVariantAssignment(ref=record_ref(record), variant_keys=keys),
            name=name,
        )
        for name, keys in (("one", (("first",),)), ("two", (("second",),)))
    )
    result = apply_occurrence_cases((record,), cases)
    assert all(item.disposition == "conflicted" for item in result.accounting)
    assert {item.code for item in result.diagnostics} == {
        "conflicting_curation_effects"
    }
    assert len(result.occurrences) == 1
    assert result.occurrences[0].variant_key is None
    assert result.occurrences[0].fields == record.fields
    assert result.occurrences[0].withheld_fields == ("variant",)
    assert apply_occurrence_cases((record,), cases[::-1]) == result


def test_variant_routing_cannot_move_a_native_edition_implicitly() -> None:
    record = _record(column="VALUE")
    case = _case(
        record,
        CheckedVariantAssignment(ref=record_ref(record), variant_keys=(("other",),)),
    )
    with pytest.raises(ValueError, match="cannot implicitly reparent"):
        apply_occurrence_cases((record,), (case,))


def test_explicit_edition_rebind_moves_native_edition() -> None:
    record = _record(column="VALUE")
    original = source_occurrence(record)
    assert original.variant_key is not None and original.edition_key is not None
    split = (*original.variant_key, "edition-split", "stock")
    case = _case(
        record, CheckedEditionRebind(ref=record_ref(record), variant_key=split)
    )
    (moved,) = apply_occurrence_cases((record,), (case,)).occurrences
    assert moved.variant_key == split
    assert moved.edition_key == (
        *split,
        *original.edition_key[len(original.variant_key) :],
    )
    assert moved.source_records == (record,)
