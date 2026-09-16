"""Resolve exact occurrence periods without choosing between conflicting facts.

The caller supplies one already-established source variable/variant identity.
This module does not equate column names, infer identity, or widen source periods.
Physical occurrences remain attached to each segment, including duplicates.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from reg_meta_build.source_occurrences import EffectiveOccurrence, effective_occurrence
from reg_meta_build.source_records import (
    SourceField,
    SourceFields,
    SourceParentObservation,
    SourceRecord,
    TemporalScope,
)

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True)
class OccurrenceIssue:
    code: str
    fields: tuple[str, ...]
    occurrences: tuple[SourceRecord, ...]
    valid_from: str | None
    valid_to: str | None
    withheld: tuple[str, ...]


@dataclass(frozen=True)
class SourceSegment:
    valid_from: str
    valid_to: str
    delivery_column_name: str
    fields: SourceFields
    occurrences: tuple[SourceRecord, ...]
    effective_occurrences: tuple[EffectiveOccurrence, ...]


@dataclass(frozen=True)
class OccurrenceResolution:
    segments: tuple[SourceSegment, ...]
    negative_segments: tuple[SourceSegment, ...]
    issues: tuple[OccurrenceIssue, ...]
    unsupported_occurrences: tuple[SourceRecord, ...]


def finite_scope_bounds(scope: TemporalScope) -> tuple[tuple[int, int], ...] | None:
    """Return real-calendar ordinal bounds for an already interpreted finite scope."""
    if scope.kind != "intervals":
        return None
    result = []
    for interval in scope.intervals:
        bounds = []
        for value, tail in ((interval.start, "01-01"), (interval.end, "12-31")):
            if len(value) == 4 and value.isascii() and value.isdigit():
                value = f"{value}-{tail}"
            try:
                parsed = date.fromisoformat(value)
            except ValueError:
                return None
            if parsed.isoformat() != value or parsed.year == 9999:
                return None
            bounds.append(parsed.toordinal())
        result.append((bounds[0], bounds[1]))
    return tuple(result)


def _periods(record: EffectiveOccurrence) -> tuple[tuple[int, int], ...] | None:
    scope = record.edition_period_scope
    if scope.kind == "not_applicable":
        scope = record.edition_scope
    return finite_scope_bounds(scope)


def _column(record: EffectiveOccurrence) -> str | None:
    field = record.fields.column_name
    if field is not None and field.status == "value":
        assert isinstance(field.value, str)
        return field.value or None
    return None


def reconcile_source_fields(
    records: tuple[SourceRecord | EffectiveOccurrence | SourceParentObservation, ...],
) -> tuple[SourceFields, tuple[str, ...]]:
    resolved = {}
    conflicts = []
    for name in SourceFields.model_fields:
        observations = tuple(
            value
            for record in records
            if (value := getattr(record.fields, name)) is not None
        )
        values = {
            (value.status, value.value)
            for value in observations
            if value.status != "unknown"
        }
        explicitly_withheld = any(
            isinstance(record, EffectiveOccurrence) and name in record.withheld_fields
            for record in records
        )
        if len(values) > 1 or explicitly_withheld:
            conflicts.append(name)
            resolved[name] = SourceField(status="unknown")
        elif values:
            status, value = next(iter(values))
            resolved[name] = SourceField(status=status, value=value)
        elif observations:
            resolved[name] = SourceField(status="unknown")
    return SourceFields.model_validate(resolved), tuple(sorted(conflicts))


def resolve_occurrence_intervals(
    records: Iterable[SourceRecord | EffectiveOccurrence],
) -> OccurrenceResolution:
    """Keep independently supported facts on exact, non-overlapping column periods.

    Each physical occurrence must explicitly supply availability, a column, and a
    finite interpreted period. Unplaced occurrences stay in the result and block
    strict publication. Competing field values become unknown on their intersection;
    positive versus negative availability withholds that column segment entirely.
    Unknown optional observations do not contradict a supplied concrete fact.
    """
    by_column: dict[str, list[tuple[int, int, int]]] = defaultdict(list)
    source_records = tuple(effective_occurrence(record) for record in records)
    issues: list[OccurrenceIssue] = []
    unsupported: list[SourceRecord] = []
    subjects = {
        (
            record.provider,
            record.variable_key,
            record.variant_key,
        )
        for record in source_records
    }
    if len(subjects) > 1:
        raise ValueError(
            "occurrence resolution requires one source variable and variant"
        )
    for ordinal, record in enumerate(source_records):
        column = _column(record)
        periods = _periods(record)
        availability = record.fields.availability
        missing = []
        if column is None:
            missing.append("column_name")
        if periods is None:
            missing.append("period")
        if availability is None or availability.status == "unknown":
            missing.append("availability")
        if missing:
            unsupported.extend(record.evidence)
            issues.append(
                OccurrenceIssue(
                    "unsupported_occurrence",
                    tuple(missing),
                    record.evidence,
                    None,
                    None,
                    ("occurrence",),
                )
            )
            continue
        assert column is not None and periods is not None
        for start, end in periods:
            by_column[column].append((start, end, ordinal))

    segments = []
    negative_segments = []
    for column, periods in sorted(by_column.items()):
        changes: dict[int, list[tuple[int, int]]] = defaultdict(list)
        for start, end, ordinal in periods:
            changes[start].append((ordinal, 1))
            changes[end + 1].append((ordinal, -1))
        active: dict[int, int] = {}
        cuts = sorted(changes)
        for position, start in enumerate(cuts[:-1]):
            for ordinal, delta in changes[start]:
                count = active.get(ordinal, 0) + delta
                if count:
                    active[ordinal] = count
                else:
                    active.pop(ordinal, None)
            if not active:
                continue
            end = cuts[position + 1] - 1
            effective = tuple(source_records[ordinal] for ordinal in sorted(active))
            occurrences = tuple(
                record for item in effective for record in item.evidence
            )
            fields, conflicts = reconcile_source_fields(effective)
            lower, upper = (
                date.fromordinal(start).isoformat(),
                date.fromordinal(end).isoformat(),
            )
            populations = {
                record.population_key
                for record in effective
                if record.population_key is not None
            }
            if len(populations) > 1:
                issues.append(
                    OccurrenceIssue(
                        "conflicting_occurrence_population",
                        ("subject.population",),
                        occurrences,
                        lower,
                        upper,
                        ("column_segment",),
                    )
                )
            if conflicts:
                issues.append(
                    OccurrenceIssue(
                        "conflicting_occurrence_facts",
                        conflicts,
                        occurrences,
                        lower,
                        upper,
                        ("column_segment",)
                        if "availability" in conflicts
                        else conflicts,
                    )
                )
            availability = fields.availability
            assert availability is not None
            if availability.status == "negative" and len(populations) <= 1:
                negative_segments.append(
                    SourceSegment(lower, upper, column, fields, occurrences, effective)
                )
            if availability.status != "value" or len(populations) > 1:
                continue
            segments.append(
                SourceSegment(lower, upper, column, fields, occurrences, effective)
            )
    return OccurrenceResolution(
        tuple(segments), tuple(negative_segments), tuple(issues), tuple(unsupported)
    )
