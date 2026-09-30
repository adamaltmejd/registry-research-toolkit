"""Resolve exact occurrence periods without choosing between conflicting facts.

The caller supplies one already-established source variable/variant identity.
This module does not equate column names, infer identity, or widen source periods.
Physical occurrences remain attached to each segment, including duplicates.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from typing import TYPE_CHECKING

from reg_meta.source_evidence import SourceField

from reg_meta_build._curation import (
    data_type_class,
    fold_column,
    widen_data_type_classes,
)
from reg_meta_build.source_occurrences import EffectiveOccurrence, effective_occurrence
from reg_meta_build.source_records import (
    SourceFields,
    SourceParentObservation,
    SourceRecord,
    TemporalScope,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.sources.swecov_column_types import StewardColumnStorage


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
    # Y-202: True when the surviving occurrences are all pooled-scope evidence,
    # i.e. this span is documented only by pooled multi-year editions. An
    # explicit (annual/precise) occurrence covering the span filters pooled
    # evidence out before field reconciliation, so the segment is ordinary —
    # pooled evidence never marks, splits, or disputes annual coverage.
    pooled: bool = False


@dataclass(frozen=True)
class OccurrenceResolution:
    segments: tuple[SourceSegment, ...]
    negative_segments: tuple[SourceSegment, ...]
    issues: tuple[OccurrenceIssue, ...]
    unsupported_occurrences: tuple[SourceRecord, ...]


@lru_cache(maxsize=4096)
def scope_bounds(scope: TemporalScope) -> tuple[tuple[int, int], ...] | None:
    """Resolve known bounds; date.max represents an explicitly open upper bound.

    Unknown and pooled scopes stay unresolved. A literal source year 9999 is not
    accepted as evidence; only an explicit open end uses the storage sentinel.
    """
    if scope.kind != "intervals":
        return None
    result = []
    for interval in scope.intervals:
        bounds = []
        for value, tail in ((interval.start, "01-01"), (interval.end, "12-31")):
            if value is None:
                bounds.append(date.max.toordinal())
                continue
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


def occurrence_bounds(
    record: EffectiveOccurrence,
) -> tuple[tuple[int, int], ...] | None:
    """Known effective coverage: one interval over the whole pooled range.

    A pooled scope carrying its range (see `source_periods.source_scopes`)
    resolves to exactly ONE interval — the hull start through the hull end — so
    formation emits one marked state and never infers annual availability
    inside the range. A pooled scope WITHOUT a range, and unknown scopes, stay
    unresolved (an unsupported occurrence, as before).
    """
    scope = record.edition_period_scope
    if scope.kind == "not_applicable":
        scope = record.edition_scope
    if scope.kind == "pooled":
        return pooled_bounds(scope)
    return scope_bounds(scope)


def coding_scope_bounds(scope: TemporalScope) -> tuple[tuple[int, int], ...] | None:
    """Occurrence-window bounds for coding membership, pooled-aware (Y-207).

    A range-carrying pooled scope yields exactly one interval over the whole
    pooled range — the membership window is clamped to that range, never split
    into annual membership inside it. A pooled scope without a range, and any
    other non-interval scope, stay unresolved as before.
    """
    if scope.kind == "pooled":
        return pooled_bounds(scope)
    return scope_bounds(scope)


def pooled_bounds(scope: TemporalScope) -> tuple[tuple[int, int], ...] | None:
    """The single whole-range interval for a range-carrying pooled scope."""
    if scope.pooled_start is None or scope.pooled_end is None:
        return None
    bounds = []
    for value in (scope.pooled_start, scope.pooled_end):
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            return None
        if parsed.isoformat() != value or parsed.year == 9999:
            return None
        bounds.append(parsed.toordinal())
    if bounds[1] < bounds[0]:
        return None
    return ((bounds[0], bounds[1]),)


def _occurrence_pooled(occurrence: EffectiveOccurrence) -> bool:
    """Whether an occurrence's effective scope is pooled (Y-202).

    Mirrors `occurrence_bounds`' `not_applicable` fallback so the pooled
    verdict follows the scope the bounds actually came from."""
    scope = occurrence.edition_period_scope
    if scope.kind == "not_applicable":
        scope = occurrence.edition_scope
    return scope.kind == "pooled"


def _segment_pooled(effective: tuple[EffectiveOccurrence, ...]) -> bool:
    """Whether a segment's span is documented ONLY by pooled editions.

    Takes the surviving occurrences (explicit covering occurrences already
    filtered pooled evidence out): a single explicit survivor makes the span
    ordinary — pooled loses to explicit annual."""
    if not effective:
        return False
    return all(_occurrence_pooled(occurrence) for occurrence in effective)


# Y-209: the state-grain facts deciding whether adjacent pooled cuts describe
# one continuous pooled coverage. These are exactly the reconciled facts
# formation carries onto the state, plus the population and coding evidence
# that shape it.
_POOLED_MERGE_FIELDS = (
    "data_type",
    "data_length",
    "operational_definition",
    "source_attribution",
    "availability",
)

# These source texts can drift between overlapping editions without establishing
# a conflicting occurrence fact. Reconciliation still marks them unknown.
_ABSORBED_OCCURRENCE_CONFLICT_FIELDS = frozenset(
    {"source_attribution", "operational_definition", "reference_period"}
)

# Closed list of observed unit spellings: (published value, other value).
_UNIT_PAIRS = (
    ("Kronor", "kronor"),
    ("Kronor (SEK)", "kronor"),
    ("Antal månader", "Månader"),
    ("Månader", "Månad"),
    ("Antal veckor", "Veckor"),
    ("Antal minuter", "Minuter"),
    ("Antal barn", "Antal"),
    ("Dagar", "Antal"),
    ("Årtal", "År"),
)


def _ordered_union[T](first: tuple[T, ...], second: tuple[T, ...]) -> tuple[T, ...]:
    """Set-union preserving order: every contributor appears exactly once."""
    merged = list(first)
    for record in second:
        if record not in merged:
            merged.append(record)
    return tuple(merged)


def _pooled_merge_key(
    segment: SourceSegment,
) -> tuple[
    tuple[SourceField | None, ...],
    frozenset,
    frozenset,
]:
    """Identity for the Y-209 merge: reconciled state-grain facts, population,
    and value-set/coding evidence."""
    return (
        tuple(getattr(segment.fields, name) for name in _POOLED_MERGE_FIELDS),
        frozenset(
            occurrence.population_key for occurrence in segment.effective_occurrences
        ),
        frozenset(
            (record.source, record.locators[0].semantic_record_key)
            for occurrence in segment.effective_occurrences
            for record in occurrence.coding_records
        ),
    )


def _merge_adjacent_pooled(segments: list[SourceSegment]) -> list[SourceSegment]:
    """Re-join adjacent pooled cuts with identical facts (Y-209).

    Overlapping pooled editions fragment at their boundaries into adjacent
    pooled cuts; where the reconciled facts, population, and coding evidence
    agree, the cuts document one continuous pooled coverage and merge into a
    single segment over the run's hull, with the union of occurrences and
    evidence. Adjacent means the next segment starts the day after the
    previous ends. An explicit segment never merges — neither with a pooled
    neighbor nor across it — and differing facts stay separate, as before.
    """
    merged: list[SourceSegment] = []
    for segment in segments:
        previous = merged[-1] if merged else None
        if (
            previous is not None
            and previous.pooled
            and segment.pooled
            and date.fromisoformat(segment.valid_from).toordinal()
            == date.fromisoformat(previous.valid_to).toordinal() + 1
            and _pooled_merge_key(previous) == _pooled_merge_key(segment)
        ):
            merged[-1] = SourceSegment(
                valid_from=previous.valid_from,
                valid_to=segment.valid_to,
                delivery_column_name=previous.delivery_column_name,
                fields=previous.fields,
                occurrences=_ordered_union(previous.occurrences, segment.occurrences),
                effective_occurrences=_ordered_union(
                    previous.effective_occurrences, segment.effective_occurrences
                ),
                pooled=True,
            )
        else:
            merged.append(segment)
    return merged


def _column(record: EffectiveOccurrence) -> str | None:
    field = record.fields.column_name
    if field is not None and field.status == "value":
        assert isinstance(field.value, str)
        return field.value or None
    return None


def reconcile_source_fields(
    records: tuple[SourceRecord | EffectiveOccurrence | SourceParentObservation, ...],
    *,
    support: tuple[SourceFields, ...] = (),
    storage: StewardColumnStorage | None = None,
) -> tuple[SourceFields, tuple[str, ...]]:
    resolved = {}
    conflicts = []
    widened_classes: frozenset[str] | None = None
    storage_capped = False
    field_sources = (*(record.fields for record in records), *support)
    checked_sensitivity = tuple(
        record.fields.sensitivity
        for record in records
        if isinstance(record, EffectiveOccurrence)
        and "sensitivity" in record.checked_fields
    )
    for name in SourceFields.model_fields:
        # A checked sensitivity choice outranks raw and support declarations;
        # those records remain attached to the effective occurrence as evidence.
        if name == "sensitivity" and checked_sensitivity:
            observations = tuple(
                value for value in checked_sensitivity if value is not None
            )
        elif name == "conditional_sensitivity" and checked_sensitivity:
            observations = ()
        else:
            observations = tuple(
                value
                for fields in field_sources
                if (value := getattr(fields, name)) is not None
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
        if (
            name == "sensitivity"
            and not explicitly_withheld
            and (("value", True) in values or ("value", "conditional") in values)
        ):
            resolved[name] = SourceField(status="value", value=True)
        elif (
            name == "measurement_unit"
            and len(values) == 2
            and (
                published := next(
                    (
                        published
                        for published, other in _UNIT_PAIRS
                        if values == {("value", published), ("value", other)}
                    ),
                    None,
                )
            )
            is not None
            and not explicitly_withheld
        ):
            resolved[name] = SourceField(status="value", value=published)
        elif name == "data_type" and len(values) > 1 and not explicitly_withheld:
            classes = tuple(
                data_type_class(value)
                if status == "value" and isinstance(value, str)
                else None
                for status, value in values
            )
            provenance = "Documented Datatyp: " + ", ".join(
                sorted(str(value) for status, value in values if status == "value")
            )
            if storage is not None:
                provenance += "; " + storage.provenance
            if all(kind is not None for kind in classes):
                widened_classes = frozenset(
                    kind for kind in classes if kind is not None
                )
                widened = widen_data_type_classes(widened_classes)
            else:
                widened = None
            if widened is not None:
                assert widened_classes is not None
                if (
                    widened == "text"
                    and storage is not None
                    and storage.classes
                    and storage.classes <= {"integer", "decimal"}
                ):
                    widened = widen_data_type_classes(
                        kind
                        for kind in storage.classes | widened_classes
                        if kind is not None and kind in {"integer", "decimal"}
                    )
                    assert widened is not None
                    storage_capped = True
                resolved[name] = SourceField(
                    status="value", value=widened, raw_value=provenance
                )
            else:
                widened_classes = None
                conflicts.append(name)
                resolved[name] = SourceField(status="unknown", raw_value=provenance)
        elif (
            name == "data_length"
            and widened_classes is not None
            and (len(widened_classes) > 1 or storage_capped)
        ):
            resolved[name] = SourceField(status="unknown")
        elif (
            name == "data_length"
            and len(values) > 1
            and not explicitly_withheld
            and all(
                status == "value"
                and isinstance(value, str)
                and value.isascii()
                and value.isdecimal()
                and (value == "0" or not value.startswith("0"))
                for status, value in values
            )
            and (data_type := resolved.get("data_type")) is not None
            and data_type.status == "value"
            and all(
                fields.data_type is not None
                and fields.data_type.status == "value"
                and (
                    fields.data_type.value == data_type.value
                    or (
                        widened_classes is not None
                        and len(widened_classes) == 1
                        and isinstance(fields.data_type.value, str)
                        and data_type_class(fields.data_type.value) == data_type.value
                    )
                )
                for fields in field_sources
                if fields.data_length is not None
                and fields.data_length.status == "value"
            )
        ):
            lengths = []
            for _, value in values:
                assert isinstance(value, str)
                lengths.append(int(value))
            resolved[name] = SourceField(status="value", value=str(max(lengths)))
        elif len(values) > 1 or explicitly_withheld:
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
    *,
    storage: Mapping[str, StewardColumnStorage] | None = None,
) -> OccurrenceResolution:
    """Keep independently supported facts on exact, non-overlapping column periods.

    Each physical occurrence must explicitly supply availability, a column, and a
    finite interpreted period. A range-carrying pooled scope supplies its whole
    pooled range as one interval (marked `pooled` on spans no explicit
    occurrence covers; where an explicit occurrence covers the span, pooled
    evidence is filtered out before field reconciliation so the explicit facts
    win outright); pooled scopes without a range, and unknown scopes, stay
    unplaced. Adjacent pooled cuts whose reconciled facts, population, and
    coding evidence agree re-join into one pooled segment over their combined
    window (Y-209). Unplaced occurrences stay in the result and block
    strict publication. An occurrence whose column claim is negative (a delivered
    blank column: the member has no physical column) is omitted on purpose and
    reported as an `omitted_columnless_occurrence` warning; formation aggregates
    one warning per variable. Competing fields other than sensitivity become unknown
    on their intersection; sensitivity ratchets to true for a true or conditional claim.
    Positive versus negative availability withholds that column segment entirely.
    Unknown optional observations do not contradict a supplied concrete fact.
    """
    by_column: dict[str, list[tuple[int, int, int]]] = defaultdict(list)
    source_records = tuple(effective_occurrence(record) for record in records)
    issues: list[OccurrenceIssue] = []
    unsupported: list[SourceRecord] = []
    columnless: list[SourceRecord] = []
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
        column_claim = record.fields.column_name
        if column_claim is not None and column_claim.status == "negative":
            # A delivered blank column states the member has no physical column:
            # omit the occurrence on purpose, without an error. Unknown columns
            # keep the unsupported path below.
            columnless.extend(record.evidence)
            continue
        column = _column(record)
        periods = occurrence_bounds(record)
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
    if columnless:
        issues.append(
            OccurrenceIssue(
                "omitted_columnless_occurrence",
                ("column_name",),
                tuple(columnless),
                None,
                None,
                ("occurrence",),
            )
        )

    segments = []
    negative_segments = []
    for column, periods in sorted(by_column.items()):
        column_segments: list[SourceSegment] = []
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
            # Explicit evidence wins its span outright (Y-202): pooled
            # occurrences covering the same span are filtered out BEFORE field
            # reconciliation, so their facts can neither conflict with nor
            # influence the resulting state.
            winners = (
                tuple(record for record in effective if not _occurrence_pooled(record))
                or effective
            )
            occurrences = tuple(record for item in winners for record in item.evidence)
            fields, conflicts = reconcile_source_fields(
                winners,
                storage=storage.get(fold_column(column))
                if storage is not None
                else None,
            )
            lower, upper = (
                date.fromordinal(start).isoformat(),
                date.fromordinal(end).isoformat(),
            )
            populations = {
                record.population_key
                for record in winners
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
            diagnostic_conflicts = tuple(
                name
                for name in conflicts
                if name not in _ABSORBED_OCCURRENCE_CONFLICT_FIELDS
            )
            if diagnostic_conflicts:
                issues.append(
                    OccurrenceIssue(
                        "conflicting_occurrence_facts",
                        diagnostic_conflicts,
                        occurrences,
                        lower,
                        upper,
                        ("column_segment",)
                        if "availability" in diagnostic_conflicts
                        else diagnostic_conflicts,
                    )
                )
            availability = fields.availability
            assert availability is not None
            pooled = _segment_pooled(winners)
            if availability.status == "negative" and len(populations) <= 1:
                negative_segments.append(
                    SourceSegment(
                        lower,
                        upper,
                        column,
                        fields,
                        occurrences,
                        winners,
                        pooled=pooled,
                    )
                )
            if availability.status != "value" or len(populations) > 1:
                continue
            column_segments.append(
                SourceSegment(
                    lower, upper, column, fields, occurrences, winners, pooled=pooled
                )
            )
        segments.extend(_merge_adjacent_pooled(column_segments))
    return OccurrenceResolution(
        tuple(segments), tuple(negative_segments), tuple(issues), tuple(unsupported)
    )
