"""Derive group edges between already identified source-definition siblings.

No catalog identity or column equivalence is inferred here. A shared native
definition supplies group membership only after checked identity resolution.
Actual edition co-delivery controls the existing code/label and shape guards.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations, product
from typing import TYPE_CHECKING, Literal

from reg_meta_build._curation import (
    _looks_like_code_label_pair,
    sibling_shape_conflict,
)
from reg_meta_build.source_coordinates import native_variable_key
from reg_meta_build.source_curation import ResolutionDiagnostic
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_intervals import scope_bounds

if TYPE_CHECKING:
    from collections.abc import Mapping

    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import SourceRecordRef
    from reg_meta_build.source_occurrences import EffectiveOccurrence


@dataclass(frozen=True)
class SiblingPairDecision:
    family: NativeKey
    a: str
    b: str
    columns: tuple[str, str]
    kind: Literal["foldable", "code_label", "shape_conflict", "unresolved_shape"]
    refs: tuple[SourceRecordRef, ...]


@dataclass(frozen=True)
class SiblingResolution:
    pairs: tuple[tuple[str, str], ...]
    decisions: tuple[SiblingPairDecision, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


def _column(occurrence: EffectiveOccurrence) -> str | None:
    if "column_name" in occurrence.withheld_fields:
        return None
    value = occurrence.fields.column_name
    if value is not None and value.status == "value" and value.value:
        assert isinstance(value.value, str)
        return value.value
    return None


def _latest_shapes(
    occurrences: list[EffectiveOccurrence],
) -> set[tuple[str | None, str | None]]:
    """Retain all equally recent evidence; a native ID is not an authority rank."""
    ranked = []
    for occurrence in occurrences:
        bounds = scope_bounds(occurrence.edition_scope)
        latest = max(hi for _, hi in bounds) if bounds else -1
        shape = []
        for name in ("data_type", "data_length"):
            value = getattr(occurrence.fields, name)
            shape.append(
                str(value.value) if value and value.status == "value" else None
            )
        ranked.append((latest, (shape[0], shape[1])))
    maximum = max(rank for rank, _ in ranked)
    return {shape for rank, shape in ranked if rank == maximum}


def resolve_sibling_pairs(
    occurrences: tuple[EffectiveOccurrence, ...],
    *,
    identities: Mapping[NativeKey, str | None],
) -> SiblingResolution:
    """Apply grouping guards to exact existing partitions in a complete scope.

    Unknown identities remain None, never invented endpoints. Within each original
    family, each known identity contributes its lexically first literal column,
    preserving the established cluster-representative rule. Co-delivery is exact
    edition identity, never an overlap of year spans. An unresolved shape matters
    only when its alternatives disagree about whether the pair can be grouped.
    """
    families: dict[NativeKey, list[EffectiveOccurrence]] = defaultdict(list)
    for occurrence in occurrences:
        if occurrence.use != "catalog" or "identity" in occurrence.withheld_fields:
            continue
        availability = occurrence.fields.availability
        if "availability" in occurrence.withheld_fields or (
            availability is not None and availability.status == "negative"
        ):
            continue
        if (
            occurrence.variable_key is not None
            and occurrence.variable_key not in identities
        ):
            raise ValueError("sibling resolution lacks an explicit identity outcome")
        for key in {native_variable_key(r) for r in occurrence.source_records} - {None}:
            assert key is not None
            if key != occurrence.variable_key and not occurrence.identity_checked:
                raise ValueError("sibling partition lacks a checked identity decision")
            families[key].append(occurrence)
    pairs, decisions, diagnostics = set(), [], []
    for family, members in sorted(families.items(), key=lambda item: repr(item[0])):
        by_column: dict[str, list[EffectiveOccurrence]] = defaultdict(list)
        owner_columns: dict[str, set[str]] = defaultdict(set)
        editions: dict[NativeKey, set[str]] = defaultdict(set)
        for occurrence in members:
            column = _column(occurrence)
            if column is None:
                continue
            by_column[column].append(occurrence)
            if occurrence.edition_key is not None:
                editions[occurrence.edition_key].add(column)
            fqid = (
                identities.get(occurrence.variable_key)
                if occurrence.variable_key
                else None
            )
            if fqid is not None:
                owner_columns[fqid].add(column)
        codelivered = {
            frozenset(pair)
            for columns in editions.values()
            for pair in combinations(columns, 2)
        }
        if not codelivered or len(owner_columns) < 2:
            continue
        shapes = {column: _latest_shapes(rows) for column, rows in by_column.items()}
        for a, b in combinations(sorted(owner_columns), 2):
            if a.rsplit("/", 1)[0] != b.rsplit("/", 1)[0]:
                raise ValueError(
                    "one source sibling family maps to different registers"
                )
            col_a, col_b = min(owner_columns[a]), min(owner_columns[b])
            refs = tuple(
                sorted(
                    {
                        record_ref(record)
                        for column in (col_a, col_b)
                        for occurrence in by_column[column]
                        for record in occurrence.evidence
                    },
                    key=repr,
                )
            )
            kind = "foldable"
            if frozenset((col_a, col_b)) in codelivered:
                if _looks_like_code_label_pair(col_a, col_b):
                    kind = "code_label"
                else:
                    outcomes = {
                        sibling_shape_conflict(left, right)
                        for left, right in product(shapes[col_a], shapes[col_b])
                    }
                    withheld_shape = any(
                        {"data_type", "data_length"} & set(row.withheld_fields)
                        for column in (col_a, col_b)
                        for row in by_column[column]
                    )
                    if outcomes == {True} and not withheld_shape:
                        kind = "shape_conflict"
                    elif len(outcomes) > 1 or withheld_shape:
                        kind = "unresolved_shape"
                        output = f"same_definition_pair:{a}:{b}"
                        diagnostics.append(
                            ResolutionDiagnostic(
                                code="ambiguous_sibling_shape",
                                severity="error",
                                subject=output,
                                detail="The sibling grouping guard has disputed or withheld source shape evidence.",
                                refs=refs,
                                fields=("data_type", "data_length"),
                                withheld_output=(output,),
                            )
                        )
            decisions.append(
                SiblingPairDecision(family, a, b, (col_a, col_b), kind, refs)
            )
            if kind == "foldable":
                pairs.add((a, b))
    # The same accepted identities can be present in multiple original families.
    # A conflicting family must not be bypassed by an independently eligible edge.
    pairs.difference_update((d.a, d.b) for d in decisions if d.kind != "foldable")
    return SiblingResolution(tuple(sorted(pairs)), tuple(decisions), tuple(diagnostics))
