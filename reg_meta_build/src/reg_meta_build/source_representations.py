"""Reconcile metadata shared by explicitly declared parallel representations.

Provider formats never choose these groupings. Checked identity/name effects run
before formation; this layer preserves their broader metadata periods and emits
only the exact physical representation windows supplied by accepted curation.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING

from reg_meta_build._resolved_common import remaining_windows
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedClassificationLink,
    ResolvedCodeSet,
    ResolvedState,
)
from reg_meta_build.source_coding import copied_coding_fingerprints
from reg_meta_build.source_coordinates import column_identity
from reg_meta_build.source_curation import (
    CurationCase,
    DeliveryMetadataColumn,
    DeliveryMetadataDecision,
    RepresentationDecision,
    ResolutionDiagnostic,
    SourceEvidence,
    evaluate_cases,
)
from reg_meta_build.source_effects import _require_checked
from reg_meta_build.source_intervals import coding_scope_bounds
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import SourceFields

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.resolved_catalog import ResolvedVariant
    from reg_meta_build.source_coding import CodingResolution
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import CaseEvaluation
    from reg_meta_build.source_records import SourceRecord


@dataclass(frozen=True)
class RepresentationResolution:
    cases: tuple[CurationCase, ...]
    evaluations: tuple[CaseEvaluation, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


def resolve_representation_cases(
    records: Iterable[SourceRecord],
    cases: tuple[CurationCase, ...],
    *,
    coding: Mapping[NativeKey, CodingResolution],
) -> RepresentationResolution:
    """Check finite original evidence without changing identities or source scopes."""
    ordered = tuple(sorted(cases, key=lambda c: c.case_id))
    if len({c.case_id for c in ordered}) != len(ordered):
        raise ValueError("representation case IDs must be unique")
    for case in ordered:
        if not isinstance(
            case.decision, (RepresentationDecision, DeliveryMetadataDecision)
        ):
            raise TypeError(
                "representation resolution requires representation decisions"
            )
        decision = case.decision
        if isinstance(decision, DeliveryMetadataDecision):
            decision.require_targets(case.targets, decision.fields)
        for column in decision.columns:
            if isinstance(decision, DeliveryMetadataDecision):
                assert isinstance(column, DeliveryMetadataColumn)
                variant_key = column.variant_key
            else:
                variant_key = decision.variant_key
            if (
                column_identity(
                    decision.variable_key,
                    variant_key,
                    column.column,
                )
                not in coding
            ):
                raise ValueError(
                    "representation has an unconverted column coding binding"
                )
        guarded = {ref for guard in case.peer_guards for ref in guard.expected_members}
        for target in (*case.targets, *case.support):
            _require_checked(
                target,
                tuple(SourceFields.model_fields)
                if isinstance(decision, DeliveryMetadataDecision)
                else (
                    "column_name",
                    "data_type",
                    "data_length",
                    "operational_definition",
                )
                + (
                    (
                        "name",
                        "description",
                        "definition",
                        "measurement_unit",
                        "source_attribution",
                    )
                    if decision.column_metadata == "per_column"
                    else ()
                ),
                case_id=case.case_id,
            )
            if target.ref not in guarded or any(
                p.code_set_references is None for p in target.alternatives
            ):
                raise ValueError(
                    "representations require guarded original membership and coding references"
                )
    evidence = (
        records if isinstance(records, SourceEvidence) else SourceEvidence(records)
    )
    evaluations = evaluate_cases(ordered, evidence)
    diagnostics = []
    applicable = []
    for case, evaluation in zip(ordered, evaluations, strict=True):
        decision = case.decision
        assert isinstance(decision, (RepresentationDecision, DeliveryMetadataDecision))
        scope_changed = []
        if isinstance(decision, DeliveryMetadataDecision):
            for column in decision.columns:
                if column.source_scope is None:
                    continue
                key = column_identity(
                    decision.variable_key, column.variant_key, column.column
                )
                scopes = (
                    evidence.effective_scopes.get(key, frozenset())
                    if evidence.effective_scopes is not None
                    else frozenset(
                        record.edition_period_scope
                        if record.edition_period_scope.kind != "not_applicable"
                        else record.edition_scope
                        for record in evidence.records
                        if source_occurrence(record).column_key == key
                    )
                )
                if scopes != {column.source_scope}:
                    scope_changed.append(column.column)
        if scope_changed:
            diagnostics.append(
                ResolutionDiagnostic(
                    code="stale_delivery_metadata_scope",
                    severity="error",
                    case_id=case.case_id,
                    subject=repr(decision.variable_key),
                    detail=f"Exact supplied delivery metadata scope changed for columns {scope_changed!r}.",
                    refs=tuple(target.ref for target in case.targets),
                    fields=("period",),
                    withheld_output=("representations",),
                )
            )
        coding_changed = []
        if (
            isinstance(decision, DeliveryMetadataDecision)
            or decision.coding_metadata == "per_column"
        ):
            for column in decision.columns:
                if isinstance(decision, DeliveryMetadataDecision):
                    assert isinstance(column, DeliveryMetadataColumn)
                    variant_key = column.variant_key
                else:
                    variant_key = decision.variant_key
                lower = date.fromisoformat(column.valid_from).toordinal()
                upper = date.fromisoformat(column.valid_to).toordinal()
                resolution = coding[
                    column_identity(
                        decision.variable_key,
                        variant_key,
                        column.column,
                    )
                ]
                claims = tuple(
                    claim
                    for claim in resolution.claims
                    if (bounds := coding_scope_bounds(claim.scope)) is None
                    or any(start <= upper and end >= lower for start, end in bounds)
                )
                if copied_coding_fingerprints(claims) != column.expected_codings:
                    coding_changed.append(column.column)
        if coding_changed:
            diagnostics.append(
                ResolutionDiagnostic(
                    code="stale_representation_coding",
                    severity="error",
                    case_id=case.case_id,
                    subject=repr(decision.variable_key),
                    detail=f"Complete source coding changed for literal columns {coding_changed!r}.",
                    refs=tuple(t.ref for t in (*case.targets, *case.support)),
                    fields=("coding",),
                    valid_from=decision.valid_from
                    if isinstance(decision, RepresentationDecision)
                    else None,
                    valid_to=decision.valid_to
                    if isinstance(decision, RepresentationDecision)
                    else None,
                    withheld_output=("representations",),
                )
            )
        if (
            evaluation.status == "applicable"
            and not coding_changed
            and not scope_changed
        ):
            applicable.append(case)
        for issue in evaluation.issues:
            diagnostics.append(
                ResolutionDiagnostic(
                    code=issue.code,
                    severity="error",
                    case_id=case.case_id,
                    subject=repr(decision.variable_key),
                    detail=issue.detail,
                    applicability_issue=issue,
                    refs=tuple(t.ref for t in (*case.targets, *case.support)),
                    fields=("representations",),
                    valid_from=decision.valid_from
                    if isinstance(decision, RepresentationDecision)
                    else None,
                    valid_to=decision.valid_to
                    if isinstance(decision, RepresentationDecision)
                    else None,
                    withheld_output=("representations",),
                )
            )
    return RepresentationResolution(tuple(applicable), evaluations, tuple(diagnostics))


def form_representations(
    states: list[ResolvedState],
    cases: tuple[CurationCase, ...],
    *,
    variable_key: NativeKey,
    variants: Mapping[NativeKey, ResolvedVariant | None],
    subject: str,
) -> tuple[
    list[ResolvedState],
    tuple[ResolvedAlias, ...],
    list[ResolutionDiagnostic],
    tuple[tuple[str, str, str, str], ...],
    tuple[tuple[str, str, str, str, str], ...],
]:
    """Combine agreeing metadata; never select a sibling as the source winner.

    Only applicable cases returned by resolve_representation_cases belong here.
    Every named column must establish metadata over the declared window. Physical
    aliases retain their separate declared periods; these never cut the metadata
    state into months. Missing members or conflicting groupings withhold states.
    Conflicting optional facts/coding withhold those aspects and retain the issues.
    The storage representative is a deterministic column label, never a metadata
    donor; precise structural windows select the actual delivered representation.
    The fourth result names every (variant, column, period) a checked decision
    withholds delivery for: a period it reports as unresolved, and the periods it
    assigns to a sibling column. What it does deliver stays a checked obligation.
    The fifth result names every (variant, column, field, period) where accepted
    parallel columns disagree on one fact, so the obligation keeps its window
    but drops that exact claimed fact like a waived delivery slice.
    """
    if not cases:
        return states, (), [], (), ()
    by_variant: dict[
        ResolvedVariant, list[tuple[CurationCase, RepresentationDecision]]
    ] = defaultdict(list)
    diagnostics = []
    for case in cases:
        decision = case.decision
        if (
            not isinstance(decision, RepresentationDecision)
            or decision.variable_key != variable_key
        ):
            raise ValueError("representation case does not belong to this variable")
        if decision.variant_key not in variants:
            raise ValueError("representation has an unconverted variant identity")
        variant = variants[decision.variant_key]
        if variant is None:
            diagnostics.append(
                ResolutionDiagnostic(
                    code="withheld_representation_dependency",
                    severity="error",
                    case_id=case.case_id,
                    subject=subject,
                    detail="The declared representation's variant is unresolved.",
                    refs=tuple(t.ref for t in (*case.targets, *case.support)),
                    fields=("variant",),
                    valid_from=decision.valid_from,
                    valid_to=decision.valid_to,
                    withheld_output=("representations",),
                )
            )
            continue
        by_variant[variant].append((case, decision))
    result = []
    withheld_windows: list[tuple[str, str, str, str]] = []
    fact_conflicts: list[tuple[str, str, str, str, str]] = []
    aliases: dict[tuple[ResolvedVariant, str], list[ResolvedAliasWindow]] = defaultdict(
        list
    )
    for variant in sorted(
        {s.variant for s in states} | set(by_variant), key=lambda v: v.slug
    ):
        members = [s for s in states if s.variant == variant]
        reviewed = by_variant[variant]
        independent = [s for s in members if s.period_scope == "year_independent"]
        result.extend(independent)
        members = [s for s in members if s.period_scope == "intervals"]
        independent_columns = {s.delivery_column_name for s in independent}
        dated_reviewed = []
        for case, decision in reviewed:
            if independent_columns.intersection(c.column for c in decision.columns):
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="unsupported_representation_scope",
                        severity="error",
                        case_id=case.case_id,
                        subject=subject,
                        detail="A dated parallel-column decision cannot establish a shared window for a year-independent table.",
                        refs=tuple(t.ref for t in (*case.targets, *case.support)),
                        fields=("representations",),
                        withheld_output=("representations",),
                    )
                )
            else:
                dated_reviewed.append((case, decision))
        reviewed = dated_reviewed
        cut_points = set()
        for item in (*members, *(d for _, d in reviewed)):
            if item.valid_from is None or item.valid_to is None:
                raise ValueError("dated scope requires both calendar bounds")
            cut_points.update(
                (
                    date.fromisoformat(item.valid_from).toordinal(),
                    date.fromisoformat(item.valid_to).toordinal() + 1,
                )
            )
        cuts = sorted(cut_points)
        for lo, hi in pairwise(cuts):
            start, end = (
                date.fromordinal(lo).isoformat(),
                date.fromordinal(hi - 1).isoformat(),
            )
            active = [
                s
                for s in members
                if s.valid_from is not None
                and s.valid_to is not None
                and s.valid_from <= start
                and s.valid_to >= end
            ]
            selected_pairs = [
                (c, d)
                for c, d in reviewed
                if d.valid_from <= start and d.valid_to >= end
            ]
            selected = [c for c, _ in selected_pairs]
            if not selected:
                result.extend(
                    s.model_copy(update={"valid_from": start, "valid_to": end})
                    for s in active
                )
                continue
            decisions = [d for _, d in selected_pairs]
            first = decisions[0]
            # Reporting the slice unresolved withholds the state for every column
            # this decision speaks for, the parallel ones it never delivered too.
            unresolved = tuple(
                (variant.slug, column, start, end)
                for column in sorted(
                    {s.delivery_column_name for s in active}
                    | {c.column for d in decisions for c in d.columns}
                )
            )

            def report(
                code: str,
                fields: tuple[str, ...],
                withheld: tuple[str, ...],
                detail: str,
                selected: list[CurationCase] = selected,
                start: str = start,
                end: str = end,
                unresolved: tuple[tuple[str, str, str, str], ...] = unresolved,
            ) -> None:
                if "state" in withheld:
                    withheld_windows.extend(unresolved)
                diagnostics.append(
                    ResolutionDiagnostic(
                        code=code,
                        severity="error",
                        subject=subject,
                        fields=fields,
                        detail=detail
                        + " Cases: "
                        + ", ".join(c.case_id for c in selected),
                        refs=tuple(
                            sorted(
                                {
                                    t.ref
                                    for c in selected
                                    for t in (*c.targets, *c.support)
                                },
                                key=repr,
                            )
                        ),
                        valid_from=start,
                        valid_to=end,
                        withheld_output=withheld,
                    )
                )

            signatures = {
                (
                    d.column_metadata,
                    d.coding_metadata,
                    tuple(
                        sorted(
                            (c.column, c.valid_from, c.valid_to, c.expected_codings)
                            for c in d.columns
                        )
                    ),
                )
                for d in decisions
            }
            if len(signatures) != 1:
                report(
                    "conflicting_representation_decisions",
                    ("representations",),
                    ("state", "representations"),
                    "Accepted parallel-column declarations disagree over this metadata period.",
                )
                continue
            columns = {c.column for c in first.columns}
            represented = [s for s in active if s.delivery_column_name in columns]
            observed = {s.delivery_column_name for s in represented}
            if observed != columns:
                report(
                    "missing_representation_metadata",
                    ("representations",),
                    ("state", "representations"),
                    f"Columns lack supported metadata in the declared period: {sorted(columns - observed)!r}.",
                )
                continue
            extra = [s for s in active if s.delivery_column_name not in columns]
            if extra:
                report(
                    "uncovered_parallel_columns",
                    ("representations",),
                    ("state", "representations"),
                    "The identity has additional parallel columns outside the accepted representation group.",
                )
                continue
            values = {}
            column_facts: dict[str, dict[str, str | None]] = {
                column: {} for column in columns
            }
            for field in (
                "name",
                "description",
                "definition",
                "measurement_unit",
                "data_type",
                "data_length",
                "operational_definition",
                "source_register_text",
            ):
                alternatives = {getattr(s, field) for s in represented}
                values[field] = (
                    next(iter(alternatives)) if len(alternatives) == 1 else None
                )
                if first.column_metadata == "per_column":
                    for column in sorted(columns):
                        observed_values = {
                            getattr(s, field)
                            for s in represented
                            if s.delivery_column_name == column
                        }
                        column_facts[column][field] = (
                            next(iter(observed_values))
                            if len(observed_values) == 1
                            else None
                        )
                        if len(observed_values) != 1:
                            report(
                                "conflicting_representation_fact",
                                (field,),
                                (f"representation.{column}.{field}",),
                                f"The literal column {column!r} has conflicting column facts; no source was selected.",
                            )
                            fact_conflicts.append(
                                (variant.slug, column, field, start, end)
                            )
                    continue
                if field == "definition":
                    # Shared definition conflicts retain the existing variable-level error.
                    # Only checked per-column mode preserves differing literal texts.
                    continue
                if len(alternatives) != 1:
                    report(
                        "conflicting_representation_fact",
                        (field,),
                        (f"state.{field}",),
                        "Parallel representations disagree; no sibling's fact was selected.",
                    )
                    fact_conflicts.extend(
                        (variant.slug, column, field, start, end)
                        for column in sorted(columns)
                    )
            code_values = {
                (
                    s.value_set,
                    s.value_set_version_label,
                    s.classification_links,
                )
                for s in represented
            }
            column_coding: dict[
                str, tuple[ResolvedCodeSet, str, tuple[ResolvedClassificationLink, ...]]
            ] = {}
            if first.coding_metadata == "per_column":
                # Each literal keeps its independently checked domain and books.
                invalid_coding = False
                for column in sorted(columns):
                    alternatives = {
                        (
                            s.value_set,
                            s.value_set_version_label,
                            s.classification_links,
                        )
                        for s in represented
                        if s.delivery_column_name == column
                    }
                    if len(alternatives) != 1 or any(
                        domain is None for domain, _, _ in alternatives
                    ):
                        invalid_coding = True
                        report(
                            "unsupported_representation_coding",
                            ("coding",),
                            (f"representation.{column}.value_set",),
                            f"Literal column {column!r} needs one complete finite domain; no coding was selected.",
                        )
                    else:
                        domain, native_label, links = next(iter(alternatives))
                        assert domain is not None
                        column_coding[column] = (domain, native_label, links)
                if invalid_coding:
                    continue
                codes, label, classification_links = None, "", ()
            elif len(code_values) == 1:
                codes, label, classification_links = next(iter(code_values))
            else:
                codes, label, classification_links = None, "", ()
                fact_conflicts.extend(
                    (variant.slug, column, "coding", start, end)
                    for column in sorted(columns)
                )
                report(
                    "conflicting_representation_coding",
                    ("coding",),
                    ("state.value_set", "state.classification"),
                    "Parallel representations do not establish one common coding.",
                )
            attribution = tuple(
                sorted(
                    {
                        f"{c.case_id}: {d.reason}\n{d.provenance}"
                        for c, d in selected_pairs
                    }
                )
            )
            provenance = "\n\n".join(
                sorted(
                    {s.provenance for s in represented if s.provenance}
                    | set(attribution)
                )
            )
            participating = [
                c for c in first.columns if c.valid_from <= end and c.valid_to >= start
            ]
            covering = [
                c for c in participating if c.valid_from <= start and c.valid_to >= end
            ]
            # Coding can split metadata inside one representation window. The
            # stored base must participate in that slice, and an exact source
            # window must remain the base when one covers the complete slice.
            representative = min(c.column for c in (covering or participating))
            result.append(
                ResolvedState(
                    variant=variant,
                    valid_from=start,
                    valid_to=end,
                    delivery_column_name=representative,
                    data_type=values["data_type"],
                    data_length=values["data_length"],
                    name=values["name"],
                    description=values["description"],
                    definition=values["definition"],
                    measurement_unit=values["measurement_unit"],
                    operational_definition=values["operational_definition"],
                    source_register_text=values["source_register_text"],
                    value_set=codes,
                    value_set_version_label=label,
                    classification_links=classification_links,
                    provenance=provenance,
                    pooled=all(s.pooled for s in represented),
                )
            )
            for column in first.columns:
                lower, upper = max(start, column.valid_from), min(end, column.valid_to)
                if lower <= upper:
                    aliases[variant, column.column].append(
                        ResolvedAliasWindow(
                            valid_from=lower,
                            valid_to=upper,
                            # The catalog distinguishes structural representation windows
                            # from additive corrections by null window provenance. The
                            # complete curation attribution lives on the shared state.
                            provenance=None,
                            column_metadata=first.column_metadata,
                            coding_metadata=first.coding_metadata,
                            value_set=column_coding[column.column][0]
                            if column.column in column_coding
                            else None,
                            value_set_version_label=column_coding[column.column][1]
                            if column.column in column_coding
                            else "",
                            classification_links=column_coding[column.column][2]
                            if column.column in column_coding
                            else (),
                            **column_facts[column.column],
                        )
                    )
                # Outside its declared window a sibling delivers this slice instead,
                # so only the declared part stays this column's delivery obligation.
                withheld_windows.extend(
                    (variant.slug, column.column, lost_from, lost_to)
                    for lost_from, lost_to in remaining_windows(
                        ((column.valid_from, column.valid_to),), start, end
                    )
                )
    return (
        result,
        tuple(
            ResolvedAlias(
                variant=variant, delivery_column_name=column, windows=tuple(windows)
            )
            for (variant, column), windows in sorted(
                aliases.items(), key=lambda item: (item[0][0].slug, item[0][1])
            )
        ),
        diagnostics,
        tuple(withheld_windows),
        tuple(fact_conflicts),
    )
