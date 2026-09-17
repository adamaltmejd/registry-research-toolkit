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

from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedState,
)
from reg_meta_build.source_coding_choices import coding_expectations
from reg_meta_build.source_coordinates import column_identity
from reg_meta_build.source_curation import (
    CurationCase,
    RepresentationDecision,
    ResolutionDiagnostic,
    evaluate_cases,
)
from reg_meta_build.source_effects import _require_checked

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
        if not isinstance(case.decision, RepresentationDecision):
            raise TypeError(
                "representation resolution requires representation decisions"
            )
        decision = case.decision
        for column in decision.columns:
            if (
                column_identity(
                    decision.variable_key, decision.variant_key, column.column
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
                ("column_name", "data_type", "data_length", "operational_definition"),
            )
            if target.ref not in guarded or any(
                p.code_set_references is None for p in target.alternatives
            ):
                raise ValueError(
                    "representations require guarded original membership and coding references"
                )
    evaluations = evaluate_cases(ordered, records)
    diagnostics = []
    applicable = []
    for case, evaluation in zip(ordered, evaluations, strict=True):
        decision = case.decision
        assert isinstance(decision, RepresentationDecision)
        coding_changed = [
            column.column
            for column in decision.columns
            if set(
                coding_expectations(
                    coding[
                        column_identity(
                            decision.variable_key, decision.variant_key, column.column
                        )
                    ].claims,
                    decision.valid_from,
                    decision.valid_to,
                )
            )
            != set(column.expected_codings)
        ]
        if evaluation.status == "applicable" and not coding_changed:
            applicable.append(case)
        elif evaluation.status == "applicable":
            diagnostics.append(
                ResolutionDiagnostic(
                    code="representation_coding_changed",
                    severity="error",
                    case_id=case.case_id,
                    subject=repr(decision.variable_key),
                    detail=f"Original coding evidence changed for columns {coding_changed!r}; the representation decision was not applied.",
                    refs=tuple(t.ref for t in (*case.targets, *case.support)),
                    fields=("coding",),
                    valid_from=decision.valid_from,
                    valid_to=decision.valid_to,
                    withheld_output=("representations",),
                )
            )
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
                    valid_from=decision.valid_from,
                    valid_to=decision.valid_to,
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
) -> tuple[list[ResolvedState], tuple[ResolvedAlias, ...], list[ResolutionDiagnostic]]:
    """Combine agreeing metadata; never select a sibling as the source winner.

    Only applicable cases returned by resolve_representation_cases belong here.
    Every named column must establish metadata over the declared window. Physical
    aliases retain their separate declared periods; these never cut the metadata
    state into months. Missing members or conflicting groupings withhold states.
    Conflicting optional facts/coding withhold those aspects and retain the issues.
    The storage representative is a deterministic column label, never a metadata
    donor; precise structural windows select the actual delivered representation.
    """
    if not cases:
        return states, (), []
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
    aliases: dict[tuple[ResolvedVariant, str], list[ResolvedAliasWindow]] = defaultdict(
        list
    )
    for variant in sorted(
        {s.variant for s in states} | set(by_variant), key=lambda v: v.slug
    ):
        members = [s for s in states if s.variant == variant]
        reviewed = by_variant[variant]
        cuts = sorted(
            {
                point
                for item in (*members, *(d for _, d in reviewed))
                for point in (
                    date.fromisoformat(item.valid_from).toordinal(),
                    date.fromisoformat(item.valid_to).toordinal() + 1,
                )
            }
        )
        for lo, hi in pairwise(cuts):
            start, end = (
                date.fromordinal(lo).isoformat(),
                date.fromordinal(hi - 1).isoformat(),
            )
            active = [s for s in members if s.valid_from <= start and s.valid_to >= end]
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

            def report(
                code: str,
                fields: tuple[str, ...],
                withheld: tuple[str, ...],
                detail: str,
                selected: list[CurationCase] = selected,
                start: str = start,
                end: str = end,
            ) -> None:
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
                tuple(sorted((c.column, c.valid_from, c.valid_to) for c in d.columns))
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
            for field in (
                "data_type",
                "data_length",
                "operational_definition",
                "source_register_text",
            ):
                alternatives = {getattr(s, field) for s in represented}
                values[field] = (
                    next(iter(alternatives)) if len(alternatives) == 1 else None
                )
                if len(alternatives) != 1:
                    report(
                        "conflicting_representation_fact",
                        (field,),
                        (f"state.{field}",),
                        "Parallel representations disagree; no sibling's fact was selected.",
                    )
            code_values = {
                (
                    s.value_set,
                    s.value_set_version_label,
                    s.classification,
                    s.conformance,
                )
                for s in represented
            }
            if len(code_values) == 1:
                codes, label, classification, conformance = next(iter(code_values))
            else:
                codes, label, classification, conformance = None, "", None, None
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
                    **values,
                    value_set=codes,
                    value_set_version_label=label,
                    classification=classification,
                    conformance=conformance,
                    provenance=provenance,
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
    )
