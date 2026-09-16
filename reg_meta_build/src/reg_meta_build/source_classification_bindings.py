"""Apply checked classification declarations before state/representation formation."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING

from reg_meta_build.source_classifications import resolve_classification_conformance
from reg_meta_build.source_coding import CodingIssue, CodingResolution, CodingSegment
from reg_meta_build.source_coding_choices import coding_expectations
from reg_meta_build.source_curation import (
    ClassificationDecision,
    CurationCase,
    ResolutionDiagnostic,
    evaluate_cases,
)
from reg_meta_build.source_effects import _require_checked
from reg_meta_build.source_records import canonical_sha256

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.resolved_catalog import ResolvedClassification
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import CaseEvaluation
    from reg_meta_build.source_records import SourceRecord


@dataclass(frozen=True)
class ClassificationBindingResolution:
    coding: dict[NativeKey, CodingResolution]
    evaluations: tuple[CaseEvaluation, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


def classification_content_sha256(classification: ResolvedClassification) -> str:
    """Pin semantic codebook content, independently of member order and succession."""
    body = classification.model_dump(mode="json", exclude={"codes", "supersedes"})
    body["codes"] = [
        code.model_dump(mode="json")
        for code in sorted(classification.codes, key=lambda c: (c.code, c.label))
    ]
    return canonical_sha256(body)


def apply_classification_cases(
    records: Iterable[SourceRecord],
    cases: tuple[CurationCase, ...],
    *,
    coding: Mapping[NativeKey, CodingResolution],
    classifications: Mapping[str, ResolvedClassification],
) -> ClassificationBindingResolution:
    """Compose finite declarations against original source and codebook evidence.

    Coding choices run first; their original claims remain the applicability
    basis. Classification never adds availability, copies canonical memberships,
    chooses an identity, or acknowledges noncanonical codes. Conflicting declared
    classes withhold only the binding on their intersection. Existing coding and
    omission diagnostics survive unchanged. An inline-only declaration has no
    effect where independently resolved inline codes are absent.
    """
    ordered = tuple(sorted(cases, key=lambda c: c.case_id))
    if len({c.case_id for c in ordered}) != len(ordered):
        raise ValueError("classification case IDs must be unique")
    for case in ordered:
        if not isinstance(case.decision, ClassificationDecision):
            raise TypeError("classification application requires classification cases")
        decision = case.decision
        if decision.column_key not in coding:
            raise ValueError("classification has an unconverted column identity")
        if decision.classification not in classifications:
            raise ValueError("classification has an unconverted canonical codebook")
        if classifications[decision.classification].slug != decision.classification:
            raise ValueError("classification mapping key differs from its identity")
        if any(
            s.classification is not None or s.conformance is not None
            for s in coding[decision.column_key].segments
        ):
            raise ValueError("classification cases must compose in one application")
        guarded = {ref for guard in case.peer_guards for ref in guard.expected_members}
        for target in (*case.targets, *case.support):
            _require_checked(target, ("column_name",))
            if target.ref not in guarded or any(
                projection.code_set_references is None
                for projection in target.alternatives
            ):
                raise ValueError(
                    "classification requires guarded original coding evidence"
                )
    evaluations = evaluate_cases(ordered, records)
    selected: dict[NativeKey, list[CurationCase]] = defaultdict(list)
    diagnostics: list[ResolutionDiagnostic] = []
    class_hashes = {}
    canonical = {}
    for case, evaluation in zip(ordered, evaluations, strict=True):
        decision = case.decision
        assert isinstance(decision, ClassificationDecision)
        classification = classifications[decision.classification]
        if classification.slug not in class_hashes:
            class_hashes[classification.slug] = classification_content_sha256(
                classification
            )
            canonical[classification.slug] = frozenset(
                c.code for c in classification.codes
            )
        for issue in evaluation.issues:
            diagnostics.append(
                ResolutionDiagnostic(
                    code=issue.code,
                    severity="error",
                    subject=repr(decision.column_key),
                    case_id=case.case_id,
                    detail=issue.detail,
                    applicability_issue=issue,
                    refs=tuple(t.ref for t in (*case.targets, *case.support)),
                    fields=("classification",),
                    valid_from=decision.valid_from,
                    valid_to=decision.valid_to,
                    withheld_output=("state.classification",),
                )
            )
        if evaluation.status != "applicable":
            continue
        observed = coding_expectations(
            coding[decision.column_key].claims, decision.valid_from, decision.valid_to
        )
        if set(observed) != set(decision.expected_codings) or (
            class_hashes[classification.slug] != decision.expected_classification
        ):
            diagnostics.append(
                ResolutionDiagnostic(
                    code="classification_evidence_changed",
                    severity="error",
                    subject=repr(decision.column_key),
                    case_id=case.case_id,
                    detail="Original coding or the selected canonical codebook changed; "
                    "the existing classification decision was not replayed. "
                    + decision.provenance,
                    refs=tuple(t.ref for t in (*case.targets, *case.support)),
                    fields=("coding", "classification"),
                    valid_from=decision.valid_from,
                    valid_to=decision.valid_to,
                    withheld_output=("state.classification",),
                )
            )
            continue
        selected[decision.column_key].append(case)

    result = dict(coding)
    for key, applicable in selected.items():
        base = coding[key]
        decisions: list[tuple[CurationCase, ClassificationDecision]] = []
        for case in applicable:
            assert isinstance(case.decision, ClassificationDecision)
            decisions.append((case, case.decision))
        cuts = sorted(
            {
                point
                for item in (*base.segments, *(d for _, d in decisions))
                for point in (
                    date.fromisoformat(item.valid_from).toordinal(),
                    date.fromisoformat(item.valid_to).toordinal() + 1,
                )
            }
        )
        segments = []
        coding_issues = list(base.issues)
        for lo, hi in pairwise(cuts):
            start, end = (
                date.fromordinal(lo).isoformat(),
                date.fromordinal(hi - 1).isoformat(),
            )
            prior = [
                s for s in base.segments if s.valid_from <= start and s.valid_to >= end
            ]
            if len(prior) > 1:
                raise ValueError("classification received overlapping coding segments")
            segment = (
                replace(prior[0], valid_from=start, valid_to=end)
                if prior
                else CodingSegment(start, end, None, ())
            )
            active = [
                (c, d)
                for c, d in decisions
                if d.valid_from <= start
                and d.valid_to >= end
                and (d.binding_scope == "declared" or segment.code_set is not None)
            ]
            if not active or segment.state_disposition != "include":
                if prior:
                    segments.append(segment)
                continue
            refs = tuple(
                sorted(
                    {t.ref for c, _ in active for t in (*c.targets, *c.support)},
                    key=repr,
                )
            )
            classes = {d.classification for _, d in active}
            if len(classes) > 1:
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="conflicting_classification_decisions",
                        severity="error",
                        subject=repr(key),
                        detail=f"Accepted declarations select different classifications {sorted(classes)!r}; cases {[c.case_id for c, _ in active]!r}.",
                        refs=refs,
                        fields=("classification",),
                        valid_from=start,
                        valid_to=end,
                        withheld_output=("state.classification",),
                    )
                )
                if prior:
                    segments.append(segment)
                continue
            slug = next(iter(classes))
            conformance = None
            if segment.code_set is not None:
                checked = resolve_classification_conformance(
                    segment.code_set,
                    classification=slug,
                    canonical_codes=canonical[slug],
                    subject=repr(key),
                    refs=refs,
                    valid_from=start,
                    valid_to=end,
                )
                diagnostics.extend(checked.diagnostics)
                conformance = checked.conformance
                if conformance.status == "severed":
                    slug = None
            elif not prior and base.claims:
                coding_issues.append(
                    CodingIssue(
                        "missing_coding_period",
                        tuple(c.claim_id for c in base.claims),
                        start,
                        end,
                    )
                )
            attribution = tuple(
                sorted(
                    set(segment.provenance)
                    | {f"{c.case_id}: {d.reason}\n{d.provenance}" for c, d in active}
                )
            )
            segments.append(
                replace(
                    segment,
                    classification=slug,
                    conformance=conformance,
                    provenance=attribution,
                )
            )
        result[key] = CodingResolution(
            tuple(segments), tuple(coding_issues), base.claims
        )
    return ClassificationBindingResolution(result, evaluations, tuple(diagnostics))
