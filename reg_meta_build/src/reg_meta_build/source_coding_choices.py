"""Apply finite coding choices after source binding and before catalog formation.

Original record guards and semantic coding expectations are checked independently.
A matching source list is selected only inside the reviewed interval. Other claims
remain available for accounting; an obsolete choice never refreshes itself.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING, Literal

from reg_meta_build.source_coding import (
    CodeListClaim,
    CodingIssue,
    CodingResolution,
    CodingSegment,
    coding_content_sha256,
    resolve_code_membership,
)
from reg_meta_build.source_curation import (
    CodingChoiceDecision,
    CurationCase,
    ResolutionDiagnostic,
    evaluate_cases,
)
from reg_meta_build.source_effects import _require_checked
from reg_meta_build.source_intervals import scope_bounds
from reg_meta_build.source_records import ScopeInterval, TemporalScope

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import CaseEvaluation
    from reg_meta_build.source_records import SourceRecord


@dataclass(frozen=True)
class CodingChoiceAccounting:
    case_id: str
    status: Literal["applied", "stale", "conflicted"]
    observed_codings: tuple[str, ...]
    incomplete_claims: tuple[str, ...]


@dataclass(frozen=True)
class CodingChoiceResolution:
    coding: dict[NativeKey, CodingResolution]
    evaluations: tuple[CaseEvaluation, ...]
    accounting: tuple[CodingChoiceAccounting, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


def coding_for_period(
    claims: tuple[CodeListClaim, ...], valid_from: str, valid_to: str
) -> tuple[CodeListClaim, ...]:
    """Project only independently dated claims onto an already established window.

    Unknown or pooled source scopes stay in original accounting and diagnostics;
    they cannot acquire annual meaning through a coding choice.
    """
    lower, upper = date.fromisoformat(valid_from), date.fromisoformat(valid_to)
    if lower > upper:
        raise ValueError("coding projection bounds are reversed")
    result = []
    for claim in claims:
        periods = scope_bounds(claim.scope)
        if periods is None:
            continue
        intervals = tuple(
            ScopeInterval(
                start=date.fromordinal(max(start, lower.toordinal())).isoformat(),
                end=date.fromordinal(min(end, upper.toordinal())).isoformat(),
            )
            for start, end in periods
            if start <= upper.toordinal() and end >= lower.toordinal()
        )
        if intervals:
            result.append(
                replace(
                    claim, scope=TemporalScope(kind="intervals", intervals=intervals)
                )
            )
    return tuple(result)


def _covers(resolution: CodingResolution, start: str, end: str) -> bool:
    segments = resolution.segments
    return bool(
        segments
        and not resolution.issues
        and segments[0].valid_from == start
        and segments[-1].valid_to == end
        and all(
            date.fromisoformat(left.valid_to).toordinal() + 1
            == date.fromisoformat(right.valid_from).toordinal()
            for left, right in pairwise(segments)
        )
    )


def _compose(
    base: CodingResolution,
    choices: list[tuple[str, CodingResolution]],
) -> tuple[CodingResolution, set[str]]:
    """Replace selected periods, withholding only contradictory overlap."""
    cuts = {
        point
        for segment in (*base.segments, *(s for _, r in choices for s in r.segments))
        for point in (
            date.fromisoformat(segment.valid_from).toordinal(),
            date.fromisoformat(segment.valid_to).toordinal() + 1,
        )
    }
    issues = []
    for issue in base.issues:
        if issue.valid_from is None or issue.valid_to is None:
            issues.append(issue)
        else:
            cuts.update(
                (
                    date.fromisoformat(issue.valid_from).toordinal(),
                    date.fromisoformat(issue.valid_to).toordinal() + 1,
                )
            )
    segments = []
    conflicted = set()
    for start, next_start in pairwise(sorted(cuts)):
        lower, upper = (
            date.fromordinal(start).isoformat(),
            date.fromordinal(next_start - 1).isoformat(),
        )
        selected = [
            (case_id, segment)
            for case_id, resolution in choices
            for segment in resolution.segments
            if segment.valid_from <= lower and segment.valid_to >= upper
        ]
        if not selected:
            segments.extend(
                replace(segment, valid_from=lower, valid_to=upper)
                for segment in base.segments
                if segment.valid_from <= lower and segment.valid_to >= upper
            )
            issues.extend(
                replace(issue, valid_from=lower, valid_to=upper)
                for issue in base.issues
                if issue.valid_from is not None
                and issue.valid_to is not None
                and issue.valid_from <= lower
                and issue.valid_to >= upper
            )
            continue
        alternatives = {
            (segment.code_set, segment.version_label) for _, segment in selected
        }
        claim_ids = tuple(
            sorted(
                {identity for _, segment in selected for identity in segment.claim_ids}
            )
        )
        if len(alternatives) != 1:
            case_ids = tuple(sorted(case_id for case_id, _ in selected))
            conflicted.update(case_ids)
            issues.append(
                CodingIssue(
                    "conflicting_coding_choices",
                    claim_ids,
                    lower,
                    upper,
                    case_ids=case_ids,
                )
            )
            segments.append(CodingSegment(lower, upper, None, claim_ids))
        else:
            code_set, label = next(iter(alternatives))
            provenance = tuple(
                sorted({item for _, segment in selected for item in segment.provenance})
            )
            segments.append(
                CodingSegment(lower, upper, code_set, claim_ids, label, provenance)
            )
    return CodingResolution(tuple(segments), tuple(issues), base.claims), conflicted


def apply_coding_choices(
    records: Iterable[SourceRecord],
    cases: tuple[CurationCase, ...],
    *,
    coding: Mapping[NativeKey, tuple[CodeListClaim, ...]],
) -> CodingChoiceResolution:
    """Resolve coding with exact original evidence and finite accepted choices."""
    ordered = tuple(sorted(cases, key=lambda case: case.case_id))
    if len({case.case_id for case in ordered}) != len(ordered):
        raise ValueError("coding choice case IDs must be unique")
    for case in ordered:
        if not isinstance(case.decision, CodingChoiceDecision):
            raise TypeError("coding application requires coding choice decisions")
        if case.decision.column_key not in coding:
            raise ValueError("coding choice has an unconverted column binding")
        guarded = {ref for guard in case.peer_guards for ref in guard.expected_members}
        for target in case.targets:
            _require_checked(target, ("column_name",))
            if target.ref not in guarded:
                raise ValueError("coding choices require guarded original membership")
    evaluations = evaluate_cases(ordered, records)
    resolved = {key: resolve_code_membership(claims) for key, claims in coding.items()}
    choices: dict[NativeKey, list[tuple[str, CodingResolution]]] = defaultdict(list)
    diagnostics = []
    accounting = []
    for case, evaluation in zip(ordered, evaluations, strict=True):
        decision = case.decision
        assert isinstance(decision, CodingChoiceDecision)

        def report(
            code: str,
            detail: str,
            case: CurationCase = case,
            decision: CodingChoiceDecision = decision,
        ) -> None:
            diagnostics.append(
                ResolutionDiagnostic(
                    code=code,
                    severity="error",
                    case_id=case.case_id,
                    subject=repr(decision.column_key),
                    detail=detail,
                    refs=tuple(target.ref for target in case.targets),
                    fields=("coding",),
                    valid_from=decision.valid_from,
                    valid_to=decision.valid_to,
                    withheld_output=("curation.coding_choice",),
                )
            )

        if evaluation.status == "stale":
            for issue in evaluation.issues:
                report(issue.code, issue.detail)
            accounting.append(CodingChoiceAccounting(case.case_id, "stale", (), ()))
            continue
        projected = coding_for_period(
            coding[decision.column_key], decision.valid_from, decision.valid_to
        )
        fingerprints = [(claim, coding_content_sha256(claim)) for claim in projected]
        observed = tuple(
            sorted({digest for _, digest in fingerprints if digest is not None})
        )
        incomplete = tuple(
            sorted({claim.claim_id for claim, digest in fingerprints if digest is None})
        )
        selected = resolve_code_membership(
            tuple(
                claim
                for claim, digest in fingerprints
                if digest == decision.selected_coding
            )
        )
        if incomplete or set(observed) != set(decision.expected_codings):
            report(
                "coding_evidence_changed",
                f"Expected complete codings {sorted(decision.expected_codings)!r}; observed {observed!r}; incomplete claims {incomplete!r}. No choice was applied.",
            )
            status = "stale"
        elif not _covers(selected, decision.valid_from, decision.valid_to):
            report(
                "incomplete_selected_coding",
                "The selected source coding does not cover the whole checked period.",
            )
            status = "stale"
        else:
            selected = replace(
                selected,
                segments=tuple(
                    replace(
                        segment,
                        provenance=(
                            f"{case.case_id}: {decision.reason}\n{decision.provenance}",
                        ),
                    )
                    for segment in selected.segments
                ),
            )
            choices[decision.column_key].append((case.case_id, selected))
            status = "applied"
        accounting.append(
            CodingChoiceAccounting(case.case_id, status, observed, incomplete)
        )
    conflicted = set()
    for key, selected_choices in choices.items():
        resolved[key], conflicts = _compose(resolved[key], selected_choices)
        conflicted.update(conflicts)
    return CodingChoiceResolution(
        resolved,
        evaluations,
        tuple(
            replace(item, status="conflicted") if item.case_id in conflicted else item
            for item in accounting
        ),
        tuple(diagnostics),
    )
