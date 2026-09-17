"""Apply exact coding decisions before catalog formation.

Selection, explicitly uncoded periods and state omission share one applicability
and conflict path. A coding extension checks both its target and finite witness;
all original claims remain available for accounting.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING, Literal

from reg_meta_build._resolved_common import covers_window
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodingIssue,
    CodingResolution,
    CodingSegment,
    coding_content_sha256,
    coding_observation_fingerprints,
    resolve_code_membership,
)
from reg_meta_build.source_curation import (
    CodingDecision,
    CodingSelection,
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
    return not resolution.issues and covers_window(
        ((s.valid_from, s.valid_to) for s in resolution.segments), start, end
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
            (segment.code_set, segment.version_label, segment.state_disposition)
            for _, segment in selected
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
                    withheld=(
                        "state"
                        if any(s.state_disposition != "include" for _, s in selected)
                        else "code_membership"
                    ),
                )
            )
            disposition = (
                "withhold"
                if any(s.state_disposition != "include" for _, s in selected)
                else "include"
            )
            segments.append(
                CodingSegment(
                    lower, upper, None, claim_ids, state_disposition=disposition
                )
            )
        else:
            code_set, label, disposition = next(iter(alternatives))
            provenance = tuple(
                sorted({item for _, segment in selected for item in segment.provenance})
            )
            segments.append(
                CodingSegment(
                    lower, upper, code_set, claim_ids, label, provenance, disposition
                )
            )
    return CodingResolution(tuple(segments), tuple(issues), base.claims), conflicted


def coding_expectations(
    claims: tuple[CodeListClaim, ...], valid_from: str, valid_to: str
) -> tuple[str, ...]:
    """Capture exact observed alternatives, including known empty/unknown lists."""
    return coding_observation_fingerprints(
        coding_for_period(claims, valid_from, valid_to)
    )


def _selection(
    decision: CodingDecision, claims: tuple[CodeListClaim, ...]
) -> tuple[CodingResolution | None, str | None]:
    selection = decision.selection
    if isinstance(selection, str):
        projected = coding_for_period(claims, decision.valid_from, decision.valid_to)
        return CodingResolution(
            (
                CodingSegment(
                    decision.valid_from,
                    decision.valid_to,
                    None,
                    tuple(sorted({claim.claim_id for claim in projected})),
                    state_disposition="omit"
                    if selection == "omit_state"
                    else "include",
                ),
            ),
            (),
            claims,
        ), None
    observed = coding_expectations(claims, selection.valid_from, selection.valid_to)
    if set(observed) != set(selection.expected_codings):
        return None, "coding_witness_changed"
    projected = coding_for_period(claims, selection.valid_from, selection.valid_to)
    selected = resolve_code_membership(
        tuple(
            claim
            for claim in projected
            if coding_content_sha256(claim) == selection.selected_coding
        )
    )
    if not _covers(selected, selection.valid_from, selection.valid_to):
        return None, "incomplete_selected_coding"
    if (selection.valid_from, selection.valid_to) != (
        decision.valid_from,
        decision.valid_to,
    ):
        alternatives = {(s.code_set, s.version_label) for s in selected.segments}
        if len(alternatives) != 1:
            return None, "nonconstant_coding_witness"
        code_set, label = next(iter(alternatives))
        selected = replace(
            selected,
            segments=(
                CodingSegment(
                    decision.valid_from,
                    decision.valid_to,
                    code_set,
                    tuple(sorted({c for s in selected.segments for c in s.claim_ids})),
                    label,
                ),
            ),
        )
    return selected, None


def apply_coding_choices(
    records: Iterable[SourceRecord],
    cases: tuple[CurationCase, ...],
    *,
    coding: Mapping[NativeKey, tuple[CodeListClaim, ...]],
) -> CodingChoiceResolution:
    """Resolve all checked coding assignments against original evidence together."""
    ordered = tuple(sorted(cases, key=lambda case: case.case_id))
    if len({case.case_id for case in ordered}) != len(ordered):
        raise ValueError("coding case IDs must be unique")
    for case in ordered:
        if not isinstance(case.decision, CodingDecision):
            raise TypeError("coding application requires coding decisions")
        if case.decision.column_key not in coding:
            raise ValueError("coding decision has an unconverted column binding")
        guarded = {ref for guard in case.peer_guards for ref in guard.expected_members}
        for target in (*case.targets, *case.support):
            _require_checked(target, ("column_name",))
            if target.ref not in guarded:
                raise ValueError("coding decisions require guarded original membership")
    evaluations = evaluate_cases(ordered, records)
    resolved = {key: resolve_code_membership(claims) for key, claims in coding.items()}
    choices: dict[NativeKey, list[tuple[str, CodingResolution]]] = defaultdict(list)
    diagnostics = []
    accounting = []
    for case, evaluation in zip(ordered, evaluations, strict=True):
        decision = case.decision
        assert isinstance(decision, CodingDecision)

        def report(
            code: str,
            detail: str,
            case: CurationCase = case,
            decision: CodingDecision = decision,
        ) -> None:
            diagnostics.append(
                ResolutionDiagnostic(
                    code=code,
                    severity="error",
                    case_id=case.case_id,
                    subject=repr(decision.column_key),
                    detail=detail,
                    refs=tuple(target.ref for target in (*case.targets, *case.support)),
                    fields=("coding",),
                    valid_from=decision.valid_from,
                    valid_to=decision.valid_to,
                    withheld_output=("curation.coding",),
                )
            )

        if evaluation.status == "stale":
            for issue in evaluation.issues:
                report(issue.code, issue.detail)
            accounting.append(CodingChoiceAccounting(case.case_id, "stale", (), ()))
            continue
        claims = coding[decision.column_key]
        projected = coding_for_period(claims, decision.valid_from, decision.valid_to)
        observed = coding_expectations(claims, decision.valid_from, decision.valid_to)
        incomplete = tuple(
            sorted(
                {
                    claim.claim_id
                    for claim in projected
                    if coding_content_sha256(claim) is None
                }
            )
        )
        if set(observed) != set(decision.expected_codings):
            report(
                "coding_evidence_changed",
                f"Expected codings {sorted(decision.expected_codings)!r}; observed {observed!r}; incomplete claims {incomplete!r}. No assignment was applied.",
            )
            status = "stale"
        else:
            selected, problem = _selection(decision, claims)
            if problem is not None:
                selection = decision.selection
                assert isinstance(selection, CodingSelection)
                report(
                    problem,
                    f"The selected coding witness in {selection.valid_from} to {selection.valid_to} changed, is incomplete, or cannot supply one constant membership for an extension.",
                )
                status = "stale"
            else:
                assert selected is not None
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
