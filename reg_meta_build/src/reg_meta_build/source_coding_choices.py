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
from typing import TYPE_CHECKING, Literal, cast

from reg_meta.source_evidence import canonical_sha256

from reg_meta_build._resolved_common import covers_window
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    CodingIssue,
    CodingResolution,
    CodingSegment,
    coding_content_sha256,
    coding_observation_fingerprints,
    coding_source_sha256,
    copied_coding_fingerprints,
    resolve_code_membership,
)
from reg_meta_build.source_curation import (
    CodingDecision,
    CodingSelection,
    CurationCase,
    DocumentedCodingSelection,
    ResolutionDiagnostic,
    SourceEvidence,
    SupportedCodingAssociation,
    evaluate_cases,
)
from reg_meta_build.source_effects import _require_checked
from reg_meta_build.source_intervals import coding_scope_bounds
from reg_meta_build.source_records import ScopeInterval, SourceFields, TemporalScope

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.curation_tree import (
        CodingChoiceEntry,
        CodingDocumentedEntry,
        CodingEntry,
        CodingExtendEntry,
    )
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

    Unknown scopes and pooled scopes without a range stay in original accounting
    and diagnostics; they cannot acquire annual meaning through a coding choice.
    A range-carrying pooled scope projects over its whole pooled range, clamped
    to the window — never as annual membership inside the range.
    """
    lower, upper = date.fromisoformat(valid_from), date.fromisoformat(valid_to)
    if lower > upper:
        raise ValueError("coding projection bounds are reversed")
    result = []
    for claim in claims:
        periods = coding_scope_bounds(claim.scope)
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
        (
            (s.valid_from, s.valid_to)
            for s in resolution.segments
            if s.period_scope == "intervals"
            and s.valid_from is not None
            and s.valid_to is not None
        ),
        start,
        end,
    )


def _compose(
    base: CodingResolution,
    choices: list[tuple[str, CodingResolution]],
) -> tuple[CodingResolution, set[str]]:
    """Replace selected periods, withholding only contradictory overlap."""
    if not choices:
        return base, set()
    all_segments = (*base.segments, *(s for _, r in choices for s in r.segments))
    if any(segment.period_scope != "intervals" for segment in all_segments):
        raise ValueError("Dated coding choices cannot rewrite independent coding")
    for segment in all_segments:
        assert segment.valid_from is not None and segment.valid_to is not None
    cuts = {
        point
        for segment in all_segments
        if segment.valid_from is not None and segment.valid_to is not None
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
            if segment.valid_from is not None
            and segment.valid_to is not None
            and segment.valid_from <= lower
            and segment.valid_to >= upper
        ]
        if not selected:
            segments.extend(
                replace(segment, valid_from=lower, valid_to=upper)
                for segment in base.segments
                if segment.valid_from is not None
                and segment.valid_to is not None
                and segment.valid_from <= lower
                and segment.valid_to >= upper
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


def _complete_lists(
    claims: tuple[CodeListClaim, ...], start: str, end: str
) -> tuple[tuple[str, str, frozenset[tuple[str, str]] | None], ...]:
    """Complete nonempty lists in one window, deduplicated by semantic content."""
    found = {}
    for claim in coding_for_period(claims, start, end):
        digest = coding_content_sha256(claim)
        if digest is None:
            continue
        resolved = resolve_code_membership((claim,))
        if not _covers(resolved, start, end):
            continue
        member_sets = {
            frozenset(segment.code_set.members)
            for segment in resolved.segments
            if segment.code_set is not None and segment.code_set.members
        }
        if not member_sets or any(
            segment.code_set is None or not segment.code_set.members
            for segment in resolved.segments
        ):
            continue
        found[digest] = (
            claim.version_label or "",
            next(iter(member_sets)) if len(member_sets) == 1 else None,
        )
    return tuple((digest, *found[digest]) for digest in sorted(found))


def _supported_association(
    selection: SupportedCodingAssociation,
    claims: tuple[CodeListClaim, ...],
    start: str,
    end: str,
) -> tuple[CodingResolution | None, str | None]:
    if tuple(sorted({coding_source_sha256(claim) for claim in claims})) != tuple(
        sorted(selection.expected_source_codings)
    ):
        return None, "support_coding_evidence_changed"
    matches = []
    authority = []
    for ci, claim in enumerate(claims):
        for mi, member in enumerate(claim.members):
            for ai, association in enumerate(member.associations):
                receipt = coding_source_sha256(association)
                if (
                    (member.code, member.label) == (selection.code, selection.label)
                    and association.locator == selection.association
                    and receipt == selection.expected_association
                ):
                    matches.append((ci, mi, ai))
                if (
                    (member.code, member.label)
                    == (selection.authority_code, selection.authority_label)
                    and association.locator == selection.authority_association
                    and receipt == selection.expected_authority_association
                ):
                    authority.append((ci, mi))
    if len(matches) != 1 or len(authority) != 1:
        return None, "support_association_or_authority_changed"
    ci, mi, ai = matches[0]
    aci, ami = authority[0]
    if ci != aci or not _covers(
        resolve_code_membership(
            (replace(claims[aci], members=(claims[aci].members[ami],)),)
        ),
        start,
        end,
    ):
        return None, "support_authority_window_changed"
    members = []
    for index, member in enumerate(claims[ci].members):
        if index == mi:
            associations = member.associations[:ai] + member.associations[ai + 1 :]
            if associations:
                members.append(replace(member, associations=associations))
        else:
            members.append(member)
    effective = tuple(
        replace(claim, members=tuple(members)) if index == ci else claim
        for index, claim in enumerate(claims)
    )
    resolved = resolve_code_membership(coding_for_period(effective, start, end))
    if not _covers(resolved, start, end):
        return None, "support_projection_still_conflicted"
    return replace(resolved, claims=claims), None


def compile_coding_selection(
    entry: CodingEntry | CodingChoiceEntry | CodingExtendEntry | CodingDocumentedEntry,
    kind: str,
    claims: tuple[CodeListClaim, ...],
    start: str,
    end: str,
) -> tuple[
    CodingSelection
    | DocumentedCodingSelection
    | SupportedCodingAssociation
    | Literal["uncoded", "omit_state"]
    | None,
    str,
    str,
]:
    """Check a literal coding declaration against the current scope's claims."""
    if kind == "support":
        selection = SupportedCodingAssociation.model_validate(
            {
                name: getattr(entry, name)
                for name in SupportedCodingAssociation.model_fields
            }
        )
        _, problem = _supported_association(selection, claims, start, end)
        return (
            (selection, "matched", "") if problem is None else (None, "stale", problem)
        )
    complete = _complete_lists(claims, start, end)
    if kind == "documented":
        if any(
            segment.code_set is not None and segment.code_set.members
            for claim in coding_for_period(claims, start, end)
            for segment in resolve_code_membership((claim,)).segments
        ):
            return None, "stale", "period already has a complete source list"
        documented = cast("CodingDocumentedEntry", entry)
        return (
            DocumentedCodingSelection(
                members=documented.members,
                version_label=documented.version_label,
                expected_source_codings=tuple(documented.source_authority.codings)
                if documented.source_authority is not None
                else None,
            ),
            "matched",
            "",
        )
    if kind in {"uncoded", "omit"}:
        if complete:
            return None, "stale", "period has a complete nonempty list"
        return ("uncoded" if kind == "uncoded" else "omit_state"), "matched", ""
    if kind == "choice":
        choice = cast("CodingChoiceEntry", entry)
        if len(complete) <= 1:
            return None, "stale", "period is no longer contested by complete lists"
        keep = choice.keep
        members = (
            frozenset(map(tuple, choice.keep_members))
            if choice.keep_members is not None
            else None
        )
        chosen = [
            digest
            for digest, label, claim_members in complete
            if label == keep and (members is None or members == claim_members)
        ]
        if not chosen:
            return None, "stale", "kept list no longer matches a complete list"
        if len(chosen) != 1:
            return None, "over_broad", "kept label matches multiple complete lists"
        others = {label for digest, label, _ in complete if digest != chosen[0]}
        if others != set(choice.over):
            return None, "stale", "competing list labels differ from over"
        return (
            CodingSelection(
                valid_from=start,
                valid_to=end,
                expected_codings=coding_expectations(claims, start, end),
                selected_coding=chosen[0],
            ),
            "matched",
            "",
        )
    assert kind == "extend"
    extension = cast("CodingExtendEntry", entry)
    members = (
        frozenset(map(tuple, extension.list_members))
        if extension.list_members is not None
        else None
    )
    witness_from, witness_to = extension.witness
    complete_witness = _complete_lists(claims, witness_from, witness_to)
    all_sets = {
        claim_members
        for _, label, claim_members in complete_witness
        if label == extension.list and claim_members is not None
    }
    matching = {item for item in all_sets if members is None or item == members}
    if not matching:
        return None, "stale", "extended list has no complete member set"
    if len(matching) != 1:
        return None, "over_broad", "extended label matches multiple member sets"
    witness = [
        digest
        for digest, label, claim_members in complete_witness
        if label == extension.list and claim_members in matching
    ]
    if not witness:
        return None, "stale", "witness no longer carries the extended list"
    if len(witness) != 1:
        return None, "over_broad", "witness matches multiple complete lists"
    if complete:
        return None, "stale", "extension period has a complete nonempty list"
    return (
        CodingSelection(
            valid_from=witness_from,
            valid_to=witness_to,
            expected_codings=coding_expectations(claims, witness_from, witness_to),
            selected_coding=witness[0],
        ),
        "matched",
        "",
    )


def _selection(
    decision: CodingDecision, claims: tuple[CodeListClaim, ...]
) -> tuple[CodingResolution | None, str | None]:
    selection = decision.selection
    if isinstance(selection, SupportedCodingAssociation):
        return _supported_association(
            selection, claims, decision.valid_from, decision.valid_to
        )
    if isinstance(selection, DocumentedCodingSelection):
        if selection.expected_source_codings is not None and copied_coding_fingerprints(
            claims
        ) != tuple(sorted(selection.expected_source_codings)):
            return None, "documented_source_coding_changed"
        scope = TemporalScope(
            kind="intervals",
            intervals=(
                ScopeInterval(start=decision.valid_from, end=decision.valid_to),
            ),
        )
        documented = resolve_code_membership(
            (
                CodeListClaim(
                    canonical_sha256(
                        (
                            "documented",
                            selection.version_label,
                            sorted(selection.members),
                            decision.provenance,
                            decision.valid_from,
                            decision.valid_to,
                        )
                    ),
                    scope,
                    tuple(
                        CodeMembershipClaim(
                            code, label, TemporalScope(kind="year_independent")
                        )
                        for code, label in selection.members
                    ),
                    version_label=selection.version_label,
                ),
            )
        )
        return replace(documented, claims=claims), None
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
    records: Iterable[SourceRecord] | SourceEvidence,
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
            _require_checked(
                target,
                tuple(SourceFields.model_fields)
                if isinstance(
                    case.decision.selection,
                    (DocumentedCodingSelection, SupportedCodingAssociation),
                )
                else ("column_name",),
                case_id=case.case_id,
            )
            if isinstance(
                case.decision.selection,
                (DocumentedCodingSelection, SupportedCodingAssociation),
            ) and any(
                alternative.edition_scope is None
                or alternative.edition_period_scope is None
                or alternative.code_set_references is None
                for alternative in target.alternatives
            ):
                raise ValueError(
                    "documented coding requires checked source scopes and coding references"
                )
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
        if isinstance(records, SourceEvidence) and records.effective_scopes is not None:
            windows = (
                (date.fromordinal(lo).isoformat(), date.fromordinal(hi).isoformat())
                for scope in records.effective_scopes.get(decision.column_key, ())
                for lo, hi in (coding_scope_bounds(scope) or ())
            )
            if not covers_window(windows, decision.valid_from, decision.valid_to):
                report(
                    "coding_delivery_changed",
                    "The checked coding window no longer has complete effective column delivery.",
                )
                accounting.append(CodingChoiceAccounting(case.case_id, "stale", (), ()))
                continue
        claims = coding[decision.column_key]
        if any(claim.scope.kind == "year_independent" for claim in claims):
            report(
                "coding_scope_changed",
                "Dated coding choices cannot rewrite independent source membership.",
            )
            accounting.append(CodingChoiceAccounting(case.case_id, "stale", (), ()))
            continue
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
                report(
                    problem,
                    "The checked coding selection or complete source-association evidence changed; no assignment was applied.",
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
                if isinstance(decision.selection, SupportedCodingAssociation):
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code="supported_erroneous_coding_association",
                            severity="warning",
                            case_id=case.case_id,
                            subject=repr(decision.column_key),
                            detail=f"Original assertion {decision.selection.association} retained as support only. {decision.reason}",
                            valid_from=decision.valid_from,
                            valid_to=decision.valid_to,
                        )
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
