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
    documented_labels_match,
    evaluate_cases,
)
from reg_meta_build.source_effects import _require_checked
from reg_meta_build.source_intervals import coding_scope_bounds
from reg_meta_build.source_records import ScopeInterval, SourceFields, TemporalScope
from reg_meta_build.source_value_bindings import _member_scope

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


def documented_source_members(
    claims: tuple[CodeListClaim, ...],
    version_label: str,
    *,
    reviewed_labels: bool,
    witness: tuple[str, str] | None = None,
) -> tuple[tuple[str | None, str | None], ...]:
    """Bound reviewed label equivalence to one exact positively supplied book."""
    if witness is not None:
        claims = tuple(
            c
            for c in coding_for_period(claims, *witness)
            if c.version_label == version_label
        )
        windows = [
            (date.fromordinal(lo).isoformat(), date.fromordinal(hi).isoformat())
            for c in claims
            for lo, hi in (coding_scope_bounds(c.scope) or ())
        ]
        keys = [{m.code for m in c.members} for c in claims]
        if (
            not claims
            or not covers_window(windows, *witness)
            or not keys[0]
            or any(k != keys[0] for k in keys)
            or any(
                m.label is None
                or not m.label.strip()
                or m.unknown_validity
                or (
                    m.scope.kind != "year_independent"
                    and not covers_window(
                        [
                            (
                                date.fromordinal(lo).isoformat(),
                                date.fromordinal(hi).isoformat(),
                            )
                            for lo, hi in (coding_scope_bounds(m.scope) or ())
                        ],
                        *witness,
                    )
                )
                for c in claims
                for m in c.members
            )
        ):
            return ()
    return tuple(
        (member.code, member.label)
        for claim in claims
        if not reviewed_labels or claim.version_label == version_label
        for member in claim.members
    )


def _documented_targets_match(
    claims: tuple[CodeListClaim, ...],
    selection: DocumentedCodingSelection,
    start: str,
    end: str,
) -> bool:
    """A witnessed label certificate cannot replace a contrary target domain."""
    selected = dict(selection.members)
    aliases = {entry.code: entry for entry in selection.label_equivalences}
    for claim in coding_for_period(claims, start, end):
        if not claim.members:
            continue
        normalized = []
        for member in claim.members:
            alias = aliases.get(member.code)
            if member.code not in selected or not (
                member.label == selected[member.code]
                or alias is not None
                and member.label in alias.labels
            ):
                return False
            normalized.append(replace(member, label=selected[member.code]))
        resolved = resolve_code_membership((replace(claim, members=tuple(normalized)),))
        if any(
            segment.code_set is None
            or set(segment.code_set.members) != set(selection.members)
            for segment in resolved.segments
        ):
            return False
    return True


def documented_period_block(
    claims: tuple[CodeListClaim, ...], anchor: str
) -> tuple[TemporalScope, tuple[tuple[str, str], ...]] | None:
    """Read one reviewed block from its positive physical-row period anchor.

    This opt-in authority never changes the original membership scopes. Every
    member must have one physical row in one sheet; blank periods can belong only
    to the preceding positively dated row until the next dated row.
    """
    if len(claims) != 1 or not claims[0].members:
        return None
    members = claims[0].members
    if any(
        len(member.associations) != 1
        or member.code is None
        or member.label is None
        or not member.label.strip()
        or member.unknown_validity
        or member.validity
        for member in members
    ):
        return None
    rows = sorted(members, key=lambda member: member.associations[0].row_number)
    associations = tuple(member.associations[0] for member in rows)
    if (
        len({(row.source_file, row.source_table) for row in associations}) != 1
        or associations[0].source_table is None
        or len({row.row_number for row in associations}) != len(associations)
    ):
        return None
    current_scope = None
    selected_scope = None
    selected = []
    selected_block = False
    for member, row in zip(rows, associations, strict=True):
        if row.section_window is not None or row.section_period is not None:
            return None
        if row.supplied_period is not None:
            window = row.supplied_window
            if (
                not row.supplied_period.strip()
                or window is None
                or window.status != "known"
                or window.start is None
            ):
                return None
            current_scope, issue = _member_scope(
                claims[0].scope,
                window,
                None,
                (),
                missing_validity="unrestricted",
                invalid_item=False,
            )
            if (
                issue is not None
                or current_scope is None
                or member.scope != current_scope
            ):
                return None
            selected_block = row.locator == anchor
            if selected_block:
                selected_scope = current_scope
        elif (
            current_scope is None
            or row.supplied_window is not None
            or member.scope.kind != "year_independent"
        ):
            return None
        if selected_block:
            assert member.code is not None and member.label is not None
            selected.append((member.code, member.label))
    if (
        selected_scope is None
        or not selected
        or len({c for c, _ in selected}) != len(selected)
    ):
        return None
    return selected_scope, tuple(selected)


def documented_period_block_matches(
    claims: tuple[CodeListClaim, ...],
    anchor: str,
    members: tuple[tuple[str, str], ...],
    start: str,
    end: str,
    source_scope: TemporalScope | None = None,
) -> bool:
    block = documented_period_block(claims, anchor)
    return (
        block is not None
        and block[1] == members
        and (source_scope is None or block[0] == source_scope)
        and covers_window(
            (
                (date.fromordinal(lo).isoformat(), date.fromordinal(hi).isoformat())
                for lo, hi in coding_scope_bounds(block[0]) or ()
            ),
            start,
            end,
        )
    )


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
        documented = cast("CodingDocumentedEntry", entry)
        witnessed_labels = (
            documented.source_authority is not None
            and documented.source_authority.witness is not None
        )
        period_block = (
            documented.source_authority.period_block
            if documented.source_authority is not None
            else None
        )
        own_certificate = (
            documented.source_authority is not None
            and bool(documented.source_authority.raw_codings)
            and documented.source_authority.enumeration is None
            and not documented.source_authority.label_equivalences
            and period_block is None
        )
        if (
            not witnessed_labels
            and period_block is None
            and not own_certificate
            and any(
                segment.code_set is not None and segment.code_set.members
                for claim in coding_for_period(claims, start, end)
                for segment in resolve_code_membership((claim,)).segments
            )
        ):
            return None, "stale", "period already has a complete source list"
        selection = DocumentedCodingSelection(
            period_block=documented.source_authority.period_block
            if documented.source_authority is not None
            else None,
            members=documented.members,
            version_label=documented.version_label,
            source_scope=documented.source_authority.source_scope
            if documented.source_authority is not None
            else None,
            enumeration=documented.source_authority.enumeration
            if documented.source_authority is not None
            else None,
            expected_marker_bindings=tuple(
                sorted(documented.source_authority.marker_bindings)
            )
            if documented.source_authority is not None
            and documented.source_authority.marker_bindings is not None
            else None,
            expected_source_codings=tuple(documented.source_authority.codings)
            if documented.source_authority is not None
            else None,
            expected_raw_codings=tuple(sorted(documented.source_authority.raw_codings))
            if documented.source_authority is not None
            and documented.source_authority.raw_codings is not None
            else None,
            witness=documented.source_authority.witness
            if documented.source_authority is not None
            else None,
            label_equivalences=tuple(documented.source_authority.label_equivalences)
            if documented.source_authority is not None
            else (),
        )
        if own_certificate:
            source = resolve_code_membership(coding_for_period(claims, start, end))
            if (
                not _covers(source, start, end)
                or not _documented_targets_match(claims, selection, start, end)
                or any(
                    segment.version_label != selection.version_label
                    for segment in source.segments
                )
            ):
                return (
                    None,
                    "stale",
                    "complete own source domain changed or lacks coverage",
                )
        if selection.period_block is not None and not documented_period_block_matches(
            claims,
            selection.period_block,
            selection.members,
            start,
            end,
            selection.source_scope,
        ):
            return None, "stale", "source period block changed or does not cover target"
        if witnessed_labels and not _documented_targets_match(
            claims, selection, start, end
        ):
            return (
                None,
                "stale",
                "target source domain contradicts witnessed label certificate",
            )
        return selection, "matched", ""
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
            if label == keep
            and (members is None or members == claim_members)
            and (
                choice.keep_members_sha256 is None
                or claim_members is not None
                and canonical_sha256(sorted(claim_members))
                == choice.keep_members_sha256
            )
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
    decision: CodingDecision,
    claims: tuple[CodeListClaim, ...],
    *,
    raw_codings: tuple[str, ...] | None = None,
) -> tuple[CodingResolution | None, str | None]:
    selection = decision.selection
    if (
        getattr(selection, "expected_raw_codings", None) is not None
        and raw_codings is None
    ):
        raw_codings = tuple(coding_source_sha256(claim) for claim in claims)
    if isinstance(selection, SupportedCodingAssociation):
        return _supported_association(
            selection, claims, decision.valid_from, decision.valid_to
        )
    if isinstance(selection, DocumentedCodingSelection):
        if selection.period_block is not None and not documented_period_block_matches(
            claims,
            selection.period_block,
            selection.members,
            decision.valid_from,
            decision.valid_to,
            selection.source_scope,
        ):
            return None, "documented_source_period_block_changed"
        if selection.witness is not None and not _documented_targets_match(
            claims, selection, decision.valid_from, decision.valid_to
        ):
            return None, "documented_target_domain_changed"
        if (
            selection.expected_raw_codings is not None
            and tuple(sorted(set(raw_codings or ()))) != selection.expected_raw_codings
        ):
            return None, "documented_source_coding_changed"
        if selection.label_equivalences and (
            selection.expected_raw_codings is None
            or tuple(sorted(set(raw_codings or ()))) != selection.expected_raw_codings
            or not documented_labels_match(
                documented_source_members(
                    claims,
                    selection.version_label,
                    reviewed_labels=True,
                    witness=selection.witness,
                ),
                selection.members,
                selection.label_equivalences,
            )
        ):
            return None, "documented_label_equivalence_changed"
        if selection.expected_source_codings is not None and copied_coding_fingerprints(
            claims
        ) != tuple(sorted(selection.expected_source_codings)):
            return None, "documented_source_coding_changed"
        if (
            selection.expected_raw_codings
            and selection.enumeration is None
            and not selection.label_equivalences
            and selection.period_block is None
        ):
            source = resolve_code_membership(
                coding_for_period(claims, decision.valid_from, decision.valid_to)
            )
            if (
                not _covers(source, decision.valid_from, decision.valid_to)
                or not _documented_targets_match(
                    claims, selection, decision.valid_from, decision.valid_to
                )
                or any(
                    segment.version_label != selection.version_label
                    for segment in source.segments
                )
            ):
                return None, "documented_own_source_domain_changed"
            return replace(source, claims=claims), None
        if (
            selection.source_scope is not None
            and selection.period_block is None
            and any(claim.scope != selection.source_scope for claim in claims)
        ):
            return None, "documented_source_scope_changed"
        block = (
            documented_period_block(claims, selection.period_block)
            if selection.period_block is not None
            else None
        )
        scope = (
            block[0] if block is not None else selection.source_scope
        ) or TemporalScope(
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
    if (
        selection.expected_raw_codings is not None
        and tuple(sorted(set(raw_codings or ()))) != selection.expected_raw_codings
    ):
        return None, "coding_source_evidence_changed"
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
    raw_coding_cache: dict[NativeKey, tuple[str, ...]] = {}
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
        if (
            isinstance(decision.selection, DocumentedCodingSelection)
            and decision.selection.enumeration is not None
        ):
            from reg_meta_build.source_value_bindings import marker_binding_fingerprints

            if (
                not isinstance(records, SourceEvidence)
                or records.value_bindings is None
                or not all(
                    decision.selection.enumeration.matches_fields(record.fields)
                    for target in case.targets
                    for record in records.grouped.get(
                        (target.ref.source, target.ref.semantic_record_key), ()
                    )
                )
                or (
                    not decision.selection.enumeration.matches_unlabelled_members(
                        (member.code, member.label)
                        for claim in coding.get(decision.column_key, ())
                        for member in claim.members
                    )
                    if decision.selection.enumeration.syntax == "kategori-alpha-equals"
                    else bool(coding.get(decision.column_key, ()))
                    if decision.selection.enumeration.syntax
                    == "ascii-decimal-comma-equals"
                    else marker_binding_fingerprints(
                        records.value_bindings.get(decision.column_key, ()),
                        decision.valid_from,
                        decision.valid_to,
                    )
                    != decision.selection.expected_marker_bindings
                )
            ):
                report(
                    "coding_binding_changed",
                    "The complete recognized marker binding evidence changed.",
                )
                accounting.append(CodingChoiceAccounting(case.case_id, "stale", (), ()))
                continue
        if isinstance(records, SourceEvidence) and records.effective_scopes is not None:
            if (
                isinstance(decision.selection, DocumentedCodingSelection)
                and decision.selection.source_scope is not None
                and decision.selection.period_block is None
                and records.effective_scopes.get(decision.column_key)
                != frozenset((decision.selection.source_scope,))
            ):
                report(
                    "coding_delivery_changed",
                    "The exact supplied source-scope delivery changed.",
                )
                accounting.append(CodingChoiceAccounting(case.case_id, "stale", (), ()))
                continue
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
            raw_codings = None
            if getattr(decision.selection, "expected_raw_codings", None) is not None:
                if decision.column_key not in raw_coding_cache:
                    raw_coding_cache[decision.column_key] = tuple(
                        coding_source_sha256(claim) for claim in claims
                    )
                raw_codings = raw_coding_cache[decision.column_key]
            selected, problem = _selection(decision, claims, raw_codings=raw_codings)
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
