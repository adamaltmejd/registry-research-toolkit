"""Reconcile already-bound code-list claims over exact occurrence periods.

The caller establishes which source lists describe the same delivery occurrence.
Names, source order, list size, and recency never establish that binding here.
Validity scopes arrive interpreted by their source format or checked curation;
unknown and pooled scopes cannot become annual membership. Original claim objects
remain in the result so duplicate source associations are not lost in accounting.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING

from reg_meta_build.resolved_catalog import ResolvedCodeSet
from reg_meta_build.source_intervals import finite_scope_bounds

if TYPE_CHECKING:
    from reg_meta_build.source_records import TemporalScope
    from reg_meta_build.source_values import SourceValueAssociation, SourceValueValidity


@dataclass(frozen=True)
class CodeMembershipClaim:
    """One supplied code/label and its independently interpreted validity.

    None means unknown. Empty strings and leading zeros are literal code values.
    A not-applicable/year-independent scope adds no restriction to the containing
    occurrence; an unknown scope cannot safely establish membership anywhere.
    """

    code: str | None
    label: str | None
    scope: TemporalScope
    associations: tuple[SourceValueAssociation, ...] = ()
    validity: tuple[SourceValueValidity, ...] = ()


@dataclass(frozen=True)
class CodeListClaim:
    """One whole source list, already bound to a finite occurrence scope."""

    claim_id: str
    scope: TemporalScope
    members: tuple[CodeMembershipClaim, ...]
    version_label: str | None = None


@dataclass(frozen=True)
class CodingIssue:
    code: str
    claim_ids: tuple[str, ...]
    valid_from: str | None
    valid_to: str | None
    member_positions: tuple[tuple[str, int], ...] = ()
    withheld: str = "code_membership"


@dataclass(frozen=True)
class CodingSegment:
    valid_from: str
    valid_to: str
    code_set: ResolvedCodeSet | None
    claim_ids: tuple[str, ...]
    version_label: str = ""


@dataclass(frozen=True)
class CodingResolution:
    segments: tuple[CodingSegment, ...]
    issues: tuple[CodingIssue, ...]
    claims: tuple[CodeListClaim, ...]


def resolve_code_membership(claims: tuple[CodeListClaim, ...]) -> CodingResolution:
    """Keep an agreed complete coding or withhold only contested coding periods.

    Code-validity cuts can split an occurrence within a year. Multiple covering
    lists must agree on their complete active memberships, including labels; a
    union or largest-list choice would manufacture an unsupported list. An empty
    active list is reported, never converted into explicit uncoded-variable proof.
    """
    identities = [claim.claim_id for claim in claims]
    if any(not identity for identity in identities) or len(identities) != len(
        set(identities)
    ):
        raise ValueError("coding claims require unique nonempty identities")
    claim_by_id = {claim.claim_id: claim for claim in claims}
    issues: list[CodingIssue] = []
    # Each event changes one active occurrence and/or one member position. Counts
    # preserve overlapping constraints while dictionary keys deduplicate content.
    claim_changes: dict[int, list[tuple[str, int]]] = defaultdict(list)
    member_changes: dict[int, list[tuple[str, int, int]]] = defaultdict(list)
    invalid_members: set[tuple[str, int]] = set()
    for claim in sorted(claims, key=lambda c: c.claim_id):
        periods = finite_scope_bounds(claim.scope)
        if periods is None:
            issues.append(
                CodingIssue("unsupported_coding_scope", (claim.claim_id,), None, None)
            )
            continue
        for start, end in periods:
            claim_changes[start].append((claim.claim_id, 1))
            claim_changes[end + 1].append((claim.claim_id, -1))
        for position, member in enumerate(claim.members):
            if member.scope.kind in {"not_applicable", "year_independent"}:
                member_periods = periods
            else:
                member_periods = finite_scope_bounds(member.scope)
            if member.code is None or member.label is None or member_periods is None:
                invalid_members.add((claim.claim_id, position))
            if member_periods is None:
                member_periods = periods
            for lower, upper in member_periods:
                for start, end in periods:
                    lo, hi = max(lower, start), min(upper, end)
                    if lo <= hi:
                        member_changes[lo].append((claim.claim_id, position, 1))
                        member_changes[hi + 1].append((claim.claim_id, position, -1))

    active_claims: dict[str, int] = {}
    active_members: dict[tuple[str, int], int] = {}
    cuts = sorted(claim_changes.keys() | member_changes.keys())
    segments = []
    for start, next_start in pairwise(cuts):
        for identity, delta in claim_changes[start]:
            count = active_claims.get(identity, 0) + delta
            if count:
                active_claims[identity] = count
            else:
                active_claims.pop(identity, None)
        for identity, position, delta in member_changes[start]:
            key = identity, position
            count = active_members.get(key, 0) + delta
            if count:
                active_members[key] = count
            else:
                active_members.pop(key, None)
        if not active_claims:
            continue
        lower = date.fromordinal(start).isoformat()
        upper = date.fromordinal(next_start - 1).isoformat()
        claim_ids = tuple(sorted(active_claims))
        invalid = tuple(sorted(active_members.keys() & invalid_members))
        if invalid:
            issues.append(
                CodingIssue("unknown_code_membership", claim_ids, lower, upper, invalid)
            )
            segments.append(CodingSegment(lower, upper, None, claim_ids))
            continue
        by_claim: dict[str, set[tuple[str, str]]] = {
            identity: set() for identity in claim_ids
        }
        for identity, position in active_members:
            member = claim_by_id[identity].members[position]
            assert member.code is not None and member.label is not None
            by_claim[identity].add((member.code, member.label))
        empty = tuple(identity for identity in claim_ids if not by_claim[identity])
        if empty:
            issues.append(CodingIssue("empty_active_coding", empty, lower, upper))
            segments.append(CodingSegment(lower, upper, None, claim_ids))
            continue
        alternatives = {frozenset(members) for members in by_claim.values()}
        if len(alternatives) > 1:
            issues.append(
                CodingIssue("conflicting_code_memberships", claim_ids, lower, upper)
            )
            segments.append(CodingSegment(lower, upper, None, claim_ids))
            continue
        members = next(iter(alternatives))
        labels = {
            label
            for identity in claim_ids
            if (label := claim_by_id[identity].version_label) is not None
        }
        version_label = next(iter(labels)) if len(labels) == 1 else ""
        if len(labels) > 1:
            issues.append(
                CodingIssue(
                    "conflicting_coding_labels",
                    claim_ids,
                    lower,
                    upper,
                    withheld="coding_label",
                )
            )
        segments.append(
            CodingSegment(
                lower,
                upper,
                ResolvedCodeSet(members=tuple(members)),
                claim_ids,
                version_label,
            )
        )
    return CodingResolution(tuple(segments), tuple(issues), claims)
