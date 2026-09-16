"""Bind typed source code lists through declared source-local relationships.

The caller keeps these sessions open across its record stream. The large native-ID
path uses prepared posting lists; it never scans or materializes the full source.
No provider column names, fuzzy names, classification guesses, or authority winners
are used here. Binding issues and original evidence remain separate from claims.
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from datetime import date
from typing import TYPE_CHECKING

from reg_meta_build.source_coding import CodeListClaim, CodeMembershipClaim
from reg_meta_build.source_intervals import scope_bounds
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceRecord,
    TemporalScope,
    canonical_sha256,
)
from reg_meta_build.source_values import SourceValueWindow

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from reg_meta_build.prepared_values import (
        PreparedSourceValues,
        PreparedValueSession,
    )
    from reg_meta_build.source_records import RecordLocator
    from reg_meta_build.source_values import (
        SourceValueAssociation,
        SourceValueValidity,
    )


@dataclass(frozen=True)
class ValueBindingIssue:
    code: str
    value_source: str
    descriptor_key: str | None = None
    record_id: str | None = None
    association_locators: tuple[str, ...] = ()
    raw_member_tokens: tuple[str | None, ...] = ()
    occurrence_count: int = 0


@dataclass(frozen=True)
class ValueListBinding:
    claim_id: str
    record_id: str
    record_locators: tuple[RecordLocator, ...]
    value_revision_id: str
    descriptor_key: str
    association_count: int
    inactive_associations: tuple[SourceValueAssociation, ...]


@dataclass(frozen=True)
class ValueBindingResult:
    claims: tuple[CodeListClaim, ...]
    bindings: tuple[ValueListBinding, ...]
    issues: tuple[ValueBindingIssue, ...]


def _unknown(reason: str) -> TemporalScope:
    return TemporalScope(kind="unknown", label=reason)


def _member_scope(
    occurrence: TemporalScope,
    association: SourceValueAssociation,
    validity: tuple[SourceValueValidity, ...],
    *,
    missing_validity: str,
    invalid_item: bool,
) -> tuple[TemporalScope | None, str | None]:
    constraints = tuple(
        window
        for window in (association.supplied_window, association.section_window)
        if window is not None
    )
    alternatives = tuple(value.window for value in validity)
    if invalid_item or (not validity and missing_validity == "unknown"):
        return _unknown("source item validity is unknown"), "unknown_code_validity"
    if any(
        window is None or window.status == "unknown" for window in alternatives
    ) or any(window.status == "unknown" for window in constraints):
        return _unknown(
            "supplied source validity is malformed or unknown"
        ), "unknown_code_validity"
    if not constraints and not alternatives:
        return TemporalScope(kind="year_independent"), None
    bounds = scope_bounds(occurrence)
    if bounds is None:
        return _unknown(
            "code membership requires a finite occurrence scope"
        ), "unsupported_coding_scope"
    # Each validity row is an alternative window. Row/section declarations are
    # independent restrictions. Clamp only to this already-established occurrence.
    effective = []
    alternatives = alternatives or (SourceValueWindow("known"),)
    for alternative in alternatives:
        assert alternative is not None
        windows = (*constraints, alternative)
        starts = [
            date.fromisoformat(window.start).toordinal()
            for window in windows
            if window.start is not None
        ]
        ends = [
            date.fromisoformat(window.end).toordinal()
            for window in windows
            if window.end is not None
        ]
        if starts and ends and max(starts) > min(ends):
            return _unknown(
                "source validity assertions have no common period"
            ), "conflicting_code_validity"
        for start, end in bounds:
            lower, upper = max([start, *starts]), min([end, *ends])
            if lower <= upper:
                effective.append((lower, upper))
    if not effective:
        return None, None
    merged: list[tuple[int, int]] = []
    for start, end in sorted(effective):
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = merged[-1][0], max(merged[-1][1], end)
        else:
            merged.append((start, end))
    return TemporalScope(
        kind="intervals",
        intervals=tuple(
            ScopeInterval(
                start=date.fromordinal(start).isoformat(),
                end=None
                if end == date.max.toordinal()
                else date.fromordinal(end).isoformat(),
            )
            for start, end in merged
        ),
    ), None


class ValueBindingSession:
    """Resolve one accepted value source through one persistent prepared session."""

    def __init__(self, session: PreparedValueSession) -> None:
        self.session = session
        self.source = session.source.manifest.revision.dataset
        self.join = session.source.manifest.join
        self._last_native_key: tuple[str, int, TemporalScope] | None = None
        self._last_native_result: ValueBindingResult | None = None
        # Native stores have thousands of descriptors but need none until selected.
        # Named-list stores have finite declared dictionaries; rows remain indexed.
        self.descriptors = (
            ()
            if self.join is not None and self.join.member_target == "native_member"
            else tuple(session.source.descriptors())
        )

    def source_issues(self) -> Iterator[ValueBindingIssue]:
        """Account for source relations that cannot safely identify any target."""
        if self.join is None:
            yield ValueBindingIssue(
                "missing_value_join_contract",
                self.source,
                occurrence_count=self.session.source.manifest.association_count,
            )
            return
        if self.join.member_target == "native_member":
            for raw, coordinate, count in self.session.member_coordinates():
                if coordinate is None:
                    yield ValueBindingIssue(
                        "unknown_native_member_token",
                        self.source,
                        raw_member_tokens=(raw,),
                        occurrence_count=count,
                    )
        elif self.join.member_target == "member_name":
            for descriptor in self.descriptors:
                # A sheet suffix cannot supply a missing explicit row/header ref.
                if (
                    not descriptor.member_references
                    and not descriptor.record_ids
                    and not any(
                        row.member_references
                        for row in self.session.lookup_descriptor(
                            descriptor.payload_key
                        )
                    )
                ):
                    yield ValueBindingIssue(
                        "unresolved_list_reference", self.source, descriptor.payload_key
                    )

    def bind(
        self, record: SourceRecord, *, scope: TemporalScope | None = None
    ) -> ValueBindingResult:
        join = self.join
        if join is None or record.source not in join.record_sources:
            return ValueBindingResult((), (), ())
        if scope is None:
            scope = record.edition_period_scope
            if scope.kind == "not_applicable":
                scope = record.edition_scope
        groups: dict[str, list[SourceValueAssociation]] = defaultdict(list)
        issues: list[ValueBindingIssue] = []
        contradictory: set[str] = set()
        if join.member_target == "native_member":
            member = record.subject.member.native_id
            if type(member) is not int:
                return ValueBindingResult(
                    (),
                    (),
                    (
                        ValueBindingIssue(
                            "unknown_record_member",
                            self.source,
                            record_id=record.record_id,
                        ),
                    ),
                )
            native_key = (record.source, member, scope)
            if native_key == self._last_native_key:
                cached = self._last_native_result
                assert cached is not None
                # Parent/prose duplicates describe the same native code list.
                # Share its immutable claims, but retain this physical record's
                # evidence binding and issue locators. Keep only one list cached.
                return ValueBindingResult(
                    cached.claims,
                    tuple(
                        replace(
                            binding,
                            record_id=record.record_id,
                            record_locators=record.locators,
                        )
                        for binding in cached.bindings
                    ),
                    tuple(
                        replace(issue, record_id=record.record_id)
                        for issue in cached.issues
                    ),
                )
            for association in self.session.lookup_native_member(member):
                groups[association.descriptor_key].append(association)
        else:
            member_name = record.subject.member.name
            declared = record.fields.value_set_declared
            declared_name = (
                declared.value
                if declared is not None and declared.status == "value"
                else None
            )
            for descriptor in self.descriptors:
                if descriptor.record_ids:
                    if record.record_id in descriptor.record_ids:
                        groups[descriptor.payload_key].extend(
                            self.session.lookup_descriptor(descriptor.payload_key)
                        )
                    continue
                if join.member_target == "declared_list":
                    if declared_name == descriptor.name:
                        groups[descriptor.payload_key].extend(
                            self.session.lookup_descriptor(descriptor.payload_key)
                        )
                    continue
                if member_name is None:
                    continue
                header_refs = set(descriptor.member_references)
                for association in self.session.lookup_descriptor(
                    descriptor.payload_key
                ):
                    row_refs = set(association.member_references)
                    candidates = (
                        header_refs & row_refs
                        if header_refs and row_refs and header_refs & row_refs
                        else header_refs | row_refs
                    )
                    if member_name not in candidates:
                        continue
                    if len(header_refs) > 1 or len(row_refs) > 1:
                        contradictory.add(association.locator)
                        issues.append(
                            ValueBindingIssue(
                                "ambiguous_list_member_references",
                                self.source,
                                descriptor.payload_key,
                                record.record_id,
                                (association.locator,),
                            )
                        )
                    if header_refs and row_refs and not header_refs & row_refs:
                        contradictory.add(association.locator)
                        issues.append(
                            ValueBindingIssue(
                                "conflicting_list_member_references",
                                self.source,
                                descriptor.payload_key,
                                record.record_id,
                                (association.locator,),
                            )
                        )
                    groups[descriptor.payload_key].append(association)
        claims, bindings = [], []
        for descriptor_key, associations in sorted(groups.items()):
            descriptor = self.session.descriptor(descriptor_key)
            claim_id = canonical_sha256(
                [
                    self.session.source.manifest.revision.revision_id,
                    ("native_member", record.source, record.subject.member.native_id)
                    if join.member_target == "native_member"
                    else ("record", record.record_id),
                    descriptor_key,
                    scope.model_dump(mode="json"),
                ]
            )
            members, inactive = [], []
            for association in associations:
                validity = self.session.validity_for(
                    item_id=association.item_id, locator=association.locator
                )
                invalid_item = (
                    join.validity_target == "item"
                    and self.session.native_item_coordinate(association.item_id) is None
                )
                member_scope, issue = _member_scope(
                    scope,
                    association,
                    validity,
                    missing_validity=join.missing_validity,
                    invalid_item=invalid_item,
                )
                if association.locator in contradictory:
                    member_scope = _unknown(
                        "source list identifies conflicting members"
                    )
                if issue:
                    issues.append(
                        ValueBindingIssue(
                            issue,
                            self.source,
                            descriptor_key,
                            record.record_id,
                            (association.locator,),
                        )
                    )
                if member_scope is None:
                    inactive.append(association)
                    continue
                value = self.session.value(association.value_key)
                members.append(
                    CodeMembershipClaim(
                        value.code, value.label, member_scope, (association,), validity
                    )
                )
            claims.append(
                CodeListClaim(
                    claim_id, scope, tuple(members), version_label=descriptor.version
                )
            )
            bindings.append(
                ValueListBinding(
                    claim_id,
                    record.record_id,
                    record.locators,
                    self.session.source.manifest.revision.revision_id,
                    descriptor_key,
                    len(associations),
                    tuple(inactive),
                )
            )
        result = ValueBindingResult(tuple(claims), tuple(bindings), tuple(issues))
        if join.member_target == "native_member":
            self._last_native_key = native_key
            self._last_native_result = result
        return result


@contextmanager
def open_value_bindings(
    sources: Iterable[PreparedSourceValues],
) -> Iterator[tuple[ValueBindingSession, ...]]:
    with ExitStack() as stack:
        yield tuple(
            ValueBindingSession(stack.enter_context(source.session()))
            for source in sources
        )


def bind_code_lists(
    record: SourceRecord,
    sessions: Iterable[ValueBindingSession],
    *,
    scope: TemporalScope | None = None,
) -> ValueBindingResult:
    """Bind original evidence at its own or an already checked effective scope.

    A corrected delivery period changes membership intersections, never the source
    locators or supplied item validity. Distinct effective periods have distinct
    claim identities even when they use the same checked donor.
    """
    sessions = tuple(sessions)
    results = tuple(session.bind(record, scope=scope) for session in sessions)
    claims = tuple(claim for result in results for claim in result.claims)
    issues = tuple(issue for result in results for issue in result.issues)
    declared = record.fields.value_set_declared
    if declared is not None and declared.status == "value":
        named = tuple(
            session
            for session in sessions
            if session.join is not None
            and session.join.member_target == "declared_list"
            and record.source in session.join.record_sources
        )
        if not any(
            descriptor.name == declared.value
            for session in named
            for descriptor in session.descriptors
        ):
            issues += (
                ValueBindingIssue(
                    "declared_value_list_not_found",
                    record.source,
                    record_id=record.record_id,
                ),
            )
    return ValueBindingResult(
        claims,
        tuple(binding for result in results for binding in result.bindings),
        issues,
    )


__all__ = [
    "ValueBindingIssue",
    "ValueBindingResult",
    "ValueBindingSession",
    "ValueListBinding",
    "bind_code_lists",
    "open_value_bindings",
]
