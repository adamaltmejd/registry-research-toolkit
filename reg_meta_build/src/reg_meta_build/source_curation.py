"""Provider-neutral applicability checks for reviewed source-curation cases.

This module checks whether a bounded, reviewed decision still describes the cleaned
source evidence it was written against.  It does not mutate source records or form
catalog entities.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from reg_meta_build.source_records import (
    FieldScalar,
    FieldState,
    NativeCoordinates,
    SourceField,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    canonical_sha256,
)

if TYPE_CHECKING:
    from collections.abc import Iterable


class _CurationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceRecordRef(_CurationModel):
    """Revision- and layout-independent reference to one semantic source member."""

    source: str
    semantic_record_key: tuple[str, ...]

    @model_validator(mode="after")
    def _non_empty(self) -> Self:
        if not self.source.strip() or not self.semantic_record_key:
            raise ValueError("source record references must be non-empty")
        if any(not part.strip() for part in self.semantic_record_key):
            raise ValueError("semantic record key parts must be non-empty")
        return self


ExpectedFieldState = Literal["absent", "value", "unknown", "negative"]


class FieldExpectation(_CurationModel):
    """One relevant cleaned field, excluding raw delivery representation."""

    name: str
    status: ExpectedFieldState
    value: FieldScalar | None = None

    @field_validator("name")
    @classmethod
    def _known_field(cls, value: str) -> str:
        if value not in SourceFields.model_fields:
            raise ValueError(f"unknown source field: {value}")
        return value

    @model_validator(mode="after")
    def _coherent(self) -> Self:
        if self.status == "absent":
            if self.value is not None:
                raise ValueError("an absent field expectation cannot carry a value")
            return self
        status: FieldState = self.status
        field = SourceField(status=status, value=self.value)
        SourceFields.model_validate({self.name: field})
        return self


class RecordProjection(_CurationModel):
    """The explicitly relevant cleaned part of one source observation."""

    fields: tuple[FieldExpectation, ...] = ()
    edition_scope: TemporalScope | None = None
    edition_period_scope: TemporalScope | None = None
    subject: SourceSubject | None = None

    @field_validator("fields")
    @classmethod
    def _ordered_fields(
        cls, fields: tuple[FieldExpectation, ...]
    ) -> tuple[FieldExpectation, ...]:
        names = [field.name for field in fields]
        if len(names) != len(set(names)):
            raise ValueError("a projection cannot name a field more than once")
        return tuple(sorted(fields, key=lambda field: field.name))

    @model_validator(mode="after")
    def _valid_projection(self) -> Self:
        if (
            not self.fields
            and self.edition_scope is None
            and self.edition_period_scope is None
            and self.subject is None
        ):
            raise ValueError("a record projection must select at least one fact")
        return self


def _projection_shape(
    projection: RecordProjection,
) -> tuple[tuple[str, ...], bool, bool, bool]:
    return (
        tuple(field.name for field in projection.fields),
        projection.edition_scope is not None,
        projection.edition_period_scope is not None,
        projection.subject is not None,
    )


def _model_token(value: _CurationModel) -> str:
    return canonical_sha256(value.model_dump(mode="json"))


class RecordExpectation(_CurationModel):
    """Expected alternatives for one exact semantic source member."""

    ref: SourceRecordRef
    alternatives: tuple[RecordProjection, ...]

    @model_validator(mode="after")
    def _finite_consistent_alternatives(self) -> Self:
        if not self.alternatives:
            raise ValueError("a record expectation needs at least one alternative")
        shapes = {_projection_shape(item) for item in self.alternatives}
        if len(shapes) != 1:
            raise ValueError("record alternatives must use one projection shape")
        tokens = [_model_token(item) for item in self.alternatives]
        if len(tokens) != len(set(tokens)):
            raise ValueError("record alternatives must be unique")
        return self


class PeerGuard(_CurationModel):
    """Review-only check for the complete finite set of relevant semantic peers."""

    guard_id: str
    source: str
    expected_members: tuple[SourceRecordRef, ...]
    native: NativeCoordinates | None = None
    register_name: str | None = None
    fields: tuple[FieldExpectation, ...] = ()

    @model_validator(mode="after")
    def _valid_guard(self) -> Self:
        if not self.guard_id.strip() or not self.source.strip():
            raise ValueError("peer guard identifiers must be non-empty")
        if self.register_name is not None and not self.register_name.strip():
            raise ValueError("peer guard register_name cannot be blank")
        if self.native is None and self.register_name is None and not self.fields:
            raise ValueError("a peer guard needs review matching criteria")
        if self.native is not None and all(
            getattr(self.native, name) is None
            for name in type(self.native).model_fields
        ):
            raise ValueError("a peer guard native match cannot be empty")
        field_names = [field.name for field in self.fields]
        if len(field_names) != len(set(field_names)):
            raise ValueError("a peer guard cannot match a field more than once")
        if not self.expected_members:
            raise ValueError("a peer guard needs expected members")
        member_keys = [_ref_key(member) for member in self.expected_members]
        if len(member_keys) != len(set(member_keys)):
            raise ValueError("peer guard members must be unique semantic keys")
        if any(member.source != self.source for member in self.expected_members):
            raise ValueError("peer guard members must use the guarded source")
        return self


class BoundedUnresolvedDecision(_CurationModel):
    """Reviewed uncertainty that preserves source facts and withholds unsafe output."""

    kind: Literal["bounded_unresolved"] = "bounded_unresolved"
    reviewed: Literal[True]
    withheld_aspects: tuple[str, ...]
    reason: str
    safe_behavior: Literal["preserve_source_records"] = "preserve_source_records"

    @model_validator(mode="after")
    def _complete(self) -> Self:
        if not self.withheld_aspects or any(
            not aspect.strip() for aspect in self.withheld_aspects
        ):
            raise ValueError("a bounded unresolved decision must name withheld aspects")
        if len(self.withheld_aspects) != len(set(self.withheld_aspects)):
            raise ValueError("withheld aspects must be unique")
        if not self.reason.strip():
            raise ValueError("a bounded unresolved decision needs a reason")
        return self


class CurationCase(_CurationModel):
    """One exact, finite reviewed discrepancy case."""

    case_id: str
    targets: tuple[RecordExpectation, ...]
    decision: BoundedUnresolvedDecision
    support: tuple[RecordExpectation, ...] = ()
    peer_guards: tuple[PeerGuard, ...] = ()

    @model_validator(mode="after")
    def _finite_membership(self) -> Self:
        if not self.case_id.strip():
            raise ValueError("curation case_id must be non-empty")
        if not self.targets:
            raise ValueError("a curation case needs exact targets")
        for role, expectations in (
            ("target", self.targets),
            ("support", self.support),
        ):
            keys = [_ref_key(item.ref) for item in expectations]
            if len(keys) != len(set(keys)):
                raise ValueError(f"{role} semantic keys must be unique")
        guard_ids = [guard.guard_id for guard in self.peer_guards]
        if len(guard_ids) != len(set(guard_ids)):
            raise ValueError("peer guard identifiers must be unique")
        return self


IssueCode = Literal[
    "target_missing",
    "target_projection_changed",
    "support_missing",
    "support_projection_changed",
    "peer_membership_changed",
]


class ApplicabilityIssue(_CurationModel):
    code: IssueCode
    subject: str
    detail: str
    missing_projections: tuple[RecordProjection, ...] = ()
    added_projections: tuple[RecordProjection, ...] = ()
    missing_members: tuple[SourceRecordRef, ...] = ()
    added_members: tuple[SourceRecordRef, ...] = ()


class CaseEvaluation(_CurationModel):
    case_id: str
    status: Literal["applicable", "stale"]
    issues: tuple[ApplicabilityIssue, ...]
    decision: BoundedUnresolvedDecision | None = None

    @model_validator(mode="after")
    def _coherent(self) -> Self:
        if self.status == "applicable" and (self.issues or self.decision is None):
            raise ValueError("an applicable case needs its decision and no issues")
        if self.status == "stale" and (not self.issues or self.decision is not None):
            raise ValueError("a stale case needs issues and cannot expose its decision")
        return self


def _ref_key(ref: SourceRecordRef) -> tuple[str, tuple[str, ...]]:
    return ref.source, ref.semantic_record_key


def _record_key(record: SourceRecord) -> tuple[str, tuple[str, ...]]:
    return record.source, record.locators[0].semantic_record_key


def _actual_field(record: SourceRecord, name: str) -> FieldExpectation:
    field = getattr(record.fields, name)
    if field is None:
        return FieldExpectation(name=name, status="absent")
    return FieldExpectation(name=name, status=field.status, value=field.value)


def _project_record(record: SourceRecord, shape: RecordProjection) -> RecordProjection:
    return RecordProjection(
        fields=tuple(_actual_field(record, field.name) for field in shape.fields),
        edition_scope=(
            record.edition_scope if shape.edition_scope is not None else None
        ),
        edition_period_scope=(
            record.edition_period_scope
            if shape.edition_period_scope is not None
            else None
        ),
        subject=record.subject if shape.subject is not None else None,
    )


def _field_matches(record: SourceRecord, expected: FieldExpectation) -> bool:
    return _actual_field(record, expected.name) == expected


def _peer_matches(record: SourceRecord, guard: PeerGuard) -> bool:
    if record.source != guard.source:
        return False
    if guard.native is not None:
        native = record.subject.native
        for name in type(guard.native).model_fields:
            expected = getattr(guard.native, name)
            if expected is not None and getattr(native, name) != expected:
                return False
    if guard.register_name is not None:
        register = record.subject.register_name
        if register.status != "value" or register.name != guard.register_name:
            return False
    return all(_field_matches(record, field) for field in guard.fields)


def _compare_expectation(
    *,
    role: Literal["target", "support"],
    expected: RecordExpectation,
    records: list[SourceRecord],
) -> ApplicabilityIssue | None:
    subject = "/".join((expected.ref.source, *expected.ref.semantic_record_key))
    if not records:
        missing_code: IssueCode = (
            "target_missing" if role == "target" else "support_missing"
        )
        return ApplicabilityIssue(
            code=missing_code,
            subject=subject,
            detail="expected semantic source member is absent",
            missing_members=(expected.ref,),
        )
    shape = expected.alternatives[0]
    actual_by_token = {
        _model_token(projected): projected
        for projected in (_project_record(record, shape) for record in records)
    }
    expected_by_token = {
        _model_token(alternative): alternative for alternative in expected.alternatives
    }
    actual_tokens = set(actual_by_token)
    expected_tokens = set(expected_by_token)
    if actual_tokens == expected_tokens:
        return None
    changed_code: IssueCode = (
        "target_projection_changed"
        if role == "target"
        else "support_projection_changed"
    )
    return ApplicabilityIssue(
        code=changed_code,
        subject=subject,
        detail=(
            f"missing {len(expected_tokens - actual_tokens)} and added "
            f"{len(actual_tokens - expected_tokens)} relevant projection "
            "alternative(s)"
        ),
        missing_projections=tuple(
            expected_by_token[token]
            for token in sorted(expected_tokens - actual_tokens)
        ),
        added_projections=tuple(
            actual_by_token[token] for token in sorted(actual_tokens - expected_tokens)
        ),
    )


def evaluate_case(
    case: CurationCase,
    records: Iterable[SourceRecord],
) -> CaseEvaluation:
    """Check one case without changing or selecting facts from source records."""

    records = tuple(records)
    grouped: defaultdict[tuple[str, tuple[str, ...]], list[SourceRecord]] = defaultdict(
        list
    )
    for record in records:
        grouped[_record_key(record)].append(record)

    issues: list[ApplicabilityIssue] = []
    for role, expectations in (
        ("target", case.targets),
        ("support", case.support),
    ):
        for expected in expectations:
            issue = _compare_expectation(
                role=role,
                expected=expected,
                records=grouped[_ref_key(expected.ref)],
            )
            if issue is not None:
                issues.append(issue)

    for guard in case.peer_guards:
        actual_members = {
            _record_key(record) for record in records if _peer_matches(record, guard)
        }
        expected_members = {_ref_key(member) for member in guard.expected_members}
        if actual_members != expected_members:
            missing_members = tuple(
                SourceRecordRef(source=source, semantic_record_key=semantic_key)
                for source, semantic_key in sorted(expected_members - actual_members)
            )
            added_members = tuple(
                SourceRecordRef(source=source, semantic_record_key=semantic_key)
                for source, semantic_key in sorted(actual_members - expected_members)
            )
            issues.append(
                ApplicabilityIssue(
                    code="peer_membership_changed",
                    subject=guard.guard_id,
                    detail=(
                        f"missing {len(missing_members)} and added "
                        f"{len(added_members)} semantic peer(s)"
                    ),
                    missing_members=missing_members,
                    added_members=added_members,
                )
            )

    if issues:
        return CaseEvaluation(
            case_id=case.case_id,
            status="stale",
            issues=tuple(issues),
        )
    return CaseEvaluation(
        case_id=case.case_id,
        status="applicable",
        issues=(),
        decision=case.decision,
    )


__all__ = [
    "ApplicabilityIssue",
    "BoundedUnresolvedDecision",
    "CaseEvaluation",
    "CurationCase",
    "FieldExpectation",
    "PeerGuard",
    "RecordExpectation",
    "RecordProjection",
    "SourceRecordRef",
    "evaluate_case",
]
