"""Provider-neutral applicability and resolution of reviewed source-curation cases.

This module checks whether a bounded, reviewed decision still describes the cleaned
source evidence it was written against. Resolution forms catalog content separately;
the source observations are never modified.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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

    from reg_meta_build.resolved_catalog import ResolvedVariable


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
    edition_scopes: tuple[TemporalScope, ...] = ()

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


class FormVariableDecision(_CurationModel):
    """Assign exact occurrences to one identity and one delivery-column spelling.

    This first operation preserves each target's own type and length. Descriptive
    metadata comes from one explicitly checked source. Coding is deliberately
    withheld until code evidence can be resolved, with a reason in every state.
    """

    kind: Literal["form_variable"] = "form_variable"
    reviewed: Literal[True]
    register_slug: str
    variant_slug: str
    variable_slug: str
    provider_key: str
    delivery_column_name: str
    canonical_source: SourceRecordRef
    is_sensitive: bool
    is_identifier: bool
    reason: str
    coding: Literal["withheld"]
    coding_reason: str

    @model_validator(mode="after")
    def _non_empty(self) -> Self:
        for name in (
            "register_slug",
            "variant_slug",
            "variable_slug",
            "provider_key",
            "delivery_column_name",
            "reason",
            "coding_reason",
        ):
            value = getattr(self, name)
            if not value.strip() or value != value.strip():
                raise ValueError(f"{name} must be non-empty and trimmed")
        return self


type CurationDecision = BoundedUnresolvedDecision | FormVariableDecision


class CurationCase(_CurationModel):
    """One exact, finite reviewed discrepancy case."""

    case_id: str
    targets: tuple[RecordExpectation, ...]
    decision: CurationDecision = Field(discriminator="kind")
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
    decision: CurationDecision | None = None

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
    if guard.edition_scopes and record.edition_scope not in guard.edition_scopes:
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


class CurationResolutionError(ValueError):
    """The reviewed cases cannot safely form the selected catalog content."""


_CANONICAL_FIELDS = (
    "name",
    "definition",
    "description",
    "operational_definition",
    "measurement_unit",
)
_STATE_FIELDS = (
    "availability",
    "column_name",
    "data_type",
    "data_length",
    "operational_definition",
)


def _require_projection(
    expected: RecordExpectation,
    names: tuple[str, ...],
    *,
    scope: bool = False,
) -> None:
    shape = expected.alternatives[0]
    missing = set(names) - {field.name for field in shape.fields}
    if (
        missing
        or shape.subject is None
        or (
            scope
            and (shape.edition_scope is None or shape.edition_period_scope is None)
        )
    ):
        raise CurationResolutionError(
            f"{expected.ref}: resolution needs checked fields {sorted(missing)}, "
            "subject and, for targets, edition and exact period scopes"
        )


def _unique_projection(
    records: tuple[SourceRecord, ...],
    shape: RecordProjection,
) -> SourceRecord:
    # All consumed fields must be checked above; original row/layout differences
    # need no winner, while competing consumed facts require another decision.
    if len({_model_token(_project_record(record, shape)) for record in records}) != 1:
        raise CurationResolutionError("resolution still has conflicting target facts")
    return records[0]


def _text(record: SourceRecord, name: str) -> str | None:
    field = getattr(record.fields, name)
    if field is None or field.status != "value":
        return None
    assert isinstance(field.value, str)
    return field.value


def resolve_cases(
    cases: tuple[CurationCase, ...],
    records: Iterable[SourceRecord],
) -> tuple[ResolvedVariable, ...]:
    """Form a finite catalog slice; reject stale or intersecting decisions first.

    Unresolved cases remain inspectable with ``evaluate_case`` but cannot silently
    become an empty/successful catalog through this first formation operation.
    """
    from reg_meta_build.resolved_catalog import (
        ResolvedRegister,
        ResolvedState,
        ResolvedVariable,
        ResolvedVariant,
    )

    records = tuple(records)
    if not cases or len({case.case_id for case in cases}) != len(cases):
        raise CurationResolutionError("resolution requires non-empty unique case IDs")
    evaluations = tuple(evaluate_case(case, records) for case in cases)
    stale = [
        result.model_dump(mode="json")
        for result in evaluations
        if result.status == "stale"
    ]
    if stale:
        import json

        raise CurationResolutionError(
            f"stale curation cases: {json.dumps(stale, ensure_ascii=False)}"
        )

    grouped: defaultdict[tuple[str, tuple[str, ...]], list[SourceRecord]] = defaultdict(
        list
    )
    for record in records:
        grouped[_record_key(record)].append(record)
    claimed: set[tuple[str, tuple[str, ...]]] = set()
    identities: set[tuple[str, str, str]] = set()
    result: list[ResolvedVariable] = []
    for case in sorted(cases, key=lambda item: item.case_id):
        decision = case.decision
        if not isinstance(decision, FormVariableDecision):
            raise CurationResolutionError(
                f"{case.case_id}: unresolved case cannot form catalog content"
            )
        if not case.support:
            raise CurationResolutionError(
                f"{case.case_id}: formation requires checked supporting evidence"
            )
        spelling = FieldExpectation(
            name="column_name", status="value", value=decision.delivery_column_name
        )
        if not any(
            all(spelling in alternative.fields for alternative in support.alternatives)
            for support in case.support
        ):
            raise CurationResolutionError(
                f"{case.case_id}: column spelling needs checked source support"
            )
        expectations = {
            _ref_key(item.ref): item for item in (*case.support, *case.targets)
        }
        anchor_expectation = expectations.get(_ref_key(decision.canonical_source))
        if anchor_expectation is None:
            raise CurationResolutionError(
                f"{case.case_id}: canonical source must be a checked target or support"
            )
        _require_projection(anchor_expectation, _CANONICAL_FIELDS)
        anchor = _unique_projection(
            tuple(grouped[_ref_key(anchor_expectation.ref)]),
            anchor_expectation.alternatives[0],
        )
        canonical_name = _text(anchor, "name")
        if not canonical_name:
            raise CurationResolutionError(f"{case.case_id}: canonical name is unknown")

        states = []
        register = None
        for target in case.targets:
            key = _ref_key(target.ref)
            if key in claimed:
                raise CurationResolutionError(
                    f"{case.case_id}: target already assigned by another case: {target.ref}"
                )
            claimed.add(key)
            _require_projection(target, _STATE_FIELDS, scope=True)
            record = _unique_projection(tuple(grouped[key]), target.alternatives[0])
            subject = record.subject
            if (
                subject.register_name.status != "value"
                or not subject.register_name.name
                or subject.variant.status != "value"
                or not subject.variant.name
            ):
                raise CurationResolutionError(
                    f"{case.case_id}: target register or variant name is unknown"
                )
            candidate_register = ResolvedRegister(
                provider=subject.provider,
                slug=decision.register_slug,
                name=subject.register_name.name,
            )
            if register is not None and register != candidate_register:
                raise CurationResolutionError(
                    f"{case.case_id}: targets disagree about the register"
                )
            register = candidate_register
            if _actual_field(record, "availability") != FieldExpectation(
                name="availability", status="value", value=True
            ):
                raise CurationResolutionError(
                    f"{case.case_id}: target does not assert availability"
                )
            if _text(record, "column_name") not in (
                None,
                decision.delivery_column_name,
            ):
                raise CurationResolutionError(
                    f"{case.case_id}: this operation cannot rename an existing different column"
                )
            scope = record.edition_scope
            if scope.kind != "intervals" or len(scope.intervals) != 1:
                raise CurationResolutionError(
                    f"{case.case_id}: formation requires one explicit annual source occurrence"
                )
            interval = scope.intervals[0]
            if (
                interval.start != interval.end
                or len(interval.start) != 4
                or not interval.start.isascii()
                or not interval.start.isdigit()
            ):
                raise CurationResolutionError(
                    f"{case.case_id}: pooled or nonannual scope cannot establish annual availability"
                )
            year = int(interval.start)
            start = date(year, 1, 1).isoformat()
            end = date(year, 12, 31).isoformat()
            period = record.edition_period_scope
            if (
                period.kind != "intervals"
                or len(period.intervals) != 1
                or period.intervals[0].start != start
                or period.intervals[0].end != end
            ):
                raise CurationResolutionError(
                    f"{case.case_id}: exact source period does not support full-year availability"
                )
            provenance = (
                f"curation:{case.case_id}\n{decision.reason}\n"
                f"Coding withheld: {decision.coding_reason}"
            )
            states.append(
                ResolvedState(
                    variant=ResolvedVariant(
                        slug=decision.variant_slug, name=subject.variant.name
                    ),
                    valid_from=start,
                    valid_to=end,
                    delivery_column_name=decision.delivery_column_name,
                    data_type=_text(record, "data_type"),
                    data_length=_text(record, "data_length"),
                    operational_definition=_text(record, "operational_definition"),
                    provenance=provenance,
                )
            )
        assert register is not None
        identity = (register.provider, register.slug, decision.variable_slug)
        if identity in identities:
            raise CurationResolutionError(
                f"multiple cases assign the same catalog identity: {identity}"
            )
        identities.add(identity)
        result.append(
            ResolvedVariable(
                register=register,
                slug=decision.variable_slug,
                provider_key=decision.provider_key,
                name=canonical_name,
                definition=_text(anchor, "definition"),
                description=_text(anchor, "description"),
                operational_definition=_text(anchor, "operational_definition"),
                measurement_unit=_text(anchor, "measurement_unit"),
                is_sensitive=decision.is_sensitive,
                is_identifier=decision.is_identifier,
                states=tuple(
                    sorted(
                        states,
                        key=lambda state: (
                            state.variant.slug,
                            state.valid_from,
                            state.valid_to,
                        ),
                    )
                ),
            )
        )
    return tuple(result)


__all__ = [
    "ApplicabilityIssue",
    "BoundedUnresolvedDecision",
    "CaseEvaluation",
    "CurationCase",
    "CurationResolutionError",
    "FieldExpectation",
    "FormVariableDecision",
    "PeerGuard",
    "RecordExpectation",
    "RecordProjection",
    "SourceRecordRef",
    "evaluate_case",
    "resolve_cases",
]
