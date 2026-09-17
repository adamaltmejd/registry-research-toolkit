"""Provider-neutral applicability and resolution of reviewed source-curation cases.

This module checks whether a bounded, reviewed decision still describes the cleaned
source evidence it was written against. Resolution forms catalog content separately;
the source observations are never modified.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from functools import cached_property
from typing import TYPE_CHECKING, Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from reg_meta_build._curation import fold_column
from reg_meta_build._resolved_common import covers_window
from reg_meta_build.source_coordinates import (
    NativeKey,
    _coordinate_key,
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_records import (
    FieldScalar,
    FieldState,
    NativeCoordinates,
    SourceCoordinate,
    SourceField,
    SourceFields,
    SourceParentKind,
    SourceParentObservation,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    canonical_sha256,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


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


class CodeSetExpectation(_CurationModel):
    """A source-local code-set identity, independent of physical layout."""

    reference_id: str = Field(min_length=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ParentFactProjection(_CurationModel):
    """Complete cleaned parent claim, excluding raw cells and physical layout."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    kind: SourceParentKind
    coordinate: SourceCoordinate
    register_name: SourceCoordinate = Field(alias="register")
    variant: SourceCoordinate | None = None
    edition: SourceCoordinate | None = None
    fields: tuple[FieldExpectation, ...]

    @field_validator("fields")
    @classmethod
    def _ordered_fields(
        cls, fields: tuple[FieldExpectation, ...]
    ) -> tuple[FieldExpectation, ...]:
        if not fields or len({field.name for field in fields}) != len(fields):
            raise ValueError("parent projection needs unique supplied fields")
        return tuple(sorted(fields, key=lambda field: field.name))


def parent_fact_projection(parent: SourceParentObservation) -> ParentFactProjection:
    return ParentFactProjection(
        kind=parent.kind,
        coordinate=parent.coordinate,
        register=parent.register_name,
        variant=parent.variant,
        edition=parent.edition,
        fields=tuple(
            FieldExpectation(name=name, status=field.status, value=field.value)
            for name in SourceFields.model_fields
            if (field := getattr(parent.fields, name)) is not None
        ),
    )


class RecordProjection(_CurationModel):
    """The explicitly relevant cleaned part of one source observation."""

    fields: tuple[FieldExpectation, ...] = ()
    edition_scope: TemporalScope | None = None
    edition_period_scope: TemporalScope | None = None
    subject: SourceSubject | None = None
    native: NativeCoordinates | None = None
    code_set_references: tuple[CodeSetExpectation, ...] | None = None
    parent_facts: tuple[ParentFactProjection, ...] | None = None

    @field_validator("native")
    @classmethod
    def _selected_native_coordinates(
        cls, native: NativeCoordinates | None
    ) -> NativeCoordinates | None:
        if native is not None and all(
            getattr(native, name) is None for name in NativeCoordinates.model_fields
        ):
            raise ValueError("a native projection must select at least one coordinate")
        return native

    @field_validator("parent_facts")
    @classmethod
    def _ordered_parents(
        cls, parents: tuple[ParentFactProjection, ...] | None
    ) -> tuple[ParentFactProjection, ...] | None:
        return None if parents is None else tuple(sorted(parents, key=_model_token))

    @field_validator("code_set_references")
    @classmethod
    def _ordered_code_references(
        cls, references: tuple[CodeSetExpectation, ...] | None
    ) -> tuple[CodeSetExpectation, ...] | None:
        if references is None:
            return None
        return tuple(
            sorted(set(references), key=lambda r: (r.reference_id, r.content_sha256))
        )

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
            and self.native is None
            and self.code_set_references is None
            and self.parent_facts is None
        ):
            raise ValueError("a record projection must select at least one fact")
        return self


def _projection_shape(
    projection: RecordProjection,
) -> tuple[tuple[str, ...], bool, bool, bool, bool, bool, tuple[str, ...]]:
    return (
        tuple(field.name for field in projection.fields),
        projection.edition_scope is not None,
        projection.edition_period_scope is not None,
        projection.subject is not None,
        projection.code_set_references is not None,
        projection.parent_facts is not None,
        tuple(
            name
            for name in NativeCoordinates.model_fields
            if projection.native is not None
            and getattr(projection.native, name) is not None
        ),
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
    folded_column: str | None = None
    coordinates: tuple[
        tuple[
            Literal["register", "variant", "variable", "population", "member"],
            SourceCoordinate,
        ],
        ...,
    ] = ()

    @model_validator(mode="after")
    def _valid_guard(self) -> Self:
        if not self.guard_id.strip() or not self.source.strip():
            raise ValueError("peer guard identifiers must be non-empty")
        if self.register_name is not None and not self.register_name.strip():
            raise ValueError("peer guard register_name cannot be blank")
        if (
            self.native is None
            and self.register_name is None
            and not self.fields
            and self.folded_column is None
            and not self.coordinates
        ):
            raise ValueError("a peer guard needs review matching criteria")
        if len({name for name, _coordinate in self.coordinates}) != len(
            self.coordinates
        ):
            raise ValueError("peer coordinate roles must be unique")
        if self.folded_column is not None and (
            not self.folded_column
            or fold_column(self.folded_column) != self.folded_column
        ):
            raise ValueError(
                "a folded column guard must contain a nonempty folded token"
            )
        if self.native is not None and all(
            getattr(self.native, name) is None
            for name in type(self.native).model_fields
        ):
            raise ValueError("a peer guard native match cannot be empty")
        field_names = [field.name for field in self.fields]
        if len(field_names) != len(set(field_names)):
            raise ValueError("a peer guard cannot match a field more than once")
        member_keys = [_ref_key(member) for member in self.expected_members]
        if len(member_keys) != len(set(member_keys)):
            raise ValueError("peer guard members must be unique semantic keys")
        if any(member.source != self.source for member in self.expected_members):
            raise ValueError("peer guard members must use the guarded source")
        return self


UnresolvedAspect = Literal[
    "identity",
    "availability",
    "column_name",
    "data_type",
    "data_length",
    "period",
    "coding",
]


class BoundedUnresolvedDecision(_CurationModel):
    """Reviewed uncertainty that preserves source facts and withholds unsafe output."""

    kind: Literal["bounded_unresolved"] = "bounded_unresolved"
    reviewed: Literal[True]
    withheld_aspects: tuple[UnresolvedAspect, ...]
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


def _unique_conditions(
    conditions: tuple[FieldExpectation, ...],
) -> tuple[FieldExpectation, ...]:
    if len({field.name for field in conditions}) != len(conditions):
        raise ValueError("effect conditions must use unique field names")
    return conditions


class CheckedFieldChange(_CurationModel):
    """Replace one checked field of an exact semantic source member."""

    kind: Literal["field"] = "field"
    ref: SourceRecordRef
    replacement: FieldExpectation
    when: tuple[FieldExpectation, ...] = ()

    _conditions = field_validator("when")(_unique_conditions)


class CheckedPeriodChange(_CurationModel):
    """Replace the interpreted scopes of one checked source member."""

    kind: Literal["period"] = "period"
    ref: SourceRecordRef
    edition_scope: TemporalScope
    edition_period_scope: TemporalScope


class CheckedIdentityChange(_CurationModel):
    """Assign a checked source occurrence to an explicitly named variable identity."""

    kind: Literal["identity"] = "identity"
    ref: SourceRecordRef
    variable_key: NativeKey
    when: tuple[FieldExpectation, ...] = ()

    _conditions = field_validator("when")(_unique_conditions)

    @model_validator(mode="after")
    def _explicit_identity(self) -> Self:
        if not self.variable_key or any(part == "" for part in self.variable_key):
            raise ValueError("an identity assignment needs an exact nonempty key")
        return self


class CheckedSourceUse(_CurationModel):
    """Retain checked lookup/documentation rows as support rather than catalog data."""

    kind: Literal["source_use"] = "source_use"
    ref: SourceRecordRef
    use: Literal["support"] = "support"


class CheckedVariantAssignment(_CurationModel):
    """Route a checked delivered row to its explicitly accepted variants."""

    kind: Literal["variants"] = "variants"
    ref: SourceRecordRef
    variant_keys: tuple[NativeKey, ...]

    @field_validator("variant_keys")
    @classmethod
    def _finite_targets(cls, keys: tuple[NativeKey, ...]) -> tuple[NativeKey, ...]:
        if not keys or any(not key or "" in key for key in keys):
            raise ValueError("variant assignment needs explicit nonempty keys")
        if len(set(keys)) != len(keys):
            raise ValueError("variant assignment keys must be unique")
        return tuple(sorted(keys, key=repr))


class CuratedOccurrenceAddition(_CurationModel):
    """A declared delivery, not an invented physical source row or native ID.

    Keys reference independently established native or curated identities. The
    caller must resolve these identities before materialization. Copied fields
    name the exact checked donor; all other supplied facts are authored claims.
    Coding is copied only when explicitly requested, independently of scalar fields.
    An authored coverage declaration may have no delivery-edition identity; its
    explicit period still applies without inventing an edition.
    """

    kind: Literal["addition"] = "addition"
    occurrence_key: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    variable_key: NativeKey
    variant_key: NativeKey
    edition_key: NativeKey | None = None
    population_key: NativeKey | None = None
    fields: SourceFields
    edition_scope: TemporalScope
    edition_period_scope: TemporalScope
    evidence: tuple[SourceRecordRef, ...]
    donor: SourceRecordRef | None = None
    copied_fields: tuple[str, ...] = ()
    copy_coding: bool = False

    @model_validator(mode="after")
    def _explicit_membership(self) -> Self:
        if not self.variable_key or not self.variant_key or self.edition_key == ():
            raise ValueError(
                "a declared occurrence needs explicit variable/variant keys and no empty edition key"
            )
        if not self.evidence or len(set(self.evidence)) != len(self.evidence):
            raise ValueError("a declared occurrence needs unique checked evidence")
        if self.donor is not None and self.donor not in self.evidence:
            raise ValueError("the donor must be included in the checked evidence")
        if bool(self.copied_fields) != (self.donor is not None):
            raise ValueError("copied fields and their donor must be supplied together")
        if self.copy_coding and self.donor is None:
            raise ValueError("copied coding requires an explicit checked donor")
        if len(set(self.copied_fields)) != len(self.copied_fields) or any(
            name not in SourceFields.model_fields for name in self.copied_fields
        ):
            raise ValueError("copied fields must be unique known source fields")
        return self


type OccurrenceEffect = (
    CheckedFieldChange
    | CheckedPeriodChange
    | CheckedIdentityChange
    | CheckedSourceUse
    | CheckedVariantAssignment
    | CuratedOccurrenceAddition
)


class OccurrenceCorrectionDecision(_CurationModel):
    kind: Literal["correct_occurrences"] = "correct_occurrences"
    reviewed: Literal[True]
    effects: tuple[OccurrenceEffect, ...]
    reason: str = Field(min_length=1)
    provenance: str = Field(min_length=1)

    @model_validator(mode="after")
    def _nonempty(self) -> Self:
        if not self.effects or not self.reason.strip() or not self.provenance.strip():
            raise ValueError("a correction needs effects, rationale and provenance")
        return self


class SearchAliasDecision(_CurationModel):
    """Add a search spelling to finite existing variants, without claiming coverage."""

    kind: Literal["search_alias"] = "search_alias"
    reviewed: Literal[True]
    variable_key: NativeKey
    variant_keys: tuple[NativeKey, ...]
    column: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    provenance: str = Field(min_length=1)

    @model_validator(mode="after")
    def _bounded(self) -> Self:
        if (
            not self.variable_key
            or not self.variant_keys
            or any(
                not key or "" in key for key in (self.variable_key, *self.variant_keys)
            )
        ):
            raise ValueError("a search alias needs exact variable and variant keys")
        if len(set(self.variant_keys)) != len(self.variant_keys):
            raise ValueError("search alias variant keys must be unique")
        if self.column != self.column.strip() or not self.column.strip():
            raise ValueError("a search alias column must be nonempty and trimmed")
        if not self.reason.strip() or not self.provenance.strip():
            raise ValueError("a search alias needs rationale and provenance")
        return self


class FiniteCurationWindow(_CurationModel):
    """An explicitly reviewed interval, with no automatic future extension."""

    valid_from: str
    valid_to: str

    @model_validator(mode="after")
    def _bounded(self) -> Self:
        for value in (self.valid_from, self.valid_to):
            parsed = date.fromisoformat(value)
            if parsed.isoformat() != value or parsed.year == 9999:
                raise ValueError("curation windows require finite ISO date bounds")
        if self.valid_from > self.valid_to:
            raise ValueError("curation window bounds are reversed")
        return self


class AliasWindowDecision(FiniteCurationWindow):
    """Make one already-owned alias orderable within an existing state period."""

    kind: Literal["alias_window"] = "alias_window"
    reviewed: Literal[True]
    variable_key: NativeKey
    variant_key: NativeKey
    column: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    provenance: str = Field(min_length=1)

    @model_validator(mode="after")
    def _exact(self) -> Self:
        if any(not key or "" in key for key in (self.variable_key, self.variant_key)):
            raise ValueError("an alias window needs exact variable and variant keys")
        if self.column != self.column.strip() or not self.column.strip():
            raise ValueError("an alias window column must be nonempty and trimmed")
        if not self.reason.strip() or not self.provenance.strip():
            raise ValueError("an alias window needs rationale and provenance")
        return self


class ColumnRepresentation(FiniteCurationWindow):
    """One exact physical representation within a reviewed metadata window."""

    column: str = Field(min_length=1)
    # Checked over the enclosing metadata window, not this alias's subperiod.
    expected_codings: tuple[Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")], ...]

    @field_validator("column")
    @classmethod
    def _trimmed(cls, value: str) -> str:
        if not value.strip() or value != value.strip():
            raise ValueError("a representation column must be nonempty and trimmed")
        return value


class RepresentationDecision(FiniteCurationWindow):
    """Share metadata across explicitly identified parallel delivery columns."""

    kind: Literal["representations"] = "representations"
    reviewed: Literal[True]
    variable_key: NativeKey
    variant_key: NativeKey
    columns: tuple[ColumnRepresentation, ...] = Field(min_length=2)
    reason: str = Field(min_length=1)
    provenance: str = Field(min_length=1)

    @model_validator(mode="after")
    def _covered(self) -> Self:
        if any(not key or "" in key for key in (self.variable_key, self.variant_key)):
            raise ValueError("representations need exact variable and variant keys")
        if len({c.column for c in self.columns}) != len(self.columns):
            raise ValueError("representation columns must be unique")
        if any(
            c.valid_from < self.valid_from or c.valid_to > self.valid_to
            for c in self.columns
        ) or not covers_window(
            ((c.valid_from, c.valid_to) for c in self.columns),
            self.valid_from,
            self.valid_to,
        ):
            raise ValueError(
                "representation windows must cover exactly the metadata window"
            )
        if not self.reason.strip() or not self.provenance.strip():
            raise ValueError("representations need rationale and provenance")
        return self


class CodingWindow(FiniteCurationWindow):
    """The complete observed coding evidence for one finite period."""

    expected_codings: tuple[Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")], ...]

    @model_validator(mode="after")
    def _unique(self) -> Self:
        if len(set(self.expected_codings)) != len(self.expected_codings):
            raise ValueError("expected coding fingerprints must be unique")
        return self


class CodingSelection(CodingWindow):
    """A checked existing list, possibly witnessed in a different finite period."""

    selected_coding: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _existing(self) -> Self:
        if self.selected_coding not in self.expected_codings:
            raise ValueError("select one of the expected existing codings")
        return self


class _ColumnDecision(CodingWindow):
    reviewed: Literal[True]
    column_key: NativeKey
    reason: str = Field(min_length=1)
    provenance: str = Field(min_length=1)

    @model_validator(mode="after")
    def _justified(self) -> Self:
        if not self.column_key or "" in self.column_key:
            raise ValueError("a coding decision requires an exact column key")
        if not self.reason.strip() or not self.provenance.strip():
            raise ValueError("a coding decision requires rationale and provenance")
        return self


class CodingDecision(_ColumnDecision):
    """Assign coding, explicit uncoded meaning, or omission to one exact window."""

    kind: Literal["coding"] = "coding"
    selection: CodingSelection | Literal["uncoded", "omit_state"]


class ClassificationDecision(_ColumnDecision):
    """Bind an exact existing codebook, without copying its codes into the source."""

    kind: Literal["classification"] = "classification"
    classification: str = Field(min_length=1)
    expected_classification: str = Field(pattern=r"^[0-9a-f]{64}$")
    binding_scope: Literal["inline_coding", "declared"]


type CurationDecision = (
    BoundedUnresolvedDecision
    | OccurrenceCorrectionDecision
    | SearchAliasDecision
    | AliasWindowDecision
    | RepresentationDecision
    | CodingDecision
    | ClassificationDecision
)


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
        native=NativeCoordinates.model_validate(
            {
                name: getattr(record.subject.native, name)
                for name in NativeCoordinates.model_fields
                if getattr(shape.native, name) is not None
            }
        )
        if shape.native is not None
        else None,
        parent_facts=tuple(
            parent_fact_projection(parent) for parent in record.parent_facts
        )
        if shape.parent_facts is not None
        else None,
        code_set_references=(
            tuple(
                CodeSetExpectation(
                    reference_id=reference.reference_id,
                    content_sha256=reference.content_sha256,
                )
                for reference in record.code_set_references
            )
            if shape.code_set_references is not None
            else None
        ),
    )


def _field_matches(record: SourceRecord, expected: FieldExpectation) -> bool:
    return _actual_field(record, expected.name) == expected


def _peer_matches(record: SourceRecord, guard: PeerGuard) -> bool:
    if record.source != guard.source:
        return False
    for name, expected in guard.coordinates:
        actual = getattr(
            record.subject, "register_name" if name == "register" else name
        )
        if actual.status != expected.status or _coordinate_key(
            actual
        ) != _coordinate_key(expected):
            return False
    if guard.edition_scopes and record.edition_scope not in guard.edition_scopes:
        return False
    if guard.folded_column is not None:
        column = record.fields.column_name
        if (
            column is None
            or column.status != "value"
            or not isinstance(column.value, str)
            or fold_column(column.value) != guard.folded_column
        ):
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

    return evaluate_cases((case,), records)[0]


def evaluate_cases(
    cases: tuple[CurationCase, ...], records: Iterable[SourceRecord]
) -> tuple[CaseEvaluation, ...]:
    """Check decisions together against one complete, unchanged source slice."""
    evidence = (
        records if isinstance(records, SourceEvidence) else SourceEvidence(records)
    )
    return tuple(_evaluate_case(case, evidence) for case in cases)


class SourceEvidence:
    """Original source slice shared by correction and naming applicability checks.

    Every index includes newly supplied evidence, not just expected peers. Reuse
    this object only for the same immutable prepared slice; corrected occurrences
    are output and must never be substituted as its original evidence.
    """

    def __init__(self, records: Iterable[SourceRecord]) -> None:
        self.records = tuple(records)
        self.grouped: dict[tuple[str, tuple[str, ...]], list[SourceRecord]] = (
            defaultdict(list)
        )
        for record in self.records:
            self.grouped[_record_key(record)].append(record)
        self.indexes: dict[
            tuple[str, str], dict[tuple[str, object], list[SourceRecord]]
        ] = {}

    def __iter__(self) -> Iterator[SourceRecord]:
        return iter(self.records)

    @cached_property
    def native_variable_anchors(self) -> dict[NativeKey, tuple[str, NativeKey | None]]:
        """Existing native identities; naming them asserts no delivery membership."""
        return {
            key: (record.subject.provider, source_register_key(record))
            for record in self.records
            if (key := native_variable_key(record)) is not None
        }

    @staticmethod
    def _value(record: SourceRecord, selector: tuple[str, str]) -> object:
        kind, name = selector
        if kind == "native":
            return getattr(record.subject.native, name)
        if kind == "coordinate":
            coordinate = getattr(record.subject, name)
            return coordinate.status, _coordinate_key(coordinate)
        if kind == "column":
            field = record.fields.column_name
            return (
                fold_column(field.value)
                if field is not None
                and field.status == "value"
                and isinstance(field.value, str)
                else None
            )
        assert kind == "register_name"
        register = record.subject.register_name
        return register.name if register.status == "value" else None

    def peers(self, guard: PeerGuard) -> Iterable[SourceRecord]:
        # Column/variable selectors make the many finite correction checks cheap.
        # All remaining predicates still pass through the same exact matcher.
        selector: tuple[str, str] | None = None
        value: object = None
        if guard.folded_column is not None:
            selector, value = ("column", ""), guard.folded_column
        elif guard.native is not None:
            for name in (
                "member_id",
                "variable_id",
                "register_variant_id",
                "edition_id",
                "register_id",
            ):
                if (value := getattr(guard.native, name)) is not None:
                    selector = "native", name
                    break
        if selector is None and guard.coordinates:
            role, coordinate = min(
                guard.coordinates,
                key=lambda item: (item[0] not in {"member", "variable"}, item[0]),
            )
            selector = "coordinate", "register_name" if role == "register" else role
            value = coordinate.status, _coordinate_key(coordinate)
        if selector is None and guard.register_name is not None:
            selector, value = ("register_name", ""), guard.register_name
        candidates: Iterable[SourceRecord]
        if selector is None:
            candidates = self.records
        else:
            if selector not in self.indexes:
                index: dict[tuple[str, object], list[SourceRecord]] = defaultdict(list)
                for record in self.records:
                    index[record.source, self._value(record, selector)].append(record)
                self.indexes[selector] = index
            candidates = self.indexes[selector].get((guard.source, value), ())
        return (record for record in candidates if _peer_matches(record, guard))


def evaluate_source_expectations(
    targets: tuple[RecordExpectation, ...],
    support: tuple[RecordExpectation, ...],
    peer_guards: tuple[PeerGuard, ...],
    records: Iterable[SourceRecord],
) -> tuple[ApplicabilityIssue, ...]:
    """Check finite source expectations without requiring a curation decision.

    The caller must include all source records eligible for each peer guard;
    passing only the previously expected members would hide newly added peers.
    """
    return _evaluate_source_expectations(
        targets,
        support,
        peer_guards,
        records if isinstance(records, SourceEvidence) else SourceEvidence(records),
    )


def _evaluate_source_expectations(
    targets: tuple[RecordExpectation, ...],
    support: tuple[RecordExpectation, ...],
    peer_guards: tuple[PeerGuard, ...],
    evidence: SourceEvidence,
) -> tuple[ApplicabilityIssue, ...]:
    issues: list[ApplicabilityIssue] = []
    for role, expectations in (
        ("target", targets),
        ("support", support),
    ):
        for expected in expectations:
            issue = _compare_expectation(
                role=role,
                expected=expected,
                records=evidence.grouped.get(_ref_key(expected.ref), []),
            )
            if issue is not None:
                issues.append(issue)

    for guard in peer_guards:
        actual_members = {_record_key(record) for record in evidence.peers(guard)}
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

    return tuple(issues)


def _evaluate_case(
    case: CurationCase,
    evidence: SourceEvidence,
) -> CaseEvaluation:
    issues = _evaluate_source_expectations(
        case.targets, case.support, case.peer_guards, evidence
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


class ResolutionDiagnostic(_CurationModel):
    """One actionable problem or explicitly retained uncertainty."""

    code: str
    severity: Literal["error", "warning"]
    case_id: str | None = None
    subject: str
    detail: str
    refs: tuple[SourceRecordRef, ...] = ()
    applicability_issue: ApplicabilityIssue | None = None
    fields: tuple[str, ...] = ()
    withheld_output: tuple[str, ...] = ()
    valid_from: str | None = None
    valid_to: str | None = None


__all__ = [
    "ApplicabilityIssue",
    "BoundedUnresolvedDecision",
    "CaseEvaluation",
    "CodeSetExpectation",
    "CurationCase",
    "FieldExpectation",
    "ParentFactProjection",
    "PeerGuard",
    "RecordExpectation",
    "RecordProjection",
    "ResolutionDiagnostic",
    "SourceRecordRef",
    "UnresolvedAspect",
    "evaluate_case",
    "parent_fact_projection",
]
