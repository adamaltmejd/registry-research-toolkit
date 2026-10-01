"""The reviewed CIS cooperation-matrix answer partitions.

SCB's machine export gives the CIS 2016 answer columns one VarId and one CVID,
while its CIS 2014 source instance names no columns at all.  The accompanying
quality declaration distinguishes both waves' answers.  This module loads the
reviewed evidence declarations activated by register curation. The common
compiler binds them at their exact source-instance boundaries.

This is intentionally not a matrix-discovery or source-mapping framework.
Other waves need their own reviewed meaning evidence before they can acquire
answer identities or continuity links.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    ValidationError,
    model_validator,
)
from reg_meta.fqid import derive_variable_slug

from ._curation import curation_error
from .fqid_slugs import SlugEntry
from .source_coordinates import source_register_key
from .source_curation import (
    CheckedFieldChange,
    CheckedIdentityChange,
    CheckedSourceUse,
    CuratedOccurrenceAddition,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    capture_expectations,
)
from .source_naming import NamingDeclaration, NativeNamingTarget
from .source_occurrences import source_occurrence
from .source_records import NativeCoordinates, SourceFields, value_field

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from .source_coding import CodeListClaim
    from .source_coordinates import NativeKey
    from .source_curation import OccurrenceEffect, SourceRecordRef
    from .source_records import SourceRecord


NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
StableKey = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$",
    ),
]
AxisKey = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        pattern=r"^[a-z0-9]+(?:_[a-z0-9]+)*$",
    ),
]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class _CurationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MatrixSelector(_CurationModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    register_fqid: NonEmpty = Field(alias="register")
    register_id: Annotated[int, Field(ge=0)]
    variant: NonEmpty
    register_variant_id: Annotated[int, Field(ge=0)]
    edition: NonEmpty
    regver_id: Annotated[int, Field(ge=0)]
    var_id: Annotated[int, Field(ge=0)]
    cvid: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def _scb_register_fqid(self) -> MatrixSelector:
        parts = self.register_fqid.split("/")
        if len(parts) != 2 or parts[0] != "scb" or not parts[1]:
            raise ValueError("selector.register must be a 2-segment scb/* FQID")
        return self


class MatrixEvidence(_CurationModel):
    document: NonEmpty
    url: NonEmpty
    sha256: Sha256
    question: NonEmpty
    noted: Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}$")]


class MatrixAxis(_CurationModel):
    key: AxisKey
    label_en: NonEmpty


class MatrixAnswer(_CurationModel):
    key: StableKey
    slug: StableKey
    columns: tuple[NonEmpty, ...]
    label_en: NonEmpty
    definition_en: NonEmpty
    partner: MatrixAxis
    response: MatrixAxis
    source_pages: dict[NonEmpty, Annotated[int, Field(ge=1)]]
    meaning_evidence: NonEmpty | None = None

    @model_validator(mode="after")
    def _complete_answer_evidence(self) -> MatrixAnswer:
        if not self.columns:
            raise ValueError("an answer needs at least one source column")
        if len(set(self.columns)) != len(self.columns):
            raise ValueError(f"answer {self.key!r} repeats a source column")
        if set(self.source_pages) != set(self.columns):
            raise ValueError(
                f"answer {self.key!r} source_pages must name exactly its columns"
            )
        if len(self.columns) > 1 and self.meaning_evidence is None:
            raise ValueError(
                f"answer {self.key!r} joins aliases without meaning_evidence"
            )
        if derive_variable_slug(self.slug) != self.slug:
            raise ValueError(f"answer {self.key!r} has an invalid variable slug")
        return self


class _CisMatrix(_CurationModel):
    selector: MatrixSelector
    evidence: MatrixEvidence
    question_label: NonEmpty
    axes: tuple[MatrixAxis, MatrixAxis]
    answers: tuple[MatrixAnswer, ...]

    @model_validator(mode="after")
    def _complete_partition(self) -> _CisMatrix:
        if tuple(axis.key for axis in self.axes) != ("partner", "response"):
            raise ValueError("axes must be ordered as partner, response")
        if len(self.answers) < 2:
            raise ValueError("the matrix partition needs at least two answers")

        for attr in ("key", "slug"):
            values = [getattr(answer, attr) for answer in self.answers]
            if len(set(values)) != len(values):
                raise ValueError(f"matrix answers repeat {attr} selectors")

        columns = [column for answer in self.answers for column in answer.columns]
        if len(set(columns)) != len(columns):
            raise ValueError("matrix answers assign a source column more than once")

        coordinates = [
            (answer.partner.key, answer.response.key) for answer in self.answers
        ]
        if len(set(coordinates)) != len(coordinates):
            raise ValueError("matrix answers repeat a partner/response coordinate")

        for axis_name in ("partner", "response"):
            labels: dict[str, str] = {}
            for answer in self.answers:
                axis = getattr(answer, axis_name)
                existing = labels.setdefault(axis.key, axis.label_en)
                if existing != axis.label_en:
                    raise ValueError(
                        f"matrix answers conflict on {axis_name} label {axis.key!r}"
                    )
        return self

    @property
    def columns(self) -> frozenset[str]:
        return frozenset(column for answer in self.answers for column in answer.columns)


class Cis2016Matrix(_CisMatrix):
    """The named-column CIS 2016 answer declaration."""


class MatrixAnswerFacts(_CurationModel):
    data_type: Literal["text", "decimal", "integer", "date"]
    data_type_evidence: NonEmpty
    is_identifier: StrictBool
    is_sensitive: StrictBool
    flag_evidence: NonEmpty


class Cis2014Matrix(_CisMatrix):
    """The documented answers for the exact blank-column CIS 2014 source."""

    source_mode: Literal["documented_blank"]
    answer_facts: MatrixAnswerFacts

    @model_validator(mode="after")
    def _single_column_answers(self) -> Cis2014Matrix:
        if any(len(answer.columns) != 1 for answer in self.answers):
            raise ValueError(
                "documented_blank answers must each name one reviewed column"
            )
        return self


def load_matrix(
    path: Path,
    *,
    source_mode: Literal["named", "documented_blank"],
    expected_selector: MatrixSelector,
) -> Cis2014Matrix | Cis2016Matrix:
    """Read activated meaning evidence bound to an exact reviewed selector."""
    model = Cis2014Matrix if source_mode == "documented_blank" else Cis2016Matrix
    try:
        matrix = model.model_validate_json(path.read_bytes())
    except (OSError, ValidationError) as exc:
        raise curation_error(
            "matrix_evidence_invalid",
            f"Could not load matrix evidence {path}: {exc}",
            f"Fix the selectors and evidence in {path}.",
        ) from exc

    if matrix.selector != expected_selector:
        raise curation_error(
            "matrix_evidence_unknown_selector",
            "Matrix evidence selector differs from the checked declaration: "
            f"expected {expected_selector!r}, observed {matrix.selector!r}.",
            "Keep the evidence and activation bound to the same reviewed source coordinate.",
        )

    return matrix


@dataclass(frozen=True)
class MatrixConversion:
    case: CurationCase
    naming: tuple[NamingDeclaration, ...]
    provider_keys: dict[NativeKey, str]


def convert_matrix(
    matrix: Cis2014Matrix | Cis2016Matrix,
    records: tuple[SourceRecord, ...],
    *,
    case_id: str,
    provenance: str,
    coding: Mapping[SourceRecordRef, tuple[CodeListClaim, ...]] | None = None,
) -> MatrixConversion:
    """Bind an accepted answer partition to its complete original source scope.

    ``records`` must include every original member under the selected native
    register/variant/edition/variable, including competitors with another CVID.
    The emitted peer guard checks that same scope on replay. Other editions and
    variables are left untouched.
    """
    from .source_coding import copied_coding_fingerprints

    selector = matrix.selector
    native = NativeCoordinates(
        register_id=selector.register_id,
        register_variant_id=selector.register_variant_id,
        edition_id=selector.regver_id,
        variable_id=selector.var_id,
    )
    selected = tuple(
        record
        for record in records
        if record.subject.provider == "scb"
        and all(
            getattr(record.subject.native, name) == value
            for name, value in native.model_dump(exclude_none=True).items()
        )
    )
    if not selected:
        raise ValueError("accepted matrix has no matching original source members")
    if len({record.source for record in selected}) != 1 or {
        record.subject.native.member_id for record in selected
    } != {selector.cvid}:
        raise ValueError("accepted matrix source or complete CVID partition changed")
    if any(record.edition_scope.label != selector.edition for record in selected):
        raise ValueError("accepted matrix edition label no longer matches its selector")
    blank = isinstance(matrix, Cis2014Matrix)
    columns = {
        field.value if field is not None and field.status == "value" else None
        for record in selected
        for field in (record.fields.column_name,)
    }
    if columns != ({None} if blank else matrix.columns):
        raise ValueError("accepted matrix complete literal column partition changed")
    expected = capture_expectations(
        selected, fields=tuple(SourceFields.model_fields), coding=True
    )
    if len(expected) != 1:
        raise ValueError("accepted matrix must identify one exact source member")
    donor = expected[0]
    if blank and len(donor.alternatives) != 1:
        raise ValueError("blank matrix donor has competing occurrence metadata")
    if blank and (
        coding is None
        or donor.ref not in coding
        or len(coding[donor.ref]) != 1
        or not coding[donor.ref][0].members
    ):
        raise ValueError(
            "blank matrix donor requires exactly one complete bound coding list"
        )
    copied_codings = (
        copied_coding_fingerprints(coding[donor.ref])
        if blank and coding is not None
        else None
    )
    guard = PeerGuard(
        guard_id=f"{case_id}:complete-partition",
        source=selected[0].source,
        native=native,
        expected_members=(donor.ref,),
    )
    register_key = source_register_key(selected[0])
    original = source_occurrence(selected[0])
    if (
        register_key is None
        or original.variant_key is None
        or original.edition_key is None
    ):
        raise ValueError("accepted matrix needs established native parent identities")
    effects: list[OccurrenceEffect] = []
    if blank:
        effects.append(CheckedSourceUse(ref=donor.ref))
    names = []
    provider_keys = {}
    for answer in sorted(matrix.answers, key=lambda item: item.key):
        key = (*register_key, "accepted-matrix", case_id, answer.key)
        provider_keys[key] = str(selector.var_id)
        authored = {
            "name": value_field(answer.label_en),
            "definition": value_field(answer.definition_en),
            "description": value_field(
                f"{matrix.question_label} ({', '.join(answer.columns)}; {selector.edition})."
            ),
            "operational_definition": value_field(answer.definition_en),
        }
        if isinstance(matrix, Cis2014Matrix):
            facts = matrix.answer_facts
            authored.update(
                data_type=value_field(facts.data_type),
                identifier=value_field(facts.is_identifier),
                sensitivity=value_field(facts.is_sensitive),
            )
        for column in answer.columns:
            if blank:
                fields = selected[0].fields.model_copy(
                    update={**authored, "column_name": value_field(column)}
                )
                effects.append(
                    CuratedOccurrenceAddition(
                        occurrence_key=f"{case_id}:{answer.key}:{column}",
                        provider="scb",
                        variable_key=key,
                        variant_key=original.variant_key,
                        edition_key=original.edition_key,
                        population_key=original.population_key,
                        fields=fields,
                        edition_scope=original.edition_scope,
                        edition_period_scope=original.edition_period_scope,
                        evidence=(donor.ref,),
                        donor=donor.ref,
                        copied_fields=tuple(
                            name
                            for name in SourceFields.model_fields
                            if name not in {*authored, "column_name"}
                        ),
                        copy_coding=True,
                        expected_codings=copied_codings,
                    )
                )
            else:
                when = (
                    FieldExpectation(name="column_name", status="value", value=column),
                )
                effects.append(
                    CheckedIdentityChange(ref=donor.ref, variable_key=key, when=when)
                )
                effects.extend(
                    CheckedFieldChange(
                        ref=donor.ref,
                        replacement=FieldExpectation(
                            name=name, status=field.status, value=field.value
                        ),
                        when=when,
                    )
                    for name, field in authored.items()
                )
        names.append(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="variable",
                    provider="scb",
                    source_key=key,
                    register_key=register_key,
                    expectations=expected,
                    peer_guards=(guard,),
                ),
                naming=SlugEntry(
                    kind="variable",
                    source_id=f"{case_id}:{answer.key}",
                    slug=answer.slug,
                    provider="scb",
                ),
                contributors=(),
            )
        )
    case = CurationCase(
        case_id=case_id,
        targets=expected,
        peer_guards=(guard,),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(effects),
            reason="Preserve the existing reviewed answer identities and exact source partition. "
            + matrix.evidence.question,
            provenance=provenance
            + "; accepted declaration="
            + matrix.model_dump_json(by_alias=True),
        ),
    )
    return MatrixConversion(case, tuple(names), provider_keys)
