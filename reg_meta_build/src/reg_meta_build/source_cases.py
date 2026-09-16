"""Strict finite source-case evaluation over retained SCB observations.

This module contains the shared source boundary for the one authored HAMN Signal
unresolved-length proposal.  It deliberately does not apply the proposal to catalog
formation: inspection and any future production consumer must use the same evaluator.
"""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from reg_meta_build.input_snapshot import SnapshotError
from reg_meta_build.source_records import (
    RecordLocator,
    SourceField,
    SourceFields,
    SourceRecord,
    SourceRevision,
    canonical_sha256,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from pathlib import Path

    from reg_meta_build.sources.scb_records import ScbObservation

HAMN_SIGNAL_CASE_ID = "hamn-signal-unresolved-length"
HAMN_SIGNAL_CASE_FILE = "hamn_signal_unresolved_length.json"

_TARGETS = (
    (2003, 204, 2181),
    (2004, 466, 6294),
    (2005, 921, 27053),
    (2006, 923, 27100),
    (2007, 1295, 103752),
    (2008, 1447, 108315),
    (2009, 2932, 172858),
    (2010, 3823, 236400),
    (2011, 4301, 274240),
    (2012, 4706, 292587),
)

CheckName = Literal[
    "source_dependency",
    "context_dependency",
    "target_coordinates",
    "annual_scope_dependency",
    "finite_membership",
    "competing_spelling_scope",
    "before_alternatives",
    "physical_multiplicity",
]


class _CaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class HamnSourceDependency(_CaseModel):
    dataset: Literal["scb-registerinformation"]
    provider: Literal["scb"]
    register_id: Literal[161]
    register_variant_id: Literal[232]
    variable_id: Literal[1880]
    register_name: str
    register_title: str
    register_purpose: str


class HamnContextDependency(_CaseModel):
    variant_title: str
    variant_name: str
    variant_description: str
    variant_secrecy: str
    document_status: str
    population_name: str
    population_definition: str
    population_comment: str
    object_type_name: str
    object_type_definition: str
    variable_name: str
    variable_definition: str
    variable_description: str
    operational_definition: str
    reference_time: str
    variable_register_source: str
    external_comment: str
    measurement_unit: str
    column_name: Literal["Signal"]
    data_type: Literal["char"]


class HamnScopeDependency(_CaseModel):
    register_id: Literal[161]
    register_variant_id: Literal[232]
    annual_editions: tuple[int, ...]
    competing_spelling: Literal["Signal"]
    spelling_comparison: Literal["trimmed_casefold"]

    @model_validator(mode="after")
    def _finite_scope(self) -> Self:
        expected = tuple(target[0] for target in _TARGETS)
        if self.annual_editions != expected:
            raise ValueError(
                f"annual_editions must be the exact finite scope {expected}"
            )
        return self


class HamnBeforeAlternative(_CaseModel):
    data_type: Literal["char"]
    data_length: Literal["10", "11"]
    occurrence_count: Literal[1]


class HamnMemberProvenance(_CaseModel):
    source_rows: tuple[int, int]
    first_approval_date: str
    last_approval_date: str

    @model_validator(mode="after")
    def _two_distinct_rows(self) -> Self:
        if self.source_rows[0] == self.source_rows[1]:
            raise ValueError("member provenance must name two distinct source rows")
        return self


class HamnMemberDependency(_CaseModel):
    edition: int
    edition_id: int
    member_id: int
    version_name: str
    version_description: str
    version_measurement_information: str
    population_date: str
    variable_source_attribution: str
    before_alternatives: tuple[HamnBeforeAlternative, HamnBeforeAlternative]
    provenance: HamnMemberProvenance

    @model_validator(mode="after")
    def _exact_alternatives(self) -> Self:
        lengths = tuple(item.data_length for item in self.before_alternatives)
        if lengths != ("10", "11"):
            raise ValueError(
                "before_alternatives must explicitly be char/10 then char/11"
            )
        if self.version_name != str(self.edition):
            raise ValueError(
                "version_name must explicitly agree with the member edition"
            )
        return self


class HamnProposal(_CaseModel):
    disposition: Literal["withhold_data_length"]
    withheld_field: Literal["data_length"]
    preserve_all_other_fields: Literal[True]
    rationale: str
    review_material: tuple[str, ...]
    production_dependencies: tuple[str, ...]

    @model_validator(mode="after")
    def _reviewable(self) -> Self:
        if (
            not self.rationale.strip()
            or not self.review_material
            or not self.production_dependencies
            or not all(
                item.strip()
                for item in self.review_material + self.production_dependencies
            )
        ):
            raise ValueError("rationale and review material must be non-empty")
        return self


class HamnCaseProvenance(_CaseModel):
    source_artifact_sha256: str
    census_ticket: Literal["Y-162"]

    @model_validator(mode="after")
    def _hash(self) -> Self:
        if len(self.source_artifact_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in self.source_artifact_sha256
        ):
            raise ValueError("source_artifact_sha256 must be a lowercase SHA-256")
        return self


class HamnSignalSourceCase(_CaseModel):
    schema_version: Literal[1]
    case_id: Literal["hamn-signal-unresolved-length"]
    source_dependency: HamnSourceDependency
    context_dependency: HamnContextDependency
    member_dependency: tuple[HamnMemberDependency, ...]
    scope_dependency: HamnScopeDependency
    proposal: HamnProposal
    provenance: HamnCaseProvenance

    @model_validator(mode="after")
    def _exact_members(self) -> Self:
        targets = tuple(
            (member.edition, member.edition_id, member.member_id)
            for member in self.member_dependency
        )
        if targets != _TARGETS:
            raise ValueError(f"member_dependency must explicitly list {_TARGETS}")
        rows = tuple(
            row
            for member in self.member_dependency
            for row in member.provenance.source_rows
        )
        if len(rows) != 20 or len(set(rows)) != 20:
            raise ValueError("member provenance must name exactly twenty source rows")
        return self


class SourceCaseArtifact(_CaseModel):
    path: str
    size: int
    sha256: str
    authored_case: HamnSignalSourceCase


class HamnCaseCheck(_CaseModel):
    name: CheckName
    passed: bool
    detail: str


class HamnCaseBlocker(_CaseModel):
    check: CheckName
    target: tuple[int, int, int] | None = None
    detail: str


class ProposedHamnSourceMember(_CaseModel):
    edition: int
    edition_id: int
    member_id: int
    disposition: Literal["withhold_data_length"]
    fields: SourceFields
    evidence_record_ids: tuple[str, str]
    evidence_locators: tuple[RecordLocator, RecordLocator]

    @model_validator(mode="after")
    def _length_is_withheld(self) -> Self:
        if self.fields.data_length != SourceField(status="unknown", raw_value=None):
            raise ValueError("proposed member must withhold data_length")
        if self.fields.data_type is None or self.fields.data_type.value != "char":
            raise ValueError("proposed member must retain char data_type")
        return self


class HamnEvaluationReceipt(_CaseModel):
    receipt_id: str
    case_id: Literal["hamn-signal-unresolved-length"]
    case_sha256: str
    source_revision: SourceRevision
    evidence_record_ids: tuple[str, ...]
    evidence_locators: tuple[RecordLocator, ...]
    checks: tuple[HamnCaseCheck, ...]

    @model_validator(mode="after")
    def _parallel_evidence(self) -> Self:
        if len(self.evidence_record_ids) != len(self.evidence_locators):
            raise ValueError("receipt record IDs and locators must be parallel")
        return self


class HamnSourceCaseEvaluation(_CaseModel):
    preview_level: Literal["source_target_only"]
    applicable: bool
    catalog_changed: Literal[False]
    production_dependencies: Literal["pending"]
    final_effects: Literal["pending"]
    evidence: tuple[SourceRecord, ...]
    proposed_members: tuple[ProposedHamnSourceMember, ...]
    blockers: tuple[HamnCaseBlocker, ...]
    receipt: HamnEvaluationReceipt

    @model_validator(mode="after")
    def _coherent(self) -> Self:
        if self.applicable != (not self.blockers):
            raise ValueError("applicability must agree with blockers")
        if self.applicable != all(check.passed for check in self.receipt.checks):
            raise ValueError("applicability must agree with named checks")
        expected_proposals = 10 if self.applicable else 0
        if len(self.proposed_members) != expected_proposals:
            raise ValueError(
                f"expected {expected_proposals} proposed members when "
                f"applicable is {self.applicable}"
            )
        return self


def load_hamn_signal_source_case(path: Path) -> SourceCaseArtifact:
    """Load one strict authored HAMN proposal without any checkout fallback."""
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise SnapshotError(f"captured source case not found: {path}") from exc
    try:
        authored = HamnSignalSourceCase.model_validate_json(payload)
    except ValidationError as exc:
        raise SnapshotError(f"invalid HAMN source case {path}: {exc}") from exc
    return SourceCaseArtifact(
        path=f"curation/{path.name}",
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        authored_case=authored,
    )


def _cells(record: SourceRecord) -> dict[str, str]:
    return {
        cell.name: cell.interpreted_value
        for cell in record.delivered_cells
        if cell.present
    }


def _annual_year(record: SourceRecord) -> int | None:
    scope = record.edition_scope
    if scope.kind != "intervals" or len(scope.intervals) != 1:
        return None
    interval = scope.intervals[0]
    if interval.start != interval.end or not interval.start.isdecimal():
        return None
    return int(interval.start)


def _spelling(record: SourceRecord) -> str | None:
    field = record.fields.column_name
    if field is None or field.status != "value" or not isinstance(field.value, str):
        return None
    return field.value.strip().casefold()


def _record_semantic_sort_key(
    record: SourceRecord,
) -> tuple[int, int, int, str, str]:
    native = record.subject.native
    return (
        _annual_year(record) or -1,
        native.edition_id or -1,
        native.member_id or -1,
        str(record.fields.data_length.value if record.fields.data_length else ""),
        record.record_id,
    )


def _locator_sort_key(
    locator: RecordLocator,
) -> tuple[tuple[str, ...], str, str, str, tuple[str, ...]]:
    return (
        locator.semantic_record_key,
        locator.physical_file,
        locator.physical_table,
        locator.physical_record,
        locator.physical_cells,
    )


def _block(
    blockers: list[HamnCaseBlocker],
    check: CheckName,
    detail: str,
    target: tuple[int, int, int] | None = None,
) -> None:
    blockers.append(HamnCaseBlocker(check=check, target=target, detail=detail))


def _compare_cells(
    *,
    cells: Mapping[str, str],
    expected: Mapping[str, str],
    check: CheckName,
    target: tuple[int, int, int],
    blockers: list[HamnCaseBlocker],
) -> None:
    for name, value in expected.items():
        if cells.get(name) != value:
            _block(
                blockers,
                check,
                f"{name} expected {value!r}, observed {cells.get(name)!r}",
                target,
            )


def _source_cells(case: HamnSignalSourceCase) -> dict[str, str]:
    dep = case.source_dependency
    return {
        "Registernamn": dep.register_name,
        "Registerrubrik": dep.register_title,
        "Registersyfte": dep.register_purpose,
        "RegisterId": str(dep.register_id),
        "RegVarID": str(dep.register_variant_id),
        "VarId": str(dep.variable_id),
    }


def _context_cells(case: HamnSignalSourceCase) -> dict[str, str]:
    dep = case.context_dependency
    return {
        "Registervariantrubrik": dep.variant_title,
        "Registervariantnamn": dep.variant_name,
        "Registervariantbeskrivning": dep.variant_description,
        "RegistervariantSekretess": dep.variant_secrecy,
        "Registerversion_DocStaus": dep.document_status,
        "Populationnamn": dep.population_name,
        "Populationdefinition": dep.population_definition,
        "Populationkommentar": dep.population_comment,
        "Objekttypnamn": dep.object_type_name,
        "Objekttypdefinition": dep.object_type_definition,
        "Variabelnamn": dep.variable_name,
        "Variabeldefinition": dep.variable_definition,
        "Variabelbeskrivning": dep.variable_description,
        "VariabelOperationell_definition": dep.operational_definition,
        "VariabelReferenstid": dep.reference_time,
        "VariabelRegister_Källa": dep.variable_register_source,
        "VariabelExtern_kommentar": dep.external_comment,
        "Mattenhet": dep.measurement_unit,
        "Kolumnnamn": dep.column_name,
        "Datatyp": dep.data_type,
    }


def _member_cells(member: HamnMemberDependency) -> dict[str, str]:
    return {
        "Registerversionnamn": member.version_name,
        "Registerversionbeskrivning": member.version_description,
        "Registerversionmätinformation": member.version_measurement_information,
        "Populationdatum": member.population_date,
        "VariabelHämtadFrån": member.variable_source_attribution,
        "CVID": str(member.member_id),
        "RegVerID": str(member.edition_id),
    }


def evaluate_hamn_signal_source_case(
    artifact: SourceCaseArtifact,
    observations: Iterable[ScbObservation],
    source_revision: SourceRevision,
) -> HamnSourceCaseEvaluation:
    """Evaluate the finite proposal using only original source observations.

    Whole-file hashes and retained original row numbers remain provenance.  The
    applicability guard is the authored semantic dependency set and exact physical
    multiplicity within the independently declared finite spelling/member scope.
    """
    case = artifact.authored_case
    expected_members = {
        (member.edition, member.edition_id, member.member_id): member
        for member in case.member_dependency
    }
    reviewed_years = set(case.scope_dependency.annual_editions)
    expected_spelling = case.scope_dependency.competing_spelling.strip().casefold()
    relevant: list[SourceRecord] = []
    blockers: list[HamnCaseBlocker] = []

    for observation in observations:
        record = observation.record
        native = record.subject.native
        if native.register_id != case.scope_dependency.register_id:
            continue
        if (
            native.register_variant_id is not None
            and native.register_variant_id != case.scope_dependency.register_variant_id
        ):
            continue
        matches_identity = native.variable_id == case.source_dependency.variable_id
        matches_spelling = _spelling(record) == expected_spelling
        if not (matches_identity or matches_spelling):
            continue
        year = _annual_year(record)
        if year is None:
            relevant.append(record)
            _block(
                blockers,
                "annual_scope_dependency",
                "potentially intersecting variable/spelling has no single annual scope",
            )
            continue
        # The exact reference period is a per-member dependency checked through
        # Populationdatum below; do not impose a calendar-year shape (2009 starts
        # on January 31 in the reviewed evidence).
        if year not in reviewed_years:
            continue
        relevant.append(record)
        if matches_spelling and not matches_identity:
            _block(
                blockers,
                "competing_spelling_scope",
                "reviewed Signal spelling is occupied by another VarId",
            )
        if observation.issue is not None:
            _block(
                blockers,
                "annual_scope_dependency",
                f"intersecting observation has {observation.issue.kind}",
            )

    relevant.sort(
        key=lambda record: (
            *_record_semantic_sort_key(record),
            min(_locator_sort_key(locator) for locator in record.locators),
        )
    )
    groups: dict[tuple[int, int, int], list[SourceRecord]] = defaultdict(list)
    source_expected = _source_cells(case)
    context_expected = _context_cells(case)

    for record in relevant:
        native = record.subject.native
        year = _annual_year(record)
        target = (
            year if year is not None else -1,
            native.edition_id if native.edition_id is not None else -1,
            native.member_id if native.member_id is not None else -1,
        )
        if target not in expected_members:
            _block(
                blockers,
                "finite_membership",
                "intersecting source occupancy is outside the ten exact targets",
                target,
            )
            continue
        groups[target].append(record)
        if native.register_variant_id is None:
            _block(
                blockers,
                "target_coordinates",
                "potentially intersecting record has unknown register variant",
                target,
            )
        elif (
            native.register_id != case.source_dependency.register_id
            or native.register_variant_id != case.source_dependency.register_variant_id
            or native.variable_id != case.source_dependency.variable_id
            or record.subject.provider != case.source_dependency.provider
        ):
            _block(blockers, "target_coordinates", "native coordinates changed", target)
        if record.source != case.source_dependency.dataset:
            _block(
                blockers,
                "source_dependency",
                f"dataset expected {case.source_dependency.dataset!r}, observed {record.source!r}",
                target,
            )
        if record.source_revision_id != source_revision.revision_id:
            _block(
                blockers,
                "source_dependency",
                "observation does not reference the supplied current source revision",
                target,
            )
        cells = _cells(record)
        _compare_cells(
            cells=cells,
            expected=source_expected,
            check="source_dependency",
            target=target,
            blockers=blockers,
        )
        _compare_cells(
            cells=cells,
            expected=context_expected,
            check="context_dependency",
            target=target,
            blockers=blockers,
        )
        _compare_cells(
            cells=cells,
            expected=_member_cells(expected_members[target]),
            check="context_dependency",
            target=target,
            blockers=blockers,
        )

    for target, member in expected_members.items():
        records = groups.get(target, [])
        if not records:
            _block(
                blockers,
                "finite_membership",
                "expected native member is missing",
                target,
            )
            continue
        occurrences = tuple(
            (record, locator) for record in records for locator in record.locators
        )
        if len(occurrences) != 2:
            _block(
                blockers,
                "physical_multiplicity",
                f"expected 2 physical occurrences, observed {len(occurrences)}",
                target,
            )
        alternatives = Counter(
            (
                record.fields.data_type.value if record.fields.data_type else None,
                record.fields.data_length.value if record.fields.data_length else None,
            )
            for record, _locator in occurrences
        )
        expected_alternatives = Counter(
            (alternative.data_type, alternative.data_length)
            for alternative in member.before_alternatives
        )
        if alternatives != expected_alternatives:
            _block(
                blockers,
                "before_alternatives",
                f"expected {dict(expected_alternatives)!r}, observed {dict(alternatives)!r}",
                target,
            )

    relevant_occurrence_count = sum(len(record.locators) for record in relevant)
    if relevant_occurrence_count != 20:
        _block(
            blockers,
            "physical_multiplicity",
            "reviewed cohort expected 20 physical occurrences, observed "
            f"{relevant_occurrence_count}",
        )
    if len(groups) != 10:
        _block(
            blockers,
            "finite_membership",
            f"reviewed cohort expected 10 native members, observed {len(groups)}",
        )

    blockers = list(dict.fromkeys(blockers))
    checks = tuple(
        HamnCaseCheck(
            name=name,
            passed=not any(blocker.check == name for blocker in blockers),
            detail={
                "source_dependency": "logical SCB source, register identity and purpose",
                "context_dependency": "reviewed variant, population, object and variable meaning",
                "target_coordinates": "ten explicit edition/RegVerID/CVID coordinates",
                "annual_scope_dependency": "independently declared annual 2003-2012 scopes",
                "finite_membership": "complete surrounding native-member occupancy",
                "competing_spelling_scope": "trimmed case-insensitive Signal occupancy across VarIds",
                "before_alternatives": "one char/10 and one char/11 alternative per member",
                "physical_multiplicity": "twenty physical occurrences without deduplication",
            }[name],
        )
        for name in (
            "source_dependency",
            "context_dependency",
            "target_coordinates",
            "annual_scope_dependency",
            "finite_membership",
            "competing_spelling_scope",
            "before_alternatives",
            "physical_multiplicity",
        )
    )
    applicable = not blockers
    proposed: list[ProposedHamnSourceMember] = []
    if applicable:
        for target in _TARGETS:
            occurrences = sorted(
                (
                    (record, locator)
                    for record in groups[target]
                    for locator in record.locators
                ),
                key=lambda occurrence: (
                    str(
                        occurrence[0].fields.data_length.value
                        if occurrence[0].fields.data_length is not None
                        else ""
                    ),
                    _locator_sort_key(occurrence[1]),
                ),
            )
            first, second = occurrences
            fields = first[0].fields.model_copy(
                update={"data_length": SourceField(status="unknown", raw_value=None)}
            )
            proposed.append(
                ProposedHamnSourceMember(
                    edition=target[0],
                    edition_id=target[1],
                    member_id=target[2],
                    disposition="withhold_data_length",
                    fields=fields,
                    evidence_record_ids=(first[0].record_id, second[0].record_id),
                    evidence_locators=(first[1], second[1]),
                )
            )

    evidence_occurrences = tuple(
        sorted(
            ((record, locator) for record in relevant for locator in record.locators),
            key=lambda occurrence: (
                *_record_semantic_sort_key(occurrence[0]),
                _locator_sort_key(occurrence[1]),
            ),
        )
    )
    evidence_ids = tuple(record.record_id for record, _locator in evidence_occurrences)
    evidence_locators = tuple(locator for _record, locator in evidence_occurrences)
    receipt_payload = {
        "case_id": case.case_id,
        "case_sha256": artifact.sha256,
        "source_revision_id": source_revision.revision_id,
        "evidence": [
            {
                "record_id": record.record_id,
                "locator": locator.model_dump(mode="json"),
            }
            for record, locator in evidence_occurrences
        ],
        "checks": [check.model_dump(mode="json") for check in checks],
    }
    receipt = HamnEvaluationReceipt(
        receipt_id=f"hamn-source-case:sha256:{canonical_sha256(receipt_payload)}",
        case_id=case.case_id,
        case_sha256=artifact.sha256,
        source_revision=source_revision,
        evidence_record_ids=evidence_ids,
        evidence_locators=evidence_locators,
        checks=checks,
    )
    return HamnSourceCaseEvaluation(
        preview_level="source_target_only",
        applicable=applicable,
        catalog_changed=False,
        production_dependencies="pending",
        final_effects="pending",
        evidence=tuple(relevant),
        proposed_members=tuple(proposed),
        blockers=tuple(blockers),
        receipt=receipt,
    )


__all__ = [
    "HAMN_SIGNAL_CASE_FILE",
    "HAMN_SIGNAL_CASE_ID",
    "HamnSignalSourceCase",
    "HamnSourceCaseEvaluation",
    "ProposedHamnSourceMember",
    "SourceCaseArtifact",
    "evaluate_hamn_signal_source_case",
    "load_hamn_signal_source_case",
]
