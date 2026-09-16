"""Diagnostic source-target inspection before curation or catalog formation."""

from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

import reg_meta.fqid as reg_meta_fqid
import reg_meta.queries as reg_meta_queries
from pydantic import BaseModel, ConfigDict, model_validator

from reg_meta_build import _curation as curation_module
from reg_meta_build.input_snapshot import (
    LISA_DATASET_ID,
    SnapshotError,
    _decode_cell,
    _tracked_source_commit,
    _update_record_hash,
)
from reg_meta_build.source_cases import (
    HAMN_SIGNAL_CASE_FILE,
    HAMN_SIGNAL_CASE_ID,
    HamnSourceCaseEvaluation,
    SourceCaseArtifact,
    evaluate_hamn_signal_source_case,
    load_hamn_signal_source_case,
)
from reg_meta_build.source_records import (
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    ScopeInterval,
    SourceField,
    SourceRecord,
    SourceRevision,
    TemporalScope,
    canonical_sha256,
)
from reg_meta_build.sources.lisa import read_lisa_source
from reg_meta_build.sources.scb_records import (
    LISA_REGISTER_ID,
    iter_scb_observations,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from reg_meta_build.input_snapshot import (
        BundleFile,
        CatalogBundleReader,
        SupplementalDataset,
    )

INTERPRETATION_ID = "scb-lisa-source-record-inspection-v3"
SCB_DATASET_ID = "scb-registerinformation"
CENSUS_INTERPRETATION_ID = "scb-registerinformation-observation-census-v3"
SOURCE_CASE_INTERPRETATION_ID = "hamn-signal-source-case-v1"
OutcomeStatus = Literal[
    "agreement",
    "unobserved_counterpart",
    "missing_edition",
    "conflict",
    "ambiguous_match",
    "unknown_spelling",
    "unknown_applicability",
    "source_only_observation",
]


class _ReportModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ComparisonOutcome(_ReportModel):
    comparison_key: tuple[str, ...]
    status: OutcomeStatus
    column_name: str
    edition_scope: TemporalScope
    workbook_record_ids: tuple[str, ...]
    scb_record_ids: tuple[str, ...]
    assumption_ids: tuple[str, ...]
    scb_edition_present: bool | None = None
    register_variant_ids: tuple[int, ...]
    variable_ids: tuple[int, ...]
    member_ids: tuple[int, ...]
    detail: str

    @model_validator(mode="after")
    def _identifies_assumptions(self) -> Self:
        if not self.assumption_ids or len(self.assumption_ids) != len(
            set(self.assumption_ids)
        ):
            raise ValueError("comparison assumptions must be non-empty and unique")
        return self


class InterpretationIssue(_ReportModel):
    kind: Literal["pooled_period", "unparseable_period"]
    incomplete: bool
    physical_record: str
    source_record_ids: tuple[str, ...]
    detail: str


class InspectionAssumption(_ReportModel):
    assumption_id: str
    status: Literal["unresolved"]
    purpose: str
    detail: str


class SourceTargetPreview(_ReportModel):
    preview_level: Literal["source_target_only"]
    executable_selector: Literal[False]
    exact_column: str
    target_field: Literal["column_name"]
    match_policy: Literal[
        "finite_name_native_anchor_then_same_variable_annual_workbook_scope"
    ]
    candidate_variable_ids: tuple[int, ...]
    candidate_variant_ids: tuple[int, ...]
    documented_edition_scope: TemporalScope | None
    expected_source_target_count: int
    target_record_ids: tuple[str, ...]
    existing_named_record_ids: tuple[str, ...]
    casefold_collision_record_ids: tuple[str, ...]
    unobserved_documented_editions: tuple[int, ...]
    assumption_ids: tuple[str, ...]

    @model_validator(mode="after")
    def _count_matches_membership(self) -> Self:
        if self.expected_source_target_count != len(self.target_record_ids):
            raise ValueError("source target count disagrees with target membership")
        if not self.assumption_ids:
            raise ValueError("source target preview must identify its assumptions")
        return self


class InspectionScope(_ReportModel):
    selected_sources: tuple[str, ...]
    excluded_inputs: tuple[str, ...]
    register_ids: tuple[int, ...]
    exact_column_filter: str | None
    filter_semantics: Literal["exact_source_spelling"]


class InspectionPins(_ReportModel):
    bundle_id: str
    input_repository_commit: str
    bundle_manifest_sha256: str
    scb_snapshot_manifest_sha256: str
    code_commit: str
    interpretation_id: str


class InspectionSummary(_ReportModel):
    workbook_total_occurrences: int
    workbook_selected_occurrences: int
    workbook_dated_occurrences: int
    workbook_year_independent_occurrences: int
    scb_total_occurrences: int
    scb_retained_occurrences: int
    comparison_outcomes: int
    source_target_count: int
    outcome_counts: dict[str, int]
    interpretation_issue_counts: dict[str, int]


class SourceInspectionReport(_ReportModel):
    format: Literal["reg-meta-build-source-record-inspection"]
    schema_version: Literal[3]
    diagnostic_only: Literal[True]
    preview_level: Literal["source_target_only"]
    complete: bool
    scope: InspectionScope
    pins: InspectionPins
    summary: InspectionSummary
    source_revisions: tuple[SourceRevision, ...]
    workbook_context: tuple[str, ...]
    source_records: tuple[SourceRecord, ...]
    comparison_outcomes: tuple[ComparisonOutcome, ...]
    interpretation_issues: tuple[InterpretationIssue, ...]
    assumptions: tuple[InspectionAssumption, ...]
    target_preview: SourceTargetPreview | None
    limitations: tuple[str, ...]

    @model_validator(mode="after")
    def _coherent_report(self) -> Self:
        expected_complete = not any(
            issue.incomplete for issue in self.interpretation_issues
        )
        if self.complete != expected_complete:
            raise ValueError("report completeness disagrees with interpretation issues")
        revision_ids = {revision.revision_id for revision in self.source_revisions}
        record_ids = [record.record_id for record in self.source_records]
        if len(record_ids) != len(set(record_ids)):
            raise ValueError("source record IDs must be unique")
        if any(
            record.source_revision_id not in revision_ids
            for record in self.source_records
        ):
            raise ValueError("source record references an unreported revision")
        reported_assumptions = {
            assumption.assumption_id for assumption in self.assumptions
        }
        referenced_assumptions = {
            assumption_id
            for outcome in self.comparison_outcomes
            for assumption_id in outcome.assumption_ids
        }
        if self.target_preview is not None:
            referenced_assumptions.update(self.target_preview.assumption_ids)
        if not referenced_assumptions <= reported_assumptions:
            raise ValueError("comparison outcome references an unreported assumption")
        return self


class SourceCaseInspectionPins(_ReportModel):
    bundle_id: str
    input_repository_commit: str
    bundle_manifest_sha256: str
    scb_snapshot_manifest_sha256: str
    code_commit: str
    interpretation_id: Literal["hamn-signal-source-case-v1"]
    case_artifact_sha256: str


class SourceCaseInspectionReport(_ReportModel):
    format: Literal["reg-meta-build-source-case-inspection"]
    schema_version: Literal[1]
    diagnostic_only: Literal[True]
    preview_level: Literal["source_target_only"]
    complete: bool
    case_artifact: SourceCaseArtifact
    pins: SourceCaseInspectionPins
    evaluation: HamnSourceCaseEvaluation
    limitations: tuple[str, ...]

    @model_validator(mode="after")
    def _coherent(self) -> Self:
        if self.complete != self.evaluation.applicable:
            raise ValueError("report completeness must agree with case applicability")
        if self.preview_level != self.evaluation.preview_level:
            raise ValueError("report and evaluation preview levels disagree")
        return self


@dataclass(frozen=True)
class _FiniteComparisonAssumption:
    assumption_id: str
    workbook_table: str
    workbook_population: str | None
    scb_variant_id: int
    first_year: int
    last_year: int
    detail: str

    def allows(self, year: int) -> bool:
        return self.first_year <= year <= self.last_year


@dataclass(frozen=True)
class _FiniteWorkbookScope:
    assumption: _FiniteComparisonAssumption
    documented_years: frozenset[int]


_ENTERPRISE_POPULATION = (
    "Populationen i LISA:s företagstabell är företag med minst 1 sysselsatt "
    "enligt RAMS/BAS.\n"
    "1990-2001: 16 år och äldre, 2002-2011: 16-84 år, 2012-: 16-74 år"
)
_FINITE_COMPARISON_ASSUMPTIONS = (
    _FiniteComparisonAssumption(
        assumption_id="lisa-individual-1990-2009-to-scb-variant-1335",
        workbook_table="individual",
        workbook_population=None,
        scb_variant_id=1335,
        first_year=1990,
        last_year=2009,
        detail=(
            "Compare the workbook individual table, whose population coordinate is "
            "unspecified, only with native SCB variant 1335 for 1990-2009."
        ),
    ),
    _FiniteComparisonAssumption(
        assumption_id="lisa-individual-2010-2024-to-scb-variant-153",
        workbook_table="individual",
        workbook_population=None,
        scb_variant_id=153,
        first_year=2010,
        last_year=2024,
        detail=(
            "Compare the workbook individual table, whose population coordinate is "
            "unspecified, only with native SCB variant 153 for 2010-2024."
        ),
    ),
    _FiniteComparisonAssumption(
        assumption_id="lisa-company-1990-2024-to-scb-variant-152",
        workbook_table="company",
        workbook_population=_ENTERPRISE_POPULATION,
        scb_variant_id=152,
        first_year=1990,
        last_year=2024,
        detail=(
            "Compare the workbook company table with its declared enterprise "
            "population only with native SCB variant 152 for 1990-2024."
        ),
    ),
    _FiniteComparisonAssumption(
        assumption_id="lisa-workplace-1990-2024-to-scb-variant-151",
        workbook_table="workplace",
        workbook_population=_ENTERPRISE_POPULATION,
        scb_variant_id=151,
        first_year=1990,
        last_year=2024,
        detail=(
            "Compare the workbook workplace table with its declared enterprise "
            "population only with native SCB variant 151 for 1990-2024."
        ),
    ),
)
_UNESTABLISHED_SCOPE_ASSUMPTION_ID = "unestablished-workbook-to-scb-variant-scope"
_SPELLING_CONTINUITY_ASSUMPTION_ID = "same-native-variable-spelling-continuity"
_CASEFOLD_CANDIDATE_ASSUMPTION_ID = "casefold-spelling-candidate"
_ASSUMPTION_REPORTS = (
    *(
        InspectionAssumption(
            assumption_id=assumption.assumption_id,
            status="unresolved",
            purpose="bound a workbook availability comparison to one native variant",
            detail=(
                f"{assumption.detail} This is a diagnostic comparison assumption, "
                "not an accepted population or identity mapping."
            ),
        )
        for assumption in _FINITE_COMPARISON_ASSUMPTIONS
    ),
    InspectionAssumption(
        assumption_id=_SPELLING_CONTINUITY_ASSUMPTION_ID,
        status="unresolved",
        purpose="surface present SCB records whose column_name field is unknown",
        detail=(
            "An exact spelling observed on one SCB VarId is used only to find blank "
            "column-name fields on that VarId within the same finite native-variant "
            "scope; it is not an identity merge or executable correction selector."
        ),
    ),
    InspectionAssumption(
        assumption_id=_CASEFOLD_CANDIDATE_ASSUMPTION_ID,
        status="unresolved",
        purpose="surface spelling candidates without merging them",
        detail=(
            "Case-insensitive spelling is used only to retain candidates in the "
            "reported finite native-variant scope; exact source spellings and native "
            "identities remain distinct."
        ),
    ),
    InspectionAssumption(
        assumption_id=_UNESTABLISHED_SCOPE_ASSUMPTION_ID,
        status="unresolved",
        purpose="mark source witnesses outside a supported finite comparison scope",
        detail=(
            "No supported workbook table/population, native SCB variant, and finite "
            "edition relationship applies; witnesses remain source observations and "
            "cannot establish counterpart availability or edition presence."
        ),
    ),
)


def _finite_comparison_assumption(
    workbook: SourceRecord, year: int
) -> _FiniteComparisonAssumption | None:
    table = workbook.subject.variant.name
    population = workbook.subject.population
    for assumption in _FINITE_COMPARISON_ASSUMPTIONS:
        if table != assumption.workbook_table or not assumption.allows(year):
            continue
        if assumption.workbook_population is None:
            if population.status == "unknown":
                return assumption
        elif (
            population.status == "value"
            and population.name == assumption.workbook_population
        ):
            return assumption
    return None


def _finite_workbook_scopes(
    workbook_records: Iterable[SourceRecord],
) -> tuple[_FiniteWorkbookScope, ...]:
    """Group the explicitly documented years by their finite comparison scope."""
    years_by_assumption: dict[str, set[int]] = defaultdict(set)
    for record in workbook_records:
        for year in _scope_years(record.edition_scope) or ():
            if assumption := _finite_comparison_assumption(record, year):
                years_by_assumption[assumption.assumption_id].add(year)
    return tuple(
        _FiniteWorkbookScope(
            assumption=assumption,
            documented_years=frozenset(years_by_assumption[assumption.assumption_id]),
        )
        for assumption in _FINITE_COMPARISON_ASSUMPTIONS
        if assumption.assumption_id in years_by_assumption
    )


def _native_variable_key(record: SourceRecord) -> tuple[int, int] | None:
    native = record.subject.native
    if native.register_variant_id is None or native.variable_id is None:
        return None
    return native.register_variant_id, native.variable_id


def _native_edition_key(record: SourceRecord) -> tuple[int, int, int] | None:
    native = record.subject.native
    if (
        native.register_variant_id is None
        or native.variable_id is None
        or native.edition_id is None
    ):
        return None
    return native.register_variant_id, native.variable_id, native.edition_id


def _single_member_native_editions(
    records: Iterable[SourceRecord],
) -> frozenset[tuple[int, int, int]]:
    """Return native editions containing exactly one distinct source member."""
    members_by_edition: dict[tuple[int, int, int], set[int]] = defaultdict(set)
    for record in records:
        edition_key = _native_edition_key(record)
        member_id = record.subject.native.member_id
        if edition_key is not None and member_id is not None:
            members_by_edition[edition_key].add(member_id)
    return frozenset(
        edition_key
        for edition_key, member_ids in members_by_edition.items()
        if len(member_ids) == 1
    )


def _annual_year(record: SourceRecord) -> int | None:
    years = _scope_years(record.edition_scope)
    if years is None or len(years) != 1:
        return None
    return years[0]


def _finite_scope_ids_for_record(
    record: SourceRecord, scopes: Iterable[_FiniteWorkbookScope]
) -> tuple[str, ...]:
    """Return scopes in which an annual native record is a finite anchor."""
    year = _annual_year(record)
    native = record.subject.native
    if year is None or native.register_variant_id is None:
        return ()
    return tuple(
        scope.assumption.assumption_id
        for scope in scopes
        if scope.assumption.scb_variant_id == native.register_variant_id
        and scope.assumption.allows(year)
    )


def _finite_name_native_anchors(
    exact_column: str,
    records: Iterable[SourceRecord],
    scopes: Iterable[_FiniteWorkbookScope],
) -> tuple[SourceRecord, ...]:
    """Select exact-name/native anchors inside an active finite assumption."""
    return tuple(
        record
        for record in _finite_named_native_records(records, scopes)
        if _column(record) == exact_column
    )


def _finite_named_native_records(
    records: Iterable[SourceRecord],
    scopes: Iterable[_FiniteWorkbookScope],
) -> tuple[SourceRecord, ...]:
    """Select all finite named witnesses on native variables in active scopes."""
    finite_scopes = tuple(scopes)
    return tuple(
        record
        for record in records
        if _column(record) is not None
        and _native_variable_key(record) is not None
        and _finite_scope_ids_for_record(record, finite_scopes)
    )


def _annual_targets_within_workbook_scope(
    exact_column: str,
    records: Iterable[SourceRecord],
    anchors: Iterable[SourceRecord],
    scopes: Iterable[_FiniteWorkbookScope],
) -> tuple[SourceRecord, ...]:
    """Select blank-name annual records only inside their workbook declaration."""
    finite_scopes = tuple(scopes)
    native_records = tuple(records)
    single_member_editions = _single_member_native_editions(native_records)
    scope_by_id = {scope.assumption.assumption_id: scope for scope in finite_scopes}
    anchor_scope_ids: dict[tuple[int, int], set[str]] = defaultdict(set)
    for anchor in anchors:
        if (native_key := _native_variable_key(anchor)) is None:
            continue
        anchor_scope_ids[native_key].update(
            _finite_scope_ids_for_record(anchor, finite_scopes)
        )
    finite_spellings: dict[tuple[int, int], set[str]] = defaultdict(set)
    for named in _finite_named_native_records(native_records, finite_scopes):
        native_key = _native_variable_key(named)
        column = _column(named)
        assert native_key is not None
        assert column is not None
        finite_spellings[native_key].add(column)

    targets: list[SourceRecord] = []
    for record in native_records:
        column = record.fields.column_name
        year = _annual_year(record)
        native_key = _native_variable_key(record)
        if (
            column is None
            or column.status != "unknown"
            or year is None
            or native_key is None
        ):
            continue
        connected_scope_ids = anchor_scope_ids.get(native_key, set())
        if (
            _native_edition_key(record) in single_member_editions
            and finite_spellings.get(native_key) == {exact_column}
            and any(
                year in scope_by_id[assumption_id].documented_years
                for assumption_id in connected_scope_ids
            )
        ):
            targets.append(record)
    return tuple(targets)


def source_interpreter_commit() -> str:
    """Pin the clean tracked implementation that gives source records meaning."""
    here = Path(__file__).resolve()
    package = here.parent
    repo = package.parents[2]
    dependency_paths: list[Path] = []
    for module in (reg_meta_fqid, reg_meta_queries, curation_module):
        module_path = getattr(module, "__file__", None)
        if module_path is None:
            raise SnapshotError(
                "source record interpreter dependency has no loaded source path: "
                f"{module.__name__}"
            )
        dependency_paths.append(Path(module_path))
    _repository, commit = _tracked_source_commit(
        (
            here,
            package / "source_records.py",
            package / "source_cases.py",
            package / "input_snapshot.py",
            package / "cli.py",
            package / "db.py",
            package / "edition_bounds.py",
            package / "sources" / "lisa.py",
            package / "sources" / "scb.py",
            package / "sources" / "scb_records.py",
            *dependency_paths,
            repo / "uv.lock",
        ),
        identity="source record interpreter",
    )
    return commit


def _scope_years(scope: TemporalScope) -> tuple[int, ...] | None:
    if scope.kind != "intervals":
        return None
    years: list[int] = []
    for interval in scope.intervals:
        if (
            len(interval.start) != 4
            or len(interval.end) != 4
            or not interval.start.isdigit()
            or not interval.end.isdigit()
        ):
            return None
        years.extend(range(int(interval.start), int(interval.end) + 1))
    return tuple(years)


def _scope_from_years(years: Iterable[int]) -> TemporalScope:
    ordered = sorted(set(years))
    if not ordered:
        raise ValueError("cannot form a temporal scope from no years")
    intervals: list[ScopeInterval] = []
    start = previous = ordered[0]
    for year in ordered[1:]:
        if year != previous + 1:
            intervals.append(ScopeInterval(start=str(start), end=str(previous)))
            start = year
        previous = year
    intervals.append(ScopeInterval(start=str(start), end=str(previous)))
    return TemporalScope(kind="intervals", intervals=tuple(intervals))


def _column(record: SourceRecord) -> str | None:
    field = record.fields.column_name
    return (
        field.value
        if field is not None
        and field.status == "value"
        and isinstance(field.value, str)
        else None
    )


def _availability(record: SourceRecord) -> SourceField | None:
    return record.fields.availability


def _native_identity(record: SourceRecord) -> tuple[int | None, ...]:
    native = record.subject.native
    return (
        native.register_id,
        native.register_variant_id,
        native.edition_id,
        native.variable_id,
        native.member_id,
    )


def _native_ids(records: Iterable[SourceRecord], attribute: str) -> tuple[int, ...]:
    return tuple(
        sorted(
            {
                value
                for record in records
                if (value := getattr(record.subject.native, attribute)) is not None
            }
        )
    )


def _outcome(
    *,
    workbook: SourceRecord,
    status: OutcomeStatus,
    years: Iterable[int] | None,
    scb_records: Iterable[SourceRecord],
    scb_edition_present: bool | None,
    assumption_ids: tuple[str, ...],
    detail: str,
) -> ComparisonOutcome:
    witnesses = tuple(sorted(scb_records, key=lambda item: item.record_id))
    scope = workbook.edition_scope if years is None else _scope_from_years(years)
    return ComparisonOutcome(
        comparison_key=(
            workbook.record_id,
            status,
            ",".join(f"{interval.start}:{interval.end}" for interval in scope.intervals)
            or scope.kind,
            *assumption_ids,
        ),
        status=status,
        column_name=_column(workbook) or "",
        edition_scope=scope,
        workbook_record_ids=(workbook.record_id,),
        scb_record_ids=tuple(item.record_id for item in witnesses),
        assumption_ids=assumption_ids,
        scb_edition_present=scb_edition_present,
        register_variant_ids=_native_ids(witnesses, "register_variant_id"),
        variable_ids=_native_ids(witnesses, "variable_id"),
        member_ids=_native_ids(witnesses, "member_id"),
        detail=detail,
    )


def _source_outcome(
    *,
    column_name: str,
    status: Literal[
        "ambiguous_match", "source_only_observation", "unknown_applicability"
    ],
    workbook_records: Iterable[SourceRecord],
    scb_record: SourceRecord,
    assumption_ids: tuple[str, ...],
    detail: str,
) -> ComparisonOutcome:
    workbook = tuple(sorted(workbook_records, key=lambda item: item.record_id))
    scope = scb_record.edition_scope
    return ComparisonOutcome(
        comparison_key=(
            "source",
            column_name,
            status,
            scb_record.record_id,
        ),
        status=status,
        column_name=column_name,
        edition_scope=scope,
        workbook_record_ids=tuple(item.record_id for item in workbook),
        scb_record_ids=(scb_record.record_id,),
        assumption_ids=assumption_ids,
        scb_edition_present=True if _scope_years(scope) is not None else None,
        register_variant_ids=_native_ids((scb_record,), "register_variant_id"),
        variable_ids=_native_ids((scb_record,), "variable_id"),
        member_ids=_native_ids((scb_record,), "member_id"),
        detail=detail,
    )


def compare_availability_records(
    workbook_records: Iterable[SourceRecord],
    scb_records: Iterable[SourceRecord],
    *,
    exact_column: str | None = None,
) -> tuple[ComparisonOutcome, ...]:
    """Derive compact scoped availability views while retaining both witnesses."""
    workbook = tuple(workbook_records)
    scb = tuple(scb_records)
    single_member_editions = _single_member_native_editions(scb)
    workbook_by_column: dict[str, list[SourceRecord]] = defaultdict(list)
    for record in workbook:
        if (column := _column(record)) is not None:
            workbook_by_column[column].append(record)
    finite_scopes_by_column = {
        column: _finite_workbook_scopes(records)
        for column, records in workbook_by_column.items()
    }
    exact: dict[tuple[int, str, int], list[SourceRecord]] = defaultdict(list)
    folded: dict[tuple[int, str, int], list[SourceRecord]] = defaultdict(list)
    exact_any: dict[tuple[str, int], list[SourceRecord]] = defaultdict(list)
    folded_any: dict[tuple[str, int], list[SourceRecord]] = defaultdict(list)
    unknown_by_variable_year: dict[tuple[int, int, int], list[SourceRecord]] = (
        defaultdict(list)
    )
    all_exact: dict[str, list[SourceRecord]] = defaultdict(list)
    unscoped_exact: dict[str, list[SourceRecord]] = defaultdict(list)
    unscoped_folded: dict[str, list[SourceRecord]] = defaultdict(list)
    unscoped_unknown_by_variable: dict[tuple[int, int], list[SourceRecord]] = (
        defaultdict(list)
    )
    unknown_by_native_variable: dict[tuple[int, int], list[SourceRecord]] = defaultdict(
        list
    )
    records_by_native_variable: dict[tuple[int, int], list[SourceRecord]] = defaultdict(
        list
    )
    edition_years: set[tuple[int, int]] = set()
    variable_ids_by_assumption_column: dict[tuple[str, str], set[int]] = defaultdict(
        set
    )
    variable_ids_by_variant_column: dict[tuple[int, str], set[int]] = defaultdict(set)

    for record in scb:
        years = _scope_years(record.edition_scope)
        column = _column(record)
        variant_id = record.subject.native.register_variant_id
        variable_id = record.subject.native.variable_id
        if variant_id is not None and variable_id is not None:
            records_by_native_variable[variant_id, variable_id].append(record)
        if column is not None:
            all_exact[column].append(record)
            if variant_id is not None and variable_id is not None:
                variable_ids_by_variant_column[variant_id, column].add(variable_id)
        elif variant_id is not None and variable_id is not None:
            unknown_by_native_variable[variant_id, variable_id].append(record)
        if years is None:
            if column is not None:
                unscoped_exact[column].append(record)
                unscoped_folded[column.casefold()].append(record)
            elif variant_id is not None and variable_id is not None:
                unscoped_unknown_by_variable[variant_id, variable_id].append(record)
            continue
        for year in years:
            if variant_id is not None:
                edition_years.add((variant_id, year))
            if column is not None:
                exact_any[column, year].append(record)
                folded_any[column.casefold(), year].append(record)
                if variant_id is not None:
                    exact[variant_id, column, year].append(record)
                    folded[variant_id, column.casefold(), year].append(record)
            elif variant_id is not None and variable_id is not None:
                unknown_by_variable_year[variant_id, variable_id, year].append(record)

    finite_anchors_by_column: dict[str, tuple[SourceRecord, ...]] = {}
    finite_named_by_column: dict[str, tuple[SourceRecord, ...]] = {}
    finite_targets_by_column: dict[str, tuple[SourceRecord, ...]] = {}
    for column, documented_records in workbook_by_column.items():
        scopes = finite_scopes_by_column[column]
        anchors = _finite_name_native_anchors(column, all_exact.get(column, ()), scopes)
        finite_anchors_by_column[column] = anchors
        anchor_native_keys = {
            native_key
            for anchor in anchors
            if (native_key := _native_variable_key(anchor)) is not None
        }
        native_records = tuple(
            record
            for native_key in anchor_native_keys
            for record in records_by_native_variable.get(native_key, ())
        )
        finite_named_by_column[column] = _finite_named_native_records(
            native_records, scopes
        )
        finite_targets_by_column[column] = _annual_targets_within_workbook_scope(
            column,
            native_records,
            anchors,
            scopes,
        )
        for anchor in anchors:
            variable_id = anchor.subject.native.variable_id
            assert variable_id is not None
            for assumption_id in _finite_scope_ids_for_record(anchor, scopes):
                variable_ids_by_assumption_column[assumption_id, column].add(
                    variable_id
                )

    outcomes: list[ComparisonOutcome] = []
    for documented in workbook:
        column = _column(documented)
        if column is None:
            continue
        years = _scope_years(documented.edition_scope)
        if years is None:
            outcomes.append(
                _outcome(
                    workbook=documented,
                    status="unknown_applicability",
                    years=None,
                    scb_records=all_exact.get(column, ()),
                    scb_edition_present=None,
                    assumption_ids=(_UNESTABLISHED_SCOPE_ASSUMPTION_ID,),
                    detail=(
                        "the workbook declaration has no annual edition scope; exact "
                        "SCB spellings are retained without inferring variant applicability"
                    ),
                )
            )
            continue

        grouped: dict[
            tuple[OutcomeStatus, bool | None, str, tuple[str, ...]],
            tuple[list[int], dict[str, SourceRecord]],
        ] = {}
        for year in years:
            assumption = _finite_comparison_assumption(documented, year)
            candidates: tuple[SourceRecord, ...] = ()
            status: OutcomeStatus
            detail: str
            assumption_ids: tuple[str, ...]
            if assumption is None:
                casefold_candidates = tuple(
                    item
                    for item in folded_any.get((column.casefold(), year), ())
                    if _column(item) != column
                )
                unscoped_casefold_candidates = tuple(
                    item
                    for item in unscoped_folded.get(column.casefold(), ())
                    if _column(item) != column
                )
                candidate_map = {
                    record.record_id: record
                    for record in (
                        *exact_any.get((column, year), ()),
                        *casefold_candidates,
                        *unscoped_exact.get(column, ()),
                        *unscoped_casefold_candidates,
                    )
                }
                continuity = False
                for (
                    variant_id,
                    known_column,
                ), variable_ids in variable_ids_by_variant_column.items():
                    if known_column != column:
                        continue
                    for variable_id in variable_ids:
                        for item in (
                            *unknown_by_variable_year.get(
                                (variant_id, variable_id, year), ()
                            ),
                            *unscoped_unknown_by_variable.get(
                                (variant_id, variable_id), ()
                            ),
                        ):
                            candidate_map[item.record_id] = item
                            continuity = True
                candidates = tuple(candidate_map.values())
                assumption_ids = (_UNESTABLISHED_SCOPE_ASSUMPTION_ID,)
                if casefold_candidates or unscoped_casefold_candidates:
                    assumption_ids += (_CASEFOLD_CANDIDATE_ASSUMPTION_ID,)
                if continuity:
                    assumption_ids += (_SPELLING_CONTINUITY_ASSUMPTION_ID,)
                status = "unknown_applicability"
                present = None
                detail = (
                    "the workbook table, population, or finite edition has no "
                    "supported SCB variant comparison assumption; relevant spelling "
                    "and native-variable candidates are retained without matching"
                )
            else:
                variant_id = assumption.scb_variant_id
                assumption_ids = (assumption.assumption_id,)
                candidates = tuple(exact.get((variant_id, column, year), ()))
            if assumption is not None and candidates:
                present = True
                identities = {_native_identity(item) for item in candidates}
                source_states = {
                    field.status
                    for item in candidates
                    if (field := _availability(item)) is not None
                }
                documented_field = _availability(documented)
                documented_state = (
                    documented_field.status
                    if documented_field is not None
                    else "unknown"
                )
                if len(identities) > 1:
                    status = "ambiguous_match"
                    detail = (
                        "the exact spelling has multiple raw SCB identities; no "
                        "VarId, CVID, population, or variant was merged"
                    )
                elif not source_states or "unknown" in source_states:
                    status = "unknown_applicability"
                    detail = "an applicable availability field is missing or unknown"
                elif documented_state == "unknown":
                    status = "unknown_applicability"
                    detail = "the workbook availability field is missing or unknown"
                elif documented_state == "negative":
                    status = (
                        "agreement" if source_states == {"negative"} else "conflict"
                    )
                    detail = (
                        "both sources explicitly report nonavailability"
                        if status == "agreement"
                        else "workbook nonavailability conflicts with an SCB occurrence"
                    )
                elif "negative" in source_states:
                    status = "conflict"
                    detail = "workbook availability conflicts with explicit SCB nonavailability"
                else:
                    status = "agreement"
                    detail = (
                        "both sources positively observe the exact physical spelling "
                        "under the reported finite comparison assumption"
                    )
            elif assumption is not None:
                connected_variable_ids = variable_ids_by_assumption_column.get(
                    (assumption.assumption_id, column), set()
                )
                blank_candidates = tuple(
                    item
                    for variable_id in sorted(connected_variable_ids)
                    for item in unknown_by_variable_year.get(
                        (assumption.scb_variant_id, variable_id, year), ()
                    )
                )
                blank_native_keys = {
                    native_key
                    for item in blank_candidates
                    if (native_key := _native_variable_key(item)) is not None
                }
                competing_named = tuple(
                    item
                    for item in finite_named_by_column.get(column, ())
                    if _native_variable_key(item) in blank_native_keys
                    and _column(item) != column
                )
                folded_candidates = tuple(
                    item
                    for item in folded.get(
                        (assumption.scb_variant_id, column.casefold(), year), ()
                    )
                    if _column(item) != column
                )
                parallel_member_candidates = tuple(
                    item
                    for item in blank_candidates
                    if _native_edition_key(item) not in single_member_editions
                )
                if blank_candidates and competing_named:
                    candidates = (*blank_candidates, *competing_named)
                    status = "ambiguous_match"
                    present = True
                    assumption_ids += (_SPELLING_CONTINUITY_ASSUMPTION_ID,)
                    detail = (
                        "the blank-name occurrence shares a native VarId with "
                        "competing finite source spellings; no exact spelling is "
                        "selected"
                    )
                elif parallel_member_candidates:
                    candidates = blank_candidates
                    status = "ambiguous_match"
                    present = True
                    assumption_ids += (_SPELLING_CONTINUITY_ASSUMPTION_ID,)
                    detail = (
                        "the blank-name occurrences include a native edition with "
                        "multiple distinct CVIDs; member continuity is not inferred"
                    )
                elif blank_candidates:
                    candidates = blank_candidates
                    status = "unknown_spelling"
                    present = True
                    assumption_ids += (_SPELLING_CONTINUITY_ASSUMPTION_ID,)
                    detail = (
                        "an SCB occurrence with a native VarId observed under the exact "
                        "spelling in the same finite variant scope has an explicitly "
                        "unknown column name"
                    )
                elif folded_candidates:
                    candidates = folded_candidates
                    status = "ambiguous_match"
                    present = True
                    assumption_ids += (_CASEFOLD_CANDIDATE_ASSUMPTION_ID,)
                    detail = (
                        "only case-insensitive spelling candidates exist; original "
                        "spellings and identities remain distinct within the finite "
                        "variant scope"
                    )
                else:
                    unscoped_map = {
                        item.record_id: item
                        for item in unscoped_exact.get(column, ())
                        if item.subject.native.register_variant_id
                        == assumption.scb_variant_id
                    }
                    unscoped_folded_candidates = tuple(
                        item
                        for item in unscoped_folded.get(column.casefold(), ())
                        if item.subject.native.register_variant_id
                        == assumption.scb_variant_id
                        and _column(item) != column
                    )
                    for item in unscoped_folded_candidates:
                        unscoped_map[item.record_id] = item
                    unscoped_blank = tuple(
                        item
                        for variable_id in sorted(connected_variable_ids)
                        for item in unscoped_unknown_by_variable.get(
                            (assumption.scb_variant_id, variable_id), ()
                        )
                    )
                    for item in unscoped_blank:
                        unscoped_map[item.record_id] = item
                    if unscoped_map:
                        candidates = tuple(unscoped_map.values())
                        status = "unknown_applicability"
                        present = (
                            True
                            if (assumption.scb_variant_id, year) in edition_years
                            else None
                        )
                        assumption_ids += (_UNESTABLISHED_SCOPE_ASSUMPTION_ID,)
                        if unscoped_folded_candidates:
                            assumption_ids += (_CASEFOLD_CANDIDATE_ASSUMPTION_ID,)
                        if unscoped_blank:
                            assumption_ids += (_SPELLING_CONTINUITY_ASSUMPTION_ID,)
                        detail = (
                            "relevant SCB spelling or native-variable candidates in "
                            "the expected variant have pooled or unparseable periods; "
                            "annual applicability remains unknown"
                        )
                    elif (assumption.scb_variant_id, year) in edition_years:
                        status = "unobserved_counterpart"
                        present = True
                        detail = (
                            "the assumed native SCB variant edition exists but no "
                            "matching source occurrence was observed"
                        )
                    else:
                        status = "missing_edition"
                        present = False
                        detail = (
                            "the raw SCB input contains no annual edition for the "
                            "assumed native variant"
                        )
            key: tuple[OutcomeStatus, bool | None, str, tuple[str, ...]] = (
                status,
                present,
                detail,
                assumption_ids,
            )
            grouped.setdefault(key, ([], {}))[0].append(year)
            grouped[key][1].update((item.record_id, item) for item in candidates)

        for (
            status,
            present,
            detail,
            assumption_ids,
        ), (group_years, witnesses) in grouped.items():
            outcomes.append(
                _outcome(
                    workbook=documented,
                    status=status,
                    years=group_years,
                    scb_records=witnesses.values(),
                    scb_edition_present=present,
                    assumption_ids=assumption_ids,
                    detail=detail,
                )
            )

    selected_columns = (
        {exact_column}
        if exact_column is not None
        else set(workbook_by_column) | set(all_exact)
    )
    for column in sorted(selected_columns):
        documented_records = workbook_by_column.get(column, ())
        scopes = finite_scopes_by_column.get(column, ())
        anchors = finite_anchors_by_column.get(column, ())
        target_ids = {
            record.record_id for record in finite_targets_by_column.get(column, ())
        }
        anchor_scope_ids_by_native: dict[tuple[int, int], set[str]] = defaultdict(set)
        for anchor in anchors:
            if (native_key := _native_variable_key(anchor)) is not None:
                anchor_scope_ids_by_native[native_key].update(
                    _finite_scope_ids_for_record(anchor, scopes)
                )
        exact_native_keys = {
            (variant_id, variable_id)
            for (variant_id, indexed_column), variable_ids in (
                variable_ids_by_variant_column.items()
            )
            if indexed_column == column
            for variable_id in variable_ids
        }
        diagnostic_unknown = {
            record.record_id: record
            for native_key in exact_native_keys
            for record in unknown_by_native_variable.get(native_key, ())
            if record.record_id not in target_ids
        }
        for record in sorted(
            diagnostic_unknown.values(), key=lambda item: item.record_id
        ):
            native_key = _native_variable_key(record)
            assert native_key is not None
            annual = _annual_year(record)
            parallel_native_edition = (
                annual is not None
                and _native_edition_key(record) not in single_member_editions
            )
            outcomes.append(
                _source_outcome(
                    column_name=column,
                    status=(
                        "ambiguous_match"
                        if parallel_native_edition
                        else "unknown_applicability"
                    ),
                    workbook_records=documented_records,
                    scb_record=record,
                    assumption_ids=(
                        *sorted(anchor_scope_ids_by_native.get(native_key, ())),
                        _UNESTABLISHED_SCOPE_ASSUMPTION_ID,
                        _SPELLING_CONTINUITY_ASSUMPTION_ID,
                    ),
                    detail=(
                        "an SCB occurrence sharing a native VarId with an exact-name "
                        "occurrence has unknown column spelling and "
                        + (
                            "no annual edition scope"
                            if annual is None
                            else (
                                "belongs to a native edition with multiple distinct "
                                "CVIDs; member continuity remains unresolved"
                                if parallel_native_edition
                                else "is ineligible under the declared target assumptions"
                            )
                        )
                        + "; it remains a witness but is not a source target"
                    ),
                )
            )

        for record in sorted(
            all_exact.get(column, ()), key=lambda item: item.record_id
        ):
            availability = _availability(record)
            if (
                availability is None
                or availability.status != "value"
                or availability.value is not True
            ):
                continue
            years = _scope_years(record.edition_scope)
            variant_id = record.subject.native.register_variant_id
            if years is None:
                outcomes.append(
                    _source_outcome(
                        column_name=column,
                        status="unknown_applicability",
                        workbook_records=(),
                        scb_record=record,
                        assumption_ids=(_UNESTABLISHED_SCOPE_ASSUMPTION_ID,),
                        detail=(
                            "raw SCB positively observes this exact spelling under a "
                            "pooled or unparseable period; annual applicability remains "
                            "unknown"
                        ),
                    )
                )
                continue
            scoped_documented_years = {
                year
                for item in documented_records
                for year in (_scope_years(item.edition_scope) or ())
                if (
                    (assumption := _finite_comparison_assumption(item, year))
                    is not None
                    and assumption.scb_variant_id == variant_id
                )
            }
            if set(years).issubset(scoped_documented_years):
                continue
            outcomes.append(
                _source_outcome(
                    column_name=column,
                    status="source_only_observation",
                    workbook_records=(),
                    scb_record=record,
                    assumption_ids=(_UNESTABLISHED_SCOPE_ASSUMPTION_ID,),
                    detail=(
                        "raw SCB positively observes this exact spelling outside the "
                        "selected workbook declarations; no workbook availability or "
                        "variant applicability is inferred"
                    ),
                )
            )
    return tuple(sorted(outcomes, key=lambda item: item.comparison_key))


def _revision_for_bundle_dataset(
    dataset: SupplementalDataset, artifact: BundleFile
) -> SourceRevision:
    assert dataset.upstream_revision is not None
    assert artifact.size is not None
    assert artifact.sha256 is not None
    return SourceRevision.create(
        dataset=dataset.dataset,
        publisher=dataset.publisher,
        purpose=dataset.purpose,
        upstream_revision=dataset.upstream_revision,
        artifact_path=dataset.artifact_path,
        artifact_size=artifact.size,
        artifact_sha256=artifact.sha256,
    )


def _target_preview(
    exact_column: str,
    workbook: tuple[SourceRecord, ...],
    scb: tuple[SourceRecord, ...],
) -> SourceTargetPreview:
    documented_years = {
        year
        for record in workbook
        for year in (_scope_years(record.edition_scope) or ())
    }
    scopes = _finite_workbook_scopes(workbook)
    exact_named = tuple(record for record in scb if _column(record) == exact_column)
    finite_anchors = _finite_name_native_anchors(exact_column, exact_named, scopes)
    unknown_targets = _annual_targets_within_workbook_scope(
        exact_column, scb, finite_anchors, scopes
    )
    used_assumption_ids = {scope.assumption.assumption_id for scope in scopes}
    scoped_workbook_years = {
        year for scope in scopes for year in scope.documented_years
    }
    has_unestablished_scope = (
        not workbook
        or any(_scope_years(record.edition_scope) is None for record in workbook)
        or not documented_years.issubset(scoped_workbook_years)
    )
    casefold_collisions = tuple(
        record
        for record in scb
        if (column := _column(record)) is not None
        and column != exact_column
        and column.casefold() == exact_column.casefold()
    )
    scb_editions = {
        (variant_id, year)
        for record in scb
        if (variant_id := record.subject.native.register_variant_id) is not None
        for year in (_scope_years(record.edition_scope) or ())
    }
    missing_documented_editions = {
        year
        for documented in workbook
        for year in (_scope_years(documented.edition_scope) or ())
        if (assumption := _finite_comparison_assumption(documented, year)) is not None
        and (assumption.scb_variant_id, year) not in scb_editions
    }
    ordered_anchors = tuple(sorted(finite_anchors, key=lambda item: item.record_id))
    ordered_named = tuple(sorted(exact_named, key=lambda item: item.record_id))
    ordered_targets = tuple(sorted(unknown_targets, key=lambda item: item.record_id))
    ordered_collisions = tuple(
        sorted(casefold_collisions, key=lambda item: item.record_id)
    )
    if ordered_targets:
        used_assumption_ids.add(_SPELLING_CONTINUITY_ASSUMPTION_ID)
        if any(
            _scope_years(record.edition_scope) is None for record in ordered_targets
        ):
            has_unestablished_scope = True
    if ordered_collisions:
        used_assumption_ids.add(_CASEFOLD_CANDIDATE_ASSUMPTION_ID)
        if any(
            not _finite_scope_ids_for_record(record, scopes)
            for record in ordered_collisions
        ):
            has_unestablished_scope = True
    if any(record not in ordered_anchors for record in ordered_named):
        has_unestablished_scope = True
    if has_unestablished_scope:
        used_assumption_ids.add(_UNESTABLISHED_SCOPE_ASSUMPTION_ID)
    all_candidates = (*ordered_anchors, *ordered_targets)
    return SourceTargetPreview(
        preview_level="source_target_only",
        executable_selector=False,
        exact_column=exact_column,
        target_field="column_name",
        match_policy=(
            "finite_name_native_anchor_then_same_variable_annual_workbook_scope"
        ),
        candidate_variable_ids=_native_ids(ordered_anchors, "variable_id"),
        candidate_variant_ids=_native_ids(all_candidates, "register_variant_id"),
        documented_edition_scope=(
            _scope_from_years(documented_years) if documented_years else None
        ),
        expected_source_target_count=len(ordered_targets),
        target_record_ids=tuple(item.record_id for item in ordered_targets),
        existing_named_record_ids=tuple(item.record_id for item in ordered_named),
        casefold_collision_record_ids=tuple(
            item.record_id for item in ordered_collisions
        ),
        unobserved_documented_editions=tuple(sorted(missing_documented_editions)),
        assumption_ids=tuple(sorted(used_assumption_ids)),
    )


def inspect_bundle_source_records(
    bundle: CatalogBundleReader,
    *,
    code_commit: str,
    exact_column: str | None = None,
) -> SourceInspectionReport:
    """Inspect selected LISA and raw SCB records from one exact bundle."""
    dataset, artifact, workbook_path = bundle.require_supplemental_dataset(
        LISA_DATASET_ID
    )
    workbook_revision = _revision_for_bundle_dataset(dataset, artifact)
    snapshot_item = next(
        item
        for item in bundle.snapshot.manifest.files
        if item.name == "Registerinformation.csv"
    )
    assert snapshot_item.raw_size is not None
    assert snapshot_item.raw_sha256 is not None
    scb_revision = SourceRevision.create(
        dataset=SCB_DATASET_ID,
        publisher="SCB",
        purpose="Provider machine metadata and native LISA occurrence coordinates",
        upstream_revision=bundle.snapshot.manifest.edition,
        artifact_path=(
            f"{bundle.manifest.scb_snapshot_path}:source/Registerinformation.csv"
        ),
        artifact_size=snapshot_item.raw_size,
        artifact_sha256=snapshot_item.raw_sha256,
    )
    workbook_read = read_lisa_source(workbook_path, workbook_revision)
    workbook_all = workbook_read.records
    scb_by_id: dict[str, SourceRecord] = {}
    raw_issues = []
    for observation in iter_scb_observations(
        bundle.snapshot, scb_revision, register_id=LISA_REGISTER_ID
    ):
        record = observation.record
        if (existing := scb_by_id.get(record.record_id)) is None:
            scb_by_id[record.record_id] = record
        else:
            scb_by_id[record.record_id] = existing.with_additional_locators(
                record.locators
            )
        if observation.issue is not None:
            raw_issues.append(observation.issue)
    scb_all = tuple(scb_by_id.values())
    raw_issues = tuple(raw_issues)
    if exact_column is None:
        workbook = workbook_all
    else:
        workbook = tuple(
            record for record in workbook_all if _column(record) == exact_column
        )
        if not workbook and not any(
            _column(record) == exact_column for record in scb_all
        ):
            raise SnapshotError(
                "neither the selected LISA workbook nor raw SCB records have exact "
                f"column spelling {exact_column!r}"
            )
    outcomes = compare_availability_records(
        workbook, scb_all, exact_column=exact_column
    )
    preview = (
        _target_preview(exact_column, workbook, scb_all)
        if exact_column is not None
        else None
    )

    if exact_column is None:
        retained_scb = scb_all
    else:
        witness_ids = {
            record_id for outcome in outcomes for record_id in outcome.scb_record_ids
        }
        assert preview is not None
        witness_ids.update(preview.target_record_ids)
        witness_ids.update(preview.existing_named_record_ids)
        witness_ids.update(preview.casefold_collision_record_ids)
        retained_scb = tuple(
            record for record in scb_all if record.record_id in witness_ids
        )
    retained_ids = {record.record_id for record in retained_scb}
    issues = tuple(
        InterpretationIssue(
            kind=issue.kind,
            incomplete=issue.kind == "unparseable_period",
            physical_record=issue.physical_record,
            source_record_ids=(issue.record_id,),
            detail=issue.detail,
        )
        for issue in raw_issues
        if issue.record_id in retained_ids
    )
    source_records = tuple(
        sorted(
            (*workbook, *retained_scb),
            key=lambda item: (
                item.source,
                item.locators[0].semantic_record_key,
                item.locators[0].physical_record,
            ),
        )
    )
    outcome_counts = Counter(item.status for item in outcomes)
    issue_counts = Counter(item.kind for item in issues)
    used_assumption_ids = {
        assumption_id
        for outcome in outcomes
        for assumption_id in outcome.assumption_ids
    }
    if preview is not None:
        used_assumption_ids.update(preview.assumption_ids)
    assumptions = tuple(
        assumption
        for assumption in _ASSUMPTION_REPORTS
        if assumption.assumption_id in used_assumption_ids
    )
    return SourceInspectionReport(
        format="reg-meta-build-source-record-inspection",
        schema_version=3,
        diagnostic_only=True,
        preview_level="source_target_only",
        complete=not any(issue.incomplete for issue in issues),
        scope=InspectionScope(
            selected_sources=(dataset.dataset, SCB_DATASET_ID),
            excluded_inputs=(
                "catalog curation and corrections",
                "final catalog database",
                "SCB Vardemangder value streams",
                "LISA handbook/PDF documentation",
                "steward holdings",
            ),
            register_ids=(LISA_REGISTER_ID,),
            exact_column_filter=exact_column,
            filter_semantics="exact_source_spelling",
        ),
        pins=InspectionPins(
            bundle_id=bundle.manifest.bundle_id,
            input_repository_commit=bundle.provenance["input_repository_commit"],
            bundle_manifest_sha256=bundle.provenance["bundle_manifest_sha256"],
            scb_snapshot_manifest_sha256=bundle.manifest.scb_manifest_sha256,
            code_commit=code_commit,
            interpretation_id=INTERPRETATION_ID,
        ),
        summary=InspectionSummary(
            workbook_total_occurrences=len(workbook_all),
            workbook_selected_occurrences=len(workbook),
            workbook_dated_occurrences=sum(
                record.edition_scope.kind == "intervals" for record in workbook
            ),
            workbook_year_independent_occurrences=sum(
                record.edition_scope.kind == "year_independent" for record in workbook
            ),
            scb_total_occurrences=sum(len(record.locators) for record in scb_all),
            scb_retained_occurrences=sum(
                len(record.locators) for record in retained_scb
            ),
            comparison_outcomes=len(outcomes),
            source_target_count=(
                preview.expected_source_target_count if preview is not None else 0
            ),
            outcome_counts=dict(sorted(outcome_counts.items())),
            interpretation_issue_counts=dict(sorted(issue_counts.items())),
        ),
        source_revisions=(workbook_revision, scb_revision),
        workbook_context=workbook_read.worksheet_context,
        source_records=source_records,
        comparison_outcomes=outcomes,
        interpretation_issues=issues,
        assumptions=assumptions,
        target_preview=preview,
        limitations=(
            "Diagnostic source-target preview only; no curation or correction was applied.",
            "No catalog impact, publication, acceptance, whole-corpus validation, identity merge, or code-set equivalence is claimed.",
            "Only the reported finite workbook-to-SCB comparison assumptions are used; all other population and variant applicability remains unresolved.",
        ),
    )


def report_semantic_sha256(
    report: SourceInspectionReport | SourceCaseInspectionReport,
) -> str:
    """Stable identity of the deterministic report payload."""
    return canonical_sha256(report.model_dump(mode="json", exclude_none=True))


def inspect_hamn_signal_source_case(
    bundle: CatalogBundleReader, *, code_commit: str
) -> SourceCaseInspectionReport:
    """Replay the captured finite HAMN proposal against original observations."""
    relative = f"curation/{HAMN_SIGNAL_CASE_FILE}"
    item = next((item for item in bundle.manifest.files if item.path == relative), None)
    if item is None:
        raise SnapshotError(
            f"catalog bundle does not list source case {HAMN_SIGNAL_CASE_ID!r}"
        )
    if not item.present:
        raise SnapshotError(
            f"source case {HAMN_SIGNAL_CASE_ID!r} was not captured when this bundle "
            "was prepared"
        )
    artifact = load_hamn_signal_source_case(bundle.root / relative)
    if item.size != artifact.size or item.sha256 != artifact.sha256:
        raise SnapshotError(
            f"captured source case {HAMN_SIGNAL_CASE_ID!r} does not match its bundle "
            "inventory identity"
        )
    revision = _scb_revision(bundle)
    observations = tuple(
        iter_scb_observations(bundle.snapshot, revision, register_id=161)
    )
    evaluation = evaluate_hamn_signal_source_case(artifact, observations, revision)
    return SourceCaseInspectionReport(
        format="reg-meta-build-source-case-inspection",
        schema_version=1,
        diagnostic_only=True,
        preview_level="source_target_only",
        complete=evaluation.applicable,
        case_artifact=artifact,
        pins=SourceCaseInspectionPins(
            bundle_id=bundle.manifest.bundle_id,
            input_repository_commit=bundle.provenance["input_repository_commit"],
            bundle_manifest_sha256=bundle.provenance["bundle_manifest_sha256"],
            scb_snapshot_manifest_sha256=bundle.manifest.scb_manifest_sha256,
            code_commit=code_commit,
            interpretation_id=SOURCE_CASE_INTERPRETATION_ID,
            case_artifact_sha256=artifact.sha256,
        ),
        evaluation=evaluation,
        limitations=(
            "Source-target-only proposal; the default builder did not apply a correction.",
            "Production identity, raw coding absence, activation and final effects remain pending.",
            "Ten absent warm projection rows are not proof that raw coding is absent.",
        ),
    )


def _scb_revision(bundle: CatalogBundleReader) -> SourceRevision:
    item = next(
        item
        for item in bundle.snapshot.manifest.files
        if item.name == "Registerinformation.csv"
    )
    assert item.raw_size is not None and item.raw_sha256 is not None
    return SourceRevision.create(
        dataset=SCB_DATASET_ID,
        publisher="SCB",
        purpose="Lossless provider Registerinformation observation census",
        upstream_revision=bundle.snapshot.manifest.edition,
        artifact_path=f"{bundle.manifest.scb_snapshot_path}:source/Registerinformation.csv",
        artifact_size=item.raw_size,
        artifact_sha256=item.raw_sha256,
    )


class CensusMembership(_ReportModel):
    record_id: str
    locator: RecordLocator
    native: NativeCoordinates
    context_fingerprint: str
    original_edition_token: DeliveredCell


class CensusAlternative(_ReportModel):
    payload_fingerprint: str
    data_type: DeliveredCell
    data_length: DeliveredCell
    members: tuple[CensusMembership, ...]

    @model_validator(mode="after")
    def _has_members(self) -> Self:
        if not self.members:
            raise ValueError("a census alternative needs at least one member")
        return self


CensusGroupKind = Literal[
    "same_complete_context_alternatives",
    "context_separated_alternatives",
    "unproved_temporal_alternatives",
    "across_column_only_alternatives",
]


class CensusAlternativeGroup(_ReportModel):
    type: CensusGroupKind
    comparison_key: tuple[str, ...]
    columns: tuple[DeliveredCell, ...]
    alternatives: tuple[CensusAlternative, ...]
    temporal_applicability: Literal[
        "same_complete_context",
        "same_native_edition_context",
        "unproved_across_delivered_edition_contexts",
        "distinct_column_assertions_not_competition",
    ]

    @model_validator(mode="after")
    def _has_alternatives(self) -> Self:
        fingerprints = [item.payload_fingerprint for item in self.alternatives]
        if len(fingerprints) < 2 or len(fingerprints) != len(set(fingerprints)):
            raise ValueError("a census group needs at least two distinct alternatives")
        if not self.columns:
            raise ValueError("a census group needs its exact supplied columns")
        return self


class CensusScalarAlternative(_ReportModel):
    context_fingerprint: str
    members: tuple[CensusMembership, ...]

    @model_validator(mode="after")
    def _has_members(self) -> Self:
        if not self.members:
            raise ValueError("a scalar alternative needs at least one member")
        return self


class CensusScalarAlternativeGroup(_ReportModel):
    type: Literal["non_shape_scalar_disagreements"]
    comparison_key: tuple[str, ...]
    column: DeliveredCell
    alternatives: tuple[CensusScalarAlternative, ...]
    comparison_scope: Literal["same_native_edition_and_exact_column"]

    @model_validator(mode="after")
    def _has_alternatives(self) -> Self:
        fingerprints = [item.context_fingerprint for item in self.alternatives]
        if len(fingerprints) < 2 or len(fingerprints) != len(set(fingerprints)):
            raise ValueError("a scalar group needs at least two distinct contexts")
        return self


class CensusInterpretationIssue(_ReportModel):
    kind: Literal["pooled_period", "unparseable_period"]
    physical_record: str
    detail: str
    record_id: str


class CensusObservation(_ReportModel):
    type: Literal["observation"]
    record: SourceRecord
    interpretation_issue: CensusInterpretationIssue | None = None


class CensusPins(_ReportModel):
    bundle_id: str
    code_commit: str
    input_repository_commit: str
    bundle_manifest_sha256: str
    scb_snapshot_manifest_sha256: str
    source_revision_id: str
    interpretation_id: str


class CensusScope(_ReportModel):
    original: str
    interpreted: str


class CensusCounts(_ReportModel):
    rows: int
    unique_observations: int
    duplicate_occurrences: int
    cvids: int
    same_complete_context_groups: int
    same_complete_context_cvids: int
    context_separated_groups: int
    context_separated_cvids: int
    non_shape_scalar_groups: int
    non_shape_scalar_cvids: int
    unproved_temporal_groups: int
    unproved_temporal_cvids: int
    across_column_only_groups: int
    across_column_only_cvids: int
    interpretation_issues: dict[str, int]


class CensusAffectedMemberships(_ReportModel):
    same_complete_context_cvids: tuple[int, ...]
    context_separated_cvids: tuple[int, ...]
    non_shape_scalar_cvids: tuple[int, ...]
    unproved_temporal_cvids: tuple[int, ...]
    across_column_only_cvids: tuple[int, ...]


class CensusCompletion(_ReportModel):
    type: Literal["completion"]
    format: Literal["reg-meta-build-scb-observation-census"]
    schema_version: Literal[3]
    complete: Literal[True]
    pins: CensusPins
    source_revision: SourceRevision
    scope: CensusScope
    counts: CensusCounts
    affected_memberships: CensusAffectedMemberships
    registerinformation_ordered_records_sha256: str
    registerinformation_logical_sha256: str
    limitations: tuple[str, ...]

    @model_validator(mode="after")
    def _coherent_revision(self) -> Self:
        if self.pins.source_revision_id != self.source_revision.revision_id:
            raise ValueError("census source revision pin disagrees with its provenance")
        return self


type _CellPayload = tuple[bool, str | None, str]
type _Member = tuple[bytes, int]
type _Native = tuple[int, int, int, int, int]
type _EditionKey = tuple[int, int, int, int, int, bytes]
type _TemporalKey = tuple[int, int, int, bytes]
type _ColumnKey = tuple[int, bytes]


@dataclass(slots=True)
class _CompleteContext:
    native: _Native
    column_fingerprint: bytes
    edition_token_fingerprint: bytes
    first_shape: bytes
    first_members: list[_Member]
    alternatives: dict[bytes, list[_Member]] | None = None

    def add(self, shape: bytes, member: _Member) -> None:
        if self.alternatives is None and shape == self.first_shape:
            self.first_members.append(member)
            return
        if self.alternatives is None:
            self.alternatives = {self.first_shape: self.first_members}
        self.alternatives.setdefault(shape, []).append(member)

    def shapes(self) -> tuple[bytes, ...]:
        if self.alternatives is None:
            return (self.first_shape,)
        return tuple(self.alternatives)

    def members(self) -> tuple[tuple[bytes, list[_Member]], ...]:
        if self.alternatives is None:
            return ((self.first_shape, self.first_members),)
        return tuple(self.alternatives.items())


def _fingerprint(value: object) -> bytes:
    return bytes.fromhex(canonical_sha256(value))


def _cell_payload(cell: DeliveredCell) -> _CellPayload:
    return (cell.present, cell.raw_value, cell.interpreted_value)


def _note_distinct[Key](
    index: dict[Key, bytes | set[bytes]], key: Key, fingerprint: bytes
) -> None:
    known = index.get(key)
    if known is None:
        index[key] = fingerprint
    elif isinstance(known, bytes):
        if known != fingerprint:
            index[key] = {known, fingerprint}
    else:
        known.add(fingerprint)


def _multiple_values(value: bytes | set[bytes]) -> bool:
    return isinstance(value, set) and len(value) > 1


def write_scb_observation_census(
    bundle: CatalogBundleReader, destination: Path, *, code_commit: str
) -> dict[str, object]:
    """Write the complete SCB observation census directly as deterministic gzip JSONL."""
    destination = destination.resolve()
    if destination.exists():
        raise SnapshotError(
            f"census destination already exists and will not be overwritten: {destination}"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    revision = _scb_revision(bundle)
    snapshot_item = next(
        item
        for item in bundle.snapshot.manifest.files
        if item.name == "Registerinformation.csv"
    )
    assert snapshot_item.ordered_records_sha256 is not None
    assert snapshot_item.logical_sha256 is not None
    raw_header = [_decode_cell(cell) for cell in snapshot_item.header]
    header = tuple(value for value in raw_header if value is not None)
    if len(header) != len(raw_header):
        raise SnapshotError("Registerinformation.csv header contains a missing cell")

    contexts: dict[bytes, _CompleteContext] = {}
    shape_payloads: dict[bytes, tuple[DeliveredCell, DeliveredCell]] = {}
    column_payloads: dict[bytes, DeliveredCell] = {}
    edition_token_payloads: dict[bytes, DeliveredCell] = {}
    cvids: set[int] = set()
    row_count = 0
    issue_counts: Counter[str] = Counter()
    records_digest = hashlib.sha256()
    logical_digest = hashlib.sha256()
    logical_digest.update(b"header\0")
    _update_record_hash(logical_digest, raw_header)

    def encoded(value: object) -> bytes:
        return (
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode()

    with (
        destination.open("xb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as output,
    ):
        for observation in iter_scb_observations(bundle.snapshot, revision):
            record = observation.record
            if record.source_revision_id != revision.revision_id:
                raise SnapshotError(
                    "SCB observation references the wrong source revision"
                )
            native = record.subject.native
            if None in (
                native.register_id,
                native.register_variant_id,
                native.edition_id,
                native.variable_id,
                native.member_id,
            ):
                raise SnapshotError("SCB observation lost a required native coordinate")
            assert native.register_id is not None
            assert native.register_variant_id is not None
            assert native.edition_id is not None
            assert native.variable_id is not None
            assert native.member_id is not None
            native_key: _Native = (
                native.register_id,
                native.register_variant_id,
                native.edition_id,
                native.variable_id,
                native.member_id,
            )
            row_count += 1
            assert native.member_id is not None
            cvids.add(native.member_id)
            cells = {cell.name: cell for cell in record.delivered_cells}
            if tuple(cells) != header:
                raise SnapshotError(
                    "SCB observation fields do not match Registerinformation.csv header"
                )
            raw_row = [
                cell.raw_value if cell.present else None
                for cell in record.delivered_cells
            ]
            _update_record_hash(records_digest, raw_row)
            logical_digest.update(b"record\0")
            _update_record_hash(logical_digest, raw_row)

            data_type = cells["Datatyp"]
            data_length = cells["Datalängd"]
            shape = _fingerprint(
                [
                    data_type.model_dump(mode="json"),
                    data_length.model_dump(mode="json"),
                ]
            )
            shape_payloads.setdefault(shape, (data_type, data_length))
            context_fingerprint = _fingerprint(
                [
                    cell.model_dump(mode="json")
                    for cell in record.delivered_cells
                    if cell.name not in {"Datatyp", "Datalängd"}
                ]
            )
            column = cells["Kolumnnamn"]
            column_fingerprint = _fingerprint(column.model_dump(mode="json"))
            column_payloads.setdefault(column_fingerprint, column)
            edition_token = cells["Registerversionnamn"]
            edition_token_fingerprint = _fingerprint(
                edition_token.model_dump(mode="json")
            )
            edition_token_payloads.setdefault(edition_token_fingerprint, edition_token)
            physical_record = record.locators[0].physical_record
            if not physical_record.startswith("row:"):
                raise SnapshotError(
                    "SCB observation has an invalid physical row locator"
                )
            try:
                row_number = int(physical_record.removeprefix("row:"))
            except ValueError as exc:
                raise SnapshotError(
                    "SCB observation has an invalid physical row locator"
                ) from exc
            member = (
                bytes.fromhex(record.record_id.rpartition(":")[2]),
                row_number,
            )
            context = contexts.get(context_fingerprint)
            if context is None:
                contexts[context_fingerprint] = _CompleteContext(
                    native=native_key,
                    column_fingerprint=column_fingerprint,
                    edition_token_fingerprint=edition_token_fingerprint,
                    first_shape=shape,
                    first_members=[member],
                )
            else:
                if (
                    context.native != native_key
                    or context.column_fingerprint != column_fingerprint
                    or context.edition_token_fingerprint != edition_token_fingerprint
                ):
                    raise SnapshotError("complete-context fingerprint collision")
                context.add(shape, member)

            issue = None
            if observation.issue is not None:
                issue_counts[observation.issue.kind] += 1
                issue = CensusInterpretationIssue(
                    kind=observation.issue.kind,
                    physical_record=observation.issue.physical_record,
                    detail=observation.issue.detail,
                    record_id=observation.issue.record_id,
                )
            line = CensusObservation(
                type="observation", record=record, interpretation_issue=issue
            )
            output.write(encoded(line.model_dump(mode="json", exclude_none=True)))

        records_sha256 = records_digest.hexdigest()
        logical_sha256 = logical_digest.hexdigest()
        if row_count != snapshot_item.record_count:
            raise SnapshotError(
                "Registerinformation.csv census did not consume the complete snapshot"
            )
        if (
            records_sha256 != snapshot_item.ordered_records_sha256
            or logical_sha256 != snapshot_item.logical_sha256
        ):
            raise SnapshotError(
                "Registerinformation.csv census differs from the accepted lossless stream"
            )

        edition_contexts: dict[_EditionKey, bytes | set[bytes]] = {}
        edition_context_shape_sets: dict[_EditionKey, bytes | set[bytes]] = {}
        temporal_shapes: dict[_TemporalKey, bytes | set[bytes]] = {}
        temporal_editions: dict[_TemporalKey, bytes | set[bytes]] = {}
        column_shapes: dict[_ColumnKey, bytes | set[bytes]] = {}
        cvid_columns: dict[int, set[bytes]] = defaultdict(set)
        unique_observations = 0
        for context_fingerprint, context in contexts.items():
            native = context.native
            edition_key: _EditionKey = (*native, context.column_fingerprint)
            temporal_key: _TemporalKey = (
                native[0],
                native[1],
                native[3],
                context.column_fingerprint,
            )
            column_key = (native[4], context.column_fingerprint)
            cvid_columns[native[4]].add(context.column_fingerprint)
            _note_distinct(edition_contexts, edition_key, context_fingerprint)
            shapes = context.shapes()
            _note_distinct(
                edition_context_shape_sets,
                edition_key,
                _fingerprint([shape.hex() for shape in sorted(shapes)]),
            )
            unique_observations += len(shapes)
            _note_distinct(
                temporal_editions,
                temporal_key,
                str(native[2]).encode(),
            )
            for shape in shapes:
                _note_distinct(temporal_shapes, temporal_key, shape)
                _note_distinct(column_shapes, column_key, shape)

        context_separated_keys = {
            key
            for key, shape_sets in edition_context_shape_sets.items()
            if _multiple_values(shape_sets)
        }
        scalar_alternative_keys = {
            key
            for key, context_fingerprints in edition_contexts.items()
            if _multiple_values(context_fingerprints)
        }
        temporal_keys = {
            key
            for key, shapes in temporal_shapes.items()
            if _multiple_values(shapes) and _multiple_values(temporal_editions[key])
        }
        across_column_cvids = {
            cvid
            for cvid, columns in cvid_columns.items()
            if len(columns) > 1
            and all(
                not _multiple_values(column_shapes[(cvid, column)])
                for column in columns
            )
            and len(
                {
                    column_shapes[(cvid, column)]
                    for column in columns
                    if isinstance(column_shapes[(cvid, column)], bytes)
                }
            )
            > 1
        }

        type _Reference = tuple[bytes, _CompleteContext, _Member]
        context_separated: dict[_EditionKey, dict[bytes, list[_Reference]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        scalar_alternatives: dict[_EditionKey, dict[bytes, list[_Reference]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        temporal: dict[_TemporalKey, dict[bytes, list[_Reference]]] = defaultdict(
            lambda: defaultdict(list)
        )
        across_columns: dict[int, dict[bytes, list[_Reference]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for context_fingerprint, context in contexts.items():
            native = context.native
            edition_key = (*native, context.column_fingerprint)
            temporal_key = (
                native[0],
                native[1],
                native[3],
                context.column_fingerprint,
            )
            for shape, members in context.members():
                references = [
                    (context_fingerprint, context, member) for member in members
                ]
                if edition_key in context_separated_keys:
                    context_separated[edition_key][shape].extend(references)
                if edition_key in scalar_alternative_keys:
                    scalar_alternatives[edition_key][context_fingerprint].extend(
                        references
                    )
                if temporal_key in temporal_keys:
                    temporal[temporal_key][shape].extend(references)
                if native[4] in across_column_cvids:
                    across_columns[native[4]][shape].extend(references)

        def membership(reference: _Reference) -> CensusMembership:
            context_fingerprint, context, member = reference
            record_digest, row_number = member
            native = context.native
            semantic_key = (
                f"register:{native[0]}",
                f"variant:{native[1]}",
                f"edition:{native[2]}",
                f"variable:{native[3]}",
                f"member:{native[4]}",
            )
            return CensusMembership(
                record_id=(f"{SCB_DATASET_ID}:record:sha256:{record_digest.hex()}"),
                locator=RecordLocator(
                    semantic_record_key=semantic_key,
                    physical_file="Registerinformation.csv",
                    physical_table="Registerinformation.csv",
                    physical_record=f"row:{row_number}",
                    physical_cells=tuple(
                        f"Registerinformation.csv:row:{row_number}:{field}"
                        for field in header
                    ),
                ),
                native=NativeCoordinates(
                    register_id=native[0],
                    register_variant_id=native[1],
                    edition_id=native[2],
                    variable_id=native[3],
                    member_id=native[4],
                ),
                context_fingerprint=context_fingerprint.hex(),
                original_edition_token=edition_token_payloads[
                    context.edition_token_fingerprint
                ],
            )

        def memberships(references: list[_Reference]) -> tuple[CensusMembership, ...]:
            return tuple(
                membership(reference)
                for reference in sorted(references, key=lambda item: item[2][1])
            )

        def alternatives(
            grouped: dict[bytes, list[_Reference]],
        ) -> tuple[CensusAlternative, ...]:
            return tuple(
                CensusAlternative(
                    payload_fingerprint=shape.hex(),
                    data_type=shape_payloads[shape][0],
                    data_length=shape_payloads[shape][1],
                    members=memberships(references),
                )
                for shape, references in sorted(grouped.items())
            )

        same_context_groups = 0
        same_context_cvids: set[int] = set()
        for context_fingerprint, context in sorted(contexts.items()):
            if context.alternatives is None:
                continue
            same_context_groups += 1
            same_context_cvids.add(context.native[4])
            grouped = {
                shape: [(context_fingerprint, context, member) for member in members]
                for shape, members in context.members()
            }
            group = CensusAlternativeGroup(
                type="same_complete_context_alternatives",
                comparison_key=(
                    f"register:{context.native[0]}",
                    f"variant:{context.native[1]}",
                    f"edition:{context.native[2]}",
                    f"variable:{context.native[3]}",
                    f"member:{context.native[4]}",
                    f"complete-context:{context_fingerprint.hex()}",
                ),
                columns=(column_payloads[context.column_fingerprint],),
                alternatives=alternatives(grouped),
                temporal_applicability="same_complete_context",
            )
            output.write(encoded(group.model_dump(mode="json")))

        for key, grouped in sorted(context_separated.items()):
            group = CensusAlternativeGroup(
                type="context_separated_alternatives",
                comparison_key=(
                    f"register:{key[0]}",
                    f"variant:{key[1]}",
                    f"edition:{key[2]}",
                    f"variable:{key[3]}",
                    f"member:{key[4]}",
                    f"column:{key[5].hex()}",
                ),
                columns=(column_payloads[key[5]],),
                alternatives=alternatives(grouped),
                temporal_applicability="same_native_edition_context",
            )
            output.write(encoded(group.model_dump(mode="json")))

        for key, grouped in sorted(scalar_alternatives.items()):
            group = CensusScalarAlternativeGroup(
                type="non_shape_scalar_disagreements",
                comparison_key=(
                    f"register:{key[0]}",
                    f"variant:{key[1]}",
                    f"edition:{key[2]}",
                    f"variable:{key[3]}",
                    f"member:{key[4]}",
                    f"column:{key[5].hex()}",
                ),
                column=column_payloads[key[5]],
                alternatives=tuple(
                    CensusScalarAlternative(
                        context_fingerprint=context_fingerprint.hex(),
                        members=memberships(references),
                    )
                    for context_fingerprint, references in sorted(grouped.items())
                ),
                comparison_scope="same_native_edition_and_exact_column",
            )
            output.write(encoded(group.model_dump(mode="json")))

        for key, grouped in sorted(temporal.items()):
            group = CensusAlternativeGroup(
                type="unproved_temporal_alternatives",
                comparison_key=(
                    f"register:{key[0]}",
                    f"variant:{key[1]}",
                    f"variable:{key[2]}",
                    f"column:{key[3].hex()}",
                ),
                columns=(column_payloads[key[3]],),
                alternatives=alternatives(grouped),
                temporal_applicability=("unproved_across_delivered_edition_contexts"),
            )
            output.write(encoded(group.model_dump(mode="json")))

        for cvid, grouped in sorted(across_columns.items()):
            group = CensusAlternativeGroup(
                type="across_column_only_alternatives",
                comparison_key=(f"member:{cvid}",),
                columns=tuple(
                    column_payloads[column] for column in sorted(cvid_columns[cvid])
                ),
                alternatives=alternatives(grouped),
                temporal_applicability="distinct_column_assertions_not_competition",
            )
            output.write(encoded(group.model_dump(mode="json")))

        context_separated_cvids = {key[4] for key in context_separated_keys}
        scalar_alternative_cvids = {key[4] for key in scalar_alternative_keys}
        temporal_cvids = {
            reference[1].native[4]
            for grouped in temporal.values()
            for references in grouped.values()
            for reference in references
        }
        completion = CensusCompletion(
            type="completion",
            format="reg-meta-build-scb-observation-census",
            schema_version=3,
            complete=True,
            pins=CensusPins(
                bundle_id=bundle.manifest.bundle_id,
                code_commit=code_commit,
                input_repository_commit=bundle.provenance["input_repository_commit"],
                bundle_manifest_sha256=bundle.provenance["bundle_manifest_sha256"],
                scb_snapshot_manifest_sha256=bundle.manifest.scb_manifest_sha256,
                source_revision_id=revision.revision_id,
                interpretation_id=CENSUS_INTERPRETATION_ID,
            ),
            source_revision=revision,
            scope=CensusScope(
                original=(
                    "every supplied Registerinformation.csv row and all 36 raw "
                    "prepared cells, in source order"
                ),
                interpreted=(
                    "Datatyp/Datalängd alternatives under exact other-34-field, "
                    "same native-edition/column, cross-edition, and distinct-column "
                    "comparison scopes, plus non-shape scalar alternatives within "
                    "one native edition and exact column"
                ),
            ),
            counts=CensusCounts(
                rows=row_count,
                unique_observations=unique_observations,
                duplicate_occurrences=row_count - unique_observations,
                cvids=len(cvids),
                same_complete_context_groups=same_context_groups,
                same_complete_context_cvids=len(same_context_cvids),
                context_separated_groups=len(context_separated),
                context_separated_cvids=len(context_separated_cvids),
                non_shape_scalar_groups=len(scalar_alternatives),
                non_shape_scalar_cvids=len(scalar_alternative_cvids),
                unproved_temporal_groups=len(temporal),
                unproved_temporal_cvids=len(temporal_cvids),
                across_column_only_groups=len(across_columns),
                across_column_only_cvids=len(across_column_cvids),
                interpretation_issues=dict(sorted(issue_counts.items())),
            ),
            affected_memberships=CensusAffectedMemberships(
                same_complete_context_cvids=tuple(sorted(same_context_cvids)),
                context_separated_cvids=tuple(sorted(context_separated_cvids)),
                non_shape_scalar_cvids=tuple(sorted(scalar_alternative_cvids)),
                unproved_temporal_cvids=tuple(sorted(temporal_cvids)),
                across_column_only_cvids=tuple(sorted(across_column_cvids)),
            ),
            registerinformation_ordered_records_sha256=records_sha256,
            registerinformation_logical_sha256=logical_sha256,
            limitations=(
                "No physical-table identity is inferred from the 36-field export.",
                "Cross-edition and pooled-period alternatives do not prove competing annual deliveries.",
                "CVID-level coding evidence cannot establish per-column coding identity or raw absence from NULL.",
                "No reconciliation winner, automatic unknown, curation, or catalog change is selected.",
            ),
        )
        output.write(encoded(completion.model_dump(mode="json")))

    summary = completion.model_dump(mode="json")
    summary["evidence_size"] = destination.stat().st_size
    evidence_digest = hashlib.sha256()
    with destination.open("rb") as evidence:
        for chunk in iter(lambda: evidence.read(1 << 20), b""):
            evidence_digest.update(chunk)
    summary["evidence_sha256"] = evidence_digest.hexdigest()
    return summary


__all__ = [
    "CensusAlternativeGroup",
    "CensusCompletion",
    "CensusScalarAlternativeGroup",
    "ComparisonOutcome",
    "InspectionSummary",
    "InterpretationIssue",
    "SourceCaseInspectionReport",
    "SourceInspectionReport",
    "SourceTargetPreview",
    "compare_availability_records",
    "inspect_bundle_source_records",
    "inspect_hamn_signal_source_case",
    "report_semantic_sha256",
    "source_interpreter_commit",
    "write_scb_observation_census",
]
