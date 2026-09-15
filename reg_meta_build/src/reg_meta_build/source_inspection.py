"""Diagnostic source-target inspection before curation or catalog formation."""

from __future__ import annotations

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
    _tracked_source_commit,
)
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceField,
    SourceRecord,
    SourceRevision,
    TemporalScope,
    canonical_sha256,
)
from reg_meta_build.sources.lisa import read_lisa_source
from reg_meta_build.sources.scb_records import LISA_REGISTER_ID, read_scb_lisa_records

if TYPE_CHECKING:
    from collections.abc import Iterable

    from reg_meta_build.input_snapshot import (
        BundleFile,
        CatalogBundleReader,
        SupplementalDataset,
    )

INTERPRETATION_ID = "scb-lisa-source-record-inspection-v2"
SCB_DATASET_ID = "scb-registerinformation"
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
        "finite_variant_scope_then_exact_spelling_and_same_native_variable_discovery"
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
    schema_version: Literal[2]
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
    status: Literal["source_only_observation", "unknown_applicability"],
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
        if column is not None:
            all_exact[column].append(record)
            if variant_id is not None and variable_id is not None:
                variable_ids_by_variant_column[variant_id, column].add(variable_id)
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
        if column is not None and variant_id is not None and variable_id is not None:
            for assumption in _FINITE_COMPARISON_ASSUMPTIONS:
                if assumption.scb_variant_id == variant_id and any(
                    assumption.allows(year) for year in years
                ):
                    variable_ids_by_assumption_column[
                        assumption.assumption_id, column
                    ].add(variable_id)

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
                folded_candidates = tuple(
                    item
                    for item in folded.get(
                        (assumption.scb_variant_id, column.casefold(), year), ()
                    )
                    if _column(item) != column
                )
                if blank_candidates:
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
                        present = None
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
                    else:
                        cross_map = {
                            item.record_id: item
                            for item in (
                                *exact_any.get((column, year), ()),
                                *(
                                    record
                                    for record in folded_any.get(
                                        (column.casefold(), year), ()
                                    )
                                    if _column(record) != column
                                ),
                                *unscoped_exact.get(column, ()),
                                *(
                                    record
                                    for record in unscoped_folded.get(
                                        column.casefold(), ()
                                    )
                                    if _column(record) != column
                                ),
                            )
                            if item.subject.native.register_variant_id
                            != assumption.scb_variant_id
                        }
                        cross_folded = any(
                            _column(item) != column for item in cross_map.values()
                        )
                        cross_blank = False
                        for (
                            other_variant,
                            known_column,
                        ), variable_ids in variable_ids_by_variant_column.items():
                            if (
                                other_variant == assumption.scb_variant_id
                                or known_column != column
                            ):
                                continue
                            for variable_id in variable_ids:
                                for item in (
                                    *unknown_by_variable_year.get(
                                        (other_variant, variable_id, year), ()
                                    ),
                                    *unscoped_unknown_by_variable.get(
                                        (other_variant, variable_id), ()
                                    ),
                                ):
                                    cross_map[item.record_id] = item
                                    cross_blank = True
                        if cross_map:
                            candidates = tuple(cross_map.values())
                            status = "unknown_applicability"
                            present = None
                            assumption_ids += (_UNESTABLISHED_SCOPE_ASSUMPTION_ID,)
                            if cross_folded:
                                assumption_ids += (_CASEFOLD_CANDIDATE_ASSUMPTION_ID,)
                            if cross_blank:
                                assumption_ids += (_SPELLING_CONTINUITY_ASSUMPTION_ID,)
                            detail = (
                                "relevant spelling or native-variable candidates exist "
                                "only outside the finite SCB variant assumption; they "
                                "are retained without establishing a counterpart"
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
            key = (status, present, detail, assumption_ids)
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

    workbook_by_column: dict[str, list[SourceRecord]] = defaultdict(list)
    for record in workbook:
        if (column := _column(record)) is None:
            continue
        workbook_by_column[column].append(record)
    selected_columns = (
        {exact_column}
        if exact_column is not None
        else set(workbook_by_column) | set(all_exact)
    )
    for column in sorted(selected_columns):
        unscoped_unknown = {
            record.record_id: record
            for (variant_id, known_column), variable_ids in (
                variable_ids_by_variant_column.items()
            )
            if known_column == column
            for variable_id in variable_ids
            for record in unscoped_unknown_by_variable.get(
                (variant_id, variable_id), ()
            )
        }
        for record in sorted(
            unscoped_unknown.values(), key=lambda item: item.record_id
        ):
            outcomes.append(
                _source_outcome(
                    column_name=column,
                    status="unknown_applicability",
                    workbook_records=workbook_by_column.get(column, ()),
                    scb_record=record,
                    assumption_ids=(
                        _UNESTABLISHED_SCOPE_ASSUMPTION_ID,
                        _SPELLING_CONTINUITY_ASSUMPTION_ID,
                    ),
                    detail=(
                        "an SCB occurrence sharing a native VarId with this exact "
                        "spelling has unknown column spelling and no annual edition "
                        "scope; spelling continuity and applicability remain unresolved"
                    ),
                )
            )

        documented_records = workbook_by_column.get(column, ())
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
    exact_named = tuple(record for record in scb if _column(record) == exact_column)
    scoped_named: list[SourceRecord] = []
    assumptions_by_native_variable: dict[tuple[int, int], set[str]] = defaultdict(set)
    used_assumption_ids: set[str] = set()
    has_unestablished_scope = not workbook

    def matching_assumptions(
        record: SourceRecord,
    ) -> tuple[_FiniteComparisonAssumption, ...]:
        years = _scope_years(record.edition_scope)
        variant_id = record.subject.native.register_variant_id
        if years is None or variant_id is None:
            return ()
        matched: dict[str, _FiniteComparisonAssumption] = {}
        for year in years:
            year_matched = False
            for documented in workbook:
                if year not in (_scope_years(documented.edition_scope) or ()):
                    continue
                assumption = _finite_comparison_assumption(documented, year)
                if assumption is not None and assumption.scb_variant_id == variant_id:
                    matched[assumption.assumption_id] = assumption
                    year_matched = True
            if not year_matched:
                return ()
        return tuple(matched.values())

    for documented in workbook:
        for year in _scope_years(documented.edition_scope) or ():
            if assumption := _finite_comparison_assumption(documented, year):
                used_assumption_ids.add(assumption.assumption_id)
            else:
                has_unestablished_scope = True
    for record in exact_named:
        assumptions = matching_assumptions(record)
        variant_id = record.subject.native.register_variant_id
        variable_id = record.subject.native.variable_id
        if not assumptions or variant_id is None or variable_id is None:
            has_unestablished_scope = True
            continue
        scoped_named.append(record)
        assumptions_by_native_variable[variant_id, variable_id].update(
            assumption.assumption_id for assumption in assumptions
        )

    unknown_targets = tuple(
        record
        for record in scb
        if record.fields.column_name is not None
        and record.fields.column_name.status == "unknown"
        and (variant_id := record.subject.native.register_variant_id) is not None
        and (variable_id := record.subject.native.variable_id) is not None
        and (
            connected_assumptions := assumptions_by_native_variable.get(
                (variant_id, variable_id), set()
            )
        )
        and (
            (years := _scope_years(record.edition_scope)) is None
            or all(
                any(
                    assumption.assumption_id in connected_assumptions
                    and assumption.allows(year)
                    for assumption in _FINITE_COMPARISON_ASSUMPTIONS
                )
                for year in years
            )
        )
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
    ordered_named = tuple(sorted(scoped_named, key=lambda item: item.record_id))
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
        if any(not matching_assumptions(record) for record in ordered_collisions):
            has_unestablished_scope = True
    if has_unestablished_scope:
        used_assumption_ids.add(_UNESTABLISHED_SCOPE_ASSUMPTION_ID)
    all_candidates = (*ordered_named, *ordered_targets)
    return SourceTargetPreview(
        preview_level="source_target_only",
        executable_selector=False,
        exact_column=exact_column,
        target_field="column_name",
        match_policy=(
            "finite_variant_scope_then_exact_spelling_and_same_native_variable_discovery"
        ),
        candidate_variable_ids=_native_ids(ordered_named, "variable_id"),
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
    scb_all, raw_issues = read_scb_lisa_records(bundle.snapshot, scb_revision)
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
                item.locator.semantic_record_key,
                item.locator.physical_record,
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
        schema_version=2,
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
            scb_total_occurrences=len(scb_all),
            scb_retained_occurrences=len(retained_scb),
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


def report_semantic_sha256(report: SourceInspectionReport) -> str:
    """Stable identity of the deterministic report payload."""
    return canonical_sha256(report.model_dump(mode="json", exclude_none=True))


__all__ = [
    "ComparisonOutcome",
    "InspectionSummary",
    "InterpretationIssue",
    "SourceInspectionReport",
    "SourceTargetPreview",
    "compare_availability_records",
    "inspect_bundle_source_records",
    "report_semantic_sha256",
    "source_interpreter_commit",
]
