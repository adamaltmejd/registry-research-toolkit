"""Ordinary catalog formation from one explicit source-native variable family.

Naming/topology and code-list bindings are supplied by the common resolver. This
path needs no handwritten whole-variable case. It never identifies variables by
column spelling, merges different source families, or chooses a conflicting fact.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING

from reg_meta_build._curation import fold_column
from reg_meta_build._resolved_common import remaining_windows
from reg_meta_build.catalog_dependencies import CoverageObligation
from reg_meta_build.resolved_catalog import (
    ResolvedState,
    ResolvedVariable,
    unresolved_variable_flags,
)
from reg_meta_build.source_curation import ResolutionDiagnostic, SourceRecordRef
from reg_meta_build.source_intervals import (
    OccurrenceResolution,
    SourceSegment,
    occurrence_bounds,
    reconcile_source_fields,
    resolve_occurrence_intervals,
)
from reg_meta_build.source_occurrences import EffectiveOccurrence, effective_occurrence
from reg_meta_build.source_representations import form_representations

if TYPE_CHECKING:
    from collections.abc import Mapping

    from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
    from reg_meta_build.source_coding import CodingResolution
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import CurationCase
    from reg_meta_build.source_records import (
        SourceField,
        SourceFields,
        SourceRecord,
    )


def _refs(
    records: tuple[SourceRecord | EffectiveOccurrence, ...],
) -> tuple[SourceRecordRef, ...]:
    keys = sorted(
        {
            (r.source, r.locators[0].semantic_record_key)
            for record in records
            for r in (
                record.evidence
                if isinstance(record, EffectiveOccurrence)
                else (record,)
            )
        }
    )
    return tuple(
        SourceRecordRef(source=source, semantic_record_key=key) for source, key in keys
    )


def _text(fields: SourceFields, name: str) -> str | None:
    value = getattr(fields, name)
    if value is not None and value.status == "value":
        assert isinstance(value.value, str)
        return value.value
    return None


def _fact_claim(field: SourceField | None) -> tuple[str, str | None] | None:
    """Carry the occurrence fact as tri-state: value, negative, or no claim."""
    if field is None or field.status == "unknown":
        return None
    if field.status == "negative":
        return ("negative", None)
    assert isinstance(field.value, str)
    return ("value", field.value)


@dataclass(frozen=True)
class VariableFormation:
    variable: ResolvedVariable | None
    diagnostics: tuple[ResolutionDiagnostic, ...]
    occurrences: tuple[SourceRecord | EffectiveOccurrence, ...]
    intervals: tuple[OccurrenceResolution, ...]
    coding: tuple[CodingResolution, ...]
    withheld_variant_states: dict[str, tuple[ResolutionDiagnostic, ...]] = field(
        default_factory=dict
    )
    withheld_representations: dict[
        tuple[str, str], tuple[ResolutionDiagnostic, ...]
    ] = field(default_factory=dict)
    # Supported delivery this family still claims, less every period an explicit
    # coding or representation outcome withholds.
    coverage: tuple[CoverageObligation, ...] = ()


def _coded_states(
    segment: SourceSegment,
    variant: ResolvedVariant,
    coding: CodingResolution,
    subject: str,
) -> tuple[list[ResolvedState], list[ResolutionDiagnostic], list[tuple[str, str]]]:
    """Cut one positive segment by coding; also report the periods coding withholds."""
    lower, upper = (
        date.fromisoformat(segment.valid_from).toordinal(),
        date.fromisoformat(segment.valid_to).toordinal(),
    )
    cuts = {lower, upper + 1}
    for code_segment in coding.segments:
        lo, hi = (
            date.fromisoformat(code_segment.valid_from).toordinal(),
            date.fromisoformat(code_segment.valid_to).toordinal(),
        )
        if lo <= upper and hi >= lower:
            cuts.update((max(lower, lo), min(upper, hi) + 1))
    states = []
    diagnostics = []
    withheld = []
    for lo, next_lo in pairwise(sorted(cuts)):
        start, end = (
            date.fromordinal(lo).isoformat(),
            date.fromordinal(next_lo - 1).isoformat(),
        )
        matches = [
            c for c in coding.segments if c.valid_from <= start and c.valid_to >= end
        ]
        if len(matches) > 1:
            raise ValueError("coding resolver returned overlapping coding segments")
        matched = matches[0] if matches else None
        if matched is not None and matched.state_disposition != "include":
            withheld.append((start, end))
            if matched.state_disposition == "omit":
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="curated_state_omission",
                        severity="warning",
                        subject=subject,
                        detail="An applicable checked decision omits this state: "
                        + "\n".join(matched.provenance),
                        refs=_refs(segment.occurrences),
                        fields=("coding",),
                        valid_from=start,
                        valid_to=end,
                        withheld_output=("state",),
                    )
                )
            continue
        if matched is None and coding.claims:
            diagnostics.append(
                ResolutionDiagnostic(
                    code="missing_coding_period",
                    severity="error",
                    subject=subject,
                    detail="Bound source code lists do not establish membership for this occurrence period.",
                    refs=_refs(segment.occurrences),
                    fields=("coding",),
                    valid_from=start,
                    valid_to=end,
                    withheld_output=("state.value_set",),
                )
            )
        states.append(
            ResolvedState(
                variant=variant,
                valid_from=start,
                valid_to=end,
                delivery_column_name=segment.delivery_column_name,
                data_type=_text(segment.fields, "data_type"),
                data_length=_text(segment.fields, "data_length"),
                operational_definition=_text(segment.fields, "operational_definition"),
                source_register_text=_text(segment.fields, "source_attribution"),
                pooled=segment.pooled,
                provenance="\n\n".join(
                    sorted(
                        {
                            correction.provenance
                            for occurrence in segment.effective_occurrences
                            for correction in occurrence.corrections
                        }
                        | set(matched.provenance if matched else ())
                    )
                )
                or None,
                value_set=matched.code_set if matched else None,
                value_set_version_label=matched.version_label if matched else "",
                classification=matched.classification if matched else None,
                conformance=matched.conformance if matched else None,
            )
        )
    return states, diagnostics, withheld


def _disjoint_representations(
    states: list[ResolvedState],
    records: tuple[EffectiveOccurrence, ...],
    subject: str,
    variants: Mapping[NativeKey, ResolvedVariant | None],
) -> tuple[
    list[ResolvedState],
    list[ResolutionDiagnostic],
    dict[tuple[str, str], tuple[ResolutionDiagnostic, ...]],
]:
    """Withhold only overlapping representations lacking a canonical choice.

    An identity assignment establishes the variable, not which parallel column
    represents it. Independently labelled codings retain their existing lanes.
    """
    groups: dict[tuple[str, str], list[ResolvedState]] = defaultdict(list)
    for state in states:
        groups[state.variant.slug, state.value_set_version_label].append(state)
    result = []
    diagnostics = []
    withheld: dict[tuple[str, str], list[ResolutionDiagnostic]] = defaultdict(list)
    for members in groups.values():
        ordered = sorted(members, key=lambda state: state.valid_from)
        if all(left.valid_to < right.valid_from for left, right in pairwise(ordered)):
            result.extend(ordered)
            continue
        changes: dict[int, list[tuple[int, bool]]] = defaultdict(list)
        for ordinal, state in enumerate(ordered):
            changes[date.fromisoformat(state.valid_from).toordinal()].append(
                (ordinal, True)
            )
            changes[date.fromisoformat(state.valid_to).toordinal() + 1].append(
                (ordinal, False)
            )
        active: set[int] = set()
        for start, next_start in pairwise(sorted(changes)):
            for ordinal, entering in changes[start]:
                if entering:
                    active.add(ordinal)
                else:
                    active.remove(ordinal)
            if not active:
                continue
            lower, upper = (
                date.fromordinal(start).isoformat(),
                date.fromordinal(next_start - 1).isoformat(),
            )
            if len(active) == 1:
                state = ordered[next(iter(active))]
                result.append(
                    state.model_copy(update={"valid_from": lower, "valid_to": upper})
                )
                continue
            columns = {ordered[index].delivery_column_name for index in active}
            if len(columns) == 1:
                raise ValueError("formation produced overlapping states for one column")
            problem = ResolutionDiagnostic(
                code="unresolved_column_representation",
                severity="error",
                subject=subject,
                detail="The checked identity has parallel columns without an unambiguous catalog representation: "
                + ", ".join(sorted(columns)),
                refs=_refs(
                    tuple(
                        record
                        for record in records
                        if _text(record.fields, "column_name") in columns
                        and record.variant_key is not None
                        and variants.get(record.variant_key) == ordered[0].variant
                        and any(
                            lo < next_start and hi >= start
                            for lo, hi in (occurrence_bounds(record) or ())
                        )
                    )
                ),
                fields=("column_name",),
                valid_from=lower,
                valid_to=upper,
                withheld_output=("state",),
            )
            diagnostics.append(problem)
            for column in sorted(columns):
                withheld[ordered[0].variant.slug, column].append(problem)
    return result, diagnostics, {key: tuple(causes) for key, causes in withheld.items()}


def _null_conflicting_facts(
    obligations: tuple[CoverageObligation, ...],
    conflicts: tuple[tuple[str, str, str, str, str], ...],
) -> tuple[CoverageObligation, ...]:
    """Keep the window but drop the exact fact an accepted representation disputes.

    Only data_type/data_length travel on the obligation; other conflicting facts
    need no claim change. Partial overlaps split the obligation so the safe slice
    stays fully checked, the same period rule waived delivery already uses.
    A nulled fact is simply None, which the boundary guard never compares.
    """
    type_windows: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    length_windows: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for variant, column, fact_field, start, end in conflicts:
        if fact_field == "data_type":
            type_windows[variant, column].append((start, end))
        elif fact_field == "data_length":
            length_windows[variant, column].append((start, end))
    if not type_windows and not length_windows:
        return obligations
    result: list[CoverageObligation] = []
    for claim in obligations:
        key = (claim.variant, claim.column)
        claimed_types = [
            window
            for window in type_windows.get(key, ())
            if window[0] <= claim.valid_to and window[1] >= claim.valid_from
        ]
        claimed_lengths = [
            window
            for window in length_windows.get(key, ())
            if window[0] <= claim.valid_to and window[1] >= claim.valid_from
        ]
        if not claimed_types and not claimed_lengths:
            result.append(claim)
            continue
        cuts = {
            date.fromisoformat(claim.valid_from).toordinal(),
            date.fromisoformat(claim.valid_to).toordinal() + 1,
        }
        for start, end in (*claimed_types, *claimed_lengths):
            lower = max(start, claim.valid_from)
            upper = min(end, claim.valid_to)
            cuts.add(date.fromisoformat(lower).toordinal())
            cuts.add(date.fromisoformat(upper).toordinal() + 1)
        ordered = sorted(cuts)
        for lo, hi in pairwise(ordered):
            piece_from = date.fromordinal(lo).isoformat()
            piece_to = date.fromordinal(hi - 1).isoformat()
            null_type = any(
                start <= piece_from and end >= piece_to for start, end in claimed_types
            )
            null_length = any(
                start <= piece_from and end >= piece_to
                for start, end in claimed_lengths
            )
            result.append(
                replace(
                    claim,
                    valid_from=piece_from,
                    valid_to=piece_to,
                    data_type_claim=None if null_type else claim.data_type_claim,
                    data_length_claim=None if null_length else claim.data_length_claim,
                )
            )
    return tuple(result)


def form_native_variable(
    records: tuple[SourceRecord | EffectiveOccurrence, ...],
    *,
    register: ResolvedRegister,
    variants: Mapping[NativeKey, ResolvedVariant | None],
    slug: str,
    provider_key: str,
    flags: SourceFields,
    coding: Mapping[NativeKey, CodingResolution],
    representations: tuple[CurationCase, ...] = (),
) -> VariableFormation:
    """Form one ordinary native identity; return unsafe aspects as explicit issues.

    Every known column needs an explicit coding resolution (empty claims mean
    the selected source supplied no code list). An omitted mapping is an incomplete
    implementation/contract, not a curation issue. Flags are already reconciled
    source facts. Unknown flags cannot be represented by the current DB contract.
    Multiple unassigned column names under one native variable require a checked
    identity decision, unless they fold to one column-identity key
    (case/diacritic twins share one physical column); the ordinary path does
    not guess whether remaining spellings are renames or different questions.
    Each physical input occurrence remains in the result.
    A formed variable also returns the delivery its supported occurrences still
    claim after the explicit coding/representation outcomes that withhold periods.
    """
    effective = tuple(effective_occurrence(record) for record in records)
    if any(record.use != "catalog" for record in effective):
        raise ValueError("support-only source records cannot form catalog variables")
    keys = {record.variable_key for record in effective}
    if not records or None in keys or len(keys) != 1:
        raise ValueError(
            "ordinary formation requires one complete native variable identity"
        )
    if any(record.provider != register.provider for record in effective):
        raise ValueError(
            "resolved register provider differs from native source identity"
        )
    subject = f"{register.provider}/{register.slug}/{slug}"
    diagnostics: list[ResolutionDiagnostic] = []

    def issue(
        code: str,
        detail: str,
        fields: tuple[str, ...],
        withheld: tuple[str, ...],
        selected: tuple[SourceRecord | EffectiveOccurrence, ...] = records,
        *,
        severity: str = "error",
        valid_from: str | None = None,
        valid_to: str | None = None,
    ) -> None:
        diagnostics.append(
            ResolutionDiagnostic.model_validate(
                {
                    "code": code,
                    "severity": severity,
                    "subject": subject,
                    "detail": detail,
                    "fields": fields,
                    "withheld_output": withheld,
                    "refs": _refs(selected),
                    "valid_from": valid_from,
                    "valid_to": valid_to,
                }
            )
        )

    columns: set[str] = set()
    for record in effective:
        field = record.fields.column_name
        if record.identity_checked or field is None or field.status != "value":
            continue
        assert isinstance(field.value, str)
        columns.add(field.value)
    if len({fold_column(column) for column in columns}) > 1:
        issue(
            "unresolved_native_identity",
            "The source-native variable has multiple column spellings; an exact partition or alias decision is required.",
            ("identity", "column_name"),
            (subject,),
        )
        return VariableFormation(None, tuple(diagnostics), records, (), ())
    chosen_spelling: str | None = None
    if len(columns) > 1:
        # Two spellings delivered side by side in one edition are two
        # columns, not one column spelled two ways: refuse the fold when a
        # single edition of one variant co-delivers two spellings of one
        # fold key, exactly as without the fold. Occurrences without edition
        # evidence cannot prove co-delivery, so they keep folding.
        by_edition: dict[
            tuple[NativeKey | None, NativeKey], dict[str, set[str]]
        ] = defaultdict(lambda: defaultdict(set))
        for record in effective:
            field = record.fields.column_name
            if (
                record.identity_checked
                or field is None
                or field.status != "value"
                or record.edition_key is None
            ):
                continue
            assert isinstance(field.value, str)
            by_edition[(record.variant_key, record.edition_key)][
                fold_column(field.value)
            ].add(field.value)
        if any(
            len(spellings) > 1
            for folds in by_edition.values()
            for spellings in folds.values()
        ):
            issue(
                "unresolved_native_identity",
                "The source-native variable has multiple column spellings; an exact partition or alias decision is required.",
                ("identity", "column_name"),
                (subject,),
            )
            return VariableFormation(None, tuple(diagnostics), records, (), ())
        # Case/diacritic twins are one physical column by the shared
        # column-identity key: formation proceeds on the most recent spelling
        # while the raw spellings stay visible on the occurrence evidence.
        spellings = sorted(columns)
        starts: dict[str, int] = {}
        for record in effective:
            field = record.fields.column_name
            if record.identity_checked or field is None or field.status != "value":
                continue
            assert isinstance(field.value, str)
            bounds = occurrence_bounds(record)
            start = min(start for start, _ in bounds) if bounds else -1
            starts[field.value] = max(starts.get(field.value, -1), start)
        chosen_spelling = max(
            spellings, key=lambda spelling: (starts.get(spelling, -1), spelling)
        )
        issue(
            "column_spelling_folded",
            "The source deliveries spell one column several ways ("
            + ", ".join(spellings)
            + "); states use the most recent spelling ("
            + chosen_spelling
            + ").",
            ("column_name",),
            (),
            severity="warning",
        )
    canonical, conflicts = reconcile_source_fields(effective)
    # operational_definition and source_attribution are state-grain, so varying
    # texts are no variable-level conflict; their summary below stays populated
    # only while reconciliation yields one stable value, and contradictions on
    # one overlapping column period remain conflicting_occurrence_facts.
    canonical_fields = {
        "name",
        "definition",
        "description",
        "measurement_unit",
    }
    for field_name in sorted(canonical_fields & set(conflicts)):
        issue(
            "conflicting_variable_fact",
            "Source occurrences disagree on a register-level variable fact; no source winner was selected.",
            (field_name,),
            (f"variable.{field_name}",),
        )
    name = _text(canonical, "name")
    if not name:
        issue(
            "unresolved_variable_name",
            "The source family does not establish one canonical variable name.",
            ("name",),
            (subject,),
        )
    by_variant: dict[NativeKey, list[EffectiveOccurrence]] = defaultdict(list)
    for record in effective:
        key = record.variant_key
        if key is None:
            issue(
                "unresolved_variant",
                "The source occurrence does not identify a variant.",
                ("variant",),
                ("occurrence",),
                (record,),
            )
            continue
        if key not in variants:
            raise ValueError(f"missing resolved variant mapping: {key!r}")
        if variants[key] is None:
            issue(
                "withheld_variant_dependency",
                "The source variant is unresolved; only its dependent delivery states are withheld.",
                ("variant",),
                ("state",),
                (record,),
            )
            continue
        by_variant[key].append(record)
    states = []
    intervals = []
    coding_results = []
    withheld_variants = {}
    columnless_records: list[SourceRecord] = []
    # Every finite positive source claim, against the periods an explicit outcome
    # withholds from it, both keyed by the exact variant and physical column.
    claims: list[CoverageObligation] = []
    # Origin of every formed state and claim: True when every contributing
    # occurrence is unchecked, i.e. the output is rooted in the folded twins
    # rather than in an explicit partition/alias decision. Keyed by id();
    # created_states keeps every state alive so the keys stay sound.
    folded_state: dict[int, bool] = {}
    folded_claim: dict[int, bool] = {}
    created_states: list[ResolvedState] = []
    waived: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for key, members in sorted(by_variant.items(), key=lambda item: repr(item[0])):
        variant = variants[key]
        assert variant is not None
        resolution = resolve_occurrence_intervals(members)
        intervals.append(resolution)
        variant_issues = []
        for problem in resolution.issues:
            if problem.code == "omitted_columnless_occurrence":
                # One warning per variable is emitted below; the per-variant
                # cause stays in the withheld bookkeeping only.
                variant_issues.append(
                    ResolutionDiagnostic(
                        code=problem.code,
                        severity="warning",
                        subject=subject,
                        detail=(
                            "The source states the member has no physical column; "
                            "the occurrence is omitted on purpose."
                        ),
                        refs=_refs(problem.occurrences),
                        fields=problem.fields,
                        valid_from=problem.valid_from,
                        valid_to=problem.valid_to,
                        withheld_output=problem.withheld,
                    )
                )
                columnless_records.extend(problem.occurrences)
                continue
            diagnosis = ResolutionDiagnostic(
                code=problem.code,
                severity="error",
                subject=subject,
                detail="Source occurrence facts cannot be safely resolved "
                "for the stated fields and period.",
                refs=_refs(problem.occurrences),
                fields=problem.fields,
                valid_from=problem.valid_from,
                valid_to=problem.valid_to,
                withheld_output=problem.withheld,
            )
            diagnostics.append(diagnosis)
            variant_issues.append(diagnosis)
        if not resolution.segments and variant_issues:
            withheld_variants[variant.slug] = tuple(variant_issues)
        by_column: dict[str, list[SourceSegment]] = defaultdict(list)
        for segment in resolution.segments:
            by_column[segment.delivery_column_name].append(segment)
        for column, segments in sorted(by_column.items()):
            code_key = segments[0].effective_occurrences[0].column_key
            assert code_key is not None
            if code_key not in coding:
                raise ValueError(f"missing explicit source coding lookup: {code_key!r}")
            code_result = coding[code_key]
            coding_results.append(code_result)
            for problem in code_result.issues:
                diagnostics.append(
                    ResolutionDiagnostic(
                        code=problem.code,
                        severity="error",
                        subject=subject,
                        detail="Bound source coding claims disagree or contain incomplete membership evidence: "
                        + ", ".join(problem.claim_ids)
                        + (
                            ". Curation cases: " + ", ".join(problem.case_ids)
                            if problem.case_ids
                            else ""
                        ),
                        refs=_refs(tuple(members)),
                        fields=("coding",),
                        valid_from=problem.valid_from,
                        valid_to=problem.valid_to,
                        withheld_output=(problem.withheld,),
                    )
                )
            for segment in segments:
                folded = all(
                    not occurrence.identity_checked
                    for occurrence in segment.effective_occurrences
                )
                new_claim = CoverageObligation(
                    subject,
                    variant.slug,
                    column,
                    segment.valid_from,
                    segment.valid_to,
                    _refs(segment.occurrences),
                    data_type_claim=_fact_claim(segment.fields.data_type),
                    data_length_claim=_fact_claim(segment.fields.data_length),
                    attributions=tuple(
                        sorted(
                            {
                                correction.provenance
                                for occurrence in segment.effective_occurrences
                                for correction in occurrence.corrections
                            }
                        )
                    ),
                )
                claims.append(new_claim)
                folded_claim[id(new_claim)] = folded
                new_states, new_issues, uncoded = _coded_states(
                    segment, variant, code_result, subject
                )
                waived[variant.slug, column].extend(uncoded)
                created_states.extend(new_states)
                for state in new_states:
                    folded_state[id(state)] = folded
                states.extend(new_states)
                diagnostics.extend(new_issues)
                if new_states and _text(segment.fields, "data_type") is None:
                    issue(
                        "unknown_data_type",
                        "The occurrence has no unambiguous documented data type.",
                        ("data_type",),
                        ("state.data_type",),
                        segment.occurrences,
                        valid_from=segment.valid_from,
                        valid_to=segment.valid_to,
                    )
    if columnless_records:
        # Resolution runs per variant; the omission is one warning per
        # variable with every columnless record as evidence.
        issue(
            "omitted_columnless_occurrence",
            "The source states the member has no physical column; "
            "the occurrence is omitted on purpose.",
            ("column_name",),
            ("occurrence",),
            tuple(columnless_records),
            severity="warning",
        )
    variable_key = effective[0].variable_key
    assert variable_key is not None
    states, aliases, grouping_issues, representation_waivers, representation_facts = (
        form_representations(
            states,
            representations,
            variable_key=variable_key,
            variants=variants,
            subject=subject,
        )
    )
    diagnostics.extend(grouping_issues)
    for variant_slug, column, start, end in representation_waivers:
        waived[variant_slug, column].append((start, end))
    previous_variants = {state.variant.slug for state in states}
    states, representation_issues, withheld_representations = _disjoint_representations(
        states, effective, subject, variants
    )
    diagnostics.extend(representation_issues)
    # Each cause names the exact parallel-column period it withholds.
    for (variant_slug, column), causes in withheld_representations.items():
        for cause in causes:
            assert cause.valid_from is not None and cause.valid_to is not None
            waived[variant_slug, column].append((cause.valid_from, cause.valid_to))
    for variant in sorted(previous_variants - {state.variant.slug for state in states}):
        causes = tuple(
            dict.fromkeys(
                problem
                for (slug, _column), problems in withheld_representations.items()
                if slug == variant
                for problem in problems
            )
        )
        if not causes:
            raise ValueError(
                "representation resolution lost a variant without an omission cause"
            )
        withheld_variants[variant] = causes
    present_representations = {
        (item.variant.slug, item.delivery_column_name) for item in (*states, *aliases)
    }
    withheld_representations = {
        key: causes
        for key, causes in withheld_representations.items()
        if key not in present_representations
    }
    if chosen_spelling is not None:
        # One folded column carries one delivery name downstream, where the
        # coverage gate matches claims to states by exact string. Only outputs
        # rooted in the unchecked folded occurrences unify, by recorded origin
        # — never by literal: a checked-exact column keeps its literal even
        # when it collides with a twin spelling. Representation and disjoint
        # handling above still see raw names, so overlapping twins keep their
        # graceful parallel-column error. Aliases stay exact: they exist only
        # via checked representation decisions naming exact columns.
        states = [
            state.model_copy(update={"delivery_column_name": chosen_spelling})
            if folded_state.get(id(state), False)
            else state
            for state in states
        ]
        claims = [
            replace(claim, column=chosen_spelling)
            if folded_claim.get(id(claim), False)
            else claim
            for claim in claims
        ]
    # What the supported occurrences still claim once every explicit outcome has
    # taken its own period back. Withholding the whole variable is one more such
    # outcome, recorded in the dependency ledger and honoured at the boundary.
    # A conflicting representation fact keeps its window but drops that exact
    # claimed fact, like a waived delivery slice.
    coverage = _null_conflicting_facts(
        tuple(
            replace(claim, valid_from=start, valid_to=end)
            for claim in claims
            for start, end in remaining_windows(
                waived[claim.variant, claim.column], claim.valid_from, claim.valid_to
            )
        ),
        representation_facts,
    )
    if not states:
        has_error = any(d.severity == "error" for d in diagnostics)
        accepted_omission = (
            any(d.code == "curated_state_omission" for d in diagnostics)
            and not has_error
        )
        # Every occurrence states it has no physical column: the variable is
        # not shown in the catalog, and the omission is an explained warning,
        # not an error. This is a positive fact about the records, not about
        # which other diagnostics stayed silent.
        columnless_only = bool(by_variant) and all(
            record.fields.column_name is not None
            and record.fields.column_name.status == "negative"
            for members in by_variant.values()
            for record in members
        )
        issue(
            "no_supported_states",
            "No source occurrence establishes a safe finite delivery state.",
            ("availability", "period"),
            (subject,),
            severity="warning" if accepted_omission or columnless_only else "error",
        )
    if not name or not states:
        return VariableFormation(
            None,
            tuple(diagnostics),
            records,
            tuple(intervals),
            tuple(coding_results),
            withheld_variants,
            withheld_representations,
            coverage,
        )
    flag_values = {}
    for source_name, output_name in (
        ("sensitivity", "is_sensitive"),
        ("identifier", "is_identifier"),
    ):
        observation = getattr(flags, source_name)
        flag_values[output_name] = (
            observation.value
            if observation is not None
            and observation.status == "value"
            and type(observation.value) is bool
            else None
        )
    conditional = flags.conditional_sensitivity
    if conditional is not None and (
        conditional.status != "value" or conditional.value is not False
    ):
        flag_values["is_sensitive"] = None
    variable = ResolvedVariable(
        register=register,
        slug=slug,
        provider_key=provider_key,
        name=name,
        definition=_text(canonical, "definition"),
        description=_text(canonical, "description"),
        operational_definition=_text(canonical, "operational_definition"),
        measurement_unit=_text(canonical, "measurement_unit"),
        source_register_text=_text(canonical, "source_attribution"),
        is_sensitive=flag_values["is_sensitive"],
        is_identifier=flag_values["is_identifier"],
        states=tuple(states),
        aliases=aliases,
    )
    if unknown := unresolved_variable_flags(variable):
        issue(
            "unresolved_flag",
            "Unknown flags cannot be represented by the current catalog schema; the variable and its dependent output are withheld.",
            unknown,
            (subject,),
        )
        variable = None
    return VariableFormation(
        variable,
        tuple(diagnostics),
        records,
        tuple(intervals),
        tuple(coding_results),
        withheld_variants,
        withheld_representations,
        coverage,
    )
