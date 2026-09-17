"""Ordinary catalog formation from one explicit source-native variable family.

Naming/topology and code-list bindings are supplied by the common resolver. This
path needs no handwritten whole-variable case. It never identifies variables by
column spelling, merges different source families, or chooses a conflicting fact.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING

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


def _coded_states(
    segment: SourceSegment,
    variant: ResolvedVariant,
    coding: CodingResolution,
    subject: str,
) -> tuple[list[ResolvedState], list[ResolutionDiagnostic]]:
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
    return states, diagnostics


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
    identity decision; the ordinary path does not guess whether they are renames
    or different questions. Each physical input occurrence remains in the result.
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

    columns = {
        record.fields.column_name.value
        for record in effective
        if not record.identity_checked
        and record.fields.column_name is not None
        and record.fields.column_name.status == "value"
    }
    if len(columns) > 1:
        issue(
            "unresolved_native_identity",
            "The source-native variable has multiple column spellings; an exact partition or alias decision is required.",
            ("identity", "column_name"),
            (subject,),
        )
        return VariableFormation(None, tuple(diagnostics), records, (), ())
    canonical, conflicts = reconcile_source_fields(effective)
    canonical_fields = {
        "name",
        "definition",
        "description",
        "operational_definition",
        "measurement_unit",
        "source_attribution",
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
    for key, members in sorted(by_variant.items(), key=lambda item: repr(item[0])):
        variant = variants[key]
        assert variant is not None
        resolution = resolve_occurrence_intervals(members)
        intervals.append(resolution)
        variant_issues = []
        for problem in resolution.issues:
            diagnosis = ResolutionDiagnostic(
                code=problem.code,
                severity="error",
                subject=subject,
                detail="Source occurrence facts cannot be safely resolved for the stated fields and period.",
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
                new_states, new_issues = _coded_states(
                    segment, variant, code_result, subject
                )
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
    variable_key = effective[0].variable_key
    assert variable_key is not None
    states, aliases, grouping_issues = form_representations(
        states,
        representations,
        variable_key=variable_key,
        variants=variants,
        subject=subject,
    )
    diagnostics.extend(grouping_issues)
    previous_variants = {state.variant.slug for state in states}
    states, representation_issues, withheld_representations = _disjoint_representations(
        states, effective, subject, variants
    )
    diagnostics.extend(representation_issues)
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
    if not states:
        accepted_omission = any(
            d.code == "curated_state_omission" for d in diagnostics
        ) and not any(d.severity == "error" for d in diagnostics)
        issue(
            "no_supported_states",
            "No source occurrence establishes a safe finite delivery state.",
            ("availability", "period"),
            (subject,),
            severity="warning" if accepted_omission else "error",
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
    )
