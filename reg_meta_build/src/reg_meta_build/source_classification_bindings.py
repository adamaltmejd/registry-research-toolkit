"""Apply checked classification declarations before state/representation formation."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING

from reg_meta_build.normalization import normalize_text
from reg_meta_build.source_classifications import resolve_classification_conformance
from reg_meta_build.source_coding import CodingIssue, CodingResolution, CodingSegment
from reg_meta_build.source_curation import (
    ClassificationDecision,
    CurationCase,
    ResolutionDiagnostic,
    evaluate_cases,
)
from reg_meta_build.source_effects import _require_checked, record_ref
from reg_meta_build.source_intervals import scope_bounds

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.resolved_catalog import ResolvedClassification
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import CaseEvaluation, SourceRecordRef
    from reg_meta_build.source_occurrences import EffectiveOccurrence
    from reg_meta_build.source_records import SourceRecord


@dataclass(frozen=True)
class ClassificationBindingResolution:
    coding: dict[NativeKey, CodingResolution]
    evaluations: tuple[CaseEvaluation, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


@dataclass(frozen=True)
class _Binding:
    valid_from: str
    valid_to: str
    classification: str | None
    refs: tuple[SourceRecordRef, ...]
    provenance: tuple[str, ...]
    inline_only: bool = False
    unresolved: bool = False
    rule: bool = False
    override: bool = False


def _source_bindings(
    occurrences: Iterable[EffectiveOccurrence],
    references: Mapping[str, str] | None,
    family_references: Mapping[str, tuple[str, ...]],
    coding: Mapping[NativeKey, CodingResolution],
    classifications: Mapping[str, ResolvedClassification],
) -> tuple[dict[NativeKey, list[_Binding]], list[ResolutionDiagnostic]]:
    """Read explicit declarations; an absent declaration is not a negative claim.

    Reference spelling is interpreted by an explicit caller-supplied dictionary,
    never by matching code contents or fragments of names/URLs. Corrected scopes
    and fields have already passed occurrence-case checks. They remain attached
    to their original evidence and cannot introduce delivery availability here.
    """
    selected: dict[NativeKey, list[_Binding]] = defaultdict(list)
    diagnostics = []
    for occurrence in occurrences:
        field = occurrence.fields.classification_declared
        if occurrence.use != "catalog" or field is None:
            continue
        refs = tuple(sorted({record_ref(r) for r in occurrence.evidence}, key=repr))
        if not refs:
            raise ValueError("source classification requires original evidence")
        key = occurrence.column_key
        scope = occurrence.edition_period_scope
        if scope.kind == "not_applicable":
            scope = occurrence.edition_scope
        bounds = scope_bounds(scope)
        code = None
        slug = None
        unspecified = (
            field.status == "unknown"
            and "classification_declared" not in occurrence.withheld_fields
        )
        if unspecified:
            code = "unknown_classification_declaration"
        elif key is None:
            code = "unknown_classification_column"
        elif bounds is None:
            code = "unsupported_classification_scope"
        elif (
            field.status == "unknown"
            or "classification_declared" in occurrence.withheld_fields
        ):
            code = "unknown_classification_declaration"
        elif field.status == "value":
            if not isinstance(field.value, str) or not field.value:
                raise ValueError("a classification reference must be nonempty text")
            if references is None:
                raise ValueError(
                    "source classification requires an explicit reference dictionary"
                )
            slug = references.get(field.value)
            if slug is None and field.value in family_references:
                members = family_references[field.value]
                if any(member not in classifications for member in members):
                    raise ValueError(
                        "classification family has an unconverted codebook"
                    )
                covering = [
                    member
                    for member in members
                    if all(
                        (
                            classifications[member].valid_from is None
                            or date.fromordinal(lo).year
                            >= classifications[member].valid_from
                        )
                        and (
                            classifications[member].valid_to is None
                            or date.fromordinal(hi).year
                            <= classifications[member].valid_to
                        )
                        for lo, hi in bounds
                    )
                ]
                if len(covering) == 1:
                    slug = covering[0]
            if slug is None:
                code = "unresolved_classification_reference"
            elif slug not in classifications or classifications[slug].slug != slug:
                raise ValueError("source classification has an unconverted codebook")
        periods = (
            tuple(
                (date.fromordinal(lo).isoformat(), date.fromordinal(hi).isoformat())
                for lo, hi in bounds
            )
            if bounds is not None
            else ((None, None),)
        )
        for start, end in periods:
            if code is not None:
                diagnostics.append(
                    ResolutionDiagnostic(
                        code=code,
                        severity="warning" if unspecified else "error",
                        subject=repr(key or occurrence.variable_key),
                        detail=(
                            "The source leaves optional classification metadata "
                            "unspecified; independently supplied declarations "
                            "remain usable."
                            if unspecified
                            else f"Source classification declaration {field.value!r} "
                            "cannot be bound at this exact column and scope; no "
                            "classification identity or period was inferred."
                        ),
                        refs=refs,
                        fields=("classification_declared",),
                        valid_from=start,
                        valid_to=end,
                        withheld_output=()
                        if unspecified
                        else ("state.classification",),
                    )
                )
            if unspecified or key is None or start is None or end is None:
                continue
            if key not in coding:
                raise ValueError("source classification has an unconverted column")
            selected[key].append(
                _Binding(
                    start,
                    end,
                    slug,
                    refs,
                    (
                        f"Source classification declaration: {field.value!r}",
                        *(
                            f"{c.case_id}: {c.provenance}"
                            for c in occurrence.corrections
                        ),
                    ),
                    unresolved=code is not None,
                )
            )
    return selected, diagnostics


def _sentinel_map(classification: ResolvedClassification) -> dict[str, str]:
    """Curated sentinel code-to-meaning map for one selected codebook."""
    return {
        sentinel.code: sentinel.meaning for sentinel in classification.sentinel_codes
    }


def _label_rule_bindings(
    coding: Mapping[NativeKey, CodingResolution],
    occurrences: Iterable[EffectiveOccurrence],
    labels: Mapping[str, str],
) -> dict[NativeKey, list[_Binding]]:
    """Bind every listed claim label on each included post-choice segment."""
    by_column: dict[NativeKey, list[tuple[int, int, tuple[SourceRecordRef, ...]]]] = (
        defaultdict(list)
    )
    for occurrence in occurrences:
        if occurrence.use == "catalog" and occurrence.column_key is not None:
            scope = occurrence.edition_period_scope
            if scope.kind == "not_applicable":
                scope = occurrence.edition_scope
            refs = tuple(sorted({record_ref(r) for r in occurrence.evidence}, key=repr))
            for lo, hi in scope_bounds(scope) or ():
                by_column[occurrence.column_key].append((lo, hi, refs))
    selected: dict[NativeKey, list[_Binding]] = defaultdict(list)
    for key, resolution in coding.items():
        claims = {claim.claim_id: claim for claim in resolution.claims}
        for segment in resolution.segments:
            if segment.state_disposition != "include":
                continue
            start = date.fromisoformat(segment.valid_from).toordinal()
            end = date.fromisoformat(segment.valid_to).toordinal()
            refs = tuple(
                sorted(
                    {
                        ref
                        for lo, hi, evidence in by_column[key]
                        if lo <= end and hi >= start
                        for ref in evidence
                    },
                    key=repr,
                )
            )
            if not refs:
                continue
            segment_labels = set()
            for claim_id in segment.claim_ids:
                claim = claims.get(claim_id)
                if claim is not None and claim.version_label is not None:
                    segment_labels.add(normalize_text(claim.version_label))
            slug_labels = {}
            for label in sorted(segment_labels):
                if label in labels:
                    slug_labels.setdefault(labels[label], label)
            for slug, label in sorted(slug_labels.items()):
                selected[key].append(
                    _Binding(
                        segment.valid_from,
                        segment.valid_to,
                        slug,
                        refs,
                        (f"label rule: {label!r} -> {slug}",),
                        rule=True,
                    )
                )
    return selected


def apply_classification_cases(
    records: Iterable[SourceRecord],
    cases: tuple[CurationCase, ...],
    *,
    coding: Mapping[NativeKey, CodingResolution],
    classifications: Mapping[str, ResolvedClassification],
    occurrences: Iterable[EffectiveOccurrence] = (),
    references: Mapping[str, str] | None = None,
    family_references: Mapping[str, tuple[str, ...]] = {},
    label_rules: Mapping[str, str] = {},
    override: tuple[str, str] | None = None,
    matched_labels: set[str] | None = None,
    duplicate_overrides: set[str] | None = None,
) -> ClassificationBindingResolution:
    """Compose source declarations and checked cases before state formation.

    Coding choices run first; their original claims remain the applicability
    basis. Classification never adds availability, copies canonical memberships,
    chooses an identity, or acknowledges noncanonical codes. Conflicting declared
    classes withhold only the binding on their intersection. Existing coding and
    omission diagnostics survive unchanged. An inline-only declaration has no
    effect where independently resolved inline codes are absent.

    Source declarations use the exact supplied reference dictionary. Their own
    explicitly open scopes remain open; finite curation windows never widen.
    Conflicting declarations are withheld, not resolved by call/file order.
    An occurrence-field correction can replace a checked original declaration.
    """
    occurrences = tuple(occurrences)
    ordered = tuple(sorted(cases, key=lambda c: c.case_id))
    if len({c.case_id for c in ordered}) != len(ordered):
        raise ValueError("classification case IDs must be unique")
    for case in ordered:
        if not isinstance(case.decision, ClassificationDecision):
            raise TypeError("classification application requires classification cases")
        decision = case.decision
        if decision.column_key not in coding:
            raise ValueError("classification has an unconverted column identity")
        if decision.classification not in classifications:
            raise ValueError("classification has an unconverted canonical codebook")
        if classifications[decision.classification].slug != decision.classification:
            raise ValueError("classification mapping key differs from its identity")
        if any(
            s.classification is not None or s.conformance is not None
            for s in coding[decision.column_key].segments
        ):
            raise ValueError("classification cases must compose in one application")
        guarded = {ref for guard in case.peer_guards for ref in guard.expected_members}
        for target in (*case.targets, *case.support):
            _require_checked(target, ("column_name",), case_id=case.case_id)
            if target.ref not in guarded or any(
                projection.code_set_references is None
                for projection in target.alternatives
            ):
                raise ValueError(
                    "classification requires guarded original coding evidence"
                )
    evaluations = evaluate_cases(ordered, records)
    selected, diagnostics = _source_bindings(
        occurrences, references, family_references, coding, classifications
    )
    rule_bindings = _label_rule_bindings(coding, occurrences, label_rules)
    for key, bindings in rule_bindings.items():
        for binding in bindings:
            assert binding.classification is not None
            book = classifications[binding.classification]
            selected[key].append(
                replace(
                    binding,
                    provenance=(
                        f"{binding.provenance[0]} (classifications/{book.short_name}.toml)",
                    ),
                )
            )
    if matched_labels is not None:
        matched_labels.update(
            normalize_text(claim.version_label)
            for resolution in coding.values()
            for claim in resolution.claims
            if claim.version_label is not None
            and normalize_text(claim.version_label) in label_rules
        )
    override_bindings: dict[NativeKey, list[_Binding]] = defaultdict(list)
    if override is not None:
        slug, ref = override
        for occurrence in occurrences:
            key = occurrence.column_key
            if occurrence.use != "catalog" or key is None or key not in coding:
                continue
            scope = occurrence.edition_period_scope
            if scope.kind == "not_applicable":
                scope = occurrence.edition_scope
            for lo, hi in scope_bounds(scope) or ():
                binding = _Binding(
                    date.fromordinal(lo).isoformat(),
                    date.fromordinal(hi).isoformat(),
                    slug,
                    tuple(
                        sorted({record_ref(r) for r in occurrence.evidence}, key=repr)
                    ),
                    (f"{ref}: override -> {slug}",),
                    inline_only=True,
                    override=True,
                )
                selected[key].append(binding)
                override_bindings[key].append(binding)
    if duplicate_overrides is not None and override is not None:
        slug, ref = override
        active_windows = [
            (
                key,
                max(binding.valid_from, segment.valid_from),
                min(binding.valid_to, segment.valid_to),
            )
            for key, bindings in override_bindings.items()
            for binding in bindings
            for segment in coding[key].segments
            if segment.state_disposition == "include"
            and segment.code_set is not None
            and binding.valid_from <= segment.valid_to
            and binding.valid_to >= segment.valid_from
        ]
        if active_windows and all(
            any(
                rule.classification == slug
                and rule.valid_from <= start
                and rule.valid_to >= end
                for rule in rule_bindings.get(key, ())
            )
            for key, start, end in active_windows
        ):
            duplicate_overrides.add(ref)
    canonical = {}
    sentinel_maps = {}
    for case, evaluation in zip(ordered, evaluations, strict=True):
        decision = case.decision
        assert isinstance(decision, ClassificationDecision)
        classification = classifications[decision.classification]
        if classification.slug not in canonical:
            canonical[classification.slug] = frozenset(
                c.code for c in classification.codes
            )
            sentinel_maps[classification.slug] = _sentinel_map(classification)
        for issue in evaluation.issues:
            diagnostics.append(
                ResolutionDiagnostic(
                    code=issue.code,
                    severity="error",
                    subject=repr(decision.column_key),
                    case_id=case.case_id,
                    detail=issue.detail,
                    applicability_issue=issue,
                    refs=tuple(t.ref for t in (*case.targets, *case.support)),
                    fields=("classification",),
                    valid_from=decision.valid_from,
                    valid_to=decision.valid_to,
                    withheld_output=("state.classification",),
                )
            )
        if evaluation.status != "applicable":
            continue
        selected[decision.column_key].append(
            _Binding(
                decision.valid_from,
                decision.valid_to,
                decision.classification,
                tuple(t.ref for t in (*case.targets, *case.support)),
                (f"{case.case_id}: {decision.reason}\n{decision.provenance}",),
                inline_only=decision.binding_scope == "inline_coding",
            )
        )

    result = dict(coding)
    for key, applicable in sorted(selected.items(), key=lambda pair: repr(pair[0])):
        applicable = sorted(set(applicable), key=repr)
        base = coding[key]
        if any(
            s.classification is not None or s.conformance is not None
            for s in base.segments
        ):
            raise ValueError(
                "classification declarations must compose in one application"
            )
        cuts = sorted(
            {
                point
                for item in (*base.segments, *applicable)
                for point in (
                    date.fromisoformat(item.valid_from).toordinal(),
                    date.fromisoformat(item.valid_to).toordinal() + 1,
                )
            }
        )
        segments = []
        coding_issues = list(base.issues)
        for lo, hi in pairwise(cuts):
            start, end = (
                date.fromordinal(lo).isoformat(),
                date.fromordinal(hi - 1).isoformat(),
            )
            prior = [
                s for s in base.segments if s.valid_from <= start and s.valid_to >= end
            ]
            if len(prior) > 1:
                raise ValueError("classification received overlapping coding segments")
            segment = (
                replace(prior[0], valid_from=start, valid_to=end)
                if prior
                else CodingSegment(start, end, None, ())
            )
            active = [
                d
                for d in applicable
                if d.valid_from <= start
                and d.valid_to >= end
                and (not d.inline_only or segment.code_set is not None)
            ]
            if any(d.override for d in active):
                active = [d for d in active if not d.rule]
            if not active or segment.state_disposition != "include":
                if prior:
                    segments.append(segment)
                continue
            refs = tuple(
                sorted(
                    {ref for d in active for ref in d.refs},
                    key=repr,
                )
            )
            classes = {d.classification for d in active if not d.unresolved}
            if len(classes) > 1:
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="conflicting_classification_decisions",
                        severity="error",
                        subject=repr(key),
                        detail=f"Declarations select different classifications {sorted(classes, key=repr)!r}; evidence {[p for d in active for p in d.provenance]!r}.",
                        refs=refs,
                        fields=("classification",),
                        valid_from=start,
                        valid_to=end,
                        withheld_output=("state.classification",),
                    )
                )
            if len(classes) > 1 or any(d.unresolved for d in active):
                if prior:
                    segments.append(segment)
                continue
            slug = next(iter(classes))
            conformance = None
            if slug is not None and segment.code_set is not None:
                if slug not in canonical:
                    canonical[slug] = frozenset(
                        c.code for c in classifications[slug].codes
                    )
                    sentinel_maps[slug] = _sentinel_map(classifications[slug])
                checked = resolve_classification_conformance(
                    segment.code_set,
                    classification=slug,
                    canonical_codes=canonical[slug],
                    subject=repr(key),
                    refs=refs,
                    valid_from=start,
                    valid_to=end,
                    sentinel_codes=sentinel_maps[slug],
                )
                diagnostics.extend(checked.diagnostics)
                conformance = checked.conformance
                if conformance.status == "severed":
                    slug = None
            elif not prior and base.claims:
                coding_issues.append(
                    CodingIssue(
                        "missing_coding_period",
                        tuple(c.claim_id for c in base.claims),
                        start,
                        end,
                    )
                )
            attribution = tuple(
                sorted(
                    set(segment.provenance) | {p for d in active for p in d.provenance}
                )
            )
            segments.append(
                replace(
                    segment,
                    classification=slug,
                    conformance=conformance,
                    provenance=attribution,
                )
            )
        result[key] = CodingResolution(
            tuple(segments), tuple(coding_issues), base.claims
        )
    return ClassificationBindingResolution(
        result, evaluations, tuple(sorted(diagnostics, key=repr))
    )
