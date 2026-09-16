"""Apply checked classification declarations before state/representation formation."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING

from reg_meta_build.source_classifications import resolve_classification_conformance
from reg_meta_build.source_coding import CodingIssue, CodingResolution, CodingSegment
from reg_meta_build.source_coding_choices import coding_expectations
from reg_meta_build.source_curation import (
    ClassificationDecision,
    CurationCase,
    ResolutionDiagnostic,
    evaluate_cases,
)
from reg_meta_build.source_effects import _require_checked, record_ref
from reg_meta_build.source_intervals import scope_bounds
from reg_meta_build.source_records import canonical_sha256

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


def _source_bindings(
    occurrences: Iterable[EffectiveOccurrence],
    references: Mapping[str, str] | None,
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


def classification_content_sha256(classification: ResolvedClassification) -> str:
    """Pin semantic codebook content, independently of member order and succession."""
    body = classification.model_dump(mode="json", exclude={"codes"})
    body["codes"] = [
        code.model_dump(mode="json")
        for code in sorted(classification.codes, key=lambda c: (c.code, c.label))
    ]
    return canonical_sha256(body)


def apply_classification_cases(
    records: Iterable[SourceRecord],
    cases: tuple[CurationCase, ...],
    *,
    coding: Mapping[NativeKey, CodingResolution],
    classifications: Mapping[str, ResolvedClassification],
    occurrences: Iterable[EffectiveOccurrence] = (),
    references: Mapping[str, str] | None = None,
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
            _require_checked(target, ("column_name",))
            if target.ref not in guarded or any(
                projection.code_set_references is None
                for projection in target.alternatives
            ):
                raise ValueError(
                    "classification requires guarded original coding evidence"
                )
    evaluations = evaluate_cases(ordered, records)
    selected, diagnostics = _source_bindings(
        occurrences, references, coding, classifications
    )
    class_hashes = {}
    canonical = {}
    for case, evaluation in zip(ordered, evaluations, strict=True):
        decision = case.decision
        assert isinstance(decision, ClassificationDecision)
        classification = classifications[decision.classification]
        if classification.slug not in class_hashes:
            class_hashes[classification.slug] = classification_content_sha256(
                classification
            )
            canonical[classification.slug] = frozenset(
                c.code for c in classification.codes
            )
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
        observed = coding_expectations(
            coding[decision.column_key].claims, decision.valid_from, decision.valid_to
        )
        if set(observed) != set(decision.expected_codings) or (
            class_hashes[classification.slug] != decision.expected_classification
        ):
            diagnostics.append(
                ResolutionDiagnostic(
                    code="classification_evidence_changed",
                    severity="error",
                    subject=repr(decision.column_key),
                    case_id=case.case_id,
                    detail="Original coding or the selected canonical codebook changed; "
                    "the existing classification decision was not replayed. "
                    + decision.provenance,
                    refs=tuple(t.ref for t in (*case.targets, *case.support)),
                    fields=("coding", "classification"),
                    valid_from=decision.valid_from,
                    valid_to=decision.valid_to,
                    withheld_output=("state.classification",),
                )
            )
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
                checked = resolve_classification_conformance(
                    segment.code_set,
                    classification=slug,
                    canonical_codes=canonical[slug],
                    subject=repr(key),
                    refs=refs,
                    valid_from=start,
                    valid_to=end,
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
