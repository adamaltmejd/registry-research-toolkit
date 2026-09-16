"""Compose checked occurrence corrections against immutable original evidence.

All applicability checks precede every effect. A preceding correction cannot make
a later decision applicable. Equal or disjoint assignments compose; contradictory
assignments withhold only the disputed fact, with every claim retained in the ledger.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal

from reg_meta_build.source_curation import (
    CaseEvaluation,
    CheckedFieldChange,
    CheckedIdentityChange,
    CheckedPeriodChange,
    CheckedSourceUse,
    CheckedVariantAssignment,
    CuratedOccurrenceAddition,
    CurationCase,
    OccurrenceCorrectionDecision,
    ResolutionDiagnostic,
    SourceEvidence,
    SourceRecordRef,
    _field_matches,
    evaluate_cases,
)
from reg_meta_build.source_intervals import scope_bounds
from reg_meta_build.source_occurrences import (
    AppliedCorrection,
    EffectiveOccurrence,
    source_occurrence,
)
from reg_meta_build.source_records import SourceField, SourceFields, TemporalScope

if TYPE_CHECKING:
    from reg_meta_build.source_curation import RecordExpectation
    from reg_meta_build.source_records import SourceRecord


@dataclass(frozen=True)
class CorrectionAccounting:
    case: CurationCase
    evaluation: CaseEvaluation
    disposition: Literal["applied", "stale", "conflicted"]
    conflicting_effects: tuple[int, ...] = ()


@dataclass(frozen=True)
class OccurrenceCorrections:
    occurrences: tuple[EffectiveOccurrence, ...]
    accounting: tuple[CorrectionAccounting, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


def record_ref(record: SourceRecord) -> SourceRecordRef:
    return SourceRecordRef(
        source=record.source, semantic_record_key=record.locators[0].semantic_record_key
    )


def _require_checked(expected: RecordExpectation, fields: tuple[str, ...] = ()) -> None:
    shape = expected.alternatives[0]
    if (
        shape.subject is None
        or shape.edition_scope is None
        or shape.edition_period_scope is None
        or set(fields) - {field.name for field in shape.fields}
    ):
        raise ValueError(
            "occurrence effects require checked subject, both scopes and every changed/copied field"
        )


def _check_contract(case: CurationCase) -> None:
    decision = case.decision
    if not isinstance(decision, OccurrenceCorrectionDecision):
        raise TypeError("occurrence application accepts only occurrence corrections")
    targets = {item.ref: item for item in case.targets}
    checked = {item.ref: item for item in (*case.support, *case.targets)}
    for effect in decision.effects:
        if isinstance(effect, CuratedOccurrenceAddition):
            if not case.peer_guards:
                raise ValueError(
                    "an occurrence addition requires finite peer membership guards"
                )
            if any(ref not in checked for ref in effect.evidence):
                raise ValueError("addition evidence must be checked targets or support")
            for ref in effect.evidence:
                _require_checked(
                    checked[ref], effect.copied_fields if ref == effect.donor else ()
                )
            scope = effect.edition_period_scope
            if scope.kind == "not_applicable":
                scope = effect.edition_scope
            if scope_bounds(scope) is None and not (
                effect.edition_key is not None and scope.kind in {"unknown", "pooled"}
            ):
                raise ValueError(
                    "an added occurrence requires a known period or an explicit edition with unresolved coverage"
                )
            if effect.donor is not None:
                for alternative in checked[effect.donor].alternatives:
                    donor_fields = {field.name: field for field in alternative.fields}
                    for name in effect.copied_fields:
                        expected = donor_fields[name]
                        actual = getattr(effect.fields, name)
                        if expected.status == "absent":
                            agrees = actual is None
                        else:
                            agrees = actual is not None and (
                                actual.status,
                                actual.value,
                            ) == (expected.status, expected.value)
                        if not agrees:
                            raise ValueError(
                                "a copied field must equal every checked donor alternative"
                            )
        else:
            if effect.ref not in targets:
                raise ValueError("an occurrence effect must name an exact target")
            if isinstance(
                effect,
                (CheckedIdentityChange, CheckedSourceUse, CheckedVariantAssignment),
            ) and not any(
                effect.ref in guard.expected_members for guard in case.peer_guards
            ):
                raise ValueError(
                    "an identity/use assignment requires guarded source membership"
                )
            if isinstance(
                effect, (CheckedFieldChange, CheckedIdentityChange)
            ) and not any(
                all(field in alternative.fields for field in effect.when)
                for alternative in targets[effect.ref].alternatives
            ):
                raise ValueError(
                    "effect conditions must match a checked source alternative"
                )
            _require_checked(
                targets[effect.ref],
                (effect.replacement.name, *(field.name for field in effect.when))
                if isinstance(effect, CheckedFieldChange)
                else ("column_name", *(field.name for field in effect.when))
                if isinstance(effect, CheckedIdentityChange)
                else (),
            )


def apply_occurrence_cases(
    records: tuple[SourceRecord, ...] | SourceEvidence, cases: tuple[CurationCase, ...]
) -> OccurrenceCorrections:
    """Resolve a complete relevant source slice; never select only expected peers.

    Contract errors are fatal in both strict and diagnostic modes. Stale decisions
    do not apply. Their evidence and errors remain available alongside source facts.
    """
    ordered = tuple(sorted(cases, key=lambda case: case.case_id))
    if len({case.case_id for case in ordered}) != len(ordered):
        raise ValueError("occurrence correction case IDs must be unique")
    for case in ordered:
        _check_contract(case)
    evaluations = evaluate_cases(ordered, records)
    if isinstance(records, SourceEvidence):
        records = records.records
    fields = defaultdict(list)
    periods = defaultdict(list)
    identities = defaultdict(list)
    variants = defaultdict(list)
    support_uses = defaultdict(list)
    additions = defaultdict(list)
    diagnostics = []
    for case, evaluation in zip(ordered, evaluations, strict=True):
        for issue in evaluation.issues:
            diagnostics.append(
                ResolutionDiagnostic(
                    code=issue.code,
                    severity="error",
                    case_id=case.case_id,
                    subject=issue.subject,
                    detail=issue.detail,
                    refs=tuple(item.ref for item in (*case.targets, *case.support)),
                    applicability_issue=issue,
                    withheld_output=("curation_effects",),
                )
            )
        if evaluation.status == "stale":
            continue
        decision = case.decision
        assert isinstance(decision, OccurrenceCorrectionDecision)
        for index, effect in enumerate(decision.effects):
            correction = AppliedCorrection(case.case_id, index, decision.provenance)
            if isinstance(effect, CheckedFieldChange):
                fields[effect.ref].append((effect, correction))
            elif isinstance(effect, CheckedPeriodChange):
                periods[effect.ref].append((effect, correction))
            elif isinstance(effect, CheckedIdentityChange):
                identities[effect.ref].append((effect, correction))
            elif isinstance(effect, CheckedSourceUse):
                support_uses[effect.ref].append(correction)
            elif isinstance(effect, CheckedVariantAssignment):
                variants[effect.ref].append((effect, correction))
            else:
                additions[effect.occurrence_key].append((effect, correction))

    # Added occurrences need checked support. Unchanged records already retain
    # their evidence directly and need no second full-slice reference index.
    support_refs = {
        ref
        for claims in additions.values()
        for effect, _ in claims
        for ref in effect.evidence
    }
    evidence: dict[SourceRecordRef, list[SourceRecord]] = defaultdict(list)
    if support_refs:
        for record in records:
            if (ref := record_ref(record)) in support_refs:
                evidence[ref].append(record)
    changed_refs = (
        fields.keys()
        | periods.keys()
        | identities.keys()
        | variants.keys()
        | support_uses.keys()
    )

    conflicted: dict[str, set[int]] = defaultdict(set)

    def conflict(claims, subject: str, refs, names, withheld) -> None:
        detail = "Competing checked assignments: " + "; ".join(
            f"{owner.case_id}[{owner.effect_index}]={effect.model_dump_json()}"
            for effect, owner in claims
        )
        for _, owner in claims:
            conflicted[owner.case_id].add(owner.effect_index)
            diagnostics.append(
                ResolutionDiagnostic(
                    code="conflicting_curation_effects",
                    severity="error",
                    case_id=owner.case_id,
                    subject=subject,
                    detail=detail,
                    refs=refs,
                    fields=names,
                    withheld_output=withheld,
                )
            )

    field_owners = defaultdict(list)
    withheld_fields = defaultdict(set)
    period_changes = {}
    for ref, claims in periods.items():
        replacements = {
            (effect.edition_scope, effect.edition_period_scope) for effect, _ in claims
        }
        if len(replacements) > 1:
            conflict(claims, str(ref), (ref,), ("period",), ("occurrence",))
            period_changes[ref] = (
                TemporalScope(kind="unknown", label="conflicting checked periods"),
                TemporalScope(kind="unknown", label="conflicting checked periods"),
            )
        else:
            period_changes[ref] = next(iter(replacements))
        field_owners[ref].extend(owner for _, owner in claims)

    reported_identity_conflicts = set()
    reported_field_conflicts = set()
    variant_changes = {}
    for ref, claims in variants.items():
        replacements = {effect.variant_keys for effect, _ in claims}
        if len(replacements) > 1:
            conflict(claims, str(ref), (ref,), ("variant",), ("occurrence.variant",))
            variant_changes[ref] = (None,)
            withheld_fields[ref].add("variant")
        else:
            variant_changes[ref] = next(iter(replacements))
        field_owners[ref].extend(owner for _, owner in claims)
    occurrences = []
    for record in records:
        ref = record_ref(record)
        occurrence = source_occurrence(record)
        if ref not in changed_refs:
            occurrences.append(occurrence)
            continue
        values = {
            name: getattr(record.fields, name) for name in SourceFields.model_fields
        }
        matching_fields = defaultdict(list)
        for effect, owner in fields.get(ref, ()):
            if all(_field_matches(record, field) for field in effect.when):
                matching_fields[effect.replacement.name].append((effect, owner))
        changed_fields = set(withheld_fields[ref])
        matching_owners = []
        for name, claims in matching_fields.items():
            replacements = {effect.replacement for effect, _ in claims}
            matching_owners.extend(owner for _, owner in claims)
            if len(replacements) > 1:
                signature = (ref, name, tuple(owner for _, owner in claims))
                if signature not in reported_field_conflicts:
                    conflict(claims, str(ref), (ref,), (name,), (name,))
                    reported_field_conflicts.add(signature)
                values[name] = SourceField(status="unknown")
                changed_fields.add(name)
            else:
                replacement = next(iter(replacements))
                values[name] = (
                    None
                    if replacement.status == "absent"
                    else SourceField(status=replacement.status, value=replacement.value)
                )
        scope, period = period_changes.get(
            ref, (record.edition_scope, record.edition_period_scope)
        )
        identity_claims = [
            (effect, owner)
            for effect, owner in identities.get(ref, ())
            if all(_field_matches(record, field) for field in effect.when)
        ]
        assigned = {effect.variable_key for effect, _ in identity_claims}
        variable_key = occurrence.variable_key
        withheld = changed_fields
        if len(assigned) > 1:
            variable_key = None
            withheld.add("identity")
            signature = (ref, tuple(owner for _, owner in identity_claims))
            if signature not in reported_identity_conflicts:
                conflict(
                    identity_claims,
                    str(ref),
                    (ref,),
                    ("identity",),
                    ("occurrence.identity",),
                )
                reported_identity_conflicts.add(signature)
        elif assigned:
            variable_key = next(iter(assigned))
        corrected = replace(
            occurrence,
            variable_key=variable_key,
            use="support" if support_uses.get(ref) else occurrence.use,
            fields=SourceFields.model_validate(values),
            edition_scope=scope,
            edition_period_scope=period,
            corrections=tuple(
                sorted(
                    (
                        *field_owners[ref],
                        *matching_owners,
                        *support_uses.get(ref, ()),
                        *(owner for _, owner in identity_claims),
                    ),
                    key=lambda c: (c.case_id, c.effect_index),
                )
            ),
            withheld_fields=tuple(sorted(withheld)),
        )
        targets = variant_changes.get(ref, (occurrence.variant_key,))
        if targets != (occurrence.variant_key,) and (
            occurrence.edition_key is not None or occurrence.population_key is not None
        ):
            # simplify: delivered token maps have no edition/population assignment;
            # require an explicit rebind operation if that source shape is needed.
            raise ValueError(
                "variant routing cannot implicitly reparent an edition or population"
            )
        occurrences.extend(replace(corrected, variant_key=key) for key in targets)
    for key, claims in sorted(additions.items()):
        # Evidence references differ legitimately for identical assertions; retain
        # all of them instead of making insertion order choose provenance.
        assertions = {
            effect.model_dump_json(exclude={"evidence", "donor", "copied_fields"})
            for effect, _ in claims
        }
        refs = tuple(
            sorted(
                {ref for effect, _ in claims for ref in effect.evidence},
                key=lambda ref: (ref.source, ref.semantic_record_key),
            )
        )
        if len(assertions) > 1:
            conflict(claims, key, refs, ("occurrence",), (key,))
            continue
        effect = claims[0][0]
        occurrences.append(
            EffectiveOccurrence(
                provider=effect.provider,
                variable_key=effect.variable_key,
                variant_key=effect.variant_key,
                edition_key=effect.edition_key,
                population_key=effect.population_key,
                fields=effect.fields,
                edition_scope=effect.edition_scope,
                edition_period_scope=effect.edition_period_scope,
                source_records=(),
                support_records=tuple(
                    record for ref in refs for record in evidence[ref]
                ),
                occurrence_key=key,
                corrections=tuple(owner for _, owner in claims),
            )
        )
    return OccurrenceCorrections(
        tuple(occurrences),
        tuple(
            CorrectionAccounting(
                case,
                evaluation,
                "stale"
                if evaluation.status == "stale"
                else "conflicted"
                if case.case_id in conflicted
                else "applied",
                tuple(sorted(conflicted[case.case_id])),
            )
            for case, evaluation in zip(ordered, evaluations, strict=True)
        ),
        tuple(diagnostics),
    )
