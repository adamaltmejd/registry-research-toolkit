"""Compose common resolution over a complete source scope, before materialization.

A scope contains every original peer needed by its decisions and every competing
alias owner in its registers. Catalog naming never gates source-level decisions.
The caller opens prepared value sessions once and seals support bindings against
the complete selected corpus before invoking this resolver.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.catalog_resolution import ParentResolution, resolve_parents
from reg_meta_build.source_annotations import apply_alias_cases
from reg_meta_build.source_classification_bindings import apply_classification_cases
from reg_meta_build.source_coding_choices import apply_coding_choices
from reg_meta_build.source_curation import (
    ClassificationDecision,
    CodingDecision,
    RepresentationDecision,
    ResolutionDiagnostic,
    SourceEvidence,
)
from reg_meta_build.source_effects import (
    OccurrenceCorrections,
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_intervals import reconcile_source_fields
from reg_meta_build.source_naming import check_naming_target
from reg_meta_build.source_representations import resolve_representation_cases
from reg_meta_build.source_value_bindings import bind_occurrence_code_lists

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from reg_meta_build.resolved_catalog import (
        ResolvedClassification,
        ResolvedVariable,
        ResolvedVariant,
    )
    from reg_meta_build.source_coding import CodeListClaim
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import CaseEvaluation, CurationCase
    from reg_meta_build.source_naming import NamingDeclaration
    from reg_meta_build.source_occurrences import EffectiveOccurrence
    from reg_meta_build.source_records import SourceRecord, SourceRevision
    from reg_meta_build.source_support import SourceSupportBindings
    from reg_meta_build.source_value_bindings import (
        ValueBindingResult,
        ValueBindingSession,
    )


@dataclass(frozen=True)
class ScopeResolution:
    parents: ParentResolution
    corrections: OccurrenceCorrections
    variables: dict[NativeKey, ResolvedVariable | None]
    evaluations: tuple[CaseEvaluation, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]
    error_count: int
    warning_count: int


def resolve_source_scope(
    originals: tuple[SourceRecord, ...],
    *,
    cases: tuple[CurationCase, ...],
    naming: tuple[NamingDeclaration, ...],
    provider_keys: Mapping[NativeKey, str | None],
    value_sessions: tuple[ValueBindingSession, ...],
    support: SourceSupportBindings,
    classifications: Mapping[str, ResolvedClassification],
    classification_references: Mapping[str, str],
    declared_variants: Mapping[NativeKey, ResolvedVariant] | None = None,
    revisions: tuple[SourceRevision, ...] = (),
    on_binding: Callable[[NativeKey, ValueBindingResult], None] | None = None,
    on_diagnostic: Callable[[ResolutionDiagnostic], None] | None = None,
) -> ScopeResolution:
    """Resolve one complete scope without IO policy or a strict/diagnostic fork.

    Missing mappings and unsupported decisions are implementation failures. An
    explicit None provider key records an unresolved catalog identity; coding and
    other source decisions still run. Checked naming and parent facts determine
    which dependencies may be materialized. No source key is parsed as a catalog
    identity, and a withheld variant does not discard safe sibling states.
    A diagnostic sink streams the full ledger instead of retaining it in memory;
    severity counts are always returned, including when the sink is used.
    """
    if len({c.case_id for c in cases}) != len(cases):
        raise ValueError("source scope case IDs must be unique")
    supported = {
        "correct_occurrences",
        "coding",
        "classification",
        "representations",
        "search_alias",
        "alias_window",
    }
    if any(c.decision.kind not in supported for c in cases):
        raise ValueError("unsupported source scope decision")
    evidence = SourceEvidence(originals)
    diagnostics = []
    counts = {"error": 0, "warning": 0}

    def emit(issue: ResolutionDiagnostic) -> None:
        counts[issue.severity] += 1
        if on_diagnostic is None:
            diagnostics.append(issue)
        else:
            on_diagnostic(issue)

    names = {}
    withheld_naming = set()
    for declaration in naming:
        target = declaration.target
        token = target.kind, target.source_key
        if token in names and names[token].naming != declaration.naming:
            raise ValueError(f"conflicting catalog naming declarations: {token!r}")
        names.setdefault(token, declaration)
        issues = check_naming_target(target, evidence, revisions=revisions)
        for issue in issues:
            emit(issue)
        if issues:
            withheld_naming.add(target.source_key)
    corrected = apply_occurrence_cases(
        evidence, tuple(c for c in cases if c.decision.kind == "correct_occurrences")
    )
    for issue in corrected.diagnostics:
        emit(issue)
    evaluations = [a.evaluation for a in corrected.accounting]
    parents = resolve_parents(
        corrected.occurrences,
        tuple(names.values()),
        withheld_naming=frozenset(withheld_naming),
    )
    for issue in parents.diagnostics:
        emit(issue)
    variants: dict[NativeKey, ResolvedVariant | None] = {
        key: parents.variants.get(key)
        for kind, key in names
        if kind == "register_variant"
    }
    for key, variant in (declared_variants or {}).items():
        declaration = names.get(("register_variant", key))
        if declaration is None or declaration.naming.slug != variant.slug:
            raise ValueError("declared variant lacks matching checked naming")
        if key in parents.variants and parents.variants[key] != variant:
            raise ValueError("declared variant conflicts with resolved source facts")
        if (
            key not in withheld_naming
            and declaration.target.register_key in parents.registers
        ):
            variants[key] = variant
            parents.variants[key] = variant

    groups: dict[NativeKey, list[EffectiveOccurrence]] = defaultdict(list)
    column_owners = {}
    for occurrence in corrected.occurrences:
        if occurrence.use != "catalog" or occurrence.variable_key is None:
            continue
        groups[occurrence.variable_key].append(occurrence)
        if occurrence.column_key is not None:
            owner = column_owners.setdefault(
                occurrence.column_key, occurrence.variable_key
            )
            if owner != occurrence.variable_key:
                raise ValueError("one exact column key has multiple variable owners")
    late = defaultdict(list)
    aliases = []
    for case in cases:
        kind = case.decision.kind
        if kind == "correct_occurrences":
            continue
        if kind in {"search_alias", "alias_window"}:
            aliases.append(case)
            continue
        if isinstance(case.decision, (CodingDecision, ClassificationDecision)):
            key = column_owners.get(case.decision.column_key)
        else:
            assert isinstance(case.decision, RepresentationDecision)
            key = case.decision.variable_key
        if key not in groups:
            raise ValueError(
                f"case has an unconverted effective identity: {case.case_id}"
            )
        late[key].append(case)
    variables = {}
    for key, items in sorted(groups.items(), key=lambda item: repr(item[0])):
        occurrences = tuple(items)
        refs = tuple(
            sorted({record_ref(r) for o in occurrences for r in o.evidence}, key=repr)
        )
        if key not in provider_keys:
            raise ValueError(f"missing explicit provider key mapping: {key!r}")
        claims: dict[NativeKey, list[CodeListClaim]] = defaultdict(list)
        for occurrence in occurrences:
            column = occurrence.column_key
            if column is None:
                continue
            bound = bind_occurrence_code_lists(occurrence, value_sessions)
            claims[column].extend(bound.claims)
            if on_binding is not None:
                on_binding(key, bound)
            for issue in bound.issues:
                emit(
                    ResolutionDiagnostic(
                        code=issue.code,
                        severity="error",
                        subject=repr(column),
                        detail=f"Prepared coding evidence cannot be bound: {issue!r}",
                        refs=tuple(
                            dict.fromkeys(record_ref(r) for r in occurrence.evidence)
                        ),
                        fields=("coding",),
                        withheld_output=("state.value_set",),
                    )
                )
        selected = tuple(late[key])
        chosen = apply_coding_choices(
            evidence,
            tuple(c for c in selected if c.decision.kind == "coding"),
            coding={column: tuple(values) for column, values in claims.items()},
        )
        classified = apply_classification_cases(
            evidence,
            tuple(c for c in selected if c.decision.kind == "classification"),
            coding=chosen.coding,
            classifications=classifications,
            occurrences=occurrences,
            references=classification_references,
        )
        representation = resolve_representation_cases(
            evidence,
            tuple(c for c in selected if c.decision.kind == "representations"),
            coding=classified.coding,
        )
        for result in (chosen, classified, representation):
            evaluations.extend(result.evaluations)
            for issue in result.diagnostics:
                emit(issue)
        declaration = names.get(("variable", key))
        provider_key = provider_keys[key]
        if provider_key is not None and (
            declaration is None or declaration.naming.slug is None
        ):
            raise ValueError(f"provider key has no converted catalog naming: {key!r}")
        register = (
            parents.registers.get(declaration.target.register_key)
            if declaration
            else None
        )
        variables[key] = None
        if provider_key is None or key in withheld_naming or register is None:
            emit(
                ResolutionDiagnostic(
                    code="unresolved_catalog_identity"
                    if provider_key is None or key in withheld_naming
                    else "withheld_register_dependency",
                    severity="error",
                    subject=repr(key),
                    detail="Catalog formation is withheld; source-level decisions were still evaluated against the complete original scope.",
                    refs=refs,
                    fields=("identity",),
                    withheld_output=("variable",),
                )
            )
            for column, coding in classified.coding.items():
                for issue in coding.issues:
                    emit(
                        ResolutionDiagnostic(
                            code=issue.code,
                            severity="error",
                            subject=repr(column),
                            detail="Source coding remains unresolved even though catalog identity is withheld: "
                            + ", ".join(issue.claim_ids),
                            refs=refs,
                            fields=("coding",),
                            valid_from=issue.valid_from,
                            valid_to=issue.valid_to,
                            withheld_output=(issue.withheld,),
                        )
                    )
            continue
        assert declaration is not None and declaration.naming.slug is not None
        fields = {
            match.record.record_id: match.fields
            for occurrence in occurrences
            for record in occurrence.source_records
            for match in support.bind(record)
        }
        flags, _conflicts = reconcile_source_fields(
            occurrences, support=tuple(fields.values())
        )
        formed = form_native_variable(
            occurrences,
            register=register,
            variants=variants,
            slug=declaration.naming.slug,
            provider_key=provider_key,
            flags=flags,
            coding=classified.coding,
            representations=representation.cases,
        )
        variables[key] = (
            formed.variable.model_copy(
                update={"deprecated": declaration.naming.deprecated}
            )
            if formed.variable is not None
            else None
        )
        for issue in formed.diagnostics:
            emit(issue)
    annotated = apply_alias_cases(
        evidence, tuple(aliases), variables=variables, variants=variants
    )
    evaluations.extend(annotated.evaluations)
    for issue in annotated.diagnostics:
        emit(issue)
    if {e.case_id for e in evaluations} != {c.case_id for c in cases} or len(
        evaluations
    ) != len(cases):
        raise ValueError(
            "source scope did not evaluate every selected case exactly once"
        )
    return ScopeResolution(
        parents,
        corrected,
        annotated.variables,
        tuple(sorted(evaluations, key=lambda e: e.case_id)),
        tuple(diagnostics),
        counts["error"],
        counts["warning"],
    )
