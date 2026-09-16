"""Checked catalog annotations composed in the common resolution stage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.resolved_catalog import ResolvedAlias
from reg_meta_build.source_curation import (
    CurationCase,
    ResolutionDiagnostic,
    SearchAliasDecision,
    evaluate_cases,
)
from reg_meta_build.source_effects import _require_checked

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.resolved_catalog import ResolvedVariable, ResolvedVariant
    from reg_meta_build.source_coordinates import NativeKey
    from reg_meta_build.source_curation import CaseEvaluation
    from reg_meta_build.source_records import SourceRecord


@dataclass(frozen=True)
class AnnotationResolution:
    variables: dict[NativeKey, ResolvedVariable | None]
    evaluations: tuple[CaseEvaluation, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


def apply_search_aliases(
    records: Iterable[SourceRecord],
    cases: tuple[CurationCase, ...],
    *,
    variables: Mapping[NativeKey, ResolvedVariable | None],
    variants: Mapping[NativeKey, ResolvedVariant | None],
) -> AnnotationResolution:
    """Preserve accepted search aliases without inventing delivery windows.

    Missing identity mappings are implementation errors. Explicit None mappings
    represent dependencies already withheld by source resolution. A search alias
    attaches only to its finite, supported variants; it never adds a state.
    """
    ordered = tuple(sorted(cases, key=lambda case: case.case_id))
    if len({case.case_id for case in ordered}) != len(ordered):
        raise ValueError("search alias case IDs must be unique")
    for case in ordered:
        if not isinstance(case.decision, SearchAliasDecision):
            raise TypeError("search alias application requires search alias decisions")
        guarded = {ref for guard in case.peer_guards for ref in guard.expected_members}
        for target in case.targets:
            _require_checked(target, ("column_name",))
            if target.ref not in guarded:
                raise ValueError("search aliases require guarded original membership")
    evaluations = evaluate_cases(ordered, records)
    resolved = dict(variables)
    diagnostics = []
    for case, evaluation in zip(ordered, evaluations, strict=True):
        decision = case.decision
        assert isinstance(decision, SearchAliasDecision)
        for issue in evaluation.issues:
            diagnostics.append(
                ResolutionDiagnostic(
                    code=issue.code,
                    severity="error",
                    case_id=case.case_id,
                    subject=issue.subject,
                    detail=issue.detail,
                    applicability_issue=issue,
                    refs=tuple(
                        expected.ref for expected in (*case.targets, *case.support)
                    ),
                    withheld_output=("search_alias",),
                )
            )
        if evaluation.status == "stale":
            continue
        if decision.variable_key not in resolved or any(
            key not in variants for key in decision.variant_keys
        ):
            raise ValueError(
                "search alias has an unconverted variable or variant identity"
            )
        variable = resolved[decision.variable_key]
        aliases = (
            {
                (alias.variant.slug, alias.delivery_column_name): alias
                for alias in variable.aliases
            }
            if variable is not None
            else {}
        )
        supported_variants = (
            {state.variant for state in variable.states}
            if variable is not None
            else set()
        )
        for key in decision.variant_keys:
            variant = variants[key]
            if (
                variable is not None
                and variant is not None
                and any(
                    state.variant.slug == variant.slug and state.variant != variant
                    for state in variable.states
                )
            ):
                raise ValueError("search alias and state variant definitions disagree")
            if variable is None or variant not in supported_variants:
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="withheld_alias_dependency",
                        severity="error",
                        case_id=case.case_id,
                        subject=repr(decision.variable_key),
                        detail=f"Search alias {decision.column!r} has no supported variable state in variant {key!r}.",
                        refs=tuple(target.ref for target in case.targets),
                        fields=("alias",),
                        withheld_output=("search_alias",),
                    )
                )
                continue
            assert variant is not None
            aliases.setdefault(
                (variant.slug, decision.column),
                ResolvedAlias(variant=variant, delivery_column_name=decision.column),
            )
        if variable is not None:
            resolved[decision.variable_key] = variable.model_copy(
                update={"aliases": tuple(aliases[key] for key in sorted(aliases))}
            )
    return AnnotationResolution(resolved, evaluations, tuple(diagnostics))
