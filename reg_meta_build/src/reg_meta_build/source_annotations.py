"""Checked catalog annotations composed in the common resolution stage."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from typing import TYPE_CHECKING

from reg_meta_build._resolved_common import covers_window
from reg_meta_build.resolved_catalog import ResolvedAlias, ResolvedAliasWindow
from reg_meta_build.source_curation import (
    AliasWindowDecision,
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


def _window_union(
    windows: tuple[ResolvedAliasWindow, ...],
) -> tuple[ResolvedAliasWindow, ...]:
    """Union equally additive declarations, retaining each attribution per slice."""
    cuts = sorted(
        {
            point
            for window in windows
            for point in (
                date.fromisoformat(window.valid_from).toordinal(),
                date.fromisoformat(window.valid_to).toordinal() + 1,
            )
        }
    )
    result = []
    for lo, hi in pairwise(cuts):
        start, end = (
            date.fromordinal(lo).isoformat(),
            date.fromordinal(hi - 1).isoformat(),
        )
        active = [w for w in windows if w.valid_from <= start and w.valid_to >= end]
        if not active:
            continue
        provenance = (
            "\n\n".join(
                sorted({w.provenance for w in active if w.provenance is not None})
            )
            or None
        )
        result.append(
            ResolvedAliasWindow(valid_from=start, valid_to=end, provenance=provenance)
        )
    return tuple(result)


def apply_alias_cases(
    records: Iterable[SourceRecord],
    cases: tuple[CurationCase, ...],
    *,
    variables: Mapping[NativeKey, ResolvedVariable | None],
    variants: Mapping[NativeKey, ResolvedVariant | None],
) -> AnnotationResolution:
    """Attach checked search spellings or finite windows to supported variables.

    Windows require existing ownership and complete state coverage. Checks use
    the original resolved variables: another annotation cannot make one apply.
    Missing conversion mappings are fatal; withheld dependencies remain issues.
    """
    ordered = tuple(sorted(cases, key=lambda case: case.case_id))
    if len({case.case_id for case in ordered}) != len(ordered):
        raise ValueError("search alias case IDs must be unique")
    for case in ordered:
        if not isinstance(case.decision, (SearchAliasDecision, AliasWindowDecision)):
            raise TypeError("alias application requires alias decisions")
        decision = case.decision
        keys = (
            (decision.variant_key,)
            if isinstance(decision, AliasWindowDecision)
            else decision.variant_keys
        )
        if decision.variable_key not in variables or any(
            key not in variants for key in keys
        ):
            raise ValueError("alias has an unconverted variable or variant identity")
        guarded = {ref for guard in case.peer_guards for ref in guard.expected_members}
        for target in (*case.targets, *case.support):
            _require_checked(target, ("column_name",))
            if target.ref not in guarded:
                raise ValueError("search aliases require guarded original membership")
    evaluations = evaluate_cases(ordered, records)
    resolved = dict(variables)
    diagnostics = []
    pending_windows: dict[
        tuple[str, str, str, str], list[tuple[CurationCase, ResolvedVariant]]
    ] = defaultdict(list)
    for case, evaluation in zip(ordered, evaluations, strict=True):
        decision = case.decision
        assert isinstance(decision, (SearchAliasDecision, AliasWindowDecision))
        variant_keys = (
            (decision.variant_key,)
            if isinstance(decision, AliasWindowDecision)
            else decision.variant_keys
        )
        output = (
            "alias_window"
            if isinstance(decision, AliasWindowDecision)
            else "search_alias"
        )
        bounds = (
            {"valid_from": decision.valid_from, "valid_to": decision.valid_to}
            if isinstance(decision, AliasWindowDecision)
            else {}
        )
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
                    withheld_output=(output,),
                    **bounds,
                )
            )
        if evaluation.status == "stale":
            continue
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
        for key in variant_keys:
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
                        withheld_output=(output,),
                        **bounds,
                    )
                )
                continue
            assert variant is not None
            alias_key = variant.slug, decision.column
            original = variables[decision.variable_key]
            assert original is not None
            if isinstance(decision, AliasWindowDecision):
                owned = any(
                    s.variant == variant and s.delivery_column_name == decision.column
                    for s in original.states
                ) or any(
                    a.variant == variant and a.delivery_column_name == decision.column
                    for a in original.aliases
                )
                covered = covers_window(
                    (
                        (s.valid_from, s.valid_to)
                        for s in original.states
                        if s.variant == variant
                    ),
                    decision.valid_from,
                    decision.valid_to,
                )
                other_owners = [
                    other.slug
                    for other_key, other in variables.items()
                    if other_key != decision.variable_key
                    and other is not None
                    and other.register_ref == original.register_ref
                    and (
                        any(
                            s.variant == variant
                            and s.delivery_column_name == decision.column
                            and s.valid_from <= decision.valid_to
                            and s.valid_to >= decision.valid_from
                            for s in other.states
                        )
                        or any(
                            a.variant == variant
                            and a.delivery_column_name == decision.column
                            and any(
                                w.valid_from <= decision.valid_to
                                and w.valid_to >= decision.valid_from
                                for w in a.windows
                            )
                            for a in other.aliases
                        )
                    )
                ]
                if not owned or not covered or other_owners:
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code="unowned_alias_window"
                            if not owned
                            else "unsupported_alias_window"
                            if not covered
                            else "conflicting_alias_window_owner",
                            severity="error",
                            case_id=case.case_id,
                            subject=repr(decision.variable_key),
                            detail="A representation window needs exact existing alias ownership and complete supported state coverage on its variant. "
                            f"Other supported owners in this period: {sorted(other_owners)!r}.",
                            refs=tuple(t.ref for t in (*case.targets, *case.support)),
                            fields=("alias",),
                            valid_from=decision.valid_from,
                            valid_to=decision.valid_to,
                            withheld_output=("alias_window",),
                        )
                    )
                    continue
                register = original.register_ref
                pending_windows[
                    register.provider, register.slug, variant.slug, decision.column
                ].append((case, variant))
            else:
                aliases.setdefault(
                    alias_key,
                    ResolvedAlias(
                        variant=variant, delivery_column_name=decision.column
                    ),
                )
        if variable is not None:
            resolved[decision.variable_key] = variable.model_copy(
                update={"aliases": tuple(aliases[key] for key in sorted(aliases))}
            )
    for pending in pending_windows.values():
        # simplify: these are finite reviewed declarations per exact header;
        # index their events if one header accumulates over a thousand windows.
        for case, variant in pending:
            decision = case.decision
            assert isinstance(decision, AliasWindowDecision)
            lo = date.fromisoformat(decision.valid_from).toordinal()
            hi = date.fromisoformat(decision.valid_to).toordinal() + 1
            cuts = {lo, hi}
            other_cases = []
            for other, _ in pending:
                other_decision = other.decision
                assert isinstance(other_decision, AliasWindowDecision)
                if other_decision.variable_key == decision.variable_key:
                    continue
                start = max(
                    lo, date.fromisoformat(other_decision.valid_from).toordinal()
                )
                end = min(
                    hi, date.fromisoformat(other_decision.valid_to).toordinal() + 1
                )
                if start < end:
                    cuts.update((start, end))
                    other_cases.append((start, end, other))
            windows = []
            for start, end in pairwise(sorted(cuts)):
                lower, upper = (
                    date.fromordinal(start).isoformat(),
                    date.fromordinal(end - 1).isoformat(),
                )
                conflicts = [
                    other for a, b, other in other_cases if a <= start and b >= end
                ]
                if conflicts:
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code="conflicting_alias_window_decisions",
                            severity="error",
                            case_id=case.case_id,
                            subject=repr(decision.variable_key),
                            detail="The same representation is assigned to different variables by cases: "
                            + ", ".join(
                                sorted({case.case_id, *(c.case_id for c in conflicts)})
                            ),
                            refs=tuple(
                                sorted(
                                    {
                                        t.ref
                                        for c in (case, *conflicts)
                                        for t in (*c.targets, *c.support)
                                    },
                                    key=repr,
                                )
                            ),
                            fields=("alias",),
                            valid_from=lower,
                            valid_to=upper,
                            withheld_output=("alias_window",),
                        )
                    )
                else:
                    windows.append(
                        ResolvedAliasWindow(
                            valid_from=lower,
                            valid_to=upper,
                            provenance=f"{case.case_id}: {decision.reason}\n{decision.provenance}",
                        )
                    )
            variable = resolved[decision.variable_key]
            assert variable is not None
            aliases = {
                (a.variant.slug, a.delivery_column_name): a for a in variable.aliases
            }
            alias_key = variant.slug, decision.column
            existing = aliases.get(alias_key)
            if windows:
                aliases[alias_key] = ResolvedAlias(
                    variant=variant,
                    delivery_column_name=decision.column,
                    windows=_window_union(
                        (*(existing.windows if existing else ()), *windows)
                    ),
                )
                resolved[decision.variable_key] = variable.model_copy(
                    update={"aliases": tuple(aliases[key] for key in sorted(aliases))}
                )
    return AnnotationResolution(resolved, evaluations, tuple(diagnostics))
