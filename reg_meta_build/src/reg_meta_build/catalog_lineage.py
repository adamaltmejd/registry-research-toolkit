"""Resolve source attribution and lineage before the catalog writer runs."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.catalog_dependencies import DEFERRED_REFERENCE, CatalogDependencies
from reg_meta_build.resolved_metadata import (
    ResolvedLineageWarning,
    ResolvedStateLineage,
    ResolvedStateRef,
)
from reg_meta_build.source_curation import ResolutionDiagnostic

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping

    from reg_meta_build.catalog_dependencies import DependencyKey
    from reg_meta_build.resolved_catalog import (
        ResolvedRegister,
        ResolvedState,
        ResolvedVariable,
        ResolvedVariant,
    )
    from reg_meta_build.resolved_metadata import ResolvedMetadata
    from reg_meta_build.source_curation import SourceRecordRef


@dataclass(frozen=True)
class LineageResolution:
    variables: tuple[ResolvedVariable, ...]
    metadata: ResolvedMetadata
    diagnostics: tuple[ResolutionDiagnostic, ...]
    skipped: int


def resolve_catalog_lineage(
    variables: tuple[ResolvedVariable, ...],
    *,
    registers: tuple[ResolvedRegister, ...],
    variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...],
    defaults: Mapping[str, str],
    metadata: ResolvedMetadata,
    evidence: Mapping[str, tuple[SourceRecordRef, ...]],
    withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
    unselected: Collection[DependencyKey] = (),
    unselected_names: Mapping[str, Collection[str]] | None = None,
    source_labels: Mapping[str, Collection[str]] | None = None,
    slice_registers: Collection[str] | None = None,
) -> LineageResolution:
    """Match literal source names and follow accepted variable identity edges.

    Equal slugs in different registers do not establish variable identity. The
    accepted same-as graph supplies that identity. A single source variant
    overlapping the consumer state needs no choice; an explicit source variant
    label selects that variant before dates are intersected. Unqualified claims
    with multiple overlapping variants require an explicit default.
    Unknown external source labels remain literal labels, not guessed registers.
    An attributed source with no supported source state withholds the edge as a
    warning; ambiguity is an error. A register-scoped build skips a lineage
    default outside its `slice_registers`, defers what its `unselected` scopes
    declare and matches their registers' observed names (`unselected_names`): a
    label naming exactly one of them is deferred, and one naming several known
    registers stays ambiguous.
    """
    if metadata.state_lineage or metadata.lineage_warnings:
        raise ValueError("lineage must be resolved once from current catalog states")
    unselected_names = unselected_names or {}
    source_labels = source_labels or {}
    by_register = {f"{r.provider}/{r.slug}": r for r in registers}
    names, abbreviations = defaultdict(set), defaultdict(set)
    for fqid, labels in {
        **unselected_names,
        **{fqid: (r.name,) for fqid, r in by_register.items()},
    }.items():
        provider = fqid.split("/", 1)[0]
        for name in labels:
            names[provider, name.casefold()].add(fqid)
            if match := re.search(r"\(([^)]+)\)", name):
                abbreviations[provider, match[1].strip().casefold()].add(fqid)
        for label in source_labels.get(fqid, ()):
            names[provider, label.casefold()].add(fqid)
    available: set[DependencyKey] = {("register", fqid) for fqid in by_register}
    available.update(("variant", f"{r.provider}/{r.slug}", v.slug) for r, v in variants)
    variant_names = defaultdict(set)
    for register, variant in variants:
        register_fqid = f"{register.provider}/{register.slug}"
        variant_names[register_fqid, variant.name.casefold()].add(variant.slug)
    dependencies = CatalogDependencies(available, withheld, unselected, slice_registers)
    usable_defaults = {}
    for register, variant in sorted(defaults.items()):
        with dependencies.entry():
            usable = dependencies.require(
                ("variant", register, variant),
                output=f"lineage_default:{register}",
                parents=(("register", register),),
            )
        if usable:
            usable_defaults[register] = variant
    dependencies.check()
    diagnostics = list(dependencies.diagnostics)
    by_fqid = {
        f"{v.register_ref.provider}/{v.register_ref.slug}/{v.slug}": v
        for v in variables
    }
    neighbours = defaultdict(set)
    for edge in metadata.variable_same_as:
        if edge.a not in by_fqid or edge.b not in by_fqid:
            raise ValueError("resolve lineage after checking same-as dependencies")
        neighbours[edge.a].add(edge.b)
        neighbours[edge.b].add(edge.a)
    components = {}
    for start in sorted(neighbours):
        if start in components:
            continue
        connected, pending = set(), [start]
        while pending:
            key = pending.pop()
            if key not in connected:
                connected.add(key)
                pending.extend(neighbours[key] - connected)
        for key in connected:
            components[key] = connected
    edges, warnings, result = set(), [], []

    def state_ref(fqid: str, state: ResolvedState) -> ResolvedStateRef:
        return ResolvedStateRef(
            variable=fqid,
            variant=state.variant.slug,
            delivery_column_name=state.delivery_column_name,
            value_set_version_label=state.value_set_version_label,
            period_scope=state.period_scope,
            valid_from=state.valid_from,
            valid_to=state.valid_to,
        )

    for fqid, variable in sorted(by_fqid.items()):
        matches = {}

        for text in sorted(
            text
            for text in {
                variable.source_register_text,
                *(s.source_register_text for s in variable.states),
            }
            if text
        ):
            provider = variable.register_ref.provider
            candidates = set(names.get((provider, text.casefold()), ()))
            if len(candidates) != 1:
                prefix = text.split(" : ", 1)[0].strip().casefold()
                prefix_candidates = set(names.get((provider, prefix), ()))
                if len(prefix_candidates) == 1 or not candidates:
                    candidates = prefix_candidates
                if not candidates and (match := re.search(r"\(([^)]+)\)", text)):
                    token = match[1].strip().casefold()
                    candidates.update(abbreviations.get((provider, token), ()))
                    candidates.update(names.get((provider, token), ()))
            only = next(iter(candidates)) if len(candidates) == 1 else None
            matches[text] = by_register.get(only) if only else None
            if ("register", only) in unselected:
                diagnostics.append(
                    ResolutionDiagnostic(
                        code=DEFERRED_REFERENCE,
                        severity="warning",
                        subject=fqid,
                        detail=f"Source label {text!r} names the unselected register {only}; the complete build attributes it.",
                        refs=evidence[fqid],
                        fields=("source_register_text",),
                        withheld_output=(fqid + ":source_register",),
                    )
                )
            if len(candidates) > 1:
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="ambiguous_source_register",
                        severity="error",
                        subject=fqid,
                        detail=f"Source label {text!r} matches multiple catalog registers.",
                        refs=evidence[fqid],
                        fields=("source_register_text",),
                        withheld_output=(fqid + ":source_register",),
                    )
                )
        attributed = matches.get(variable.source_register_text)
        label = variable.source_register_text
        if attributed is not None:
            match = re.search(r"\(([^)]+)\)", attributed.name)
            label = match[1].strip() if match else attributed.name
        result.append(
            variable.model_copy(
                update={"source_register": attributed, "source_label": label}
            )
        )
        for state in variable.states:
            source_text = state.source_register_text
            origin = matches.get(source_text)
            if source_text is None or origin is None or origin == variable.register_ref:
                continue
            source_fqid = f"{origin.provider}/{origin.slug}"
            identities = sorted(
                key
                for key in components.get(fqid, ())
                if by_fqid[key].register_ref == origin
            )
            _, separator, source_variant_label = source_text.partition(" : ")
            source_variant = usable_defaults.get(source_fqid)
            unresolved_variant = None
            ambiguous_variant = False
            if separator:
                named_variants = variant_names.get(
                    (source_fqid, source_variant_label.strip().casefold()), set()
                )
                source_variant = (
                    next(iter(named_variants)) if len(named_variants) == 1 else None
                )
                if source_variant is None:
                    ambiguous_variant = bool(identities) and len(named_variants) > 1
                    unresolved_variant = (
                        f"Explicit source variant {source_variant_label!r} "
                        + (
                            "matches multiple admitted source variants"
                            if len(named_variants) > 1
                            else "matches no admitted source variant"
                        )
                        + "; no default or other variant has been substituted."
                    )
            source_states = [
                (key, item)
                for key in identities
                for item in by_fqid[key].states
                if unresolved_variant is None
                and (source_variant is None or item.variant.slug == source_variant)
            ]
            if source_states and state.period_scope != "year_independent":
                assert state.valid_from is not None and state.valid_to is not None
                source_states = [
                    (key, item)
                    for key, item in source_states
                    if item.period_scope == "year_independent"
                    or (
                        item.valid_from is not None
                        and item.valid_to is not None
                        and item.valid_from <= state.valid_to
                        and item.valid_to >= state.valid_from
                    )
                ]
                if not source_states:
                    continue
            kinds = {item.variant.slug for _, item in source_states}
            if unresolved_variant is not None or not source_states or len(kinds) > 1:
                kind = (
                    "no_source_state"
                    if not source_states and not ambiguous_variant
                    else "ambiguous_source_variant"
                )
                if unresolved_variant is not None:
                    detail = (
                        "No source variable identity is established. "
                        if not identities
                        else ""
                    ) + unresolved_variant
                elif not source_states and separator:
                    detail = (
                        f"The explicitly named source variant {source_variant_label!r} "
                        "has no source state supported by accepted variable identity."
                    )
                else:
                    detail = (
                        "No source state is supported by accepted variable identity and variant declarations."
                        if not source_states
                        else "Multiple source variants need an explicit lineage choice."
                    )
                warnings.append(
                    ResolvedLineageWarning(
                        consumer=state_ref(fqid, state), kind=kind, message=detail
                    )
                )
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="unresolved_lineage_" + kind,
                        severity="warning" if kind == "no_source_state" else "error",
                        subject=fqid,
                        detail=detail,
                        refs=evidence[fqid],
                        valid_from=state.valid_from,
                        valid_to=state.valid_to,
                        withheld_output=(fqid + ":lineage",),
                    )
                )
                continue
            if state.period_scope == "year_independent" or any(
                item.period_scope == "year_independent" for _, item in source_states
            ):
                # simplify: lineage edges remain dated; add an independent edge
                # contract when positively linked nonannual endpoints require it.
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="unsupported_lineage_scope",
                        severity="error",
                        subject=fqid,
                        detail="A source attribution involving year-independent delivery cannot establish a dated lineage intersection.",
                        refs=evidence[fqid],
                        withheld_output=(fqid + ":lineage",),
                    )
                )
                continue
            for key, item in source_states:
                assert state.valid_from is not None and state.valid_to is not None
                assert item.valid_from is not None and item.valid_to is not None
                start, end = (
                    max(state.valid_from, item.valid_from),
                    min(state.valid_to, item.valid_to),
                )
                if start <= end:
                    edges.add(
                        ResolvedStateLineage(
                            consumer=state_ref(fqid, state),
                            source=state_ref(key, item),
                            valid_from=start,
                            valid_to=end,
                        )
                    )
    return LineageResolution(
        tuple(result),
        metadata.model_copy(
            update={
                "state_lineage": tuple(
                    sorted(edges, key=lambda e: e.model_dump_json())
                ),
                "lineage_warnings": tuple(warnings),
            }
        ),
        tuple(diagnostics),
        dependencies.skipped,
    )
