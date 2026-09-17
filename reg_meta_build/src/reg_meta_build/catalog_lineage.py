"""Resolve source attribution and lineage before the catalog writer runs."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.catalog_dependencies import CatalogDependencies
from reg_meta_build.resolved_metadata import (
    ResolvedLineageWarning,
    ResolvedStateLineage,
    ResolvedStateRef,
)
from reg_meta_build.source_curation import ResolutionDiagnostic

if TYPE_CHECKING:
    from collections.abc import Mapping

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


def resolve_catalog_lineage(
    variables: tuple[ResolvedVariable, ...],
    *,
    registers: tuple[ResolvedRegister, ...],
    variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...],
    defaults: Mapping[str, str],
    metadata: ResolvedMetadata,
    evidence: Mapping[str, tuple[SourceRecordRef, ...]],
    withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
) -> LineageResolution:
    """Match literal source names and follow accepted variable identity edges.

    Equal slugs in different registers do not establish variable identity. The
    accepted same-as graph supplies that identity. A single observed source
    variant needs no choice; multiple variants require an explicit default.
    Unknown external source labels remain literal labels, not guessed registers.
    """
    if metadata.state_lineage or metadata.lineage_warnings:
        raise ValueError("lineage must be resolved once from current catalog states")
    names, abbreviations = defaultdict(set), defaultdict(set)
    for register in registers:
        names[register.provider, register.name.casefold()].add(register)
        if match := re.search(r"\(([^)]+)\)", register.name):
            abbreviations[register.provider, match[1].strip().casefold()].add(register)
    available: set[DependencyKey] = {
        ("register", f"{r.provider}/{r.slug}") for r in registers
    }
    available.update(("variant", f"{r.provider}/{r.slug}", v.slug) for r, v in variants)
    dependencies = CatalogDependencies(available, withheld)
    usable_defaults = {}
    for register, variant in sorted(defaults.items()):
        if dependencies.require(
            ("variant", register, variant),
            output=f"lineage_default:{register}",
            parents=(("register", register),),
        ):
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
            prefix = text.split(" : ", 1)[0].strip().casefold()
            candidates.update(names.get((provider, prefix), ()))
            if match := re.search(r"\(([^)]+)\)", text):
                token = match[1].strip().casefold()
                candidates.update(abbreviations.get((provider, token), ()))
                candidates.update(names.get((provider, token), ()))
            matches[text] = next(iter(candidates)) if len(candidates) == 1 else None
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
            origin = matches.get(state.source_register_text)
            if origin is None or origin == variable.register_ref:
                continue
            source_fqid = f"{origin.provider}/{origin.slug}"
            identities = sorted(
                key
                for key in components.get(fqid, ())
                if by_fqid[key].register_ref == origin
            )
            source_states = [
                (key, item)
                for key in identities
                for item in by_fqid[key].states
                if source_fqid not in usable_defaults
                or item.variant.slug == usable_defaults[source_fqid]
            ]
            kinds = {item.variant.slug for _, item in source_states}
            if not source_states or len(kinds) > 1:
                kind = (
                    "no_source_state"
                    if not source_states
                    else "ambiguous_source_variant"
                )
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
                        severity="error",
                        subject=fqid,
                        detail=detail,
                        refs=evidence[fqid],
                        valid_from=state.valid_from,
                        valid_to=state.valid_to,
                        withheld_output=(fqid + ":lineage",),
                    )
                )
                continue
            for key, item in source_states:
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
    )
