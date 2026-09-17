"""Resolve catalog dependencies against explicit source-resolution outcomes.

Missing conversion or broken references remain fatal. Only a dependency already
withheld with source evidence can withhold dependent metadata. The same result is
used for strict publication and diagnostic materialization.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedEdition,
    ResolvedRegister,
    ResolvedVariable,
    ResolvedVariant,
    validate_resolved_variables,
)
from reg_meta_build.resolved_metadata import (
    ResolvedMetadata,
    ResolvedStateRef,
    validate_metadata_structure,
)
from reg_meta_build.source_curation import ResolutionDiagnostic

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


type DependencyKey = tuple[str, ...]


@dataclass(frozen=True)
class MissingCatalogDependency:
    key: DependencyKey
    output: str


class CatalogDependencyError(ValueError):
    """All unexplained missing references; no resolved result may be written."""

    def __init__(self, missing: tuple[MissingCatalogDependency, ...]) -> None:
        self.missing = missing
        super().__init__(
            f"{len(missing)} unexplained missing catalog dependencies: "
            + "; ".join(f"{m.key!r} for {m.output}" for m in missing[:10])
        )


class CatalogDependencies:
    """Exact available references and evidenced omissions, never a missing fallback.

    A key starts with its kind, followed by its complete catalog coordinates.
    Callers build the withheld map from resolution accounting, not the baseline DB.
    Descendant lookup may inherit a known withheld parent supplied by the caller.
    """

    def __init__(
        self,
        available: set[DependencyKey],
        withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
    ) -> None:
        if overlap := available & withheld.keys():
            raise ValueError(
                f"catalog dependencies both present and withheld: {sorted(overlap)}"
            )
        for key, causes in withheld.items():
            if not causes or any(not cause.refs for cause in causes):
                raise ValueError(f"withheld dependency lacks source evidence: {key!r}")
        self.available = available
        self.withheld = withheld
        self.diagnostics: list[ResolutionDiagnostic] = []
        self.missing: list[MissingCatalogDependency] = []

    def check(self) -> None:
        if self.missing:
            raise CatalogDependencyError(tuple(self.missing))

    def require(
        self,
        key: DependencyKey,
        *,
        output: str,
        parents: tuple[DependencyKey, ...] = (),
    ) -> bool:
        if key in self.available:
            return True
        causes = next(
            (self.withheld[k] for k in (key, *parents) if k in self.withheld),
            None,
        )
        if causes is None:
            self.missing.append(MissingCatalogDependency(key, output))
            return False
        for cause in causes:
            self.diagnostics.append(
                cause.model_copy(
                    update={
                        "code": "withheld_catalog_dependency",
                        "subject": output,
                        "detail": f"Dependency {key!r} is withheld: {cause.code}: {cause.detail}",
                        "withheld_output": (output,),
                    }
                )
            )
        return False


@dataclass(frozen=True)
class PanelResolution:
    variables: tuple[ResolvedVariable, ...]
    registers: tuple[ResolvedRegister, ...]
    variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...]
    editions: tuple[ResolvedEdition, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


@dataclass(frozen=True)
class MetadataResolution:
    metadata: ResolvedMetadata
    diagnostics: tuple[ResolutionDiagnostic, ...]


def resolve_metadata_dependencies(
    metadata: ResolvedMetadata,
    variables: tuple[ResolvedVariable, ...],
    *,
    registers: tuple[ResolvedRegister, ...],
    variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...],
    classifications: tuple[ResolvedClassification, ...],
    withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
) -> MetadataResolution:
    """Withhold only declared metadata depending on evidenced unresolved facts.

    Tags and sufficiently populated groups retain their independent members.
    Binary relations are indivisible. Missing references without an explicit
    source-resolution cause are fatal even if another endpoint is withheld.
    """
    metadata = ResolvedMetadata.model_validate(metadata)
    validate_metadata_structure(metadata)
    available: set[DependencyKey] = {
        ("register", f"{r.provider}/{r.slug}") for r in registers
    }
    available.update(("variant", f"{r.provider}/{r.slug}", v.slug) for r, v in variants)
    available.update(("classification", c.slug) for c in classifications)
    available.update(
        ("source_column", c.table_name, c.column_name) for c in metadata.source_columns
    )
    for variable in variables:
        fqid = f"{variable.register_ref.provider}/{variable.register_ref.slug}/{variable.slug}"
        available.add(("variable", fqid))
        for item in (*variable.states, *variable.aliases):
            column = item.delivery_column_name
            available.add(("representation", fqid, column))
            available.add(("succession_representation", fqid, column.lower()))
            available.add(
                (
                    "succession_representation",
                    fqid,
                    column.lower(),
                    item.variant.slug,
                )
            )
        for item in variable.states:
            available.add(("variant_states", fqid, item.variant.slug))
            available.add(
                (
                    "state",
                    fqid,
                    item.variant.slug,
                    item.valid_from,
                    item.valid_to,
                    item.delivery_column_name,
                    item.value_set_version_label,
                )
            )
    dependencies = CatalogDependencies(available, withheld)

    def entity(fqid: str, output: str) -> bool:
        kind = "register" if fqid.count("/") == 1 else "variable"
        parents = () if kind == "register" else (("register", fqid.rsplit("/", 1)[0]),)
        return dependencies.require((kind, fqid), output=output, parents=parents)

    def representation(
        fqid: str,
        column: str,
        output: str,
        *,
        variant: str | None = None,
        literal: bool = False,
    ) -> bool:
        key = (
            "representation" if literal else "succession_representation",
            fqid,
            column if literal else column.lower(),
        )
        if variant is not None:
            key = (*key, variant)
        return dependencies.require(
            key,
            output=output,
            parents=(("variable", fqid), ("register", fqid.rsplit("/", 1)[0])),
        )

    def state(ref: ResolvedStateRef, output: str) -> bool:
        return dependencies.require(
            (
                "state",
                ref.variable,
                ref.variant,
                ref.valid_from,
                ref.valid_to,
                ref.delivery_column_name,
                ref.value_set_version_label,
            ),
            output=output,
            parents=(
                ("variant_states", ref.variable, ref.variant),
                ("variable", ref.variable),
                ("register", ref.variable.rsplit("/", 1)[0]),
            ),
        )

    def withheld_group(output: str, start: int) -> None:
        causes = dependencies.diagnostics[start:]
        dependencies.diagnostics.append(
            ResolutionDiagnostic(
                code="withheld_catalog_dependency",
                severity="error"
                if any(c.severity == "error" for c in causes)
                else "warning",
                subject=output,
                detail="The group lacks its parent or at least two supported members.",
                refs=tuple(
                    dict.fromkeys(ref for issue in causes for ref in issue.refs)
                ),
                withheld_output=(output,),
            )
        )

    groups = []
    for group in metadata.variable_groups:
        output = f"variable_group:{group.register_ref}/{group.key}"
        start = len(dependencies.diagnostics)
        parent = entity(group.register_ref, output)
        members = []
        for member in group.members:
            member_output = (
                f"{output}:member:{member.variable}:{member.delivery_column_name or ''}"
            )
            supported = (
                entity(member.variable, member_output)
                if member.delivery_column_name is None
                else representation(
                    member.variable,
                    member.delivery_column_name,
                    member_output,
                    literal=True,
                )
            )
            if supported:
                members.append(member)
        if parent and len(members) >= 2:
            groups.append(group.model_copy(update={"members": tuple(members)}))
        else:
            withheld_group(output, start)
    classification_groups = []
    for group in metadata.classification_groups:
        output = f"classification_group:{group.key}"
        start = len(dependencies.diagnostics)
        members = tuple(
            member
            for member in group.members
            if dependencies.require(
                ("classification", member.classification),
                output=f"{output}:member:{member.classification}",
            )
        )
        if len(members) >= 2:
            classification_groups.append(group.model_copy(update={"members": members}))
        else:
            withheld_group(output, start)
    tags = tuple(
        tag.model_copy(
            update={
                "members": tuple(
                    member
                    for member in tag.members
                    if entity(member.target, f"tag:{tag.slug}:member:{member.target}")
                )
            }
        )
        for tag in metadata.tags
    )

    def selected[T](
        items: tuple[T, ...], field: str, references: Callable[[T, str], list[bool]]
    ) -> tuple[T, ...]:
        result = []
        for index, item in enumerate(items):
            checks = references(item, f"{field}:{index}")
            if all(checks):
                result.append(item)
        return tuple(result)

    historical = {h.target: h for h in metadata.historical_predecessors}
    for target, declaration in historical.items():
        if (declaration.kind, target) in available:
            raise ValueError(
                f"obsolete historical predecessor declaration now names a live entity: {target}"
            )
        if (declaration.kind, target) in withheld:
            raise ValueError(
                f"historical predecessor is a withheld selected entity: {target}"
            )
    successions = selected(
        metadata.successions,
        "successions",
        lambda e, o: [
            True if e.predecessor in historical else entity(e.predecessor, o),
            entity(e.successor, o),
        ],
    )
    updates = {
        "variable_groups": tuple(groups),
        "classification_groups": tuple(classification_groups),
        "tags": tags,
        "variable_same_as": selected(
            metadata.variable_same_as,
            "variable_same_as",
            lambda e, o: [entity(e.a, o), entity(e.b, o)],
        ),
        "classification_same_as": selected(
            metadata.classification_same_as,
            "classification_same_as",
            lambda e, o: [
                dependencies.require(("classification", end.classification), output=o)
                for end in (e.a, e.b)
            ],
        ),
        "successions": successions,
        "historical_predecessors": tuple(
            h
            for h in metadata.historical_predecessors
            if any(e.predecessor == h.target for e in successions)
        ),
        "variant_successions": selected(
            metadata.variant_successions,
            "variant_successions",
            lambda e, o: [
                dependencies.require(
                    ("variant", end.register_ref, end.variant),
                    output=o,
                    parents=(("register", end.register_ref),),
                )
                for end in (e.predecessor, e.successor)
            ],
        ),
        "representation_successions": selected(
            metadata.representation_successions,
            "representation_successions",
            lambda e, o: [
                representation(
                    end.variable, end.delivery_column_name, o, variant=e.variant
                )
                for end in (e.predecessor, e.successor)
            ],
        ),
        "classification_derivations": selected(
            metadata.classification_derivations,
            "classification_derivations",
            lambda e, o: [
                dependencies.require(("classification", end), output=o)
                for end in (e.derived, e.source)
            ],
        ),
        "state_lineage": selected(
            metadata.state_lineage,
            "state_lineage",
            lambda e, o: [state(e.consumer, o), state(e.source, o)],
        ),
        "lineage_warnings": selected(
            metadata.lineage_warnings,
            "lineage_warnings",
            lambda e, o: [state(e.consumer, o)],
        ),
        "source_join_keys": selected(
            metadata.source_join_keys,
            "source_join_keys",
            lambda e, o: [
                dependencies.require(
                    ("source_column", e.table_name, e.column_name), output=o
                )
            ],
        ),
    }
    dependencies.check()
    return MetadataResolution(
        metadata.model_copy(update=updates), tuple(dependencies.diagnostics)
    )


def resolve_panel_dependencies(
    variables: tuple[ResolvedVariable, ...],
    *,
    registers: tuple[ResolvedRegister, ...] = (),
    variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...] = (),
    editions: tuple[ResolvedEdition, ...] = (),
    withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
) -> PanelResolution:
    """Keep independent parent facts when an exact panel axis is unsupported.

    A composite key is one assertion: losing a member withholds the complete key,
    not a shorter invented key. The other axis, grain, names and prose survive.
    States, aliases, editions and standalone parents share the rewritten variant.
    """
    variables, by_register, by_variant = validate_resolved_variables(variables)

    def register_parent(register: ResolvedRegister) -> None:
        key = register.provider, register.slug
        if key in by_register and by_register[key] != register:
            raise ValueError(f"inconsistent resolved parent register: {key!r}")
        by_register[key] = register

    def variant_parent(register: ResolvedRegister, variant: ResolvedVariant) -> None:
        register_parent(register)
        key = register.provider, register.slug, variant.slug
        if key in by_variant and by_variant[key] != variant:
            raise ValueError(f"inconsistent resolved parent variant: {key!r}")
        by_variant[key] = variant

    for register in registers:
        register_parent(register)
    for register, variant in variants:
        variant_parent(register, variant)
    for edition in editions:
        variant_parent(edition.register_ref, edition.variant)

    available: set[DependencyKey] = {
        (
            "variant_states",
            f"{v.register_ref.provider}/{v.register_ref.slug}/{v.slug}",
            s.variant.slug,
        )
        for v in variables
        for s in v.states
    }
    available.update(
        ("variable", f"{v.register_ref.provider}/{v.register_ref.slug}/{v.slug}")
        for v in variables
    )
    dependencies = CatalogDependencies(available, withheld)
    for key, variant in by_variant.items():
        register = "/".join(key[:2])
        updates = {}
        for field in ("panel_entity_key", "panel_time_key"):
            value = getattr(variant, field)
            if value is None or (field == "panel_time_key" and value == "period"):
                continue
            members = value if isinstance(value, tuple) else (value,)
            if not members:
                raise ValueError("panel keys cannot be empty tuples")
            supported = [
                dependencies.require(
                    ("variant_states", f"{register}/{slug}", variant.slug),
                    parents=(("variable", f"{register}/{slug}"),),
                    output=f"{register}/{variant.slug}:{field}",
                )
                for slug in members
            ]
            if not all(supported):
                updates[field] = None
        if updates:
            by_variant[key] = variant.model_copy(update=updates)

    dependencies.check()

    def resolved_variant(
        register: ResolvedRegister, variant: ResolvedVariant
    ) -> ResolvedVariant:
        return by_variant[register.provider, register.slug, variant.slug]

    return PanelResolution(
        variables=tuple(
            variable.model_copy(
                update={
                    "states": tuple(
                        s.model_copy(
                            update={
                                "variant": resolved_variant(
                                    variable.register_ref, s.variant
                                )
                            }
                        )
                        for s in variable.states
                    ),
                    "aliases": tuple(
                        a.model_copy(
                            update={
                                "variant": resolved_variant(
                                    variable.register_ref, a.variant
                                )
                            }
                        )
                        for a in variable.aliases
                    ),
                }
            )
            for variable in variables
        ),
        registers=tuple(by_register[key] for key in sorted(by_register)),
        variants=tuple(
            (by_register[key[:2]], by_variant[key]) for key in sorted(by_variant)
        ),
        editions=tuple(
            e.model_copy(
                update={"variant": resolved_variant(e.register_ref, e.variant)}
            )
            for e in editions
        ),
        diagnostics=tuple(dependencies.diagnostics),
    )
