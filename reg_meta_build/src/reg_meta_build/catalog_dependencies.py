"""Resolve catalog dependencies against explicit source-resolution outcomes.

Missing conversion or broken references remain fatal. Only a dependency already
withheld with source evidence can withhold dependent metadata. The same result is
used for strict publication and diagnostic materialization.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from reg_meta_build._components import DisjointSet
from reg_meta_build.concept_groups import (
    _MONTH_LABELS,
    classification_succession_edges,
    month_group_candidates,
)
from reg_meta_build.relations import (
    _REPLACED_BY_NOTE_VINTAGE_LIFT,
    variable_vintage_succession_edges,
)
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationSuccession,
    ResolvedEdition,
    ResolvedRegister,
    ResolvedVariable,
    ResolvedVariant,
    _prepare_classification_succession,
    validate_resolved_variables,
)
from reg_meta_build.resolved_metadata import (
    ResolvedGroupAxis,
    ResolvedGroupFacet,
    ResolvedGroupVariable,
    ResolvedMetadata,
    ResolvedStateRef,
    ResolvedSuccession,
    ResolvedVariableGroup,
    validate_metadata_structure,
)
from reg_meta_build.source_curation import ResolutionDiagnostic

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from reg_meta_build.concept_groups import CodeLabelPair
    from reg_meta_build.source_curation import SourceRecordRef


type DependencyKey = tuple[str, ...]


def resolve_classification_successions(
    classifications: tuple[ResolvedClassification, ...],
    declared: tuple[ResolvedClassificationSuccession, ...] = (),
) -> tuple[ResolvedClassificationSuccession, ...]:
    """Combine existing automatic edition chains and explicit accepted edges.

    Check the whole graph before materialization. An explicit edge cannot hide a
    duplicate, missing endpoint or cycle by being checked in a separate pass.
    """
    if len({c.slug for c in classifications}) != len(classifications):
        raise ValueError("duplicate classification identity in succession resolution")
    derived = tuple(
        ResolvedClassificationSuccession(
            predecessor=a,
            successor=b,
            effective_year=year,
            note="derived:vintage_chain",
        )
        for a, b, year in classification_succession_edges(
            (c.slug, c.name) for c in classifications
        )
    )
    combined = tuple(
        sorted((*derived, *declared), key=lambda e: (e.predecessor, e.successor))
    )
    _prepare_classification_succession(classifications, combined)
    return combined


def resolve_variable_successions(
    metadata: ResolvedMetadata,
    variables: tuple[ResolvedVariable, ...],
    classifications: tuple[ResolvedClassificationSuccession, ...],
) -> ResolvedMetadata:
    """Add existing classification-derived variable links before writing the DB.

    Explicit source/curated edges retain their richer attribution. Validate the
    combined graph so a derived edge cannot close a cycle through those edges.
    """
    edges = variable_vintage_succession_edges(
        (
            (
                f"{v.register_ref.provider}/{v.register_ref.slug}",
                v.name,
                v.slug,
                state.classification,
            )
            for v in variables
            for state in v.states
            if state.classification is not None
        ),
        ((e.predecessor, e.successor, e.effective_year) for e in classifications),
    )
    existing = {(e.predecessor, e.successor) for e in metadata.successions}
    combined = metadata.model_copy(
        update={
            "successions": metadata.successions
            + tuple(
                ResolvedSuccession(
                    predecessor=a,
                    successor=b,
                    effective_year=year,
                    note=_REPLACED_BY_NOTE_VINTAGE_LIFT,
                )
                for a, b, year in edges
                if (a, b) not in existing
            )
        }
    )
    validate_metadata_structure(combined)
    return combined


def variable_dependency_keys(variable: ResolvedVariable) -> set[DependencyKey]:
    """Exact materialized references, shared by resolution and dependency checks."""
    fqid = (
        f"{variable.register_ref.provider}/{variable.register_ref.slug}/{variable.slug}"
    )
    keys: set[DependencyKey] = {("variable", fqid)}
    for item in (*variable.states, *variable.aliases):
        column = item.delivery_column_name
        keys.update(
            (
                ("representation", fqid, column),
                ("succession_representation", fqid, column.lower()),
                ("succession_representation", fqid, column.lower(), item.variant.slug),
            )
        )
    for item in variable.states:
        keys.add(("variant_states", fqid, item.variant.slug))
        keys.add(
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
    return keys


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


@dataclass(frozen=True)
class GroupEdgeDisposition:
    source: Literal["code_label", "same_definition"]
    a: str
    b: str
    status: Literal["grouped", "claimed_by_curated", "withheld"]
    group_key: str | None = None


@dataclass(frozen=True)
class GroupEdgeResolution:
    groups: tuple[ResolvedVariableGroup, ...]
    dispositions: tuple[GroupEdgeDisposition, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


@dataclass(frozen=True)
class MonthGroupResolution:
    groups: tuple[ResolvedVariableGroup, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]


def resolve_month_groups(
    variables: tuple[ResolvedVariable, ...],
    *,
    edge_groups: tuple[ResolvedVariableGroup, ...],
    curated_groups: tuple[ResolvedVariableGroup, ...],
    evidence: Mapping[str, tuple[SourceRecordRef, ...]],
) -> MonthGroupResolution:
    """Derive existing month families before writing, retaining collision reports.

    Already grouped edge members do not participate. Curated keys are reserved;
    conflicting membership in a differently named curated group is still fatal,
    as in the original rule. Missing variables are accounted for by source
    resolution, not invented here to meet the three-month threshold.
    """
    by_fqid = {
        f"{v.register_ref.provider}/{v.register_ref.slug}/{v.slug}": v
        for v in variables
    }
    if len(by_fqid) != len(variables):
        raise ValueError("duplicate variable identity in month grouping")
    claimed = {m.variable for g in edge_groups for m in g.members}
    if missing := claimed - by_fqid.keys():
        raise ValueError(
            f"resolved edge groups have missing variables: {sorted(missing)}"
        )
    existing = (*edge_groups, *curated_groups)
    validate_metadata_structure(ResolvedMetadata(variable_groups=existing))
    candidates = month_group_candidates(
        (
            (fqid.rsplit("/", 1)[0], v.slug, v.name)
            for fqid, v in by_fqid.items()
            if fqid not in claimed
        ),
        reserved_keys=frozenset((g.register_ref, g.key) for g in existing),
    )
    groups, diagnostics = [], []
    for candidate in candidates:
        fqids = tuple(f"{candidate.register}/{slug}" for _, slug in candidate.members)
        if any(not evidence.get(fqid) for fqid in fqids):
            raise ValueError(f"month-group candidate lacks source evidence: {fqids!r}")
        if candidate.issue is not None:
            output = f"variable_group:{candidate.register}/{candidate.key}"
            diagnostics.append(
                ResolutionDiagnostic(
                    code="month_group_" + candidate.issue,
                    severity="warning",
                    subject=output,
                    detail=(
                        "Distinct qualifying month stems collapse to the same group key."
                        if candidate.issue == "stem_collision"
                        else "The month stem collides with an existing or declared group key."
                    ),
                    refs=tuple(
                        sorted({r for fqid in fqids for r in evidence[fqid]}, key=repr)
                    ),
                    fields=("group",),
                    withheld_output=(output,),
                )
            )
            continue
        assert candidate.label is not None
        groups.append(
            ResolvedVariableGroup(
                register=candidate.register,
                key=candidate.key,
                label=candidate.label,
                source="token",
                axes=(ResolvedGroupAxis(axis="month", ordinal=0, label="månad"),),
                members=tuple(
                    ResolvedGroupVariable(
                        variable=f"{candidate.register}/{slug}",
                        facets=(
                            ResolvedGroupFacet(
                                axis="month",
                                value=f"{month:02d}",
                                label=_MONTH_LABELS[month],
                            ),
                        ),
                    )
                    for month, slug in candidate.members
                ),
            )
        )
    validate_metadata_structure(ResolvedMetadata(variable_groups=(*existing, *groups)))
    return MonthGroupResolution(tuple(groups), tuple(diagnostics))


def resolve_variable_edge_groups(
    pairs: tuple[CodeLabelPair, ...],
    variables: tuple[ResolvedVariable, ...],
    *,
    foldable_sibling_pairs: tuple[tuple[str, str], ...] = (),
    curated_groups: tuple[ResolvedVariableGroup, ...],
    evidence: Mapping[str, tuple[SourceRecordRef, ...]],
    withheld: Mapping[DependencyKey, tuple[ResolutionDiagnostic, ...]],
) -> GroupEdgeResolution:
    """Resolve existing code/label and checked same-definition group edges.

    Code/label pairs retain their coding and shared-variant guards. The caller
    supplies already checked foldable sibling pairs, never slug patterns. A common
    native definition alone does not bypass the existing code/label and type-shape
    exclusions; those belong to the source-to-pair derivation.
    Both edge kinds share one component pass; deriving them separately could put
    a variable in two groups. Exclude curated members before connecting components.
    Invalid/unavailable edges retain source evidence; unexplained refs stay fatal.
    """
    by_fqid = {
        f"{v.register_ref.provider}/{v.register_ref.slug}/{v.slug}": v
        for v in variables
    }
    if len(by_fqid) != len(variables):
        raise ValueError("duplicate variable identity in group-edge resolution")
    declared = tuple(
        (
            f"{p.code_provider}/{p.code_register}/{p.code_variable}",
            f"{p.label_provider}/{p.label_register}/{p.label_variable}",
        )
        for p in pairs
    )
    if len(set(declared)) != len(declared) or any(a == b for a, b in declared):
        raise ValueError("code-label declarations must be unique non-self pairs")
    siblings = tuple(tuple(sorted(pair)) for pair in foldable_sibling_pairs)
    if len(set(siblings)) != len(siblings) or any(a == b for a, b in siblings):
        raise ValueError("same-definition declarations must be unique non-self pairs")
    dependencies = CatalogDependencies(
        {("variable", fqid) for fqid in by_fqid}, withheld
    )
    claimed = {m.variable for g in curated_groups for m in g.members}
    components: DisjointSet[str] = DisjointSet()
    dispositions = []
    edges: tuple[tuple[Literal["code_label", "same_definition"], str, str], ...] = (
        *(("code_label", a, b) for a, b in declared),
        *(("same_definition", a, b) for a, b in siblings),
    )
    for source, code, label in edges:
        output = f"{source}_pair:{code}:{label}"
        available = [
            dependencies.require(
                ("variable", fqid),
                output=output,
                parents=(("register", fqid.rsplit("/", 1)[0]),),
            )
            for fqid in (code, label)
        ]
        if not all(available):
            dispositions.append(GroupEdgeDisposition(source, code, label, "withheld"))
            continue
        if any(not evidence.get(fqid) for fqid in (code, label)):
            raise ValueError(f"group-edge declaration lacks source evidence: {output}")
        coded, named = by_fqid[code], by_fqid[label]
        problems = []
        if source == "code_label":
            if not any(s.value_set is not None for s in coded.states):
                problems.append("the code endpoint has no supported value set")
            if any(s.value_set is not None for s in named.states):
                problems.append("the label endpoint owns a value set")
            if not (
                {s.variant.slug for s in coded.states}
                & {s.variant.slug for s in named.states}
            ):
                problems.append("the endpoints have no shared register variant")
        if code.rsplit("/", 1)[0] != label.rsplit("/", 1)[0]:
            problems.append("the endpoints belong to different registers")
        if problems:
            dependencies.diagnostics.append(
                ResolutionDiagnostic(
                    code=f"unresolved_{source}_pair",
                    severity="error",
                    subject=output,
                    detail="; ".join(problems),
                    refs=tuple(dict.fromkeys((*evidence[code], *evidence[label]))),
                    fields=("coding", "identity"),
                    withheld_output=(output,),
                )
            )
            dispositions.append(GroupEdgeDisposition(source, code, label, "withheld"))
        elif code in claimed or label in claimed:
            dispositions.append(
                GroupEdgeDisposition(source, code, label, "claimed_by_curated")
            )
        else:
            components.add(code)
            components.add(label)
            components.union(code, label)
            dispositions.append(GroupEdgeDisposition(source, code, label, "grouped"))
    dependencies.check()
    groups = []
    membership = {}
    reserved = {(g.register_ref, g.key) for g in curated_groups}
    for members in sorted(sorted(c) for c in components.components().values()):
        register, key = members[0].rsplit("/", 1)
        if (register, key) in reserved:
            raise ValueError(
                f"derived edge-group key conflicts with curated group: {register}/{key}"
            )
        groups.append(
            ResolvedVariableGroup(
                register=register,
                key=key,
                label=by_fqid[members[0]].name,
                source="edge",
                members=tuple(ResolvedGroupVariable(variable=m) for m in members),
            )
        )
        membership.update(dict.fromkeys(members, key))
    return GroupEdgeResolution(
        tuple(groups),
        tuple(
            GroupEdgeDisposition(d.source, d.a, d.b, d.status, membership[d.a])
            if d.status == "grouped"
            else d
            for d in dispositions
        ),
        tuple(dependencies.diagnostics),
    )


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
        available.update(variable_dependency_keys(variable))
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
    # All variables may have been withheld for source errors. Independent parent
    # facts still resolve; the writer enforces the strict publication floor.
    variables, by_register, by_variant = validate_resolved_variables(
        variables, allow_empty=True
    )

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

    available = {
        key for variable in variables for key in variable_dependency_keys(variable)
    }
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
