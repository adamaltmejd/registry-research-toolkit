"""Compile tracked curation beside a pinned, transitional selection.

Families are closed: every stored declaration has one owner, and COMPILED replaces
that owner's declarations in every scope. Later children move their converters here.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter, ValidationError
from reg_meta.fqid import FqidKind, parse as parse_fqid

from ._curation import SentinelCode
from .concept_groups import CodeLabelPair
from .normalization import normalize_token
from .resolved_catalog import ResolvedClassificationSuccession
from .resolved_metadata import (
    ResolvedClassificationDerivation,
    ResolvedClassificationGroup,
    ResolvedGroupAxis,
    ResolvedGroupClassification,
    ResolvedGroupFacet,
    ResolvedGroupVariable,
    ResolvedMetadata,
    ResolvedRepresentationRef,
    ResolvedRepresentationSuccession,
    ResolvedSuccession,
    ResolvedTag,
    ResolvedTagMember,
    ResolvedVariableGroup,
    ResolvedVariableSameAs,
    ResolvedVariantRef,
    ResolvedVariantSuccession,
)
from .source_curation import (
    AcknowledgeDecision,
    CurationCase,
    ResolutionDiagnostic,
    SourceRecordRef,
)

if TYPE_CHECKING:
    from .curation_tree import CurationTree
    from .pipeline import ScopeDeclarations
    from .prepared_catalog import PreparedCatalogSources


@dataclass(frozen=True)
class Family:
    case_prefixes: tuple[str, ...] = ()
    case_ids: tuple[str, ...] = ()
    naming_shapes: tuple[str, ...] = ()
    selection_fields: tuple[str, ...] = ()
    unapplied_datasets: tuple[str, ...] = ()
    scope_fields: tuple[str, ...] = ()


# A shape is the discriminator in NativeNamingTarget.source_key. The fallback is
# explicitly the native/parent family, never an unowned wildcard case prefix.
FAMILIES: dict[str, Family] = {
    "classifications": Family(selection_fields=("classifications",)),
    "classification_successions": Family(
        selection_fields=("classification_successions",)
    ),
    "metadata": Family(selection_fields=("metadata",)),
    "code_label_pairs": Family(selection_fields=("code_label_pairs",)),
    "lineage_defaults": Family(selection_fields=("lineage_defaults",)),
    "identifier_sources": Family(selection_fields=("identifier_sources",)),
    "event_sources": Family(selection_fields=("event_sources",)),
    "acknowledge": Family(case_prefixes=("curation/registers/",)),
    "identity": Family(
        case_prefixes=(
            "accepted-column-partitions:",
            "accepted-sos-routes:",
            "accepted-sos-identity:",
            "existing-source-use:",
        ),
        naming_shapes=("accepted-partition",),
        unapplied_datasets=("fqid_slugs/",),
    ),
    "errata": Family(
        case_prefixes=(
            "scb_errata.toml/column/",
            "scb_errata.toml/delivered/",
            "accepted-errata:",
        ),
        naming_shapes=("declared-column",),
        unapplied_datasets=("curation/scb_errata.toml",),
    ),
    "matrix": Family(
        case_ids=("accepted-cis2014-answers", "accepted-cis2016-answers"),
        naming_shapes=("accepted-matrix",),
        unapplied_datasets=("curation/cis",),
    ),
    "representation": Family(
        case_prefixes=("accepted-period-family:", "accepted-alias-window:"),
        naming_shapes=("period-family",),
    ),
    "coding": Family(
        case_prefixes=("accepted-coding:", "accepted-codeless:"),
        unapplied_datasets=("curation/codeless_overlap.toml",),
    ),
    "classification_bindings": Family(
        case_prefixes=(
            "accepted-classification-seed:",
            "accepted-classification-override:",
        ),
        unapplied_datasets=("curation/classifications.toml",),
    ),
    "annotations": Family(
        case_prefixes=(
            "delivery_enrichment.generated.toml/description/",
            "delivery_enrichment.generated.toml/alias/",
        ),
        unapplied_datasets=(
            "scb-registerinformation",
            "sos-",
            "curation/delivery_enrichment.generated.toml",
        ),
    ),
    "naming": Family(
        naming_shapes=("accepted-shape", "accepted-name", "native"),
        scope_fields=("naming_ambiguities", "provider_keys", "variants"),
    ),
    "thin_provider": Family(
        case_prefixes=("accepted-authored:",), naming_shapes=("thin-provider",)
    ),
}
COMPILED = frozenset(
    {
        "classifications",
        "classification_successions",
        "metadata",
        "code_label_pairs",
        "lineage_defaults",
        "identifier_sources",
        "event_sources",
        "acknowledge",
    }
)


@dataclass(frozen=True)
class CompiledCuration:
    fields: dict[str, Any]
    cases: dict[tuple[str, tuple[str | int, ...] | None], tuple[CurationCase, ...]]
    report: dict[str, dict[str, list[str]]]
    diagnostics: tuple[ResolutionDiagnostic, ...] = ()


def tree_sha256(root: Path) -> str:
    """Hash sorted paths and file contents, including the transitional slug tree."""
    members = []
    for directory in (root, root.parent / "fqid_slugs"):
        if directory.is_dir():
            members.extend(path for path in directory.rglob("*") if path.is_file())
    rows = [
        (
            path.relative_to(root.parent).as_posix(),
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(members)
    ]
    return hashlib.sha256(
        json.dumps(
            rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


def source_event_id(ref: SourceRecordRef) -> str:
    literal = json.dumps(
        ref.semantic_record_key, ensure_ascii=False, separators=(",", ":")
    )
    digest = hashlib.sha256(literal.encode()).hexdigest()
    return f"prepared://{ref.source}#/event/{digest}"


_SENTINELS = TypeAdapter(list[SentinelCode])


def validate_sentinels(raw: object, *, subject: str) -> tuple[SentinelCode, ...]:
    """The one strict validator for selection and tracked classification sentinels."""
    if raw is None:
        return ()
    try:
        sentinels = _SENTINELS.validate_python(raw)
    except ValidationError as exc:
        raise ValueError(
            f"classification {subject!r} sentinel_codes must be a list of "
            f"{{code, meaning}} tables with exact-string codes: "
            f"{exc.errors(include_url=False)[0]['msg']}."
        ) from exc
    codes = [sentinel.code for sentinel in sentinels]
    if len(codes) != len(set(codes)):
        raise ValueError(
            f"classification {subject!r} lists a sentinel code more than once."
        )
    return tuple(sentinels)


def _case_family(case_id: str) -> str:
    owners = [
        name
        for name, family in FAMILIES.items()
        if case_id in family.case_ids
        or any(
            case_id.startswith(prefix)
            and ("#/acknowledge/" in case_id if name == "acknowledge" else True)
            for prefix in family.case_prefixes
        )
    ]
    if len(owners) != 1:
        raise ValueError(f"stored case has an unowned or ambiguous prefix: {case_id}")
    return owners[0]


def _naming_family(declaration: Any) -> str:
    source_key = declaration.target.source_key
    markers = {
        "accepted-partition": "identity",
        "declared-column": "errata",
        "accepted-matrix": "matrix",
        "period-family": "representation",
        "accepted-shape": "naming",
        "accepted-name": "naming",
    }
    for part in source_key:
        if part in markers:
            return markers[part]
    if declaration.target.provider not in {"scb", "sos"}:
        return "thin_provider"
    return "naming"


def _gap_family(gap: Any) -> str:
    dataset = gap.revision.dataset
    owners = [
        name
        for name, family in FAMILIES.items()
        if any(dataset.startswith(prefix) for prefix in family.unapplied_datasets)
    ]
    if len(owners) != 1:
        raise ValueError(
            f"stored unapplied curation has no family: {dataset}#{gap.pointer}"
        )
    return owners[0]


def merge_scope(
    scope: ScopeDeclarations, compiled: CompiledCuration
) -> ScopeDeclarations:
    """Replace compiled families and reject every unowned stored declaration."""
    from .pipeline import ScopeDeclarations

    key = scope.source, scope.register_key
    owned_fields = {
        field for family in FAMILIES.values() for field in family.scope_fields
    }
    if owned_fields != {"naming_ambiguities", "provider_keys", "variants"}:
        raise ValueError("scope family ownership is incomplete")
    cases = tuple(
        case for case in scope.cases if _case_family(case.case_id) not in COMPILED
    )
    naming = tuple(
        item for item in scope.naming if _naming_family(item) not in COMPILED
    )
    gaps = tuple(
        gap for gap in scope.unapplied_curation if _gap_family(gap) not in COMPILED
    )
    # Child 06 will derive keys of compiled naming declarations. Until then all
    # naming is stored, and no provider key can silently outlive its owner.
    stored_keys = {item.target.source_key for item in naming}
    provider_keys = tuple(
        item for item in scope.provider_keys if item[0] in stored_keys
    )
    merged = scope.model_copy(
        update={
            "cases": (*cases, *compiled.cases.get(key, ())),
            "naming": naming,
            "provider_keys": provider_keys,
            "unapplied_curation": gaps,
        }
    )
    return ScopeDeclarations.model_validate_json(merged.model_dump_json())


def merge_selection(selected: Any, compiled: CompiledCuration):
    from .pipeline import PipelineSelection

    owned = {
        field: name
        for name, family in FAMILIES.items()
        for field in family.selection_fields
    }
    actual = set(PipelineSelection.model_fields) - {
        "format",
        "prepared_path",
        "prepared_commit",
        "prepared_sha256",
        "scopes",
        "unconverted",
    }
    if set(owned) != actual:
        raise ValueError(
            f"selection family ownership is incomplete: {sorted(actual ^ set(owned))}"
        )
    merged = selected.model_copy(
        update={
            field: value
            for field, value in compiled.fields.items()
            if owned[field] in COMPILED
        }
    )
    return PipelineSelection.model_validate_json(merged.model_dump_json())


def _register(ref: str) -> str:
    return "/".join(ref.split("/")[:2])


def _edge_registers(refs: tuple[str, ...], selected: set[str] | None) -> bool:
    return selected is None or all(_register(ref) in selected for ref in refs)


def compile_declared_metadata(
    tree: CurationTree,
    *,
    selected: set[str] | None = None,
    dropped: list[str] | None = None,
) -> tuple[ResolvedMetadata, tuple[ResolvedClassificationSuccession, ...]]:
    """Translate each tracked global edge exactly once into resolved declarations."""
    dropped = dropped if dropped is not None else []
    books = {entry.classification.slug for entry in tree.classifications}
    groups = []
    for register in sorted(tree.registers, key=lambda item: item.source_file):
        for index, group in enumerate(register.group, 1):
            if not _edge_registers((group.register_fqid,), selected):
                dropped.append(f"{register.source_file}#/group/{index}")
                continue
            axes = (
                tuple(
                    ResolvedGroupAxis(axis=axis.axis, ordinal=i, label=axis.label)
                    for i, axis in enumerate(group.axes)
                )
                if group.axes is not None
                else (
                    (ResolvedGroupAxis(axis=group.axis, ordinal=0, label=group.axis),)
                    if group.axis is not None
                    else ()
                )
            )
            members = []
            for member in group.members:
                coords = (
                    member.coords
                    if member.coords is not None
                    else (
                        [
                            {
                                "axis": axes[0].axis,
                                "value": member.value,
                                "label": member.label,
                            }
                        ]
                        if len(axes) == 1 and member.value is not None
                        else []
                    )
                )
                members.append(
                    ResolvedGroupVariable(
                        variable=f"{group.register_fqid}/{member.variable}",
                        delivery_column_name=member.delivery_column,
                        facets=tuple(
                            ResolvedGroupFacet.model_validate(
                                coord.model_dump()
                                if hasattr(coord, "model_dump")
                                else coord
                            )
                            for coord in coords
                        ),
                    )
                )
            groups.append(
                ResolvedVariableGroup.model_validate(
                    {
                        "register": group.register_fqid,
                        "key": group.key,
                        "label": group.label,
                        "source": "curated",
                        "axes": axes,
                        "members": tuple(members),
                    }
                )
            )
    class_groups = []
    for group in tree.classification_groups.classification_group:
        for member in group.members:
            if member.classification not in books:
                raise ValueError(
                    f"classification group {group.key} names uncompiled book {member.classification}"
                )
        class_groups.append(
            ResolvedClassificationGroup(
                key=group.key,
                label=group.label,
                source="curated",
                axes=(ResolvedGroupAxis(axis=group.axis, ordinal=0, label=group.axis),)
                if group.axis
                else (),
                members=tuple(
                    ResolvedGroupClassification(
                        classification=m.classification,
                        facet_value=m.value,
                        facet_label=m.label,
                    )
                    for m in group.members
                ),
            )
        )
    tags = []
    for tag in tree.tags:
        members = []
        for member in tag.members:
            target = "/".join(
                x
                for x in (member.provider, member.register, member.variable)
                if x is not None
            )
            if not _edge_registers((target,), selected):
                dropped.append(f"tags.toml#/tag/{tag.slug}/member/{target}")
                continue
            members.append(
                ResolvedTagMember(
                    target=target,
                    rank=member.rank,
                    starred=member.starred,
                    note=member.note,
                )
            )
        tags.append(
            ResolvedTag(
                slug=tag.slug,
                label=tag.label,
                description=tag.description,
                members=tuple(members),
            )
        )
    same_as, successions, variants, representations, class_successions, derivations = (
        [],
        [],
        [],
        [],
        [],
        [],
    )
    for edge in tree.relations.same_as:
        if edge.grain != FqidKind.VARIABLE_BINDING:
            raise ValueError(
                f"same_as must have variable binding endpoints: {edge.a_fqid()}"
            )
        if _edge_registers((edge.a_fqid(), edge.b_fqid()), selected):
            same_as.append(ResolvedVariableSameAs(a=edge.a_fqid(), b=edge.b_fqid()))
        else:
            dropped.append(f"relations.toml#/same_as/{edge.a_fqid()}/{edge.b_fqid()}")
    for edge in tree.relations.replaced_by:
        a, b = str(edge.predecessor), str(edge.successor)
        if edge.predecessor.kind == FqidKind.CLASSIFICATION:
            predecessor, successor = a.removeprefix("class/"), b.removeprefix("class/")
            if predecessor not in books or successor not in books:
                raise ValueError(
                    f"classification succession names uncompiled book: {a} -> {b}"
                )
            class_successions.append(
                ResolvedClassificationSuccession(
                    predecessor=predecessor,
                    successor=successor,
                    effective_year=edge.effective_year,
                    note="curated:slug_toml",
                )
            )
            continue
        if not _edge_registers((a, b), selected):
            dropped.append(f"relations.toml#/replaced_by/{a}/{b}")
            continue
        common = {
            "effective_year": edge.effective_year,
            "note": "curated:slug_toml",
            "description": edge.note,
        }
        if edge.predecessor_column is not None:
            assert edge.successor_column is not None
            representations.append(
                ResolvedRepresentationSuccession(
                    predecessor=ResolvedRepresentationRef(
                        variable=a, delivery_column_name=edge.predecessor_column
                    ),
                    successor=ResolvedRepresentationRef(
                        variable=b, delivery_column_name=edge.successor_column
                    ),
                    variant=edge.variant or None,
                    **common,
                )
            )
        elif edge.predecessor_variant is not None:
            assert edge.successor_variant is not None
            variants.append(
                ResolvedVariantSuccession(
                    predecessor=ResolvedVariantRef.model_validate(
                        {"register": a, "variant": edge.predecessor_variant}
                    ),
                    successor=ResolvedVariantRef.model_validate(
                        {"register": b, "variant": edge.successor_variant}
                    ),
                    **common,
                )
            )
        else:
            successions.append(ResolvedSuccession(predecessor=a, successor=b, **common))
    for edge in tree.relations.derived_from:
        a, b = (
            str(edge.derived).removeprefix("class/"),
            str(edge.source).removeprefix("class/"),
        )
        if a not in books or b not in books:
            raise ValueError(
                f"classification derivation names uncompiled book: {a} -> {b}"
            )
        derivations.append(
            ResolvedClassificationDerivation(derived=a, source=b, note=edge.note)
        )
    return ResolvedMetadata(
        variable_groups=tuple(groups),
        classification_groups=tuple(class_groups),
        tags=tuple(tags),
        variable_same_as=tuple(same_as),
        successions=tuple(successions),
        variant_successions=tuple(variants),
        representation_successions=tuple(representations),
        classification_derivations=tuple(derivations),
    ), tuple(class_successions)


def _scope_registers(
    scope: ScopeDeclarations,
) -> tuple[tuple[str, tuple[str | int, ...]], ...]:
    return tuple(
        (f"{item.naming.provider}/{item.naming.slug}", item.target.source_key)
        for item in scope.naming
        if item.target.kind == "register"
        and item.naming.provider is not None
        and item.naming.slug is not None
    )


def compile_curation(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[ScopeDeclarations, ...],
    *,
    subset: bool = False,
) -> CompiledCuration:
    """Compile global families, wiring, and exact issue acknowledgements."""
    from .pipeline import CodebookDeclaration

    selected = {register for scope in scopes for register, _ in _scope_registers(scope)}
    dropped: list[str] = []
    metadata, class_successions = compile_declared_metadata(
        tree, selected=selected, dropped=dropped
    )
    books = tuple(
        CodebookDeclaration(
            source=f"classifications/{entry.classification.codes_file}",
            descriptor=normalize_token(Path(entry.classification.codes_file).stem),
            metadata={
                **entry.classification.model_dump(
                    mode="json", exclude={"codes_file", "sentinel_codes"}
                ),
                "sentinel_codes": [
                    s.model_dump(mode="json")
                    for s in validate_sentinels(
                        [
                            s.model_dump(mode="json")
                            for s in entry.classification.sentinel_codes
                        ],
                        subject=entry.classification.slug,
                    )
                ],
            },
        )
        for entry in tree.classifications
    )
    pairs = []
    seen_pairs: set[tuple[str, str]] = set()
    for register in sorted(tree.registers, key=lambda item: item.source_file):
        for index, pair in enumerate(register.code_label_pair, 1):
            case_id = f"{register.source_file}#/code_label_pair/{index}"
            for ref in (pair.code, pair.label):
                try:
                    if parse_fqid(ref).kind != FqidKind.VARIABLE_BINDING:
                        raise ValueError("wrong FQID grain")
                except ValueError as exc:
                    raise ValueError(
                        f"{case_id}: invalid variable FQID {ref!r}"
                    ) from exc
            if (pair.code, pair.label) in seen_pairs:
                raise ValueError(f"{case_id}: duplicate code/label pair")
            seen_pairs.add((pair.code, pair.label))
            if not _edge_registers((pair.code, pair.label), selected):
                dropped.append(case_id)
                continue
            code, label = pair.code.split("/"), pair.label.split("/")
            if code == label:
                raise ValueError(f"{case_id}: identical endpoints")
            pairs.append(
                CodeLabelPair(
                    code_provider=code[0],
                    code_register=code[1],
                    code_variable=code[2],
                    label_provider=label[0],
                    label_register=label[1],
                    label_variable=label[2],
                )
            )
    defaults = []
    for key, variant in sorted(tree.lineage.defaults.items()):
        ref = "/".join(key)
        if _edge_registers((ref,), selected):
            defaults.append((ref, variant))
        else:
            dropped.append(f"lineage.toml#/lineage_defaults/{ref}")
    inputs = prepared.manifest.inputs
    identifiers = tuple(
        e.revision.dataset
        for e in inputs
        if e.origin == "snapshot"
        and e.path == "Identifierare.csv"
        and e.revision is not None
    )
    all_events = tuple(
        (e.revision.dataset, target.revision.dataset)
        for e in inputs
        for target in inputs
        if e.origin == target.origin == "snapshot"
        and e.path == "Timeseries.csv"
        and target.path == "Registerinformation.csv"
        and e.revision is not None
        and target.revision is not None
    )
    selected_sources = {scope.source for scope in scopes}
    events = tuple(item for item in all_events if item[1] in selected_sources)
    dropped.extend(
        f"prepared://{source}->{target}"
        for source, target in all_events
        if target not in selected_sources
    )
    skipped_event_sources = {
        source for source, target in all_events if target not in selected_sources
    }
    if skipped_event_sources:
        from .prepared_catalog import ReferenceEvidence
        from .source_reference_records import SourceEventDeclaration

        for evidence in prepared.iter_evidence():
            if not isinstance(evidence, ReferenceEvidence) or not isinstance(
                evidence.declaration, SourceEventDeclaration
            ):
                continue
            declaration = evidence.declaration
            if declaration.revision.dataset not in skipped_event_sources:
                continue
            dropped.append(
                source_event_id(
                    SourceRecordRef(
                        source=declaration.revision.dataset,
                        semantic_record_key=declaration.locator.semantic_record_key,
                    )
                )
            )
    cases: dict[tuple[str, tuple[str | int, ...] | None], list[CurationCase]] = {
        (scope.source, scope.register_key): [] for scope in scopes
    }
    owners: dict[
        str,
        list[tuple[tuple[str, tuple[str | int, ...] | None], tuple[str | int, ...]]],
    ] = {}
    for scope in scopes:
        for register, native_key in _scope_registers(scope):
            owners.setdefault(register, []).append(
                ((scope.source, scope.register_key), native_key)
            )
    report: dict[str, dict[str, list[str]]] = {}
    diagnostics: list[ResolutionDiagnostic] = []
    for register in sorted(tree.registers, key=lambda item: item.source_file):
        name = f"{register.register_info.provider}/{register.register_info.slug}"
        statuses = report.setdefault(
            name,
            {
                key: []
                for key in (
                    "entries_read",
                    "entries_matched",
                    "stale",
                    "over_broad",
                    "not_evaluated_in_subset",
                )
            },
        )
        for table, entries in (
            ("group", register.group),
            ("code_label_pair", register.code_label_pair),
        ):
            for index, _ in enumerate(entries, 1):
                entry_id = f"{register.source_file}#/{table}/{index}"
                statuses["entries_read"].append(entry_id)
                if name in selected and entry_id not in dropped:
                    statuses["entries_matched"].append(entry_id)
                elif subset:
                    statuses["not_evaluated_in_subset"].append(entry_id)
                else:
                    statuses["stale"].append(entry_id)
        for index, ack in enumerate(register.acknowledge, 1):
            case_id = f"{register.source_file}#/acknowledge/{index}"
            statuses["entries_read"].append(case_id)
            try:
                refs = tuple(
                    SourceRecordRef.model_validate_json(ref) for ref in ack.refs
                )
            except ValueError as exc:
                raise ValueError(
                    f"{case_id}: refs must be serialized SourceRecordRef JSON objects"
                ) from exc
            matched = owners.get(name, [])
            if not matched:
                if subset:
                    statuses["not_evaluated_in_subset"].append(case_id)
                else:
                    statuses["stale"].append(case_id)
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code="stale_curation_entry",
                            severity="error",
                            case_id=case_id,
                            subject=ack.subject,
                            detail=f"{case_id} matches no selected register scope",
                            refs=refs,
                            withheld_output=(case_id,),
                        )
                    )
            elif len(matched) != 1:
                statuses["over_broad"].append(case_id)
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="overbroad_curation_entry",
                        severity="error",
                        case_id=case_id,
                        subject=ack.subject,
                        detail=f"{case_id} matches {len(matched)} register scopes",
                        refs=refs,
                        withheld_output=(case_id,),
                    )
                )
            else:
                statuses["entries_matched"].append(case_id)
                scope_key, register_key = matched[0]
                cases[scope_key].append(
                    CurationCase(
                        case_id=case_id,
                        targets=(),
                        decision=AcknowledgeDecision(
                            code=ack.code,
                            subject=ack.subject,
                            refs=refs,
                            register_key=register_key,
                            reason=ack.reason,
                            evidence=ack.evidence,
                        ),
                    )
                )
    variable_matches: Counter[str] = Counter()
    for scope in scopes:
        registers = {
            native_key: register for register, native_key in _scope_registers(scope)
        }
        for name in scope.naming:
            if (
                name.target.kind == "variable"
                and name.naming.slug is not None
                and (register := registers.get(name.target.register_key)) is not None
            ):
                variable_matches[f"{register}/{name.naming.slug}"] += 1
    selected_providers = {register.split("/", 1)[0] for register in selected}
    classification_report = {
        key: []
        for key in (
            "entries_read",
            "entries_matched",
            "stale",
            "over_broad",
            "not_evaluated_in_subset",
        )
    }
    for entry in tree.classifications:
        base = f"classifications/{entry.classification.short_name}.toml"
        classification_report["entries_read"].append(base)
        classification_report["entries_matched"].append(base)
        for index, _ in enumerate(entry.binding.value_set_labels, 1):
            ref = f"{base}#/binding/value_set_labels/{index}"
            classification_report["entries_read"].append(ref)
            # The binding family is still stored. Its source labels can only be
            # known to lie outside a subset when their provider is absent.
            if (
                subset
                and (entry.binding.label_source or "scb") not in selected_providers
            ):
                classification_report["not_evaluated_in_subset"].append(ref)
        for index, bound in enumerate(entry.binding.variable, 1):
            ref = f"{base}#/binding/variable/{index}"
            classification_report["entries_read"].append(ref)
            if _register(bound.variable) not in selected:
                if subset:
                    classification_report["not_evaluated_in_subset"].append(ref)
                    continue
                status = "stale"
            else:
                matches = variable_matches[bound.variable]
                status = (
                    "entries_matched"
                    if matches == 1
                    else "over_broad"
                    if matches > 1
                    else "stale"
                )
            classification_report[status].append(ref)
            if status != "entries_matched":
                diagnostics.append(
                    ResolutionDiagnostic(
                        code=(
                            "overbroad_curation_entry"
                            if status == "over_broad"
                            else "stale_curation_entry"
                        ),
                        severity="error",
                        subject=bound.variable,
                        detail=f"{ref} matches {variable_matches[bound.variable]} selected variable declarations",
                        withheld_output=(ref,),
                    )
                )
    report["_classifications"] = classification_report
    report["_subset"] = {"dropped": sorted(dropped)}
    if not subset:
        diagnostics.extend(
            ResolutionDiagnostic(
                code="stale_curation_entry",
                severity="error",
                subject=entry_id,
                detail=f"{entry_id} matches no selected register",
                withheld_output=(entry_id,),
            )
            for entry_id in sorted(dropped)
        )
    return CompiledCuration(
        fields={
            "classifications": books,
            "classification_successions": class_successions,
            "metadata": metadata,
            "code_label_pairs": tuple(pairs),
            "lineage_defaults": tuple(defaults),
            "identifier_sources": identifiers,
            "event_sources": events,
        },
        cases={
            key: tuple(sorted(value, key=lambda item: item.case_id))
            for key, value in cases.items()
        },
        report=report,
        diagnostics=tuple(diagnostics),
    )
