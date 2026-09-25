"""Compile tracked curation beside a pinned, transitional selection.

Families are closed: every stored declaration has one owner, and COMPILED replaces
that owner's declarations in every scope. Later children move their converters here.
"""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import TypeAdapter, ValidationError
from reg_meta.fqid import FqidKind, derive_variable_slug, parse as parse_fqid

from ._curation import SentinelCode, fold_column
from ._resolved_common import covers_window
from .cis2016_matrix import (
    Cis2014Matrix,
    convert_matrix,
    load_cis2014_matrix,
    load_cis2016_matrix,
)
from .concept_groups import _MONTH_TOKENS, CodeLabelPair
from .curation_tree import EnrichmentAliasEntry, EnrichmentDescriptionEntry
from .delivery_enrichment import load_delivery_enrichment
from .fqid_slugs import EntityKind, SlugEntry, _load_register_auto_file, _validate_entry
from .normalization import normalize_token
from .resolved_catalog import ResolvedClassificationSuccession, ResolvedVariant
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
from .scb_errata import (
    convert_column_entry,
    convert_delivered_entry,
    edition_bindings,
    load_scb_errata,
)
from .source_coding import copied_coding_fingerprints
from .source_coding_choices import coding_expectations, compile_coding_selection
from .source_coordinates import (
    column_identity,
    native_parent_key,
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from .source_curation import (
    AcknowledgeDecision,
    AliasWindowDecision,
    CheckedFieldChange,
    CheckedIdentityChange,
    CheckedSourceUse,
    CheckedVariantAssignment,
    CodingDecision,
    ColumnRepresentation,
    CuratedOccurrenceAddition,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    RepresentationDecision,
    ResolutionDiagnostic,
    SearchAliasDecision,
    SourceRecordRef,
    capture_expectations,
)
from .source_effects import record_ref
from .source_intervals import coding_scope_bounds, scope_bounds
from .source_naming import (
    AcceptedNamingEntry,
    LegacyNamingBinding,
    NamingAmbiguity,
    NamingDeclaration,
    NamingFreezeSetting,
    NamingSelection,
    NativeNamingTarget,
    authored_naming_id,
    convert_naming,
    native_provider_keys,
    native_scb_naming_id,
)
from .source_occurrences import source_occurrence
from .source_records import (
    ScopeInterval,
    SourceFields,
    SourceRevision,
    TemporalScope,
    canonical_sha256,
)
from .source_value_bindings import bind_code_lists, open_value_bindings

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .curation_tree import (
        CurationTree,
        IdentityRenameEntry,
        IdentitySplitEntry,
        RegisterCuration,
    )
    from .pipeline import ScopeDeclarations
    from .prepared_catalog import PreparedCatalogSources
    from .source_coding import CodeListClaim
    from .source_coordinates import NativeKey
    from .source_records import SourceRecord


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
    "identity": Family(unapplied_datasets=("fqid_slugs/",)),
    "partition": Family(
        case_prefixes=("accepted-column-partitions:", "accepted-sos-identity:"),
        naming_shapes=("accepted-partition", "accepted-shape", "accepted-name"),
        scope_fields=("naming_ambiguities",),
    ),
    "errata": Family(
        case_prefixes=(
            "scb_errata.toml/column/",
            "scb_errata.toml/delivered/",
        ),
        naming_shapes=("declared-column",),
        unapplied_datasets=("curation/scb_errata.toml",),
    ),
    "matrix_repr": Family(
        case_ids=("accepted-cis2014-answers", "accepted-cis2016-answers"),
        case_prefixes=("accepted-period-family:", "accepted-alias-window:"),
        naming_shapes=("accepted-matrix", "period-family"),
        unapplied_datasets=("curation/cis",),
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
    "enrichment": Family(
        case_prefixes=(
            "delivery_enrichment.generated.toml/description/",
            "delivery_enrichment.generated.toml/alias/",
        ),
        unapplied_datasets=("curation/delivery_enrichment.generated.toml",),
    ),
    "annotations": Family(
        # The synthetic SOS pipeline fixture supplies this exact reviewed case.
        case_ids=("accepted-errata:sos-declared-flags",),
        unapplied_datasets=("scb-registerinformation", "sos-"),
    ),
    "naming": Family(
        naming_shapes=("native",),
        scope_fields=("provider_keys", "variants"),
    ),
    "sos_thin": Family(
        case_prefixes=(
            "accepted-sos-routes:",
            "existing-source-use:",
            "accepted-authored:",
        ),
        naming_shapes=("thin-provider",),
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
        "classification_bindings",
        "coding",
        "naming",
        "sos_thin",
        "partition",
        "matrix_repr",
        "errata",
        "enrichment",
    }
)


@dataclass(frozen=True)
class CompiledCuration:
    fields: dict[str, Any]
    cases: dict[tuple[str, tuple[str | int, ...] | None], tuple[CurationCase, ...]]
    report: dict[str, dict[str, list[str]]]
    diagnostics: tuple[ResolutionDiagnostic, ...] = ()
    naming: dict[tuple[str, tuple[str | int, ...] | None], tuple[Any, ...]] | None = (
        None
    )
    variants: dict[tuple[str, tuple[str | int, ...] | None], tuple[Any, ...]] | None = (
        None
    )
    provider_keys: (
        dict[tuple[str, tuple[str | int, ...] | None], tuple[Any, ...]] | None
    ) = None
    naming_ambiguities: (
        dict[tuple[str, tuple[str | int, ...] | None], tuple[NamingAmbiguity, ...]]
        | None
    ) = None
    partition_bases: (
        dict[tuple[str, tuple[str | int, ...] | None], frozenset[NativeKey]] | None
    ) = None


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


def _naming_family(target: Any) -> str:
    source_key = target.source_key
    if target.provider not in {"scb", "sos"}:
        return "sos_thin"
    markers = {
        "accepted-partition": "partition",
        "declared-column": "errata",
        "accepted-matrix": "matrix_repr",
        "period-family": "matrix_repr",
        "accepted-shape": "partition",
        "accepted-name": "partition",
        "thin-provider": "sos_thin",
    }
    for part in source_key:
        if part in markers:
            return markers[part]
    if target.kind in {"register", "register_variant"} or (
        target.kind == "variable"
        and len(source_key) >= 3
        and source_key[-3] == "variable"
        and source_key[-2] in {"native-int", "native-str"}
    ):
        return "naming"
    raise ValueError(f"stored naming has no family: {source_key!r}")


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
        item for item in scope.naming if _naming_family(item.target) not in COMPILED
    )
    gaps = tuple(
        gap for gap in scope.unapplied_curation if _gap_family(gap) not in COMPILED
    )
    # A null key is the unsplit base of a partitioned identity. It has no naming
    # declaration, but remains stored until partition is compiled.
    naming_owners = {
        item.target.source_key: _naming_family(item.target) for item in scope.naming
    }
    naming_owners.update(
        (item.family.source_key, _naming_family(item.family))
        for item in scope.naming_ambiguities
    )
    # Stored attribution and unresolved base keys still explain dependencies
    # for families with no tracked split in this transitional selection.
    partition_bases = (compiled.partition_bases or {}).get(key, frozenset())
    untouched_nulls = {
        source_key
        for source_key, value in scope.provider_keys
        if value is None and source_key not in partition_bases
    }
    provider_keys = []
    for item in scope.provider_keys:
        source_key, value = item
        owner = "partition" if value is None else naming_owners.get(source_key)
        if owner is None:
            raise ValueError(
                f"provider key has no converted catalog naming: {source_key!r}"
            )
        if owner not in COMPILED or source_key in untouched_nulls:
            provider_keys.append(item)
    merged = scope.model_copy(
        update={
            "cases": (*cases, *compiled.cases.get(key, ())),
            "naming": (*naming, *(compiled.naming or {}).get(key, ())),
            "provider_keys": (
                *provider_keys,
                *(
                    item
                    for item in (compiled.provider_keys or {}).get(key, ())
                    if item[0] not in untouched_nulls
                ),
            ),
            "naming_ambiguities": (
                *(
                    item
                    for item in scope.naming_ambiguities
                    if item.family.source_key not in partition_bases
                ),
                *(compiled.naming_ambiguities or {}).get(key, ()),
            ),
            "variants": (compiled.variants or {}).get(key, ()),
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
    selected: set[str],
    unmatched: list[str],
) -> tuple[ResolvedMetadata, tuple[ResolvedClassificationSuccession, ...]]:
    """Translate every tracked global edge; record unmatched entries without dropping them."""
    books = {entry.classification.slug for entry in tree.classifications}
    groups = []
    for register in sorted(tree.registers, key=lambda item: item.source_file):
        for index, group in enumerate(register.group, 1):
            if not _edge_registers((group.register_fqid,), selected):
                unmatched.append(f"{register.source_file}#/group/{index}")
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
                unmatched.append(f"tags.toml#/tag/{tag.slug}/member/{target}")
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
        if not _edge_registers((edge.a_fqid(), edge.b_fqid()), selected):
            unmatched.append(f"relations.toml#/same_as/{edge.a_fqid()}/{edge.b_fqid()}")
        same_as.append(ResolvedVariableSameAs(a=edge.a_fqid(), b=edge.b_fqid()))
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
            unmatched.append(f"relations.toml#/replaced_by/{a}/{b}")
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


def _naming_source_id(
    kind: Literal["register", "register_variant", "variable"],
    key: tuple[str | int, ...],
    *,
    member: str | None = None,
) -> str:
    source, provider = key[:2]
    register = (
        key
        if kind == "register"
        else key[: key.index("variable" if kind == "variable" else "variant")]
    )
    canonical_scb = (
        str(source).startswith("scb_canonical/") or source == "scb_canonical"
    )
    if provider == "scb" and not canonical_scb and register[-2] == "native-int":
        register_id = register[-1]
        member_id = None if kind == "register" else key[-1]
        if not isinstance(register_id, int) or (
            member_id is not None and not isinstance(member_id, int)
        ):
            raise ValueError(f"SCB native naming requires integer IDs: {key!r}")
        return native_scb_naming_id(kind, register_id, member_id)
    if provider == "sos":
        matches = re.findall(r"\(([^()]+)\)", Path(str(source)).name)
        if not matches:
            raise ValueError(
                f"SOS workbook lacks parenthesized register code: {source}"
            )
        register_id = matches[-1].lower()
    else:
        register_id = str(register[-1])
    return authored_naming_id(
        kind,
        provider=str(provider),
        register_key=register_id,
        member_key=member
        if member is not None
        else (None if kind == "register" else str(key[-1])),
        canonical_scb=provider == "scb",
    )


def _curation_naming_revision(path: Path) -> SourceRevision:
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    return SourceRevision.create(
        dataset="curation/naming",
        publisher="maintainer",
        purpose="tracked naming",
        upstream_revision=digest,
        artifact_path=f"curation/registers/{path.parent.name}/{path.name}",
        artifact_size=len(payload),
        artifact_sha256=digest,
    )


def _tracked_naming_entry(
    revision: SourceRevision,
    kind: EntityKind,
    native_id: str,
    raw: dict[str, Any],
    provider: str,
    origin: Literal["authored", "generated"],
) -> AcceptedNamingEntry:
    return AcceptedNamingEntry(
        revision=revision,
        origin=origin,
        entry=_validate_entry(kind, native_id, raw, provider=provider),
        supplied_fields=tuple(sorted(raw)),
        content_sha256=canonical_sha256(raw),
    )


def _register_naming_entries(tree: CurationTree, register: RegisterCuration):
    path = tree.root / register.source_file.removeprefix("curation/")
    info = register.register_info
    revision = _curation_naming_revision(path)
    entries = [
        (
            _tracked_naming_entry(
                revision,
                "register",
                str(info.native_id),
                {"slug": info.slug},
                info.provider,
                "authored",
            ),
            "[register]",
        )
    ]
    for table, rows, kind in (
        ("variant", register.variant, "register_variant"),
        ("variable", register.variable, "variable"),
    ):
        for index, row in enumerate(rows, 1):
            raw = row.model_dump(mode="json", exclude={"native_id"}, exclude_unset=True)
            entries.append(
                (
                    _tracked_naming_entry(
                        revision,
                        cast("EntityKind", kind),
                        row.native_id,
                        raw,
                        info.provider,
                        "authored",
                    ),
                    f"[[{table}]] entry {index}",
                )
            )
    auto = path.with_name(path.stem + ".auto.toml")
    if auto.is_file():
        auto_revision = _curation_naming_revision(auto)
        raw_rows = tomllib.loads(auto.read_text(encoding="utf-8")).get("variable", [])
        for index, entry in enumerate(
            _load_register_auto_file(auto, info.provider, str(info.native_id)), 1
        ):
            raw = {
                key: value
                for key, value in raw_rows[index - 1].items()
                if key != "native_id"
            }
            entries.append(
                (
                    _tracked_naming_entry(
                        auto_revision,
                        "variable",
                        entry.source_id,
                        raw,
                        info.provider,
                        "generated",
                    ),
                    f"[[variable]] entry {index}",
                )
            )
    return tuple(entries)


@dataclass(frozen=True)
class ColumnPartitionConversion:
    bindings: tuple[LegacyNamingBinding, ...]
    case: CurationCase | None
    diagnostics: tuple[ResolutionDiagnostic, ...]


def convert_column_partitions(
    records: tuple[SourceRecord, ...],
    *,
    source_id: str,
    split_ids: tuple[str, ...],
    declared_columns: Mapping[str, str | None] | None = None,
    declaration_reference: str | None = None,
    scoped_owners: Mapping[SourceRecordRef, str] | None = None,
) -> ColumnPartitionConversion:
    """Check a complete family and bind independently identifiable accepted splits.

    ``split_ids`` must include every active split key for this native family. Blank
    source columns remain original evidence; they are not assigned to a sibling.
    New members or changed relevant facts invalidate the entire checked decision.
    ``declared_columns`` is a complete literal column-to-split-key map already
    asserted by accepted curation, with its original declaration reference. This
    converter verifies its coverage; it never discovers column equivalence. An
    explicit None records an unassigned original column, not an identity claim
    or a curation waiver. Such rows retain their native unresolved identity while
    independently declared ownership is converted and guarded by the whole family.
    """
    keys = {native_variable_key(record) for record in records}
    if not records or None in keys or len(keys) != 1:
        raise ValueError("partition conversion requires one complete native family")
    if (
        len(set(split_ids)) != len(split_ids)
        or not split_ids
        or any(
            not key.startswith(source_id + ".") or not key[len(source_id) + 1 :]
            for key in split_ids
        )
    ):
        raise ValueError("split keys must uniquely discriminate this source identity")
    columns: dict[str, list[SourceRecord]] = defaultdict(list)
    for record in records:
        column = record.fields.column_name
        if column is not None and column.status == "value" and column.value:
            assert isinstance(column.value, str)
            columns[column.value].append(record)
    scoped_owners = scoped_owners or {}
    scoped_columns = {
        record.fields.column_name.value
        for record in records
        if record_ref(record) in scoped_owners
        and record.fields.column_name is not None
        and record.fields.column_name.status == "value"
    }
    if set(scoped_owners.values()) - set(split_ids):
        raise ValueError("scoped owner names a split outside this family")
    candidates: dict[str, list[str]] = defaultdict(list)
    for column in columns:
        if column not in scoped_columns:
            candidates[derive_variable_slug(column) or "x"].append(column)
    suffixes = {key[len(source_id) + 1 :] for key in split_ids}
    diagnostics = ()
    if declared_columns is not None:
        if not declaration_reference or not declaration_reference.strip():
            raise ValueError(
                "explicit column ownership needs its declaration reference"
            )
        if set(declared_columns) != set(columns) or {
            owner for owner in declared_columns.values() if owner is not None
        } != set(split_ids):
            missing = sorted(set(columns) - set(declared_columns))
            absent = sorted(set(declared_columns) - set(columns))
            owners = {owner for owner in declared_columns.values() if owner is not None}
            raise ValueError(
                "explicit column ownership must cover the complete columns and split keys; "
                f"uncovered literals={missing!r}; absent literals={absent!r}; "
                f"unnamed owners={sorted(owners - set(split_ids))!r}; "
                f"unowned splits={sorted(set(split_ids) - owners)!r}"
            )
        partition_columns = {
            key: tuple(
                sorted(
                    column for column, owner in declared_columns.items() if owner == key
                )
            )
            for key in split_ids
        }
        unassigned = tuple(
            sorted(
                column for column, owner in declared_columns.items() if owner is None
            )
        )
        if unassigned:
            diagnostics = (
                ResolutionDiagnostic(
                    code="unassigned_original_columns",
                    severity="error",
                    subject=source_id,
                    detail="Accepted literal ownership covers only part of the original family. "
                    f"Unassigned original columns={unassigned!r}; their identity remains unresolved.",
                    refs=tuple(
                        sorted(
                            {
                                record_ref(record)
                                for column in unassigned
                                for record in columns[column]
                            },
                            key=str,
                        )
                    ),
                    fields=("identity", "column_name"),
                    withheld_output=(source_id,),
                ),
            )
    elif declaration_reference is not None:
        raise ValueError("a declaration reference requires explicit column ownership")
    else:
        partition_columns = {
            key: tuple(candidates[key[len(source_id) + 1 :]])
            for key in split_ids
            if len(candidates[key[len(source_id) + 1 :]]) == 1
        }
        unresolved = (
            set(split_ids) - partition_columns.keys() - set(scoped_owners.values())
        )
        if unresolved:
            diagnostics = (
                ResolutionDiagnostic(
                    code="split_identity_conversion_pending",
                    severity="error",
                    subject=source_id,
                    detail=(
                        "Some accepted naming discriminators do not identify unique literal column partitions. "
                        f"Unresolved split keys={sorted(unresolved)!r}; "
                        f"independently bound keys={sorted(partition_columns)!r}. "
                        f"Accepted suffixes={sorted(suffixes)!r}; source columns={dict(candidates)!r}. "
                        "Convert the existing rename/shape/identity decision; do not guess from column similarity."
                    ),
                    refs=tuple(
                        sorted({record_ref(record) for record in records}, key=str)
                    ),
                    fields=("identity", "column_name"),
                    withheld_output=tuple(sorted(unresolved)),
                ),
            )
    if not partition_columns and not scoped_owners:
        return ColumnPartitionConversion((), None, diagnostics)
    first = records[0]
    native = native_variable_key(first)
    register = source_register_key(first)
    assert native is not None and register is not None
    expectations = capture_expectations(records, fields=("column_name",))
    guard = PeerGuard(
        guard_id=f"accepted-partitions:{first.source}:{source_id}",
        source=first.source,
        coordinates=(
            ("register", first.subject.register_name),
            ("variable", first.subject.variable),
        ),
        expected_members=tuple(item.ref for item in expectations),
    )
    effects = {}
    bindings = []
    for split_id in sorted(set(partition_columns) | set(scoped_owners.values())):
        key = (*native, "accepted-partition", split_id)
        for column in partition_columns.get(split_id, ()):
            for record in columns[column]:
                ref = record_ref(record)
                if ref in scoped_owners:
                    continue
                effect = CheckedIdentityChange(
                    ref=ref,
                    variable_key=key,
                    when=(
                        FieldExpectation(
                            name="column_name", status="value", value=column
                        ),
                    ),
                )
                effects[ref, column] = effect
        for record in records:
            ref = record_ref(record)
            if scoped_owners.get(ref) != split_id:
                continue
            column = record.fields.column_name
            assert column is not None and column.status == "value"
            effects[ref, column.value] = CheckedIdentityChange(
                ref=ref,
                variable_key=key,
                when=(
                    FieldExpectation(
                        name="column_name", status="value", value=column.value
                    ),
                ),
            )
        bindings.append(
            LegacyNamingBinding(
                kind="variable",
                provider=first.subject.provider,
                source_id=split_id,
                target=NativeNamingTarget(
                    kind="variable",
                    provider=first.subject.provider,
                    source_key=key,
                    register_key=register,
                    expectations=expectations,
                    peer_guards=(guard,),
                ),
            )
        )
    selected = {ref for ref, _column in effects}
    case = CurationCase(
        case_id=f"accepted-column-partitions:{first.source}:{source_id}",
        targets=tuple(item for item in expectations if item.ref in selected),
        support=tuple(item for item in expectations if item.ref not in selected),
        peer_guards=(guard,),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(effects[ref] for ref in sorted(effects, key=str)),
            reason="Preserve the exact column partitions already named by the accepted split entries.",
            provenance=(
                "existing naming entries: "
                + ", ".join(
                    sorted(set(partition_columns) | set(scoped_owners.values()))
                )
                + (
                    "; column ownership: " + declaration_reference
                    if declaration_reference
                    else ""
                )
            ),
        ),
    )
    return ColumnPartitionConversion(tuple(bindings), case, diagnostics)


def _stale_partition(ref: str, subject: str, detail: str) -> ResolutionDiagnostic:
    return ResolutionDiagnostic(
        code="stale_curation_entry",
        severity="error",
        case_id=ref,
        subject=subject,
        detail=f"{ref}: {detail}",
        withheld_output=(ref,),
    )


def _matrix_repr_guard(record: SourceRecord, case_id: str) -> PeerGuard:
    ref = record_ref(record)
    subject = record.subject.native
    return PeerGuard(
        guard_id=f"{case_id}:{canonical_sha256((ref.model_dump(mode='json'), subject.model_dump(mode='json')))}",
        source=record.source,
        native=subject,
        expected_members=(ref,),
    )


def _matrix_repr_guards(
    records: tuple[SourceRecord, ...], case_id: str
) -> tuple[PeerGuard, ...]:
    guards = (_matrix_repr_guard(record, case_id) for record in records)
    return tuple(
        sorted(
            {guard.guard_id: guard for guard in guards}.values(),
            key=lambda guard: guard.guard_id,
        )
    )


def _overbroad_matrix_repr(ref: str, subject: str, detail: str) -> ResolutionDiagnostic:
    return ResolutionDiagnostic(
        code="overbroad_curation_entry",
        severity="error",
        case_id=ref,
        subject=subject,
        detail=f"{ref}: {detail}",
        withheld_output=(ref,),
    )


def _curation_year(record: SourceRecord) -> int | None:
    bounds = scope_bounds(record.edition_scope)
    if bounds is None or len(bounds) != 1:
        return None
    lo, hi = bounds[0]
    start, end = date.fromordinal(lo), date.fromordinal(hi)
    return start.year if start.year == end.year else None


def _edition_label(record: SourceRecord) -> str | None:
    labels = {
        parent.coordinate.name
        for parent in record.parent_facts
        if parent.kind == "edition" and parent.coordinate.name
    }
    return next(iter(labels)) if len(labels) == 1 else None


def compile_period_families(
    register: RegisterCuration,
    records: tuple[SourceRecord, ...],
) -> tuple[
    tuple[CurationCase, ...],
    tuple[NamingDeclaration, ...],
    tuple[tuple[tuple[str | int, ...], str], ...],
    tuple[ResolutionDiagnostic, ...],
]:
    """Compile complete calendar-month groups from one original SCB register."""
    cases: list[CurationCase] = []
    names: list[NamingDeclaration] = []
    keys = []
    diagnostics = []
    for index, entry in enumerate(register.representation.period_family, 1):
        ref = f"{register.source_file}#/representation.period_family/{index}"
        stem = derive_variable_slug(entry.family_stem)
        if stem is None:
            diagnostics.append(
                _stale_partition(
                    ref, entry.family_stem, "family stem has no variable slug"
                )
            )
            continue
        groups: dict[
            tuple[tuple[str | int, ...], int], dict[int, list[SourceRecord]]
        ] = defaultdict(lambda: defaultdict(list))
        unassigned = []
        for record in records:
            column = _literal_field(record, "column_name")
            if column is None:
                continue
            slug = derive_variable_slug(column)
            for token, month in _MONTH_TOKENS.items():
                if slug == stem + token or slug == stem + "-" + token:
                    variant = native_variant_key(record)
                    year = _curation_year(record)
                    if variant is None or year is None:
                        unassigned.append(column)
                        break
                    groups[variant, year][month].append(record)
                    break
        if unassigned:
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.family_stem,
                    f"month members lack one variant and annual year: {sorted(set(unassigned))!r}",
                )
            )
            continue
        if not groups:
            diagnostics.append(
                _stale_partition(ref, entry.family_stem, "no month members")
            )
            continue
        family_key = (
            "curation",
            "period-family",
            register.register_info.provider,
            register.register_info.slug,
            entry.family_stem,
        )
        named_members = []
        for (variant, year), months in sorted(
            groups.items(), key=lambda item: repr(item[0])
        ):
            if set(months) != set(range(1, 13)) or any(
                len({_literal_field(r, "column_name") for r in members}) != 1
                for members in months.values()
            ):
                diagnostics.append(
                    _stale_partition(
                        ref,
                        entry.family_stem,
                        f"{variant!r} {year} lacks one exact column per month",
                    )
                )
                continue
            members = tuple(
                record for month in range(1, 13) for record in months[month]
            )
            named_members.extend(members)
            identity_id = f"{ref}:{variant[-1]}:{year}:identity"
            representation_id = f"{ref}:{variant[-1]}:{year}:representations"
            expected = capture_expectations(
                members,
                fields=(
                    "column_name",
                    "data_type",
                    "data_length",
                    "operational_definition",
                    "name",
                ),
                coding=True,
            )
            guards = _matrix_repr_guards(members, identity_id)
            effects = tuple(
                effect
                for record in members
                for effect in (
                    CheckedIdentityChange(
                        ref=record_ref(record), variable_key=family_key
                    ),
                    CheckedFieldChange(
                        ref=record_ref(record),
                        replacement=FieldExpectation(
                            name="name", status="value", value=entry.label
                        ),
                    ),
                )
            )
            cases.append(
                CurationCase(
                    case_id=identity_id,
                    targets=expected,
                    peer_guards=guards,
                    decision=OccurrenceCorrectionDecision(
                        reviewed=True,
                        effects=effects,
                        reason="Reviewed month columns form one period family.",
                        provenance=ref,
                    ),
                )
            )
            cases.append(
                CurationCase(
                    case_id=representation_id,
                    targets=expected,
                    peer_guards=guards,
                    decision=RepresentationDecision(
                        reviewed=True,
                        variable_key=family_key,
                        variant_key=variant,
                        valid_from=f"{year}-01-01",
                        valid_to=f"{year}-12-31",
                        columns=tuple(
                            ColumnRepresentation(
                                column=cast(
                                    "str",
                                    _literal_field(months[month][0], "column_name"),
                                ),
                                valid_from=f"{year}-{month:02}-01",
                                valid_to=f"{year}-{month:02}-{monthrange(year, month)[1]:02}",
                            )
                            for month in range(1, 13)
                        ),
                        reason="Reviewed month columns provide the exact calendar windows.",
                        provenance=ref,
                    ),
                )
            )
        if named_members:
            expected = capture_expectations(
                tuple(named_members), fields=("column_name",)
            )
            names.append(
                NamingDeclaration(
                    target=NativeNamingTarget(
                        kind="variable",
                        provider=register.register_info.provider,
                        source_key=family_key,
                        register_key=source_register_key(named_members[0]),
                        expectations=expected,
                        peer_guards=_matrix_repr_guards(tuple(named_members), ref),
                    ),
                    naming=SlugEntry(
                        kind="variable",
                        provider=register.register_info.provider,
                        source_id=ref,
                        slug=entry.slug,
                    ),
                    contributors=(),
                )
            )
            keys.append(
                (
                    family_key,
                    f"period-family:{register.register_info.slug}:{entry.family_stem}",
                )
            )
    return tuple(cases), tuple(names), tuple(keys), tuple(diagnostics)


def compile_alias_windows(
    register: RegisterCuration,
    records: tuple[SourceRecord, ...],
    naming: tuple[NamingDeclaration, ...],
) -> tuple[tuple[CurationCase, ...], tuple[ResolutionDiagnostic, ...]]:
    """Resolve authored FQIDs against the names compiled for this register."""
    cases = []
    diagnostics = []
    provider = register.register_info.provider
    register_name = register.register_info.slug
    register_keys = {
        name.target.source_key
        for name in naming
        if name.target.kind == "register"
        and name.naming.provider == provider
        and name.naming.slug == register_name
    }
    for index, entry in enumerate(register.representation.alias_window, 1):
        ref = f"{register.source_file}#/representation.alias_window/{index}"
        parts = entry.variable.split("/")
        variable_names = [
            name
            for name in naming
            if name.target.kind == "variable"
            and name.target.register_key in register_keys
            and name.naming.slug == parts[-1]
            and name.naming.provider == provider
        ]
        variant_names = [
            name
            for name in naming
            if name.target.kind == "register_variant"
            and name.target.register_key in register_keys
            and name.naming.slug == entry.variant
            and name.naming.provider == provider
        ]
        variable_keys = {name.target.source_key for name in variable_names}
        variant_keys = {name.target.source_key for name in variant_names}
        if len(variable_keys) > 1 or len(variant_keys) > 1:
            diagnostics.append(
                _overbroad_matrix_repr(
                    ref,
                    entry.variable,
                    "variable FQID or variant slug resolves to multiple compiled keys",
                )
            )
            continue
        if (
            len(parts) != 3
            or parts[:2] != [provider, register_name]
            or len(variable_keys) != 1
            or len(variant_keys) != 1
        ):
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.variable,
                    "variable FQID or variant slug does not resolve to exactly one compiled key",
                )
            )
            continue
        variable_key = next(iter(variable_keys))
        variant_key = next(iter(variant_keys))
        expected_refs = {
            item.ref for name in variable_names for item in name.target.expectations
        }
        selected = tuple(
            record
            for record in records
            if native_variant_key(record) == variant_key
            and _edition_label(record) in entry.source_editions
            and (
                record_ref(record) in expected_refs
                if expected_refs
                else native_variable_key(record) == variable_key
            )
        )
        if not selected or {_edition_label(record) for record in selected} != set(
            entry.source_editions
        ):
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.variable,
                    "source editions have no exact compiled variable members",
                )
            )
            continue
        targets_by_edition: dict[
            str, set[tuple[SourceRecordRef, tuple[str | int, ...] | None, str | None]]
        ] = defaultdict(set)
        for record in selected:
            edition = _edition_label(record)
            if edition is not None:
                targets_by_edition[edition].add(
                    (
                        record_ref(record),
                        native_variable_key(record),
                        _literal_field(record, "column_name"),
                    )
                )
        if any(len(targets) != 1 for targets in targets_by_edition.values()):
            diagnostics.append(
                _overbroad_matrix_repr(
                    ref,
                    entry.variable,
                    "source edition matches multiple record or column targets",
                )
            )
            continue
        years = {_curation_year(record) for record in selected}
        if None in years or len(years) != len(entry.source_editions):
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.variable,
                    "source editions do not establish distinct annual windows",
                )
            )
            continue
        years = sorted(cast("set[int]", years))
        if years != list(range(years[0], years[-1] + 1)):
            diagnostics.append(
                _stale_partition(
                    ref, entry.variable, "source edition windows are not contiguous"
                )
            )
            continue
        expected = capture_expectations(selected, fields=("column_name",), coding=True)
        cases.append(
            CurationCase(
                case_id=ref,
                targets=expected,
                peer_guards=_matrix_repr_guards(selected, ref),
                decision=AliasWindowDecision(
                    reviewed=True,
                    variable_key=variable_key,
                    variant_key=variant_key,
                    column=entry.column,
                    valid_from=f"{years[0]}-01-01",
                    valid_to=f"{years[-1]}-12-31",
                    reason=entry.evidence,
                    provenance=ref,
                ),
            )
        )
    return tuple(cases), tuple(diagnostics)


def compile_matrix_repr(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[ScopeDeclarations, ...],
    naming: dict[Any, tuple[NamingDeclaration, ...]],
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    dict[Any, tuple[NamingDeclaration, ...]],
    dict[Any, tuple[tuple[tuple[str | int, ...], str], ...]],
    tuple[ResolutionDiagnostic, ...],
]:
    """Replace the final stored cases with tracked, checked declarations."""
    registers = {
        f"{item.register_info.provider}/{item.register_info.slug}": item
        for item in tree.registers
    }
    if not any(
        name in registers
        and (
            registers[name].representation.period_family
            or registers[name].representation.alias_window
            or name == "scb/innovation-foretag"
        )
        for scope in scopes
        for name, _ in _scope_registers(scope)
    ):
        return {}, {}, {}, ()
    cases: dict[Any, list[CurationCase]] = defaultdict(list)
    names: dict[Any, list[NamingDeclaration]] = defaultdict(list)
    keys: dict[Any, list[tuple[tuple[str | int, ...], str]]] = defaultdict(list)
    diagnostics = []
    matrices = (
        load_cis2014_matrix(
            tree.root
            / "registers/scb/innovation-foretag/cis2014-matrix-meaning-evidence.json"
        ),
        load_cis2016_matrix(
            tree.root
            / "registers/scb/innovation-foretag/cis2016-matrix-meaning-evidence.json"
        ),
    )
    with open_value_bindings(prepared.value_sources) as sessions:
        for scope in sorted(
            scopes, key=lambda item: (item.source, repr(item.register_key))
        ):
            scope_key = scope.source, scope.register_key
            selected_registers = tuple(
                (registers[name], native)
                for name, native in _scope_registers(scope)
                if name in registers
                and (
                    registers[name].representation.period_family
                    or registers[name].representation.alias_window
                    or name == "scb/innovation-foretag"
                )
            )
            if not selected_registers:
                continue
            wanted = {native for _, native in selected_registers}
            if scope.register_key is None:
                grouped: dict[tuple[str | int, ...], list[SourceRecord]] = defaultdict(
                    list
                )
                for record in prepared.records.iter_records(source=scope.source):
                    if (key := source_register_key(record)) in wanted:
                        grouped[key].append(record)
                by_register = {key: tuple(value) for key, value in grouped.items()}
            else:
                by_register = dict(
                    prepared.records.iter_register_slices(scope.source, wanted)
                )
            for register, native in selected_registers:
                records = by_register.get(native, ())
                period_cases, period_names, period_keys, issues = (
                    compile_period_families(register, records)
                )
                cases[scope_key].extend(period_cases)
                names[scope_key].extend(period_names)
                keys[scope_key].extend(period_keys)
                diagnostics.extend(issues)
                if (
                    register.register_info.slug == "innovation-foretag"
                    and register.register_info.provider == "scb"
                ):
                    for matrix in matrices:
                        if matrix is None:
                            continue
                        ref = (
                            f"{register.source_file}#/matrix/{matrix.selector.edition}"
                        )
                        if native[-1] != matrix.selector.register_id:
                            diagnostics.append(
                                _stale_partition(
                                    ref,
                                    matrix.selector.edition,
                                    "matrix register selector changed",
                                )
                            )
                            continue
                        coding = None
                        if isinstance(matrix, Cis2014Matrix):
                            donors = tuple(
                                record
                                for record in records
                                if record.subject.native.edition_id
                                == matrix.selector.regver_id
                                and record.subject.native.variable_id
                                == matrix.selector.var_id
                                and record.subject.native.member_id
                                == matrix.selector.cvid
                            )
                            if len({record_ref(record) for record in donors}) != 1:
                                diagnostics.append(
                                    _stale_partition(
                                        ref,
                                        matrix.selector.edition,
                                        "blank matrix has no unique semantic donor",
                                    )
                                )
                                continue
                            bound = bind_code_lists(donors[0], sessions)
                            if (
                                bound.issues
                                or len(bound.claims) != 1
                                or not bound.claims[0].members
                            ):
                                diagnostics.append(
                                    _stale_partition(
                                        ref,
                                        matrix.selector.edition,
                                        "blank matrix donor lacks one complete list",
                                    )
                                )
                                continue
                            coding = {record_ref(donors[0]): bound.claims}
                        try:
                            converted = convert_matrix(
                                matrix,
                                records,
                                case_id=ref,
                                provenance=ref,
                                coding=coding,
                            )
                        except ValueError as exc:
                            diagnostics.append(
                                _stale_partition(ref, matrix.selector.edition, str(exc))
                            )
                            continue
                        cases[scope_key].append(converted.case)
                        names[scope_key].extend(converted.naming)
                        keys[scope_key].extend(converted.provider_keys.items())
                alias_cases, alias_issues = compile_alias_windows(
                    register,
                    records,
                    (*naming.get(scope_key, ()), *names[scope_key]),
                )
                cases[scope_key].extend(alias_cases)
                diagnostics.extend(alias_issues)
    return (
        {key: tuple(value) for key, value in cases.items()},
        {key: tuple(value) for key, value in names.items()},
        {key: tuple(value) for key, value in keys.items()},
        tuple(diagnostics),
    )


def _literal_field(record: SourceRecord, name: str) -> str | None:
    field = getattr(record.fields, name)
    return (
        field.value
        if field is not None
        and field.status == "value"
        and isinstance(field.value, str)
        else None
    )


def _partition_ambiguity(
    native: tuple[str | int, ...],
    records: tuple[SourceRecord, ...],
    entries: tuple[AcceptedNamingEntry, ...],
    split_ids: tuple[str, ...],
    expectations: tuple[Any, ...],
    guard: PeerGuard,
) -> NamingAmbiguity:
    columns = {
        column
        for record in records
        if (column := _literal_field(record, "column_name")) is not None
    }
    return NamingAmbiguity(
        family=NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=native,
            register_key=native[:5],
            expectations=expectations,
            peer_guards=(guard,),
        ),
        entries=entries,
        candidate_columns=tuple(
            sorted(
                (split, column)
                for split in split_ids
                for column in columns
                if derive_variable_slug(column) == split.rsplit(".", 1)[1]
            )
        ),
        reason="Some accepted split keys lack exact literal ownership.",
    )


def compile_partitions(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[ScopeDeclarations, ...],
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    dict[Any, tuple[Any, ...]],
    dict[Any, tuple[Any, ...]],
    dict[Any, tuple[NamingAmbiguity, ...]],
    dict[Any, set[tuple[str | int, ...]]],
    tuple[ResolutionDiagnostic, ...],
]:
    """Convert accepted native splits and SOS shape/name decisions."""
    scope_map = {(scope.source, scope.register_key): scope for scope in scopes}
    registers = {}
    entries_by_register = {}
    all_entries_by_register = {}
    for scope_key, scope in scope_map.items():
        for name, native_register in _scope_registers(scope):
            register = next(
                (
                    item
                    for item in tree.registers
                    if name
                    == f"{item.register_info.provider}/{item.register_info.slug}"
                ),
                None,
            )
            if register is None:
                continue
            registers[scope.source, native_register] = scope_key, register
            all_entries_by_register[scope.source, native_register] = tuple(
                item
                for item, _ in _register_naming_entries(tree, register)
                if item.entry.kind == "variable"
            )
            entries_by_register[scope.source, native_register] = tuple(
                item
                for item in all_entries_by_register[scope.source, native_register]
                if len(item.entry.source_id.split(".")) == 3
            )
    cases = defaultdict(list)
    bindings = defaultdict(list)
    bound_entries = defaultdict(list)
    ambiguities = defaultdict(list)
    split_bases = defaultdict(set)
    null_bases = defaultdict(set)
    diagnostics = []
    seen = set()
    named_rename_owners = set()
    for source in sorted({scope.source for scope in scopes}):
        for native, records in prepared.records.iter_native_families(source):
            location = registers.get((source, native[:5]))
            if location is None:
                continue
            scope_key, register = location
            source_id = f"{register.register_info.native_id}.{native[-1]}"
            entries = tuple(
                item
                for item in entries_by_register[source, native[:5]]
                if item.entry.source_id.startswith(source_id + ".")
            )
            partitions = [
                (i, item)
                for i, item in enumerate(register.identity.partition, 1)
                if item.variable == source_id
            ]
            scoped_entries = [
                (i, item)
                for i, item in enumerate(register.identity.column_owner, 1)
                if item.variable == source_id
            ]
            sos_splits = [
                (i, item)
                for i, item in enumerate(register.identity.split, 1)
                if item.variable == str(native[-1])
            ]
            sos_renames = [
                (i, item)
                for i, item in enumerate(register.identity.rename, 1)
                if item.variable == str(native[-1])
            ]
            if not (
                entries or partitions or scoped_entries or sos_splits or sos_renames
            ):
                continue
            seen.add((source, native[:5], source_id))
            expectations = capture_expectations(
                records,
                fields=("column_name",)
                if native[1] == "scb"
                else ("column_name", "name", "data_type"),
            )
            guard = PeerGuard(
                guard_id=f"accepted-partitions:{source}:{source_id}",
                source=source,
                coordinates=(
                    ("register", records[0].subject.register_name),
                    ("variable", records[0].subject.variable),
                ),
                expected_members=tuple(item.ref for item in expectations),
            )
            if native[1] == "scb":
                if not entries:
                    for i, _ in partitions:
                        diagnostics.append(
                            _stale_partition(
                                f"{register.source_file}#/identity.partition/{i}",
                                source_id,
                                "partition has no active split naming",
                            )
                        )
                    for i, _ in scoped_entries:
                        diagnostics.append(
                            _stale_partition(
                                f"{register.source_file}#/identity.column_owner/{i}",
                                source_id,
                                "column owner has no active split naming",
                            )
                        )
                    continue
                split_ids = tuple(sorted({item.entry.source_id for item in entries}))
                split_bases[scope_key].add(native)
                scoped = {}
                for i, owner in scoped_entries:
                    ref = f"{register.source_file}#/identity.column_owner/{i}"
                    matched = tuple(
                        record
                        for record in records
                        if f"{register.register_info.native_id}.{record.subject.variant.native_id}"
                        == owner.variant
                        and record.fields.column_name is not None
                        and record.fields.column_name.status == "value"
                        and record.fields.column_name.value == owner.column
                    )
                    if not matched or owner.owner not in split_ids:
                        diagnostics.append(
                            _stale_partition(
                                ref, source_id, "owner matches no named split member"
                            )
                        )
                    else:
                        scoped.update(
                            {record_ref(record): owner.owner for record in matched}
                        )
                if len(partitions) > 1:
                    raise ValueError(
                        f"{register.source_file}: duplicate partition map for {source_id}"
                    )
                declared = None
                reference = None
                if partitions:
                    declaration = partitions[0][1]
                    declared = {
                        **declaration.columns,
                        **dict.fromkeys(declaration.unassigned_columns),
                    }
                    reference = declaration.columns_ref
                try:
                    converted = convert_column_partitions(
                        records,
                        source_id=source_id,
                        split_ids=split_ids,
                        declared_columns=declared,
                        declaration_reference=reference,
                        scoped_owners=scoped,
                    )
                except ValueError as exc:
                    ref = (
                        f"{register.source_file}#/identity.partition/{partitions[0][0]}"
                        if partitions
                        else register.source_file
                    )
                    diagnostics.append(_stale_partition(ref, source_id, str(exc)))
                    null_bases[scope_key].add(native)
                    converted = ColumnPartitionConversion((), None, ())
                if partitions:
                    diagnostics.extend(converted.diagnostics)
                if converted.case is not None:
                    cases[scope_key].append(converted.case)
                    if converted.case.support:
                        null_bases[scope_key].add(native)
                else:
                    null_bases[scope_key].add(native)
                bindings[scope_key].extend(converted.bindings)
                bound = {item.source_id for item in converted.bindings}
                bound_entries[scope_key].extend(
                    item for item in entries if item.entry.source_id in bound
                )
                if bound != set(split_ids):
                    ambiguities[scope_key].append(
                        _partition_ambiguity(
                            native, records, entries, split_ids, expectations, guard
                        )
                    )
            elif native[1] == "sos":
                if len(sos_splits) + len(sos_renames) != 1:
                    raise ValueError(
                        f"{register.source_file}: expected one SOS identity declaration for {source_id}"
                    )
                is_split = bool(sos_splits)
                i, declaration = (sos_splits or sos_renames)[0]
                ref = f"{register.source_file}#/identity.{'split' if is_split else 'rename'}/{i}"
                if is_split:
                    declaration = cast("IdentitySplitEntry", declaration)
                    split_bases[scope_key].add(native)
                    owners = {part.data_type: part.owner for part in declaration.parts}
                    actual = {
                        value
                        for record in records
                        if (value := _literal_field(record, "data_type")) is not None
                    }
                    if (
                        actual != set(owners)
                        or len(owners) != len(declaration.parts)
                        or any(
                            _literal_field(record, "data_type") is None
                            for record in records
                        )
                    ):
                        diagnostics.append(
                            _stale_partition(
                                ref,
                                source_id,
                                f"data types {sorted(actual)!r} do not equal declared {sorted(owners)!r}",
                            )
                        )
                        null_bases[scope_key].add(native)
                        continue
                    effects = tuple(
                        CheckedIdentityChange(
                            ref=record_ref(record),
                            variable_key=(
                                *native,
                                "accepted-shape",
                                cast("str", _literal_field(record, "data_type")),
                            ),
                            when=(
                                FieldExpectation(
                                    name="data_type",
                                    status="value",
                                    value=cast(
                                        "str", _literal_field(record, "data_type")
                                    ),
                                ),
                            ),
                        )
                        for record in records
                    )
                else:
                    declaration = cast("IdentityRenameEntry", declaration)
                    owners = {
                        declaration.column: f"{register.register_info.native_id}.{declaration.column}"
                    }
                    named_rename_owners.add(
                        (source, native[:5], next(iter(owners.values())))
                    )
                    selected = tuple(
                        record
                        for record in records
                        if record.subject.variant.name == declaration.deldatamangd
                        and record.fields.name is not None
                        and record.fields.name.status == "value"
                        and record.fields.name.value == declaration.name
                    )
                    if not selected:
                        diagnostics.append(
                            _stale_partition(
                                ref, source_id, "rename matches no declared subset name"
                            )
                        )
                        continue
                    condition = (
                        FieldExpectation(
                            name="name", status="value", value=declaration.name
                        ),
                    )
                    effects = tuple(
                        effect
                        for record in selected
                        for effect in (
                            CheckedFieldChange(
                                ref=record_ref(record),
                                replacement=FieldExpectation(
                                    name="column_name",
                                    status="value",
                                    value=declaration.column,
                                ),
                                when=condition,
                            ),
                            CheckedIdentityChange(
                                ref=record_ref(record),
                                variable_key=(
                                    *native,
                                    "accepted-name",
                                    declaration.column,
                                ),
                                when=condition,
                            ),
                        )
                    )
                owner_entries = tuple(
                    item
                    for item in all_entries_by_register[source, native[:5]]
                    if item.entry.source_id in owners.values()
                )
                if set(owners.values()) - {
                    item.entry.source_id for item in owner_entries
                }:
                    diagnostics.append(
                        _stale_partition(
                            ref, source_id, "declared owner lacks a tracked split name"
                        )
                    )
                    if is_split:
                        null_bases[scope_key].add(native)
                    continue
                selected_refs = {effect.ref for effect in effects}
                cases[scope_key].append(
                    CurationCase(
                        case_id=f"accepted-sos-identity:{register.register_info.slug}:{native[-1]}",
                        targets=tuple(
                            item for item in expectations if item.ref in selected_refs
                        ),
                        support=tuple(
                            item
                            for item in expectations
                            if item.ref not in selected_refs
                        ),
                        peer_guards=(guard,),
                        decision=OccurrenceCorrectionDecision(
                            reviewed=True,
                            effects=effects,
                            reason="Preserve the accepted SOS identity decision.",
                            provenance=ref,
                        ),
                    )
                )
                for value, owner in sorted(owners.items()):
                    key = (
                        *native,
                        "accepted-shape" if is_split else "accepted-name",
                        value,
                    )
                    bindings[scope_key].append(
                        LegacyNamingBinding(
                            kind="variable",
                            provider="sos",
                            source_id=owner,
                            target=NativeNamingTarget(
                                kind="variable",
                                provider="sos",
                                source_key=key,
                                register_key=native[:5],
                                expectations=expectations,
                                peer_guards=(guard,),
                            ),
                        )
                    )
                    bound_entries[scope_key].extend(
                        item for item in owner_entries if item.entry.source_id == owner
                    )
    for (source, register_key), entries in entries_by_register.items():
        for source_id in sorted(
            {item.entry.source_id.rsplit(".", 1)[0] for item in entries}
        ):
            if (source, register_key, source_id) not in seen and (
                source,
                register_key,
                source_id,
            ) not in named_rename_owners:
                diagnostics.append(
                    _stale_partition(
                        next(
                            item.entry_id
                            for item in entries
                            if item.entry.source_id.startswith(source_id + ".")
                        ),
                        source_id,
                        "split naming matches no native family",
                    )
                )
    for (source, register_key), (_scope_key, register) in registers.items():
        for table, items in (
            ("identity.partition", register.identity.partition),
            ("identity.column_owner", register.identity.column_owner),
            ("identity.split", register.identity.split),
            ("identity.rename", register.identity.rename),
        ):
            for index, item in enumerate(items, 1):
                source_id = (
                    item.variable
                    if table in {"identity.partition", "identity.column_owner"}
                    else f"{register.register_info.native_id}.{item.variable}"
                )
                if (source, register_key, source_id) not in seen:
                    diagnostics.append(
                        _stale_partition(
                            f"{register.source_file}#/{table}/{index}",
                            source_id,
                            "identity declaration matches no native family",
                        )
                    )
    naming = {}
    provider_keys = {}
    for scope_key in scope_map:
        entries = tuple(bound_entries[scope_key])
        conversion = convert_naming(
            NamingSelection(
                files=(),
                entries=entries,
                freeze=tuple(
                    NamingFreezeSetting(zone=provider, state="curating")
                    for provider in sorted(
                        {str(item.entry.provider) for item in entries}
                    )
                ),
            ),
            bindings[scope_key],
        )
        diagnostics.extend(conversion.diagnostics)
        naming[scope_key] = tuple(
            sorted(
                conversion.declarations,
                key=lambda item: (item.target.kind, repr(item.target.source_key)),
            )
        )
        keys = {
            item.target.source_key: item.naming.source_id.split(".", 1)[1]
            for item in conversion.declarations
            if item.target.kind == "variable"
        }
        keys.update(dict.fromkeys(null_bases[scope_key]))
        provider_keys[scope_key] = tuple(
            sorted(keys.items(), key=lambda item: repr(item[0]))
        )
    return (
        {
            key: tuple(sorted(value, key=lambda item: item.case_id))
            for key, value in cases.items()
        },
        naming,
        provider_keys,
        {key: tuple(value) for key, value in ambiguities.items()},
        split_bases,
        tuple(diagnostics),
    )


def _partition_owned_naming_entry(
    register: RegisterCuration, entry: AcceptedNamingEntry
) -> bool:
    if entry.entry.kind != "variable":
        return False
    source_id = entry.entry.source_id
    return (
        len(source_id.split(".")) == 3
        or any(
            source_id == f"{register.register_info.native_id}.{rename.column}"
            for rename in register.identity.rename
        )
        or any(
            source_id == f"{register.register_info.native_id}.{column.column}"
            for column in register.errata.column
        )
    )


def compile_native_naming(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[ScopeDeclarations, ...],
    *,
    subset: bool,
) -> tuple[
    dict[Any, tuple[Any, ...]],
    dict[Any, tuple[Any, ...]],
    dict[Any, tuple[Any, ...]],
    tuple[ResolutionDiagnostic, ...],
    dict[str, dict[str, list[str]]],
]:
    """Bind tracked register slugs to exact native families and parent facts."""
    scope_map = {(scope.source, scope.register_key): scope for scope in scopes}
    bindings: dict[Any, list[LegacyNamingBinding]] = {key: [] for key in scope_map}
    for source in sorted({scope.source for scope in scopes}):
        for family_key, members in prepared.records.iter_native_families(source):
            scope_key = (source, family_key[:5])
            if scope_key not in scope_map:
                scope_key = (source, None)
            if scope_key not in scope_map or family_key[-2] not in {
                "native-int",
                "native-str",
            }:
                continue
            provider = str(family_key[1])
            expectations = (
                capture_expectations(members, fields=())
                if provider not in {"scb", "sos"}
                else ()
            )
            guards = (
                (
                    PeerGuard(
                        guard_id=f"thin:{source}:{family_key!r}",
                        source=source,
                        coordinates=(
                            ("register", members[0].subject.register_name),
                            ("variable", members[0].subject.variable),
                        ),
                        expected_members=tuple(item.ref for item in expectations),
                    ),
                )
                if expectations
                else ()
            )
            target = NativeNamingTarget(
                kind="variable",
                provider=provider,
                source_key=family_key,
                register_key=family_key[:5],
                expectations=expectations,
                peer_guards=guards,
            )
            bindings[scope_key].append(
                LegacyNamingBinding(
                    kind="variable",
                    provider=target.provider,
                    source_id=_naming_source_id("variable", family_key),
                    target=target,
                )
            )
        wanted = {scope.register_key for scope in scopes if scope.source == source}
        slices = (
            ((None, tuple(prepared.records.iter_records(source=source))),)
            if None in wanted
            else prepared.records.iter_register_slices(source, wanted)
        )
        for register_key, records in slices:
            scope_key = source, register_key
            if scope_key not in scope_map:
                continue
            parents: dict[tuple[str, tuple[str | int, ...]], LegacyNamingBinding] = {}
            explicit_variants: set[tuple[str | int, ...]] = set()
            defaults: dict[
                tuple[str | int, ...], tuple[str, tuple[str | int, ...]]
            ] = {}
            for record in records:
                for parent in record.parent_facts:
                    if parent.kind not in {"register", "variant"}:
                        continue
                    native_key = native_parent_key(
                        record.source, record.subject.provider, parent
                    )
                    if native_key is None:
                        continue
                    kind = (
                        "register" if parent.kind == "register" else "register_variant"
                    )
                    if kind == "register_variant":
                        explicit_variants.add(native_key[:5])
                    target = NativeNamingTarget(
                        kind=kind,
                        provider=record.subject.provider,
                        source_key=native_key,
                        register_key=None if kind == "register" else native_key[:5],
                    )
                    member = None
                    if kind == "register_variant" and record.subject.provider != "scb":
                        coordinate = parent.variant
                        member = (
                            coordinate.name
                            if coordinate is not None
                            and record.subject.provider == "sos"
                            else str(coordinate.native_id)
                            if coordinate is not None
                            and coordinate.native_id is not None
                            else None
                        )
                        if not member:
                            raise ValueError(
                                f"variant parent lacks its native naming member: {native_key!r}"
                            )
                    parents[kind, native_key] = LegacyNamingBinding(
                        kind=kind,
                        provider=target.provider,
                        source_id=_naming_source_id(kind, native_key, member=member),
                        target=target,
                    )
                native_register = source_register_key(record)
                if (
                    native_register is not None
                    and record.subject.variant.status == "not_applicable"
                ):
                    defaults[native_register] = (
                        record.subject.provider,
                        native_register,
                    )
            for native_register, (provider, _) in defaults.items():
                if native_register in explicit_variants or (
                    provider == "scb"
                    and not (
                        source == "scb_canonical" or source.startswith("scb_canonical/")
                    )
                ):
                    continue
                default_key = (*native_register, "variant", "not-applicable")
                target = NativeNamingTarget(
                    kind="register_variant",
                    provider=provider,
                    source_key=default_key,
                    register_key=native_register,
                )
                parents["register_variant", default_key] = LegacyNamingBinding(
                    kind="register_variant",
                    provider=provider,
                    source_id=_naming_source_id(
                        "register_variant", default_key, member="_default"
                    ),
                    target=target,
                )
            bindings[scope_key].extend(parents.values())
    compiled_names = {}
    compiled_variants = {}
    compiled_provider_keys = {}
    diagnostics = []
    report: dict[str, dict[str, list[str]]] = {}
    register_scopes: dict[str, list[tuple[str, tuple[str | int, ...] | None]]] = {}
    for scope_key, scope in scope_map.items():
        for name, _ in _scope_registers(scope):
            register_scopes.setdefault(name, []).append(scope_key)
    for register in sorted(tree.registers, key=lambda item: item.source_file):
        name = f"{register.register_info.provider}/{register.register_info.slug}"
        matches = register_scopes.get(name, ())
        if len(matches) == 1:
            continue
        status = (
            "over_broad"
            if len(matches) > 1
            else "not_evaluated_in_subset"
            if subset
            else "stale"
        )
        statuses = report.setdefault(
            name,
            {
                key: []
                for key in (
                    "entries_read",
                    "entries_matched",
                    "stale",
                    "over_broad",
                    "not_evaluated",
                    "not_evaluated_in_subset",
                )
            },
        )
        for entry, where in _register_naming_entries(tree, register):
            if _partition_owned_naming_entry(register, entry):
                continue
            ref = f"{entry.revision.artifact_path} {where}"
            statuses["entries_read"].append(ref)
            statuses[status].append(ref)
            if status != "not_evaluated_in_subset":
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="overbroad_curation_entry"
                        if matches
                        else "stale_curation_entry",
                        severity="error",
                        case_id=ref,
                        subject=entry.entry.source_id,
                        detail=f"{ref} matches {len(matches)} register scopes; expected one",
                        withheld_output=(ref,),
                    )
                )
    for scope_key, scope in sorted(scope_map.items(), key=lambda item: repr(item[0])):
        selected = {name for name, _ in _scope_registers(scope)}
        entries = []
        locations = {}
        entry_owners = {}
        for register in sorted(tree.registers, key=lambda item: item.source_file):
            name = f"{register.register_info.provider}/{register.register_info.slug}"
            if name not in selected or len(register_scopes[name]) != 1:
                continue
            statuses = report.setdefault(
                name,
                {
                    key: []
                    for key in (
                        "entries_read",
                        "entries_matched",
                        "stale",
                        "over_broad",
                        "not_evaluated",
                        "not_evaluated_in_subset",
                    )
                },
            )
            for entry, where in _register_naming_entries(tree, register):
                if _partition_owned_naming_entry(register, entry):
                    continue
                ref = f"{entry.revision.artifact_path} {where}"
                statuses["entries_read"].append(ref)
                entries.append(entry)
                locations[entry.entry_id] = ref
                entry_owners[entry.entry_id] = name
        bound = {(b.kind, b.provider, b.source_id) for b in bindings[scope_key]}
        supplied = {
            (entry.entry.kind, entry.entry.provider, entry.entry.source_id)
            for entry in entries
        }
        unbound = supplied - bound
        uncompiled = set()
        for item in scope.naming:
            token = item.naming.kind, item.naming.provider, item.naming.source_id
            if token in unbound and _naming_family(item.target) not in COMPILED:
                uncompiled.add(token)
        for binding in bindings[scope_key]:
            if binding.target.source_key[-2:] != (
                "variant",
                "not-applicable",
            ):
                continue
            token = binding.kind, binding.provider, binding.source_id
            if token not in supplied:
                ref = f"{binding.provider}/{binding.source_id} default variant"
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="stale_curation_entry",
                        severity="error",
                        case_id=ref,
                        subject=binding.source_id,
                        detail=f"{ref} has no tracked default slug entry",
                        withheld_output=(ref,),
                    )
                )
        matched = []
        for entry in entries:
            ref = locations[entry.entry_id]
            token = entry.entry.kind, entry.entry.provider, entry.entry.source_id
            if token in bound:
                matched.append(entry)
                report[entry_owners[entry.entry_id]]["entries_matched"].append(ref)
            elif token in uncompiled:
                report[entry_owners[entry.entry_id]]["not_evaluated"].append(ref)
            else:
                name = entry_owners[entry.entry_id]
                report[name]["stale"].append(ref)
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="stale_curation_entry",
                        severity="error",
                        case_id=ref,
                        subject=entry.entry.source_id,
                        detail=f"{ref} binds no native family or parent",
                        withheld_output=(ref,),
                    )
                )
        conversion = convert_naming(
            NamingSelection(
                files=(),
                entries=tuple(matched),
                freeze=tuple(
                    NamingFreezeSetting(zone=provider, state="curating")
                    for provider in sorted(
                        {str(entry.entry.provider) for entry in matched}
                    )
                ),
            ),
            bindings[scope_key],
        )
        diagnostics.extend(conversion.diagnostics)
        compiled_names[scope_key] = tuple(
            sorted(
                conversion.declarations,
                key=lambda item: (item.target.kind, repr(item.target.source_key)),
            )
        )
        compiled_provider_keys[scope_key] = tuple(
            sorted(
                native_provider_keys(
                    (
                        binding.target.source_key
                        for binding in bindings[scope_key]
                        if binding.kind == "variable"
                    ),
                    conversion.declarations,
                ).items(),
                key=lambda item: repr(item[0]),
            )
        )
        variants = []
        for declaration in conversion.declarations:
            target = declaration.target
            if target.kind != "register_variant" or target.source_key[-2:] != (
                "variant",
                "not-applicable",
            ):
                continue
            slug = declaration.naming.slug
            if slug is None:
                continue
            variants.append(
                (
                    target.source_key,
                    ResolvedVariant.model_validate(
                        {
                            "slug": slug,
                            "name": "_default",
                            "description": None,
                            "display_group": declaration.naming.display_group,
                            "panel_entity_key": declaration.naming.panel_entity_key,
                            "panel_time_key": declaration.naming.panel_time_key,
                            "panel_time_grain": declaration.naming.panel_time_grain,
                        }
                    ),
                )
            )
        compiled_variants[scope_key] = tuple(
            sorted(variants, key=lambda item: repr(item[0]))
        )
    return (
        compiled_names,
        compiled_variants,
        compiled_provider_keys,
        tuple(diagnostics),
        report,
    )


def _source_text(field: Any) -> str | None:
    return (
        field.value
        if field is not None
        and field.status == "value"
        and isinstance(field.value, str)
        else None
    )


def compile_sos_thin(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[ScopeDeclarations, ...],
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    tuple[ResolutionDiagnostic, ...],
    dict[str, dict[str, list[str]]],
]:
    """Compile SOS routing and authored coverage from selected original records."""
    registers = {
        f"{entry.register_info.provider}/{entry.register_info.slug}": entry
        for entry in tree.registers
    }
    cases: dict[Any, list[CurationCase]] = {}
    diagnostics: list[ResolutionDiagnostic] = []
    report: dict[str, dict[str, list[str]]] = {}
    if not any(
        name in registers and registers[name].register_info.provider != "scb"
        for scope in scopes
        for name, _ in _scope_registers(scope)
    ):
        return {}, (), {}
    with open_value_bindings(prepared.value_sources) as sessions:
        for scope in sorted(
            scopes, key=lambda item: (item.source, repr(item.register_key))
        ):
            scope_key = scope.source, scope.register_key
            wanted = tuple(
                sorted(
                    (
                        (registers[name], key)
                        for name, key in _scope_registers(scope)
                        if name in registers
                        and registers[name].register_info.provider != "scb"
                    ),
                    key=lambda item: item[0].source_file,
                )
            )
            if not wanted:
                continue
            if scope.register_key is None:
                records = tuple(prepared.records.iter_records(source=scope.source))
            else:
                records = tuple(
                    record
                    for _, members in prepared.records.iter_register_slices(
                        scope.source, (scope.register_key,)
                    )
                    for record in members
                )
            for register, register_key in wanted:
                selected = tuple(
                    record
                    for record in records
                    if source_register_key(record) == register_key
                )
                provider = register.register_info.provider
                if not selected and provider != "sos":
                    continue
                if provider == "sos":
                    new_cases, issues, statuses = _compile_sos_register(
                        register, selected
                    )
                    diagnostics.extend(issues)
                    report[f"sos/{register.register_info.slug}"] = statuses
                else:
                    new_cases = _compile_thin_register(selected, sessions)
                cases.setdefault(scope_key, []).extend(new_cases)
    return (
        {
            key: tuple(sorted(value, key=lambda case: case.case_id))
            for key, value in cases.items()
        },
        tuple(diagnostics),
        report,
    )


def _compile_sos_register(
    register: RegisterCuration, records: tuple[SourceRecord, ...]
) -> tuple[
    tuple[CurationCase, ...], tuple[ResolutionDiagnostic, ...], dict[str, list[str]]
]:
    from collections import defaultdict

    statuses = {
        key: []
        for key in (
            "entries_read",
            "entries_matched",
            "stale",
            "over_broad",
            "not_evaluated_in_subset",
        )
    }
    parent_by_name: dict[str, set[NativeKey]] = defaultdict(set)
    lookup_signals: dict[str, list[bool]] = defaultdict(list)
    lookup_records = []
    diagnostics = []
    has_variant_parent = False
    for record in records:
        for parent in record.parent_facts:
            if parent.kind != "variant":
                continue
            has_variant_parent = True
            if parent.variant is None or not parent.variant.name:
                continue
            name = parent.variant.name
            key = native_parent_key(record.source, "sos", parent)
            if key is not None:
                parent_by_name[name].add(key)
            label = _source_text(parent.fields.name)
            level = _source_text(parent.fields.aggregation_level)
            named = label is not None and label.casefold().startswith("styrtabell")
            aggregated = level is not None and level.casefold() == "ej relevant"
            if named != aggregated:
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="invalid_sos_lookup_signals",
                        severity="error",
                        subject=name,
                        detail=f"{record.source}: variant {name!r} has only one styrtabell lookup signal",
                        refs=(record_ref(record),),
                        withheld_output=(name,),
                    )
                )
            lookup_signals[name].append(named and aggregated)
            if named and aggregated:
                lookup_records.append(record)
    lookup_names = {
        name for name, signals in lookup_signals.items() if signals and all(signals)
    }
    routes = {}
    for index, entry in enumerate(register.identity.route, 1):
        if entry.deldatamangd in routes:
            raise ValueError(
                f"{register.source_file}#/identity.route/{index}: duplicate Deldatamängd token {entry.deldatamangd!r}"
            )
        routes[entry.deldatamangd] = tuple(entry.variants)
    matched = set()
    source_use_records = [
        record
        for record in lookup_records
        if record.subject.variant.name in lookup_names
    ]
    variables = tuple(
        record for record in records if native_variable_key(record) is not None
    )
    for record in variables:
        token = record.subject.variant.name
        names = routes.get(token, (token,) if token is not None else ())
        if token in routes:
            matched.add(token)
        if names and all(name in lookup_names for name in names):
            source_use_records.append(record)
    source_use_refs = {record_ref(record) for record in source_use_records}
    cases = []
    if source_use_records:
        all_refs = tuple(sorted({record_ref(record) for record in records}, key=str))
        targets = capture_expectations(tuple(source_use_records), fields=())
        first = records[0]
        case_id = f"existing-source-use:{first.source}"
        cases.append(
            CurationCase(
                case_id=case_id,
                targets=targets,
                peer_guards=(
                    PeerGuard(
                        guard_id=case_id,
                        source=first.source,
                        coordinates=(("register", first.subject.register_name),),
                        expected_members=all_refs,
                    ),
                ),
                decision=OccurrenceCorrectionDecision(
                    reviewed=True,
                    effects=tuple(
                        CheckedSourceUse(ref=target.ref) for target in targets
                    ),
                    reason="Lookup-table records are source support.",
                    provenance=register.source_file,
                ),
            )
        )
    families: dict[NativeKey, list[SourceRecord]] = defaultdict(list)
    for record in variables:
        key = native_variable_key(record)
        assert key is not None
        families[key].append(record)
    for family_key, family in sorted(families.items(), key=lambda item: repr(item[0])):
        effects = {}
        for record in family:
            ref = record_ref(record)
            if ref in source_use_refs:
                continue
            token = record.subject.variant.name
            if not has_variant_parent:
                register_key = source_register_key(record)
                assert register_key is not None
                variant_keys = ((*register_key, "variant", "not-applicable"),)
            elif token in routes:
                variant_keys = tuple(
                    key
                    for name in routes[token]
                    for key in sorted(parent_by_name.get(name, ()), key=repr)
                )
            else:
                continue
            own_variant = native_variant_key(record)
            variant_keys = tuple(key for key in variant_keys if key != own_variant)
            if not variant_keys:
                continue
            effect = CheckedVariantAssignment(ref=ref, variant_keys=variant_keys)
            if ref in effects and effects[ref] != effect:
                raise ValueError(
                    f"{record.source}: one SOS record has conflicting routes"
                )
            effects[ref] = effect
        if not effects:
            continue
        case_id = f"accepted-sos-routes:{register.register_info.slug}:{family_key[-1]}"
        expectations = capture_expectations(tuple(family), fields=())
        targets = tuple(item for item in expectations if item.ref in effects)
        cases.append(
            CurationCase(
                case_id=case_id,
                targets=targets,
                support=tuple(item for item in expectations if item not in targets),
                peer_guards=(
                    PeerGuard(
                        guard_id=case_id,
                        source=family[0].source,
                        coordinates=(
                            ("register", family[0].subject.register_name),
                            ("variable", family[0].subject.variable),
                        ),
                        expected_members=tuple(item.ref for item in expectations),
                    ),
                ),
                decision=OccurrenceCorrectionDecision(
                    reviewed=True,
                    effects=tuple(effects[ref] for ref in sorted(effects, key=str)),
                    reason="Route the declared Deldatamängd token to its named variants.",
                    provenance=register.source_file,
                ),
            )
        )
    for index, entry in enumerate(register.identity.route, 1):
        ref = f"{register.source_file}#/identity.route/{index}"
        statuses["entries_read"].append(ref)
        missing = [name for name in entry.variants if name not in parent_by_name]
        if entry.deldatamangd in matched and entry.variants and not missing:
            statuses["entries_matched"].append(ref)
        else:
            statuses["stale"].append(ref)
            diagnostics.append(
                ResolutionDiagnostic(
                    code="stale_curation_entry",
                    severity="error",
                    case_id=ref,
                    subject=entry.deldatamangd,
                    detail=f"{ref}: unmatched token or missing variant parents {missing!r}",
                    withheld_output=(ref,),
                )
            )
    return tuple(cases), tuple(diagnostics), statuses


def _compile_thin_register(
    records: tuple[SourceRecord, ...], sessions: Any
) -> tuple[CurationCase, ...]:
    register_facts = tuple(
        parent
        for record in records
        for parent in record.parent_facts
        if parent.kind == "register"
    )
    if len(register_facts) != 1:
        raise ValueError(
            f"{records[0].source}: thin register needs one parent declaration"
        )
    register = register_facts[0]
    variants = {}
    for record in records:
        for parent in record.parent_facts:
            if parent.kind != "variant":
                continue
            name = parent.variant.native_id if parent.variant is not None else None
            if not isinstance(name, str) or name in variants:
                raise ValueError(
                    f"{record.source}: duplicate or missing thin variant key {name!r}"
                )
            variants[name] = parent
    reg_from = _source_text(register.fields.coverage_from)
    reg_to = _source_text(register.fields.coverage_to)
    cases = []
    for record in records:
        variable_key = native_variable_key(record)
        if variable_key is None:
            continue
        ref = record_ref(record)
        register_key = source_register_key(record)
        assert register_key is not None
        col = record.subject.variable.native_id
        assert isinstance(col, str)
        case_id = f"accepted-authored:{record.source}:{register_key[-1]}:{col}"
        selected = (
            tuple(
                reference.native_id for reference in record.subject.variant_references
            )
            if record.subject.variant_references
            else tuple(sorted(variants))
            if variants
            else ("_default",)
        )
        effects: list[Any] = [CheckedSourceUse(ref=ref)]
        for name in selected:
            if name == "_default" and not variants:
                variant_key = (*register_key, "variant", "not-applicable")
                variant_from = variant_to = None
            else:
                parent = variants.get(name)
                if parent is None:
                    raise ValueError(f"{case_id}: unknown declared variant {name!r}")
                variant_key = native_parent_key(
                    record.source, record.subject.provider, parent
                )
                if variant_key is None:
                    raise ValueError(f"{case_id}: variant has no native parent key")
                variant_from = _source_text(parent.fields.coverage_from)
                variant_to = _source_text(parent.fields.coverage_to)
            starts = [
                value
                for value in (
                    _source_text(record.fields.coverage_from) or reg_from,
                    variant_from,
                )
                if value is not None
            ]
            ends = [
                value
                for value in (
                    _source_text(record.fields.coverage_to) or reg_to,
                    variant_to,
                )
                if value is not None
            ]
            start = max(starts) if starts else None
            end = min(ends) if ends else None
            if start is None or (end is not None and end < start):
                raise ValueError(
                    f"{case_id}: empty or inverted thin coverage window for {name!r}"
                )
            period = TemporalScope(
                kind="intervals", intervals=(ScopeInterval(start=start, end=end),)
            )
            copied = record.fields.value_set_declared is not None
            effects.append(
                CuratedOccurrenceAddition(
                    occurrence_key=f"{case_id}:{name}",
                    provider=record.subject.provider,
                    variable_key=variable_key,
                    variant_key=variant_key,
                    fields=record.fields,
                    edition_scope=TemporalScope(kind="not_applicable"),
                    edition_period_scope=period,
                    evidence=(ref,),
                    donor=ref,
                    copied_fields=tuple(SourceFields.model_fields),
                    copy_coding=copied,
                    expected_codings=copied_coding_fingerprints(
                        bind_code_lists(record, sessions, scope=period).claims
                    )
                    if copied
                    else None,
                )
            )
        expectations = capture_expectations(
            (record,), fields=tuple(SourceFields.model_fields), coding=True
        )
        cases.append(
            CurationCase(
                case_id=case_id,
                targets=expectations,
                peer_guards=(
                    PeerGuard(
                        guard_id=case_id,
                        source=record.source,
                        coordinates=(
                            ("register", record.subject.register_name),
                            ("variable", record.subject.variable),
                        ),
                        expected_members=(ref,),
                    ),
                ),
                decision=OccurrenceCorrectionDecision(
                    reviewed=True,
                    effects=tuple(effects),
                    reason="Authored thin-provider record declares its own finite coverage.",
                    provenance=record.source,
                ),
            )
        )
    return tuple(cases)


def compile_coding_register(
    register: RegisterCuration,
    scope: ScopeDeclarations,
    *,
    originals: tuple[SourceRecord, ...],
    columns: Mapping[NativeKey, tuple[SourceRecord, ...]],
    coding: Mapping[NativeKey, tuple[CodeListClaim, ...]],
) -> tuple[tuple[CurationCase, ...], tuple[ResolutionDiagnostic, ...]]:
    """Compile one register's coding from established scope identities and claims.

    The caller supplies the same effective column groups and original claims that
    source resolution uses.
    """
    register_fqid = f"{register.register_info.provider}/{register.register_info.slug}"
    register_keys = {
        key for name, key in _scope_registers(scope) if name == register_fqid
    }
    cases = []
    diagnostics = []
    for kind, entries in (
        ("choice", register.coding.choice),
        ("uncoded", register.coding.uncoded),
        ("omit", register.coding.omit),
        ("extend", register.coding.extend),
    ):
        for index, entry in enumerate(entries, 1):
            ref = f"{register.source_file}#/coding.{kind}/{index}"
            variables = {
                item.target.source_key
                for item in scope.naming
                if item.target.kind == "variable"
                and item.target.register_key in register_keys
                and (
                    item.naming.source_id == entry.variable
                    or (
                        item.target.source_key[-2] == "accepted-partition"
                        and item.naming.source_id.startswith(entry.variable + ".")
                    )
                )
            }
            variants = {
                item.target.source_key
                for item in scope.naming
                if item.target.kind == "register_variant"
                and item.target.register_key in register_keys
                and entry.variant in {item.naming.source_id, item.naming.slug}
            }
            matches = {
                key
                for variable in variables
                for variant in variants
                if (key := column_identity(variable, variant, entry.column)) in columns
            }
            if len(matches) != 1:
                status = "overbroad" if len(matches) > 1 else "stale"
                for period_index, (start, end) in enumerate(entry.periods, 1):
                    case_id = f"{ref}/period/{period_index}"
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code=f"{status}_curation_entry",
                            severity="error",
                            case_id=case_id,
                            subject=entry.variable,
                            detail=f"{case_id} resolves to {len(matches)} column keys; expected one",
                            valid_from=start,
                            valid_to=end,
                            withheld_output=(case_id,),
                        )
                    )
                continue
            column = next(iter(matches))
            records = columns[column]
            claims = coding.get(column, ())
            for period_index, (start, end) in enumerate(entry.periods, 1):
                case_id = f"{ref}/period/{period_index}"
                source_windows = (
                    (date.fromordinal(lo).isoformat(), date.fromordinal(hi).isoformat())
                    for record in records
                    for lo, hi in (
                        coding_scope_bounds(
                            record.edition_period_scope
                            if record.edition_period_scope.kind != "not_applicable"
                            else record.edition_scope
                        )
                        or ()
                    )
                )
                used = covers_window(source_windows, start, end)
                selection, status, detail = compile_coding_selection(
                    entry, kind, claims, start, end
                )
                if not used:
                    status, detail = "stale", "period has no column occurrence"
                if status != "matched":
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code=(
                                "overbroad_curation_entry"
                                if status == "over_broad"
                                else "stale_curation_entry"
                            ),
                            severity="error",
                            case_id=case_id,
                            subject=entry.variable,
                            detail=f"{case_id}: {detail}",
                            valid_from=start,
                            valid_to=end,
                            withheld_output=(case_id,),
                        )
                    )
                    continue
                assert selection is not None
                target_refs = {record_ref(record) for record in records}
                target_records = tuple(
                    record for record in originals if record_ref(record) in target_refs
                )
                if {record_ref(record) for record in target_records} != target_refs:
                    raise ValueError(f"{case_id}: target refs left the original scope")
                targets = capture_expectations(target_records, fields=("column_name",))
                first = records[0]
                guard = PeerGuard(
                    guard_id=case_id,
                    source=scope.source,
                    coordinates=(
                        ("register", first.subject.register_name),
                        ("variant", first.subject.variant),
                        ("variable", first.subject.variable),
                    ),
                    fields=(
                        FieldExpectation(
                            name="column_name", status="value", value=entry.column
                        ),
                    ),
                    expected_members=tuple(target.ref for target in targets),
                )
                cases.append(
                    CurationCase(
                        case_id=case_id,
                        targets=targets,
                        peer_guards=(guard,),
                        decision=CodingDecision(
                            reviewed=True,
                            column_key=column,
                            valid_from=start,
                            valid_to=end,
                            expected_codings=coding_expectations(claims, start, end),
                            selection=selection,
                            reason=entry.reason,
                            provenance=entry.source,
                        ),
                    )
                )
    return tuple(sorted(cases, key=lambda item: item.case_id)), tuple(diagnostics)


def _snapshot_key(entry: Any) -> tuple[str, str]:
    revision = entry.revision
    suffix = f":source/{entry.path}"
    if revision is None or not revision.artifact_path.endswith(suffix):
        raise ValueError(f"snapshot input lacks its source artifact: {entry.path}")
    return revision.artifact_path[: -len(suffix)], revision.upstream_revision


def _family_status(report: dict[str, dict[str, list[str]]], name: str):
    return report.setdefault(
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


def _family_diagnostic(
    case_id: str,
    subject: str,
    detail: str,
    *,
    overbroad: bool = False,
    refs: tuple[SourceRecordRef, ...] = (),
) -> ResolutionDiagnostic:
    return ResolutionDiagnostic(
        code="overbroad_curation_entry" if overbroad else "stale_curation_entry",
        severity="error",
        case_id=case_id,
        subject=subject,
        detail=f"{case_id}: {detail}",
        refs=refs,
        withheld_output=(case_id,),
    )


def compile_errata(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[ScopeDeclarations, ...],
    partition_naming: dict[Any, tuple[Any, ...]],
    *,
    subset: bool,
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    dict[Any, tuple[NamingDeclaration, ...]],
    dict[Any, tuple[tuple[tuple[str | int, ...], str], ...]],
    tuple[ResolutionDiagnostic, ...],
    dict[str, dict[str, list[str]]],
]:
    """Compile the SCB omission ledger against complete native variant slices."""
    loaded = load_scb_errata(
        tree.root,
        None,
        classifications=frozenset(
            c.classification.short_name for c in tree.classifications
        ),
    )

    if not loaded:
        return {}, {}, {}, (), {}
    locations: dict[int, list[tuple[Any, tuple[str | int, ...]]]] = defaultdict(list)
    for scope in scopes:
        for name, key in _scope_registers(scope):
            if name.startswith("scb/") and isinstance(key[-1], int):
                locations[key[-1]].append(((scope.source, scope.register_key), key))
    versions = defaultdict(list)
    for entry in loaded.versions:
        versions[entry.register_variant_id].append(entry)
    delivered = {
        (entry.register_id, entry.register_variant_id, fold_column(entry.column)): entry
        for entry in loaded.delivered
    }
    delivered_positions = {
        (entry.register_id, entry.register_variant_id, fold_column(entry.column)): index
        for index, entry in enumerate(loaded.delivered, 1)
    }
    columns = {
        (entry.register_id, entry.register_variant_id, fold_column(entry.column)): entry
        for entry in loaded.columns
    }
    cases: dict[Any, list[CurationCase]] = defaultdict(list)
    naming: dict[Any, dict[tuple[str | int, ...], NamingDeclaration]] = defaultdict(
        dict
    )
    keys: dict[Any, dict[tuple[str | int, ...], str]] = defaultdict(dict)
    diagnostics: list[ResolutionDiagnostic] = []
    report: dict[str, dict[str, list[str]]] = {}
    for register in sorted(tree.registers, key=lambda item: item.source_file):
        if register.register_info.provider != "scb" or not (
            register.errata.version
            or register.errata.delivered
            or register.errata.column
        ):
            continue
        native_id = register.register_info.native_id
        if native_id is None:
            raise ValueError(f"{register.source_file}: SCB register has no native_id")
        register_id = int(native_id)
        name = f"scb/{register.register_info.slug}"
        statuses = _family_status(report, name)
        accepted_names = {
            item.entry.source_id: item
            for item, _ in reversed(_register_naming_entries(tree, register))
            if item.entry.kind == "variable"
        }
        matches = locations.get(register_id, ())
        records = ()
        if len(matches) == 1:
            scope_key, register_key = matches[0]
            records = tuple(
                record
                for _, members in prepared.records.iter_register_slices(
                    scope_key[0], (register_key,)
                )
                for record in members
            )
        by_variant: dict[int, tuple[SourceRecord, ...]] = {}
        for variant in register.variant:
            variant_id = int(variant.native_id.split(".")[-1])
            by_variant[variant_id] = tuple(
                r for r in records if r.subject.native.register_variant_id == variant_id
            )
        bound_editions = {}
        for variant_id, members in by_variant.items():
            if members:
                bound_editions[variant_id] = edition_bindings(
                    members, tuple(versions.get(variant_id, ()))
                )
        variant_ids = {
            item.slug: int(item.native_id.split(".")[-1]) for item in register.variant
        }
        for index, row in enumerate(register.errata.version, 1):
            case_id = f"{register.source_file}#/errata.version/{index}"
            statuses["entries_read"].append(case_id)
            variant_id = variant_ids[row.variant]
            if variant_id in bound_editions:
                statuses["entries_matched"].append(case_id)
            elif not matches and subset:
                statuses["not_evaluated_in_subset"].append(case_id)
            else:
                statuses["stale"].append(case_id)
                diagnostics.append(
                    _family_diagnostic(
                        case_id,
                        f"{name}/{row.variant}/{row.name}",
                        "declared edition has no native variant records",
                    )
                )
        for table, entries in (
            ("delivered", register.errata.delivered),
            ("column", register.errata.column),
        ):
            for index, row in enumerate(entries, 1):
                case_id = f"{register.source_file}#/errata.{table}/{index}"
                statuses["entries_read"].append(case_id)
                subject = (
                    f"curation/scb_errata.toml#/delivered/"
                    f"{delivered_positions[register_id, variant_ids[row.variant], fold_column(row.column)]}"
                    if table == "delivered"
                    else f"{name}/{row.variant}/{row.column}"
                )
                if len(matches) != 1:
                    status = (
                        "over_broad"
                        if matches
                        else "not_evaluated_in_subset"
                        if subset
                        else "stale"
                    )
                    statuses[status].append(case_id)
                    if status != "not_evaluated_in_subset":
                        diagnostics.append(
                            _family_diagnostic(
                                case_id,
                                subject,
                                f"matches {len(matches)} register scopes; expected one",
                                overbroad=bool(matches),
                            )
                        )
                    continue
                variant_id = variant_ids[row.variant]
                members = by_variant.get(variant_id, ())
                editions = bound_editions.get(variant_id, ())
                if not members:
                    blockers = ("no native variant records",)
                    converted = None
                elif missing := sorted(
                    set(row.versions or ()) - {e.name for e in editions}
                ):
                    blockers = (f"missing named version {missing!r}",)
                    converted = None
                elif any(
                    sum(edition.name == version for edition in editions) > 1
                    for version in row.versions or ()
                ):
                    blockers = ("ambiguous named version",)
                    converted = None
                elif table == "column" and (
                    accepted_names.get(f"{register_id}.{row.column}") is None
                    or accepted_names[f"{register_id}.{row.column}"].entry.slug is None
                ):
                    blockers = ("declared column has no tracked slug",)
                    converted = None
                elif table == "delivered":
                    entry = delivered[register_id, variant_id, fold_column(row.column)]
                    result = convert_delivered_entry(
                        entry, case_id=case_id, records=members, editions=editions
                    )
                    blockers, converted = result.blockers, result.case
                else:
                    entry = columns[register_id, variant_id, fold_column(row.column)]
                    result = convert_column_entry(
                        entry,
                        case_id=case_id,
                        records=members,
                        editions=editions,
                        declared_flags=frozenset(row.model_fields_set)
                        & {"is_identifier", "is_sensitive"},
                    )
                    blockers, converted = result.blockers, result.case
                if converted is None:
                    overbroad = any("ambiguous" in blocker for blocker in blockers)
                    statuses["over_broad" if overbroad else "stale"].append(case_id)
                    diagnostics.append(
                        _family_diagnostic(
                            case_id,
                            subject,
                            "; ".join(blockers),
                            overbroad=overbroad,
                            refs=(record_ref(members[0]),) if members else (),
                        )
                    )
                    continue
                if table == "delivered":
                    assert isinstance(converted.decision, OccurrenceCorrectionDecision)
                    effects = []
                    for effect in converted.decision.effects:
                        if not isinstance(effect, CuratedOccurrenceAddition):
                            effects.append(effect)
                            continue
                        owners = {
                            declaration.target.source_key
                            for declaration in partition_naming.get(scope_key, ())
                            if len(declaration.target.source_key)
                            > len(effect.variable_key)
                            and declaration.target.source_key[
                                : len(effect.variable_key)
                            ]
                            == effect.variable_key
                            and declaration.target.source_key[len(effect.variable_key)]
                            == "accepted-partition"
                            and any(
                                expectation.ref == record_ref(record)
                                for expectation in declaration.target.expectations
                                for record in members
                                if source_occurrence(record).variable_key
                                == effect.variable_key
                                and _literal_field(record, "column_name") == row.column
                            )
                        }
                        effects.append(
                            effect.model_copy(
                                update={"variable_key": next(iter(owners))}
                            )
                            if len(owners) == 1
                            else effect
                        )
                    converted = converted.model_copy(
                        update={
                            "decision": converted.decision.model_copy(
                                update={"effects": tuple(effects)}
                            )
                        }
                    )
                else:
                    variable_key = (
                        *register_key,
                        "declared-column",
                        fold_column(row.column),
                    )
                    if variable_key not in naming[scope_key]:
                        anchor = members[0]
                        ref = record_ref(anchor)
                        source_id = f"{register_id}.{row.column}"
                        accepted = accepted_names[source_id]
                        slug = accepted.entry.slug
                        assert slug is not None
                        naming[scope_key][variable_key] = NamingDeclaration(
                            target=NativeNamingTarget(
                                kind="variable",
                                provider="scb",
                                source_key=variable_key,
                                register_key=register_key,
                                expectations=capture_expectations((anchor,), fields=()),
                                peer_guards=(
                                    PeerGuard(
                                        guard_id=f"{case_id}:naming-anchor",
                                        source=anchor.source,
                                        native=anchor.subject.native,
                                        expected_members=(ref,),
                                    ),
                                ),
                            ),
                            naming=SlugEntry(
                                kind="variable",
                                provider="scb",
                                source_id=source_id,
                                slug=slug,
                            ),
                            contributors=(accepted,),
                        )
                        keys[scope_key][variable_key] = row.column
                cases[scope_key].append(converted)
                statuses["entries_matched"].append(case_id)
    return (
        {key: tuple(value) for key, value in cases.items()},
        {
            key: tuple(value[k] for k in sorted(value, key=repr))
            for key, value in naming.items()
        },
        {
            key: tuple(sorted(value.items(), key=lambda item: repr(item[0])))
            for key, value in keys.items()
        },
        tuple(diagnostics),
        report,
    )


def compile_enrichment(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[ScopeDeclarations, ...],
    naming: dict[Any, tuple[Any, ...]],
    errata_cases: dict[Any, tuple[CurationCase, ...]],
    *,
    subset: bool,
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    tuple[ResolutionDiagnostic, ...],
    dict[str, dict[str, list[str]]],
]:
    """Bind delivery prose and search spellings to one compiled catalog name."""
    loaded = load_delivery_enrichment(tree.root)
    description_positions = {
        (item.provider, item.register, item.variable): index
        for index, item in enumerate(loaded.descriptions, 1)
    }
    alias_positions = {
        (item.provider, item.register, item.variable, item.delivery_column): index
        for index, item in enumerate(loaded.aliases, 1)
    }
    by_name: dict[str, list[tuple[Any, NamingDeclaration]]] = defaultdict(list)
    register_keys: dict[str, tuple[str | int, ...]] = {}
    register_locations: dict[str, list[tuple[Any, tuple[str | int, ...]]]] = (
        defaultdict(list)
    )
    for scope in scopes:
        scope_key = (scope.source, scope.register_key)
        for register, register_key in _scope_registers(scope):
            register_keys[register] = register_key
            register_locations[register].append((scope_key, register_key))
            for item in naming.get(scope_key, ()):
                if (
                    item.target.kind == "variable"
                    and item.target.register_key == register_key
                    and item.naming.slug is not None
                ):
                    by_name[f"{register}/{item.naming.slug}"].append((scope_key, item))
    cases: dict[Any, list[CurationCase]] = defaultdict(list)
    diagnostics: list[ResolutionDiagnostic] = []
    report: dict[str, dict[str, list[str]]] = {}
    record_cache: dict[tuple[Any, tuple[str | int, ...]], tuple[SourceRecord, ...]] = {}

    def register_anchor(name: str) -> tuple[SourceRecordRef, ...]:
        locations = register_locations.get(name, ())
        if len(locations) != 1:
            return ()
        scope_key, register_key = locations[0]
        cache_key = (scope_key, register_key)
        if cache_key not in record_cache:
            record_cache[cache_key] = tuple(
                record
                for _, members in prepared.records.iter_register_slices(
                    scope_key[0], (register_key,)
                )
                for record in members
            )
        members = record_cache[cache_key]
        return (record_ref(members[0]),) if members else ()

    for register in sorted(tree.registers, key=lambda item: item.source_file):
        if not (register.enrichment.description or register.enrichment.alias):
            continue
        name = f"{register.register_info.provider}/{register.register_info.slug}"
        statuses = _family_status(report, name)
        for table, entries in (
            ("description", register.enrichment.description),
            ("alias", register.enrichment.alias),
        ):
            for index, row in enumerate(entries, 1):
                case_id = f"{register.source_file}#/enrichment.{table}/{index}"
                target_name = f"{name}/{row.variable}"
                position = (
                    description_positions[
                        register.register_info.provider,
                        register.register_info.slug,
                        row.variable,
                    ]
                    if isinstance(row, EnrichmentDescriptionEntry)
                    else alias_positions[
                        register.register_info.provider,
                        register.register_info.slug,
                        row.variable,
                        row.delivery_column,
                    ]
                )
                subject = (
                    f"curation/delivery_enrichment.generated.toml#/{table}/{position}"
                )
                statuses["entries_read"].append(case_id)
                targets = by_name.get(target_name, ())
                if len(targets) != 1:
                    status = (
                        "over_broad"
                        if len(targets) > 1
                        else "not_evaluated_in_subset"
                        if subset and name not in register_keys
                        else "stale"
                    )
                    statuses[status].append(case_id)
                    if status != "not_evaluated_in_subset":
                        diagnostics.append(
                            _family_diagnostic(
                                case_id,
                                subject,
                                f"matches {len(targets)} compiled naming targets; expected one",
                                overbroad=len(targets) > 1,
                                refs=register_anchor(name),
                            )
                        )
                    continue
                scope_key, target = targets[0]
                register_key = target.target.register_key
                assert register_key is not None
                cache_key = (scope_key, register_key)
                if cache_key not in record_cache:
                    record_cache[cache_key] = tuple(
                        record
                        for _, members in prepared.records.iter_register_slices(
                            scope_key[0], (register_key,)
                        )
                        for record in members
                    )
                all_records = record_cache[cache_key]
                variable_key = target.target.source_key
                if "accepted-partition" in variable_key:
                    refs = {item.ref for item in target.target.expectations}
                    chosen = tuple(r for r in all_records if record_ref(r) in refs)
                elif "declared-column" in variable_key:
                    chosen = ()
                else:
                    chosen = tuple(
                        r
                        for r in all_records
                        if source_occurrence(r).variable_key == variable_key
                    )
                if isinstance(row, EnrichmentDescriptionEntry):
                    if not chosen or any(
                        _literal_field(r, "description") for r in chosen
                    ):
                        statuses["stale"].append(case_id)
                        diagnostics.append(
                            _family_diagnostic(
                                case_id,
                                subject,
                                "target has no original records or already has a description",
                                refs=(record_ref(chosen[0]),)
                                if chosen
                                else register_anchor(name),
                            )
                        )
                        continue
                    expectations = capture_expectations(chosen, fields=("description",))
                    effects = tuple(
                        CheckedFieldChange(
                            ref=item.ref,
                            replacement=FieldExpectation(
                                name="description",
                                status="value",
                                value=row.description,
                            ),
                        )
                        for item in expectations
                    )
                    decision = OccurrenceCorrectionDecision(
                        reviewed=True,
                        effects=effects,
                        reason=row.provenance
                        or f"Accepted delivery description for {target_name}",
                        provenance=row.provenance or f"curation:{case_id}",
                    )
                else:
                    assert isinstance(row, EnrichmentAliasEntry)
                    variant_keys = {
                        key
                        for r in chosen
                        if (key := source_occurrence(r).variant_key) is not None
                    }
                    variant_keys.update(
                        effect.variant_key
                        for case in errata_cases.get(scope_key, ())
                        if isinstance(case.decision, OccurrenceCorrectionDecision)
                        for effect in case.decision.effects
                        if isinstance(effect, CuratedOccurrenceAddition)
                        and effect.variable_key == variable_key
                    )
                    if not variant_keys:
                        statuses["stale"].append(case_id)
                        diagnostics.append(
                            _family_diagnostic(
                                case_id,
                                subject,
                                "target has no occurrence variants",
                                refs=register_anchor(name),
                            )
                        )
                        continue
                    expectations = (
                        capture_expectations(chosen, fields=())
                        if chosen
                        else target.target.expectations
                    )
                    decision = SearchAliasDecision(
                        reviewed=True,
                        variable_key=variable_key,
                        variant_keys=tuple(sorted(variant_keys, key=repr)),
                        column=row.delivery_column,
                        reason=row.provenance
                        or f"Accepted delivery alias for {target_name}",
                        provenance=row.provenance or f"curation:{case_id}",
                    )
                cases[scope_key].append(
                    CurationCase(
                        case_id=case_id, targets=expectations, decision=decision
                    )
                )
                statuses["entries_matched"].append(case_id)
    return (
        {key: tuple(value) for key, value in cases.items()},
        tuple(diagnostics),
        report,
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
    unmatched: list[str] = []
    metadata, class_successions = compile_declared_metadata(
        tree, selected=selected, unmatched=unmatched
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
                unmatched.append(case_id)
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
        if not _edge_registers((ref,), selected):
            unmatched.append(f"lineage.toml#/lineage_defaults/{ref}")
        defaults.append((ref, variant))
    inputs = prepared.manifest.inputs
    identifiers = tuple(
        e.revision.dataset
        for e in inputs
        if e.origin == "snapshot"
        and e.path == "Identifierare.csv"
        and e.revision is not None
    )
    event_targets = {
        _snapshot_key(entry): entry.revision.dataset
        for entry in inputs
        if entry.origin == "snapshot"
        and entry.path == "Registerinformation.csv"
        and entry.revision is not None
    }
    all_events = tuple(
        (entry.revision.dataset, event_targets[key])
        for entry in inputs
        if entry.origin == "snapshot"
        and entry.path == "Timeseries.csv"
        and entry.revision is not None
        if (key := _snapshot_key(entry)) in event_targets
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
                if name in selected and (subset or entry_id not in unmatched):
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
    variable_families: dict[str, set[tuple[str, tuple[str | int, ...]]]] = {}
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
                variable_families.setdefault(
                    f"{register}/{name.naming.slug}", set()
                ).add((scope.source, name.target.source_key))
    classification_report = {
        key: []
        for key in (
            "entries_read",
            "entries_matched",
            "stale",
            "over_broad",
            "not_evaluated_in_subset",
            "duplicate_overrides",
        )
    }
    for entry in tree.classifications:
        base = f"classifications/{entry.classification.short_name}.toml"
        classification_report["entries_read"].append(base)
        classification_report["entries_matched"].append(base)
        for index, _ in enumerate(entry.binding.value_set_labels, 1):
            ref = f"{base}#/binding/value_set_labels/{index}"
            classification_report["entries_read"].append(ref)
        for index, bound in enumerate(entry.binding.variable, 1):
            ref = f"{base}#/binding/variable/{index}"
            classification_report["entries_read"].append(ref)
            if _register(bound.variable) not in selected:
                if subset:
                    classification_report["not_evaluated_in_subset"].append(ref)
                    continue
                status = "stale"
            else:
                matches = len(variable_families.get(bound.variable, ()))
                status = "entries_matched" if matches == 1 else "stale"
            classification_report[status].append(ref)
            if status != "entries_matched":
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="stale_curation_entry",
                        severity="error",
                        subject=bound.variable,
                        detail=f"{ref} matches {len(variable_families.get(bound.variable, ()))} selected native families; expected exactly one",
                        withheld_output=(ref,),
                    )
                )
    report["_classifications"] = classification_report
    # The scoped resolver records what it actually skipped in this list.
    report["_subset"] = {"dropped": []}
    if not subset:
        diagnostics.extend(
            ResolutionDiagnostic(
                code="stale_curation_entry",
                severity="error",
                subject=entry_id,
                detail=f"{entry_id} matches no selected register",
                withheld_output=(entry_id,),
            )
            for entry_id in sorted(unmatched)
        )
    naming, variants, provider_keys, naming_diagnostics, naming_report = (
        compile_native_naming(tree, prepared, scopes, subset=subset)
    )
    (
        partition_cases,
        partition_naming,
        partition_keys,
        ambiguities,
        split_bases,
        partition_diagnostics,
    ) = compile_partitions(tree, prepared, scopes)
    for key, extra in partition_cases.items():
        cases[key].extend(extra)
    naming = {
        key: tuple(
            item
            for item in values
            if item.target.source_key not in split_bases.get(key, set())
        )
        + partition_naming.get(key, ())
        for key, values in naming.items()
    }
    provider_keys = {
        key: tuple(
            item for item in values if item[0] not in split_bases.get(key, set())
        )
        + partition_keys.get(key, ())
        for key, values in provider_keys.items()
    }
    matrix_cases, matrix_names, matrix_keys, matrix_diagnostics = compile_matrix_repr(
        tree, prepared, scopes, naming
    )
    for key, values in matrix_cases.items():
        cases[key].extend(values)
    naming = {
        key: (*values, *matrix_names.get(key, ())) for key, values in naming.items()
    }
    provider_keys = {
        key: (*values, *matrix_keys.get(key, ()))
        for key, values in provider_keys.items()
    }
    (
        errata_cases,
        errata_naming,
        errata_keys,
        errata_diagnostics,
        errata_report,
    ) = compile_errata(tree, prepared, scopes, partition_naming, subset=subset)
    for key, extra in errata_cases.items():
        cases[key].extend(extra)
    for key, extra in errata_naming.items():
        naming[key] = (*naming.get(key, ()), *extra)
    for key, extra in errata_keys.items():
        provider_keys[key] = (*provider_keys.get(key, ()), *extra)
    enrichment_cases, enrichment_diagnostics, enrichment_report = compile_enrichment(
        tree, prepared, scopes, naming, errata_cases, subset=subset
    )
    for key, extra in enrichment_cases.items():
        cases[key].extend(extra)
    for family_report in (errata_report, enrichment_report):
        for register, statuses in family_report.items():
            current = report.setdefault(register, {key: [] for key in statuses})
            for status, entries in statuses.items():
                current.setdefault(status, []).extend(entries)
    for register, statuses in naming_report.items():
        current = report.setdefault(register, {key: [] for key in statuses})
        for key, values in statuses.items():
            current.setdefault(key, []).extend(values)
    thin_cases, thin_diagnostics, thin_report = compile_sos_thin(tree, prepared, scopes)
    for key, new_cases in thin_cases.items():
        cases[key].extend(new_cases)
    for register, statuses in thin_report.items():
        current = report.setdefault(register, {key: [] for key in statuses})
        for key, values in statuses.items():
            current.setdefault(key, []).extend(values)
    return CompiledCuration(
        fields={
            "classifications": books,
            "classification_successions": class_successions,
            "metadata": metadata,
            "code_label_pairs": tuple(pairs),
            "lineage_defaults": tuple(defaults),
            "identifier_sources": identifiers,
            "event_sources": all_events,
        },
        cases={
            key: tuple(sorted(value, key=lambda item: item.case_id))
            for key, value in cases.items()
        },
        report=report,
        diagnostics=(
            *diagnostics,
            *naming_diagnostics,
            *partition_diagnostics,
            *matrix_diagnostics,
            *errata_diagnostics,
            *enrichment_diagnostics,
            *thin_diagnostics,
        ),
        naming=naming,
        variants=variants,
        provider_keys=provider_keys,
        naming_ambiguities=ambiguities,
        partition_bases={key: frozenset(value) for key, value in split_bases.items()},
    )


def finalize_classification_bindings(
    compiled: CompiledCuration,
    tree: CurationTree,
    *,
    matched_labels: set[str],
    duplicate_overrides: set[str],
    subset: bool,
) -> tuple[ResolutionDiagnostic, ...]:
    """Report descriptor matches after all selected scopes have been resolved."""
    if not tree.classifications or not compiled.report:
        return ()
    statuses = compiled.report["_classifications"]
    diagnostics = []
    for entry in tree.classifications:
        base = f"classifications/{entry.classification.short_name}.toml"
        for index, label in enumerate(entry.binding.value_set_labels, 1):
            ref = f"{base}#/binding/value_set_labels/{index}"
            status = (
                "entries_matched"
                if label in matched_labels
                else "not_evaluated_in_subset"
                if subset
                else "stale"
            )
            statuses[status].append(ref)
            if status == "stale":
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="stale_curation_entry",
                        severity="error",
                        subject=ref,
                        detail=f"{ref} label {label!r} matches no source descriptor",
                        withheld_output=(ref,),
                    )
                )
    statuses["duplicate_overrides"] = sorted(duplicate_overrides)
    return tuple(diagnostics)
