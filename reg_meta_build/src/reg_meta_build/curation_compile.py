"""Compile tracked curation beside a pinned, transitional selection.

Families are closed: every stored declaration has one owner, and COMPILED replaces
that owner's declarations in every scope. Later children move their converters here.
"""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import TypeAdapter, ValidationError
from reg_meta.fqid import FqidKind, parse as parse_fqid

from ._curation import SentinelCode
from ._resolved_common import covers_window
from .concept_groups import CodeLabelPair
from .convert_errata import capture_expectations
from .fqid_slugs import EntityKind, _load_register_auto_file, _validate_entry
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
from .source_coding_choices import coding_expectations, compile_coding_selection
from .source_coordinates import (
    column_identity,
    native_parent_key,
    source_register_key,
)
from .source_curation import (
    AcknowledgeDecision,
    CodingDecision,
    CurationCase,
    FieldExpectation,
    PeerGuard,
    ResolutionDiagnostic,
    SourceRecordRef,
)
from .source_effects import record_ref
from .source_intervals import coding_scope_bounds
from .source_naming import (
    AcceptedNamingEntry,
    LegacyNamingBinding,
    NamingFreezeSetting,
    NamingSelection,
    NativeNamingTarget,
    authored_naming_id,
    convert_naming,
    native_provider_keys,
    native_scb_naming_id,
)
from .source_records import SourceRevision, canonical_sha256

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .curation_tree import CurationTree, RegisterCuration
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
    "identity": Family(
        case_prefixes=(
            "accepted-column-partitions:",
            "accepted-sos-routes:",
            "accepted-sos-identity:",
            "existing-source-use:",
        ),
        naming_shapes=("accepted-partition", "accepted-shape", "accepted-name"),
        unapplied_datasets=("fqid_slugs/",),
        scope_fields=("naming_ambiguities",),
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
        naming_shapes=("native",),
        scope_fields=("provider_keys", "variants"),
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
        "classification_bindings",
        "coding",
        "naming",
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
    markers = {
        "accepted-partition": "identity",
        "declared-column": "errata",
        "accepted-matrix": "matrix",
        "period-family": "representation",
        "accepted-shape": "identity",
        "accepted-name": "identity",
        "thin-provider": "thin_provider",
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
    if target.provider not in {"scb", "sos"}:
        return "thin_provider"
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
    # declaration, but remains stored until identity is compiled.
    naming_owners = {
        item.target.source_key: _naming_family(item.target) for item in scope.naming
    }
    naming_owners.update(
        (item.family.source_key, _naming_family(item.family))
        for item in scope.naming_ambiguities
    )
    provider_keys = []
    for item in scope.provider_keys:
        source_key, value = item
        owner = "identity" if value is None else naming_owners.get(source_key)
        if owner is None:
            raise ValueError(
                f"provider key has no converted catalog naming: {source_key!r}"
            )
        if owner not in COMPILED:
            provider_keys.append(item)
    merged = scope.model_copy(
        update={
            "cases": (*cases, *compiled.cases.get(key, ())),
            "naming": (*naming, *(compiled.naming or {}).get(key, ())),
            "provider_keys": tuple(provider_keys),
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
        for family_key, _members in prepared.records.iter_native_families(source):
            scope_key = (source, family_key[:5])
            if scope_key not in scope_map:
                scope_key = (source, None)
            if scope_key not in scope_map or family_key[-2] not in {
                "native-int",
                "native-str",
            }:
                continue
            target = NativeNamingTarget(
                kind="variable",
                provider=str(family_key[1]),
                source_key=family_key,
                register_key=family_key[:5],
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
            if (
                entry.entry.kind == "variable"
                and len(entry.entry.source_id.split(".")) == 3
            ):
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
                if (
                    entry.entry.kind == "variable"
                    and len(entry.entry.source_id.split(".")) == 3
                ):
                    continue  # Split naming is owned by identity until child 08.
                ref = f"{entry.revision.artifact_path} {where}"
                statuses["entries_read"].append(ref)
                entries.append(entry)
                locations[entry.entry_id] = ref
                entry_owners[entry.entry_id] = name
        bound = {(b.kind, b.provider, b.source_id) for b in bindings[scope_key]}
        uncompiled = {
            (item.naming.kind, item.naming.provider, item.naming.source_id)
            for item in scope.naming
            if _naming_family(item.target) not in COMPILED
        }
        supplied = {
            (entry.entry.kind, entry.entry.provider, entry.entry.source_id)
            for entry in entries
        }
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
    for register, statuses in naming_report.items():
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
        diagnostics=(*diagnostics, *naming_diagnostics),
        naming=naming,
        variants=variants,
        provider_keys=provider_keys,
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
