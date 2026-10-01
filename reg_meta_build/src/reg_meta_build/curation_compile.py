"""Compile tracked curation against prepared source scopes."""

from __future__ import annotations

import hashlib
import json
import re
from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from itertools import combinations
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import TypeAdapter, ValidationError
from reg_meta.fqid import FqidKind, derive_variable_slug, parse as parse_fqid
from reg_meta.source_evidence import canonical_sha256

from ._curation import SentinelCode, curation_error, fold_column
from ._resolved_common import covers_window, remaining_windows
from .cis2016_matrix import (
    Cis2014Matrix,
    convert_matrix,
    load_matrix,
)
from .concept_groups import _MONTH_TOKENS, CodeLabelPair
from .curation_tree import (
    CodingChoiceEntry,
    CodingDocumentedEntry,
    CodingEntry,
    CodingExtendEntry,
    CodingSentinelEntry,
    EnrichmentAliasEntry,
    EnrichmentDescriptionEntry,
    ErrataColumnEntry,
    ErrataDataTypeEntry,
    ErrataDeliveredEntry,
    ErrataFieldEntry,
    ErrataOccurrencePeriodEntry,
    ErrataSupportEntry,
    _OccurrenceCorrectionEntry,
)
from .fqid_slugs import SlugEntry, _parse_variant_id, freeze_state, load_freeze_states
from .normalization import normalize_token
from .prepared_catalog import ReferenceEvidence
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
    ErrataVariantContext,
    convert_column_entry,
    convert_delivered_entry,
    edition_bindings,
    resolve_scb_errata,
)
from .source_coding import (
    coding_source_sha256,
    copied_coding_fingerprints,
    resolve_code_membership,
)
from .source_coding_choices import (
    coding_expectations,
    compile_coding_selection,
    documented_source_members,
)
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
    CheckedEditionRebind,
    CheckedFieldChange,
    CheckedIdentityChange,
    CheckedPeriodChange,
    CheckedSourceUse,
    CheckedVariantAssignment,
    ClassificationDecision,
    CodingDecision,
    ColumnRepresentation,
    CuratedOccurrenceAddition,
    CurationCase,
    DeliveryMetadataDecision,
    FieldExpectation,
    GuardValidationContext,
    OccurrenceCorrectionDecision,
    PeerGuard,
    RepresentationDecision,
    ResolutionDiagnostic,
    SearchAliasDecision,
    SourceRecordRef,
    _field_matches,
    acknowledgement_evidence_sha256,
    capture_expectations,
    documented_labels_match,
    evaluate_cases,
    evaluate_source_expectations,
)
from .source_effects import apply_occurrence_cases, record_ref
from .source_intervals import coding_scope_bounds, reconcile_source_fields, scope_bounds
from .source_naming import (
    AcceptedNamingEntry,
    LegacyNamingBinding,
    NamingAmbiguity,
    NamingDeclaration,
    NamingFreezeSetting,
    NamingSelection,
    NativeNamingTarget,
    _register_naming_entries,
    authored_naming_id,
    convert_naming,
    native_provider_keys,
    native_scb_naming_id,
)
from .source_occurrences import EffectiveOccurrence, source_occurrence
from .source_periods import source_scopes
from .source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceFields,
    SourceRecord,
    TemporalScope,
)
from .source_reference_records import SourceColumnTypeDeclaration
from .source_value_bindings import (
    bind_code_lists,
    marker_binding_fingerprints,
    open_value_bindings,
)
from .sources.swecov_column_types import (
    SWECOV_COLUMN_TYPES_PATH,
    index_swecov_column_types,
    steward_column_types,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .curation_tree import (
        CurationTree,
        IdentityColumnOwnerEntry,
        IdentityRenameEntry,
        IdentitySplitEntry,
        RegisterCuration,
    )
    from .pipeline import CompiledScope
    from .prepared_catalog import PreparedCatalogSources
    from .resolved_catalog import ResolvedClassification
    from .source_coding import CodeListClaim
    from .source_coordinates import NativeKey
    from .source_value_bindings import ValueBindingSession, ValueListBinding


@dataclass(frozen=True)
class CompiledCodebook:
    source: str
    descriptor: str
    metadata: dict[str, str | int | list[dict[str, str]] | None]


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
    """Hash sorted tracked curation paths and source slug files."""
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
    """Validate tracked classification sentinels at the compile boundary."""
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
    scope: CompiledScope,
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
        split = kind == "register_variant" and key[-2] == "edition-split"
        member_id = None if kind == "register" else key[-3] if split else key[-1]
        if not isinstance(register_id, int) or (
            member_id is not None and not isinstance(member_id, int)
        ):
            raise ValueError(f"SCB native naming requires integer IDs: {key!r}")
        native_id = native_scb_naming_id(kind, register_id, member_id)
        return f"{native_id}.{key[-1]}" if split else native_id
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


@dataclass(frozen=True)
class ColumnPartitionConversion:
    bindings: tuple[LegacyNamingBinding, ...]
    case: CurationCase | None
    diagnostics: tuple[ResolutionDiagnostic, ...]


def _bindable_split_literals(
    literals: tuple[str, ...], columns: dict[str, list[SourceRecord]]
) -> bool:
    if len(literals) == 1:
        return True
    if not literals or len({fold_column(column) for column in literals}) != 1:
        return False
    delivered: dict[tuple[Any, Any], set[str]] = defaultdict(set)
    for column in literals:
        for record in columns[column]:
            occurrence = source_occurrence(record)
            if occurrence.identity_checked:
                continue
            if occurrence.edition_key is not None:
                delivered[occurrence.variant_key, occurrence.edition_key].add(column)
    return all(len(spellings) == 1 for spellings in delivered.values())


@dataclass(frozen=True)
class _ColumnPartitionPlan:
    columns: dict[str, list[SourceRecord]]
    owners: dict[str, tuple[str, ...]]
    scoped_owners: Mapping[tuple[SourceRecordRef, str], str]
    unassigned: tuple[str, ...]
    unresolved: set[str]
    candidates: dict[str, list[str]]


def _column_partition_plan(
    records: tuple[SourceRecord, ...],
    *,
    source_id: str,
    split_ids: tuple[str, ...],
    declared_columns: Mapping[str, str | None] | None,
    declaration_reference: str | None,
    scoped_owners: Mapping[tuple[SourceRecordRef, str], str] | None,
) -> _ColumnPartitionPlan:
    """Resolve literal ownership without constructing slice-only diagnostics."""
    keys = {native_variable_key(record) for record in records}
    if not records or None in keys or len(keys) != 1:
        raise ValueError("partition conversion requires one complete native family")
    if (
        len(set(split_ids)) != len(split_ids)
        or not split_ids
        or any(
            key != source_id
            and (not key.startswith(source_id + ".") or not key[len(source_id) + 1 :])
            for key in split_ids
        )
    ):
        raise ValueError("split keys must uniquely discriminate this source identity")
    if source_id in split_ids and (
        split_ids != (source_id,)
        or declared_columns is None
        or set(declared_columns.values()) != {source_id}
    ):
        raise ValueError(
            "native ownership requires an explicit complete column map without split owners"
        )
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
        if (record_ref(record), _literal_field(record, "column_name")) in scoped_owners
        and record.fields.column_name is not None
        and record.fields.column_name.status == "value"
    }
    if set(scoped_owners.values()) - set(split_ids):
        raise ValueError("scoped owner names a split outside this family")
    candidates: dict[str, list[str]] = defaultdict(list)
    for column in sorted(columns):
        if column not in scoped_columns:
            candidates[derive_variable_slug(column) or "x"].append(column)
    unassigned = ()
    unresolved = set()
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
                column
                for column, owner in declared_columns.items()
                if owner is None
                and any(
                    (record_ref(record), column) not in scoped_owners
                    for record in columns[column]
                )
            )
        )
    elif declaration_reference is not None:
        raise ValueError("a declaration reference requires explicit column ownership")
    else:
        partition_columns = {
            key: tuple(sorted(candidates[key[len(source_id) + 1 :]]))
            for key in split_ids
            if _bindable_split_literals(
                tuple(candidates[key[len(source_id) + 1 :]]), columns
            )
        }
        unresolved = (
            set(split_ids) - partition_columns.keys() - set(scoped_owners.values())
        )
    return _ColumnPartitionPlan(
        columns, partition_columns, scoped_owners, unassigned, unresolved, candidates
    )


def convert_column_partitions(
    records: tuple[SourceRecord, ...],
    *,
    source_id: str,
    split_ids: tuple[str, ...],
    declared_columns: Mapping[str, str | None] | None = None,
    declaration_reference: str | None = None,
    scoped_owners: Mapping[tuple[SourceRecordRef, str], str] | None = None,
    guard_fields: tuple[str, ...] = (),
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
    plan = _column_partition_plan(
        records,
        source_id=source_id,
        split_ids=split_ids,
        declared_columns=declared_columns,
        declaration_reference=declaration_reference,
        scoped_owners=scoped_owners,
    )
    columns = plan.columns
    partition_columns = plan.owners
    scoped_owners = plan.scoped_owners
    diagnostics = ()
    if plan.unassigned:
        diagnostics = (
            ResolutionDiagnostic(
                code="unassigned_original_columns",
                severity="error",
                subject=source_id,
                detail="Accepted literal ownership covers only part of the original family. "
                f"Unassigned original columns={plan.unassigned!r}; their identity remains unresolved.",
                refs=tuple(
                    sorted(
                        {
                            record_ref(record)
                            for column in plan.unassigned
                            for record in columns[column]
                        },
                        key=str,
                    )
                ),
                fields=("identity", "column_name"),
                withheld_output=(source_id,),
            ),
        )
    elif plan.unresolved:
        diagnostics = (
            ResolutionDiagnostic(
                code="split_identity_conversion_pending",
                severity="error",
                subject=source_id,
                detail=(
                    "Some accepted naming discriminators do not identify unique literal column partitions. "
                    f"Unresolved split keys={sorted(plan.unresolved)!r}; "
                    f"independently bound keys={sorted(partition_columns)!r}. "
                    f"Accepted suffixes={sorted(key[len(source_id) + 1 :] for key in split_ids)!r}; "
                    f"source columns={dict(plan.candidates)!r}. "
                    "Convert the existing rename/shape/identity decision; do not guess from column similarity."
                ),
                refs=tuple(sorted({record_ref(record) for record in records}, key=str)),
                fields=("identity", "column_name"),
                withheld_output=tuple(sorted(plan.unresolved)),
            ),
        )
    if not partition_columns and not scoped_owners:
        return ColumnPartitionConversion((), None, diagnostics)
    first = records[0]
    native = native_variable_key(first)
    register = source_register_key(first)
    assert native is not None and register is not None
    if source_id in split_ids:
        guard_fields = tuple(SourceFields.model_fields)
    expectations = capture_expectations(
        records,
        fields=tuple(dict.fromkeys(("column_name", *guard_fields))),
        parents=bool(guard_fields),
        coding=bool(guard_fields),
    )
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
        key = (
            native
            if split_id == source_id
            else (*native, "accepted-partition", split_id)
        )
        for column in partition_columns.get(split_id, ()):
            for record in columns[column]:
                ref = record_ref(record)
                if (ref, column) in scoped_owners:
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
            if (
                scoped_owners.get((ref, _literal_field(record, "column_name")))
                != split_id
            ):
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


def _scoped_column_owners(
    register: RegisterCuration,
    entries: list[tuple[int, IdentityColumnOwnerEntry]],
    records: tuple[SourceRecord, ...],
    split_ids: tuple[str, ...],
) -> tuple[dict[tuple[SourceRecordRef, str], str], tuple[ResolutionDiagnostic, ...]]:
    owners: dict[tuple[SourceRecordRef, str], str] = {}
    diagnostics = []
    for index, entry in entries:
        ref = f"{register.source_file}#/identity.column_owner/{index}"
        matched = tuple(
            record
            for record in records
            if f"{register.register_info.native_id}.{record.subject.variant.native_id}"
            == entry.variant
            and _literal_field(record, "column_name") == entry.column
            and (
                not entry.source_editions
                or _edition_label(record) in entry.source_editions
            )
        )
        if (
            not matched
            or any(
                not _field_matches(record, field)
                for record in matched
                for field in entry.expected_fields
            )
            or entry.owner not in split_ids
            or (
                entry.source_editions
                and {_edition_label(record) for record in matched}
                != set(entry.source_editions)
            )
        ):
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.variable,
                    "owner, column or exact source editions changed",
                )
            )
            continue
        for record in matched:
            key = (record_ref(record), entry.column)
            if key in owners and owners[key] != entry.owner:
                diagnostics.append(
                    _stale_partition(
                        ref, entry.variable, "competing scoped column owners"
                    )
                )
            else:
                owners[key] = entry.owner
    # A stale selector cannot leave a partly applied family ownership decision.
    return ({} if diagnostics else owners), tuple(diagnostics)


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
        if entry.expected_definitions is not None and any(
            _literal_field(record, "definition")
            != entry.expected_definitions[f"{month:02}"]
            for months in groups.values()
            for month, members in months.items()
            for record in members
        ):
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.family_stem,
                    "literal monthly definitions disagree with the complete reviewed map",
                )
            )
            continue
        family_key = (
            "curation",
            "period-family",
            register.register_info.provider,
            register.register_info.slug,
            entry.family_stem,
        )
        if entry.expected_definitions is not None and any(
            set(months) != set(range(1, 13))
            or any(
                len({_literal_field(r, "column_name") for r in members}) != 1
                for members in months.values()
            )
            for months in groups.values()
        ):
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.family_stem,
                    "reviewed definition family lacks one exact column for every month",
                )
            )
            continue
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
                    "source_attribution",
                    "definition",
                    "measurement_unit",
                    "name",
                    "description",
                ),
                coding=True,
                parents=True,
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
                        column_metadata="per_column"
                        if entry.expected_definitions is not None
                        else "shared",
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


def compile_delivery_metadata(
    register: RegisterCuration,
    records: tuple[SourceRecord, ...],
    naming: tuple[NamingDeclaration, ...],
    *,
    ownership_cases: tuple[CurationCase, ...],
) -> tuple[tuple[CurationCase, ...], tuple[ResolutionDiagnostic, ...]]:
    """Authorize exact literal delivery metadata only for completely pinned accepted owners."""
    if not register.representation.delivery_metadata:
        return (), ()
    cases = []
    diagnostics = []
    unit_names = {entry.variable for entry in register.representation.delivery_metadata}
    owner_keys = {
        name.target.source_key
        for name in naming
        if name.target.kind == "variable" and name.naming.source_id in unit_names
    }
    identities = tuple(
        case
        for case in ownership_cases
        if isinstance(case.decision, OccurrenceCorrectionDecision)
    )
    corrected = apply_occurrence_cases(records, identities)
    if corrected.diagnostics:
        return (), tuple(
            _stale_partition(
                f"{register.source_file}#/representation.delivery_metadata",
                repr(key),
                "checked effective unit ownership is stale",
            )
            for key in sorted(owner_keys, key=repr)
        )
    for index, entry in enumerate(register.representation.delivery_metadata, 1):
        ref = f"{register.source_file}#/representation.delivery_metadata/{index}"
        declarations = tuple(
            name
            for name in naming
            if name.target.kind == "variable"
            and name.naming.source_id == entry.variable
            and name.naming.provider == register.register_info.provider
        )
        keys = {name.target.source_key for name in declarations}
        if len(keys) != 1:
            diagnostics.append(
                _stale_partition(ref, entry.variable, "unit owner is not unique")
            )
            continue
        key = next(iter(keys))
        owned_occurrences = tuple(
            occurrence
            for occurrence in corrected.occurrences
            if occurrence.variable_key == key and occurrence.use == "catalog"
        )
        owned = tuple(
            record
            for occurrence in owned_occurrences
            for record in occurrence.source_records
        )
        if {record_ref(record) for record in owned} != {
            target.ref for target in entry.records
        }:
            diagnostics.append(
                _stale_partition(
                    ref, entry.variable, "complete accepted unit owner changed"
                )
            )
            continue
        supplied_windows = {}
        supplied_scopes = defaultdict(set)
        unsupported_scope = False
        for occurrence in owned_occurrences:
            bounds = coding_scope_bounds(
                occurrence.edition_period_scope
                if occurrence.edition_period_scope.kind != "not_applicable"
                else occurrence.edition_scope
            )
            if not bounds:
                unsupported_scope = True
                continue
            coordinate = (
                occurrence.variant_key,
                _literal_field(occurrence, "column_name"),
            )
            supplied_windows.setdefault(coordinate, []).extend(bounds)
            supplied_scopes[coordinate].add(
                occurrence.edition_period_scope
                if occurrence.edition_period_scope.kind != "not_applicable"
                else occurrence.edition_scope
            )
        authored_windows = {
            (column.variant_key, column.column): column for column in entry.columns
        }
        if (
            unsupported_scope
            or len(authored_windows) != len(entry.columns)
            or set(authored_windows) != set(supplied_windows)
            or any(
                column.source_scope is not None
                and supplied_scopes[coordinate] != {column.source_scope}
                for coordinate, column in authored_windows.items()
            )
            or any(
                (
                    date.fromisoformat(column.valid_from).toordinal(),
                    date.fromisoformat(column.valid_to).toordinal(),
                )
                != (
                    min(start for start, _ in supplied_windows[coordinate]),
                    max(end for _, end in supplied_windows[coordinate]),
                )
                for coordinate, column in authored_windows.items()
            )
        ):
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.variable,
                    "exact complete source-column unit windows changed",
                )
            )
            continue
        guards = []
        all_expected = (*entry.records, *entry.support)
        native_families = {
            native_variable_key(record)
            for record in records
            if record_ref(record) in {item.ref for item in all_expected}
        }
        for native in sorted(native_families, key=repr):
            peers = tuple(
                record for record in records if native_variable_key(record) == native
            )
            first = peers[0]
            peer_refs = {record_ref(record) for record in peers}
            authored = {item.ref for item in all_expected if item.ref in peer_refs}
            register_id = first.subject.native.register_id
            variable_id = first.subject.native.variable_id
            guards.append(
                PeerGuard(
                    guard_id=f"{ref}:{first.source}:{register_id}:{variable_id}"
                    if register_id is not None and variable_id is not None
                    else f"{ref}:{native!r}",
                    source=first.source,
                    native=NativeCoordinates(
                        register_id=register_id, variable_id=variable_id
                    )
                    if register_id is not None and variable_id is not None
                    else None,
                    coordinates=()
                    if register_id is not None and variable_id is not None
                    else (
                        ("register", first.subject.register_name),
                        ("variable", first.subject.variable),
                    ),
                    expected_members=tuple(sorted(authored, key=repr)),
                )
            )
        case = CurationCase(
            case_id=ref,
            targets=tuple(entry.records),
            support=tuple(entry.support),
            peer_guards=tuple(guards),
            decision=DeliveryMetadataDecision(
                reviewed=True,
                variable_key=key,
                fields=tuple(entry.fields),
                columns=tuple(entry.columns),
                reason=entry.evidence,
                provenance=f"{ref}: {entry.noted}: {entry.evidence}",
            ),
        )
        if evaluate_cases((case,), records)[0].status != "applicable":
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.variable,
                    "checked unit source projections or family peers changed",
                )
            )
            continue
        cases.append(case)
    return tuple(cases), tuple(diagnostics)


def compile_parallel_representations(
    register: RegisterCuration,
    records: tuple[SourceRecord, ...],
    naming: tuple[NamingDeclaration, ...],
    *,
    ownership_cases: tuple[CurationCase, ...] = (),
) -> tuple[tuple[CurationCase, ...], tuple[ResolutionDiagnostic, ...]]:
    """Bind finite reviewed column windows without changing original scopes."""
    if not register.representation.parallel:
        return (), ()
    cases = []
    diagnostics = []
    corrections = tuple(
        case
        for case in ownership_cases
        if isinstance(case.decision, OccurrenceCorrectionDecision)
    )
    corrected = apply_occurrence_cases(records, corrections) if corrections else None
    if corrected is not None and corrected.diagnostics:
        return (), (
            _stale_partition(
                f"{register.source_file}#/representation.parallel",
                register.register_info.slug,
                "checked effective representation ownership is stale",
            ),
        )
    for index, entry in enumerate(register.representation.parallel, 1):
        ref = f"{register.source_file}#/representation.parallel/{index}"
        variable_names = tuple(
            name
            for name in naming
            if name.target.kind == "variable"
            and name.naming.source_id == entry.variable
            and name.naming.provider == register.register_info.provider
        )
        variable_keys = {name.target.source_key for name in variable_names}
        variant_keys = {
            name.target.source_key
            for name in naming
            if name.target.kind == "register_variant"
            and entry.variant in {name.naming.source_id, name.naming.slug}
            and name.target.register_key
            in {variable.target.register_key for variable in variable_names}
        }
        if len(variable_keys) != 1 or len(variant_keys) != 1:
            diagnostics.append(
                _stale_partition(ref, entry.variable, "owner/variant is not unique")
            )
            continue
        variable_key, variant_key = next(iter(variable_keys)), next(iter(variant_keys))
        expected_refs = {
            item.ref for name in variable_names for item in name.target.expectations
        }
        owned = tuple(
            record
            for record in records
            if native_variant_key(record) == variant_key
            and (
                record_ref(record) in expected_refs
                if expected_refs
                else native_variable_key(record) == variable_key
            )
        )
        # Keep the uncorrected path byte-identical. A checked field/identity
        # correction supplies effective coordinates, never replacement originals.
        effective = (
            tuple(
                occurrence
                for occurrence in corrected.occurrences
                if occurrence.variable_key == variable_key
                and occurrence.variant_key == variant_key
                and occurrence.use == "catalog"
            )
            if corrected is not None
            else ()
        )
        effective_by_ref = {
            record_ref(record): occurrence
            for occurrence in effective
            for record in occurrence.source_records
        }
        projected = tuple(
            (
                record,
                occurrence.fields.column_name.value
                if occurrence.fields.column_name is not None
                and occurrence.fields.column_name.status == "value"
                else None,
                occurrence.edition_period_scope
                if occurrence.edition_period_scope.kind != "not_applicable"
                else occurrence.edition_scope,
            )
            for occurrence in effective
            for record in occurrence.source_records
        )
        raw_projection = tuple(
            (
                record,
                _literal_field(record, "column_name"),
                record.edition_period_scope
                if record.edition_period_scope.kind != "not_applicable"
                else record.edition_scope,
            )
            for record in owned
        )
        lower, upper = (
            date.fromisoformat(entry.valid_from).toordinal(),
            date.fromisoformat(entry.valid_to).toordinal(),
        )
        use_effective = any(
            native_variable_key(record) != variable_key
            or native_variant_key(record) != variant_key
            or literal != _literal_field(record, "column_name")
            or source_scope
            != (
                record.edition_period_scope
                if record.edition_period_scope.kind != "not_applicable"
                else record.edition_scope
            )
            for record, literal, source_scope in projected
            if any(
                lo <= upper and hi >= lower
                for lo, hi in (coding_scope_bounds(source_scope) or ())
            )
        )
        projection = projected if use_effective else raw_projection
        if use_effective:
            owned = tuple(record for record, _, _ in projection)
        selected = []
        selected_projection = []
        invalid = False
        for column in entry.columns:
            members = tuple(
                (record, literal, source_scope)
                for record, literal, source_scope in projection
                if literal == column.column
                and _edition_label(record) in column.source_editions
            )
            bounds = (
                date.fromisoformat(column.valid_from).toordinal(),
                date.fromisoformat(column.valid_to).toordinal(),
            )
            if (
                not members
                or {_edition_label(r) for r, _, _ in members}
                != set(column.source_editions)
                or any(
                    not (intervals := coding_scope_bounds(source_scope))
                    or any(lo < bounds[0] or hi > bounds[1] for lo, hi in intervals)
                    for _, _, source_scope in members
                )
                or remaining_windows(
                    tuple(
                        (
                            date.fromordinal(lo).isoformat(),
                            date.fromordinal(hi).isoformat(),
                        )
                        for _, _, source_scope in members
                        for lo, hi in (coding_scope_bounds(source_scope) or ())
                    ),
                    column.valid_from,
                    column.valid_to,
                )
            ):
                invalid = True
            # Nested editions share a physical window only when their overlapping
            # metadata agrees under the ordinary source reconciliation rules.
            # simplify: pairwise edition contributors are small; index intervals
            # if a literal column grows to thousands of overlapping originals.
            for left, right in combinations(members, 2):
                if any(
                    max(lo, other_lo, lower) <= min(hi, other_hi, upper)
                    for lo, hi in (coding_scope_bounds(left[2]) or ())
                    for other_lo, other_hi in (coding_scope_bounds(right[2]) or ())
                ):
                    _, conflicts = reconcile_source_fields(
                        tuple(
                            effective_by_ref.get(record_ref(item[0]), item[0])
                            for item in (left, right)
                        )
                    )
                    if set(conflicts) & {
                        "name",
                        "description",
                        "definition",
                        "measurement_unit",
                        "data_type",
                        "data_length",
                        "operational_definition",
                        "source_attribution",
                        "reference_period",
                        "representation",
                        "aggregation_level",
                        "measurement_information",
                        "base_register",
                        "population_definition",
                        "population_date",
                        "geographic_coverage",
                    }:
                        invalid = True
            selected.extend(record for record, _, _ in members)
            selected_projection.extend(members)
        by_edition: dict[str | None, set[str | None]] = defaultdict(set)
        for record, literal, _ in selected_projection:
            by_edition[_edition_label(record)].add(literal)
        overlapping_refs = {
            record_ref(record)
            for record, literal, source_scope in projection
            if literal is not None
            and any(
                lo <= upper and hi >= lower
                for lo, hi in (coding_scope_bounds(source_scope) or ())
            )
        }
        if entry.co_delivered:
            same_members: dict[SourceRecordRef, set[str | None]] = defaultdict(set)
            for record, literal, _ in selected_projection:
                same_members[record_ref(record)].add(literal)
            declared_literals = {column.column for column in entry.columns}
            common_quantity = (
                len({native_variable_key(record) for record in selected}) == 1
                and all(native_variable_key(record) is not None for record in selected)
                and all(
                    len(
                        values := {_literal_field(record, field) for record in selected}
                    )
                    == 1
                    and None not in values
                    and all(value.strip() for value in values if value is not None)
                    for field in ("name", "definition")
                )
                and len(
                    {
                        record.fields.identifier.value
                        for record in selected
                        if record.fields.identifier is not None
                        and record.fields.identifier.status == "value"
                    }
                )
                <= 1
            )
            invalid = (
                invalid
                or not common_quantity
                or any(
                    literals != declared_literals for literals in same_members.values()
                )
            )
        if (
            invalid
            or (
                not entry.co_delivered
                and any(len(columns) != 1 for columns in by_edition.values())
            )
            or (overlapping_refs != {record_ref(record) for record in selected})
        ):
            diagnostics.append(
                _stale_partition(
                    ref,
                    entry.variable,
                    "exact source editions/windows, overlapping literal metadata or complete parallel peers changed",
                )
            )
            continue
        first = selected[0]
        peers = tuple(
            record
            for record in records
            if record.source == first.source
            and native_variable_key(record) == native_variable_key(first)
            and native_variant_key(record) == variant_key
        )
        targets = capture_expectations(
            tuple(selected),
            fields=tuple(SourceFields.model_fields),
            coding=True,
            parents=use_effective,
        )
        guard = PeerGuard(
            guard_id=ref,
            source=first.source,
            coordinates=(
                ("register", first.subject.register_name),
                ("variant", first.subject.variant),
                ("variable", first.subject.variable),
            ),
            expected_members=tuple(sorted({record_ref(r) for r in peers}, key=repr)),
        )
        owned_refs = {record_ref(record) for record in owned}
        related_corrections = (
            tuple(
                case
                for case in corrections
                if {item.ref for item in case.targets} & owned_refs
            )
            if use_effective
            else ()
        )
        support_refs = {
            expectation.ref
            for case in related_corrections
            for expectation in (*case.targets, *case.support)
        }
        correction_support = tuple(
            record for record in records if record_ref(record) in support_refs
        )
        cases.append(
            CurationCase(
                case_id=ref,
                targets=targets,
                support=capture_expectations(
                    correction_support,
                    fields=tuple(SourceFields.model_fields),
                    parents=True,
                    coding=True,
                )
                if use_effective
                else (),
                peer_guards=(
                    guard,
                    *_matrix_repr_guards(correction_support, ref + ":support"),
                    *tuple(
                        peer
                        for case in related_corrections
                        for peer in case.peer_guards
                    ),
                )
                if use_effective
                else (guard,),
                decision=RepresentationDecision(
                    reviewed=True,
                    variable_key=variable_key,
                    variant_key=variant_key,
                    valid_from=entry.valid_from,
                    valid_to=entry.valid_to,
                    columns=tuple(
                        ColumnRepresentation(
                            column=column.column,
                            expected_codings=column.expected_codings,
                            valid_from=entry.valid_from,
                            valid_to=entry.valid_to,
                        )
                        for column in entry.columns
                    ),
                    reason=entry.evidence,
                    provenance=ref,
                    column_metadata=entry.column_metadata,
                    coding_metadata=entry.coding_metadata,
                ),
            )
        )
    return tuple(cases), tuple(diagnostics)


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
    scopes: tuple[CompiledScope, ...],
    naming: dict[Any, tuple[NamingDeclaration, ...]],
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    dict[Any, tuple[NamingDeclaration, ...]],
    dict[Any, tuple[tuple[tuple[str | int, ...], str], ...]],
    tuple[ResolutionDiagnostic, ...],
]:
    """Compile checked matrix and representation declarations from tracked curation."""
    registers = {
        f"{item.register_info.provider}/{item.register_info.slug}": item
        for item in tree.registers
        if any(
            (
                item.representation.matrix,
                item.representation.period_family,
                item.representation.alias_window,
                item.representation.parallel,
                item.representation.delivery_metadata,
            )
        )
    }
    if not any(
        name in registers for scope in scopes for name, _ in _scope_registers(scope)
    ):
        return {}, {}, {}, ()
    cases: dict[Any, list[CurationCase]] = defaultdict(list)
    names: dict[Any, list[NamingDeclaration]] = defaultdict(list)
    keys: dict[Any, list[tuple[tuple[str | int, ...], str]]] = defaultdict(list)
    diagnostics = []
    with open_value_bindings(prepared.value_sources) as sessions:
        for scope in sorted(
            scopes, key=lambda item: (item.source, repr(item.register_key))
        ):
            scope_key = scope.source, scope.register_key
            selected_registers = tuple(
                (registers[name], native)
                for name, native in _scope_registers(scope)
                if name in registers
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
                for declaration in register.representation.matrix:
                    path = tree.root / declaration.evidence_file
                    if not path.resolve().is_relative_to(tree.root.resolve()):
                        raise curation_error(
                            "matrix_evidence_invalid",
                            f"{declaration.evidence_file}: matrix evidence escapes curation root.",
                            "Keep reviewed matrix evidence inside the curation tree.",
                        )
                    matrix = load_matrix(
                        path,
                        source_mode=declaration.source_mode,
                        expected_selector=declaration.selector,
                    )
                    ref = f"{register.source_file}#/matrix/{matrix.selector.edition}"
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
                            and record.subject.native.member_id == matrix.selector.cvid
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
                parallel_cases, parallel_issues = compile_parallel_representations(
                    register,
                    records,
                    (*naming.get(scope_key, ()), *names[scope_key]),
                    ownership_cases=scope.cases,
                )
                cases[scope_key].extend(parallel_cases)
                diagnostics.extend(parallel_issues)
                unit_cases, unit_issues = compile_delivery_metadata(
                    register,
                    records,
                    (*naming.get(scope_key, ()), *names[scope_key]),
                    ownership_cases=scope.cases,
                )
                cases[scope_key].extend(unit_cases)
                diagnostics.extend(unit_issues)

    return (
        {key: tuple(value) for key, value in cases.items()},
        {key: tuple(value) for key, value in names.items()},
        {key: tuple(value) for key, value in keys.items()},
        tuple(diagnostics),
    )


def _sos_owner_discriminators(owners: dict[str, str]) -> dict[str, str]:
    """Reuse one identity for literal split values assigned to the same owner."""
    representatives: dict[str, str] = {}
    for value, owner in sorted(owners.items()):
        representatives.setdefault(owner, value)
    return {value: representatives[owner] for value, owner in owners.items()}


def _literal_field(record: SourceRecord | EffectiveOccurrence, name: str) -> str | None:
    field = getattr(record.fields, name)
    return (
        field.value
        if field is not None
        and field.status == "value"
        and isinstance(field.value, str)
        else None
    )


def _scoped_partition_entries(
    entries: tuple[AcceptedNamingEntry, ...],
    records: tuple[SourceRecord, ...],
    scoped: Mapping[tuple[SourceRecordRef, str], str],
) -> tuple[AcceptedNamingEntry, ...]:
    """Replace generated discriminators only after complete checked ownership."""
    if not records or any(
        (record_ref(record), _literal_field(record, "column_name")) not in scoped
        for record in records
    ):
        return entries
    owners = set(scoped.values())
    return tuple(
        entry
        for entry in entries
        if entry.origin != "generated" or entry.entry.source_id in owners
    )


def _partition_ambiguity(
    native: tuple[str | int, ...],
    records: tuple[SourceRecord, ...],
    entries: tuple[AcceptedNamingEntry, ...],
    split_ids: tuple[str, ...],
    bound: set[str],
    expectations: tuple[Any, ...],
    guard: PeerGuard,
) -> NamingAmbiguity:
    entries = tuple(entry for entry in entries if entry.entry.source_id not in bound)
    split_ids = tuple(split for split in split_ids if split not in bound)
    columns: dict[str, list[SourceRecord]] = defaultdict(list)
    for record in records:
        if (column := _literal_field(record, "column_name")) is not None:
            columns[column].append(record)
    candidates: dict[str, list[str]] = defaultdict(list)
    for column in columns:
        if suffix := derive_variable_slug(column):
            candidates[suffix].append(column)
    bound_suffixes = {split.rsplit(".", 1)[1] for split in bound}
    ambiguous_candidates = {
        suffix: literals
        for suffix, literals in candidates.items()
        if len(literals) == 1
        or not _bindable_split_literals(tuple(literals), columns)
        or suffix not in bound_suffixes
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
                for column in ambiguous_candidates.get(split.rsplit(".", 1)[1], ())
            )
        ),
        reason="Some accepted split keys lack exact literal ownership.",
    )


def _partition_originals(
    prepared: PreparedCatalogSources,
    source: str,
    native: NativeKey,
    records: tuple[SourceRecord, ...],
    *,
    guarded: bool,
) -> tuple[SourceRecord, ...]:
    """Promote guarded projections through the existing indexed original reader."""
    if not guarded or not records or isinstance(records[0], SourceRecord):
        return records
    families = tuple(
        prepared.records.iter_native_families(
            source,
            (native[:5],),
            select_family=lambda key: key == native,
        )
    )
    if len(families) != 1 or families[0][0] != native:
        raise ValueError(
            f"guarded partition lacks one complete original family: {native!r}"
        )
    originals = families[0][1]
    if {record_ref(r) for r in originals} != {record_ref(r) for r in records}:
        raise ValueError(
            f"guarded partition projection differs from its originals: {native!r}"
        )
    return originals


def compile_partitions(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    dict[Any, tuple[Any, ...]],
    dict[Any, tuple[Any, ...]],
    dict[Any, tuple[NamingAmbiguity, ...]],
    dict[Any, set[tuple[str | int, ...]]],
    tuple[ResolutionDiagnostic, ...],
]:
    """Convert accepted native splits and SOS shape/name decisions."""
    states = load_freeze_states(tree.root)
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
    active_registers: dict[str, set[tuple[str | int, ...]]] = defaultdict(set)
    for (source, native_register), (_scope_key, register) in registers.items():
        if (
            entries_by_register[source, native_register]
            or register.identity.partition
            or register.identity.column_owner
            or register.identity.split
            or register.identity.rename
        ):
            active_registers[source].add(native_register)
    for source in sorted({scope.source for scope in scopes}):
        for native, projected in prepared.records.iter_partition_families(
            source, active_registers[source]
        ):
            # The projection has exactly the source facts consumed below. The
            # existing conversion helpers are structural readers of those facts.
            records = cast("tuple[SourceRecord, ...]", projected)
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
            if any(source_id in item.columns.values() for _, item in partitions):
                entries = tuple(
                    item
                    for item in all_entries_by_register[source, native[:5]]
                    if item.entry.source_id == source_id
                    or item.entry.source_id.startswith(source_id + ".")
                )
            if not (
                entries or partitions or scoped_entries or sos_splits or sos_renames
            ):
                continue
            seen.add((source, native[:5], source_id))
            records = _partition_originals(
                prepared,
                source,
                native,
                records,
                guarded=source_id in {item.entry.source_id for item in entries}
                or any(entry.expected_fields for _, entry in scoped_entries),
            )
            expectations = capture_expectations(
                records,
                fields=("column_name",)
                if native[1] == "scb"
                else ("column_name", "name", "data_type", "description")
                if sos_splits and sos_splits[0][1].by == "description"
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
                scoped, scoped_issues = _scoped_column_owners(
                    register, scoped_entries, records, split_ids
                )
                diagnostics.extend(scoped_issues)
                if scoped_issues:
                    null_bases[scope_key].add(native)
                    continue
                entries = _scoped_partition_entries(entries, records, scoped)
                split_ids = tuple(sorted({item.entry.source_id for item in entries}))
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
                        guard_fields=(
                            tuple(SourceFields.model_fields)
                            if source_id in split_ids
                            or any(entry.expected_fields for _, entry in scoped_entries)
                            else ()
                        ),
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
                    annotated_partitions = [
                        entry
                        for _, entry in partitions
                        if entry.data_warning is not None
                    ]
                    annotated_owners = [
                        entry
                        for _, entry in scoped_entries
                        if entry.data_warning is not None
                    ]
                    summaries = {
                        entry.data_warning
                        for entry in (*annotated_partitions, *annotated_owners)
                    }
                    if len(summaries) > 1:
                        raise ValueError(
                            "one identity family needs one consistent data warning summary"
                        )
                    if summaries:
                        annotated_refs = {
                            record_ref(record)
                            for record in records
                            if annotated_partitions
                            or any(
                                entry.variant
                                == f"{register.register_info.native_id}.{record.subject.variant.native_id}"
                                and entry.column
                                == _literal_field(record, "column_name")
                                and (
                                    not entry.source_editions
                                    or _edition_label(record) in entry.source_editions
                                )
                                for entry in annotated_owners
                            )
                        }
                        converted = ColumnPartitionConversion(
                            converted.bindings,
                            converted.case.model_copy(
                                update={
                                    "decision": converted.case.decision.model_copy(
                                        update={
                                            "data_warning": next(iter(summaries)),
                                            "data_warning_refs": tuple(
                                                sorted(annotated_refs, key=str)
                                            ),
                                            "data_warning_fields": ("identity",),
                                        }
                                    )
                                }
                            ),
                            converted.diagnostics,
                        )
                    assert converted.case is not None
                    cases[scope_key].append(converted.case)
                    identity_effects: dict[
                        SourceRecordRef, list[CheckedIdentityChange]
                    ] = defaultdict(list)
                    assert isinstance(
                        converted.case.decision, OccurrenceCorrectionDecision
                    )
                    for effect in converted.case.decision.effects:
                        if isinstance(effect, CheckedIdentityChange):
                            identity_effects[effect.ref].append(effect)
                    if source_id not in split_ids and (
                        converted.case.support
                        or any(
                            _literal_field(record, "column_name") is not None
                            and not any(
                                all(
                                    _field_matches(record, field)
                                    for field in effect.when
                                )
                                for effect in identity_effects.get(
                                    record_ref(record), ()
                                )
                            )
                            for record in records
                        )
                    ):
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
                            native,
                            records,
                            entries,
                            split_ids,
                            bound,
                            expectations,
                            guard,
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
                    by = declaration.by
                    owners = {
                        cast("str", getattr(part, by)): part.owner
                        for part in declaration.parts
                    }
                    values = tuple(
                        _literal_field(record, by)
                        if by in ("data_type", "name", "description")
                        else (
                            record.subject.variant.name
                            if record.subject.variant.status == "value"
                            else None
                        )
                        for record in records
                    )
                    actual = {value for value in values if value is not None}
                    if (
                        actual != set(owners)
                        or len(owners) != len(declaration.parts)
                        or None in values
                    ):
                        diagnostics.append(
                            _stale_partition(
                                ref,
                                source_id,
                                f"{'data types' if by == 'data_type' else 'names' if by == 'name' else 'descriptions' if by == 'description' else 'deldatamangd values'} "
                                f"{sorted(actual)!r} do not equal declared {sorted(owners)!r}",
                            )
                        )
                        null_bases[scope_key].add(native)
                        continue
                    discriminators = _sos_owner_discriminators(owners)
                    effects = tuple(
                        CheckedIdentityChange(
                            ref=record_ref(record),
                            variable_key=(
                                *native,
                                "accepted-shape",
                                discriminators[cast("str", value)],
                            ),
                            when=(
                                FieldExpectation(name=by, status="value", value=value),
                            )
                            if by in ("data_type", "name", "description")
                            else (),
                        )
                        for record, value in zip(records, values, strict=True)
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
                discriminators = _sos_owner_discriminators(owners)
                for value, owner in sorted(owners.items()):
                    if value != discriminators[value]:
                        continue
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
                    NamingFreezeSetting(
                        zone=provider, state=freeze_state(states, provider)
                    )
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


def compile_deferred_partitions(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
) -> tuple[
    dict[Any, tuple[Any, ...]],
    dict[Any, tuple[NamingAmbiguity, ...]],
    dict[Any, set[NativeKey]],
    dict[Any, dict[NativeKey, frozenset[tuple[SourceRecordRef, str]]]],
]:
    """Compile partition names and errata ownership for outside-slice references."""
    states = load_freeze_states(tree.root)
    registers = {}
    entries_by_register = {}
    all_entries_by_register = {}
    active_registers: dict[str, set[NativeKey]] = defaultdict(set)
    for scope in scopes:
        scope_key = scope.source, scope.register_key
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
            key = scope.source, native_register
            registers[key] = scope_key, register
            all_entries_by_register[key] = tuple(
                item
                for item, _ in _register_naming_entries(tree, register)
                if item.entry.kind == "variable"
            )
            entries_by_register[key] = tuple(
                item
                for item in all_entries_by_register[key]
                if len(item.entry.source_id.split(".")) == 3
            )
            if (
                entries_by_register[key]
                or register.identity.partition
                or register.identity.column_owner
                or register.identity.split
                or register.identity.rename
            ):
                active_registers[scope.source].add(native_register)

    bindings = defaultdict(list)
    bound_entries = defaultdict(list)
    ambiguities = defaultdict(list)
    split_bases = defaultdict(set)
    members: dict[Any, dict[NativeKey, set[tuple[SourceRecordRef, str]]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for source in sorted({scope.source for scope in scopes}):

        def relevant_family(
            native: NativeKey, *, selected_source: str = source
        ) -> bool:
            location = registers.get((selected_source, native[:5]))
            if location is None:
                return False
            _scope_key, register = location
            source_id = f"{register.register_info.native_id}.{native[-1]}"
            return (
                any(
                    item.entry.source_id.startswith(source_id + ".")
                    for item in entries_by_register[selected_source, native[:5]]
                )
                or any(
                    item.variable == source_id for item in register.identity.partition
                )
                or any(
                    item.variable == source_id
                    for item in register.identity.column_owner
                )
                or any(
                    item.variable == str(native[-1]) for item in register.identity.split
                )
                or any(
                    item.variable == str(native[-1])
                    for item in register.identity.rename
                )
            )

        for native, projected in prepared.records.iter_partition_families(
            source, active_registers[source], select_family=relevant_family
        ):
            location = registers.get((source, native[:5]))
            if location is None:
                continue
            records = cast("tuple[SourceRecord, ...]", projected)
            scope_key, register = location
            source_id = f"{register.register_info.native_id}.{native[-1]}"
            entries = tuple(
                item
                for item in entries_by_register[source, native[:5]]
                if item.entry.source_id.startswith(source_id + ".")
            )
            partitions = [
                item
                for item in register.identity.partition
                if item.variable == source_id
            ]
            scoped_entries = [
                (i, item)
                for i, item in enumerate(register.identity.column_owner, 1)
                if item.variable == source_id
            ]
            sos_splits = [
                item
                for item in register.identity.split
                if item.variable == str(native[-1])
            ]
            sos_renames = [
                item
                for item in register.identity.rename
                if item.variable == str(native[-1])
            ]
            if any(source_id in item.columns.values() for item in partitions):
                entries = tuple(
                    item
                    for item in all_entries_by_register[source, native[:5]]
                    if item.entry.source_id == source_id
                    or item.entry.source_id.startswith(source_id + ".")
                )
            if not (
                entries or partitions or scoped_entries or sos_splits or sos_renames
            ):
                continue
            if native[1] == "scb":
                if not entries:
                    continue
                split_ids = tuple(sorted({item.entry.source_id for item in entries}))
                records = _partition_originals(
                    prepared,
                    source,
                    native,
                    records,
                    guarded=source_id in {item.entry.source_id for item in entries}
                    or any(entry.expected_fields for _, entry in scoped_entries),
                )
                split_bases[scope_key].add(native)
                scoped, scoped_issues = _scoped_column_owners(
                    register, scoped_entries, records, split_ids
                )
                if scoped_issues:
                    continue
                entries = _scoped_partition_entries(entries, records, scoped)
                split_ids = tuple(sorted({item.entry.source_id for item in entries}))
                if len(partitions) > 1:
                    raise ValueError(
                        f"{register.source_file}: duplicate partition map for {source_id}"
                    )
                declared = None
                reference = None
                if partitions:
                    declaration = partitions[0]
                    declared = {
                        **declaration.columns,
                        **dict.fromkeys(declaration.unassigned_columns),
                    }
                    reference = declaration.columns_ref
                try:
                    plan = _column_partition_plan(
                        records,
                        source_id=source_id,
                        split_ids=split_ids,
                        declared_columns=declared,
                        declaration_reference=reference,
                        scoped_owners=scoped,
                    )
                except ValueError:
                    columns = {}
                    ownership = {}
                    scoped = {}
                else:
                    columns = plan.columns
                    ownership = plan.owners
                    scoped = plan.scoped_owners
                bound = set(ownership) | set(scoped.values())
                expectations = capture_expectations(
                    records,
                    fields=(
                        tuple(SourceFields.model_fields)
                        if source_id in split_ids
                        or any(entry.expected_fields for _, entry in scoped_entries)
                        else ("column_name",)
                    ),
                    parents=source_id in split_ids
                    or any(entry.expected_fields for _, entry in scoped_entries),
                    coding=source_id in split_ids
                    or any(entry.expected_fields for _, entry in scoped_entries),
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
                for split_id in sorted(bound):
                    key = (
                        native
                        if split_id == source_id
                        else (*native, "accepted-partition", split_id)
                    )
                    bindings[scope_key].append(
                        LegacyNamingBinding(
                            kind="variable",
                            provider="scb",
                            source_id=split_id,
                            target=NativeNamingTarget(
                                kind="variable",
                                provider="scb",
                                source_key=key,
                                register_key=native[:5],
                                expectations=expectations,
                                peer_guards=(guard,),
                            ),
                        )
                    )
                    pairs = {
                        (record_ref(record), column)
                        for column in ownership.get(split_id, ())
                        for record in columns[column]
                        if (record_ref(record), column) not in scoped
                    }
                    pairs.update(
                        (ref, cast("str", _literal_field(record, "column_name")))
                        for record in records
                        if scoped.get(
                            (
                                (ref := record_ref(record)),
                                cast("str", _literal_field(record, "column_name")),
                            )
                        )
                        == split_id
                    )
                    if pairs:
                        members[scope_key][key].update(pairs)
                bound_entries[scope_key].extend(
                    item for item in entries if item.entry.source_id in bound
                )
                if bound != set(split_ids):
                    ambiguities[scope_key].append(
                        _partition_ambiguity(
                            native,
                            records,
                            entries,
                            split_ids,
                            bound,
                            expectations,
                            guard,
                        )
                    )
            elif native[1] == "sos":
                if len(sos_splits) + len(sos_renames) != 1:
                    raise ValueError(
                        f"{register.source_file}: expected one SOS identity declaration for {source_id}"
                    )
                is_split = bool(sos_splits)
                if is_split:
                    split_bases[scope_key].add(native)
                    declaration = sos_splits[0]
                    owners = {
                        cast("str", getattr(part, declaration.by)): part.owner
                        for part in declaration.parts
                    }
                    values = tuple(
                        _literal_field(record, declaration.by)
                        if declaration.by in ("data_type", "name", "description")
                        else (
                            record.subject.variant.name
                            if record.subject.variant.status == "value"
                            else None
                        )
                        for record in records
                    )
                    if (
                        set(values) != set(owners)
                        or len(owners) != len(declaration.parts)
                        or None in values
                    ):
                        continue
                else:
                    declaration = sos_renames[0]
                    owners = {
                        declaration.column: f"{register.register_info.native_id}.{declaration.column}"
                    }
                    if not any(
                        record.subject.variant.name == declaration.deldatamangd
                        and _literal_field(record, "name") == declaration.name
                        for record in records
                    ):
                        continue
                owner_entries = tuple(
                    item
                    for item in all_entries_by_register[source, native[:5]]
                    if item.entry.source_id in owners.values()
                )
                if set(owners.values()) - {
                    item.entry.source_id for item in owner_entries
                }:
                    continue
                expectations = capture_expectations(
                    records,
                    fields=("column_name", "name", "data_type", "description")
                    if is_split and sos_splits[0].by == "description"
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
                discriminators = _sos_owner_discriminators(owners)
                for value, owner in sorted(owners.items()):
                    if value != discriminators[value]:
                        continue
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

    naming = {}
    for scope in scopes:
        scope_key = scope.source, scope.register_key
        entries = tuple(bound_entries[scope_key])
        conversion = convert_naming(
            NamingSelection(
                files=(),
                entries=entries,
                freeze=tuple(
                    NamingFreezeSetting(
                        zone=provider, state=freeze_state(states, provider)
                    )
                    for provider in sorted(
                        {str(item.entry.provider) for item in entries}
                    )
                ),
            ),
            bindings[scope_key],
        )
        naming[scope_key] = tuple(
            sorted(
                conversion.declarations,
                key=lambda item: (item.target.kind, repr(item.target.source_key)),
            )
        )
    return (
        naming,
        {key: tuple(value) for key, value in ambiguities.items()},
        split_bases,
        {
            scope_key: {key: frozenset(rows) for key, rows in by_key.items()}
            for scope_key, by_key in members.items()
        },
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
            source_id in partition.columns.values()
            for partition in register.identity.partition
        )
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
    scopes: tuple[CompiledScope, ...],
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
    states = load_freeze_states(tree.root)
    scope_map = {(scope.source, scope.register_key): scope for scope in scopes}
    named_sos_registers = {
        register.register_info.native_id
        for register in tree.registers
        if register.register_info.provider == "sos"
        and any(variant.slug != "_default" for variant in register.variant)
    }
    bindings: dict[Any, list[LegacyNamingBinding]] = {key: [] for key in scope_map}
    subject_variant_keys: set[NativeKey] = set()
    for source in sorted({scope.source for scope in scopes}):
        wanted = {scope.register_key for scope in scopes if scope.source == source}
        for family_key, members in prepared.records.iter_naming_families(
            source, None if None in wanted else wanted
        ):
            scope_key = (source, family_key[:5])
            if None in wanted and scope_key not in scope_map:
                scope_key = (source, None)
            if family_key[-2] not in {
                "native-int",
                "native-str",
            }:
                continue
            provider = str(family_key[1])
            expectations = (
                capture_expectations(
                    cast("tuple[SourceRecord, ...]", members), fields=()
                )
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
        slices = prepared.records.iter_naming_register_slices(source, wanted)
        for register_key, records in slices:
            scope_key = source, register_key
            if scope_key not in scope_map:
                continue
            missing_sos_variant_parents = (
                bool(records)
                and records[0].subject.provider == "sos"
                and not any(
                    parent.kind == "variant"
                    for record in records
                    for parent in record.parent_facts
                )
            )
            subject_variant_members: dict[NativeKey, list[SourceRecord]] = defaultdict(
                list
            )
            if missing_sos_variant_parents and any(
                _naming_source_id("register", native_register) in named_sos_registers
                for record in records
                if (
                    native_register := source_register_key(cast("SourceRecord", record))
                )
                is not None
            ):
                # The naming projection omits source refs for SOS parents. This
                # authored parentless topology needs exact row-membership evidence.
                for member in prepared.records.iter_records(source=source):
                    if (
                        register_key is None
                        or source_register_key(member) == register_key
                    ) and member.subject.variant.status == "value":
                        native_key = native_variant_key(member)
                        assert native_key is not None
                        subject_variant_members[native_key].append(member)
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
                native_register = source_register_key(cast("SourceRecord", record))
                if (
                    record.subject.provider == "sos"
                    and missing_sos_variant_parents
                    and native_register is not None
                    and _naming_source_id("register", native_register)
                    in named_sos_registers
                    and record.subject.variant.status == "value"
                    and native_variant_key(cast("SourceRecord", record))
                    not in subject_variant_keys
                ):
                    native_key = native_variant_key(cast("SourceRecord", record))
                    assert native_key is not None
                    subject_variant_keys.add(native_key)
                    explicit_variants.add(native_register)
                    expectations = capture_expectations(
                        tuple(subject_variant_members[native_key]), fields=()
                    )
                    target = NativeNamingTarget(
                        kind="register_variant",
                        provider="sos",
                        source_key=native_key,
                        register_key=native_register,
                        expectations=expectations,
                        peer_guards=(
                            PeerGuard(
                                guard_id=f"native-table:{source}:{native_key!r}",
                                source=source,
                                coordinates=(
                                    ("register", record.subject.register_name),
                                    ("variant", record.subject.variant),
                                ),
                                expected_members=tuple(
                                    item.ref for item in expectations
                                ),
                            ),
                        ),
                    )
                    parents["register_variant", native_key] = LegacyNamingBinding(
                        kind="register_variant",
                        provider="sos",
                        source_id=_naming_source_id(
                            "register_variant",
                            native_key,
                            member=record.subject.variant.name,
                        ),
                        target=target,
                    )
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
            for register in tree.registers:
                if (
                    register.register_info.provider != "scb"
                    or not register.identity.edition_split
                ):
                    continue
                native_id = register.register_info.native_id
                assert native_id is not None
                native_register_id = int(native_id)
                matching = tuple(
                    record
                    for record in records
                    if record.subject.provider == "scb"
                    and (
                        native_register := source_register_key(
                            cast("SourceRecord", record)
                        )
                    )
                    is not None
                    and native_register[-1] == native_register_id
                )
                if not matching:
                    continue
                for entry in register.identity.edition_split:
                    for record in matching:
                        native = native_variant_key(cast("SourceRecord", record))
                        if native is None or native[-1] != int(
                            entry.variant.split(".")[1]
                        ):
                            continue
                        split_key = (
                            *native,
                            "edition-split",
                            entry.split.split(".")[2],
                        )
                        target = NativeNamingTarget(
                            kind="register_variant",
                            provider="scb",
                            source_key=split_key,
                            register_key=native[:5],
                        )
                        parents["register_variant", split_key] = LegacyNamingBinding(
                            kind="register_variant",
                            provider="scb",
                            source_id=entry.split,
                            target=target,
                        )
                        break
            bindings[scope_key].extend(parents.values())
    compiled_names = {}
    compiled_variants = {}
    compiled_provider_keys = {}
    diagnostics = []
    report: dict[str, dict[str, list[str]]] = {}
    register_scopes: dict[str, list[tuple[str, tuple[str | int, ...] | None]]] = {}
    for register in tree.registers:
        info = register.register_info
        name = f"{info.provider}/{info.slug}"
        for scope_key, candidates in bindings.items():
            if any(
                item.kind == "register"
                and item.provider == info.provider
                and item.source_id == str(info.native_id)
                for item in candidates
            ):
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
                    "not_evaluated_in_subset",
                )
            },
        )
        for entry, where in _register_naming_entries(tree, register):
            if _partition_owned_naming_entry(register, entry):
                continue
            ref = f"{entry.revision} {where}"
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
        selected = {
            name for name, owners in register_scopes.items() if scope_key in owners
        }
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
                        "not_evaluated_in_subset",
                    )
                },
            )
            for entry, where in _register_naming_entries(tree, register):
                if _partition_owned_naming_entry(register, entry):
                    continue
                ref = f"{entry.revision} {where}"
                statuses["entries_read"].append(ref)
                entries.append(entry)
                locations[entry.entry_id] = ref
                entry_owners[entry.entry_id] = name
        bound = {(b.kind, b.provider, b.source_id) for b in bindings[scope_key]}
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
                    NamingFreezeSetting(
                        zone=provider, state=freeze_state(states, provider)
                    )
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
            if target.kind != "register_variant" or (
                target.source_key[-2:] != ("variant", "not-applicable")
                and target.source_key not in subject_variant_keys
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
                            "name": "_default"
                            if target.source_key[-1] == "not-applicable"
                            else target.source_key[-1],
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


def compile_scb_preliminary(
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
) -> dict[Any, tuple[CurationCase, ...]]:
    """Retain paired preliminary editions as support for final native variables."""
    cases: dict[Any, tuple[CurationCase, ...]] = {}
    for scope in sorted(
        scopes, key=lambda item: (item.source, repr(item.register_key))
    ):
        if scope.register_key is None:
            records = prepared.records.iter_records(source=scope.source)
        else:
            records = (
                record
                for _, members in prepared.records.iter_register_slices(
                    scope.source, (scope.register_key,)
                )
                for record in members
            )
        editions: dict[tuple[Any, str, str], list[SourceRecord]] = defaultdict(list)
        for record in records:
            if record.subject.provider != "scb" or len(record.context) < 3:
                continue
            match = re.fullmatch(
                r"(\d{4}), (preliminär|slutlig) version", record.context[2]
            )
            variant = native_variant_key(record)
            if match is None or variant is None:
                continue
            editions[variant, match[1], match[2]].append(record)
        selected = []
        for (variant, year, kind), preliminary in sorted(
            editions.items(), key=lambda item: repr(item[0])
        ):
            if kind != "preliminär" or not (
                final := editions.get((variant, year, "slutlig"))
            ):
                continue
            final_variables = {
                record.subject.native.variable_id
                for record in final
                if record.subject.native.variable_id is not None
            }
            superseded = tuple(
                record
                for record in preliminary
                if record.subject.native.variable_id in final_variables
            )
            if not superseded:
                continue
            members = (*preliminary, *final)
            first = preliminary[0]
            register_id = first.subject.native.register_id
            variant_id = first.subject.native.register_variant_id
            case_id = f"superseded-preliminary:{register_id}:{variant_id}:{year}"
            targets = capture_expectations(superseded, fields=())
            selected.append(
                CurationCase(
                    case_id=case_id,
                    targets=targets,
                    # Source-use effects require a guard within this build.
                    peer_guards=(
                        PeerGuard(
                            guard_id=case_id,
                            source=first.source,
                            coordinates=(
                                ("register", first.subject.register_name),
                                ("variant", first.subject.variant),
                            ),
                            edition_scopes=tuple(
                                scope
                                for _, scope in sorted(
                                    {
                                        record.edition_scope.model_dump_json(): record.edition_scope
                                        for record in members
                                    }.items()
                                )
                            ),
                            expected_members=tuple(
                                sorted(
                                    {record_ref(record) for record in members}, key=str
                                )
                            ),
                        ),
                    ),
                    decision=OccurrenceCorrectionDecision(
                        reviewed=True,
                        effects=tuple(
                            CheckedSourceUse(ref=target.ref) for target in targets
                        ),
                        reason="The final SCB edition supersedes this native variable's preliminary delivery.",
                        provenance="SCB edition-name rule",
                    ),
                )
            )
        if selected:
            cases[scope.source, scope.register_key] = tuple(
                sorted(selected, key=lambda case: case.case_id)
            )
    return cases


def compile_provider_declarations(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
    *,
    subset: bool,
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    tuple[ResolutionDiagnostic, ...],
    dict[str, dict[str, list[str]]],
]:
    """Compile SOS routing and coverage from selected maintained provider declarations."""
    thin_sources = {
        entry.revision.dataset
        for entry in prepared.manifest.inputs
        if entry.role == "thin_provider" and entry.revision is not None
    }
    registers = {
        f"{entry.register_info.provider}/{entry.register_info.slug}": entry
        for entry in tree.registers
    }
    cases: dict[Any, list[CurationCase]] = {}
    diagnostics: list[ResolutionDiagnostic] = []
    report: dict[str, dict[str, list[str]]] = {}
    has_selected_provider = any(
        name in registers
        and (
            registers[name].register_info.provider == "sos"
            or scope.source in thin_sources
        )
        for scope in scopes
        for name, _ in _scope_registers(scope)
    )
    with open_value_bindings(
        prepared.value_sources if has_selected_provider else ()
    ) as sessions:
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
                        and (
                            registers[name].register_info.provider == "sos"
                            or scope.source in thin_sources
                        )
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
    for register in tree.registers:
        if register.register_info.provider != "sos" or not (
            register.errata.data_type or register.errata.classification_reference
        ):
            continue
        name = f"sos/{register.register_info.slug}"
        if name in report:
            continue
        statuses = report[name] = {
            key: []
            for key in (
                "entries_read",
                "entries_matched",
                "stale",
                "over_broad",
                "not_evaluated_in_subset",
            )
        }
        for table, entries in (
            ("data_type", register.errata.data_type),
            ("classification_reference", register.errata.classification_reference),
        ):
            for index, entry in enumerate(entries, 1):
                case_id = f"{register.source_file}#/errata.{table}/{index}"
                statuses["entries_read"].append(case_id)
                statuses["not_evaluated_in_subset" if subset else "stale"].append(
                    case_id
                )
                if not subset:
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code="stale_curation_entry",
                            severity="error",
                            case_id=case_id,
                            subject=entry.variable,
                            detail=f"{case_id}: owning SOS register has no selected source scope",
                            withheld_output=(case_id,),
                        )
                    )
    return (
        {
            key: tuple(sorted(value, key=lambda case: case.case_id))
            for key, value in cases.items()
        },
        tuple(diagnostics),
        report,
    )


def compile_edition_splits(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
    *,
    subset: bool,
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    tuple[ResolutionDiagnostic, ...],
    dict[str, dict[str, list[str]]],
]:
    cases: dict[Any, list[CurationCase]] = defaultdict(list)
    diagnostics = []
    report = {}
    for register in tree.registers:
        if not register.identity.edition_split:
            continue
        name = f"scb/{register.register_info.slug}"
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
        owners = [
            (scope.source, scope.register_key)
            for scope in scopes
            if any(item == name for item, _ in _scope_registers(scope))
        ]
        for index, entry in enumerate(register.identity.edition_split, 1):
            ref = f"{register.source_file}#/identity.edition_split/{index}"
            statuses["entries_read"].append(ref)
            if len(owners) != 1:
                status = (
                    "over_broad"
                    if owners
                    else "not_evaluated_in_subset"
                    if subset
                    else "stale"
                )
                statuses[status].append(ref)
                if status != "not_evaluated_in_subset":
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code="overbroad_curation_entry"
                            if owners
                            else "stale_curation_entry",
                            severity="error",
                            case_id=ref,
                            subject=entry.split,
                            detail=f"{ref}: matches {len(owners)} register scopes; expected one",
                            withheld_output=(ref,),
                        )
                    )
                continue
            scope_key = owners[0]
            native_register = next(
                key
                for item, key in _scope_registers(
                    next(
                        scope
                        for scope in scopes
                        if (scope.source, scope.register_key) == scope_key
                    )
                )
                if item == name
            )
            records = tuple(
                record
                for _, members in prepared.records.iter_register_slices(
                    scope_key[0], (native_register,)
                )
                for record in members
                if (native := native_variant_key(record)) is not None
                and native[-1] == int(entry.variant.split(".")[1])
            )
            by_name: dict[str, list[SourceRecord]] = defaultdict(list)
            edition_keys: dict[str, set[Any]] = defaultdict(set)
            for record in records:
                if label := _edition_label(record):
                    by_name[label].append(record)
                    edition_keys[label].update(
                        key
                        for parent in record.parent_facts
                        if parent.kind == "edition"
                        if (
                            key := native_parent_key(
                                record.source, record.subject.provider, parent
                            )
                        )
                        is not None
                    )
            declared = set(entry.editions) | set(entry.source_editions)
            unlisted = sorted(set(by_name) - declared)
            missing_or_ambiguous = sorted(
                edition
                for edition in declared
                if len(edition_keys[edition]) != 1 or not by_name[edition]
            )
            if unlisted or missing_or_ambiguous:
                statuses["stale"].append(ref)
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="stale_curation_entry",
                        severity="error",
                        case_id=ref,
                        subject=entry.split,
                        detail=(
                            f"{ref}: unlisted native editions {unlisted!r}; "
                            "listed names without exactly one native edition "
                            f"{missing_or_ambiguous!r}"
                        ),
                        withheld_output=(ref,),
                    )
                )
                continue
            selected = tuple(
                record for edition in entry.editions for record in by_name[edition]
            )
            source_variant = native_variant_key(records[0])
            assert source_variant is not None
            split_key = (*source_variant, "edition-split", entry.split.split(".")[2])
            targets = capture_expectations(
                selected,
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
            first = records[0]
            cases[scope_key].append(
                CurationCase(
                    case_id=ref,
                    targets=targets,
                    peer_guards=(
                        PeerGuard(
                            guard_id=ref,
                            source=first.source,
                            coordinates=(
                                ("register", first.subject.register_name),
                                ("variant", first.subject.variant),
                            ),
                            expected_members=tuple(
                                sorted(
                                    {record_ref(record) for record in records}, key=str
                                )
                            ),
                        ),
                    ),
                    decision=OccurrenceCorrectionDecision(
                        reviewed=True,
                        effects=tuple(
                            CheckedEditionRebind(ref=target.ref, variant_key=split_key)
                            for target in targets
                        ),
                        reason=entry.evidence,
                        provenance=ref,
                        data_warning=entry.data_warning,
                        data_warning_refs=tuple(target.ref for target in targets)
                        if entry.data_warning is not None
                        else (),
                        data_warning_fields=("availability",)
                        if entry.data_warning is not None
                        else (),
                    ),
                )
            )
            statuses["entries_matched"].append(ref)
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
    has_variant_parent = any(
        parent.kind == "variant" for record in records for parent in record.parent_facts
    )
    preserve_native_variants = not has_variant_parent and any(
        variant.slug != "_default" for variant in register.variant
    )
    for record in records:
        if preserve_native_variants and record.subject.variant.status == "value":
            key = native_variant_key(record)
            if key is not None and record.subject.variant.name is not None:
                parent_by_name[record.subject.variant.name].add(key)
        for parent in record.parent_facts:
            if parent.kind != "variant":
                continue
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
            if not has_variant_parent and not preserve_native_variants:
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
    for table, entries in (
        ("data_type", register.errata.data_type),
        ("classification_reference", register.errata.classification_reference),
    ):
        for index, entry in enumerate(entries, 1):
            case_id = f"{register.source_file}#/errata.{table}/{index}"
            statuses["entries_read"].append(case_id)
            peers = tuple(
                record
                for record in variables
                if record.subject.variable.status == "value"
                and record.subject.variable.native_id == entry.variable
                and record.subject.variant.status == "value"
                and record.subject.variant.name == entry.deldatamangd
            )
            matching = tuple(
                record
                for record in peers
                if _source_text(record.fields.column_name) == entry.column
            )
            is_type = isinstance(entry, ErrataDataTypeEntry)
            field_name = "data_type" if is_type else "classification_declared"
            expected_label = "type" if is_type else "classification reference"
            expected = entry.expected_type if is_type else entry.expected_reference
            expected_description = None if is_type else entry.expected_description
            valid = (
                len(peers) == 1
                and len(matching) == 1
                and _source_text(getattr(matching[0].fields, field_name)) == expected
                and _source_text(matching[0].fields.representation)
                == entry.expected_representation
                and (
                    expected_description is None
                    or _source_text(matching[0].fields.description)
                    == expected_description
                )
            )
            if not valid:
                status = "over_broad" if len(peers) > 1 else "stale"
                statuses[status].append(case_id)
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
                        detail=f"{case_id}: expected one original {entry.deldatamangd}/{entry.variable} peer and one {entry.column!r} record with {expected_label} {expected!r} and representation {entry.expected_representation!r}; found {len(peers)} peers and {len(matching)} column records",
                        refs=tuple(record_ref(record) for record in peers),
                        withheld_output=(case_id,),
                    )
                )
                continue
            record = matching[0]
            ref = record_ref(record)
            targets = capture_expectations(
                (record,),
                fields=("column_name", field_name, "representation")
                + (("description",) if expected_description is not None else ()),
            )
            replacement = (
                FieldExpectation(
                    name="data_type", status="value", value=entry.data_type
                )
                if is_type
                else FieldExpectation(
                    name="classification_declared", status="unknown", value=None
                )
            )
            cases.append(
                CurationCase(
                    case_id=case_id,
                    targets=targets,
                    peer_guards=(
                        PeerGuard(
                            guard_id=case_id,
                            source=record.source,
                            coordinates=(
                                ("register", record.subject.register_name),
                                ("variable", record.subject.variable),
                                ("variant", record.subject.variant),
                            ),
                            expected_members=(ref,),
                        ),
                    ),
                    decision=OccurrenceCorrectionDecision(
                        reviewed=True,
                        effects=(
                            CheckedFieldChange(
                                ref=ref,
                                replacement=replacement,
                                when=(
                                    FieldExpectation(
                                        name="column_name",
                                        status="value",
                                        value=entry.column,
                                    ),
                                    FieldExpectation(
                                        name=field_name,
                                        status="value",
                                        value=expected,
                                    ),
                                    FieldExpectation(
                                        name="representation",
                                        status="value",
                                        value=entry.expected_representation,
                                    ),
                                )
                                + (
                                    (
                                        FieldExpectation(
                                            name="description",
                                            status="value",
                                            value=expected_description,
                                        ),
                                    )
                                    if expected_description is not None
                                    else ()
                                ),
                            ),
                        ),
                        reason=entry.evidence,
                        provenance=f"{case_id}: {entry.evidence}",
                    ),
                )
            )
            statuses["entries_matched"].append(case_id)
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
    variant_periods = {}
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
            variant_periods[name] = record.edition_period_scope
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
            if name in variant_periods and variant_periods[name].kind == "pooled":
                if end is None:
                    raise ValueError(f"{case_id}: pooled thin coverage needs an end")
                period = TemporalScope(
                    kind="pooled",
                    label=variant_periods[name].label,
                    pooled_start=start,
                    pooled_end=end,
                )
            else:
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
        data_warning = next(
            (
                cell.interpreted_value
                for cell in record.delivered_cells
                if cell.name == "data_warning"
            ),
            None,
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
                    data_warning=data_warning,
                    data_warning_refs=(ref,) if data_warning is not None else (),
                    data_warning_fields=("name", "definition", "coding")
                    if data_warning is not None
                    else (),
                    effects=tuple(effects),
                    reason="Authored thin-provider record declares its own finite coverage.",
                    provenance=record.source,
                ),
            )
        )
    return tuple(cases)


def coding_entry_windows(entry: CodingEntry) -> tuple[tuple[str, str], ...]:
    """Normalize authored periods and exact supplied documentary scope alike."""
    source_scope = (
        entry.source_authority.source_scope
        if isinstance(entry, CodingDocumentedEntry)
        and entry.source_authority is not None
        else None
    )
    if source_scope is not None:
        bounds = coding_scope_bounds(source_scope)
        assert bounds is not None and len(bounds) == 1
        lower, upper = bounds[0]
        return (
            (date.fromordinal(lower).isoformat(), date.fromordinal(upper).isoformat()),
        )
    return tuple((start, end) for start, end in entry.periods)


def compile_coding_register(
    register: RegisterCuration,
    scope: CompiledScope,
    *,
    originals: tuple[SourceRecord, ...],
    columns: Mapping[NativeKey, tuple[SourceRecord, ...]],
    column_scopes: Mapping[NativeKey, frozenset[TemporalScope]],
    coding: Mapping[NativeKey, tuple[CodeListClaim, ...]],
    classifications: Mapping[str, ResolvedClassification] | None = None,
    value_bindings: Mapping[
        NativeKey, tuple[tuple[TemporalScope, ValueListBinding], ...]
    ]
    | None = None,
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
    compact_evidence = {}
    for kind, entries in (
        ("choice", register.coding.choice),
        ("uncoded", register.coding.uncoded),
        ("omit", register.coding.omit),
        ("extend", register.coding.extend),
        ("documented", register.coding.documented),
        ("sentinel", register.coding.sentinel),
        ("support", register.coding.support),
    ):
        for index, entry in enumerate(entries, 1):
            ref = f"{register.source_file}#/coding.{kind}/{index}"
            periods = coding_entry_windows(entry)
            source_scope = (
                entry.source_authority.source_scope
                if isinstance(entry, CodingDocumentedEntry)
                and entry.source_authority is not None
                else None
            )
            variables = {
                item.target.source_key
                for item in scope.naming
                if item.target.kind == "variable"
                and item.target.register_key in register_keys
                and (
                    item.naming.source_id == entry.variable
                    or (
                        kind not in {"documented", "sentinel", "support"}
                        and item.target.source_key[-2] == "accepted-partition"
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
                for period_index, (start, end) in enumerate(periods, 1):
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
            for period_index, (start, end) in enumerate(periods, 1):
                case_id = f"{ref}/period/{period_index}"
                source_windows = (
                    (date.fromordinal(lo).isoformat(), date.fromordinal(hi).isoformat())
                    for column_scope in column_scopes.get(column, ())
                    for lo, hi in (coding_scope_bounds(column_scope) or ())
                )
                used = covers_window(source_windows, start, end)
                selection = None
                if isinstance(entry, CodingSentinelEntry):
                    if (
                        classifications is None
                        or entry.classification not in classifications
                    ):
                        raise ValueError(
                            "scoped sentinel requires a converted classification"
                        )
                    book = classifications[entry.classification]
                    canonical_codes = {code.code for code in book.codes}
                    accepted = set(map(tuple, entry.members))
                    resolved = resolve_code_membership(claims)
                    matching = [
                        (segment.valid_from, segment.valid_to)
                        for segment in resolved.segments
                        if segment.period_scope == "intervals"
                        and segment.valid_from is not None
                        and segment.valid_to is not None
                        and segment.code_set is not None
                        and accepted <= set(segment.code_set.members)
                        and all(
                            code not in canonical_codes
                            and all(
                                c != code or label == meaning
                                for c, label in segment.code_set.members
                            )
                            for code, meaning in accepted
                        )
                    ]
                    status, detail = (
                        ("matched", "")
                        if covers_window(matching, start, end)
                        else (
                            "stale",
                            "exact noncanonical sentinel members no longer cover the window",
                        )
                    )
                else:
                    selection, status, detail = compile_coding_selection(
                        entry, kind, claims, start, end
                    )
                evidence_digest = (
                    entry.expected_evidence_sha256
                    if isinstance(entry, (CodingChoiceEntry, CodingExtendEntry))
                    else None
                )
                compact = evidence_digest is not None
                if compact:
                    if column not in compact_evidence:
                        refs = {record_ref(record) for record in records}
                        full_originals = tuple(
                            record for record in originals if record_ref(record) in refs
                        )
                        raw_codings = tuple(coding_source_sha256(c) for c in claims)
                        compact_evidence[column] = (
                            full_originals,
                            raw_codings,
                            acknowledgement_evidence_sha256(
                                full_originals, raw_codings
                            ),
                            capture_expectations(
                                full_originals,
                                fields=tuple(SourceFields.model_fields),
                                parents=True,
                                coding=True,
                            ),
                        )
                    if compact_evidence[column][2] != evidence_digest:
                        status, detail = (
                            "stale",
                            "complete source coding evidence changed",
                        )
                if (
                    isinstance(
                        entry,
                        (CodingDocumentedEntry, CodingChoiceEntry, CodingExtendEntry),
                    )
                    and entry.source_authority is not None
                ):
                    authority = entry.source_authority
                    authority_refs = {record_ref(record) for record in records}
                    authority_records = tuple(
                        r for r in originals if record_ref(r) in authority_refs
                    )
                    expected = capture_expectations(
                        authority_records,
                        fields=tuple(SourceFields.model_fields),
                        parents=True,
                        coding=True,
                    )
                    enumeration_matches = (
                        isinstance(entry, CodingDocumentedEntry)
                        and authority.enumeration is not None
                        and authority.enumeration.matches_members(entry.members)
                        and all(
                            authority.enumeration.matches_fields(record.fields)
                            for record in authority_records
                        )
                        and (
                            authority.enumeration.matches_unlabelled_members(
                                (member.code, member.label)
                                for claim in claims
                                for member in claim.members
                            )
                            if authority.enumeration.syntax == "kategori-alpha-equals"
                            else not claims
                            if authority.enumeration.syntax
                            == "ascii-decimal-comma-equals"
                            else value_bindings is not None
                            and marker_binding_fingerprints(
                                value_bindings.get(column, ()), start, end
                            )
                            == tuple(sorted(authority.marker_bindings or ()))
                        )
                    )
                    if (
                        tuple(authority.records) != expected
                        or (
                            source_scope is not None
                            and authority.period_block is None
                            and (
                                column_scopes.get(column) != frozenset((source_scope,))
                                or any(claim.scope != source_scope for claim in claims)
                            )
                        )
                        or {
                            locator
                            for record in authority_records
                            for locator in record.locators
                        }
                        != set(authority.locators)
                        or any(
                            record.source_revision_id != authority.revision.revision_id
                            for record in records
                        )
                        or (
                            authority.raw_codings is not None
                            and tuple(sorted(authority.raw_codings))
                            != tuple(
                                sorted(
                                    {coding_source_sha256(claim) for claim in claims}
                                )
                            )
                        )
                        or tuple(sorted(authority.codings))
                        != copied_coding_fingerprints(claims)
                        or (
                            isinstance(entry, CodingDocumentedEntry)
                            and (
                                not enumeration_matches
                                if authority.enumeration is not None
                                else not authority.label_equivalences
                                and authority.period_block is None
                                and {
                                    member.code
                                    for claim in claims
                                    for member in claim.members
                                }
                                != {code for code, _ in entry.members}
                            )
                        )
                        or (
                            isinstance(entry, CodingDocumentedEntry)
                            and authority.enumeration is None
                            and authority.period_block is None
                            and not documented_labels_match(
                                documented_source_members(
                                    claims,
                                    entry.version_label,
                                    reviewed_labels=bool(authority.label_equivalences),
                                    witness=authority.witness,
                                ),
                                entry.members,
                                authority.label_equivalences,
                            )
                        )
                    ):
                        status, detail = (
                            "stale",
                            "exact source-row coding authority changed",
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
                if isinstance(entry, (CodingChoiceEntry, CodingExtendEntry)) and (
                    entry.source_authority is not None or compact
                ):
                    assert selection is not None and not isinstance(selection, str)
                    selection = selection.model_copy(
                        update={
                            "expected_raw_codings": tuple(
                                sorted(set(compact_evidence[column][1]))
                                if compact
                                else sorted(entry.source_authority.raw_codings or ())
                                if entry.source_authority is not None
                                else ()
                            )
                        }
                    )
                assert selection is not None or isinstance(entry, CodingSentinelEntry)
                target_refs = {record_ref(record) for record in records}
                target_records = (
                    authority_records
                    if isinstance(entry, (CodingChoiceEntry, CodingExtendEntry))
                    and entry.source_authority is not None
                    else tuple(
                        record
                        for record in originals
                        if record_ref(record) in target_refs
                    )
                )
                if {record_ref(record) for record in target_records} != target_refs:
                    raise ValueError(f"{case_id}: target refs left the original scope")
                targets = capture_expectations(
                    target_records,
                    fields=tuple(SourceFields.model_fields)
                    if kind in {"documented", "uncoded", "sentinel", "support"}
                    else ("column_name",),
                    coding=kind in {"documented", "sentinel", "support"},
                )
                if (
                    isinstance(
                        entry,
                        (CodingDocumentedEntry, CodingChoiceEntry, CodingExtendEntry),
                    )
                    and entry.source_authority is not None
                ):
                    targets = tuple(entry.source_authority.records)
                elif compact:
                    targets = compact_evidence[column][3]
                guard = PeerGuard(
                    guard_id=case_id,
                    source=scope.source,
                    effective_column=column,
                    expected_members=tuple(target.ref for target in targets),
                )
                if isinstance(entry, CodingSentinelEntry):
                    assert classifications is not None
                    cases.append(
                        CurationCase(
                            case_id=case_id,
                            targets=targets,
                            peer_guards=(guard,),
                            decision=ClassificationDecision(
                                reviewed=True,
                                column_key=column,
                                valid_from=start,
                                valid_to=end,
                                expected_codings=coding_expectations(
                                    claims, start, end
                                ),
                                expected_source_codings=copied_coding_fingerprints(
                                    claims
                                ),
                                classification=entry.classification,
                                expected_classification=canonical_sha256(
                                    classifications[entry.classification].model_dump(
                                        mode="json"
                                    )
                                ),
                                binding_scope="inline_coding",
                                sentinel_members=tuple(map(tuple, entry.members)),
                                reason=entry.reason,
                                provenance=entry.source,
                            ),
                        )
                    )
                    continue
                assert selection is not None
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
                            data_warning=getattr(entry, "data_warning", None),
                            reason=entry.reason,
                            provenance=(
                                f"{entry.source}\nSource rows: "
                                f"{entry.source_authority.revision.artifact_path}\n"
                                f"SHA256: {entry.source_authority.revision.artifact_sha256}\n"
                                f"Rows: {', '.join(locator.physical_table + ':' + locator.physical_record for locator in entry.source_authority.locators)}"
                                if isinstance(
                                    entry,
                                    (
                                        CodingDocumentedEntry,
                                        CodingChoiceEntry,
                                        CodingExtendEntry,
                                    ),
                                )
                                and entry.source_authority is not None
                                else f"{entry.source}\nDocument: {entry.document_url}\n"
                                f"SHA256: {entry.document_sha256}\n"
                                f"Pages: {', '.join(map(str, entry.document_pages or ()))}"
                                if isinstance(entry, CodingDocumentedEntry)
                                else entry.source
                            ),
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


def _select_occurrence_correction(
    register: RegisterCuration,
    entry: _OccurrenceCorrectionEntry,
    family: tuple[SourceRecord, ...],
) -> tuple[SourceRecord, ...]:
    return tuple(
        record
        for record in family
        if (
            f"{register.register_info.native_id}.{record.subject.variant.native_id}"
            if record.subject.variant.native_id is not None
            else record.subject.variant.name
        )
        == entry.variant
        and _literal_field(record, "column_name")
        == (
            None
            if isinstance(entry, ErrataSupportEntry)
            and entry.kind == "nonphysical_projection"
            else entry.column
        )
        and (
            entry.edition is None
            or (
                record.subject.native.edition_id is not None
                and str(record.subject.native.edition_id) == entry.edition
            )
        )
        and (
            not isinstance(entry, ErrataFieldEntry)
            or (
                entry.field
                not in {
                    "classification_declared",
                    "measurement_unit",
                    "column_name",
                    "representation",
                }
                and not (
                    entry.field == "name" and entry.expected_scope.kind == "intervals"
                )
            )
            or (
                record.edition_scope == entry.expected_scope
                and record.edition_period_scope == entry.expected_period
                and record.original_period_text == entry.expected_period_text
            )
        )
        and (
            not isinstance(entry, ErrataOccurrencePeriodEntry)
            or _occurrence_correction_matches(entry, (record,))
        )
        and (
            not isinstance(entry, ErrataFieldEntry)
            or entry.field != "measurement_unit"
            or (
                record.fields.measurement_unit is not None
                and record.fields.measurement_unit.value
                == next(
                    field.value
                    for field in entry.expected_fields
                    if field.name == "measurement_unit"
                )
            )
        )
    )


def _occurrence_correction_matches(
    entry: _OccurrenceCorrectionEntry, selected: tuple[SourceRecord, ...]
) -> bool:
    if not selected or any(
        record.original_period_text != entry.expected_period_text
        or record.edition_scope != entry.expected_scope
        or record.edition_period_scope != entry.expected_period
        for record in selected
    ):
        return False
    if isinstance(entry, ErrataFieldEntry) and entry.expected_records is not None:
        return tuple(entry.expected_records) == capture_expectations(
            selected, fields=tuple(SourceFields.model_fields), parents=True, coding=True
        )
    expected = {field.name: field for field in entry.expected_fields}
    return all(
        all(
            (
                FieldExpectation(name=field, status="absent")
                if value is None
                else FieldExpectation(
                    name=field, status=value.status, value=value.value
                )
            )
            == expected[field]
            for field in expected
            for value in (getattr(record.fields, field),)
        )
        for record in selected
    )


def _support_coding_sha256(
    records: tuple[SourceRecord, ...], sessions: tuple[ValueBindingSession, ...]
) -> str:
    observations: dict[tuple[SourceRecordRef, str | None], set[tuple[str, ...]]] = (
        defaultdict(set)
    )
    for record in records:
        observations[record_ref(record), _literal_field(record, "column_name")].add(
            copied_coding_fingerprints(bind_code_lists(record, sessions).claims)
        )
    return canonical_sha256(
        tuple(
            (ref.model_dump(mode="json"), column, sorted(fingerprints))
            for (ref, column), fingerprints in sorted(
                observations.items(), key=lambda item: str(item[0])
            )
        )
    )


def compile_occurrence_corrections(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
    *,
    subset: bool,
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    tuple[ResolutionDiagnostic, ...],
    dict[str, dict[str, list[str]]],
]:
    """Compile literal text and occurrence-period corrections from original evidence."""
    parent_metadata: dict[str, tuple[SourceRecord, ...]] = {}
    cases: dict[Any, list[CurationCase]] = defaultdict(list)
    diagnostics = []
    report: dict[str, dict[str, list[str]]] = {}
    locations: dict[str, list[Any]] = defaultdict(list)
    for scope in scopes:
        for name, key in _scope_registers(scope):
            locations[name].append((scope, key))
    guarded_fields = (
        "column_name",
        "name",
        "definition",
        "description",
        "operational_definition",
        "data_type",
        "representation",
        "classification_declared",
    )
    for register in tree.registers:
        entries = (
            ("support", register.errata.support),
            ("field", register.errata.field),
            ("occurrence_period", register.errata.occurrence_period),
        )
        if not any(rows for _, rows in entries):
            continue
        name = f"{register.register_info.provider}/{register.register_info.slug}"
        statuses = _family_status(report, name)
        matches = locations.get(name, ())
        location = matches[0] if len(matches) == 1 else None
        families = {}
        if location is not None:
            scope, register_key = location
            wanted = {
                entry.variable.rsplit(".", 1)[-1]
                for _, rows in entries
                for entry in rows
            }
            wanted.update(
                entry.authority.variable.rsplit(".", 1)[-1]
                for entry in register.errata.support
            )
            families = {
                str(key[-1]): members
                for key, members in prepared.records.iter_native_families(
                    scope.source,
                    (register_key,),
                    select_family=lambda key, wanted=wanted: str(key[-1]) in wanted,
                )
            }
        for table, rows in entries:
            for index, entry in enumerate(rows, 1):
                case_id = f"{register.source_file}#/errata.{table}/{index}"
                statuses["entries_read"].append(case_id)
                if not matches and subset:
                    statuses["not_evaluated_in_subset"].append(case_id)
                    continue
                family = families.get(entry.variable.rsplit(".", 1)[-1], ())
                selected = _select_occurrence_correction(register, entry, family)
                expected = {field.name: field for field in entry.expected_fields}
                chosen_refs = {record_ref(record) for record in selected}
                by_edition: dict[int | None, set[SourceRecordRef]] = defaultdict(set)
                for record in selected:
                    by_edition[record.subject.native.edition_id].add(record_ref(record))
                overbroad = (
                    len(matches) > 1
                    or any(len(refs) > 1 for refs in by_edition.values())
                    or (
                        isinstance(entry, ErrataOccurrencePeriodEntry)
                        and any(
                            record_ref(record) in chosen_refs
                            and _literal_field(record, "column_name") != entry.column
                            for record in family
                        )
                    )
                )
                valid = (
                    bool(selected)
                    and (
                        not isinstance(entry, ErrataOccurrencePeriodEntry)
                        or register.register_info.provider != "scb"
                        or entry.edition is not None
                    )
                    and (
                        not isinstance(entry, ErrataOccurrencePeriodEntry)
                        or register.register_info.provider != "sos"
                        or {"coverage_from", "coverage_to"}.issubset(expected)
                    )
                    and not overbroad
                    and _occurrence_correction_matches(entry, selected)
                )
                period_authority: tuple[SourceRecord, ...] = ()
                authority_guards: tuple[PeerGuard, ...] = ()
                if (
                    location is not None
                    and isinstance(entry, ErrataOccurrencePeriodEntry)
                    and entry.authority
                ):
                    authority_groups = tuple(
                        tuple(
                            prepared.records.lookup(
                                expected.ref.source, expected.ref.semantic_record_key
                            )
                        )
                        for expected in entry.authority
                    )
                    period_authority = tuple(
                        record for group in authority_groups for record in group
                    )
                    if scope.source not in parent_metadata:
                        parent_metadata[scope.source] = tuple(
                            prepared.records.iter_without_native_family(scope.source)
                        )
                    authority_guards = tuple(
                        PeerGuard(
                            guard_id=f"{case_id}:parent:{index}",
                            source=record.source,
                            coordinates=(
                                ("register", record.subject.register_name),
                                ("variant", record.subject.variant),
                                ("variable", record.subject.variable),
                            ),
                            expected_members=tuple(
                                expected.ref
                                for expected in entry.authority
                                if any(
                                    projection.subject == record.subject
                                    for projection in expected.alternatives
                                )
                            ),
                        )
                        for index, record in enumerate(period_authority)
                    )
                    routed_names = {
                        name
                        for route in register.identity.route
                        if route.deldatamangd == entry.variant
                        for name in route.variants
                    } | {entry.variant}
                    valid = valid and (
                        len({expected.ref for expected in entry.authority})
                        == len(entry.authority)
                        and all(len(group) == 1 for group in authority_groups)
                        and not evaluate_source_expectations(
                            (),
                            tuple(entry.authority),
                            authority_guards,
                            parent_metadata[scope.source],
                        )
                        and all(
                            record.source == scope.source
                            and source_register_key(record) == register_key
                            and record.subject.variable.status == "not_applicable"
                            and record.subject.variant.name in routed_names
                            and record.edition_scope.kind == "intervals"
                            and any(
                                parent.kind == "variant"
                                and parent.coordinate == record.subject.variant
                                and parent.fields.coverage_from is not None
                                and parent.fields.coverage_from.status == "value"
                                for parent in record.parent_facts
                            )
                            for record in period_authority
                        )
                        and all(
                            {field.name for field in projection.fields}
                            == set(SourceFields.model_fields)
                            and projection.subject is not None
                            and projection.edition_scope is not None
                            and projection.edition_period_scope is not None
                            and projection.parent_facts is not None
                            and projection.code_set_references is not None
                            for expected in entry.authority
                            for projection in expected.alternatives
                        )
                        and bool(
                            replacement_bounds := scope_bounds(entry.edition_scope)
                        )
                        and all(
                            any(
                                lower <= start and end <= upper
                                for lower, upper in (
                                    scope_bounds(record.edition_scope) or ()
                                )
                            )
                            for record in period_authority
                            for start, end in replacement_bounds
                        )
                    )
                authority_family: tuple[SourceRecord, ...] = ()
                if isinstance(entry, ErrataSupportEntry):
                    authority_family = families.get(
                        entry.authority.variable.rsplit(".", 1)[-1], ()
                    )
                    authority = _select_occurrence_correction(
                        register, entry.authority, authority_family
                    )
                    valid = (
                        valid
                        and _occurrence_correction_matches(entry.authority, authority)
                        and len({record_ref(r) for r in authority}) == 1
                    )
                    if valid:
                        with open_value_bindings(prepared.value_sources) as sessions:
                            valid = (
                                _support_coding_sha256(
                                    tuple(
                                        {
                                            r.record_id: r
                                            for r in family + authority_family
                                        }.values()
                                    ),
                                    sessions,
                                )
                                == entry.expected_coding_sha256
                            )
                if not valid:
                    statuses["over_broad" if overbroad else "stale"].append(case_id)
                    diagnostics.append(
                        _family_diagnostic(
                            case_id,
                            entry.variable,
                            "literal source coordinate has no originals or differs from the guarded prose/scopes",
                            refs=tuple(record_ref(record) for record in selected),
                            overbroad=overbroad,
                        )
                    )
                    continue
                assert location is not None
                scope, register_key = location
                effects = tuple(
                    CheckedFieldChange(
                        ref=record_ref(record),
                        replacement=FieldExpectation(
                            name=entry.field, status="value", value=entry.value
                        ),
                        when_scope=entry.expected_scope
                        if entry.field == "name"
                        and entry.expected_scope.kind == "intervals"
                        else None,
                        when_period=entry.expected_period
                        if entry.field == "name"
                        and entry.expected_scope.kind == "intervals"
                        else None,
                        when=(
                            FieldExpectation(
                                name="column_name", status="value", value=entry.column
                            ),
                            *(
                                tuple(
                                    field
                                    for field in entry.expected_fields
                                    if field.name != "column_name"
                                )
                                if entry.field
                                in {
                                    "classification_declared",
                                    "measurement_unit",
                                    "name",
                                    "column_name",
                                    "representation",
                                }
                                or entry.expected_records is not None
                                else ()
                            ),
                        ),
                    )
                    if isinstance(entry, ErrataFieldEntry)
                    else CheckedSourceUse(
                        ref=record_ref(record),
                        when=(
                            next(
                                field
                                for field in entry.expected_fields
                                if field.name == "column_name"
                            )
                            if entry.kind == "nonphysical_projection"
                            else FieldExpectation(
                                name="column_name", status="value", value=entry.column
                            ),
                        ),
                    )
                    if isinstance(entry, ErrataSupportEntry)
                    else CheckedPeriodChange(
                        ref=record_ref(record),
                        edition_scope=entry.edition_scope,
                        edition_period_scope=entry.edition_period_scope,
                    )
                    for record in selected
                )
                expected_names = set(expected)
                if isinstance(entry, ErrataSupportEntry):
                    expected_names.update(
                        field.name for field in entry.authority.expected_fields
                    )
                entry_guarded_fields = (
                    tuple(SourceFields.model_fields)
                    if isinstance(entry, ErrataFieldEntry)
                    and entry.expected_records is not None
                    else guarded_fields
                    + tuple(sorted(expected_names - set(guarded_fields)))
                )
                cases[scope.source, scope.register_key].append(
                    CurationCase(
                        case_id=case_id,
                        targets=capture_expectations(
                            tuple(
                                record
                                for record in family
                                if record_ref(record) in chosen_refs
                            ),
                            fields=entry_guarded_fields,
                            coding=True,
                            parents=True,
                        ),
                        support=capture_expectations(
                            tuple(
                                record
                                for record in {
                                    r.record_id: r for r in family + authority_family
                                }.values()
                                if record_ref(record) not in chosen_refs
                            ),
                            fields=entry_guarded_fields,
                            coding=True,
                            parents=(
                                isinstance(entry, ErrataSupportEntry)
                                and entry.kind == "nonphysical_projection"
                            )
                            or (
                                isinstance(entry, ErrataFieldEntry)
                                and entry.field == "column_name"
                            ),
                        )
                        + (
                            tuple(entry.authority)
                            if isinstance(entry, ErrataOccurrencePeriodEntry)
                            else ()
                        ),
                        peer_guards=tuple(
                            PeerGuard(
                                guard_id=f"{case_id}:{guarded_family[0].subject.variable}",
                                source=scope.source,
                                coordinates=(
                                    (
                                        "register",
                                        guarded_family[0].subject.register_name,
                                    ),
                                    ("variable", guarded_family[0].subject.variable),
                                ),
                                expected_members=tuple(
                                    sorted(
                                        {
                                            record_ref(record)
                                            for record in guarded_family
                                        },
                                        key=str,
                                    )
                                ),
                            )
                            for guarded_family in (
                                family,
                                () if authority_family == family else authority_family,
                            )
                            if guarded_family
                        )
                        + authority_guards,
                        decision=OccurrenceCorrectionDecision(
                            reviewed=True,
                            effects=effects,
                            reason=entry.evidence,
                            provenance=f"{case_id}: {entry.evidence}",
                        ),
                    )
                )
                statuses["entries_matched"].append(case_id)
    return (
        {key: tuple(value) for key, value in cases.items()},
        tuple(diagnostics),
        report,
    )


def _partition_memberships(
    partition_cases: dict[Any, tuple[CurationCase, ...]],
) -> dict[Any, dict[tuple[str | int, ...], frozenset[tuple[SourceRecordRef, str]]]]:
    """Read the literal, checked owners emitted by the partition compiler."""
    members: dict[
        Any, dict[tuple[str | int, ...], set[tuple[SourceRecordRef, str]]]
    ] = defaultdict(lambda: defaultdict(set))
    for scope_key, cases in partition_cases.items():
        for case in cases:
            if not isinstance(case.decision, OccurrenceCorrectionDecision):
                continue
            for effect in case.decision.effects:
                if not isinstance(effect, CheckedIdentityChange) or (
                    "accepted-partition" not in effect.variable_key
                    and not case.case_id.startswith("accepted-column-partitions:")
                ):
                    continue
                condition = effect.when[0] if len(effect.when) == 1 else None
                if (
                    condition is None
                    or condition.name != "column_name"
                    or condition.status != "value"
                    or not isinstance(condition.value, str)
                ):
                    raise ValueError(
                        f"{case.case_id}: partition owner lacks an exact literal column"
                    )
                members[scope_key][effect.variable_key].add(
                    (effect.ref, condition.value)
                )
    return {
        scope_key: {key: frozenset(rows) for key, rows in by_key.items()}
        for scope_key, by_key in members.items()
    }


def _swecov_columns(
    prepared: PreparedCatalogSources,
) -> dict[tuple[str, str], SourceColumnTypeDeclaration]:
    return index_swecov_column_types(
        item.declaration
        for item in prepared.iter_evidence()
        if isinstance(item, ReferenceEvidence)
        and isinstance(item.declaration, SourceColumnTypeDeclaration)
        and item.declaration.revision.artifact_path == SWECOV_COLUMN_TYPES_PATH
    )


def compile_errata(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
    partition_members: dict[
        Any, dict[tuple[str | int, ...], frozenset[tuple[SourceRecordRef, str]]]
    ],
    *,
    subset: bool,
    storage_columns: dict[tuple[str, str], SourceColumnTypeDeclaration] | None = None,
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    dict[Any, tuple[NamingDeclaration, ...]],
    dict[Any, tuple[tuple[tuple[str | int, ...], str], ...]],
    tuple[ResolutionDiagnostic, ...],
    dict[str, dict[str, list[str]]],
]:
    """Compile the SCB omission ledger against complete native variant slices."""
    loaded = resolve_scb_errata(
        tree.registers,
        classifications=frozenset(
            c.classification.short_name for c in tree.classifications
        ),
    )

    if not loaded and not any(
        register.errata.edition_period for register in tree.registers
    ):
        return {}, {}, {}, (), {}
    if storage_columns is None:
        storage_columns = _swecov_columns(prepared)
    locations: dict[int, list[tuple[Any, tuple[str | int, ...]]]] = defaultdict(list)
    for scope in scopes:
        for name, key in _scope_registers(scope):
            if name.startswith("scb/") and isinstance(key[-1], int):
                locations[key[-1]].append(((scope.source, scope.register_key), key))
    versions = defaultdict(list)
    for entry in loaded.versions:
        versions[entry.register_variant_id].append(entry)
    delivered = {
        (
            entry.register_id,
            entry.register_variant_id,
            fold_column(entry.column),
            entry.versions,
        ): entry
        for entry in loaded.delivered
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
            or register.errata.edition_period
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
        register_storage_columns = steward_column_types(
            register.register_info.steward_table_prefixes, storage_columns
        )
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
        variant_ids: dict[str, int] = {}
        for variant in register.variant:
            parsed = _parse_variant_id(variant.native_id)
            if len(parsed) == 3:
                continue  # Errata targets native records before edition rebind.
            _, variant_id = parsed
            variant_ids[variant.slug] = variant_id
            by_variant[variant_id] = tuple(
                r for r in records if r.subject.native.register_variant_id == variant_id
            )
        bound_editions = {}
        for variant_id, members in by_variant.items():
            if members:
                bound_editions[variant_id] = edition_bindings(
                    members, tuple(versions.get(variant_id, ()))
                )
        moved_editions = {}
        for split in register.identity.edition_split:
            variant_id = _parse_variant_id(split.variant)[1]
            for edition in bound_editions.get(variant_id, ()):
                if edition.native_id is not None and edition.name in split.editions:
                    moved_editions[edition.key] = (edition.name, split.split)
        variant_contexts: dict[int, ErrataVariantContext] = {}
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
        for index, row in enumerate(register.errata.edition_period, 1):
            case_id = f"{register.source_file}#/errata.edition_period/{index}"
            statuses["entries_read"].append(case_id)
            subject = f"{name}/{row.variant}/{row.name}"
            variant_id = variant_ids[row.variant]
            matching = tuple(
                record
                for record in by_variant.get(variant_id, ())
                if record.original_period_text == row.name
                and record.subject.native.edition_id is not None
            )
            edition_ids = {record.subject.native.edition_id for record in matching}
            if not matches and subset:
                statuses["not_evaluated_in_subset"].append(case_id)
                continue
            if len(matches) != 1 or len(edition_ids) != 1:
                overbroad = len(matches) > 1 or len(edition_ids) > 1
                statuses["over_broad" if overbroad else "stale"].append(case_id)
                diagnostics.append(
                    _family_diagnostic(
                        case_id,
                        subject,
                        f"matches {len(edition_ids)} native editions in "
                        f"{len(matches)} register scopes; expected one",
                        overbroad=overbroad,
                    )
                )
                continue
            if source_scopes(row.name)[2] != "unparseable_period" or any(
                record.edition_period_scope.kind != "unknown" for record in matching
            ):
                statuses["stale"].append(case_id)
                diagnostics.append(
                    _family_diagnostic(
                        case_id, subject, "named edition already has a parsed period"
                    )
                )
                continue
            interval = TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start=row.valid_from, end=row.valid_to),),
            )
            expectations = capture_expectations(matching, fields=())
            cases[scope_key].append(
                CurationCase(
                    case_id=case_id,
                    targets=expectations,
                    decision=OccurrenceCorrectionDecision(
                        reviewed=True,
                        effects=tuple(
                            CheckedPeriodChange(
                                ref=item.ref,
                                edition_scope=interval,
                                edition_period_scope=interval,
                            )
                            for item in expectations
                        ),
                        reason=row.evidence,
                        provenance=f"{case_id}: {row.evidence}",
                    ),
                )
            )
            statuses["entries_matched"].append(case_id)
        for table, entries in (
            ("delivered", register.errata.delivered),
            ("column", register.errata.column),
        ):
            for index, row in enumerate(entries, 1):
                case_id = f"{register.source_file}#/errata.{table}/{index}"
                statuses["entries_read"].append(case_id)
                subject = case_id
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
                else:
                    context = variant_contexts.get(variant_id)
                    if context is None:
                        context = ErrataVariantContext(
                            members, register_id, variant_id, editions
                        )
                        variant_contexts[variant_id] = context
                    if table == "delivered":
                        assert isinstance(row, ErrataDeliveredEntry)
                        assert row.versions is not None
                        entry = delivered[
                            register_id,
                            variant_id,
                            fold_column(row.column),
                            tuple(row.versions),
                        ]
                        donor_keys = {
                            native_variable_key(record)
                            for record in context.by_column.get(
                                fold_column(row.column), ()
                            )
                            if entry.native_variable_id is None
                            or record.subject.native.variable_id
                            == entry.native_variable_id
                        }
                        component_base_key = None
                        if len(donor_keys) == 1 and None not in donor_keys:
                            base = next(iter(donor_keys))
                            assert base is not None
                            native_id = next(
                                record.subject.native.variable_id
                                for record in context.by_column[fold_column(row.column)]
                                if native_variable_key(record) == base
                            )
                            by_edition: dict[NativeKey, set[NativeKey]] = defaultdict(
                                set
                            )
                            literal_owners = set()
                            for owner, pairs in partition_members.get(
                                scope_key, {}
                            ).items():
                                if owner[: len(base)] != base:
                                    continue
                                for record in context.by_variable.get(native_id, ()):
                                    literal = _literal_field(record, "column_name")
                                    if (
                                        literal is None
                                        or (record_ref(record), literal) not in pairs
                                    ):
                                        continue
                                    occurrence = source_occurrence(record)
                                    if occurrence.edition_key is None:
                                        continue
                                    by_edition[occurrence.edition_key].add(owner)
                                    if fold_column(literal) == fold_column(row.column):
                                        literal_owners.add(owner)
                            # Simultaneous reviewed components cannot all rewrite
                            # one negative omnibus row. Keep that row as evidence.
                            if len(literal_owners) == 1 and any(
                                len(owners) > 1 for owners in by_edition.values()
                            ):
                                component_base_key = base
                        result = convert_delivered_entry(
                            entry,
                            case_id=case_id,
                            context=context,
                            steward_table_prefixes=register.register_info.steward_table_prefixes,
                            storage_columns=register_storage_columns,
                            preserve_blank_native_base=component_base_key is not None,
                            additional_physical_column=(
                                row.upstream == "additional-physical-column-in-version"
                            ),
                            storage_column=row.storage_column,
                        )
                        blockers, converted = result.blockers, result.case
                    else:
                        entry = columns[
                            register_id, variant_id, fold_column(row.column)
                        ]
                        result = convert_column_entry(
                            entry,
                            case_id=case_id,
                            context=context,
                            declared_flags=frozenset(row.model_fields_set)
                            & {"is_identifier", "is_sensitive"},
                            steward_table_prefixes=register.register_info.steward_table_prefixes,
                            storage_columns=register_storage_columns,
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
                assert isinstance(converted.decision, OccurrenceCorrectionDecision)
                if isinstance(row, ErrataColumnEntry) and row.data_warning is not None:
                    converted = converted.model_copy(
                        update={
                            "decision": converted.decision.model_copy(
                                update={
                                    "data_warning": row.data_warning,
                                    "data_warning_fields": ("data_type",),
                                }
                            )
                        }
                    )
                assert isinstance(converted.decision, OccurrenceCorrectionDecision)
                moved_additions = sorted(
                    {
                        moved_editions[effect.edition_key]
                        for effect in converted.decision.effects
                        if isinstance(effect, CuratedOccurrenceAddition)
                        and effect.edition_key is not None
                        and effect.edition_key in moved_editions
                    }
                )
                if moved_additions:
                    statuses["stale"].append(case_id)
                    diagnostics.append(
                        _family_diagnostic(
                            case_id,
                            subject,
                            "errata addition targets editions moved to split variants "
                            f"{moved_additions!r}; add explicit rebinding for errata "
                            "additions before curating those editions",
                        )
                    )
                    continue
                if table == "delivered":
                    effects = []
                    rebinding_blockers = []
                    corrected_families = {}
                    for effect in converted.decision.effects:
                        blank_target = isinstance(effect, CheckedFieldChange) and (
                            effect.replacement.name == "column_name"
                            and effect.replacement.status == "value"
                        )
                        if isinstance(effect, CuratedOccurrenceAddition):
                            variable_key = effect.variable_key
                            if component_base_key == variable_key:
                                corrected_families[variable_key] = tuple(
                                    record
                                    for record in records
                                    if native_variable_key(record) == variable_key
                                )
                        elif blank_target:
                            assert isinstance(effect, CheckedFieldChange)
                            target_records = tuple(
                                record
                                for record in members
                                if record_ref(record) == effect.ref
                            )
                            native_keys = {
                                native_variable_key(r) for r in target_records
                            }
                            if len(native_keys) != 1 or None in native_keys:
                                rebinding_blockers.append(
                                    "corrected column has no unique native identity"
                                )
                                continue
                            variable_key = next(iter(native_keys))
                            assert variable_key is not None
                        else:
                            effects.append(effect)
                            continue
                        original_pairs = {
                            (record_ref(record), row.column)
                            for record in members
                            if source_occurrence(record).variable_key == variable_key
                            and _literal_field(record, "column_name") == row.column
                        }
                        owners = {
                            key
                            for key, pairs in partition_members.get(
                                scope_key, {}
                            ).items()
                            if key[: len(variable_key)] == variable_key
                            and pairs & original_pairs
                        }
                        partitioned = any(
                            key[: len(variable_key)] == variable_key
                            for key in partition_members.get(scope_key, {})
                        )
                        if len(owners) > 1:
                            rebinding_blockers.append(
                                f"literal column {row.column!r} has multiple split owners"
                            )
                        elif partitioned and not owners:
                            rebinding_blockers.append(
                                f"literal column {row.column!r} has no split owner"
                            )
                        if blank_target and len(owners) == 1:
                            assert isinstance(effect, CheckedFieldChange)
                            targets = tuple(
                                t for t in converted.targets if t.ref == effect.ref
                            )
                            if (
                                effect.replacement.value != row.column
                                or effect.when
                                or effect.when_scope is not None
                                or effect.when_period is not None
                                or len(targets) != 1
                                or any(
                                    not any(
                                        f.name == "column_name"
                                        and f.status == "negative"
                                        for f in projection.fields
                                    )
                                    for projection in targets[0].alternatives
                                )
                            ):
                                rebinding_blockers.append(
                                    "corrected literal lacks an exact negative-column guard"
                                )
                                continue
                            effects.extend(
                                (
                                    effect,
                                    CheckedIdentityChange(
                                        ref=effect.ref,
                                        variable_key=next(iter(owners)),
                                        when=(
                                            FieldExpectation(
                                                name="column_name", status="negative"
                                            ),
                                        ),
                                    ),
                                )
                            )
                            corrected_families[variable_key] = tuple(
                                record
                                for record in records
                                if native_variable_key(record) == variable_key
                            )
                        else:
                            effects.append(
                                effect.model_copy(
                                    update={"variable_key": next(iter(owners))}
                                )
                                if isinstance(effect, CuratedOccurrenceAddition)
                                and len(owners) == 1
                                else effect
                            )
                    if rebinding_blockers:
                        overbroad = any(
                            "multiple" in item for item in rebinding_blockers
                        )
                        statuses["over_broad" if overbroad else "stale"].append(case_id)
                        diagnostics.append(
                            _family_diagnostic(
                                case_id,
                                subject,
                                "; ".join(rebinding_blockers),
                                overbroad=overbroad,
                                refs=(record_ref(members[0]),),
                            )
                        )
                        continue
                    updates = {
                        "decision": converted.decision.model_copy(
                            update={"effects": tuple(effects)}
                        )
                    }
                    if corrected_families:
                        guarded = tuple(
                            record
                            for family in corrected_families.values()
                            for record in family
                        )
                        targets = capture_expectations(
                            guarded,
                            fields=tuple(SourceFields.model_fields),
                            parents=True,
                            coding=True,
                        )
                        refs = {t.ref for t in targets}
                        assert {t.ref for t in converted.targets} <= refs
                        updates.update(
                            targets=targets,
                            support=tuple(
                                t for t in converted.support if t.ref not in refs
                            ),
                            peer_guards=(
                                *converted.peer_guards,
                                *(
                                    PeerGuard(
                                        guard_id=f"{case_id}:post-delivery:{key!r}",
                                        source=family[0].source,
                                        coordinates=(
                                            (
                                                "register",
                                                family[0].subject.register_name,
                                            ),
                                            ("variable", family[0].subject.variable),
                                        ),
                                        expected_members=tuple(
                                            sorted(
                                                {record_ref(r) for r in family},
                                                key=repr,
                                            )
                                        ),
                                    )
                                    for key, family in sorted(
                                        corrected_families.items(),
                                        key=lambda item: repr(item[0]),
                                    )
                                ),
                            ),
                        )
                    converted = converted.model_copy(update=updates)
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
    scopes: tuple[CompiledScope, ...],
    naming: dict[Any, tuple[Any, ...]],
    partition_cases: dict[Any, tuple[CurationCase, ...]],
    errata_cases: dict[Any, tuple[CurationCase, ...]],
    *,
    subset: bool,
) -> tuple[
    dict[Any, tuple[CurationCase, ...]],
    tuple[ResolutionDiagnostic, ...],
    dict[str, dict[str, list[str]]],
]:
    """Bind delivery prose and search spellings to one compiled catalog name."""
    partition_members = _partition_memberships(partition_cases)
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
                subject = case_id
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
                by_split = partition_members.get(scope_key, {})
                if "accepted-partition" in variable_key:
                    base_key = variable_key[: variable_key.index("accepted-partition")]
                    siblings = {
                        key: pairs
                        for key, pairs in by_split.items()
                        if key[: len(base_key)] == base_key
                    }
                    selected_pairs = siblings.get(variable_key, frozenset())
                    if any(
                        pair in other_pairs
                        for pair in selected_pairs
                        for key, other_pairs in siblings.items()
                        if key != variable_key
                    ):
                        statuses["over_broad"].append(case_id)
                        diagnostics.append(
                            _family_diagnostic(
                                case_id,
                                subject,
                                "one literal source member belongs to multiple split variables",
                                overbroad=True,
                                refs=register_anchor(name),
                            )
                        )
                        continue
                    chosen = tuple(
                        record
                        for record in all_records
                        if (
                            record_ref(record),
                            _literal_field(record, "column_name"),
                        )
                        in selected_pairs
                    )
                    if not chosen:
                        statuses["stale"].append(case_id)
                        diagnostics.append(
                            _family_diagnostic(
                                case_id,
                                subject,
                                "split has no checked literal source members",
                                refs=register_anchor(name),
                            )
                        )
                        continue
                elif "declared-column" in variable_key:
                    selected_pairs = frozenset()
                    chosen = ()
                else:
                    selected_pairs = frozenset()
                    if (
                        sum(
                            key[: len(variable_key)] == variable_key for key in by_split
                        )
                        > 1
                    ):
                        statuses["over_broad"].append(case_id)
                        diagnostics.append(
                            _family_diagnostic(
                                case_id,
                                subject,
                                "native family resolves to multiple split variables",
                                overbroad=True,
                                refs=register_anchor(name),
                            )
                        )
                        continue
                    chosen = tuple(
                        r
                        for r in all_records
                        if source_occurrence(r).variable_key == variable_key
                    )
                chosen_refs = {record_ref(record) for record in chosen}
                checked_records = tuple(
                    record
                    for record in all_records
                    if record_ref(record) in chosen_refs
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
                    expectations = capture_expectations(
                        checked_records,
                        fields=("column_name", "description")
                        if selected_pairs
                        else ("description",),
                    )
                    effect_pairs = (
                        sorted(selected_pairs, key=lambda pair: (str(pair[0]), pair[1]))
                        if selected_pairs
                        else [(item.ref, None) for item in expectations]
                    )
                    effects = tuple(
                        CheckedFieldChange(
                            ref=ref,
                            replacement=FieldExpectation(
                                name="description",
                                status="value",
                                value=row.description,
                            ),
                            when=(
                                FieldExpectation(
                                    name="column_name", status="value", value=column
                                ),
                            )
                            if column is not None
                            else (),
                        )
                        for ref, column in effect_pairs
                    )
                    decision = OccurrenceCorrectionDecision(
                        reviewed=True,
                        effects=effects,
                        reason=row.provenance
                        or f"Accepted delivery description for {target_name}",
                        provenance=row.provenance or f"curation:{case_id}",
                    )
                    peer_guards = target.target.peer_guards
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
                    anchor_refs = {item.ref for item in target.target.expectations}
                    alias_records = chosen or tuple(
                        record
                        for record in all_records
                        if record_ref(record) in anchor_refs
                    )
                    if not alias_records:
                        statuses["stale"].append(case_id)
                        diagnostics.append(
                            _family_diagnostic(
                                case_id,
                                subject,
                                "target has no checked source records",
                                refs=register_anchor(name),
                            )
                        )
                        continue
                    alias_refs = {record_ref(record) for record in alias_records}
                    expectations = capture_expectations(
                        tuple(
                            record
                            for record in all_records
                            if record_ref(record) in alias_refs
                        ),
                        fields=("column_name",),
                    )
                    anchor = alias_records[0]
                    peer_guards = target.target.peer_guards or (
                        PeerGuard(
                            guard_id=f"{case_id}:variable",
                            source=anchor.source,
                            native=NativeCoordinates(
                                register_id=anchor.subject.native.register_id,
                                variable_id=anchor.subject.native.variable_id,
                            ),
                            expected_members=tuple(item.ref for item in expectations),
                        ),
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
                        case_id=case_id,
                        targets=expectations,
                        peer_guards=peer_guards,
                        decision=decision,
                    )
                )
                statuses["entries_matched"].append(case_id)
    return (
        {key: tuple(value) for key, value in cases.items()},
        tuple(diagnostics),
        report,
    )


def _partitioned_naming(
    naming: dict[Any, tuple[Any, ...]],
    partition_naming: dict[Any, tuple[Any, ...]],
    split_bases: dict[Any, set[NativeKey]],
) -> dict[Any, tuple[Any, ...]]:
    return {
        key: tuple(
            item
            for item in values
            if item.target.source_key not in split_bases.get(key, set())
        )
        + partition_naming.get(key, ())
        for key, values in naming.items()
    }


def compile_deferred_naming(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
    storage_columns: dict[tuple[str, str], SourceColumnTypeDeclaration] | None = None,
) -> tuple[
    dict[Any, tuple[Any, ...]],
    dict[Any, tuple[NamingAmbiguity, ...]],
]:
    """Compile only declarations used to classify out-of-slice references."""
    from .pipeline import CompiledScope

    naming, _, _, _, _ = compile_native_naming(tree, prepared, scopes, subset=True)
    scopes = tuple(
        CompiledScope.model_validate_json(
            scope.model_copy(
                update={"naming": naming.get((scope.source, scope.register_key), ())}
            ).model_dump_json(),
            context=GuardValidationContext(),
        )
        for scope in scopes
    )
    partition_naming, ambiguities, split_bases, partition_members = (
        compile_deferred_partitions(tree, prepared, scopes)
    )
    naming = _partitioned_naming(naming, partition_naming, split_bases)
    _, matrix_naming, _, _ = compile_matrix_repr(tree, prepared, scopes, naming)
    naming = {
        key: (*values, *matrix_naming.get(key, ())) for key, values in naming.items()
    }
    _, errata_naming, _, _, _ = compile_errata(
        tree,
        prepared,
        scopes,
        partition_members,
        subset=True,
        storage_columns=storage_columns,
    )
    for key, extra in errata_naming.items():
        naming[key] = (*naming.get(key, ()), *extra)
    return naming, ambiguities


def compile_curation(
    tree: CurationTree,
    prepared: PreparedCatalogSources,
    scopes: tuple[CompiledScope, ...],
    *,
    subset: bool = False,
    storage_columns: dict[tuple[str, str], SourceColumnTypeDeclaration] | None = None,
) -> CompiledCuration:
    """Compile global families, wiring, and exact issue acknowledgements."""
    from .pipeline import CompiledScope

    naming, variants, provider_keys, naming_diagnostics, naming_report = (
        compile_native_naming(tree, prepared, scopes, subset=subset)
    )
    scopes = tuple(
        CompiledScope.model_validate_json(
            scope.model_copy(
                update={"naming": naming.get((scope.source, scope.register_key), ())}
            ).model_dump_json(),
            context=GuardValidationContext(),
        )
        for scope in scopes
    )

    selected = {register for scope in scopes for register, _ in _scope_registers(scope)}
    unmatched: list[str] = []
    metadata, class_successions = compile_declared_metadata(
        tree, selected=selected, unmatched=unmatched
    )
    books = tuple(
        CompiledCodebook(
            source=f"classifications/{entry.classification.codes_file}",
            descriptor=normalize_token(Path(entry.classification.codes_file).stem),
            metadata={
                **entry.classification.model_dump(
                    mode="json",
                    exclude={
                        "aliases",
                        "family",
                        "family_aliases",
                        "codes_file",
                        "sentinel_codes",
                    },
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
                            fields=tuple(ack.fields),
                            valid_from=ack.valid_from,
                            valid_to=ack.valid_to,
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
                        fields=tuple(ack.fields),
                        valid_from=ack.valid_from,
                        valid_to=ack.valid_to,
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
                            fields=tuple(ack.fields),
                            valid_from=ack.valid_from,
                            valid_to=ack.valid_to,
                            register_key=register_key,
                            reason=ack.reason,
                            evidence=ack.evidence,
                            expected_evidence_sha256=ack.expected_evidence_sha256,
                        ),
                    )
                )
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
    naming = _partitioned_naming(naming, partition_naming, split_bases)
    provider_keys = {
        key: tuple(
            item for item in values if item[0] not in split_bases.get(key, set())
        )
        + partition_keys.get(key, ())
        for key, values in provider_keys.items()
    }
    correction_cases, correction_diagnostics, correction_report = (
        compile_occurrence_corrections(tree, prepared, scopes, subset=subset)
    )
    thin_cases, thin_diagnostics, thin_report = compile_provider_declarations(
        tree, prepared, scopes, subset=subset
    )
    preliminary_cases = compile_scb_preliminary(prepared, scopes)
    (
        errata_cases,
        errata_naming,
        errata_keys,
        errata_diagnostics,
        errata_report,
    ) = compile_errata(
        tree,
        prepared,
        scopes,
        _partition_memberships(partition_cases),
        subset=subset,
        storage_columns=storage_columns,
    )
    matrix_cases, matrix_names, matrix_keys, matrix_diagnostics = compile_matrix_repr(
        tree,
        prepared,
        tuple(
            scope.model_copy(
                update={
                    "cases": (
                        *scope.cases,
                        *partition_cases.get((scope.source, scope.register_key), ()),
                        *correction_cases.get((scope.source, scope.register_key), ()),
                        *thin_cases.get((scope.source, scope.register_key), ()),
                        *preliminary_cases.get((scope.source, scope.register_key), ()),
                        *errata_cases.get((scope.source, scope.register_key), ()),
                    )
                }
            )
            for scope in scopes
        ),
        naming,
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
    for key, extra in errata_cases.items():
        cases[key].extend(extra)
    for key, extra in errata_naming.items():
        naming[key] = (*naming.get(key, ()), *extra)
    for key, extra in errata_keys.items():
        provider_keys[key] = (*provider_keys.get(key, ()), *extra)
    for key, extra in correction_cases.items():
        cases[key].extend(extra)
    diagnostics.extend(correction_diagnostics)
    enrichment_cases, enrichment_diagnostics, enrichment_report = compile_enrichment(
        tree, prepared, scopes, naming, partition_cases, errata_cases, subset=subset
    )
    for key, extra in enrichment_cases.items():
        cases[key].extend(extra)
    for family_report in (errata_report, enrichment_report, correction_report):
        for register, statuses in family_report.items():
            current = report.setdefault(register, {key: [] for key in statuses})
            for status, entries in statuses.items():
                current.setdefault(status, []).extend(entries)
    for register, statuses in naming_report.items():
        current = report.setdefault(register, {key: [] for key in statuses})
        for key, values in statuses.items():
            current.setdefault(key, []).extend(values)
    for key, new_cases in thin_cases.items():
        cases[key].extend(new_cases)
    for key, new_cases in preliminary_cases.items():
        cases[key].extend(new_cases)
    for register, statuses in thin_report.items():
        current = report.setdefault(register, {key: [] for key in statuses})
        for key, values in statuses.items():
            current.setdefault(key, []).extend(values)
    split_cases, split_diagnostics, split_report = compile_edition_splits(
        tree, prepared, scopes, subset=subset
    )
    for key, new_cases in split_cases.items():
        cases[key].extend(new_cases)
    for register, statuses in split_report.items():
        current = report.setdefault(register, {key: [] for key in statuses})
        for key, values in statuses.items():
            current.setdefault(key, []).extend(values)
    variable_families: dict[str, set[tuple[str, tuple[str | int, ...]]]] = {}
    for scope in scopes:
        registers = {
            native_key: register for register, native_key in _scope_registers(scope)
        }
        for name in naming.get((scope.source, scope.register_key), ()):
            if (
                name.target.kind == "variable"
                and name.naming.slug is not None
                and (register := registers.get(name.target.register_key)) is not None
            ):
                variable_families.setdefault(
                    f"{register}/{name.naming.slug}", set()
                ).add((scope.source, name.target.source_key))
    from .source_documentary import compile_documentary_bindings

    documentary, documentary_issues = compile_documentary_bindings(
        tree, prepared, scopes, variable_families
    )
    metadata = metadata.model_copy(update={"documentary_relationships": documentary})
    diagnostics.extend(documentary_issues)
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
            *split_diagnostics,
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
