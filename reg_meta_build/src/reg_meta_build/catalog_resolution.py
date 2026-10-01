"""Common catalog resolution from prepared source facts and checked declarations."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta.source_evidence import SourceField

from reg_meta_build.resolved_catalog import (
    ResolvedEdition,
    ResolvedObjectType,
    ResolvedPopulation,
    ResolvedRegister,
    ResolvedVariant,
)
from reg_meta_build.source_coordinates import (
    NativeKey,
    _coordinate_key,
    native_parent_key,
    native_variant_key,
)
from reg_meta_build.source_curation import ResolutionDiagnostic
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_intervals import reconcile_source_fields
from reg_meta_build.source_occurrences import EffectiveOccurrence

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from reg_meta_build.source_curation import ParentFactProjection, SourceRecordRef
    from reg_meta_build.source_naming import NamingDeclaration
    from reg_meta_build.source_records import (
        SourceFields,
        SourceParentObservation,
        SourceRecord,
    )


@dataclass(frozen=True)
class ParentResolution:
    registers: dict[NativeKey, ResolvedRegister]
    variants: dict[NativeKey, ResolvedVariant]
    editions: dict[NativeKey, ResolvedEdition]
    fields: dict[NativeKey, SourceFields]
    diagnostics: tuple[ResolutionDiagnostic, ...]
    other_language_refs: tuple[SourceRecordRef, ...] = ()
    support_only_refs: tuple[SourceRecordRef, ...] = ()


def _text(fields: SourceFields, name: str) -> str | None:
    claim = getattr(fields, name)
    if claim is not None and claim.status == "value":
        if not isinstance(claim.value, str):
            raise TypeError(f"parent metadata field {name} must be text")
        return claim.value or None
    return None


def _parent_projection_key(
    source: str, provider: str | None, fact: ParentFactProjection
) -> NativeKey | None:
    """Rebuild the native parent key for one expected parent fact, if pinned."""
    if provider is None:
        return None
    register = _coordinate_key(fact.register_name)
    if register is None:
        return None
    key: NativeKey = (source, provider, "register", *register)
    if fact.kind == "register":
        return key
    variant = _coordinate_key(fact.variant) if fact.variant is not None else None
    if fact.kind != "variant" or variant is None:
        return None
    return (*key, "variant", *variant)


def _declared_parent_name(declaration: NamingDeclaration, key: NativeKey) -> str | None:
    """The single checked name a parent naming declaration pins for this parent.

    The checked name lives in the target's record expectations' parent facts.
    Identity-revision targets pin no name, and several distinct pinned names
    cannot resolve a conflict without a positional selection rule, so neither
    yields a declared name.
    """
    provider = declaration.target.provider
    pinned: set[str] = set()
    for expectation in declaration.target.expectations:
        for projection in expectation.alternatives:
            for fact in projection.parent_facts or ():
                if (
                    _parent_projection_key(expectation.ref.source, provider, fact)
                    != key
                ):
                    continue
                pinned.update(
                    field.value
                    for field in fact.fields
                    if field.name == "name"
                    and field.status == "value"
                    and isinstance(field.value, str)
                    and field.value
                )
    return next(iter(pinned)) if len(pinned) == 1 else None


def resolve_parents(
    records: Iterable[SourceRecord | EffectiveOccurrence],
    naming: tuple[NamingDeclaration, ...],
    *,
    language: str = "sv",
    withheld_naming: frozenset[NativeKey] = frozenset(),
    rebinds: Mapping[SourceRecordRef, NativeKey] = {},
) -> ParentResolution:
    """Resolve supplied parent facts; names come from checked naming declarations.

    All distinct parent assertions are reconciled. Repeated physical rows do not
    choose a winner or multiply prose. The complete occurrence ledger remains in
    the prepared source artifact; each competing assertion keeps a representative
    source member for an actionable diagnostic.
    """
    claims: dict[
        NativeKey, dict[SourceFields, tuple[SourceParentObservation, SourceRecordRef]]
    ] = defaultdict(dict)
    ownership: dict[
        NativeKey, tuple[str, NativeKey, NativeKey | None, NativeKey | None]
    ] = {}
    diagnostics = []
    other_language_refs = set()
    support_only_refs = set()

    names = {}
    for declaration in naming:
        if declaration.target.kind not in {"register", "register_variant"}:
            continue
        key = declaration.target.source_key
        if key in names and names[key] != declaration:
            raise ValueError(f"multiple checked names for parent: {key!r}")
        names[key] = declaration

    def catalog_records():
        for record in records:
            if isinstance(record, EffectiveOccurrence):
                if record.use == "support":
                    support_only_refs.update(
                        record_ref(source) for source in record.source_records
                    )
                yield from (
                    (source, record.use == "support")
                    for source in record.source_records
                )
            else:
                yield record, False

    for record, support_only in catalog_records():
        # A supplied translation is not a competing assertion in the catalog's
        # requested language. Retain its role explicitly alongside the raw source.
        if record.parent_facts and record.language not in {None, language}:
            other_language_refs.add(record_ref(record))
            continue
        for parent in record.parent_facts:
            key = native_parent_key(record.source, record.subject.provider, parent)
            if key is None:
                if support_only:
                    continue
                # Empty population/object metadata is legitimately unspecified.
                if any(
                    field is not None and field.status != "unknown"
                    for name in type(parent.fields).model_fields
                    for field in (getattr(parent.fields, name),)
                ):
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code="unplaced_parent_metadata",
                            severity="error",
                            subject=parent.kind,
                            detail="Parent metadata has supplied facts but lacks its source coordinate.",
                            refs=(record_ref(record),),
                            fields=("identity",),
                            withheld_output=(parent.kind,),
                        )
                    )
                continue
            split_key = rebinds.get(record_ref(record))
            raw_variant_key = native_variant_key(record)
            if (
                split_key is not None
                and raw_variant_key is not None
                and parent.kind != "register"
                and key[: len(raw_variant_key)] == raw_variant_key
            ):
                key = (*split_key, *key[len(raw_variant_key) :])
            register = _coordinate_key(parent.register_name)
            assert register is not None
            register_key = (
                record.source,
                record.subject.provider,
                "register",
                *register,
            )
            variant = (
                _coordinate_key(parent.variant) if parent.variant is not None else None
            )
            variant_key = (
                (*register_key, "variant", *variant) if variant is not None else None
            )
            if split_key is not None and variant_key == raw_variant_key:
                variant_key = split_key
            edition = (
                _coordinate_key(parent.edition) if parent.edition is not None else None
            )
            edition_key = (
                (*variant_key, "edition", *edition)
                if variant_key is not None and edition is not None
                else None
            )
            if support_only and (
                register_key not in names
                or register_key in withheld_naming
                or (
                    parent.kind != "register"
                    and (variant_key not in names or variant_key in withheld_naming)
                )
            ):
                # Lookup/support-only topology remains raw evidence. Independent
                # parent facts survive only within already admitted catalog parents.
                continue
            alternatives = claims[key]
            if parent.fields not in alternatives:
                alternatives[parent.fields] = parent, record_ref(record)
            owner = parent.kind, register_key, variant_key, edition_key
            if key in ownership and ownership[key] != owner:
                raise ValueError(f"inconsistent native parent topology: {key!r}")
            ownership[key] = owner
    resolved_fields = {}
    for key, alternatives in claims.items():
        observations = tuple(item[0] for item in alternatives.values())
        fields, conflicts = reconcile_source_fields(observations)
        if (
            "name" in conflicts
            and ownership[key][0] in {"register", "variant"}
            and key in names
            and key not in withheld_naming
        ):
            # A checked naming declaration endorses one observed parent name. The
            # declaration only resolves the name it pins; every other conflict
            # stays unresolved exactly as without it, and no positional rule
            # picks between several pinned names or overrides a stale declaration.
            declared = _declared_parent_name(names[key], key)
            observed = {
                field.value
                for observation in observations
                if (field := observation.fields.name) is not None
                and field.status == "value"
                and isinstance(field.value, str)
            }
            if declared is not None and declared in observed:
                fields = fields.model_copy(
                    update={"name": SourceField(status="value", value=declared)}
                )
                conflicts = tuple(name for name in conflicts if name != "name")
        resolved_fields[key] = fields
        if conflicts:
            diagnostics.append(
                ResolutionDiagnostic(
                    code="conflicting_parent_metadata",
                    severity="error",
                    subject=repr(key),
                    detail="Source observations disagree on parent metadata; no first/latest source value was selected.",
                    refs=tuple(
                        dict.fromkeys(item[1] for item in alternatives.values())
                    ),
                    fields=conflicts,
                    withheld_output=tuple(
                        f"{ownership[key][0]}.{name}" for name in conflicts
                    ),
                )
            )

    registers: dict[NativeKey, ResolvedRegister] = {}
    variants: dict[NativeKey, ResolvedVariant] = {}
    populations = defaultdict(list)
    objects = defaultdict(list)
    for kind in ("register", "variant", "population", "object_type"):
        for key, (
            parent_kind,
            register_key,
            _variant_key,
            edition_key,
        ) in ownership.items():
            if parent_kind != kind:
                continue
            fields = resolved_fields[key]
            name = _text(fields, "name")
            if name is None:
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="unknown_parent_name",
                        severity="error",
                        subject=repr(key),
                        detail="The source does not establish one parent name.",
                        refs=tuple(
                            dict.fromkeys(item[1] for item in claims[key].values())
                        ),
                        fields=("name",),
                        withheld_output=(kind,),
                    )
                )
                continue
            if kind in {"register", "variant"}:
                if key in withheld_naming:
                    diagnostics.append(
                        ResolutionDiagnostic(
                            code="withheld_parent_naming",
                            severity="error",
                            subject=repr(key),
                            detail="The parent's naming declaration no longer applies to the selected source evidence.",
                            refs=tuple(
                                dict.fromkeys(r for _, r in claims[key].values())
                            ),
                            fields=("identity",),
                            withheld_output=(kind,),
                        )
                    )
                    continue
                if key not in names:
                    raise ValueError(f"missing checked parent naming binding: {key!r}")
                declaration = names[key]
                expected_kind = "register" if kind == "register" else "register_variant"
                if (
                    declaration.target.kind != expected_kind
                    or declaration.target.provider != key[1]
                ):
                    raise ValueError(
                        f"naming binding disagrees with parent identity: {key!r}"
                    )
                slug = declaration.naming.slug
                if slug is None:
                    raise ValueError(
                        f"parent naming conversion did not establish a slug: {key!r}"
                    )
                if kind == "register":
                    registers[key] = ResolvedRegister(
                        provider=str(key[1]),
                        slug=slug,
                        name=name,
                        purpose=_text(fields, "purpose"),
                    )
                elif register_key in registers:
                    variants[key] = ResolvedVariant.model_validate(
                        {
                            "slug": slug,
                            "name": name,
                            "description": _text(fields, "description"),
                            "display_group": declaration.naming.display_group,
                            "panel_entity_key": declaration.naming.panel_entity_key,
                            "panel_time_key": declaration.naming.panel_time_key,
                            "panel_time_grain": declaration.naming.panel_time_grain,
                        }
                    )
            elif kind == "population":
                populations[edition_key].append(
                    ResolvedPopulation(
                        name=name,
                        definition=_text(fields, "population_definition"),
                        comment=_text(fields, "population_comment"),
                        date_range=_text(fields, "population_date"),
                    )
                )
            else:
                objects[edition_key].append(
                    ResolvedObjectType(
                        name=name, definition=_text(fields, "definition")
                    )
                )
    editions = {}
    for key, (kind, register_key, variant_key, _edition_key) in ownership.items():
        if (
            kind != "edition"
            or register_key not in registers
            or variant_key not in variants
        ):
            continue
        fields = resolved_fields[key]
        name = _text(fields, "name")
        if name is None:
            diagnostics.append(
                ResolutionDiagnostic(
                    code="unknown_parent_name",
                    severity="error",
                    subject=repr(key),
                    detail="The source does not establish one edition name.",
                    refs=tuple(dict.fromkeys(item[1] for item in claims[key].values())),
                    fields=("name",),
                    withheld_output=("edition",),
                )
            )
            continue
        editions[key] = ResolvedEdition(
            register=registers[register_key],
            variant=variants[variant_key],
            name=name,
            description=_text(fields, "description"),
            measurement_information=_text(fields, "measurement_information"),
            documentation_status=_text(fields, "documentation_status"),
            first_approved_at=_text(fields, "first_approved_at"),
            last_approved_at=_text(fields, "last_approved_at"),
            populations=tuple(sorted(populations[key], key=lambda item: item.name)),
            object_types=tuple(sorted(objects[key], key=lambda item: item.name)),
        )
    return ParentResolution(
        registers,
        variants,
        editions,
        resolved_fields,
        tuple(diagnostics),
        tuple(
            sorted(
                other_language_refs,
                key=lambda ref: (ref.source, ref.semantic_record_key),
            )
        ),
        tuple(
            sorted(
                support_only_refs, key=lambda ref: (ref.source, ref.semantic_record_key)
            )
        ),
    )
