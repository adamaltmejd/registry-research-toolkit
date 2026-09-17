"""Explicit resolved discovery, relationship and reference metadata.

The preparation below resolves only exact declared references into storage IDs.
It never derives groups, relations, lineage, source precedence or missing facts.
"""

from __future__ import annotations

import json
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import Field, field_validator, model_validator
from reg_meta.fqid import FqidKind, parse, validate_slug

from reg_meta_build._resolved_common import (
    _classification_id,
    _require_trimmed,
    _ResolvedModel,
    _ResolvedWindow,
    _storage_id,
)
from reg_meta_build.concept_groups import _is_path_safe_key
from reg_meta_build.id import mint
from reg_meta_build.relations import (
    _reject_oversized_components,
    _reject_same_as_cycles,
    reject_nonmonotone_representation_cycles,
    reject_replaced_by_cycles,
)

if TYPE_CHECKING:
    import sqlite3

    from reg_meta_build.resolved_catalog import (
        ResolvedClassification,
        ResolvedRegister,
        ResolvedVariable,
        ResolvedVariant,
    )


def _fqid(value: str, *kinds: FqidKind) -> str:
    if parse(value).kind not in kinds:
        raise ValueError(f"wrong resolved reference kind: {value}")
    return value


def _variable(value: str) -> str:
    return _fqid(value, FqidKind.VARIABLE_BINDING)


def _register(value: str) -> str:
    return _fqid(value, FqidKind.REGISTER)


def _slug(value: str) -> str:
    validate_slug(value, "slug")
    return value


def _variant_slug(value: str) -> str:
    validate_slug(value, "register_variant", allow_default=True)
    return value


class ResolvedGroupAxis(_ResolvedModel):
    axis: str
    ordinal: int = Field(ge=0)
    label: str

    _text = field_validator("axis", "label")(_require_trimmed)


class ResolvedGroupFacet(_ResolvedModel):
    axis: str
    value: str
    label: str

    _text = field_validator("axis", "value", "label")(_require_trimmed)


class ResolvedGroupVariable(_ResolvedModel):
    variable: str
    delivery_column_name: str | None = None
    facets: tuple[ResolvedGroupFacet, ...] = ()

    _ref = field_validator("variable")(_variable)

    @field_validator("delivery_column_name")
    @classmethod
    def _column(cls, value: str | None) -> str | None:
        return _require_trimmed(value) if value is not None else None


class ResolvedGroupClassification(_ResolvedModel):
    classification: str
    facet_value: str
    facet_label: str

    _classification = field_validator("classification")(_slug)
    _text = field_validator("facet_value", "facet_label")(_require_trimmed)


class _ResolvedGroup(_ResolvedModel):
    key: str
    label: str
    source: Literal["edge", "token", "curated"]
    axes: tuple[ResolvedGroupAxis, ...] = ()

    _label = field_validator("label")(_require_trimmed)

    @field_validator("key")
    @classmethod
    def _key(cls, value: str) -> str:
        if not _is_path_safe_key(value):
            raise ValueError("resolved group key must be one path-safe segment")
        return value

    @model_validator(mode="after")
    def _axes(self) -> Self:
        _unique((axis.axis for axis in self.axes), "group axis")
        _unique((axis.ordinal for axis in self.axes), "group axis ordinal")
        return self


class ResolvedVariableGroup(_ResolvedGroup):
    register_ref: str = Field(alias="register")
    members: tuple[ResolvedGroupVariable, ...] = Field(min_length=2)

    _ref = field_validator("register_ref")(_register)

    @model_validator(mode="after")
    def _members(self) -> Self:
        _unique(
            ((m.variable, m.delivery_column_name) for m in self.members),
            "group member",
        )
        axes = {axis.axis for axis in self.axes}
        grains: dict[str, set[bool]] = defaultdict(set)
        for member in self.members:
            if member.variable.rsplit("/", 1)[0] != self.register_ref:
                raise ValueError("variable group member belongs to another register")
            _unique((facet.axis for facet in member.facets), "member facet axis")
            if {facet.axis for facet in member.facets} != axes:
                raise ValueError("member must supply one facet per declared axis")
            grains[member.variable].add(member.delivery_column_name is None)
        if any(len(value) > 1 for value in grains.values()):
            raise ValueError("group mixes whole-variable and representation members")
        return self


class ResolvedClassificationGroup(_ResolvedGroup):
    members: tuple[ResolvedGroupClassification, ...] = Field(min_length=2)

    @model_validator(mode="after")
    def _members(self) -> Self:
        _unique(
            (member.classification for member in self.members),
            "classification group member",
        )
        if len(self.axes) > 1:
            raise ValueError("classification groups support at most one axis")
        return self


class ResolvedTagMember(_ResolvedModel):
    target: str
    rank: int
    starred: bool
    note: str | None = None

    @field_validator("target")
    @classmethod
    def _target(cls, value: str) -> str:
        return _fqid(value, FqidKind.REGISTER, FqidKind.VARIABLE_BINDING)


class ResolvedTag(_ResolvedModel):
    slug: str
    label: str
    description: str | None = None
    members: tuple[ResolvedTagMember, ...] = ()

    _slug = field_validator("slug")(_slug)
    _label = field_validator("label")(_require_trimmed)

    @model_validator(mode="after")
    def _members(self) -> Self:
        _unique((member.target for member in self.members), "tag member")
        return self


class ResolvedVariableSameAs(_ResolvedModel):
    a: str
    b: str

    _refs = field_validator("a", "b")(_variable)


class ResolvedClassificationRef(_ResolvedModel):
    """The same_as table's explicit source namespace, never guessed from publisher."""

    provider: str
    classification: str

    _slugs = field_validator("provider", "classification")(_slug)


class ResolvedClassificationSameAs(_ResolvedModel):
    a: ResolvedClassificationRef
    b: ResolvedClassificationRef

    @model_validator(mode="after")
    def _distinct(self) -> Self:
        if self.a.classification == self.b.classification:
            raise ValueError(
                "classification same_as needs distinct global classifications"
            )
        return self


class ResolvedSuccession(_ResolvedModel):
    predecessor: str
    successor: str
    effective_year: int | None = None
    note: str | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _kind(self) -> Self:
        for value in (self.predecessor, self.successor):
            _fqid(value, FqidKind.REGISTER, FqidKind.VARIABLE_BINDING)
        if parse(self.predecessor).kind != parse(self.successor).kind:
            raise ValueError("succession endpoint kinds differ")
        return self


class ResolvedHistoricalPredecessor(_ResolvedModel):
    """One reviewed absent predecessor; this declaration creates no catalog entity."""

    target: str
    kind: Literal["register", "variable"]
    reason: str
    decision_reference: str

    _text = field_validator("reason", "decision_reference")(_require_trimmed)

    @model_validator(mode="after")
    def _kind(self) -> Self:
        expected = (
            FqidKind.REGISTER if self.kind == "register" else FqidKind.VARIABLE_BINDING
        )
        _fqid(self.target, expected)
        return self


class ResolvedVariantRef(_ResolvedModel):
    register_ref: str = Field(alias="register")
    variant: str

    _register = field_validator("register_ref")(_register)
    _variant = field_validator("variant")(_variant_slug)


class ResolvedVariantSuccession(_ResolvedModel):
    predecessor: ResolvedVariantRef
    successor: ResolvedVariantRef
    effective_year: int | None = None
    note: str | None = None
    description: str | None = None


class ResolvedRepresentationRef(_ResolvedModel):
    variable: str
    delivery_column_name: str

    _variable = field_validator("variable")(_variable)
    _column = field_validator("delivery_column_name")(_require_trimmed)


class ResolvedRepresentationSuccession(_ResolvedModel):
    predecessor: ResolvedRepresentationRef
    successor: ResolvedRepresentationRef
    variant: str | None = None
    effective_year: int | None = None
    note: str | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _scope(self) -> Self:
        if self.variant is not None:
            _slug(self.variant)
            if self.effective_year is None:
                raise ValueError(
                    "variant-scoped representation succession needs an effective year"
                )
            if (
                self.predecessor.variable.rsplit("/", 1)[0]
                != self.successor.variable.rsplit("/", 1)[0]
            ):
                raise ValueError(
                    "variant-scoped representation endpoints need one register"
                )
        return self


class ResolvedClassificationDerivation(_ResolvedModel):
    derived: str
    source: str
    note: str | None = None

    _slugs = field_validator("derived", "source")(_slug)


class ResolvedStateRef(_ResolvedWindow):
    variable: str
    variant: str
    delivery_column_name: str
    value_set_version_label: str = ""

    _variable = field_validator("variable")(_variable)
    _variant = field_validator("variant")(_variant_slug)
    _column = field_validator("delivery_column_name")(_require_trimmed)


class ResolvedStateLineage(_ResolvedWindow):
    consumer: ResolvedStateRef
    source: ResolvedStateRef

    @model_validator(mode="after")
    def _scope(self) -> Self:
        if self.valid_from < max(
            self.consumer.valid_from, self.source.valid_from
        ) or self.valid_to > min(self.consumer.valid_to, self.source.valid_to):
            raise ValueError("lineage scope exceeds endpoint state intersection")
        return self


class ResolvedLineageWarning(_ResolvedModel):
    consumer: ResolvedStateRef
    kind: Literal["no_source_state", "ambiguous_source_variant"]
    message: str

    _message = field_validator("message")(_require_trimmed)


class ResolvedSourceColumn(_ResolvedModel):
    table_name: str
    column_name: str
    sql_type: str
    nullable: bool

    _text = field_validator("table_name", "column_name", "sql_type")(_require_trimmed)


class ResolvedSourceJoinKey(_ResolvedModel):
    table_name: str
    column_name: str
    description: str | None = None

    _text = field_validator("table_name", "column_name")(_require_trimmed)


class ResolvedIdentifierMetadata(_ResolvedModel):
    native_variable_id: int
    name: str | None
    definition: str | None


class ResolvedTimeseriesEvent(_ResolvedModel):
    name: str | None
    event: str | None
    description: str | None
    entity: str | None
    first_token: str | None
    second_token: str | None
    file_token: str | None


class ResolvedMetadata(_ResolvedModel):
    variable_groups: tuple[ResolvedVariableGroup, ...] = ()
    classification_groups: tuple[ResolvedClassificationGroup, ...] = ()
    tags: tuple[ResolvedTag, ...] = ()
    variable_same_as: tuple[ResolvedVariableSameAs, ...] = ()
    classification_same_as: tuple[ResolvedClassificationSameAs, ...] = ()
    successions: tuple[ResolvedSuccession, ...] = ()
    historical_predecessors: tuple[ResolvedHistoricalPredecessor, ...] = ()
    variant_successions: tuple[ResolvedVariantSuccession, ...] = ()
    representation_successions: tuple[ResolvedRepresentationSuccession, ...] = ()
    classification_derivations: tuple[ResolvedClassificationDerivation, ...] = ()
    state_lineage: tuple[ResolvedStateLineage, ...] = ()
    lineage_warnings: tuple[ResolvedLineageWarning, ...] = ()
    source_columns: tuple[ResolvedSourceColumn, ...] = ()
    source_join_keys: tuple[ResolvedSourceJoinKey, ...] = ()
    identifiers: tuple[ResolvedIdentifierMetadata, ...] = ()
    timeseries_events: tuple[ResolvedTimeseriesEvent, ...] = ()


def _unique(values: Any, label: str) -> None:
    seen = set()
    for value in values:
        if value in seen:
            raise ValueError(f"duplicate resolved {label}: {value!r}")
        seen.add(value)


def validate_metadata_structure(metadata: ResolvedMetadata) -> None:
    """Reject invalid declarations before diagnostic dependency withholding.

    A missing endpoint must not hide duplicate membership or a graph cycle. These
    checks need only declarations; exact live-reference checks remain in preparation.
    """
    _unique(
        ((g.register_ref, g.key) for g in metadata.variable_groups),
        "variable group key",
    )
    _unique((g.key for g in metadata.classification_groups), "classification group key")
    owners = {}
    for group in metadata.variable_groups:
        for member in group.members:
            owner = group.register_ref, group.key
            if member.variable in owners and owners[member.variable] != owner:
                raise ValueError("variable belongs to multiple resolved groups")
            owners[member.variable] = owner
    _unique(
        (m.classification for g in metadata.classification_groups for m in g.members),
        "classification group membership",
    )
    _unique((tag.slug for tag in metadata.tags), "tag slug")
    for label, graph in (
        ("variable same_as", [(e.a, e.b) for e in metadata.variable_same_as]),
        (
            "classification same_as",
            [
                ((e.a.provider, e.a.classification), (e.b.provider, e.b.classification))
                for e in metadata.classification_same_as
            ],
        ),
    ):
        _unique((tuple(sorted(pair)) for pair in graph), label)
        _reject_same_as_cycles(graph, label=f"resolved {label}")
        _reject_oversized_components(graph, label=f"resolved {label}")
    graphs = defaultdict(list)
    for edge in metadata.successions:
        graphs[parse(edge.predecessor).kind].append((edge.predecessor, edge.successor))
    graphs["variant succession"] = [
        (e.predecessor, e.successor) for e in metadata.variant_successions
    ]
    graphs["classification derivation"] = [
        (e.derived, e.source) for e in metadata.classification_derivations
    ]
    graphs["state lineage"] = [
        (state_reference_key(e.consumer), state_reference_key(e.source))
        for e in metadata.state_lineage
    ]
    unscoped, scoped = [], []
    for edge in metadata.representation_successions:
        a, b = (
            (e.variable, e.delivery_column_name.lower(), edge.variant or "")
            for e in (edge.predecessor, edge.successor)
        )
        if edge.variant is None:
            unscoped.append((a, b))
        else:
            scoped.append((a, b, edge.effective_year))
    _unique((*unscoped, *((a, b) for a, b, _ in scoped)), "representation succession")
    graphs["unscoped representation succession"] = unscoped
    reject_nonmonotone_representation_cycles(scoped)
    for label, graph in graphs.items():
        _unique(graph, str(label))
        reject_replaced_by_cycles(graph)
    linked = {state_reference_key(e.consumer) for e in metadata.state_lineage}
    for warning in metadata.lineage_warnings:
        if (
            warning.kind == "no_source_state"
            and state_reference_key(warning.consumer) in linked
        ):
            raise ValueError("no_source_state warning contradicts explicit lineage")
    _unique(
        ((state_reference_key(w.consumer), w.kind) for w in metadata.lineage_warnings),
        "lineage warning",
    )
    _unique(
        (item.target for item in metadata.historical_predecessors),
        "historical predecessor declaration",
    )
    unused = {h.target for h in metadata.historical_predecessors} - {
        e.predecessor for e in metadata.successions
    }
    if unused:
        raise ValueError(
            f"unused historical predecessor declarations: {sorted(unused)}"
        )
    _unique(
        ((c.table_name, c.column_name) for c in metadata.source_columns),
        "source column",
    )
    _unique(
        ((c.table_name, c.column_name) for c in metadata.source_join_keys),
        "source join key",
    )
    _unique(
        (i.native_variable_id for i in metadata.identifiers),
        "native identifier metadata",
    )


def state_reference_key(ref: ResolvedStateRef) -> tuple[str, str, str, str]:
    return ref.variable, ref.variant, ref.valid_from, ref.value_set_version_label


_COLUMNS = {
    "concept_group": "group_id,kind,register_id,group_key,label,source",
    "concept_group_axis": "group_id,axis,ordinal,label",
    "concept_group_variable": "member_id,group_id,variable_id,delivery_column_name",
    "concept_group_variable_facet": "member_id,axis,value,label",
    "concept_group_classification": "classification_id,group_id,facet_value,facet_label",
    "tag": "tag_id,slug,label,description",
    "tag_member": "tag_id,register_id,variable_id,rank,starred,note",
    "variable_same_as": "a_provider,a_register,a_variable,b_provider,b_register,b_variable",
    "classification_same_as": "a_provider,a_classification_slug,b_provider,b_classification_slug",
    "register_replaced_by": "predecessor_provider,predecessor_register,successor_provider,successor_register,effective_year,note,beskrivning",
    "variable_replaced_by": "predecessor_provider,predecessor_register,predecessor_variable,successor_provider,successor_register,successor_variable,effective_year,note,beskrivning",
    "variant_replaced_by": "predecessor_provider,predecessor_register,predecessor_variant,successor_provider,successor_register,successor_variant,effective_year,note,beskrivning",
    "representation_replaced_by": "predecessor_provider,predecessor_register,predecessor_variable,predecessor_column,successor_provider,successor_register,successor_variable,successor_column,variant,effective_year,note,beskrivning",
    "classification_derived_from": "derived_slug,source_slug,note",
    "variable_state_lineage": "consumer_state_id,source_state_id,valid_from,valid_to",
    "variable_state_lineage_warning": "consumer_state_id,warning_kind,message",
    "source_column_type": "table_name,column_name,sql_type,nullable",
    "source_join_key": "table_name,column_name,description",
    "identifier_semantics": "var_id,variabelnamn,variabeldefinition",
    "timeseries_event": "namn,handelse,beskrivning,entitet,id1,id2,fil_id",
}


def prepare_resolved_metadata(
    metadata: ResolvedMetadata,
    variables: tuple[ResolvedVariable, ...],
    registers: dict[tuple[str, str], ResolvedRegister],
    variants: dict[tuple[str, str, str], ResolvedVariant],
    classifications: tuple[ResolvedClassification, ...],
) -> dict[str, list[tuple[Any, ...]]]:
    """Validate exact dependent references before any staged or live DB is touched."""
    metadata = ResolvedMetadata.model_validate(metadata)
    validate_metadata_structure(metadata)
    rows: dict[str, list[tuple[Any, ...]]] = {name: [] for name in _COLUMNS}
    if not any(getattr(metadata, name) for name in type(metadata).model_fields):
        return rows
    register_ids = {
        "/".join(key): _storage_id(key[0], "register", key[1]) for key in registers
    }
    variable_ids = {
        f"{v.register_ref.provider}/{v.register_ref.slug}/{v.slug}": _storage_id(
            v.register_ref.provider, "variable", v.register_ref.slug, v.slug
        )
        for v in variables
    }
    variant_keys = {("/".join(key[:2]), key[2]) for key in variants}
    classification_ids = {
        item.slug: _classification_id(item.slug) for item in classifications
    }
    historical = {item.target: item for item in metadata.historical_predecessors}
    for target, declaration in historical.items():
        live = register_ids if declaration.kind == "register" else variable_ids
        if target in live:
            raise ValueError(
                f"obsolete historical predecessor declaration now names a live entity: {target}"
            )
    representations = set()
    states = {}
    for variable in variables:
        fqid = f"{variable.register_ref.provider}/{variable.register_ref.slug}/{variable.slug}"
        for state in variable.states:
            key = (
                fqid,
                state.variant.slug,
                state.valid_from,
                state.value_set_version_label,
            )
            states[key] = state
            representations.add((fqid, state.variant.slug, state.delivery_column_name))
        for alias in variable.aliases:
            representations.add((fqid, alias.variant.slug, alias.delivery_column_name))
    literal_representation_columns = {
        (variable, column) for variable, _, column in representations
    }
    # Groups enumerate literal delivered spellings; succession has a separate
    # case-insensitive endpoint contract. Keep both indexes without merging facts.
    representations = {
        (variable, variant, column.lower())
        for variable, variant, column in representations
    }
    representation_columns = {
        (variable, column) for variable, _, column in representations
    }

    def require(mapping: Any, key: Any, label: str) -> Any:
        if key not in mapping:
            raise ValueError(f"unknown resolved {label}: {key!r}")
        return mapping[key] if isinstance(mapping, dict) else key

    def representation(variable: str, column: str, variant: str | None = None) -> None:
        require(variable_ids, variable, "variable")
        if (
            (variable, column.lower()) not in representation_columns
            if variant is None
            else (variable, variant, column.lower()) not in representations
        ):
            raise ValueError(
                f"unknown resolved representation: {variable}, {column}, {variant}"
            )

    def state_id(ref: ResolvedStateRef) -> int:
        key = (ref.variable, ref.variant, ref.valid_from, ref.value_set_version_label)
        state = require(states, key, "state")
        if (state.valid_to, state.delivery_column_name) != (
            ref.valid_to,
            ref.delivery_column_name,
        ):
            raise ValueError(
                "resolved state reference does not match its exact scope/column"
            )
        provider, register, variable = ref.variable.split("/")
        return _storage_id(
            provider,
            "state",
            register,
            variable,
            ref.variant,
            ref.valid_from,
            ref.value_set_version_label,
        )

    for group in (*metadata.variable_groups, *metadata.classification_groups):
        is_variable = isinstance(group, ResolvedVariableGroup)
        scope = group.register_ref if is_variable else ""
        register_id = require(register_ids, scope, "register") if is_variable else None
        kind = "variable" if is_variable else "classification"
        group_id = mint("resolved-catalog", "group", kind, scope, group.key)
        rows["concept_group"].append(
            (group_id, kind, register_id, group.key, group.label, group.source)
        )
        rows["concept_group_axis"].extend(
            (group_id, axis.axis, axis.ordinal, axis.label) for axis in group.axes
        )
        if isinstance(group, ResolvedVariableGroup):
            for member in group.members:
                variable_id = require(variable_ids, member.variable, "group variable")
                if member.delivery_column_name is not None:
                    require(
                        literal_representation_columns,
                        (member.variable, member.delivery_column_name),
                        "group representation",
                    )
                member_id = mint(
                    "resolved-catalog",
                    "group-member",
                    str(group_id),
                    member.variable,
                    member.delivery_column_name or "",
                )
                rows["concept_group_variable"].append(
                    (member_id, group_id, variable_id, member.delivery_column_name)
                )
                rows["concept_group_variable_facet"].extend(
                    (member_id, f.axis, f.value, f.label) for f in member.facets
                )
        else:
            for member in group.members:
                classification_id = require(
                    classification_ids, member.classification, "group classification"
                )
                rows["concept_group_classification"].append(
                    (
                        classification_id,
                        group_id,
                        member.facet_value,
                        member.facet_label,
                    )
                )
    for tag in metadata.tags:
        tag_id = mint("resolved-catalog", "tag", tag.slug)
        rows["tag"].append((tag_id, tag.slug, tag.label, tag.description))
        for member in tag.members:
            register_id = variable_id = None
            if parse(member.target).kind == FqidKind.REGISTER:
                register_id = require(register_ids, member.target, "tag register")
            else:
                variable_id = require(variable_ids, member.target, "tag variable")
            rows["tag_member"].append(
                (
                    tag_id,
                    register_id,
                    variable_id,
                    member.rank,
                    member.starred,
                    member.note,
                )
            )
    for edge in metadata.variable_same_as:
        require(variable_ids, edge.a, "same_as variable")
        require(variable_ids, edge.b, "same_as variable")
        a, b = tuple(edge.a.split("/")), tuple(edge.b.split("/"))
        rows["variable_same_as"].extend(((*a, *b), (*b, *a)))
    for edge in metadata.classification_same_as:
        for endpoint in (edge.a, edge.b):
            require(
                classification_ids, endpoint.classification, "same_as classification"
            )
        a, b = (
            (edge.a.provider, edge.a.classification),
            (edge.b.provider, edge.b.classification),
        )
        rows["classification_same_as"].extend(((*a, *b), (*b, *a)))
    for edge in metadata.successions:
        kind = parse(edge.predecessor).kind
        targets = register_ids if kind == FqidKind.REGISTER else variable_ids
        table = (
            "register_replaced_by"
            if kind == FqidKind.REGISTER
            else "variable_replaced_by"
        )
        if edge.predecessor not in targets:
            require(historical, edge.predecessor, "succession predecessor")
        require(targets, edge.successor, "succession successor")
        rows[table].append(
            (
                *edge.predecessor.split("/"),
                *edge.successor.split("/"),
                edge.effective_year,
                edge.note,
                edge.description,
            )
        )
    for edge in metadata.variant_successions:
        a, b = edge.predecessor, edge.successor
        for endpoint in (a, b):
            require(
                variant_keys,
                (endpoint.register_ref, endpoint.variant),
                "succession variant",
            )
        ka, kb = (
            (*a.register_ref.split("/"), a.variant),
            (*b.register_ref.split("/"), b.variant),
        )
        rows["variant_replaced_by"].append(
            (*ka, *kb, edge.effective_year, edge.note, edge.description)
        )
    for edge in metadata.representation_successions:
        a, b = edge.predecessor, edge.successor
        for endpoint in (a, b):
            representation(
                endpoint.variable, endpoint.delivery_column_name, edge.variant
            )
        rows["representation_replaced_by"].append(
            (
                *a.variable.split("/"),
                a.delivery_column_name,
                *b.variable.split("/"),
                b.delivery_column_name,
                edge.variant or "",
                edge.effective_year,
                edge.note,
                edge.description,
            )
        )
    for edge in metadata.classification_derivations:
        require(classification_ids, edge.derived, "derived classification")
        require(classification_ids, edge.source, "source classification")
        rows["classification_derived_from"].append(
            (edge.derived, edge.source, edge.note)
        )
    for edge in metadata.state_lineage:
        consumer, source = state_id(edge.consumer), state_id(edge.source)
        rows["variable_state_lineage"].append(
            (consumer, source, edge.valid_from, edge.valid_to)
        )
    for warning in metadata.lineage_warnings:
        consumer = state_id(warning.consumer)
        rows["variable_state_lineage_warning"].append(
            (consumer, warning.kind, warning.message)
        )
    source_columns = {
        (column.table_name, column.column_name) for column in metadata.source_columns
    }
    for column in metadata.source_columns:
        rows["source_column_type"].append(
            (column.table_name, column.column_name, column.sql_type, column.nullable)
        )
    for key in metadata.source_join_keys:
        require(
            source_columns, (key.table_name, key.column_name), "join-key source column"
        )
        rows["source_join_key"].append(
            (key.table_name, key.column_name, key.description)
        )
    rows["identifier_semantics"].extend(
        (item.native_variable_id, item.name, item.definition)
        for item in metadata.identifiers
    )
    rows["timeseries_event"].extend(
        (
            item.name,
            item.event,
            item.description,
            item.entity,
            item.first_token,
            item.second_token,
            item.file_token,
        )
        for item in metadata.timeseries_events
    )
    return rows


def write_resolved_metadata(
    conn: sqlite3.Connection, rows: dict[str, list[tuple[Any, ...]]]
) -> None:
    for table, columns in _COLUMNS.items():
        placeholders = ",".join("?" for _ in columns.split(","))
        conn.executemany(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
            sorted(rows[table], key=lambda row: json.dumps(row, ensure_ascii=False)),
        )
