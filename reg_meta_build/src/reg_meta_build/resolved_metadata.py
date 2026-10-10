"""Explicit resolved discovery, relationship and reference metadata.

The preparation (materialize.py) resolves only exact declared references into
storage IDs. It never derives groups, relations, lineage, source precedence or
missing facts.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Literal, Self

from pydantic import Field, field_validator, model_validator
from reg_core_py import parse_fqid

from reg_meta_build._resolved_common import (
    _require_trimmed,
    _ResolvedDeliveryScope,
    _ResolvedModel,
    _ResolvedWindow,
)
from reg_meta_build.concept_groups import _is_path_safe_key
from reg_meta_build.relations import (
    _reject_oversized_components,
    _reject_same_as_cycles,
    reject_cycles,
    reject_nonmonotone_representation_cycles,
)
from reg_meta_build.slug_grammar import validate_slug

from .documentary import (  # noqa: TC001
    DocumentaryRelationship,
    LiteralSourceRelationship,
)


class RetainedDocumentaryRelationship(_ResolvedModel):
    """An exact source declaration without an established catalog endpoint."""

    relationship_id: int = Field(gt=0)
    register_ref: str
    declaration: LiteralSourceRelationship
    binding_status: Literal["retained_unattached"] = "retained_unattached"
    reason: str
    provenance: str

    _text = field_validator("reason", "provenance")(_require_trimmed)

    @field_validator("register_ref")
    @classmethod
    def _register_ref(cls, value: str) -> str:
        if parse_fqid(value).kind != "register":
            raise ValueError("retained documentary scope must be a register")
        return value


def _fqid(value: str, *kinds: str) -> str:
    if parse_fqid(value).kind not in kinds:
        raise ValueError(f"wrong resolved reference kind: {value}")
    return value


def _variable(value: str) -> str:
    return _fqid(value, "variable")


def _register(value: str) -> str:
    return _fqid(value, "register")


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
        return _fqid(value, "register", "variable")


class ResolvedTag(_ResolvedModel):
    slug: str
    label: str
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


class ResolvedSuccession(_ResolvedModel):
    predecessor: str
    successor: str
    effective_year: int | None = None
    note: str | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _kind(self) -> Self:
        for value in (self.predecessor, self.successor):
            _fqid(value, "register", "variable")
        if parse_fqid(self.predecessor).kind != parse_fqid(self.successor).kind:
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
        expected = "register" if self.kind == "register" else "variable"
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
            _variant_slug(self.variant)
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


class ResolvedStateRef(_ResolvedDeliveryScope):
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
        if (
            self.consumer.valid_from is None
            or self.consumer.valid_to is None
            or self.source.valid_from is None
            or self.source.valid_to is None
        ):
            raise ValueError("dated lineage requires dated endpoint states")
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
    documentary_relationships: tuple[
        DocumentaryRelationship | RetainedDocumentaryRelationship, ...
    ] = ()


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
    _unique(
        (r.relationship_id for r in metadata.documentary_relationships),
        "documentary relationship identity",
    )
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
    same_as = [(e.a, e.b) for e in metadata.variable_same_as]
    _unique((tuple(sorted(pair)) for pair in same_as), "variable same_as")
    _reject_same_as_cycles(same_as, label="resolved variable same_as")
    _reject_oversized_components(same_as, label="resolved variable same_as")
    graphs = defaultdict(list)
    for edge in metadata.successions:
        graphs[parse_fqid(edge.predecessor).kind].append(
            (edge.predecessor, edge.successor)
        )
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
    relations: dict[str, Literal["derived_from", "lineage"]] = {
        "classification derivation": "derived_from",
        "state lineage": "lineage",
    }
    for label, graph in graphs.items():
        _unique(graph, str(label))
        reject_cycles(graph, relation=relations.get(label, "replaced_by"))
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
    coordinate = ref.valid_from if ref.valid_from is not None else "year_independent"
    return ref.variable, ref.variant, coordinate, ref.value_set_version_label
