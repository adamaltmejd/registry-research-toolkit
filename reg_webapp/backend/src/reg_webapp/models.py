"""Webapp-local Pydantic response models.

These are reg_webapp's OWN response models — NOT reg_schema models and NOT
reg_meta dataclasses (§9.6). reg_meta's library surface is plain dataclasses;
the backend wraps its domain types in per-endpoint Pydantic models (the only
place a 1:1 wrapper remains). reg_schema models are used directly only for
project_data-shaped responses (A5.1b+).
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field
from reg_meta.fqid import CLASSIFICATION_PREFIX


class StewardInfo(BaseModel):
    """Deployment identity + branding, from ``steward.toml`` (§9.1)."""

    id: str
    name: str
    long_name: str


class RegMetaInfo(BaseModel):
    """reg_meta build provenance, read from the DB ``import_manifest``."""

    schema_version: str = Field(
        description="Schema version of the reg_meta DB build (e.g. '5.1.0')."
    )
    import_date: str = Field(
        description="UTC timestamp the reg_meta DB was built/imported."
    )


class WebappInfo(BaseModel):
    """Package versions for the deployed backend + its reg_meta dependency."""

    version: str = Field(description="Installed reg_webapp package version.")
    reg_meta_version: str = Field(
        description=(
            "Installed reg_meta package version — distinct from "
            "reg_meta.schema_version, which is the DB build."
        )
    )


class ContextResponse(BaseModel):
    """``GET /api/context`` — deployment identity, branding, build info (§9.5).

    No git sha (decision: no new provenance dep). The reg_meta block reflects
    the DB the backend booted against; the webapp block reflects the installed
    packages.
    """

    steward: StewardInfo
    reg_meta: RegMetaInfo
    webapp: WebappInfo


# ── Catalog browse (§9.5) ──────────────────────────────────────────────────
# Webapp-local 1:1 Pydantic wrappers of reg_meta's frozen Catalog dataclasses
# (§9.6). reg_meta dataclasses are NOT imported as response models. FQID fields
# serialize as plain `str` (the catalog mapper passes `str(fqid)`), NOT nested
# models — so `openapi-typescript` emits flat string fields. Each node model
# carries a `kind` Literal discriminator so the catch-all's response is a
# Pydantic discriminated union (clean tagged union in the codegen'd TS).


class ProviderNode(BaseModel):
    """A provider node (1-seg FQID, e.g. `scb`). A child of the root and a
    resolvable node (its `children` are the provider's registers)."""

    kind: Literal["provider"] = "provider"
    fqid: str
    name: str | None = None


class ClassificationRootNode(BaseModel):
    """The classification-root sentinel (`class`, 1 seg) — a child of the root
    and a resolvable node whose `children` are every classification (§5.2: `class`
    is a reserved slug, not a real provider)."""

    kind: Literal["classification-root"] = "classification-root"
    fqid: str = CLASSIFICATION_PREFIX
    name: str = "Classifications"


class RegisterNode(BaseModel):
    """A register node (2-seg FQID, e.g. `scb/lisa`). Its `children` are the
    register's bindings; `variants` is a forward-declared reference stub for
    A5.2's variant browser (a link, not data)."""

    kind: Literal["register"] = "register"
    fqid: str
    name: str | None = None
    purpose: str | None = None


class ClassificationNode(BaseModel):
    """A classification leaf (`class/<slug>`, 2 seg)."""

    kind: Literal["classification"] = "classification"
    fqid: str
    short_name: str
    name: str
    # §5.5: present (non-None) when the queried slug resolved via a curated
    # `classification_same_as` edge rather than directly; the hop path as FQIDs.
    via_same_as: list[str] | None = None


class BindingChild(BaseModel):
    """A binding child under a register node — a thin (fqid, name) entry, NOT
    the embedded longitudinal record (that is only on the binding LEAF response)."""

    kind: Literal["binding"] = "binding"
    fqid: str
    name: str | None = None


class VariantsRef(BaseModel):
    """Forward-declared reference to a register's variant browser (A5.2). A
    declared slot so the discriminated union / TS types are stable before the
    `/{provider}/{register}/variants` sub-resource exists — `available` is False
    until A5.2 wires it."""

    kind: Literal["variants-ref"] = "variants-ref"
    register_fqid: str
    available: bool = False


# ── Binding-leaf embedded longitudinal record (§9.5) ───────────────────────
# The binding LEAF (3-seg) embeds the variable's FULL record from one
# `Catalog.resolve` call: every state + variable-grain edges (§9.5). These
# mirror the reg_meta dataclasses 1:1. `ResolvedVariable` does NOT carry
# lineage_warnings, so they are OMITTED here (they arrive via A5.2's
# `/lineage_warnings`).


class ValueSetMember(BaseModel):
    """One (code, label) pair of a state's value set."""

    code: str
    label: str


class VariableStateModel(BaseModel):
    """One `variable_state` row — a per-delivery shape tagged with its variant
    coordinate (§5.1). `value_set` is the hydrated (code, label) pairs, None when
    the state carries no value set."""

    state_id: int
    variant: str
    register_variant_id: int
    valid_from: str
    valid_to: str
    data_type: str | None
    data_length: str | None
    delivery_column_name: str | None
    value_set_version_label: str
    value_set_id: int | None
    value_set: list[ValueSetMember] | None


class VariableRefModel(BaseModel):
    """A variable-grain edge endpoint (`same_as` / succession). `fqid` is the
    neighbor's 3-seg binding FQID (None when its slug isn't populated); the
    `provider`/`register`/`variable` triple is the load-bearing identity when
    `fqid` is None. `reason` / `effective_year` are succession-only (#142), None
    on `same_as`.

    The Python attribute is `register_name` because a bare `register` field
    shadows `BaseModel.register` (a Pydantic v2 method) and warns; the wire/JSON
    key stays `register` via the alias (the alias is also the canonical init
    param — the mapper constructs with `register=`). FastAPI serializes by alias
    by default, so the response key is `register`.
    """

    fqid: str | None
    provider: str
    register_name: str = Field(alias="register")
    variable: str
    reason: str | None = None
    effective_year: int | None = None


class RelatedRefModel(BaseModel):
    """A split-sibling edge (`variable_related_to`, §5.7) with its
    `relation_kind`. `register` is the alias for `register_name` (see
    `VariableRefModel`) — avoids the `BaseModel.register` method shadow."""

    fqid: str | None
    provider: str
    register_name: str = Field(alias="register")
    variable: str
    relation_kind: str


class LineageEdgeModel(BaseModel):
    """A consumer-side lineage edge (state grain, §5.6) tying a consumer state to
    a source state over their validity intersection. `source_fqid` is the source
    state's 3-seg binding FQID (None when the source slugs aren't populated)."""

    consumer_state_id: int
    source_state_id: int
    valid_from: str
    valid_to: str
    source_fqid: str | None = None


class BindingNode(BaseModel):
    """A binding LEAF (3-seg FQID) — the addressable variable plus its FULL
    longitudinal record embedded from one `Catalog.resolve` call (§9.5): shared
    metadata, every state (each tagged with its variant), and the variable-grain
    `same_as` / `replaced_by` / `related_to` / `lineage` edges.

    `lineage_warnings` are intentionally OMITTED — `ResolvedVariable` doesn't
    carry them; they arrive via A5.2's `/lineage_warnings` endpoint. This full-node
    shape is the binding leaf with NO narrowing query: a `?period` resolves via
    `resolve_at` (→ `StatesResponse`) instead, and a narrowing modifier (`?variant`
    / `?value_set_version` / an `@version` pin) WITHOUT `?period` is a 422 — it is
    inert without a period, so it errors rather than silently embedding full
    history."""

    kind: Literal["binding"] = "binding"
    fqid: str
    variable_id: int
    register_id: int
    name: str | None
    definition: str | None
    description: str | None
    measurement_unit: str | None
    is_sensitive: bool
    is_identifier: bool
    source_register_id: int | None
    source_register_text: str | None
    states: list[VariableStateModel]
    same_as: list[VariableRefModel]
    replaced_by: list[VariableRefModel]
    related_to: list[RelatedRefModel]
    lineage: list[LineageEdgeModel]
    via_same_as: list[str] | None = None


# Children of a register node: its bindings plus the variant-browser reference
# stub. A discriminated union so the TS type is a clean tagged union (a binding
# child vs the single variants-ref).
RegisterChild = Annotated[BindingChild | VariantsRef, Field(discriminator="kind")]


class ProviderResponse(ProviderNode):
    """`GET /api/catalog/{provider}` — the provider + its registers as children."""

    children: list[RegisterNode]


class RegisterResponse(RegisterNode):
    """`GET /api/catalog/{provider}/{register}` — the register + its bindings and
    the variant-browser reference stub as children (§9.5)."""

    children: list[RegisterChild]


class ClassificationRootResponse(ClassificationRootNode):
    """`GET /api/catalog/class` — the classification-root + every classification
    as children."""

    children: list[ClassificationNode]


class RootResponse(BaseModel):
    """`GET /api/catalog` — the catalog root: every provider plus the
    classification-root sentinel (§9.5)."""

    kind: Literal["root"] = "root"
    children: list[ProviderNode | ClassificationRootNode]


# The catch-all `/api/catalog/{fqid:path}` returns one of these, discriminated
# by `kind` so the codegen'd TS is a tagged union (A5.3). A binding leaf is a
# `BindingNode` (full record embedded); a classification leaf a
# `ClassificationNode`.
CatalogNode = Annotated[
    ProviderResponse
    | RegisterResponse
    | BindingNode
    | ClassificationRootResponse
    | ClassificationNode,
    Field(discriminator="kind"),
]


# ── A5.2a-ii sub-endpoint models (§9.5) ─────────────────────────────────────
# The 7 suffixed/sub-resource read endpoints. Each returns a thin envelope
# echoing the queried `binding` (or `register`) FQID plus the mapped reg_meta
# dataclass list, so the SPA codegen sees one response type per endpoint. These
# REUSE the leaf edge models above (`VariableStateModel`, `VariableRefModel`,
# `RelatedRefModel`, `LineageEdgeModel`) — the sub-endpoints are the standalone
# accessors for the same edges the leaf embeds.


class LineageWarningModel(BaseModel):
    """A build-time lineage warning for a consumer state (§5.6):
    `variable_state_lineage_warning`. `warning_kind` is `no_source_state` or
    `ambiguous_source_variant`. Maps 1:1 to `reg_meta.catalog.LineageWarning`."""

    consumer_state_id: int
    warning_kind: str
    message: str


class VariantModel(BaseModel):
    """One register variant (the `register_variant` sub-resource, §9.5) — the
    `?variant=` browse axis. A variant is NOT FQID-addressable (the variant left
    the binding FQID, §5.0.1), so it carries the variant `slug` (the browse
    coordinate) + display fields, not an `Fqid`. Maps 1:1 to
    `reg_meta.catalog.VariantSummary`. The §9.5 `panel_*` fields are NOT here —
    those columns don't exist on `register_variant` yet (A4.4 curation)."""

    slug: str
    name: str | None = None
    description: str | None = None
    display_group: str | None = None


class StatesResponse(BaseModel):
    """`GET /api/catalog/{fqid}/states` — the binding's full state history (§9.5).
    Same `states` shape the binding leaf embeds, as a standalone envelope. With a
    `?period` query on the catch-all this same shape carries the resolve_at
    subset (uniform: codegen sees one state-list type)."""

    binding: str
    states: list[VariableStateModel]


class PredecessorsResponse(BaseModel):
    """`GET /api/catalog/{fqid}/predecessors` — inbound succession (§9.5)."""

    binding: str
    predecessors: list[VariableRefModel]


class SuccessorsResponse(BaseModel):
    """`GET /api/catalog/{fqid}/successors` — outbound succession (§9.5)."""

    binding: str
    successors: list[VariableRefModel]


class RelatedResponse(BaseModel):
    """`GET /api/catalog/{fqid}/related` — split-sibling edges (§5.7 / §9.5)."""

    binding: str
    related: list[RelatedRefModel]


class LineageResponse(BaseModel):
    """`GET /api/catalog/{fqid}/lineage` — consumer-side lineage edges (§5.6).

    Maps what `reg_meta.LineageEdge` carries (consumer/source state ids, the
    validity intersection, source_fqid). The §9.5 richer per-source-state shape
    (embedding each source state's variant / value_set / column) is a possible
    reg_meta enhancement, NOT blocked on here — see DESIGN.md."""

    binding: str
    lineage_edges: list[LineageEdgeModel]


class LineageWarningsResponse(BaseModel):
    """`GET /api/catalog/{fqid}/lineage_warnings` — build-time lineage warnings
    (§5.6 / §9.5). Empty list when lineage resolved cleanly."""

    binding: str
    lineage_warnings: list[LineageWarningModel]


class VariantsResponse(BaseModel):
    """`GET /api/catalog/{provider}/{register}/variants` — the variant browser
    (§9.5). The wire key `register` is the 2-seg register FQID; `variants` the
    register's `register_variant` sub-resource list.

    The Python attr is `register_name` (aliased to `register`) for the same reason
    as `VariableRefModel`: a bare `register` field shadows `BaseModel.register` (a
    Pydantic v2 method) and warns. FastAPI serializes by alias, so the wire key
    stays `register`; the alias is also the canonical init param."""

    register_name: str = Field(alias="register")
    variants: list[VariantModel]
