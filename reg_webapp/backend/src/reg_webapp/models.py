"""Webapp-local Pydantic response models.

These are reg_webapp's OWN response models (see DESIGN.md → Pydantic boundary):
the node/envelope shapes that carry the `kind` discriminator and server-computed
enrichment (coverage, edition chains). reg_meta's catalog surface is now frozen
Pydantic too (#681), so the per-endpoint 1:1 LEAF wrappers are gone — these models
EMBED reg_meta's `VariableState` / `VariableRef` / `ConceptGroupSummary` / … directly
instead of re-modeling them. reg_schema models are used directly for
project_data-shaped responses (A5.1b+).
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field
from reg_meta.catalog import (
    BindingGroupRef,
    CatalogStorageId,
    ClassificationCode,
    ClassificationEdition,
    ClassificationExtensionMember,
    ConceptGroupMember,
    ConceptGroupSummary,
    DataWarning,
    GroupAxis,
    LineageEdge,
    LineageWarning,
    RegisterCoverage,
    TagMembership,
    ValueSetMember,
    VariableCoverage,
    VariableDelivery,
    VariableEdition,
    VariableRef,
    VariableState,
    VariantSummary,
)
from reg_meta.fqid import CLASSIFICATION_PREFIX
from reg_meta.order import OrderFinding  # noqa: TC002 — a runtime model field

from reg_meta import ClassificationDerivedFromRef  # noqa: TC001

# ── Catalog browse (see DESIGN.md → Catalog router structure) ───────────────
# The NODE / envelope models below carry the `kind` discriminator and any
# server-computed enrichment; their embedded leaf fields are reg_meta's frozen
# Pydantic models (`VariableState` / `VariableRef` / `ConceptGroupSummary` / …),
# imported directly as response models (#681 — the prior 1:1 leaf wrappers are
# gone). reg_meta's `Fqid` fields serialize as plain `str` via reg_meta's own
# Pydantic core schema, so `openapi-typescript` still emits flat string fields.
# Each node model carries a `kind` Literal discriminator so the catch-all's
# response is a Pydantic discriminated union (clean tagged union in the codegen'd
# TS).


class ProviderNode(BaseModel):
    """A provider node (1-seg FQID, e.g. `scb`). A child of the root and a
    resolvable node (its `children` are the provider's registers)."""

    kind: Literal["provider"] = "provider"
    fqid: str
    name: str | None = None


class ClassificationRootNode(BaseModel):
    """The classification-root sentinel (`class`, 1 seg) — a child of the root
    and a resolvable node whose `children` are every classification (see
    reg_meta/DESIGN.md → FQID grammar: `class` is a reserved slug, not a real
    provider)."""

    kind: Literal["classification-root"] = "classification-root"
    fqid: str = CLASSIFICATION_PREFIX
    name: str = "Classifications"


# ── Coverage aggregates (#351; see DESIGN.md → Coverage aggregates) ─────────
# ADDITIVE, query-time browse aggregates over `variable_state`, embedded directly
# as reg_meta's `VariableCoverage` / `RegisterCoverage` (#681). `coverage` is None
# on a node that wasn't enriched (e.g. a register's OWN node — coverage is
# populated only in the PROVIDER-children and REGISTER-children listings). The SPA
# does not read these yet and must tolerate their absence (payload-skew rule #317).


class RegisterNode(BaseModel):
    """A register node (2-seg FQID, e.g. `scb/lisa`). Its `children` are the
    register's bindings; `variants` is a forward-declared reference stub for
    A5.2's variant browser (a link, not data). `coverage` (#351) is populated
    when the node is a PROVIDER child (the register listing); None on the
    register's own node."""

    kind: Literal["register"] = "register"
    fqid: str
    name: str | None = None
    purpose: str | None = None
    warnings: tuple[DataWarning, ...] = ()
    coverage: RegisterCoverage | None = None
    tags: list[TagMembership] = Field(default_factory=list)


class ClassificationFamilyNode(BaseModel):
    """A one-dimensional classification succession family as a browsable subject.

    Served from the same stable `/catalog/group/class/{key}` route as curated
    classification umbrellas, but kept as a distinct `kind`: it is browse identity
    over `classification_replaced_by`, not concept-group membership.
    """

    kind: Literal["classification-family"] = "classification-family"
    key: str
    label: str
    editions: list[ClassificationEdition]


class ClassificationNode(BaseModel):
    """A classification leaf (`class/<slug>`, 2 seg)."""

    kind: Literal["classification"] = "classification"
    fqid: str
    short_name: str
    name: str
    # Present (non-None) when the queried slug resolved via a curated
    # `classification_same_as` edge rather than directly (see reg_meta/DESIGN.md →
    # Classifications); the hop path as FQIDs.
    via_same_as: list[str] | None = None
    # #571: the FULL classification succession timeline (every edition in the
    # chain, oldest first, terminal last), resolved server-side and embedded so the
    # SPA renders the whole edition chain synchronously — superseding the immediate
    # neighbor fetch. A standalone classification carries a single self+current
    # edition. #605: querying a 1→many SPLIT root (#579) fans the chain out into ALL
    # downstream branches, so it can carry MULTIPLE `is_current` editions (one per
    # branch tip); a leaf's chain stays its single linear path.
    edition_chain: list[ClassificationEdition] = Field(default_factory=list)
    # #609: the RESOLVED edition's value-set codes (code-ordered), embedded so the
    # SPA's code viewer renders synchronously — mirroring `edition_chain`. Scoped to
    # the viewed edition only (codes are per-edition); a different edition's codes
    # arrive on ITS `class/<slug>` leaf. Empty when the edition carries no codes.
    codes: list[ClassificationCode] = Field(default_factory=list)
    # #609: the curated umbrella group(s) this edition belongs to (e.g. `group:sun`)
    # — the niva ↔ aggregate granularity cross-reference (#585/#608). Read off the
    # existing concept-group table; reuses the browse `ConceptGroupSummary`. Empty
    # for an ungrouped classification (the common case).
    dimensions: list[ConceptGroupSummary] = Field(default_factory=list)
    # #1116: the derived one-dimensional succession family this edition belongs to,
    # when one exists (e.g. ICD/SSYK/LKF/SNI). Curated umbrella membership stays in
    # `dimensions`; this field is only for family-route canonicalization.
    family: ClassificationFamilyNode | None = None
    # #779: non-temporal classification derivation refs. These are see-also links
    # distinct from `edition_chain` / succession.
    derived_from: list[ClassificationDerivedFromRef] = Field(default_factory=list)
    derivatives: list[ClassificationDerivedFromRef] = Field(default_factory=list)


class BindingChild(BaseModel):
    """A binding child under a register node — a thin (fqid, name) entry, NOT
    the embedded longitudinal record (that is only on the binding LEAF response).
    `coverage` (#351) is the per-variable study-window aggregate.

    `deliveries` (Y-82) is every `(variant, delivery column)` the variable is
    delivered under, each with its own window — the column names a researcher
    knows the variable by, and the variants the register page's chips filter on.
    Empty for a variable with no states; a filtered steward keeps only its held
    columns, the same held-column semantics `coverage` follows."""

    kind: Literal["binding"] = "binding"
    fqid: str
    name: str | None = None
    coverage: VariableCoverage | None = None
    deliveries: list[VariableDelivery] = Field(default_factory=list)


class VariantsRef(BaseModel):
    """Reference to a register's variant browser — the `/{provider}/{register}/
    variants` sub-resource (wired in A5.2a). A discriminated slot in
    `RegisterChild` so the union / TS types carry the navigable `register_fqid`;
    the client GETs `{register_fqid}/variants` to list them."""

    kind: Literal["variants-ref"] = "variants-ref"
    register_fqid: str


# ── Derived concept groups (#303; see reg_meta/DESIGN.md → Concept groups) ──
# PRESENTATION-ONLY browse folding: the register / classification-root responses
# carry `groups` ALONGSIDE the full flat children list (members repeat in both);
# the SPA hides grouped leaves and renders group rows that expand to a facet
# picker. A group is not FQID-addressable — members carry the real leaf FQIDs.
# The group/member/facet shapes are reg_meta's `ConceptGroupSummary` /
# `ConceptGroupMember` / `GroupFacet`, embedded directly (#681).


# ── Binding-leaf embedded longitudinal record ──────────────────────────────
# The binding LEAF (3-seg) embeds the variable's FULL record from one
# `Catalog.resolve` call: every state + variable-grain edges. These are reg_meta's
# frozen Pydantic models (`VariableState` / `VariableRef` / `LineageEdge` /
# `VariableEdition`) embedded directly (#681). `ResolvedVariable` does NOT carry
# lineage_warnings, so they are OMITTED here (they arrive via A5.2's
# `/lineage_warnings`).


class BindingNode(BaseModel):
    """A binding LEAF (3-seg FQID) — the addressable variable plus its FULL
    longitudinal record embedded from one `Catalog.resolve` call: shared
    metadata, every state (each tagged with its variant), the variable-grain
    `same_as` / `lineage` edges, and the full variable
    `succession_chain` (#582).

    `lineage_warnings` are intentionally OMITTED — `ResolvedVariable` doesn't
    carry them; they arrive via A5.2's `/lineage_warnings` endpoint. This full-node
    shape is the binding leaf with NO narrowing query: a `?period` resolves via
    `resolve_at` (→ `StatesResponse`) instead, and a narrowing modifier (`?variant`
    / `?value_set_version`) WITHOUT `?period` is a 422 — it is inert without a
    period, so it errors rather than silently embedding full history."""

    warnings: tuple[DataWarning, ...] = ()
    kind: Literal["binding"] = "binding"
    fqid: str
    variable_id: CatalogStorageId
    register_id: CatalogStorageId
    name: str | None
    definition: str | None
    description: str | None
    # SCB's "operationell definition" — per-(split-)variable distinguishing text
    # (#892/#932). Disambiguates parallel concept-group members whose only differing
    # metadata is this field (e.g. owner / previous-owner näringsgren). Defaulted
    # (additive) per the #317 rule — the SPA tolerates one edge-cache generation of
    # payloads missing it.
    operational_definition: str | None = None
    measurement_unit: str | None
    is_sensitive: bool
    is_identifier: bool
    deprecated: bool = False
    source_register_id: CatalogStorageId | None
    source_register_text: str | None
    states: list[VariableState]
    same_as: list[VariableRef]
    # #582: the FULL variable succession timeline (every edition in the chain,
    # oldest first, terminal last), resolved server-side (`Catalog.variable_chain`)
    # and embedded so the SPA renders the whole edition chain synchronously —
    # superseding the immediate-neighbor `replaced_by` embed. A variable with no
    # succession carries a single self+current edition. The `/predecessors` /
    # `/successors` sub-resources stay (they back the #411 permalink-redirect rails).
    succession_chain: list[VariableEdition] = Field(default_factory=list)
    lineage: list[LineageEdge]
    # #616/#617: the binding's owning concept group as its addressable
    # `(provider, register, key)` when it is a group member, else None. Lets a
    # member page render group-aware (a link to the group subject) without a
    # second fetch. Membership is 1:1 (DB PK), so this is singular; the member
    # list lives behind the group route. Defaulted (additive) per the #317 rule —
    # the SPA must tolerate one edge-cache generation of payloads missing it.
    group: BindingGroupRef | None = None
    tags: list[TagMembership] = Field(default_factory=list)
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
    the variant-browser reference stub as children, plus the derived concept
    `groups` (#303; grouped bindings ALSO appear in `children` — the flat list
    stays complete, the SPA folds it)."""

    children: list[RegisterChild]
    groups: list[ConceptGroupSummary] = []


class ClassificationRootResponse(ClassificationRootNode):
    """`GET /api/catalog/class` — the classification-root + every classification
    as children, plus the derived vintage `groups` (#303; grouped
    classifications ALSO appear in `children`)."""

    children: list[ClassificationNode]
    groups: list[ConceptGroupSummary] = []
    families: list[ClassificationFamilyNode] = Field(default_factory=list)


class RootResponse(BaseModel):
    """`GET /api/catalog` — the catalog root: every provider plus the
    classification-root sentinel."""

    kind: Literal["root"] = "root"
    children: list[ProviderNode | ClassificationRootNode]


# ── Concept-group SUBJECT node (#616/#617) ──────────────────────────────────
# A concept group addressed by `/catalog/group/<provider>/<register>/<key>` — a
# browsable subject in its own right (a group's default selection is "all
# members", which a single member FQID can't express, so it needs its own
# address). DISTINCT from the presentation-only `ConceptGroupSummary` folded into
# a register/classification listing: this is the resolved group as a first-class
# node, carrying per-member coverage so the page renders without a second fetch.


class ConceptGroupNodeMember(ConceptGroupMember):
    """A concept-group member on the group SUBJECT node — reg_meta's browse
    `ConceptGroupMember` (fqid + name + facets) PLUS the per-member study-window
    `coverage` (#351/#819; reg_meta's `VariableCoverage`, zipped on by the group
    route from variable- or column-grain coverage as appropriate). A member that is
    known to have no states uses the zero-state `VariableCoverage` shape
    (`state_count == 0`, null bounds, not open-ended); `coverage` is None only when no
    coverage enrichment could be attached. Subclassing the frozen, `extra="forbid"`
    reg_meta model to declare one new field is supported in Pydantic v2 — the subclass
    owns `coverage`."""

    coverage: VariableCoverage | None = None


class ConceptGroupNode(BaseModel):
    """The concept group as a browsable subject (#617): the group identity
    (provider/register/key + label/source/axes) and its members WITH per-member
    coverage. Returned by `/catalog/group/{provider}/{register}/{key}` — a
    fixed-shape route, NOT an FQID kind (a group is not FQID-addressable; its
    members carry the real leaf FQIDs).

    `member` echoes a validated `?member=<slug>` focus hint (a member leaf slug to
    highlight), or None when absent / unrecognized — the page stays first-class
    either way (a bad hint is ignored, not a 404)."""

    kind: Literal["concept-group"] = "concept-group"
    provider: str
    register_name: str = Field(
        alias="register",
        description="The group's register slug. The Python attr is `register_name` "
        "to avoid the BaseModel.register method shadow (see reg_meta's VariableRef); "
        "the wire key is `register` via the alias.",
    )
    key: str
    label: str
    source: Literal["edge", "token", "curated"]
    # #819: reg_meta's `GroupAxis(name, label)` embedded directly (like
    # `ConceptGroupMember`) — the SPA matches on `name`, displays `label`.
    axes: list[GroupAxis]
    members: list[ConceptGroupNodeMember]
    # #982: thematic tags aggregated from member bindings. Defaulted per the
    # additive payload-skew rule; older edge-cache generations simply omit it.
    tags: list[TagMembership] = Field(default_factory=list)
    # The validated `?member=` focus hint (a member's leaf slug), echoed so the SPA
    # highlights it; None when absent or not a member of this group.
    member: str | None = None


class ClassificationGroupNode(BaseModel):
    """The classification umbrella group as a browsable subject (#756) — the
    classification SIBLING of `ConceptGroupNode`, served only by its own fixed
    route `/catalog/group/class/{key}`. Distinct from `ConceptGroupNode` because
    classification members carry NO provider/register/coverage: a classification
    umbrella (e.g. the SUN umbrella, key `sun`) groups version-independent
    classification editions across the whole catalog (`register_id NULL`), so
    there is no register scope to key on and no per-member study-window coverage
    to zip — members are reg_meta's frozen browse `ConceptGroupMember` used
    DIRECTLY (fqid + name + facets), NOT subclassed.

    Like `ConceptGroupNode`, NOT a `CatalogNode` arm: a group is not
    FQID-addressable (its members carry the real `class/<slug>` leaf FQIDs), and
    it is served only by its fixed route, so the catch-all union never advertises
    it. There is no `member` focus-hint field — no consumer needs one (member
    highlight is #757's surface), so this stays minimal."""

    kind: Literal["classification-group"] = "classification-group"
    key: str
    label: str
    source: Literal["edge", "token", "curated"]
    # #819: reg_meta's `GroupAxis(name, label)` embedded directly (like
    # `ConceptGroupMember`) — the SPA matches on `name`, displays `label`.
    axes: list[GroupAxis]
    members: list[ConceptGroupMember]


ClassificationGroupSubject = Annotated[
    ClassificationGroupNode | ClassificationFamilyNode, Field(discriminator="kind")
]


# The catch-all `/api/catalog/{fqid:path}` returns one of these, discriminated
# by `kind` so the codegen'd TS is a tagged union (A5.3). A binding leaf is a
# `BindingNode` (full record embedded); a classification leaf a
# `ClassificationNode`. `ConceptGroupNode` / `ClassificationGroupNode` are
# deliberately NOT arms: the group SUBJECTS (#617/#756) are served ONLY by their
# fixed-shape `/catalog/group/...` routes (which declare their own response_model
# directly), never by the catch-all — so this union advertises exactly the kinds
# the catch-all can return.
CatalogNode = Annotated[
    ProviderResponse
    | RegisterResponse
    | BindingNode
    | ClassificationRootResponse
    | ClassificationNode,
    Field(discriminator="kind"),
]


# ── A5.2a-ii sub-endpoint models (see DESIGN.md → Catalog router structure) ──
# The suffixed/sub-resource read endpoints. Each returns a thin envelope
# echoing the queried `binding` (or `register`) FQID plus the reg_meta model list,
# so the SPA codegen sees one response type per endpoint. These EMBED the same
# reg_meta models the leaf embeds (`VariableState` / `VariableRef` /
# `LineageEdge` / `LineageWarning` / `VariantSummary`) — the sub-endpoints are the
# standalone accessors for the same edges the leaf embeds (#681).


class StatesResponse(BaseModel):
    """`GET /api/catalog/{fqid}/states` — the binding's full state history.
    Same `states` shape the binding leaf embeds, as a standalone envelope. With a
    `?period` query on the catch-all this same shape carries the resolve_at
    subset (uniform: codegen sees one state-list type)."""

    binding: str
    states: list[VariableState]


class ValueSetCodesResponse(BaseModel):
    """`GET /api/value-sets/{value_set_id}/codes` — ONE bounded page of a value
    set's (code, label) membership, or (with `?state=`) of one state's stored
    classification mismatch list.

    The binding leaf and its `?period` subset carry only `value_set_summary` per
    state, so the code panel reads the actual codes here when it is opened. `total`
    is the count AFTER `?q` and BEFORE the page window, so a caller knows how much
    is left without walking it; `codes` is code/label-ordered, the same order
    reg_meta hydrates a value set in."""

    value_set_id: CatalogStorageId
    # Echoes `?state=` when the page is a state's mismatch list, else None.
    state_id: CatalogStorageId | None = None
    # The `?q` the page was filtered by (empty = unfiltered).
    q: str
    total: int
    offset: int
    limit: int
    codes: list[ClassificationExtensionMember | ValueSetMember]


class PredecessorsResponse(BaseModel):
    """`GET /api/catalog/{fqid}/predecessors` — inbound succession."""

    binding: str
    predecessors: list[VariableRef]


class SuccessorsResponse(BaseModel):
    """`GET /api/catalog/{fqid}/successors` — outbound succession."""

    binding: str
    successors: list[VariableRef]


class DimensionsResponse(BaseModel):
    """`GET /api/catalog/{fqid}/dimensions` (#489) — the concept-group
    dimension memberships containing this binding's variable (the
    'pick your variant' facet groups: level / population / rank / …). A
    `ConceptGroupSummary` per containing group; empty when the variable is in
    no group."""

    binding: str
    dimensions: list[ConceptGroupSummary]


class LineageResponse(BaseModel):
    """`GET /api/catalog/{fqid}/lineage` — consumer-side lineage edges.

    Maps what `reg_meta.LineageEdge` carries (consumer/source state ids, the
    validity intersection, source_fqid). The richer per-source-state shape
    (embedding each source state's variant / value_set / column) is a possible
    reg_meta enhancement, NOT blocked on here — see DESIGN.md."""

    binding: str
    lineage_edges: list[LineageEdge]


class LineageWarningsResponse(BaseModel):
    """`GET /api/catalog/{fqid}/lineage_warnings` — build-time lineage warnings.
    Empty list when lineage resolved cleanly."""

    binding: str
    lineage_warnings: list[LineageWarning]


class VariantsResponse(BaseModel):
    """`GET /api/catalog/{provider}/{register}/variants` — the variant browser.
    The wire key `register` is the 2-seg register FQID; `variants` the
    register's `register_variant` sub-resource list (reg_meta's `VariantSummary`).

    The Python attr is `register_name` (aliased to `register`) for the same reason
    as reg_meta's `VariableRef`: a bare `register` field shadows `BaseModel.register`
    (a Pydantic v2 method) and warns. FastAPI serializes by alias, so the wire key
    stays `register`; the alias is also the canonical init param."""

    register_name: str = Field(alias="register")
    variants: list[VariantSummary]


# ── A5.2b-ii write surface (see DESIGN.md → Project-write surface
# (routes/project.py)) ───────────────────────────────────────────────────────
# `POST /api/project/validate` returns the concatenated issue list. The
# webapp wraps reg_schema's FROZEN `ValidationResult` / `ValidationIssue`
# dataclasses (see DESIGN.md → Pydantic boundary: reg_schema stays a dataclass —
# it's consumed by the SPA — so the webapp Pydantic-wraps it 1:1, exactly
# like the catalog node wrappers). This is the ONLY place reg_schema's
# ValidationResult is re-modeled; the rest of the write surface (`/order`) takes
# `reg_schema.ProjectData` directly as the typed request body.


class ValidationIssueModel(BaseModel):
    """One validation issue — a 1:1 Pydantic wrapper of reg_schema's frozen
    ``ValidationIssue`` dataclass. ``level`` is the tri-state severity; ``path`` is
    an RFC-6901 JSON pointer into ``project_data.json`` (empty for whole-document
    issues); ``code`` is the stable, namespaced rule identifier the SPA maps to a
    UI affordance."""

    level: Literal["error", "warning", "info"]
    code: str
    path: str
    message: str
    successor_fqid: str | None = None


class ValidationResultModel(BaseModel):
    """`POST /api/project/validate` response — the concatenated issue list
    (structural ⧺ semantic) plus the derived ``ok`` flag. The body is reg_meta's
    ``semantic.validation_json`` verbatim; this model types it for OpenAPI.

    ``ok`` mirrors ``reg_schema.ValidationResult.ok``: True iff NO error-level
    issue is present (warnings/info do not flip it). A validation FAILURE is a
    SUCCESSFUL validation RESPONSE — this carries HTTP 200 with ``ok=false`` and
    the issues; 4xx is reserved for a malformed request (bad JSON / oversized body
    / wrong content-type), never for a spec that simply failed to validate."""

    ok: bool
    issues: list[ValidationIssueModel]


class OrderBlockedModel(BaseModel):
    """`POST /api/project/order` 422 body — the "this is not an order" result.

    Fail-closed is a CONTRACT, not a message: the findings ride as reg_meta's own
    frozen ``OrderFinding`` models (embedded directly, like every other reg_meta
    shape here), each carrying its stable ``code``, its message, and the optional
    ``source`` / ``variable`` / ``period`` coordinates that say WHERE — so the SPA
    renders them through the same per-finding path as a validation issue, and any
    other client can act on them, instead of parsing one flattened string.

    ``detail`` is the same flattened one-liner ``order.blocked_message`` gives the
    CLI, kept because every 4xx on this API carries a ``detail`` string (FastAPI's
    ``HTTPException`` shape) and generic error handling reads it. ``findings`` is
    EMPTY when the spec never reached the materializer (a structurally invalid
    project — the gate's own 422), which is exactly the truth: nothing found it
    unorderable, it was never ordered."""

    detail: str
    findings: list[OrderFinding]
