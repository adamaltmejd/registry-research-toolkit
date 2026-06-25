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
    ClassificationCode,
    ClassificationEdition,
    ConceptGroupMember,
    ConceptGroupSummary,
    LineageEdge,
    LineageWarning,
    RegisterCoverage,
    RelatedRef,
    VariableCoverage,
    VariableEdition,
    VariableRef,
    VariableState,
    VariantSummary,
)
from reg_meta.fqid import CLASSIFICATION_PREFIX
from reg_meta.search import (
    ClassificationSearchResult,
    ClassificationSuccessionSearchResult,
    CodeSearchResult,
    ConceptGroupSearchResult,
    RegisterSearchResult,
    VariableSearchResult,
)


class StewardInfo(BaseModel):
    """Deployment identity + branding, from ``steward.toml``."""

    id: str
    name: str
    long_name: str


class RegMetaInfo(BaseModel):
    """reg_meta build provenance, read from the DB ``import_manifest``."""

    schema_version: str = Field(
        description="Schema version of the reg_meta DB build (e.g. '5.2.0')."
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


class CatalogDriftWarning(BaseModel):
    """One boot-time steward-catalog drift warning.

    Emitted when the steward's committed ``steward.project_data.json`` references
    an FQID reg_meta no longer admits: the steward-mode semantic validator
    downgrades the miss to a warning, the binding drops from the in-memory index,
    and this carries the warning to the SPA so it can show a "catalog drift"
    banner. ``code`` is the ValidationIssue code (``fqid_unresolved`` /
    ``value_set_missing`` / ``period_outside_state_validity``); ``path`` is the
    JSON pointer into the steward catalog. Always empty for the ``global``
    deployment (no filter)."""

    code: str
    path: str
    message: str


class ContextResponse(BaseModel):
    """``GET /api/context`` — deployment identity, branding, build info.

    No git sha (decision: no new provenance dep). The reg_meta block reflects
    the DB the backend booted against; the webapp block reflects the installed
    packages. ``catalog_drift_warnings`` is the steward-catalog
    drift surfaced at boot — empty for ``global`` and for an up-to-date catalog.
    """

    steward: StewardInfo
    reg_meta: RegMetaInfo
    webapp: WebappInfo
    catalog_drift_warnings: list[CatalogDriftWarning] = Field(default_factory=list)


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


class ClassificationSuccessionEdge(BaseModel):
    """One explicit classification succession edge inside a leaf's embedded
    edition graph. Unlike ``edition_chain`` ordering, this is the real pairwise
    edge from ``classification_replaced_by``."""

    predecessor_slug: str
    predecessor_fqid: str | None = None
    successor_slug: str
    successor_fqid: str | None = None
    effective_year: int | None = None
    note: str | None = None


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
    coverage: RegisterCoverage | None = None


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
    # The explicit pairwise edges among ``edition_chain`` nodes, read from
    # ``classification_replaced_by``. The chain is a flattened traversal; this carries
    # the actual graph edges so the SPA does not infer split/fan-out topology from
    # slugs or ordering.
    edition_edges: list[ClassificationSuccessionEdge] = Field(default_factory=list)
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


class BindingChild(BaseModel):
    """A binding child under a register node — a thin (fqid, name) entry, NOT
    the embedded longitudinal record (that is only on the binding LEAF response).
    `coverage` (#351) is the per-variable study-window aggregate."""

    kind: Literal["binding"] = "binding"
    fqid: str
    name: str | None = None
    coverage: VariableCoverage | None = None


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
# frozen Pydantic models (`VariableState` / `VariableRef` / `RelatedRef` /
# `LineageEdge` / `VariableEdition`) embedded directly (#681). `ResolvedVariable`
# does NOT carry lineage_warnings, so they are OMITTED here (they arrive via A5.2's
# `/lineage_warnings`).


class BindingNode(BaseModel):
    """A binding LEAF (3-seg FQID) — the addressable variable plus its FULL
    longitudinal record embedded from one `Catalog.resolve` call: shared
    metadata, every state (each tagged with its variant), the variable-grain
    `same_as` / `related_to` / `lineage` edges, and the full variable
    `succession_chain` (#582).

    `lineage_warnings` are intentionally OMITTED — `ResolvedVariable` doesn't
    carry them; they arrive via A5.2's `/lineage_warnings` endpoint. This full-node
    shape is the binding leaf with NO narrowing query: a `?period` resolves via
    `resolve_at` (→ `StatesResponse`) instead, and a narrowing modifier (`?variant`
    / `?value_set_version`) WITHOUT `?period` is a 422 — it is inert without a
    period, so it errors rather than silently embedding full history."""

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
    states: list[VariableState]
    same_as: list[VariableRef]
    # #582: the FULL variable succession timeline (every edition in the chain,
    # oldest first, terminal last), resolved server-side (`Catalog.variable_chain`)
    # and embedded so the SPA renders the whole edition chain synchronously —
    # superseding the immediate-neighbor `replaced_by` embed. A variable with no
    # succession carries a single self+current edition. The `/predecessors` /
    # `/successors` sub-resources stay (they back the #411 permalink-redirect rails).
    succession_chain: list[VariableEdition] = Field(default_factory=list)
    related_to: list[RelatedRef]
    lineage: list[LineageEdge]
    # #616/#617: the binding's owning concept group as its addressable
    # `(provider, register, key)` when it is a group member, else None. Lets a
    # member page render group-aware (a link to the group subject) without a
    # second fetch. Membership is 1:1 (DB PK), so this is singular; the member
    # list lives behind the group route. Defaulted (additive) per the #317 rule —
    # the SPA must tolerate one edge-cache generation of payloads missing it.
    group: BindingGroupRef | None = None
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


class RootResponse(BaseModel):
    """`GET /api/catalog` — the catalog root: every provider plus the
    classification-root sentinel."""

    kind: Literal["root"] = "root"
    children: list[ProviderNode | ClassificationRootNode]


# ── Concept-group SUBJECT node (#616/#617) ──────────────────────────────────
# A concept group addressed by `/catalog/group/<provider>/<register>/<key>` for
# variable groups or `/catalog/group/class/<key>` for classification groups — a
# browsable subject in its own right (a group's default selection is "all members",
# which a single member FQID can't express, so it needs its own address). DISTINCT
# from the presentation-only `ConceptGroupSummary` folded into a register /
# classification listing: this is the resolved group as a first-class node.


class ConceptGroupNodeMember(ConceptGroupMember):
    """A concept-group member on the group SUBJECT node — reg_meta's browse
    `ConceptGroupMember` (fqid + name + facets) PLUS optional per-member
    study-window `coverage` (#351). Variable groups zip this on from
    `register_variable_coverage`; classification groups have no delivery coverage
    and leave it None. Subclassing the frozen, `extra="forbid"` reg_meta model to
    declare one new field is supported in Pydantic v2 — the subclass owns
    `coverage`."""

    coverage: VariableCoverage | None = None


class ConceptGroupNode(BaseModel):
    """The concept group as a browsable subject (#617): the group identity
    (scope/key + label/source/axes) and its members. Returned by
    `/catalog/group/{provider}/{register}/{key}` for variable groups and
    `/catalog/group/class/{key}` for classification groups — fixed-shape routes,
    NOT FQID kinds (a group is not FQID-addressable; its members carry the real
    leaf FQIDs).

    `member` echoes a validated `?member=<slug>` focus hint (a member leaf slug to
    highlight), or None when absent / unrecognized — the page stays first-class
    either way (a bad hint is ignored, not a 404)."""

    kind: Literal["concept-group"] = "concept-group"
    provider: str
    register_name: str | None = Field(
        default=None,
        alias="register",
        description="The group's register slug for register-scoped variable groups; "
        "None for classification groups. The Python attr is `register_name` to "
        "avoid the BaseModel.register method shadow (see reg_meta's VariableRef); "
        "the wire key is `register` via the alias.",
    )
    key: str
    label: str
    source: Literal["edge", "token", "curated"]
    axes: list[str]
    members: list[ConceptGroupNodeMember]
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
    axes: list[str]
    members: list[ConceptGroupMember]


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
# The 7 suffixed/sub-resource read endpoints. Each returns a thin envelope
# echoing the queried `binding` (or `register`) FQID plus the reg_meta model list,
# so the SPA codegen sees one response type per endpoint. These EMBED the same
# reg_meta models the leaf embeds (`VariableState` / `VariableRef` / `RelatedRef` /
# `LineageEdge` / `LineageWarning` / `VariantSummary`) — the sub-endpoints are the
# standalone accessors for the same edges the leaf embeds (#681).


class StatesResponse(BaseModel):
    """`GET /api/catalog/{fqid}/states` — the binding's full state history.
    Same `states` shape the binding leaf embeds, as a standalone envelope. With a
    `?period` query on the catch-all this same shape carries the resolve_at
    subset (uniform: codegen sees one state-list type)."""

    binding: str
    states: list[VariableState]


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


class RelatedResponse(BaseModel):
    """`GET /api/catalog/{fqid}/related` — split-sibling edges."""

    binding: str
    related: list[RelatedRef]


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


class ValidationResultModel(BaseModel):
    """`POST /api/project/validate` response — the concatenated issue list
    (structural ⧺ block ⧺ semantic) plus the derived ``ok`` flag.

    ``ok`` mirrors ``reg_schema.ValidationResult.ok``: True iff NO error-level
    issue is present (warnings/info do not flip it). A validation FAILURE is a
    SUCCESSFUL validation RESPONSE — this carries HTTP 200 with ``ok=false`` and
    the issues; 4xx is reserved for a malformed request (bad JSON / oversized body
    / wrong content-type), never for a spec that simply failed to validate."""

    ok: bool
    issues: list[ValidationIssueModel]


# ── Global catalog search (#350; see DESIGN.md → Global catalog search) ──────
# `GET /api/search` returns TYPED RESULT GROUPS over the shipped FTS5 indexes
# (register_fts / variable_fts / classification_fts). THE GROUP LIST IS THE
# EXTENSION POINT: codes (#352) DID join as a new arm of the `SearchGroup` union
# (+ result model, own `group` literal) — the codes group is part of this
# endpoint. Docs (#354) deliberately did NOT join: its separate-optional-DB +
# `ingested` degradation doesn't map onto a group's `total_count`/`results`
# shape, so docs is served by the SEPARATE optional `GET /api/docs/search`
# endpoint that the SPA consumes directly as a 5th group + the `/doc/<filename>`
# viewer (#394); the `docs` arm of `SearchGroup` stays RESERVED / unused. The
# group list remains the extension point for any FUTURE arm: a new arm is a new
# `SearchGroup` member + result model with its own `group` literal — existing
# groups and their result models are never reshaped. The SPA must tolerate an
# unknown `group` value (skip it), so a new arm can ship before the SPA learns
# to render it. Each result carries its navigable `fqid`. The per-result MODELS
# now live in reg_meta (`reg_meta.search`, #701) — the search-surface analog of
# the #681 catalog-typing collapse: `reg_meta.queries.search` returns them
# directly, and the webapp embeds them as FastAPI response models instead of
# re-wrapping. Each result also carries a `rank` (the CLI's doc-merge interleaves
# by it); results within a group are pre-sorted server-side, so the SPA may ignore it.


# Discriminated on `type`: a variables/classifications group mixes leaf hits with
# folded concept-group rows; a registers group has leaves only.
VariableSearchItem = Annotated[
    VariableSearchResult | ConceptGroupSearchResult, Field(discriminator="type")
]
ClassificationSearchItem = Annotated[
    ClassificationSearchResult
    | ClassificationSuccessionSearchResult
    | ConceptGroupSearchResult,
    Field(discriminator="type"),
]


class RegisterSearchGroup(BaseModel):
    """The `registers` result group. `total_count` is the folded result count
    for this group BEFORE the per-group display limit (so the SPA can show
    "showing N of M")."""

    group: Literal["registers"] = "registers"
    total_count: int
    results: list[RegisterSearchResult]


class VariableSearchGroup(BaseModel):
    """The `variables` result group (leaf hits ⧺ folded concept groups)."""

    group: Literal["variables"] = "variables"
    total_count: int
    results: list[VariableSearchItem]


class ClassificationSearchGroup(BaseModel):
    """The `classifications` result group: leaf hits ⧺ folded classification
    succession rows (#571, edition chains) ⧺ folded umbrella concept-group rows
    (#516, e.g. `group:sun`)."""

    group: Literal["classifications"] = "classifications"
    total_count: int
    results: list[ClassificationSearchItem]


class CodeSearchGroup(BaseModel):
    """The `codes` result group (#352). `total_count` is the result count before
    the per-group display limit (so the SPA can show "showing N of M")."""

    group: Literal["codes"] = "codes"
    total_count: int
    results: list[CodeSearchResult]


# The extension seam: append `DocSearchGroup` (#354) arms here — each a new
# `group` literal with its own result model — without touching the others.
SearchGroup = Annotated[
    RegisterSearchGroup
    | VariableSearchGroup
    | ClassificationSearchGroup
    | CodeSearchGroup,
    Field(discriminator="group"),
]


class SearchResponse(BaseModel):
    """`GET /api/search?q=` — typed result groups over the shipped FTS indexes.
    `query` echoes the raw user query; `groups` is the ordered list of typed
    groups (see the `SearchGroup` union for the extension contract)."""

    kind: Literal["search"] = "search"
    query: str
    groups: list[SearchGroup]


# ── Docs library (#354; see DESIGN.md → Docs library endpoints) ─────────────
# Read surface over the prebuilt `reg_meta_docs.db` FTS index. POLICY: serve
# EXCERPTS + a pointer to the SCB source, NEVER the full converted body
# (marker+Gemini conversion quality + republication exposure). Coverage is
# LISA-only today — the response distinguishes "no docs INGESTED" (the index or
# this register isn't covered) from "no doc found for this query/variable", so a
# UI never implies a variable is undocumented when it's merely un-ingested.


class DocResult(BaseModel):
    """One documentation hit. `snippet` is a query-context EXCERPT (the FTS5
    snippet), not full text. `source` is the SCB source-document identifier the
    doc was derived from; `source_url` is the resolved SCB PDF link (a curated
    source→PDF map applied at doc-DB build, #372), None when the source is
    uncurated; `source_title` is the human-readable publication title for that
    source (also None when uncurated). `fuzzy` marks a name/provider_key match
    (the "mentioned in documentation" variable hook) as a heuristic text match,
    not an authoritative variable→doc link. `register` is the wire key; the
    Python attr is `register_name` (avoids the `BaseModel.register` shadow)."""

    register_name: str | None = Field(default=None, alias="register")
    variable: str | None = None
    filename: str
    display_name: str | None = None
    tags: list[str] = []
    snippet: str | None = None
    source: str | None = None
    source_url: str | None = None
    source_title: str | None = None
    fuzzy: bool = False


class DocSearchResponse(BaseModel):
    """`GET /api/docs/search`. `ingested` is False when the deployment ships no
    docs index at all (`reg_meta_docs.db` absent) — "no docs ingested", distinct
    from an empty `results` for a real query against a present index."""

    kind: Literal["doc-search"] = "doc-search"
    query: str
    ingested: bool
    total_count: int
    results: list[DocResult]


class DocDetail(BaseModel):
    """`GET /api/docs/doc/{identifier}` — metadata + source pointer + a BOUNDED
    `excerpt` (never the full converted body). `source_url` is the resolved SCB
    PDF link (curated source→PDF map applied at doc-DB build, #372), None when
    uncurated; `source_title` is the human-readable publication title for that
    source. `register` is the wire key (Python attr `register_name`)."""

    kind: Literal["doc"] = "doc"
    register_name: str | None = Field(default=None, alias="register")
    variable: str | None = None
    filename: str
    display_name: str | None = None
    tags: list[str] = []
    source: str | None = None
    source_url: str | None = None
    source_title: str | None = None
    excerpt: str | None = None


class DocVariableMentions(BaseModel):
    """`GET /api/docs/for-variable` — the "mentioned in documentation" hook for a
    variable leaf. `ingested` is whether the docs index exists at all;
    `register_ingested` whether THIS register has any ingested docs — absence
    means "no docs ingested for this register" (coverage is LISA-only), NOT "no
    documentation exists". `results` are FUZZY name/provider_key text matches
    (each `fuzzy=True`). `register` is the wire key (Python attr
    `register_name`)."""

    kind: Literal["doc-mentions"] = "doc-mentions"
    ingested: bool
    register_ingested: bool
    register_name: str | None = Field(default=None, alias="register")
    total_count: int
    results: list[DocResult]
