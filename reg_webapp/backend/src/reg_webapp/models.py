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
# A concept group addressed by `/catalog/group/<provider>/<register>/<key>` — a
# browsable subject in its own right (a group's default selection is "all
# members", which a single member FQID can't express, so it needs its own
# address). DISTINCT from the presentation-only `ConceptGroupSummary` folded into
# a register/classification listing: this is the resolved group as a first-class
# node, carrying per-member coverage so the page renders without a second fetch.


class ConceptGroupNodeMember(ConceptGroupMember):
    """A concept-group member on the group SUBJECT node — reg_meta's browse
    `ConceptGroupMember` (fqid + name + facets) PLUS the per-member study-window
    `coverage` (#351; reg_meta's `VariableCoverage`, zipped on by the group route
    from `register_variable_coverage`). `coverage` is None for a member with no
    coverage row (a stateless variable, or a member whose leaf slug didn't match
    the register's coverage map — defensive). Subclassing the frozen,
    `extra="forbid"` reg_meta model to declare one new field is supported in
    Pydantic v2 — the subclass owns `coverage`."""

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
    axes: list[str]
    members: list[ConceptGroupNodeMember]
    # The validated `?member=` focus hint (a member's leaf slug), echoed so the SPA
    # highlights it; None when absent or not a member of this group.
    member: str | None = None


# The catch-all `/api/catalog/{fqid:path}` returns one of these, discriminated
# by `kind` so the codegen'd TS is a tagged union (A5.3). A binding leaf is a
# `BindingNode` (full record embedded); a classification leaf a
# `ClassificationNode`. `ConceptGroupNode` is deliberately NOT an arm: the group
# SUBJECT (#617) is served ONLY by the fixed-shape `/catalog/group/...` route
# (which declares `response_model=ConceptGroupNode` directly), never by the
# catch-all — so this union advertises exactly the kinds the catch-all can return.
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
# to render it. Each result carries its navigable `fqid`; results within a group
# are pre-sorted by FTS rank (after the #311 golden-boost seam), so the wire
# carries no raw rank.


class RegisterSearchResult(BaseModel):
    """A register hit (`register_fts` name/purpose)."""

    type: Literal["register"] = "register"
    fqid: str | None
    name: str | None = None
    purpose: str | None = None


class VariableSearchResult(BaseModel):
    """A variable hit (`variable_fts` name/definition/description). `register` is
    the owning register's display name (context for the omnibox). When the hit is
    a LONE member of a concept group (#322 — the family didn't fold because only
    one member matched), `concept_group`/`concept_group_label` annotate the
    family so it stays discoverable; both None otherwise."""

    type: Literal["variable"] = "variable"
    fqid: str | None
    name: str | None = None
    # `register_name` aliased to the wire key `register` — a bare `register`
    # field shadows `BaseModel.register` (see reg_meta's `VariableRef`). The alias is
    # the canonical init param; FastAPI serializes by alias, so the JSON key is
    # `register`.
    register_name: str | None = Field(default=None, alias="register")
    definition: str | None = None
    concept_group: str | None = None
    concept_group_label: str | None = None


class ClassificationSearchResult(BaseModel):
    """A classification hit (`classification_fts` short_name/name/name_en/
    description — #350 activates this previously-unsearched index). When the hit
    is a LONE member of a vintage group (the family didn't fold because only one
    member matched), `concept_group`/`concept_group_label` annotate the family so
    it stays discoverable — symmetric with `VariableSearchResult`; both None
    otherwise."""

    type: Literal["classification"] = "classification"
    fqid: str | None
    short_name: str | None = None
    name: str | None = None
    concept_group: str | None = None
    concept_group_label: str | None = None
    terminal_fqid: str | None = Field(
        default=None,
        description="When this is a non-current edition that the query hit alone, "
        "the fqid of the current/terminal edition in its succession chain (#571) — "
        "lets the UI link to the current edition; None for a current edition or a "
        "non-edition classification.",
    )


class ClassificationEditionModel(BaseModel):
    """One edition of a folded classification succession chain (#571): a vintage
    of the same classification (e.g. `sun1996`, `sun2000`). Carried by
    `ClassificationSuccessionSearchResult.editions`, terminal-first then descending
    `effective_year`. Every edition is a live `classification` row (the build
    validator guarantees succession editions are live), so `fqid` is None only when
    the slug is malformed/unresolvable."""

    slug: str = Field(description="The edition's literal slug (e.g. 'sun2000').")
    fqid: str | None = Field(
        default=None,
        description="The edition's 2-seg classification FQID, None only when the "
        "slug is malformed/unresolvable (succession editions are live rows).",
    )
    name: str | None = Field(
        default=None, description="The edition's display name, None when un-hydrated."
    )
    effective_year: int | None = Field(
        default=None,
        description="The year this edition was superseded by its successor (from its "
        "outbound succession edge); None for the terminal (head) edition, which has "
        "no outbound edge.",
    )


class ClassificationSuccessionSearchResult(BaseModel):
    """A folded classification-succession row (#571): a query hit ≥2 distinct
    editions of one classification chain (the vintages, e.g. SUN 1996/2000), so they
    collapse to a single result keyed on the TERMINAL (current) edition. `editions`
    is the full chain (terminal-first, descending year) so the SPA can render "this
    classification has editions …"; `matched_count` is how many editions the query
    actually hit. A succession row is NOT itself a concept group — the terminal
    `fqid` is the navigable target."""

    type: Literal["classification_succession"] = "classification_succession"
    fqid: str | None = Field(
        description="The terminal (current) edition's classification FQID — the "
        "navigable target. None only when the slug is malformed/unresolvable (the "
        "terminal is always a live classification row)."
    )
    short_name: str | None = Field(
        default=None, description="The terminal edition's short name (e.g. 'SUN')."
    )
    name: str | None = Field(
        default=None, description="The terminal edition's display name."
    )
    editions: list[ClassificationEditionModel] = Field(
        default_factory=list,
        description="The full edition chain, terminal-first then descending year.",
    )
    matched_count: int = Field(
        default=0, description="How many editions in the chain the query hit."
    )


class ConceptGroupSearchResult(BaseModel):
    """A folded concept-group row (#322): ≥2 sibling members matched OR the
    group's own label matched, so the family collapses to one result. `kind` is
    'variable' or 'classification' (which group bucket it belongs to);
    `member_count` is the family's full size, `matched_count` how many members
    the query hit, `label_matched` whether the group label/key matched directly.
    `members` is the full facet-ordered member list (each a real leaf FQID) so
    the SPA can expand the family inline — a group is NOT itself FQID-addressable."""

    type: Literal["group"] = "group"
    kind: Literal["variable", "classification"]
    group_key: str
    group_label: str
    source: str | None = None
    # `register_name` aliased to the wire key `register` (avoids the
    # `BaseModel.register` shadow — see reg_meta's `VariableRef`). None for a
    # classification-kind group (catalog-scoped, no owning register).
    register_name: str | None = Field(default=None, alias="register")
    member_count: int = 0
    matched_count: int = 0
    label_matched: bool = False
    # reg_meta's `ConceptGroupMember` embedded directly (#681) — the search path
    # constructs them from the `reg_meta.queries.search` member dicts (`fqid` is a
    # string the `Fqid` field parses; `facets` are `{axis, value, label}` dicts).
    members: list[ConceptGroupMember] = []


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


# ── Code/value search (#352; see reg_meta queries.search type="value") ───────
# A code hit's actionable target is the VARIABLE or CLASSIFICATION carrying the
# code, not the bare (code, label) pair — so each hit surfaces a bounded
# representative slice of its owners plus the full count (the SPA shows "+N more").


class CodeOwnerVariable(BaseModel):
    """A variable that carries a code (#352). `register` is the owning register's
    display name (context for the omnibox); the Python attr is `register_name` to
    avoid the `BaseModel.register` method shadow (see reg_meta's `VariableRef`)."""

    fqid: str | None
    name: str | None = None
    register_name: str | None = Field(default=None, alias="register")


class CodeOwnerClassification(BaseModel):
    """A classification that carries a code (#352) — catalog-scoped (no owning
    register)."""

    fqid: str | None
    short_name: str | None = None
    name: str | None = None


class CodeSearchResult(BaseModel):
    """A code/value hit (`value_code_fts` label match + code-shape match, #352).
    `code`/`label` are the SCB value pair; `variables`/`classifications` are a
    bounded representative slice of the owning entities (the researcher's actual
    target), and `variable_count`/`classification_count` are the full totals before
    the slice cap."""

    type: Literal["code"] = "code"
    code: str
    label: str
    variables: list[CodeOwnerVariable] = []
    variable_count: int = 0
    classifications: list[CodeOwnerClassification] = []
    classification_count: int = 0
    # Inferred from the owning classification: the primary/first owning
    # classification's short_name (fall back to its name). None for
    # register-local / bespoke codes with no owning classification (#393 item 3).
    # The SPA groups the codes group into per-code-system subsections off this.
    code_system: str | None = None


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
