"""Catalog relationship-graph contract (#761).

A typed graph object the webapp renders as-is: ``reg_meta`` owns the topology and
the domain predicates that shape it (is a single-variable graph meaningful? how
does a group expand? which editions dedup? where does a representation run break?),
so the SPA never assembles graph *semantics* (the #667 spike, PR #753, settled
that). The renderer (#678) consumes this object; layout / visual grain / time axis
/ the lineage-provenance affordance are all its concern, NOT this module's.

Placement (see DESIGN.md → Relationship graph): the model + the builders live here,
off ``catalog.py`` (which is already ~2.6k lines), and ``Catalog`` exposes three
thin accessors (``graph_for_fqid`` / ``graph_for_group`` /
``graph_for_classification_group``) that delegate here.
``graph.py`` imports from ``catalog.py``; ``catalog.py`` imports ``graph`` lazily
inside those two methods, so the dependency stays one-directional (``catalog`` is
the lower layer). The graph models are FROZEN ``_CatalogModel``s used DIRECTLY as
the webapp's FastAPI response models (no wrapper, per #681).

**Compose, don't re-query.** The builder orchestrates the existing ``Catalog``
accessors — each the single source of truth for its edge type (``variable_chain``
for variable succession, ``representation_successions`` for representation-grain
succession, ``resolve``'s ``ResolvedVariable.group``
+ ``concept_group`` for variable-group membership, ``classification_group`` for the
classification umbrella, ``classification_chain`` for classification editions,
``resolve`` for same_as canonicalization). The only genuinely new logic is group
expansion, edition dedup, and the representation-run computation — no fresh SQL
re-deriving succession / lineage (that would fork the edge logic; the repo's
leaf-duplication trap).

**One edge kind: ``succession``.** Everything else is metadata / affordance:
``lineage`` and ``source_register`` are #678's provenance affordance (not edges);
group membership is shared ``group_key`` metadata + renderer clustering (no
``group:<key>`` node); ``same_as`` is resolved away to the canonical node and
exposed as node metadata (``same_as[]``); ordinary representation / value-set
transitions are states-within-a-node (the run ids), while curated
``representation_replaced_by`` rows are succession edges with column/variant
endpoint metadata.
"""

from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import Field

from .catalog import (
    OPEN_ENDED_VALID_TO,
    UNKNOWN_VALID_FROM,
    GroupFacet,
    ResolvedVariable,
    _CatalogModel,
)
from .errors import EXIT_NOT_FOUND, RegMetaError
from .fqid import Fqid

if TYPE_CHECKING:
    from .catalog import (
        Catalog,
        ClassificationFamilySummary,
        ConceptGroupSummary,
        RepresentationSuccessionRef,
        VariableEdition,
        VariableState,
    )


# ── The graph model (see DESIGN.md → Relationship graph) ────────────────────


class GraphState(_CatalogModel):
    """One ``variable_state`` inside a variable node, emitted ordered by
    ``(variant, valid_from)``. ``representation_run_id`` groups consecutive states
    into rendered cells: states sharing it form ONE cell. The id increments at each
    #526 representation boundary and at every ``variant`` change (a run never spans
    variants). Raw ``data_type`` / ``data_length`` are intentionally NOT on the wire
    AND are NOT boundary signals: SCB's per-delivery ``Datatyp`` / length is
    low-trust passthrough (#526 blanks it), so a type/length wobble alone never
    opens a new run — the boundary is value-set identity (id + version label) /
    classification / column identity only (see ``_is_representation_boundary``)."""

    state_id: int
    variant: str
    # `register_variant.name` — the variant's curator display name (e.g. "Snöskotrar"
    # for slug `snoskotrar`), surfaced for DISPLAY (the picker shows it instead of the
    # ASCII-folded slug). None for a NULL-named variant → the consumer falls back to
    # the `variant` slug. Display-only; `variant` stays the add coordinate.
    variant_label: str | None
    # Variant-family metadata (#376), copied from `VariableState`. Null when the
    # concrete variant is not part of a curated variant succession family.
    variant_family: str | None = None
    variant_family_label: str | None = None
    representation_run_id: int
    delivery_column_name: str | None
    value_set_id: int | None
    value_set_version_label: str
    classification_slug: str | None
    # ISO 'YYYY-MM-DD'; None = unknown/open start. The unknown-start `0001-01-01`
    # sentinel is normalized to None here (mirroring the open-END `9999-12-31` →
    # None below), so the renderer's time axis reads "unknown start" rather than a
    # year-1 tick.
    valid_from: str | None
    valid_to: str | None


class SameAsRef(_CatalogModel):
    """A ``same_as`` alias resolved away to the canonical node — node metadata, not
    an edge (the canonical IS the node; the aliases are the FQIDs that resolve to
    it). ``register`` is the wire/init name; the Python attr is ``register_name``
    to avoid the ``BaseModel.register`` shadow (#681)."""

    fqid: Fqid
    register_name: str = Field(alias="register")


class _GraphNodeBase(_CatalogModel):
    id: str  # FQID for variables; 'class/<slug>' for editions — the node dedup key
    fqid: Fqid | None  # navigation target; None when not addressable
    label: str
    group_key: str | None  # shared clustering key; None = ungrouped


class VariableGraphNode(_GraphNodeBase):
    """A variable node: ONE node per variable, its full ``variable_state`` history
    as sub-structure (ordered ``(variant, valid_from)``; the run ids drive cells).
    Nodes are variables — not states — because succession / same_as / group are
    all variable-grain and the FQID must map to exactly one node."""

    kind: Literal["variable"] = "variable"
    # The variable's shared metadata (`ResolvedVariable.definition`/`description`) —
    # the human-readable concept text. Carried on the node so a group page can surface
    # its shared concept definition/description from the member union alone (#678),
    # without a separate per-member resolve. None on a succession-chain edition node
    # minted thin (no full resolve) — fine; those carry no metadata.
    definition: str | None
    description: str | None
    # Per-(split-)variable distinguishing text (#892/#932) — disambiguates parallel
    # concept-group members whose only differing metadata is this field. Carried on the
    # node so a group page surfaces each member's op-def from the member union alone.
    # None on a thin succession-chain edition node (no full resolve).
    operational_definition: str | None
    states: list[GraphState]
    same_as: list[SameAsRef]
    # The resolved variable's facet assignments WITHIN its canonical concept group
    # (`resolved.group`), in the group's member order — the canonical-group member's
    # own `GroupFacet`s (reused directly as the wire type, #681). Empty when the
    # variable is ungrouped, or when the group/member can't be located (skew). Lets
    # the #678 binding-leaf header derive its #670 facet identity from the graph
    # alone, without a separate `/dimensions` fetch.
    facets: list[GroupFacet]
    # The canonical concept group's display label (`ConceptGroupSummary.label`);
    # None when the variable is ungrouped (or on group/member skew). The renderer's
    # group-link text.
    group_label: str | None


class ClassificationGraphNode(_GraphNodeBase):
    """A classification-edition node: time is a POINT (``version_year``), never an
    interval — an edition is not "dead" after its successor. ``is_current`` marks a
    terminal (head) edition."""

    kind: Literal["classification"] = "classification"
    short_name: str | None
    version_year: int | None
    is_current: bool
    # The umbrella group's display label (the ``ConceptGroupSummary.label``, e.g.
    # "SUN — Svensk utbildningsnomenklatur"); None when the edition is not a curated
    # umbrella member. The classification analog of `VariableGraphNode.group_label`:
    # it gives a SUN/related-granularities umbrella cluster a heading (the renderer
    # has no other label for a classification cluster, whose `group_key` is the bare
    # `class/<key>` slug, not a display string). Carried only on curated members
    # (mirrors `group_key`), so a non-member spine edition pulled in by a #579 split
    # walk stays headless.
    group_label: str | None = None


GraphNode = Annotated[
    VariableGraphNode | ClassificationGraphNode, Field(discriminator="kind")
]


class GraphEdge(_CatalogModel):
    """A graph edge. ``id`` is stable and doubles as the dedup key. ``succession``
    is DIRECTED (predecessor → successor). ``label`` is the succession reason.

    ``effective_year`` is the supersession year — the year the source edition was
    replaced by the target (the ``*_replaced_by`` row's ``effective_year``). It is
    carried independently of ``label`` so the #678 timeline can annotate the
    transition with its year even when there is no human reason (a year-only edge
    would otherwise render as an unlabelled arrow)."""

    id: str
    kind: Literal["succession"] = "succession"
    source: str  # node id
    target: str  # node id
    label: str | None
    effective_year: int | None = None
    # Representation-grain succession (#843/#846): when populated, the edge is a
    # column-level succession inside the variable-grain source/target nodes. A
    # null `variant` means the edge is unscoped; otherwise it applies only inside
    # that register-variant slug.
    source_column: str | None = None
    target_column: str | None = None
    variant: str | None = None


class RelationshipGraph(_CatalogModel):
    """The relationship graph for a subject. ``nodes: []`` is the "don't render"
    signal (the frontend gate is ``nodes.length === 0``): a lone variable with no
    succession / group siblings / meaningful representation boundary, or
    a lone classification edition with no succession chain and no group context.
    ``focus_id`` is the node matching the requested FQID (post same_as); None for
    group-addressed calls."""

    nodes: list[GraphNode]
    edges: list[GraphEdge]
    focus_id: str | None


# ── Representation-run computation (the #526 fold, query-side mirror) ────────


def _is_representation_boundary(prev: VariableState, cur: VariableState) -> bool:
    """Whether ``cur`` opens a NEW representation run relative to the preceding
    state ``prev`` in the same variant — the #526 fold rule, scoped to materialized
    rows (#761).

    A boundary is EXACTLY one of FOUR identity changes between adjacent states:
    the value-set IDENTITY — both its ``value_set_id`` AND its
    ``value_set_version_label`` (the #526 state-identity gkey for a VALUED state is
    keyed on both; two states sharing a ``value_set_id`` but differing in label are
    DISTINCT materialized states, so the label is part of value-set identity, not a
    low-trust wobble) — the classification (``classification_slug``), or the
    coalesced ``delivery_column_name`` (the per-era surviving column — a cross-era
    column RENAME). These are precisely the distinctions that survive #526's
    value-set-anchored fold in ``variable_state``. The label is ``''`` for valueless
    states, so it never spuriously fires there. Raw ``data_type`` / ``data_length``
    are NEVER a boundary signal on their own: SCB's per-delivery ``Datatyp`` / length
    is low-trust passthrough that #526 blanks, so an `int -> bigint` or char↔varchar
    wobble does NOT open a run. Per-period alias multiplexing (monthly families' 12
    columns, held in ``variable_alias_window`` not in ``states``) is an alias
    concern, NOT a coding boundary — those expanded windows share a ``state_id`` and
    are never compared here (a node folds its states by state_id before this runs)."""
    return (
        prev.value_set_id != cur.value_set_id
        or prev.value_set_version_label != cur.value_set_version_label
        or prev.classification_slug != cur.classification_slug
        or prev.delivery_column_name != cur.delivery_column_name
    )


def _graph_states(states: tuple[VariableState, ...]) -> list[GraphState]:
    """Fold a variable's ``variable_state`` history into ``GraphState`` rows with
    ``representation_run_id`` assigned. Ordered by ``(variant, valid_from)``; the
    run id increments at each #526 representation boundary AND at every variant
    change (a run never spans variants). States are first deduped by
    ``(state_id, delivery_column_name)`` — a monthly-family annual state expands
    READ-TIME into N per-month windows that SHARE one ``state_id`` but carry
    DISTINCT delivery columns. Those columns are genuinely selectable (the group
    picker enumerates them), so they must SURVIVE the fold; only a true duplicate
    (same state_id AND same column) collapses. The run-id logic below still folds
    them into ONE run (a same-state_id column change is alias multiplexing, not a
    coding boundary) so a family doesn't mint phantom runs."""
    by_key: dict[tuple[int, str | None], VariableState] = {}
    for s in states:
        by_key.setdefault((s.state_id, s.delivery_column_name), s)
    ordered = sorted(by_key.values(), key=lambda s: (s.variant, s.valid_from))

    out: list[GraphState] = []
    run_id = 0
    prev: VariableState | None = None
    for s in ordered:
        if prev is not None and (
            s.variant != prev.variant
            # A pure delivery-column change among windows SHARING a state_id is alias
            # multiplexing (one annual claim delivered as N month-columns), NOT a
            # representation boundary — fold them into one run so the monthly family
            # mints no phantom runs while its columns still survive the dedup above.
            or (s.state_id != prev.state_id and _is_representation_boundary(prev, s))
        ):
            run_id += 1
        out.append(
            GraphState(
                state_id=s.state_id,
                variant=s.variant,
                variant_label=s.variant_label,
                variant_family=s.variant_family,
                variant_family_label=s.variant_family_label,
                representation_run_id=run_id,
                delivery_column_name=s.delivery_column_name,
                value_set_id=s.value_set_id,
                value_set_version_label=s.value_set_version_label,
                classification_slug=s.classification_slug,
                valid_from=None if s.valid_from == UNKNOWN_VALID_FROM else s.valid_from,
                valid_to=None if s.valid_to == OPEN_ENDED_VALID_TO else s.valid_to,
            )
        )
        prev = s
    return out


def _has_meaningful_runs(states: list[GraphState]) -> bool:
    """Whether the variable's states span ≥2 representation runs — the "renders as
    ≥2 cells" signal for a lone variable with no edges/group (a value-set / column
    change with no succession). A single run (one cell) on an otherwise-edgeless,
    ungrouped node is the empty-graph case (the `akters` `int -> bigint` split:
    raw ``data_type`` is not a boundary signal at all → one run → don't render)."""
    return any(s.representation_run_id > 0 for s in states)


# ── Edge dedup helpers ───────────────────────────────────────────────────────


def _succession_edge(
    source: str,
    target: str,
    label: str | None,
    effective_year: int | None = None,
) -> GraphEdge:
    """A DIRECTED succession edge (predecessor → successor). The id encodes the
    direction so two members surfacing the SAME succession edge during group
    expansion dedup by id. ``effective_year`` is the year the source edition was
    superseded by the target (surfaced on the #678 timeline annotation)."""
    return GraphEdge(
        id=f"succession:{source}->{target}",
        kind="succession",
        source=source,
        target=target,
        label=label,
        effective_year=effective_year,
    )


def _representation_succession_edge(edge: RepresentationSuccessionRef) -> GraphEdge:
    """A representation-grain succession rendered as a graph succession edge.

    The graph stays variable-node-grained, so source/target are the endpoint
    variable FQIDs while the optional column/variant fields identify the
    representation endpoints inside those nodes.
    """
    if edge.predecessor_fqid is None or edge.successor_fqid is None:
        msg = "representation succession edge missing a variable endpoint FQID"
        raise ValueError(msg)
    source = str(edge.predecessor_fqid)
    target = str(edge.successor_fqid)
    scope = edge.variant or ""
    return GraphEdge(
        id=(
            "succession:representation:"
            f"{source}:{edge.predecessor_column}:{scope}->"
            f"{target}:{edge.successor_column}:{scope}"
        ),
        kind="succession",
        source=source,
        target=target,
        label=edge.reason,
        effective_year=edge.effective_year,
        source_column=edge.predecessor_column,
        target_column=edge.successor_column,
        variant=edge.variant,
    )


# ── The builder ──────────────────────────────────────────────────────────────


class _GraphBuilder:
    """Accumulates nodes + edges across a member union, deduping both by id. A node
    is built ONCE per canonical id (the first member to reach it wins); edges dedup
    by their stable id (so a shared succession edge surfaced from two ends
    collapses)."""

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._nodes: dict[str, GraphNode] = {}
        self._edges: dict[str, GraphEdge] = {}
        # Node ids that carry a FULLY hydrated variable node (states + same_as +
        # group_key), as opposed to a THIN placeholder minted by `_ensure_edition_node`
        # for a succession-chain edition reached before its own `add_variable`. A LIVE
        # edition reached thin-first (as another node's predecessor/successor) must be
        # UPGRADABLE when later added as the focus or a group member — otherwise it
        # stays thin and violates the variable-node contract (#761). A dead/unresolvable
        # edition that `resolve` rejects never enters this set, so it stays thin.
        self._hydrated: set[str] = set()
        # Classification slugs `add_classification` has WALKED a chain FROM (been the
        # anchor of). The early-out gates on THIS — not node-presence — because at a
        # #579 SPLIT, `classification_chain(leaf)` returns only that leaf's linear path
        # (root spine + its own subtree), NOT sibling branches. So a split predecessor P
        # whose node already exists (minted as a descendant D's ancestor) still needs
        # its OWN walk to surface P's other branches. Keying on anchor-walked walks each
        # distinct member-anchor once (correct for splits) while still deduping repeat
        # references to the same slug; the node/edge `setdefault` absorbs spine overlap.
        self._class_anchors_walked: set[str] = set()
        # Classification nodes already carrying their umbrella `class/<key>` group_key.
        # A curated member can be built FIRST by a non-member-anchored walk (a #579
        # split where a member is also another member's ancestor) → built ungrouped,
        # then upgraded once when its own (member-anchored) walk reaches it. Keying on
        # this makes the upgrade idempotent (no repeated model_copy).
        self._class_grouped: set[str] = set()
        # Representation succession can legally contain variant-scoped round trips
        # (#846), so the graph walk must be edge-key guarded rather than assuming a
        # topological terminal successor exists.
        self._representation_anchors_walked: set[str] = set()
        self._representation_edges_walked: set[tuple[str, str, str, str, str]] = set()
        # Concept-group cache, keyed by the member's `(provider, register, key)`
        # triple — the group's address (#616). A variable node's facets/group_label
        # come from its canonical group's member list, so a grouped variable needs
        # ONE `concept_group` fetch per DISTINCT group, not one per node (every
        # member of one group shares the same fetch). Honors graph.py's "compose,
        # don't re-query" — the SQL is `Catalog`'s, memoized here. `None` is a
        # cached miss (a stale `group` ref with no live group).
        self._group_cache: dict[tuple[str, str, str], ConceptGroupSummary | None] = {}

    def _concept_group(
        self, provider: str, register: str, key: str
    ) -> ConceptGroupSummary | None:
        """The cached `concept_group(provider, register, key)` — one fetch per
        distinct group across the whole build (memoized), `None` for a missing
        group. Compose, don't re-query."""
        cache_key = (provider, register, key)
        if cache_key not in self._group_cache:
            self._group_cache[cache_key] = self._catalog.concept_group(
                provider, register, key
            )
        return self._group_cache[cache_key]

    def add_variable(self, fqid: Fqid) -> str | None:
        """Build the variable node for ``fqid`` (resolving same_as to canonical) and
        its succession edges, recursing into succession-chain members so the union
        is complete. Returns the CANONICAL node id (the resolved variable's own
        identity, not the caller's same_as alias), or None when the FQID resolves
        to no live variable."""
        try:
            resolved = self._catalog.resolve(fqid)
        except RegMetaError as exc:
            # A dead/not-found member (e.g. a renamed slug surfaced by a chain walk)
            # is skipped from the union, not fatal to the whole graph. A genuinely
            # malformed FQID (any other RegMetaError) fails fast — it must not vanish
            # silently from the union.
            if exc.exit_code == EXIT_NOT_FOUND:
                return None
            raise
        if not isinstance(resolved, ResolvedVariable):
            return None
        node_id = str(resolved.canonical_fqid)
        # Build/hydrate the node exactly once, but UPGRADE a thin placeholder: a node
        # may already exist as a thin `_ensure_edition_node` stub (reached first via a
        # chain walk). Resolving here proves it is LIVE, so replace the stub with the
        # full node. Walk the succession chain only on first build (it is idempotent
        # via edge/node `setdefault`, but re-walking is wasted SQL).
        first_build = node_id not in self._nodes
        if node_id not in self._hydrated:
            self._nodes[node_id] = self._variable_node(resolved)
            self._hydrated.add(node_id)
        if first_build:
            self._add_succession(resolved.canonical_fqid)
        self._add_representation_succession_once(node_id, resolved.canonical_fqid)
        return node_id

    def _variable_node(self, resolved: ResolvedVariable) -> VariableGraphNode:
        # Concept-group keys are only register-unique, so namespace by
        # provider/register to make `group_key` globally unique — a graph spanning >1
        # register (cross-register succession) must not cluster two unrelated
        # same-keyed groups together. Still STABLE across members of one group (all
        # share provider/register/key), so the renderer clusters a group correctly.
        group_key = (
            f"{resolved.group.provider}/{resolved.group.register_name}/{resolved.group.key}"
            if resolved.group is not None
            else None
        )
        facets, group_label = self._group_facets(resolved)
        node_id = str(resolved.canonical_fqid)
        return VariableGraphNode(
            id=node_id,
            fqid=resolved.canonical_fqid,
            label=resolved.name or node_id,
            group_key=group_key,
            definition=resolved.definition,
            description=resolved.description,
            operational_definition=resolved.operational_definition,
            states=_graph_states(resolved.states),
            same_as=[
                SameAsRef(fqid=ref.fqid, register=ref.register_name)
                for ref in resolved.same_as
                if ref.fqid is not None
            ],
            facets=facets,
            group_label=group_label,
        )

    def _group_facets(
        self, resolved: ResolvedVariable
    ) -> tuple[list[GroupFacet], str | None]:
        """The resolved variable's facets within its canonical concept group plus
        that group's display label (#670 header identity). Empty/`None` when the
        variable is ungrouped, or when the group/member can't be located — a stale
        `group` ref or member skew degrades gracefully, never crashes. The group is
        fetched through the builder's memoized `_concept_group` (one fetch per
        distinct group), and members are matched on `canonical_fqid` — the node's
        identity, which the group member's binding FQID equals.

        Post-#819 a single variable can be carried as SEVERAL members of one group
        (one per `delivery_column` — the iot disposable-income family), each with
        its own facets. These per-`delivery_column` representations are MUTUALLY
        EXCLUSIVE (an inclusive vs. an exclusive variant of the same concept), so
        their facets must NOT be unioned onto the one variable-grain node — that
        produced an incoherent qualifier mixing both variants in the #678 leaf
        header (P2). The node is variable-grain, so it carries ONE REPRESENTATIVE
        member's facets: the first matching member in the group's member order
        (deterministic — members are ordered by first facet value, then slug). The
        per-representation split is the renderer's job once representations are
        first-class (#757); until then a single coherent member is the honest
        variable-grain identity."""
        if resolved.group is None:
            return [], None
        group = self._concept_group(
            resolved.group.provider, resolved.group.register_name, resolved.group.key
        )
        if group is None:
            return [], None
        # First matching member only: a representative, never the union of
        # mutually-exclusive representation-member facets (#678 P2).
        representative = next(
            (m for m in group.members if m.fqid == resolved.canonical_fqid), None
        )
        if representative is None:
            return [], None
        return list(representative.facets), group.label

    def _add_succession(self, fqid: Fqid) -> None:
        """Walk the variable succession chain (``variable_chain`` — the single
        source of truth) and add a directed edge between each adjacent edition,
        ensuring every edition has a node. The chain is oldest → terminal."""
        chain = self._catalog.variable_chain(fqid)
        if len(chain) < 2:
            return
        for prev, cur in itertools.pairwise(chain):
            prev_id = self._ensure_edition_node(prev)
            cur_id = self._ensure_edition_node(cur)
            if prev_id is None or cur_id is None:
                continue
            edge = _succession_edge(prev_id, cur_id, prev.reason, prev.effective_year)
            self._edges.setdefault(edge.id, edge)

    def _add_representation_succession(self, fqid: Fqid) -> None:
        """Add representation-grain succession edges touching ``fqid``.

        Unlike variable-grain ``variable_chain``, representation succession may be
        a permitted variant-scoped round trip. Walk by explicit edge identity and
        never by "terminal successor" so a column returning later cannot loop.
        """
        for edge in self._catalog.representation_successions(fqid):
            if edge.predecessor_fqid is None or edge.successor_fqid is None:
                continue
            edge_key = (
                str(edge.predecessor_fqid),
                edge.predecessor_column,
                str(edge.successor_fqid),
                edge.successor_column,
                edge.variant or "",
            )
            if edge_key in self._representation_edges_walked:
                continue
            self._representation_edges_walked.add(edge_key)
            source_id = self.add_variable(edge.predecessor_fqid)
            target_id = self.add_variable(edge.successor_fqid)
            if source_id is None or target_id is None:
                continue
            graph_edge = _representation_succession_edge(edge)
            self._edges.setdefault(
                graph_edge.id,
                graph_edge,
            )

    def _add_representation_succession_once(self, node_id: str, fqid: Fqid) -> None:
        if node_id in self._representation_anchors_walked:
            return
        self._representation_anchors_walked.add(node_id)
        self._add_representation_succession(fqid)

    def _ensure_edition_node(self, edition: VariableEdition) -> str | None:
        """A succession-chain edition's node id, building a node for it if absent.

        A LIVE edition (one that ``resolve`` accepts) gets the SAME full node
        ``add_variable`` would build — states + same_as + group_key — no matter that
        the succession walk is what first reached it: the #761 contract says every
        live variable node carries its full ``variable_state`` history (the #678
        timeline renders each node's states as cells, so a stateless live node would
        render empty). It is marked ``_hydrated`` so a later focus/group-member
        arrival skips the rebuild.

        A DEAD/renamed predecessor (no live row, #355/#411) instead gets a THIN
        placeholder (no states/edges): it still carries a binding ``fqid`` so a
        citation 301-redirects and the chain has a node + label, but it is not live to
        resolve (``name is None`` per ``VariableEdition``; ``resolve`` raises
        ``fqid_not_found``). It never enters ``_hydrated``, so it stays thin.

        This only BUILDS the node — it never recurses into ``_add_succession``; the
        succession EDGES are still added by the caller's ``_add_succession`` loop,
        and group expansion stays seeded by focus/members. ``fqid`` is None only on a
        malformed triple — then it can't be a node."""
        if edition.fqid is None:
            return None
        node_id = str(edition.fqid)
        # Already fully hydrated (via add_variable or a prior live walk) — done.
        if node_id in self._hydrated:
            return node_id
        try:
            resolved = self._catalog.resolve(edition.fqid)
        except RegMetaError as exc:
            # A dead/renamed predecessor (not_found) keeps its thin placeholder; any
            # other RegMetaError is a real fault and must not vanish silently.
            if exc.exit_code != EXIT_NOT_FOUND:
                raise
            resolved = None
        if isinstance(resolved, ResolvedVariable):
            # Live edition: full node.
            self._nodes[node_id] = self._variable_node(resolved)
            self._hydrated.add(node_id)
            self._add_representation_succession_once(node_id, resolved.canonical_fqid)
            return node_id
        # Dead/renamed (or non-variable resolution): thin placeholder, built once.
        self._nodes.setdefault(
            node_id,
            VariableGraphNode(
                id=node_id,
                fqid=edition.fqid,
                label=edition.name or node_id,
                group_key=None,
                definition=None,
                description=None,
                operational_definition=None,
                states=[],
                same_as=[],
                facets=[],
                group_label=None,
            ),
        )
        return node_id

    def add_classification(
        self,
        slug: str,
        *,
        group_key: str | None = None,
        group_label: str | None = None,
        member_slugs: frozenset[str] = frozenset(),
    ) -> str | None:
        """Build the classification-edition nodes + succession edges for the chain
        the edition ``slug`` sits on (``classification_chain`` — the single source
        of truth, branch-aware at #579 splits). Returns the queried (self) edition's
        node id, or None when it doesn't resolve.

        ``group_key`` / ``group_label`` / ``member_slugs`` carry the
        classification-umbrella membership (``class/<key>``): a node whose slug ∈
        ``member_slugs`` is a CURATED umbrella member and carries ``group_key`` (shared
        clustering, mirroring the variable side's own-membership rule) plus
        ``group_label`` (the umbrella's display heading — #794 P3); editions surfaced
        by the chain walk that are NOT curated members (a shared ancestor pulled in by
        the #579 split walk) keep ``group_key=None`` and no label. Nodes are frozen, so
        the key/label are applied at build time (not mutated post-build); a member node
        first built by a non-member-anchored walk is upgraded once via
        ``_class_grouped``.

        Early-out: gated on whether THIS slug has already ANCHORED a walk, NOT on
        node-presence. A SUN-style umbrella's niva/inriktning/grupp members share one
        chain, so re-walking a previously-anchored slug is wasted SQL. But at a #579
        SPLIT, `classification_chain(leaf)` returns only that leaf's linear path, so a
        split predecessor P whose node already exists (as a descendant's ancestor) must
        STILL walk its own chain to surface its other branches — a node-presence early-
        out would wrongly skip them. The chain walk is idempotent under node/edge
        `setdefault`, so re-anchoring a shared spine just dedups."""
        self_fqid = Fqid.classification_fqid(slug)
        self_node_id = str(self_fqid)
        if slug in self._class_anchors_walked:
            # Re-anchor: the chain SQL is skipped (idempotent), but a member arriving
            # AFTER its node was anchored ungrouped — the focus add in
            # `graph_for_classification_fqid` walks the focus's chain WITHOUT the
            # umbrella key before the member union does — must still get its own
            # umbrella `group_key`. Mirrors the in-loop `_class_grouped` upgrade.
            self._apply_class_group_key(self_node_id, group_key, group_label)
            return self_node_id
        self._class_anchors_walked.add(slug)
        try:
            chain = self._catalog.classification_chain(self_fqid)
        except RegMetaError:
            return None
        if not chain:
            return None
        self_id: str | None = None
        slug_to_id: dict[str, str] = {}
        for edition in chain:
            # A chain edition's fqid is None only on a malformed slug (build-
            # prevented; mirrors `_ensure_edition_node`) — it can't be a node.
            if edition.fqid is None:
                continue
            node_id = str(edition.fqid)
            slug_to_id[edition.slug] = node_id
            is_member = edition.slug in member_slugs
            if node_id not in self._nodes:
                self._nodes[node_id] = ClassificationGraphNode(
                    id=node_id,
                    fqid=edition.fqid,
                    label=edition.name or edition.short_name or node_id,
                    short_name=edition.short_name,
                    group_key=group_key if is_member else None,
                    group_label=group_label if is_member else None,
                    version_year=edition.version_year,
                    is_current=edition.is_current,
                )
                if is_member and group_key is not None:
                    self._class_grouped.add(node_id)
            elif is_member:
                # A curated member first built by a non-member-anchored walk (frozen
                # node) — upgrade once with its umbrella key + heading.
                self._apply_class_group_key(node_id, group_key, group_label)
            if edition.is_self:
                self_id = node_id
        self._add_classification_edges(slug_to_id)
        return self_id

    def _apply_class_group_key(
        self, node_id: str, group_key: str | None, group_label: str | None = None
    ) -> None:
        """Stamp a built classification node with its umbrella ``group_key`` (and
        display ``group_label`` heading — #794 P3) once. Idempotent via
        ``_class_grouped``: a member node built ungrouped first (as a non-member-
        anchored walk's ancestor, or as the focus's own pre-union chain walk) is
        upgraded exactly once; a no-key call or a re-stamp is a no-op. Nodes are
        frozen, so this is a ``model_copy`` replace, not a mutation."""
        if (
            group_key is None
            or node_id in self._class_grouped
            or node_id not in self._nodes
        ):
            return
        self._nodes[node_id] = self._nodes[node_id].model_copy(
            update={"group_key": group_key, "group_label": group_label}
        )
        self._class_grouped.add(node_id)

    def _add_classification_edges(self, slug_to_id: dict[str, str]) -> None:
        """Add the directed succession edges among the chain editions, read off the
        authoritative ``classification_predecessors`` (each edition's inbound edge).
        Reading the edges (not pairing the flat chain list) is correct at a #579
        split: the flat ``classification_chain`` interleaves branches, so adjacent
        list entries are not always a real edge — but each edition's predecessor
        IS."""
        # Read each chain edition's inbound predecessors on EVERY walk — no memo on
        # the successor slug. A succession edge needs BOTH endpoints in THIS walk's
        # `slug_to_id`; at a classification MERGE (a successor C with predecessors A
        # and B on different branches), the walk that reaches C via A has A+C but not
        # B, so it can only add A→C — B→C is added by B's own later walk. Memoizing
        # on the successor slug would mark C read after the A-walk and drop B→C
        # forever. `_edges.setdefault` already dedups shared spine edges, so the
        # repeat reads are free of duplicates; the perf cost is marginal indexed
        # lookups, not worth the merge-shape correctness risk.
        for slug, node_id in slug_to_id.items():
            for pred in self._catalog.classification_predecessors(
                Fqid.classification_fqid(slug)
            ):
                pred_id = slug_to_id.get(pred.slug)
                if pred_id is None:
                    continue
                edge = _succession_edge(
                    pred_id, node_id, pred.note, pred.effective_year
                )
                self._edges.setdefault(edge.id, edge)

    def build(self, focus_id: str | None) -> RelationshipGraph:
        """Assemble the graph. EMPTY-graph gate: a single ungrouped, edgeless node
        whose states carry no meaningful representation run renders nothing
        (``nodes: []``). A lone variable WITH a value-set/column change (≥2 runs)
        keeps its node — it renders as ≥2 cells."""
        if len(self._nodes) == 1 and not self._edges:
            (only,) = self._nodes.values()
            if _is_empty_solo(only):
                return RelationshipGraph(nodes=[], edges=[], focus_id=None)
        return RelationshipGraph(
            nodes=list(self._nodes.values()),
            edges=list(self._edges.values()),
            focus_id=focus_id,
        )


def _is_empty_solo(node: GraphNode) -> bool:
    """Whether the graph's ONLY node, with no edges, should NOT render. A lone
    classification edition (no chain) never renders — even when it carries an
    umbrella ``group_key`` (a group of one has nobody to cluster WITH, so the key is
    degenerate on a solo node). A variable renders only when it spans ≥2
    representation runs AND is ungrouped — a grouped variable keeps its node (a
    one-member variable group still renders that member as its group view)."""
    if isinstance(node, ClassificationGraphNode):
        return True
    return node.group_key is None and not _has_meaningful_runs(node.states)


# ── The two accessors (delegated to by Catalog) ──────────────────────────────


def graph_for_fqid(catalog: Catalog, resolved: ResolvedVariable) -> RelationshipGraph:
    """The variable subject graph for an already-resolved binding (the ``Catalog``
    method does the binding parse + resolve so a non-binding / dead FQID raises the
    standard ``not_a_binding_fqid`` / ``fqid_not_found`` like the sibling
    accessors). Root set = the resolved variable's ``.group`` members (or itself
    when ungrouped) → union → ``focus_id`` is the resolved node. A member page
    renders the SAME group union as the group page (Fork B), with the current node
    highlighted client-side via ``focus_id``."""
    builder = _GraphBuilder(catalog)
    focus_id = builder.add_variable(resolved.fqid)
    # Build the focus node first; it fetches its own concept group once (via
    # `_group_facets` → the memoized `_concept_group`). The member union reads the
    # same group through the builder's memo, so it hits that cache — the group is
    # fetched exactly once, with no separate priming contract.
    if resolved.group is not None:
        group = builder._concept_group(
            resolved.group.provider, resolved.group.register_name, resolved.group.key
        )
        if group is not None:
            _add_group_members(builder, group)
    return builder.build(focus_id)


def graph_for_concept_group(
    catalog: Catalog, provider: str, register: str, key: str
) -> RelationshipGraph | None:
    """The register concept group's union graph, keyed by ``(provider, register,
    key)`` (group keys are NOT FQIDs). ``focus_id = None``. Resolved via the
    existing ``concept_group`` accessor. None when the group doesn't exist (the
    webapp maps it to 404)."""
    group = catalog.concept_group(provider, register, key)
    if group is None:
        return None
    builder = _GraphBuilder(catalog)
    _add_group_members(builder, group)
    return builder.build(focus_id=None)


def graph_for_classification_group(
    catalog: Catalog, key: str
) -> RelationshipGraph | None:
    """The classification subject graph addressed by ``key``.

    Curated umbrellas resolve via ``classification_group``; derived
    one-dimensional succession families resolve via ``classification_family``. In
    both cases each member/edition is a ``class/<slug>`` node and the union is its
    succession chain + co-members. None when neither subject exists (the webapp maps
    it to 404).
    """
    group = catalog.classification_group(key)
    if group is not None:
        builder = _GraphBuilder(catalog)
        _add_classification_group_members(builder, group)
        return builder.build(focus_id=None)
    family = catalog.classification_family(key)
    if family is None:
        return None
    builder = _GraphBuilder(catalog)
    _add_classification_family_editions(builder, family)
    return builder.build(focus_id=None)


def graph_for_classification_fqid(
    catalog: Catalog,
    canonical_slug: str,
    groups: list[ConceptGroupSummary],
) -> RelationshipGraph:
    """The classification-leaf subject graph (#792) — the classification analog of
    ``graph_for_fqid``. The ``Catalog`` method resolves the FQID to its canonical
    live edition (raising the standard ``not_a_classification_fqid`` /
    ``fqid_not_found`` like the sibling classification accessors, the webapp's 4xx
    path) and hands in the canonical slug + the edition's curated umbrella group(s),
    mirroring how ``graph_for_fqid`` receives an already-resolved ``ResolvedVariable``
    (the private resolvers stay in ``catalog.py``).

    Root set (Fork B): when grouped, every umbrella group is unioned exactly as
    ``graph_for_classification_group`` does (each member's full edition chain +
    succession edges, deduped), so the leaf renders the SAME umbrella union as the
    group page (the niva↔aggregate cross-reference #678 retires
    ``ClassificationDimensionsPanel`` for). When ungrouped (the common case), just
    the canonical edition's OWN succession chain (``ClassificationLineagePanels``).
    ``focus_id`` is the canonical edition. The empty-graph gate makes a lone edition
    with no chain and no group render nothing (parity with today's panels)."""
    builder = _GraphBuilder(catalog)
    # Add the focus edition's own chain FIRST (mirroring how `graph_for_fqid` adds
    # the focus binding before unioning group members), so `focus_id` always points
    # to a node that exists — even when the canonical edition references the umbrella
    # but isn't itself a walked member of any group (a curation skew / #579 spine
    # edition). `add_classification` is idempotent (`_class_anchors_walked` +
    # node/edge `setdefault`), so when the focus IS a walked member this adds no real
    # work; the member-anchored walk just re-applies the umbrella `group_key`.
    focus_id = builder.add_classification(canonical_slug)
    for group in groups:
        _add_classification_group_members(builder, group)
    return builder.build(focus_id)


def _add_classification_group_members(
    builder: _GraphBuilder, group: ConceptGroupSummary
) -> None:
    """Union every member of a classification umbrella group into the builder. Each
    member is a ``class/<slug>`` edition; ``add_classification`` walks its full
    succession chain + co-members and dedups shared nodes/edges. Members carry the
    shared ``class/<key>`` ``group_key`` (clustering) AND the group's display
    ``label`` as ``group_label`` (the umbrella heading — #794 P3) so the renderer
    clusters the umbrella under a real title; non-member spine editions surfaced by a
    #579 split walk stay ungrouped + headless. Shared
    by ``graph_for_classification_group`` (group page) and
    ``graph_for_classification_fqid`` (leaf, Fork B)."""
    group_key = f"class/{group.key}"
    member_slugs = frozenset(
        member.fqid.classification
        for member in group.members
        if member.fqid is not None and member.fqid.classification is not None
    )
    for member in group.members:
        if member.fqid is None or member.fqid.classification is None:
            continue
        builder.add_classification(
            member.fqid.classification,
            group_key=group_key,
            group_label=group.label,
            member_slugs=member_slugs,
        )


def _add_classification_family_editions(
    builder: _GraphBuilder, family: ClassificationFamilySummary
) -> None:
    """Union every edition of a derived one-dimensional classification family.

    Family editions are not curated concept-group members, but the graph renderer
    uses the same ``group_key`` / ``group_label`` cluster metadata to title the
    edition chain consistently with umbrella groups.
    """
    group_key = f"class/{family.key}"
    member_slugs = frozenset(
        edition.fqid.classification
        for edition in family.editions
        if edition.fqid is not None and edition.fqid.classification is not None
    )
    for edition in family.editions:
        if edition.fqid is None or edition.fqid.classification is None:
            continue
        builder.add_classification(
            edition.fqid.classification,
            group_key=group_key,
            group_label=family.label,
            member_slugs=member_slugs,
        )


def _add_group_members(builder: _GraphBuilder, group: ConceptGroupSummary) -> None:
    """Union every member of a register concept group into the builder. Members are
    variable bindings; ``add_variable`` recurses into each member's succession
    chain and dedups shared nodes/edges."""
    for member in group.members:
        if member.fqid is not None:
            builder.add_variable(member.fqid)
