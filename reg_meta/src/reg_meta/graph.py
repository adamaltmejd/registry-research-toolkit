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
for succession, ``related`` for see-also, ``resolve``'s ``ResolvedVariable.group``
+ ``concept_group`` for variable-group membership, ``classification_group`` for the
classification umbrella, ``classification_chain`` for classification editions,
``resolve`` for same_as canonicalization). The only genuinely new logic is group
expansion, edition dedup, and the representation-run computation — no fresh SQL
re-deriving succession / lineage (that would fork the edge logic; the repo's
leaf-duplication trap).

**Two edge kinds only: ``succession`` + ``related``.** Everything else is
metadata / affordance: ``lineage`` and ``source_register`` are #678's provenance
affordance (not edges); group membership is shared ``group_key`` metadata +
renderer clustering (no ``group:<key>`` node); ``same_as`` is resolved away to the
canonical node and exposed as node metadata (``same_as[]``); representation /
value-set transitions are states-within-a-node (the run ids), not edges.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import Field

from .catalog import OPEN_ENDED_VALID_TO, ResolvedVariable, _CatalogModel
from .errors import EXIT_NOT_FOUND, RegMetaError
from .fqid import Fqid

if TYPE_CHECKING:
    from .catalog import (
        Catalog,
        ConceptGroupSummary,
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
    representation_run_id: int
    delivery_column_name: str | None
    value_set_id: int | None
    value_set_version_label: str
    classification_slug: str | None
    # ISO 'YYYY-MM-DD'; None = unknown/open start. The open-ended `9999-12-31`
    # sentinel is normalized to None here (an open END), so the renderer's time
    # axis reads "ongoing" rather than a year-9999 tick.
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
    Nodes are variables — not states — because succession / related / same_as /
    group are all variable-grain and the FQID must map to exactly one node."""

    kind: Literal["variable"] = "variable"
    states: list[GraphState]
    same_as: list[SameAsRef]


class ClassificationGraphNode(_GraphNodeBase):
    """A classification-edition node: time is a POINT (``version_year``), never an
    interval — an edition is not "dead" after its successor. ``is_current`` marks a
    terminal (head) edition."""

    kind: Literal["classification"] = "classification"
    version_year: int | None
    is_current: bool


GraphNode = Annotated[
    VariableGraphNode | ClassificationGraphNode, Field(discriminator="kind")
]


class GraphEdge(_CatalogModel):
    """A graph edge. ``id`` is stable and doubles as the dedup key. ``succession``
    is DIRECTED (predecessor → successor); ``related`` is UNDIRECTED — its
    endpoints are canonicalized (sorted by node id) so the same relation seen from
    both ends during group expansion collapses to one edge. ``label`` is the
    succession reason / the related ``relation_kind``."""

    id: str
    kind: Literal["succession", "related"]
    source: str  # node id (canonicalized order for related)
    target: str  # node id
    label: str | None


class RelationshipGraph(_CatalogModel):
    """The relationship graph for a subject. ``nodes: []`` is the "don't render"
    signal (the frontend gate is ``nodes.length === 0``): a lone variable with no
    succession / related / group siblings / meaningful representation boundary, or
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
    change (a run never spans variants). States are first deduped by ``state_id``
    (a monthly-family annual state expands READ-TIME into N per-month windows
    sharing one ``state_id`` — alias multiplexing, not a coding boundary; we fold
    those back to the single claim so a family doesn't mint phantom runs)."""
    by_state: dict[int, VariableState] = {}
    for s in states:
        by_state.setdefault(s.state_id, s)
    ordered = sorted(by_state.values(), key=lambda s: (s.variant, s.valid_from))

    out: list[GraphState] = []
    run_id = 0
    prev: VariableState | None = None
    for s in ordered:
        if prev is not None and (
            s.variant != prev.variant or _is_representation_boundary(prev, s)
        ):
            run_id += 1
        out.append(
            GraphState(
                state_id=s.state_id,
                variant=s.variant,
                representation_run_id=run_id,
                delivery_column_name=s.delivery_column_name,
                value_set_id=s.value_set_id,
                value_set_version_label=s.value_set_version_label,
                classification_slug=s.classification_slug,
                valid_from=s.valid_from,
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


def _succession_edge(source: str, target: str, label: str | None) -> GraphEdge:
    """A DIRECTED succession edge (predecessor → successor). The id encodes the
    direction so two members surfacing the SAME succession edge during group
    expansion dedup by id."""
    return GraphEdge(
        id=f"succession:{source}->{target}",
        kind="succession",
        source=source,
        target=target,
        label=label,
    )


def _related_edge(a: str, b: str, label: str | None) -> GraphEdge:
    """An UNDIRECTED related edge. Endpoints are canonicalized (sorted by node id)
    so the same relation seen from both ends — `a→b` and `b→a`, both stored in
    `variable_related_to` — collapses to one edge by id."""
    lo, hi = sorted((a, b))
    return GraphEdge(
        id=f"related:{lo}--{hi}", kind="related", source=lo, target=hi, label=label
    )


# ── The builder ──────────────────────────────────────────────────────────────


class _GraphBuilder:
    """Accumulates nodes + edges across a member union, deduping both by id. A node
    is built ONCE per canonical id (the first member to reach it wins); edges dedup
    by their stable id (so a shared succession edge or an undirected related edge
    surfaced from two ends collapses)."""

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._nodes: dict[str, GraphNode] = {}
        self._edges: dict[str, GraphEdge] = {}
        # Node-build dedup and related-expansion are decoupled: a node first reached
        # as another node's one-hop related neighbor (follow_related=False) builds the
        # node but does NOT expand its related edges, so a later focus/group-member
        # arrival (follow_related=True) must still be able to COMPLETE that expansion
        # on the already-built node — otherwise a member's one-hop neighbors would be
        # dropped purely by traversal order (#761).
        self._related_expanded: set[str] = set()

    def add_variable(self, fqid: Fqid, *, follow_related: bool = True) -> str | None:
        """Build the variable node for ``fqid`` (resolving same_as to canonical) and
        all its variable-grain edges (succession + related), recursing into
        succession-chain members and related neighbors so the union is complete.
        Returns the CANONICAL node id (the resolved variable's own identity, not the
        caller's same_as alias), or None when the FQID resolves to no live variable.

        ``follow_related=False`` adds the node + its succession chain but does NOT
        chase its related edges — so a related neighbor is one hop, never the
        transitive closure of related-of-related (#761 scopes the union to the
        subject/group + their succession chains + related edges among/from the
        union, not related-of-related). Only the focus + group members seed related
        expansion (``follow_related=True``)."""
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
        # Build the node (+ its succession chain) exactly once.
        if node_id not in self._nodes:
            self._nodes[node_id] = self._variable_node(resolved)
            self._add_succession(resolved.canonical_fqid)
        # Related expansion is gated separately: a follow_related=True arrival
        # completes the one-hop neighbor walk even on an already-built node, but
        # only once (no transitive closure — neighbors are added with
        # follow_related=False and stay un-expanded so a later True arrival can
        # still expand them).
        if follow_related and node_id not in self._related_expanded:
            self._related_expanded.add(node_id)
            self._add_related(resolved.canonical_fqid)
        return node_id

    def _variable_node(self, resolved: ResolvedVariable) -> VariableGraphNode:
        group_key = resolved.group.key if resolved.group is not None else None
        node_id = str(resolved.canonical_fqid)
        return VariableGraphNode(
            id=node_id,
            fqid=resolved.canonical_fqid,
            label=resolved.name or node_id,
            group_key=group_key,
            states=_graph_states(resolved.states),
            same_as=[
                SameAsRef(fqid=ref.fqid, register=ref.register_name)
                for ref in resolved.same_as
                if ref.fqid is not None
            ],
        )

    def _add_succession(self, fqid: Fqid) -> None:
        """Walk the variable succession chain (``variable_chain`` — the single
        source of truth) and add a directed edge between each adjacent edition,
        ensuring every edition has a node. The chain is oldest → terminal."""
        chain = self._catalog.variable_chain(fqid)
        if len(chain) < 2:
            return
        for prev, cur in zip(chain, chain[1:]):
            prev_id = self._ensure_edition_node(prev)
            cur_id = self._ensure_edition_node(cur)
            if prev_id is None or cur_id is None:
                continue
            edge = _succession_edge(prev_id, cur_id, prev.reason)
            self._edges.setdefault(edge.id, edge)

    def _ensure_edition_node(self, edition: VariableEdition) -> str | None:
        """A succession-chain edition's node id, building a node for it if absent. A
        chain edition may be a DEAD/renamed predecessor (no live row, #355/#411): it
        still carries a binding ``fqid`` (301-redirects) so it IS a node, just a
        thin one (no states/edges — it's not live to resolve). The ``is_self``
        edition is the queried variable, already a full node from ``add_variable``.
        ``fqid`` is None only on a malformed triple — then it can't be a node."""
        if edition.fqid is None:
            return None
        node_id = str(edition.fqid)
        if node_id not in self._nodes:
            self._nodes[node_id] = VariableGraphNode(
                id=node_id,
                fqid=edition.fqid,
                label=edition.name or node_id,
                group_key=None,
                states=[],
                same_as=[],
            )
        return node_id

    def _add_related(self, fqid: Fqid) -> None:
        """Add undirected related edges for ``fqid`` (``related`` — the single
        source of truth). ``fqid`` is the CANONICAL fqid (the caller passes
        ``resolved.canonical_fqid``), so ``source_id`` matches the node id. Each
        neighbor becomes a node WITH its own succession chain, but with
        ``follow_related=False`` so the union does NOT chase the neighbor's OWN
        related edges — the union is one related hop, not the transitive closure
        (#761). ``variable_related_to`` stores both directions, but the
        canonicalized edge id collapses the pair to one."""
        source_id = str(fqid)
        for ref in self._catalog.related(fqid):
            if ref.fqid is None:
                continue
            target_id = self.add_variable(ref.fqid, follow_related=False)
            if target_id is None:
                continue
            edge = _related_edge(source_id, target_id, ref.relation_kind)
            self._edges.setdefault(edge.id, edge)

    def add_classification(self, slug: str) -> str | None:
        """Build the classification-edition nodes + succession edges for the chain
        the edition ``slug`` sits on (``classification_chain`` — the single source
        of truth, branch-aware at #579 splits). Returns the queried (self) edition's
        node id, or None when it doesn't resolve.

        Early-out: if the queried edition's node is already present, the whole chain
        was walked by a prior member (a SUN-style umbrella's niva/inriktning/grupp
        members share one chain), so we skip re-walking ``classification_chain`` +
        the predecessor SQL — the same node-dedup short-circuit ``add_variable``
        has."""
        self_fqid = Fqid.classification_fqid(slug)
        self_node_id = str(self_fqid)
        if self_node_id in self._nodes:
            return self_node_id
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
            if node_id not in self._nodes:
                self._nodes[node_id] = ClassificationGraphNode(
                    id=node_id,
                    fqid=edition.fqid,
                    label=edition.name or node_id,
                    group_key=None,
                    version_year=edition.version_year,
                    is_current=edition.is_current,
                )
            if edition.is_self:
                self_id = node_id
        self._add_classification_edges(slug_to_id)
        return self_id

    def _add_classification_edges(self, slug_to_id: dict[str, str]) -> None:
        """Add the directed succession edges among the chain editions, read off the
        authoritative ``classification_predecessors`` (each edition's inbound edge).
        Reading the edges (not pairing the flat chain list) is correct at a #579
        split: the flat ``classification_chain`` interleaves branches, so adjacent
        list entries are not always a real edge — but each edition's predecessor
        IS."""
        for slug, node_id in slug_to_id.items():
            for pred in self._catalog.classification_predecessors(
                Fqid.classification_fqid(slug)
            ):
                pred_id = slug_to_id.get(pred.slug)
                if pred_id is None:
                    continue
                edge = _succession_edge(pred_id, node_id, pred.note)
                self._edges.setdefault(edge.id, edge)

    def build(self, focus_id: str | None) -> RelationshipGraph:
        """Assemble the graph. EMPTY-graph gate: a single ungrouped, edgeless node
        whose states carry no meaningful representation run renders nothing
        (``nodes: []``). A lone variable WITH a value-set/column change (≥2 runs)
        keeps its node — it renders as ≥2 cells."""
        if len(self._nodes) == 1 and not self._edges:
            (only,) = self._nodes.values()
            if only.group_key is None and _is_empty_solo(only):
                return RelationshipGraph(nodes=[], edges=[], focus_id=None)
        return RelationshipGraph(
            nodes=list(self._nodes.values()),
            edges=list(self._edges.values()),
            focus_id=focus_id,
        )


def _is_empty_solo(node: GraphNode) -> bool:
    """A solo, edgeless, ungrouped node that should NOT render. A variable renders
    only when it spans ≥2 representation runs; a classification edition alone (no
    chain) never renders."""
    if isinstance(node, ClassificationGraphNode):
        return True
    return not _has_meaningful_runs(node.states)


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

    if resolved.group is not None:
        group = catalog.concept_group(
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
    """The classification umbrella group's union graph, keyed by its derivation
    ``key`` (resolved via the new ``classification_group`` accessor). Each member is
    a ``class/<slug>`` edition; the union is its succession chain + co-members. None
    when the group doesn't exist (the webapp maps it to 404)."""
    group = catalog.classification_group(key)
    if group is None:
        return None
    builder = _GraphBuilder(catalog)
    for member in group.members:
        if member.fqid is None or member.fqid.classification is None:
            continue
        builder.add_classification(member.fqid.classification)
    return builder.build(focus_id=None)


def _add_group_members(builder: _GraphBuilder, group: ConceptGroupSummary) -> None:
    """Union every member of a register concept group into the builder. Members are
    variable bindings; ``add_variable`` recurses into each member's succession +
    related neighbors and dedups shared nodes/edges."""
    for member in group.members:
        if member.fqid is not None:
            builder.add_variable(member.fqid)
