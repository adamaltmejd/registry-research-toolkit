"""`GET /api/catalog` browse — the canonical catalog endpoint.

See DESIGN.md → Catalog router structure. Two routes:

- ``/catalog`` — the root: every provider plus the classification-root sentinel.
- ``/catalog/{fqid:path}`` — the single catch-all covering provider (1 seg) →
  registers, register (2 seg) → bindings + a `variants` reference stub, binding
  leaf (3 seg) → the variable's FULL embedded longitudinal record, and
  classification (`class/<slug>`, 2 seg). The classification-root literal `class`
  (1 seg) is special-cased before parse.

**Connection model = per-request open** (LOCKED). A shared `sqlite3` connection
is not safe across FastAPI's sync-handler threadpool (per-connection cursor
state races), so each handler opens a FRESH read-only connection per request via
the `_catalog_conn` contextmanager — used as a plain `with` INSIDE the sync
handler body, NOT a FastAPI dependency. (A generator *dependency* is entered on a
possibly-different threadpool thread than the handler, so a dependency-opened
connection would be used cross-thread → `sqlite3.ProgrammingError`; see
`_catalog_conn`.) It opens from the boot-resolved `app.state.db_path`, the handler
wraps it in a `Catalog`, and it closes in a `finally`. The connection is owned by
the handling thread (`check_same_thread` default True) — correct. No long-lived
shared connection, no lock, no `check_same_thread=False`. The schema was already
validated at boot (`open_db` in the lifespan), so the per-request open skips the
re-check (`check_schema=False`).

**Path guard runs BEFORE any DB access** (see DESIGN.md → FQID path guard
(catalog_fqid.py)). Every catch-all request first runs
`validate_fqid_path` (the per-segment slug-grammar allow-list, own module
`catalog_fqid.py`) as a dependency; a rejection raises 422 with zero SQL executed
AND zero connection opens, because the guard resolves before the handler body
opens `_catalog_conn`.

**Router ordering (A5.2 seam).** A5.2's suffixed routes (`/states`,
`/predecessors`, ..., `/{provider}/{register}/variants`) MUST be declared ABOVE
the catch-all — Starlette matches in declaration order and the `{fqid:path}`
converter greedy-consumes any suffix. The catch-all MUST stay last.
"""

from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from reg_meta.catalog import (
    Catalog,
    Period,
    RegisterCoverage,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
    TagMembership,
    VariableCoverage,
    VariableState,
)
from reg_meta.errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from reg_meta.fqid import (
    CLASSIFICATION_PREFIX,
    Fqid,
    FqidError,
    FqidKind,
    parse,
    validate_slug,
)
from reg_meta.graph import RelationshipGraph, VariableGraphNode
from reg_meta.queries import list_classifications

from reg_meta import ResolvedClassification
from reg_webapp.catalog_fqid import (
    FqidPathError,
    ValidatedFqidPath,
    validate_fqid_path,
)
from reg_webapp.conn import catalog_conn as _catalog_conn
from reg_webapp.models import (
    BindingChild,
    BindingNode,
    CatalogNode,
    ClassificationFamilyNode,
    ClassificationGroupNode,
    ClassificationGroupSubject,
    ClassificationNode,
    ClassificationRootNode,
    ClassificationRootResponse,
    ConceptGroupNode,
    ConceptGroupNodeMember,
    DimensionsResponse,
    LineageResponse,
    LineageWarningsResponse,
    PredecessorsResponse,
    ProviderNode,
    ProviderResponse,
    RegisterChild,
    RegisterNode,
    RegisterResponse,
    RootResponse,
    StatesResponse,
    SuccessorsResponse,
    VariantsRef,
    VariantsResponse,
)
from reg_webapp.period_param import (
    VALUE_SET_VERSION_NONE,
    PeriodParamError,
    ValueSetVersionParamError,
    VariantParamError,
    parse_period_query,
    parse_value_set_version,
    parse_variant,
)

if TYPE_CHECKING:
    import sqlite3

    from reg_webapp.catalog_index import CatalogIndex

router = APIRouter(prefix="/api")

# `_catalog_conn` (the per-request read-only connection seam) now lives in
# `reg_webapp.conn` so `routes/search.py` shares it without importing this route
# module; imported above under its original local name to keep the call sites
# (`with _catalog_conn(request)`) unchanged.


def _validated_fqid(fqid: str) -> ValidatedFqidPath:
    """The path-guard allow-list as a dependency (see DESIGN.md → FQID path guard
    (catalog_fqid.py)) — FastAPI resolves it before the handler
    body runs, so a malformed / traversal-shaped path returns 422 **before** the
    handler opens any connection (no DB hit at all, not just no SQL). It holds no
    connection itself, so it's safe across the threadpool. Reused by A5.2's
    suffixed routes."""
    try:
        return validate_fqid_path(fqid)
    except FqidPathError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validated_period(period: str | None = None) -> list[Period] | None:
    """``?period`` allow-list as a pre-open dependency (see DESIGN.md → query
    allow-list (period_param.py)) — FastAPI resolves it
    before the handler body, so a malformed period (SQLi / traversal / NUL /
    percent-encoded) returns 422 **before** any connection opens (zero SQL, zero
    opens). Holds no connection, so it's threadpool-safe. Parses to resolve
    SEGMENTS (#340): the #307 comma list form yields one ``Period`` per member,
    a scalar a one-segment list — the handler resolves per segment and unions.
    ``None`` (no query) means "no period filter" — distinct from the parsed
    ``_default`` sentinel, but the catch-all treats an absent ``?period`` as a
    plain (no-period) resolve, not a `resolve_at`. reg_meta's
    ``_period_bounds`` is the SEMANTIC backstop."""
    if period is None:
        return None
    try:
        return parse_period_query(period)
    except PeriodParamError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validated_variant(variant: str | None = None) -> str | None:
    """``?variant`` allow-list as a pre-open dependency. ADMITS ``_default``
    (a real register_variant slug) unlike the path guard. 422s a non-slug
    value before any connection opens (zero SQL, zero opens)."""
    if variant is None:
        return None
    try:
        return parse_variant(variant)
    except VariantParamError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validated_value_set_version(value_set_version: str | None = None) -> str | None:
    """``?value_set_version`` allow-list as a pre-open dependency. This is the
    read-only catalog-browse label filter (NOT a binding pin — the FQID ``@version``
    pin is retired). The value is a FREE-TEXT value-set-version label (matched
    against ``value_set_version_label`` by a Python filter in ``resolve_at``, NOT
    SQL), so the gate is a sanity check (non-empty, length-capped, no control
    chars) — 422s a malformed value before any connection opens."""
    if value_set_version is None:
        return None
    try:
        return parse_value_set_version(value_set_version)
    except ValueSetVersionParamError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validated_member(member: str | None = None) -> str | None:
    """``?member`` (the concept-group #617 focus hint) allow-list as a pre-open
    dependency. The value is a member's leaf SLUG, so it is validated by
    delegating to reg_meta's authoritative `validate_slug` (the same grammar the
    path guard uses) — a malformed value 422s BEFORE any connection opens (zero
    SQL, zero opens). ``None`` (no query) means "no focus hint"."""
    if member is None:
        return None
    try:
        validate_slug(member, "variable")
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return member


# reg_meta's genuine "this FQID resolves to no row" code. The OTHER
# EXIT_NOT_FOUND code, `state_variant_unresolved`, is a build-invariant break on a
# corrupt DB — a server fault (not a client 404) whose message carries internal
# row IDs we must not echo. So only `fqid_not_found` maps to 404; the rest
# re-raise to a generic 500.
_FQID_NOT_FOUND_CODE = "fqid_not_found"


def _is_fqid_not_found(exc: RegMetaError) -> bool:
    """True iff `exc` is reg_meta's genuine "this FQID resolves to no row"
    (`fqid_not_found`) — a dead/renamed slug. The single source of this predicate,
    reused by `_resolves_live`, `_http_404_if_not_found`, and `_redirect_or_4xx` so
    the dead-slug test is spelled ONCE. Any OTHER `EXIT_NOT_FOUND` code (e.g.
    `state_variant_unresolved`) is a corrupt-DB / build-invariant break — a server
    fault, NOT a client 404 — so it is NOT this predicate."""
    return exc.exit_code == EXIT_NOT_FOUND and exc.code == _FQID_NOT_FOUND_CODE


def _http_404_if_not_found(exc: RegMetaError) -> None:
    """Map reg_meta's genuine FQID-not-found to HTTP 404; re-raise anything else
    (a corrupt-DB / build-invariant break) so it surfaces as a generic 500 — its
    message may carry internal IDs, and it's a server fault, not a client 404."""
    if _is_fqid_not_found(exc):
        raise HTTPException(status_code=404, detail=exc.message) from exc
    raise exc


# ── Steward catalog-index filtering (#859) ──────────────────────────────────
# Every handler gates on `request.app.state.catalog_index`: None for the `global`
# deployment (full universe, byte-for-byte unchanged), a `CatalogIndex` for a
# filtered steward (scope to its held holdings). The helpers below keep the
# filtering DRY at the handler boundary — the pure mapper functions
# (`_register_response`, `_concept_group_node`, …) are NOT threaded with the
# index; a thin wrapper narrows their output instead.

_NOT_IN_CATALOG_DETAIL = "not in this steward's catalog"


def _index(request: Request) -> CatalogIndex | None:
    """The boot-time steward `CatalogIndex`, or None for the `global` deployment."""
    return request.app.state.catalog_index


def _filter_states_to_held(
    states: list[VariableState], index: CatalogIndex, fqid: str
) -> list[VariableState]:
    """Narrow a binding's states to the delivery columns the steward holds for
    `fqid` (#859, browse = column-grain faithful). A kept binding has a non-empty
    held-column set; the filter compares each state's RESOLVED `delivery_column_name`
    against it, mirroring the index's `(fqid, resolved column)` admission (#206). The
    `held_columns` set is reused, never re-derived."""
    held = index.held_columns(fqid)
    return [s for s in states if s.delivery_column_name in held]


def _held_graph_node_columns(
    node: VariableGraphNode, index: CatalogIndex, focus_fqid: str | None
) -> frozenset[str | None]:
    """Held columns for a graph node, accepting held same_as aliases as identity."""
    candidate_fqids = {
        str(fqid)
        for fqid in (
            node.fqid,
            *(ref.fqid for ref in node.same_as if ref.fqid is not None),
        )
        if fqid is not None
    }
    if focus_fqid is not None:
        candidate_fqids.add(focus_fqid)
    held: set[str | None] = set()
    for fqid in candidate_fqids:
        held.update(index.held_columns(fqid))
    return frozenset(held)


def _narrow_graph_to_held(
    graph: RelationshipGraph, index: CatalogIndex, focus_fqid: str | None = None
) -> RelationshipGraph:
    """Narrow a relationship graph to the steward's held variable nodes and delivery
    columns (#865). Classification nodes pass through; variable nodes are kept when
    their canonical FQID or a same_as alias is held, their `states` are reduced to held
    delivery columns, and edges whose endpoint node was dropped are removed.

    Per variable node: keep a state iff its resolved `delivery_column_name` is in the
    steward's `held_columns` for that node's canonical FQID or a held same_as alias
    (the same `(fqid, resolved column)` admission, #206). A variable node emptied of all
    states is DROPPED. Same-as metadata is narrowed with the same held-FQID rule as leaf
    refs, so a kept node does not retain an unheld alias chip. Only invoked for a
    FILTERED steward (`index is not None`); the `global` deployment never calls it, so
    its graph is untouched. The
    `representation_run_id`s on surviving states may skip values (a dropped run leaves a
    gap) — harmless: the run id groups ADJACENT surviving states into render cells; it
    is not a dense sequence the consumers rely on."""
    kept_nodes: list = []
    kept_ids: set[str] = set()
    for node in graph.nodes:
        if not isinstance(node, VariableGraphNode) or node.fqid is None:
            kept_nodes.append(node)
            kept_ids.add(node.id)
            continue
        held = _held_graph_node_columns(
            node, index, focus_fqid if node.id == graph.focus_id else None
        )
        states = [s for s in node.states if s.delivery_column_name in held]
        if states:
            kept_nodes.append(
                node.model_copy(
                    update={
                        "states": states,
                        "same_as": _narrow_refs_to_held(list(node.same_as), index),
                    }
                )
            )
            kept_ids.add(node.id)
    kept_edges = [
        edge
        for edge in graph.edges
        if edge.source in kept_ids and edge.target in kept_ids
    ]
    focus_id = graph.focus_id if graph.focus_id in kept_ids else None
    return graph.model_copy(
        update={"nodes": kept_nodes, "edges": kept_edges, "focus_id": focus_id}
    )


def _narrow_group_members(group, index: CatalogIndex, catalog: Catalog):  # noqa: ANN001 — reg_meta ConceptGroupSummary
    """Return `group` with its `members` narrowed to the steward's holdings (#859),
    or None if no member survives. A representation member (`delivery_column` set) is
    kept iff `index.admits(str(member.fqid), member.delivery_column)`; a whole-variable
    member (`delivery_column` None) iff its bare FQID is in `admitted_variable_fqids`.
    Reuses the existing `admits` / `admitted_variable_fqids` probes (no re-derivation).
    The group's tags are recomputed from the surviving members so a filtered steward
    never inherits thematic tags from excluded siblings. A frozen Pydantic model, so
    the narrowed copy is via `model_copy`."""
    admitted = index.admitted_variable_fqids
    kept = [
        m
        for m in group.members
        if (
            index.admits(str(m.fqid), m.delivery_column)
            if m.delivery_column is not None
            else str(m.fqid) in admitted
        )
    ]
    if not kept:
        return None
    # ty 0.0.54 resolves reg_webapp's workspace reg_meta dependency from the main
    # checkout in this worktree layout, so it may not see this PR's new helper.
    catalog_with_tag_scope = cast("Any", catalog)
    return group.model_copy(
        update={
            "members": tuple(kept),
            "tags": catalog_with_tag_scope.tags_for_variable_members(
                m.fqid for m in kept
            ),
        }
    )


def _narrow_groups(groups: list, index: CatalogIndex, catalog: Catalog) -> list:  # noqa: ANN001 — reg_meta ConceptGroupSummary list
    """Narrow each concept group's members to the steward's holdings (#859),
    dropping a group with no surviving member. The shared body of the identical
    walrus comprehension in `_register_response` and `get_binding_dimensions`
    (each reused `_narrow_group_members`, the per-member probe)."""
    return [
        narrowed
        for g in groups
        if (narrowed := _narrow_group_members(g, index, catalog)) is not None
    ]


def _is_admitted(parsed: Fqid, index: CatalogIndex) -> bool | None:
    """Steward admission for `parsed`, dispatched on its grain (#859):

    - VARIABLE_BINDING → the bare binding FQID is in `admitted_variable_fqids`;
    - REGISTER → `admits_register`; PROVIDER → `admits_provider`;
    - CLASSIFICATION (and any other kind) → None = PASS-THROUGH: classifications
      are catalog-global (decision 2), so they are never steward-gated.

    A `bool` answers admitted/not-admitted; `None` means "this kind is not gated"
    (the caller proceeds without a gate). Same held-set probes the browse mappers
    use (`admits_register` / `admits_provider` read the cached held sets → O(1)),
    so the gate stays consistent with the narrowing."""
    if parsed.kind is FqidKind.VARIABLE_BINDING:
        return str(parsed) in index.admitted_variable_fqids
    if parsed.kind is FqidKind.REGISTER:
        return index.admits_register(str(parsed))
    if parsed.kind is FqidKind.PROVIDER:
        assert parsed.provider is not None
        return index.admits_provider(parsed.provider)
    return None


def _require_admitted(
    catalog: Catalog,
    parsed: Fqid,
    index: CatalogIndex,
    request: Request,
    suffix: str = "",
) -> RedirectResponse | None:
    """The ONE pre-resolve admission gate for a filtered steward (#859), covering
    ALL grains. Applied BEFORE `_resolve_to_node` (and by the binding sub-endpoints /
    the `?period` branch). Returns:

    - None when `parsed` is admitted, OR its kind is not gated (a classification —
      `_is_admitted` returns None → pass-through, decision 2): the caller proceeds.
    - A 301 `RedirectResponse` when `parsed` is an UNADMITTED but DEAD/renamed slug
      whose terminal successor IS held.
    - raises 404 otherwise (a LIVE entity the steward doesn't hold, or a dead slug
      with no held successor).

    Why this is ONE gate, pre-resolve, replacing the old binding-only gate plus the
    post-resolve provider/register 404s inside `_resolve_to_node`: a post-resolve
    404 was caught by `get_catalog_node`'s generic `except HTTPException → 301`
    branch and converted to a redirect with NO live-vs-dead and NO held check — so a
    LIVE unheld register/provider with a succession edge wrongly 301'd (possibly to
    an UNHELD successor). Gating pre-resolve at every grain closes that leak: a LIVE
    unheld entity 404s here and never reaches the generic redirect.

    The live-vs-dead split (uniform across grains): a LIVE entity the steward simply
    doesn't hold must 404, NEVER 301 — `variable_replaced_by` / `register_replaced_by`
    carry succession edges between LIVE entities too, so walking the terminal
    successor of a live entity would wrongly redirect a simply-unheld entity to a
    held successor, diverging from the global deployment (which only redirects DEAD
    slugs). Only a DEAD/renamed slug (does not resolve) is a 301 candidate, and only
    when its terminal successor is itself HELD (held-check dispatched on the
    terminal's own kind), query + `suffix` preserved (uniform with `_redirect_or_4xx`).

    The `catalog.resolve` (`_resolves_live`) is paid ONLY on the unadmitted path —
    an admitted/pass-through subject early-outs before it, so the common case incurs
    no redundant resolve."""
    admitted = _is_admitted(parsed, index)
    if admitted is None or admitted:
        return None
    # Not admitted. A LIVE entity the steward simply doesn't hold must 404 (NOT 301):
    # succession edges exist between live entities, so a terminal-successor walk here
    # would mis-redirect. Only a DEAD/renamed slug (does not resolve) is a 301 candidate.
    if _resolves_live(catalog, parsed):
        raise HTTPException(status_code=404, detail=_NOT_IN_CATALOG_DETAIL)
    redirect = _successor_redirect(catalog, parsed, request, suffix, require_held=index)
    if redirect is not None:
        return redirect
    raise HTTPException(status_code=404, detail=_NOT_IN_CATALOG_DETAIL)


def _resolves_live(catalog: Catalog, parsed: Fqid) -> bool:
    """True iff `parsed` resolves to a LIVE entity; False when it is a dead/renamed
    slug (reg_meta raises `fqid_not_found`). Any OTHER `RegMetaError` (a corrupt-DB /
    build-invariant break) re-raises — it is a server fault, not a "dead slug" signal.
    Used by `_require_admitted` to keep a live-but-unheld subject (404) from being
    301'd as if it were a renamed slug."""
    try:
        catalog.resolve(parsed)
    except RegMetaError as exc:
        if exc.exit_code == EXIT_NOT_FOUND and exc.code == _FQID_NOT_FOUND_CODE:
            return False
        raise
    return True


def _narrow_refs_to_held(refs: list, index: CatalogIndex) -> list:  # noqa: ANN001 — VariableRef list
    """Narrow a list of variable-grain edge refs (predecessors/successors/same_as)
    to those whose `fqid` is a held binding (#859). A ref with `fqid is None`
    (unaddressable) can be in no steward catalog, so it drops. Reuses
    `admitted_variable_fqids`."""
    admitted = index.admitted_variable_fqids
    return [r for r in refs if r.fqid is not None and str(r.fqid) in admitted]


def _narrow_lineage_to_held(
    edges: list,  # noqa: ANN001 — LineageEdge list
    index: CatalogIndex,
    consumer_state_ids: frozenset[int],
) -> list:
    """Narrow `LineageEdge`s to held source bindings AND held consumer states
    (#865). `source_fqid` keeps unheld source variables out; `consumer_state_id`
    keeps lineage for unheld delivery columns of the held subject out. Shared by the
    binding leaf embed and the standalone `/lineage` sub-endpoint so they agree."""
    admitted = index.admitted_variable_fqids
    return [
        e
        for e in edges
        if e.consumer_state_id in consumer_state_ids
        and e.source_fqid is not None
        and str(e.source_fqid) in admitted
    ]


def _narrow_lineage_warnings_to_held(
    warnings: list,
    consumer_state_ids: frozenset[int],  # noqa: ANN001 — LineageWarning list
) -> list:
    """Narrow lineage warnings to held consumer states (#865)."""
    return [w for w in warnings if w.consumer_state_id in consumer_state_ids]


def _held_consumer_state_ids(
    catalog: Catalog, parsed: Fqid, index: CatalogIndex
) -> frozenset[int]:
    """State IDs of `parsed` whose delivery columns the filtered steward holds."""
    states = _filter_states_to_held(catalog.states(parsed), index, str(parsed))
    return frozenset(s.state_id for s in states)


def _coverage_from_rows(rows: list[VariableCoverage]) -> VariableCoverage | None:
    """Aggregate per-column coverage rows into one browse-row coverage object."""
    if not rows:
        return None
    froms = [r.coverage_from for r in rows if r.coverage_from is not None]
    open_ended = any(r.open_ended for r in rows)
    tos = [r.coverage_to for r in rows if r.coverage_to is not None]
    return VariableCoverage(
        coverage_from=min(froms) if froms else None,
        coverage_to=(max(tos) if tos else None) if not open_ended else None,
        open_ended=open_ended,
        state_count=sum(r.state_count for r in rows),
    )


_NO_STATE_COVERAGE = VariableCoverage(
    coverage_from=None,
    coverage_to=None,
    open_ended=False,
    state_count=0,
)


def _held_variable_coverage(
    per_column: dict[tuple[str, str], VariableCoverage],
    per_unnamed: dict[str, VariableCoverage],
    variable_slug: str,
    held_columns: frozenset[str | None],
) -> VariableCoverage | None:
    """Coverage for one held binding from held delivery-column rows only (#865).

    A named held column with no per-column state row yields no coverage instead of
    borrowing the whole-variable union; that mirrors representation-member coverage
    and avoids overstating partial-column holdings. A genuinely unnamed held column
    (`delivery_column_name is None`) has no `register_column_coverage` key, so it uses
    the exact unnamed-column coverage row.
    """
    rows = [
        per_column[(variable_slug, column)]
        for column in held_columns
        if column is not None and (variable_slug, column) in per_column
    ]
    if None in held_columns and variable_slug in per_unnamed:
        rows.append(per_unnamed[variable_slug])
    return _coverage_from_rows(rows)


def _binding_tags(
    catalog: Catalog, resolved: ResolvedVariable, index: CatalogIndex | None
) -> list[TagMembership]:
    """Variable tags scoped to a filtered steward's held group siblings."""
    if index is None or resolved.group is None:
        return list(resolved.tags)

    group = catalog.concept_group(
        resolved.group.provider,
        resolved.group.register_name,
        resolved.group.key,
    )
    narrowed_group = (
        None if group is None else _narrow_group_members(group, index, catalog)
    )
    narrowed_members = () if narrowed_group is None else narrowed_group.members
    canonical_fqid = str(resolved.canonical_fqid)
    target_survived = any(
        str(member.fqid) == canonical_fqid for member in narrowed_members
    )
    group_member_fqids = (
        tuple(member.fqid for member in narrowed_members) if target_survived else ()
    )
    # See `_narrow_group_members`: local ty can see the pre-PR reg_meta surface here.
    catalog_with_tag_scope = cast("Any", catalog)
    return list(
        catalog_with_tag_scope.tags_for_variable(
            resolved.canonical_fqid,
            group_member_fqids=group_member_fqids,
        )
    )


# ── reg_meta model → catalog node mappers (see DESIGN.md → Pydantic boundary) ──
# The per-leaf 1:1 wrappers are gone (#681): reg_meta now returns frozen Pydantic
# models whose `Fqid` fields serialize to the canonical string and whose
# register-bearing models already dump `register`, so the leaf shapes pass straight
# through. Only the NODE mappers remain — they carry genuine server-side
# enrichment (the `kind` discriminator, the `catalog.*_chain` / `classification_*`
# server-side resolution, the coverage zip, the `via_same_as` stringify).


def _binding_node(
    catalog: Catalog, resolved: ResolvedVariable, index: CatalogIndex | None
) -> BindingNode:
    """Map a `ResolvedVariable` to the embedded-record leaf. Embeds the
    full wire-relevant record; the internal `provider_key` (SCB build-time join
    key, redundant with the FQID) is intentionally not exposed, and
    `lineage_warnings` are NOT on `ResolvedVariable` so they're omitted (A5.2
    `/lineage_warnings`).

    The leaf edges (`states` / `same_as` / `lineage`) are reg_meta's
    frozen Pydantic models, passed straight through (#681). The FULL variable
    succession chain (#582) is embedded as `succession_chain` — resolved server-side
    (`Catalog.variable_chain`, same_as-canonicalized + walked
    terminal→predecessors) so the SPA renders the whole timeline synchronously,
    superseding the immediate-neighbor `replaced_by` embed. The `/predecessors` /
    `/successors` sub-resources are unaffected — they back the #411 redirect rails.

    #859: for a filtered steward the embedded `states` are narrowed to the delivery
    columns the steward holds for this binding (column-grain faithful), AND the embedded
    edge collections (`same_as` / `succession_chain` / `lineage`) are
    narrowed to held neighbors — otherwise a held binding's leaf would leak unheld
    neighbors, inconsistent with the already-narrowed `/predecessors` / `/successors`
    sub-endpoints. Admission of the binding ITSELF is gated by the caller
    (`_require_admitted`) before this is reached, so a kept binding always has ≥1 held
    column. NOTE: the binding `/graph` node-set narrowing remains a tracked deferral —
    this covers only the leaf-EMBEDDED edge collections."""
    states = list(resolved.states)
    same_as = list(resolved.same_as)
    succession_chain = catalog.variable_chain(resolved.fqid)
    lineage = list(resolved.lineage)
    if index is not None:
        states = _filter_states_to_held(states, index, str(resolved.fqid))
        consumer_state_ids = frozenset(s.state_id for s in states)
        # `same_as` carries `.fqid`; succession-chain editions carry a navigable
        # `.fqid` too, so the same held-FQID narrower applies to both.
        same_as = _narrow_refs_to_held(same_as, index)
        succession_chain = _narrow_refs_to_held(succession_chain, index)
        # `LineageEdge` narrows on held source FQID AND held consumer state IDs —
        # factored into the shared helper so the leaf embed and `/lineage` agree.
        lineage = _narrow_lineage_to_held(lineage, index, consumer_state_ids)
    return BindingNode(
        fqid=str(resolved.fqid),
        variable_id=resolved.variable_id,
        register_id=resolved.register_id,
        name=resolved.name,
        definition=resolved.definition,
        description=resolved.description,
        operational_definition=resolved.operational_definition,
        measurement_unit=resolved.measurement_unit,
        is_sensitive=resolved.is_sensitive,
        is_identifier=resolved.is_identifier,
        deprecated=resolved.deprecated,
        source_register_id=resolved.source_register_id,
        source_register_text=resolved.source_register_text,
        # `ResolvedVariable`'s edge collections are tuples (frozen model); the
        # response model fields are `list`, so coerce — wire-identical (#681).
        states=states,
        same_as=same_as,
        succession_chain=succession_chain,
        lineage=lineage,
        # #616/#617: the binding's owning group as `(provider, register, key)` so a
        # member page links to the group subject without a second fetch; None when
        # ungrouped. Keyed on the RESOLVED variable's triple, so a same_as alias
        # reports its target's group (reg_meta sets it on `ResolvedVariable.group`).
        group=resolved.group,
        tags=_binding_tags(catalog, resolved, index),
        via_same_as=(
            [str(f) for f in resolved.via_same_as]
            if resolved.via_same_as is not None
            else None
        ),
    )


def _classification_node(
    catalog: Catalog, resolved: ResolvedClassification
) -> ClassificationNode:
    """Map a resolved classification onto its leaf node, embedding the FULL
    succession edition chain (#571) so the browse panel renders the whole timeline
    synchronously — no per-neighbor fetch. The chain resolves `same_as`
    server-side (`Catalog.classification_chain`); every edition is a live
    `classification` row (the build validator guarantees succession editions are
    live).

    #609 embeds two more leaf surfaces server-side (same synchronous-render
    rationale): `codes` — the RESOLVED edition's value-set codes (per-edition, so
    only the viewed edition's list; other editions are reached via the chain) — and
    `dimensions` — the curated umbrella group(s) this edition belongs to (the niva ↔
    aggregate granularity cross-reference, read off the existing concept-group
    table). The chain / codes / dimensions are reg_meta's frozen Pydantic models,
    embedded directly (#681)."""
    node = ClassificationNode(
        fqid=str(resolved.fqid),
        short_name=resolved.short_name,
        name=resolved.name,
        via_same_as=(
            [str(f) for f in resolved.via_same_as]
            if resolved.via_same_as is not None
            else None
        ),
        edition_chain=catalog.classification_chain(resolved.fqid),
        codes=catalog.classification_codes(resolved.fqid),
        dimensions=catalog.classification_dimensions(resolved.fqid),
    )
    # ty 0.0.54 sees the workspace reg_meta surface without these new Pydantic
    # fields here, even though runtime/OpenAPI generation resolve them correctly.
    resolved_with_derivation = cast("Any", resolved)
    return node.model_copy(
        update={
            "derived_from": list(resolved_with_derivation.derived_from),
            "derivatives": list(resolved_with_derivation.derivatives),
        }
    )


def _concept_group_node(
    catalog: Catalog,
    provider_slug: str,
    register_slug: str,
    group,
    member_hint: str | None,
) -> ConceptGroupNode:
    """Map a reg_meta `ConceptGroupSummary` (#303) onto the group SUBJECT node
    (#617), zipping per-member study-window `coverage` (#351) onto each member.
    `ConceptGroupNodeMember` extends reg_meta's `ConceptGroupMember` with `coverage`,
    so the member's `fqid` / `name` / `facets` (reg_meta `GroupFacet`s) and the #819
    `delivery_column` representation discriminator pass straight through; only
    `coverage` is added (#681).

    Coverage is sourced PER REPRESENTATION (#819): a whole-variable member
    (`delivery_column` None) gets its variable-level coverage from
    `register_variable_coverage` (keyed by variable SLUG — the binding-FQID leaf
    segment, mirroring the register listing); a representation member
    (`delivery_column` set) gets its OWN per-column window from
    `register_column_coverage` (keyed by `(slug, delivery_column)`), so two
    representations sharing one variable (e.g. CDISP 1968– vs CDISP5 2020– on one
    `disponibel-inkomst` member) show DIFFERENT spans instead of both inheriting the
    variable's union. A representation whose column has NO per-column window gets the
    zero-state coverage object — NOT `None` and NOT the variable union: SCB keeps the
    full historical `variable_alias` set apart from `variable_state`, so a column with
    an alias but no state row is known never-delivered, not unknown and not delivered
    through its siblings' years. `member_hint` is the validated `?member=` focus slug,
    echoed for the SPA to highlight (None when absent/unrecognized — a bad hint is
    ignored, keeping the group page first-class)."""
    coverage = catalog.register_variable_coverage(provider_slug, register_slug)
    column_coverage = catalog.register_column_coverage(provider_slug, register_slug)
    members: list[ConceptGroupNodeMember] = []
    for m in group.members:
        # The member FQID's leaf segment IS its variable slug — the key
        # `register_variable_coverage` returns (mirrors `_register_response`).
        leaf_slug = str(m.fqid).rsplit("/", 1)[-1]
        # #819: a representation member (delivery_column set) uses ONLY its
        # per-column window. #840: a missing per-column key is still a CURATED
        # representation member, so serialize the zero-state coverage object rather
        # than `None` (unknown) or the variable-level union.
        if m.delivery_column is not None:
            member_cov = column_coverage.get(
                (leaf_slug, m.delivery_column), _NO_STATE_COVERAGE
            )
        else:
            member_cov = coverage.get(leaf_slug)
        members.append(
            ConceptGroupNodeMember(
                fqid=m.fqid,
                name=m.name,
                facets=m.facets,
                # #819: the per-representation discriminator — None for a
                # whole-variable member, the SCB delivery column for a
                # representation member (two members can share an `fqid`).
                delivery_column=m.delivery_column,
                coverage=member_cov,
            )
        )
    return ConceptGroupNode.model_validate(
        {
            "provider": provider_slug,
            "register": register_slug,
            "key": group.key,
            "label": group.label,
            "source": group.source,
            "axes": list(group.axes),
            "members": members,
            "tags": list(group.tags),
            "member": member_hint,
        }
    )


def _classification_group_node(group) -> ClassificationGroupNode:
    """Map a reg_meta `ConceptGroupSummary` (a classification umbrella, #756) onto
    its group SUBJECT node. The classification SIBLING of `_concept_group_node`,
    but simpler: classification members carry NO provider/register/coverage, so the
    members (reg_meta's frozen browse `ConceptGroupMember` — fqid + name + facets)
    map straight through with nothing zipped on."""
    return ClassificationGroupNode(
        key=group.key,
        label=group.label,
        source=group.source,
        axes=list(group.axes),
        members=list(group.members),
    )


def _classification_family_node(family) -> ClassificationFamilyNode:  # noqa: ANN001
    """Map a derived classification succession family (#771) onto its subject node."""
    return ClassificationFamilyNode(
        key=family.key,
        label=family.label,
        editions=list(family.editions),
    )


def _held_register_coverage(
    per_column: dict[tuple[str, str], VariableCoverage],
    per_unnamed: dict[str, VariableCoverage],
    held_columns_by_slug: dict[str, frozenset[str | None]],
) -> RegisterCoverage | None:
    """Recompute a register's `RegisterCoverage` from held delivery columns (#865),
    so a filtered steward's provider page doesn't overstate a partial-column hold by
    inheriting the whole-variable span. `variable_count` remains browse-grain: distinct
    held variable slugs with coverage from a named held column or unnamed-column
    fallback.

    Mirrors `provider_register_coverage`'s semantics exactly so the held-only number
    is the same KIND of number, just over held column coverage:
    - `variable_count` = held variable slugs WITH at least one held coverage row.
    - the span = min `coverage_from` / max `coverage_to` over those held variables,
      `open_ended` if ANY held variable is open-ended (its `coverage_to` is None +
      `open_ended` True, the open-ended sentinel reg_meta already mapped per row).

    Returns None when NO held variable has coverage — a held register with no coverage
    greys the hint, NOT a fabricated zero."""
    rows: list[VariableCoverage] = []
    for slug, held_columns in held_columns_by_slug.items():
        cov = _held_variable_coverage(per_column, per_unnamed, slug, held_columns)
        if cov is not None:
            rows.append(cov)
    if not rows:
        return None
    froms = [r.coverage_from for r in rows if r.coverage_from is not None]
    open_ended = any(r.open_ended for r in rows)
    tos = [r.coverage_to for r in rows if r.coverage_to is not None]
    return RegisterCoverage(
        variable_count=len(rows),
        coverage_from=min(froms) if froms else None,
        # An open-ended span has no finite upper bound (None + `open_ended` True),
        # mirroring reg_meta's `_coverage_bounds`.
        coverage_to=(max(tos) if tos else None) if not open_ended else None,
        open_ended=open_ended,
    )


def _provider_response(
    catalog: Catalog, resolved: ResolvedProvider, index: CatalogIndex | None
) -> ProviderResponse:
    provider_slug = resolved.fqid.provider
    assert provider_slug is not None
    registers = catalog.list_registers(provider_slug)
    # #859: a filtered steward keeps only the registers it holds. Hoist the held set
    # ONCE (it's a full index scan) and test membership directly — `admits_register`
    # would rebuild it per register (O(registers × index)); mirrors how
    # `get_catalog_root` / `_register_response` hoist. The `global` deployment (index
    # None) keeps all.
    if index is not None:
        held = index.held_register_fqids
        registers = [r for r in registers if str(r.fqid) in held]

    if index is None:
        # `global`: ONE GROUP BY over the full provider, keyed by register slug
        # (~40 ms for scb's 238 registers), query-time behind the ETag/edge cache.
        # Held-only recompute below would change this path's numbers — keep it exact.
        coverage = catalog.provider_register_coverage(provider_slug)

        def coverage_for(register_slug: str) -> RegisterCoverage | None:
            return coverage.get(register_slug)
    else:
        # #865: a filtered steward must NOT inherit the full-register aggregate —
        # `provider_register_coverage` counts EVERY variable in each register, so a
        # held register would overstate spans for partial-column holds. Recompute each
        # held register's coverage from its HELD delivery columns only
        # (`register_column_coverage`, keyed by `(variable slug, delivery column)`). The
        # held FQIDs per register are derived ONCE from `admitted_variable_fqids`.
        held_columns_by_register: dict[str, dict[str, frozenset[str | None]]] = {}
        for fqid in index.admitted_variable_fqids:
            provider, register, variable = fqid.split("/")
            held_columns_by_register.setdefault(f"{provider}/{register}", {})[
                variable
            ] = index.held_columns(fqid)

        def coverage_for(register_slug: str) -> RegisterCoverage | None:
            held_columns = held_columns_by_register.get(
                f"{provider_slug}/{register_slug}"
            )
            if not held_columns:
                return None
            per_column = catalog.register_column_coverage(provider_slug, register_slug)
            per_unnamed = catalog.register_unnamed_column_coverage(
                provider_slug, register_slug
            )
            return _held_register_coverage(per_column, per_unnamed, held_columns)

    return ProviderResponse(
        fqid=str(resolved.fqid),
        name=resolved.name,
        children=[
            RegisterNode(
                fqid=str(r.fqid),
                name=r.name,
                purpose=r.purpose,
                # r.fqid.register is always set for a register summary; the guard
                # keeps the dict key str-typed. reg_meta's `RegisterCoverage` passes
                # straight through (#681).
                coverage=coverage_for(r.fqid.register) if r.fqid.register else None,
                tags=list(catalog.tags_for_register(r.fqid)),
            )
            for r in registers
        ],
    )


def _register_response(
    catalog: Catalog, resolved: ResolvedRegister, index: CatalogIndex | None
) -> RegisterResponse:
    provider_slug = resolved.fqid.provider
    register_slug = resolved.fqid.register
    assert provider_slug is not None and register_slug is not None
    bindings = catalog.list_bindings(provider_slug, register_slug)
    groups = catalog.list_concept_groups(provider_slug, register_slug)
    # #859: a filtered steward keeps only the bindings it admits (by bare binding
    # FQID) and narrows each concept group's members to held; a group with no held
    # member is dropped. `children` and `groups` stay consistent — the SPA folds the
    # surviving group members under the surviving flat children.
    if index is not None:
        admitted = index.admitted_variable_fqids
        bindings = [b for b in bindings if str(b.fqid) in admitted]
        groups = _narrow_groups(groups, index, catalog)
    # #351/#865 coverage, keyed by variable slug for global and by
    # (variable slug, delivery column) for filtered stewards. The filtered path uses
    # only held delivery-column rows so a partial-column steward does not inherit the
    # whole-variable span/count.
    if index is None:
        variable_coverage = catalog.register_variable_coverage(
            provider_slug, register_slug
        )

        def coverage_for(variable_slug: str) -> VariableCoverage | None:
            return variable_coverage.get(variable_slug)

    else:
        column_coverage = catalog.register_column_coverage(provider_slug, register_slug)
        unnamed_coverage = catalog.register_unnamed_column_coverage(
            provider_slug, register_slug
        )

        def coverage_for(variable_slug: str) -> VariableCoverage | None:
            held_columns = index.held_columns(
                f"{provider_slug}/{register_slug}/{variable_slug}"
            )
            return _held_variable_coverage(
                column_coverage, unnamed_coverage, variable_slug, held_columns
            )

    # A register's children are its bindings PLUS a `variants` reference
    # stub (the declared A5.2 variant-browser slot — a link, not data).
    children: list[RegisterChild] = [
        BindingChild(
            fqid=str(b.fqid),
            name=b.name,
            # b.fqid.variable is always set for a binding summary; the guard keeps
            # the dict key str-typed. reg_meta's `VariableCoverage` passes straight
            # through (#681).
            coverage=coverage_for(b.fqid.variable) if b.fqid.variable else None,
        )
        for b in bindings
    ]
    children.append(VariantsRef(register_fqid=str(resolved.fqid)))
    return RegisterResponse(
        fqid=str(resolved.fqid),
        name=resolved.name,
        purpose=resolved.purpose,
        tags=list(resolved.tags),
        children=children,
        # #303 concept groups: grouped bindings ALSO stay in `children` (the flat
        # list is complete); the SPA folds members under the group rows. reg_meta's
        # `ConceptGroupSummary` passes straight through (#681).
        groups=groups,
    )


def _classification_root_response(
    conn: sqlite3.Connection,
) -> ClassificationRootResponse:
    """The `class` (1 seg) classification-root: the CURRENT/TERMINAL
    classifications as children, derived succession families, plus the #303/#516
    umbrella groups. The
    CHILDREN list still reuses `reg_meta.queries.list_classifications` (LOCKED
    — the children enumeration grew no Catalog method); the GROUPS come from
    `Catalog.list_classification_groups`, and FAMILIES come from
    `Catalog.list_classification_families` over `classification_replaced_by` — a
    `Catalog(conn)` wrapper over the request connection. The wrapper is
    construction-only (no connection ownership); `close()` is never called on it —
    the connection stays owned by the handler's `_catalog_conn` contextmanager. A
    classification with a NULL slug isn't FQID-addressable, so it's excluded from
    children and group members alike (symmetric with `list_registers`'s slug filter).

    Bare children exclude superseded editions (`superseded_by` set) and every
    edition represented by a one-dimensional succession family row. Superseded
    and future family editions are reached through the family/leaf edition-chain
    panels (ClassificationLineagePanels, incl. the #605 split-root fan-out) or by
    direct URL. One-dimensional succession families (SSYK/ICD/LKF/SNI) replace
    their bare edition children with a family row, so the root reads as a concept
    entrypoint rather than only the current edition. Group members
    are themselves terminal (the 2020 SUN editions + the version-independent nivå
    aggregates), so they stay in `children` and the SPA folds them under the group
    row."""
    rows = list_classifications(conn)
    catalog = Catalog(conn)
    families = [
        _classification_family_node(family)
        for family in catalog.list_classification_families()
    ]
    family_edition_fqids = {
        str(edition.fqid)
        for family in families
        for edition in family.editions
        if edition.fqid is not None
    }
    children: list[ClassificationNode] = []
    for row in rows:
        slug = row.get("slug")
        if not slug:
            continue
        # superseded_by is a GROUP_CONCAT of successor short_names; truthy ⇒ a
        # newer edition supersedes this one ⇒ not a current edition, skip it.
        if row.get("superseded_by"):
            continue
        if str(Fqid.classification_fqid(slug)) in family_edition_fqids:
            continue
        children.append(
            ClassificationNode(
                fqid=str(Fqid.classification_fqid(slug)),
                short_name=row["short_name"],
                name=row["name"],
            )
        )
    # Curated classification umbrella groups (e.g. group:sun over its dimensions;
    # #516). Grouped classifications ALSO stay in `children`; the SPA folds them.
    # Members are terminal editions, so the superseded-by filter above keeps them.
    # reg_meta's `ConceptGroupSummary` list passes straight through (#681).
    groups = catalog.list_classification_groups()
    return ClassificationRootResponse(
        children=children, groups=groups, families=families
    )


def _catalog_url(fqid: Fqid) -> str:
    """The canonical catalog API URL for a terminal FQID — `/api/catalog/<path>`
    with each path segment percent-encoded (#355 PART 2 redirect target). Mirrors
    the frontend `encodeFqid` intent: split the FQID string on `/`, `quote` each
    segment, rejoin on `/`. A no-op for valid slugs (they have no reserved chars),
    but correct/defensive — and `quote` does NOT touch `/`, so the segments stay
    separate."""
    path = "/".join(urllib.parse.quote(seg) for seg in str(fqid).split("/"))
    return f"/api/catalog/{path}"


def _resolve_to_node(
    catalog: Catalog, fqid: Fqid, index: CatalogIndex | None
) -> CatalogNode:
    """Dispatch a parsed FQID to its Catalog resolver and map to a Pydantic node.
    Raises 404 (via `_http_404_if_not_found`) when the FQID resolves to nothing.

    #859: `index` (a filtered steward's `CatalogIndex`, else None) scopes each arm —
    provider/register listings narrow their CHILDREN, the binding leaf narrows its
    states. ADMISSION (the 404 / held-successor-301 decision for an unheld
    provider/register/binding) is NOT done here: it is enforced by the caller via
    the ONE pre-resolve `_require_admitted` gate, so a live unheld entity 404s before
    this resolve and can't slip into the generic redirect branch (the old
    post-resolve provider/register 404s here did exactly that — see
    `_require_admitted`). This function keeps ONLY child-narrowing; classifications
    pass through (decision 2)."""
    try:
        resolved = catalog.resolve(fqid)
    except RegMetaError as exc:
        _http_404_if_not_found(exc)
        raise  # unreachable; _http_404_if_not_found re-raises non-404s
    if isinstance(resolved, ResolvedProvider):
        return _provider_response(catalog, resolved, index)
    if isinstance(resolved, ResolvedRegister):
        return _register_response(catalog, resolved, index)
    if isinstance(resolved, ResolvedVariable):
        return _binding_node(catalog, resolved, index)
    if isinstance(resolved, ResolvedClassification):
        return _classification_node(catalog, resolved)
    # Unreachable: resolve() returns only the four ResolvedEntity arms.
    raise HTTPException(
        status_code=500, detail="unknown catalog entity"
    )  # pragma: no cover


def _http_4xx_from_regmeta(exc: RegMetaError) -> None:
    """Map a reg_meta query error from the period/edge accessors to HTTP: a
    genuine FQID-not-found → 404; a USAGE error on client input → 422; anything
    else (a corrupt-DB / build-invariant break) re-raises to a generic 500.

    EXIT_USAGE covers `not_a_binding_fqid` (a suffixed/period accessor handed a
    non-binding FQID) AND `invalid_period` (a syntactically-valid but lo>hi
    `?period` range that resolve_at rejects) — both are client-controlled input, so
    a 422, not a 500. EXIT_USAGE messages are input-validation text (no internal row
    IDs), so they're safe to echo; the catch-all maps only `fqid_not_found` to 404
    and keeps build-invariant breaks (e.g. `state_variant_unresolved`) as 500."""
    if _is_fqid_not_found(exc):
        raise HTTPException(status_code=404, detail=exc.message) from exc
    if exc.exit_code == EXIT_USAGE:
        raise HTTPException(status_code=422, detail=exc.message) from exc
    raise exc


def _successor_redirect(
    catalog: Catalog,
    parsed: Fqid,
    request: Request,
    suffix: str,
    *,
    require_held: CatalogIndex | None,
) -> RedirectResponse | None:
    """Walk `parsed`'s TERMINAL successor and build the 301 to it — the shared
    redirect-target stitching (`_catalog_url` + sub-endpoint `suffix` + the request
    query string), used by BOTH the dead-slug error layer (`_redirect_or_4xx`) and
    the steward pre-resolve gate (`_require_admitted`). Returns None when there is no
    successor edge (the caller falls back to its own 404 / 4xx mapping).

    `require_held` gates the held-check: pass a `CatalogIndex` (the steward gate) to
    301 ONLY when the terminal is itself HELD — held-check dispatched on the
    TERMINAL's own kind (binding → `admitted_variable_fqids`, register →
    `admits_register`, provider → `admits_provider`) — and return None (caller 404s)
    when it isn't, so a steward never redirects to an UNHELD successor. Pass None
    (the `global`/no-filter dead-slug path) to 301 to any terminal, the #411
    citation-stays-alive behavior unchanged."""
    terminal = catalog.resolve_terminal_successor(parsed)
    if terminal is None:
        return None
    if require_held is not None and not _is_admitted(terminal, require_held):
        return None
    target = f"{_catalog_url(terminal)}{suffix}"
    if request.url.query:
        target = f"{target}?{request.url.query}"
    return RedirectResponse(target, status_code=301)


def _redirect_or_4xx(
    catalog: Catalog,
    parsed: Fqid,
    exc: RegMetaError,
    request: Request,
    suffix: str = "",
) -> RedirectResponse:
    """On a `fqid_not_found` 404 for a (dead/renamed) binding, walk to the TERMINAL
    successor and 301-redirect — preserving the query string (so `?period=2019` /
    `?variant` ride along) and the sub-endpoint `suffix` (e.g. `/states`), so a cited
    dead-slug URL stays alive regardless of query or sub-resource (#411). Falls back
    to `_http_4xx_from_regmeta` (422 on usage, 404 when there is no successor edge,
    re-raise on a build-invariant 500) — those never redirect. Returns the redirect
    so the caller can `return` it. Shares the SAME not-found predicate
    (`_is_fqid_not_found`) as `_http_4xx_from_regmeta` (only `fqid_not_found`
    redirects; a 422 usage error or a 500 build-invariant break NEVER becomes a
    redirect), and the SAME redirect-target stitching (`_successor_redirect`) as the
    steward gate — here with `require_held=None` (this is the `global`/no-filter dead
    layer; a filtered steward's binding/register/provider is preempted by
    `_require_admitted` and never reaches here).

    SIBLING: the no-period node path in `get_catalog_node` implements the SAME 301
    successor policy for the already-mapped `HTTPException` layer — keep the two in
    sync (e.g. a 301→308 switch or a redirect header must land in both)."""
    if _is_fqid_not_found(exc):
        redirect = _successor_redirect(
            catalog, parsed, request, suffix, require_held=None
        )
        if redirect is not None:
            return redirect
    _http_4xx_from_regmeta(exc)  # raises 422 / 404 / re-raises 500
    raise exc  # unreachable — _http_4xx_from_regmeta always raises (satisfies the type)


def _parsed_binding(validated: ValidatedFqidPath) -> Fqid:
    """Parse a validated path into an Fqid, mapping a grammar/arity FqidError to
    422 (DB-free — runs before any connection opens). Used by the suffixed
    sub-endpoints, which only accept binding FQIDs (reg_meta's `_parse_binding`
    raises the 422-mapped `not_a_binding_fqid` for a non-binding kind). The path is
    a bare FQID — the `@version` pin is retired, so there is none to reject here."""
    try:
        return parse(validated.fqid)
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


# ── Routes ─────────────────────────────────────────────────────────────────
# Router ordering (see DESIGN.md → Catalog router structure): the suffixed
# sub-resource routes (`/states`, ..., `/lineage_warnings`) and the
# register-sub-resource `/{provider}/{register}/variants` MUST be declared ABOVE
# the `{fqid:path}` catch-all — Starlette matches in declaration order and the
# `{fqid:path}` converter greedy-consumes any suffix into `fqid`. The catch-all
# MUST stay last. `test_boot.py` (`routes_declared_before`) pins the order in CI.


@router.get("/catalog", response_model=RootResponse)
def get_catalog_root(request: Request) -> RootResponse:
    """The catalog root: every provider plus the classification-root sentinel.

    #859: a filtered steward keeps only providers it holds (`held_provider_slugs`);
    the classification-root sentinel is ALWAYS appended (classifications pass through
    — decision 2). The `global` deployment lists every provider (index None)."""
    index = _index(request)
    with _catalog_conn(request) as conn:
        providers = Catalog(conn).list_providers()
    if index is not None:
        held = index.held_provider_slugs
        providers = [p for p in providers if p.fqid.provider in held]
    children: list[ProviderNode | ClassificationRootNode] = [
        ProviderNode(fqid=str(p.fqid), name=p.name) for p in providers
    ]
    children.append(ClassificationRootNode())
    return RootResponse(children=children)


# The register-sub-resource variant browser. A FIXED 3-seg shape with a
# literal `variants` tail — NOT an `{fqid:path}` suffix — so it's declared with
# explicit `{provider}`/`{register}` segments, ABOVE the catch-all. The two
# segments are guarded as slugs (reusing the path guard on the 2-seg register
# FQID) before any connection opens.
@router.get("/catalog/{provider}/{register}/variants", response_model=VariantsResponse)
def get_register_variants(
    request: Request, provider: str, register: str
) -> VariantsResponse:
    """List a register's variants (the `?variant=` browse axis). `_default`
    is a real variant and IS returned (not filtered). 404 when the register
    doesn't resolve (so a typo'd register isn't a silent empty list)."""
    register_fqid = f"{provider}/{register}"
    # Validate both segments BEFORE opening a connection — as a strict
    # provider/register FQID, NOT the generic catalog path (which legitimately
    # admits the `class/<slug>` classification prefix). `Fqid.register_fqid` runs
    # reg_meta's authoritative `validate_slug` on both segments, rejecting
    # `class`/`_default`/traversal/period-shaped tokens (FqidError → 422, zero SQL).
    # `class` is NOT a valid provider, so `class/<x>/variants` is a clean 422 here,
    # not a 500. The constructed fqid is reused for the resolve below.
    try:
        fqid = Fqid.register_fqid(provider, register)
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    index = _index(request)
    # #859: a filtered steward 404s a register it doesn't hold (before any resolve)
    # — uniform with the binding leaf's "not in this steward's catalog".
    if index is not None and not index.admits_register(register_fqid):
        raise HTTPException(status_code=404, detail=_NOT_IN_CATALOG_DETAIL)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        # Resolve the register first so a bad (provider, register) is a 404, not a
        # 200 with an empty list (list_variants alone can't distinguish them).
        try:
            catalog.resolve(fqid)
        except RegMetaError as exc:
            _http_404_if_not_found(exc)
        variants = catalog.list_variants(provider, register)
        # #859: filter to the variant coords the steward actually holds data under
        # (a drift-emptied variant slot admits nothing → excluded). Match on the
        # variant slug (the coord's 3rd segment).
        if index is not None:
            held_slugs = {
                coord.split("/")[2]
                for coord in index.held_variant_coords_for_register(register_fqid)
            }
            variants = [v for v in variants if v.slug in held_slugs]
    # Construct via the alias `register=` (the canonical init param; the Python
    # attr is `register_name` to avoid the BaseModel.register method shadow).
    # reg_meta's `VariantSummary` list passes straight through (#681 — the
    # composite panel keys are tuples that serialize as JSON arrays directly).
    return VariantsResponse(register=register_fqid, variants=variants)


# ── Group `/graph` sub-resources (#761) ─────────────────────────────────────
# Sub-resources of the #756 group subject routes. Declaration-order gotcha (greedy
# `{key:path}`): each `…/{key:path}/graph` route MUST be declared ABOVE its
# `…/{key:path}` subject route — otherwise `group/class/sun/graph` is captured as
# `key="sun/graph"` by the subject route. And the literal-`class` graph route goes
# above the register `{provider}` graph route (mirroring #756's `class` beats
# `{provider}` ordering), all above the catch-all. `test_boot.py` pins the order.


@router.get("/catalog/group/class/{key:path}/graph", response_model=RelationshipGraph)
def get_classification_group_graph(request: Request, key: str) -> RelationshipGraph:
    """The relationship graph for a classification umbrella group (#761) — the
    union of its member editions' succession chains (`focus_id=None`). 404 when no
    classification group has that key. Shares the `/api/catalog` cache. By-key
    resolution lives in reg_meta (`Catalog.graph_for_classification_group` →
    `classification_group(key)`)."""
    with _catalog_conn(request) as conn:
        graph = Catalog(conn).graph_for_classification_group(key)
    if graph is None:
        raise HTTPException(status_code=404, detail=f"no classification group {key!r}")
    return graph


@router.get(
    "/catalog/group/{provider}/{register}/{key:path}/graph",
    response_model=RelationshipGraph,
)
def get_concept_group_graph(
    request: Request, provider: str, register: str, key: str
) -> RelationshipGraph:
    """The relationship graph for a register concept group (#761) — the union of
    its member variables' graphs (`focus_id=None`). 404 when no group with that key
    exists for the (provider, register) pair. `provider`/`register` are validated as
    a register FQID before the connection opens (mirrors `get_concept_group`); `key`
    is the derivation key (not slug-validated — an unknown key is a clean 404)."""
    try:
        register_fqid = str(Fqid.register_fqid(provider, register))
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    index = _index(request)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        # #859: a filtered steward must NOT reach an unheld group's (or unheld
        # register's) graph — that would bypass the scoping the subject route
        # `get_concept_group` applies. Gate it the SAME way: 404 the register if
        # unheld, then resolve the group and narrow its members with
        # `_narrow_group_members`; a group with no held member 404s. The graph's
        # internal NODE-SET narrowing for a held group stays DEFERRED (same as the
        # binding `/graph` deferral) — this gate only keeps an UNHELD group/register
        # out, mirroring the subject route.
        if index is not None:
            if not index.admits_register(register_fqid):
                raise HTTPException(status_code=404, detail=_NOT_IN_CATALOG_DETAIL)
            group = catalog.concept_group(provider, register, key)
            if group is None or _narrow_group_members(group, index, catalog) is None:
                raise HTTPException(status_code=404, detail=_NOT_IN_CATALOG_DETAIL)
        graph = catalog.graph_for_group(provider, register, key)
    if graph is None:
        raise HTTPException(
            status_code=404,
            detail=f"no concept group {key!r} in {provider}/{register}",
        )
    # #865: scope the graph's variable nodes and their states to held columns. Global
    # (`index is None`) passes the full graph through unchanged.
    if index is not None:
        graph = _narrow_graph_to_held(graph, index)
    return graph


# The classification-group/family SUBJECT route (#756/#771) — the classification
# sibling of the register-scoped `get_concept_group` below. Declared IMMEDIATELY
# ABOVE it (and thus above the greedy catch-all) so the LITERAL `class` segment is
# matched before the register route's `{provider}` param could capture it:
# `/catalog/group/class/sun` resolves here, NOT as a register group with
# provider=`class`. The `class` literal is fixed in the path (no provider/register
# to slug-validate), and `key` is a derivation key (NOT slug-validated — an unknown
# key is a clean 404).
@router.get(
    "/catalog/group/class/{key:path}", response_model=ClassificationGroupSubject
)
def get_classification_group(request: Request, key: str) -> ClassificationGroupSubject:
    """The classification subject addressed by `key`.

    A key can name either a curated umbrella group (#756, e.g. SUN) or a derived
    one-dimensional succession family (#771, e.g. SSYK/ICD). The two are distinct
    response `kind`s because an umbrella is concept-group membership, while a
    family is browse identity over `classification_replaced_by`. 404 when neither
    surface has that key.

    By-key group resolution delegates to `Catalog.classification_group(key)` (#761
    shipped the reg_meta accessor; #756 did this filter inline here to avoid a
    release). Family resolution delegates to `Catalog.classification_family(key)`.

    No provider/register/key is slug-validated: `class` is a fixed literal in the
    path, and `key` is a derivation key (not a slug). So there is no
    `Fqid.register_fqid` pre-check (unlike `get_concept_group` / `get_register_variants`)
    — just open the connection (mirroring the register route's per-request model)
    and resolve."""
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        group = catalog.classification_group(key)
        if group is not None:
            return _classification_group_node(group)
        family = catalog.classification_family(key)
        if family is not None:
            return _classification_family_node(family)
    raise HTTPException(
        status_code=404, detail=f"no classification group or family {key!r}"
    )


# The concept-group SUBJECT route (#617). A FIXED 4-seg shape with a literal
# `group` PREFIX — NOT an `{fqid:path}` suffix — so it's declared with explicit
# `{provider}`/`{register}`/`{key}` segments, ABOVE the catch-all (Starlette
# matches in declaration order; the greedy `{fqid:path}` would otherwise consume
# it). The `group` literal IS reserved in the PROVIDER slot of the slug grammar
# (`RESERVED_GROUP_SLUG`, see reg_meta/DESIGN.md → FQID grammar): with `group` as a
# non-leading path segment here, a provider literally named `group` would mint a
# binding-suffix URL `/catalog/group/<register>/<variable>/states` (5 segments) that
# THIS earlier-declared 5-seg route captures (provider=<register>, register=<variable>,
# key=`states`) → a wrong 404 instead of the binding's `/states`. Reserving `group` in
# the provider slot makes that collision unconstructable. The `provider`/`register`
# segments are validated as a register FQID before any connection opens (mirrors
# `get_register_variants`); `key` is the group's scope-unique derivation key (NOT
# a slug — it's a curated/token/edge derivation key), so it is NOT slug-validated,
# only resolved (a non-existent key is a clean 404).
@router.get(
    "/catalog/group/{provider}/{register}/{key:path}",
    response_model=ConceptGroupNode,
)
def get_concept_group(
    request: Request,
    provider: str,
    register: str,
    key: str,
    member: str | None = Depends(_validated_member),
) -> ConceptGroupNode:
    """The concept group addressed by `(provider, register, key)` (#617) — a
    browsable subject (all members selected). 404 when no group with that key
    exists for the (provider, register) pair, OR the pair names no register
    (`Catalog.concept_group` returns None for both). `?member=<slug>` is an
    optional FOCUS hint (a member leaf slug to highlight): validated as a slug
    before any connection opens, then echoed on the node only when it actually
    names a member of this group — an unrecognized hint is IGNORED (the group page
    stays first-class), not a 404.

    Validate `provider`/`register` as a register FQID BEFORE opening a connection
    (mirrors `get_register_variants`): `Fqid.register_fqid` runs reg_meta's
    authoritative `validate_slug` on both, rejecting `class`/`_default`/traversal/
    period-shaped tokens (FqidError → 422, zero SQL). `key` is the group's
    derivation key, not a slug, so it is NOT slug-validated here — an unknown key
    is a clean 404 from `concept_group`."""
    try:
        Fqid.register_fqid(provider, register)
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    index = _index(request)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        group = catalog.concept_group(provider, register, key)
        if group is None:
            raise HTTPException(
                status_code=404,
                detail=f"no concept group {key!r} in {provider}/{register}",
            )
        # #859: a filtered steward narrows the group's members to held; a group with
        # no held member is a 404 (not in this steward's catalog). The `?member`
        # hint is then matched against the NARROWED member set.
        if index is not None:
            group = _narrow_group_members(group, index, catalog)
            if group is None:
                raise HTTPException(status_code=404, detail=_NOT_IN_CATALOG_DETAIL)
        # The `?member` hint is admitted onto the node only when it names a real
        # member (matched on the member FQID's leaf slug); else ignored (None).
        member_hint = (
            member
            if member is not None
            and any(str(m.fqid).rsplit("/", 1)[-1] == member for m in group.members)
            else None
        )
        return _concept_group_node(catalog, provider, register, group, member_hint)


# ── The 6 binding-suffix sub-endpoints (see DESIGN.md → Catalog router
# structure) — ALL above the catch-all. ───────────────────────────────────
# Each follows the LOCKED connection model: path guard (`_validated_fqid`) +
# `parse` run BEFORE the connection opens; the connection is opened and used
# within the sync body (one thread — see `_catalog_conn`). reg_meta's accessor
# raises `not_a_binding_fqid` (→ 422) for a non-binding FQID and `_not_found`
# (→ 404) for an absent binding, both mapped by `_http_4xx_from_regmeta`.


@router.get("/catalog/{fqid:path}/states", response_model=StatesResponse)
def get_binding_states(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> StatesResponse | RedirectResponse:
    """Full state history for a binding. ≡ the leaf's embedded `states`,
    standalone. Same shape the `?period` catch-all returns (codegen sees one
    state-list type). A dead/renamed binding 301s to `/states` on its terminal
    successor (#411)."""
    parsed = _parsed_binding(validated)
    index = _index(request)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        # #859: gate admission (held-successor 301 / 404) before resolving.
        if index is not None:
            redirect = _require_admitted(
                catalog, parsed, index, request, suffix="/states"
            )
            if redirect is not None:
                return redirect
        try:
            states = catalog.states(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/states")
        if index is not None:
            states = _filter_states_to_held(states, index, str(parsed))
    return StatesResponse(binding=str(parsed), states=states)


@router.get("/catalog/{fqid:path}/predecessors", response_model=PredecessorsResponse)
def get_binding_predecessors(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> PredecessorsResponse | RedirectResponse:
    """Variables this binding's variable replaced (inbound succession). A
    dead/renamed binding 301s to `/predecessors` on its terminal successor (#411)."""
    parsed = _parsed_binding(validated)
    index = _index(request)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        if index is not None:
            redirect = _require_admitted(
                catalog, parsed, index, request, suffix="/predecessors"
            )
            if redirect is not None:
                return redirect
        try:
            refs = catalog.predecessors(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(
                catalog, parsed, exc, request, suffix="/predecessors"
            )
        # #859: narrow the returned refs to held variable FQIDs.
        if index is not None:
            refs = _narrow_refs_to_held(refs, index)
    return PredecessorsResponse(binding=str(parsed), predecessors=refs)


@router.get("/catalog/{fqid:path}/successors", response_model=SuccessorsResponse)
def get_binding_successors(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> SuccessorsResponse | RedirectResponse:
    """Variables that replaced this binding's variable (outbound succession). A
    dead/renamed binding 301s to `/successors` on its terminal successor (#411)."""
    parsed = _parsed_binding(validated)
    index = _index(request)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        if index is not None:
            redirect = _require_admitted(
                catalog, parsed, index, request, suffix="/successors"
            )
            if redirect is not None:
                return redirect
        try:
            refs = catalog.successors(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/successors")
        # #859: narrow the returned refs to held variable FQIDs.
        if index is not None:
            refs = _narrow_refs_to_held(refs, index)
    return SuccessorsResponse(binding=str(parsed), successors=refs)


@router.get("/catalog/{fqid:path}/dimensions", response_model=DimensionsResponse)
def get_binding_dimensions(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> DimensionsResponse | RedirectResponse:
    """Concept-group dimension memberships for this binding's variable (#489):
    the 'pick your variant' facet groups (level / population / rank / …) that
    contain it. Delegates to `Catalog.dimensions`, which resolves `same_as` like
    the sibling edge endpoints — an alias cites its resolved target's groups, not
    the requested register's. Binding-only (a non-binding kind 422s); a
    dead/renamed binding 301s to `/dimensions` on its terminal successor (#411)."""
    parsed = _parsed_binding(validated)
    index = _index(request)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        if index is not None:
            redirect = _require_admitted(
                catalog, parsed, index, request, suffix="/dimensions"
            )
            if redirect is not None:
                return redirect
        try:
            groups = catalog.dimensions(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/dimensions")
        # #859: `dimensions` returns concept GROUPS — narrow each group's members to
        # held (same rule as `_register_response`), dropping a group with no held
        # member, so a steward sees only its own holdings in the facet groups.
        if index is not None:
            groups = _narrow_groups(groups, index, catalog)
    return DimensionsResponse(binding=str(parsed), dimensions=groups)


@router.get("/catalog/{fqid:path}/graph", response_model=RelationshipGraph)
def get_binding_graph(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> RelationshipGraph | RedirectResponse:
    # Name kept `get_binding_graph` (not `get_leaf_graph`) on purpose: FastAPI
    # derives the operationId from it, and that id is baked into the frontend's
    # generated `api-types.ts` — renaming would churn the codegen for no behavioral
    # gain. The route now serves both leaf kinds (see below); the name is historical.
    """The relationship graph for a catalog LEAF — a binding (3-seg) OR a
    classification edition (2-seg) — dispatched on FQID kind (#761/#792). A binding:
    one node per variable with its representation-run state history +
    succession edges + same_as/group metadata, unioned over the variable's
    concept group (Fork B). A classification: the edition's succession chain unioned
    with its curated umbrella group(s) (the #678 unified-graph payload that retires
    the lineage/dimensions panels). An empty graph (`nodes: []`) is the "don't
    render" signal. A dead/renamed binding 301s to `/graph` on its terminal
    successor (#411); shares the `/api/catalog` cache. Topology + predicates live in
    reg_meta (`Catalog.graph_for_fqid` / `graph_for_classification_fqid`)."""
    parsed = _parsed_binding(validated)
    index = _index(request)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        # #865: gate the SUBJECT binding (held-successor 301 / 404), then narrow the
        # graph's variable node set and states to held columns before returning.
        if index is not None:
            redirect = _require_admitted(
                catalog, parsed, index, request, suffix="/graph"
            )
            if redirect is not None:
                return redirect
        try:
            if parsed.kind is FqidKind.CLASSIFICATION:
                return catalog.graph_for_classification_fqid(parsed)
            graph = catalog.graph_for_fqid(parsed)
            if index is not None:
                graph = _narrow_graph_to_held(graph, index, focus_fqid=str(parsed))
            return graph
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/graph")


@router.get("/catalog/{fqid:path}/lineage", response_model=LineageResponse)
def get_binding_lineage(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> LineageResponse | RedirectResponse:
    """Consumer-side composite lineage edges (state grain — see
    reg_meta_build/DESIGN.md → Consumer-side lineage (variable_state_lineage)).
    Maps what reg_meta's `LineageEdge` carries; the richer per-source-state shape is a
    possible reg_meta enhancement (not blocked on here — see DESIGN.md). A
    dead/renamed binding 301s to `/lineage` on its terminal successor (#411)."""
    parsed = _parsed_binding(validated)
    index = _index(request)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        if index is not None:
            redirect = _require_admitted(
                catalog, parsed, index, request, suffix="/lineage"
            )
            if redirect is not None:
                return redirect
        try:
            edges = catalog.lineage(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/lineage")
        # #865: narrow the returned edges to held source FQIDs AND held consumer
        # state IDs — the SAME narrowing the leaf embed applies in `_binding_node`.
        if index is not None:
            edges = _narrow_lineage_to_held(
                edges, index, _held_consumer_state_ids(catalog, parsed, index)
            )
    return LineageResponse(binding=str(parsed), lineage_edges=edges)


@router.get(
    "/catalog/{fqid:path}/lineage_warnings", response_model=LineageWarningsResponse
)
def get_binding_lineage_warnings(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> LineageWarningsResponse | RedirectResponse:
    """Build-time lineage warnings for the binding. Empty when lineage
    resolved cleanly. The leaf does NOT embed these — this is their endpoint. A
    dead/renamed binding 301s to `/lineage_warnings` on its terminal successor
    (#411)."""
    parsed = _parsed_binding(validated)
    index = _index(request)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        if index is not None:
            redirect = _require_admitted(
                catalog, parsed, index, request, suffix="/lineage_warnings"
            )
            if redirect is not None:
                return redirect
        try:
            warnings = catalog.lineage_warnings(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(
                catalog, parsed, exc, request, suffix="/lineage_warnings"
            )
        if index is not None:
            warnings = _narrow_lineage_warnings_to_held(
                warnings, _held_consumer_state_ids(catalog, parsed, index)
            )
    return LineageWarningsResponse(binding=str(parsed), lineage_warnings=warnings)


# The catch-all — MUST be the last route declared in this router (see seam above).
# Response is the discriminated `CatalogNode` union OR — on a binding leaf with a
# `?period` query — a `StatesResponse` (the resolve_at subset, uniform with
# `/states`). The two are a plain (non-discriminated) Union; the discriminator
# applies only WITHIN `CatalogNode`.
@router.get("/catalog/{fqid:path}", response_model=CatalogNode | StatesResponse)
def get_catalog_node(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
    period: list[Period] | None = Depends(_validated_period),
    variant: str | None = Depends(_validated_variant),
    value_set_version: str | None = Depends(_validated_value_set_version),
) -> CatalogNode | StatesResponse | RedirectResponse:
    """Resolve any catalog node by FQID path; on a binding leaf, an optional
    `?period` (with `?variant` / `?value_set_version`) narrows to the resolve_at
    state subset.

    The guards (`_validated_fqid` for the path, `_validated_period` /
    `_validated_variant` for the queries) run as dependencies, BEFORE this body —
    so a malformed path OR a malformed period/variant returns 422 **before** any
    connection opens (zero SQL, zero opens). `parse` is DB-free and runs before
    the open too. The classification-root literal `class` (1 seg) is special-cased
    before `parse`.

    `?period` semantics: present + binding leaf → `{states: [...]}` (the
    resolve_at subset, narrowed by `?variant` / `?value_set_version`; the #307
    comma list form resolves per segment, unioned + deduped by state_id). present +
    non-binding kind → IGNORED (resolve normally). absent on a binding leaf → the
    full node (full history) UNLESS a narrowing modifier (`?variant` /
    `?value_set_version`) is set: those are inert without `?period`, so they 422
    ("requires ?period") rather than silently no-op. absent on a non-binding kind →
    the full node. `?value_set_version` is a read-only browse-narrowing label
    filter — there is no FQID `@version` pin (retired). The connection is opened and
    used within this sync body (one thread — see `_catalog_conn`).
    """
    # `class` (1 seg) is the classification-root sentinel (see reg_meta/DESIGN.md →
    # FQID grammar) — a reserved slug `parse` rejects, so special-case it BEFORE parse. `class/<slug>` (2 seg)
    # flows through `parse` as a normal classification FQID. The `?period` query is
    # ignored on this (non-binding) kind.
    if validated.fqid == CLASSIFICATION_PREFIX:
        with _catalog_conn(request) as conn:
            return _classification_root_response(conn)

    try:
        # `parse` is DB-free, so a grammar/arity 422 here costs no connection.
        parsed = parse(validated.fqid)
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # `?value_set_version` is the read-only browse-narrowing label filter (no FQID
    # `@version` pin — retired). Used directly as the resolve_at version filter.
    vsv = value_set_version

    # #859: the filtered-steward index (None for `global`). Threaded into the node
    # mappers (provider/register children + binding states narrowing) and gating the
    # binding-leaf admission (404 / held-successor 301) below.
    index = _index(request)

    # A `?period` query on a binding leaf returns the resolve_at state subset
    # (uniform with `/states`), narrowed by `?variant` / `vsv`. On any other kind
    # `?period` is IGNORED.
    if parsed.kind is FqidKind.VARIABLE_BINDING:
        if period is None:
            # `?variant` / `?value_set_version` are MODIFIERS of the resolve_at
            # narrowing — inert without `?period`. Require `?period` rather than
            # silently no-op (the params narrow-or-422 everywhere else, so a silent
            # no-op here is a surprising surface). 422s before the connection opens.
            if vsv is not None or variant is not None:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "?variant and ?value_set_version narrow the resolve_at "
                        "state subset and require ?period"
                    ),
                )
        else:
            # The `_none` sentinel selects the empty/default label (`''`): the
            # empty string can't ride in the query (≡ absent), so map it here,
            # just before resolve_at's Python `label == value_set_version` filter.
            resolved_vsv = "" if vsv == VALUE_SET_VERSION_NONE else vsv
            with _catalog_conn(request) as conn:
                catalog = Catalog(conn)
                # #859: gate admission FIRST (preserving the renamed/dead-slug 301 to
                # a HELD successor); a held binding then narrows its resolved states
                # to held columns. None index = `global`, no gate, no narrowing.
                if index is not None:
                    redirect = _require_admitted(
                        catalog, parsed, index, request, suffix=""
                    )
                    if redirect is not None:
                        return redirect
                # Resolve PER SEGMENT (#340) — `resolve_at` never sees the #307
                # list form (mirrors `semantic._check_binding_period`). The union
                # dedupes by the COMPOUND (state_id, delivery_column_name,
                # valid_from): one state can intersect several segments, AND a
                # merged monthly-family variable (#319) expands one annual state
                # into 12 same-state_id per-month windows — keying on state_id
                # alone would collapse 11 of them. Insertion order keeps the
                # per-segment resolve_at ordering, chronological across a sorted
                # list.
                states_by_id: dict[tuple[int, str | None, str], VariableState] = {}
                try:
                    for segment in period:
                        for s in catalog.resolve_at(
                            parsed,
                            segment,
                            variant=variant,
                            value_set_version=resolved_vsv,
                        ):
                            states_by_id.setdefault(
                                (s.state_id, s.delivery_column_name, s.valid_from), s
                            )
                except RegMetaError as exc:
                    # #411: a dead/renamed binding cited WITH `?period` 301s to its
                    # terminal successor, query string preserved (so `?period=2019` /
                    # `?variant` ride along), uniform with the no-period node path.
                    return _redirect_or_4xx(catalog, parsed, exc, request)
            states = list(states_by_id.values())
            # #859: narrow the resolve_at subset to held columns (the binding is
            # already admitted by the gate above).
            if index is not None:
                states = _filter_states_to_held(states, index, str(parsed))
            return StatesResponse(binding=str(parsed), states=states)

    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        # #859: ONE pre-resolve admission gate covering ALL grains (binding /
        # register / provider), preserving the renamed/dead-slug 301 to a HELD
        # successor and 404ing a LIVE unheld entity ("not in this steward's
        # catalog"). This preempts the generic renamed-slug redirect below for those
        # grains — a steward must never 301 to an UNHELD successor, and a LIVE unheld
        # entity must 404, NOT ride that branch (the leak this gate closes: a
        # post-resolve 404 was caught below and 301'd with no live-vs-dead/held
        # check). A CLASSIFICATION passes through (`_require_admitted` returns None,
        # decision 2), so a dead classification slug still rides the generic redirect
        # branch below — intact.
        if index is not None:
            redirect = _require_admitted(catalog, parsed, index, request, suffix="")
            if redirect is not None:
                return redirect
        try:
            return _resolve_to_node(catalog, parsed, index)
        except HTTPException as exc:
            # #355 PART 2 / #412: a renamed/dead slug 404s (its `variable` or
            # `register` row is gone). Before surfacing that 404, walk to the
            # TERMINAL successor and 301-redirect the citation there.
            # `resolve_terminal_successor` dispatches on FQID kind (binding →
            # `variable_replaced_by`, register → `register_replaced_by`), so this
            # branch handles both grains with no kind-branching here. A non-404
            # (corrupt-DB / 500) must propagate UNCHANGED — only a genuine
            # `fqid_not_found` (mapped to 404 by `_resolve_to_node`) is a candidate.
            # SIBLING: `_redirect_or_4xx` is the same 301 successor policy for the
            # `?period`/sub-endpoint `RegMetaError` layer — keep the two in sync.
            if exc.status_code != 404:
                raise
            terminal = catalog.resolve_terminal_successor(parsed)
            if terminal is None:
                raise  # genuinely unknown — re-raise the original 404
            return RedirectResponse(_catalog_url(terminal), status_code=301)
