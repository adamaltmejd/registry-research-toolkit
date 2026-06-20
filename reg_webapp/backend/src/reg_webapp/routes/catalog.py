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
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from reg_meta.catalog import (
    OPEN_ENDED_VALID_TO,
    Catalog,
    Period,
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
    VariableState,
    VariantSummary,
)
from reg_meta.errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from reg_meta.fqid import (
    CLASSIFICATION_PREFIX,
    Fqid,
    FqidError,
    FqidKind,
    parse,
    period_token_for_bounds,
    validate_slug,
)
from reg_meta.queries import list_classifications

from reg_webapp.catalog_fqid import (
    FqidPathError,
    ValidatedFqidPath,
    validate_fqid_path,
)
from reg_webapp.conn import catalog_conn as _catalog_conn
from reg_webapp.models import (
    BindingChild,
    BindingGroupRefModel,
    BindingNode,
    CatalogNode,
    ClassificationChainEdition,
    ClassificationCodeModel,
    ClassificationNode,
    ClassificationRootNode,
    ClassificationRootResponse,
    ConceptGroupMemberModel,
    ConceptGroupModel,
    ConceptGroupNode,
    ConceptGroupNodeMember,
    DimensionsResponse,
    GroupFacetModel,
    LineageEdgeModel,
    LineageResponse,
    LineageWarningModel,
    LineageWarningsResponse,
    PredecessorsResponse,
    ProviderNode,
    ProviderResponse,
    RegisterChild,
    RegisterCoverageModel,
    RegisterNode,
    RegisterResponse,
    RelatedRefModel,
    RelatedResponse,
    RootResponse,
    StatesResponse,
    SuccessorsResponse,
    ValueSetMember,
    VariableCoverageModel,
    VariableEditionModel,
    VariableRefModel,
    VariableStateModel,
    VariantModel,
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


def _http_404_if_not_found(exc: RegMetaError) -> None:
    """Map reg_meta's genuine FQID-not-found to HTTP 404; re-raise anything else
    (a corrupt-DB / build-invariant break) so it surfaces as a generic 500 — its
    message may carry internal IDs, and it's a server fault, not a client 404."""
    if exc.exit_code == EXIT_NOT_FOUND and exc.code == _FQID_NOT_FOUND_CODE:
        raise HTTPException(status_code=404, detail=exc.message) from exc
    raise exc


# ── reg_meta dataclass → Pydantic mappers (1:1 wrappers; see DESIGN.md →
# Pydantic boundary) ───────────────────────────────────────────────────────

# `OPEN_ENDED_VALID_TO` (the open-ended `variable_state.valid_to` sentinel — an
# open window has no finite period token, #321) is now imported from
# `reg_meta.catalog`, the single source of the DDL-default constant (#351 added
# it there for the coverage aggregates).


def _state_model(state) -> VariableStateModel:
    return VariableStateModel(
        state_id=state.state_id,
        variant=state.variant,
        register_variant_id=state.register_variant_id,
        valid_from=state.valid_from,
        valid_to=state.valid_to,
        data_type=state.data_type,
        data_length=state.data_length,
        delivery_column_name=state.delivery_column_name,
        value_set_version_label=state.value_set_version_label,
        value_set_id=state.value_set_id,
        value_set=(
            [ValueSetMember(code=c, label=lbl) for c, lbl in state.value_set]
            if state.value_set is not None
            else None
        ),
        is_identifier=state.is_identifier,
        classification_slug=state.classification_slug,
        # #321: the coarsest exact display token for the window; None for an
        # open-ended state (the `9999-12-31` DDL-default sentinel — see
        # reg_meta_build db.py — has no finite token; the SPA renders "since
        # valid_from").
        period_token=(
            None
            if state.valid_to == OPEN_ENDED_VALID_TO
            else period_token_for_bounds(state.valid_from, state.valid_to)
        ),
    )


def _var_ref_model(ref) -> VariableRefModel:
    # Construct via the alias `register=` (the canonical init param); the Python
    # attr is `register_name` (avoids the BaseModel.register method shadow).
    return VariableRefModel(
        fqid=str(ref.fqid) if ref.fqid is not None else None,
        provider=ref.provider,
        register=ref.register,
        variable=ref.variable,
        reason=ref.reason,
        effective_year=ref.effective_year,
    )


def _related_ref_model(ref) -> RelatedRefModel:
    return RelatedRefModel(
        fqid=str(ref.fqid) if ref.fqid is not None else None,
        provider=ref.provider,
        register=ref.register,
        variable=ref.variable,
        relation_kind=ref.relation_kind,
    )


def _lineage_edge_model(edge) -> LineageEdgeModel:
    return LineageEdgeModel(
        consumer_state_id=edge.consumer_state_id,
        source_state_id=edge.source_state_id,
        valid_from=edge.valid_from,
        valid_to=edge.valid_to,
        source_fqid=str(edge.source_fqid) if edge.source_fqid is not None else None,
    )


def _lineage_warning_model(warning) -> LineageWarningModel:
    return LineageWarningModel(
        consumer_state_id=warning.consumer_state_id,
        warning_kind=warning.warning_kind,
        message=warning.message,
    )


def _variant_model(variant: VariantSummary) -> VariantModel:
    # A4.4c: VariantSummary.panel_entity_key / panel_time_key are
    # str | tuple | None; the wire model uses a JSON list for the composite case.
    entity_key = variant.panel_entity_key
    if isinstance(entity_key, tuple):
        entity_key = list(entity_key)
    time_key = variant.panel_time_key
    if isinstance(time_key, tuple):
        time_key = list(time_key)
    return VariantModel(
        slug=variant.slug,
        name=variant.name,
        description=variant.description,
        display_group=variant.display_group,
        panel_entity_key=entity_key,
        panel_time_key=time_key,
        panel_time_grain=variant.panel_time_grain,
    )


def _variable_chain_edition(edition) -> VariableEditionModel:
    """Map a `reg_meta.VariableEdition` (#582) 1:1 onto the wire model — one node of
    the full variable succession timeline. Every chain edition is a live `variable`
    row (the build validator guarantees succession editions are live), so `fqid` is
    None only when the triple is malformed/unresolvable, which the SPA renders as
    plain text, not a link. Constructs via the alias `register=` (the canonical init
    param; the Python attr is `register_name`, avoiding the BaseModel.register
    shadow — see `_var_ref_model`)."""
    return VariableEditionModel(
        fqid=str(edition.fqid) if edition.fqid is not None else None,
        provider=edition.provider,
        register=edition.register,
        variable=edition.variable,
        name=edition.name,
        effective_year=edition.effective_year,
        reason=edition.reason,
        is_current=edition.is_current,
        is_self=edition.is_self,
    )


def _binding_group_ref_model(ref) -> BindingGroupRefModel | None:
    """Map a reg_meta `BindingGroupRef` (#616) 1:1 onto the wire model — the
    binding's owning group as `(provider, register, key)`. None when the binding
    is ungrouped (`ResolvedVariable.group` is None). Constructs via the alias
    `register=` (the canonical init param; the Python attr is `register_name`,
    avoiding the BaseModel.register shadow — see `_var_ref_model`)."""
    if ref is None:
        return None
    return BindingGroupRefModel(
        provider=ref.provider, register=ref.register, key=ref.key
    )


def _binding_node(catalog: Catalog, resolved: ResolvedVariable) -> BindingNode:
    """Map a `ResolvedVariable` to the embedded-record leaf. Embeds the
    full wire-relevant record; the internal `provider_key` (SCB build-time join
    key, redundant with the FQID) is intentionally not exposed, and
    `lineage_warnings` are NOT on `ResolvedVariable` so they're omitted (A5.2
    `/lineage_warnings`).

    The FULL variable succession chain (#582) is embedded as `succession_chain` —
    resolved server-side (`Catalog.variable_chain`, same_as-canonicalized + walked
    terminal→predecessors) so the SPA renders the whole timeline synchronously,
    superseding the immediate-neighbor `replaced_by` embed. The `/predecessors` /
    `/successors` sub-resources are unaffected — they back the #411 redirect rails."""
    return BindingNode(
        fqid=str(resolved.fqid),
        variable_id=resolved.variable_id,
        register_id=resolved.register_id,
        name=resolved.name,
        definition=resolved.definition,
        description=resolved.description,
        measurement_unit=resolved.measurement_unit,
        is_sensitive=resolved.is_sensitive,
        is_identifier=resolved.is_identifier,
        source_register_id=resolved.source_register_id,
        source_register_text=resolved.source_register_text,
        states=[_state_model(s) for s in resolved.states],
        same_as=[_var_ref_model(r) for r in resolved.same_as],
        succession_chain=[
            _variable_chain_edition(e) for e in catalog.variable_chain(resolved.fqid)
        ],
        related_to=[_related_ref_model(r) for r in resolved.related_to],
        lineage=[_lineage_edge_model(e) for e in resolved.lineage],
        # #616/#617: the binding's owning group as `(provider, register, key)` so a
        # member page links to the group subject without a second fetch; None when
        # ungrouped. Keyed on the RESOLVED variable's triple, so a same_as alias
        # reports its target's group (reg_meta sets it on `ResolvedVariable.group`).
        group=_binding_group_ref_model(resolved.group),
        via_same_as=(
            [str(f) for f in resolved.via_same_as]
            if resolved.via_same_as is not None
            else None
        ),
    )


def _classification_chain_edition(edition) -> ClassificationChainEdition:
    """Map a `reg_meta.ClassificationEdition` (#571) 1:1 onto the wire model — one
    node of the full succession timeline. Every chain edition is a live
    `classification` row (the build validator guarantees succession editions are
    live), so `fqid` is None only when the slug is malformed/unresolvable, which the
    SPA renders as plain text, not a link."""
    return ClassificationChainEdition(
        slug=edition.slug,
        fqid=str(edition.fqid) if edition.fqid is not None else None,
        name=edition.name,
        effective_year=edition.effective_year,
        is_current=edition.is_current,
        is_self=edition.is_self,
    )


def _classification_code_model(code) -> ClassificationCodeModel:
    """Map a `reg_meta.ClassificationCode` (#609) 1:1 onto the wire model — one
    code/label entry of the resolved edition's value set (these are PUBLIC
    classification codes, not row-level data). `is_valid` is passed through as
    canonical/observed/unknown (True/False/None)."""
    return ClassificationCodeModel(
        code=code.code,
        label=code.label,
        level=code.level,
        is_valid=code.is_valid,
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
    table)."""
    return ClassificationNode(
        fqid=str(resolved.fqid),
        short_name=resolved.short_name,
        name=resolved.name,
        via_same_as=(
            [str(f) for f in resolved.via_same_as]
            if resolved.via_same_as is not None
            else None
        ),
        edition_chain=[
            _classification_chain_edition(e)
            for e in catalog.classification_chain(resolved.fqid)
        ],
        codes=[
            _classification_code_model(c)
            for c in catalog.classification_codes(resolved.fqid)
        ],
        dimensions=[
            _concept_group_model(g)
            for g in catalog.classification_dimensions(resolved.fqid)
        ],
    )


def _concept_group_model(group) -> ConceptGroupModel:
    """Map a reg_meta `ConceptGroupSummary` (#303) 1:1 onto the wire model."""
    return ConceptGroupModel(
        key=group.key,
        label=group.label,
        source=group.source,
        axes=list(group.axes),
        members=[
            ConceptGroupMemberModel(
                fqid=str(m.fqid),
                name=m.name,
                facets=[
                    GroupFacetModel(axis=f.axis, value=f.value, label=f.label)
                    for f in m.facets
                ],
            )
            for m in group.members
        ],
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

    Coverage reuses the SAME `register_variable_coverage` map the register
    listing uses (keyed by variable SLUG — the binding-FQID leaf segment), so no
    new reg_meta accessor is needed: a member's coverage is its leaf-slug lookup
    in that map (None for a stateless member or a leaf that isn't in the map —
    defensive). `member_hint` is the validated `?member=` focus slug, echoed for
    the SPA to highlight (None when absent/unrecognized — a bad hint is ignored,
    keeping the group page first-class)."""
    coverage = catalog.register_variable_coverage(provider_slug, register_slug)
    members: list[ConceptGroupNodeMember] = []
    for m in group.members:
        # The member FQID's leaf segment IS its variable slug — the key
        # `register_variable_coverage` returns (mirrors `_register_response`).
        leaf_slug = str(m.fqid).rsplit("/", 1)[-1]
        members.append(
            ConceptGroupNodeMember(
                fqid=str(m.fqid),
                name=m.name,
                facets=[
                    GroupFacetModel(axis=f.axis, value=f.value, label=f.label)
                    for f in m.facets
                ],
                coverage=_variable_coverage_model(coverage.get(leaf_slug)),
            )
        )
    return ConceptGroupNode(
        provider=provider_slug,
        register=register_slug,
        key=group.key,
        label=group.label,
        source=group.source,
        axes=list(group.axes),
        members=members,
        member=member_hint,
    )


def _register_coverage_model(cov) -> RegisterCoverageModel | None:
    if cov is None:
        return None
    return RegisterCoverageModel(
        variable_count=cov.variable_count,
        coverage_from=cov.coverage_from,
        coverage_to=cov.coverage_to,
        open_ended=cov.open_ended,
    )


def _variable_coverage_model(cov) -> VariableCoverageModel | None:
    if cov is None:
        return None
    return VariableCoverageModel(
        coverage_from=cov.coverage_from,
        coverage_to=cov.coverage_to,
        open_ended=cov.open_ended,
        state_count=cov.state_count,
    )


def _provider_response(
    catalog: Catalog, resolved: ResolvedProvider
) -> ProviderResponse:
    provider_slug = resolved.fqid.provider
    assert provider_slug is not None
    registers = catalog.list_registers(provider_slug)
    # #351 per-register coverage, keyed by register slug — one GROUP BY (~40 ms
    # for scb's 238 registers), query-time behind the ETag/edge cache.
    coverage = catalog.provider_register_coverage(provider_slug)
    return ProviderResponse(
        fqid=str(resolved.fqid),
        name=resolved.name,
        children=[
            RegisterNode(
                fqid=str(r.fqid),
                name=r.name,
                purpose=r.purpose,
                # r.fqid.register is always set for a register summary; the guard
                # keeps the dict key str-typed.
                coverage=_register_coverage_model(
                    coverage.get(r.fqid.register) if r.fqid.register else None
                ),
            )
            for r in registers
        ],
    )


def _register_response(
    catalog: Catalog, resolved: ResolvedRegister
) -> RegisterResponse:
    provider_slug = resolved.fqid.provider
    register_slug = resolved.fqid.register
    assert provider_slug is not None and register_slug is not None
    bindings = catalog.list_bindings(provider_slug, register_slug)
    # #351 per-variable coverage, keyed by variable slug — one GROUP BY (~9 ms on
    # the worst real register, scb/ulf 7.3k vars), query-time behind ETag.
    coverage = catalog.register_variable_coverage(provider_slug, register_slug)
    # A register's children are its bindings PLUS a `variants` reference
    # stub (the declared A5.2 variant-browser slot — a link, not data).
    children: list[RegisterChild] = [
        BindingChild(
            fqid=str(b.fqid),
            name=b.name,
            # b.fqid.variable is always set for a binding summary; the guard keeps
            # the dict key str-typed.
            coverage=_variable_coverage_model(
                coverage.get(b.fqid.variable) if b.fqid.variable else None
            ),
        )
        for b in bindings
    ]
    children.append(VariantsRef(register_fqid=str(resolved.fqid)))
    return RegisterResponse(
        fqid=str(resolved.fqid),
        name=resolved.name,
        purpose=resolved.purpose,
        children=children,
        # #303 concept groups: grouped bindings ALSO stay in `children` (the
        # flat list is complete); the SPA folds members under the group rows.
        groups=[
            _concept_group_model(g)
            for g in catalog.list_concept_groups(provider_slug, register_slug)
        ],
    )


def _classification_root_response(
    conn: sqlite3.Connection,
) -> ClassificationRootResponse:
    """The `class` (1 seg) classification-root: the CURRENT/TERMINAL
    classifications as children, plus the #303/#516 umbrella groups. The
    CHILDREN list still reuses `reg_meta.queries.list_classifications` (LOCKED
    — the children enumeration grew no Catalog method); the GROUPS come from
    `Catalog.list_classification_groups`, the reg_meta-owned read surface for
    the concept-group layer — a `Catalog(conn)` wrapper over the request
    connection. The wrapper is construction-only (no connection ownership);
    `close()` is never called on it — the connection stays owned by the
    handler's `_catalog_conn` contextmanager. A classification with a NULL
    slug isn't FQID-addressable, so it's excluded from children and group
    members alike (symmetric with `list_registers`'s slug filter).

    Only TERMINAL editions surface as children: a row whose `superseded_by`
    is set (a successor exists) is a superseded edition and is dropped here.
    Superseded editions are reached by drilling into a terminal leaf's
    edition-chain panel (ClassificationLineagePanels, incl. the #605 split-root
    fan-out) or by direct URL. This is generic — lkf (47 editions) / ssyk / sni
    each collapse to their current edition too. Group members are themselves
    terminal (the 2020 SUN editions + the version-independent nivå aggregates),
    so they stay in `children` and the SPA folds them under the group row."""
    rows = list_classifications(conn)
    children: list[ClassificationNode] = []
    for row in rows:
        slug = row.get("slug")
        if not slug:
            continue
        # superseded_by is a GROUP_CONCAT of successor short_names; truthy ⇒ a
        # newer edition supersedes this one ⇒ not a current edition, skip it.
        if row.get("superseded_by"):
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
    groups = [
        _concept_group_model(g) for g in Catalog(conn).list_classification_groups()
    ]
    return ClassificationRootResponse(children=children, groups=groups)


def _catalog_url(fqid: Fqid) -> str:
    """The canonical catalog API URL for a terminal FQID — `/api/catalog/<path>`
    with each path segment percent-encoded (#355 PART 2 redirect target). Mirrors
    the frontend `encodeFqid` intent: split the FQID string on `/`, `quote` each
    segment, rejoin on `/`. A no-op for valid slugs (they have no reserved chars),
    but correct/defensive — and `quote` does NOT touch `/`, so the segments stay
    separate."""
    path = "/".join(urllib.parse.quote(seg) for seg in str(fqid).split("/"))
    return f"/api/catalog/{path}"


def _resolve_to_node(catalog: Catalog, fqid: Fqid) -> CatalogNode:
    """Dispatch a parsed FQID to its Catalog resolver and map to a Pydantic node.
    Raises 404 (via `_http_404_if_not_found`) when the FQID resolves to nothing."""
    try:
        resolved = catalog.resolve(fqid)
    except RegMetaError as exc:
        _http_404_if_not_found(exc)
        raise  # unreachable; _http_404_if_not_found re-raises non-404s
    if isinstance(resolved, ResolvedProvider):
        return _provider_response(catalog, resolved)
    if isinstance(resolved, ResolvedRegister):
        return _register_response(catalog, resolved)
    if isinstance(resolved, ResolvedVariable):
        return _binding_node(catalog, resolved)
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
    if exc.exit_code == EXIT_NOT_FOUND and exc.code == _FQID_NOT_FOUND_CODE:
        raise HTTPException(status_code=404, detail=exc.message) from exc
    if exc.exit_code == EXIT_USAGE:
        raise HTTPException(status_code=422, detail=exc.message) from exc
    raise exc


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
    so the caller can `return` it. Shares the SAME not-found predicate as
    `_http_4xx_from_regmeta` (only `fqid_not_found` redirects; a 422 usage error or a
    500 build-invariant break NEVER becomes a redirect).

    SIBLING: the no-period node path in `get_catalog_node` implements the SAME 301
    successor policy for the already-mapped `HTTPException` layer — keep the two in
    sync (e.g. a 301→308 switch or a redirect header must land in both)."""
    is_not_found = exc.exit_code == EXIT_NOT_FOUND and exc.code == _FQID_NOT_FOUND_CODE
    if is_not_found:
        terminal = catalog.resolve_terminal_successor(parsed)
        if terminal is not None:
            target = f"{_catalog_url(terminal)}{suffix}"
            if request.url.query:
                target = f"{target}?{request.url.query}"
            return RedirectResponse(target, status_code=301)
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
    """The catalog root: every provider plus the classification-root sentinel."""
    with _catalog_conn(request) as conn:
        children: list[ProviderNode | ClassificationRootNode] = [
            ProviderNode(fqid=str(p.fqid), name=p.name)
            for p in Catalog(conn).list_providers()
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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        # Resolve the register first so a bad (provider, register) is a 404, not a
        # 200 with an empty list (list_variants alone can't distinguish them).
        try:
            catalog.resolve(fqid)
        except RegMetaError as exc:
            _http_404_if_not_found(exc)
        variants = catalog.list_variants(provider, register)
    # Construct via the alias `register=` (the canonical init param; the Python
    # attr is `register_name` to avoid the BaseModel.register method shadow).
    return VariantsResponse(
        register=register_fqid,
        variants=[_variant_model(v) for v in variants],
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
    "/catalog/group/{provider}/{register}/{key}", response_model=ConceptGroupNode
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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        group = catalog.concept_group(provider, register, key)
        if group is None:
            raise HTTPException(
                status_code=404,
                detail=f"no concept group {key!r} in {provider}/{register}",
            )
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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        try:
            states = catalog.states(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/states")
    return StatesResponse(binding=str(parsed), states=[_state_model(s) for s in states])


@router.get("/catalog/{fqid:path}/predecessors", response_model=PredecessorsResponse)
def get_binding_predecessors(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> PredecessorsResponse | RedirectResponse:
    """Variables this binding's variable replaced (inbound succession). A
    dead/renamed binding 301s to `/predecessors` on its terminal successor (#411)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        try:
            refs = catalog.predecessors(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(
                catalog, parsed, exc, request, suffix="/predecessors"
            )
    return PredecessorsResponse(
        binding=str(parsed), predecessors=[_var_ref_model(r) for r in refs]
    )


@router.get("/catalog/{fqid:path}/successors", response_model=SuccessorsResponse)
def get_binding_successors(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> SuccessorsResponse | RedirectResponse:
    """Variables that replaced this binding's variable (outbound succession). A
    dead/renamed binding 301s to `/successors` on its terminal successor (#411)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        try:
            refs = catalog.successors(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/successors")
    return SuccessorsResponse(
        binding=str(parsed), successors=[_var_ref_model(r) for r in refs]
    )


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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        try:
            groups = catalog.dimensions(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/dimensions")
    return DimensionsResponse(
        binding=str(parsed), dimensions=[_concept_group_model(g) for g in groups]
    )


@router.get("/catalog/{fqid:path}/related", response_model=RelatedResponse)
def get_binding_related(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> RelatedResponse | RedirectResponse:
    """Split-sibling variables (variable grain — see reg_meta_build/DESIGN.md →
    Build-time triage (SCB)). A dead/renamed binding 301s to `/related` on its
    terminal successor (#411)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        try:
            refs = catalog.related(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/related")
    return RelatedResponse(
        binding=str(parsed), related=[_related_ref_model(r) for r in refs]
    )


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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        try:
            edges = catalog.lineage(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/lineage")
    return LineageResponse(
        binding=str(parsed), lineage_edges=[_lineage_edge_model(e) for e in edges]
    )


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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        try:
            warnings = catalog.lineage_warnings(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(
                catalog, parsed, exc, request, suffix="/lineage_warnings"
            )
    return LineageWarningsResponse(
        binding=str(parsed),
        lineage_warnings=[_lineage_warning_model(w) for w in warnings],
    )


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
            return StatesResponse(
                binding=str(parsed),
                states=[_state_model(s) for s in states_by_id.values()],
            )

    with _catalog_conn(request) as conn:
        catalog = Catalog(conn)
        try:
            return _resolve_to_node(catalog, parsed)
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
