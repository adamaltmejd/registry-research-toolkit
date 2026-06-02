"""`GET /api/catalog` browse (§9.5) — the canonical catalog endpoint.

Two routes:

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

**§16 guard runs BEFORE any DB access.** Every catch-all request first runs
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

from contextlib import contextmanager
from typing import TYPE_CHECKING

import reg_meta.db
from fastapi import APIRouter, Depends, HTTPException, Request
from reg_meta.catalog import (
    Catalog,
    Period,
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
    VariantSummary,
)
from reg_meta.errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from reg_meta.fqid import (
    CLASSIFICATION_PREFIX,
    Fqid,
    FqidError,
    FqidKind,
    parse,
)
from reg_meta.queries import list_classifications

from reg_webapp.catalog_fqid import (
    FqidPathError,
    ValidatedFqidPath,
    validate_fqid_path,
)
from reg_webapp.models import (
    BindingChild,
    BindingNode,
    CatalogNode,
    ClassificationNode,
    ClassificationRootNode,
    ClassificationRootResponse,
    LineageEdgeModel,
    LineageResponse,
    LineageWarningModel,
    LineageWarningsResponse,
    PredecessorsResponse,
    ProviderNode,
    ProviderResponse,
    RegisterChild,
    RegisterNode,
    RegisterResponse,
    RelatedRefModel,
    RelatedResponse,
    RootResponse,
    StatesResponse,
    SuccessorsResponse,
    ValueSetMember,
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
    parse_period,
    parse_value_set_version,
    parse_variant,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator

router = APIRouter(prefix="/api")


@contextmanager
def _catalog_conn(request: Request) -> Iterator[sqlite3.Connection]:
    """A per-request reg_meta read-only connection, opened ON THE CALLING THREAD.

    Used as a plain ``with`` INSIDE the sync route handler — NOT a FastAPI
    ``Depends``. A sync endpoint's generator *dependency* is entered via the AnyIO
    threadpool on a possibly-DIFFERENT thread than the handler runs on, so a
    dependency-opened sqlite connection (default ``check_same_thread=True``) gets
    used cross-thread → intermittent ``sqlite3.ProgrammingError`` under concurrency
    (Codex P1 on #168, reproduced 72/80 before this fix). Opening within the
    handler body keeps open + query + close on one thread. ``check_schema=False``:
    the lifespan already validated the schema at boot."""
    conn = reg_meta.db.open_db(request.app.state.db_path, check_schema=False)
    try:
        yield conn
    finally:
        conn.close()


def _validated_fqid(fqid: str) -> ValidatedFqidPath:
    """The §16 allow-list as a dependency — FastAPI resolves it before the handler
    body runs, so a malformed / traversal-shaped path returns 422 **before** the
    handler opens any connection (no DB hit at all, not just no SQL). It holds no
    connection itself, so it's safe across the threadpool. Reused by A5.2's
    suffixed routes."""
    try:
        return validate_fqid_path(fqid)
    except FqidPathError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validated_period(period: str | None = None) -> Period | None:
    """§16 ``?period`` allow-list as a pre-open dependency — FastAPI resolves it
    before the handler body, so a malformed period (SQLi / traversal / NUL /
    percent-encoded) returns 422 **before** any connection opens (zero SQL, zero
    opens). Holds no connection, so it's threadpool-safe. ``None`` (no query)
    means "no period filter" — distinct from the parsed ``_default`` sentinel,
    but the catch-all treats an absent ``?period`` as a plain (no-period) resolve,
    not a `resolve_at`. reg_meta's ``_period_bounds`` is the SEMANTIC backstop."""
    if period is None:
        return None
    try:
        return parse_period(period)
    except PeriodParamError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validated_variant(variant: str | None = None) -> str | None:
    """§16 ``?variant`` allow-list as a pre-open dependency. ADMITS ``_default``
    (a real register_variant slug, §5.1) unlike the path guard. 422s a non-slug
    value before any connection opens (zero SQL, zero opens)."""
    if variant is None:
        return None
    try:
        return parse_variant(variant)
    except VariantParamError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validated_value_set_version(value_set_version: str | None = None) -> str | None:
    """§16 ``?value_set_version`` allow-list as a pre-open dependency. The value
    is a FREE-TEXT value-set-version label (matched against ``value_set_version_label``
    by a Python filter in ``resolve_at``, NOT SQL), so the gate is a sanity check
    (non-empty, length-capped, no control chars) — 422s a malformed value before
    any connection opens. Reconciled with the binding-leaf ``@version`` pin in the
    handler (`_reconcile_value_set_version`)."""
    if value_set_version is None:
        return None
    try:
        return parse_value_set_version(value_set_version)
    except ValueSetVersionParamError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


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


# ── reg_meta dataclass → Pydantic mappers (§9.6 1:1 wrappers) ──────────────


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
    # A4.4c: VariantSummary.panel_entity_key is str | tuple | None; the wire
    # model uses a JSON list for the composite case.
    entity_key = variant.panel_entity_key
    if isinstance(entity_key, tuple):
        entity_key = list(entity_key)
    return VariantModel(
        slug=variant.slug,
        name=variant.name,
        description=variant.description,
        display_group=variant.display_group,
        panel_entity_key=entity_key,
        panel_time_key=variant.panel_time_key,
        panel_time_grain=variant.panel_time_grain,
    )


def _binding_node(resolved: ResolvedVariable) -> BindingNode:
    """Map a `ResolvedVariable` to the embedded-record leaf (§9.5). Embeds the
    full wire-relevant record; the internal `provider_key` (SCB build-time join
    key, redundant with the FQID) is intentionally not exposed, and
    `lineage_warnings` are NOT on `ResolvedVariable` so they're omitted (A5.2
    `/lineage_warnings`)."""
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
        replaced_by=[_var_ref_model(r) for r in resolved.replaced_by],
        related_to=[_related_ref_model(r) for r in resolved.related_to],
        lineage=[_lineage_edge_model(e) for e in resolved.lineage],
        via_same_as=(
            [str(f) for f in resolved.via_same_as]
            if resolved.via_same_as is not None
            else None
        ),
    )


def _classification_node(resolved: ResolvedClassification) -> ClassificationNode:
    return ClassificationNode(
        fqid=str(resolved.fqid),
        short_name=resolved.short_name,
        name=resolved.name,
        via_same_as=(
            [str(f) for f in resolved.via_same_as]
            if resolved.via_same_as is not None
            else None
        ),
    )


def _provider_response(
    catalog: Catalog, resolved: ResolvedProvider
) -> ProviderResponse:
    provider_slug = resolved.fqid.provider
    assert provider_slug is not None
    registers = catalog.list_registers(provider_slug)
    return ProviderResponse(
        fqid=str(resolved.fqid),
        name=resolved.name,
        children=[
            RegisterNode(fqid=str(r.fqid), name=r.name, purpose=r.purpose)
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
    # §9.5: a register's children are its bindings PLUS a `variants` reference
    # stub (the declared A5.2 variant-browser slot — a link, not data).
    children: list[RegisterChild] = [
        BindingChild(fqid=str(b.fqid), name=b.name) for b in bindings
    ]
    children.append(VariantsRef(register_fqid=str(resolved.fqid)))
    return RegisterResponse(
        fqid=str(resolved.fqid),
        name=resolved.name,
        purpose=resolved.purpose,
        children=children,
    )


def _classification_root_response(
    conn: sqlite3.Connection,
) -> ClassificationRootResponse:
    """The `class` (1 seg) classification-root: every classification as children.
    Reuses `reg_meta.queries.list_classifications` (LOCKED — no new Catalog
    method); the catch-all hands it the request connection directly, so there's
    no reach into `Catalog`'s private `_conn`. A classification with a NULL slug
    isn't FQID-addressable, so it's excluded from the browse children (symmetric
    with `list_registers`'s slug filter)."""
    rows = list_classifications(conn)
    children: list[ClassificationNode] = []
    for row in rows:
        slug = row.get("slug")
        if not slug:
            continue
        children.append(
            ClassificationNode(
                fqid=str(Fqid.classification_fqid(slug)),
                short_name=row["short_name"],
                name=row["name"],
            )
        )
    return ClassificationRootResponse(children=children)


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
        return _binding_node(resolved)
    if isinstance(resolved, ResolvedClassification):
        return _classification_node(resolved)
    # Unreachable: resolve() returns only the four ResolvedEntity arms.
    raise HTTPException(
        status_code=500, detail="unknown catalog entity"
    )  # pragma: no cover


def _reconcile_value_set_version(pinned: str | None, query: str | None) -> str | None:
    """Reconcile the binding-leaf `@version` pin (parsed into
    `ValidatedFqidPath.value_set_version`) with the `?value_set_version` query.

    LOCKED: if BOTH present and they DIFFER → 422 (ambiguous); if both present
    and equal, or only one present → use it; neither → None. The conflict is a
    422 (a client contradiction), not a silent precedence — pinning the same
    version two ways is fine, two different ones is a usage error."""
    if pinned is not None and query is not None and pinned != query:
        raise HTTPException(
            status_code=422,
            detail=(
                f"ambiguous value-set-version: FQID pin @{pinned} "
                f"conflicts with ?value_set_version={query}"
            ),
        )
    return pinned if pinned is not None else query


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


def _parsed_binding(validated: ValidatedFqidPath) -> Fqid:
    """Parse a validated path into an Fqid, mapping a grammar/arity FqidError to
    422 (DB-free — runs before any connection opens). Used by the suffixed
    sub-endpoints, which only accept binding FQIDs (reg_meta's `_parse_binding`
    raises the 422-mapped `not_a_binding_fqid` for a non-binding kind).

    A binding-leaf `@version` pin is REJECTED here (422): the suffixed endpoints
    return the FULL state history / edge set and do NOT narrow by value-set-version,
    so a pin would silently no-op — the same inert-modifier surface the catch-all
    422s. Version-narrowing lives only on the catch-all leaf (`?period` + the
    reconciled `?value_set_version`)."""
    if validated.value_set_version is not None:
        raise HTTPException(
            status_code=422,
            detail=(
                "an @version pin is not supported on this endpoint; value-set-version "
                "narrowing is on the catalog leaf with ?period"
            ),
        )
    try:
        return parse(validated.fqid)
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


# ── Routes ─────────────────────────────────────────────────────────────────
# §9.5 router ordering: the suffixed sub-resource routes (`/states`, ...,
# `/lineage_warnings`) and the register-sub-resource `/{provider}/{register}/
# variants` MUST be declared ABOVE the `{fqid:path}` catch-all — Starlette
# matches in declaration order and the `{fqid:path}` converter greedy-consumes
# any suffix into `fqid`. The catch-all MUST stay last. `test_boot.py`
# (§9.5 `routes_declared_before`) pins the order in CI.


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


# The register-sub-resource variant browser (§9.5). A FIXED 3-seg shape with a
# literal `variants` tail — NOT an `{fqid:path}` suffix — so it's declared with
# explicit `{provider}`/`{register}` segments, ABOVE the catch-all. The two
# segments are §16-guarded as slugs (reusing the path guard on the 2-seg register
# FQID) before any connection opens.
@router.get("/catalog/{provider}/{register}/variants", response_model=VariantsResponse)
def get_register_variants(
    request: Request, provider: str, register: str
) -> VariantsResponse:
    """List a register's variants (the `?variant=` browse axis, §9.5). `_default`
    is a real variant and IS returned (not filtered). 404 when the register
    doesn't resolve (so a typo'd register isn't a silent empty list)."""
    register_fqid = f"{provider}/{register}"
    # §16: validate both segments BEFORE opening a connection — as a strict
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


# ── The 6 binding-suffix sub-endpoints (§9.5) — ALL above the catch-all. ────
# Each follows the LOCKED connection model: §16 guard (`_validated_fqid`) +
# `parse` run BEFORE the connection opens; the connection is opened and used
# within the sync body (one thread — see `_catalog_conn`). reg_meta's accessor
# raises `not_a_binding_fqid` (→ 422) for a non-binding FQID and `_not_found`
# (→ 404) for an absent binding, both mapped by `_http_4xx_from_regmeta`.


@router.get("/catalog/{fqid:path}/states", response_model=StatesResponse)
def get_binding_states(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> StatesResponse:
    """Full state history for a binding (§9.5). ≡ the leaf's embedded `states`,
    standalone. Same shape the `?period` catch-all returns (codegen sees one
    state-list type)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        try:
            states = Catalog(conn).states(parsed)
        except RegMetaError as exc:
            _http_4xx_from_regmeta(exc)
    return StatesResponse(binding=str(parsed), states=[_state_model(s) for s in states])


@router.get("/catalog/{fqid:path}/predecessors", response_model=PredecessorsResponse)
def get_binding_predecessors(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> PredecessorsResponse:
    """Variables this binding's variable replaced (inbound succession, §9.5)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        try:
            refs = Catalog(conn).predecessors(parsed)
        except RegMetaError as exc:
            _http_4xx_from_regmeta(exc)
    return PredecessorsResponse(
        binding=str(parsed), predecessors=[_var_ref_model(r) for r in refs]
    )


@router.get("/catalog/{fqid:path}/successors", response_model=SuccessorsResponse)
def get_binding_successors(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> SuccessorsResponse:
    """Variables that replaced this binding's variable (outbound succession)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        try:
            refs = Catalog(conn).successors(parsed)
        except RegMetaError as exc:
            _http_4xx_from_regmeta(exc)
    return SuccessorsResponse(
        binding=str(parsed), successors=[_var_ref_model(r) for r in refs]
    )


@router.get("/catalog/{fqid:path}/related", response_model=RelatedResponse)
def get_binding_related(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> RelatedResponse:
    """Split-sibling variables (variable grain, §5.7)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        try:
            refs = Catalog(conn).related(parsed)
        except RegMetaError as exc:
            _http_4xx_from_regmeta(exc)
    return RelatedResponse(
        binding=str(parsed), related=[_related_ref_model(r) for r in refs]
    )


@router.get("/catalog/{fqid:path}/lineage", response_model=LineageResponse)
def get_binding_lineage(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> LineageResponse:
    """Consumer-side composite lineage edges (state grain, §5.6). Maps what
    reg_meta's `LineageEdge` carries; the §9.5 richer per-source-state shape is a
    possible reg_meta enhancement (not blocked on here — see DESIGN.md)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        try:
            edges = Catalog(conn).lineage(parsed)
        except RegMetaError as exc:
            _http_4xx_from_regmeta(exc)
    return LineageResponse(
        binding=str(parsed), lineage_edges=[_lineage_edge_model(e) for e in edges]
    )


@router.get(
    "/catalog/{fqid:path}/lineage_warnings", response_model=LineageWarningsResponse
)
def get_binding_lineage_warnings(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> LineageWarningsResponse:
    """Build-time lineage warnings for the binding (§5.6). Empty when lineage
    resolved cleanly. The leaf does NOT embed these — this is their endpoint."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        try:
            warnings = Catalog(conn).lineage_warnings(parsed)
        except RegMetaError as exc:
            _http_4xx_from_regmeta(exc)
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
    period: Period | None = Depends(_validated_period),
    variant: str | None = Depends(_validated_variant),
    value_set_version: str | None = Depends(_validated_value_set_version),
) -> CatalogNode | StatesResponse:
    """Resolve any catalog node by FQID path; on a binding leaf, an optional
    `?period` (with `?variant` / `?value_set_version`) narrows to the resolve_at
    state subset.

    The §16 guards (`_validated_fqid` for the path, `_validated_period` /
    `_validated_variant` for the queries) run as dependencies, BEFORE this body —
    so a malformed path OR a malformed period/variant returns 422 **before** any
    connection opens (zero SQL, zero opens). `parse` is DB-free and runs before
    the open too. The classification-root literal `class` (1 seg) is special-cased
    before `parse`.

    `?period` semantics (§9.5): present + binding leaf → `{states: [...]}` (the
    resolve_at subset, narrowed by `?variant` / `?value_set_version`; the leaf's
    `@version` pin reconciles with `?value_set_version` — equal/one-sided uses it,
    conflicting is 422). present + non-binding kind → IGNORED (resolve normally).
    absent on a binding leaf → the full node (full history) UNLESS a narrowing
    modifier (`?variant` / `?value_set_version` / `@version`) is set: those are inert
    without `?period`, so they 422 ("requires ?period") rather than silently no-op.
    absent on a non-binding kind → the full node. The connection is opened and used
    within this sync body (one thread — see `_catalog_conn`).
    """
    # §5.2: `class` (1 seg) is the classification-root sentinel — a reserved slug
    # `parse` rejects, so special-case it BEFORE parse. `class/<slug>` (2 seg)
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

    # LOCKED: the binding-leaf `@version` pin reconciles with `?value_set_version`
    # — a CONFLICT (both present, different) is 422 (ambiguous), regardless of
    # `?period`. Run it before the connection opens, so the 422 costs no SQL.
    vsv = _reconcile_value_set_version(validated.value_set_version, value_set_version)

    # A `?period` query on a binding leaf returns the resolve_at state subset
    # (uniform with `/states`), narrowed by `?variant` / the reconciled `vsv`. On
    # any other kind `?period` is IGNORED (§9.5).
    if parsed.kind is FqidKind.VARIABLE_BINDING:
        if period is None:
            # `?variant` / `?value_set_version` / the `@version` pin are MODIFIERS
            # of the resolve_at narrowing — inert without `?period`. Require
            # `?period` rather than silently no-op (the param narrows-or-422s
            # everywhere else, so a silent no-op here is a surprising surface).
            # Maintainer call: vsv/@version → 422; extended to `?variant` for the
            # identical inert-modifier surface. 422s before the connection opens.
            if vsv is not None or variant is not None:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "?variant, ?value_set_version, and the @version pin narrow "
                        "the resolve_at state subset and require ?period"
                    ),
                )
        else:
            # The `_none` sentinel selects the empty/default label (`''`): the
            # empty string can't ride in the query (≡ absent), so map it here,
            # just before resolve_at's Python `label == value_set_version` filter.
            resolved_vsv = "" if vsv == VALUE_SET_VERSION_NONE else vsv
            with _catalog_conn(request) as conn:
                try:
                    states = Catalog(conn).resolve_at(
                        parsed,
                        period,
                        variant=variant,
                        value_set_version=resolved_vsv,
                    )
                except RegMetaError as exc:
                    _http_4xx_from_regmeta(exc)
            return StatesResponse(
                binding=str(parsed), states=[_state_model(s) for s in states]
            )

    with _catalog_conn(request) as conn:
        return _resolve_to_node(Catalog(conn), parsed)
