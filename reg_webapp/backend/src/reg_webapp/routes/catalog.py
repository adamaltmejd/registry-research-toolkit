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
state races), so the `_catalog` dependency opens a FRESH read-only connection per
request from the boot-resolved `app.state.db_path`, wraps it in a `Catalog`, and
closes it in a `finally`. The connection is owned by the handling thread
(`check_same_thread` default True) — correct. No long-lived shared connection,
no lock, no `check_same_thread=False`. The schema was already validated at boot
(`open_db` in the lifespan), so the per-request open skips the re-check
(`check_schema=False`).

**§16 guard runs BEFORE any DB access.** Every catch-all request first runs
`validate_fqid_path` (the per-segment slug-grammar allow-list, own module
`catalog_fqid.py`); a rejection raises 422 with zero SQL executed, because the
guard precedes the `_catalog` Catalog dispatch.

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
    ResolvedClassification,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
)
from reg_meta.errors import EXIT_NOT_FOUND, RegMetaError
from reg_meta.fqid import CLASSIFICATION_PREFIX, Fqid, FqidError, parse
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
    ProviderNode,
    ProviderResponse,
    RegisterChild,
    RegisterNode,
    RegisterResponse,
    RelatedRefModel,
    RootResponse,
    ValueSetMember,
    VariableRefModel,
    VariableStateModel,
    VariantsRef,
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


# ── Routes ─────────────────────────────────────────────────────────────────
# A5.2 suffixed routes (`/catalog/{fqid:path}/states`, `/predecessors`, ...,
# `/catalog/{provider}/{register}/variants`) go ABOVE this line; the catch-all
# MUST stay last (Starlette matches in declaration order, and the `{fqid:path}`
# converter greedy-consumes any suffix into `fqid`).


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


# The catch-all — MUST be the last route declared in this router (see seam above).
@router.get("/catalog/{fqid:path}", response_model=CatalogNode)
def get_catalog_node(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> CatalogNode:
    """Resolve any catalog node by FQID path.

    The §16 per-segment allow-list runs as the `_validated_fqid` dependency, which
    FastAPI resolves before this body — so a malformed / traversal-shaped path
    returns 422 **before** any connection opens (no DB hit at all). `parse` is
    DB-free and runs BEFORE the connection opens too, so a grammar/arity-invalid
    path (e.g. a reserved literal in an illegal slot) also 422s with no open. The
    classification-root literal `class` (1 seg) is special-cased before `parse`.
    `@version` is validated but not yet narrowing (A5.2 `?value_set_version`); the
    bare 3-seg FQID is handed to `parse`/`resolve`. The connection is opened and
    used within this sync body (one thread — see `_catalog_conn`).
    """
    # §5.2: `class` (1 seg) is the classification-root sentinel — a reserved slug
    # `parse` rejects, so special-case it BEFORE parse. `class/<slug>` (2 seg)
    # flows through `parse` as a normal classification FQID.
    if validated.fqid == CLASSIFICATION_PREFIX:
        with _catalog_conn(request) as conn:
            return _classification_root_response(conn)

    try:
        # `parse` is DB-free, so a grammar/arity 422 here costs no connection.
        parsed = parse(validated.fqid)
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    with _catalog_conn(request) as conn:
        return _resolve_to_node(Catalog(conn), parsed)
