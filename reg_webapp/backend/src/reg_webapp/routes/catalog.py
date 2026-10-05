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
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Path as PathParameter,
    Query,
    Request,
)
from fastapi.responses import RedirectResponse
from reg_meta.catalog import (
    Catalog,
    DataWarning,
    Period,
    RegisterCoverage,
    ResolvedProvider,
    ResolvedRegister,
    ResolvedVariable,
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
from reg_meta.graph import RelationshipGraph
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
    ValueSetCodesResponse,
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
from reg_webapp.query_input import clamp_limit, matches_filter, validate_text_query
from reg_webapp.scope import browse_scope

if TYPE_CHECKING:
    import sqlite3


router = APIRouter(prefix="/api", dependencies=[Depends(browse_scope)])

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


_NOT_IN_CATALOG_DETAIL = "not in this steward's catalog"


def _require_admitted(
    catalog: Catalog, parsed: Fqid, request: Request, suffix: str = ""
) -> RedirectResponse | None:
    """404 live unheld owners; redirect dead citations only to an admitted terminal."""
    if catalog.scope != "holdings" or parsed.kind is FqidKind.CLASSIFICATION:
        return None
    if catalog.exists(parsed):
        return None
    with_catalog = Catalog(catalog.holdings.conn, scope="reference")
    if not with_catalog.exists(parsed):
        redirect = _successor_redirect(catalog, parsed, request, suffix)
        if redirect is not None:
            return redirect
    raise HTTPException(status_code=404, detail=_NOT_IN_CATALOG_DETAIL)


_NO_STATE_COVERAGE = VariableCoverage(
    coverage_from=None,
    coverage_to=None,
    open_ended=False,
    state_count=0,
)


# ── reg_meta model → catalog node mappers (see DESIGN.md → Pydantic boundary) ──
# The per-leaf 1:1 wrappers are gone (#681): reg_meta now returns frozen Pydantic
# models whose `Fqid` fields serialize to the canonical string and whose
# register-bearing models already dump `register`, so the leaf shapes pass straight
# through. Only the NODE mappers remain — they carry genuine server-side
# enrichment (the `kind` discriminator, the `catalog.*_chain` / `classification_*`
# server-side resolution, the coverage zip, the `via_same_as` stringify).


def _binding_node(catalog: Catalog, resolved: ResolvedVariable) -> BindingNode:
    """Map scoped states and reference relationship evidence to the binding leaf."""
    states = list(resolved.states)
    same_as = list(resolved.same_as)
    succession_chain = catalog.variable_chain(resolved.fqid)
    lineage = list(resolved.lineage)
    return BindingNode(
        warnings=tuple(w for w in resolved.warnings if w.variable_fqid is not None),
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
        tags=list(resolved.tags),
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
    family = cast("Any", catalog).classification_family_for_fqid(resolved.fqid)
    return node.model_copy(
        update={
            "family": _classification_family_node(family)
            if family is not None
            else None,
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
    column_coverage = {
        (slug, column.lower()): coverage
        for (slug, column), coverage in catalog.register_column_coverage(
            provider_slug, register_slug
        ).items()
    }
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
            # A curated member names the column in an ALIAS spelling (the build
            # validates it against `variable_alias`), which need not be the one
            # reg_meta keys the coverage under, so this match folds too (Y-102).
            member_cov = column_coverage.get(
                (leaf_slug, m.delivery_column.lower()), _NO_STATE_COVERAGE
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


def _classification_family_node(family) -> ClassificationFamilyNode:
    """Map a derived classification succession family (#771) onto its subject node."""
    return ClassificationFamilyNode(
        key=family.key,
        label=family.label,
        editions=list(family.editions),
    )


def _provider_coverage(
    request: Request, catalog: Catalog, provider_slug: str
) -> dict[str, RegisterCoverage]:
    """`provider_register_coverage`, memoized for the app's lifetime.

    The artifact is immutable while the app serves it, and the holdings fusion
    behind a provider's coverage costs hundreds of milliseconds on a steward
    artifact. The key carries the generation and the read scope (coverage differs
    by scope); only resolved providers reach here, so the memo holds at most one
    entry per provider and scope."""
    key = (
        request.app.state.manifest["generation_id"],
        catalog.scope,
        provider_slug,
    )
    memo: dict[tuple[str, str, str], dict[str, RegisterCoverage]] = (
        request.app.state.provider_coverage
    )
    if key not in memo:
        memo[key] = catalog.provider_register_coverage(provider_slug)
    return memo[key]


def _provider_response(
    request: Request, catalog: Catalog, resolved: ResolvedProvider
) -> ProviderResponse:
    provider_slug = resolved.fqid.provider
    assert provider_slug is not None
    registers = catalog.list_registers(provider_slug)
    coverage = _provider_coverage(request, catalog, provider_slug)
    return ProviderResponse(
        fqid=str(resolved.fqid),
        name=resolved.name,
        children=[
            RegisterNode(
                fqid=str(r.fqid),
                name=r.name,
                purpose=r.purpose,
                coverage=coverage.get(r.fqid.register),
                tags=list(catalog.tags_for_register(r.fqid)),
            )
            for r in registers
        ],
    )


def _register_response(
    catalog: Catalog, resolved: ResolvedRegister
) -> RegisterResponse:
    provider = resolved.fqid.provider
    register = resolved.fqid.register
    assert provider is not None and register is not None
    coverage = catalog.register_variable_coverage(provider, register)
    deliveries = catalog.register_variable_deliveries(provider, register)
    children: list[RegisterChild] = [
        BindingChild(
            fqid=str(b.fqid),
            name=b.name,
            coverage=coverage.get(b.fqid.variable),
            deliveries=deliveries.get(b.fqid.variable, []),
        )
        for b in catalog.list_bindings(provider, register)
    ]
    children.append(VariantsRef(register_fqid=str(resolved.fqid)))
    return RegisterResponse(
        warnings=tuple(w for w in resolved.warnings if w.variable_fqid is None),
        fqid=str(resolved.fqid),
        name=resolved.name,
        purpose=resolved.purpose,
        tags=list(resolved.tags),
        children=children,
        groups=catalog.list_concept_groups(provider, register),
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
    `Catalog(conn, scope=request.state.read_scope)` wrapper over the request connection. The wrapper is
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
    catalog = Catalog(conn, scope="reference")
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


def _resolve_to_node(request: Request, catalog: Catalog, fqid: Fqid) -> CatalogNode:
    """Resolve a scoped node and enrich its HTTP response."""
    try:
        # The binding arm takes the LIGHT hydration: full history and every
        # coding reference, but the members of a state's value set are read
        # separately (`/value-sets/{id}/codes`) rather than embedded once per
        # state that shares them. Every other kind resolves as before.
        resolved = (
            catalog.resolve_binding(fqid, with_codes=False, with_code_summary=True)
            if fqid.kind is FqidKind.VARIABLE_BINDING
            else catalog.resolve(fqid)
        )
    except RegMetaError as exc:
        _http_404_if_not_found(exc)
        raise  # unreachable; _http_404_if_not_found re-raises non-404s
    if isinstance(resolved, ResolvedProvider):
        return _provider_response(request, catalog, resolved)
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
    if _is_fqid_not_found(exc):
        raise HTTPException(status_code=404, detail=exc.message) from exc
    if exc.exit_code == EXIT_USAGE:
        raise HTTPException(status_code=422, detail=exc.message) from exc
    raise exc


def _successor_redirect(
    catalog: Catalog, parsed: Fqid, request: Request, suffix: str
) -> RedirectResponse | None:
    """Resolve the scoped terminal and retain the citation's suffix and query."""
    terminal = catalog.resolve_terminal_successor(parsed)
    if terminal is None:
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
    """Redirect a missing citation to its scoped terminal; map usage errors to 422."""
    if _is_fqid_not_found(exc):
        redirect = _successor_redirect(
            catalog,
            parsed,
            request,
            suffix,
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
    """List scoped providers plus the reference classification root."""
    with _catalog_conn(request) as conn:
        providers = Catalog(conn, scope=request.state.read_scope).list_providers()
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
) -> VariantsResponse | RedirectResponse:
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
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(catalog, fqid, request, suffix="/variants")
        if redirect is not None:
            return redirect
        # Resolve the register first so a bad (provider, register) is a 404, not a
        # 200 with an empty list (list_variants alone can't distinguish them).
        try:
            catalog.resolve(fqid)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, fqid, exc, request, suffix="/variants")
        variants = catalog.list_variants(provider, register)
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
    """The relationship graph for a classification subject (#761/#1119).

    Curated umbrella groups and derived one-dimensional succession families share
    the same canonical `/catalog/group/class/{key}` subject route; this graph
    endpoint mirrors that by-key resolution and returns the union of member/edition
    succession chains (`focus_id=None`). 404 when no classification group or family
    has that key. Shares the `/api/catalog` cache.
    """
    with _catalog_conn(request) as conn:
        graph = Catalog(
            conn, scope=request.state.read_scope
        ).graph_for_classification_group(key)
    if graph is None:
        raise HTTPException(
            status_code=404, detail=f"no classification group or family {key!r}"
        )
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
        Fqid.register_fqid(provider, register)
    except FqidError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        graph = catalog.graph_for_group(provider, register, key)
    if graph is None:
        raise HTTPException(
            status_code=404,
            detail=f"no concept group {key!r} in {provider}/{register}",
        )
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
        catalog = Catalog(conn, scope=request.state.read_scope)
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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        group = catalog.concept_group(provider, register, key)
        if group is None:
            raise HTTPException(
                status_code=404,
                detail=f"no concept group {key!r} in {provider}/{register}",
            )
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


@router.get(
    "/catalog/{fqid:path}/data_warnings", response_model=tuple[DataWarning, ...]
)
def get_data_warnings(
    request: Request,
    unassigned_only: bool = False,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
    period: list[Period] | None = Depends(_validated_period),
    variant: str | None = Depends(_validated_variant),
    representation: str | None = None,
) -> tuple[DataWarning, ...] | RedirectResponse:
    """Source limitations for a register or selected delivery of a binding."""
    parsed = _parsed_binding(validated)
    if parsed.kind not in (FqidKind.REGISTER, FqidKind.VARIABLE_BINDING):
        raise HTTPException(
            status_code=422, detail="Warnings require a register or binding"
        )
    if representation is not None and (
        not representation.strip()
        or len(representation) > 255
        or any(ord(c) < 32 for c in representation)
    ):
        raise HTTPException(status_code=422, detail="Invalid representation column")
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(catalog, parsed, request, suffix="/data_warnings")
        if redirect is not None:
            return redirect
        try:
            if parsed.kind == FqidKind.VARIABLE_BINDING:
                catalog.resolve_binding(parsed, with_codes=False)
            else:
                catalog.resolve(parsed)
            warnings = {
                warning.warning_id: warning
                for member in (period if period is not None else [None])
                for warning in catalog.data_warnings(
                    parsed,
                    period=member,
                    variant=variant,
                    representation=representation,
                    unassigned_only=unassigned_only,
                )
            }
        except RegMetaError as exc:
            return _redirect_or_4xx(
                catalog, parsed, exc, request, suffix="/data_warnings"
            )
    return tuple(warnings[k] for k in sorted(warnings))


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
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(catalog, parsed, request, suffix="/states")
        if redirect is not None:
            return redirect
        try:
            states = catalog.states(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/states")
    return StatesResponse(binding=str(parsed), states=states)


@router.get("/catalog/{fqid:path}/predecessors", response_model=PredecessorsResponse)
def get_binding_predecessors(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> PredecessorsResponse | RedirectResponse:
    """Variables this binding's variable replaced (inbound succession). A
    dead/renamed binding 301s to `/predecessors` on its terminal successor (#411)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(catalog, parsed, request, suffix="/predecessors")
        if redirect is not None:
            return redirect
        try:
            refs = catalog.predecessors(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(
                catalog, parsed, exc, request, suffix="/predecessors"
            )
    return PredecessorsResponse(binding=str(parsed), predecessors=refs)


@router.get("/catalog/{fqid:path}/successors", response_model=SuccessorsResponse)
def get_binding_successors(
    request: Request,
    validated: ValidatedFqidPath = Depends(_validated_fqid),
) -> SuccessorsResponse | RedirectResponse:
    """Variables that replaced this binding's variable (outbound succession). A
    dead/renamed binding 301s to `/successors` on its terminal successor (#411)."""
    parsed = _parsed_binding(validated)
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(catalog, parsed, request, suffix="/successors")
        if redirect is not None:
            return redirect
        try:
            refs = catalog.successors(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/successors")
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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(catalog, parsed, request, suffix="/dimensions")
        if redirect is not None:
            return redirect
        try:
            groups = catalog.dimensions(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/dimensions")
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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(catalog, parsed, request, suffix="/graph")
        if redirect is not None:
            return redirect
        try:
            if parsed.kind is FqidKind.CLASSIFICATION:
                return catalog.graph_for_classification_fqid(parsed)
            return catalog.graph_for_fqid(parsed)
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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(catalog, parsed, request, suffix="/lineage")
        if redirect is not None:
            return redirect
        try:
            edges = catalog.lineage(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(catalog, parsed, exc, request, suffix="/lineage")
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
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(
            catalog, parsed, request, suffix="/lineage_warnings"
        )
        if redirect is not None:
            return redirect
        try:
            warnings = catalog.lineage_warnings(parsed)
        except RegMetaError as exc:
            return _redirect_or_4xx(
                catalog, parsed, exc, request, suffix="/lineage_warnings"
            )
    return LineageWarningsResponse(binding=str(parsed), lineage_warnings=warnings)


# ── Bounded value-set code reads (#Y-46) ────────────────────────────────────
# The binding leaf and its `?period` subset carry each state's value_set_id and a
# cardinality-independent `value_set_summary`, NOT the members — a variable whose
# 290 states share a few large codings would otherwise embed those codings 290
# times. This is where the members are actually read, one bounded page at a time,
# for the panel the researcher opens.
#
# NOT steward-gated, and its integer ids are enumerable BY DESIGN. A value set's
# code→label members — and the stored mismatch list `?state=` reads — are
# catalog-global REFERENCE data, the same footing as a classification's codes, which
# pass the steward gate through untouched (DESIGN.md → Classification pass-through
# (decision 2)). A steward inventory maps the variable COLUMNS that steward delivers;
# it does not describe the code vocabularies those columns draw on, so there is no
# holdings basis to scope this read by: seeing a binding is not what authorizes
# reading a coding, and not seeing one is no reason to withhold it. That policy is
# the reason for the pass-through — never an assumption that an id is hard to guess
# or only reachable from an admitted leaf. What this route DOES enforce is the exact
# state/value-set pairing below: a state id can never read a coding it does not carry.

# Page sizes: a default that covers the ordinary coding in one request, and a
# ceiling that keeps a hand-written `?limit` from asking for a whole LKF edition.
_VALUE_SET_CODES_DEFAULT_LIMIT = 200
_VALUE_SET_CODES_MAX_LIMIT = 1000


def _validated_code_limit(limit: int = _VALUE_SET_CODES_DEFAULT_LIMIT) -> int:
    """``?limit`` page size, clamped to [1, _VALUE_SET_CODES_MAX_LIMIT] — the
    clamp-don't-422 convention the other paged reads use."""
    return clamp_limit(limit, maximum=_VALUE_SET_CODES_MAX_LIMIT)


@router.get("/value-sets/{value_set_id}/codes", response_model=ValueSetCodesResponse)
def get_value_set_codes(
    request: Request,
    value_set_id: Annotated[int, PathParameter(ge=-(1 << 63), le=(1 << 63) - 1)],
    state: Annotated[int | None, Query(ge=-(1 << 63), le=(1 << 63) - 1)] = None,
    partition: Literal[
        "source_extensions", "canonical", "nonstandard", "sentinels"
    ] = "source_extensions",
    classification: str | None = None,
    column: str | None = None,
    alias_window_from: str | None = None,
    q: str = "",
    offset: int = 0,
    limit: int = Depends(_validated_code_limit),
) -> ValueSetCodesResponse:
    """A filtered, bounded source-code page, optionally scoped to an exact book.

    State partitions require the declared classification; alias partitions also
    require both the physical column and original coding-window identity. Unknown
    ownership returns 404. Filtering precedes paging and preserves source labels.
    """
    validate_text_query(q)
    offset = max(0, offset)
    if (column is None) != (alias_window_from is None):
        raise HTTPException(
            status_code=422, detail="column and alias_window_from are required together"
        )
    if state is None and (
        classification is not None
        or partition != "source_extensions"
        or column is not None
    ):
        raise HTTPException(
            status_code=422, detail="state is required with classification"
        )
    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        if state is None:
            codes = catalog.value_set_codes(value_set_id)
            missing = f"no value set {value_set_id} in this catalog"
        else:
            if classification is None:
                raise HTTPException(
                    status_code=422, detail="classification is required with state"
                )
            reader = {
                "canonical": catalog.state_canonical_codes,
                "source_extensions": catalog.state_nonconforming_codes,
                "nonstandard": catalog.state_nonstandard_codes,
                "sentinels": catalog.state_sentinel_codes,
            }[partition]
            codes = reader(
                state,
                value_set_id,
                classification_slug=classification,
                delivery_column_name=column,
                alias_window_from=alias_window_from,
            )
            missing = f"state {state} does not carry value set {value_set_id}"
    if codes is None:
        raise HTTPException(status_code=404, detail=missing)
    matched = (
        tuple(c for c in codes if matches_filter(q, c.code, c.label))
        if q.strip()
        else codes
    )
    return ValueSetCodesResponse(
        value_set_id=value_set_id,
        state_id=state,
        q=q,
        total=len(matched),
        offset=offset,
        limit=limit,
        codes=matched[offset : offset + limit],
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
                catalog = Catalog(conn, scope=request.state.read_scope)
                redirect = _require_admitted(catalog, parsed, request, suffix="")
                if redirect is not None:
                    return redirect
                # Resolve PER SEGMENT (#340) — `resolve_at` never sees the #307
                # list form, and its monthly-family fallback is decided per
                # query, so one segment's window must not suppress another
                # segment's (the shared `order.resolve_binding` pass resolves
                # the same way). The union dedupes by the COMPOUND
                # (state_id, delivery_column_name,
                # valid_from): one state can intersect several segments, AND a
                # merged monthly-family variable (#319) expands one annual state
                # into 12 same-state_id per-month windows — keying on state_id
                # alone would collapse 11 of them. Insertion order keeps the
                # per-segment resolve_at ordering, chronological across a sorted
                # list.
                states_by_id: dict[
                    tuple[int, str | None, str | None], VariableState
                ] = {}
                try:
                    for segment in period:
                        for s in catalog.resolve_at(
                            parsed,
                            segment,
                            variant=variant,
                            value_set_version=resolved_vsv,
                            # Same light hydration as the no-period leaf: the
                            # narrowed subset is the same states, so it must not
                            # embed what the full node no longer does.
                            with_codes=False,
                            with_code_summary=True,
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
            return StatesResponse(binding=str(parsed), states=states)

    with _catalog_conn(request) as conn:
        catalog = Catalog(conn, scope=request.state.read_scope)
        redirect = _require_admitted(catalog, parsed, request, suffix="")
        if redirect is not None:
            return redirect
        try:
            return _resolve_to_node(request, catalog, parsed)
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
