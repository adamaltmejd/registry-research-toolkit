"""`POST /api/project/*` — the project-WRITE surface (A5.2b-ii).

See DESIGN.md → Project-write surface (routes/project.py).
Two endpoints:

- ``POST /api/project/validate`` — runs the two-layer validator over a
  raw ``project_data.json`` and returns the CONCATENATED issue list (structural
  ⧺ semantic) as a ``ValidationResultModel``.
- ``POST /api/project/order`` — materializes the JSON order manifest through
  reg_meta's shared ``order.materialize_order`` and serves it as an
  ``order.json`` download; anything that is not an order is a 422 carrying the
  typed findings (``OrderBlockedModel``).

**Status discipline.** ``/validate`` is a *diagnostic*: a spec
that FAILS validation is a SUCCESSFUL validation RESPONSE — HTTP 200 with
``ok=false`` + the issues. 4xx is reserved for a malformed REQUEST: non-JSON body,
duplicate JSON keys, a too-deeply-nested body, a non-object top level, or an
oversized body (the last handled by ``BodySizeLimitMiddleware`` before the handler
runs). The body is parsed as JSON regardless of ``Content-Type`` (lenient — a
researcher tool, not a strict public API). An extra/typo KEY on a closed object
(ProjectData/Source/Binding/Panel/PanelMember) surfaces as the structural ``unexpected_field``
issue; a residual model-construction failure is a thin defensive issue (still coded
``invalid_field``) — a 200 ISSUE either way, NEVER a 500.

**Connection model = per-request open ON ONE THREAD** (LOCKED). ``/validate`` is
``async`` only to read the body off the wire; the BLOCKING work (structural parse
+ the semantic layer's per-binding sqlite resolution) is offloaded to the
threadpool via ``run_in_threadpool`` so it never stalls the event loop — the
catalog routes are plain ``def`` for the same reason.
``project_validation.per_request_conn`` opens the reg_meta connection on that
worker thread (``/order``'s blocking half runs on its threadpool thread too):
open + query + close stay on ONE thread — NOT a generator ``Depends``, which would run on
a possibly-different AnyIO thread → cross-thread ``sqlite3.ProgrammingError`` (the
A5.2a/b-i P1). The body parse + structural layer are DB-FREE and run BEFORE the
open, so a malformed or structurally-rejected body costs no DB hit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError
from reg_meta.errors import RegMetaError
from reg_meta.order import (
    OrderFinding,
    OrderManifest,
    blocked_message,
    materialize_order,
    project_from_raw,
)
from reg_schema.project_data import ProjectData
from reg_schema.structural import validate_structural
from reg_schema.validation import ValidationIssue, ValidationResult

from reg_webapp.models import (
    OrderBlockedModel,
    ValidationIssueModel,
    ValidationResultModel,
)
from reg_webapp.project_validation import (
    per_request_conn,
    semantic_issues,
)
from reg_webapp.request_body import read_raw_json_object

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from reg_meta.catalog import Catalog
    from reg_meta.inventory import DeliveryInventory

    from reg_webapp.catalog_index import CatalogIndex

router = APIRouter(prefix="/api/project")


def openapi_schemas() -> dict[str, dict[str, Any]]:
    """Return ProjectData and its nested models as OpenAPI components.

    The handlers intentionally use raw request ingress, so FastAPI cannot discover
    these request-only models itself. The app factory registers this Pydantic-
    generated component set, keeping the canonical schema as the single source of
    truth while the operations reference it normally.
    """
    schema = ProjectData.model_json_schema(ref_template="#/components/schemas/{model}")
    definitions = schema.pop("$defs", {})
    return {**definitions, "ProjectData": schema}


# Document the canonical closed contract even though runtime intentionally reads
# a raw dict so malformed projects can receive accumulated diagnostics.
_PROJECT_BODY_SCHEMA = {"$ref": "#/components/schemas/ProjectData"}


def _model_issue(message: str, exc: ValidationError) -> ValidationIssue:
    """Turn a residual ``ProjectData.model_validate`` failure into an error issue.

    THIN DEFENSIVE catch. ``validate_structural`` now owns the structural problems
    (missing / mistyped / unexpected keys — incl. ``unexpected_field`` on all closed
    project objects), and the caller only builds the model
    once structural passed. So the common extra-key case never reaches here (it is
    ``unexpected_field`` from reg_schema); a model ``ValidationError`` here is a
    constraint structural did NOT replicate (rare — effectively unreachable under
    today's models) — surfaced as a 200 issue (code ``invalid_field``), never a 500.
    The path points at the first offending field."""
    errors = exc.errors()
    loc = errors[0]["loc"] if errors else ()
    # RFC 6901: "" points at the whole document; "/" would mean a property keyed
    # by the empty string (unresolvable). A model-level error has an empty loc.
    path = "/" + "/".join(str(p) for p in loc) if loc else ""
    return ValidationIssue(
        level="error", code="invalid_field", path=path, message=message
    )


def _to_result_model(result: ValidationResult) -> ValidationResultModel:
    """Wrap reg_schema's frozen ``ValidationResult`` in the webapp response model."""
    return ValidationResultModel(
        ok=result.ok,
        issues=[
            ValidationIssueModel(
                level=i.level,
                code=i.code,
                path=i.path,
                message=i.message,
                successor_fqid=i.successor_fqid,
            )
            for i in result.issues
        ],
    )


def _semantic_issues(
    raw: dict[str, Any], catalog: Catalog, index: CatalogIndex | None
) -> list[ValidationIssue]:
    """Build the ``ProjectData`` model, then run the reg_meta-backed semantic layer
    (``project_validation.semantic_issues``).

    ``index`` is the deployment's loaded steward ``CatalogIndex`` (``None`` for the
    ``global`` deployment); the semantic layer consults it to flag a resolvable
    binding outside the steward's filtered subset (``fqid_outside_steward_catalog``
    / ``representation_outside_steward_catalog`` — column-based admission, #206).

    Reached only when the structural layer passed. A residual model
    ``ValidationError`` is a constraint structural didn't replicate — surfaced as a
    200 ISSUE (NOT a 500), and the semantic step is skipped (it needs a built
    model)."""
    try:
        project = ProjectData.model_validate(raw)
    except ValidationError as exc:
        return [
            _model_issue(
                "project_data failed model construction (a constraint the "
                f"structural layer did not catch?): {exc}",
                exc,
            )
        ]
    return semantic_issues(project, catalog, index)


# The body is read RAW (not a typed param), so FastAPI emits no `requestBody` in
# the OpenAPI schema. Document the canonical closed ProjectData schema explicitly;
# runtime ingress remains raw so invalid values and unknown keys survive long
# enough for `/validate` to diagnose them.
@router.post(
    "/validate",
    response_model=ValidationResultModel,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _PROJECT_BODY_SCHEMA}},
        }
    },
)
async def validate_project(request: Request) -> ValidationResultModel:
    """Validate a ``project_data.json``. Returns 200 with the concatenated
    structural ⧺ semantic issue list + the derived ``ok`` flag; a 4xx is
    reserved for a malformed REQUEST (``read_raw_json_object`` / the body cap).

    This is the SEMANTIC validator (reg_meta-backed).

    ``async`` only to read the body off the wire; the BLOCKING work (the structural
    parse + the semantic layer's per-binding sqlite resolution) is offloaded to the
    threadpool via ``run_in_threadpool`` so it never stalls the event loop (the
    catalog routes are plain ``def`` for the same reason). The reg_meta connection
    opens on that threadpool thread (one thread → the cross-thread sqlite P1 can't
    recur)."""
    raw = await read_raw_json_object(request)
    # The `CatalogIndex` is an immutable in-memory dataclass (no DB conn), so reading
    # it on the threadpool thread is safe — mirrors how `db_path` is already passed.
    return await run_in_threadpool(
        _validate_blocking,
        request.app.state.db_path,
        raw,
        request.app.state.catalog_index,
    )


def _validate_blocking(
    db_path: Path, raw: dict[str, Any], index: CatalogIndex | None
) -> ValidationResultModel:
    """The two-layer composition, run on a threadpool thread (off the
    event loop). Layer order (DB-free first, so a structurally-rejected body costs
    no DB hit): structural → (model build + semantic). When structural fails we SKIP
    the model build + semantic step (they assume a structurally valid spec).

    ``index`` is the deployment's loaded steward ``CatalogIndex`` (``None`` for the
    ``global`` deployment), threaded into the semantic layer for the steward
    catalog filter (``fqid_outside_steward_catalog`` /
    ``representation_outside_steward_catalog``)."""
    issues: list[ValidationIssue] = []
    structural = validate_structural(raw)
    issues.extend(structural.issues)

    if structural.ok:
        # The connection opens HERE on this threadpool thread (one thread), AFTER
        # the DB-free layers — a structurally invalid body never reaches the open.
        from reg_meta.catalog import Catalog

        with per_request_conn(db_path) as conn:
            issues.extend(_semantic_issues(raw, Catalog(conn), index))

    return _to_result_model(ValidationResult(issues=tuple(issues)))


# The 200 body IS `OrderManifest.to_json()` VERBATIM — the manifest's own
# canonical serialization (sorted keys, stable entry order, trailing newline),
# which is what makes this adapter and `reg-meta order` byte-identical (§12).
# So the handler returns a raw `Response` (FastAPI passes a `Response` through
# without re-serializing) while `response_model=` still publishes the reg_meta
# model as the typed contract for the OpenAPI snapshot + the SPA codegen. It is
# served as an attachment because the SPA's action is a file download.
@router.post(
    "/order",
    response_model=OrderManifest,
    responses={
        422: {
            "model": OrderBlockedModel,
            "description": (
                "Not an order: the spec is invalid, or the materializer "
                "fail-closed on it. Carries the typed findings."
            ),
        }
    },
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _PROJECT_BODY_SCHEMA}},
        }
    },
)
async def order_project(request: Request) -> Response:
    """Materialize a ``project_data.json`` into the JSON order manifest.

    A THIN adapter over ``reg_meta.order.materialize_order`` (REFACTOR_SPEC.md
    §12): no gate, no fallback and no rendering lives here, so this endpoint and
    the ``reg-meta order`` CLI emit byte-identical manifests. The deployment's
    delivery inventory is read once at boot (``app.state.inventory``); ``None``
    is §12's global-deployment fallback, which the materializer takes directly.

    200 is the manifest — ``application/json``, downloaded as ``order.json``.
    Anything else is NOT AN ORDER: 422 either because the spec is invalid
    (``project_from_raw``) or because the materializer blocked it, carrying the
    typed ``OrderBlockedModel`` — the findings as DATA (each with its code and
    its source/variable/period coordinates), not one flattened line. There is
    deliberately no partial 200.

    ``async`` + ``run_in_threadpool`` (blocking sqlite resolution off the event
    loop), mirroring ``/validate``."""
    raw = await read_raw_json_object(request)
    return await run_in_threadpool(
        _order_blocking,
        request.app.state.db_path,
        raw,
        request.app.state.inventory,
    )


def _order_blocking(
    db_path: Path, raw: dict[str, Any], inventory: DeliveryInventory | None
) -> Response:
    """Gate, materialize and serialize, on a threadpool thread.

    Both failure modes are a 422 of the SAME shape — an invalid spec and a
    fail-closed blocked order are equally "this is not an order" (unlike
    ``/validate``, which DIAGNOSES an invalid spec at 200); only the gate's has
    no findings to carry. ``RegMetaError`` is what ``order.project_from_raw``
    raises for a structurally invalid or model-rejected spec; its message is the
    same one the CLI envelopes."""
    try:
        project = project_from_raw(raw)
    except RegMetaError as exc:
        return _not_an_order(exc.message, ())

    with per_request_conn(db_path) as conn:
        result = materialize_order(project, inventory, conn)
    if result.manifest is None:
        return _not_an_order(blocked_message(result), result.findings)
    return Response(
        content=result.manifest.to_json(),
        media_type="application/json",
        headers={"content-disposition": 'attachment; filename="order.json"'},
    )


def _not_an_order(detail: str, findings: Sequence[OrderFinding]) -> JSONResponse:
    """The 422 body: the flattened ``detail`` line AND the typed findings.

    Returned, not raised: ``HTTPException`` can only carry ``detail``, and the
    findings are the contract — a client (the SPA's per-finding rendering, a
    future extractor) must be able to read a finding's ``code`` and its
    ``source`` / ``variable`` / ``period`` coordinates without parsing prose.
    ``findings`` is empty only for a spec the gate rejected before the
    materializer ever saw it."""
    body = OrderBlockedModel(detail=detail, findings=list(findings))
    return JSONResponse(status_code=422, content=body.model_dump(mode="json"))
