"""`POST /api/project/*` — the project-WRITE surface (A5.2b-ii).

See DESIGN.md → Project-write surface (routes/project.py).
Two endpoints:

- ``POST /api/project/validate`` — a thin adapter over reg_meta's shared
  ``semantic.validate_project``: the CONCATENATED issue list (structural ⧺
  semantic) served as ``semantic.validation_json`` VERBATIM, typed as a
  ``ValidationResultModel``, byte-identical to ``reg-meta validate``. A
  ``schema_version`` this build does not read is answered by that one issue
  alone (``order.schema_version_issue``, shared with ``/order`` and the CLI),
  before any layer interprets the document as the current contract.
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
issue; a residual model-construction failure is reg_meta's thin defensive issue (still
coded ``invalid_field``) — a 200 ISSUE either way, NEVER a 500.

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
open (``validate_project`` calls the adapter's opener only after they pass), so a
malformed or structurally-rejected body costs no DB hit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response
from reg_meta.errors import RegMetaError
from reg_meta.order import (
    OrderFinding,
    OrderManifest,
    blocked_message,
    materialize_order,
    project_from_raw,
)

# Aliased: the route handler below is also named `validate_project`, and its
# name is the published OpenAPI operationId.
from reg_meta.semantic import validate_project as validate_raw_project, validation_json
from reg_schema.project_data import ProjectData

from reg_webapp.models import OrderBlockedModel, ValidationResultModel
from reg_webapp.project_validation import per_request_conn
from reg_webapp.request_body import read_raw_json_object
from reg_webapp.scope import reject_project_scope

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


router = APIRouter(prefix="/api/project", dependencies=[Depends(reject_project_scope)])


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
async def validate_project(request: Request) -> Response:
    """Validate a ``project_data.json``. Returns 200 with the concatenated
    structural ⧺ semantic issue list + the derived ``ok`` flag; a 4xx is
    reserved for a malformed REQUEST (``read_raw_json_object`` / the body cap).

    A THIN adapter over reg_meta's ``semantic.validate_project``
    (REFACTOR_SPEC.md §12): the composition, every issue and the serialization
    live there, so this endpoint and ``reg-meta validate`` emit byte-identical
    findings. The 200 body is ``semantic.validation_json`` VERBATIM, returned
    as a raw ``Response`` (FastAPI passes it through without re-serializing)
    while ``response_model=`` still publishes ``ValidationResultModel`` as the
    typed contract for the OpenAPI snapshot + the SPA codegen — the ``/order``
    pattern.

    ``async`` only to read the body off the wire; the BLOCKING work (the structural
    parse + the semantic layer's per-binding sqlite resolution) is offloaded to the
    threadpool via ``run_in_threadpool`` so it never stalls the event loop (the
    catalog routes are plain ``def`` for the same reason). The reg_meta connection
    opens on that threadpool thread (one thread → the cross-thread sqlite P1 can't
    recur)."""
    raw = await read_raw_json_object(request)
    return await run_in_threadpool(
        _validate_blocking,
        request.app.state.db_path,
        raw,
    )


def _validate_blocking(db_path: Path, raw: dict[str, Any]) -> Response:
    """Validate and serialize, on a threadpool thread (off the event loop).

    ``per_request_conn`` is handed over as the opener, not opened here: the
    shared door calls it only after the DB-free layers pass, and on THIS thread,
    so a structurally invalid body never reaches the open."""
    result = validate_raw_project(raw, lambda: per_request_conn(db_path))
    return Response(content=validation_json(result), media_type="application/json")


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
    the ``reg-meta order`` CLI emit byte-identical manifests. The selected
    artifact determines orderability: catalog artifacts use global fallback;
    steward artifacts use their compiled holdings and steward identity.

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
    )


def _order_blocking(db_path: Path, raw: dict[str, Any]) -> Response:
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
        result = materialize_order(project, conn)
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
