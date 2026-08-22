"""`POST /api/project/*` — the project-WRITE surface (A5.2b-ii).

See DESIGN.md → Project-write surface (routes/project.py).
Two endpoints:

- ``POST /api/project/validate`` — runs the two-layer validator over a
  raw ``project_data.json`` and returns the CONCATENATED issue list (structural
  ⧺ semantic) as a ``ValidationResultModel``.
- ``POST /api/project/order`` — renders the current provisional order-export CSV
  (``order_export.render_order_csv``) as a ``text/csv`` download.

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
worker thread (``/order`` is a sync ``def``, so on its threadpool thread): open +
query + close stay on ONE thread — NOT a generator ``Depends``, which would run on
a possibly-different AnyIO thread → cross-thread ``sqlite3.ProgrammingError`` (the
A5.2a/b-i P1). The body parse + structural layer are DB-FREE and run BEFORE the
open, so a malformed or structurally-rejected body costs no DB hit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from pydantic import ValidationError
from reg_schema.project_data import ProjectData
from reg_schema.structural import validate_structural
from reg_schema.validation import ValidationIssue, ValidationResult

from reg_webapp.models import (
    ValidationIssueModel,
    ValidationResultModel,
)
from reg_webapp.order_export import render_order_csv
from reg_webapp.project_validation import (
    per_request_conn,
    semantic_issues,
)
from reg_webapp.request_body import read_raw_json_object

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta.catalog import Catalog

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


# The order export is a CSV DOWNLOAD, so this endpoint cannot declare a
# Pydantic `response_model=` — it returns raw `text/csv` bytes, the ONE documented
# exception to the "every route declares a response_model" lint. Documented here
# so the carve-out is explicit (a binary/download response, not a JSON model).
# `responses=` + `response_class` declare the `text/csv` media type so the
# OpenAPI contract (and the SPA's codegen) sees a download, not a JSON body.
@router.post(
    "/order",
    response_class=Response,
    responses={200: {"content": {"text/csv": {}}}},
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _PROJECT_BODY_SCHEMA}},
        }
    },
)
async def order_project(request: Request) -> Response:
    """Render the current provisional order-export CSV.

    Reads the raw dict and runs the STRUCTURAL gate (see reg_schema/DESIGN.md →
    Structural rules and issue codes) before rendering: the
    ``ProjectData`` model enforces only field types, while the structural rules
    (FQID shape, period grammar, the binding/source-prefix match) live in
    ``validate_structural`` — so a Pydantic-valid-but-structurally-invalid spec
    (e.g. a malformed ``register_variant`` or bad period token) would otherwise
    render a bad provider order at 200. A structurally invalid spec → 422.
    ``async`` + ``run_in_threadpool`` (blocking display_name resolution off the
    event loop), mirroring ``/validate``."""
    raw = await read_raw_json_object(request)
    return await run_in_threadpool(_order_blocking, request.app.state.db_path, raw)


def _order_blocking(db_path: Path, raw: dict[str, Any]) -> Response:
    """Structural-gate then render the order CSV, on a threadpool thread. A
    structurally invalid spec, or one the model rejects (extra/typo field), → 422
    — you cannot render a provider order from an invalid spec (unlike ``/validate``,
    which DIAGNOSES it at 200)."""
    structural = validate_structural(raw)
    if not structural.ok:
        errors = [i for i in structural.issues if i.level == "error"]
        raise HTTPException(
            status_code=422,
            detail="cannot render an order for a structurally invalid spec: "
            + "; ".join(f"{i.code}@{i.path}" for i in errors),
        )
    try:
        project = ProjectData.model_validate(raw)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    from reg_meta.catalog import Catalog

    with per_request_conn(db_path) as conn:
        csv_text = render_order_csv(project, Catalog(conn))
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"content-disposition": 'attachment; filename="order.csv"'},
    )
