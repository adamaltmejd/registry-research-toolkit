"""`POST /api/project/*` — the project-WRITE surface (A5.2b-ii).

See DESIGN.md → Project-write surface (routes/project.py + routes/bundle.py).
Two endpoints:

- ``POST /api/project/validate`` — runs the three-layer validator over a
  raw ``project_data.json`` and returns the CONCATENATED issue list (structural
  ⧺ block ⧺ semantic) as a ``ValidationResultModel``.
- ``POST /api/project/order`` — renders the steward's default v1 order-export CSV
  (``order_export.render_order_csv``) as a ``text/csv`` download.

**Status discipline.** ``/validate`` is a *diagnostic*: a spec
that FAILS validation is a SUCCESSFUL validation RESPONSE — HTTP 200 with
``ok=false`` + the issues. 4xx is reserved for a malformed REQUEST: non-JSON body,
duplicate JSON keys, a too-deeply-nested body, a non-object top level, or an
oversized body (the last handled by ``BodySizeLimitMiddleware`` before the handler
runs). The body is parsed as JSON regardless of ``Content-Type`` (lenient — a
researcher tool, not a strict public API). An extra/typo KEY on a closed object
(Source/Binding/Panel/PanelMember) surfaces as the structural ``unexpected_field``
issue; a residual model-construction failure is a thin defensive issue (still coded
``invalid_field``) — a 200 ISSUE either way, NEVER a 500.

**Connection model = per-request open ON ONE THREAD** (LOCKED). ``/validate`` is
``async`` only to read the body off the wire; the BLOCKING work (structural parse
+ the semantic layer's per-binding sqlite resolution) is offloaded to the
threadpool via ``run_in_threadpool`` so it never stalls the event loop — the
catalog routes are plain ``def`` for the same reason. ``_project_conn`` opens the
reg_meta connection on that worker thread (``/order`` is a sync ``def``, so on its
threadpool thread): open + query + close stay on ONE thread — NOT a generator
``Depends``, which would run on a possibly-different AnyIO thread → cross-thread
``sqlite3.ProgrammingError`` (the A5.2a/b-i P1). The body parse + structural/block
layers are DB-FREE and run BEFORE the open, so a malformed or structurally-rejected
body costs no DB hit.

Import boundary (see reg_monabundle/DESIGN.md → The two halves): this module
imports ``reg_schema`` (structural validator + ``ProjectData``)
and the BUILD-side ``reg_monabundle.build.spec_loader`` issue forms (``block_issue``
/ ``binding_options_issues``, which wrap the block + the cross-block
referential checks as canonical ``ValidationIssue``s) — NOT
``reg_monabundle.runtime.*`` / duckdb / pyodbc (``spec_loader`` imports the runtime
lazily, so this stays out of the import graph; the import-graph test pins it).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import reg_meta.db
from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from pydantic import ValidationError
from reg_monabundle.build.spec_loader import binding_options_issues, block_issue
from reg_schema.project_data import ProjectData
from reg_schema.structural import validate_structural
from reg_schema.validation import ValidationIssue, ValidationResult

from reg_webapp.models import (
    ValidationIssueModel,
    ValidationResultModel,
)
from reg_webapp.order_export import render_order_csv
from reg_webapp.request_body import read_raw_json_object
from reg_webapp.semantic import validate_semantic

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator
    from pathlib import Path

    from reg_meta.catalog import Catalog

    from reg_webapp.catalog_index import CatalogIndex

router = APIRouter(prefix="/api/project")


@contextmanager
def _project_conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    """A per-request reg_meta read-only connection, opened ON THE CALLING THREAD
    (the threadpool thread for ``/validate``'s offloaded work, the sync handler
    thread for ``/order``).

    Used as a plain ``with`` (NOT a FastAPI ``Depends``) so open + query + close
    stay on ONE thread — the load-bearing cross-thread-safety property (a generator
    dependency runs on a possibly-different AnyIO threadpool thread →
    ``sqlite3.ProgrammingError`` under concurrency). ``check_schema=False``: the
    lifespan already validated the schema at boot."""
    conn = reg_meta.db.open_db(db_path, check_schema=False)
    try:
        yield conn
    finally:
        conn.close()


def _model_issue(message: str, exc: ValidationError) -> ValidationIssue:
    """Turn a residual ``ProjectData.model_validate`` failure into an error issue.

    THIN DEFENSIVE catch. ``validate_structural`` now owns the structural problems
    (missing / mistyped / unexpected keys — incl. ``unexpected_field`` on the closed
    Source/Binding/Panel/PanelMember objects), and the caller only builds the model
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
                level=i.level, code=i.code, path=i.path, message=i.message
            )
            for i in result.issues
        ],
    )


def _block_issues(raw: dict[str, Any]) -> list[ValidationIssue]:
    """The ``reg_monabundle`` block layer (see reg_monabundle/DESIGN.md → The two
    halves), as an issue list (tuple-concatenation composition). The code now
    lives in its OWNER:
    ``reg_monabundle.build.spec_loader.block_issue`` runs the amalgamation-safe
    raise-based ``validate_block`` and wraps its single raise into one canonical
    ``invalid_block`` ``ValidationIssue`` (None when clean) — the webapp no longer
    invents the code. An absent block validates trivially."""
    issue = block_issue(raw.get("reg_monabundle"))
    return [issue] if issue is not None else []


def _semantic_issues(
    raw: dict[str, Any], catalog: Catalog, index: CatalogIndex | None
) -> list[ValidationIssue]:
    """Build the ``ProjectData`` model, run the semantic layer (see DESIGN.md →
    Semantic validation (semantic.py)), AND the
    build-time cross-block referential checks (orphan ``binding_options`` keys /
    suppress_k-on-non-categorical, via
    ``reg_monabundle.build.spec_loader.binding_options_issues``).
    The cross-block check closes the documented ``/validate``↔``/bundle``
    divergence — a spec that bundles must also validate clean on that class.

    ``index`` is the deployment's loaded steward ``CatalogIndex`` (``None`` for the
    ``global`` deployment); the semantic layer consults it to flag a resolvable
    binding outside the steward's filtered subset (``fqid_outside_steward_catalog``
    / ``representation_outside_steward_catalog`` — column-based admission, #206).

    Reached only when the structural layer passed. The model build is now a THIN
    DEFENSIVE catch: ``validate_structural`` already flags missing / mistyped /
    unexpected keys (incl. ``unexpected_field`` on the closed objects), so a
    residual model ``ValidationError`` is a constraint structural didn't replicate
    — surfaced as an issue (NOT a 500), and the semantic step is skipped (it needs
    a built model)."""
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
    issues = list(
        validate_semantic(project, catalog, caller="researcher", index=index).issues
    )
    issues.extend(binding_options_issues(raw.get("reg_monabundle"), project))
    return issues


# The body is read RAW (not a typed param), so FastAPI emits no `requestBody` in
# the OpenAPI schema — document it explicitly as an unconstrained JSON object
# (a project_data.json) so the SPA codegen sees a body to send. We deliberately
# don't pin the ProjectData schema here: /validate must accept malformed specs to
# diagnose them.
@router.post(
    "/validate",
    response_model=ValidationResultModel,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    # `additionalProperties: true` → openapi-typescript emits an OPEN
                    # object (`Record<string, unknown>`); a bare `type: object` would
                    # codegen as `Record<string, never>` (empty), unassignable from a
                    # real project_data.json.
                    "schema": {"type": "object", "additionalProperties": True}
                }
            },
        }
    },
)
async def validate_project(request: Request) -> ValidationResultModel:
    """Validate a ``project_data.json``. Returns 200 with the concatenated
    structural ⧺ block ⧺ semantic issue list + the derived ``ok`` flag; a 4xx is
    reserved for a malformed REQUEST (``read_raw_json_object`` / the body cap).

    This is the SEMANTIC validator (reg_meta-backed). It now ALSO runs the
    build-time cross-block referential checks (orphan ``binding_options`` keys /
    suppress_k-on-non-categorical) — that half of the old ``/validate``↔``/bundle``
    divergence is CLOSED. The ONLY residual gap: ``/bundle`` additionally runs the
    step-4 capability gates (e.g. a build-required ``display_name``), which
    ``/validate`` does NOT — so a spec ``/validate`` greenlights can still 422 at
    ``/bundle`` on a capability gate (an intentional lenient residual: ``/validate``
    defaults ``display_name`` from reg_meta).

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
    """The three-layer composition, run on a threadpool thread (off the
    event loop). Layer order (DB-free first, so a structurally-rejected body costs
    no DB hit): structural → block → (model build + semantic). When structural
    fails we SKIP the model build + semantic step (they assume a structurally valid
    spec) but STILL report the block issues — the block validator is independent.

    ``index`` is the deployment's loaded steward ``CatalogIndex`` (``None`` for the
    ``global`` deployment), threaded into the semantic layer for the steward
    catalog filter (``fqid_outside_steward_catalog`` /
    ``representation_outside_steward_catalog``)."""
    issues: list[ValidationIssue] = []
    structural = validate_structural(raw)
    issues.extend(structural.issues)
    issues.extend(_block_issues(raw))

    if structural.ok:
        # The connection opens HERE on this threadpool thread (one thread), AFTER
        # the DB-free layers — a structurally invalid body never reaches the open.
        from reg_meta.catalog import Catalog  # noqa: PLC0415 — lazy: DB-bound

        with _project_conn(db_path) as conn:
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
            "content": {
                "application/json": {
                    # `additionalProperties: true` → openapi-typescript emits an OPEN
                    # object (`Record<string, unknown>`); a bare `type: object` would
                    # codegen as `Record<string, never>` (empty), unassignable from a
                    # real project_data.json.
                    "schema": {"type": "object", "additionalProperties": True}
                }
            },
        }
    },
)
async def order_project(request: Request) -> Response:
    """Render the steward's default v1 order-export CSV.

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

    from reg_meta.catalog import Catalog  # noqa: PLC0415 — lazy: DB-bound

    with _project_conn(db_path) as conn:
        csv_text = render_order_csv(project, Catalog(conn))
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"content-disposition": 'attachment; filename="order.csv"'},
    )
