"""`POST /api/project/*` — the project-WRITE surface (§9.5, A5.2b-ii).

Two endpoints:

- ``POST /api/project/validate`` — runs the §6.8.0 three-layer validator over a
  raw ``project_data.json`` and returns the CONCATENATED issue list (structural
  ⧺ block ⧺ semantic) as a ``ValidationResultModel``.
- ``POST /api/project/order`` — renders the steward's default v1 order-export CSV
  (``order_export.render_order_csv``) as a ``text/csv`` download.

**Status discipline (§6.8.0 / §9.5).** ``/validate`` is a *diagnostic*: a spec
that FAILS validation is a SUCCESSFUL validation RESPONSE — HTTP 200 with
``ok=false`` + the issues. 4xx is reserved for a malformed REQUEST: non-JSON
body, duplicate JSON keys, wrong content-type, or an oversized body (the last
handled by ``BodySizeLimitMiddleware`` before the handler runs). An extra/typo
KEY in the spec must surface as a structural (or model-construction) ISSUE in the
200 body — NEVER a 500: ``validate_structural`` does not enforce ``extra=forbid``
but the ``ProjectData`` model does, so the model build is wrapped and a
``ValidationError`` is turned into an issue rather than escaping as a 500.

**Connection model = per-request open IN THE HANDLER BODY** (LOCKED, copied from
``routes/catalog.py``). The semantic layer needs a live ``Catalog``; we open a
fresh read-only connection via the ``_project_conn`` contextmanager used as a
plain ``with`` INSIDE the sync handler (NOT a generator ``Depends`` — a
dependency is entered on a possibly-different threadpool thread than the handler,
so a dependency-opened sqlite connection is used cross-thread →
``sqlite3.ProgrammingError`` under concurrency; the A5.2a/b-i P1). The body parse
+ structural layer are DB-FREE and run BEFORE the open, so a malformed or
structurally-rejected body costs no DB hit.

§9.6: this module imports ``reg_schema`` (structural validator + ``ProjectData``)
and ``reg_monabundle.validate_block`` (the pure-stdlib §6.8.2 block gate) — NOT
``reg_monabundle.runtime.*`` / duckdb / pyodbc.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import reg_meta.db
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import ValidationError
from reg_schema.project_data import ProjectData
from reg_schema.structural import validate_structural
from reg_schema.validation import ValidationIssue, ValidationResult

from reg_monabundle import validate_block
from reg_webapp.models import (
    ValidationIssueModel,
    ValidationResultModel,
)
from reg_webapp.order_export import render_order_csv
from reg_webapp.semantic import validate_semantic

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator

    from reg_meta.catalog import Catalog

router = APIRouter(prefix="/api/project")


@contextmanager
def _project_conn(request: Request) -> Iterator[sqlite3.Connection]:
    """A per-request reg_meta read-only connection, opened ON THE CALLING THREAD.

    Identical contract to ``routes/catalog.py``'s ``_catalog_conn``: used as a
    plain ``with`` INSIDE the sync handler body (NOT a FastAPI ``Depends``), so
    open + query + close stay on one thread — the load-bearing cross-thread-safety
    property (a generator dependency runs on a possibly-different AnyIO threadpool
    thread → ``sqlite3.ProgrammingError`` under concurrency). ``check_schema=False``:
    the lifespan already validated the schema at boot."""
    conn = reg_meta.db.open_db(request.app.state.db_path, check_schema=False)
    try:
        yield conn
    finally:
        conn.close()


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """``json.loads`` ``object_pairs_hook`` that raises on a duplicate JSON key.

    The default keeps the last value silently — a hand-edited project_data.json
    with a duplicated field would validate against the wrong (last-wins) value. We
    own a webapp-local copy rather than importing the runtime one
    (``reg_monabundle.runtime.spec._reject_duplicate_keys``): that module is the
    MONA-amalgamated runtime, off-limits to the webapp per the §9.6 import
    boundary (and the import-graph test)."""
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise ValueError(f"duplicate key {key!r} in request body")
        seen[key] = value
    return seen


async def _read_raw_project(request: Request) -> dict[str, Any]:
    """Read the request body as a RAW dict (§9.5).

    Deliberately NOT a typed Pydantic body param: a typed body would make FastAPI
    422 the very malformed specs ``/validate`` exists to DIAGNOSE (they belong in
    the 200 issue list, not a framework 422). So we read the bytes and json.loads
    them ourselves, rejecting non-JSON / duplicate-key / non-object bodies as a
    malformed REQUEST (4xx) — distinct from a well-formed JSON object that simply
    fails validation (200 with ``ok=false``)."""
    raw_bytes = await request.body()
    try:
        parsed = json.loads(raw_bytes, object_pairs_hook=_reject_duplicate_keys)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400, detail=f"request body is not valid JSON: {exc}"
        ) from exc
    except ValueError as exc:
        # `_reject_duplicate_keys` raises ValueError (not JSONDecodeError) from
        # inside json.loads on a duplicate key.
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=400,
            detail="request body must be a JSON object (project_data.json shape)",
        )
    return parsed


def _model_issue(message: str, exc: ValidationError) -> ValidationIssue:
    """Turn a ``ProjectData.model_validate`` failure into a STRUCTURAL-level issue.

    ``validate_structural`` (pure stdlib) does NOT enforce ``extra=forbid``, but
    the ``ProjectData`` Pydantic model does — so an extra/typo key that the
    structural layer admits trips here when we build the model for the semantic
    step. That is user error in the SPEC (a typo'd field), so it belongs in the
    200 issue list as an error, NOT a 500.

    ``invalid_field`` is a WEBAPP-composition code, not yet a canonical §6.8.0
    code — the cleaner home is reg_schema's structural validator emitting it
    directly (the open ``validate_structural`` ``extra=forbid`` question, see the
    MIGRATION_PLAN A5.2 note). No consumer maps it yet (the SPA is A5.3), so
    relocating it before A5.3 is churn-free; flagged for the maintainer's call.
    The path points at the first offending field when Pydantic reports one."""
    errors = exc.errors()
    path = "/" + "/".join(str(p) for p in errors[0]["loc"]) if errors else ""
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
    """Run the §6.8.2 ``reg_monabundle`` block validator and adapt its raise to an
    issue list (tuple-concatenation composition, §6.8.0).

    ``validate_block`` is fail-fast (raises ``ValueError`` on a bad block) rather
    than issue-accumulating, so we translate a raise into a single
    ``invalid_block`` error issue — keeping the three layers uniform as one
    concatenated list. An absent ``reg_monabundle`` block validates trivially
    (``validate_block(None)`` is a no-op).

    ``invalid_block`` is a WEBAPP-composition code (the webapp owns the three-layer
    concatenation). The cleaner home is an issue-based ``validate_block`` in
    reg_monabundle returning ``list[ValidationIssue]`` directly — same relocation
    question as ``invalid_field``; no consumer maps it yet (SPA = A5.3), so it's
    churn-free to revisit. Flagged for the maintainer."""
    block = raw.get("reg_monabundle")
    try:
        validate_block(block)
    except ValueError as exc:
        return [
            ValidationIssue(
                level="error",
                code="invalid_block",
                path="/reg_monabundle",
                message=str(exc),
            )
        ]
    return []


def _semantic_issues(raw: dict[str, Any], catalog: Catalog) -> list[ValidationIssue]:
    """Build the ``ProjectData`` model and run the §6.8.3 semantic layer.

    Reached only when the structural layer passed (the caller short-circuits a
    structural failure). The model build can still raise on an ``extra=forbid``
    violation the structural layer doesn't catch — that becomes an
    ``invalid_field`` issue (NOT a 500), and the semantic step is skipped (it
    needs a constructed model)."""
    try:
        project = ProjectData.model_validate(raw)
    except ValidationError as exc:
        return [
            _model_issue(
                "project_data failed model construction (an unrecognized or "
                f"invalid field?): {exc}",
                exc,
            )
        ]
    result = validate_semantic(project, catalog, caller="researcher")
    return list(result.issues)


@router.post("/validate", response_model=ValidationResultModel)
async def validate_project(request: Request) -> ValidationResultModel:
    """Validate a ``project_data.json`` (§6.8.0). Returns 200 with the concatenated
    structural ⧺ block ⧺ semantic issue list and the derived ``ok`` flag; a 4xx is
    reserved for a malformed REQUEST (see ``_read_raw_project`` / the body cap).

    Layer order (DB-free layers first, so a structurally-rejected body costs no DB
    hit): structural → block → (model build + semantic). The semantic layer needs
    a live ``Catalog``, opened per-request in THIS body. If structural fails we
    SKIP the model build + semantic step (they assume a structurally valid spec)
    but STILL report the block issues — the block validator is independent of the
    structural one and a researcher benefits from seeing both."""
    raw = await _read_raw_project(request)

    issues: list[ValidationIssue] = []
    structural = validate_structural(raw)
    issues.extend(structural.issues)
    issues.extend(_block_issues(raw))

    if structural.ok:
        # Semantic resolution assumes well-formed FQIDs / period grammar (the
        # structural layer's job), so it only runs when structural passed. The
        # connection opens HERE (in the handler body, one thread) and AFTER the
        # DB-free layers — a structurally invalid body never reaches the open.
        with _project_conn(request) as conn:
            from reg_meta.catalog import Catalog  # noqa: PLC0415 — lazy: DB-bound

            issues.extend(_semantic_issues(raw, Catalog(conn)))

    return _to_result_model(ValidationResult(issues=tuple(issues)))


# §9.5: the order export is a CSV DOWNLOAD, so this endpoint cannot declare a
# Pydantic `response_model=` — it returns raw `text/csv` bytes, the ONE documented
# exception to the "every route declares a response_model" lint. Documented here
# so the carve-out is explicit (a binary/download response, not a JSON model).
# `responses=` + `response_class` declare the `text/csv` media type so the
# OpenAPI contract (and the SPA's codegen) sees a download, not a JSON body.
@router.post(
    "/order",
    response_class=Response,
    responses={200: {"content": {"text/csv": {}}}},
)
def order_project(request: Request, project: ProjectData) -> Response:
    """Render the steward's default v1 order-export CSV (§9.5).

    Takes a ``project_data.json``-shaped body typed as ``ProjectData`` directly
    (the §9.6 sanctioned reg_schema-as-request-model path — unlike ``/validate``,
    which needs the raw dict to diagnose malformed specs, ``/order`` operates on a
    well-formed spec, so a framework 422 on a malformed body is the right behavior
    here). Resolves missing ``display_name``s from reg_meta via a per-request
    connection opened IN THIS BODY (the locked connection model). Returns the CSV
    as a ``text/csv`` attachment."""
    with _project_conn(request) as conn:
        from reg_meta.catalog import Catalog  # noqa: PLC0415 — lazy: DB-bound

        csv_text = render_order_csv(project, Catalog(conn))
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"content-disposition": 'attachment; filename="order.csv"'},
    )
