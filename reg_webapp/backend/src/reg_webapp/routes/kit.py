"""`POST /api/kit` — build the downloadable generation kit (A5.2c, §8).

See DESIGN.md → Kit-build surface and ``reg_webapp.kit`` for the domain logic.
Validates a ``project_data.json`` and, when it passes, streams back a ZIP of the
**generation kit** (``project_data.json`` with materialized display names +
``project_data.codes.json`` + ``README.md``) the researcher runs ``reg-mockdata
generate`` against locally.

**Reads the RAW dict** (like ``/api/project/validate`` / ``/api/bundle``), NOT a
typed ``ProjectData`` body: a typed ``extra="ignore"`` body would silently drop
steward-namespaced blocks (``swecov`` / ``reg_mockdata``) that the kit's
``project_data.json`` must faithfully reproduce, and the codes/materialization
read the raw dict.

**Status discipline = GATE (like ``/order`` / ``/bundle``, NOT ``/validate``).**
A kit is built FROM a *validated* project, so an invalid spec is a **422** with
the blocking errors in the detail — not a 200 carrying a half-built kit. A
malformed REQUEST (non-JSON / duplicate keys / oversized) is a 4xx from
``read_raw_json_object`` / the body cap. The validation composition mirrors
``/api/project/validate`` (structural → block → semantic → cross-block referential)
PLUS the kit-only ``panel_inheritance_unresolvable`` check
(``validate_panel_inheritance``); the structural layer keeps a pre-kit SPA spec
valid while panel inheritance is unresolved, so that check lives here, at the
point inheritance is materialized. A ``/validate``-clean spec can therefore still
422 here on panel inheritance (or a ``/bundle``-style capability gate) — the
documented kit-build residual.

**Connection model = per-request open ON ONE THREAD** (the locked cross-thread
guard, identical to ``routes/project.py``). ``async`` only to read the body; the
blocking work (validation + reg_meta resolution + archive build) runs on a
threadpool thread, and the reg_meta connection opens on THAT thread inside a
``with`` block — NEVER a generator ``Depends`` (cross-thread ``sqlite3``).
DB-free layers (structural / block) run before the open.

**Import boundary** (see reg_monabundle/DESIGN.md → The two halves). Imports
``reg_schema`` + the BUILD-side ``reg_monabundle.build.spec_loader`` issue forms
(same as ``routes/project.py``) — NOT ``reg_monabundle.runtime.*`` / duckdb /
pyodbc; the kit is pure file packaging with no ``reg_mockdata`` dependency.
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

from reg_webapp.kit import build_kit_archive
from reg_webapp.request_body import read_raw_json_object
from reg_webapp.semantic import validate_panel_inheritance, validate_semantic

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterator
    from pathlib import Path

    from reg_schema.validation import ValidationIssue

    from reg_webapp.catalog_index import CatalogIndex

router = APIRouter(prefix="/api")


@contextmanager
def _kit_conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    """A per-request reg_meta read-only connection, opened ON THE CALLING THREAD
    (the threadpool thread running the offloaded build). Used as a plain ``with``
    (NOT a FastAPI ``Depends``) so open + query + close stay on ONE thread — the
    load-bearing cross-thread-safety property the A5.2a/b-i P1 established (see
    ``routes/project.py`` ``_project_conn``). ``check_schema=False``: the lifespan
    already validated the schema at boot."""
    conn = reg_meta.db.open_db(db_path, check_schema=False)
    try:
        yield conn
    finally:
        conn.close()


# The bundle/kit is a binary ZIP DOWNLOAD (application/zip), the same
# response_model-lint carve-out as `/project/order` + `/bundle`. `responses=` +
# `response_class` declare the media type so the OpenAPI contract / SPA codegen
# sees a download, not a JSON body.
@router.post(
    "/kit",
    response_class=Response,
    responses={200: {"content": {"application/zip": {}}}},
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    # Open object (Record<string, unknown>) so the codegen'd client
                    # can post a real project_data.json — a bare `type: object`
                    # codegens to the empty `Record<string, never>`.
                    "schema": {"type": "object", "additionalProperties": True}
                }
            },
        }
    },
)
async def build_kit(request: Request) -> Response:
    """Build the generation kit from the posted ``project_data.json`` and return
    the ZIP bytes. Reads the raw dict (preserving namespaced blocks) and offloads
    the blocking validate-then-build to the threadpool."""
    raw = await read_raw_json_object(request)
    return await run_in_threadpool(
        _kit_blocking,
        request.app.state.db_path,
        raw,
        request.app.state.catalog_index,
    )


def _kit_blocking(
    db_path: Path, raw: dict[str, Any], index: CatalogIndex | None
) -> Response:
    """The blocking kit build, run on a threadpool thread. Gates on validation
    errors (422) BEFORE assembling the archive. DB-free layers (structural / block)
    run first; the reg_meta connection opens on THIS thread (one thread → the
    cross-thread sqlite P1 can't recur) for the semantic layer + the
    dereference/materialize the archive needs."""
    # Structural is DB-free and a precondition for building the model the rest of
    # the pipeline (and the archive) needs — gate on it first, like `/order`.
    structural = validate_structural(raw)
    if not structural.ok:
        _raise_422("a structurally invalid spec", structural.issues)
    try:
        project = ProjectData.model_validate(raw)
    except ValidationError as exc:
        # A constraint the structural layer did not replicate (effectively
        # unreachable under today's models) — a 422, never a 500.
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    issues: list[ValidationIssue] = list(structural.issues)
    issue = block_issue(raw.get("reg_monabundle"))
    if issue is not None:
        issues.append(issue)

    from reg_meta.catalog import Catalog  # noqa: PLC0415 — lazy: DB-bound

    with _kit_conn(db_path) as conn:
        catalog = Catalog(conn)
        issues.extend(
            validate_semantic(project, catalog, caller="researcher", index=index).issues
        )
        issues.extend(binding_options_issues(raw.get("reg_monabundle"), project))
        issues.extend(validate_panel_inheritance(project, catalog).issues)

        errors = [i for i in issues if i.level == "error"]
        if errors:
            _raise_422("an invalid spec", tuple(errors))

        # Validation passed (warnings/info don't block) — assemble the archive on
        # the same connection (it dereferences codes + resolves display names).
        archive = build_kit_archive(raw, project, catalog, conn)

    return Response(
        content=archive,
        media_type="application/zip",
        headers={"content-disposition": 'attachment; filename="kit.zip"'},
    )


def _raise_422(reason: str, issues: tuple[ValidationIssue, ...]) -> None:
    """422 with the blocking ERROR issues summarized in the detail (input-validation
    text, no internal IDs — mirrors ``/order``). The SPA normally ``/validate``s
    first for the structured issue list; this is the build-time backstop, and it
    additionally carries the kit-only ``panel_inheritance_unresolvable`` errors
    ``/validate`` does not emit."""
    errors = [i for i in issues if i.level == "error"]
    detail = "; ".join(f"{i.code}@{i.path}" for i in errors)
    raise HTTPException(
        status_code=422,
        detail=f"cannot build a kit from {reason}: {detail}",
    )
