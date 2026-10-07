"""Raw JSON request-body reader for the project-WRITE endpoints.

See DESIGN.md → Project-write surface (routes/project.py).

``POST /api/project/validate`` reads the body as a RAW dict rather than a typed
Pydantic body: ``/validate`` must DIAGNOSE a malformed spec (a typed body would
make FastAPI 422 the very inputs it exists to report). In particular, unknown
root keys must reach the structural validator verbatim so it can emit one stable
``unexpected_field`` issue per key; they must not be dropped by typed model
construction first. This reader parses the body through reg_meta's shared reader and maps a malformed
REQUEST (non-JSON, duplicate key, non-object, pathologically nested) to a 4xx —
distinct from a well-formed object that simply fails validation (a 200 with
``ok=false`` on ``/validate``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import HTTPException
from reg_meta.errors import RegMetaError
from reg_meta.order import parse_project

if TYPE_CHECKING:
    from fastapi import Request


async def read_raw_json_object(request: Request) -> dict[str, Any]:
    """Read the request body as a RAW JSON object (dict), or raise 400.

    Reads the bytes via the async ``request.body()`` (so the caller stays an
    ``async def`` handler that then offloads blocking work to the threadpool)
    and parses them with reg_meta's shared ``order.parse_project`` — the same
    reader ``reg-meta validate`` / ``reg-meta order`` use for a file, so both
    adapters refuse the same malformed bytes (non-JSON, a duplicate key, a
    non-object top level, a pathologically nested body — see DESIGN.md →
    input-validation gates (security boundary)) with the same words: the 400
    ``detail`` is the shared error's ``message``, which the CLI envelopes under
    ``error.message`` with exit 10."""
    try:
        return parse_project(await request.body())
    except RegMetaError as exc:
        raise HTTPException(status_code=400, detail=exc.message) from exc
