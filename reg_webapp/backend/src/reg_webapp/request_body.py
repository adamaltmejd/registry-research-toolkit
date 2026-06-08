"""Raw JSON request-body reader for the project-WRITE endpoints.

See DESIGN.md → Project-write surface (routes/project.py + routes/bundle.py).

``POST /api/project/validate`` and ``POST /api/bundle`` both read the body as a
RAW dict rather than a typed Pydantic body: ``/validate`` must DIAGNOSE a malformed
spec (a typed body would make FastAPI 422 the very inputs it exists to report), and
``/bundle`` must EMBED the spec verbatim — including steward-namespaced blocks
(``swecov`` / ``reg_mockdata`` — see reg_monabundle/DESIGN.md → The two halves)
that the ``ProjectData`` model drops
(``extra="ignore"``). So both share this reader, which json.loads the body
ourselves and maps a malformed REQUEST (non-JSON, duplicate key, non-object,
pathologically nested) to a 4xx — distinct from a well-formed object that simply
fails validation (a 200 with ``ok=false`` on ``/validate``).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException

if TYPE_CHECKING:
    from fastapi import Request


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """``json.loads`` ``object_pairs_hook`` that raises on a duplicate JSON key.

    The default keeps the last value silently — a hand-edited project_data.json
    with a duplicated field would validate against the wrong (last-wins) value.
    Fires at every nesting depth (the hook runs per object). We own a webapp-local
    copy rather than importing ``reg_monabundle.runtime.spec._reject_duplicate_keys``:
    that module is the MONA-amalgamated runtime, off-limits per the import
    boundary (the lightweight/runtime split — see reg_monabundle/DESIGN.md → The
    two halves; enforced by the import-graph test)."""
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise ValueError(f"duplicate key {key!r} in request body")
        seen[key] = value
    return seen


async def read_raw_json_object(request: Request) -> dict[str, Any]:
    """Read the request body as a RAW JSON object (dict), or raise 4xx.

    Reads the bytes via the async ``request.body()`` (so the caller stays an
    ``async def`` handler that then offloads blocking work to the threadpool),
    json.loads them ourselves, and rejects a malformed REQUEST with 400:
    non-JSON, a duplicate key, a non-object top level, or a pathologically nested
    body. ``RecursionError`` (a ``RuntimeError``, NOT a ``ValueError`` /
    ``JSONDecodeError``) is caught explicitly — a deeply-nested array that fits
    under the body-size cap would otherwise escape as a 500 (a write-side
    input crash — see DESIGN.md → input-validation gates (security boundary))."""
    raw_bytes = await request.body()
    try:
        parsed = json.loads(raw_bytes, object_pairs_hook=_reject_duplicate_keys)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400, detail=f"request body is not valid JSON: {exc}"
        ) from exc
    except RecursionError as exc:
        # Deeply-nested JSON exhausts the recursion limit before the cap notices
        # (a small body, huge depth). A malformed REQUEST, not a server fault.
        raise HTTPException(
            status_code=400, detail="request body is nested too deeply"
        ) from exc
    except ValueError as exc:
        # Two malformed-body cases, both caught here as ValueError: the
        # `_reject_duplicate_keys` duplicate-key ValueError (raised inside
        # json.loads), AND a UnicodeDecodeError from invalid-UTF-8 bytes —
        # `UnicodeDecodeError` IS a `ValueError` subclass (json.loads decodes the
        # bytes before parsing), so this clause covers it. Both → 400.
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=400,
            detail="request body must be a JSON object (project_data.json shape)",
        )
    return parsed
