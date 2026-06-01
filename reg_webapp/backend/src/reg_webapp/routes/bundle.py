"""`POST /api/bundle` — build the MONA upload bundle (§9.5, A5.2b-ii).

Embeds the supplied ``project_data.json`` into a single-file ``.py`` MONA bundle
and streams it back as ``application/octet-stream``. The bundle is a PURE
function of its input (§9.5) — building the same spec twice yields byte-identical
output (the §16 bundle-determinism property).

**Reads the RAW dict** (like ``/api/project/validate``), NOT a typed ``ProjectData``
body: the model is ``extra="ignore"`` at the top level, so a typed body would
silently DROP steward-namespaced blocks (``swecov`` / ``reg_mockdata``, §6.8.2)
from the embedded spec. Embedding the raw dict makes the bundle faithfully
reproduce the submitted spec — it IS the file-load path (``json.load`` →
``build_bundle``), which is why the output matches the canonical CLI bundle.

**The verified 3-call reuse chain** (mirrors ``mock_data_wizard.cli``
``_cmd_build_bundle``, the canonical caller):

1. ``validate_project_data(raw)`` — the §6.8.1 Pydantic structural gate; runs the
   full reg_schema validator + the §6.8.2 block validator + the cross-block
   referential checks. Raises ``ValueError`` / ``ValidationError`` on bad input.
2. ``project_data_to_loadedspec(validated)`` — exercises the step-4 runtime
   capability gates (datetime / composite key / missing display_name), so an
   unsupported spec fails fast HERE rather than deep inside the MONA runner. Its
   ``LoadedSpec`` result is discarded; we embed the original raw dict.
3. ``build_bundle(output, project_data=raw)`` — amalgamates the runtime into
   ``output`` (a Path) with the raw dict embedded.

**Status discipline.** A ``ValueError`` / ``ValidationError`` from step 1/2 is bad
INPUT → 422 (input-validation text, no internal IDs — mirrors the catalog
``EXIT_USAGE`` → 422 rationale). A malformed JSON / duplicate-key / non-object body
is a malformed REQUEST → 400 (``read_raw_json_object``). A successful build is 200
with the ``.py`` bytes.

``async`` only to read the body; the BLOCKING build (validation + amalgamation +
file IO) is offloaded to the threadpool (``run_in_threadpool``) so it never stalls
the event loop. DB-free — the bundle build does not touch reg_meta, so no
connection is opened (and no ETag: a pure function of input → content-hash
cacheable at the edge, §9.4).

**§9.6 boundary.** Imports ``reg_monabundle.build.spec_loader`` (the Pydantic
BUILD side — runs locally, never amalgamated) and the top-level
``reg_monabundle.build_bundle`` — NOT ``reg_monabundle.runtime.*`` / duckdb /
pyodbc. ``spec_loader``'s ``project_data_to_loadedspec`` imports the runtime
LAZILY inside its own body, so importing it here does not pull the runtime into
the webapp import graph (the import-graph test pins this).
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from pydantic import ValidationError

from reg_webapp.request_body import read_raw_json_object

router = APIRouter(prefix="/api")


# §9.5: the bundle is a binary `.py` DOWNLOAD (application/octet-stream), the same
# response_model-lint carve-out as `/project/order`. `responses=` + `response_class`
# declare the octet-stream media type so the OpenAPI contract / SPA codegen sees a
# download, not a JSON body.
@router.post(
    "/bundle",
    response_class=Response,
    responses={200: {"content": {"application/octet-stream": {}}}},
)
async def build_mona_bundle(request: Request) -> Response:
    """Build the MONA bundle embedding the posted ``project_data.json`` and return
    the ``.py`` bytes. Reads the raw dict (preserving namespaced blocks) and
    offloads the blocking build to the threadpool."""
    raw = await read_raw_json_object(request)
    return await run_in_threadpool(_build_bundle_blocking, raw)


def _build_bundle_blocking(raw: dict[str, Any]) -> Response:
    """The blocking bundle build (the 3-call reuse chain + amalgamation + file IO),
    run on a threadpool thread. Embeds the RAW dict so steward-namespaced blocks
    survive into the bundle. A build-gate raise (bad input) → 422."""
    # Lazy imports: keep the spec_loader (Pydantic build side) import inside the
    # handler so module import of this router stays light, and so the import-graph
    # introspection sees a runtime-free surface. spec_loader itself imports the
    # runtime LAZILY (inside project_data_to_loadedspec), so this does not pull
    # reg_monabundle.runtime.* / duckdb / pyodbc.
    from reg_monabundle.build.spec_loader import (  # noqa: PLC0415
        project_data_to_loadedspec,
        validate_project_data,
    )

    from reg_monabundle import build_bundle  # noqa: PLC0415

    try:
        validated = validate_project_data(raw)
        # Exercises the step-4 runtime capability gates so an unsupported spec
        # fails HERE (422), not deep inside the MONA runner. Result discarded —
        # we embed the original raw dict, not the LoadedSpec.
        project_data_to_loadedspec(validated)
    except (ValueError, ValidationError) as exc:
        # Bad input → 422 (input-validation text; ValueError messages from the
        # build gate are user-facing, not internal-ID-bearing).
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # build_bundle writes to a Path; build into a TemporaryDirectory and read the
    # bytes back before the directory (and file) are removed on context exit. The
    # per-request unique tempdir keeps concurrent builds from colliding on a path.
    with tempfile.TemporaryDirectory(prefix="reg_webapp_bundle_") as tmpdir:
        output = Path(tmpdir) / "mona_bundle.py"
        build_bundle(output, project_data=raw)
        bundle_bytes = output.read_bytes()

    return Response(
        content=bundle_bytes,
        media_type="application/octet-stream",
        headers={"content-disposition": 'attachment; filename="mona_bundle.py"'},
    )
