"""`POST /api/bundle` — build the MONA upload bundle (§9.5, A5.2b-ii).

Embeds the supplied ``project_data.json`` into a single-file ``.py`` MONA bundle
and streams it back as ``application/octet-stream``. The bundle is a PURE
function of its input (§9.5) — building the same spec twice yields byte-identical
output (the §16 bundle-determinism property).

**The verified 3-call reuse chain** (mirrors ``mock_data_wizard.cli`` ``
_cmd_build_bundle``, the canonical caller):

1. ``validate_project_data(payload)`` — the §6.8.1 Pydantic structural gate; runs
   the full reg_schema validator + the §6.8.2 block validator + the cross-block
   referential checks. Raises ``ValueError`` / ``ValidationError`` on bad input.
2. ``project_data_to_loadedspec(validated)`` — exercises the step-4 runtime
   capability gates (datetime / composite key / missing display_name), so an
   unsupported spec fails fast HERE rather than deep inside the MONA runner. Its
   ``LoadedSpec`` result is discarded; we embed the original dict.
3. ``build_bundle(output, project_data=payload)`` — amalgamates the runtime into
   ``output`` (a Path) with the dict embedded.

**Status discipline.** A ``ValueError`` / ``ValidationError`` from step 1/2 is bad
INPUT → 422 (input-validation text, no internal IDs — mirrors the catalog
``EXIT_USAGE`` → 422 rationale). A malformed JSON body (the raw-dict path, same as
``/project/validate``) is a malformed REQUEST → 400. A successful build is 200
with the ``.py`` bytes.

**§9.6 boundary.** Imports ``reg_monabundle.build.spec_loader`` (the Pydantic
BUILD side — runs locally, never amalgamated) and the top-level
``reg_monabundle.build_bundle`` — NOT ``reg_monabundle.runtime.*`` / duckdb /
pyodbc. ``spec_loader``'s ``project_data_to_loadedspec`` imports the runtime
LAZILY inside its own body, so importing ``spec_loader`` here does not pull the
runtime into the webapp import graph (the import-graph test pins this).

``build_bundle`` writes to a PATH, so the handler builds into a
``TemporaryDirectory`` and reads the bytes back before the directory (and the
file) are cleaned up on context exit. The bytes are held in memory (the v1 bundle
is capped at 1 MB, §12) and returned in one ``Response``.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import ValidationError

# Runtime import (not TYPE_CHECKING): FastAPI reads the handler's body annotation
# at runtime (get_type_hints) to build the request model, so ProjectData must be
# importable at runtime — moving it into a type-checking block breaks the route.
from reg_schema.project_data import ProjectData  # noqa: TC002

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
def build_mona_bundle(project: ProjectData) -> Response:
    """Build the MONA bundle embedding ``project`` and return the ``.py`` bytes.

    The body is typed ``ProjectData`` (the §9.6 reg_schema-as-request-model path):
    a structurally malformed body is a framework 422 before this runs, so the
    handler sees a model-valid spec. We still re-run the build-side validation
    gate (``validate_project_data``) on the round-tripped dict because it performs
    the §6.8.2 block + cross-block + step-4 capability checks that the model alone
    does NOT (a model-valid spec can still fail those build-time gates) — those
    failures are bad INPUT → 422.

    DB-free: the bundle build does not touch reg_meta, so no connection is opened
    here (unlike ``/project/*``). Pure function of input → safe to cache by
    content-hash at the edge (§9.4), which is why this endpoint sets no ETag.
    """
    # Round-trip to a JSON-canonical dict so the build chain sees the same bytes
    # `json.load`(project_data.json) produces (the canonical mdw CLI path):
    #   - `by_alias` so `period`'s `from` alias + the discriminated time-key
    #     wrappers round-trip;
    #   - `mode="json"` so the tuple-backed `sources` / `panels` fields serialize
    #     as LISTS, not tuples — the stdlib structural validator asserts
    #     `isinstance(value, list)` ("must be an array"), which a raw tuple fails;
    #   - `exclude_none=True` so an UNSET optional block (`reg_monabundle=None`)
    #     and unset optional binding fields are ABSENT rather than `null` — the
    #     structural validator rejects `reg_monabundle: null` ("must be an object")
    #     since a file-loaded spec simply omits the key. Optional binding fields
    #     are read with `.get()` downstream, so dropping their nulls is safe.
    payload = project.model_dump(by_alias=True, mode="json", exclude_none=True)

    # Lazy imports: keep the spec_loader (Pydantic build side) import inside the
    # handler so module import of this router stays light, and so a future
    # introspection of the import graph sees the runtime-free surface. spec_loader
    # itself imports the runtime LAZILY (inside project_data_to_loadedspec), so
    # this does not pull reg_monabundle.runtime.* / duckdb / pyodbc.
    from reg_monabundle.build.spec_loader import (  # noqa: PLC0415
        project_data_to_loadedspec,
        validate_project_data,
    )

    from reg_monabundle import build_bundle  # noqa: PLC0415

    try:
        validated = validate_project_data(payload)
        # Exercises the step-4 runtime capability gates so an unsupported spec
        # fails HERE (422), not deep inside the MONA runner. Result discarded —
        # we embed the original validated dict, not the LoadedSpec.
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
        build_bundle(output, project_data=payload)
        bundle_bytes = output.read_bytes()

    return Response(
        content=bundle_bytes,
        media_type="application/octet-stream",
        headers={"content-disposition": 'attachment; filename="mona_bundle.py"'},
    )
