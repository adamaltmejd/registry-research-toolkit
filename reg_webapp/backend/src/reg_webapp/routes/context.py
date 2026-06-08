"""``GET /api/context`` — deployment identity, branding, build info.

See DESIGN.md → API surface. Reads only the reg_meta ``import_manifest``
(stashed on ``app.state`` by the
lifespan) + the loaded steward + installed package versions. No git
dependency.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

import reg_meta
from reg_webapp import __version__
from reg_webapp.models import (
    CatalogDriftWarning,
    ContextResponse,
    RegMetaInfo,
    StewardInfo,
    WebappInfo,
)

router = APIRouter(prefix="/api")


@router.get("/context", response_model=ContextResponse)
def get_context(request: Request) -> ContextResponse:
    manifest = request.app.state.manifest
    steward = request.app.state.steward
    # The in-memory index (None for the global deployment) carries the
    # boot-time steward-catalog drift warnings. Surface them so the SPA
    # can show a "catalog drift" banner; empty for global / an up-to-date catalog.
    index = request.app.state.catalog_index
    drift = [] if index is None else index.drift_warnings
    return ContextResponse(
        steward=StewardInfo(
            id=steward.id, name=steward.name, long_name=steward.long_name
        ),
        reg_meta=RegMetaInfo(
            schema_version=manifest["schema_version"],
            import_date=manifest["import_date"],
        ),
        webapp=WebappInfo(version=__version__, reg_meta_version=reg_meta.__version__),
        catalog_drift_warnings=[
            CatalogDriftWarning(code=w.code, path=w.path, message=w.message)
            for w in drift
        ],
    )
