"""Deployment branding and admitted catalog generation identity."""

from __future__ import annotations

from fastapi import APIRouter, Request

import reg_meta
from reg_webapp import __version__
from reg_webapp.models import (
    CatalogPeriodSpan,
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
    kind = manifest["catalog_artifact_kind"]
    bounds = request.app.state.catalog_period_bounds
    span = None
    if bounds is not None and bounds[0] is not None:
        first = int(bounds[0][:4])
        last = min(int(bounds[1][:4]), int(manifest["import_date"][:4]))
        if first <= last:
            span = CatalogPeriodSpan.model_validate({"from": first, "to": last})
    return ContextResponse(
        steward=StewardInfo(
            id=steward.id,
            name=steward.name,
            long_name=steward.long_name,
            catalog_period_span=span,
        ),
        reg_meta=RegMetaInfo(
            schema_version=manifest["schema_version"],
            import_date=manifest["import_date"],
            catalog_artifact_kind=kind,
            steward=manifest.get("steward") if kind == "steward" else None,
            generation_id=manifest["generation_id"],
            default_scope="holdings" if kind == "steward" else "reference",
        ),
        webapp=WebappInfo(version=__version__, reg_meta_version=reg_meta.__version__),
    )
