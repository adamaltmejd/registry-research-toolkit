"""Headline browse counts in the selected artifact and read scope."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from reg_meta.catalog import Catalog, CatalogSizes

from reg_webapp.conn import catalog_conn
from reg_webapp.scope import browse_scope

router = APIRouter(prefix="/api", dependencies=[Depends(browse_scope)])


@router.get("/stats", response_model=CatalogSizes)
def get_stats(request: Request) -> CatalogSizes:
    """Headline counts after the shared SQL scope predicate."""
    with catalog_conn(request) as conn:
        return Catalog(conn, scope=request.state.read_scope).catalog_sizes()
