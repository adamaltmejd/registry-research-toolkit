"""``GET /api/stats`` — headline catalog-size counts for the landing page.

See DESIGN.md → Catalog stats. A TOP-LEVEL route (sibling of ``/api/context``),
deliberately NOT under ``/api/catalog`` (that prefix is a ``{fqid:path}``
catch-all). The ``global`` deployment uses reg_meta's
``Catalog.catalog_sizes()`` (slug-aware — matches the browse listings). A filtered
steward uses the webapp's boot-time ``CatalogIndex`` so the landing-page counts
reflect only that steward's catalog. Both paths return reg_meta's
``CatalogSizes`` response model directly (#681).
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from reg_meta.catalog import Catalog, CatalogSizes

from reg_webapp.conn import catalog_conn as _catalog_conn

router = APIRouter(prefix="/api")


@router.get("/stats", response_model=CatalogSizes)
def get_stats(request: Request) -> CatalogSizes:
    """Headline catalog counts (providers / registers / variables) for the
    landing page — full-universe for ``global``, steward-filtered when the
    deployment loaded a ``CatalogIndex``."""
    index = request.app.state.catalog_index
    if index is not None:
        return index.catalog_sizes()
    with _catalog_conn(request) as conn:
        return Catalog(conn).catalog_sizes()
