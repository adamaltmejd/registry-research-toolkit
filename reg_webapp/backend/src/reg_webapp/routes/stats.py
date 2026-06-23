"""``GET /api/stats`` — headline catalog-size counts for the landing page.

See DESIGN.md → Catalog stats. A TOP-LEVEL route (sibling of ``/api/context``),
deliberately NOT under ``/api/catalog`` (that prefix is a ``{fqid:path}``
catch-all). The counts come from reg_meta's ``Catalog.catalog_sizes()``
(slug-aware — matches the browse listings), consumed as the response model
directly (#681). Opened through the SAME per-request ``catalog_conn`` seam
(``conn.py``) the catalog routes use.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from reg_meta.catalog import Catalog, CatalogSizes

from reg_webapp.conn import catalog_conn as _catalog_conn

router = APIRouter(prefix="/api")


@router.get("/stats", response_model=CatalogSizes)
def get_stats(request: Request) -> CatalogSizes:
    """Headline catalog counts (providers / registers / variables) for the
    landing page — slug-aware, matching the browse. A filtered steward still
    sees full-universe counts (a follow-up; see DESIGN.md → Catalog stats)."""
    with _catalog_conn(request) as conn:
        return Catalog(conn).catalog_sizes()
