"""FastAPI app factory + lifespan (the reg_meta boot seam).

The lifespan opens the real reg_meta DB read-only via reg_meta's own helpers
(``db_path_from_args`` + ``open_db``). ``open_db`` already opens ``mode=ro``
AND runs ``_check_schema_compat`` (the load-bearing SCHEMA_VERSION gate vs the
DB manifest) — we do NOT hardcode the path or reimplement the check. A5.1a
needs only the manifest snapshot, so the boot connection is closed once it's
read; the parsed manifest lives on ``app.state`` alongside the resolved
``db_path``. A single ``sqlite3`` connection from this lifespan is NOT safe to
query from FastAPI's sync-handler threadpool, so the catalog routes (A5.1b) open
a FRESH read-only connection PER REQUEST from ``app.state.db_path`` instead of
holding a long-lived shared one — see ``routes/catalog.py`` ``_catalog_conn``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import reg_meta.db
from fastapi import FastAPI

from . import __version__
from .middleware import ETagMiddleware
from .routes import catalog, context
from .stewards import load_steward

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

# Manifest keys /api/context surfaces; validated at boot so a malformed DB fails
# fast instead of as an opaque per-request 500. schema_version is already
# guaranteed by open_db's gate; import_date is the one this adds.
_REQUIRED_MANIFEST_KEYS = ("schema_version", "import_date")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # db_arg=None → reg_meta's default-path resolution (REG_META_DB > XDG >
    # platform default). open_db opens mode=ro AND runs _check_schema_compat,
    # the load-bearing SCHEMA_VERSION gate vs the DB manifest — an incompatible
    # major (or too-old minor) raises RegMetaError here, failing startup fast.
    db_path = reg_meta.db.db_path_from_args(None)
    conn = reg_meta.db.open_db(db_path)
    try:
        manifest = reg_meta.db.get_manifest(conn)
    finally:
        conn.close()
    if missing := [key for key in _REQUIRED_MANIFEST_KEYS if key not in manifest]:
        raise RuntimeError(
            f"reg_meta manifest at {db_path} missing key(s): {', '.join(missing)}"
        )
    app.state.manifest = manifest
    app.state.steward = load_steward()
    # The catalog routes open a FRESH read-only connection PER REQUEST from this
    # boot-resolved path (the connection model is locked: a shared sqlite3 conn
    # isn't safe across FastAPI's sync-handler threadpool). The schema was
    # already validated by open_db above, so the per-request open skips the
    # re-check (check_schema=False) — see routes/catalog.py `_catalog_conn`.
    app.state.db_path = db_path
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="reg_webapp", version=__version__, lifespan=lifespan)
    # §9.4 read-cache: stamp ETag + Cache-Control on GET reads and serve 304 on a
    # matching If-None-Match. Skips write endpoints (method gate, A5.2b). Added
    # before the routers so it wraps every read response.
    app.add_middleware(ETagMiddleware)
    app.include_router(context.router)
    app.include_router(catalog.router)
    return app
