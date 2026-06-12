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
import reg_meta.doc_db
from fastapi import FastAPI
from reg_meta.catalog import Catalog
from reg_meta.errors import RegMetaError

from . import __version__
from .limits import (
    RATE_LIMIT_PER_MINUTE,
    BodySizeLimitMiddleware,
    RateLimitMiddleware,
)
from .middleware import ETagMiddleware
from .routes import bundle, catalog, context, docs, project, search
from .stewards import load_catalog_index, load_steward

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

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
    # Resolve the steward BEFORE opening the conn: load_steward raises on a
    # misconfigured deployment, and doing it first means that raise can't leak the
    # just-opened connection.
    steward = load_steward()
    conn = reg_meta.db.open_db(db_path)
    try:
        manifest = reg_meta.db.get_manifest(conn)
        # Build the in-memory steward catalog index on the SAME boot
        # connection, BEFORE it closes. Boot is single-threaded, so the
        # per-request open-a-fresh-conn rule (which guards the sync-handler
        # threadpool) does NOT apply here — reusing the boot conn is correct.
        # A reg_meta-drift'd steward catalog still BOOTS: the steward-mode
        # downgrade (see DESIGN.md → Semantic validation (semantic.py)) turns
        # unresolved FQIDs into warnings, drops the
        # affected bindings from the index, and surfaces the drift on
        # /api/context — it does NOT crash startup. `None` for the global
        # deployment (no filter, full universe).
        catalog_index = load_catalog_index(steward, Catalog(conn))
    finally:
        conn.close()
    if missing := [key for key in _REQUIRED_MANIFEST_KEYS if key not in manifest]:
        raise RuntimeError(
            f"reg_meta manifest at {db_path} missing key(s): {', '.join(missing)}"
        )
    app.state.manifest = manifest
    app.state.steward = steward
    app.state.catalog_index = catalog_index
    # The catalog routes open a FRESH read-only connection PER REQUEST from this
    # boot-resolved path (the connection model is locked: a shared sqlite3 conn
    # isn't safe across FastAPI's sync-handler threadpool). The schema was
    # already validated by open_db above, so the per-request open skips the
    # re-check (check_schema=False) — see routes/catalog.py `_catalog_conn`.
    app.state.db_path = db_path
    # Docs library (#354) is OPTIONAL: the deployed container ships
    # reg_meta_docs.db, but a dev/test env (or a steward without docs) may lack
    # it. Resolve + validate it ONCE here; on absence OR schema-incompat, leave
    # `docs_db_path` None so the docs endpoints degrade to "not ingested" rather
    # than 500 — a broken/missing docs index must NOT take down the catalog API.
    # The validated path feeds the per-request open (check_schema=False) in
    # routes/docs.py `docs_conn`.
    app.state.docs_db_path = _resolve_docs_db_path()
    yield


def _resolve_docs_db_path() -> Path | None:
    """Resolve the docs DB the same way reg_meta does (REG_META_DB > XDG >
    platform), validating it once. Returns the path when present + schema-compat,
    else None (docs gracefully unavailable). Never raises — docs are auxiliary."""
    docs_db_path = reg_meta.doc_db.doc_db_path(None)
    try:
        # Raises RegMetaError on missing file OR incompatible schema.
        conn = reg_meta.doc_db.open_doc_db(docs_db_path)
    except RegMetaError:
        return None
    conn.close()
    return docs_db_path


def create_app(*, rate_limit_per_minute: int = RATE_LIMIT_PER_MINUTE) -> FastAPI:
    """Build the FastAPI app.

    ``rate_limit_per_minute`` defaults to the cost-protection budget; it's a parameter ONLY
    so tests that need to drive the write endpoints harder than 30 req/min (the
    cross-thread concurrency smoke tests) can raise it without disabling the
    middleware — the limiter is still IN the stack, just with a higher ceiling.
    Production callers use the default."""
    # redoc_url=None: the deployed edge worker forwards a fixed backend-path set
    # (/api, /openapi.json, /docs — see reg_webapp/edge/), and /redoc would fall
    # through to the SPA shell there; disable it so the local and deployed
    # surfaces match. Swagger at /docs is the one interactive-docs surface.
    app = FastAPI(
        title="reg_webapp", version=__version__, lifespan=lifespan, redoc_url=None
    )
    # Middleware ordering (Starlette executes add_middleware in REVERSE order —
    # last-added runs OUTERMOST / first on the way in). Cost protection (see
    # DESIGN.md → Cost protection (limits.py)) must
    # gate a write BEFORE the handler reads the body, so the cap + limiter run
    # outermost. Adding the rate limiter LAST puts it outermost (it rejects an
    # over-budget IP before the body is even buffered); the body cap next (it
    # streams + counts the body before the handler reads it); the ETag middleware
    # innermost (GET/HEAD-only — writes pass through it untouched, confirmed in
    # middleware.py: `_CACHEABLE_METHODS == {"GET"}`).
    app.add_middleware(ETagMiddleware)
    app.add_middleware(BodySizeLimitMiddleware)
    app.add_middleware(RateLimitMiddleware, per_minute=rate_limit_per_minute)
    app.include_router(context.router)
    app.include_router(catalog.router)
    # Global FTS search (#350) — a GET read, so it rides the same ETag/edge-cache
    # axis as the catalog routes.
    app.include_router(search.router)
    # Docs library (#354) — GET reads over the optional reg_meta_docs.db; same
    # ETag/edge-cache axis as the catalog routes.
    app.include_router(docs.router)
    # A5.2b-ii write surface: project validate/order + bundle build. The ETag
    # middleware skips these (method gate); the cap + limiter gate them.
    app.include_router(project.router)
    app.include_router(bundle.router)
    return app
