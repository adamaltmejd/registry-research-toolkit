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
from typing import TYPE_CHECKING, Any

import reg_meta.db
from fastapi import FastAPI
from reg_meta.holdings import resolve_scope

from . import __version__
from .limits import (
    RATE_LIMIT_PER_MINUTE,
    BodySizeLimitMiddleware,
    RateLimitMiddleware,
)
from .middleware import ETagMiddleware
from .routes import catalog, project
from .stewards import load_steward

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class _RegistryApp(FastAPI):
    """FastAPI app with the raw-ingress ProjectData request schema registered."""

    def openapi(self) -> dict[str, Any]:
        schema = super().openapi()
        components = schema.setdefault("components", {}).setdefault("schemas", {})
        for name, model_schema in project.openapi_schemas().items():
            existing = components.setdefault(name, model_schema)
            if existing != model_schema:
                raise RuntimeError(f"OpenAPI schema component collision: {name}")
        return schema


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
        kind = manifest["catalog_artifact_kind"]
        artifact_steward = manifest["steward"] if kind == "steward" else None
        if steward.id != (artifact_steward or "global"):
            raise RuntimeError(
                f"{db_path}: REG_WEBAPP_STEWARD={steward.id!r} does not match "
                f"artifact steward {artifact_steward!r} ({kind})"
            )
        app.state.default_scope = resolve_scope(conn)
    finally:
        conn.close()
    app.state.manifest = manifest
    app.state.steward = steward
    # The catalog routes open a FRESH read-only connection PER REQUEST from this
    # boot-resolved path (the connection model is locked: a shared sqlite3 conn
    # isn't safe across FastAPI's sync-handler threadpool). The schema was
    # already validated by open_db above, so the per-request open skips the
    # re-check (check_schema=False) — see routes/catalog.py `_catalog_conn`.
    app.state.db_path = db_path
    # Provider coverage memo for this app's one artifact (routes/catalog.py
    # `_provider_coverage`); discarded with the app.
    app.state.provider_coverage = {}
    yield


def create_app(*, rate_limit_per_minute: int = RATE_LIMIT_PER_MINUTE) -> FastAPI:
    """Build the FastAPI app.

    ``rate_limit_per_minute`` defaults to the cost-protection budget; it's a parameter ONLY
    so tests that need to drive the write endpoints harder than 30 req/min (the
    cross-thread concurrency smoke tests, and the run-reg-webapp skill's one-shot
    driver modes, which replay a scenario matrix from one IP) can raise it without
    disabling the middleware — the limiter is still IN the stack, just with a higher
    ceiling. Production callers use the default."""
    # redoc_url=None: the deployed edge worker forwards a fixed backend-path set
    # (/api, /openapi.json, /docs — see reg_webapp/edge/), and /redoc would fall
    # through to the SPA shell there; disable it so the local and deployed
    # surfaces match. Swagger at /docs is the one interactive-docs surface.
    app = _RegistryApp(
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
    app.include_router(catalog.router)
    # A5.2b-ii write surface: project validate/order. The ETag middleware skips
    # these (method gate); the cap + limiter gate them.
    app.include_router(project.router)

    return app
