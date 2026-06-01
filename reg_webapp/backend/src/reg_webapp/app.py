"""FastAPI app factory + lifespan (the reg_meta boot seam).

The lifespan opens the real reg_meta DB read-only via reg_meta's own helpers
(``db_path_from_args`` + ``open_db``). ``open_db`` already opens ``mode=ro``
AND runs ``_check_schema_compat`` (the load-bearing SCHEMA_VERSION gate vs the
DB manifest) — we do NOT hardcode the path or reimplement the check. The
connection + parsed manifest live on ``app.state`` for the request handlers;
the connection is closed on shutdown.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import reg_meta.db
from fastapi import FastAPI

from . import __version__
from .routes import context
from .stewards import load_steward

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # db_arg=None → reg_meta's default-path resolution (REG_META_DB > XDG >
    # platform default). open_db opens mode=ro AND runs _check_schema_compat,
    # the load-bearing SCHEMA_VERSION gate vs the DB manifest — an incompatible
    # major (or too-old minor) raises RegMetaError here, failing startup fast.
    db_path = reg_meta.db.db_path_from_args(None)
    conn = reg_meta.db.open_db(db_path)
    try:
        app.state.db = conn
        app.state.manifest = reg_meta.db.get_manifest(conn)
        app.state.steward = load_steward()
        yield
    finally:
        conn.close()


def create_app() -> FastAPI:
    app = FastAPI(title="reg_webapp", version=__version__, lifespan=lifespan)
    app.include_router(context.router)
    return app
