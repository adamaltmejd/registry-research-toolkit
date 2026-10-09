"""FastAPI app factory + lifespan (the reg_meta boot seam).

The lifespan opens the real reg_meta DB read-only via reg_meta's own helpers
(``db_path_from_args`` + ``open_db``). ``open_db`` already opens ``mode=ro``
AND runs ``_check_schema_compat`` (the load-bearing SCHEMA_VERSION gate vs the
DB manifest) — we do NOT hardcode the path or reimplement the check. The boot
connection is closed once the manifest is read; the parsed manifest lives on
``app.state``.

FastAPI serves no route: the SPA reads the Rust server (``reg-meta serve``) alone.
Package F of ``RUST_RUNTIME_SPEC.md`` deletes this app.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import reg_meta.db
from fastapi import FastAPI

from . import __version__
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
    finally:
        conn.close()
    app.state.manifest = manifest
    app.state.steward = steward
    yield


def create_app() -> FastAPI:
    """Build the FastAPI app."""
    # redoc_url=None: the deployed edge worker forwards a fixed backend-path set
    # (/api, /openapi.json, /docs — see reg_webapp/edge/), and /redoc would fall
    # through to the SPA shell there; disable it so the local and deployed
    # surfaces match. Swagger at /docs is the one interactive-docs surface.
    return FastAPI(
        title="reg_webapp", version=__version__, lifespan=lifespan, redoc_url=None
    )
