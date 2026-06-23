"""Flat ``APIRoute`` view of an assembled app, for the boot/response-model guards.

FastAPI 0.137.0 (PR #15745, "preserve ``APIRouter``/``APIRoute`` instances")
stopped flattening included routers into ``app.routes``: it now holds
``_IncludedRouter`` tree nodes wrapping the original routers, so ``app.routes``
is no longer a flat list of ``APIRoute``. These guards assert over the flat view
(catch-all ordering, response-model coverage, reserved-slug mirror), so recover
it by walking the tree and descending each included router's original route list.

Declaration order is preserved (FastAPI matches in declaration order), which the
catch-all-ordering guard depends on. The ``original_router`` descent is the only
coupling to a private FastAPI attribute; if a future bump renames it the
``getattr`` fallback yields nothing for that node and the guards fail loudly
(``expected at least one /api route``) rather than silently passing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi.routing import APIRoute

if TYPE_CHECKING:
    from collections.abc import Iterator

    from fastapi import FastAPI
    from starlette.routing import BaseRoute


def flat_api_routes(app: FastAPI) -> list[APIRoute]:
    """Every ``APIRoute`` the assembled ``app`` exposes, in declaration order."""
    return list(_walk(app.routes))


def _walk(routes: list[BaseRoute]) -> Iterator[APIRoute]:
    for route in routes:
        if isinstance(route, APIRoute):
            yield route
            continue
        # _IncludedRouter (0.137.0+) wraps the original APIRouter; pre-0.137
        # app.routes was already flat, so this descent is simply a no-op there.
        sub = getattr(getattr(route, "original_router", None), "routes", None)
        if sub:
            yield from _walk(sub)
