"""Flat route view of an assembled app, for the boot/response-model/boundary guards.

FastAPI 0.137.0 (PR #15745, "preserve ``APIRouter``/``APIRoute`` instances")
stopped flattening included routers into ``app.routes``: it now holds
``_IncludedRouter`` tree nodes wrapping the original routers, so ``app.routes``
is no longer a flat list of leaf routes. Several guards assert over the flat view
(catch-all ordering, response-model coverage, reserved-slug mirror, provenance-DB
confinement), so recover it by walking the tree and descending each included
router's original route list.

Declaration order is preserved (FastAPI matches in declaration order), which the
catch-all-ordering guard depends on. The ``original_router`` descent is the only
coupling to a private FastAPI attribute; if a future bump renames it the
``getattr`` fallback treats the node as a leaf and the guards fail loudly
(``expected at least one /api route`` / an empty provenance scan would still scan
the directly-added routes) rather than silently passing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi.routing import APIRoute

if TYPE_CHECKING:
    from collections.abc import Iterator

    from fastapi import FastAPI
    from starlette.routing import BaseRoute


def flat_routes(app: FastAPI) -> list[BaseRoute]:
    """Every leaf route the assembled ``app`` exposes, in declaration order.

    Both included-router ``APIRoute``s and directly-added Starlette ``Route``s —
    the pre-0.137 ``app.routes`` view, reconstructed from the tree.
    """
    return list(_walk(app.routes))


def flat_api_routes(app: FastAPI) -> list[APIRoute]:
    """The ``APIRoute`` subset of :func:`flat_routes` (the OpenAPI / catalog guards)."""
    return [route for route in flat_routes(app) if isinstance(route, APIRoute)]


def _walk(routes: list[BaseRoute]) -> Iterator[BaseRoute]:
    for route in routes:
        # _IncludedRouter (0.137.0+) is the only non-leaf node — descend into the
        # original router it wraps; everything else is a leaf route, yielded as-is.
        # (pre-0.137 app.routes was already flat, so every node is a leaf there.)
        sub = getattr(getattr(route, "original_router", None), "routes", None)
        if sub is None:
            yield route
        else:
            yield from _walk(sub)
