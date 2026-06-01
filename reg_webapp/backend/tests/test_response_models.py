"""Every ``/api`` route declares a non-None ``response_model`` (§9.2).

Lint-enforced invariant: the SPA codegens TS types from the response models,
so an endpoint without one is a typed-contract hole.
"""

from __future__ import annotations

from fastapi.routing import APIRoute
from reg_webapp.app import create_app


def test_every_api_route_has_response_model():
    app = create_app()
    api_routes = [
        r for r in app.routes if isinstance(r, APIRoute) and r.path.startswith("/api")
    ]
    assert api_routes, "expected at least one /api route"
    missing = [r.path for r in api_routes if r.response_model is None]
    assert not missing, f"routes missing response_model: {missing}"
