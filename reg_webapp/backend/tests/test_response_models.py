"""Every ``/api`` route declares a typed response contract.

See DESIGN.md → OpenAPI snapshot + TS codegen (the drift gate). Lint-enforced
invariant: the SPA codegens TS types from the OpenAPI schema, so an
endpoint without a typed contract is a hole.

Two contract shapes are allowed:

- a Pydantic ``response_model`` (the JSON endpoints), OR
- a documented BINARY/DOWNLOAD media type (``/api/project/order`` → ``text/csv``).
  These cannot declare a Pydantic ``response_model`` (they return raw bytes), but
  they DO declare their media type in the route's ``responses=`` so the OpenAPI
  contract (and the SPA codegen) sees a download, not an untyped JSON body. This is
  the sanctioned carve-out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _route_helpers import flat_api_routes
from reg_webapp.app import create_app

if TYPE_CHECKING:
    from fastapi.routing import APIRoute

# The binary/download endpoints: no Pydantic response_model (raw bytes), but
# each MUST declare its download media type in OpenAPI instead. Pinned by path +
# expected media type so a new download endpoint is a deliberate addition here.
_DOWNLOAD_ENDPOINTS: dict[str, str] = {
    "/api/project/order": "text/csv",
}


def _api_routes() -> list[APIRoute]:
    routes = [r for r in flat_api_routes(create_app()) if r.path.startswith("/api")]
    assert routes, "expected at least one /api route"
    return routes


def test_every_json_api_route_has_response_model():
    """Every JSON ``/api`` route (i.e. not a documented download) declares a
    Pydantic ``response_model``."""
    missing = [
        r.path
        for r in _api_routes()
        if r.path not in _DOWNLOAD_ENDPOINTS and r.response_model is None
    ]
    assert not missing, f"JSON routes missing response_model: {missing}"


def test_download_endpoints_declare_their_media_type():
    """The download carve-outs declare their media type in OpenAPI (a typed
    contract for the SPA) — they don't silently fall back to ``application/json``."""
    schema = create_app().openapi()
    for path, media_type in _DOWNLOAD_ENDPOINTS.items():
        content = schema["paths"][path]["post"]["responses"]["200"]["content"]
        assert media_type in content, (
            f"{path} should declare 200 content-type {media_type!r}, got "
            f"{list(content)}"
        )
        assert "application/json" not in content, (
            f"{path} is a download — it must not also advertise application/json"
        )
