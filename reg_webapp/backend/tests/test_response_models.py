"""Every ``/api`` route declares a typed response contract in ``openapi.json``.

See DESIGN.md → OpenAPI snapshot + TS codegen (the drift gate). The SPA codegens
TS types from the OpenAPI schema, so an endpoint without a typed contract is a
hole. The committed snapshot is pinned to the live app by
``test_openapi_snapshot.py``, so it is the contract read here.

Two contract shapes are allowed:

- every media type the route advertises carries a schema (a Pydantic
  ``response_model``; a route without one renders as an empty ``{}`` schema), OR
- the route is an allowlisted BINARY/DOWNLOAD endpoint (the PDF file route),
  pinned by method, path and media type. It returns opaque bytes, so it
  declares its media type in the route's ``responses=`` — and then ONLY that
  media type — so the SPA codegen sees a download, not an untyped JSON body.
  A new download endpoint is a deliberate addition to the allowlist.
"""

from __future__ import annotations

import json
from pathlib import Path

_OPENAPI = Path(__file__).resolve().parents[1] / "openapi.json"

_DOWNLOAD_ENDPOINTS: dict[tuple[str, str], str] = {
    ("get", "/api/docs/file/{register}/{filename}"): "application/pdf",
}


def _api_operations() -> dict[tuple[str, str], dict]:
    paths = json.loads(_OPENAPI.read_text(encoding="utf-8"))["paths"]
    return {
        (method, path): operation
        for path, operations in paths.items()
        if path.startswith("/api")
        for method, operation in operations.items()
    }


def test_every_api_route_declares_a_typed_200_body():
    untyped = [
        f"{method.upper()} {path}"
        for (method, path), operation in _api_operations().items()
        if (method, path) not in _DOWNLOAD_ENDPOINTS
        for content in [operation["responses"]["200"].get("content", {})]
        if not content or not all(entry.get("schema") for entry in content.values())
    ]
    assert not untyped, f"routes without a typed 200 contract: {untyped}"


def test_download_endpoints_declare_only_their_media_type():
    operations = _api_operations()
    for (method, path), media_type in _DOWNLOAD_ENDPOINTS.items():
        content = operations[(method, path)]["responses"]["200"].get("content", {})
        assert list(content) == [media_type], (
            f"{method.upper()} {path} should declare only {media_type!r}, "
            f"got {list(content)}"
        )
