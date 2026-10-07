"""Every ``/api`` route declares a typed response contract in ``openapi.json``.

See DESIGN.md → OpenAPI snapshot + TS codegen (the drift gate). The SPA codegens
TS types from the OpenAPI schema, so an endpoint without a typed contract is a
hole. The committed snapshot is pinned to the live app by
``test_openapi_snapshot.py``, so it is the contract read here.

Two contract shapes are allowed:

- a JSON body with a schema (a Pydantic ``response_model``; a route without
  one renders as an empty ``{}`` schema), OR
- a documented BINARY/DOWNLOAD media type (the PDF file route), declared in the
  route's ``responses=`` so the SPA codegen sees a download, not an untyped JSON
  body — and then ONLY that media type.
"""

from __future__ import annotations

import json
from pathlib import Path

_OPENAPI = Path(__file__).resolve().parents[1] / "openapi.json"


def test_every_api_route_declares_a_typed_200_body():
    paths = json.loads(_OPENAPI.read_text(encoding="utf-8"))["paths"]
    untyped = [
        f"{method.upper()} {path}"
        for path, operations in paths.items()
        if path.startswith("/api")
        for method, operation in operations.items()
        for content in [operation["responses"]["200"].get("content", {})]
        if not content
        or ("application/json" in content and not content["application/json"]["schema"])
        or ("application/json" in content and len(content) > 1)
    ]
    assert not untyped, f"routes without a typed 200 contract: {untyped}"
