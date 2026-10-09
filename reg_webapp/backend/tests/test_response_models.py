"""Every ``/api`` route declares a typed response contract in ``openapi.json``.

See DESIGN.md → OpenAPI snapshot + TS codegen (the drift gate). The SPA codegens
TS types from the OpenAPI schema, so an endpoint without a typed contract is a
hole. The committed snapshot is pinned to the live app by
``test_openapi_snapshot.py``, so it is the contract read here.

Every media type a route advertises carries a schema (a Pydantic
``response_model``; a route without one renders as an empty ``{}`` schema).
"""

from __future__ import annotations

import json
from pathlib import Path

_OPENAPI = Path(__file__).resolve().parents[1] / "openapi.json"


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
        for content in [operation["responses"]["200"].get("content", {})]
        if not content or not all(entry.get("schema") for entry in content.values())
    ]
    assert not untyped, f"routes without a typed 200 contract: {untyped}"
