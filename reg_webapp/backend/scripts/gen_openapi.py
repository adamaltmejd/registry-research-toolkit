"""Deterministically dump the FastAPI OpenAPI schema to ``openapi.json``.

The committed snapshot is the canonical API contract (§9.2): CI snapshot-tests
it and the SPA codegens TS types from it. ``app.openapi()`` builds the schema
without needing the lifespan (no DB), so this runs offline. Dumped with
``sort_keys=True`` + a trailing newline so the snapshot is byte-stable across
runs and machines.
"""

from __future__ import annotations

import json
from pathlib import Path

from reg_webapp.app import create_app

OPENAPI_PATH = Path(__file__).resolve().parents[1] / "openapi.json"


def render_openapi() -> str:
    schema = create_app().openapi()
    return json.dumps(schema, sort_keys=True, indent=2) + "\n"


def main() -> None:
    OPENAPI_PATH.write_text(render_openapi(), encoding="utf-8")
    print(f"wrote {OPENAPI_PATH}")


if __name__ == "__main__":
    main()
