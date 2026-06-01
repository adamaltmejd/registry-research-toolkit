"""The committed ``openapi.json`` must equal the freshly-rendered schema.

Mirrors the CI drift guard: regenerating must produce no diff. If this fails,
run ``uv run python reg_webapp/backend/scripts/gen_openapi.py`` and commit.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

# Load the sibling script directly, without mutating sys.path (which would leak
# module-shadowing risk into the rest of the test session).
_GEN_OPENAPI_PATH = Path(__file__).resolve().parents[1] / "scripts" / "gen_openapi.py"
_spec = importlib.util.spec_from_file_location(
    "reg_webapp_gen_openapi", _GEN_OPENAPI_PATH
)
assert _spec and _spec.loader
gen_openapi = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_openapi)


def test_openapi_snapshot_matches_committed():
    committed = gen_openapi.OPENAPI_PATH.read_text(encoding="utf-8")
    assert gen_openapi.render_openapi() == committed
