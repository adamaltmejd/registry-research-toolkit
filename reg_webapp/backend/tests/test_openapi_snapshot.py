"""The committed ``openapi.json`` must equal the freshly-rendered schema.

Mirrors the CI drift guard: regenerating must produce no diff. If this fails,
run ``uv run python reg_webapp/backend/scripts/gen_openapi.py`` and commit.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import gen_openapi  # noqa: E402  (path-injected sibling script)


def test_openapi_snapshot_matches_committed():
    committed = gen_openapi.OPENAPI_PATH.read_text(encoding="utf-8")
    assert gen_openapi.render_openapi() == committed
