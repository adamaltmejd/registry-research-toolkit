"""Belt-and-suspenders: assert ``static/index.html`` is committed.

The wheel ships ``src/mock_data_wizard/static/`` alongside the Python
sources; an absent or zero-byte ``index.html`` means a release would
boot ``mock-data-wizard ui`` into a 404. This test catches accidental
deletion at PR time, regardless of whether CI also runs a separate
frontend-build step.
"""

from __future__ import annotations

import importlib.resources

MIN_INDEX_BYTES = 200


def test_static_index_is_committed():
    static_dir = importlib.resources.files("mock_data_wizard") / "static"
    index = static_dir / "index.html"
    assert index.is_file(), (
        f"{index} missing — the frontend bundle (or its placeholder) "
        f"must be committed for `mock-data-wizard ui` to serve anything. "
        f"Either run `cd mock_data_wizard/web && bun run build` or "
        f"restore the placeholder."
    )
    data = index.read_bytes()
    assert len(data) >= MIN_INDEX_BYTES, (
        f"{index} is {len(data)} bytes; expected >= {MIN_INDEX_BYTES}. "
        f"A truncated index suggests a broken commit."
    )
