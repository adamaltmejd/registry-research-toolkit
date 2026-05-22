"""1 MB bundle-size budget gate (``REFACTOR_SPEC.md`` §12).

The MONA bundle is uploaded through MONA's GUI on every round-trip;
the v1 budget keeps the upload responsive and forces deliberation
before adding heavy amalgamated code. The cap is a forward-looking
v1 ceiling, not a tight bound on today's shape — current bundles on
the load fixture sit well under 200 KB, leaving 5×-plus headroom.

This test embeds the committed 200-column load-test fixture (see
``reg_schema/test_corpus/load_test_200col/``) into a bundle and
asserts the emitted ``.py`` stays under cap. A regression here means
the bundle grew enough to threaten the v1 ceiling — investigate
before raising the cap.
"""

from __future__ import annotations

import json
from pathlib import Path

from reg_monabundle import build_bundle

# ``REFACTOR_SPEC.md`` §12. Bumping this constant requires a spec edit
# and a steward conversation — the cap is part of the bundle contract,
# not a test tuning knob.
BUNDLE_SIZE_CAP_BYTES = 1_048_576

LOAD_FIXTURE = (
    Path(__file__).resolve().parent.parent.parent
    / "reg_schema"
    / "test_corpus"
    / "load_test_200col"
    / "input.json"
)


def test_bundle_with_200col_load_fixture_under_1mb_cap(tmp_path: Path) -> None:
    project_data = json.loads(LOAD_FIXTURE.read_text(encoding="utf-8"))
    out = build_bundle(tmp_path / "mdw_runner.py", project_data=project_data)
    size = out.stat().st_size
    pct = 100 * size / BUNDLE_SIZE_CAP_BYTES
    # Print so a passing run still reports headroom — useful for
    # spotting creeping growth before it trips the gate.
    print(
        f"bundle size: {size:,} bytes ({pct:.1f}% of {BUNDLE_SIZE_CAP_BYTES:,} cap, "
        f"headroom {BUNDLE_SIZE_CAP_BYTES - size:,} bytes)"
    )
    assert size <= BUNDLE_SIZE_CAP_BYTES, (
        f"bundle is {size:,} bytes — over the 1 MB v1 cap "
        f"({BUNDLE_SIZE_CAP_BYTES:,}). Either trim what was just added "
        f"to the runtime amalgamation, or raise the cap in "
        f"REFACTOR_SPEC.md §12 (requires a spec edit, not a test tweak)."
    )
