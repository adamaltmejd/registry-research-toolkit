"""1 MB bundle-size budget gate (see ARCHITECTURE.md → Repo-wide invariants).

The MONA bundle is uploaded through MONA's GUI on every round-trip;
the v1 budget keeps the upload responsive and forces deliberation
before adding heavy amalgamated code. The cap is a forward-looking
v1 ceiling, not a tight bound on today's shape — current bundles on
the load fixture sit well under 200 KB, leaving 5×-plus headroom.

This test embeds the committed 200-binding load-test fixture (see
``reg_schema/test_corpus/load_test_200col/``) into a bundle and
asserts the emitted ``.py`` stays under cap. A regression here means
the bundle grew enough to threaten the v1 ceiling — investigate
before raising the cap.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from reg_monabundle import build_bundle

if TYPE_CHECKING:
    import pytest

# See ARCHITECTURE.md → Repo-wide invariants. Bumping this constant requires
# a spec edit and a steward conversation — the cap is part of the bundle
# contract,
# not a test tuning knob.
BUNDLE_SIZE_CAP_BYTES = 1_048_576

# Fixture-shape invariants. The cap measurement is only meaningful at
# representative load — if someone accidentally shrinks the fixture,
# the gate would silently relax. Pin the shape here so drift fails
# fast (Codex review on PR #124).
LOAD_FIXTURE_EXPECTED_SOURCES = 8
# Model A renamed `columns` -> `bindings`; count is unchanged (200).
LOAD_FIXTURE_EXPECTED_BINDINGS = 200

LOAD_FIXTURE = (
    Path(__file__).resolve().parent.parent.parent
    / "reg_schema"
    / "test_corpus"
    / "load_test_200col"
    / "input.json"
)


def test_bundle_with_200col_load_fixture_under_1mb_cap(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    project_data = json.loads(LOAD_FIXTURE.read_text(encoding="utf-8"))

    # Pin fixture shape so a stealth shrink (fewer sources, fewer
    # bindings) can't silently weaken the cap measurement.
    n_sources = len(project_data["sources"])
    n_bindings = sum(len(s["bindings"]) for s in project_data["sources"])
    assert n_sources == LOAD_FIXTURE_EXPECTED_SOURCES, (
        f"load fixture has {n_sources} sources, expected "
        f"{LOAD_FIXTURE_EXPECTED_SOURCES}. Regenerate "
        f"reg_schema/test_corpus/load_test_200col/input.json via "
        f"build.py, or update the constant here if the shape change "
        f"is intentional."
    )
    assert n_bindings == LOAD_FIXTURE_EXPECTED_BINDINGS, (
        f"load fixture has {n_bindings} bindings, expected "
        f"{LOAD_FIXTURE_EXPECTED_BINDINGS}. Same regeneration / update "
        f"as above."
    )

    out = build_bundle(tmp_path / "mdw_runner.py", project_data=project_data)
    size = out.stat().st_size
    pct = 100 * size / BUNDLE_SIZE_CAP_BYTES

    # Emit the size report through ``capsys.disabled()`` so it survives
    # pytest's default stdout capture and lands in CI logs. The
    # creeping-growth signal is the point — silent passes defeat it
    # (Copilot review on PR #124).
    with capsys.disabled():
        print(
            f"bundle size: {size:,} bytes ({pct:.1f}% of "
            f"{BUNDLE_SIZE_CAP_BYTES:,} cap, headroom "
            f"{BUNDLE_SIZE_CAP_BYTES - size:,} bytes)"
        )

    assert size <= BUNDLE_SIZE_CAP_BYTES, (
        f"bundle is {size:,} bytes — over the 1 MB v1 cap "
        f"({BUNDLE_SIZE_CAP_BYTES:,}). Either trim what was just added "
        f"to the runtime amalgamation, or raise the cap in "
        f"ARCHITECTURE.md → Repo-wide invariants (requires a spec edit, not a test tweak)."
    )
