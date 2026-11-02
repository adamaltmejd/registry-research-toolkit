"""Provider namespaces refuse at the lock boundary, before ids are published."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from reader_artifacts import REG_BUILD, REPO


def test_reserved_provider_prefixes_refuse_at_lock(tmp_path):
    # Regression: a manifest or source frame uses a reserved discriminator as its
    # provider, making all emitted public ids unreachable. Existing ref grammar
    # cases never read builder inputs. The base fixture is the allowed twin;
    # expected.json names every reserved prefix and its authored source location.
    cases = Path(__file__).parent / "cases"
    fixture = cases / "fixtures/base"
    expected = json.loads((cases / "lock/reserved-provider/expected.json").read_text())
    curation = tmp_path / "curation"
    inputs = tmp_path / "inputs"
    shutil.copytree(fixture / "curation", curation)
    shutil.copytree(fixture / "inputs", inputs)
    manifest = curation / "inputs.toml"
    frame = inputs / "frame/base.jsonl"
    original_manifest = manifest.read_text()
    original_frame = frame.read_text()

    def lock():
        return subprocess.run(
            [REG_BUILD, "lock", "--inputs", inputs, "--curation", curation],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        )

    allowed = lock()
    assert allowed.returncode == 0, allowed.stderr
    for provider in expected["providers"]:
        manifest.write_text(
            original_manifest.replace('pid = "demo"', f'pid = "{provider}"')
        )
        refused = lock()
        assert refused.returncode == 1, refused.stderr
        assert (
            f"{expected['manifest_location']}: {expected['code']}:" in refused.stderr
        ), refused.stderr

        # A frame's provider must not bypass a valid manifest's namespace check.
        manifest.write_text(original_manifest)
        frame.write_text(
            original_frame.replace('"provider":"demo"', f'"provider":"{provider}"')
        )
        refused = lock()
        assert refused.returncode == 1, refused.stderr
        assert f"{expected['frame_location']}: {expected['code']}:" in refused.stderr
        frame.write_text(original_frame)
