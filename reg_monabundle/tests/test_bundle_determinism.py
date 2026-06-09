"""Byte-identical determinism gate for ``build_bundle`` (see DESIGN.md → Bundle determinism).

The MONA bundle is a build artifact that round-trips through MONA's GUI;
two builds of the same spec must produce the *same bytes* so a rebuild is
a no-op in version control and so a researcher can verify a bundle they
were handed matches a clean rebuild. Any nondeterminism in the
amalgamation (dict iteration order, a stray timestamp, a path leak,
unstable JSON key order in the embedded spec) would defeat both.

This test builds the committed 200-binding load-test fixture (see
``reg_schema/test_corpus/load_test_200col/``) *twice* into separate
output paths and asserts the emitted ``.py`` files are ``read_bytes()``
-equal. It mirrors the build invocation in
``test_bundle_size_budget.py``.

If this test ever fails, it has caught a real determinism bug in the
bundle build — fix the source of nondeterminism, do not weaken the
assertion.
"""

from __future__ import annotations

import json
from pathlib import Path

from reg_monabundle import build_bundle

LOAD_FIXTURE = (
    Path(__file__).resolve().parent.parent.parent
    / "reg_schema"
    / "test_corpus"
    / "load_test_200col"
    / "input.json"
)


def _shuffle_keys(value: object) -> object:
    """Recursively rebuild dicts with reversed key insertion order.

    Same content, different key order — so two builds differ in embedded
    bytes unless the serializer canonicalizes (json.dumps(sort_keys=True)).
    """
    if isinstance(value, dict):
        return {k: _shuffle_keys(value[k]) for k in reversed(list(value))}
    if isinstance(value, list):
        return [_shuffle_keys(v) for v in value]
    return value


def test_build_bundle_is_byte_identical_across_runs(tmp_path: Path) -> None:
    project_data = json.loads(LOAD_FIXTURE.read_text(encoding="utf-8"))

    first = build_bundle(
        tmp_path / "first" / "mdw_runner.py", project_data=project_data
    )
    second = build_bundle(
        tmp_path / "second" / "mdw_runner.py", project_data=project_data
    )

    assert first.read_bytes() == second.read_bytes(), (
        "build_bundle is not deterministic: two builds of the same spec "
        "produced different bytes. The bundle is a version-controlled build "
        "artifact and a rebuild must be a no-op — track down the source of "
        "nondeterminism (dict/set iteration order, an embedded timestamp, a "
        "leaked absolute path, or unstable JSON key order in the embedded "
        "project_data) rather than relaxing this assertion."
    )


def test_build_bundle_is_byte_identical_under_key_reorder(tmp_path: Path) -> None:
    """Embedded bytes depend on spec content, not the caller's dict key order.

    Guards the json.dumps(sort_keys=True) contract: a caller that parsed the
    same project_data.json into a differently-ordered dict (e.g. via a
    re-serializing round-trip) must still get a byte-identical bundle.
    """
    project_data = json.loads(LOAD_FIXTURE.read_text(encoding="utf-8"))
    reordered = _shuffle_keys(project_data)
    assert reordered == project_data  # same content
    assert list(reordered) != list(project_data)  # different top-level key order

    canonical = build_bundle(
        tmp_path / "canonical" / "mdw_runner.py", project_data=project_data
    )
    shuffled = build_bundle(
        tmp_path / "shuffled" / "mdw_runner.py", project_data=reordered
    )

    assert canonical.read_bytes() == shuffled.read_bytes(), (
        "build_bundle embeds project_data with unstable key order: two builds "
        "of the same spec content but different dict key order produced "
        "different bytes. The embedded JSON must be canonicalized "
        "(json.dumps(sort_keys=True)) so reproducibility does not depend on "
        "the caller's dict ordering."
    )
