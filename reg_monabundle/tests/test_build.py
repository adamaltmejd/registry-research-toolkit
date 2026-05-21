"""Tests for ``reg_monabundle.build``.

Coverage focus: the builder's generic-over-runtime guarantees. The
mdw-runtime-shape integration tests live in
``mock_data_wizard/tests/test_build_mona_bundle.py`` and exercise the
real classify/sql_emit/sources/summarize/spec/scan/extract pipeline
end-to-end including subprocess execution.
"""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

import pytest

from reg_monabundle import build_bundle

if TYPE_CHECKING:
    from pathlib import Path


def _write_fake_runtime(pkg_dir: Path, pkg_name: str, body: str) -> None:
    """Materialize a minimal one-module 'runtime' package on disk.

    The slicer reads from disk, so a tmp_path-based throwaway is the
    simplest way to feed it a custom package name. ``body`` is the
    module source for ``<pkg_dir>/mod.py``.
    """
    pkg_dir.mkdir(parents=True, exist_ok=True)
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (pkg_dir / "mod.py").write_text(body, encoding="utf-8")
    # Sanity: the directory's ``.name`` is what build_bundle reads as the
    # runtime package name. Confirm it matches what the test passed in.
    assert pkg_dir.name == pkg_name, (
        f"test setup error: expected pkg_dir.name == {pkg_name!r}, got {pkg_dir.name!r}"
    )


def test_slicer_drops_caller_runtime_intra_pkg_imports(tmp_path: Path):
    """``_slice_module`` must drop imports of the caller's runtime
    package — not just the hardcoded mdw prefix.

    Pins the fix for the chatgpt-codex-connector P2 review on PR #121:
    `build_bundle` advertises a generic ``runtime_pkg_dir`` /
    ``runtime_module_order`` API, so the slicer's drop list cannot
    stay tied to ``mock_data_wizard``.
    """
    fake_pkg = tmp_path / "myruntime"
    _write_fake_runtime(
        fake_pkg,
        "myruntime",
        "from __future__ import annotations\n"
        "from myruntime.helpers import helper_fn\n"
        "from myruntime import other\n"
        "import myruntime.deep.submodule\n"
        "import os\n\n"
        "def main():\n"
        "    return helper_fn() + other.value\n",
    )
    out = tmp_path / "bundle.py"
    build_bundle(
        out,
        runtime_pkg_dir=fake_pkg,
        runtime_module_order=("mod",),
    )
    text = out.read_text(encoding="utf-8")
    # The slicer's job: nothing in the emitted bundle should reference
    # the caller's runtime package by name. The runtime is amalgamated
    # in-place; cross-module imports would be ImportErrors on MONA.
    for line in text.splitlines():
        s = line.lstrip()
        assert "myruntime" not in s, f"caller-runtime import leaked: {line!r}"
    # The non-amalgamated stdlib import must survive.
    assert "import os" in text
    # The function body itself must still be there.
    assert "def main():" in text


def test_slicer_drops_reg_schema_and_reg_monabundle_imports(tmp_path: Path):
    """The static amalgamated prefixes (reg_schema, reg_monabundle) must
    keep being dropped — this guards against a future refactor that
    accidentally relaxes the static set when generalising for the
    dynamic runtime prefix."""
    fake_pkg = tmp_path / "tinypkg"
    _write_fake_runtime(
        fake_pkg,
        "tinypkg",
        "from __future__ import annotations\n"
        "from reg_schema import ProjectData\n"
        "from reg_monabundle import SUPPRESS_K\n"
        "import reg_schema.structural\n\n"
        "def x():\n"
        "    return SUPPRESS_K\n",
    )
    out = tmp_path / "bundle.py"
    build_bundle(
        out,
        runtime_pkg_dir=fake_pkg,
        runtime_module_order=("mod",),
    )
    # Find the amalgamated mod.py block and inspect that specifically —
    # ``from reg_schema`` and ``from reg_monabundle`` appear elsewhere
    # in the bundle (the header docstring, the amalgamated module
    # comments). What matters is that they don't appear as live
    # ``ImportFrom`` statements inside the sliced mod body.
    text = out.read_text(encoding="utf-8")
    mod_marker = "# mod.py"
    runner_marker = "# Runner"
    mod_start = text.index(mod_marker)
    mod_end = text.index(runner_marker, mod_start)
    mod_block = text[mod_start:mod_end]
    for line in mod_block.splitlines():
        s = line.lstrip()
        assert not s.startswith("from reg_schema"), (
            f"reg_schema import leaked into sliced runtime: {line!r}"
        )
        assert not s.startswith("from reg_monabundle"), (
            f"reg_monabundle import leaked into sliced runtime: {line!r}"
        )
        assert not s.startswith("import reg_schema"), (
            f"reg_schema absolute-import leaked: {line!r}"
        )


def test_slicer_emits_valid_python(tmp_path: Path):
    """The amalgamated output must parse as Python after slicing."""
    fake_pkg = tmp_path / "yetanother"
    _write_fake_runtime(
        fake_pkg,
        "yetanother",
        "from __future__ import annotations\n\ndef helper() -> int:\n    return 42\n",
    )
    out = tmp_path / "bundle.py"
    build_bundle(
        out,
        runtime_pkg_dir=fake_pkg,
        runtime_module_order=("mod",),
    )
    # ast.parse raises SyntaxError on a malformed amalgamation.
    ast.parse(out.read_text(encoding="utf-8"))


def test_static_prefix_uses_exact_or_dotted_match(tmp_path: Path):
    """``reg_schema_v2`` would naively-match a raw ``startswith("reg_schema")``;
    the slicer must keep that import."""
    fake_pkg = tmp_path / "exactmatchpkg"
    _write_fake_runtime(
        fake_pkg,
        "exactmatchpkg",
        # Hypothetical sibling package whose name is a prefix of an
        # amalgamated one. A bare startswith() would over-strip.
        "from __future__ import annotations\n"
        "# (reg_schema_v2 isn't a real package — this test only inspects\n"
        "#  whether the slicer would have removed the line.)\n"
        "import reg_schema_v2\n\n"
        "def y(): pass\n",
    )
    out = tmp_path / "bundle.py"
    build_bundle(
        out,
        runtime_pkg_dir=fake_pkg,
        runtime_module_order=("mod",),
    )
    text = out.read_text(encoding="utf-8")
    # The hypothetical sibling import must SURVIVE the slicer.
    # (Even though the result is a runtime ImportError on a real MONA
    # box — that's the runtime author's problem; the slicer's job is
    # to be precise.)
    assert "import reg_schema_v2" in text


def test_default_output_name_is_mdw_runner(tmp_path: Path):
    """Sanity: the default bundle filename hasn't drifted. Tests in the
    mdw integration suite rely on this string for fixture paths."""
    from reg_monabundle import DEFAULT_OUTPUT_NAME

    assert DEFAULT_OUTPUT_NAME == "mdw_runner.py"


def test_build_bundle_writes_to_supplied_path(tmp_path: Path):
    """Smoke test that the output path argument is honored."""
    fake_pkg = tmp_path / "spot"
    _write_fake_runtime(
        fake_pkg,
        "spot",
        "from __future__ import annotations\n\ndef z(): pass\n",
    )
    out = tmp_path / "nested" / "dir" / "bundle.py"
    result = build_bundle(
        out,
        runtime_pkg_dir=fake_pkg,
        runtime_module_order=("mod",),
    )
    assert result == out
    assert out.exists()
    assert out.stat().st_size > 0


def test_build_bundle_requires_runtime_pkg_dir(tmp_path: Path):
    """``runtime_pkg_dir`` is keyword-only and required."""
    with pytest.raises(TypeError):
        build_bundle(tmp_path / "x.py")  # ty: ignore[missing-argument]
