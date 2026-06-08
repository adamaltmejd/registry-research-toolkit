"""Tests for ``reg_monabundle.build``.

Coverage focus: the builder's generic-over-runtime guarantees. The
real-runtime integration tests (default runtime path + subprocess
execution) live in ``reg_monabundle/tests/test_build_mona_bundle.py``
and exercise the actual ``reg_monabundle.runtime.*`` modules
end-to-end.
"""

from __future__ import annotations

import ast
from typing import TYPE_CHECKING

import pytest

from reg_monabundle import build_bundle

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@pytest.fixture
def build(tmp_path: Path) -> Callable[..., str]:
    """Materialize a tmp_path-based fake runtime package, amalgamate it,
    and return the bundle source.

    The slicer reads from disk, so a tmp_path-based throwaway is the
    simplest way to feed it a custom package name. ``pkg_name`` is the
    runtime package directory's ``.name`` — ``build_bundle`` derives
    the drop-prefix from it.
    """

    def _build(body: str, *, pkg_name: str = "pkg") -> str:
        pkg = tmp_path / pkg_name
        pkg.mkdir()
        (pkg / "__init__.py").write_text("", encoding="utf-8")
        (pkg / "mod.py").write_text(body, encoding="utf-8")
        out = tmp_path / "bundle.py"
        build_bundle(out, runtime_pkg_dir=pkg, runtime_module_order=("mod",))
        return out.read_text(encoding="utf-8")

    return _build


def test_slicer_drops_caller_runtime_intra_pkg_imports(build):
    """``_slice_module`` must drop imports of the caller's runtime
    package — not just the hardcoded mdw prefix."""
    text = build(
        "from __future__ import annotations\n"
        "from myruntime.helpers import helper_fn\n"
        "from myruntime import other\n"
        "import myruntime.deep.submodule\n"
        "import os\n\n"
        "def main():\n"
        "    return helper_fn() + other.value\n",
        pkg_name="myruntime",
    )
    for line in text.splitlines():
        s = line.lstrip()
        assert "myruntime" not in s, f"caller-runtime import leaked: {line!r}"
    assert "import os" in text
    assert "def main():" in text


def test_slicer_drops_reg_monabundle_imports(build):
    """The static amalgamated prefix (``reg_monabundle``) must keep being
    dropped — this guards against a future refactor that accidentally
    relaxes the static set when generalising for the dynamic runtime
    prefix.

    ``reg_schema`` is intentionally NOT in the static set post-A3.4: it
    is never amalgamated (see DESIGN.md → The two halves), so a stray
    ``from reg_schema import …``
    would NOT be dropped and would leak as a live import — which is the
    exact failure ``test_build_mona_bundle.test_bundle_carries_no_pydantic_or_reg_schema``
    gates. The runtime is reg_schema-free by construction, so no such
    import exists in the real runtime modules to drop."""
    text = build(
        "from __future__ import annotations\n"
        "from reg_monabundle import SUPPRESS_K\n"
        "import reg_monabundle.constants\n\n"
        "def x():\n"
        "    return SUPPRESS_K\n",
        pkg_name="tinypkg",
    )
    # Scope to the sliced mod.py block — ``from reg_monabundle`` appears
    # elsewhere in the bundle (the header docstring, the amalgamated
    # module comments). What matters is that it doesn't appear as a live
    # ImportFrom statement inside mod's body.
    mod_block = text[
        text.index("# mod.py") : text.index("# Runner", text.index("# mod.py"))
    ]
    for line in mod_block.splitlines():
        s = line.lstrip()
        assert not s.startswith("from reg_monabundle"), (
            f"reg_monabundle import leaked into sliced runtime: {line!r}"
        )
        assert not s.startswith("import reg_monabundle"), (
            f"reg_monabundle absolute-import leaked: {line!r}"
        )


def test_slicer_emits_valid_python(build):
    """The amalgamated output must parse as Python after slicing."""
    text = build(
        "from __future__ import annotations\n\ndef helper() -> int:\n    return 42\n",
        pkg_name="yetanother",
    )
    ast.parse(text)


def test_static_prefix_uses_exact_or_dotted_match(build):
    """A bare ``startswith("reg_monabundle")`` would match
    ``reg_monabundle_v2`` — the slicer's exact-or-dotted rule must keep
    that import."""
    text = build(
        "from __future__ import annotations\n"
        "import reg_monabundle_v2\n\ndef y(): pass\n",
        pkg_name="exactmatchpkg",
    )
    # The hypothetical sibling import must SURVIVE the slicer — even
    # though running the bundle would ImportError on a real MONA box,
    # that's the runtime author's problem; the slicer's job is to be
    # precise.
    assert "import reg_monabundle_v2" in text


def test_build_bundle_writes_to_supplied_path(tmp_path: Path):
    """Smoke test that the output path argument is honored, including
    nested directory creation."""
    pkg = tmp_path / "spot"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "mod.py").write_text(
        "from __future__ import annotations\n\ndef z(): pass\n", encoding="utf-8"
    )
    out = tmp_path / "nested" / "dir" / "bundle.py"
    result = build_bundle(out, runtime_pkg_dir=pkg, runtime_module_order=("mod",))
    assert result == out
    assert out.exists()
    assert out.stat().st_size > 0


def test_build_bundle_rejects_half_specified_runtime_override(tmp_path: Path):
    """Passing only ``runtime_pkg_dir`` would silently apply the default
    module list to a different directory; same for the inverse. The
    paired check catches misconfiguration with an actionable error
    instead of a downstream missing-file crash."""
    pkg = tmp_path / "halfpkg"
    pkg.mkdir()
    out = tmp_path / "bundle.py"
    with pytest.raises(ValueError, match="supplied together"):
        build_bundle(out, runtime_pkg_dir=pkg)
    with pytest.raises(ValueError, match="supplied together"):
        build_bundle(out, runtime_module_order=("mod",))
