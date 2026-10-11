"""The sources `reg_core_py` is built from: `crates/reg-core-py`'s uv `cache-keys`.

`uv run` rebuilds the extension when they change, so the source-record interpreter
pin (`source_inspection`) and every cache of builder output (`scripts/keyed_cache.py`)
must cover the same paths. This is their one definition, and `native_doc` is the one
rule for what under them no key hashes. The tooling runs without the project
environment and loads this file by path, so it stays stdlib-only and imports nothing
from `reg_meta_build`.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

NATIVE_PROJECT = "crates/reg-core-py"


def native_sources(repo: Path) -> tuple[str, ...]:
    """`NATIVE_PROJECT`'s uv `cache-keys` as `repo`-relative paths, in their order.

    Each entry must be a `file` key naming one path or a whole directory (`<dir>/**`);
    any other key or glob is refused rather than dropped, since a dropped source would
    go unkeyed.
    """
    project = repo / NATIVE_PROJECT
    keys = tomllib.loads((project / "pyproject.toml").read_text())["tool"]["uv"][
        "cache-keys"
    ]
    sources = []
    for key in keys:
        base = (
            key["file"].removesuffix("**").removesuffix("/")
            if isinstance(key, dict) and key.keys() == {"file"}
            else None
        )
        if base is None or any(char in base for char in "*?[{"):
            raise RuntimeError(
                f"{NATIVE_PROJECT}/pyproject.toml cache-key {key!r} is not a path or "
                "`<dir>/**`; extend reg_meta_build.native_sources to key it"
            )
        path = Path(os.path.normpath(project / base))
        sources.append(path.relative_to(repo).as_posix())
    return tuple(sources)


def native_doc(path: str, sources: Iterable[str]) -> bool:
    """Whether the repo-relative file `path` is documentation under one of the native
    `sources`, which no key hashes.

    Markdown never reaches the compiled extension, so a docs edit there must not move
    a real-seed key or the source-record pin (#1476). uv's own `cache-keys` stay
    broad: a spurious extension rebuild is cheap, a missed one is not.
    """
    return path.endswith(".md") and any(
        path == source or path.startswith(f"{source}/") for source in sources
    )
