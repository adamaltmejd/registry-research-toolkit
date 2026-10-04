"""Semantic generation identity and the builder's committed naming authority."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta.source_evidence import canonical_sha256

from .input_snapshot import _git

if TYPE_CHECKING:
    from collections.abc import Mapping

GENERATION_KEYS = (
    "schema_version",
    "builder_commit",
    "catalog_artifact_kind",
    "prepared_commit",
    "prepared_manifest_sha256",
    "curation_tree_sha256",
)
STEWARD_GENERATION_KEYS = (
    "steward",
    "base_generation_id",
    "holdings_input_commit",
    "holdings_manifest_sha256",
    "holdings_policy_sha256",
    "holdings_accounting_sha256",
)


def builder_commit() -> str:
    """Capture code identity before a build can regenerate slug files."""
    return _git(Path(__file__).resolve().parents[3], "rev-parse", "HEAD")


def generation_id(manifest: Mapping[str, str]) -> str:
    """Hash only the ratified semantic inputs in canonical JSON encoding."""
    keys = GENERATION_KEYS
    if manifest["catalog_artifact_kind"] == "steward":
        keys += STEWARD_GENERATION_KEYS
    return canonical_sha256({key: manifest[key] for key in keys})


def committed_steward_slugs(steward: str, *, revision: str) -> Path:
    """Admit only ordinary tracked slug bytes from the clean builder checkout."""
    from reg_meta.fqid import validate_slug

    from .input_snapshot import _git_bytes

    validate_slug(steward, "steward")
    root = Path(__file__).resolve().parents[3]
    if _git(root, "rev-parse", "HEAD") != revision:
        raise ValueError("Builder revision changed during holdings compilation")
    if _git(root, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("Publishable extension requires a clean builder checkout")
    directory = root / "reg_meta_build/fqid_slugs" / steward
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError("Committed steward slug directory is absent or a symlink")
    relative = directory.relative_to(root).as_posix()
    committed = set(
        _git(
            root, "ls-tree", "-r", "--name-only", revision, "--", relative
        ).splitlines()
    )
    actual = {
        path.relative_to(root).as_posix()
        for path in directory.rglob("*")
        if path.is_file()
    }
    if not committed or actual != committed:
        raise ValueError(
            "Steward slugs contain uncommitted supplements or missing files"
        )
    for name in sorted(actual):
        path = root / name
        if path.is_symlink() or path.read_bytes() != _git_bytes(
            root, "cat-file", "blob", f"{revision}:{name}"
        ):
            raise ValueError(f"Steward slug differs from committed bytes: {name}")
    if _git(root, "rev-parse", "HEAD") != revision:
        raise ValueError("Builder revision changed during holdings compilation")
    return directory
