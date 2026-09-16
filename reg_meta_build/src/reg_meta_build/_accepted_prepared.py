"""Cheap Git-bound identity checks shared by prepared source artifacts."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta_build.input_snapshot import (
    _committed_inventory_sizes,
    _git,
    _git_bytes,
    _index_tags,
    _snapshot_repo_path,
    clean_git_commit,
    input_bundle_repository,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


class PreparedInputError(ValueError):
    """A selected prepared artifact no longer matches its preparation proof."""


@dataclass(frozen=True)
class AcceptedPreparedInput:
    root: Path
    repository: Path
    relative: str
    input_commit: str
    manifest_bytes: bytes


def read_accepted_manifest(
    path: Path, *, expected_sha256: str, input_commit: str
) -> AcceptedPreparedInput:
    """Check the exact accepted commit and its small manifest; no data reads."""
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256) or not re.fullmatch(
        r"[0-9a-f]{40}", input_commit
    ):
        raise PreparedInputError(
            "prepared selection requires a full input_commit and manifest SHA-256"
        )
    root = path.expanduser().resolve()
    repo = input_bundle_repository(root)
    relative = root.relative_to(repo).as_posix()
    if clean_git_commit(repo) != input_commit:
        raise PreparedInputError("prepared input commit pin mismatch")
    manifest_path = _snapshot_repo_path(relative, "manifest.json")
    committed = _git_bytes(repo, "cat-file", "blob", f"{input_commit}:{manifest_path}")
    if hashlib.sha256(committed).hexdigest() != expected_sha256:
        raise PreparedInputError("prepared source manifest hash mismatch")
    if (root / "manifest.json").is_symlink() or (
        root / "manifest.json"
    ).read_bytes() != committed:
        raise PreparedInputError("prepared source manifest differs from pinned commit")
    return AcceptedPreparedInput(root, repo, relative, input_commit, committed)


def check_accepted_files(
    selection: AcceptedPreparedInput, proofs: Mapping[str, tuple[int, str]]
) -> None:
    """Check exact files/, sizes and prepared Git blob identities without rehashing.

    A new accepted commit alone cannot bless a same-size payload edit while
    retaining a stale preparation manifest. Blob IDs bind that manifest to the
    bytes actually validated during preparation.
    """
    root, repo = selection.root, selection.repository
    relative, commit = selection.relative, selection.input_commit
    for name, (size, blob) in proofs.items():
        parts = Path(name).parts
        if (
            len(parts) < 2
            or parts[0] != "files"
            or ".." in parts
            or Path(name).as_posix() != name
            or type(size) is not int
            or size < 0
            or re.fullmatch(r"[0-9a-f]{40}", blob) is None
        ):
            raise PreparedInputError(f"invalid prepared file proof: {name}")
    inventory = _committed_inventory_sizes(
        repo,
        commit,
        relative,
        (_snapshot_repo_path(relative, "files"),),
        context="prepared sources",
    )
    if inventory != {name: proof[0] for name, proof in proofs.items()}:
        raise PreparedInputError(
            "prepared source committed inventory differs from manifest"
        )
    for name, (_, blob) in proofs.items():
        member = _snapshot_repo_path(relative, name)
        if _git(repo, "rev-parse", "--verify", f"{commit}:{member}") != blob:
            raise PreparedInputError(
                "prepared source committed database differs from its preparation proof"
            )
    tags = _index_tags(repo)
    if any(
        tags.get(_snapshot_repo_path(relative, name)) != "H"
        for name in ("manifest.json", *proofs)
    ):
        raise PreparedInputError(
            "prepared input files must be fully materialized with ordinary Git index flags"
        )
    files = tuple((root / "files").rglob("*"))
    actual = {
        item.relative_to(root).as_posix(): item.stat().st_size
        for item in files
        if item.is_file()
    }
    if (
        (root / "files").is_symlink()
        or any(item.is_symlink() for item in files)
        or actual != inventory
    ):
        raise PreparedInputError(
            "prepared source worktree inventory differs from manifest"
        )
    if _git(repo, "rev-parse", "HEAD") != commit:
        raise PreparedInputError("prepared input commit changed during selection")
