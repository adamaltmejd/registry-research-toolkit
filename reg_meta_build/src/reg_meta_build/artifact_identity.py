"""Semantic generation identity and the builder's committed naming authority."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .input_snapshot import SnapshotError, _builder_source_identity, _git
from .source_evidence import canonical_sha256

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

GENERATION_KEYS = (
    "schema_version",
    "builder_commit",
    "catalog_artifact_kind",
    "prepared_commit",
    "prepared_manifest_sha256",
    "curation_tree_sha256",
    "search_pins_sha256",
    "search_synonyms_sha256",
)
STEWARD_GENERATION_KEYS = (
    "steward",
    "base_generation_id",
    "holdings_input_commit",
    "holdings_manifest_sha256",
    "steward_spec_sha256",
    "holdings_accounting_sha256",
)


def search_rows_sha256(rows: Iterable[tuple[str | int, ...]]) -> str:
    """Hash of a curated search table's rows in key order: `search_pin`
    `(key, type, position, entity)` or `search_synonym` `(key, expansion)`.

    It hashes what the artifact stores, so a curation file's comments and notes
    leave this key unchanged (`curation_tree_sha256` still hashes the file's
    bytes); no rows hash as `[]`.
    """
    return canonical_sha256(sorted(rows))


def builder_commit() -> str:
    """Capture the clean tracked builder source, never an enclosing wheel's repo."""
    try:
        _repository, revision = _builder_source_identity()
    except SnapshotError as exc:
        raise ValueError(
            "Publishable builds require clean tracked builder sources; "
            "run from the source checkout, not an installed wheel. " + str(exc)
        ) from exc
    return revision


def generation_id(manifest: Mapping[str, str]) -> str:
    """Hash only the ratified semantic inputs in canonical JSON encoding."""
    keys = GENERATION_KEYS
    if manifest["catalog_artifact_kind"] == "steward":
        keys += STEWARD_GENERATION_KEYS
    # Only a standalone derive records it, so built artifacts keep their ids. Derive
    # rewrites schema_version and builder_commit; this keeps two inputs that differ
    # only in those distinct.
    if "derived_from_generation_id" in manifest:
        keys += ("derived_from_generation_id",)
    if missing := [key for key in keys if key not in manifest]:
        raise ValueError(
            f"Artifact manifest lacks identity keys {missing}: it predates this "
            "builder's generation identity; rebuild (or re-extend) it with this builder"
        )
    return canonical_sha256({key: manifest[key] for key in keys})


def committed_steward_slugs(steward: str, *, revision: str) -> Path:
    """Admit only ordinary tracked slug bytes from the clean builder checkout."""
    return _committed_directory(
        f"reg_meta_build/fqid_slugs/{steward}", steward, "slug", revision=revision
    )


def committed_steward_spec(steward: str, *, revision: str) -> Path:
    """Admit only the tracked steward spec (`reg_webapp/stewards/<id>/`) bytes."""
    from .steward_spec import STEWARDS_DIR

    return _committed_directory(
        f"{STEWARDS_DIR}/{steward}", steward, "spec", revision=revision
    )


def _committed_directory(
    relative: str, steward: str, label: str, *, revision: str
) -> Path:
    """A steward directory whose every file is the clean checkout's committed bytes."""
    from .input_snapshot import _git_bytes
    from .slug_grammar import validate_slug

    validate_slug(steward, "steward")
    root = Path(__file__).resolve().parents[3]
    if _git(root, "rev-parse", "HEAD") != revision:
        raise ValueError("Builder revision changed during holdings compilation")
    if _git(root, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("Publishable extension requires a clean builder checkout")
    directory = root / relative
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError(f"Committed steward {label} directory is absent or a symlink")
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
            f"Steward {label} directory contains uncommitted supplements or missing files"
        )
    for name in sorted(actual):
        path = root / name
        if path.is_symlink() or path.read_bytes() != _git_bytes(
            root, "cat-file", "blob", f"{revision}:{name}"
        ):
            raise ValueError(f"Steward {label} differs from committed bytes: {name}")
    if _git(root, "rev-parse", "HEAD") != revision:
        raise ValueError("Builder revision changed during holdings compilation")
    return directory
