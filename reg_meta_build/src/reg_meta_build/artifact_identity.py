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
