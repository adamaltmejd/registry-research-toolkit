"""Tier 3 admission of a selected artifact (`--artifact-dir`).

Failure messages deliberately omit real binding and physical identifiers.
"""

from __future__ import annotations

import pytest
from artifact_requests import require
from reg_meta_build.db import get_manifest, open_db
from reg_meta_build.validate import validate_built_db


def test_selected_artifact_passes_the_build_validator(artifact_dir, request):
    """SQLite integrity, then `validate_built_db`, the one structural authority:
    foreign keys, manifest identity and the holdings accounting included. It runs
    in the mode each published asset is built in: a catalog with the real-corpus
    floors, a steward artifact with the minted-id band for steward providers.
    The curation `slug_dir` is build input that no asset ships, so the
    entity-key curation gate stays with the build. Synthetic artifacts pass the
    validator when they are built, so only a selected one is checked here."""
    if request.config.getoption("--artifact-dir") is None:
        pytest.skip("synthetic artifacts are validated when they are built")
    db = artifact_dir / "reg_meta.db"
    with open_db(db) as conn:
        require(
            [r[0] for r in conn.execute("PRAGMA integrity_check")] == ["ok"],
            "SQLite integrity check failed",
        )
        kind = get_manifest(conn)["catalog_artifact_kind"]
    result = validate_built_db(db, corpus=kind == "catalog", flavored=kind == "steward")
    # Counts only: the report can name private physical identifiers.
    require(result.passed, f"{len(result.failures)} build validator checks failed")
