"""Shared locations and the loaded committed curation tree."""

from __future__ import annotations

from pathlib import Path

import pytest
from reg_meta_build.curation_tree import CurationTree, load_curation_tree

# reg_meta_build/ package root (tests/ sits beside the curation/ directory).
REPO_ROOT = Path(__file__).resolve().parent.parent
REPO_CURATION = REPO_ROOT / "curation"


# Session scope: loading the committed tree takes 10-20 s; tests only read it.
@pytest.fixture(scope="session")
def repo_tree() -> CurationTree:
    return load_curation_tree(REPO_CURATION)
