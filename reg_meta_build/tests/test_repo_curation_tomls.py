"""The REAL repo curation TOMLs must always parse.

Synthetic builds run with EMPTY curation maps (`_no_repo_curation`, the
session-scoped autouse fixture in `_shared_fixtures.py`) because the repo TOMLs
are keyed on real SCB source ids that collide with fixture register ids. That
fixture removed the incidental coverage the fixture build used to provide: a
malformed entry in the maintainer-edited TOMLs would otherwise surface only on
a real-data `build-db`. This test loads the actual files by DIRECT path — the
autouse fixture only nulls the `repo_*_path` helpers, not the loaders.

Scope: load-time validation only (TOML shape, canonical ints, folded-column
group rules). The build-time half (named columns exist for the var) needs the
real corpus and stays maintainer-build-only by design.
"""

from __future__ import annotations

from pathlib import Path

from reg_meta_build.codelivery import load_codelivery
from reg_meta_build.column_merges import load_column_merges
from reg_meta_build.concept_groups import load_concept_groups
from reg_meta_build.fold_overrides import load_fold_overrides

# reg_meta_build/ package root (tests/ sits beside the TOMLs).
_ROOT = Path(__file__).resolve().parent.parent


def test_repo_codelivery_parses() -> None:
    assert load_codelivery(_ROOT / "codelivery.toml")


def test_repo_fold_overrides_parses() -> None:
    assert load_fold_overrides(_ROOT / "fold_overrides.toml")


def test_repo_column_merges_parses() -> None:
    assert load_column_merges(_ROOT / "column_merges.toml")


def test_repo_concept_groups_parses() -> None:
    groups = load_concept_groups(_ROOT / "concept_groups.toml")
    assert groups  # the LISA agi rank family ships with the repo
    # Build-time resolution (register/group/variable exist) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(len(g.members) >= 2 for g in groups)
