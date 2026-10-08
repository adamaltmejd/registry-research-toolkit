"""The `cases/build/` corpus: readable sources and curation through the real build.

Each case directory is one boundary claim (`cases/build/README.md`): its request
names the source set and build options, its curation tree is what a curator would
commit, and its `expected.json` is the oracle, read from the replaced test it
names. A case fails when the build's result status, refusal, report ledger or
built artifact stops matching that oracle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _build_case_runner import (
    PreparedCache,
    cache_root,
    case_dirs,
    case_steps,
    run_step,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(scope="session")
def prepared_cache(tmp_path_factory: pytest.TempPathFactory) -> PreparedCache:
    return PreparedCache(cache_root(tmp_path_factory.getbasetemp()))


@pytest.mark.parametrize("case", case_dirs(), ids=lambda case: case.name)
def test_build_case(case: Path, prepared_cache: PreparedCache, tmp_path: Path) -> None:
    for number, step in enumerate(case_steps(case)):
        actual, expected = run_step(step, prepared_cache, tmp_path / str(number))
        assert actual == expected, step.relative_to(case.parent)
