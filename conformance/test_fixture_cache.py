"""The fixture cache: keys cover every input, pruning stays in bounds.

The conformance suite running on the cache proves hits work end to end; these
two guard what nothing else would catch.
"""

from __future__ import annotations

import os
import shutil
import time
from typing import TYPE_CHECKING

from reader_artifacts import (
    CASES,
    FIXTURE_CACHE_ENV,
    FIXTURE_CACHE_RETENTION_SECONDS,
    REG_BUILD,
    artifact_key,
    build_inputs_digest,
    cached_reader_artifact,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_only_idle_generations_are_pruned(tmp_path, monkeypatch):
    # Fails if pruning reaches outside `generations/` or ignores the idle cutoff.
    cache = tmp_path / "cache"
    monkeypatch.setenv(FIXTURE_CACHE_ENV, str(cache))
    idle = time.time() - FIXTURE_CACHE_RETENTION_SECONDS - 60
    recent = time.time() - FIXTURE_CACHE_RETENTION_SECONDS + 600
    stale = cache / "generations/stale"
    active = cache / "generations/active"
    neighbour = cache / "neighbour"
    for directory, stamp in ((stale, idle), (active, recent), (neighbour, idle)):
        directory.mkdir(parents=True)
        os.utime(directory, (stamp, stamp))
    # Creating this checkout's generation prunes; only idle generations go.
    path = cached_reader_artifact("base")
    assert path.is_relative_to(cache / "generations")
    assert (stale.exists(), active.exists(), neighbour.exists()) == (False, True, True)


def _perturb(path: Path) -> None:
    with path.open("ab") as handle:
        handle.write(b"\n")


def test_every_build_input_changes_the_key(tmp_path):
    # Fails if a fixture file's path or bytes, or the `reg-build` binary's bytes, drop
    # out of the key, which would serve a stale artifact after that input changes.
    fixture = tmp_path / "fixture"
    shutil.copytree(CASES / "fixtures/base", fixture)
    binary = tmp_path / "reg-build"
    shutil.copyfile(REG_BUILD, binary)

    # Content-addressed: a copy elsewhere keys the same as the live checkout.
    assert artifact_key(fixture) == artifact_key("base")
    assert build_inputs_digest(binary) == build_inputs_digest()
    baseline = artifact_key(fixture), build_inputs_digest(binary)
    _perturb(fixture / "inputs/frame/base.jsonl")
    edited = artifact_key(fixture)
    (fixture / "inputs/frame/base.jsonl").rename(fixture / "inputs/frame/moved.jsonl")
    renamed = artifact_key(fixture)
    _perturb(binary)
    rebuilt = build_inputs_digest(binary)
    assert len({baseline[0], edited, renamed}) == 3
    assert rebuilt != baseline[1]
