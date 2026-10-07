"""The synthetic-artifact cache: keys cover every input, pruning stays in bounds.

The conformance suite running on the cache proves hits work end to end; these
two guard what nothing else would catch.
"""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

from reader_artifacts import (
    BUILD_PACKAGES,
    CASES,
    FIXTURE_CACHE_ENV,
    FIXTURE_CACHE_RETENTION_SECONDS,
    artifact_key,
    build_inputs_digest,
    cached_reader_artifact,
    installed_distributions,
    runtime_versions,
)


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
    path = cached_reader_artifact("reader", "catalog")
    assert path.is_relative_to(cache / "generations")
    assert (stale.exists(), active.exists(), neighbour.exists()) == (False, True, True)


def _perturb(path: Path) -> None:
    with path.open("ab") as handle:
        handle.write(b"\n")


def test_every_build_input_class_changes_the_key(tmp_path):
    # Fails if any input class drops out of the key, which would serve a stale
    # artifact after that input changes.
    packages = [tmp_path / package.name for package in BUILD_PACKAGES]
    for source, copy in zip(BUILD_PACKAGES, packages, strict=True):
        shutil.copytree(source, copy, ignore=shutil.ignore_patterns("__pycache__"))
    builder = tmp_path / "reader_artifacts.py"
    shutil.copyfile(
        Path(__file__).parents[1] / "reg_meta/tests/reader_artifacts.py", builder
    )
    distributions = installed_distributions()
    fixture = tmp_path / "fixture"
    shutil.copytree(CASES / "reader/fixture", fixture)
    runtime = runtime_versions()

    def inputs(**changed):
        return build_inputs_digest(
            **{
                "packages": packages,
                "builder": builder,
                "distributions": distributions,
                "runtime": runtime,
                **changed,
            }
        )

    def key(**changed):
        options = {
            "kind": "steward",
            "identity_overrides": {"import_date": "2024-01-01"},
            "build_inputs": inputs(),
            **changed,
        }
        return artifact_key(fixture, options.pop("kind"), **options)

    # Content-addressed: a copy elsewhere keys the same as the live checkout.
    assert inputs() == build_inputs_digest()
    baseline = key()
    changed = {
        "kind": key(kind="catalog"),
        "identity_overrides": key(identity_overrides={"import_date": "2024-01-02"}),
        "python": key(build_inputs=inputs(runtime={**runtime, "python": "3.99"})),
        "sqlite": key(build_inputs=inputs(runtime={**runtime, "sqlite": "9.0.0"})),
        "distributions": key(
            build_inputs=inputs(distributions=[*distributions, "extra==1.0"])
        ),
    }
    for label, path in [
        ("fixture", fixture / "catalog.json"),
        ("builder", builder),
        *((package.name, min(package.rglob("*.py"))) for package in packages),
    ]:
        # Cumulative edits: each must move the key off every earlier one.
        _perturb(path)
        changed[label] = key(build_inputs=inputs())
    assert len(changed) == 10
    assert baseline not in changed.values()
    assert len(set(changed.values())) == len(changed)
