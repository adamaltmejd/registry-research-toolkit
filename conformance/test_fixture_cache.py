"""The synthetic-artifact cache: built once, immutable, keyed by every input."""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest
from reader_artifacts import (
    BUILD_PACKAGES,
    CASES,
    FIXTURE_CACHE_ENV,
    FIXTURE_CACHE_RETENTION_SECONDS,
    artifact_key,
    build_inputs_digest,
    build_reader_artifact,
    cached_reader_artifact,
    installed_distributions,
    runtime_versions,
)

SCRIPT = Path(__file__).with_name("fixture_cache.py")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_a_cached_artifact_is_never_rebuilt(tmp_path, monkeypatch):
    monkeypatch.setenv(FIXTURE_CACHE_ENV, str(tmp_path / "cache"))
    first = cached_reader_artifact("reader", "catalog")
    before = first.stat()
    second = cached_reader_artifact("reader", "catalog")
    after = second.stat()
    assert second == first
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)


def test_only_idle_generations_are_pruned(tmp_path, monkeypatch):
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


def test_a_lookup_rebuilds_a_removed_generation(tmp_path, monkeypatch):
    monkeypatch.setenv(FIXTURE_CACHE_ENV, str(tmp_path / "cache"))
    path = cached_reader_artifact("reader", "catalog")
    shutil.rmtree(path.parents[1])
    assert cached_reader_artifact("reader", "catalog") == path
    assert path.exists()


def test_a_cached_artifact_is_read_only(tmp_path, monkeypatch):
    monkeypatch.setenv(FIXTURE_CACHE_ENV, str(tmp_path / "cache"))
    path = cached_reader_artifact("reader", "catalog")
    with (
        sqlite3.connect(path) as conn,
        pytest.raises(sqlite3.OperationalError, match="readonly"),
    ):
        conn.execute("CREATE TABLE mutation (value)")
    # Mutating callers get a private copy; the entry keeps its bytes.
    entry = _sha256(path)
    copy = build_reader_artifact(tmp_path / "copy", "reader", "catalog")
    with sqlite3.connect(copy) as conn:
        conn.execute("CREATE TABLE mutation (value)")
    conn.close()
    assert _sha256(copy) != entry
    assert _sha256(path) == entry


@pytest.mark.parametrize("kind", ["catalog", "steward"])
def test_a_cache_hit_is_byte_identical_to_a_fresh_build(kind, tmp_path, monkeypatch):
    monkeypatch.setenv(FIXTURE_CACHE_ENV, str(tmp_path / "warm"))
    cached_reader_artifact("reader", kind)
    hit = cached_reader_artifact("reader", kind)
    monkeypatch.setenv(FIXTURE_CACHE_ENV, str(tmp_path / "fresh"))
    fresh = cached_reader_artifact("reader", kind)
    assert hit != fresh
    assert _sha256(hit) == _sha256(fresh)


def test_the_script_prints_the_cached_artifact(tmp_path, monkeypatch):
    monkeypatch.setenv(FIXTURE_CACHE_ENV, str(tmp_path / "cache"))
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--fixture", "reader", "steward"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert Path(completed.stdout.strip()) == cached_reader_artifact("reader", "steward")


def _perturb(path: Path) -> None:
    with path.open("ab") as handle:
        handle.write(b"\n")


def test_every_build_input_class_changes_the_key(tmp_path):
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
