"""Readable sources through the old builder's catalog writer, until milestone F.

The conformance harness builds with `reg-build` (`conformance/reader_artifacts.py`).
"""

from __future__ import annotations

import functools
import importlib.metadata
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta_build.data_warnings import DataWarning
from reg_meta_build.db import open_db
from reg_meta_build.materialize import write_resolved_catalog
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationSuccession,
    ResolvedEdition,
    ResolvedVariable,
)
from reg_meta_build.resolved_metadata import ResolvedMetadata
from reg_meta_build.source_evidence import canonical_sha256
from reg_meta_build.validate import validate_built_db

import reg_meta_build

# The digest and staging helpers are shared with the tooling caches.
sys.path.append(str(Path(__file__).resolve().parents[2] / "scripts"))
from keyed_cache import (
    NATIVE_SOURCES as _NATIVE_SOURCES,
    native_doc,
    staged,
    tree_digest,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

CASES = Path(__file__).resolve().parents[2] / "conformance/cases"
FIXTURE_IMPORT_DATE = json.loads(
    (CASES / "reader/fixture/import_metadata.json").read_text()
)["import_date"]

FIXTURE_CACHE_ENV = "REG_FIXTURE_CACHE"
# A generation idle this long is pruned when a new one is created. Every lookup
# marks its generation as used, so a returned path stays valid this long after
# its last lookup.
FIXTURE_CACHE_RETENTION_SECONDS = 6 * 3600
BUILD_PACKAGES = (Path(reg_meta_build.__file__).parent,)
# The sources `reg-core-py` is built from (its uv `cache-keys`): `uv run` rebuilds the
# extension when they change, and so must the cache.
NATIVE_SOURCES = tuple(CASES.parents[1] / path for path in _NATIVE_SOURCES)


def fixture_source(fixture: str | Path) -> Path:
    """A fixture path or a source directory under the conformance cases."""
    return fixture if isinstance(fixture, Path) else CASES / fixture


def fixture_cache_dir() -> Path:
    """Artifact cache shared by worktrees and pytest-xdist workers.

    The temp directory is writable inside agent sandboxes and cleared on reboot.
    """
    configured = os.environ.get(FIXTURE_CACHE_ENV)
    if configured:
        return Path(configured).expanduser()
    return Path(tempfile.gettempdir()) / "registry-research-toolkit-fixtures"


def runtime_versions() -> dict[str, str]:
    """Interpreter and SQLite library versions the pipeline runs on."""
    return {
        "python": sys.version,
        "sqlite": sqlite3.sqlite_version,
    }


def installed_distributions() -> list[str]:
    """Every installed distribution as a sorted `name==version` pair."""
    return sorted(
        {
            f"{distribution.metadata['Name']}=={distribution.version}"
            for distribution in importlib.metadata.distributions()
        }
    )


def _native_files(root: Path) -> list[Path]:
    """The files under the native source `root` the key hashes: not its
    documentation (`native_doc`)."""
    files = [root] if root.is_file() else [p for p in root.rglob("*") if p.is_file()]
    repo = CASES.parents[1]
    return [p for p in files if not native_doc(p.relative_to(repo).as_posix())]


def build_inputs_digest(
    *,
    packages: Sequence[Path] = BUILD_PACKAGES,
    builder: Path = Path(__file__),
    distributions: Sequence[str] | None = None,
    runtime: Mapping[str, str] | None = None,
) -> str:
    """Digest of every fixture-independent build input.

    The defaults are the imported `reg_meta_build`
    sources (file contents, so uncommitted edits count), the Rust sources of
    `reg-core-py`, this builder,
    `installed_distributions()` and `runtime_versions()`: what actually runs,
    not what a lockfile asks for. The arguments let a caller digest other copies.
    Any change to this builder, the cache code included, is a new key.
    """
    return canonical_sha256(
        {
            "packages": [tree_digest(package) for package in packages],
            "native": [
                tree_digest(path, _native_files(path)) for path in NATIVE_SOURCES
            ],
            "builder": tree_digest(builder),
            "distributions": list(
                distributions
                if distributions is not None
                else installed_distributions()
            ),
            "runtime": dict(runtime if runtime is not None else runtime_versions()),
        }
    )


@functools.cache
def _live_build_inputs() -> str:
    return build_inputs_digest()


def artifact_key(
    fixture: str | Path,
    *,
    identity_overrides: Mapping[str, str] | None = None,
    build_inputs: str | None = None,
) -> str:
    """Cache key of a synthetic catalog: sources, identity and build inputs."""
    return canonical_sha256(
        {
            "build_inputs": build_inputs or _live_build_inputs(),
            "source": tree_digest(fixture_source(fixture)),
            "defaults": tree_digest(CASES / "reader/fixture"),
            "identity_overrides": dict(identity_overrides or {}),
        }
    )


def _generation_dir() -> Path:
    """This build-input generation's cache directory, marked as in use."""
    return generation_dir(_live_build_inputs())


def generation_dir(build_inputs: str) -> Path:
    """The cache directory of the generation keyed ``build_inputs``, marked as in use.

    Other fixture caches (the `reg_meta_build` prepared inputs) key their own
    generations and share this one pruning policy.
    """
    generations = fixture_cache_dir() / "generations"
    generation = generations / build_inputs
    if not generation.is_dir():
        generation.mkdir(parents=True, exist_ok=True)
        # simplify: every source edit starts a generation (~35 MB), so ones idle
        # for 6 hours are pruned; switch to size-based eviction if that still grows.
        # Pruning only ever touches `generations/`, never the cache's parent.
        cutoff = time.time() - FIXTURE_CACHE_RETENTION_SECONDS
        for stale in generations.iterdir():
            if stale.is_dir() and stale.stat().st_mtime < cutoff:
                shutil.rmtree(stale, ignore_errors=True)
    os.utime(generation)
    return generation


def cached_reader_artifact(
    fixture: str | Path,
    *,
    identity_overrides: Mapping[str, str] | None = None,
) -> Path:
    """Return the cached, read-only `reg_meta.db`, building it on first use.

    Entries are immutable: callers that mutate an artifact use
    `build_reader_artifact`, which copies one. A miss builds into a private staging
    directory and renames it into place, so concurrent builders of the same key
    never expose a partial entry; the loser discards its copy.
    """
    entry = _generation_dir() / artifact_key(
        fixture,
        identity_overrides=identity_overrides,
    )
    path = entry / "reg_meta.db"
    if path.exists():
        return path
    source = fixture_source(fixture)
    with staged(entry) as staging:
        _build_artifact(staging, source, identity_overrides)
        (staging / "reg_meta.db").chmod(0o444)
    return path


def build_reader_artifact(
    directory: Path,
    fixture: str | Path,
    *,
    identity_overrides: Mapping[str, str] | None = None,
) -> Path:
    """Copy the cached catalog into `directory` as a private, writable file."""
    cached = cached_reader_artifact(fixture, identity_overrides=identity_overrides)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "reg_meta.db"
    shutil.copyfile(cached, path)
    return path


def _build_artifact(
    directory: Path,
    source: Path,
    identity_overrides: Mapping[str, str] | None,
) -> Path:
    identity = json.loads((CASES / "reader/fixture/identity.json").read_text())
    identity.update(identity_overrides or {})
    # The writer hashes the pins and synonyms it stores.
    del identity["search_pins_sha256"], identity["search_synonyms_sha256"]
    variables = tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads((source / "catalog.json").read_text())
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "reg_meta.db"
    classification_source = source / "classifications.json"
    classifications = (
        tuple(
            ResolvedClassification.model_validate_json(json.dumps(value))
            for value in json.loads(classification_source.read_text())
        )
        if classification_source.exists()
        else ()
    )
    succession_source = source / "classification_successions.json"
    successions = (
        tuple(
            ResolvedClassificationSuccession.model_validate_json(json.dumps(value))
            for value in json.loads(succession_source.read_text())
        )
        if succession_source.exists()
        else ()
    )
    metadata_source = source / "metadata.json"
    metadata = (
        ResolvedMetadata.model_validate_json(metadata_source.read_text())
        if metadata_source.exists()
        else None
    )
    warning_source = source / "data_warnings.json"
    warnings = (
        tuple(
            DataWarning.model_validate_json(json.dumps(value))
            for value in json.loads(warning_source.read_text())
        )
        if warning_source.exists()
        else ()
    )
    edition_source = source / "editions.json"
    write_resolved_catalog(
        variables,
        path,
        manifest=identity,
        editions=tuple(
            ResolvedEdition.model_validate_json(json.dumps(value))
            for value in json.loads(edition_source.read_text())
        )
        if edition_source.exists()
        else (),
        classifications=classifications,
        classification_successions=successions,
        metadata=metadata,
        data_warnings=warnings,
    )
    validation = validate_built_db(path)
    assert validation.passed, validation.format_report()
    open_db(path).close()
    return path
