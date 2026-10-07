"""Readable source contracts through the real catalog writer and holdings compiler."""

from __future__ import annotations

import functools
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta.catalog import DataWarning
from reg_meta.db import SCHEMA_VERSION, open_db
from reg_meta.source_evidence import canonical_json, canonical_sha256
from reg_meta_build.artifact_identity import generation_id
from reg_meta_build.holdings_compile import compile_holdings
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedClassificationSuccession,
    ResolvedVariable,
    write_resolved_catalog,
)
from reg_meta_build.resolved_metadata import ResolvedMetadata
from reg_meta_build.validate import validate_built_db

import reg_meta
import reg_meta_build
import reg_schema

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

CASES = Path(__file__).resolve().parents[2] / "conformance/cases"
BUILDER_CASES = CASES.parents[1] / "reg_meta_build/tests/cases/holdings"
FIXTURE_IMPORT_DATE = json.loads(
    (CASES / "reader/fixture/import_metadata.json").read_text()
)["import_date"]

FIXTURE_CACHE_ENV = "REG_FIXTURE_CACHE"
# Bump when the cache layout or key composition changes.
FIXTURE_CACHE_LAYOUT = 1
# A generation idle this long is pruned when a new one is created. Every lookup
# marks its generation as used, so a returned path stays valid this long after
# its last lookup.
FIXTURE_CACHE_RETENTION_SECONDS = 6 * 3600
BUILD_PACKAGES = tuple(
    Path(package.__file__).parent for package in (reg_meta_build, reg_meta, reg_schema)
)


def replicate_filler(case: Path, destination: Path) -> Path:
    """Copy a reader case, expanding its `request.json` filler into copies.

    `request.json` names one `filler` binding of the case's `catalog.json` and a
    `copies` count; the filler is replaced by that many copies whose slug and
    provider key carry a `-N` suffix. An optional `held_table` `{id, edition}`
    also holds every copy: one inventory column and census row per copy, with the
    holdings policy re-pinned to the expanded census. Returns the expanded source
    directory.
    """
    request = json.loads((case / "request.json").read_text())
    shutil.copytree(case, destination)
    catalog = json.loads((case / "catalog.json").read_text())
    filler = next(
        variable
        for variable in catalog
        if "/".join(
            (
                variable["register"]["provider"],
                variable["register"]["slug"],
                variable["slug"],
            )
        )
        == request["filler"]
    )
    catalog.remove(filler)
    catalog.extend(
        {
            **filler,
            "slug": f"{filler['slug']}-{index}",
            "provider_key": f"{filler['provider_key']}-{index}",
        }
        for index in range(request["copies"])
    )
    (destination / "catalog.json").write_text(json.dumps(catalog))
    table = request.get("held_table")
    if table is not None:
        state = filler["states"][0]
        provider, register = filler["register"]["provider"], filler["register"]["slug"]
        mappings = "".join(
            f'[[table.column]]\nname = "Copy{index}"\n[[table.column.mapping]]\n'
            f'register_variant = "{provider}/{register}/{state["variant"]["slug"]}"\n'
            f'variable = "{provider}/{register}/{filler["slug"]}-{index}"\n'
            f'representation = "{state["delivery_column_name"]}"\n'
            for index in range(request["copies"])
        )
        with (destination / "inventory.toml").open("a") as inventory:
            inventory.write(
                f'\n[[table]]\nid = "{table["id"]}"\nedition = {table["edition"]}\n'
                + mappings
            )
        census = destination / "census.csv"
        with census.open("a") as rows:
            rows.writelines(
                f"Fixture,,{table['id']},Copy{index},\n"
                for index in range(request["copies"])
            )
        digest = hashlib.sha256(census.read_bytes()).hexdigest()
        (destination / "holdings_policy.toml").write_text(
            f'source_sha256 = "{digest}"\n'
        )
    return destination


def fixture_source(fixture: str | Path) -> Path:
    """The readable source directory a fixture name (or path) builds from."""
    return (
        fixture
        if isinstance(fixture, Path)
        else CASES / "reader/fixture"
        if fixture == "reader"
        else CASES / fixture
        if fixture.startswith("reader/")
        else BUILDER_CASES / fixture
    )


def fixture_cache_dir() -> Path:
    """Artifact cache shared by worktrees and pytest-xdist workers.

    The temp directory is writable inside agent sandboxes and cleared on reboot.
    """
    configured = os.environ.get(FIXTURE_CACHE_ENV)
    if configured:
        return Path(configured).expanduser()
    return Path(tempfile.gettempdir()) / "registry-research-toolkit-fixtures"


def _tree_digest(root: Path) -> str:
    """Content digest of a file or directory, independent of where it lives."""
    files = (
        [root]
        if root.is_file()
        else sorted(
            path
            for path in root.rglob("*")
            if path.is_file()
            and "__pycache__" not in path.parts
            and path.suffix != ".pyc"
        )
    )
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        with path.open("rb") as handle:
            digest.update(hashlib.file_digest(handle, "sha256").digest())
    return digest.hexdigest()


def runtime_versions() -> dict[str, str]:
    """Interpreter and SQLite library versions the pipeline runs on."""
    return {
        "python": sys.version,
        "sqlite": sqlite3.sqlite_version,
        "platform": f"{sys.platform}-{platform.machine()}",
    }


def installed_distributions() -> list[str]:
    """Every installed distribution as a sorted `name==version` pair."""
    return sorted(
        {
            f"{distribution.metadata['Name']}=={distribution.version}"
            for distribution in importlib.metadata.distributions()
        }
    )


def build_inputs_digest(
    *,
    packages: Sequence[Path] = BUILD_PACKAGES,
    builder: Path = Path(__file__),
    distributions: Sequence[str] | None = None,
    runtime: Mapping[str, str] | None = None,
) -> str:
    """Digest of every fixture-independent build input.

    The defaults are the imported `reg_meta_build`, `reg_meta` and `reg_schema`
    sources (file contents, so uncommitted edits count), this builder,
    `installed_distributions()` and `runtime_versions()`: what actually runs,
    not what a lockfile asks for. The arguments let a caller digest other copies.
    """
    return canonical_sha256(
        {
            "layout": FIXTURE_CACHE_LAYOUT,
            "packages": [_tree_digest(package) for package in packages],
            "builder": _tree_digest(builder),
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
    kind: str,
    *,
    identity_overrides: Mapping[str, str] | None = None,
    build_inputs: str | None = None,
) -> str:
    """Cache key of one synthetic artifact: its sources, options and build inputs.

    Every build also reads the shared `reader/fixture` identity and steward policy
    fallbacks, so that directory is keyed alongside the named source.
    """
    return canonical_sha256(
        {
            "build_inputs": build_inputs or _live_build_inputs(),
            "source": _tree_digest(fixture_source(fixture)),
            "defaults": _tree_digest(CASES / "reader/fixture"),
            "kind": kind,
            "identity_overrides": dict(identity_overrides or {}),
        }
    )


def _generation_dir() -> Path:
    """This build-input generation's cache directory, marked as in use."""
    generations = fixture_cache_dir() / "generations"
    generation = generations / _live_build_inputs()
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


def _retry_if_pruned[T](lookup: Callable[[], T]) -> T:
    """Run a cache lookup, once more if a concurrent prune removed its generation.

    A prune can read a generation's old mtime just before a lookup touches it;
    the retry recreates the generation and rebuilds the entry.
    """
    try:
        return lookup()
    except FileNotFoundError:
        return lookup()


def cached_reader_artifact(
    fixture: str | Path,
    kind: str,
    *,
    identity_overrides: Mapping[str, str] | None = None,
) -> Path:
    """Return the cached, read-only `reg_meta.db`, building it on first use.

    Entries are immutable: callers that mutate an artifact use
    `build_reader_artifact`, which copies one. A miss builds into a private
    staging directory and renames it into place, so concurrent builders of the
    same key never expose a partial entry; the loser discards its copy.
    """
    return _retry_if_pruned(lambda: _cached_artifact(fixture, kind, identity_overrides))


def _cached_artifact(
    fixture: str | Path,
    kind: str,
    identity_overrides: Mapping[str, str] | None,
) -> Path:
    entry = _generation_dir() / artifact_key(
        fixture, kind, identity_overrides=identity_overrides
    )
    path = entry / "reg_meta.db"
    if path.exists():
        return path
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=entry.parent))
    try:
        _build_artifact(staging, fixture_source(fixture), kind, identity_overrides)
        (staging / "reg_meta.db").chmod(0o444)
        try:
            staging.rename(entry)
        except OSError:
            if not path.exists():
                raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return path


def build_reader_artifact(
    directory: Path,
    fixture: str | Path,
    kind: str,
    *,
    identity_overrides: Mapping[str, str] | None = None,
) -> Path:
    """Copy the cached artifact into `directory` as a private, writable file."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "reg_meta.db"

    def copy() -> Path:
        shutil.copyfile(_cached_artifact(fixture, kind, identity_overrides), path)
        return path

    return _retry_if_pruned(copy)


def _build_artifact(
    directory: Path,
    source: Path,
    kind: str,
    identity_overrides: Mapping[str, str] | None,
) -> Path:
    identity = json.loads((CASES / "reader/fixture/identity.json").read_text())
    identity.update(identity_overrides or {})
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
    write_resolved_catalog(
        variables,
        path,
        manifest=identity,
        classifications=classifications,
        classification_successions=successions,
        metadata=metadata,
        data_warnings=warnings,
    )
    if kind == "steward":
        base_digest = hashlib.file_digest(path.open("rb"), "sha256").hexdigest()
        candidate = directory / "candidate"
        (candidate / "policy").mkdir(parents=True)
        (candidate / "swecov").mkdir()
        for filename in (
            "inventory.toml",
            "holdings_policy.toml",
            "source_policy.toml",
            "inventory_overlay.toml",
        ):
            item = source / filename
            fallback = CASES / "reader/fixture" / filename
            shutil.copyfile(
                item if item.exists() else fallback, candidate / "policy" / filename
            )
        shutil.copyfile(
            source / "census.csv",
            candidate / "swecov/SWECOV_variables_full_fixture.csv",
        )
        with sqlite3.connect(path) as conn:
            compiled = compile_holdings(conn, candidate, steward="swecov")
            manifest = dict(conn.execute("SELECT key, value FROM import_manifest"))
            manifest.update(
                catalog_artifact_kind="steward",
                steward="swecov",
                base_db_sha256=base_digest,
                base_generation_id=manifest["generation_id"],
                holdings_input_commit="e" * 40,
                holdings_manifest_sha256="f" * 64,
                holdings_policy_sha256=compiled.accounting.policy_sha256,
                holdings_accounting_sha256=compiled.accounting.sha256,
                holdings_accounting_counts=canonical_json(compiled.accounting.counts),
            )
            manifest["generation_id"] = generation_id(manifest)
            conn.executemany(
                "INSERT OR REPLACE INTO import_manifest VALUES (?, ?)",
                sorted(manifest.items()),
            )
        shutil.rmtree(candidate)
    validation = validate_built_db(path)
    assert validation.passed, validation.format_report()
    open_db(path).close()
    return path


def stamp_catalog_identity(conn: sqlite3.Connection) -> None:
    """Complete identity metadata for legacy synthetic reader catalogs."""
    identity = json.loads((CASES / "reader/fixture/identity.json").read_text())
    identity.update(
        schema_version=SCHEMA_VERSION,
        catalog_artifact_kind="catalog",
        catalog_publishable="true",
        catalog_completeness="complete",
    )
    identity["generation_id"] = generation_id(identity)
    conn.executemany(
        "INSERT OR REPLACE INTO import_manifest VALUES (?, ?)", sorted(identity.items())
    )
