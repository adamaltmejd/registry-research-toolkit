"""Readable source contracts through the real catalog writer and holdings compiler."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

from reg_meta.catalog import DataWarning
from reg_meta.db import SCHEMA_VERSION, open_db
from reg_meta.source_evidence import canonical_json
from reg_meta_build.artifact_identity import generation_id
from reg_meta_build.holdings_compile import compile_holdings
from reg_meta_build.resolved_catalog import (
    ResolvedClassification,
    ResolvedVariable,
    write_resolved_catalog,
)
from reg_meta_build.resolved_metadata import ResolvedMetadata
from reg_meta_build.validate import validate_built_db

CASES = Path(__file__).parent / "cases"
BUILDER_CASES = CASES.parents[2] / "reg_meta_build/tests/cases/holdings"


def build_reader_artifact(
    directory: Path,
    fixture: str,
    kind: str,
    *,
    identity_overrides: dict[str, str] | None = None,
) -> Path:
    source = (
        CASES / "reader/fixture"
        if fixture == "reader"
        else CASES / fixture
        if fixture.startswith("reader/")
        else BUILDER_CASES / fixture
    )
    identity = json.loads((CASES / "reader/fixture/identity.json").read_text())
    identity.update(identity_overrides or {})
    variables = tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads((source / "catalog.json").read_text())
    )
    directory.mkdir(parents=True)
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
