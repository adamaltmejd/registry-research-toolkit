"""Strict accepted-input compilation through CLI and immutable artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from _prepared_fixtures import accept_prepared
from catalog_manifest import synthetic_manifest
from reg_meta.catalog import DataWarning
from reg_meta_build.extend_db import extend_db
from reg_meta_build.resolved_catalog import ResolvedVariable, write_resolved_catalog

import reg_meta_build

CASE = Path(__file__).parent / "cases/holdings/steward-boundary"
REQUEST = json.loads((CASE / "request.json").read_text())
EXPECTED = json.loads((CASE / "expected.json").read_text())


def _builder_checkout(directory: Path) -> tuple[Path, str]:
    checkout = directory / "builder"
    source = checkout / "reg_meta_build/src"
    source.mkdir(parents=True)
    assert reg_meta_build.__file__ is not None
    package = Path(reg_meta_build.__file__).parent
    shutil.copytree(
        package,
        source / "reg_meta_build",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    repository = package.parents[2]
    scripts = checkout / "scripts"
    scripts.mkdir()
    shutil.copyfile(
        repository / "scripts/prototype_scb_inputs.py",
        scripts / "prototype_scb_inputs.py",
    )
    shutil.copyfile(repository / "uv.lock", checkout / "uv.lock")
    (checkout / ".gitignore").write_text("__pycache__/\n")
    slugs = checkout / "reg_meta_build/fqid_slugs" / REQUEST["steward"]
    slugs.mkdir(parents=True)
    shutil.copyfile(CASE / "slugs.toml", slugs / "fixture.toml")
    accept_prepared(checkout / "reg_meta_build")
    subprocess.run(
        ["git", "-C", str(checkout), "add", "--", ".gitignore", "scripts", "uv.lock"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(checkout), "commit", "-q", "-m", "Pin builder tools"],
        check=True,
    )
    revision = subprocess.check_output(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True
    ).strip()
    return source, revision


def _accepted_candidate(directory: Path, *, inventory: str = "inventory.toml"):
    root = directory / "candidate"
    for subdirectory in ("policy", "providers", "swecov"):
        (root / subdirectory).mkdir(parents=True)
    for source, destination in (
        (inventory, "policy/inventory.toml"),
        ("source_policy.toml", "policy/source_policy.toml"),
        ("inventory_overlay.toml", "policy/inventory_overlay.toml"),
        ("holdings_policy.toml", "policy/holdings_policy.toml"),
        ("provider.toml", "providers/fixture.toml"),
        ("census.csv", REQUEST["census_name"]),
    ):
        shutil.copyfile(CASE / source, root / destination)
    manifest = root / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "artifact_kind": "swecov-private-extension-input-candidate",
                "files": [
                    {
                        "path": path.relative_to(root).as_posix(),
                        "size": path.stat().st_size,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    }
                    for path in sorted(root.rglob("*"))
                    if path.is_file()
                ],
            },
            sort_keys=True,
        )
    )
    revision = accept_prepared(root)
    return root, revision, hashlib.sha256(manifest.read_bytes()).hexdigest()


def _extend(source: Path, base: Path, candidate: tuple, output: Path):
    root, revision, digest = candidate
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "reg_meta_build.cli",
            "--db",
            str(output),
            "extend-db",
            "--base-db",
            str(base),
            "--holdings-input",
            str(root),
            "--input-commit",
            revision,
            "--input-manifest-sha256",
            digest,
            "--steward",
            REQUEST["steward"],
        ],
        env={**os.environ, "PYTHONPATH": str(source)},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture(scope="module")
def strict_builds(tmp_path_factory: pytest.TempPathFactory) -> dict:
    directory = tmp_path_factory.mktemp("strict-holdings")
    source, builder_revision = _builder_checkout(directory)
    candidate = _accepted_candidate(directory / "accepted")
    variables = tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads((CASE / "base_catalog.json").read_text())
    )
    base = directory / "base/reg_meta.db"
    write_resolved_catalog(variables, base, manifest=synthetic_manifest())
    original = base.read_bytes()
    builds = []
    for name in ("first", "replay"):
        output = directory / name
        result = _extend(source, base, candidate, output)
        assert result.returncode == 0, result.stdout + result.stderr
        builds.append((output / "reg_meta.db", json.loads(result.stdout)))
    assert base.read_bytes() == original
    return {
        "directory": directory,
        "source": source,
        "builder_revision": builder_revision,
        "candidate": candidate,
        "base": base,
        "builds": builds,
    }


def test_strict_extension_publishes_pinned_steward_artifact(
    strict_builds: dict,
) -> None:
    output, counts = strict_builds["builds"][0]
    assert {key: counts[key] for key in EXPECTED["counts"]} == EXPECTED["counts"]
    with sqlite3.connect(output) as conn:
        manifest = dict(conn.execute("SELECT key,value FROM import_manifest"))
        conn.row_factory = sqlite3.Row
        mappings = [
            dict(row)
            for row in conn.execute(
                'SELECT ht.physical_id AS "table", hc.name AS "column", '
                "p.slug || '/' || r.slug || '/' || rv.slug AS variant, "
                "p.slug || '/' || r.slug || '/' || v.slug AS variable, "
                "hm.representation_literal AS literal, "
                "hm.representation_canonical AS canonical "
                "FROM holding_mapping hm JOIN holding_column hc USING(column_id) "
                "JOIN holding_table ht USING(table_id) "
                "JOIN variable v USING(variable_id) JOIN register r USING(register_id) "
                "JOIN provider p USING(provider_id) "
                "JOIN register_variant rv ON rv.register_variant_id=hm.variant_id "
                "ORDER BY ht.physical_id,hc.name"
            )
        ]
    with sqlite3.connect(strict_builds["base"]) as conn:
        base_manifest = dict(conn.execute("SELECT key,value FROM import_manifest"))
    assert {key: manifest[key] for key in EXPECTED["manifest"]} == EXPECTED["manifest"]
    assert manifest["builder_commit"] == strict_builds["builder_revision"]
    assert manifest["holdings_input_commit"] == strict_builds["candidate"][1]
    assert manifest["holdings_manifest_sha256"] == strict_builds["candidate"][2]
    assert manifest["base_generation_id"] == base_manifest["generation_id"]
    assert (
        manifest["base_db_sha256"]
        == hashlib.sha256(strict_builds["base"].read_bytes()).hexdigest()
    )
    assert json.loads(manifest["holdings_accounting_counts"]) == EXPECTED["accounting"]
    assert manifest["generation_id"] != base_manifest["generation_id"]
    assert mappings == EXPECTED["mappings"]
    assert not output.with_suffix(".db.tmp").exists()


def test_fresh_strict_extensions_are_byte_identical(strict_builds: dict) -> None:
    first, replay = (build[0] for build in strict_builds["builds"])
    assert first.read_bytes() == replay.read_bytes()


def test_unaccounted_census_is_rejected_before_output_creation(
    strict_builds: dict,
) -> None:
    directory = strict_builds["directory"]
    candidate = _accepted_candidate(
        directory / "unaccounted", inventory=REQUEST["unaccounted_inventory"]
    )
    output = directory / "rejected-output"
    result = _extend(strict_builds["source"], strict_builds["base"], candidate, output)
    assert result.returncode != 0
    assert (
        json.loads(result.stdout)["error"]["message"] == EXPECTED["unaccounted_error"]
    )
    assert not output.exists()


@pytest.mark.parametrize("supplement", REQUEST["supplemental_inputs"])
def test_strict_extension_rejects_unpinned_supplements(
    strict_builds: dict, supplement: str
) -> None:
    root, revision, digest = strict_builds["candidate"]
    output = strict_builds["directory"] / f"rejected-{supplement}"
    warnings = (
        (
            DataWarning.model_validate_json(
                (CASE.parent / "warning/warning.json").read_text()
            ),
        )
        if supplement == "data_warnings"
        else ()
    )
    with pytest.raises(ValueError, match=EXPECTED["supplement_error"]):
        extend_db(
            base_db=strict_builds["base"],
            providers_dir=None,
            db_dir=output,
            steward=REQUEST["steward"],
            holdings_input=root,
            input_commit=revision,
            input_manifest_sha256=digest,
            data_warnings=warnings,
            pre_rename_hook=(lambda _: None)
            if supplement == "pre_rename_hook"
            else None,
        )
    assert not output.exists()
