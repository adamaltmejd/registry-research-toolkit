"""Builder provenance and accepted snapshots at CLI and built-artifact boundaries."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from _prepared_fixtures import accept_prepared
from catalog_manifest import synthetic_manifest
from reg_meta_build.extend_db import read_private_holdings_input
from reg_meta_build.holdings_compile import compile_holdings
from reg_meta_build.resolved_catalog import ResolvedVariable, write_resolved_catalog

import reg_meta_build

CASES = Path(__file__).parent / "cases/holdings"


def _copied_builder_checkout(tmp_path: Path) -> tuple[Path, Path]:
    checkout = tmp_path / "checkout"
    package_dir = checkout / "reg_meta_build/src"
    package_dir.mkdir(parents=True)
    assert reg_meta_build.__file__ is not None
    shutil.copytree(
        Path(reg_meta_build.__file__).parent,
        package_dir / "reg_meta_build",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    source_root = Path(reg_meta_build.__file__).resolve().parents[3]
    for name in (
        "scripts/prototype_scb_inputs.py",
        "uv.lock",
    ):
        target = checkout / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_root / name, target)
    ignored = checkout / ".gitignore"
    ignored.write_text("ignored/\n__pycache__/\n")
    accept_prepared(ignored)
    for name in ("reg_meta_build", "scripts", "uv.lock"):
        accept_prepared(checkout / name)
    return checkout, package_dir


@pytest.mark.parametrize(
    "case", json.loads((CASES / "tracked-builder/request.json").read_text())
)
def test_writer_pins_tracked_sources_with_untracked_outputs(
    tmp_path: Path, case: dict
) -> None:
    expected = json.loads((CASES / "tracked-builder/expected.json").read_text())
    checkout, package_dir = _copied_builder_checkout(tmp_path)
    local_config = checkout / ".codex/config.toml"
    local_config.parent.mkdir()
    local_config.write_text("# unrelated local configuration\n")
    source = package_dir / "reg_meta_build/pipeline.py"
    if case["tracked_change"]:
        source.write_bytes(source.read_bytes() + b"\n")
    output = checkout / case["output"]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """import json,sys
from pathlib import Path
from reg_meta_build.resolved_catalog import ResolvedVariable,write_resolved_catalog
manifest=json.loads(Path(sys.argv[3]).read_text())
manifest.pop('builder_commit')
variables=tuple(ResolvedVariable.model_validate_json(json.dumps(x)) for x in json.loads(Path(sys.argv[2]).read_text()))
try:
    write_resolved_catalog(variables,Path(sys.argv[1]),manifest=manifest)
except ValueError as exc:
    print(json.dumps({'error':str(exc)}))
    sys.exit(1)
""",
            str(output),
            str(CASES / "annual-series/catalog.json"),
            str(CASES / "manifest/request.json"),
        ],
        env={**os.environ, "PYTHONPATH": str(package_dir)},
        capture_output=True,
        text=True,
        check=False,
    )
    if case["tracked_change"]:
        assert result.returncode != 0
        assert expected["error_contains"] in json.loads(result.stdout)["error"]
        assert not output.exists()
    else:
        assert result.returncode == 0, result.stderr
        with sqlite3.connect(output) as conn:
            manifest = dict(conn.execute("SELECT key,value FROM import_manifest"))
        assert manifest["catalog_publishable"] == "true"
        assert (
            manifest["builder_commit"]
            == subprocess.check_output(
                ["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True
            ).strip()
        )
        assert (
            subprocess.check_output(
                [
                    "git",
                    "-C",
                    str(checkout),
                    "status",
                    "--porcelain",
                    "--untracked-files=no",
                ],
                text=True,
            )
            == ""
        )
    assert local_config.read_text() == "# unrelated local configuration\n"


@pytest.mark.parametrize(
    "installation",
    json.loads((CASES / "builder-identity/request.json").read_text())["installations"],
)
def test_cli_refuses_unpinned_builder_sources(
    tmp_path: Path, installation: str
) -> None:
    expected = json.loads((CASES / "builder-identity/expected.json").read_text())
    package_dir = tmp_path / "repository/installation"
    package_dir.mkdir(parents=True)
    assert reg_meta_build.__file__ is not None
    copied = package_dir / "reg_meta_build"
    shutil.copytree(
        Path(reg_meta_build.__file__).parent,
        copied,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    if installation == "ignored-wheel-in-unrelated-repo":
        (package_dir / ".gitignore").write_text("reg_meta_build/\n")
        accept_prepared(package_dir)
    elif installation == "dirty-checkout":
        accept_prepared(package_dir)
        source = copied / "artifact_identity.py"
        source.write_bytes(source.read_bytes() + b"\n")
    base = tmp_path / "base.db"
    base.touch()
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "reg_meta_build.cli",
            "--db",
            str(tmp_path / "output"),
            "extend-db",
            "--base-db",
            str(base),
            "--holdings-input",
            str(tmp_path / "absent-candidate"),
            "--input-commit",
            "a" * 40,
            "--input-manifest-sha256",
            "b" * 64,
        ],
        env={**os.environ, "PYTHONPATH": str(package_dir)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    error = json.loads(result.stdout)["error"]
    assert error["code"] == expected["error_code"]
    assert expected["error_contains"] in error["message"]
    assert not (tmp_path / "output").exists()


def test_derive_keeps_inputs_differing_only_in_identity_distinct(
    tmp_path: Path,
) -> None:
    # Derive rewrites schema_version and builder_commit, so two bases that differ
    # only in builder_commit stay distinct through derived_from_generation_id alone.
    # Fails if derived_from_generation_id leaves the generation hash.
    _checkout, package_dir = _copied_builder_checkout(tmp_path)
    variables = tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads((CASES / "annual-series/catalog.json").read_text())
    )
    manifests = []
    for builder in ("0" * 40, "1" * 40):
        base = tmp_path / builder[0] / "base.db"
        base.parent.mkdir()
        write_resolved_catalog(
            variables,
            base,
            manifest={**synthetic_manifest(), "builder_commit": builder},
        )
        out = base.with_name("derived.db")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "reg_meta_build.cli",
                "derive",
                "--base",
                str(base),
                "--out",
                str(out),
            ],
            env={**os.environ, "PYTHONPATH": str(package_dir)},
            capture_output=True,
            check=True,
        )
        for path in (base, out):
            with closing(sqlite3.connect(path)) as conn:
                manifests.append(
                    dict(conn.execute("SELECT key, value FROM import_manifest"))
                )
    base_a, derived_a, base_b, derived_b = manifests
    assert derived_a["derived_from_generation_id"] == base_a["generation_id"]
    assert derived_b["derived_from_generation_id"] == base_b["generation_id"]
    assert derived_a["builder_commit"] == derived_b["builder_commit"]
    assert derived_a["generation_id"] != derived_b["generation_id"]


def test_compilation_consumes_snapshot_after_accepted_source_changes(
    tmp_path: Path,
) -> None:
    case = CASES / "accepted-snapshot"
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    source_case = case / request["source_case"]
    candidate = tmp_path / "acceptance/candidate"
    (candidate / "policy").mkdir(parents=True)
    (candidate / "swecov").mkdir()
    (candidate / "providers").mkdir()
    for source, destination in (
        (source_case / "inventory.toml", "policy/inventory.toml"),
        (source_case / "holdings_policy.toml", "policy/holdings_policy.toml"),
        (source_case / "census.csv", "swecov/SWECOV_variables_full_fixture.csv"),
        (case / "source_policy.toml", "policy/source_policy.toml"),
        (case / "inventory_overlay.toml", "policy/inventory_overlay.toml"),
        (case / "provider.toml", "providers/fixture.toml"),
    ):
        shutil.copyfile(source, candidate / destination)
    files = [
        {
            "path": path.relative_to(candidate).as_posix(),
            "size": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in sorted(candidate.rglob("*"))
        if path.is_file()
    ]
    manifest = candidate / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "artifact_kind": "swecov-private-extension-input-candidate",
                "files": files,
            },
            sort_keys=True,
        )
    )
    commit = accept_prepared(candidate)
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    read_private_holdings_input(
        candidate,
        input_commit=commit,
        input_manifest_sha256=digest,
        materialize_to=snapshot,
    )
    variables = tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads((source_case / "catalog.json").read_text())
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(variables, output, manifest=synthetic_manifest())
    inventory = candidate / "policy/inventory.toml"
    accepted = inventory.read_bytes()
    changed = accepted.replace(
        request["replacement_from"].encode(), request["replacement_to"].encode()
    )
    inventory.write_bytes(changed)
    with sqlite3.connect(output) as conn:
        compile_holdings(conn, snapshot, steward="swecov")
        actual = {
            "physical_ids": [
                row[0]
                for row in conn.execute(
                    "SELECT physical_id FROM holding_table ORDER BY physical_id"
                )
            ],
            "representations": [
                list(row)
                for row in conn.execute(
                    "SELECT representation_literal,representation_canonical FROM holding_mapping ORDER BY column_id"
                )
            ],
        }
    assert actual == expected
    assert inventory.read_bytes() == changed
    inventory.write_bytes(accepted)
    read_private_holdings_input(
        candidate, input_commit=commit, input_manifest_sha256=digest
    )
