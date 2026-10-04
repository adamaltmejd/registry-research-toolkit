"""Builder provenance and accepted snapshots at CLI and built-artifact boundaries."""

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
from reg_meta_build.extend_db import read_private_holdings_input
from reg_meta_build.holdings_compile import compile_holdings
from reg_meta_build.resolved_catalog import ResolvedVariable, write_resolved_catalog

import reg_meta_build

CASES = Path(__file__).parent / "cases/holdings"


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


def test_compilation_consumes_accepted_bytes_despite_transient_source_edits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
    read_text = Path.read_text

    def interleaved_read(path: Path, *args, **kwargs):
        if path.name != "inventory.toml":
            return read_text(path, *args, **kwargs)
        inventory.write_bytes(changed)
        try:
            return read_text(path, *args, **kwargs)
        finally:
            inventory.write_bytes(accepted)

    monkeypatch.setattr(Path, "read_text", interleaved_read)
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
    assert inventory.read_bytes() == accepted
    read_private_holdings_input(
        candidate, input_commit=commit, input_manifest_sha256=digest
    )
