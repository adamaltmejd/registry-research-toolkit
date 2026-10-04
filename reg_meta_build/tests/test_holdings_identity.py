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
    ignored = checkout / ".gitignore"
    ignored.write_text("ignored/\n*.db\n__pycache__/\n")
    accept_prepared(ignored)
    accept_prepared(checkout / "reg_meta_build")
    return checkout, package_dir


@pytest.mark.parametrize(
    "case", json.loads((CASES / "output-preflight/request.json").read_text())
)
def test_cli_checks_output_directories_before_consuming_inputs(
    tmp_path: Path, case: dict
) -> None:
    expected = json.loads((CASES / "output-preflight/expected.json").read_text())
    checkout, package_dir = _copied_builder_checkout(tmp_path)
    destinations = {
        "db": tmp_path / "output",
        "report": tmp_path / "report",
        "decisions": tmp_path / "decisions",
    }
    destination = (
        checkout / "ignored/output"
        if case["layout"] in {"ignored", "tracked-in-ignored"}
        else checkout
        if case["layout"] == "db-file-ignore-only"
        else tmp_path / "selected-output"
        if case["layout"] == "outside"
        else checkout / "output"
    )
    marker = destination / "marker.txt"
    if case["layout"] == "tracked-in-ignored":
        destination.mkdir(parents=True)
        marker.write_text("tracked bytes must survive\n")
        subprocess.run(
            ["git", "-C", str(checkout), "add", "-f", "--", str(marker)],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(checkout), "commit", "-q", "-m", "Track output marker"],
            check=True,
        )
    if case["layout"] == "symlink-into-checkout":
        destination.mkdir()
        alias = tmp_path / "output-alias"
        alias.symlink_to(destination, target_is_directory=True)
        destination = alias
    destinations[case["destination"]] = destination
    base = tmp_path / "base.db"
    base.touch()
    curation = tmp_path / "curation"
    curation.mkdir()
    arguments = ["--db", str(destinations["db"]), case["command"]]
    if case["command"] == "build-db":
        arguments += [
            "--prepared",
            str(tmp_path / "absent-input"),
            "--report-dir",
            str(destinations["report"]),
            "--curation-dir",
            str(curation),
            "--dump-decisions",
            str(destinations["decisions"]),
        ]
    else:
        arguments += [
            "--base-db",
            str(base),
            "--holdings-input",
            str(tmp_path / "absent-input"),
        ]
    arguments += ["--input-commit", "a" * 40, "--input-manifest-sha256", "b" * 64]
    result = subprocess.run(
        [sys.executable, "-m", "reg_meta_build.cli", *arguments],
        env={**os.environ, "PYTHONPATH": str(package_dir)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    error = json.loads(result.stdout)["error"]
    assert error["code"] == expected["error_codes"][case["command"]]
    message = expected["accepted_contains" if case["accepted"] else "rejected_contains"]
    assert message in error["message"]
    assert not (destinations["db"] / "reg_meta.db").exists()
    assert not (destinations["report"] / "summary.json").exists()
    assert not (destinations["decisions"] / "global.json").exists()
    if marker.exists():
        assert marker.read_text() == "tracked bytes must survive\n"
    assert (
        subprocess.check_output(
            [
                "git",
                "-C",
                str(checkout),
                "status",
                "--porcelain",
                "--untracked-files=all",
            ],
            text=True,
        )
        == ""
    )


@pytest.mark.parametrize(
    "case", json.loads((CASES / "output-preflight/writer-request.json").read_text())
)
def test_writer_preserves_clean_checkout_with_its_output_footprint(
    tmp_path: Path, case: dict
) -> None:
    expected = json.loads((CASES / "output-preflight/expected.json").read_text())
    checkout, package_dir = _copied_builder_checkout(tmp_path)
    directory = (
        checkout / "ignored/output"
        if case["layout"].startswith("ignored")
        else tmp_path / "output"
        if case["layout"] == "outside"
        else checkout
    )
    output = directory / "reg_meta.db"
    marker = output.with_suffix(".db.prev")
    if case["layout"] == "ignored-tracked-backup-symlink":
        directory.mkdir(parents=True)
        original = tmp_path / "backup-target"
        original.write_text("preserve backup target\n")
        marker.symlink_to(original)
        subprocess.run(
            ["git", "-C", str(checkout), "add", "-f", "--", str(marker)],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(checkout), "commit", "-q", "-m", "Track backup link"],
            check=True,
        )
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
    if case["accepted"]:
        assert result.returncode == 0, result.stderr
        assert output.is_file()
        with sqlite3.connect(output) as conn:
            manifest = dict(conn.execute("SELECT key,value FROM import_manifest"))
        assert manifest["catalog_publishable"] == "true"
        assert (
            manifest["builder_commit"]
            == subprocess.check_output(
                ["git", "-C", str(checkout), "rev-parse", "HEAD"],
                text=True,
            ).strip()
        )
    else:
        assert result.returncode != 0
        assert expected["rejected_contains"] in json.loads(result.stdout)["error"]
        assert not output.exists()
    if marker.is_symlink():
        assert marker.read_text() == "preserve backup target\n"
    assert (
        subprocess.check_output(
            [
                "git",
                "-C",
                str(checkout),
                "status",
                "--porcelain",
                "--untracked-files=all",
            ],
            text=True,
        )
        == ""
    )


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
