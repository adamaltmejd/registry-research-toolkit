"""Builder provenance and strict accepted-input extension through the CLI and
immutable artifacts.

Every builder-identity leg runs the CLI as a subprocess from a committed copy of the
builder package (`copied_builder_checkout`), because identity resolves from the
running module's own checkout; no case runner reaches that layout.
"""

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
from _input_repo_fixtures import commit_fixture, init_fixture_repo
from _prepared_fixtures import accept_prepared
from _snapshot_fixtures import copied_builder_checkout, git
from catalog_manifest import synthetic_manifest
from reg_meta_build.errors import EXIT_CONFIG
from reg_meta_build.extend_db import CANDIDATE_KIND, read_private_holdings_input
from reg_meta_build.holdings_compile import compile_holdings
from reg_meta_build.materialize import write_resolved_catalog
from reg_meta_build.resolved_catalog import ResolvedVariable
from reg_meta_build.steward_spec import (
    STEWARDS_DIR,
    load_steward_spec,
    steward_spec_sha256,
)

import reg_meta_build

HOLDINGS = Path(__file__).parent / "cases/holdings"
CASE = HOLDINGS / "steward-boundary"
REQUEST = json.loads((CASE / "request.json").read_text())
EXPECTED = json.loads((CASE / "expected.json").read_text())
IDENTITY = HOLDINGS / "builder-identity"
SNAPSHOT = HOLDINGS / "accepted-snapshot"


def _variables(catalog: Path) -> tuple[ResolvedVariable, ...]:
    return tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads(catalog.read_text())
    )


def _manifest(path: Path) -> dict[str, str]:
    with sqlite3.connect(path) as conn:
        return dict(conn.execute("SELECT key,value FROM import_manifest"))


def _builder_checkout(directory: Path) -> tuple[Path, str]:
    """A committed builder copy holding the fixture's steward slugs and spec
    (`scope.toml` only, so the real series never meet the fixture census)."""
    checkout = copied_builder_checkout(directory)
    slugs = checkout / "reg_meta_build/fqid_slugs" / REQUEST["steward"]
    slugs.mkdir(parents=True)
    shutil.copyfile(CASE / "slugs.toml", slugs / "fixture.toml")
    spec = checkout / STEWARDS_DIR / REQUEST["steward"]
    spec.mkdir(parents=True)
    shutil.copyfile(CASE / "scope.toml", spec / "scope.toml")
    git(checkout, "add", "--", "reg_meta_build/fqid_slugs", STEWARDS_DIR)
    git(checkout, "commit", "-q", "-m", "Pin steward slugs and spec")
    return checkout / "reg_meta_build/src", git(checkout, "rev-parse", "HEAD")


def _accept(
    root: Path,
    files: dict[str, Path],
    *,
    spec_sha256: str,
    global_sha256: str,
    storage_export: str = REQUEST["storage_export_name"],
) -> tuple[Path, str, str]:
    """Commit `files` (candidate path -> source) under `root` with its manifest:
    the `census` role is REQUEST's name, the `storage_export` role REQUEST's
    unless overridden, the inventory its fixed path."""
    for destination, source in files.items():
        (root / destination).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, root / destination)
    manifest = root / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "artifact_kind": CANDIDATE_KIND,
                "roles": {
                    "census": REQUEST["census_name"],
                    "storage_export": storage_export,
                    "inventory": "policy/inventory.toml",
                },
                "steward_spec_sha256": spec_sha256,
                "global_db_sha256": global_sha256,
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


def _accepted_candidate(
    directory: Path,
    base: Path,
    *,
    inventory: str = "inventory.toml",
    spec_sha256: str = steward_spec_sha256(CASE),
    storage_export: str = REQUEST["storage_export_name"],
):
    """The fixture candidate, pinned to `base` and (by default) the committed spec."""
    return _accept(
        directory / "candidate",
        {
            "policy/inventory.toml": CASE / inventory,
            "providers/fixture.toml": CASE / "provider.toml",
            REQUEST["census_name"]: CASE / "census.csv",
            REQUEST["storage_export_name"]: CASE / "storage_types.csv",
        },
        spec_sha256=spec_sha256,
        global_sha256=hashlib.sha256(base.read_bytes()).hexdigest(),
        storage_export=storage_export,
    )


def _cli(source: Path, *args: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "reg_meta_build.cli", *(str(arg) for arg in args)],
        env={**os.environ, "PYTHONPATH": str(source)},
        capture_output=True,
        text=True,
        check=False,
    )


def _extend(source: Path, base: Path, candidate: tuple, output: Path):
    root, revision, digest = candidate
    return _cli(
        source,
        "--db",
        output,
        "extend-db",
        "--base-db",
        base,
        "--holdings-input",
        root,
        "--input-commit",
        revision,
        "--input-manifest-sha256",
        digest,
        "--steward",
        REQUEST["steward"],
    )


@pytest.fixture(scope="module")
def strict_builds(tmp_path_factory: pytest.TempPathFactory) -> dict:
    directory = tmp_path_factory.mktemp("strict-holdings")
    source, builder_revision = _builder_checkout(directory)
    # The candidate shares its acceptance repository with an unrelated committed
    # folder, as in the private inputs repository (catalog/ beside swecov/). Fails if
    # the completeness check lists the whole repository instead of the candidate's
    # folder.
    accepted = directory / "accepted"
    (accepted / "catalog").mkdir(parents=True)
    (accepted / "catalog/README.md").write_text("Prepared public sources.\n")
    init_fixture_repo(
        accepted, name="Prepared fixture", email="prepared@example.invalid"
    )
    commit_fixture(accepted, "Add an unrelated folder", "catalog")
    base = directory / "base/reg_meta.db"
    write_resolved_catalog(
        _variables(CASE / "base_catalog.json"), base, manifest=synthetic_manifest()
    )
    candidate = _accepted_candidate(accepted, base)
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
        directory / "unaccounted",
        strict_builds["base"],
        inventory=REQUEST["unaccounted_inventory"],
    )
    output = directory / "rejected-output"
    result = _extend(strict_builds["source"], strict_builds["base"], candidate, output)
    assert result.returncode != 0
    assert (
        json.loads(result.stdout)["error"]["message"] == EXPECTED["unaccounted_error"]
    )
    assert not output.exists()


def test_strict_extension_refuses_a_candidate_generated_from_another_spec(
    strict_builds: dict, tmp_path: Path
) -> None:
    # The candidate pins a spec that differs from the builder revision's by one
    # meaning-free comment line; everything else is the published candidate's
    # twin. Fails if extend-db compiles with a spec other than the one the
    # inventory was generated from, compares parsed content instead of bytes, or
    # reads the spec from anywhere but the builder revision.
    edited = tmp_path / "spec"
    edited.mkdir()
    (edited / "scope.toml").write_bytes(
        (CASE / "scope.toml").read_bytes() + b"# reviewed again\n"
    )
    directory = strict_builds["directory"]
    candidate = _accepted_candidate(
        directory / "other-spec",
        strict_builds["base"],
        spec_sha256=steward_spec_sha256(edited),
    )
    output = directory / "other-spec-output"
    result = _extend(strict_builds["source"], strict_builds["base"], candidate, output)
    error = json.loads(result.stdout)["error"]
    assert (result.returncode, error["code"]) == (
        EXIT_CONFIG,
        EXPECTED["spec_mismatch_code"],
    )
    assert not output.exists()


def test_strict_extension_refuses_a_base_the_candidate_was_not_generated_against(
    strict_builds: dict,
) -> None:
    # The candidate's inventory was generated against another global: the same
    # catalog stamped with another builder commit, so only its bytes differ.
    # Fails if extend-db compiles an inventory auto-mapped against one catalog
    # onto another, or checks the base only after copying it.
    directory = strict_builds["directory"]
    other = directory / "other-base/reg_meta.db"
    write_resolved_catalog(
        _variables(CASE / "base_catalog.json"),
        other,
        manifest={**synthetic_manifest(), "builder_commit": "1" * 40},
    )
    candidate = _accepted_candidate(directory / "other-base-candidate", other)
    output = directory / "other-base-output"
    result = _extend(strict_builds["source"], strict_builds["base"], candidate, output)
    error = json.loads(result.stdout)["error"]
    assert (result.returncode, error["code"]) == (
        EXIT_CONFIG,
        EXPECTED["base_mismatch_code"],
    )
    assert not output.exists()


def test_strict_extension_refuses_a_storage_export_role_naming_the_census(
    strict_builds: dict,
) -> None:
    # The published candidate's twin whose storage_export role names the census
    # member. Fails if extend-db admits a role that only has to name some member,
    # so the storage export the generator read is never pinned.
    directory = strict_builds["directory"]
    candidate = _accepted_candidate(
        directory / "census-as-storage",
        strict_builds["base"],
        storage_export=REQUEST["census_name"],
    )
    output = directory / "census-as-storage-output"
    result = _extend(strict_builds["source"], strict_builds["base"], candidate, output)
    error = json.loads(result.stdout)["error"]
    expected = EXPECTED["role_error"]
    assert (result.returncode, error["code"]) == (EXIT_CONFIG, expected["code"])
    assert expected["message_contains"] in error["message"]
    assert not output.exists()


def test_strict_extension_refuses_a_candidate_drifted_from_its_accepted_commit(
    strict_builds: dict,
) -> None:
    # A meaning-free byte edit to an accepted file, made after acceptance, with the
    # commit and manifest pins unchanged. The refusal names the drifted file
    # repo-relative and never the checkout's absolute path. Fails if extend-db
    # compiles the candidate's working tree, or compares parsed content, instead of
    # requiring the accepted commit's bytes, or if the refusal prints the
    # host-specific checkout path.
    directory = strict_builds["directory"]
    candidate = _accepted_candidate(directory / "drifted", strict_builds["base"])
    inventory = candidate[0] / "policy/inventory.toml"
    inventory.write_bytes(inventory.read_bytes() + b"\n")
    output = directory / "drifted-output"
    result = _extend(strict_builds["source"], strict_builds["base"], candidate, output)
    error = json.loads(result.stdout)["error"]
    expected = json.loads((SNAPSHOT / "expected.json").read_text())["drift_error"]
    assert (result.returncode, error["code"]) == (EXIT_CONFIG, expected["code"])
    assert expected["message_contains"] in error["message"]
    assert str(directory.resolve()) not in error["message"]
    assert str(directory) not in error["message"]
    assert not output.exists()


def test_compiling_a_materialized_snapshot_ignores_later_candidate_edits(
    tmp_path: Path,
) -> None:
    # extend-db materializes the accepted bytes and compiles that copy within one
    # call, so no CLI run can edit the candidate between the two steps; this drives
    # the two steps directly. Its boundary twin is the drift refusal above.
    # Fails if materialization links or re-reads the candidate instead of writing
    # the committed bytes into the build-owned snapshot.
    request = json.loads((SNAPSHOT / "request.json").read_text())
    expected = json.loads((SNAPSHOT / "expected.json").read_text())
    source_case = SNAPSHOT / request["source_case"]
    spec_dir = tmp_path / STEWARDS_DIR / REQUEST["steward"]
    spec_dir.mkdir(parents=True)
    shutil.copyfile(source_case / "scope.toml", spec_dir / "scope.toml")
    candidate, commit, digest = _accept(
        tmp_path / "acceptance/candidate",
        {
            "policy/inventory.toml": source_case / "inventory.toml",
            REQUEST["census_name"]: source_case / "census.csv",
            REQUEST["storage_export_name"]: CASE / "storage_types.csv",
            "providers/fixture.toml": SNAPSHOT / "provider.toml",
        },
        spec_sha256=steward_spec_sha256(spec_dir),
        global_sha256="0" * 64,
    )
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    read_private_holdings_input(
        candidate,
        input_commit=commit,
        input_manifest_sha256=digest,
        materialize_to=snapshot,
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        _variables(source_case / "catalog.json"), output, manifest=synthetic_manifest()
    )
    inventory = candidate / "policy/inventory.toml"
    accepted = inventory.read_bytes()
    changed = accepted.replace(
        request["replacement_from"].encode(), request["replacement_to"].encode()
    )
    inventory.write_bytes(changed)
    with sqlite3.connect(output) as conn:
        compile_holdings(
            conn, snapshot, steward="swecov", spec=load_steward_spec(spec_dir)
        )
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
    assert actual == {key: expected[key] for key in actual}
    assert inventory.read_bytes() == changed
    inventory.write_bytes(accepted)
    read_private_holdings_input(
        candidate, input_commit=commit, input_manifest_sha256=digest
    )


def test_derive_stamps_the_checkout_commit_and_keeps_identity_only_inputs_distinct(
    tmp_path: Path,
) -> None:
    # The bases, their derived copies and a local config file are untracked files
    # inside the builder checkout; the bases' generation ids differ
    # only through builder_commit.
    # Fails if builder identity counts untracked files as a dirty checkout or
    # cleans them away, if derive stamps any commit but the checkout's HEAD, or if
    # derived_from_generation_id leaves the generation hash (the two derived copies
    # would then share one generation_id). The dirty-tracked refusal is
    # test_cli_refuses_unpinned_builder_sources[dirty-checkout] (same guard).
    checkout = copied_builder_checkout(tmp_path)
    local_config = checkout / ".codex/config.toml"
    local_config.parent.mkdir()
    local_config.write_text("# unrelated local configuration\n")
    variables = _variables(HOLDINGS / "annual-series/catalog.json")
    manifests = []
    for builder in ("0" * 40, "1" * 40):
        base = checkout / builder[0] / "base.db"
        write_resolved_catalog(
            variables,
            base,
            manifest={**synthetic_manifest(), "builder_commit": builder},
        )
        derived = base.with_name("reg_meta.db")
        result = _cli(
            checkout / "reg_meta_build/src", "derive", "--base", base, "--out", derived
        )
        assert result.returncode == 0, result.stdout + result.stderr
        manifests += [_manifest(base), _manifest(derived)]
    base_a, derived_a, base_b, derived_b = manifests
    head = git(checkout, "rev-parse", "HEAD")
    assert derived_a["builder_commit"] == derived_b["builder_commit"] == head
    assert derived_a["derived_from_generation_id"] == base_a["generation_id"]
    assert derived_b["derived_from_generation_id"] == base_b["generation_id"]
    assert derived_a["generation_id"] != derived_b["generation_id"]
    assert git(checkout, "status", "--porcelain", "--untracked-files=no") == ""
    assert local_config.read_text() == "# unrelated local configuration\n"


@pytest.mark.parametrize(
    "installation", json.loads((IDENTITY / "request.json").read_text())["installations"]
)
def test_cli_refuses_unpinned_builder_sources(
    tmp_path: Path, installation: str
) -> None:
    # The base is an empty file and the candidate absent, so the builder guard must
    # refuse before any input is read. Each installation keeps every other identity
    # source tracked, so its own reason is the only one left. Fails if extend-db
    # accepts a builder package that is dirty, untracked (an ignored copy inside a
    # checkout) or outside any checkout, or reads its inputs first.
    expected = json.loads((IDENTITY / "expected.json").read_text())
    if installation == "outside-checkout":
        source = tmp_path / "installation"
        assert reg_meta_build.__file__ is not None
        shutil.copytree(
            Path(reg_meta_build.__file__).parent,
            source / "reg_meta_build",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
    else:
        checkout = copied_builder_checkout(tmp_path)
        source = checkout / "reg_meta_build/src"
        if installation == "dirty-checkout":
            tracked = source / "reg_meta_build/artifact_identity.py"
            tracked.write_bytes(tracked.read_bytes() + b"\n")
        else:
            # `ignored/` is gitignored by the copied checkout.
            shutil.copytree(
                source / "reg_meta_build", checkout / "ignored/reg_meta_build"
            )
            source = checkout / "ignored"
    base = tmp_path / "base.db"
    base.touch()
    output = tmp_path / "output"
    result = _cli(
        source,
        "--db",
        output,
        "extend-db",
        "--base-db",
        base,
        "--holdings-input",
        tmp_path / "absent-candidate",
        "--input-commit",
        "a" * 40,
        "--input-manifest-sha256",
        "b" * 64,
        "--steward",
        REQUEST["steward"],
    )
    error = json.loads(result.stdout)["error"]
    assert (result.returncode, error["code"]) == (EXIT_CONFIG, expected["error_code"])
    assert expected["error_contains"] in error["message"]
    assert expected["reasons"][installation] in error["message"]
    assert not output.exists()
