"""Inventory/source cases observed at the built-artifact and located-error boundary."""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from catalog_manifest import synthetic_manifest
from reg_meta_build.artifact_identity import committed_steward_slugs, generation_id
from reg_meta_build.holdings_compile import compile_holdings
from reg_meta_build.resolved_catalog import ResolvedVariable, write_resolved_catalog
from reg_meta_build.validate import validate_built_db

from reg_meta_build.fqid_slugs import populate_variable_slugs

CASES = Path(__file__).parent / "cases/holdings"


@pytest.mark.parametrize(
    "case",
    sorted(path for path in CASES.iterdir() if (path / "catalog.json").exists()),
    ids=lambda path: path.name,
)
def test_inventory_compiles_to_physical_facts(case: Path, tmp_path: Path) -> None:
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    variables = tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads((case / request["catalog"]).read_text())
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(variables, output, manifest=synthetic_manifest())
    candidate = tmp_path / "candidate"
    (candidate / "policy").mkdir(parents=True)
    (candidate / "swecov").mkdir()
    shutil.copyfile(case / request["inventory"], candidate / "policy/inventory.toml")
    shutil.copyfile(
        case / request["holdings_policy"], candidate / "policy/holdings_policy.toml"
    )
    shutil.copyfile(
        case / request["census"], candidate / "swecov/SWECOV_variables_full_fixture.csv"
    )
    if "source_policy" in request:
        shutil.copyfile(
            case / request["source_policy"], candidate / "policy/source_policy.toml"
        )
    else:
        (candidate / "policy/source_policy.toml").write_text(
            "non_catalog_categories = {}\nflavor_registers = []\nroute = []\nflavor = []\nprovider_scope = []\nregister_scope = []\n"
        )
    if "inventory_overlay" in request:
        shutil.copyfile(
            case / request["inventory_overlay"],
            candidate / "policy/inventory_overlay.toml",
        )
    else:
        (candidate / "policy/inventory_overlay.toml").write_text("")
    with sqlite3.connect(output) as conn:
        if "error_contains" in expected:
            with pytest.raises(ValueError) as error:
                compile_holdings(conn, candidate, steward="swecov")
            message = str(error.value)
            assert expected["error_contains"] in message
            for locator in expected.get("error_locators", []):
                assert locator in message
            if "rejected_mappings" in expected:
                assert f"rejected {expected['rejected_mappings']} mappings" in message
            assert conn.execute("SELECT COUNT(*) FROM holding_table").fetchone()[0] == 0
            return
        compiled = compile_holdings(conn, candidate, steward="swecov")
        actual = {
            "warnings": [
                row[0]
                for row in conn.execute(
                    "SELECT json_extract(warning_json, '$.code') FROM data_warning ORDER BY warning_id"
                )
            ],
            "tables": [
                {
                    "physical_id": row["physical_id"],
                    "scope": row["scope"],
                    "edition": json.loads(row["edition_json"])
                    if row["edition_json"] is not None
                    else None,
                    "partition": row["partition"],
                    "retain_unknown_reason": row["retain_unknown_reason"],
                }
                for row in conn.execute(
                    "SELECT physical_id, scope, edition_json, partition, retain_unknown_reason FROM holding_table ORDER BY physical_id"
                )
            ],
            "periods": [
                dict(row)
                for row in conn.execute(
                    "SELECT physical_id, lo, hi FROM holding_period JOIN holding_table USING(table_id) ORDER BY physical_id, lo, hi"
                )
            ],
            "columns": [
                dict(row)
                for row in conn.execute(
                    "SELECT physical_id, name, unmapped_reason FROM holding_column JOIN holding_table USING(table_id) ORDER BY physical_id, name"
                )
            ],
            "mappings": [
                dict(row)
                for row in conn.execute(
                    "SELECT ht.physical_id, hc.name, p.slug||'/'||r.slug||'/'||rv.slug AS register_variant, p.slug||'/'||r.slug||'/'||v.slug AS variable, representation_literal, representation_canonical FROM holding_mapping hm JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) JOIN variable v USING(variable_id) JOIN register r USING(register_id) JOIN provider p USING(provider_id) JOIN register_variant rv ON rv.register_variant_id=hm.variant_id ORDER BY ht.physical_id, hc.name, rv.slug, representation_literal"
                )
            ],
        }
        if request.get("observe_catalog_states"):
            actual["states"] = [
                dict(row)
                for row in conn.execute(
                    "SELECT p.slug||'/'||r.slug||'/'||v.slug AS variable, "
                    "p.slug||'/'||r.slug||'/'||rv.slug AS register_variant, "
                    "delivery_column_name, valid_from, valid_to, pooled "
                    "FROM variable_state JOIN variable v USING(variable_id) "
                    "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
                    "JOIN register_variant rv USING(register_variant_id) "
                    "ORDER BY variable, register_variant, valid_from, delivery_column_name"
                )
            ]
        if request.get("observe_accounting"):
            actual["accounting"] = compiled.accounting.counts
        assert actual == expected
        manifest = dict(conn.execute("SELECT key, value FROM import_manifest"))
        manifest.update(
            catalog_artifact_kind="steward",
            steward="swecov",
            base_db_sha256="d" * 64,
            base_generation_id=manifest["generation_id"],
            holdings_input_commit="e" * 40,
            holdings_manifest_sha256="f" * 64,
            holdings_policy_sha256=compiled.accounting.policy_sha256,
            holdings_accounting_sha256=compiled.accounting.sha256,
            holdings_accounting_counts=json.dumps(
                compiled.accounting.counts, sort_keys=True, separators=(",", ":")
            ),
        )
        manifest["generation_id"] = generation_id(manifest)
        conn.executemany(
            "INSERT OR REPLACE INTO import_manifest(key, value) VALUES (?, ?)",
            sorted(manifest.items()),
        )
    result = validate_built_db(output)
    assert result.passed, result.format_report()


@pytest.mark.parametrize(
    "case", sorted((CASES / "cli").iterdir()), ids=lambda path: path.name
)
def test_publishable_extension_rejects_unpinned_or_skipped_inputs(case: Path) -> None:
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    result = subprocess.run(
        [sys.executable, "-m", "reg_meta_build.cli", *request["args"]],
        capture_output=True,
        text=True,
        check=False,
    )
    if "stderr_contains" in expected:
        assert result.returncode == expected["returncode"]
        assert expected["stderr_contains"] in result.stderr
    else:
        assert result.returncode != 0
        output = json.loads(result.stdout)
        assert output["error"]["code"] == expected["error_code"]


def test_public_artifact_records_canonical_generation(tmp_path: Path) -> None:
    case = CASES / "annual-series"
    variables = tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads((case / "catalog.json").read_text())
    )
    expected = json.loads((CASES / "manifest/expected.json").read_text())
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(variables, output, manifest=synthetic_manifest())
    with sqlite3.connect(output) as conn:
        actual = dict(conn.execute("SELECT key, value FROM import_manifest"))
    assert {key: actual[key] for key in expected} == expected
    result = validate_built_db(output)
    assert result.passed, result.format_report()


def test_slug_authority_rejects_a_different_builder_revision() -> None:
    case = CASES / "slug-authority"
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    with pytest.raises(ValueError) as error:
        committed_steward_slugs(**request)
    assert expected["error_contains"] in str(error.value)


def test_artifact_variable_naming_leaves_pin_inputs_unchanged(tmp_path: Path) -> None:
    case = CASES / "variable-naming"
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    variables = tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads((case / request["catalog"]).read_text())
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(variables, output, manifest=synthetic_manifest())
    pins = tmp_path / "pins"
    pins.mkdir()
    with sqlite3.connect(output) as conn:
        conn.execute("UPDATE variable SET slug=NULL")
        populate_variable_slugs(conn, pins, incremental=True, persist_auto=False)
        actual = {
            "variable_slugs": [
                row[0]
                for row in conn.execute("SELECT slug FROM variable ORDER BY slug")
            ],
            "generated_slug_files": sorted(path.name for path in pins.iterdir()),
        }
    assert actual == expected
    result = validate_built_db(output)
    assert result.passed, result.format_report()


@pytest.mark.parametrize(
    "mode", json.loads((CASES / "partial-identity/request.json").read_text())["modes"]
)
def test_partial_artifact_does_not_claim_a_publishable_generation(
    tmp_path: Path, mode: dict[str, bool]
) -> None:
    case = CASES / "partial-identity"
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    variables = tuple(
        ResolvedVariable.model_validate_json(json.dumps(value))
        for value in json.loads((case / request["catalog"]).read_text())
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(variables, output, manifest=synthetic_manifest(), **mode)
    with sqlite3.connect(output) as conn:
        manifest = dict(conn.execute("SELECT key,value FROM import_manifest"))
    assert set(expected["absent_fields"]).isdisjoint(manifest)
    assert {
        key: manifest[key] for key in ("catalog_publishable", "catalog_completeness")
    } == {key: expected[key] for key in ("catalog_publishable", "catalog_completeness")}
    result = validate_built_db(output)
    assert result.passed, result.format_report()
