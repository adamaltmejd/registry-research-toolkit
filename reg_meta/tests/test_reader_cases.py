"""CLI JSON and order bytes observed against independently authored case data."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from reader_artifacts import CASES, build_reader_artifact
from reg_meta.catalog import Catalog
from reg_meta.cli import run
from reg_meta.db import open_db
from reg_meta.order import materialize_order, project_from_raw

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "case", sorted((CASES / "order").iterdir()), ids=lambda p: p.name
)
def test_order_manifest_or_located_finding(case: Path, tmp_path: Path, capsys) -> None:
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    conn = open_db(path)
    try:
        result = materialize_order(project_from_raw(request["project"]), conn)
        model = result.manifest or result
        actual = model.model_dump(mode="json", exclude_none=True)
        assert {field: actual[field] for field in request["observe"]} == expected
        if result.manifest is not None:
            project = tmp_path / "project.json"
            project.write_text(json.dumps(request["project"]))
            assert run(["--db", str(path.parent), "order", str(project)]) == 0
            assert capsys.readouterr().out == result.manifest.to_json()
            again = materialize_order(project_from_raw(request["project"]), conn)
            assert again.manifest is not None
            assert again.manifest.to_json() == result.manifest.to_json()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "case", sorted((CASES / "scope").iterdir()), ids=lambda p: p.name
)
def test_scoped_cli_json(case: Path, tmp_path: Path, capsys) -> None:
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    argv = ["--db", str(path.parent), "--format", "json", *request["argv"]]
    code = run(argv)
    output = json.loads(capsys.readouterr().out)
    if request.get("page") == 2:
        assert output["next_cursor"]
        code = run([*argv, "--cursor", output["next_cursor"]])
        output = json.loads(capsys.readouterr().out)
    observe = request["observe"]
    if observe == "error":
        actual = {"exit_code": code, "error": output["error"]}
    else:
        assert code == 0, output
        if observe == "register-variants":
            actual = {
                "fqid": output["fqid"],
                "variants": [v["slug"] for v in output["variants"]],
            }
        elif observe == "schema-columns":
            versions = [
                version
                for variant in output["variants"]
                for version in variant["versions"]
            ]
            actual = {
                "columns": sorted({c["fqid"] for v in versions for c in v["columns"]}),
                "windows": sorted({(v["valid_from"], v["valid_to"]) for v in versions}),
            }
            actual["windows"] = [list(window) for window in actual["windows"]]
        elif observe == "search-page":
            actual = {
                "names": [row["name"] for row in output["results"]],
                "has_more": output["has_more"],
            }
        else:
            actual = {field: output[field] for field in observe}
    assert actual == expected


def test_catalog_listing_and_counts_share_scope(tmp_path: Path) -> None:
    case = CASES / "reader/listing"
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    catalog = Catalog.open(str(path.parent))
    try:
        binding = catalog.resolve_binding(request["delivery_variable"])
        states = catalog.states(request["delivery_variable"])
        actual = {
            "sizes": catalog.catalog_sizes().model_dump(),
            "providers": [str(p.fqid) for p in catalog.list_providers()],
            "registers": [str(r.fqid) for r in catalog.list_registers("scb")],
            "bindings": [str(v.fqid) for v in catalog.list_bindings("scb", "example")],
            "delivery_columns": sorted(
                catalog.delivery_columns(
                    int(binding.variable_id), int(states[0].register_variant_id)
                )
            ),
        }
    finally:
        catalog.close()
    assert actual == expected


def test_cursor_uses_generation_across_connections(tmp_path: Path) -> None:
    from reg_meta.errors import RegMetaError
    from reg_meta.queries import search

    case = CASES / "reader/cursor"
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    paths = [
        build_reader_artifact(
            tmp_path / label,
            request["fixture"],
            request["artifact"],
            identity_overrides=request[f"{label}_identity"],
        )
        for label in ("first", "second", "changed")
    ]
    connections = [open_db(path) for path in paths]
    try:
        first = search(connections[0], **request["search"])
        assert first.next_cursor is not None
        second = search(connections[1], **request["search"], cursor=first.next_cursor)
        with pytest.raises(RegMetaError) as error:
            search(connections[2], **request["search"], cursor=first.next_cursor)
        actual = {
            "first": [row.model_dump()["name"] for row in first.results],
            "second": [row.model_dump()["name"] for row in second.results],
            "changed_generation_error": error.value.code,
        }
        assert actual == expected
    finally:
        for conn in connections:
            conn.close()


def test_holdings_public_probes(tmp_path: Path) -> None:
    from reg_meta.holdings import Holdings

    case = CASES / "reader/holdings-probes"
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    conn = open_db(path)
    try:
        holdings = Holdings(conn)
        ids = holdings.binding_ids(request["variable"], request["variant"])
        assert ids is not None
        actual = {
            "admitted": sorted(holdings.admitted_variable_fqids),
            "registers": sorted(holdings.held_register_fqids),
            "providers": sorted(holdings.held_provider_slugs),
            "columns": sorted(holdings.held_columns(request["variable"])),
            "variant_columns": sorted(
                holdings.held_columns_for_variant(
                    request["variable"], request["variant"]
                )
            ),
            "variants": sorted(
                holdings.held_variant_coords_for_register(request["register"])
            ),
            "matches": [
                {
                    "table": match.table,
                    "column": match.column,
                    "partition": match.partition,
                    "periods": [
                        list(period) for period in holdings.periods(match.table_id)
                    ],
                }
                for match in holdings.matches(
                    *ids, request["representation"], bounds=tuple(request["bounds"])
                )
            ],
        }
        assert actual == expected
    finally:
        conn.close()


@pytest.mark.parametrize(
    "case", sorted((CASES / "inventory").iterdir()), ids=lambda p: p.name
)
def test_inventory_findings(case: Path, tmp_path: Path) -> None:
    from reg_meta.inventory import load_inventory
    from reg_meta.inventory_check import check_inventory

    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    path = build_reader_artifact(
        tmp_path / "artifact", request["fixture"], request["artifact"]
    )
    conn = open_db(path)
    try:
        actual = {
            "findings": [
                finding.model_dump(mode="json")
                for finding in check_inventory(
                    load_inventory(case / "inventory.toml"), conn
                )
            ]
        }
        assert actual == expected
    finally:
        conn.close()
