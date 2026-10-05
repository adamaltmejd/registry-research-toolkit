"""CLI JSON and order bytes observed against independently authored case data."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from reader_artifacts import CASES, build_reader_artifact
from reg_meta.catalog import Catalog
from reg_meta.db import open_db

if TYPE_CHECKING:
    from pathlib import Path


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
