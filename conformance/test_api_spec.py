"""A corpus lint for the API spec (`conformance/api/operations.toml`, `errors.toml`).

Not a runner: the `api` cases run for real once a server exists. This checks that every
`api` case parses, names operations in the table, uses only catalogued error codes and
references an existing fixture, and that every `replaced` route or command row in
`surface.toml` names an operation.
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
API = ROOT / "conformance/api"
CASES = ROOT / "conformance/cases"

OPS = {
    op["name"]: op
    for op in tomllib.loads((API / "operations.toml").read_text(encoding="utf-8"))[
        "operation"
    ]
}
CODES = {
    e["code"]
    for e in tomllib.loads((API / "errors.toml").read_text(encoding="utf-8"))["code"]
}


def test_operations_name_catalogued_codes():
    unknown = {n: set(op.get("errors", [])) - CODES for n, op in OPS.items()}
    assert not any(unknown.values()), unknown


def test_replaced_routes_and_commands_name_an_operation():
    rows = tomllib.loads((API / "surface.toml").read_text(encoding="utf-8"))["row"]
    for row in rows:
        if row["kind"] in ("route", "command") and row["disposition"] == "replaced":
            assert row.get("operation") in OPS, f"{row['kind']} {row['id']!r}"


@pytest.mark.parametrize(
    "case", sorted((CASES / "api").iterdir()), ids=lambda p: p.name
)
def test_api_case_is_well_formed(case):
    request = json.loads((case / "request.json").read_text(encoding="utf-8"))
    expected = json.loads((case / "expected.json").read_text(encoding="utf-8"))
    fixture = request.get("fixture", "compiled")
    source = (
        "reader/fixture"
        if fixture == "reader"
        else fixture
        if fixture.startswith("reader/")
        else f"fixtures/{fixture}"
    )
    assert (CASES / source).is_dir(), fixture
    assert all(step["operation"] in OPS for step in request["requests"])
    oracles = [expected] if isinstance(expected, dict) else expected
    codes = {
        o.get("startup_error", {}).get("code") or o.get("json", {}).get("/error/code")
        for o in oracles
    } - {None}
    assert codes <= CODES, codes - CODES
