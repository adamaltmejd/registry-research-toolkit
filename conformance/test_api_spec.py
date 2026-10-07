"""The API spec (`conformance/api/operations.toml`, `errors.toml`) is consistent.

A lint, not a runner: the operation table and error catalog agree with each other,
every `replaced` route or command row in `surface.toml` names an operation, and every
`api` case parses, names an operation through one of its routes, uses only that
operation's error codes with their status, and references an existing fixture.
"""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
API = ROOT / "conformance/api"
CASES = ROOT / "conformance/cases"
API_CASES = sorted((CASES / "api").iterdir())

OPERATIONS = tomllib.loads((API / "operations.toml").read_text(encoding="utf-8"))
OPS = {op["name"]: op for op in OPERATIONS["operation"]}
ERRORS = {
    e["code"]: e
    for e in tomllib.loads((API / "errors.toml").read_text(encoding="utf-8"))["code"]
}
# Every operation can return these; `errors` lists the rest.
BASE = {"invalid_parameter", "catalog_unavailable", "internal_error"}
STATUSES = {
    "usage": {400, 422},
    "not_found": {404},
    "conflict": {409},
    "order_blocked": {422},
    "unavailable": {503},
    "internal": {500},
    "limit": {413, 429},
}


def _route_pattern(route: str) -> re.Pattern[str]:
    """`{ref}` spans segments (a FQID); any other placeholder is one segment."""
    method, path = route.split(" ", 1)
    parts = re.split(r"(\{\w+\})", path)
    body = "".join(
        ".+" if p == "{ref}" else "[^/]+" if p.startswith("{") else re.escape(p)
        for p in parts
    )
    return re.compile(f"{method} {body}")


def test_operations_are_well_formed():
    assert len(OPS) == len(OPERATIONS["operation"]), "duplicate operation names"
    routes = [r for op in OPS.values() for r in op.get("http", [])]
    routes += [d["route"] for d in OPERATIONS["download"]]
    assert len(routes) == len(set(routes)), "a route is listed twice"
    for name, op in OPS.items():
        assert bool(op.get("process")) != bool(op.get("http")), name
        assert not set(op.get("errors", [])) - set(ERRORS), name
        for route in op.get("http", []):
            assert set(re.findall(r"\{(\w+)\}", route)) <= set(op["params"]), route
    for download in OPERATIONS["download"]:
        assert download["operation"] in OPS, download["route"]


def test_error_codes_map_class_to_status_or_exit():
    for code, error in ERRORS.items():
        if error["surface"] in ("operation", "http"):
            assert error["status"] in STATUSES[error["class"]], code
        else:
            assert error["surface"] in ("startup", "fetch") and "exit" in error, code


def test_replaced_routes_and_commands_name_an_operation():
    rows = tomllib.loads((API / "surface.toml").read_text(encoding="utf-8"))["row"]
    for row in rows:
        label = f"{row['kind']} {row['id']!r}"
        if row["kind"] in ("route", "command") and row["disposition"] == "replaced":
            assert row.get("operation") in OPS, label
        else:
            assert "operation" not in row, label


@pytest.mark.parametrize("case", API_CASES, ids=lambda p: p.name)
def test_api_case_is_well_formed(case):
    request = json.loads((case / "request.json").read_text(encoding="utf-8"))
    expected = json.loads((case / "expected.json").read_text(encoding="utf-8"))
    fixture = request.get("fixture", "compiled")
    source = CASES / (
        "reader/fixture"
        if fixture == "reader"
        else fixture
        if fixture.startswith("reader/")
        else f"fixtures/{fixture}"
    )
    assert source.is_dir(), fixture
    assert set(request.get("artifacts", {})) >= {
        s["artifact"] for s in request["requests"] if "artifact" in s
    }
    if "startup_error" in expected:
        assert ERRORS[expected["startup_error"]["code"]]["surface"] == "startup"
        oracles = [None] * len(request["requests"])
    else:
        assert len(expected) == len(request["requests"])
        oracles = expected
    for step, oracle in zip(request["requests"], oracles, strict=True):
        op = OPS[step["operation"]]
        sent = f"{step.get('method', 'GET')} {step['path']}"
        assert any(_route_pattern(r).fullmatch(sent) for r in op["http"]), (
            f"{sent} is not a route of {step['operation']}"
        )
        code = (oracle or {}).get("json", {}).get("/error/code")
        if code is not None:
            assert code in BASE | set(op.get("errors", [])), code
            assert oracle["status"] == ERRORS[code]["status"], code
