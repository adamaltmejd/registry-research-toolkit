"""MCP equivalence (RUST_RUNTIME_SPEC.md section 9): the MCP tools answer exactly what
the HTTP operations answer.

Raw JSON-RPC over `httpx2` against `/mcp` on the `--server-cmd` server (stateless
streamable HTTP, JSON responses), and one stdio session against `--mcp-cmd`. Both run
only out of process, like the `api` cases; the oracles are the HTTP responses, which
`test_http.py` pins, and the operation table.
"""

from __future__ import annotations

import json
import subprocess
import tomllib

import pytest
from http_cases import (
    CASES,
    ServerPool,
    artifact_env,
    cached_case_artifact,
    run_http_requests,
    select_json,
)

PROTOCOL = "2025-11-25"
HEADERS = {
    "accept": "application/json, text/event-stream",
    "mcp-protocol-version": PROTOCOL,
}
# `search` cases of the `api` corpus whose steps cover a success and every domain
# error `search` lists in `operations.toml`. Every `/mcp` request in this module on a
# worker's shared server draws on one 127.0.0.1 rate bucket (60 a minute), so keep
# their total well under it.
SEARCH_CASES = (
    "meta",
    "invalid-parameters",
    "search-register",
    "search-period",
    "scope-unavailable",
    "cursor-invalid",
    "cursor-stale",
)
READER = {"fixture": "reader"}
OPERATIONS = tomllib.loads((CASES.parent / "api/operations.toml").read_text())


@pytest.fixture
def servers(http_servers):
    if http_servers is None:
        pytest.skip("MCP cases run against --server-cmd")
    return http_servers


def rpc(client, method, params):
    """One JSON-RPC request on `/mcp`; returns the HTTP response."""
    return client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        headers=HEADERS,
    )


def call(client, arguments):
    """`tools/call search` as (is_error, structuredContent)."""
    result = rpc(client, "tools/call", {"name": "search", "arguments": arguments})
    result = result.json()["result"]
    return result["isError"], result["structuredContent"]


def case_clients(case, servers):
    request = json.loads((case / "request.json").read_text())
    kind = request.get("kind", "steward")
    clients = {
        name: servers.client(
            artifact_env(cached_case_artifact(request, case, spec["identity"]), kind)
        )
        for name, spec in request.get("artifacts", {}).items()
    }
    clients[None] = servers.client(
        artifact_env(cached_case_artifact(request, case), kind)
    )
    return request["requests"], clients


def test_search_tool_matches_http(servers):
    # Fails when a tool call's document drifts from the HTTP body (envelope, meta,
    # error fields, argument conversion), or when the cases stop covering an error.
    codes = set()
    for name in SEARCH_CASES:
        steps, clients = case_clients(CASES / "api" / name, servers)
        http = run_http_requests(steps, clients)
        documents = []
        for index, step in enumerate(steps):
            arguments = dict(step.get("query", {}))
            if "cursor_from" in step:
                source, pointer = step["cursor_from"]
                arguments["cursor"] = select_json(documents[source], pointer)
            is_error, document = call(clients[step.get("artifact")], arguments)
            documents.append(document)
            assert (is_error, document) == (
                http[index]["status"] != 200,
                http[index]["body"],
            ), f"{name} step {index}"
            if is_error:
                codes.add(document["error"]["code"])
    (search,) = (op for op in OPERATIONS["operation"] if op["name"] == "search")
    assert codes == {"invalid_parameter", *search["errors"]}


def inline(schema, defs, prefix):
    """`schema` with every `$ref` under `prefix` replaced by its definition."""
    if isinstance(schema, list):
        return [inline(item, defs, prefix) for item in schema]
    if not isinstance(schema, dict):
        return schema
    if "$ref" in schema:
        return inline(defs[schema["$ref"].removeprefix(prefix)], defs, prefix)
    return {key: inline(value, defs, prefix) for key, value in schema.items()}


def test_tools_list_matches_operations_and_openapi(servers):
    # Fails when a tool is missing, extra or renamed against `operations.toml`, or
    # its input or output schema disagrees with the operation's OpenAPI entry.
    client = servers.client(artifact_env(cached_case_artifact(READER), "steward"))
    openapi = client.get("/openapi.json").json()
    components = openapi["components"]["schemas"]
    served = {
        operation["operationId"]: operation
        for item in openapi["paths"].values()
        for operation in item.values()
    }
    expected = {
        op["tool"]: served[op["name"]]
        for op in OPERATIONS["operation"]
        if op["name"] in served and "tool" in op
    }
    tools = rpc(client, "tools/list", {}).json()["result"]["tools"]
    assert sorted(tool["name"] for tool in tools) == sorted(expected)
    for tool in tools:
        operation = expected[tool["name"]]
        defs = tool["inputSchema"].get("$defs", {})
        params = {
            p["name"]: inline(p["schema"], components, "#/components/schemas/")
            for p in operation["parameters"]
        }
        properties = inline(tool["inputSchema"]["properties"], defs, "#/$defs/")
        assert properties == params, tool["name"]
        assert sorted(tool["inputSchema"]["required"]) == sorted(
            p["name"] for p in operation["parameters"] if p["required"]
        )
        responses = [
            operation["responses"][status]["content"]["application/json"]["schema"]
            for status in ("200", "default")
        ]
        output = tool["outputSchema"]
        assert inline(output["oneOf"], output.get("$defs", {}), "#/$defs/") == inline(
            responses, components, "#/components/schemas/"
        ), tool["name"]


def test_body_over_the_cap_is_payload_too_large(servers):
    # Fails when `/mcp` stops capping bodies at 1 MiB (today's write-body cap) or
    # answers the cap with anything but the error document.
    client = servers.client(artifact_env(cached_case_artifact(READER), "steward"))
    response = client.post("/mcp", content=b" " * (1024 * 1024 + 1), headers=HEADERS)
    assert response.status_code == 413
    assert response.json()["error"]["fields"] == {"limit_bytes": 1024 * 1024}


def test_burst_is_rate_limited(request, tmp_path):
    # Fails when `/mcp` stops limiting a client's burst, or answers it without the
    # error document. A server of its own: a drained bucket would refuse the other
    # MCP cases from this address.
    template = request.config.getoption("--server-cmd")
    if template is None:
        pytest.skip("MCP cases run against --server-cmd")
    pool = ServerPool(template, tmp_path)
    try:
        client = pool.client(artifact_env(cached_case_artifact(READER), "steward"))
        statuses = []
        while 429 not in statuses and len(statuses) < 500:
            response = rpc(client, "tools/list", {})
            statuses.append(response.status_code)
        assert statuses[0] == 200
        assert response.status_code == 429
        assert response.headers["retry-after"] == "1"
        assert response.json()["error"]["fields"] == {"retry_after_seconds": 1}
    finally:
        pool.close()


def test_stdio_session(request, servers):
    # Fails when `reg-meta mcp` cannot initialize, list its tools or answer a call
    # with the document `/api/search` returns.
    template = request.config.getoption("--mcp-cmd")
    if template is None:
        pytest.skip("the stdio session runs against --mcp-cmd")
    path = cached_case_artifact(READER)
    arguments = {"q": "Value", "type": "variable", "limit": 1}
    expected = servers.client(artifact_env(path, "steward")).get(
        "/api/search", params=arguments
    )
    messages = [
        {
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "conformance", "version": "0"},
            },
        },
        {"method": "notifications/initialized"},
        {"method": "tools/list"},
        {"method": "tools/call", "params": {"name": "search", "arguments": arguments}},
    ]
    # `ServerPool.argv` substitutes `{db}` and `{catalog}`; stdio takes no port.
    argv = ServerPool(template, None).argv(path.parent, "swecov", 0)
    with subprocess.Popen(
        argv,
        cwd=CASES.parents[1],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    ) as process:
        replies = []
        for number, message in enumerate(messages):
            if not message["method"].startswith("notifications/"):
                message = {"id": number} | message
            process.stdin.write(json.dumps({"jsonrpc": "2.0"} | message) + "\n")
            process.stdin.flush()
            if "id" in message:
                replies.append(json.loads(process.stdout.readline()))
        process.stdin.close()
        assert process.wait(timeout=10) == 0
    initialize, tools, result = (reply["result"] for reply in replies)
    assert initialize["serverInfo"]["name"] == "reg-meta"
    assert [tool["name"] for tool in tools["tools"]] == ["search"]
    assert result["isError"] is False
    assert result["structuredContent"] == expected.json()
