"""MCP equivalence (RUST_RUNTIME_SPEC.md section 9): the MCP tools answer exactly what
the HTTP operations answer.

Raw JSON-RPC over `httpx2` against `/mcp` on the `--server-cmd` server (stateless
streamable HTTP, JSON responses), and one stdio session against `--mcp-cmd`. Both run
only out of process, like the `api` cases; the oracles are the HTTP responses, which
`test_http.py` pins, the operation table and the `tools/list` golden
(`cases/mcp/tools-list.json`).
"""

from __future__ import annotations

import json
import shlex
import subprocess
import tomllib

import pytest
from http_cases import (
    CASES,
    ServerPool,
    artifact_env,
    cached_case_artifact,
    case_clients,
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
# The `tools/list` result, schemas included: a change to a tool is a reviewed diff here.
TOOLS = json.loads((CASES / "mcp/tools-list.json").read_text())
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


def test_search_tool_matches_http(servers):
    # Fails when a tool call's document drifts from the HTTP body (envelope, meta,
    # error fields, argument conversion), or when the cases stop covering an error.
    codes = set()
    for name in SEARCH_CASES:
        case = CASES / "api" / name
        request = json.loads((case / "request.json").read_text())
        steps, clients = request["requests"], case_clients(request, case, servers)
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


def test_tools_list_matches_golden(servers):
    # Fails when a tool, its description or a schema changes without updating
    # `cases/mcp/tools-list.json`, or a tool name drifts from `operations.toml`.
    client = servers.client(artifact_env(cached_case_artifact(READER), "steward"))
    served = {
        operation["operationId"]
        for item in client.get("/openapi.json").json()["paths"].values()
        for operation in item.values()
    }
    tools = rpc(client, "tools/list", {}).json()["result"]["tools"]
    assert tools == TOOLS
    assert sorted(tool["name"] for tool in tools) == sorted(
        op["tool"]
        for op in OPERATIONS["operation"]
        if op["name"] in served and "tool" in op
    )


def test_body_over_the_cap_is_payload_too_large(servers):
    # Fails when `/mcp` stops capping bodies at 1 MiB (today's write-body cap) or
    # answers the cap with anything but the error document.
    client = servers.client(artifact_env(cached_case_artifact(READER), "steward"))
    response = client.post("/mcp", content=b" " * (1024 * 1024 + 1), headers=HEADERS)
    assert response.status_code == 413
    assert response.json()["error"]["fields"] == {"limit_bytes": 1024 * 1024}


def test_burst_is_rate_limited(request, tmp_path):
    # Fails when `/mcp` stops limiting a client's burst, answers it without the error
    # document, keys a request on a `CF-Connecting-IP` that lacks the edge token (each
    # forged address would get a fresh bucket and the burst would never be refused),
    # or keys an edge request on its peer (every edge client would share one bucket).
    # A server of its own: a drained bucket would refuse the other MCP cases from
    # this address.
    template = request.config.getoption("--server-cmd")
    if template is None:
        pytest.skip("MCP cases run against --server-cmd")
    pool = ServerPool(template, tmp_path)
    env = artifact_env(cached_case_artifact(READER), "steward")
    try:
        client = pool.client(env | {"REG_META_EDGE_TOKEN": "edge-secret"})

        def tools_list(address, token):
            return client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                headers=HEADERS | {"cf-connecting-ip": address, "x-edge-token": token},
            )

        statuses = []
        while 429 not in statuses and len(statuses) < 500:
            forged = f"203.0.113.{len(statuses) % 250}"
            response = tools_list(forged, "not-the-secret")
            statuses.append(response.status_code)
        assert statuses[0] == 200
        assert response.status_code == 429
        assert response.headers["retry-after"] == "1"
        assert response.json()["error"]["fields"] == {"retry_after_seconds": 1}
        assert tools_list("198.51.100.7", "edge-secret").status_code == 200
    finally:
        pool.close()


def test_stdio_session(request, servers):
    # Fails when `reg-meta mcp` cannot initialize, list the golden's tools or answer
    # a call with the document `/api/search` returns.
    template = request.config.getoption("--mcp-cmd")
    if template is None:
        pytest.skip("the stdio session runs against --mcp-cmd")
    env = artifact_env(cached_case_artifact(READER), "steward")
    arguments = {"q": "Value", "type": "variable", "limit": 1}
    expected = servers.client(env).get("/api/search", params=arguments)
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
    argv = [
        word.replace("{db}", env["REG_META_DB"]).replace(
            "{catalog}", env["REG_WEBAPP_STEWARD"]
        )
        for word in shlex.split(template)
    ]
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
    assert tools["tools"] == TOOLS
    assert result["isError"] is False
    assert result["structuredContent"] == expected.json()
