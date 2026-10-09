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
import re
import shlex
import subprocess
import tomllib
from urllib.parse import unquote

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
# Per operation, the `api` cases whose steps of that operation cover a success and
# every domain error it lists in `operations.toml`. Every `/mcp` request in this
# module on a worker's server for one artifact draws on one 127.0.0.1 rate bucket (60
# a minute), so keep each artifact's total well under it.
EQUIVALENCE = {
    "search": (
        "meta",
        "invalid-parameters",
        "search-register",
        "search-period",
        "scope-unavailable",
        "cursor-invalid",
        "cursor-stale",
    ),
    "docs_get": ("docs-get", "docs-unavailable", "docs-scope-unavailable"),
    "docs_related": (
        "docs-related",
        "docs-related-ambiguous",
        "docs-unavailable",
        "docs-scope-unavailable",
    ),
    "show": ("show-provider", "show-bare-names"),
    "states": ("states-paging", "states-errors", "states-stale-cursor"),
    "values": (
        "values-classification",
        "values-state",
        "values-errors",
        "values-stale-cursor",
    ),
    "graph": ("graph-succession", "graph-unheld", "graph-errors"),
    "lineage": ("lineage", "lineage-unheld-reference", "graph-errors"),
    "schema": ("schema-paging", "schema-errors"),
    "diff": ("diff-register", "diff-errors"),
    "coverage": ("coverage-register", "coverage-errors"),
    "coded_variables": ("coded-variables-order",),
    # `resolve-columns` sends `columns` as a JSON array, `resolve-errors` 201 of them
    # and a repeated scalar as an array.
    "resolve": ("resolve-columns", "resolve-errors"),
}
READER = {"fixture": "reader"}
# The `tools/list` result, schemas included: a change to a tool is a reviewed diff here.
TOOLS = json.loads((CASES / "mcp/tools-list.json").read_text())
OPERATIONS = {
    op["name"]: op
    for op in tomllib.loads((CASES.parent / "api/operations.toml").read_text())[
        "operation"
    ]
}


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


def call(client, tool, arguments):
    """`tools/call` as (is_error, structuredContent)."""
    result = rpc(client, "tools/call", {"name": tool, "arguments": arguments})
    result = result.json()["result"]
    return result["isError"], result["structuredContent"]


def path_arguments(op, path):
    """The parameters `path` carries by one of `op`'s routes (`{ref}` spans
    segments, other placeholders one), or None when it is none of them (a
    download, which is HTTP only)."""
    for route in op["http"]:
        template = route.split(" ", 1)[1]
        pattern = "".join(
            f"(?P<{part[1:-1]}>{'.+' if part == '{ref}' else '[^/]+'})"
            if part.startswith("{")
            else re.escape(part)
            for part in re.split(r"(\{\w+\})", template)
        )
        if match := re.fullmatch(pattern, path):
            return {name: unquote(value) for name, value in match.groupdict().items()}
    return None


@pytest.mark.parametrize("operation", EQUIVALENCE)
def test_tool_matches_http(servers, operation):
    # Fails when a tool call's document drifts from the HTTP body (envelope, meta,
    # error fields, argument conversion, the `operation` argument of a tool of
    # several), or when the cases stop covering an error.
    op = OPERATIONS[operation]
    (tool,) = (t for t in TOOLS if t["name"] == op["tool"])
    selector = (
        {"operation": operation}
        if "operation" in tool["inputSchema"]["properties"]
        else {}
    )
    codes = set()
    for name in EQUIVALENCE[operation]:
        case = CASES / "api" / name
        request = json.loads((case / "request.json").read_text())
        steps, clients = request["requests"], case_clients(request, case, servers)
        http = run_http_requests(steps, clients)
        documents = {}
        for index, step in enumerate(steps):
            path = path_arguments(op, step["path"])
            query = step.get("query", {})
            # A download, and a parameter sent in both the path and the query, have
            # no tool-call spelling.
            if step["operation"] != operation or path is None or path.keys() & query:
                continue
            arguments = selector | path | query
            if "cursor_from" in step:
                source, pointer = step["cursor_from"]
                arguments["cursor"] = select_json(documents[source], pointer)
            # The identifier comes from the HTTP response: its source step may be
            # another operation's, which has no tool call here.
            for name, (source, pointer) in step.get("query_from", {}).items():
                arguments[name] = select_json(http[source]["body"], pointer)
            is_error, document = call(
                clients[step.get("artifact")], tool["name"], arguments
            )
            documents[index] = document
            assert (is_error, document) == (
                http[index]["status"] != 200,
                http[index]["body"],
            ), f"{name} step {index}"
            if is_error:
                codes.add(document["error"]["code"])
    assert codes == {"invalid_parameter", *op["errors"]}


def test_tool_of_several_needs_an_operation(servers):
    # Fails when a tool of several operations answers a call that names none of them
    # (it would have to guess one), or one that omits its operation's required
    # parameter, with anything but `invalid_parameter` naming the parameter.
    client = servers.client(artifact_env(cached_case_artifact(READER), "steward"))
    for arguments, parameter in (
        ({"identifier": "Kon"}, "operation"),
        ({"operation": "search", "q": "Kon"}, "operation"),
        ({"operation": "docs_get"}, "identifier"),
    ):
        is_error, document = call(client, "docs", arguments)
        assert is_error
        assert document["error"]["code"] == "invalid_parameter"
        assert document["error"]["fields"] == {"parameter": parameter}


def test_array_argument_only_for_an_array_parameter(servers):
    # Fails when a tool call flattens an array for a parameter that is not
    # `string[]` (`ref: ["…"]` resolved, `register: []` read as no filter) or
    # accepts an empty array for one that is (`columns: []`), instead of refusing
    # it with `invalid_parameter` naming the parameter. HTTP has no spelling of an
    # empty array, so these have no `api` twin.
    client = servers.client(artifact_env(cached_case_artifact(READER), "steward"))
    for tool, arguments, parameter in (
        ("resolve", {"register": [], "columns": ["Value"]}, "register"),
        ("resolve", {"columns": []}, "columns"),
        ("coverage", {"ref": ["scb/example"]}, "ref"),
    ):
        is_error, document = call(client, tool, arguments)
        assert is_error
        assert document["error"]["code"] == "invalid_parameter"
        assert document["error"]["fields"] == {"parameter": parameter}


def test_tools_list_matches_golden(servers):
    # Fails when a tool, its description or a schema changes without updating
    # `cases/mcp/tools-list.json`, or a tool name drifts from `operations.toml`.
    client = servers.client(artifact_env(cached_case_artifact(READER), "steward"))
    served = {
        operation.get("operationId")
        for item in client.get("/openapi.json").json()["paths"].values()
        for operation in item.values()
    }
    tools = rpc(client, "tools/list", {}).json()["result"]["tools"]
    assert tools == TOOLS
    assert sorted(tool["name"] for tool in tools) == sorted(
        {
            op["tool"]
            for name, op in OPERATIONS.items()
            if name in served and "tool" in op
        }
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
    # keys an edge client on its full IPv6 address instead of its /64 (rotating within
    # the /64 would never be refused), or keys an edge request on its peer (every edge
    # client would share one bucket). A server of its own: a drained bucket would
    # refuse the other MCP cases from this address.
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

        def drain(address, token):
            statuses = []
            while 429 not in statuses and len(statuses) < 500:
                response = tools_list(address(len(statuses)), token)
                statuses.append(response.status_code)
            assert statuses[0] == 200
            return response

        response = drain(lambda n: f"203.0.113.{n % 250}", "not-the-secret")
        assert response.status_code == 429
        assert response.headers["retry-after"] == "1"
        assert response.json()["error"]["fields"] == {"retry_after_seconds": 1}
        response = drain(lambda n: f"2001:db8::{n + 1:x}", "edge-secret")
        assert response.status_code == 429
        assert tools_list("2001:db8:0:1::1", "edge-secret").status_code == 200
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
