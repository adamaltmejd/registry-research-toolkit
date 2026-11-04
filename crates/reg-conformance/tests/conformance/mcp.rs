//! MCP equivalence (conformance/README.md): the MCP tools answer exactly what the HTTP
//! operations answer.
//!
//! Raw JSON-RPC against `/mcp` on the case servers (stateless streamable HTTP, JSON
//! responses), and one stdio session against `reg-meta mcp`. The oracles are the HTTP
//! responses, which the `api` surface pins, the operation table and the `tools/list`
//! golden (`cases/mcp/tools-list.json`). The stdio session writes that golden under
//! `REG_META_BLESS=1`:
//! `REG_META_BLESS=1 cargo test -p reg-conformance --test conformance stdio_session`
//! (`scripts/gate.py regen`).
//!
//! Every `/mcp` request here shares its server's one rate bucket for this address with
//! the `api` cases; the servers' raised write limit (`server::serve_argv`) keeps the
//! suite inside it. Only the burst case runs at the production default, on a server of
//! its own.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::{BufRead as _, BufReader, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use serde_json::{Map, Value, json};

use crate::common::http_case::{
    StepResponse, case_ports, json_eq, raw_body, run_http_requests, select_json,
};
use crate::common::{client, read_json, read_toml, server, steward};

const PROTOCOL: &str = "2025-11-25";

/// Per operation, the `api` cases whose steps of that operation cover a success and
/// every domain error it lists in `operations.toml`.
const EQUIVALENCE: &[(&str, &[&str])] = &[
    // r-show
    (
        "show",
        &[
            "show-provider",
            "show-refs",
            "show-variable",
            "show-variable-evidence",
            "show-set",
            "provider-text-dedup",
        ],
    ),
    ("resolve", &["resolve-columns"]),
    // r-schema
    ("schema", &["schema-paging", "schema-errors"]),
    ("diff", &["diff-register"]),
    ("coverage", &["coverage-register", "coverage-variable"]),
    // r-segments-values
    (
        "segments",
        &["segments-fields", "segments-filters", "segments-errors"],
    ),
    (
        "values",
        &[
            "values-blank-code",
            "values-pair",
            "values-tree",
            "values-errors",
        ],
    ),
    // r-findings-graph
    (
        "graph",
        &["graph-succession", "graph-reference-edges", "graph-errors"],
    ),
    // r-search
    (
        "search",
        &[
            "occurrence-descriptions",
            "search-register",
            "search-tag",
            "search-period",
            "invalid-parameters",
            "cursor-stale",
        ],
    ),
    // r-docs
    (
        "docs_search",
        &[
            "docs-search",
            "docs-search-errors",
            "docs-search-cursor",
            "docs-search-ambiguous",
            "docs-unavailable",
        ],
    ),
    ("docs_get", &["docs-get", "docs-unavailable"]),
    (
        "docs_related",
        &["docs-related", "docs-related-ambiguous", "docs-unavailable"],
    ),
    // r-order-check
    ("validate", &["validate-blocking", "validate-warnings"]),
    ("order", &["order-errors"]),
];

/// Raised only by a shared helper, pinned once and not repeated per operation:
/// `stale_cursor` by the cursor helper (api/cursor-stale), `retired_ref` by the ref
/// resolver (api/show-refs).
const PINNED_ONCE: &[&str] = &["stale_cursor", "retired_ref"];

fn golden() -> PathBuf {
    reg_conformance::cases().join("mcp/tools-list.json")
}

/// The `tools/list` result, schemas included: a change to a tool is a reviewed diff
/// here.
fn tools() -> &'static [Value] {
    static TOOLS: OnceLock<Vec<Value>> = OnceLock::new();
    TOOLS.get_or_init(|| match read_json(&golden()).unwrap() {
        Value::Array(tools) => tools,
        other => panic!("tools-list.json is not a list: {other}"),
    })
}

fn tool(name: &str) -> &'static Value {
    tools()
        .iter()
        .find(|t| t["name"] == name)
        .unwrap_or_else(|| panic!("no tool {name} in tools-list.json"))
}

/// `conformance/api/operations.toml`, by operation name.
fn operations() -> &'static BTreeMap<String, toml::Table> {
    static OPERATIONS: OnceLock<BTreeMap<String, toml::Table>> = OnceLock::new();
    OPERATIONS.get_or_init(|| {
        let table =
            read_toml(&reg_conformance::repo().join("conformance/api/operations.toml")).unwrap();
        table["operation"]
            .as_array()
            .unwrap()
            .iter()
            .map(|op| {
                let op = op.as_table().unwrap().clone();
                (op["name"].as_str().unwrap().to_owned(), op)
            })
            .collect()
    })
}

fn strings(value: Option<&toml::Value>) -> Vec<String> {
    value
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect()
}

/// The port of the `base` fixture's server.
fn reader() -> u16 {
    let artifact =
        reg_conformance::cached_reader_artifact(&reg_conformance::fixture("base")).unwrap();
    server::server(artifact.parent().unwrap()).unwrap().port
}

fn mcp_headers() -> Vec<(&'static str, String)> {
    vec![
        ("accept", "application/json, text/event-stream".to_owned()),
        ("mcp-protocol-version", PROTOCOL.to_owned()),
    ]
}

/// One JSON-RPC request on `/mcp`: the HTTP status and document.
fn rpc(port: u16, method: &str, params: &Value) -> Result<(u16, Value), String> {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    client::json(port, "POST", "/mcp", &mcp_headers(), Some(&body))
}

/// `tools/call` as `(isError, structuredContent)`.
fn call(port: u16, tool: &str, arguments: &Value) -> Result<(bool, Value), String> {
    let (_, reply) = rpc(
        port,
        "tools/call",
        &json!({"name": tool, "arguments": arguments}),
    )?;
    let result = &reply["result"];
    let is_error = result["isError"]
        .as_bool()
        .ok_or_else(|| format!("tools/call {tool}: {reply}"))?;
    Ok((is_error, result["structuredContent"].clone()))
}

/// A tool's advertised `outputSchema`, compiled as draft 2020-12.
struct OutputSchema {
    schemas: boon::Schemas,
    index: boon::SchemaIndex,
}

impl OutputSchema {
    fn of(tool: &Value) -> Self {
        let mut schemas = boon::Schemas::new();
        let mut compiler = boon::Compiler::new();
        compiler.set_default_draft(boon::Draft::V2020_12);
        let location = format!(
            "https://conformance.invalid/{}.json",
            tool["name"].as_str().unwrap()
        );
        compiler
            .add_resource(&location, tool["outputSchema"].clone())
            .unwrap();
        let index = compiler.compile(&location, &mut schemas).unwrap();
        Self { schemas, index }
    }

    fn check(&self, document: &Value) -> Result<(), String> {
        self.schemas
            .validate(document, self.index)
            .map_err(|e| format!("outputSchema: {e}"))
    }
}

/// The parameters `path` carries by one of `op`'s routes (`{ref}` spans segments, other
/// placeholders one), or `None` when it is none of them (a download, which is HTTP
/// only).
fn path_arguments(op: &toml::Table, path: &str) -> Option<Map<String, Value>> {
    let placeholder = regex::Regex::new(r"\{(\w+)\}").unwrap();
    for route in strings(op.get("http")) {
        let template = route.split_once(' ').map_or(route.as_str(), |(_, t)| t);
        let mut pattern = String::from("^");
        let mut last = 0;
        for found in placeholder.captures_iter(template) {
            let whole = found.get(0).unwrap();
            pattern += &regex::escape(&template[last..whole.start()]);
            let name = &found[1];
            let part = if name == "ref" { ".+" } else { "[^/]+" };
            write!(pattern, "(?P<{name}>{part})").expect("write to String");
            last = whole.end();
        }
        pattern += &regex::escape(&template[last..]);
        pattern += "$";
        let route = regex::Regex::new(&pattern).unwrap();
        if let Some(found) = route.captures(path) {
            return Some(
                route
                    .capture_names()
                    .flatten()
                    .map(|name| {
                        let value = percent_encoding::percent_decode_str(&found[name])
                            .decode_utf8_lossy()
                            .into_owned();
                        (name.to_owned(), Value::from(value))
                    })
                    .collect(),
            );
        }
    }
    None
}

#[test]
fn tool_matches_http() {
    // Fails when a tool call's document drifts from the HTTP body (envelope, meta,
    // error fields, argument conversion, a POST body as an object argument, the
    // `operation` argument of a tool of several), when the cases stop covering an
    // error, or when a document fails the tool's advertised `outputSchema` (reg-meta
    // 0.57.0: a state's `{code, label}` value row also matched `ClassificationCode` in
    // the `ValueRow` oneOf, so clients rejected every `values` call on a state).
    let failures: Vec<String> = EQUIVALENCE
        .iter()
        .filter_map(|(operation, cases)| {
            equivalent(operation, cases)
                .err()
                .map(|why| format!("{operation}: {why}"))
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn equivalent(operation: &str, cases: &[&str]) -> Result<(), String> {
    let op = &operations()[operation];
    let tool = tool(op["tool"].as_str().ok_or("no tool")?);
    let schema = OutputSchema::of(tool);
    let mut selector = Map::new();
    if tool["inputSchema"]["properties"].get("operation").is_some() {
        selector.insert("operation".to_owned(), Value::from(operation));
    }
    // A POST operation's body is its `project` argument.
    let body = op
        .get("params")
        .and_then(toml::Value::as_table)
        .into_iter()
        .flatten()
        .find(|(_, t)| t.as_str() == Some("project"))
        .map(|(p, _)| p.clone());
    let mut codes = BTreeSet::new();
    for name in cases {
        let at = |why: String| format!("{name}: {why}");
        let request = read_json(
            &reg_conformance::cases()
                .join("api")
                .join(name)
                .join("request.json"),
        )?;
        let steps = request["requests"].as_array().ok_or("requests is a list")?;
        let ports = case_ports(&request).map_err(at)?;
        let http = run_http_requests(steps, &ports).map_err(at)?;
        let mut documents: BTreeMap<usize, Value> = BTreeMap::new();
        for (index, step) in steps.iter().enumerate() {
            let at = |why: String| format!("{name} step {index}: {why}");
            let Some(arguments) = tool_arguments(&Call {
                op,
                operation,
                selector: &selector,
                body: body.as_deref(),
                step,
                documents: &documents,
                http: &http,
            })
            .map_err(at)?
            else {
                continue;
            };
            let artifact = step
                .get("artifact")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let port = ports[&artifact];
            let (is_error, document) =
                call(port, tool["name"].as_str().unwrap(), &arguments).map_err(at)?;
            schema.check(&document).map_err(at)?;
            let want_error = http[index].status != 200;
            if is_error != want_error || !json_eq(&document, &http[index].body) {
                return Err(at(format!(
                    "tool ({is_error}) {document}\nHTTP ({}) {}",
                    http[index].status, http[index].body
                )));
            }
            if is_error {
                codes.insert(
                    document["error"]["code"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                );
            }
            documents.insert(index, document);
        }
    }
    // An exempt code may still be reached (a case can pin `stale_cursor` per
    // operation); it is only not required.
    let mut want: BTreeSet<String> = strings(op.get("errors")).into_iter().collect();
    want.insert("invalid_parameter".to_owned());
    for exempt in PINNED_ONCE {
        codes.remove(*exempt);
        want.remove(*exempt);
    }
    if codes != want {
        return Err(format!("error codes reached {codes:?}, listed {want:?}"));
    }
    Ok(())
}

/// What a step's tool call is built from.
struct Call<'a> {
    op: &'a toml::Table,
    operation: &'a str,
    /// `{"operation": …}` for a tool of several operations.
    selector: &'a Map<String, Value>,
    /// The parameter a POST body is sent as.
    body: Option<&'a str>,
    step: &'a Value,
    /// The tool documents of the earlier steps.
    documents: &'a BTreeMap<usize, Value>,
    http: &'a [StepResponse],
}

/// An earlier step's index and a pointer into its response: `[step, pointer]`.
fn source(from: &Value) -> Result<(usize, &str), String> {
    let step = from[0].as_u64().and_then(|s| usize::try_from(s).ok());
    match (step, from[1].as_str()) {
        (Some(step), Some(pointer)) => Ok((step, pointer)),
        _ => Err(format!("{from} is not [step, pointer]")),
    }
}

/// The step's tool-call arguments, or `None` when it has no tool-call spelling: a
/// step of another operation, a download, a parameter sent in both the path and the
/// query, or raw body bytes.
fn tool_arguments(call: &Call) -> Result<Option<Value>, String> {
    let step = call.step;
    let query = step
        .get("query")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let path = step["path"]
        .as_str()
        .and_then(|p| path_arguments(call.op, p));
    let Some(path) = path.filter(|_| step["operation"] == call.operation) else {
        return Ok(None);
    };
    if path.keys().any(|k| query.contains_key(k)) || raw_body(step)?.is_some() {
        return Ok(None);
    }
    let mut arguments = call.selector.clone();
    arguments.extend(path);
    arguments.extend(query);
    if let Some(body) = call.body {
        arguments.insert(body.to_owned(), step["body"].clone());
    }
    if let Some(from) = step.get("cursor_from") {
        // The cursor comes from the source step's tool document.
        let (index, pointer) = source(from)?;
        let document = call
            .documents
            .get(&index)
            .ok_or(format!("no tool document of step {index}"))?;
        arguments.insert("cursor".to_owned(), select_json(document, pointer)?);
    }
    // The identifier comes from the HTTP response: its source step may be another
    // operation's, which has no tool call here.
    for (key, from) in step
        .get("query_from")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let (index, pointer) = source(from)?;
        let response = call.http.get(index).ok_or(format!("no step {index}"))?;
        arguments.insert(key.clone(), select_json(&response.body, pointer)?);
    }
    Ok(Some(Value::Object(arguments)))
}

#[test]
fn materialize_tool_matches_http() {
    // Fails when the `materialize_order` call of the `order` tool drifts from `POST
    // /api/project/materialize` (an order file, `order_blocked`, `project_invalid`,
    // `project_unnamed`, or `holdings_unavailable` on a server without holdings), or a
    // document fails the tool's `outputSchema`. Not in `EQUIVALENCE`: that runs on
    // reader artifacts, where every call is `holdings_unavailable`; this needs the
    // steward.
    let cases = reg_conformance::cases();
    let request = |path: &str| read_json(&cases.join(path)).unwrap();
    let project = request("materialize/key-only/request.json")["project"].clone();
    let blocked = request("materialize/contested-column/request.json")["project"].clone();
    let old = request("api/order-errors/request.json")["requests"][0]["body"].clone();
    let mut unnamed = project.clone();
    unnamed["name"] = json!(" \t ");
    let base =
        reg_conformance::cached_reader_artifact(&reg_conformance::holdings_case().join("base"))
            .unwrap();
    let reference_only = server::server(base.parent().unwrap()).unwrap().port;
    let steward = steward::materialize().unwrap();
    let schema = OutputSchema::of(tool("order"));
    let mut codes = BTreeSet::new();
    for (port, body) in [
        (steward, &project),
        (steward, &blocked),
        (steward, &old),
        (steward, &unnamed),
        (reference_only, &project),
    ] {
        let (status, http) =
            client::json(port, "POST", "/api/project/materialize", &[], Some(body)).unwrap();
        let (is_error, document) = call(
            port,
            "order",
            &json!({"operation": "materialize_order", "project": body}),
        )
        .unwrap();
        schema.check(&document).unwrap();
        assert_eq!(is_error, status != 200, "{document}");
        assert!(json_eq(&document, &http), "tool {document}\nHTTP {http}");
        if is_error {
            codes.insert(document["error"]["code"].as_str().unwrap().to_owned());
        }
    }
    let want: BTreeSet<String> = strings(operations()["materialize_order"].get("errors"))
        .into_iter()
        .collect();
    assert_eq!(codes, want);
}

/// Each call refuses with `invalid_parameter` naming the parameter.
fn refuses_parameter(port: u16, calls: &[(&str, Value, &str)]) {
    for (tool, arguments, parameter) in calls {
        let (is_error, document) = call(port, tool, arguments).unwrap();
        assert!(is_error, "{tool} {arguments}: {document}");
        assert_eq!(document["error"]["code"], "invalid_parameter", "{document}");
        assert_eq!(
            document["error"]["fields"],
            json!({"parameter": parameter}),
            "{tool} {arguments}"
        );
    }
}

#[test]
fn tool_of_several_needs_an_operation() {
    // Fails when a tool of several operations answers a call that names none of them
    // (it would have to guess one), or one that omits its operation's required
    // parameter, with anything but `invalid_parameter` naming the parameter.
    refuses_parameter(
        reader(),
        &[
            ("docs", json!({"identifier": "Kon"}), "operation"),
            (
                "docs",
                json!({"operation": "search", "q": "Kon"}),
                "operation",
            ),
            ("docs", json!({"operation": "docs_get"}), "doc"),
        ],
    );
}

#[test]
fn array_argument_only_for_an_array_parameter() {
    // Fails when a tool call flattens an array for a parameter that is not `string[]`
    // (`ref: ["…"]` resolved, `register: []` read as no filter) or accepts an empty
    // array for one that is (`columns: []`), instead of refusing it with
    // `invalid_parameter` naming the parameter. HTTP has no spelling of an empty array,
    // so these have no `api` twin.
    refuses_parameter(
        reader(),
        &[
            (
                "resolve",
                json!({"register": [], "columns": ["Value"]}),
                "register",
            ),
            ("resolve", json!({"columns": []}), "columns"),
            ("coverage", json!({"ref": ["scb/example"]}), "ref"),
        ],
    );
}

#[test]
fn tools_list_matches_golden() {
    // Fails when a tool, its description or a schema changes without updating
    // `cases/mcp/tools-list.json`, or a tool name drifts from `operations.toml`.
    let port = reader();
    let (_, openapi) = client::json(port, "GET", "/openapi.json", &[], None).unwrap();
    let served: BTreeSet<&str> = openapi["paths"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|item| item.as_object().unwrap().values())
        .filter_map(|operation| operation["operationId"].as_str())
        .collect();
    let (_, reply) = rpc(port, "tools/list", &json!({})).unwrap();
    let listed = reply["result"]["tools"].as_array().unwrap();
    assert!(
        json_eq(&Value::from(listed.clone()), &Value::from(tools().to_vec())),
        "tools/list differs from tools-list.json"
    );
    let names: BTreeSet<&str> = listed.iter().filter_map(|t| t["name"].as_str()).collect();
    let catalogued: BTreeSet<&str> = operations()
        .iter()
        .filter(|(name, _)| served.contains(name.as_str()))
        .filter_map(|(_, op)| op.get("tool").and_then(toml::Value::as_str))
        .collect();
    assert_eq!(names.len(), listed.len(), "a tool is listed twice");
    assert_eq!(names, catalogued);
}

#[test]
fn body_over_the_cap_is_payload_too_large() {
    // Fails when `/mcp` stops capping bodies at 1 MiB (today's write-body cap) or
    // answers the cap with anything but the error document.
    let mut headers = mcp_headers();
    headers.push(("content-type", "application/json".to_owned()));
    let response = client::request(
        reader(),
        "POST",
        "/mcp",
        &headers,
        Some(vec![b' '; 1024 * 1024 + 1]),
        Duration::from_secs(60),
    )
    .unwrap();
    assert_eq!(response.status, 413);
    let document: Value = serde_json::from_slice(&response.content).unwrap();
    assert_eq!(
        document["error"]["fields"],
        json!({"limit_bytes": 1024 * 1024})
    );
}

#[test]
fn burst_is_rate_limited() {
    // Fails when `/mcp` stops limiting a client's burst, answers it without the error
    // document, stops sharing the client's bucket with the project POSTs (one token
    // bucket per client, `crates/reg-meta/src/limit.rs`) or limits a read, keys a
    // request on a `CF-Connecting-IP` that lacks the edge token (each forged address
    // would get a fresh bucket and the burst would never be refused), keys an edge
    // client on its full IPv6 address instead of its /64 (rotating within the /64 would
    // never be refused), or keys an edge request on its peer (every edge client would
    // share one bucket). A server of its own: a drained bucket would refuse the other
    // MCP cases from this address. It runs at the production default: the suite's
    // servers raise `--write-limit` so their own cases are not refused.
    let artifact =
        reg_conformance::cached_reader_artifact(&reg_conformance::fixture("base")).unwrap();
    let db = artifact.parent().unwrap();
    let mut argv = server::serve_argv(db);
    let at = argv.iter().position(|a| a == "--write-limit").unwrap();
    argv.drain(at..at + 2);
    let own = server::start(db, &argv, &[("REG_META_EDGE_TOKEN", "edge-secret")]).unwrap();
    let port = own.port;
    let tools_list = |address: &str, token: &str| {
        let mut headers = mcp_headers();
        headers.push(("cf-connecting-ip", address.to_owned()));
        headers.push(("x-edge-token", token.to_owned()));
        headers.push(("content-type", "application/json".to_owned()));
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
        client::request(
            port,
            "POST",
            "/mcp",
            &headers,
            Some(serde_json::to_vec(&body).unwrap()),
            Duration::from_secs(60),
        )
        .unwrap()
    };
    let drain = |address: &dyn Fn(usize) -> String, token: &str| {
        let mut statuses = Vec::new();
        loop {
            let response = tools_list(&address(statuses.len()), token);
            statuses.push(response.status);
            if response.status == 429 || statuses.len() >= 500 {
                assert_eq!(statuses[0], 200);
                return response;
            }
        }
    };
    let response = drain(&|n| format!("203.0.113.{}", n % 250), "not-the-secret");
    assert_eq!(response.status, 429);
    assert_eq!(response.headers["retry-after"], "1");
    let document: Value = serde_json::from_slice(&response.content).unwrap();
    assert_eq!(
        document["error"]["fields"],
        json!({"retry_after_seconds": 1})
    );
    let response = client::request(
        port,
        "POST",
        "/api/project/validate",
        &[("content-type", "application/json".to_owned())],
        Some(b"{}".to_vec()),
        Duration::from_secs(60),
    )
    .unwrap();
    assert_eq!(response.status, 429);
    assert_eq!(response.headers["retry-after"], "1");
    let document: Value = serde_json::from_slice(&response.content).unwrap();
    assert_eq!(document["error"]["code"], "rate_limited");
    let (status, _) = client::json(port, "GET", "/api/context", &[], None).unwrap();
    assert_eq!(status, 200);
    let response = drain(&|n| format!("2001:db8::{:x}", n + 1), "edge-secret");
    assert_eq!(response.status, 429);
    assert_eq!(tools_list("2001:db8:0:1::1", "edge-secret").status, 200);
}

/// `reg-meta mcp` on the artifact directory `db`, from the repository root.
fn mcp_command(db: &Path) -> Command {
    let mut command = Command::new(server::reg_meta());
    command
        .arg("mcp")
        .arg("--db")
        .arg(db)
        .current_dir(reg_conformance::repo());
    command
}

#[test]
fn stdio_session() {
    // Fails when `reg-meta mcp` cannot initialize, list the golden's tools (byte for
    // byte as the golden spells them) or answer a call with the document `/api/search`
    // returns. Under `REG_META_BLESS=1` it writes the golden instead.
    let artifact =
        reg_conformance::cached_reader_artifact(&reg_conformance::fixture("base")).unwrap();
    let db = artifact.parent().unwrap();
    let port = server::server(db).unwrap().port;
    let (status, expected) = client::json(
        port,
        "GET",
        "/api/search?q=Svar&type=variable&limit=1",
        &[],
        None,
    )
    .unwrap();
    assert_eq!(status, 200, "{expected}");
    // Fails when a client that disconnects before `initialize` makes `reg-meta mcp`
    // panic (exit 101, #1326) instead of exiting cleanly.
    let closed = mcp_command(db).stdin(Stdio::null()).output().unwrap();
    assert_eq!(
        (
            closed.status.code(),
            closed.stdout.as_slice(),
            closed.stderr.as_slice()
        ),
        (Some(0), &b""[..], &b""[..]),
        "{}",
        String::from_utf8_lossy(&closed.stderr)
    );
    let arguments = json!({"q": "Svar", "type": "variable", "limit": 1});
    let messages = [
        json!({"id": 0, "method": "initialize", "params": {
            "protocolVersion": PROTOCOL,
            "capabilities": {},
            "clientInfo": {"name": "conformance", "version": "0"},
        }}),
        json!({"method": "notifications/initialized"}),
        json!({"id": 2, "method": "tools/list"}),
        json!({"id": 3, "method": "tools/call", "params": {"name": "search", "arguments": arguments}}),
    ];
    let mut process = mcp_command(db)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = process.stdin.take().unwrap();
    let mut stdout = BufReader::new(process.stdout.take().unwrap());
    let mut replies = Vec::new();
    for mut message in messages {
        message["jsonrpc"] = json!("2.0");
        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
        if message.get("id").is_some() {
            let mut line = String::new();
            stdout.read_line(&mut line).unwrap();
            replies.push(line);
        }
    }
    drop(stdin);
    assert!(process.wait().unwrap().success());
    let [initialize, listed, searched] = &replies[..] else {
        panic!("{} replies: {replies:?}", replies.len());
    };
    let initialize: Value = serde_json::from_str(initialize).unwrap();
    assert_eq!(initialize["result"]["serverInfo"]["name"], "reg-meta");
    let searched: Value = serde_json::from_str(searched).unwrap();
    assert_eq!(searched["result"]["isError"], false);
    assert!(
        json_eq(&searched["result"]["structuredContent"], &expected),
        "{searched}"
    );

    // The golden keeps the server's key order: `serde_json::Value` would sort it.
    let listed: xtask::json::Json = serde_json::from_str(listed).unwrap();
    let tools = listed
        .get("result")
        .and_then(|result| result.get("tools"))
        .unwrap();
    let rendered = serde_json::to_string_pretty(tools).unwrap() + "\n";
    let path = golden();
    if std::env::var_os("REG_META_BLESS").is_some() {
        std::fs::write(&path, &rendered).unwrap();
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        committed == rendered,
        "{} is stale; regenerate it with REG_META_BLESS=1 cargo test -p reg-conformance --test conformance stdio_session",
        path.display()
    );
}
