//! The usage label (`reg_webapp/DESIGN.md` → "Usage statistics"): the `x-reg-usage`
//! header the edge worker records. Oracles: the scrub rule and the pick rule the
//! maintainer set for #1323 (2026-11-04, G11 and G16), and the order case's own
//! project.

use std::time::Duration;

use serde_json::{Value, json};

use crate::common::{client, read_json, server};

const PROTOCOL: &str = "2025-11-25";

fn port(fixture: &str) -> u16 {
    let artifact =
        reg_conformance::cached_reader_artifact(&reg_conformance::fixture(fixture)).unwrap();
    server::server(artifact.parent().unwrap()).unwrap().port
}

/// A request's `x-reg-usage` header as JSON.
fn label(port: u16, method: &str, target: &str, body: Option<&Value>, mcp: bool) -> Value {
    let mut headers = vec![("content-type", "application/json".to_owned())];
    if mcp {
        headers.push(("accept", "application/json, text/event-stream".to_owned()));
        headers.push(("mcp-protocol-version", PROTOCOL.to_owned()));
    }
    let body = body.map(|b| serde_json::to_vec(b).unwrap());
    let response = client::request(
        port,
        method,
        target,
        &headers,
        body,
        Duration::from_secs(60),
    )
    .unwrap();
    let header = response
        .headers
        .get("x-reg-usage")
        .unwrap_or_else(|| panic!("{method} {target}: no x-reg-usage"));
    serde_json::from_str(header).unwrap()
}

/// Fails if a person number or an email address reaches the label unscrubbed, if a
/// short number is scrubbed too, or if a search finding nothing stops carrying `n: 0`
/// (the zero-result signal).
#[test]
fn search_text_is_scrubbed() {
    let port = port("base");
    let target = "/api/search?q=19800101-1234%20anna%40example.se%20lan%202020";
    assert_eq!(
        label(port, "GET", target, None, false),
        json!({"op": "search", "q": "<redacted> <redacted> lan 2020", "n": 0})
    );
}

/// Fails if an MCP `order` stops counting each bound variable as a pick, or if the
/// SPA's `order` (sent after every edit) starts counting them.
#[test]
fn only_an_mcp_order_names_its_variables() {
    let port = port("orders");
    let case = reg_conformance::cases().join("order/binding-type-override/request.json");
    let project = read_json(&case).unwrap()["project"].clone();
    let rpc = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "order", "arguments": {"operation": "order", "project": project}},
    });
    assert_eq!(
        label(port, "POST", "/mcp", Some(&rpc), true),
        json!({"op": "order", "fqids": ["demo/typ/belopp", "demo/typ/namn", "demo/typ/okand"]})
    );
    assert_eq!(
        label(port, "POST", "/api/project/order", Some(&project), false),
        json!({"op": "order"})
    );
}
