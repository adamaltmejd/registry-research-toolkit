//! The origin guards of `reg-meta serve` (`reg_webapp/DESIGN.md`, "Origin lock" and "Cost
//! protection"): with the edge token set, the lock in front of every route, and the
//! write rate limit behind it, keyed on the edge's client address.

use std::time::Duration;

use serde_json::{Value, json};

use crate::common::{client, server};

#[test]
fn edge_lock_and_burst_limit() {
    // Fails when a server with the edge token set answers a request without it (the Fly
    // origin would serve around Cloudflare Access) or refuses `/healthz` (Fly's health
    // check would fail every deploy); when `/mcp` stops limiting a client's burst,
    // answers it without the error document, stops sharing the client's bucket with the
    // project POSTs (one token bucket per client, `crates/reg-meta/src/limit.rs`) or
    // limits a read; keys an edge client on its full IPv6 address instead of its /64
    // (rotating within the /64 would never be refused), or keys an edge request on its
    // peer (every edge client would share one bucket). A server of its own: a drained
    // bucket would refuse the other MCP cases from this address. It runs at the
    // production default: the suite's servers raise `--write-limit` so their own cases
    // are not refused.
    let artifact =
        reg_conformance::cached_reader_artifact(&reg_conformance::fixture("base")).unwrap();
    let db = artifact.parent().unwrap();
    let mut argv = server::serve_argv(db);
    let at = argv.iter().position(|a| a == "--write-limit").unwrap();
    argv.drain(at..at + 2);
    let own = server::start(db, &argv, &[("REG_META_EDGE_TOKEN", "edge-secret")]).unwrap();
    let port = own.port;
    let edge = |address: &str, token: &str| {
        vec![
            ("cf-connecting-ip", address.to_owned()),
            ("x-edge-token", token.to_owned()),
        ]
    };
    let tools_list = |address: &str, token: &str| {
        let mut headers = vec![
            ("accept", "application/json, text/event-stream".to_owned()),
            ("mcp-protocol-version", "2025-11-25".to_owned()),
        ];
        headers.extend(edge(address, token));
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

    let response = tools_list("203.0.113.1", "not-the-secret");
    assert_eq!(response.status, 403);
    let document: Value = serde_json::from_slice(&response.content).unwrap();
    assert_eq!(document["error"]["code"], "edge_token_required");
    let (status, document) = client::json(port, "GET", "/api/context", &[], None).unwrap();
    assert_eq!(
        (status, &document["error"]["code"]),
        (403, &json!("edge_token_required"))
    );
    let response =
        client::request(port, "GET", "/healthz", &[], None, Duration::from_secs(60)).unwrap();
    assert_eq!(response.status, 200);

    let drain = |address: &dyn Fn(usize) -> String| {
        let mut statuses = Vec::new();
        loop {
            let response = tools_list(&address(statuses.len()), "edge-secret");
            statuses.push(response.status);
            if response.status == 429 || statuses.len() >= 500 {
                assert_eq!(statuses[0], 200);
                return response;
            }
        }
    };
    let response = drain(&|_| "203.0.113.1".to_owned());
    assert_eq!(response.status, 429);
    assert_eq!(response.headers["retry-after"], "1");
    let document: Value = serde_json::from_slice(&response.content).unwrap();
    assert_eq!(
        document["error"]["fields"],
        json!({"retry_after_seconds": 1})
    );
    let mut headers = edge("203.0.113.1", "edge-secret");
    headers.push(("content-type", "application/json".to_owned()));
    let response = client::request(
        port,
        "POST",
        "/api/project/validate",
        &headers,
        Some(b"{}".to_vec()),
        Duration::from_secs(60),
    )
    .unwrap();
    assert_eq!(response.status, 429);
    assert_eq!(response.headers["retry-after"], "1");
    let document: Value = serde_json::from_slice(&response.content).unwrap();
    assert_eq!(document["error"]["code"], "rate_limited");
    let (status, _) = client::json(
        port,
        "GET",
        "/api/context",
        &edge("203.0.113.1", "edge-secret"),
        None,
    )
    .unwrap();
    assert_eq!(status, 200);
    let response = drain(&|n| format!("2001:db8::{:x}", n + 1));
    assert_eq!(response.status, 429);
    assert_eq!(tools_list("2001:db8:0:1::1", "edge-secret").status, 200);
}
