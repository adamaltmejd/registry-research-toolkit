//! The usage label (#1323, `reg_webapp/DESIGN.md` → "Usage statistics"): what one
//! request asked for, sent as the [`HEADER`] response header for the edge worker to
//! record and strip, and `Server-Timing` with the server's own time. The server keeps
//! nothing: no log, no counter, no address.
//!
//! The label names the operation and the public ids the request names, and, for a
//! search, its text with personal-looking values replaced ([`scrub`]) and the number
//! of hits. Nothing else from a request or its answer is in it.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use reg_catalog::Error;
use reg_catalog::ops::{Operation, Type};
use serde_json::{Map, Value, json};

/// The response header carrying the label, a compact JSON object.
pub const HEADER: &str = "x-reg-usage";
/// The placeholder [`scrub`] puts in place of a personal-looking value.
const PLACEHOLDER: &str = "<redacted>";
/// Digits in one run, `-` or `+` allowed between them, from which [`scrub`] replaces
/// it: a person number (10 or 12), an org number, a date, a phone number. A 6-digit
/// code is replaced too; accepted (maintainer, 2026-11-04).
const SCRUB_DIGITS: usize = 6;
/// Characters of search text kept, as `search`'s own `q` cap.
const MAX_TEXT: usize = 200;
/// Variable ids of one MCP `order` counted as picks.
const MAX_PICKS: usize = 100;
/// The header's bytes: ids are dropped, then the text cut, to stay under it.
const MAX_HEADER: usize = 4096;

/// One request's label.
#[derive(Default)]
pub struct Usage {
    /// The operation name, or the JSON-RPC method of an MCP request that runs none.
    op: String,
    /// The public ids the request names, scrubbed: its ref parameters, and over MCP
    /// the variables an `order` binds.
    fqids: Vec<String>,
    /// A search's text, scrubbed and cut to [`MAX_TEXT`].
    q: Option<String>,
    /// A search's hits on the page answered.
    n: Option<usize>,
    /// The refusal's code.
    err: Option<String>,
}

/// Where an MCP tool call leaves its label for [`mcp`] to send: the HTTP request's
/// extension, which rmcp hands the handler with the request's parts.
#[derive(Clone, Default)]
pub struct Slot(Arc<Mutex<Option<Usage>>>);

impl Slot {
    pub fn put(&self, usage: Usage) {
        *self.0.lock().expect("usage slot") = Some(usage);
    }
}

impl Usage {
    /// The label of `op` run on `query` (the request's parameters), answered by
    /// `result`. `mcp` adds an `order`'s bound variables as picks: the SPA calls
    /// `order` after every edit, so only an agent's call is a pick.
    pub fn of(
        op: &Operation,
        query: &[(String, String)],
        result: &Result<Value, Error>,
        mcp: bool,
    ) -> Self {
        let declared = |name: &str| op.params.iter().find(|p| p.name == name);
        let mut fqids: Vec<String> = query
            .iter()
            .filter(|(name, _)| declared(name).is_some_and(|p| matches!(p.ty, Type::Ref)))
            .map(|(_, value)| scrub(value))
            .collect();
        if mcp && op.name == "order" {
            let body = query
                .iter()
                .find(|(name, _)| declared(name).is_some_and(|p| matches!(p.ty, Type::Project)))
                .and_then(|(_, text)| serde_json::from_str::<Value>(text).ok());
            fqids.extend(bound_variables(body.as_ref()).take(MAX_PICKS).map(scrub));
        }
        let text = query.iter().find(|(name, _)| name == "q").map(|(_, q)| q);
        Self {
            op: op.name.to_owned(),
            fqids,
            q: text.map(|q| scrub(q).chars().take(MAX_TEXT).collect()),
            n: text.and(result.as_ref().ok()).map(hits),
            err: result.as_ref().err().map(code),
        }
    }

    /// The label of an MCP request that runs no operation, by its JSON-RPC method.
    fn method(method: &str) -> Self {
        Self {
            op: method.chars().take(64).collect(),
            ..Self::default()
        }
    }

    /// The header value: compact JSON, ASCII only (a header value is), under
    /// [`MAX_HEADER`] bytes.
    pub fn header(mut self) -> HeaderValue {
        loop {
            let mut object = Map::new();
            object.insert("op".into(), json!(self.op));
            if !self.fqids.is_empty() {
                object.insert("fqids".into(), json!(self.fqids));
            }
            for (key, value) in [
                ("q", self.q.as_ref().map(|q| json!(q))),
                ("n", self.n.map(|n| json!(n))),
                ("err", self.err.as_ref().map(|err| json!(err))),
            ] {
                if let Some(value) = value {
                    object.insert(key.into(), value);
                }
            }
            let text = ascii(&Value::Object(object).to_string());
            if text.len() <= MAX_HEADER {
                return HeaderValue::from_str(&text).expect("ASCII JSON is a header value");
            }
            if self.fqids.pop().is_none() {
                self.q = self
                    .q
                    .map(|q| q.chars().take(q.chars().count() / 2).collect());
            }
        }
    }
}

/// A search's hits on the answered page: its `items`, a paired steward group's too,
/// or `search_facets`' `registers`.
fn hits(data: &Value) -> usize {
    let count = |value: &Value| value.as_array().map_or(0, Vec::len);
    count(&data["items"]) + count(&data["steward"]["items"]) + count(&data["registers"])
}

/// The refusal's code, as its error document names it.
fn code(err: &Error) -> String {
    serde_json::to_value(err).expect("Error serializes")["code"]
        .as_str()
        .expect("an error code")
        .to_owned()
}

/// The `variable` of every binding of a `project_data.json` document.
fn bound_variables(project: Option<&Value>) -> impl Iterator<Item = &str> {
    project
        .and_then(|p| p["sources"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|source| source["bindings"].as_array())
        .flatten()
        .filter_map(|binding| binding["variable"].as_str())
}

/// `text` with each personal-looking value replaced by [`PLACEHOLDER`]: a
/// whitespace-separated word holding an `@` between other characters (an email
/// address), and a run of [`SCRUB_DIGITS`] or more digits, `-` or `+` allowed between
/// them.
pub fn scrub(text: &str) -> String {
    text.split_inclusive(char::is_whitespace)
        .map(|word| {
            let trimmed = word.trim_end();
            let at = trimmed.find('@');
            if at.is_some_and(|at| at > 0 && at + 1 < trimmed.len()) {
                format!("{PLACEHOLDER}{}", &word[trimmed.len()..])
            } else {
                scrub_digits(word)
            }
        })
        .collect()
}

fn scrub_digits(word: &str) -> String {
    let chars: Vec<char> = word.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if !chars[i].is_ascii_digit() {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        // The run: digits, with a single `-` or `+` between two of them.
        let (start, mut end, mut digits) = (i, i, 0);
        while end < chars.len() {
            if chars[end].is_ascii_digit() {
                digits += 1;
                end += 1;
            } else if matches!(chars[end], '-' | '+')
                && chars.get(end + 1).is_some_and(char::is_ascii_digit)
            {
                end += 1;
            } else {
                break;
            }
        }
        if digits >= SCRUB_DIGITS {
            out.push_str(PLACEHOLDER);
        } else {
            out.extend(&chars[start..end]);
        }
        i = end;
    }
    out
}

/// `text` with every non-ASCII character as a JSON `\u` escape.
fn ascii(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_ascii() {
            out.push(c);
        } else {
            let mut units = [0; 2];
            for unit in c.encode_utf16(&mut units) {
                write!(out, "\\u{unit:04x}").expect("writing to a String");
            }
        }
    }
    out
}

/// Every response's `Server-Timing: app;dur=<ms>`: the server's own time, which the
/// edge records beside its time to the origin.
pub async fn timing(request: Request, next: Next) -> Response {
    let start = Instant::now();
    let mut response = next.run(request).await;
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    let value = HeaderValue::from_str(&format!("app;dur={ms:.1}")).expect("a header value");
    response.headers_mut().insert("server-timing", value);
    response
}

/// `/mcp`'s label: the slot a tool call fills, else the request's JSON-RPC method.
/// Inside the write limits, so the body is already read and capped.
pub async fn mcp(request: Request, next: Next) -> Response {
    let (mut parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_else(|_| Bytes::new());
    let method = serde_json::from_slice::<Value>(&bytes)
        .ok()
        .and_then(|rpc| rpc["method"].as_str().map(str::to_owned));
    let slot = Slot::default();
    parts.extensions.insert(slot.clone());
    let mut response = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    let usage = slot.0.lock().expect("usage slot").take();
    if let Some(usage) = usage.or_else(|| method.as_deref().map(Usage::method)) {
        response.headers_mut().insert(HEADER, usage.header());
    }
    response
}
