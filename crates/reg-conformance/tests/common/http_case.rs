//! The steps of a readable HTTP case (`request.json`), sent to the server of each
//! case artifact (conformance/README.md, "Case contract").

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{Map, Value};

use super::client;
use super::server;

/// One step's response, as the oracles read it.
#[derive(Debug)]
pub struct StepResponse {
    pub status: u16,
    /// Lower-cased names.
    pub headers: BTreeMap<String, String>,
    pub content: Vec<u8>,
    /// JSON when the content type is `application/json`, else the text, else null.
    pub body: Value,
}

impl StepResponse {
    /// The content type without parameters.
    pub fn media_type(&self) -> &str {
        self.headers
            .get("content-type")
            .map_or("", |t| t.split(';').next().unwrap_or("").trim())
    }
}

/// JSON-pointer projection; `*` collects a list in response order.
pub fn select_json(value: &Value, pointer: &str) -> Result<Value, String> {
    let parts: Vec<&str> = if pointer == "/" {
        Vec::new()
    } else {
        pointer.trim_matches('/').split('/').collect()
    };
    walk(value, &parts).map_err(|why| format!("{pointer}: {why}"))
}

fn walk(current: &Value, parts: &[&str]) -> Result<Value, String> {
    let Some((head, tail)) = parts.split_first() else {
        return Ok(current.clone());
    };
    match current {
        Value::Array(items) if *head == "*" => items.iter().map(|item| walk(item, tail)).collect(),
        Value::Array(items) => {
            let index: usize = head
                .parse()
                .map_err(|_| format!("{head:?} indexes a list"))?;
            walk(items.get(index).ok_or(format!("no item {index}"))?, tail)
        }
        Value::Object(map) => walk(map.get(*head).ok_or(format!("no key {head:?}"))?, tail),
        other => Err(format!("{head:?} on {other}")),
    }
}

/// JSON equality as the corpus means it: numbers compare by value (`1` = `1.0`).
pub fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x == y || (x.is_f64() || y.is_f64()) && x.as_f64() == y.as_f64()
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| json_eq(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| json_eq(v, w)))
        }
        _ => a == b,
    }
}

/// A step's raw request bytes, or `None` when it sends `body` as JSON.
///
/// `content` is a string sent verbatim, encoded with the step's `encoding` (default
/// UTF-8), for documents a JSON value cannot spell: a duplicate key, a byte-order
/// mark, another encoding. `nested_arrays: N` is a root object whose one value nests N
/// arrays deep, too large to spell literally.
pub fn raw_body(step: &Value) -> Result<Option<Vec<u8>>, String> {
    if let Some(depth) = step.get("nested_arrays") {
        let depth = usize::try_from(depth.as_u64().ok_or("nested_arrays is a count")?)
            .map_err(|e| e.to_string())?;
        let mut body = b"{\"nested\": ".to_vec();
        body.extend(std::iter::repeat_n(b'[', depth));
        body.extend(std::iter::repeat_n(b']', depth));
        body.push(b'}');
        return Ok(Some(body));
    }
    let Some(content) = step.get("content") else {
        return Ok(None);
    };
    let text = content.as_str().ok_or("content is a string")?;
    match step
        .get("encoding")
        .and_then(Value::as_str)
        .unwrap_or("utf-8")
    {
        "utf-8" => Ok(Some(text.as_bytes().to_vec())),
        // Python's "utf-16": a byte-order mark, then little-endian units.
        "utf-16" => Ok(Some(
            [0xFF, 0xFE]
                .into_iter()
                .chain(text.encode_utf16().flat_map(u16::to_le_bytes))
                .collect(),
        )),
        other => Err(format!("unsupported encoding {other:?}")),
    }
}

/// A case's fixture directory: `fixture` names one under `cases/fixtures` (default
/// `base`).
pub fn fixture_source(request: &Value) -> PathBuf {
    reg_conformance::fixture(
        request
            .get("fixture")
            .and_then(Value::as_str)
            .unwrap_or("base"),
    )
}

/// The server ports of a case: its artifact's under `None`, each `artifacts` entry's
/// under its name. An entry may name its own `fixture`.
pub fn case_ports(request: &Value) -> Result<HashMap<Option<String>, u16>, String> {
    let port = |source: PathBuf| -> Result<u16, String> {
        let artifact = reg_conformance::cached_reader_artifact(&source)?;
        Ok(server::server(artifact.parent().expect("the artifact directory"))?.port)
    };
    let mut ports = HashMap::from([(None, port(fixture_source(request))?)]);
    for (name, spec) in request
        .get("artifacts")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        let source = spec
            .get("fixture")
            .map_or_else(|| fixture_source(request), |_| fixture_source(spec));
        ports.insert(Some(name.clone()), port(source)?);
    }
    Ok(ports)
}

/// The body is JSON when the content type says so, else the text, else null.
fn step_response(response: client::Response) -> Result<StepResponse, String> {
    let json = response
        .headers
        .get("content-type")
        .is_some_and(|t| t.starts_with("application/json"));
    let body = if json {
        serde_json::from_slice(&response.content).map_err(|e| format!("JSON body: {e}"))?
    } else if response.content.is_empty() {
        Value::Null
    } else {
        Value::String(String::from_utf8_lossy(&response.content).into_owned())
    };
    Ok(StepResponse {
        status: response.status,
        headers: response.headers,
        content: response.content,
        body,
    })
}

/// A query value as a form field: a list repeats the field, a scalar is its text.
fn push_query(form: &mut form_urlencoded::Serializer<'_, String>, name: &str, value: &Value) {
    match value {
        Value::Array(items) => items.iter().for_each(|item| push_query(form, name, item)),
        Value::String(text) => {
            form.append_pair(name, text);
        }
        Value::Null => {
            form.append_pair(name, "");
        }
        other => {
            form.append_pair(name, &other.to_string());
        }
    }
}

/// Send the steps through `ports` (see `case_ports`), in order.
pub fn run_http_requests(
    steps: &[Value],
    ports: &HashMap<Option<String>, u16>,
) -> Result<Vec<StepResponse>, String> {
    let mut responses: Vec<StepResponse> = Vec::new();
    for (index, step) in steps.iter().enumerate() {
        let at = |why: String| format!("step {index}: {why}");
        let artifact = step
            .get("artifact")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let port = *ports
            .get(&artifact)
            .ok_or_else(|| at(format!("no artifact {artifact:?}")))?;
        let earlier = |pair: &Value| -> Result<Value, String> {
            let (Some(step), Some(pointer)) = (
                pair.get(0).and_then(Value::as_u64),
                pair.get(1).and_then(Value::as_str),
            ) else {
                return Err(format!("{pair} is not [step, pointer]"));
            };
            let response = usize::try_from(step).ok().and_then(|s| responses.get(s));
            select_json(&response.ok_or(format!("no step {step}"))?.body, pointer)
        };
        let mut params: Map<String, Value> = step
            .get("query")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Some(from) = step.get("cursor_from") {
            let cursor = earlier(from).map_err(at)?;
            if cursor.is_null() {
                return Err(at(format!("cursor_from {from} is null")));
            }
            params.insert("cursor".to_owned(), cursor);
        }
        // An identifier only an earlier response carries (a storage id).
        for (name, from) in step
            .get("query_from")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            params.insert(name.clone(), earlier(from).map_err(at)?);
        }
        let mut headers = Vec::new();
        let body = match raw_body(step).map_err(at)? {
            Some(raw) => Some(raw),
            None => step
                .get("body")
                .filter(|b| !b.is_null())
                .map(|b| serde_json::to_vec(b).expect("a JSON value serializes")),
        };
        if body.is_some() {
            headers.push(("content-type", "application/json".to_owned()));
        }
        if let Some(from) = step.get("etag_from") {
            // A conditional read that revalidates an earlier step's response.
            let etag = from
                .as_u64()
                .and_then(|s| responses.get(usize::try_from(s).ok()?))
                .and_then(|r| r.headers.get("etag"))
                .ok_or_else(|| at(format!("etag_from {from}: no ETag")))?;
            headers.push(("if-none-match", etag.clone()));
        }
        if let Some(tag) = step.get("if_none_match").and_then(Value::as_str) {
            // A conditional read with a literal validator: a stale or foreign tag.
            headers.push(("if-none-match", tag.to_owned()));
        }
        let mut target = step
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| at("no path".into()))?
            .to_owned();
        if !params.is_empty() {
            let mut form = form_urlencoded::Serializer::new(String::new());
            for (name, value) in &params {
                push_query(&mut form, name, value);
            }
            target = format!("{target}?{}", form.finish());
        }
        let method = step.get("method").and_then(Value::as_str).unwrap_or("GET");
        let response = client::request(
            port,
            method,
            &target,
            &headers,
            body,
            Duration::from_secs(60),
        )
        .map_err(at)?;
        responses.push(step_response(response).map_err(at)?);
    }
    Ok(responses)
}
