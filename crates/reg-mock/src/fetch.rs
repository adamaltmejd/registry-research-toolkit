//! `fetch`: save the catalog answers `compile` reads, from the catalog's MCP tools
//! (DESIGN.md, "Fetch"). The crate's only network use: it sends the project document
//! and reads metadata answers. `compile` and `generate` stay offline.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use reg_core::manifest::Manifest;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use crate::Refusal;
use crate::compile::load_answers;
use crate::input::{Envelope, LogicalOrder, SegmentPage};

/// Which order answer to save: the logical order (any catalog) as `order.json`, or a
/// steward's order file, with the delivery's own names, as `manifest.json`.
#[derive(Clone, Copy)]
pub enum Mode {
    Order,
    Materialize,
}

impl Mode {
    fn file(self) -> &'static str {
        match self {
            Self::Order => "order.json",
            Self::Materialize => "manifest.json",
        }
    }

    fn operation(self) -> &'static str {
        match self {
            Self::Order => "order",
            Self::Materialize => "materialize_order",
        }
    }
}

/// `segments` takes up to this many variables, and every listing up to this many items
/// a page.
const BATCH: usize = 200;
/// The hosted rate limit answers 429 with `Retry-After` (60 calls, then one a second),
/// so a call rarely waits more than once.
const ATTEMPTS: u32 = 30;
const MAX_ANSWER_BYTES: u64 = 256 * 1024 * 1024;

/// A catalog's MCP endpoint: the plugin's `catalog_url`.
pub struct Catalog {
    agent: ureq::Agent,
    url: String,
}

impl Catalog {
    #[must_use]
    pub fn new(url: &str) -> Self {
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(300)))
            .build()
            .into();
        Self {
            agent,
            url: url.to_owned(),
        }
    }

    /// One `tools/call`: its `structuredContent`, the document the HTTP API returns.
    /// Waits out a 429 or 503 for its `Retry-After` (one second without one).
    ///
    /// # Errors
    ///
    /// `network` for a failed request; `catalog_unreadable` for a reply that is not a
    /// tool result; `catalog_refused`, one per error finding, for the catalog's error.
    pub fn call(&self, tool: &str, arguments: &Value) -> Result<Value, Vec<Refusal>> {
        let body = json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": tool, "arguments": arguments },
        })
        .to_string();
        let network = |e: ureq::Error| vec![Refusal::new("network", &self.url, e.to_string())];
        let mut attempt = 1;
        let (status, text) = loop {
            let mut response = self
                .agent
                .post(&self.url)
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .send(&body)
                .map_err(network)?;
            let status = response.status().as_u16();
            if matches!(status, 429 | 503) && attempt < ATTEMPTS {
                let wait = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok()?.trim().parse().ok())
                    .unwrap_or(1u64);
                std::thread::sleep(Duration::from_secs(wait.min(60)));
                attempt += 1;
                continue;
            }
            let text = response
                .body_mut()
                .with_config()
                .limit(MAX_ANSWER_BYTES)
                .read_to_string()
                .map_err(network)?;
            break (status, text);
        };
        let at = format!("{}: {tool}", self.url);
        let reply: Value = serde_json::from_str(&text).map_err(|_| {
            let start: String = text.chars().take(300).collect();
            vec![Refusal::new(
                "catalog_unreadable",
                &at,
                format!("HTTP {status}, not a JSON-RPC reply: {start}"),
            )]
        })?;
        if let Some(error) = reply.get("error") {
            return Err(vec![Refusal::new(
                "catalog_unreadable",
                &at,
                format!("HTTP {status}, JSON-RPC error: {error}"),
            )]);
        }
        let result = &reply["result"];
        let document = result.get("structuredContent").cloned();
        match (result["isError"].as_bool(), document) {
            (Some(false), Some(document)) => Ok(document),
            (Some(true), Some(document)) => Err(catalog_refusals(&at, &document)),
            _ => Err(vec![Refusal::new(
                "catalog_unreadable",
                &at,
                format!("HTTP {status}, not a tool result: {reply}"),
            )]),
        }
    }
}

/// The catalog's error document as refusals: its code, message and remediation, then one
/// per error-level item of its `issues` (`project_invalid`) or `findings`
/// (`order_blocked`), located by the item's path.
fn catalog_refusals(at: &str, document: &Value) -> Vec<Refusal> {
    let error = &document["error"];
    let text = |v: &Value| v.as_str().unwrap_or_default().to_owned();
    let mut refusals = vec![Refusal::new(
        "catalog_refused",
        at,
        format!(
            "{}: {} {}",
            text(&error["code"]),
            text(&error["message"]),
            text(&error["remediation"])
        ),
    )];
    let fields = &error["fields"];
    let findings = ["issues", "findings"]
        .into_iter()
        .filter_map(|key| fields[key].as_array())
        .flatten();
    refusals.extend(
        findings
            .filter(|f| f["level"] == "error")
            .map(|f| Refusal::new("catalog_refused", text(&f["path"]), text(&f["message"]))),
    );
    refusals
}

/// Save every answer `compile` reads into `out`: the order answer, every page of
/// `segments` per table (its variables in batches) and per variable without a binding
/// (a row key may be another register's), and every page of each value set those
/// segments use. Pages are named `<answer>-0001.json`, ... Replaces the answers saved
/// before only once all are fetched, then checks them as `compile` will.
///
/// # Errors
///
/// The first call's refusals, `answer_invalid` for an answer of the wrong shape, `io`,
/// or `load_answers`'s refusal for the saved set (`mixed_generations` when the catalog
/// was redeployed during the fetch).
pub fn fetch(
    catalog: &Catalog,
    project: &Value,
    mode: Mode,
    out: &Path,
) -> Result<usize, Vec<Refusal>> {
    let mut saved: Vec<(PathBuf, Value)> = Vec::new();
    let order = catalog.call(
        "order",
        &json!({ "operation": mode.operation(), "project": project }),
    )?;
    let (tables, unbound) = match mode {
        Mode::Order => logical_refs(&parse::<LogicalOrder>(&order, mode.file())?),
        Mode::Materialize => delivery_refs(&parse::<Manifest>(&order, mode.file())?),
    };
    saved.push((PathBuf::from(mode.file()), order));

    let mut calls: Vec<(&str, Map<String, Value>)> = Vec::new();
    for (table, variables) in &tables {
        for batch in variables.chunks(BATCH) {
            calls.push((
                "table",
                args([("ref", json!(table)), ("variable", json!(batch))]),
            ));
        }
    }
    for variable in &unbound {
        calls.push(("variable", args([("ref", json!(variable))])));
    }
    let mut hashes = BTreeSet::new();
    for (i, (kind, arguments)) in calls.into_iter().enumerate() {
        let name = format!("segments/{:04}-{kind}", i + 1);
        for (path, page) in pages(catalog, "segments", &arguments, &name, &mut saved)? {
            let page: SegmentPage = parse(&page, &path)?;
            hashes.extend(
                page.items
                    .into_iter()
                    .filter_map(|s| Some(s.value_set?.hash)),
            );
        }
    }
    for hash in hashes {
        let arguments = args([("value_set", json!(hash))]);
        pages(
            catalog,
            "values",
            &arguments,
            &format!("values/{hash}"),
            &mut saved,
        )?;
    }

    let io = |path: &Path, e: std::io::Error| {
        vec![Refusal::new(
            "io",
            path.display().to_string(),
            e.to_string(),
        )]
    };
    for stale in ["order.json", "manifest.json", "segments", "values"] {
        let path = out.join(stale);
        let removed = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        match removed {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(io(&path, e)),
            _ => {}
        }
    }
    for dir in ["segments", "values"] {
        std::fs::create_dir_all(out.join(dir)).map_err(|e| io(&out.join(dir), e))?;
    }
    let count = saved.len();
    for (path, document) in saved {
        let path = out.join(path);
        let text = format!("{document:#}\n");
        std::fs::write(&path, text).map_err(|e| io(&path, e))?;
    }
    load_answers(out).map_err(|r| vec![r])?;
    Ok(count)
}

fn args<const N: usize>(pairs: [(&str, Value); N]) -> Map<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

/// Each (table, variable) the order reads, as table → variables, and the variables of
/// its row keys, which may have no segment in their table.
type Refs = (BTreeMap<String, Vec<String>>, BTreeSet<String>);

fn logical_refs(order: &LogicalOrder) -> Refs {
    let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut unbound = BTreeSet::new();
    for entry in &order.entries {
        tables
            .entry(entry.table.clone())
            .or_default()
            .insert(entry.variable.clone());
    }
    for keys in &order.row_keys {
        for column in &keys.columns {
            tables
                .entry(keys.table.clone())
                .or_default()
                .insert(column.variable.clone());
            unbound.insert(column.variable.clone());
        }
    }
    (into_lists(tables), unbound)
}

fn delivery_refs(manifest: &Manifest) -> Refs {
    let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut unbound = BTreeSet::new();
    for r in manifest
        .files
        .iter()
        .flat_map(|f| &f.columns)
        .flat_map(|c| &c.refs)
    {
        tables
            .entry(r.table.clone())
            .or_default()
            .insert(r.variable.clone());
        if r.binding.is_none() {
            unbound.insert(r.variable.clone());
        }
    }
    (into_lists(tables), unbound)
}

fn into_lists(tables: BTreeMap<String, BTreeSet<String>>) -> BTreeMap<String, Vec<String>> {
    tables
        .into_iter()
        .map(|(t, vs)| (t, vs.into_iter().collect()))
        .collect()
}

/// Every page of a listing, following `next_cursor`, each saved as `<name>-NNNN.json`;
/// returns the new pages.
fn pages(
    catalog: &Catalog,
    tool: &str,
    arguments: &Map<String, Value>,
    name: &str,
    saved: &mut Vec<(PathBuf, Value)>,
) -> Result<Vec<(String, Value)>, Vec<Refusal>> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    for number in 1.. {
        let mut arguments = arguments.clone();
        arguments.insert("limit".into(), json!(BATCH));
        if let Some(cursor) = cursor.take() {
            arguments.insert("cursor".into(), json!(cursor));
        }
        let page = catalog.call(tool, &Value::Object(arguments))?;
        let path = format!("{name}-{number:04}.json");
        cursor = page["data"]["next_cursor"].as_str().map(str::to_owned);
        saved.push((PathBuf::from(&path), page.clone()));
        out.push((path, page));
        if cursor.is_none() {
            break;
        }
    }
    Ok(out)
}

fn parse<T: DeserializeOwned>(document: &Value, at: &str) -> Result<T, Vec<Refusal>> {
    serde_json::from_value::<Envelope<T>>(document.clone())
        .map(|e| e.data)
        .map_err(|e| {
            vec![Refusal::new(
                "answer_invalid",
                at,
                format!("the catalog's answer is not the expected document ({e}): is the catalog URL right?"),
            )]
        })
}
