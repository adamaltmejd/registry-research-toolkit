//! A steward pair serves only what the steward holds (maintainer 2026-11-04). The
//! filter lives in the reader once per operation (search's `search_filter`, segments'
//! held rows, `show`'s lists), and the build cannot guarantee it, so an operation that
//! forgets it leaks. The synthetic pair needs an overlay to reach the (variable,
//! table) pair filter; the real pair reaches it from the browse root.

use std::collections::{BTreeSet, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::Connection;
use serde_json::Value;

use crate::common::run_all;
use crate::{artifact, get, rows, target};

/// What the holdings file places, read from it alone: every placed table and variable,
/// the registers and providers above the tables (their pid prefixes), and the
/// (variable, table) pairs.
struct Held {
    ids: HashSet<String>,
    pairs: HashSet<(String, String)>,
}

impl Held {
    fn read(holdings: &Connection) -> Self {
        let pairs: HashSet<(String, String)> = rows(
            holdings,
            "SELECT DISTINCT variable_pid, table_pid FROM holding_mapping",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .into_iter()
        .collect();
        let mut ids = HashSet::new();
        for (variable, table) in &pairs {
            ids.insert(variable.clone());
            let mut parts = table.splitn(3, '/');
            let (provider, register) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
            ids.insert(format!("{provider}/{register}"));
            ids.insert(provider.to_owned());
            ids.insert(table.clone());
        }
        Self { ids, pairs }
    }

    /// Every page of `variable`'s segments names only held ids and placed pairs; the
    /// classification families its rows carry go into `families`.
    fn segments(
        &self,
        port: u16,
        variable: &str,
        families: &Mutex<BTreeSet<String>>,
    ) -> Result<(), String> {
        let mut cursor: Option<String> = None;
        loop {
            let mut query = vec![("limit", "200")];
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor));
            }
            let page = get(port, &target(&format!("/api/segments/{variable}"), &query))?;
            self.check("segments", &page)?;
            for row in page["data"]["items"].as_array().into_iter().flatten() {
                let pair = (
                    row["variable"].as_str().unwrap_or(variable).to_owned(),
                    row["table"].as_str().unwrap_or_default().to_owned(),
                );
                if !self.pairs.contains(&pair) {
                    return Err(format!("segments row in unplaced table {}", pair.1));
                }
                if let Some((family, _)) = row["classification"]
                    .as_str()
                    .and_then(|c| c.rsplit_once('/'))
                {
                    families
                        .lock()
                        .expect("the families")
                        .insert(family.to_owned());
                }
            }
            match page["data"]["next_cursor"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => return Ok(()),
            }
        }
    }

    /// Every `fqid` in `document` that names a provider, register, table or variable
    /// and is not held.
    fn unheld(&self, document: &Value, out: &mut Vec<String>) {
        match document {
            Value::Object(map) => {
                if let Some(Value::String(fqid)) = map.get("fqid")
                    && !["class/", "set/", "tag/"]
                        .iter()
                        .any(|p| fqid.starts_with(p))
                    && !self.ids.contains(fqid)
                {
                    out.push(fqid.clone());
                }
                for value in map.values() {
                    self.unheld(value, out);
                }
            }
            Value::Array(items) => {
                for value in items {
                    self.unheld(value, out);
                }
            }
            _ => {}
        }
    }

    fn check(&self, what: &str, document: &Value) -> Result<(), String> {
        let mut leaked = Vec::new();
        self.unheld(document, &mut leaked);
        leaked.sort();
        leaked.dedup();
        match leaked.as_slice() {
            [] => Ok(()),
            [first, ..] => Err(format!(
                "{what}: {} unheld ids, first {first}",
                leaked.len()
            )),
        }
    }
}

#[test]
fn steward_serves_only_held() {
    // Fails when the browse root, a register, table or classification page, a search
    // hit (either group) or a segments row names a provider, register, table or
    // variable the steward places nothing in, or a segments row or a table page's
    // variable pairs a variable with a table it is not placed in.
    let artifact = artifact();
    let Some(holdings) = artifact.holdings() else {
        eprintln!("steward_serves_only_held: skipped, no steward catalog is served");
        return;
    };
    let held = Held::read(&holdings);
    let port = artifact.port();

    let root = get(port, "/api/catalog").expect("the browse root");
    held.check("root", &root).unwrap_or_else(|e| panic!("{e}"));
    let registers: Vec<String> = root["data"]["providers"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|p| p["registers"].as_array().cloned().unwrap_or_default())
        .filter_map(|r| r["fqid"].as_str().map(str::to_owned))
        .collect();
    assert!(!registers.is_empty(), "the browse root lists no register");

    let tables = Mutex::new(Vec::new());
    let variables = Mutex::new(Vec::new());
    run_all("held registers", &registers, Clone::clone, |register| {
        let page = get(port, &format!("/api/catalog/{register}"))?;
        held.check("show", &page)?;
        for (list, into) in [("tables", &tables), ("variables", &variables)] {
            into.lock().expect("the list").extend(
                page["data"][list]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|e| e["fqid"].as_str().map(str::to_owned)),
            );
        }
        Ok(())
    });
    let tables = tables.into_inner().expect("the tables");
    let variables = variables.into_inner().expect("the variables");
    run_all("held tables", &tables, Clone::clone, |table| {
        let page = get(port, &format!("/api/catalog/{table}"))?;
        held.check("show", &page)?;
        for variable in page["data"]["variables"].as_array().into_iter().flatten() {
            let variable = variable["fqid"].as_str().unwrap_or_default().to_owned();
            if !held.pairs.contains(&(variable.clone(), table.clone())) {
                return Err(format!("table page lists unplaced {variable}"));
            }
        }
        Ok(())
    });

    // Typed: an untyped page of a one-letter query can be all codes and documents, which
    // are shared and carry no held id.
    let searches: Vec<(String, &str)> = ('a'..='z')
        .flat_map(|q| ["register", "table", "variable"].map(|kind| (q.to_string(), kind)))
        .collect();
    let hits = AtomicUsize::new(0);
    run_all(
        "held search",
        &searches,
        |(q, kind)| format!("{q} {kind}"),
        |(q, kind)| {
            let query = [("q", q.as_str()), ("type", kind), ("limit", "200")];
            let page = get(port, &target("/api/search", &query))?;
            let data = &page["data"];
            let count = |group: &Value| group["items"].as_array().map_or(0, Vec::len);
            hits.fetch_add(count(data) + count(&data["steward"]), Ordering::Relaxed);
            held.check("search", &page)
        },
    );

    let families = Mutex::new(BTreeSet::new());
    run_all("held segments", &variables, Clone::clone, |variable| {
        held.segments(port, variable, &families)
    });
    // A classification is shared, but the variables that use it are the steward's.
    let families: Vec<String> = families
        .into_inner()
        .expect("the families")
        .into_iter()
        .collect();
    run_all("held classifications", &families, Clone::clone, |family| {
        held.check("show", &get(port, &format!("/api/catalog/{family}"))?)
    });
    assert!(
        hits.load(Ordering::Relaxed) > 0,
        "no search returned a register, table or variable hit to check"
    );
    eprintln!(
        "held sweep: {} registers, {} tables, {} variables, {} classifications, {} search hits",
        registers.len(),
        tables.len(),
        variables.len(),
        families.len(),
        hits.load(Ordering::Relaxed)
    );
}
