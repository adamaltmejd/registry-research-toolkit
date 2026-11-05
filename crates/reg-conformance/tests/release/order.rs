//! Whole-table orders on a deterministic sample of tables, against an independent SQL
//! census of the artifact (and, on a steward pair, of its holdings file). Failure
//! messages carry public ids and counts, never row content.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use rusqlite::Connection;
use serde_json::{Value, json};

use crate::common::{client, run_all};
use crate::{artifact, rows};

const SAMPLE: usize = 20;
const MAX_BINDINGS: usize = 50;
/// The first sampled tables also order through the MCP `order` tool.
const MCP_TABLES: usize = 3;

/// One sampled table: its project and what the census expects of its order.
struct Sampled {
    pid: String,
    project: Value,
    bindings: usize,
    /// Whether the table also orders through MCP.
    mcp: bool,
    blocked: bool,
    /// The segments of the tables serving the period, each clipped to its serving
    /// window: (table pid, variable pid, start, end, exact spelling).
    spans: Vec<(String, String, i64, i64, String)>,
}

/// A segment as the census reads it.
struct Segment {
    table: String,
    variable: String,
    start: i64,
    end: i64,
    state: Option<String>,
    exact: String,
    role: Option<String>,
}

#[test]
fn sampled_whole_table_orders_agree_with_census() {
    // Fails when `validate` or `order` blocks a table the census finds orderable (or
    // orders one it finds blocked: an ambiguous or preliminary window let through),
    // when an entry's spelling is not the one the artifact holds at the entry's start,
    // when repeated orders serialize different bytes, or when the MCP `order` tool
    // answers a different document than HTTP. On a steward pair it also
    // fails when validate accepts a binding the holdings place on no day of the period
    // (or leave a contested day unplaced), or refuses one they place.
    let artifact = artifact();
    let (sampled, open_start) = census(&artifact.base(), artifact.holdings().as_ref());
    let blocked = sampled.iter().filter(|t| t.blocked).count();
    eprintln!(
        "release order: {} tables sampled, {blocked} blocked",
        sampled.len()
    );
    // A sample that orders nothing would leave the entry checks covering nothing.
    assert!(
        blocked < sampled.len(),
        "no sampled table orders ({blocked} of {} blocked)",
        sampled.len()
    );
    let port = artifact.port();
    run_all(
        "release order",
        &sampled,
        |table| table.pid.clone(),
        |table| check(port, table, open_start),
    );
}

fn check(port: u16, table: &Sampled, open_start: i64) -> Result<(), String> {
    let (status, validated) = client::json(
        port,
        "POST",
        "/api/project/validate",
        &[],
        Some(&table.project),
    )?;
    if status != 200 {
        return Err(format!("validate {status}"));
    }
    let ok = validated["data"]["ok"] == true;
    if ok == table.blocked {
        return Err(format!(
            "validate ok={ok}, census blocked={} over {} bindings",
            table.blocked, table.bindings
        ));
    }
    let ordered = order(port, &table.project)?;
    let want = if table.blocked { 422 } else { 200 };
    if ordered.status != want {
        return Err(format!(
            "order {}, census blocked={}",
            ordered.status, table.blocked
        ));
    }
    let answer: Value = serde_json::from_slice(&ordered.content).map_err(|e| e.to_string())?;
    if table.mcp {
        mcp_agrees(port, table, &answer)?;
    }
    if table.blocked {
        return Ok(());
    }
    if ordered.content != order(port, &table.project)?.content {
        return Err("repeated order bytes differ".to_owned());
    }
    let entries = answer["data"]["entries"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for (number, entry) in entries.iter().enumerate() {
        let at = match entry["period"]["start"].as_str() {
            None => open_start,
            Some(start) => reg_core::iso_day_number(start).ok_or("an unparsed entry start")?,
        };
        // A set: a steward may hold one column in several files.
        let spellings: Vec<&str> = table
            .spans
            .iter()
            .filter(|(t, v, lo, hi, _)| {
                entry["table"] == t.as_str()
                    && entry["variable"] == v.as_str()
                    && *lo <= at
                    && at < *hi
            })
            .map(|span| span.4.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        if spellings.len() != 1 || entry["spelling"] != spellings[0] {
            return Err(format!(
                "entry {number} of {} ({}) has no single census spelling at its start",
                entries.len(),
                entry["variable"]
            ));
        }
    }
    Ok(())
}

/// The MCP `order` tool answers the HTTP document. The fixture tier replays only
/// `order`'s error cases as tool calls, so a successful order's tool path is checked
/// here.
fn mcp_agrees(port: u16, table: &Sampled, http: &Value) -> Result<(), String> {
    let call = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "order",
            "arguments": {"operation": "order", "project": table.project},
        },
    });
    let headers = [
        ("accept", "application/json, text/event-stream".to_owned()),
        ("mcp-protocol-version", "2025-11-25".to_owned()),
    ];
    let (_, reply) = client::json(port, "POST", "/mcp", &headers, Some(&call))?;
    let result = &reply["result"];
    if result["isError"] != table.blocked {
        return Err(format!("MCP order isError={}", result["isError"]));
    }
    if &result["structuredContent"] != http {
        return Err("MCP order differs from HTTP".to_owned());
    }
    Ok(())
}

fn order(port: u16, project: &Value) -> Result<client::Response, String> {
    client::request(
        port,
        "POST",
        "/api/project/order",
        &[("content-type", "application/json".to_owned())],
        Some(serde_json::to_vec(project).expect("a project serializes")),
        Duration::from_secs(60),
    )
}

/// The tables to sample from, by pid: every table, or on a steward pair the reference
/// tables the holdings place something in.
fn candidates(db: &Connection, holdings: Option<&Connection>) -> Vec<String> {
    let all = rows(db, "SELECT pid FROM tbl ORDER BY pid", [], |r| r.get(0));
    let Some(holdings) = holdings else {
        return all;
    };
    let reference: HashSet<String> = all.into_iter().collect();
    rows(
        holdings,
        "SELECT DISTINCT table_pid FROM holding_mapping ORDER BY table_pid",
        [],
        |r| r.get(0),
    )
    .into_iter()
    .filter(|pid| reference.contains(pid))
    .collect()
}

/// The sampled tables, read from the artifact alone (plus the steward's holdings file
/// when one is served), and `meta`'s `open_start`.
///
/// On a steward pair the sample is the reference tables the holdings place something
/// in, each bound to the variables placed in it: a table bound to an unheld variable
/// is blocked, so a sample of all tables would leave the order checks covering little.
fn census(db: &Connection, holdings: Option<&Connection>) -> (Vec<Sampled>, i64) {
    let pids = candidates(db, holdings);
    let key_pairs: HashSet<(String, String)> = holdings
        .map(|h| {
            rows(
                h,
                "SELECT DISTINCT table_pid, variable_pid FROM holding_mapping
                 WHERE role = 'row_key'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
        })
        .unwrap_or_default()
        .into_iter()
        .collect();
    let step = (pids.len() / SAMPLE).max(1);
    // The period grammar's day tokens hold years 1900..=2099: a hull past them (an open
    // sentinel above all) is ordered over the part inside them.
    let first = reg_core::iso_day_number("1900-01-01").expect("a date");
    let last = reg_core::iso_day_number("2100-01-01").expect("a date");
    let mut sampled = Vec::new();
    for pid in pids.iter().step_by(step).take(SAMPLE) {
        let (table_id, timeless, lo, hi): (i64, bool, i64, i64) = db
            .query_row(
                "SELECT t.id, t.timeless, min(s.start), max(s.end)
                 FROM tbl t JOIN tbl_span s ON s.table_id = t.id WHERE t.pid = ?1",
                [pid],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap_or_else(|e| panic!("{pid}: {e}"));
        let (period, bounds) = if timeless {
            (json!("_default"), None)
        } else {
            let (lo, hi) = (lo.max(first), hi.min(last));
            let to = reg_core::issue::iso_day(hi - 1);
            (
                json!({"from": reg_core::issue::iso_day(lo), "to": to}),
                Some((lo, hi)),
            )
        };
        let placed: Option<HashSet<String>> = holdings.map(|h| {
            rows(
                h,
                "SELECT DISTINCT variable_pid FROM holding_mapping WHERE table_pid = ?1",
                [pid],
                |r| r.get(0),
            )
            .into_iter()
            .collect()
        });
        let variables: Vec<String> = rows(
            db,
            "SELECT DISTINCT v.pid FROM segment s JOIN variable v ON v.id = s.variable_id
             WHERE s.table_id = ?1 ORDER BY v.pid",
            [table_id],
            |r| r.get::<_, String>(0),
        )
        .into_iter()
        .filter(|v| placed.as_ref().is_none_or(|placed| placed.contains(v)))
        .take(MAX_BINDINGS)
        .collect();
        // A table whose occurrences all sit in unparsed releases has no segment, so no
        // variable to bind: its project is refused structurally (`empty_bindings`).
        if variables.is_empty() {
            continue;
        }
        let segments = clipped(db, holdings, table_id, bounds);
        // An ambiguous segment of a bound variable or of a row key (row keys join every
        // order) blocks, and so does a bound variable with no segment over the period;
        // a preliminary segment orders with a warning. A steward takes its row keys from
        // the holdings (ops/order/rows.rs), so there only a row key the holdings place
        // in the serving table joins.
        // simplify: approximates the server's `key_requirements` (entity/time over the
        // composed segments) by the holdings' `role = 'row_key'` placements. Ceiling: a
        // declared key column whose mapping role is not `row_key`, or the reverse, is
        // modelled wrong. It surfaces as a census/server disagreement, which stops the
        // release. Upgrade trigger: the first such disagreement; then read
        // `key_requirements` as the server does.
        let joins = |s: &Segment| {
            s.role.as_deref() == Some("row_key")
                && holdings
                    .is_none_or(|_| key_pairs.contains(&(s.table.clone(), s.variable.clone())))
        };
        let covered: HashSet<&str> = segments.iter().map(|s| s.variable.as_str()).collect();
        let mut blocked = segments.iter().any(|s| {
            s.state.as_deref() == Some("ambiguous") && (variables.contains(&s.variable) || joins(s))
        }) || variables.iter().any(|v| !covered.contains(v.as_str()));
        // A steward server also blocks what its holdings cannot deliver (#2210).
        if let Some(holdings) = holdings
            && !blocked
        {
            blocked = unheld(holdings, &variables, &segments);
        }
        sampled.push(Sampled {
            pid: pid.clone(),
            project: project(pid, &period, &variables),
            bindings: variables.len(),
            mcp: sampled.len() < MCP_TABLES,
            blocked,
            spans: segments
                .into_iter()
                .map(|s| (s.table, s.variable, s.start, s.end, s.exact))
                .collect(),
        });
    }
    assert!(!sampled.is_empty(), "the artifact has no table to sample");
    let open_start: String = db
        .query_row("SELECT value FROM meta WHERE key = 'open_start'", [], |r| {
            r.get(0)
        })
        .expect("meta open_start");
    (sampled, open_start.parse().expect("an integer open_start"))
}

/// The segments of the tables serving `bounds` (all of time when `None`) from
/// `table_id`, each clipped to its serving window. A steward's holdings file adds its
/// own segments of a reference table (steward attestation), which the server reads
/// beside the catalog's (ops/order, the composed view).
fn clipped(
    db: &Connection,
    holdings: Option<&Connection>,
    table_id: i64,
    bounds: Option<(i64, i64)>,
) -> Vec<Segment> {
    const SEGMENTS: &str = "SELECT v.pid, s.start, s.end, s.state, sp.exact, s.role
        FROM segment s JOIN tbl t ON t.id = s.table_id
        JOIN variable v ON v.id = s.variable_id
        JOIN spelling sp ON sp.id = s.spelling_id WHERE t.pid = ?1";
    let mut segments = Vec::new();
    for (_, serving_pid, w_lo, w_hi) in serving(db, table_id) {
        let start = [bounds.map(|b| b.0), w_lo].into_iter().flatten().max();
        let end = [bounds.map(|b| b.1), w_hi].into_iter().flatten().min();
        for conn in std::iter::once(db).chain(holdings) {
            for (variable, lo, hi, state, exact, role) in
                rows(conn, SEGMENTS, [&serving_pid], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                })
            {
                let lo = start.map_or(lo, |start| lo.max(start));
                let hi = end.map_or(hi, |end| hi.min(end));
                if lo < hi {
                    segments.push(Segment {
                        table: serving_pid.clone(),
                        variable,
                        start: lo,
                        end: hi,
                        state,
                        exact,
                        role,
                    });
                }
            }
        }
    }
    segments
}

/// The table and its dated `continues` successors, followed transitively, as (table
/// id, table pid, window start, window end), `None` for an open bound: each day goes
/// to the table whose `at_start` window holds it (crates/DESIGN.md, "The logical
/// order").
fn serving(db: &Connection, table_id: i64) -> Vec<(i64, String, Option<i64>, Option<i64>)> {
    let mut chain: Vec<(Option<i64>, i64)> = vec![(None, table_id)];
    let mut queue = vec![table_id];
    let mut seen = HashSet::from([table_id]);
    while !queue.is_empty() {
        let object = queue.remove(0);
        for (subject, at) in rows(
            db,
            "SELECT r.subject_id, r.at_start FROM relation r JOIN tbl t ON t.id = r.subject_id
             WHERE r.kind = 'continues' AND r.subject_kind = 'table'
             AND r.object_kind = 'table' AND r.object_id = ?1
             AND r.at_start IS NOT NULL ORDER BY r.at_start, t.pid",
            [object],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        ) {
            if seen.insert(subject) {
                chain.push((Some(at), subject));
                queue.push(subject);
            }
        }
    }
    // Stable, and `None` (the sampled table's own open start) sorts first.
    chain.sort_by_key(|link| link.0);
    let ends: Vec<Option<i64>> = chain
        .iter()
        .skip(1)
        .map(|link| link.0)
        .chain([None])
        .collect();
    chain
        .into_iter()
        .zip(ends)
        .map(|((at, table), end)| {
            let pid = db
                .query_row("SELECT pid FROM tbl WHERE id = ?1", [table], |r| r.get(0))
                .expect("a serving table's pid");
            (table, pid, at, end)
        })
        .collect()
}

/// Whether the steward's holdings leave a bound variable `mapping_missing` over its
/// clipped segments (the logical order's entries), read from the holdings file alone:
/// either no `holding_mapping` row places any day of the variable's entries, or a day
/// no mapping places lies in an `ambiguous_delivered_owners` notice naming it. Partial
/// coverage only warns. A projection's copy of its parent's days is dropped by the
/// server but never empties a held variable, so it is not modelled here
/// (crates/reg-catalog/src/ops/order/materialize.rs, `gaps` and `clip`).
fn unheld(holdings: &Connection, variables: &[String], segments: &[Segment]) -> bool {
    let mut held: HashMap<&str, bool> = variables.iter().map(|v| (v.as_str(), false)).collect();
    let mut contested = false;
    for segment in segments {
        let Some(found) = held.get_mut(segment.variable.as_str()) else {
            continue;
        };
        let (lo, hi) = (segment.start, segment.end);
        let windows = |sql: &str| -> Vec<(i64, i64)> {
            rows(holdings, sql, [&segment.table, &segment.variable], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
        };
        let mut placed: Vec<(i64, i64)> = windows(
            "SELECT start, end FROM holding_mapping WHERE table_pid = ?1 AND variable_pid = ?2",
        )
        .into_iter()
        .filter(|&(a, z)| a < hi && z > lo)
        .map(|(a, z)| (a.max(lo), z.min(hi)))
        .collect();
        placed.sort_unstable();
        *found = *found || !placed.is_empty();
        let mut missing = Vec::new();
        let mut at = lo;
        for (a, z) in placed {
            if a > at {
                missing.push((at, a));
            }
            at = at.max(z);
        }
        if at < hi {
            missing.push((at, hi));
        }
        if !contested && !missing.is_empty() {
            let notices = windows(
                "SELECT n.start, n.end FROM holding_notice n
                 JOIN holding_notice_candidate c ON c.notice_id = n.id
                 WHERE n.code = 'ambiguous_delivered_owners' AND n.start IS NOT NULL
                 AND c.table_pid = ?1 AND c.variable_pid = ?2",
            );
            contested = missing
                .iter()
                .any(|&(a, z)| notices.iter().any(|&(n_lo, n_hi)| a < n_hi && n_lo < z));
        }
    }
    contested || !held.values().all(|&h| h)
}

fn project(pid: &str, period: &Value, variables: &[String]) -> Value {
    json!({
        "schema_version": "6.0.0",
        "reg_meta_version": "0.64.0",
        "name": "Release order",
        "sources": [{
            "name": "s",
            "table": pid,
            "period": period,
            "bindings": variables
                .iter()
                .map(|v| json!({"variable": v}))
                .collect::<Vec<_>>(),
        }],
    })
}
