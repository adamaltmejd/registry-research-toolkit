//! The delivered table per era (crates/DESIGN.md, "Merged variants"). A table that merges
//! variants holds releases its provider delivered under other table names
//! (`release.delivered_text`), so an order cuts each entry and row-key piece where the
//! delivered name changes and names it in `delivered_table`.

use std::collections::BTreeMap;

use rusqlite::Connection;

use super::{OrderEntry, OrderRowKeys};
use crate::ops::Server;
use crate::{Error, rows};

/// A table's delivered-name eras: (first day, name), ascending, adjacent names distinct.
/// Each era reaches to the next one's first day, and the first one back to the open start,
/// so a day no primary release covers takes the era before it. Empty for a table with no
/// placed primary release (a releaseless provider): its own name stands.
type Eras = Vec<(i64, String)>;

/// Cut `entries` and `row_keys` at their tables' delivered-name changes and name each piece's
/// delivered table. Runs after naming: a piece keeps its entry's names, and a table with no
/// eras is delivered under its own name. `composed` adds the holdings database's releases of
/// a base table, by public id.
pub(super) fn split(
    server: &Server,
    conn: &Connection,
    composed: bool,
    entries: &mut Vec<OrderEntry>,
    row_keys: &mut Vec<OrderRowKeys>,
) -> Result<(), Error> {
    let mut eras: BTreeMap<String, Eras> = BTreeMap::new();
    for table in entries
        .iter()
        .map(|e| &e.table)
        .chain(row_keys.iter().map(|k| &k.table))
    {
        if !eras.contains_key(table) {
            eras.insert(table.clone(), read(conn, composed, table)?);
        }
    }
    let open = server.open;
    *entries = std::mem::take(entries)
        .into_iter()
        .flat_map(|entry| {
            let (lo, hi) = entry.period.days(open);
            pieces(&eras[&entry.table], lo, hi)
                .into_iter()
                .map(|(lo, hi, name)| OrderEntry {
                    period: server.period(lo, hi),
                    delivered_table: name.unwrap_or(&entry.table_name).to_owned(),
                    ..entry.clone()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    *row_keys = std::mem::take(row_keys)
        .into_iter()
        .flat_map(|keys| {
            let (lo, hi) = keys.period.days(open);
            pieces(&eras[&keys.table], lo, hi)
                .into_iter()
                .map(|(lo, hi, name)| OrderRowKeys {
                    period: server.period(lo, hi),
                    delivered_table: name.unwrap_or(&keys.table_name).to_owned(),
                    ..keys.clone()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    Ok(())
}

/// The eras of the table `pid` from its primary placed releases.
fn read(conn: &Connection, composed: bool, pid: &str) -> Result<Eras, Error> {
    let part = |db: &str| {
        format!(
            "SELECT r.start, n.body FROM {db}.release r JOIN {db}.tbl t ON t.id = r.table_id
             JOIN {db}.text n ON n.id = r.delivered_text
             WHERE t.pid = ?1 AND r.is_primary = 1"
        )
    };
    let sql = if composed {
        format!("{} UNION {} ORDER BY 1, 2", part("main"), part("held"))
    } else {
        format!("{} ORDER BY 1, 2", part("main"))
    };
    let starts: Vec<(i64, String)> = rows(conn, &sql, [pid], |r| Ok((r.get(0)?, r.get(1)?)))?;
    let mut eras: Eras = Vec::new();
    for (start, name) in starts {
        if eras.last().is_none_or(|(_, last)| *last != name) {
            eras.push((start, name));
        }
    }
    Ok(eras)
}

/// `[lo, hi)` cut at the eras' first days inside it, each piece with its era's name (`None`
/// when the table has no eras).
fn pieces(eras: &Eras, lo: i64, hi: i64) -> Vec<(i64, i64, Option<&str>)> {
    let name_at = |day: i64| {
        eras.iter()
            .rev()
            .find(|(first, _)| *first <= day)
            .or(eras.first())
            .map(|(_, name)| name.as_str())
    };
    let mut cuts: Vec<i64> = eras
        .iter()
        .map(|(first, _)| *first)
        .filter(|first| lo < *first && *first < hi)
        .collect();
    cuts.push(hi);
    let mut out = Vec::new();
    let mut from = lo;
    for to in cuts {
        out.push((from, to, name_at(from)));
        from = to;
    }
    out
}
