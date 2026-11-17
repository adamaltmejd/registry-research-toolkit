//! Cumulative tables (crates/DESIGN.md, "Cumulative tables"): each delivery holds the
//! table from its start to the delivery's own date. Of a steward's extracts of one such
//! table in one linkage space, the newest holds the table from its start to its own date
//! and the older ones are superseded by it.
use std::collections::{BTreeMap, BTreeSet};

use reg_readers::holdings as census;
use rusqlite::Connection;

use super::compile::{self, Identity};
use super::input;
use crate::error::{Result, rows};

/// Each cumulative table's first day, read on its origin as `compile::primaries` reads
/// releases: the base catalog's, or the holdings database's for a table only it defines.
pub(super) type Starts = BTreeMap<String, i64>;

pub(super) fn starts(base: &Connection, db: &Connection) -> Result<Starts> {
    let read = |conn: &Connection| {
        rows(conn, "SELECT pid,start,cumulative FROM tbl", [], |r| {
            Ok((
                r.get::<_, String>(0)?,
                (r.get::<_, i64>(1)?, r.get::<_, bool>(2)?),
            ))
        })
    };
    let mut out = BTreeMap::new();
    for (pid, entry) in read(db)?.into_iter().chain(read(base)?) {
        out.insert(pid, entry);
    }
    Ok(out
        .into_iter()
        .filter_map(|(pid, (start, cumulative))| cumulative.then_some((pid, start)))
        .collect())
}

/// Rewrite the authored topology to the effective one: each extract of a cumulative
/// table is grouped by space, routes and release; the newest own date (the last day of
/// its authored `delivery`) keeps `dated` from the routes' first start, the older are
/// `superseded` by the first newest table. Compile emits, and verify compares, the
/// result.
pub(super) fn resolve(
    authored: &mut BTreeMap<Identity, input::Table>,
    census: &[census::Table],
    spaces: &super::spaces::Spaces,
    starts: &Starts,
    primaries: &compile::Primaries,
) -> Result<()> {
    let space: BTreeMap<Identity, &str> = census
        .iter()
        .map(|t| (compile::census_identity(t), spaces.of(t)))
        .collect();
    let mut groups = BTreeMap::<(&str, Vec<String>, Option<String>), Vec<(i64, Identity)>>::new();
    for (identity, table) in authored.iter() {
        let locate = |why: &str| format!("locator={}: {why}", table.locator);
        if table.disposition == "superseded" {
            return Err(locate(
                "holdings_disposition_invalid: superseded is derived from cumulative tables, not authored",
            )
            .into());
        }
        let cumulative = table
            .routes
            .iter()
            .filter(|r| starts.contains_key(*r))
            .count();
        if cumulative == 0 || table.disposition != "dated" {
            continue;
        }
        if cumulative != table.routes.len() {
            return Err(locate(
                "holdings_cumulative_invalid: routes mix cumulative and other tables; split the file's routes",
            )
            .into());
        }
        let own = compile::table_periods(table)?
            .last()
            .map(|span| span.1)
            .expect("a dated table has a delivery");
        let mut routes = table.routes.clone();
        routes.sort();
        let Some(space) = space.get(identity) else {
            continue; // Accounting refuses a table the census lacks.
        };
        groups
            // The resolved release, so two spellings of one release are one group.
            .entry((space, routes, compile::stated_release(primaries, table)?))
            .or_default()
            .push((own, identity.clone()));
    }
    for ((_, routes, _), members) in groups {
        let newest = members.iter().map(|(own, _)| *own).max().expect("a member");
        let start = routes.iter().map(|r| starts[r]).min().expect("a route");
        let kept = members
            .iter()
            .find(|(own, _)| *own == newest)
            .map(|(_, identity)| identity.clone())
            .expect("the newest member");
        for (own, identity) in members {
            let table = authored.get_mut(&identity).expect("a grouped table");
            if own == newest {
                if start >= own {
                    return Err(format!(
                        "locator={}: holdings_cumulative_invalid: the extract's own date is before its table's start",
                        table.locator
                    )
                    .into());
                }
                table.delivery = Some(reg_core::render(&[(
                    reg_core::issue::iso_day(start),
                    reg_core::issue::iso_day(own - 1),
                )]));
            } else {
                table.disposition = "superseded".into();
                table.delivery = None;
                table.release = None;
                // A superseded extract serves no rows, so it requires no row key.
                table.key.clear();
                table.superseded_by = Some(kept.clone());
            }
        }
    }
    Ok(())
}

/// The artifact's own invariants for cumulative tables, independent of `resolve`: a
/// dated table routed to one holds a single span from the routes' first start, at most
/// one dated vintage per space, routes and release, and no other table places a column
/// in one.
pub(super) fn check(db: &Connection, starts: &Starts) -> Result<()> {
    let routes = rows(
        db,
        "SELECT h.id,h.space,h.release,r.table_pid FROM holding_table h
         JOIN holding_route r ON r.holding_id=h.id WHERE h.disposition='dated'",
        [],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                (r.get::<_, String>(1)?, r.get::<_, Option<String>>(2)?),
                r.get::<_, String>(3)?,
            ))
        },
    )?;
    let mut tables = BTreeMap::<i64, ((String, Option<String>), Vec<String>)>::new();
    for (id, lane, route) in routes {
        tables.entry(id).or_insert((lane, vec![])).1.push(route);
    }
    let mut vintages = BTreeMap::<_, BTreeSet<Vec<(i64, i64)>>>::new();
    for (id, (lane, routes)) in &tables {
        let Some(start) = routes.iter().filter_map(|r| starts.get(r)).min() else {
            continue;
        };
        let periods: Vec<(i64, i64)> = rows(
            db,
            "SELECT start,end FROM holding_period WHERE holding_id=? ORDER BY start",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if !matches!(&periods[..], [(first, _)] if first == start) {
            return Err(format!(
                "holdings:table={id}: holdings_cumulative_partial: an extract of a cumulative table holds it from the table's start to its own date"
            )
            .into());
        }
        let vintage = vintages.entry((lane, routes)).or_default();
        vintage.insert(periods);
        if vintage.len() > 1 {
            return Err(format!(
                "holdings:table={id}: holdings_cumulative_partial: two vintages of one cumulative table are both dated; the older is superseded"
            )
            .into());
        }
    }
    for (id, table) in rows(
        db,
        "SELECT DISTINCT c.holding_id,m.table_pid FROM holding_mapping m
         JOIN holding_column c ON c.id=m.column_id
         WHERE NOT EXISTS(SELECT 1 FROM holding_route r WHERE r.holding_id=c.holding_id AND r.table_pid=m.table_pid)
         ORDER BY 1,2",
        [],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
    )? {
        if starts.contains_key(&table) {
            return Err(format!(
                "holdings:table={id}: holdings_cumulative_unrouted: it places columns in the cumulative table {table} without routing to it; route it with a delivery date, or mark it excluded"
            )
            .into());
        }
    }
    Ok(())
}
