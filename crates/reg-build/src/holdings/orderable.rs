//! How much of a steward's delivery an order can reach (crates/DESIGN.md, "Orderable
//! columns"). Orderable is what `materialize_order` would put in a file for an order of
//! everything the steward holds: the placements its own selection keeps
//! (`reg_core::placement`, every (table, variable) over all days, every database and
//! lane), plus the period column a declared key reads its time from
//! (`period_column_offered`) in a table that keeps one. A verified artifact has cleared
//! the other refusals that path can raise (keys, storage, file names). A column whose
//! placements a parent table's cover wholly is delivered through that parent. Every
//! other column gets exactly one reason, read from what the compiler recorded: the
//! table's disposition and held row key first, then the column's own decision and
//! notices. Never a refusal: the build prints it as a warning and writes it to the report.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use reg_core::placement::{Placement, key_variables, placements_sql, select};
use rusqlite::{Connection, named_params};
use serde_json::{Value, json};

use crate::error::{Result, rows};

/// Why a delivered column is not orderable, in precedence order: a table-level reason
/// wins over the column's own, so a held row key's stamped column holds count once,
/// under the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Reason {
    DeliveredThroughParent,
    TableExcluded,
    TableSuperseded,
    TableLookup,
    TableUnknown,
    RowKeyHeld,
    ColumnUnmapped,
    ColumnHeld,
    Ambiguous,
    Unmatched,
    Other,
}

impl Reason {
    pub(crate) const ALL: [Self; 11] = [
        Self::DeliveredThroughParent,
        Self::TableExcluded,
        Self::TableSuperseded,
        Self::TableLookup,
        Self::TableUnknown,
        Self::RowKeyHeld,
        Self::ColumnUnmapped,
        Self::ColumnHeld,
        Self::Ambiguous,
        Self::Unmatched,
        Self::Other,
    ];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::DeliveredThroughParent => "delivered_through_parent",
            Self::TableExcluded => "table_excluded",
            Self::TableSuperseded => "table_superseded",
            Self::TableLookup => "table_lookup",
            Self::TableUnknown => "table_unknown",
            Self::RowKeyHeld => "row_key_held",
            Self::ColumnUnmapped => "column_unmapped",
            Self::ColumnHeld => "column_held",
            Self::Ambiguous => "ambiguous",
            Self::Unmatched => "unmatched",
            Self::Other => "other",
        }
    }

    /// Work still owed, as opposed to a column accounted for: delivered through a parent
    /// table, or a stated decision not to order it (an excluded, superseded or lookup
    /// table, or a permanent column `unmap`).
    pub(crate) fn gap(self) -> bool {
        !matches!(
            self,
            Self::DeliveredThroughParent
                | Self::TableExcluded
                | Self::TableSuperseded
                | Self::TableLookup
                | Self::ColumnUnmapped
        )
    }
}

/// One delivered (census) column and why it is not orderable, `None` when it is.
pub(crate) struct Column {
    pub table: i64,
    pub schema: Option<String>,
    pub physical: String,
    pub name: String,
    pub reason: Option<Reason>,
}

/// What an order of everything draws on: the columns materialization keeps (its
/// placement selection, then each opened file's declared keys), the tables whose files
/// it opens, and each column it keeps nothing of, with the parent table that delivers it.
///
/// simplify: whole-delivery grain. Selection runs over all days, and a file's keys and
/// period column count wherever the table keeps any placement, where materialization
/// resolves both over the days the file extracts; the stat may overcount a key or period
/// column when a parent covers part of the year. Resolve per extracted day if the count
/// is ever used as a gate or drifts from orders by more than a handful of columns.
fn selection(db: &Connection) -> Result<Selection> {
    let pairs: Vec<(String, String)> = rows(
        db,
        "SELECT DISTINCT table_pid, variable_pid FROM holding_mapping",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let everything = (i64::MIN, i64::MAX);
    let mut query = db.prepare(&placements_sql())?;
    let mut lanes = db.prepare(
        "SELECT DISTINCT h.release FROM holding_mapping m
           JOIN holding_column c ON c.id = m.column_id JOIN holding_table h ON h.id = c.holding_id
         WHERE m.table_pid = ? AND m.variable_pid = ? AND h.release IS NOT NULL",
    )?;
    let mut kept = BTreeSet::new();
    let mut parents = BTreeMap::new();
    for (table, variable) in pairs {
        // An entry of no release draws on every file; a concurrent table's shared days
        // are entries of one release each, folded within that release's files.
        let releases: Vec<Option<String>> = std::iter::once(Ok(None))
            .chain(lanes.query_map([&table, &variable], |r| r.get(0).map(Some))?)
            .collect::<rusqlite::Result<_>>()?;
        for release in releases {
            let found: Vec<Placement> = query
                .query_map(
                    named_params! {
                        ":table": table,
                        ":variable": variable,
                        ":lo": everything.0,
                        ":hi": everything.1,
                        ":space": None::<String>,
                        ":release": release,
                    },
                    |r| {
                        Ok(Placement {
                            holding: r.get(0)?,
                            projection_of: r.get(1)?,
                            column: r.get(2)?,
                            days: (r.get(3)?, r.get(4)?),
                            caveat: r.get(5)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<_>>()?;
            for p in &found {
                if let Some(parent) = p.projection_of {
                    parents.insert(p.column, parent);
                }
            }
            kept.extend(select(found, everything).into_iter().map(|p| p.column));
        }
    }
    let tables: BTreeSet<i64> = rows(
        db,
        "SELECT DISTINCT holding_id FROM holding_column WHERE id IN (SELECT value FROM json_each(?))",
        [serde_json::to_string(&kept)?],
        |r| r.get(0),
    )?
    .into_iter()
    .collect();
    // An opened file carries every column its unheld declared keys map to, a
    // parent-covered (`carried`) key copy included, as materialization's `keys` adds them.
    let mut key_columns = db.prepare(
        "SELECT DISTINCT c.id FROM holding_mapping m JOIN holding_column c ON c.id = m.column_id
         WHERE c.holding_id = ? AND m.variable_pid = ?",
    )?;
    for &table in &tables {
        let stored: String = db.query_row(
            "SELECT key_requirements FROM holding_table WHERE id = ?",
            [table],
            |r| r.get(0),
        )?;
        let stored: Vec<Value> = serde_json::from_str(&stored).unwrap_or_default();
        for requirement in stored.iter().filter(|k| k["hold"].is_null()) {
            for variable in key_variables(requirement) {
                for column in key_columns
                    .query_map(rusqlite::params![table, variable], |r| r.get::<_, i64>(0))?
                {
                    kept.insert(column?);
                }
            }
        }
    }
    parents.retain(|column, _| !kept.contains(column));
    Ok(Selection {
        kept,
        tables,
        through: parents,
    })
}

/// [`selection`]'s answer.
struct Selection {
    kept: BTreeSet<i64>,
    tables: BTreeSet<i64>,
    through: BTreeMap<i64, i64>,
}

/// Every census column of a compiled holdings file with its reason.
pub(crate) fn columns(db: &Connection) -> Result<Vec<Column>> {
    let Selection {
        kept,
        tables: tables_kept,
        through,
    } = selection(db)?;
    let mut query = db.prepare(
        "SELECT t.id, t.schema_name, t.physical_name, t.disposition, t.key_requirements,
                c.id, c.name, c.disposition,
                (SELECT group_concat(DISTINCT n.code) FROM holding_notice n
                 WHERE n.column_id = c.id),
                c.id IS t.period_column_id, t.projection_of
         FROM holding_column c JOIN holding_table t ON t.id = c.holding_id
         ORDER BY t.id, c.ordinal",
    )?;
    let rows = query.query_map([], |r| {
        Ok((
            (
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ),
            (
                r.get::<_, i64>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, Option<String>>(8)?,
                r.get::<_, bool>(9)?,
                r.get::<_, Option<i64>>(10)?,
            ),
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (
            (table, schema, physical, table_disposition, keys),
            (id, name, disposition, notices, period_column, projection_of),
        ) = row?;
        // The period column a key reads its time from joins every file of its table as a
        // row key with no mapping of its own; a table no order selects has no file.
        let offered = period_column && super::keys::period_column_offered(db, table)?;
        let notices = notices.unwrap_or_default();
        let has_notice = |code: &str| notices.split(',').any(|n| n == code);
        let reason = if kept.contains(&id) || offered && tables_kept.contains(&table) {
            None
        } else if through.contains_key(&id) || offered && projection_of.is_some() {
            Some(Reason::DeliveredThroughParent)
        } else {
            Some(match table_disposition.as_str() {
                "excluded" => Reason::TableExcluded,
                "superseded" => Reason::TableSuperseded,
                "lookup" => Reason::TableLookup,
                "unknown" => Reason::TableUnknown,
                _ if !key_holds(&keys).is_empty() => Reason::RowKeyHeld,
                _ if disposition == "unmap" => Reason::ColumnUnmapped,
                _ if disposition == "hold" || has_notice("mapping_held") => Reason::ColumnHeld,
                _ if has_notice("ambiguous_delivered_owners")
                    || has_notice("ambiguous_catalog_segment") =>
                {
                    Reason::Ambiguous
                }
                _ if has_notice("unmatched") || has_notice("mapping_missing") => Reason::Unmatched,
                _ => Reason::Other,
            })
        };
        out.push(Column {
            table,
            schema,
            physical,
            name,
            reason,
        });
    }
    Ok(out)
}

/// The held entries of a table's pinned `key_requirements`: (hold reason, follow-up).
fn key_holds(keys: &str) -> Vec<(String, Option<String>)> {
    serde_json::from_str::<Vec<Value>>(keys)
        .unwrap_or_default()
        .iter()
        .filter_map(|key| {
            Some((
                key["hold"].as_str()?.to_owned(),
                key["follow_up"].as_str().map(str::to_owned),
            ))
        })
        .collect()
}

/// The orderability summary of a compiled holdings file: totals, columns by reason, and
/// each table with a column that is not orderable. A table with no orderable column and
/// some gap is blocked as a whole.
pub(crate) fn summary(db: &Connection) -> Result<Value> {
    let columns = columns(db)?;
    let mut reasons: BTreeMap<Reason, u64> = BTreeMap::new();
    let mut tables: BTreeMap<i64, (u64, u64, BTreeMap<Reason, u64>)> = BTreeMap::new();
    for column in &columns {
        let table = tables.entry(column.table).or_default();
        table.0 += 1;
        match column.reason {
            None => table.1 += 1,
            Some(reason) => {
                *reasons.entry(reason).or_default() += 1;
                *table.2.entry(reason).or_default() += 1;
            }
        }
    }
    let orderable = columns.iter().filter(|c| c.reason.is_none()).count() as u64;
    let gaps: u64 = reasons
        .iter()
        .filter(|(r, _)| r.gap())
        .map(|(_, n)| n)
        .sum();
    let not_orderable = columns.len() as u64 - orderable;
    let mut listed = Vec::new();
    let mut detail = db.prepare(
        "SELECT schema_name, physical_name, key_requirements,
                (SELECT json_group_array(table_pid) FROM
                   (SELECT table_pid FROM holding_route WHERE holding_id = t.id ORDER BY table_pid)),
                (SELECT p.physical_name FROM holding_table p WHERE p.id = t.projection_of)
         FROM holding_table t WHERE id = ?",
    )?;
    for (id, (total, ordered, by)) in &tables {
        if by.is_empty() {
            continue;
        }
        let (schema, name, keys, routes, parent): (
            Option<String>,
            String,
            String,
            String,
            Option<String>,
        ) = detail.query_row([id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })?;
        let holds = key_holds(&keys);
        let blocked = (*ordered == 0 && by.keys().any(|r| r.gap())).then(|| {
            let reason = if !holds.is_empty() {
                Reason::RowKeyHeld.as_str()
            } else if by.contains_key(&Reason::TableUnknown) {
                Reason::TableUnknown.as_str()
            } else {
                "no_orderable_column"
            };
            json!({
                "reason": reason,
                "holds": holds.iter().map(|(hold, follow_up)| json!({"hold": hold, "follow_up": follow_up})).collect::<Vec<_>>(),
            })
        });
        listed.push(json!({
            "schema": schema,
            "table": name,
            "routes": serde_json::from_str::<Value>(&routes)?,
            "columns": total,
            "orderable": ordered,
            "reasons": by.iter().map(|(r, n)| (r.as_str().to_owned(), json!(n))).collect::<serde_json::Map<_, _>>(),
            "blocked": blocked,
            // The parent table that delivers this one's `delivered_through_parent` columns.
            "through": parent.filter(|_| by.contains_key(&Reason::DeliveredThroughParent)),
        }));
    }
    Ok(json!({
        "columns": columns.len(),
        "orderable": orderable,
        "not_orderable": not_orderable,
        "gaps": gaps,
        "accounted": not_orderable - gaps,
        "reasons": Reason::ALL.iter().filter_map(|r| {
            reasons.get(r).map(|n| (r.as_str().to_owned(), json!({"columns": n, "gap": r.gap()})))
        }).collect::<serde_json::Map<_, _>>(),
        "tables": listed,
    }))
}

/// The summary as warning lines: the totals, each reason, then each blocked table.
pub(crate) fn warnings(summary: &Value) -> Vec<String> {
    let n = |v: &Value| v.as_u64().unwrap_or_default();
    let mut out = vec![format!(
        "warning: holdings: orderable {} of {} delivered columns; not orderable {} \
         (still to place {}, accounted for {})",
        n(&summary["orderable"]),
        n(&summary["columns"]),
        n(&summary["not_orderable"]),
        n(&summary["gaps"]),
        n(&summary["accounted"]),
    )];
    let tables = summary["tables"].as_array().map_or(&[][..], Vec::as_slice);
    if let Some(reasons) = summary["reasons"].as_object() {
        for (reason, entry) in reasons {
            let in_tables = tables
                .iter()
                .filter(|t| t["reasons"].get(reason).is_some())
                .count();
            let kind = if entry["gap"].as_bool() == Some(true) {
                "to place"
            } else {
                "accounted for"
            };
            out.push(format!(
                "warning: holdings: not orderable, {reason} ({kind}): columns {}, tables {in_tables}",
                n(&entry["columns"])
            ));
        }
    }
    for table in tables.iter().filter(|t| !t["blocked"].is_null()) {
        // Qualified by schema only when another listed table has the same name.
        let bare = table["table"].as_str().unwrap_or_default();
        let repeated = tables
            .iter()
            .filter(|t| t["table"] == table["table"])
            .count()
            > 1;
        let name = match table["schema"].as_str() {
            Some(schema) if repeated => format!("{schema}.{bare}"),
            _ => bare.to_owned(),
        };
        let mut line = format!(
            "warning: holdings: blocked table {name} ({} columns): {}",
            n(&table["columns"]),
            table["blocked"]["reason"].as_str().unwrap_or_default()
        );
        for hold in table["blocked"]["holds"].as_array().into_iter().flatten() {
            let _ = write!(
                line,
                "; held: {} (follow-up: {})",
                hold["hold"].as_str().unwrap_or_default(),
                hold["follow_up"].as_str().unwrap_or("none")
            );
        }
        out.push(line);
    }
    out
}
