//! How much of a steward's delivery an order can reach (crates/DESIGN.md, "Orderable
//! columns"). An order reaches a delivered column only through its `holding_mapping`
//! rows: the server's possession and `materialize_order`'s placements read nothing else,
//! and a verified artifact has already cleared the other refusals that path can raise
//! (keys, storage, file names). So a column is orderable when it has a mapping row, and
//! every other column gets exactly one reason, read from what the compiler recorded:
//! the table's disposition and held row key first, then the column's own decision and
//! notices. Never a refusal: the build prints it as a warning and writes it to the report.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use rusqlite::Connection;
use serde_json::{Value, json};

use crate::error::Result;

/// Why a delivered column is not orderable, in precedence order: a table-level reason
/// wins over the column's own, so a held row key's stamped column holds count once,
/// under the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Reason {
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
    pub(crate) const ALL: [Self; 10] = [
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

    /// A stated decision that the column is not ordered, as opposed to work still owed:
    /// an excluded, superseded or lookup table, or a permanent column `unmap`.
    pub(crate) fn decided(self) -> bool {
        matches!(
            self,
            Self::TableExcluded | Self::TableSuperseded | Self::TableLookup | Self::ColumnUnmapped
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

/// Every census column of a compiled holdings file with its reason.
pub(crate) fn columns(db: &Connection) -> Result<Vec<Column>> {
    let mut query = db.prepare(
        "SELECT t.id, t.schema_name, t.physical_name, t.disposition, t.key_requirements,
                c.name, c.disposition,
                EXISTS(SELECT 1 FROM holding_mapping m WHERE m.column_id = c.id),
                (SELECT group_concat(DISTINCT n.code) FROM holding_notice n
                 WHERE n.column_id = c.id)
         FROM holding_column c JOIN holding_table t ON t.id = c.holding_id
         ORDER BY t.id, c.ordinal",
    )?;
    let rows = query.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, String>(6)?,
            r.get::<_, bool>(7)?,
            r.get::<_, Option<String>>(8)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (table, schema, physical, table_disposition, keys, name, disposition, mapped, notices) =
            row?;
        let notices = notices.unwrap_or_default();
        let has_notice = |code: &str| notices.split(',').any(|n| n == code);
        let reason = if mapped {
            None
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
/// no decided reason is blocked as a whole.
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
    let decided: u64 = reasons
        .iter()
        .filter(|(r, _)| r.decided())
        .map(|(_, n)| n)
        .sum();
    let not_orderable = columns.len() as u64 - orderable;
    let mut listed = Vec::new();
    let mut detail = db.prepare(
        "SELECT schema_name, physical_name, key_requirements,
                (SELECT json_group_array(table_pid) FROM
                   (SELECT table_pid FROM holding_route WHERE holding_id = t.id ORDER BY table_pid))
         FROM holding_table t WHERE id = ?",
    )?;
    for (id, (total, ordered, by)) in &tables {
        if by.is_empty() {
            continue;
        }
        let (schema, name, keys, routes): (Option<String>, String, String, String) =
            detail.query_row([id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
        let holds = key_holds(&keys);
        let blocked = (*ordered == 0 && !by.keys().all(|r| r.decided())).then(|| {
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
        }));
    }
    Ok(json!({
        "columns": columns.len(),
        "orderable": orderable,
        "not_orderable": not_orderable,
        "decided": decided,
        "gaps": not_orderable - decided,
        "reasons": Reason::ALL.iter().filter_map(|r| {
            reasons.get(r).map(|n| (r.as_str().to_owned(), json!({"columns": n, "decided": r.decided()})))
        }).collect::<serde_json::Map<_, _>>(),
        "tables": listed,
    }))
}

/// The summary as warning lines: the totals, each reason, then each blocked table.
pub(crate) fn warnings(summary: &Value) -> Vec<String> {
    let n = |v: &Value| v.as_u64().unwrap_or_default();
    let mut out = vec![format!(
        "warning: holdings: orderable {} of {} delivered columns; not orderable {} \
         (still to place {}, not ordered by decision {})",
        n(&summary["orderable"]),
        n(&summary["columns"]),
        n(&summary["not_orderable"]),
        n(&summary["gaps"]),
        n(&summary["decided"]),
    )];
    let tables = summary["tables"].as_array().map_or(&[][..], Vec::as_slice);
    if let Some(reasons) = summary["reasons"].as_object() {
        for (reason, entry) in reasons {
            let in_tables = tables
                .iter()
                .filter(|t| t["reasons"].get(reason).is_some())
                .count();
            let kind = if entry["decided"].as_bool() == Some(true) {
                "by decision"
            } else {
                "to place"
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
