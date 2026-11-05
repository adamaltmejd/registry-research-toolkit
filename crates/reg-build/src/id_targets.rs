//! Published identities are tied to natural targets, never build-local row ids.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::Result;
use rusqlite::Connection;
use serde::Serialize;

#[derive(PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Target {
    table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    spelling: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    period: Option<(i64, i64)>,
}

pub struct Moved {
    pub pid: String,
    pub kind: String,
    pub old: BTreeSet<Target>,
    pub new: BTreeSet<Target>,
}

impl Moved {
    pub fn text(&self) -> String {
        let targets = |set: &BTreeSet<Target>| {
            set.iter()
                .map(|target| {
                    let mut text = target.table.clone();
                    if let Some(spelling) = &target.spelling {
                        text.push(':');
                        text.push_str(spelling);
                    }
                    if let Some((start, end)) = target.period {
                        text.push(' ');
                        text.push_str(&crate::diff::span(start, end));
                    }
                    text
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "{} ({}): {} → {}",
            self.pid,
            self.kind,
            targets(&self.old),
            targets(&self.new)
        )
    }
}

type Targets = BTreeMap<(String, String), BTreeSet<Target>>;

// Schema names come only from the callers' fixed main/prev names.
fn targets(db: &Connection, schema: &str) -> Result<Targets> {
    let mut stmt = db.prepare(&format!(
        "SELECT v.pid, 'variable', t.pid, f.folded, o.start, o.end
             FROM {schema}.variable v
             LEFT JOIN {schema}.occurrence o ON o.variable_id = v.id AND o.spelling_id IS NOT NULL
             LEFT JOIN {schema}.tbl t ON t.id = o.table_id
             LEFT JOIN {schema}.spelling s ON s.id = o.spelling_id
             LEFT JOIN {schema}.name_fold f ON f.id = s.fold_id
             UNION ALL
             SELECT t.pid, 'table', n.body, NULL, NULL, NULL
             FROM {schema}.tbl t JOIN {schema}.text n ON n.id = t.name_text
             WHERE t.implicit = 0"
    ))?;
    let mut rows = stmt.query([])?;
    let mut targets = Targets::new();
    while let Some(row) = rows.next()? {
        let pid: String = row.get(0)?;
        let kind: String = row.get(1)?;
        let set = targets.entry((pid, kind)).or_default();
        if let Some(table) = row.get(2)? {
            set.insert(Target {
                table,
                spelling: row.get(3)?,
                period: row.get::<_, Option<i64>>(4)?.zip(row.get(5)?),
            });
        }
    }
    Ok(targets)
}

pub fn moved(
    old: &Connection,
    old_schema: &str,
    new: &Connection,
    new_schema: &str,
) -> Result<Vec<Moved>> {
    let mut new = targets(new, new_schema)?;
    Ok(targets(old, old_schema)?
        .into_iter()
        .filter_map(|((pid, kind), old)| {
            let new = new.remove(&(pid.clone(), kind.clone()))?;
            // simplify: pairwise scan per id; index by table/spelling if one id's
            // occurrence targets exceed a few thousand.
            let overlaps = old.iter().any(|o| {
                new.iter().any(|n| {
                    o.table == n.table
                        && o.spelling == n.spelling
                        && match (o.period, n.period) {
                            (Some((os, oe)), Some((ns, ne))) => os < ne && ns < oe,
                            (None, None) => true, // Explicit tables target their name alone.
                            _ => false,
                        }
                })
            });
            (!old.is_empty() && !new.is_empty() && !overlaps).then_some(Moved {
                pid,
                kind,
                old,
                new,
            })
        })
        .collect())
}
