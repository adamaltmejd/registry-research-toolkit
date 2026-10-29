//! Published identities are tied to natural targets, never build-local row ids.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::Connection;
use serde::Serialize;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Target {
    table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    spelling: Option<String>,
}

pub struct Moved {
    pub pid: String,
    pub kind: String,
    pub old: BTreeSet<Target>,
    pub new: BTreeSet<Target>,
}

type Targets = BTreeMap<(String, String), BTreeSet<Target>>;

// Schema names come only from the callers' fixed main/prev names.
fn targets(db: &Connection, schema: &str) -> Result<Targets, String> {
    let mut stmt = db
        .prepare(&format!(
            "SELECT v.pid, 'variable', t.pid, f.folded
             FROM {schema}.variable v
             LEFT JOIN {schema}.occurrence o ON o.variable_id = v.id AND o.spelling_id IS NOT NULL
             LEFT JOIN {schema}.tbl t ON t.id = o.table_id
             LEFT JOIN {schema}.spelling s ON s.id = o.spelling_id
             LEFT JOIN {schema}.name_fold f ON f.id = s.fold_id
             UNION ALL
             SELECT t.pid, 'table', n.body, NULL
             FROM {schema}.tbl t JOIN {schema}.text n ON n.id = t.name_text
             WHERE t.implicit = 0"
        ))
        .map_err(|e| e.to_string())?;
    let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
    let mut targets = Targets::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let pid: String = row.get(0).map_err(|e| e.to_string())?;
        let kind: String = row.get(1).map_err(|e| e.to_string())?;
        let set = targets.entry((pid, kind)).or_default();
        if let Some(table) = row.get(2).map_err(|e| e.to_string())? {
            set.insert(Target {
                table,
                spelling: row.get(3).map_err(|e| e.to_string())?,
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
) -> Result<Vec<Moved>, String> {
    let mut new = targets(new, new_schema)?;
    Ok(targets(old, old_schema)?
        .into_iter()
        .filter_map(|((pid, kind), old)| {
            let new = new.remove(&(pid.clone(), kind.clone()))?;
            old.is_disjoint(&new).then_some(Moved {
                pid,
                kind,
                old,
                new,
            })
        })
        .collect())
}
