//! Explicit same-identity supplemental occurrences. Public ids cross the pair;
//! source occurrences, their local ids and provenance remain in the steward file.
use super::{compile, input};
use rusqlite::{Connection, params};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub(super) struct Occurrence {
    table_pid: String,
    variable_pid: String,
    locator: String,
    evidence: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct References {
    occurrence: Vec<Occurrence>,
}

pub(super) fn read(root: &Path, config: &input::Config) -> Result<BTreeSet<Occurrence>, String> {
    let Some(path) = &config.inputs.reference_occurrences else {
        return Ok(BTreeSet::new());
    };
    let input: References = crate::toml_file(&input::path(root, path)?)
        .map_err(|_| "holdings_reference_occurrence_invalid: malformed declarations")?;
    let mut out = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for (index, row) in input.occurrence.into_iter().enumerate() {
        if row.locator.trim().is_empty()
            || row.evidence.trim().is_empty()
            || !keys.insert((row.table_pid.clone(), row.variable_pid.clone()))
        {
            return Err(format!(
                "reference_occurrences:occurrence={}: holdings_reference_occurrence_invalid: located evidence and unique declaration required",
                index + 1
            ));
        }
        out.insert(row);
    }
    Ok(out)
}

pub(super) fn stored(db: &Connection) -> Result<BTreeSet<Occurrence>, String> {
    db.prepare("SELECT table_pid,variable_pid,locator,evidence FROM holding_reference_occurrence")
        .map_err(|e| e.to_string())?
        .query_map([], |r| {
            Ok(Occurrence {
                table_pid: r.get(0)?,
                variable_pid: r.get(1)?,
                locator: r.get(2)?,
                evidence: r.get(3)?,
            })
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())
}

pub(super) fn write(db: &Connection, rows: &BTreeSet<Occurrence>) -> Result<(), String> {
    db.execute_batch(
        "CREATE TABLE holding_reference_occurrence (
      table_pid TEXT NOT NULL, variable_pid TEXT NOT NULL, locator TEXT NOT NULL,
      evidence TEXT NOT NULL, PRIMARY KEY(table_pid,variable_pid)) STRICT, WITHOUT ROWID;",
    )
    .map_err(|e| e.to_string())?;
    for r in rows {
        db.execute(
            "INSERT INTO holding_reference_occurrence VALUES (?,?,?,?)",
            params![r.table_pid, r.variable_pid, r.locator, r.evidence],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub(super) fn admitted(db: &Connection, base: &Connection) -> Result<BTreeSet<String>, String> {
    let mut allowed = BTreeSet::new();
    for r in stored(db)? {
        let refuse = |code| format!("{}: {code}", r.locator);
        if r.locator.trim().is_empty() || r.evidence.trim().is_empty() {
            return Err(refuse("holdings_reference_occurrence_invalid"));
        }
        let query = "SELECT v.register_id=t.register_id,r.pid,p.pid,t.timeless
         FROM variable v JOIN tbl t ON t.pid=? JOIN register r ON r.id=v.register_id
         JOIN provider p ON p.id=r.provider_id WHERE v.pid=?";
        let identity = |conn: &Connection| {
            conn.query_row(query, params![r.table_pid, r.variable_pid], |row| {
                Ok((
                    row.get::<_, bool>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            })
        };
        let a =
            identity(base).map_err(|_| refuse("holdings_reference_occurrence_target_missing"))?;
        let b = identity(db).map_err(|_| refuse("holdings_reference_occurrence_source_missing"))?;
        if a != b || !a.0 || a.3 {
            return Err(refuse("holdings_reference_occurrence_identity_mismatch"));
        }
        let intervals = |conn: &Connection| -> Result<Vec<(i64, i64)>, String> {
            conn.prepare(
                "SELECT s.start,s.end FROM segment s JOIN variable v ON v.id=s.variable_id
              JOIN tbl t ON t.id=s.table_id WHERE v.pid=? AND t.pid=?",
            )
            .map_err(|e| e.to_string())?
            .query_map(params![r.variable_pid, r.table_pid], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())
        };
        let global = intervals(base)?;
        let extra = intervals(db)?;
        if extra.is_empty() {
            return Err(refuse("holdings_reference_occurrence_empty"));
        }
        if !compile::overlap(&extra, &global).is_empty() {
            return Err(refuse("holdings_reference_occurrence_overlap"));
        }
        // One declaration owns every supplemental segment of this canonical variable.
        let other: bool = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM segment s
          JOIN variable v ON v.id=s.variable_id JOIN tbl t ON t.id=s.table_id
          WHERE v.pid=? AND NOT EXISTS(SELECT 1 FROM holding_reference_occurrence x
          WHERE x.variable_pid=v.pid AND x.table_pid=t.pid))",
                [&r.variable_pid],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if other {
            return Err(refuse("holdings_reference_occurrence_undeclared"));
        }
        allowed.extend([r.variable_pid, r.table_pid, a.1]);
    }
    Ok(allowed)
}

/// Reference registers and tables the steward extends with variables of its own: the
/// same entity in both origins (a register under the same provider, a table in the same
/// register with the same timelessness). A register and its implicit table share one
/// pid, so every steward row behind a pid must match a reference row. Readers keep the
/// reference row; only the steward's distinct variables and their segments are added.
pub(super) fn overlays(db: &Connection, base: &Connection) -> Result<BTreeSet<String>, String> {
    let entities = |conn: &Connection| -> Result<BTreeSet<(String, String, i64)>, String> {
        conn.prepare(
            "SELECT r.pid, p.pid, -1 FROM register r JOIN provider p ON p.id = r.provider_id
             UNION ALL
             SELECT t.pid, r.pid, t.timeless FROM tbl t JOIN register r ON r.id = t.register_id",
        )
        .map_err(|e| e.to_string())?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .map_err(|e| e.to_string())?
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())
    };
    let global = entities(base)?;
    let held = entities(db)?;
    let mismatched: BTreeSet<&String> = held.difference(&global).map(|(pid, ..)| pid).collect();
    Ok(held
        .intersection(&global)
        .map(|(pid, ..)| pid)
        .filter(|pid| !mismatched.contains(pid))
        .cloned()
        .collect())
}
