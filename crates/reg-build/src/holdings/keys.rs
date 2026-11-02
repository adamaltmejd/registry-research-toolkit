//! Pinned entity/time key obligations. Identifier roles alone do not make keys.
use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, params};

use super::{compile, input};

type Span = (i64, i64);

fn periods(db: &Connection, holding: i64) -> Result<Vec<Span>, String> {
    db.prepare("SELECT start,end FROM holding_period WHERE holding_id=?")
        .map_err(|e| e.to_string())?
        .query_map([holding], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(|e| e.to_string())?
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())
}

/// A table period supplies one constant key value, never a pooled row's year.
fn constant_period(db: &Connection, spans: &[Span], encoding: &str) -> Result<bool, String> {
    let width = match encoding {
        "year" => 4,
        "month" => 7,
        "day" => 10,
        "period" => return Ok(!spans.is_empty()),
        _ => return Err("holdings_period_key_invalid: expected year, month or day".into()),
    };
    let mut values = BTreeSet::new();
    for (start, end) in spans {
        let (lo, hi): (Option<String>, Option<String>) = db
            .query_row(
                "SELECT date(?1*86400,'unixepoch'),date((?2-1)*86400,'unixepoch')",
                params![start, end],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|e| e.to_string())?;
        let (Some(lo), Some(hi)) = (lo, hi) else {
            return Ok(false);
        };
        if lo.get(..width) != hi.get(..width) {
            return Ok(false);
        }
        values.insert(lo[..width].to_owned());
    }
    Ok(values.len() == 1)
}

fn validate(
    db: &Connection,
    base: &Connection,
    holding: i64,
    key: &input::Key,
) -> Result<(), String> {
    let invalid = || {
        format!(
            "holdings:table={holding}: {}: holdings_row_key_invalid",
            key.locator
        )
    };
    if key.hold.is_none() && key.entity.is_empty()
        || key.locator.trim().is_empty()
        || key.entity.iter().collect::<BTreeSet<_>>().len() != key.entity.len()
    {
        return Err(invalid());
    }
    let exists = |table: &str, pid: &str| -> Result<bool, String> {
        for conn in [db, base] {
            if conn
                .query_row(
                    &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE pid=?)"),
                    [pid],
                    |r| r.get::<_, bool>(0),
                )
                .map_err(|e| e.to_string())?
            {
                return Ok(true);
            }
        }
        Ok(false)
    };
    if let Some(reason) = &key.hold {
        if reason.trim().is_empty()
            || key.follow_up.as_deref().is_none_or(|v| v.trim().is_empty())
            || key.period.is_some()
        {
            return Err(invalid());
        }
        return Ok(());
    }
    for pid in key.entity.iter().chain(key.time.iter()) {
        if !exists("variable", pid)? {
            return Err(invalid());
        }
    }
    if key.period_key.as_deref() == Some("column") {
        let supplied: bool = db.query_row("SELECT period_column_id IS NOT NULL AND period_encoding IS NOT NULL FROM holding_table WHERE id=?", [holding], |r| r.get(0)).map_err(|e| e.to_string())?;
        return if supplied {
            Ok(())
        } else {
            Err(format!(
                "holdings:table={holding}: {}: holdings_period_key_invalid: no period column",
                key.locator
            ))
        };
    }
    if let Some(encoding) = &key.period_key
        && !constant_period(db, &periods(db, holding)?, encoding)?
    {
        return Err(format!(
            "holdings:table={holding}: {}: holdings_period_key_invalid: table does not supply one constant value",
            key.locator
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) fn verify(db: &Connection, base: &Connection) -> Result<(), String> {
    let mut declarations = BTreeMap::<i64, Vec<input::Key>>::new();
    let mut tables = db
        .prepare("SELECT id,key_requirements FROM holding_table")
        .map_err(|e| e.to_string())?;
    for row in tables
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .map_err(|e| e.to_string())?
    {
        let (holding, json) = row.map_err(|e| e.to_string())?;
        let keys: Vec<input::Key> =
            serde_json::from_str(&json).map_err(|_| "holdings_row_key_invalid")?;
        for key in &keys {
            validate(db, base, holding, key)?;
        }
        declarations.insert(holding, keys);
    }
    let mut held = BTreeMap::<(i64, String), Vec<Span>>::new();
    let mut query = db.prepare("SELECT c.holding_id,m.variable_pid,m.start,m.end FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id").map_err(|e| e.to_string())?;
    for row in query
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .map_err(|e| e.to_string())?
    {
        let (holding, pid, start, end) = row.map_err(|e| e.to_string())?;
        held.entry((holding, pid)).or_default().push((start, end));
    }
    let mut failures = BTreeSet::new();
    let mut mappings = db.prepare("SELECT c.holding_id,m.table_pid,m.variable_pid,m.start,m.end,m.carried FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id ORDER BY m.id").map_err(|e| e.to_string())?;
    for row in mappings
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, bool>(5)?,
            ))
        })
        .map_err(|e| e.to_string())?
    {
        let (holding, table, variable, start, end, carried) = row.map_err(|e| e.to_string())?;
        let keys = &declarations[&holding];
        let mut applicable = Vec::new();
        let mut cuts = BTreeSet::from([start, end]);
        for key in keys {
            let scope = key
                .period
                .as_deref()
                .map(compile::periods)
                .transpose()?
                .unwrap_or_else(|| vec![(start, end)]);
            let scope = compile::overlap(&[(start, end)], &scope);
            for (a, b) in &scope {
                cuts.extend([*a, *b]);
            }
            applicable.push((key, scope));
        }
        let cuts: Vec<_> = cuts.into_iter().collect();
        for slice in cuts.windows(2) {
            let span = (slice[0], slice[1]);
            let active: Vec<_> = applicable
                .iter()
                .filter(|(_, s)| compile::covered(&[span], s))
                .collect();
            let [entry] = active.as_slice() else {
                failures.insert(format!("holdings:table={holding}: route={table}: period={}..{}: holdings_row_key_undeclared_or_ambiguous", span.0, span.1));
                continue;
            };
            let key = entry.0;
            if key.hold.is_some() {
                failures.insert(format!(
                    "holdings:table={holding}: {}: holdings_held_key_mapping",
                    key.locator
                ));
                continue;
            }

            if carried && !key.entity.contains(&variable) && !key.time.contains(&variable) {
                failures.insert(format!(
                    "holdings:table={holding}: {}: holdings_carried_key_invalid",
                    key.locator
                ));
            }
            for pid in key.entity.iter().chain(key.time.iter()) {
                let available = held.get(&(holding, pid.clone()));
                if available.is_none_or(|s| !compile::covered(&[span], s)) {
                    failures.insert(format!(
                        "holdings:table={holding}: key={pid}: period={}..{}: holdings_key_missing",
                        span.0, span.1
                    ));
                }
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.into_iter().collect::<Vec<_>>().join("\n"))
    }
}
