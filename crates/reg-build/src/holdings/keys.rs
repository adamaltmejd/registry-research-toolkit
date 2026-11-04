//! Pinned entity/time key obligations. Identifier roles alone do not make keys.
use std::collections::{BTreeMap, BTreeSet};

use reg_core::days::{self, Span};
use rusqlite::{Connection, params};

use super::{compile, input};

type Declarations = BTreeMap<i64, Vec<input::RowKey>>;
type Bindings = BTreeMap<(i64, String), Vec<(i64, Span)>>;

pub(super) fn validate_shapes(holding: i64, keys: &[input::RowKey]) -> Result<(), String> {
    for key in keys {
        let components: Vec<_> = key.entity.iter().chain(&key.time).collect();
        if key.hold.is_none() && key.entity.is_empty()
            || key.locator.trim().is_empty()
            || components.iter().collect::<BTreeSet<_>>().len() != components.len()
            || key.hold.as_ref().is_some_and(|reason| {
                keys.len() != 1
                    || reason.trim().is_empty()
                    || key
                        .follow_up
                        .as_deref()
                        .is_none_or(|value| value.trim().is_empty())
                    || key.period.is_some()
            })
        {
            return Err(format!(
                "holdings:table={holding}: {}: holdings_row_key_invalid",
                key.locator
            ));
        }
    }
    Ok(())
}

fn declarations(db: &Connection) -> Result<Declarations, String> {
    db.prepare("SELECT id,key_requirements FROM holding_table")
        .map_err(|e| e.to_string())?
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (holding, json) = row.map_err(|e| e.to_string())?;
            let keys = serde_json::from_str(&json)
                .map_err(|_| format!("holdings:table={holding}: holdings_row_key_invalid"))?;
            Ok((holding, keys))
        })
        .collect()
}

fn bindings(
    db: &Connection,
    routes: Option<&BTreeMap<i64, BTreeSet<String>>>,
) -> Result<Bindings, String> {
    let mut held = BTreeMap::new();
    let mut query = db.prepare("SELECT c.holding_id,m.table_pid,m.variable_pid,c.id,m.start,m.end FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id").map_err(|e| e.to_string())?;
    for row in query
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(|e| e.to_string())?
    {
        let (holding, route, pid, column, start, end) = row.map_err(|e| e.to_string())?;
        if routes.is_some_and(|routes| !routes[&holding].contains(&route)) {
            continue;
        }
        held.entry((holding, pid))
            .or_insert_with(Vec::new)
            .push((column, (start, end)));
    }
    Ok(held)
}

fn key_scope(key: &input::RowKey, span: Span) -> Result<Vec<Span>, String> {
    key.period
        .as_deref()
        .map(compile::periods)
        .transpose()
        .map(|scope| days::overlap(&[span], &scope.unwrap_or_else(|| vec![span])))
}

fn held_spans(held: &Bindings, holding: i64, pid: &str) -> Vec<Span> {
    held.get(&(holding, pid.to_owned()))
        .into_iter()
        .flatten()
        .map(|(_, span)| *span)
        .collect()
}

/// The projection shares only declared parent/child entity or time components.
/// Child-only data and child-only key components are ordinary child placements.
fn projected_copies(
    db: &Connection,
    declarations: &Declarations,
    held: &Bindings,
    failures: &mut BTreeSet<String>,
) -> Result<BTreeSet<i64>, String> {
    let mut carried = BTreeSet::new();
    let mut query = db.prepare("SELECT m.id,h.id,h.projection_of,m.variable_pid,m.start,m.end FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id JOIN holding_table h ON h.id=c.holding_id WHERE h.projection_of IS NOT NULL").map_err(|e| e.to_string())?;
    for row in query
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(|e| e.to_string())?
    {
        let (id, child, parent, variable, start, end) = row.map_err(|e| e.to_string())?;
        let mut spans = vec![(start, end)];
        for holding in [child, parent] {
            for key in &declarations[&holding] {
                spans.extend(key_scope(key, (start, end))?);
            }
        }
        let mut copied = true;
        for span in days::slices(spans) {
            let mut active = Vec::new();
            for holding in [child, parent] {
                let mut keys = Vec::new();
                for key in &declarations[&holding] {
                    if key.hold.is_none() && days::covered(&[span], &key_scope(key, span)?) {
                        keys.push(key);
                    }
                }
                active.push(keys);
            }
            let shared: BTreeSet<_> = match (active[0].as_slice(), active[1].as_slice()) {
                ([child], [parent]) => child
                    .entity
                    .iter()
                    .filter(|pid| parent.entity.contains(pid))
                    .chain(child.time.iter().filter(|pid| parent.time.contains(pid)))
                    .collect(),
                _ => BTreeSet::new(),
            };
            if shared.is_empty() {
                failures.insert(format!("holdings:table={child}:parent={parent}:period={}..{}: holdings_projection_key_missing: no shared declared entity/time component", span.0, span.1));
            }
            copied &= shared.contains(&variable);
            for pid in shared {
                if !days::covered(&[span], &held_spans(held, parent, pid)) {
                    copied = false;
                    failures.insert(format!("holdings:table={child}:parent={parent}:key={pid}:period={}..{}: holdings_projection_key_missing: parent does not hold shared component", span.0, span.1));
                }
            }
        }
        if copied {
            carried.insert(id);
        }
    }
    Ok(carried)
}

pub(super) fn carry_projections(db: &Connection) -> Result<(), String> {
    let declarations = declarations(db)?;
    let held = bindings(db, None)?;
    let mut failures = BTreeSet::new();
    for id in projected_copies(db, &declarations, &held, &mut failures)? {
        db.execute("UPDATE holding_mapping SET carried=1 WHERE id=?", [id])
            .map_err(|e| e.to_string())?;
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.into_iter().collect::<Vec<_>>().join("\n"))
    }
}

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
        "period" => {
            let merged = days::merge(spans.to_vec());
            return Ok(matches!(merged.as_slice(), [(start, end)] if
                *start > crate::emit::start(reg_frame::OPEN_START)
                    && *end < crate::emit::end(reg_frame::OPEN_END)
                    && start < end));
        }
        _ => return Err("holdings_period_key_invalid: expected year, month, day or period".into()),
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
    key: &input::RowKey,
) -> Result<(), String> {
    let invalid = || {
        format!(
            "holdings:table={holding}: {}: holdings_row_key_invalid",
            key.locator
        )
    };
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
    if key.hold.is_some() {
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
pub(super) fn verify(
    db: &Connection,
    base: &Connection,
    topology: &input::Topology,
) -> Result<(), String> {
    let declarations = declarations(db)?;
    let mut routes = BTreeMap::new();
    for table in &topology.table {
        let holding: i64 = db
            .query_row(
                "SELECT id FROM holding_table WHERE physical_name=?",
                [&table.physical_name],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())?;
        let authored: BTreeSet<_> = table
            .routes
            .iter()
            .cloned()
            .chain(
                table
                    .column
                    .iter()
                    .flat_map(|column| &column.mapping)
                    .map(|mapping| mapping.table_pid.clone()),
            )
            .collect();
        routes.insert(holding, authored);
    }
    let held = bindings(db, Some(&routes))?;
    let mut failures = BTreeSet::new();
    for (holding, keys) in &declarations {
        validate_shapes(*holding, keys)?;
        for key in keys {
            validate(db, base, *holding, key)?;
            if key.hold.is_some() {
                continue;
            }
            let physical = periods(db, *holding)?;
            let scope = key
                .period
                .as_deref()
                .map(compile::periods)
                .transpose()?
                .unwrap_or_else(|| physical.clone());
            let scope = days::overlap(&scope, &physical);
            for pid in key.entity.iter().chain(&key.time) {
                if days::overlap(&scope, &held_spans(&held, *holding, pid)).is_empty() {
                    failures.insert(format!(
                        "holdings:table={holding}:key={pid}: {}: holdings_key_missing: declared component is not mapped on an authored route",
                        key.locator
                    ));
                }
            }
        }
    }
    let projection_copies = projected_copies(db, &declarations, &held, &mut failures)?;
    let mut mappings = db.prepare("SELECT c.holding_id,m.table_pid,m.variable_pid,m.start,m.end,m.carried,m.id,h.projection_of FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id JOIN holding_table h ON h.id=c.holding_id ORDER BY m.id").map_err(|e| e.to_string())?;
    for row in mappings
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, bool>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, Option<i64>>(7)?,
            ))
        })
        .map_err(|e| e.to_string())?
    {
        let (holding, table, variable, start, end, carried, id, projection) =
            row.map_err(|e| e.to_string())?;
        let keys = &declarations[&holding];
        let mut applicable = Vec::new();
        let mut spans = vec![(start, end)];
        for key in keys {
            let scope = key
                .period
                .as_deref()
                .map(compile::periods)
                .transpose()?
                .unwrap_or_else(|| vec![(start, end)]);
            let scope = days::overlap(&[(start, end)], &scope);
            spans.extend(&scope);
            applicable.push((key, scope));
        }
        for span in days::slices(spans) {
            let active: Vec<_> = applicable
                .iter()
                .filter(|(_, s)| days::covered(&[span], s))
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

            if carried
                && (!key.entity.contains(&variable) && !key.time.contains(&variable)
                    || projection.is_some() && !projection_copies.contains(&id))
            {
                failures.insert(format!(
                    "holdings:table={holding}: {}: holdings_carried_key_invalid",
                    key.locator
                ));
            }
            for pid in key.entity.iter().chain(key.time.iter()) {
                let available = held.get(&(holding, pid.clone()));
                let bindings = available.map_or(&[][..], Vec::as_slice);
                let intervals: Vec<_> = bindings.iter().map(|(_, s)| *s).collect();
                if !days::covered(&[span], &intervals) {
                    failures.insert(format!(
                        "holdings:table={holding}: key={pid}: period={}..{}: holdings_key_missing",
                        span.0, span.1
                    ));
                }
                // simplify: pairwise within one physical key; index if a key exceeds 1,000 bindings.
                for (column, interval) in bindings {
                    if bindings.iter().any(|(other, reach)| {
                        other != column
                            && !days::overlap(&[span], &days::overlap(&[*interval], &[*reach]))
                                .is_empty()
                    }) {
                        failures.insert(format!(
                            "holdings:table={holding}: key={pid}: holdings_key_ambiguous"
                        ));
                    }
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
