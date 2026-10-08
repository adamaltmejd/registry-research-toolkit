//! A register's browse deliveries and the coverage `show` lists them with: today's
//! `register_variable_deliveries`, `register_variable_coverage`,
//! `register_column_coverage` and `provider_register_coverage`.
//!
//! Deliveries are the compiled `browse_delivery` rows of the scope. Holdings
//! coverage folds the held deliveries, as today. Reference coverage counts states, as
//! today, by one `GROUP BY` over the register's states (today's ~9 ms on the largest
//! register), not by folding deliveries: a delivery also counts its alias windows.

use std::collections::BTreeMap;

use rusqlite::Connection;
use serde::Serialize;
use utoipa::ToSchema;

use super::rows;
use crate::{Error, Scope};

/// The start of a state whose start is unknown, and the end of one still delivered.
const UNKNOWN_FROM: &str = "0001-01-01";
const OPEN_ENDED_TO: &str = "9999-12-31";

/// The span a variable or column is delivered over: its earliest start and latest
/// finite end (null when unknown, or when `open_ended`), and the states (held
/// periods in holdings) behind it.
#[derive(Clone, Serialize, ToSchema)]
pub struct Coverage {
    #[serde(rename = "coverage_from")]
    from: Option<String>,
    #[serde(rename = "coverage_to")]
    to: Option<String>,
    open_ended: bool,
    state_count: i64,
}

impl Coverage {
    /// Today's `_coverage_bounds` over a `(MIN(valid_from), MAX(valid_to))`.
    fn new(from: Option<String>, to: Option<String>, state_count: i64) -> Self {
        let open_ended = to.as_deref() == Some(OPEN_ENDED_TO);
        Self {
            from: from.filter(|f| f != UNKNOWN_FROM),
            to: to.filter(|_| !open_ended),
            open_ended,
            state_count,
        }
    }

    /// Today's `_delivery_coverage`: the span of the deliveries' windows, their
    /// counts summed.
    fn of<'a>(deliveries: impl Iterator<Item = &'a Delivery> + Clone) -> Self {
        let windows = deliveries.clone().flat_map(|d| &d.windows);
        Self::new(
            windows.clone().map(|w| w.valid_from.clone()).min(),
            windows.map(|w| w.valid_to.clone()).max(),
            deliveries.map(|d| d.coverage.state_count).sum(),
        )
    }
}

/// A register's span: its variables and the earliest and latest of their states.
#[derive(Serialize, ToSchema)]
pub struct RegisterCoverage {
    variable_count: i64,
    coverage_from: Option<String>,
    coverage_to: Option<String>,
    open_ended: bool,
}

/// One `(variant, delivery column)` a variable is delivered under, with its disjoint
/// windows by start and their span. `column` is null for a state SCB named no
/// column for.
#[derive(Serialize, ToSchema)]
pub struct Delivery {
    variant: String,
    column: Option<String>,
    /// `intervals`, or `year_independent` (no windows).
    period_scope: String,
    coverage: Coverage,
    windows: Vec<Window>,
}

#[derive(Serialize, ToSchema)]
pub struct Window {
    valid_from: String,
    valid_to: String,
}

fn scope_name(scope: Scope) -> &'static str {
    match scope {
        Scope::Reference => "reference",
        Scope::Holdings => "holdings",
    }
}

/// A register's deliveries in scope, by variable id, each variable's in today's
/// order.
pub(super) fn deliveries(
    conn: &Connection,
    scope: Scope,
    register_id: i64,
) -> Result<BTreeMap<i64, Vec<Delivery>>, Error> {
    let mut windows: BTreeMap<i64, Vec<Window>> = BTreeMap::new();
    for (id, window) in rows(
        conn,
        "SELECT dw.browse_delivery_id, dw.valid_from, dw.valid_to FROM browse_delivery bd \
         JOIN variable v USING(variable_id) \
         JOIN delivery_window dw ON dw.browse_delivery_id = bd.browse_delivery_id \
         WHERE bd.scope = ?1 AND v.register_id = ?2 ORDER BY 1, 2",
        (scope_name(scope), register_id),
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                Window {
                    valid_from: row.get(1)?,
                    valid_to: row.get(2)?,
                },
            ))
        },
    )? {
        windows.entry(id).or_default().push(window);
    }
    let found = rows(
        conn,
        "SELECT bd.browse_delivery_id, bd.variable_id, rv.slug, bd.delivery_column_name, \
         bd.period_scope, bd.state_count FROM browse_delivery bd \
         JOIN variable v USING(variable_id) \
         JOIN register_variant rv ON rv.register_variant_id = bd.register_variant_id \
         WHERE bd.scope = ?1 AND v.register_id = ?2 ORDER BY bd.browse_delivery_id",
        (scope_name(scope), register_id),
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    let mut out: BTreeMap<i64, Vec<Delivery>> = BTreeMap::new();
    for (id, variable_id, variant, column, period_scope, state_count) in found {
        let windows = windows.remove(&id).unwrap_or_default();
        // The windows are disjoint and ordered, so the last ends latest.
        let coverage = Coverage::new(
            windows.first().map(|w| w.valid_from.clone()),
            windows.last().map(|w| w.valid_to.clone()),
            state_count,
        );
        out.entry(variable_id).or_default().push(Delivery {
            variant,
            column,
            period_scope,
            coverage,
            windows,
        });
    }
    Ok(out)
}

/// Each variable's coverage, by variable id: in holdings over its held deliveries
/// (none without one); in reference over the states of every slugged variable.
pub(super) fn variable_coverage(
    conn: &Connection,
    scope: Scope,
    register_id: i64,
    deliveries: &BTreeMap<i64, Vec<Delivery>>,
) -> Result<BTreeMap<i64, Coverage>, Error> {
    if scope == Scope::Holdings {
        return Ok(deliveries
            .iter()
            .map(|(id, offered)| (*id, Coverage::of(offered.iter())))
            .collect());
    }
    Ok(rows(
        conn,
        "SELECT v.variable_id, MIN(vs.valid_from), MAX(vs.valid_to), COUNT(vs.state_id) \
         FROM variable v LEFT JOIN variable_state vs ON vs.variable_id = v.variable_id \
         WHERE v.register_id = ? AND v.slug IS NOT NULL GROUP BY v.variable_id",
        [register_id],
        |row| {
            Ok((
                row.get(0)?,
                Coverage::new(row.get(1)?, row.get(2)?, row.get(3)?),
            ))
        },
    )?
    .into_iter()
    .collect())
}

/// Each named delivery column's coverage, by variable id and the column's
/// `fold_identity` (a curated member names its column in any spelling): in holdings
/// over the held deliveries of that spelling (the last spelling of a fold, by byte
/// order, answers, as today); in reference over the states of every spelling of the
/// fold.
fn column_coverage(
    conn: &Connection,
    scope: Scope,
    register_id: i64,
    deliveries: &BTreeMap<i64, Vec<Delivery>>,
) -> Result<BTreeMap<(i64, String), Coverage>, Error> {
    let mut out = BTreeMap::new();
    if scope == Scope::Holdings {
        for (id, offered) in deliveries {
            let mut columns: Vec<&str> =
                offered.iter().filter_map(|d| d.column.as_deref()).collect();
            columns.sort_unstable();
            columns.dedup();
            for column in columns {
                let coverage = Coverage::of(
                    offered
                        .iter()
                        .filter(|d| d.column.as_deref() == Some(column)),
                );
                out.insert((*id, reg_core::fold_identity(column)), coverage);
            }
        }
        return Ok(out);
    }
    // A state's bounds may be null; a merge keeps SQL's MIN and MAX, which skip nulls.
    let mut spans: BTreeMap<(i64, String), Span> = BTreeMap::new();
    for (id, column, from, to, count) in rows(
        conn,
        "SELECT v.variable_id, vs.delivery_column_name, MIN(vs.valid_from), MAX(vs.valid_to), \
         COUNT(vs.state_id) FROM variable v JOIN variable_state vs \
         ON vs.variable_id = v.variable_id WHERE v.register_id = ? AND v.slug IS NOT NULL \
         AND vs.delivery_column_name IS NOT NULL GROUP BY v.variable_id, vs.delivery_column_name",
        [register_id],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        },
    )? {
        let span = spans
            .entry((id, reg_core::fold_identity(&column)))
            .or_insert((None, None, 0));
        span.0 = [span.0.take(), from].into_iter().flatten().min();
        span.1 = [span.1.take(), to].into_iter().flatten().max();
        span.2 += count;
    }
    out.extend(
        spans
            .into_iter()
            .map(|(key, (from, to, count))| (key, Coverage::new(from, to, count))),
    );
    Ok(out)
}

/// A merged `(MIN(valid_from), MAX(valid_to), COUNT(*))`.
type Span = (Option<String>, Option<String>, i64);

/// The coverage of a register's group members, as today's group node zips it.
pub(super) struct MemberCoverage {
    variables: BTreeMap<i64, Coverage>,
    columns: BTreeMap<(i64, String), Coverage>,
}

impl MemberCoverage {
    pub(super) fn new(conn: &Connection, scope: Scope, register_id: i64) -> Result<Self, Error> {
        let deliveries = deliveries(conn, scope, register_id)?;
        Ok(Self {
            variables: variable_coverage(conn, scope, register_id, &deliveries)?,
            columns: column_coverage(conn, scope, register_id, &deliveries)?,
        })
    }

    /// A whole-variable member's variable coverage; a representation member's
    /// column's, or an empty one when no state delivers its column (the column is
    /// known never delivered, not delivered through its siblings).
    pub(super) fn of(&self, variable_id: i64, column: Option<&str>) -> Option<Coverage> {
        match column {
            Some(column) => Some(
                self.columns
                    .get(&(variable_id, reg_core::fold_identity(column)))
                    .cloned()
                    .unwrap_or_else(|| Coverage::new(None, None, 0)),
            ),
            None => self.variables.get(&variable_id).cloned(),
        }
    }
}

/// Each register's coverage, by register id, for the provider's slugged registers:
/// in holdings its variables with held deliveries over their windows; in reference
/// its slugged variables over their states.
pub(super) fn register_coverage(
    conn: &Connection,
    scope: Scope,
    provider_id: i64,
) -> Result<BTreeMap<i64, RegisterCoverage>, Error> {
    let sql = match scope {
        Scope::Holdings => {
            "SELECT r.register_id, COUNT(DISTINCT bd.variable_id), MIN(dw.valid_from), \
             MAX(dw.valid_to) FROM register r \
             LEFT JOIN variable v ON v.register_id = r.register_id \
             LEFT JOIN browse_delivery bd ON bd.variable_id = v.variable_id \
             AND bd.scope = 'holdings' \
             LEFT JOIN delivery_window dw ON dw.browse_delivery_id = bd.browse_delivery_id \
             WHERE r.provider_id = ? AND r.slug IS NOT NULL GROUP BY r.register_id"
        }
        Scope::Reference => {
            "SELECT r.register_id, COUNT(DISTINCT v.variable_id), MIN(vs.valid_from), \
             MAX(vs.valid_to) FROM register r \
             LEFT JOIN variable v ON v.register_id = r.register_id AND v.slug IS NOT NULL \
             LEFT JOIN variable_state vs ON vs.variable_id = v.variable_id \
             WHERE r.provider_id = ? AND r.slug IS NOT NULL GROUP BY r.register_id"
        }
    };
    Ok(rows(conn, sql, [provider_id], |row| {
        let span = Coverage::new(row.get(2)?, row.get(3)?, 0);
        Ok((
            row.get(0)?,
            RegisterCoverage {
                variable_count: row.get(1)?,
                coverage_from: span.from,
                coverage_to: span.to,
                open_ended: span.open_ended,
            },
        ))
    })?
    .into_iter()
    .collect())
}
