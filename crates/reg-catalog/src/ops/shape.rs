//! The result shapes several operations share, with their loaders: `ValueSetRef`
//! (segments, values, schema, show, coverage) and `Release` (show, coverage,
//! segments).

use rusqlite::{Connection, Row};
use serde::Serialize;
use utoipa::ToSchema;

use super::{Period, Server, iso_day};
use crate::Error;

/// `shape.ValueSetRef`: a value set by its hash, with the names sources gave it.
#[derive(Serialize, ToSchema)]
pub struct ValueSetRef {
    hash: String,
    n_items: i64,
    hierarchical: bool,
    names: Vec<String>,
    /// The curated classification version's ref; `null` until a version is curated.
    #[schema(required = true)]
    classification: Option<String>,
}

/// `valueset_id`'s [`ValueSetRef`].
///
/// # Errors
///
/// `internal_error` when the query fails or no such value set exists.
pub fn value_set_ref(conn: &Connection, valueset_id: i64) -> Result<ValueSetRef, Error> {
    let mut value_set = conn.query_row(
        "SELECT v.hash, v.n_items, v.hierarchical, c.pid
         FROM valueset v LEFT JOIN class_version c ON c.id = v.class_version_id
         WHERE v.id = ?",
        [valueset_id],
        |row| {
            Ok(ValueSetRef {
                hash: row.get(0)?,
                n_items: row.get(1)?,
                hierarchical: row.get(2)?,
                names: vec![],
                classification: row.get(3)?,
            })
        },
    )?;
    value_set.names = conn
        .prepare(
            "SELECT DISTINCT t.body
             FROM valueset_name n JOIN text t ON t.id = n.name_text
             WHERE n.valueset_id = ? ORDER BY t.body",
        )?
        .query_map([valueset_id], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(value_set)
}

/// `shape.Release`: a delivery of a table.
#[derive(Serialize, ToSchema)]
pub struct Release {
    name: String,
    /// `null` when the release is unparsed.
    #[schema(required = true)]
    period: Option<Period>,
    edition: String,
    qualifier: String,
    /// `null` when the release is unparsed.
    #[schema(required = true)]
    primary: Option<bool>,
    parse_state: String,
    populations: Vec<Population>,
}

/// One population a release states (`shape.Release.populations`).
#[derive(Serialize, ToSchema)]
pub struct Population {
    name: String,
    #[schema(required = true)]
    definition: Option<String>,
    /// The ISO day the population is stated for.
    #[schema(required = true)]
    ref_day: Option<String>,
}

const RELEASE_COLUMNS: &str = "SELECT r.id, n.body, r.start, r.end, r.edition, r.qualifier,
        r.is_primary, r.parse_state
    FROM release r JOIN text n ON n.id = r.name_text";

fn release_row(server: &Server, row: &Row) -> rusqlite::Result<(i64, Release)> {
    let start: Option<i64> = row.get(2)?;
    let end: Option<i64> = row.get(3)?;
    Ok((
        row.get(0)?,
        Release {
            name: row.get(1)?,
            period: start.zip(end).map(|(s, e)| server.period(s, e)),
            edition: row.get(4)?,
            qualifier: row.get(5)?,
            primary: row.get(6)?,
            parse_state: row.get(7)?,
            populations: vec![],
        },
    ))
}

fn populations(conn: &Connection, release_id: i64) -> rusqlite::Result<Vec<Population>> {
    conn.prepare(
        "SELECT n.body, d.body, p.ref_day
         FROM release_population p
           JOIN text n ON n.id = p.name_text
           LEFT JOIN text d ON d.id = p.definition_text
         WHERE p.release_id = ? ORDER BY p.ord",
    )?
    .query_map([release_id], |row| {
        Ok(Population {
            name: row.get(0)?,
            definition: row.get(1)?,
            ref_day: row.get::<_, Option<i64>>(2)?.map(iso_day),
        })
    })?
    .collect()
}

fn with_populations(conn: &Connection, rows: Vec<(i64, Release)>) -> Result<Vec<Release>, Error> {
    rows.into_iter()
        .map(|(id, mut release)| {
            release.populations = populations(conn, id)?;
            Ok(release)
        })
        .collect()
}

/// A table's releases: unparsed last, then by period, qualifier, edition and source
/// key, so the order is total.
///
/// # Errors
///
/// `internal_error` when a query fails.
pub fn releases(server: &Server, conn: &Connection, table_id: i64) -> Result<Vec<Release>, Error> {
    let rows = conn
        .prepare(&format!(
            "{RELEASE_COLUMNS} WHERE r.table_id = ?
             ORDER BY r.start IS NULL, r.start, r.end, r.qualifier, r.edition, r.source_key"
        ))?
        .query_map([table_id], |row| release_row(server, row))?
        .collect::<rusqlite::Result<_>>()?;
    with_populations(conn, rows)
}

/// One release.
///
/// # Errors
///
/// `internal_error` when a query fails or no such release exists.
pub fn release(server: &Server, conn: &Connection, release_id: i64) -> Result<Release, Error> {
    let row = conn.query_row(
        &format!("{RELEASE_COLUMNS} WHERE r.id = ?"),
        [release_id],
        |row| release_row(server, row),
    )?;
    Ok(with_populations(conn, vec![row])?.remove(0))
}
