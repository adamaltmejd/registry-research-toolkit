//! Concurrent tables (crates/DESIGN.md, "Concurrent releases"): on a day two primary
//! releases of a serving table share, the releases the source names (`releases`) serve,
//! each as its own entry. A shared day none of them covers blocks the source with
//! `release_ambiguous`, listing the releases that share it.

use reg_core::project::CatalogSource;
use reg_core::{Interval, IssueLevel, fold_identity};
use rusqlite::Connection;

use super::{Issue, Serving, Table, interval, span};
use crate::ops::Server;
use crate::{Error, rows};

/// Requested days of one serving table that the same two or more primary releases
/// share.
/// Half-open day numbers.
type Days = (i64, i64);

pub(super) struct Shared {
    pub days: Interval,
    /// The releases sharing them, in start order.
    pub sharing: Vec<String>,
    /// Those the source names: they serve these days. Empty: the days block.
    pub chosen: Vec<String>,
}

/// A table's primary releases with their days (half-open day numbers; `None` unplaced),
/// in start order.
fn primaries(conn: &Connection, table: i64) -> Result<Vec<(Option<Days>, String)>, Error> {
    Ok(rows(
        conn,
        "SELECT r.start, r.end, n.body FROM main.release r
         JOIN main.text n ON n.id = r.name_text
         WHERE r.table_id = ?1 AND r.is_primary = 1
         ORDER BY r.start IS NULL, r.start, n.body",
        [table],
        |row| {
            let start: Option<i64> = row.get(0)?;
            let end: Option<i64> = row.get(1)?;
            Ok((start.zip(end), row.get(2)?))
        },
    )?)
}

/// The days two or more of table `table`'s placed primary releases share (half-open day
/// numbers), cut where the set sharing them changes, each with those releases' names in
/// start order. `show` lists them; an order serves them per named release.
pub(in crate::ops) fn sharing(
    conn: &Connection,
    table: i64,
) -> Result<Vec<(Days, Vec<String>)>, Error> {
    let placed: Vec<((i64, i64), String)> = primaries(conn, table)?
        .into_iter()
        .filter_map(|(days, name)| days.map(|d| (d, name)))
        .collect();
    let mut out: Vec<((i64, i64), Vec<String>)> = Vec::new();
    for (lo, hi) in reg_core::days::slices(placed.iter().map(|(d, _)| *d)) {
        let sharing: Vec<String> = placed
            .iter()
            .filter(|((a, b), _)| *a <= lo && hi <= *b)
            .map(|(_, name)| name.clone())
            .collect();
        if sharing.len() < 2 {
            continue;
        }
        match out.last_mut() {
            Some(((_, end), last)) if *end == lo && *last == sharing => *end = hi,
            _ => out.push(((lo, hi), sharing)),
        }
    }
    Ok(out)
}

/// The requested days of serving table `s` that two or more of its primary releases
/// share ([`sharing`]), each run with the releases of `named` among them.
// simplify: the base database's releases only; a holdings database whose own releases
// of a base table share days is the holdings schema's to model (holdings PR).
pub(super) fn shared(
    conn: &Connection,
    s: &Serving,
    named: &[String],
) -> Result<Vec<Shared>, Error> {
    let requested = super::days(&s.requested);
    let mut out = Vec::new();
    for (days, sharing) in sharing(conn, s.table.id)? {
        for (lo, hi) in reg_core::days::overlap(&[days], &requested) {
            out.push(Shared {
                days: interval(lo, hi),
                chosen: sharing
                    .iter()
                    .filter(|n| named.contains(n))
                    .cloned()
                    .collect(),
                sharing: sharing.clone(),
            });
        }
    }
    Ok(out)
}

/// The source's `releases` as the primary release names of its table, and the findings
/// refusing the others, located at each: `release_unresolved` (no primary of the table
/// has the name under the identity fold; lists the names) and `release_outside_period`
/// (the release has no requested day).
// simplify: names resolve against the source's table only. No concurrent table is in a
// table succession today (0.87.0 candidate), so a successor's shared days still block
// whatever is named; resolve per serving table if one ever is.
pub(super) fn named(
    server: &Server,
    conn: &Connection,
    table: &Table,
    index: usize,
    source: &CatalogSource,
    requested: &[Interval],
) -> Result<(Vec<String>, Vec<Issue>), Error> {
    let Some(asked) = &source.releases else {
        return Ok((Vec::new(), Vec::new()));
    };
    let primaries = primaries(conn, table.id)?;
    let requested = super::days(requested);
    let mut named = Vec::new();
    let mut found = Vec::new();
    let issue = |k: usize, code: &str, message: String| Issue {
        source: Some(source.name.clone()),
        table: Some(table.pid.clone()),
        ..Issue::new(
            IssueLevel::Error,
            code,
            format!("/sources/{index}/releases/{k}"),
            message,
        )
    };
    for (k, name) in asked.iter().enumerate() {
        let folded = fold_identity(name);
        let Some((days, body)) = primaries.iter().find(|(_, body)| body == name).or_else(|| {
            primaries
                .iter()
                .find(|(_, body)| fold_identity(body) == folded)
        }) else {
            let listed: Vec<String> = primaries.iter().map(|(_, n)| format!("\"{n}\"")).collect();
            found.push(issue(
                k,
                "release_unresolved",
                format!(
                    "{} has no release named \"{name}\"; its releases are {}",
                    table.pid,
                    listed.join(", ")
                ),
            ));
            continue;
        };
        let meets = days.is_some_and(|(lo, hi)| requested.iter().any(|(a, b)| *a < hi && lo < *b));
        if !meets {
            let when = days.map_or_else(
                || "no stated period".to_owned(),
                |(lo, hi)| span(server, &interval(lo, hi)),
            );
            found.push(issue(
                k,
                "release_outside_period",
                format!(
                    "the release \"{body}\" of {} covers {when}, none of the requested period",
                    table.pid
                ),
            ));
            continue;
        }
        named.push(body.clone());
    }
    Ok((named, found))
}

/// `release_ambiguous`: per serving table, each run of requested days two or more primary
/// releases share and the source names none of, located at the source.
pub(super) fn ambiguous(
    server: &Server,
    conn: &Connection,
    index: usize,
    source: &CatalogSource,
    serving: &[Serving],
    named: &[String],
) -> Result<Vec<Issue>, Error> {
    let mut found = Vec::new();
    for s in serving {
        for x in shared(conn, s, named)? {
            if !x.chosen.is_empty() {
                continue;
            }
            let listed: Vec<String> = x.sharing.iter().map(|n| format!("\"{n}\"")).collect();
            found.push(Issue {
                source: Some(source.name.clone()),
                table: Some(s.table.pid.clone()),
                period: Some(super::period(server, &x.days)),
                ..Issue::new(
                    IssueLevel::Error,
                    "release_ambiguous",
                    format!("/sources/{index}"),
                    format!(
                        "{} was delivered in more than one release covering {}: {}. Name the \
                         releases to use in the source's \"releases\", or choose a period \
                         outside those days",
                        s.table.pid,
                        span(server, &x.days),
                        listed.join(", ")
                    ),
                )
            });
        }
    }
    found.sort_by_key(Issue::key);
    Ok(found)
}
