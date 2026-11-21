//! `variable_absent_from_files` (crates/DESIGN.md, order findings; #2603): the primary
//! releases (the delivered files) of a serving table a binding's variable has no segment
//! in, on requested days the source reads that file. [`files`] collects them per check
//! (each hop of a followed binding), and [`findings`] reports them once against the whole
//! binding's clips, so only a gap no `period_clipped` reports is named: one inside a
//! requested year the binding delivers on some day (#2389). A gap inside a file, or with
//! no file, gives nothing.

use std::collections::BTreeMap;

use reg_core::{Interval, IssueLevel, gaps, merge, overlap};
use rusqlite::params;

use super::window::{Scope, not_selected, segments};
use super::{Issue, Unit, day, days, interval, period, words};
use crate::{Error, rows};

/// A file the variable is missing from, with the requested days it leaves without data.
pub(super) struct Missing {
    pub table: String,
    pub variable: String,
    pub file: Interval,
    pub days: Vec<Interval>,
}

/// The files of `scope`'s serving tables that variable `id` (`pid`) has no segment in,
/// neither its own release's nor a releaseless one, over the `wanted` days the source
/// reads each (`lanes`: the shared days each named release serves), less the days the
/// check orders from any serving table. Only releases meeting those days are read.
pub(super) fn files(
    scope: &Scope,
    id: i64,
    pid: &str,
    wanted: &[Interval],
    ordered: &[Interval],
    lanes: &[(String, Interval)],
) -> Result<Vec<Missing>, Error> {
    let left = gaps(wanted, ordered.to_vec());
    if left.is_empty() {
        return Ok(Vec::new());
    }
    let base = "SELECT r.start AS start, r.end AS end, n.body AS name FROM main.release r
                JOIN main.text n ON n.id = r.name_text
                WHERE r.table_id = ?1 AND r.is_primary = 1 AND r.start < ?3 AND r.end > ?2";
    let sql = if scope.composed {
        format!(
            "SELECT DISTINCT start, end, name FROM ({base} UNION ALL
               SELECT r.start, r.end, n.body FROM held.release r
                 JOIN held.text n ON n.id = r.name_text
                 JOIN held.tbl ht ON ht.id = r.table_id JOIN main.tbl bt ON bt.pid = ht.pid
               WHERE bt.id = ?1 AND r.is_primary = 1 AND r.start < ?3 AND r.end > ?2)
             ORDER BY start, end, name"
        )
    } else {
        format!("SELECT DISTINCT start, end, name FROM ({base}) ORDER BY start, end, name")
    };
    let holds = format!(
        "SELECT EXISTS (SELECT 1 FROM ({}) WHERE variable_id = ?1 AND table_id = ?2
           AND start < ?4 AND end > ?3 AND (release IS NULL OR release = ?5))",
        segments(scope)
    );
    let mut found = Vec::new();
    for s in scope.serving {
        let open = overlap(&s.requested, &left);
        let Some((lo, hi)) = reg_core::hull(&open) else {
            continue;
        };
        let releases: Vec<(i64, i64, String)> = rows(
            scope.conn,
            &sql,
            params![s.table.id, day(&lo), day(&hi) + 1],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        for (start, end, name) in releases {
            let file = interval(start, end);
            let hit = gaps(
                &overlap(&open, std::slice::from_ref(&file)),
                not_selected(lanes, &name),
            );
            if hit.is_empty() {
                continue;
            }
            let held: bool =
                scope
                    .conn
                    .query_row(&holds, params![id, s.table.id, start, end, name], |row| {
                        row.get(0)
                    })?;
            if !held {
                found.push(Missing {
                    table: s.table.pid.clone(),
                    variable: pid.to_owned(),
                    file,
                    days: hit,
                });
            }
        }
    }
    Ok(found)
}

/// The findings for `missing` of binding `b`, less the binding's reported clips
/// (`removed`): one per table, variable and requested year, naming the files' periods; a
/// file crossing a year boundary counts in each year it misses.
pub(super) fn findings(
    scope: &Scope,
    b: usize,
    missing: &[Missing],
    removed: &[Interval],
) -> Vec<Issue> {
    let unit = Unit::of(&days(scope.requested));
    let mut by: BTreeMap<(i32, &str, &str), Vec<Interval>> = BTreeMap::new();
    for m in missing {
        for (lo, hi) in gaps(&m.days, removed.to_vec()) {
            for year in unit.year(day(&lo))..=unit.year(day(&hi)) {
                let files = by.entry((year, &m.table, &m.variable)).or_default();
                if !files.contains(&m.file) {
                    files.push(m.file.clone());
                }
            }
        }
    }
    by.into_iter()
        .map(|((year, table, variable), mut files)| {
            files.sort();
            let year_days = unit
                .days(year)
                .map(|(lo, hi)| overlap(scope.requested, &[interval(lo, hi)]))
                .filter(|d| !d.is_empty())
                .unwrap_or_else(|| merge(files.clone()));
            let first = year_days.first().expect("non-empty days");
            let last = year_days.last().expect("non-empty days");
            let periods: Vec<_> = files.iter().map(|f| period(scope.server, f)).collect();
            Issue {
                source: Some(scope.source.name.clone()),
                variable: Some(variable.to_owned()),
                table: Some(table.to_owned()),
                period: Some(period(scope.server, &(first.0.clone(), last.1.clone()))),
                ..Issue::new(
                    IssueLevel::Info,
                    "variable_absent_from_files",
                    format!("/sources/{}/bindings/{b}", scope.index),
                    format!(
                        "There is no data on {variable} in the files of {table} for {}.",
                        words::plain(&periods)
                    ),
                )
            }
        })
        .collect()
}
