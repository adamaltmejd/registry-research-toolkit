//! Row keys and panels: a table's row keys are its segments with role `row_key`
//! (schema §0), whether or not a binding orders them.

use std::collections::BTreeSet;

use reg_core::IssueLevel;
use reg_core::project::{Panel, Source};
use rusqlite::Connection;

use super::{Issue, OrderRowKeys, RowKeyColumn, Serving, day};
use crate::Error;
use crate::ops::Server;

/// Row-key columns as (variable pid, spelling).
type Columns = BTreeSet<(String, String)>;

/// A source's row keys: per serving table, the requested days cut where its row-key
/// columns change, each piece with the columns over it (sorted by variable and
/// spelling); days without a row-key column are left out. Row keys join the order
/// unbound, so an ambiguous row-key segment inside the requested days is an
/// `ambiguous_window` here, located at the source.
pub(super) fn row_keys(
    server: &Server,
    conn: &Connection,
    index: usize,
    source: &Source,
    serving: &[Serving],
) -> Result<(Vec<OrderRowKeys>, Vec<Issue>), Error> {
    let mut query = conn.prepare(
        "SELECT v.pid, sp.exact, s.start, s.end, s.state
         FROM segment s JOIN variable v ON v.id = s.variable_id
           JOIN spelling sp ON sp.id = s.spelling_id
         WHERE s.table_id = ? AND s.role = 'row_key'
         ORDER BY v.pid, sp.exact, s.start",
    )?;
    let mut out = Vec::new();
    let mut findings = Vec::new();
    for s in serving {
        let rows: Vec<(String, String, i64, i64, String)> = query
            .query_map([s.table.id], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;
        // Day-number pieces `[start, end)` of each column inside the requested days.
        let mut pieces = Vec::new();
        for (variable, spelling, start, end, state) in rows {
            for (lo, hi) in &s.requested {
                let (lo, hi) = (start.max(day(lo)), end.min(day(hi) + 1));
                if lo >= hi {
                    continue;
                }
                if state == "ambiguous" {
                    let period = server.period(lo, hi);
                    let over = format!(
                        "[{}, {})",
                        period.start.as_deref().unwrap_or("open"),
                        period.end.as_deref().unwrap_or("open")
                    );
                    findings.push(Issue {
                        level: IssueLevel::Error,
                        code: "ambiguous_window".to_owned(),
                        path: format!("/sources/{index}"),
                        message: format!(
                            "Row key {variable} cannot be placed on one form in {} over {}; \
                             the window needs curation",
                            s.table.pid, over
                        ),
                        source: Some(source.name.clone()),
                        variable: Some(variable.clone()),
                        table: Some(s.table.pid.clone()),
                        period: Some(period),
                        successor_fqid: None,
                    });
                }
                pieces.push((lo, hi, (variable.clone(), spelling.clone())));
            }
        }
        let cuts: BTreeSet<i64> = pieces.iter().flat_map(|(lo, hi, _)| [*lo, *hi]).collect();
        let cuts: Vec<i64> = cuts.into_iter().collect();
        let mut keyed: Vec<(i64, i64, Columns)> = Vec::new();
        for pair in cuts.windows(2) {
            let (lo, hi) = (pair[0], pair[1]);
            let columns: Columns = pieces
                .iter()
                .filter(|(a, b, _)| *a <= lo && hi <= *b)
                .map(|(_, _, column)| column.clone())
                .collect();
            if columns.is_empty() {
                continue;
            }
            match keyed.last_mut() {
                Some((_, end, last)) if *end == lo && *last == columns => *end = hi,
                _ => keyed.push((lo, hi, columns)),
            }
        }
        out.extend(keyed.into_iter().map(|(lo, hi, columns)| {
            OrderRowKeys {
                source: source.name.clone(),
                table: s.table.pid.clone(),
                period: server.period(lo, hi),
                columns: columns
                    .into_iter()
                    .map(|(variable, spelling)| RowKeyColumn { variable, spelling })
                    .collect(),
            }
        }));
    }
    Ok((out, findings))
}

/// `panel_row_keys_differ`: every member's row-key segments over its requested days
/// carry a known identifier kind (the full kind, pseudonymised form included) and the
/// members' kinds differ. A member with an unknown kind, or whose source did not
/// resolve, gives no finding.
pub(super) fn panel(
    conn: &Connection,
    index: usize,
    panel: &Panel,
    served: &[(&str, Option<Vec<Serving>>)],
) -> Result<Vec<Issue>, Error> {
    let mut query = conn.prepare(
        "SELECT DISTINCT k.slug FROM segment s LEFT JOIN id_kind k ON k.id = s.id_kind_id
         WHERE s.table_id = ?1 AND s.role = 'row_key' AND s.start <= ?2 AND s.end > ?3",
    )?;
    let mut kinds: Vec<(&str, BTreeSet<String>)> = Vec::new();
    for member in &panel.members {
        let Some((_, Some(serving))) = served.iter().find(|(name, _)| name == member) else {
            return Ok(vec![]);
        };
        let mut found: BTreeSet<Option<String>> = BTreeSet::new();
        for s in serving {
            for (lo, hi) in &s.requested {
                let rows = query
                    .query_map(rusqlite::params![s.table.id, day(hi), day(lo)], |row| {
                        row.get::<_, Option<String>>(0)
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                found.extend(rows);
            }
        }
        if found.is_empty() || found.contains(&None) {
            return Ok(vec![]);
        }
        kinds.push((member, found.into_iter().flatten().collect()));
    }
    if kinds.windows(2).all(|pair| pair[0].1 == pair[1].1) {
        return Ok(vec![]);
    }
    let listed: Vec<String> = kinds
        .iter()
        .map(|(member, k)| {
            format!(
                "{member} ({})",
                k.iter().cloned().collect::<Vec<_>>().join(", ")
            )
        })
        .collect();
    Ok(vec![Issue {
        level: IssueLevel::Warning,
        code: "panel_row_keys_differ".to_owned(),
        path: format!("/panels/{index}"),
        message: format!(
            "The members of panel {} key their rows on different identifier kinds: {}.",
            panel.panel_id,
            listed.join("; ")
        ),
        source: None,
        variable: None,
        table: None,
        period: None,
        successor_fqid: None,
    }])
}
