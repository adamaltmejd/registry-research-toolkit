//! Row keys and panels use the steward's declared physical entity/time components.
//! Reference-only orders use catalog segments with role `row_key`.

use std::collections::BTreeSet;

use reg_core::IssueLevel;
use reg_core::project::{Panel, Source};
use rusqlite::Connection;

use super::{Issue, OrderRowKeys, RowKeyColumn, Serving, day};
use crate::ops::Server;
use crate::{Code, Error, rows};

/// Row-key columns as (variable pid, spelling).
type Columns = BTreeSet<(String, String)>;

#[derive(Clone)]
struct KeySegment {
    variable: String,
    spelling: String,
    start: i64,
    end: i64,
    state: String,
    kind: Option<String>,
    entity: bool,
}

/// Physical key declarations select the key set; metadata supplies state/kind only.
fn key_segments(
    server: &Server,
    conn: &Connection,
    serving: &Serving,
) -> Result<Vec<KeySegment>, Error> {
    if server.catalog.steward().is_none() {
        return Ok(rows(
            conn,
            "SELECT v.pid,sp.exact,s.start,s.end,s.state,k.slug FROM segment s
             JOIN variable v ON v.id=s.variable_id JOIN spelling sp ON sp.id=s.spelling_id
             LEFT JOIN id_kind k ON k.id=s.id_kind_id
             WHERE s.table_id=? AND s.role='row_key' ORDER BY v.pid,sp.exact,s.start",
            [serving.table.id],
            |r| {
                Ok(KeySegment {
                    variable: r.get(0)?,
                    spelling: r.get(1)?,
                    start: r.get(2)?,
                    end: r.get(3)?,
                    state: r.get(4)?,
                    kind: r.get(5)?,
                    entity: true,
                })
            },
        )?);
    }
    let mut mappings = conn.prepare(
        "WITH semantics AS (
           SELECT t.pid AS table_pid,v.pid AS variable_pid,s.start,s.end,s.state,k.slug
           FROM main.segment s JOIN main.tbl t ON t.id=s.table_id
           JOIN main.variable v ON v.id=s.variable_id LEFT JOIN main.id_kind k ON k.id=s.id_kind_id
           UNION ALL
           SELECT t.pid,v.pid,s.start,s.end,s.state,k.slug
           FROM held.segment s JOIN held.tbl t ON t.id=s.table_id
           JOIN held.variable v ON v.id=s.variable_id LEFT JOIN held.id_kind k ON k.id=s.id_kind_id
         )
         SELECT DISTINCT m.variable_pid,c.name,max(m.start,served.start,s.start),
                min(m.end,served.end,s.end),s.state,s.slug,json_extract(k.value,'$.period'),
                EXISTS(SELECT 1 FROM json_each(k.value,'$.entity') e WHERE e.value=m.variable_pid)
         FROM held.holding_table h JOIN held.holding_column c ON c.holding_id=h.id
         JOIN held.holding_mapping m ON m.column_id=c.id JOIN json_each(h.key_requirements) k
         JOIN held.holding_column sc ON sc.holding_id=h.id
         JOIN held.holding_mapping served ON served.column_id=sc.id AND served.table_pid=?
           AND served.start<m.end AND served.end>m.start
         JOIN semantics s ON s.table_pid=m.table_pid AND s.variable_pid=m.variable_pid
           AND s.start<min(m.end,served.end) AND s.end>max(m.start,served.start)
         WHERE json_extract(k.value,'$.hold') IS NULL AND (
           EXISTS(SELECT 1 FROM json_each(k.value,'$.entity') e WHERE e.value=m.variable_pid)
           OR EXISTS(SELECT 1 FROM json_each(k.value,'$.time') e WHERE e.value=m.variable_pid))
         ORDER BY m.variable_pid,c.name,m.start",
    )?;
    let mut out = Vec::new();
    for row in mappings.query_map([&serving.table.pid], |r| {
        Ok((
            KeySegment {
                variable: r.get(0)?,
                spelling: r.get(1)?,
                start: r.get(2)?,
                end: r.get(3)?,
                state: r.get(4)?,
                kind: r.get(5)?,
                entity: r.get(7)?,
            },
            r.get::<_, Option<String>>(6)?,
        ))
    })? {
        let (key, period) = row?;
        let scopes = period.map_or_else(
            || Ok(vec![(key.start, key.end)]),
            |period| {
                period
                    .split(',')
                    .map(|token| {
                        let (lo, hi) = token
                            .trim()
                            .parse::<reg_core::Period>()
                            .map_err(|_| {
                                Error::new(
                                    Code::InternalError,
                                    "The holder's row-key data is invalid.",
                                    vec![],
                                )
                            })?
                            .iso_bounds();
                        Ok((key.start.max(day(&lo)), key.end.min(day(&hi) + 1)))
                    })
                    .collect::<Result<Vec<_>, Error>>()
            },
        )?;
        for (start, end) in scopes {
            if start < end {
                out.push(KeySegment {
                    start,
                    end,
                    ..key.clone()
                });
            }
        }
    }
    Ok(merge_key_segments(out))
}

fn merge_key_segments(mut out: Vec<KeySegment>) -> Vec<KeySegment> {
    // Several served columns can expose overlapping pieces of the same physical key.
    out.sort_by(|a, b| {
        (
            &a.variable,
            &a.spelling,
            &a.state,
            &a.kind,
            a.entity,
            a.start,
            a.end,
        )
            .cmp(&(
                &b.variable,
                &b.spelling,
                &b.state,
                &b.kind,
                b.entity,
                b.start,
                b.end,
            ))
    });
    let mut merged: Vec<KeySegment> = Vec::new();
    for key in out {
        if let Some(last) = merged.last_mut()
            && (
                &last.variable,
                &last.spelling,
                &last.state,
                &last.kind,
                last.entity,
            ) == (
                &key.variable,
                &key.spelling,
                &key.state,
                &key.kind,
                key.entity,
            )
            && key.start <= last.end
        {
            last.end = last.end.max(key.end);
        } else {
            merged.push(key);
        }
    }
    merged
}

/// A source's row keys: per serving table, the requested days cut where its row-key
/// columns change, each piece with the columns over it (sorted by variable and
/// spelling); days without a row-key column are left out. Row keys join the order
/// unbound, so their window findings are made here, located at the source: an
/// ambiguous row-key segment inside the requested days is an `ambiguous_window`, a
/// preliminary one a `preliminary_data` warning.
pub(super) fn row_keys(
    server: &Server,
    conn: &Connection,
    index: usize,
    source: &Source,
    serving: &[Serving],
) -> Result<(Vec<OrderRowKeys>, Vec<Issue>), Error> {
    let mut out = Vec::new();
    let mut findings = Vec::new();
    for s in serving {
        let rows = key_segments(server, conn, s)?;
        // Day-number pieces `[start, end)` of each column inside the requested days.
        let mut pieces = Vec::new();
        for KeySegment {
            variable,
            spelling,
            start,
            end,
            state,
            ..
        } in rows
        {
            for (lo, hi) in &s.requested {
                let (lo, hi) = (start.max(day(lo)), end.min(day(hi) + 1));
                if lo >= hi {
                    continue;
                }
                let found = match state.as_str() {
                    "ambiguous" => Some((
                        IssueLevel::Error,
                        "ambiguous_window",
                        "has more than one form in",
                        " (name, type or codes differ); pick a shorter period or ask the maintainer",
                    )),
                    "preliminary" => Some((
                        IssueLevel::Warning,
                        "preliminary_data",
                        "in",
                        " is preliminary data: no final release covers it",
                    )),
                    _ => None,
                };
                if let Some((level, code, verb, tail)) = found {
                    let period = server.period(lo, hi);
                    let over = format!(
                        "[{}, {})",
                        period.start.as_deref().unwrap_or("open"),
                        period.end.as_deref().unwrap_or("open")
                    );
                    findings.push(Issue {
                        source: Some(source.name.clone()),
                        variable: Some(variable.clone()),
                        table: Some(s.table.pid.clone()),
                        period: Some(period),
                        ..Issue::new(
                            level,
                            code,
                            format!("/sources/{index}"),
                            format!(
                                "Row key {variable} {verb} {} over {over}{tail}",
                                s.table.pid
                            ),
                        )
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

/// `panel_row_keys_differ`: every member's entity keys over its requested days
/// carry a known identifier kind (the full kind, pseudonymised form included) and the
/// members' kinds differ. A member with an unknown kind, or whose source did not
/// resolve, gives no finding.
pub(super) fn panel(
    server: &Server,
    conn: &Connection,
    index: usize,
    panel: &Panel,
    members: &[(&str, Option<Vec<Serving>>)],
) -> Result<Vec<Issue>, Error> {
    let mut kinds: Vec<(&str, BTreeSet<String>)> = Vec::new();
    for member in &panel.members {
        let Some((_, Some(serving))) = members.iter().find(|(name, _)| name == member) else {
            return Ok(vec![]);
        };
        let mut found: BTreeSet<Option<String>> = BTreeSet::new();
        for s in serving {
            for key in key_segments(server, conn, s)? {
                if key.entity
                    && s.requested
                        .iter()
                        .any(|(lo, hi)| key.start <= day(hi) && key.end > day(lo))
                {
                    found.insert(key.kind);
                }
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
    Ok(vec![Issue::new(
        IssueLevel::Warning,
        "panel_row_keys_differ",
        format!("/panels/{index}"),
        format!(
            "The members of panel {} key their rows on different identifier kinds: {}.",
            panel.panel_id,
            listed.join("; ")
        ),
    )])
}

/// Suppress duplicate window findings already reported by an explicit binding.
pub(super) fn unbound(findings: &mut Vec<Issue>, bound: &[Issue]) {
    // A bound row key already has its binding's finding of that code in that table.
    findings.retain(|k| {
        !bound
            .iter()
            .any(|f| (&f.code, &f.variable, &f.table) == (&k.code, &k.variable, &k.table))
    });
}
