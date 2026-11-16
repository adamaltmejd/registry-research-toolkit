//! Row keys use the steward's declared physical entity/time components.
//! Reference-only orders use the catalog segments curated as row keys (`row_key`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use reg_core::IssueLevel;
use reg_core::project::CatalogSource;
use rusqlite::Connection;

use super::{Issue, OrderRowKeys, RowKeyColumn, Serving, day};
use crate::ops::Server;
use crate::{Error, rows};

/// Row-key columns as (variable pid, spelling).
type Columns = BTreeSet<(String, String)>;
/// A row-key column over half-open day numbers `lo..hi`: (lo, hi, (variable, spelling)).
type KeyPiece = (i64, i64, (String, String));
/// A key segment over half-open day numbers in a lane (the named release of a shared
/// day, `None` elsewhere): (lane, lo, hi, segment).
type LanePiece<'a> = (Option<&'a String>, i64, i64, &'a KeySegment);

#[derive(Clone)]
pub(crate) struct KeySegment {
    pub(crate) variable: String,
    pub(crate) spelling: String,
    pub(crate) start: i64,
    pub(crate) end: i64,
    state: String,
    entity: bool,
    /// The release its segment belongs to (a concurrent table's), by name.
    release: Option<String>,
}

/// The steward branch of [`key_segments`] per (table pid, database). Its query scans
/// every segment of both databases (~350 ms per table), and its answer depends on the
/// table and the database alone: the catalog and holdings are opened immutable and
/// replaced only by a redeploy, so an answer holds for the life of the process. Errors
/// are not cached.
// simplify: unbounded, one entry per table asked about (at most the catalog's tables;
// ~154 held today), filled lazily; move the computation into the holdings build if
// the table count grows or the map shows in memory.
#[derive(Default)]
pub(crate) struct StewardKeys(Mutex<HashMap<KeyedTable, Arc<Vec<KeySegment>>>>);

/// (table pid, database id).
type KeyedTable = (String, Option<String>);

/// Physical key declarations select the key set; metadata supplies state only.
fn key_segments(
    server: &Server,
    conn: &Connection,
    space: Option<&str>,
    serving: &Serving,
) -> Result<Arc<Vec<KeySegment>>, Error> {
    if server.catalog.steward().is_none() {
        return Ok(Arc::new(rows(
            conn,
            "SELECT v.pid,sp.exact,s.start,s.end,s.state,n.body FROM segment s
             JOIN variable v ON v.id=s.variable_id JOIN spelling sp ON sp.id=s.spelling_id
             LEFT JOIN release r ON r.id=s.release_id LEFT JOIN text n ON n.id=r.name_text
             WHERE s.table_id=? AND s.row_key=1 ORDER BY v.pid,sp.exact,s.start",
            [serving.table.id],
            |r| {
                Ok(KeySegment {
                    variable: r.get(0)?,
                    spelling: r.get(1)?,
                    start: r.get(2)?,
                    end: r.get(3)?,
                    state: r.get(4)?,
                    entity: true,
                    release: r.get(5)?,
                })
            },
        )?));
    }
    steward_keys(server, conn, &serving.table.pid, space)
}

/// The steward's declared key columns of `pid`, cached per (table, database). `conn`
/// has the holdings attached as `held`.
pub(crate) fn steward_keys(
    server: &Server,
    conn: &Connection,
    pid: &str,
    space: Option<&str>,
) -> Result<Arc<Vec<KeySegment>>, Error> {
    let cache = &server.steward_keys.0;
    let at = (pid.to_owned(), space.map(str::to_owned));
    if let Some(hit) = cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&at)
    {
        return Ok(Arc::clone(hit));
    }
    // Computed outside the lock: two first requests for one table both compute it.
    let keys = Arc::new(steward_key_segments(conn, pid, space)?);
    cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(at, Arc::clone(&keys));
    Ok(keys)
}

/// The declared key columns of the tables serving `table_pid`, in database `space`
/// (every database when `None`, an order already blocked by its `linkage`).
fn steward_key_segments(
    conn: &Connection,
    table_pid: &str,
    space: Option<&str>,
) -> Result<Vec<KeySegment>, Error> {
    let mut mappings = conn.prepare(
        "WITH semantics AS (
           SELECT t.pid AS table_pid,v.pid AS variable_pid,s.start,s.end,s.state
           FROM main.segment s JOIN main.tbl t ON t.id=s.table_id
           JOIN main.variable v ON v.id=s.variable_id
           UNION ALL
           SELECT t.pid,v.pid,s.start,s.end,s.state
           FROM held.segment s JOIN held.tbl t ON t.id=s.table_id
           JOIN held.variable v ON v.id=s.variable_id
         )
         SELECT DISTINCT m.variable_pid,c.name,max(m.start,served.start,s.start),
                min(m.end,served.end,s.end),s.state,json_extract(k.value,'$.period'),
                EXISTS(SELECT 1 FROM json_each(k.value,'$.entity') e WHERE e.value=m.variable_pid)
         FROM held.holding_table h JOIN held.holding_column c ON c.holding_id=h.id
         JOIN held.holding_mapping m ON m.column_id=c.id JOIN json_each(h.key_requirements) k
         JOIN held.holding_column sc ON sc.holding_id=h.id
         JOIN held.holding_mapping served ON served.column_id=sc.id AND served.table_pid=?1
           AND served.start<m.end AND served.end>m.start
         JOIN semantics s ON s.table_pid=m.table_pid AND s.variable_pid=m.variable_pid
           AND s.start<min(m.end,served.end) AND s.end>max(m.start,served.start)
         WHERE (?2 IS NULL OR h.space=?2) AND json_extract(k.value,'$.hold') IS NULL AND (
           EXISTS(SELECT 1 FROM json_each(k.value,'$.entity') e WHERE e.value=m.variable_pid)
           OR EXISTS(SELECT 1 FROM json_each(k.value,'$.time') e WHERE e.value=m.variable_pid))
         ORDER BY m.variable_pid,c.name,m.start",
    )?;
    let mut out = Vec::new();
    for row in mappings.query_map(rusqlite::params![table_pid, space], |r| {
        Ok((
            KeySegment {
                variable: r.get(0)?,
                spelling: r.get(1)?,
                start: r.get(2)?,
                end: r.get(3)?,
                state: r.get(4)?,
                entity: r.get(6)?,
                release: None,
            },
            r.get::<_, Option<String>>(5)?,
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
                            .map_err(|_| Error::internal("The holder's row-key data is invalid."))?
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

fn merge_key_segments(out: Vec<KeySegment>) -> Vec<KeySegment> {
    // Several served columns can expose overlapping pieces of the same physical key.
    let mut groups: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for key in out {
        groups
            .entry((key.variable, key.spelling, key.state, key.entity))
            .or_default()
            .push((key.start, key.end));
    }
    groups
        .into_iter()
        .flat_map(|((variable, spelling, state, entity), spans)| {
            reg_core::days::merge(spans)
                .into_iter()
                .map(move |(start, end)| KeySegment {
                    variable: variable.clone(),
                    spelling: spelling.clone(),
                    start,
                    end,
                    state: state.clone(),
                    entity,
                    release: None,
                })
        })
        .collect()
}

/// `pieces` as each lane reads them, by release shadowing per key variable
/// (`reg_core::days::shadow`), as the order's columns are.
fn shadowed(pieces: Vec<LanePiece<'_>>) -> Vec<LanePiece<'_>> {
    let mut groups: BTreeMap<(Option<&String>, &str), Vec<LanePiece<'_>>> = BTreeMap::new();
    for piece in pieces {
        groups
            .entry((piece.0, piece.3.variable.as_str()))
            .or_default()
            .push(piece);
    }
    let mut out = Vec::new();
    for ((lane, _), group) in groups {
        let spans: Vec<((i64, i64), Option<&str>)> = group
            .iter()
            .map(|(_, lo, hi, key)| ((*lo, *hi), key.release.as_deref()))
            .collect();
        let kept = reg_core::days::shadow(&spans, lane.map(String::as_str));
        for ((_, _, _, key), keep) in group.into_iter().zip(kept) {
            out.extend(keep.into_iter().map(|(lo, hi)| (lane, lo, hi, key)));
        }
    }
    out
}

/// A source's row keys: per serving table, the requested days cut where its row-key
/// columns change, each piece with the columns over it (sorted by variable and
/// spelling); days without a row-key column are left out. On a day two primary releases
/// share, each release the source names gets its own pieces, from its own key columns
/// and the releaseless ones; a day none of them is named for blocks the source and has
/// none. Row keys join the order unbound, so their window findings are made here,
/// located at the source: an ambiguous row-key segment inside the served days is an
/// `ambiguous_window`, a preliminary one a `preliminary_data` warning. Key columns read
/// `conn`, the base connection; shared days read `origin`, the serving table's own, as
/// entries do: a table only the holdings database defines has a held id.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(super) fn row_keys(
    server: &Server,
    conn: &Connection,
    origin: &Connection,
    space: Option<&str>,
    index: usize,
    source: &CatalogSource,
    serving: &[Serving],
    named: &[String],
) -> Result<(Vec<OrderRowKeys>, Vec<Issue>), Error> {
    let mut out = Vec::new();
    let mut findings = Vec::new();
    for s in serving {
        let rows = key_segments(server, conn, space, s)?;
        let shared = super::releases::shared(origin, s, named)?;
        let shared_days: Vec<(i64, i64)> =
            super::days(&shared.iter().map(|x| x.days.clone()).collect::<Vec<_>>());
        // Day-number pieces `[start, end)` of each column on the served days, by release.
        let mut pieces: Vec<LanePiece<'_>> = Vec::new();
        for key in rows.iter() {
            for (lo, hi) in super::days(&s.requested) {
                let (lo, hi) = (key.start.max(lo), key.end.min(hi));
                if lo >= hi {
                    continue;
                }
                for (a, b) in reg_core::days::minus(&[(lo, hi)], &shared_days) {
                    pieces.push((None, a, b, key));
                }
                for (x, (xa, xb)) in shared.iter().zip(&shared_days) {
                    let (a, b) = (lo.max(*xa), hi.min(*xb));
                    if a >= b {
                        continue;
                    }
                    for name in &x.chosen {
                        pieces.push((Some(name), a, b, key));
                    }
                }
            }
        }
        let pieces = shadowed(pieces);
        let mut found: Vec<(&str, i64, i64, &str)> = Vec::new();
        for (_, lo, hi, key) in &pieces {
            let found_here = match key.state.as_str() {
                "ambiguous" => Some((
                    IssueLevel::Error,
                    "ambiguous_window",
                    "shares its column with another variable in",
                    "; pick a shorter period or ask the maintainer",
                )),
                "preliminary" => Some((
                    IssueLevel::Warning,
                    "preliminary_data",
                    "in",
                    " is preliminary data: no final release covers it",
                )),
                _ => None,
            };
            let Some((level, code, verb, tail)) = found_here else {
                continue;
            };
            // Each release's pieces of one segment repeat its days: one finding.
            if found.contains(&(key.variable.as_str(), *lo, *hi, code)) {
                continue;
            }
            found.push((key.variable.as_str(), *lo, *hi, code));
            let period = server.period(*lo, *hi);
            let over = super::words::plain(std::slice::from_ref(&period));
            findings.push(Issue {
                source: Some(source.name.clone()),
                variable: Some(key.variable.clone()),
                table: Some(s.table.pid.clone()),
                period: Some(period),
                ..Issue::new(
                    level,
                    code,
                    format!("/sources/{index}"),
                    format!(
                        "Row key {} {verb} {} for {over}{tail}",
                        key.variable, s.table.pid
                    ),
                )
            });
        }
        let mut lanes: BTreeMap<Option<&String>, Vec<KeyPiece>> = BTreeMap::new();
        for (lane, lo, hi, key) in pieces {
            lanes.entry(lane).or_default().push((
                lo,
                hi,
                (key.variable.clone(), key.spelling.clone()),
            ));
        }
        let mut keyed: Vec<(i64, Option<&String>, i64, Columns)> = Vec::new();
        for (lane, pieces) in lanes {
            let mut run: Vec<(i64, Option<&String>, i64, Columns)> = Vec::new();
            for (lo, hi) in reg_core::days::slices(pieces.iter().map(|(lo, hi, _)| (*lo, *hi))) {
                let columns: Columns = pieces
                    .iter()
                    .filter(|(a, b, _)| *a <= lo && hi <= *b)
                    .map(|(_, _, column)| column.clone())
                    .collect();
                if columns.is_empty() {
                    continue;
                }
                match run.last_mut() {
                    Some((_, _, end, last)) if *end == lo && *last == columns => *end = hi,
                    _ => run.push((lo, lane, hi, columns)),
                }
            }
            keyed.extend(run);
        }
        keyed.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        out.extend(keyed.into_iter().map(|(lo, lane, hi, columns)| {
            OrderRowKeys {
                source: source.name.clone(),
                table: s.table.pid.clone(),
                table_name: String::new(),
                delivered_table: String::new(),
                release: lane.cloned(),
                period: server.period(lo, hi),
                columns: columns
                    .into_iter()
                    .map(|(variable, spelling)| RowKeyColumn {
                        variable,
                        variable_name: String::new(),
                        spelling,
                    })
                    .collect(),
            }
        }));
    }
    Ok((out, findings))
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
