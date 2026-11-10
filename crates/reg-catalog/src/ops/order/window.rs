//! One binding of a source: its ids, then the request over the variable's primary
//! segments in the serving tables. The request minus the segments is the clip; the rest, split by merged form, is the binding's entries; the
//! window findings come from the segments the entries cover.

use reg_core::project::{Binding, Source};
use reg_core::{Interval, IssueLevel, gaps, merge, next_iso_day, overlap};
use rusqlite::{Connection, OptionalExtension, Row};

use super::{
    Issue, OrderClip, OrderEntry, OrderValueSet, Serving, day, interval, iso_day, period, refused,
    span,
};
use crate::ops::{Server, refs};
use crate::{Code, Error, rows};

/// What a binding is checked against: its source and the source's serving tables.
pub(super) struct Scope<'a> {
    pub server: &'a Server,
    pub conn: &'a Connection,
    /// The source's index in the project.
    pub index: usize,
    pub source: &'a Source,
    /// The source's table.
    pub table: &'a str,
    pub serving: &'a [Serving],
    /// The ids of the source's table and its dated successors (spec 02 rule 4).
    pub chain: &'a [i64],
    /// Every requested day (inclusive ISO intervals, ascending).
    pub requested: &'a [Interval],
    /// The steward's composed view over a base table: its variables' held segments
    /// (reference occurrences) join the base ones.
    pub composed: bool,
}

/// A binding's findings, entries and clip.
#[derive(Default)]
pub(super) struct Outcome {
    pub findings: Vec<Issue>,
    pub entries: Vec<OrderEntry>,
    pub clip: Option<OrderClip>,
    pub serving: Vec<Serving>,
}

/// A resolved binding variable.
pub(super) struct Variable {
    id: i64,
    pid: String,
    /// The pinned class version's public id.
    classification: Option<String>,
}

/// One served piece of the request: part of a primary segment inside the requested days.
#[derive(Clone)]
struct Piece {
    /// The index of its serving table.
    serving: usize,
    days: Interval,
    spelling: String,
    dtype: Option<String>,
    length: Option<i64>,
    /// The value set's hash and size.
    valueset: Option<(String, i64)>,
    /// The class version's public id.
    class_version: Option<String>,
    sensitive: bool,
    identifier: bool,
    state: String,
    /// Its source row ends at the open sentinel: no coverage end is stated.
    open: bool,
}

/// The segments a binding reads, keyed by the connection's own table and variable ids,
/// with value sets and class versions by public identity so rows never cross origins.
/// In the composed view a base table's held segments join through public ids: a base
/// variable's (a reference occurrence, never overlapping the base ones) by its base id,
/// a variable only the holdings database defines by its negated held id (`held_id`).
pub(super) fn segments(scope: &Scope) -> String {
    let part = |db: &str, ids: &str, joins: &str| {
        format!(
            "SELECT {ids}, x.start AS start, x.end AS end, sp.exact AS exact, \
             x.dtype AS dtype, x.length AS length, vs.hash AS vs_hash, vs.n_items AS vs_n, \
             cv.pid AS cv_pid, COALESCE(x.sensitive, 0) AS sensitive, \
             COALESCE(x.identifier, 0) AS identifier, x.state AS state
             FROM {db}.segment x {joins} JOIN {db}.spelling sp ON sp.id = x.spelling_id
             LEFT JOIN {db}.valueset vs ON vs.id = x.valueset_id
             LEFT JOIN {db}.class_version cv ON cv.id = x.class_version_id"
        )
    };
    let base = part(
        "main",
        "x.table_id AS table_id, x.variable_id AS variable_id",
        "",
    );
    if !scope.composed {
        return base;
    }
    let held = part(
        "held",
        "bt.id, COALESCE(bv.id, -hv.id)",
        "JOIN held.tbl ht ON ht.id = x.table_id JOIN main.tbl bt ON bt.pid = ht.pid
         JOIN held.variable hv ON hv.id = x.variable_id
         LEFT JOIN main.variable bv ON bv.pid = hv.pid",
    );
    format!("{base} UNION ALL {held}")
}

/// The binding's variable and its classification pin, and the findings refusing them
/// (`fqid_unresolved`, `retired_ref`), located at the field. A resolved variable is
/// returned even when its pin is refused, so its window checks still run: a blocked
/// order reports every finding at once. In the composed view (`composed`) a pin the base
/// does not know may name a class version defined only in the holdings database.
pub(super) fn refs(
    conn: &Connection,
    composed: bool,
    index: usize,
    b: usize,
    source: &Source,
    binding: &Binding,
    table: Option<&str>,
) -> Result<(Option<Variable>, Vec<Issue>), Error> {
    let mut findings = Vec::new();
    let mut issue = |field: &str, variable: Option<&str>, (code, message): (&str, String)| {
        findings.push(Issue {
            source: Some(source.name.clone()),
            variable: variable.map(str::to_owned),
            table: table.map(str::to_owned),
            ..Issue::new(
                IssueLevel::Error,
                code,
                format!("/sources/{index}/bindings/{b}/{field}"),
                message,
            )
        });
    };
    let unresolved = |value: &str, what: &str| {
        (
            "fqid_unresolved",
            format!("{value} names no {what} in the catalog."),
        )
    };
    let variable = match refs::resolve(conn, &binding.variable) {
        Ok(hit) if hit.kind == "variable" => Some((hit.id, hit.pid)),
        Ok(_) => {
            issue("variable", None, unresolved(&binding.variable, "variable"));
            None
        }
        // The base wins when it knows the id; only a miss falls through.
        Err(err) => {
            let held = if composed && err.kind() == Code::NotFound {
                held_id(conn, &binding.variable)?
            } else {
                None
            };
            if held.is_none() {
                issue(
                    "variable",
                    None,
                    refused(conn, &binding.variable, "variable", &err)?,
                );
            }
            held.map(|id| (id, binding.variable.clone()))
        }
    };
    let pid = variable.as_ref().map(|(_, pid)| pid.as_str());
    let mut classification = None;
    if let Some(pin) = &binding.classification {
        match refs::resolve(conn, pin) {
            Ok(hit) if hit.kind == "class_version" => classification = Some(hit.pid),
            Ok(_) => issue(
                "classification",
                pid,
                unresolved(pin, "classification version"),
            ),
            Err(err) => {
                // The base wins when it knows the id; only a miss falls through.
                let held = if composed && err.kind() == Code::NotFound {
                    held_class_version(conn, pin)?
                } else {
                    None
                };
                match held {
                    Some(pid) => classification = Some(pid),
                    None => issue(
                        "classification",
                        pid,
                        refused(conn, pin, "classification version", &err)?,
                    ),
                }
            }
        }
    }
    let variable = variable.map(|(id, pid)| Variable {
        id,
        pid,
        classification,
    });
    Ok((variable, findings))
}

/// A variable only the attached holdings database defines, by exact public id, keyed as
/// `segments` keys it in the composed view: its negated held id, which no base id equals.
fn held_id(conn: &Connection, pid: &str) -> Result<Option<i64>, Error> {
    Ok(conn
        .query_row(
            "SELECT -id FROM held.variable WHERE pid = ?1",
            [pid],
            |row| row.get(0),
        )
        .optional()?)
}

/// A class version of the attached holdings database, by exact public id.
fn held_class_version(conn: &Connection, pin: &str) -> Result<Option<String>, Error> {
    Ok(conn
        .query_row(
            "SELECT pid FROM held.ident WHERE pid = ?1 AND kind = 'class_version'",
            [pin],
            |row| row.get(0),
        )
        .optional()?)
}

/// One binding over its source's serving tables.
pub(super) fn binding(scope: &Scope, b: usize, binding: &Binding) -> Result<Outcome, Error> {
    let (variable, findings) = refs(
        scope.conn,
        scope.composed,
        scope.index,
        b,
        scope.source,
        binding,
        Some(scope.table),
    )?;
    let mut outcome = Outcome {
        findings,
        ..Outcome::default()
    };
    let Some(variable) = variable else {
        return Ok(outcome);
    };
    let at = Located {
        scope,
        b,
        variable: &variable,
    };
    if !in_chain(scope, variable.id)? {
        outcome.findings.push(at.issue(
            IssueLevel::Error,
            "variable_not_in_table",
            scope.table,
            None,
            format!("{} has no column in {}.", variable.pid, scope.table),
        ));
        return Ok(outcome);
    }
    if binding.follow_successor == Some(true) {
        if variable.id > 0 {
            return super::successor::binding(scope, b, binding);
        }
        // simplify: succession of a variable only the holdings database defines
        // (negative id) on a reference table is refused, not followed; a source whose
        // table only the holdings database defines runs on that origin and does follow.
        // Make `successor.rs` origin-aware when a steward needs this.
        if let Some(mut issue) = replaced(&at)?.into_iter().next() {
            issue.level = IssueLevel::Error;
            issue.message.push_str(
                "; following it on a reference table is not supported, so order each variable on its own.",
            );
            outcome.findings.push(issue);
            return Ok(outcome);
        }
    }
    materialize(scope, b, binding, &variable, outcome)
}

#[allow(clippy::too_many_lines)]
fn materialize(
    scope: &Scope,
    b: usize,
    binding: &Binding,
    variable: &Variable,
    mut outcome: Outcome,
) -> Result<Outcome, Error> {
    let at = Located { scope, b, variable };
    let mut pieces = Vec::new();
    for k in 0..scope.serving.len() {
        pieces.extend(served(scope, k, variable.id)?);
    }
    if let Some(version) = &variable.classification {
        // Ambiguous windows must still block: their unresolved version is not a clip.
        pieces.retain(|p| p.state == "ambiguous" || p.class_version.as_ref() == Some(version));
        let mut compatible = !pieces.is_empty();
        for &table in scope.chain {
            if compatible {
                break;
            }
            compatible = scope.conn.query_row(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM ({}) WHERE variable_id = ?1 \
                     AND table_id = ?2 AND cv_pid = ?3)",
                    segments(scope)
                ),
                rusqlite::params![variable.id, table, version],
                |row| row.get(0),
            )?;
        }
        if !compatible {
            let mut finding = at.issue(
                IssueLevel::Error,
                "classification_incompatible",
                scope.table,
                Some(&hull(scope.requested)),
                format!(
                    "{} has no codes from {} over the requested period.",
                    variable.pid,
                    binding.classification.as_deref().expect("resolved pin")
                ),
            );
            finding.path.push_str("/classification");
            outcome.findings.push(finding);
            outcome.findings.extend(replaced(&at)?);
            return Ok(outcome);
        }
    }
    pieces.sort_by(|a, b| a.days.cmp(&b.days));
    let ordered = merge(pieces.iter().map(|p| p.days.clone()).collect());
    if ordered.is_empty() {
        let requested = hull(scope.requested);
        outcome.findings.push(at.issue(
            IssueLevel::Error,
            "outside_availability",
            scope.table,
            Some(&requested),
            format!(
                "{} is not available over {}.",
                variable.pid,
                span(scope.server, &requested)
            ),
        ));
        outcome.findings.extend(replaced(&at)?);
        return Ok(outcome);
    }
    let removed = gaps(scope.requested, ordered.clone());
    if !removed.is_empty() {
        outcome.clip = Some(OrderClip {
            source: scope.source.name.clone(),
            variable: variable.pid.clone(),
            variable_name: String::new(),
            requested: periods(scope.server, scope.requested),
            ordered: periods(scope.server, &ordered),
        });
        for days in &removed {
            outcome.findings.push(at.issue(
                IssueLevel::Info,
                "period_clipped",
                scope.table,
                Some(days),
                format!(
                    "There is no data on {} in {} for {}. Those years will not be delivered.",
                    variable.pid,
                    scope.table,
                    span(scope.server, days)
                ),
            ));
        }
    }
    outcome.findings.extend(windows(&at, &pieces));
    outcome.findings.extend(editions(&at, &pieces)?);
    outcome.findings.extend(replaced(&at)?);
    let versions: std::collections::BTreeSet<&String> = pieces
        .iter()
        .filter_map(|p| p.class_version.as_ref())
        .collect();
    if versions.len() > 1 && variable.classification.is_none() {
        let days = hull(&ordered);
        outcome.findings.push(at.issue(
            IssueLevel::Warning,
            "classification_version_ambiguous",
            scope.table,
            Some(&days),
            format!(
                "{} carries more than one classification version over {}; pin one with \
                 \"classification\".",
                variable.pid,
                span(scope.server, &days)
            ),
        ));
    }
    let (entries, change) = entries(&at, binding, &pieces);
    outcome
        .findings
        .extend(type_differs(&at, binding, &entries));
    outcome.entries = entries;
    outcome.findings.extend(change);
    Ok(outcome)
}

/// `binding_type_differs`: one per entry whose catalog type is known and is not the
/// binding's type override, at the entry's period. An unknown catalog type is what the
/// override fills, so it is not reported. A steward's order drops the finding where the
/// held storage is of the override's family (`materialize::build`, G14).
fn type_differs(at: &Located, binding: &Binding, entries: &[OrderEntry]) -> Vec<Issue> {
    let Some(stated) = binding.column_type else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|e| {
            let catalog = e.dtype.as_deref()?;
            (catalog != stated.as_str()).then(|| {
                let mut issue = at.issue(
                    IssueLevel::Warning,
                    "binding_type_differs",
                    &e.table,
                    None,
                    format!(
                        "the project gives {} the type {}, but the catalog has {catalog} in \
                         {} over [{}, {})",
                        e.variable,
                        stated.as_str(),
                        e.table,
                        e.period.start.as_deref().unwrap_or("open"),
                        e.period.end.as_deref().unwrap_or("open"),
                    ),
                );
                issue.period = Some(e.period.clone());
                issue
            })
        })
        .collect()
}

/// A binding's finding constructor.
struct Located<'a> {
    scope: &'a Scope<'a>,
    b: usize,
    variable: &'a Variable,
}

impl Located<'_> {
    fn issue(
        &self,
        level: IssueLevel,
        code: &str,
        table: &str,
        days: Option<&Interval>,
        message: String,
    ) -> Issue {
        Issue {
            source: Some(self.scope.source.name.clone()),
            variable: Some(self.variable.pid.clone()),
            table: Some(table.to_owned()),
            period: days.map(|d| period(self.scope.server, d)),
            ..Issue::new(
                level,
                code,
                format!("/sources/{}/bindings/{}", self.scope.index, self.b),
                message,
            )
        }
    }

    /// The pid of serving table `k`.
    fn table_of(&self, k: usize) -> &str {
        &self.scope.serving[k].table.pid
    }
}

/// Whether the variable has a segment anywhere in the succession chain; one there but not
/// on the requested days is `outside_availability`, not `variable_not_in_table`.
pub(super) fn in_chain(scope: &Scope, variable: i64) -> Result<bool, Error> {
    for &table in scope.chain {
        let hit: bool = scope.conn.query_row(
            &format!(
                "SELECT EXISTS (SELECT 1 FROM ({}) WHERE variable_id = ?1 AND table_id = ?2)",
                segments(scope)
            ),
            [variable, table],
            |row| row.get(0),
        )?;
        if hit {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The pieces serving table `k` gives the variable over its requested days: the primary
/// segments, clipped to them.
fn served(scope: &Scope, k: usize, variable: i64) -> Result<Vec<Piece>, Error> {
    let s = &scope.serving[k];
    let rows: Vec<Piece> = rows(
        scope.conn,
        &format!(
            "SELECT start, end, exact, dtype, length, vs_hash, vs_n, cv_pid, sensitive, \
             identifier, state, end = ?3
             FROM ({}) WHERE variable_id = ?1 AND table_id = ?2
             ORDER BY start",
            segments(scope)
        ),
        rusqlite::params![variable, s.table.id, scope.server.open.1],
        |row| piece(k, row),
    )?;
    let mut pieces = Vec::new();
    for row in rows {
        for days in overlap(std::slice::from_ref(&row.days), &s.requested) {
            pieces.push(Piece {
                days,
                ..row.clone()
            });
        }
    }
    Ok(pieces)
}

/// A [`segments`] row as [`served`] selects it, then `open`.
fn piece(serving: usize, row: &Row) -> rusqlite::Result<Piece> {
    let hash: Option<String> = row.get(5)?;
    Ok(Piece {
        serving,
        days: interval(row.get(0)?, row.get(1)?),
        spelling: row.get(2)?,
        dtype: row.get(3)?,
        length: row.get(4)?,
        valueset: hash.map(|h| row.get(6).map(|n| (h, n))).transpose()?,
        class_version: row.get(7)?,
        sensitive: row.get(8)?,
        identifier: row.get(9)?,
        state: row.get(10)?,
        open: row.get(11)?,
    })
}

/// The window findings: the blocking kind (an ambiguous piece) and the warnings, flags
/// and open ends, one per run of adjacent pieces in one table. A preliminary piece is one
/// only a preliminary release covers (final wins where both exist); it orders, warned.
fn windows(at: &Located, pieces: &[Piece]) -> Vec<Issue> {
    let scope = at.scope;
    let pid = &at.variable.pid;
    let mut found = Vec::new();
    let mut each =
        |test: &dyn Fn(&Piece) -> bool, level, code, message: &dyn Fn(&str, &str) -> String| {
            for (k, days) in runs(pieces, test) {
                let table = at.table_of(k);
                let text = message(table, &span(scope.server, &days));
                found.push(at.issue(level, code, table, Some(&days), text));
            }
        };
    each(
        &|p| p.state == "ambiguous",
        IssueLevel::Error,
        "ambiguous_window",
        &|t, p| {
            format!(
                "{pid} has more than one form in {t} over {p} (name, type or codes differ); pick a shorter period or ask the maintainer"
            )
        },
    );
    each(
        &|p| p.state == "preliminary",
        IssueLevel::Warning,
        "preliminary_data",
        &|t, p| format!("{pid} in {t} over {p} is preliminary data: no final release covers it"),
    );
    each(
        &|p| p.sensitive,
        IssueLevel::Warning,
        "variable_sensitive",
        &|t, p| format!("{pid} is sensitive in {t} over {p}"),
    );
    each(
        &|p| p.identifier,
        IssueLevel::Info,
        "identifier",
        &|t, p| format!("{pid} is an identifier in {t} over {p}"),
    );
    each(
        &|p| p.open && !scope.serving[p.serving].table.timeless,
        IssueLevel::Info,
        "coverage_end_unstated",
        &|t, p| {
            format!(
                "{pid} in {t}: the provider states no end year, so data over {p} is not confirmed"
            )
        },
    );
    found
}

/// The runs of adjacent pieces in one table that pass `test`, as (serving index, days).
fn runs(pieces: &[Piece], test: &dyn Fn(&Piece) -> bool) -> Vec<(usize, Interval)> {
    let mut runs: Vec<(usize, Interval)> = Vec::new();
    for p in pieces.iter().filter(|p| test(p)) {
        match runs.last_mut() {
            Some((k, days)) if *k == p.serving && next_iso_day(&days.1) == p.days.0 => {
                days.1.clone_from(&p.days.1);
            }
            _ => runs.push((p.serving, p.days.clone())),
        }
    }
    runs
}

/// `edition_disagreement`: reg-resolve's F7 marks a release
/// (`release.edition_disagreement`), so it reaches the binding through a release of a
/// serving table that documents the variable and overlaps its pieces there.
// simplify: F7 marks the release, not the keys that disagree, so a variable the release
// documents in agreement is reported too; revisit if F7 gains a variable subject.
fn editions(at: &Located, pieces: &[Piece]) -> Result<Vec<Issue>, Error> {
    let scope = at.scope;
    let mut found = Vec::new();
    let part = |db: &str, table: &str, variable: &str, joins: &str| {
        format!(
            "SELECT r.start, r.end
             FROM {db}.release r {joins}
             WHERE r.edition_disagreement = 1
               AND r.start IS NOT NULL AND {table} = ?1
               AND EXISTS (SELECT 1 FROM {db}.occurrence o {variable})"
        )
    };
    let base = part(
        "main",
        "r.table_id",
        "WHERE o.release_id = r.id AND o.variable_id = ?2",
        "",
    );
    // The holdings database's own releases reach a base table and variable by public id.
    let sql = if scope.composed {
        let held = part(
            "held",
            "bt.id",
            "JOIN held.variable hv ON hv.id = o.variable_id \
             LEFT JOIN main.variable bv ON bv.pid = hv.pid \
             WHERE o.release_id = r.id AND COALESCE(bv.id, -hv.id) = ?2",
            "JOIN held.tbl ht ON ht.id = r.table_id JOIN main.tbl bt ON bt.pid = ht.pid",
        );
        format!("SELECT DISTINCT start, end FROM ({base} UNION ALL {held}) ORDER BY start, end")
    } else {
        format!("SELECT DISTINCT start, end FROM ({base}) ORDER BY start, end")
    };
    let mut query = scope.conn.prepare(&sql)?;
    for (k, s) in scope.serving.iter().enumerate() {
        let here = merge(
            pieces
                .iter()
                .filter(|p| p.serving == k)
                .map(|p| p.days.clone())
                .collect(),
        );
        let rows: Vec<(i64, i64)> = query
            .query_map([s.table.id, at.variable.id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        for (start, end) in rows {
            let hit = overlap(&here, &[interval(start, end)]);
            if hit.is_empty() {
                continue;
            }
            let days = hull(&hit);
            found.push(at.issue(
                IssueLevel::Warning,
                "edition_disagreement",
                &s.table.pid,
                Some(&days),
                format!(
                    "the preliminary and final releases of {} differ on {} over {}",
                    s.table.pid,
                    at.variable.pid,
                    span(scope.server, &days)
                ),
            ));
        }
    }
    Ok(found)
}

/// `variable_replaced`: an incoming variable `continues` edge whose `at_start` lies
/// before the request end, or that states no `at` (then with no period: no day is
/// inferred). This notice does not route the request; opt-in following is handled
/// by `successor` before the per-window checks.
fn replaced(at: &Located) -> Result<Vec<Issue>, Error> {
    let scope = at.scope;
    let requested = hull(scope.requested);
    // A variable only the holdings database defines states its succession there.
    let (origin, id) = if at.variable.id < 0 {
        ("held.", -at.variable.id)
    } else {
        ("", at.variable.id)
    };
    let rows: Vec<(String, Option<i64>, Option<String>)> = rows(
        scope.conn,
        &format!(
            "SELECT v.pid, r.at_start, c.body
             FROM {origin}relation r JOIN {origin}variable v ON v.id = r.subject_id
               LEFT JOIN {origin}text c ON c.id = r.caveat_text
             WHERE r.kind = 'continues' AND r.subject_kind = 'variable'
               AND r.object_kind = 'variable' AND r.object_id = ?1
               AND (r.at_start IS NULL OR r.at_start <= ?2)
             ORDER BY r.at_start, v.pid"
        ),
        rusqlite::params![id, day(&requested.1)],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok(rows
        .into_iter()
        .map(|(successor, at_start, caveat)| {
            let days = at_start.map(|a| (iso_day(a).max(requested.0.clone()), requested.1.clone()));
            let from = at_start.map_or_else(
                || "; the date is not stated".to_owned(),
                |a| format!(" from {}", iso_day(a)),
            );
            let caveat = caveat.map_or_else(String::new, |c| format!(": {c}"));
            let mut issue = at.issue(
                IssueLevel::Warning,
                "variable_replaced",
                scope.table,
                days.as_ref(),
                format!(
                    "{} is continued by {successor}{from}{caveat}",
                    at.variable.pid
                ),
            );
            issue.successor_fqid = Some(successor);
            issue
        })
        .collect())
}

/// The entries: adjacent pieces of one table merge when spelling, type and value set
/// and classification agree, so a flag-only boundary never splits one; the latest
/// piece's length stands. More than one spelling/type/value-set form is a `form_change`.
fn entries(at: &Located, binding: &Binding, pieces: &[Piece]) -> (Vec<OrderEntry>, Option<Issue>) {
    let scope = at.scope;
    let mut merged: Vec<&Piece> = Vec::new();
    let mut spans: Vec<Interval> = Vec::new();
    for p in pieces {
        let joins = merged.last().is_some_and(|last| {
            last.serving == p.serving
                && next_iso_day(&spans[spans.len() - 1].1) == p.days.0
                && (&last.spelling, &last.dtype, &last.valueset)
                    == (&p.spelling, &p.dtype, &p.valueset)
                && last.class_version == p.class_version
        });
        if joins {
            let n = merged.len() - 1;
            merged[n] = p;
            spans[n].1.clone_from(&p.days.1);
        } else {
            merged.push(p);
            spans.push(p.days.clone());
        }
    }
    let mut boundaries = Vec::new();
    let mut spellings = vec![merged[0].spelling.as_str()];
    for (pair, days) in merged.windows(2).zip(&spans[1..]) {
        if (&pair[0].spelling, &pair[0].dtype, &pair[0].valueset)
            != (&pair[1].spelling, &pair[1].dtype, &pair[1].valueset)
        {
            boundaries.push(days.0.clone());
            spellings.push(&pair[1].spelling);
        }
    }
    let change = (!boundaries.is_empty()).then(|| {
        let days = (spans[0].0.clone(), spans[spans.len() - 1].1.clone());
        at.issue(
            IssueLevel::Info,
            "form_change",
            scope.table,
            Some(&days),
            format!(
                "{} changes form at {} ({})",
                at.variable.pid,
                boundaries.join(", "),
                spellings.join(" -> ")
            ),
        )
    });
    let mut entries = Vec::new();
    for (p, days) in merged.into_iter().zip(&spans) {
        let table = &scope.serving[p.serving].table;
        entries.push(OrderEntry {
            source: scope.source.name.clone(),
            binding: at.b,
            provider: table.provider.clone(),
            register: table.register.clone(),
            table: table.pid.clone(),
            table_name: String::new(),
            variable: at.variable.pid.clone(),
            variable_name: String::new(),
            display_name: binding.display_name.clone(),
            period: period(scope.server, days),
            spelling: p.spelling.clone(),
            dtype: p.dtype.clone(),
            length: p.length,
            value_set: p
                .valueset
                .clone()
                .map(|(hash, n_items)| OrderValueSet { hash, n_items }),
            classification: p.class_version.clone(),
        });
    }
    (entries, change)
}

/// The result periods of inclusive ISO intervals.
pub(super) fn periods(server: &Server, intervals: &[Interval]) -> Vec<crate::ops::Period> {
    intervals.iter().map(|i| period(server, i)).collect()
}

/// The first to the last day of non-empty intervals.
fn hull(intervals: &[Interval]) -> Interval {
    reg_core::hull(intervals).expect("non-empty intervals")
}
