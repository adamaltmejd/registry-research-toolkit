//! One binding of a source: its ids, then the request over the variable's primary
//! segments in the serving tables. The request minus the segments is the clip; the rest, split by merged form, is the binding's entries; the
//! window findings come from the segments the entries cover.

use reg_core::project::{Binding, Source};
use reg_core::{Interval, IssueLevel, gaps, merge, next_iso_day, overlap};
use rusqlite::{Connection, Row};

use super::{
    Issue, OrderClip, OrderEntry, OrderValueSet, Serving, day, interval, iso_day, period, refused,
    span,
};
use crate::Error;
use crate::ops::{Server, refs};

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
    classification: Option<i64>,
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
    valueset: Option<i64>,
    class_version: Option<i64>,
    sensitive: bool,
    identifier: bool,
    state: String,
    /// Its source row ends at the open sentinel: no coverage end is stated.
    open: bool,
}

/// The columns [`piece`] reads, after `start, end`.
const FORM: &str = "sp.exact, x.dtype, x.length, x.valueset_id, x.class_version_id, \
    COALESCE(x.sensitive, 0), COALESCE(x.identifier, 0)";

/// The binding's variable and its classification pin, and the findings refusing them
/// (`fqid_unresolved`, `retired_ref`), located at the field. A resolved variable is
/// returned even when its pin is refused, so its window checks still run: a blocked
/// order reports every finding at once.
pub(super) fn refs(
    conn: &Connection,
    index: usize,
    b: usize,
    source: &Source,
    binding: &Binding,
    table: Option<&str>,
) -> Result<(Option<Variable>, Vec<Issue>), Error> {
    let mut findings = Vec::new();
    let mut issue = |field: &str, variable: Option<&str>, (code, message): (&str, String)| {
        findings.push(Issue {
            level: IssueLevel::Error,
            code: code.to_owned(),
            path: format!("/sources/{index}/bindings/{b}/{field}"),
            message,
            source: Some(source.name.clone()),
            variable: variable.map(str::to_owned),
            table: table.map(str::to_owned),
            period: None,
            successor_fqid: None,
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
        Err(err) => {
            issue(
                "variable",
                None,
                refused(conn, &binding.variable, "variable", &err)?,
            );
            None
        }
    };
    let pid = variable.as_ref().map(|(_, pid)| pid.as_str());
    let mut classification = None;
    if let Some(pin) = &binding.classification {
        match refs::resolve(conn, pin) {
            Ok(hit) if hit.kind == "class_version" => classification = Some(hit.id),
            Ok(_) => issue(
                "classification",
                pid,
                unresolved(pin, "classification version"),
            ),
            Err(err) => issue(
                "classification",
                pid,
                refused(conn, pin, "classification version", &err)?,
            ),
        }
    }
    let variable = variable.map(|(id, pid)| Variable {
        id,
        pid,
        classification,
    });
    Ok((variable, findings))
}

/// One binding over its source's serving tables.
pub(super) fn binding(scope: &Scope, b: usize, binding: &Binding) -> Result<Outcome, Error> {
    let (variable, findings) = refs(
        scope.conn,
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
        return super::successor::binding(scope, b, binding);
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
    if let Some(version) = variable.classification {
        // Ambiguous windows must still block: their unresolved version is not a clip.
        pieces.retain(|p| p.state == "ambiguous" || p.class_version == Some(version));
        let mut compatible = !pieces.is_empty();
        for &table in scope.chain {
            if compatible {
                break;
            }
            compatible = scope.conn.query_row(
                "SELECT EXISTS (SELECT 1 FROM segment WHERE variable_id = ?1 \
                 AND table_id = ?2 AND class_version_id = ?3)",
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
                    "{} has no curated window for {} over the requested period.",
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
                    "{} is not available over {}; that part is not ordered",
                    variable.pid,
                    span(scope.server, days)
                ),
            ));
        }
    }
    outcome.findings.extend(windows(&at, &pieces));
    outcome.findings.extend(editions(&at, &pieces)?);
    outcome.findings.extend(replaced(&at)?);
    let versions: std::collections::BTreeSet<i64> =
        pieces.iter().filter_map(|p| p.class_version).collect();
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
    let (entries, change) = entries(&at, binding, &pieces)?;
    outcome.entries = entries;
    outcome.findings.extend(change);
    Ok(outcome)
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
            level,
            code: code.to_owned(),
            path: format!("/sources/{}/bindings/{}", self.scope.index, self.b),
            message,
            source: Some(self.scope.source.name.clone()),
            variable: Some(self.variable.pid.clone()),
            table: Some(table.to_owned()),
            period: days.map(|d| period(self.scope.server, d)),
            successor_fqid: None,
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
            "SELECT EXISTS (SELECT 1 FROM segment WHERE variable_id = ?1 AND table_id = ?2)",
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
    let rows: Vec<Piece> = scope
        .conn
        .prepare(&format!(
            "SELECT x.start, x.end, {FORM}, x.state, x.end = ?3
             FROM segment x JOIN spelling sp ON sp.id = x.spelling_id
             WHERE x.variable_id = ?1 AND x.table_id = ?2
             ORDER BY x.start"
        ))?
        .query_map(
            rusqlite::params![variable, s.table.id, scope.server.open.1],
            |row| piece(k, row),
        )?
        .collect::<rusqlite::Result<_>>()?;
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

/// A segment row: `start, end`, the [`FORM`] columns, `state` and `open`.
fn piece(serving: usize, row: &Row) -> rusqlite::Result<Piece> {
    Ok(Piece {
        serving,
        days: interval(row.get(0)?, row.get(1)?),
        spelling: row.get(2)?,
        dtype: row.get(3)?,
        length: row.get(4)?,
        valueset: row.get(5)?,
        class_version: row.get(6)?,
        sensitive: row.get(7)?,
        identifier: row.get(8)?,
        state: row.get(9)?,
        open: row.get(10)?,
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
            format!("{pid} cannot be placed on one form in {t} over {p}; the window needs curation")
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
        &|t, p| format!("{pid} identifies units in {t} over {p}"),
    );
    each(
        &|p| p.open && !scope.serving[p.serving].table.timeless,
        IssueLevel::Info,
        "coverage_end_unstated",
        &|t, p| format!("{pid} in {t} has no stated coverage end; data over {p} is not confirmed"),
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

/// `edition_disagreement`: reg-resolve's F7 finding is on a release, so it reaches the
/// binding through a release of a serving table that documents the variable and
/// overlaps its pieces there.
// simplify: F7 names the release, not the keys that disagree, so a variable the release
// documents in agreement is reported too; revisit if F7 gains a variable subject.
fn editions(at: &Located, pieces: &[Piece]) -> Result<Vec<Issue>, Error> {
    let scope = at.scope;
    let mut found = Vec::new();
    let mut query = scope.conn.prepare(
        "SELECT DISTINCT f.start, f.end
         FROM finding f JOIN release r ON r.id = f.subject_id
         WHERE f.subject_kind = 'release' AND f.code = 'F7:edition-disagreement'
           AND f.start IS NOT NULL AND r.table_id = ?1
           AND EXISTS (SELECT 1 FROM occurrence o WHERE o.release_id = r.id AND o.variable_id = ?2)
         ORDER BY f.start, f.end",
    )?;
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
                    "editions of {} disagree on {} over {}",
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
    let rows: Vec<(String, Option<i64>, Option<String>)> = scope
        .conn
        .prepare(
            "SELECT v.pid, r.at_start, c.body
             FROM relation r JOIN variable v ON v.id = r.subject_id
               LEFT JOIN text c ON c.id = r.caveat_text
             WHERE r.kind = 'continues' AND r.subject_kind = 'variable'
               AND r.object_kind = 'variable' AND r.object_id = ?1
               AND (r.at_start IS NULL OR r.at_start <= ?2)
             ORDER BY r.at_start, v.pid",
        )?
        .query_map(
            rusqlite::params![at.variable.id, day(&requested.1)],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?
        .collect::<rusqlite::Result<_>>()?;
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
fn entries(
    at: &Located,
    binding: &Binding,
    pieces: &[Piece],
) -> Result<(Vec<OrderEntry>, Option<Issue>), Error> {
    let scope = at.scope;
    let mut merged: Vec<&Piece> = Vec::new();
    let mut spans: Vec<Interval> = Vec::new();
    for p in pieces {
        let joins = merged.last().is_some_and(|last| {
            last.serving == p.serving
                && next_iso_day(&spans[spans.len() - 1].1) == p.days.0
                && (&last.spelling, &last.dtype, last.valueset)
                    == (&p.spelling, &p.dtype, p.valueset)
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
        if (&pair[0].spelling, &pair[0].dtype, pair[0].valueset)
            != (&pair[1].spelling, &pair[1].dtype, pair[1].valueset)
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
            variable: at.variable.pid.clone(),
            display_name: binding.display_name.clone(),
            period: period(scope.server, days),
            spelling: p.spelling.clone(),
            dtype: p.dtype.clone(),
            length: p.length,
            value_set: p.valueset.map(|id| value_set(scope.conn, id)).transpose()?,
            classification: p
                .class_version
                .map(|id| {
                    scope.conn.query_row(
                        "SELECT pid FROM class_version WHERE id = ?",
                        [id],
                        |row| row.get(0),
                    )
                })
                .transpose()?,
        });
    }
    Ok((entries, change))
}

fn value_set(conn: &Connection, id: i64) -> rusqlite::Result<OrderValueSet> {
    conn.query_row(
        "SELECT hash, n_items FROM valueset WHERE id = ?",
        [id],
        |row| {
            Ok(OrderValueSet {
                hash: row.get(0)?,
                n_items: row.get(1)?,
            })
        },
    )
}

/// The result periods of inclusive ISO intervals.
pub(super) fn periods(server: &Server, intervals: &[Interval]) -> Vec<crate::ops::Period> {
    intervals.iter().map(|i| period(server, i)).collect()
}

/// The first to the last day of ascending intervals.
fn hull(intervals: &[Interval]) -> Interval {
    (
        intervals[0].0.clone(),
        intervals[intervals.len() - 1].1.clone(),
    )
}
