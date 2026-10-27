//! One binding of a source: its ids, then the request over the variable's segments in
//! the serving tables. The request minus the segments is the clip; the rest, split by
//! merged form, is the binding's entries; the window findings come from the segments
//! the entries cover.

use reg_core::project::Edition;
use reg_core::project::{Binding, Source};
use reg_core::{Interval, IssueLevel, gaps, intersect, merge, next_iso_day, overlap};
use rusqlite::{Connection, OptionalExtension, Row};

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
}

/// A resolved binding variable.
pub(super) struct Variable {
    id: i64,
    pid: String,
    pinned: bool,
}

/// One served piece of the request: part of a segment (or, in an edition's window, of
/// an occurrence of that edition's release) inside the requested days.
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

/// The binding's variable and its classification pin, or the findings refusing them
/// (`fqid_unresolved`, `retired_ref`), located at the field.
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
    if let Some(pin) = &binding.classification {
        match refs::resolve(conn, pin) {
            Ok(hit) if hit.kind == "class_version" => {}
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
    let variable = match variable {
        Some((id, pid)) if findings.is_empty() => Some(Variable {
            id,
            pid,
            pinned: binding.classification.is_some(),
        }),
        _ => None,
    };
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
    let mut pieces = Vec::new();
    for k in 0..scope.serving.len() {
        pieces.extend(served(scope, k, variable.id)?);
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
    if versions.len() > 1 && !variable.pinned {
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

/// Whether the variable has a segment anywhere in the succession chain; one there but
/// not on the requested days is `outside_availability`, not `variable_not_in_table`.
fn in_chain(scope: &Scope, variable: i64) -> Result<bool, Error> {
    for &table in scope.chain {
        let hit: Option<i64> = scope
            .conn
            .query_row(
                "SELECT 1 FROM segment WHERE variable_id = ? AND table_id = ? LIMIT 1",
                [variable, table],
                |row| row.get(0),
            )
            .optional()?;
        if hit.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The pieces serving table `k` gives the variable over its requested days. With an
/// edition or a qualifier, the windows where a release of that edition (and qualifier;
/// with a qualifier alone, the primary releases of that qualifier) exists are served
/// from that release's occurrences (project-data-next §1); elsewhere the segments, the
/// primary edition, serve, except that a qualifier serves nothing outside its releases.
fn served(scope: &Scope, k: usize, variable: i64) -> Result<Vec<Piece>, Error> {
    let s = &scope.serving[k];
    let edition = scope.source.edition.map(reg_core::project::Edition::as_str);
    let qualifier = scope.source.qualifier.as_deref();
    let mut pieces = Vec::new();
    let mut by_segment = s.requested.clone();
    if edition.is_some() || qualifier.is_some() {
        let releases: Vec<(i64, i64, i64)> = scope
            .conn
            .prepare(
                "SELECT id, start, end FROM release
                 WHERE table_id = ?1 AND start IS NOT NULL
                   AND (?2 IS NULL OR edition = ?2) AND (?3 IS NULL OR qualifier = ?3)
                   AND (?2 IS NOT NULL OR is_primary = 1)
                 ORDER BY start, end, id",
            )?
            .query_map(rusqlite::params![s.table.id, edition, qualifier], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        let windows = overlap(
            &s.requested,
            &merge(releases.iter().map(|r| interval(r.1, r.2)).collect()),
        );
        // simplify: two selected releases over one day (two qualifiers of one edition)
        // serve it from the first by (start, end, id); revisit when a source names an
        // edition over qualified releases.
        let mut taken: Vec<Interval> = Vec::new();
        let mut occurrences = scope.conn.prepare(&format!(
            "SELECT x.start, x.end, {FORM},
                    CASE WHEN g.state = 'ambiguous' THEN 'ambiguous' ELSE 'clear' END,
                    x.end = ?3
             FROM occurrence x JOIN spelling sp ON sp.id = x.spelling_id
               LEFT JOIN segment g ON g.id = x.segment_id
             WHERE x.variable_id = ?1 AND x.release_id = ?2 AND x.status IN ('used', 'shadowed')
             ORDER BY x.start, x.end, x.id"
        ))?;
        for (release, ..) in &releases {
            let rows: Vec<Piece> = occurrences
                .query_map(
                    rusqlite::params![variable, release, scope.server.open.1],
                    |row| piece(k, row),
                )?
                .collect::<rusqlite::Result<_>>()?;
            let contested = contested(&rows);
            for row in rows {
                for days in gaps(
                    &overlap(std::slice::from_ref(&row.days), &windows),
                    taken.clone(),
                ) {
                    taken.push(days.clone());
                    let whole = std::slice::from_ref(&days);
                    for days in overlap(whole, &contested) {
                        pieces.push(Piece {
                            days,
                            state: "ambiguous".to_owned(),
                            ..row.clone()
                        });
                    }
                    for days in gaps(whole, contested.clone()) {
                        pieces.push(Piece {
                            days,
                            ..row.clone()
                        });
                    }
                }
            }
        }
        by_segment = if qualifier.is_some() {
            Vec::new()
        } else {
            gaps(&s.requested, windows)
        };
    }
    if by_segment.is_empty() {
        return Ok(pieces);
    }
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
    for row in rows {
        for days in overlap(std::slice::from_ref(&row.days), &by_segment) {
            pieces.push(Piece {
                days,
                ..row.clone()
            });
        }
    }
    Ok(pieces)
}

/// The days two occurrences of one release cover in different forms. A shadowed
/// occurrence has no segment to be ambiguous in, so they are found here, before
/// first-wins subtraction drops the second form.
fn contested(rows: &[Piece]) -> Vec<Interval> {
    let mut days = Vec::new();
    for (i, a) in rows.iter().enumerate() {
        for b in &rows[i + 1..] {
            if (&a.spelling, &a.dtype, a.valueset) != (&b.spelling, &b.dtype, b.valueset)
                && let Some(both) = intersect(&a.days, &b.days)
            {
                days.push(both);
            }
        }
    }
    merge(days)
}

/// A segment or occurrence row: `start, end`, the [`FORM`] columns, `state` and `open`.
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

/// The window findings: the two blocking kinds (an ambiguous piece; a preliminary one
/// unless the source names exactly `"edition": "preliminary"`) and the flags and open
/// ends, one per run of adjacent pieces in one table.
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
    if scope.source.edition != Some(Edition::Preliminary) {
        each(
            &|p| p.state == "preliminary",
            IssueLevel::Error,
            "preliminary_without_edition",
            &|t, p| {
                format!(
                    "{pid} in {t} over {p} has only a preliminary edition; name it with \
                     \"edition\": \"preliminary\""
                )
            },
        );
    }
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
/// before the request end. Reported, never followed (following waits for D).
fn replaced(at: &Located) -> Result<Vec<Issue>, Error> {
    let scope = at.scope;
    let requested = hull(scope.requested);
    let rows: Vec<(String, i64, Option<String>)> = scope
        .conn
        .prepare(
            "SELECT v.pid, r.at_start, c.body
             FROM relation r JOIN variable v ON v.id = r.subject_id
               LEFT JOIN text c ON c.id = r.caveat_text
             WHERE r.kind = 'continues' AND r.subject_kind = 'variable'
               AND r.object_kind = 'variable' AND r.object_id = ?1
               AND r.at_start IS NOT NULL AND r.at_start <= ?2
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
            let from = iso_day(at_start).max(requested.0.clone());
            let days = (from, requested.1.clone());
            let caveat = caveat.map_or_else(String::new, |c| format!(": {c}"));
            let mut issue = at.issue(
                IssueLevel::Warning,
                "variable_replaced",
                scope.table,
                Some(&days),
                format!(
                    "{} is continued by {successor} from {}{caveat}",
                    at.variable.pid,
                    iso_day(at_start)
                ),
            );
            issue.successor_fqid = Some(successor);
            issue
        })
        .collect())
}

/// The entries: adjacent pieces of one table merge when spelling, type and value set
/// agree (v4 §4.6), so a flag-only boundary never splits one; the latest piece's length
/// and class version stand. More than one merged form is a `form_change`.
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
            edition: scope.source.edition.map(|e| e.as_str().to_owned()),
            qualifier: scope.source.qualifier.clone(),
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
fn periods(server: &Server, intervals: &[Interval]) -> Vec<crate::ops::Period> {
    intervals.iter().map(|i| period(server, i)).collect()
}

/// The first to the last day of ascending intervals.
fn hull(intervals: &[Interval]) -> Interval {
    (
        intervals[0].0.clone(),
        intervals[intervals.len() - 1].1.clone(),
    )
}
