//! `series_break` (crates/DESIGN.md, "Series breaks"): the provider's series breaks an
//! order meets. A dated break is met when the ordered days span its day; an undated
//! release break when they overlap the release; any other undated break whenever its
//! entity is ordered. One finding per entity and text, a warning when the provider says
//! what changed, else an info.

use std::collections::BTreeMap;

use reg_core::{Interval, IssueLevel, overlap};
use rusqlite::Connection;

use super::{Issue, Serving, interval, iso_day, period, words};
use crate::ops::Server;
use crate::{Error, rows};

/// One stored break: its day, text and, for a release break, the release's name and
/// days.
pub(super) struct Stored {
    at: Option<String>,
    text: Option<String>,
    release: Option<(String, Interval)>,
}

/// Where the met breaks of one entity are reported.
pub(super) struct Anchor<'a> {
    pub server: &'a Server,
    /// The entity's public id, named in the message.
    pub pid: &'a str,
    /// The finding's fields before its level, code, period and message.
    pub base: &'a dyn Fn(IssueLevel, String) -> Issue,
}

/// A table's breaks, its releases' and its register's, keyed by subject kind.
fn table(conn: &Connection, table: i64) -> Result<BTreeMap<&'static str, Vec<Stored>>, Error> {
    let mut by = BTreeMap::new();
    for (kind, sql) in [
        (
            "table",
            "SELECT b.at, t.body, NULL, NULL, NULL FROM series_break b
               LEFT JOIN text t ON t.id = b.text_id
             WHERE b.subject_kind = 'table' AND b.subject_id = ?1",
        ),
        (
            "table",
            "SELECT b.at, t.body, n.body, x.start, x.end FROM series_break b
               JOIN release x ON x.id = b.subject_id JOIN text n ON n.id = x.name_text
               LEFT JOIN text t ON t.id = b.text_id
             WHERE b.subject_kind = 'release' AND x.table_id = ?1",
        ),
        (
            "register",
            "SELECT b.at, t.body, NULL, NULL, NULL FROM series_break b
               JOIN tbl g ON g.register_id = b.subject_id AND g.id = ?1
               LEFT JOIN text t ON t.id = b.text_id
             WHERE b.subject_kind = 'register'",
        ),
    ] {
        let found = load(conn, sql, table)?;
        by.entry(kind).or_insert_with(Vec::new).extend(found);
    }
    Ok(by)
}

/// A variable's own breaks (its id in `conn`).
pub(super) fn variable(conn: &Connection, variable: i64) -> Result<Vec<Stored>, Error> {
    load(
        conn,
        "SELECT b.at, t.body, NULL, NULL, NULL FROM series_break b
           LEFT JOIN text t ON t.id = b.text_id
         WHERE b.subject_kind = 'variable' AND b.subject_id = ?1",
        variable,
    )
}

fn load(conn: &Connection, sql: &str, id: i64) -> Result<Vec<Stored>, Error> {
    Ok(rows(conn, &format!("{sql} ORDER BY 1, 2, 4"), [id], |row| {
        let at: Option<i64> = row.get(0)?;
        let name: Option<String> = row.get(2)?;
        let release = match name {
            Some(name) => Some((name, interval(row.get(3)?, row.get(4)?))),
            None => None,
        };
        Ok(Stored {
            at: at.map(iso_day),
            text: row.get(1)?,
            release,
        })
    })?)
}

/// The findings for `breaks` of one entity over the ordered `days` (ascending).
pub(super) fn met(anchor: &Anchor, breaks: &[Stored], days: &[Interval]) -> Vec<Issue> {
    let (Some(first), Some(last)) = (days.first(), days.last()) else {
        return vec![];
    };
    // By text: the days it is dated at, the releases it is on, and whether it is undated.
    let mut by_text: BTreeMap<Option<&str>, (Vec<&str>, Vec<(&str, Interval)>, bool)> =
        BTreeMap::new();
    for b in breaks {
        let entry = by_text.entry(b.text.as_deref()).or_default();
        match (&b.at, &b.release) {
            (Some(at), _) => {
                if first.0 < *at && *at <= last.1 {
                    entry.0.push(at);
                }
            }
            (None, Some((name, scope))) => {
                let hit = overlap(days, std::slice::from_ref(scope));
                if let (Some(lo), Some(hi)) = (hit.first(), hit.last()) {
                    entry.1.push((name, (lo.0.clone(), hi.1.clone())));
                }
            }
            (None, None) => entry.2 = true,
        }
    }
    let mut found = Vec::new();
    for (text, (dated, releases, undated)) in by_text {
        if dated.is_empty() && releases.is_empty() && !undated {
            continue;
        }
        let mut when = Vec::new();
        if !dated.is_empty() {
            let plain: Vec<String> = dated.iter().map(|d| words::plain_start(d)).collect();
            when.push(format!("from {}", plain.join(" and ")));
        }
        if !releases.is_empty() {
            let names: Vec<&str> = releases.iter().map(|(n, _)| *n).collect();
            when.push(format!("in the {} release{}", names.join(", "), if names.len() > 1 { "s" } else { "" }));
        }
        if undated {
            when.push("on no stated day".to_owned());
        }
        let pid = anchor.pid;
        let (level, message) = match text {
            Some(text) => (
                IssueLevel::Warning,
                format!("The provider notes a series break in {pid} {}: {text}", when.join("; ")),
            ),
            None => (
                IssueLevel::Info,
                format!(
                    "The provider marks a series break in {pid} {} but says nothing more about it.",
                    when.join("; ")
                ),
            ),
        };
        let mut issue = (anchor.base)(level, message);
        // The finding's period: the first stated day, else the first met release's days.
        let days = dated
            .first()
            .map(|d| ((*d).to_owned(), (*d).to_owned()))
            .or_else(|| releases.first().map(|(_, d)| d.clone()));
        issue.period = days.map(|d| period(anchor.server, &d));
        found.push(issue);
    }
    found
}

/// The breaks of every serving table, its releases and its register, each register
/// once, over that table's requested days.
pub(super) fn source(
    server: &Server,
    conn: &Connection,
    serving: &[Serving],
    issue: &dyn Fn(IssueLevel, &str, String) -> Issue,
) -> Result<Vec<Issue>, Error> {
    let mut found = Vec::new();
    let mut registers: BTreeMap<&str, (Vec<Stored>, Vec<Interval>)> = BTreeMap::new();
    for s in serving {
        let mut by = table(conn, s.table.id)?;
        let base = |level, message| issue(level, &s.table.pid, message);
        found.extend(met(
            &Anchor {
                server,
                pid: &s.table.pid,
                base: &base,
            },
            &by.remove("table").unwrap_or_default(),
            &s.requested,
        ));
        let register = registers.entry(&s.table.register).or_default();
        if register.0.is_empty() {
            register.0 = by.remove("register").unwrap_or_default();
        }
        register.1.extend(s.requested.iter().cloned());
    }
    for (pid, (breaks, days)) in registers {
        let days = reg_core::merge(days);
        let table = serving
            .iter()
            .find(|s| s.table.register == pid)
            .map_or("", |s| s.table.pid.as_str());
        let base = |level, message| issue(level, table, message);
        found.extend(met(
            &Anchor {
                server,
                pid,
                base: &base,
            },
            &breaks,
            &days,
        ));
    }
    Ok(found)
}
