//! The build's findings (resolution's review items, curation proposals and source
//! notices) as JSON Lines for curation work. They are not published in the catalog
//! (#2335); `build --findings FILE` writes them.
//!
//! One object per line, sorted by register, code, subject kind, subject and period, so
//! two builds from the same pins write the same bytes. A subject is named by keys that
//! survive a rebuild, never by catalog row id: a register, table, release or variable by
//! its public id (a release as `<table>/<delivery name>`), a segment as
//! `<variable>@<table>@<start>`, an occurrence as `<table>/<source key>@<start>`. Days
//! are ISO dates; a period is inclusive.

use std::path::Path;

use reg_resolve::Subject;
use serde::Serialize;

use crate::emit::{Catalog, pid_of_register, pid_of_table, pid_of_variable};
use crate::error::Result;
use crate::link::Entity;

/// Field order is the sort order.
#[derive(Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct Line {
    register: String,
    code: &'static str,
    subject_kind: &'static str,
    subject: String,
    period: Option<(String, String)>,
    severity: &'static str,
    message: String,
}

/// Every finding of `catalog`, sorted.
fn lines(catalog: &Catalog) -> Vec<Line> {
    let registers = &catalog.registers;
    let mut lines = Vec::new();
    for (ri, r) in registers.iter().enumerate() {
        for f in &r.findings {
            let (subject_kind, subject) = match f.subject {
                Subject::Register => ("register", pid_of_register(r)),
                Subject::Release(x) => ("release", Entity::Release(ri, x).pid(registers)),
                Subject::Variable(v) => ("variable", pid_of_variable(r, v)),
                Subject::Segment(v, s) => {
                    let segment = &r.variables[v].segments[s];
                    (
                        "segment",
                        format!(
                            "{}@{}@{}",
                            pid_of_variable(r, v),
                            pid_of_table(r, segment.table),
                            segment.period.0
                        ),
                    )
                }
                Subject::Occurrence(o) => {
                    let occ = &r.occurrences[o];
                    (
                        "occurrence",
                        format!(
                            "{}/{}@{}",
                            pid_of_table(r, occ.table),
                            occ.occurrence.source_key,
                            occ.period.as_ref().map_or("", |p| p.0.as_str())
                        ),
                    )
                }
            };
            lines.push(Line {
                register: pid_of_register(r),
                code: f.code,
                subject_kind,
                subject,
                period: f.period.clone(),
                severity: f.severity,
                message: f.message.clone(),
            });
        }
    }
    for notice in &catalog.notices {
        lines.push(Line {
            register: pid_of_register(&registers[notice.subject.register()]),
            code: "S0:source-notice",
            subject_kind: notice.subject.kind(),
            subject: notice.subject.pid(registers),
            period: notice.at.clone().map(|at| (at.clone(), at)),
            severity: "notice",
            message: notice.text.clone(),
        });
    }
    lines.sort();
    lines
}

/// Refuse a findings path that names another output of the run (the catalog, the
/// report), however spelled: writing it would replace that file with JSON Lines.
///
/// # Errors
/// `findings` and one of `others` are the same file.
pub(crate) fn refuse_alias(findings: &Path, others: &[&Path]) -> Result<()> {
    // An existing path resolves fully (a symlink names its target); a new one is its
    // parent canonicalized (it must exist for the write anyway) plus the file name.
    let key = |p: &Path| {
        if let Ok(file) = p.canonicalize() {
            return Some(file);
        }
        let dir = match p.parent() {
            Some(d) if !d.as_os_str().is_empty() => d.canonicalize().ok()?,
            _ => std::env::current_dir().ok()?,
        };
        Some(dir.join(p.file_name()?))
    };
    // A dangling link would be written through to a target this check cannot name.
    if findings.is_symlink() && !findings.exists() {
        return Err(format!(
            "--findings {} is a symlink to a missing file",
            findings.display()
        )
        .into());
    }
    let own = key(findings);
    if own.is_some() && others.iter().any(|o| key(o) == own) {
        return Err(format!(
            "--findings {} names another output of this run",
            findings.display()
        )
        .into());
    }
    Ok(())
}

/// Write `catalog`'s findings to `out`.
///
/// # Errors
/// The file cannot be written.
pub(crate) fn write(catalog: &Catalog, out: &Path) -> Result<()> {
    let mut text = String::new();
    for line in lines(catalog) {
        text.push_str(&serde_json::to_string(&line)?);
        text.push('\n');
    }
    std::fs::write(out, text).map_err(|e| format!("{}: {e}", out.display()))?;
    Ok(())
}
