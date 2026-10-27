//! `reg-resolve`: one register's source frame plus its curation becomes a resolved
//! register (`crates/DESIGN.md`, "The catalog builder"). A pure function: it sees one
//! register's records and ledger, never another register's, and reads nothing else.
//!
//! It resolves with the default rules only: no statements, no classification binding,
//! no unit types.
//!
//! - **Periods.** An occurrence of a release takes the release's period; an unplaced
//!   release leaves its occurrences unplaced. Otherwise an occurrence's unstated bound is
//!   its table's stated bound, else the register's, else open. A table with a stated
//!   bound clamps its occurrences to it (T1). A table with no stated bound and no dated
//!   occurrence is timeless.
//! - **Editions (T2).** Among a table's releases of one period and qualifier, the
//!   primary is final, then unstated, then preliminary; two of the best edition are a
//!   collision (F8), and the first by handle stays primary. An occurrence of a
//!   non-primary release whose key the primary also documents is shadowed, whatever its
//!   spelling; a key only a non-primary release documents is used, in a `preliminary`
//!   segment. Editions that disagree on a
//!   key's spellings or types are F7.
//! - **Identity.** Occurrences with one source key are one variable, unless the key
//!   covers two folded spellings in overlapping periods anywhere in the register, among
//!   used occurrences (after shadowing): then it splits by spelling. A key documented only without a
//!   column is no variable (F5).
//! - **Segments.** Per variable and table, maximal runs of one form. A run where the
//!   variable's own occurrences disagree on label, type or value set, or where another
//!   variable claims the same folded spelling, is ambiguous (F2).
//! - **Gaps (T5).** In a table with releases, the holes in the table's coverage (no
//!   release), and the days inside a variable's hull a release covers without the
//!   variable's column: documented without a column, or not documented.
//! - **Ids.** The ledger maps each public id to anchor occurrences; a variable no
//!   anchor reaches gets a proposed id from its folded spelling.

mod gaps;
mod ledger;

use std::collections::{BTreeMap, BTreeSet};

use reg_core::{Interval, fold_identity, intersect, merge, next_iso_day, prev_iso_day};
use reg_frame::{Locator, OPEN_END, OPEN_START, Occurrence, Record, Register, Release, Table};
use serde::Serialize;

use gaps::gaps;
pub use ledger::Ledger;
use ledger::assign_ids;

#[derive(Debug)]
pub struct Resolved {
    /// The register, named: its handle when the source gives no name.
    pub register: Register,
    /// The hull of its dated tables' coverage; open when every table is timeless.
    pub coverage: Interval,
    pub tables: Vec<ResolvedTable>,
    pub releases: Vec<ResolvedRelease>,
    pub variables: Vec<Variable>,
    pub occurrences: Vec<ResolvedOccurrence>,
    pub gaps: Vec<Gap>,
    pub findings: Vec<Finding>,
    pub report: Report,
}

#[derive(Debug)]
pub struct ResolvedTable {
    /// The table, named: the implicit one by its register, an implied one by its handle.
    pub table: Table,
    /// The table's id segment; `None` for the implicit table, which takes the
    /// register's id.
    pub slug: Option<String>,
    pub timeless: bool,
    /// Coverage: ascending, disjoint inclusive intervals.
    pub spans: Vec<Interval>,
}

#[derive(Debug)]
pub struct ResolvedRelease {
    pub release: Release,
    /// Index into [`Resolved::tables`].
    pub table: usize,
    /// For a placed release, whether it is the primary edition of its period and
    /// qualifier; `None` when unplaced.
    pub primary: Option<bool>,
}

#[derive(Debug)]
pub struct Variable {
    pub slug: String,
    pub pinned: bool,
    pub source_key: String,
    pub fold: String,
    /// Every exact spelling the variable was documented under.
    pub spellings: BTreeSet<String>,
    pub label: String,
    pub definition: Option<String>,
    pub hull: Interval,
    pub segments: Vec<Segment>,
    pub texts: Vec<TextRun>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    /// Index into [`Resolved::tables`].
    pub table: usize,
    pub period: Interval,
    pub spelling: String,
    pub dtype: Option<String>,
    pub valueset: Option<String>,
    pub coded: &'static str,
    pub sensitive: Option<bool>,
    pub identifier: Option<bool>,
    pub role: &'static str,
    /// `clear`, `ambiguous` or `preliminary`.
    pub state: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextRun {
    pub period: Interval,
    pub label: String,
    pub definition: Option<String>,
    pub operational_definition: Option<String>,
    pub unit: Option<String>,
}

#[derive(Debug)]
pub struct ResolvedOccurrence {
    pub occurrence: Occurrence,
    /// Index into [`Resolved::tables`].
    pub table: usize,
    /// Index into [`Resolved::releases`].
    pub release: Option<usize>,
    /// `None` only when unplaced.
    pub period: Option<Interval>,
    /// The period before the T1 clamp, when the clamp changed it.
    pub stated: Option<Interval>,
    /// Stated by a release of a worse edition than its period's primary.
    pub preliminary: bool,
    pub variable: Option<usize>,
    /// Index into the variable's segments, when used.
    pub segment: Option<usize>,
    /// `used`, `shadowed`, `rejected`, `withheld`, `gap_marker` or `unplaced`.
    pub status: &'static str,
}

/// Days a subject is not covered inside its hull, with the reason (T5).
#[derive(Debug)]
pub struct Gap {
    /// `None` for a gap in the table's own coverage.
    pub variable: Option<usize>,
    pub table: usize,
    pub period: Interval,
    pub reason: &'static str,
}

#[derive(Debug)]
pub enum Subject {
    Register,
    Release(usize),
    Variable(usize),
    Segment(usize, usize),
    Occurrence(usize),
}

#[derive(Debug)]
pub struct Finding {
    pub code: &'static str,
    pub severity: &'static str,
    pub subject: Subject,
    pub period: Option<Interval>,
    pub message: String,
}

/// The register's line in the build report.
#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub split_keys: usize,
    pub unminted: Vec<String>,
    pub unplaced_ids: Vec<String>,
    pub merged_ids: Vec<String>,
    /// Ledger ids whose anchors reach several variables; the first free one takes it.
    pub split_ids: Vec<String>,
}

/// Resolve one register from its records (the register, its tables, releases and
/// occurrences) and its ledger.
///
/// # Errors
/// No register record or two, no occurrence, a release stated twice, an occurrence of
/// a release its table does not state, an occurrence documented to end before it
/// starts, an id that is not a slug, or a ledger variable id equal to a table id.
///
/// # Panics
/// Never: every variable is built from at least one used occurrence.
#[allow(clippy::too_many_lines)]
pub fn resolve(records: &[Record], ledger: &Ledger) -> Result<Resolved, String> {
    let registers: Vec<&Register> = records
        .iter()
        .filter_map(|r| match r {
            Record::Register(r) => Some(r),
            _ => None,
        })
        .collect();
    let mut register = match registers[..] {
        [r] => r.clone(),
        [] => return Err("no register record".to_owned()),
        [a, b, ..] => {
            return Err(format!(
                "stated twice, at {}:{} and {}:{}",
                a.locator.input, a.locator.at, b.locator.input, b.locator.at
            ));
        }
    };
    let mut report = Report::default();
    let mut findings = Vec::new();
    let occurrences: Vec<&Occurrence> = records
        .iter()
        .filter_map(|r| match r {
            Record::Occurrence(o) => Some(o),
            _ => None,
        })
        .collect();
    let release_records: Vec<&Release> = records
        .iter()
        .filter_map(|r| match r {
            Record::Release(r) => Some(r),
            _ => None,
        })
        .collect();
    register
        .name
        .get_or_insert_with(|| register.register.clone());
    if occurrences.is_empty() {
        return Err(format!(
            "states no columns ({}:{})",
            register.locator.input, register.locator.at
        ));
    }

    // Tables: stated ones, then those only releases or occurrences name (the implicit
    // one included).
    let mut stated: BTreeMap<&str, &Table> = BTreeMap::new();
    for record in records {
        if let Record::Table(t) = record {
            stated.entry(t.table.as_str()).or_insert(t);
        }
    }
    let handles: BTreeSet<&str> = stated
        .keys()
        .copied()
        .chain(release_records.iter().map(|r| r.table.as_str()))
        .chain(occurrences.iter().map(|o| o.table.as_str()))
        .collect();
    let mut tables: Vec<ResolvedTable> = Vec::new();
    let mut table_index = BTreeMap::new();
    for handle in handles {
        let mut table = stated.get(handle).map_or_else(
            || Table {
                provider: register.provider.clone(),
                register: register.register.clone(),
                table: handle.to_owned(),
                name: None,
                description: None,
                from: None,
                to: None,
                cadence: None,
                locator: register.locator.clone(),
            },
            |t| (*t).clone(),
        );
        table.name.get_or_insert_with(|| {
            if handle.is_empty() {
                register.name.clone().unwrap_or_default()
            } else {
                handle.to_owned()
            }
        });
        let slug = (!handle.is_empty()).then(|| {
            ledger
                .tables
                .get(handle)
                .cloned()
                .unwrap_or_else(|| slugify(handle))
        });
        table_index.insert(handle.to_owned(), tables.len());
        tables.push(ResolvedTable {
            table,
            slug,
            timeless: false,
            spans: Vec::new(),
        });
    }

    // Releases and the primary edition (T2).
    let mut releases: Vec<ResolvedRelease> = Vec::new();
    let mut release_index: BTreeMap<(usize, &str), usize> = BTreeMap::new();
    for r in &release_records {
        let t = table_index[&r.table];
        if release_index
            .insert((t, r.release.as_str()), releases.len())
            .is_some()
        {
            return Err(format!(
                "{}:{}: release {} is stated twice",
                r.locator.input, r.locator.at, r.release
            ));
        }
        releases.push(ResolvedRelease {
            release: (*r).clone(),
            table: t,
            primary: None,
        });
    }
    let rank = |edition: &str| match edition {
        "final" => 0,
        "unstated" => 1,
        _ => 2,
    };
    let mut editions: BTreeMap<(usize, Interval, Option<String>), Vec<usize>> = BTreeMap::new();
    for (i, r) in releases.iter().enumerate() {
        if let Some(period) = placed(&r.release) {
            editions
                .entry((r.table, period, r.release.qualifier.clone()))
                .or_default()
                .push(i);
        } else {
            findings.push(Finding {
                code: "F9:unparsed-delivery",
                severity: "review",
                subject: Subject::Release(i),
                period: None,
                message: format!(
                    "delivery {:?} is not placed in time; its columns are unplaced",
                    r.release.name
                ),
            });
        }
    }
    // A non-primary release → the primary of its period and qualifier.
    let mut primary_of: BTreeMap<usize, usize> = BTreeMap::new();
    for members in editions.values() {
        let primary = *members
            .iter()
            .min_by_key(|&&i| {
                (
                    rank(&releases[i].release.edition),
                    &releases[i].release.release,
                )
            })
            .expect("a group has a release");
        for &i in members {
            releases[i].primary = Some(i == primary);
            if i == primary {
                continue;
            }
            primary_of.insert(i, primary);
            if rank(&releases[i].release.edition) == rank(&releases[primary].release.edition) {
                findings.push(Finding {
                    code: "F8:release-collision",
                    severity: "review",
                    subject: Subject::Release(i),
                    period: placed(&releases[i].release),
                    message: format!(
                        "deliveries {:?} and {:?} state one period, edition and qualifier",
                        releases[primary].release.name, releases[i].release.name
                    ),
                });
            }
        }
    }

    // Periods and the T1 clamp.
    let bound = |table: &Table| -> Option<Interval> {
        let from = table.from.clone().or_else(|| register.from.clone());
        let to = table.to.clone().or_else(|| register.to.clone());
        (from.is_some() || to.is_some()).then(|| {
            (
                from.unwrap_or_else(|| OPEN_START.to_owned()),
                to.unwrap_or_else(|| OPEN_END.to_owned()),
            )
        })
    };
    let mut resolved: Vec<ResolvedOccurrence> = Vec::new();
    let mut dated = vec![false; tables.len()];
    for o in &occurrences {
        let t = table_index[&o.table];
        let release = match &o.release {
            None => None,
            Some(handle) => Some(*release_index.get(&(t, handle.as_str())).ok_or_else(|| {
                format!(
                    "{}:{}: release {handle} is not stated for table {:?}",
                    o.locator.input, o.locator.at, o.table
                )
            })?),
        };
        // An occurrence of a release states the release's bounds.
        let (from, to) = match release {
            Some(r) => (
                releases[r].release.from.clone(),
                releases[r].release.to.clone(),
            ),
            None => (o.from.clone(), o.to.clone()),
        };
        // A colliding twin of the primary's edition is not preliminary.
        let preliminary = release.is_some_and(|r| {
            primary_of.get(&r).is_some_and(|&p| {
                rank(&releases[r].release.edition) > rank(&releases[p].release.edition)
            })
        });
        let unplaced = release.is_some_and(|r| releases[r].primary.is_none());
        let mut entry = ResolvedOccurrence {
            occurrence: (*o).clone(),
            table: t,
            release,
            period: None,
            stated: None,
            preliminary,
            variable: None,
            segment: None,
            status: "used",
        };
        if unplaced {
            entry.status = "unplaced";
            resolved.push(entry);
            continue;
        }
        dated[t] |= from.is_some() || to.is_some();
        let limit = bound(&tables[t].table);
        let stated = (
            from.clone().unwrap_or_else(|| OPEN_START.to_owned()),
            to.clone().unwrap_or_else(|| OPEN_END.to_owned()),
        );
        if stated.0 > stated.1 {
            return Err(format!(
                "{}:{}: {} is documented from {} to the earlier {}",
                o.locator.input, o.locator.at, o.column, stated.0, stated.1
            ));
        }
        let gap_marker = o.column.is_empty();
        let (period, stated_period, status) =
            match limit.as_ref().map(|l| (l, intersect(&stated, l))) {
                None => (stated, None, "used"),
                Some((_, None)) => (stated.clone(), Some(stated), "rejected"),
                // An unstated bound takes the table's; only a stated one is clamped.
                Some((l, Some(clamped))) => {
                    let inherited = (
                        from.clone().unwrap_or_else(|| l.0.clone()),
                        to.clone().unwrap_or_else(|| l.1.clone()),
                    );
                    if clamped == inherited {
                        (clamped, None, "used")
                    } else {
                        (clamped, Some(inherited), "used")
                    }
                }
            };
        if gap_marker {
            entry.period = Some(period);
            entry.status = "gap_marker";
        } else {
            entry.period = Some(period);
            entry.stated = stated_period;
            entry.status = status;
        }
        resolved.push(entry);
    }
    for (i, t) in tables.iter_mut().enumerate() {
        t.spans = match bound(&t.table) {
            Some(b) => vec![b],
            None => merge(
                resolved
                    .iter()
                    .filter(|r| r.table == i && r.status == "used")
                    .map(|r| span(r).clone())
                    .collect(),
            ),
        };
        t.timeless = bound(&t.table).is_none() && !dated[i];
        if t.spans.is_empty() {
            t.spans = vec![(OPEN_START.to_owned(), OPEN_END.to_owned())];
        }
    }

    // T2: shadowed occurrences, and editions that disagree (F7).
    // The keys each release documents with a column.
    let primary_keys: BTreeSet<(usize, &str)> = resolved
        .iter()
        .filter(|r| r.status == "used" && !r.preliminary)
        .filter_map(|r| Some((r.release?, r.occurrence.source_key.as_str())))
        .collect();
    let mut shadowed = Vec::new();
    for (i, r) in resolved.iter().enumerate() {
        let Some(&primary) = r.release.and_then(|x| primary_of.get(&x)) else {
            continue;
        };
        if r.status == "used" && primary_keys.contains(&(primary, r.occurrence.source_key.as_str()))
        {
            shadowed.push(i);
        }
    }
    for i in shadowed {
        resolved[i].status = "shadowed";
    }
    // Per release and key: the (fold, type) pairs it documents.
    let mut forms: BTreeMap<usize, Forms> = BTreeMap::new();
    for r in &resolved {
        if let Some(x) = r.release
            && !r.occurrence.column.is_empty()
            && matches!(r.status, "used" | "shadowed")
        {
            forms
                .entry(x)
                .or_default()
                .entry(r.occurrence.source_key.as_str())
                .or_default()
                .insert((
                    fold_identity(&r.occurrence.column),
                    r.occurrence.dtype.as_deref(),
                ));
        }
    }
    let empty = BTreeMap::new();
    for (&x, &primary) in &primary_of {
        let own = forms.get(&x).unwrap_or(&empty);
        let differ: Vec<&str> = own
            .iter()
            .filter(|(key, set)| {
                forms
                    .get(&primary)
                    .and_then(|f| f.get(*key))
                    .is_some_and(|other| other != *set)
            })
            .map(|(key, _)| *key)
            .collect();
        if !differ.is_empty() {
            findings.push(Finding {
                code: "F7:edition-disagreement",
                severity: "review",
                subject: Subject::Release(x),
                period: placed(&releases[x].release),
                message: format!(
                    "delivery {:?} disagrees with the primary edition on the spelling or \
                     type of {} key(s): {}",
                    releases[x].release.name,
                    differ.len(),
                    differ
                        .iter()
                        .take(5)
                        .copied()
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
    }

    // Identity: a key splits by fold when two folds overlap anywhere in the register,
    // among used rows. Shadowing leaves a worse edition's row only where the primary
    // lacks its key, so a preliminary and a final edition of one period never split it.
    let mut by_key: BTreeMap<&str, BTreeMap<String, Vec<Interval>>> = BTreeMap::new();
    for r in &resolved {
        if r.status == "used" {
            by_key
                .entry(r.occurrence.source_key.as_str())
                .or_default()
                .entry(fold_identity(&r.occurrence.column))
                .or_default()
                .push(span(r).clone());
        }
    }
    let mut split = BTreeSet::new();
    for (key, folds) in &by_key {
        let merged: Vec<Vec<Interval>> = folds.values().map(|p| merge(p.clone())).collect();
        let overlaps = merged.iter().enumerate().any(|(i, a)| {
            merged[i + 1..].iter().any(|b| {
                a.iter()
                    .any(|x| b.iter().any(|y| intersect(x, y).is_some()))
            })
        });
        if overlaps {
            split.insert(*key);
        }
    }
    report.split_keys = split.len();
    let mut variable_keys: BTreeMap<(String, String), Vec<usize>> = BTreeMap::new();
    for (i, r) in resolved.iter().enumerate() {
        if !matches!(r.status, "used" | "shadowed" | "rejected") {
            continue;
        }
        let key = r.occurrence.source_key.clone();
        let f = fold_identity(&r.occurrence.column);
        let sub = if split.contains(key.as_str()) {
            f
        } else {
            String::new()
        };
        variable_keys.entry((key, sub)).or_default().push(i);
    }
    // A variable needs a used occurrence. One documented only outside its tables'
    // coverage is withheld, with its T1 notices.
    variable_keys.retain(|_, members| {
        let keep = members.iter().any(|&i| resolved[i].status == "used");
        if !keep {
            for &i in members.iter() {
                resolved[i].status = "withheld";
            }
        }
        keep
    });

    // Variables and their segments.
    let mut variables: Vec<Variable> = Vec::new();
    let mut population_keys = BTreeSet::new();
    for (v, ((key, _), members)) in variable_keys.iter().enumerate() {
        for &i in members {
            resolved[i].variable = Some(v);
        }

        let last = latest(&resolved, members).expect("a variable has an occurrence");
        let spellings: BTreeSet<String> = members
            .iter()
            .map(|&i| resolved[i].occurrence.column.clone())
            .collect();
        let used: Vec<usize> = members
            .iter()
            .copied()
            .filter(|&i| resolved[i].status == "used")
            .collect();
        if members
            .iter()
            .any(|&i| !resolved[i].occurrence.population_valuesets.is_empty())
            && population_keys.insert(key.clone())
        {
            findings.push(Finding {
                code: "F6:source-conflict",
                severity: "review",
                subject: Subject::Variable(v),
                period: None,
                message: format!(
                    "{} states different value sets across its rows",
                    resolved[last].occurrence.column
                ),
            });
        }
        // Fields the key's merged source rows disagree on, and flags its covering
        // occurrences state differently (the segment keeps the max): one F6 per key.
        let mut conflicts: BTreeSet<String> = members
            .iter()
            .flat_map(|&i| resolved[i].occurrence.conflicts.iter().cloned())
            .collect();
        let tables_used: BTreeSet<usize> = members.iter().map(|&i| resolved[i].table).collect();
        if tables_used.len() > 1 {
            let labels: BTreeSet<String> = members
                .iter()
                .filter_map(|&i| resolved[i].occurrence.label.as_deref().map(label_key))
                .collect();
            if labels.len() > 1 {
                findings.push(Finding {
                    code: "F1:label-differs",
                    severity: "review",
                    subject: Subject::Variable(v),
                    period: None,
                    message: format!(
                        "{} is labelled differently in different tables",
                        resolved[last].occurrence.column
                    ),
                });
            }
        }
        let mut segments = Vec::new();
        for &t in &tables_used {
            let own: Vec<usize> = used
                .iter()
                .copied()
                .filter(|&i| resolved[i].table == t)
                .collect();
            // A segment holds whole occurrences: the variable's occurrences that
            // overlap form one component, and adjacent components of one form join.
            let mut sorted = own.clone();
            sorted.sort_by(|&a, &b| span(&resolved[a]).cmp(span(&resolved[b])));
            let mut components: Vec<(Interval, Vec<usize>)> = Vec::new();
            for i in sorted {
                let p = span(&resolved[i]).clone();
                match components.last_mut() {
                    Some((span, members)) if p.0 <= span.1 => {
                        if p.1 > span.1 {
                            span.1 = p.1;
                        }
                        members.push(i);
                    }
                    _ => components.push((p, vec![i])),
                }
            }
            let mut runs: Vec<Segment> = Vec::new();
            for (period, covering) in components {
                let top = latest(&resolved, &covering).expect("a component has a member");
                let forms: BTreeSet<Form> = covering.iter().map(|&i| form(&resolved[i])).collect();
                let o = &resolved[top].occurrence;
                let agree = forms.len() == 1;
                // A form field is stated only where every occurrence agrees on it.
                let agreed = |field: fn(&Form) -> &Option<String>| {
                    let values: BTreeSet<&Option<String>> = forms.iter().map(field).collect();
                    match values.len() {
                        1 => values.into_iter().next().cloned().flatten(),
                        _ => None,
                    }
                };
                let state = if !agree {
                    "ambiguous"
                } else if covering.iter().all(|&i| resolved[i].preliminary) {
                    "preliminary"
                } else {
                    "clear"
                };
                // Flags hold if any covering occurrence states them: under-flagging is the
                // dangerous direction. An identifier is a row key whichever occurrence says so.
                let flag = |f: fn(&Occurrence) -> Option<bool>| {
                    covering
                        .iter()
                        .map(|&i| f(&resolved[i].occurrence))
                        .max()
                        .flatten()
                };
                let identifier = flag(|o| o.identifier);
                for (name, f) in [
                    (
                        "sensitive",
                        (|o| o.sensitive) as fn(&Occurrence) -> Option<bool>,
                    ),
                    ("identifier", |o| o.identifier),
                ] {
                    let stated: BTreeSet<bool> = covering
                        .iter()
                        .filter_map(|&i| f(&resolved[i].occurrence))
                        .collect();
                    if stated.len() > 1 {
                        conflicts.insert(name.to_owned());
                    }
                }
                let segment = Segment {
                    table: t,
                    period,
                    spelling: o.column.clone(),
                    dtype: agreed(|f| &f.1),
                    valueset: agreed(|f| &f.2),
                    coded: coded(o),
                    sensitive: flag(|o| o.sensitive),
                    identifier,
                    role: role(o, identifier),
                    state,
                };
                let index = match runs.last_mut() {
                    Some(prev)
                        if next_iso_day(&prev.period.1) == segment.period.0
                            && same_form(prev, &segment) =>
                    {
                        prev.period.1 = segment.period.1;
                        prev.spelling = segment.spelling;
                        runs.len() - 1
                    }
                    _ => {
                        runs.push(segment);
                        runs.len() - 1
                    }
                };
                for i in covering {
                    resolved[i].segment = Some(segments.len() + index);
                }
            }
            segments.extend(runs);
        }
        if !conflicts.is_empty() {
            findings.push(Finding {
                code: "F6:source-conflict",
                severity: "review",
                subject: Subject::Variable(v),
                period: None,
                message: format!(
                    "{}: its source rows disagree on {}",
                    resolved[last].occurrence.column,
                    conflicts.into_iter().collect::<Vec<_>>().join(", ")
                ),
            });
        }

        // Descriptive text over time: the latest occurrence of each piece.
        let mut texts: Vec<TextRun> = Vec::new();
        let periods: Vec<Interval> = used.iter().map(|&i| span(&resolved[i]).clone()).collect();
        for piece in pieces(&periods) {
            let covering: Vec<usize> = used
                .iter()
                .copied()
                .filter(|&i| covers(span(&resolved[i]), &piece))
                .collect();
            let Some(top) = latest(&resolved, &covering) else {
                continue;
            };
            let o = &resolved[top].occurrence;
            let run = TextRun {
                period: piece,
                label: o.label.clone().unwrap_or_else(|| o.column.clone()),
                definition: o.definition.clone(),
                operational_definition: o.operational_definition.clone(),
                unit: o.unit.clone(),
            };
            match texts.last_mut() {
                Some(prev)
                    if next_iso_day(&prev.period.1) == run.period.0
                        && (
                            &prev.label,
                            &prev.definition,
                            &prev.operational_definition,
                            &prev.unit,
                        ) == (
                            &run.label,
                            &run.definition,
                            &run.operational_definition,
                            &run.unit,
                        ) =>
                {
                    prev.period.1 = run.period.1;
                }
                _ => texts.push(run),
            }
        }

        let o = &resolved[last].occurrence;
        let hull = (
            periods
                .iter()
                .map(|p| p.0.clone())
                .min()
                .expect("a variable is used"),
            periods
                .iter()
                .map(|p| p.1.clone())
                .max()
                .expect("a variable is used"),
        );
        variables.push(Variable {
            slug: String::new(),
            pinned: false,
            source_key: key.clone(),
            fold: fold_identity(&o.column),
            spellings,
            label: texts.last().map_or_else(
                || o.label.clone().unwrap_or_else(|| o.column.clone()),
                |t| t.label.clone(),
            ),
            definition: texts
                .last()
                .map_or_else(|| o.definition.clone(), |t| t.definition.clone()),
            hull,
            segments,
            texts,
        });
    }
    // Rival claimants: segments of two variables under one folded spelling, in one
    // table, that overlap are both ambiguous.
    let mut by_fold: BTreeMap<(usize, String), Vec<(usize, usize)>> = BTreeMap::new();
    for (v, variable) in variables.iter().enumerate() {
        for (s, segment) in variable.segments.iter().enumerate() {
            by_fold
                .entry((segment.table, fold_identity(&segment.spelling)))
                .or_default()
                .push((v, s));
        }
    }
    let mut contested = BTreeSet::new();
    for members in by_fold.values() {
        for (a, &(va, sa)) in members.iter().enumerate() {
            for &(vb, sb) in &members[a + 1..] {
                if va != vb
                    && intersect(
                        &variables[va].segments[sa].period,
                        &variables[vb].segments[sb].period,
                    )
                    .is_some()
                {
                    contested.insert((va, sa));
                    contested.insert((vb, sb));
                }
            }
        }
    }
    for &(v, s) in &contested {
        variables[v].segments[s].state = "ambiguous";
    }
    for (v, variable) in variables.iter().enumerate() {
        let segments = &variable.segments;
        for (s, segment) in segments.iter().enumerate() {
            if segment.state == "ambiguous" {
                findings.push(Finding {
                    code: "F2:ambiguous",
                    severity: "review",
                    subject: Subject::Segment(v, s),
                    period: Some(segment.period.clone()),
                    message: format!(
                        "{} cannot be placed on one variable form here",
                        segment.spelling
                    ),
                });
            }
        }
    }

    // Gap markers: on their key's variable when the key is one variable. A key
    // documented only without a column is no variable (F5).
    let mut key_variables: BTreeMap<&str, BTreeSet<usize>> = BTreeMap::new();
    for ((key, _), members) in &variable_keys {
        key_variables
            .entry(key.as_str())
            .or_default()
            .insert(resolved[members[0]].variable.expect("assigned above"));
    }
    let columned: BTreeSet<&str> = resolved
        .iter()
        .filter(|r| !r.occurrence.column.is_empty())
        .map(|r| r.occurrence.source_key.as_str())
        .collect();
    let mut markers: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut marker_variable = Vec::new();
    for (i, r) in resolved.iter().enumerate() {
        if r.status != "gap_marker" {
            continue;
        }
        let key = r.occurrence.source_key.as_str();
        if let Some(vs) = key_variables.get(key)
            && vs.len() == 1
        {
            marker_variable.push((i, *vs.first().expect("one")));
        }
        if !columned.contains(key) {
            markers
                .entry(key.to_owned())
                .or_insert_with(|| r.occurrence.label.clone());
        }
    }
    for (i, v) in marker_variable {
        resolved[i].variable = Some(v);
    }
    for (key, label) in markers {
        findings.push(Finding {
            code: "F5:documented-without-column",
            severity: "notice",
            subject: Subject::Register,
            period: None,
            message: format!(
                "key {key} ({}) is documented but never delivered as a column",
                label.unwrap_or_default()
            ),
        });
    }

    // T1 findings.
    for (i, r) in resolved.iter().enumerate() {
        if r.stated.is_some() {
            findings.push(Finding {
                code: "T1:clamp",
                severity: "notice",
                subject: Subject::Occurrence(i),
                period: r.period.clone(),
                message: if r.status == "used" {
                    "documented beyond its table's coverage; clamped".to_owned()
                } else {
                    "documented entirely outside its table's coverage".to_owned()
                },
            });
        }
    }

    let gaps = gaps(&mut tables, &releases, &variables, &resolved);

    assign_ids(&mut variables, &resolved, &tables, ledger, &mut report)?;

    let dated: Vec<&Interval> = tables
        .iter()
        .filter(|t| !t.timeless)
        .flat_map(|t| &t.spans)
        .collect();
    let coverage = match (
        dated.iter().map(|p| &p.0).min(),
        dated.iter().map(|p| &p.1).max(),
    ) {
        (Some(lo), Some(hi)) => (lo.clone(), hi.clone()),
        _ => (OPEN_START.to_owned(), OPEN_END.to_owned()),
    };
    Ok(Resolved {
        register,
        coverage,
        tables,
        releases,
        variables,
        occurrences: resolved,
        gaps,
        findings,
        report,
    })
}

/// The default coded state: `no` when the source says the column carries no codes,
/// `yes` when it binds codes or names a code system.
#[must_use]
pub fn coded(o: &Occurrence) -> &'static str {
    if o.coded == Some(false) {
        "no"
    } else if o.valueset.is_some() || !o.population_valuesets.is_empty() || o.code_system.is_some()
    {
        "yes"
    } else {
        "unknown"
    }
}

/// The default role: identifiers are row keys, coded columns codes, the rest measures.
#[must_use]
pub fn role(o: &Occurrence, identifier: Option<bool>) -> &'static str {
    if identifier == Some(true) {
        "row_key"
    } else if o.valueset.is_some() || !o.population_valuesets.is_empty() {
        "code"
    } else {
        "measure"
    }
}

/// A placed release's period.
pub(crate) fn placed(r: &Release) -> Option<Interval> {
    Some((r.from.clone()?, r.to.clone()?))
}

/// The period of an occurrence that is placed.
pub(crate) fn span(r: &ResolvedOccurrence) -> &Interval {
    r.period.as_ref().expect("a placed occurrence")
}

/// The latest of `set`, by [`order`].
fn latest(resolved: &[ResolvedOccurrence], set: &[usize]) -> Option<usize> {
    set.iter()
        .copied()
        .max_by(|&a, &b| order(&resolved[a]).cmp(&order(&resolved[b])))
}

/// The ordering that makes an occurrence "latest": later start, then later end, then
/// its locator.
fn order(r: &ResolvedOccurrence) -> (&Option<Interval>, &Locator) {
    (&r.period, &r.occurrence.locator)
}

type Form = (Option<String>, Option<String>, Option<String>);

/// Per key, the (fold, type) pairs a release documents.
type Forms<'a> = BTreeMap<&'a str, BTreeSet<(String, Option<&'a str>)>>;

/// What two overlapping occurrences of one variable must agree on.
fn form(r: &ResolvedOccurrence) -> Form {
    let o = &r.occurrence;
    (
        o.label.as_deref().map(label_key),
        o.dtype.clone(),
        o.valueset.clone(),
    )
}

fn same_form(a: &Segment, b: &Segment) -> bool {
    fold_identity(&a.spelling) == fold_identity(&b.spelling)
        && (
            a.state,
            &a.dtype,
            &a.valueset,
            a.coded,
            a.sensitive,
            a.identifier,
            a.role,
        ) == (
            b.state,
            &b.dtype,
            &b.valueset,
            b.coded,
            b.sensitive,
            b.identifier,
            b.role,
        )
}

/// Labels compare case- and spacing-insensitively.
fn label_key(label: &str) -> String {
    reg_core::fold_identity(&label.split_whitespace().collect::<Vec<_>>().join(" "))
}

pub(crate) fn covers(p: &Interval, piece: &Interval) -> bool {
    p.0 <= piece.0 && piece.1 <= p.1
}

/// The elementary partition of `intervals`: the ascending pieces between every start
/// and every day after an end, each inside or outside every interval, keeping those
/// inside at least one.
pub(crate) fn pieces(intervals: &[Interval]) -> Vec<Interval> {
    let cuts: BTreeSet<String> = intervals
        .iter()
        .flat_map(|(lo, hi)| {
            let after = (hi.as_str() != OPEN_END).then(|| next_iso_day(hi));
            std::iter::once(lo.clone()).chain(after)
        })
        .collect();
    let cuts: Vec<String> = cuts.into_iter().collect();
    cuts.iter()
        .enumerate()
        .map(|(i, lo)| {
            let hi = cuts
                .get(i + 1)
                .map_or_else(|| OPEN_END.to_owned(), |next| prev_iso_day(next));
            (lo.clone(), hi)
        })
        .filter(|piece| intervals.iter().any(|p| covers(p, piece)))
        .collect()
}

/// A proposed id segment from a name: lowercase ASCII, `å`/`ä` as `a`, `ö` as `o`,
/// other runs of non-alphanumerics as one `-`, led by a letter.
#[must_use]
pub fn slugify(name: &str) -> String {
    let mut out = String::new();
    for c in name.to_lowercase().chars() {
        let c = match c {
            'å' | 'ä' | 'á' | 'à' => 'a',
            'ö' | 'ó' => 'o',
            'é' | 'è' => 'e',
            'ü' => 'u',
            c => c,
        };
        if c.is_ascii_alphanumeric() {
            out.push(c);
        } else if !out.is_empty() && !out.ends_with('-') {
            out.push('-');
        }
    }
    let out = out.trim_end_matches('-');
    if out.starts_with(|c: char| c.is_ascii_lowercase()) {
        out.to_owned()
    } else {
        format!("v{}{out}", if out.is_empty() { "" } else { "-" })
    }
}
