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

use std::collections::{BTreeMap, BTreeSet};

use reg_core::{Interval, fold_identity, intersect, merge, next_iso_day, prev_iso_day};
use reg_frame::{Locator, OPEN_END, OPEN_START, Occurrence, Record, Register, Release, Table};
use serde::{Deserialize, Serialize};

/// A register's `ids.toml`: public ids of its tables and variables. An anchor is
/// `<table>: <spelling>`, or a bare spelling for the implicit table, matched by fold.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    #[serde(default)]
    pub tables: BTreeMap<String, String>,
    #[serde(default)]
    pub variables: BTreeMap<String, Vec<String>>,
}

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
    pub tables: usize,
    /// Named tables only occurrences cite; the implicit table is not one.
    pub implied_tables: usize,
    pub timeless_tables: usize,
    pub releases: usize,
    pub unparsed_releases: usize,
    pub release_collisions: usize,
    pub edition_disagreements: usize,
    pub occurrences: usize,
    pub clamped: usize,
    pub rejected: usize,
    pub shadowed: usize,
    pub unplaced: usize,
    pub gap_markers: usize,
    /// Keys documented only without a column: not variables.
    pub keys_without_column: usize,
    pub withheld_variables: usize,
    pub variables: usize,
    pub split_keys: usize,
    pub ambiguous_segments: usize,
    pub ambiguous_pairs: usize,
    pub preliminary_segments: usize,
    pub valueset_conflict_keys: usize,
    /// Keys whose merged source rows disagree on a field.
    pub row_conflict_keys: usize,
    pub gaps: usize,
    pub keys_in_several_tables: usize,
    pub label_differs_across_tables: usize,
    pub case_variants: usize,
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
            || {
                if !handle.is_empty() {
                    report.implied_tables += 1;
                }
                Table {
                    provider: register.provider.clone(),
                    register: register.register.clone(),
                    table: handle.to_owned(),
                    name: None,
                    description: None,
                    from: None,
                    to: None,
                    cadence: None,
                    locator: register.locator.clone(),
                }
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
    report.releases = releases.len();
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
            report.unparsed_releases += 1;
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
                report.release_collisions += 1;
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
            report.unplaced += 1;
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
            report.gap_markers += 1;
            entry.period = Some(period);
            entry.status = "gap_marker";
        } else {
            match (status, &stated_period) {
                ("rejected", _) => report.rejected += 1,
                (_, Some(_)) => report.clamped += 1,
                _ => {}
            }
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
    report.shadowed = shadowed.len();
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
            report.edition_disagreements += 1;
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
            report.withheld_variables += 1;
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
        let used: Vec<usize> = members
            .iter()
            .copied()
            .filter(|&i| resolved[i].status == "used")
            .collect();
        let spellings: BTreeSet<String> = members
            .iter()
            .map(|&i| resolved[i].occurrence.column.clone())
            .collect();
        if spellings.len() > 1 {
            report.case_variants += 1;
        }
        if members
            .iter()
            .any(|&i| !resolved[i].occurrence.population_valuesets.is_empty())
            && population_keys.insert(key.clone())
        {
            report.valueset_conflict_keys += 1;
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
        let conflicts: BTreeSet<&str> = members
            .iter()
            .flat_map(|&i| resolved[i].occurrence.conflicts.iter().map(String::as_str))
            .collect();
        if !conflicts.is_empty() {
            report.row_conflict_keys += 1;
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
        let tables_used: BTreeSet<usize> = members.iter().map(|&i| resolved[i].table).collect();
        if tables_used.len() > 1 {
            report.keys_in_several_tables += 1;
            let labels: BTreeSet<String> = members
                .iter()
                .filter_map(|&i| resolved[i].occurrence.label.as_deref().map(label_key))
                .collect();
            if labels.len() > 1 {
                report.label_differs_across_tables += 1;
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
                let segment = Segment {
                    table: t,
                    period,
                    spelling: o.column.clone(),
                    dtype: agreed(|f| &f.1),
                    valueset: agreed(|f| &f.2),
                    coded: coded(o),
                    sensitive: o.sensitive,
                    identifier: o.identifier,
                    role: role(o),
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
            // Pairs of the variable's own occurrences that disagree where they overlap.
            for (a, &x) in own.iter().enumerate() {
                for &y in &own[a + 1..] {
                    let overlap = intersect(span(&resolved[x]), span(&resolved[y])).is_some();
                    if overlap && form(&resolved[x]) != form(&resolved[y]) {
                        report.ambiguous_pairs += 1;
                    }
                }
            }
            segments.extend(runs);
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
            match segment.state {
                "ambiguous" => {
                    report.ambiguous_segments += 1;
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
                "preliminary" => report.preliminary_segments += 1,
                _ => {}
            }
        }
    }
    report.variables = variables.len();

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
    report.keys_without_column = markers.len();
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
    report.gaps = gaps.len();

    assign_ids(&mut variables, &resolved, &tables, ledger, &mut report)?;

    report.tables = tables.len();
    report.timeless_tables = tables.iter().filter(|t| t.timeless).count();
    report.occurrences = resolved.len();
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

/// A table's cadence in months: the greatest common step between its primary releases'
/// starts, when every start is a whole number of months after the one before on the
/// same day of the month (or month end). `None` with fewer than two starts.
fn cadence(releases: &[Interval]) -> Option<u32> {
    let starts: BTreeSet<&str> = releases.iter().map(|r| r.0.as_str()).collect();
    let month = |s: &str| -> Option<u32> {
        Some(s.get(..4)?.parse::<u32>().ok()? * 12 + s.get(5..7)?.parse::<u32>().ok()?)
    };
    let mut step: Option<u32> = None;
    for (a, b) in starts.iter().zip(starts.iter().skip(1)) {
        let months = month(b)? - month(a)?;
        if months == 0 || reg_core::add_iso_months(a, months).as_deref() != Some(*b) {
            return None;
        }
        step = Some(step.map_or(months, |g| gcd(g, months)));
    }
    step
}

/// A cadence name's step in months; `None` for `day` and `irregular`.
fn months(cadence: &str) -> Option<u32> {
    CADENCES
        .iter()
        .find(|(n, _)| *n == cadence)
        .map(|&(_, m)| m)
}

/// The cadence name of a step in months; a step no name fits is `irregular`.
fn cadence_name(step: u32) -> &'static str {
    CADENCES
        .iter()
        .find(|&&(_, m)| m == step)
        .map_or("irregular", |&(n, _)| n)
}

/// The named cadences and their steps in months.
const CADENCES: [(&str, u32); 4] = [("year", 12), ("half", 6), ("quarter", 3), ("month", 1)];

/// Each span widened to cover at least its cadence step from its start.
fn stretch(step: Option<u32>, spans: Vec<Interval>) -> Vec<Interval> {
    let Some(step) = step else {
        return spans;
    };
    spans
        .into_iter()
        .map(|(from, to)| {
            let end = reg_core::add_iso_months(&from, step)
                .map_or_else(|| to.clone(), |next| reg_core::prev_iso_day(&next));
            let to = to.max(end);
            (from, to)
        })
        .collect()
}

fn gcd(a: u32, b: u32) -> u32 {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// The smallest interval holding every one of `spans`.
fn hull(spans: &[Interval]) -> Option<Interval> {
    let from = spans.iter().map(|s| &s.0).min()?;
    let to = spans.iter().map(|s| &s.1).max()?;
    Some((from.clone(), to.clone()))
}

/// T5 in the tables with placed releases: the days between a table's used columns and
/// primary releases that no used column covers (under a release with no columns, or no
/// release), and each variable's days inside its hull there that a release covers
/// without its column. Where the table has a cadence, every column, release and marker
/// covers its whole cadence step, so a gap is a missing step.
#[allow(clippy::too_many_lines)]
fn gaps(
    tables: &mut [ResolvedTable],
    releases: &[ResolvedRelease],
    variables: &[Variable],
    resolved: &[ResolvedOccurrence],
) -> Vec<Gap> {
    let mut delivered: BTreeMap<usize, Vec<Interval>> = BTreeMap::new();
    for r in releases {
        if r.primary == Some(true) {
            delivered
                .entry(r.table)
                .or_default()
                .extend(placed(&r.release));
        }
    }
    // A stated cadence stands; otherwise the inferred one is recorded on the table.
    let steps: BTreeMap<usize, u32> = delivered
        .iter()
        .filter_map(|(&t, r)| {
            let step = if let Some(stated) = tables[t].table.cadence.as_deref() {
                months(stated)?
            } else {
                let step = cadence(r)?;
                tables[t].table.cadence = Some(cadence_name(step).to_owned());
                step
            };
            Some((t, step))
        })
        .collect();
    let stretch = |t: usize, spans: Vec<Interval>| stretch(steps.get(&t).copied(), spans);
    let delivered: BTreeMap<usize, Vec<Interval>> = delivered
        .into_iter()
        .map(|(t, r)| (t, stretch(t, r)))
        .collect();
    let mut out = Vec::new();
    for (&t, releases) in &delivered {
        // The table's days between its used columns and primary releases that no used
        // column covers. The hull is theirs, not the table's bound: a bound may be open on
        // a side, while a placed release is bounded.
        let covered = stretch(
            t,
            resolved
                .iter()
                .filter(|r| r.table == t && r.status == "used")
                .map(|r| span(r).clone())
                .collect(),
        );
        let Some(hull) = hull(&[covered.as_slice(), releases].concat()) else {
            continue;
        };
        let mut all = vec![hull.clone()];
        all.extend(covered.iter().cloned());
        all.extend(releases.iter().cloned());
        let holes = runs(pieces(&all).into_iter().filter_map(|piece| {
            if !covers(&hull, &piece) || covered.iter().any(|c| covers(c, &piece)) {
                return None;
            }
            let reason = if releases.iter().any(|r| covers(r, &piece)) {
                "release_without_column"
            } else {
                "no_release"
            };
            Some((piece, reason))
        }));
        out.extend(holes.into_iter().map(|(period, reason)| Gap {
            variable: None,
            table: t,
            period,
            reason,
        }));
    }
    let mut marked: BTreeMap<(usize, usize), Vec<Interval>> = BTreeMap::new();
    for r in resolved {
        if r.status == "gap_marker"
            && let (Some(v), Some(p)) = (r.variable, &r.period)
        {
            marked.entry((v, r.table)).or_default().push(p.clone());
        }
    }
    for (v, variable) in variables.iter().enumerate() {
        let in_tables: BTreeSet<usize> = variable.segments.iter().map(|s| s.table).collect();
        for t in in_tables {
            let Some(delivered) = delivered.get(&t) else {
                continue;
            };
            let segments = stretch(
                t,
                variable
                    .segments
                    .iter()
                    .filter(|s| s.table == t)
                    .map(|s| s.period.clone())
                    .collect(),
            );
            let hull = hull(&segments).expect("a segment");
            let marks = stretch(t, marked.get(&(v, t)).cloned().unwrap_or_default());
            let mut all = vec![hull.clone()];
            all.extend(segments.iter().cloned());
            all.extend(delivered.iter().cloned());
            all.extend(marks.iter().cloned());
            let runs = runs(pieces(&all).into_iter().filter_map(|piece| {
                if !covers(&hull, &piece) || segments.iter().any(|s| covers(s, &piece)) {
                    return None;
                }
                // Days no release covers are the table's gap, not the variable's.
                if marks.iter().any(|m| covers(m, &piece)) {
                    Some((piece, "documented_without_column"))
                } else if delivered.iter().any(|d| covers(d, &piece)) {
                    Some((piece, "release_without_column"))
                } else {
                    None
                }
            }));
            out.extend(runs.into_iter().map(|(period, reason)| Gap {
                variable: Some(v),
                table: t,
                period,
                reason,
            }));
        }
    }
    out
}

/// Ascending gap pieces joined into runs of one reason.
fn runs(
    pieces: impl IntoIterator<Item = (Interval, &'static str)>,
) -> Vec<(Interval, &'static str)> {
    let mut out: Vec<(Interval, &'static str)> = Vec::new();
    for (piece, reason) in pieces {
        match out.last_mut() {
            Some((p, r)) if *r == reason && next_iso_day(&p.1) == piece.0 => p.1 = piece.1,
            _ => out.push((piece, reason)),
        }
    }
    out
}

/// Ledger ids onto variables by anchor; proposed ids for the rest.
fn assign_ids(
    variables: &mut [Variable],
    resolved: &[ResolvedOccurrence],
    tables: &[ResolvedTable],
    ledger: &Ledger,
    report: &mut Report,
) -> Result<(), String> {
    // Tables and variables share the register's namespace, and a ledger id stays
    // reserved even where it anchors nothing.
    let table_ids: BTreeSet<String> = tables.iter().filter_map(|t| t.slug.clone()).collect();
    for id in table_ids.iter().chain(ledger.variables.keys()) {
        if !reg_core::is_slug(id) {
            return Err(format!("id {id:?} is not a slug"));
        }
    }
    let mut taken = table_ids.clone();
    for id in ledger.variables.keys() {
        if !taken.insert(id.clone()) {
            return Err(format!("ledger id {id:?} is also a table id"));
        }
    }
    // Anchors match the folded spellings of each table's placed columns.
    let mut by_anchor: BTreeMap<(&str, String), BTreeSet<usize>> = BTreeMap::new();
    for r in resolved {
        if let Some(v) = r.variable
            && !r.occurrence.column.is_empty()
        {
            by_anchor
                .entry((
                    tables[r.table].table.table.as_str(),
                    fold_identity(&r.occurrence.column),
                ))
                .or_default()
                .insert(v);
        }
    }
    for (id, anchors) in &ledger.variables {
        let targets: BTreeSet<usize> = anchors
            .iter()
            .flat_map(|anchor| {
                let (table, spelling) = anchor.split_once(": ").unwrap_or(("", anchor));
                by_anchor
                    .get(&(table.trim(), fold_identity(spelling.trim())))
                    .into_iter()
                    .flatten()
                    .copied()
            })
            .collect();
        if targets.len() > 1 {
            report.split_ids.push(id.clone());
        }
        let Some(&target) = targets.iter().find(|&&v| !variables[v].pinned) else {
            if targets.is_empty() {
                report.unplaced_ids.push(id.clone());
            } else {
                report.merged_ids.push(id.clone());
            }
            continue;
        };
        variables[target].slug.clone_from(id);
        variables[target].pinned = true;
    }
    for v in variables.iter_mut().filter(|v| !v.pinned) {
        let base = slugify(&v.fold);
        let mut slug = base.clone();
        let mut n = 2;
        while taken.contains(&slug) || !reg_core::is_slug(&slug) {
            slug = format!("{base}-{n}");
            n += 1;
        }
        report.unminted.push(slug.clone());
        taken.insert(slug.clone());
        v.slug = slug;
    }
    Ok(())
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
pub fn role(o: &Occurrence) -> &'static str {
    if o.identifier == Some(true) {
        "row_key"
    } else if o.valueset.is_some() || !o.population_valuesets.is_empty() {
        "code"
    } else {
        "measure"
    }
}

/// A placed release's period.
fn placed(r: &Release) -> Option<Interval> {
    Some((r.from.clone()?, r.to.clone()?))
}

/// The period of an occurrence that is placed.
fn span(r: &ResolvedOccurrence) -> &Interval {
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

fn covers(p: &Interval, piece: &Interval) -> bool {
    p.0 <= piece.0 && piece.1 <= p.1
}

/// The elementary partition of `intervals`: the ascending pieces between every start
/// and every day after an end, each inside or outside every interval, keeping those
/// inside at least one.
fn pieces(intervals: &[Interval]) -> Vec<Interval> {
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
