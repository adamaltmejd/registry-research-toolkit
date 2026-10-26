//! `reg-resolve`: one register's source frame plus its curation becomes a resolved
//! register (`crates/DESIGN.md`, "The catalog builder"). A pure function: it sees one
//! register's records and ledger, never another register's, and reads nothing else.
//!
//! Milestone A resolves with the default rules only: no statements, no releases, no
//! gaps, no classification binding, no unit types.
//!
//! - **Periods.** An occurrence's unstated bound is its table's stated bound, else the
//!   register's, else open. A table with a stated bound clamps its occurrences to it
//!   (T1). A table with no stated bound and no dated occurrence is timeless.
//! - **Identity.** Occurrences with one source key are one variable, unless the key
//!   covers two folded spellings in overlapping periods anywhere in the register: then
//!   it splits by spelling.
//! - **Segments.** Per variable and table, maximal runs of one form. A run where the
//!   variable's own occurrences disagree on label, type or value set, or where another
//!   variable claims the same folded spelling, is ambiguous (F2).
//! - **Ids.** The ledger maps each public id to anchor occurrences; a variable no
//!   anchor reaches gets a proposed id from its folded spelling.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::{Interval, fold_identity, intersect, merge, next_iso_day, prev_iso_day};
use reg_frame::{Locator, Occurrence, Record, Register, Table};
use serde::{Deserialize, Serialize};

/// The first and last day of open time.
pub const OPEN_START: &str = "0001-01-01";
pub const OPEN_END: &str = "9999-12-31";

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
    pub variables: Vec<Variable>,
    pub occurrences: Vec<ResolvedOccurrence>,
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
    pub ambiguous: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextRun {
    pub period: Interval,
    pub label: String,
    pub definition: Option<String>,
    pub unit: Option<String>,
}

#[derive(Debug)]
pub struct ResolvedOccurrence {
    pub occurrence: Occurrence,
    /// Index into [`Resolved::tables`].
    pub table: usize,
    pub period: Interval,
    /// The period before the T1 clamp, when the clamp changed it.
    pub stated: Option<Interval>,
    pub variable: Option<usize>,
    /// Index into the variable's segments, when used.
    pub segment: Option<usize>,
    pub status: &'static str,
}

#[derive(Debug)]
pub enum Subject {
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
    pub occurrences: usize,
    pub clamped: usize,
    pub rejected: usize,
    pub withheld_variables: usize,
    pub variables: usize,
    pub split_keys: usize,
    pub ambiguous_segments: usize,
    pub ambiguous_pairs: usize,
    pub keys_in_several_tables: usize,
    pub label_differs_across_tables: usize,
    pub case_variants: usize,
    pub unminted: Vec<String>,
    pub unplaced_ids: Vec<String>,
    pub merged_ids: Vec<String>,
    /// Ledger ids whose anchors reach several variables; the first free one takes it.
    pub split_ids: Vec<String>,
}

/// Resolve one register from its records (the register, its tables and occurrences)
/// and its ledger.
///
/// # Errors
/// No register record or two, no occurrence, an occurrence documented to end before
/// it starts, an id that is not a slug, or a ledger variable id equal to a table id.
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
    register
        .name
        .get_or_insert_with(|| register.register.clone());
    if occurrences.is_empty() {
        return Err(format!(
            "states no columns ({}:{})",
            register.locator.input, register.locator.at
        ));
    }

    // Tables: stated ones, then those only occurrences name (the implicit one included).
    let mut stated: BTreeMap<&str, &Table> = BTreeMap::new();
    for record in records {
        if let Record::Table(t) = record {
            stated.entry(t.table.as_str()).or_insert(t);
        }
    }
    let handles: BTreeSet<&str> = stated
        .keys()
        .copied()
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
    for o in &occurrences {
        let t = table_index[&o.table];
        let limit = bound(&tables[t].table);
        let stated = (
            o.from.clone().unwrap_or_else(|| OPEN_START.to_owned()),
            o.to.clone().unwrap_or_else(|| OPEN_END.to_owned()),
        );
        if stated.0 > stated.1 {
            return Err(format!(
                "{}:{}: {} is documented from {} to the earlier {}",
                o.locator.input, o.locator.at, o.column, stated.0, stated.1
            ));
        }
        let (period, stated_period, status) =
            match limit.as_ref().map(|l| (l, intersect(&stated, l))) {
                None => (stated, None, "used"),
                Some((_, None)) => {
                    report.rejected += 1;
                    (stated.clone(), Some(stated), "rejected")
                }
                // An unstated bound takes the table's; only a stated one is clamped.
                Some((l, Some(clamped))) => {
                    let inherited = (
                        o.from.clone().unwrap_or_else(|| l.0.clone()),
                        o.to.clone().unwrap_or_else(|| l.1.clone()),
                    );
                    if clamped == inherited {
                        (clamped, None, "used")
                    } else {
                        report.clamped += 1;
                        (clamped, Some(inherited), "used")
                    }
                }
            };
        resolved.push(ResolvedOccurrence {
            occurrence: (*o).clone(),
            table: t,
            period,
            stated: stated_period,
            variable: None,
            segment: None,
            status,
        });
    }
    for (i, t) in tables.iter_mut().enumerate() {
        let dated = occurrences
            .iter()
            .any(|o| table_index[&o.table] == i && (o.from.is_some() || o.to.is_some()));
        t.spans = match bound(&t.table) {
            Some(b) => vec![b],
            None => merge(
                resolved
                    .iter()
                    .filter(|r| r.table == i && r.status == "used")
                    .map(|r| r.period.clone())
                    .collect(),
            ),
        };
        t.timeless = bound(&t.table).is_none() && !dated;
        if t.spans.is_empty() {
            t.spans = vec![(OPEN_START.to_owned(), OPEN_END.to_owned())];
        }
    }

    // Identity: a key splits by fold when two folds overlap anywhere in the register.
    let mut by_key: BTreeMap<&str, BTreeMap<String, Vec<Interval>>> = BTreeMap::new();
    for r in &resolved {
        by_key
            .entry(r.occurrence.source_key.as_str())
            .or_default()
            .entry(fold_identity(&r.occurrence.column))
            .or_default()
            .push(r.period.clone());
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
            let f = fold_identity(&resolved[last].occurrence.column);
            // simplify: a scan of the register per (variable, table); index occurrences
            // by (table, fold) if a register passes a few thousand occurrences.
            let rivals: Vec<usize> = (0..resolved.len())
                .filter(|&i| {
                    let r = &resolved[i];
                    r.table == t
                        && r.status == "used"
                        && !members.contains(&i)
                        && fold_identity(&r.occurrence.column) == f
                })
                .collect();
            // A segment holds whole occurrences: the variable's occurrences that
            // overlap form one component, and adjacent components of one form join.
            let mut sorted = own.clone();
            sorted.sort_by(|&a, &b| resolved[a].period.cmp(&resolved[b].period));
            let mut components: Vec<(Interval, Vec<usize>)> = Vec::new();
            for i in sorted {
                let p = resolved[i].period.clone();
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
            for (span, covering) in components {
                let top = latest(&resolved, &covering).expect("a component has a member");
                let forms: BTreeSet<Form> = covering.iter().map(|&i| form(&resolved[i])).collect();
                let contested = rivals
                    .iter()
                    .any(|&i| intersect(&resolved[i].period, &span).is_some());
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
                let segment = Segment {
                    table: t,
                    period: span,
                    spelling: o.column.clone(),
                    dtype: agreed(|f| &f.1),
                    valueset: agreed(|f| &f.2),
                    coded: coded(o),
                    sensitive: o.sensitive,
                    identifier: o.identifier,
                    role: role(o),
                    ambiguous: !agree || contested,
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
                    let overlap = intersect(&resolved[x].period, &resolved[y].period).is_some();
                    if overlap && form(&resolved[x]) != form(&resolved[y]) {
                        report.ambiguous_pairs += 1;
                    }
                }
            }
            segments.extend(runs);
        }
        for (s, segment) in segments.iter().enumerate() {
            if segment.ambiguous {
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
        }

        // Descriptive text over time: the latest occurrence of each piece.
        let mut texts: Vec<TextRun> = Vec::new();
        let periods: Vec<Interval> = used.iter().map(|&i| resolved[i].period.clone()).collect();
        for piece in pieces(&periods) {
            let covering: Vec<usize> = used
                .iter()
                .copied()
                .filter(|&i| covers(&resolved[i].period, &piece))
                .collect();
            let Some(top) = latest(&resolved, &covering) else {
                continue;
            };
            let o = &resolved[top].occurrence;
            let run = TextRun {
                period: piece,
                label: o.label.clone().unwrap_or_else(|| o.column.clone()),
                definition: o.definition.clone(),
                unit: o.unit.clone(),
            };
            match texts.last_mut() {
                Some(prev)
                    if next_iso_day(&prev.period.1) == run.period.0
                        && (&prev.label, &prev.definition, &prev.unit)
                            == (&run.label, &run.definition, &run.unit) =>
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
    report.variables = variables.len();

    // T1 findings.
    for (i, r) in resolved.iter().enumerate() {
        if r.stated.is_some() {
            findings.push(Finding {
                code: "T1:clamp",
                severity: "notice",
                subject: Subject::Occurrence(i),
                period: Some(r.period.clone()),
                message: if r.status == "used" {
                    "documented beyond its table's coverage; clamped".to_owned()
                } else {
                    "documented entirely outside its table's coverage".to_owned()
                },
            });
        }
    }

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
        variables,
        occurrences: resolved,
        findings,
        report,
    })
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
    for (id, anchors) in &ledger.variables {
        let targets: BTreeSet<usize> = anchors
            .iter()
            .flat_map(|anchor| {
                let (table, spelling) = anchor.split_once(": ").unwrap_or(("", anchor));
                let f = fold_identity(spelling.trim());
                resolved.iter().filter_map(move |r| {
                    (tables[r.table].table.table == table.trim()
                        && fold_identity(&r.occurrence.column) == f)
                        .then_some(r.variable)
                        .flatten()
                })
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

/// The default coded state: `yes` when the source binds codes or names a code system.
#[must_use]
pub fn coded(o: &Occurrence) -> &'static str {
    if o.valueset.is_some() || o.code_system.is_some() {
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
    } else if o.valueset.is_some() {
        "code"
    } else {
        "measure"
    }
}

/// The latest of `set`, by [`order`].
fn latest(resolved: &[ResolvedOccurrence], set: &[usize]) -> Option<usize> {
    set.iter()
        .copied()
        .max_by(|&a, &b| order(&resolved[a]).cmp(&order(&resolved[b])))
}

/// The ordering that makes an occurrence "latest": later start, then later end, then
/// its locator.
fn order(r: &ResolvedOccurrence) -> (&Interval, &Locator) {
    (&r.period, &r.occurrence.locator)
}

type Form = (Option<String>, Option<String>, Option<String>);

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
    (
        a.ambiguous,
        &a.dtype,
        &a.valueset,
        a.coded,
        a.sensitive,
        a.identifier,
        a.role,
    ) == (
        b.ambiguous,
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
