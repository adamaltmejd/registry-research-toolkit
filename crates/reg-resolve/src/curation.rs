//! Resolve curation against source coordinates. The apply module prepares the
//! moved/asserted frame; other overrides never feed selector resolution. Selectors
//! and premises describe the source the curator reviewed.

pub mod apply;

use std::collections::{BTreeMap, BTreeSet};

use reg_core::{Interval, fold_identity, intersect};
use reg_frame::address::{Address, Comparison, GuardField, PeriodSet};
use reg_frame::statement::{AssertFact, SetFields, Statement};
use reg_frame::{Locator, OPEN_END, OPEN_START, Record};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Ledger;

/// The reviewed source facts, independent of statement layout and input row order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Premise {
    pub subjects: Vec<SourcePremise>,
    pub objects: Vec<SourcePremise>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePremise {
    pub locator: Locator,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug)]
pub struct ResolvedStatement {
    pub premise: Premise,
    /// A set states only values already stated by all its source matches.
    pub redundant: bool,
    /// Indices into the supplied statements; the caller adds both authoring locations.
    pub conflicts: Vec<usize>,
}

struct Source {
    fields: BTreeMap<String, Value>,
    locators: Vec<Locator>,
    period: Option<Interval>,
}

impl Source {
    fn text(&self, name: &str) -> Option<&str> {
        self.fields.get(name).and_then(Value::as_str)
    }
}

fn periods(set: &PeriodSet) -> Vec<Interval> {
    set.to_string()
        .split(',')
        .map(|text| {
            if let Some(end) = text.strip_prefix("..") {
                (
                    OPEN_START.to_owned(),
                    end.parse::<reg_core::Period>()
                        .expect("validated period")
                        .iso_bounds()
                        .1,
                )
            } else if let Some(start) = text.strip_suffix("..") {
                (
                    start
                        .parse::<reg_core::Period>()
                        .expect("validated period")
                        .iso_bounds()
                        .0,
                    OPEN_END.to_owned(),
                )
            } else {
                text.parse::<reg_core::Period>()
                    .expect("validated period")
                    .iso_bounds()
            }
        })
        .collect()
}

fn bounds(fields: &BTreeMap<String, Value>) -> Interval {
    let bound = |name, default: &str| {
        fields
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or(default)
            .to_owned()
    };
    (bound("from", OPEN_START), bound("to", OPEN_END))
}

#[allow(clippy::too_many_lines)]
fn sources(records: &[Record], ledger: &Ledger) -> Vec<Source> {
    let context = crate::OccurrenceContext::new(records);
    let registers = &context.registers;
    let tables = &context.tables;
    let mut sources: Vec<_> = records
        .iter()
        .filter_map(|record| {
            let (mut locators, period) = match record {
                Record::Register(r) => (
                    vec![r.locator.clone()],
                    Some((
                        r.from.clone().unwrap_or_else(|| OPEN_START.into()),
                        r.to.clone().unwrap_or_else(|| OPEN_END.into()),
                    )),
                ),
                Record::Table(t) => (
                    vec![t.locator.clone()],
                    Some((
                        t.from.clone().unwrap_or_else(|| OPEN_START.into()),
                        t.to.clone().unwrap_or_else(|| OPEN_END.into()),
                    )),
                ),
                Record::Release(r) => (
                    vec![r.locator.clone()],
                    r.from
                        .as_ref()
                        .zip(r.to.as_ref())
                        .map(|(a, b)| (a.clone(), b.clone())),
                ),
                Record::Occurrence(o) => {
                    let mut locators = vec![o.locator.clone()];
                    locators.extend(o.also.iter().cloned());
                    let period = context.period(o);
                    (locators, period)
                }
                Record::ValueSet(v) => (vec![v.locator.clone()], None),
                _ => return None,
            };
            locators.sort();
            locators.dedup();
            let Value::Object(value) =
                serde_json::to_value(record).expect("source record serializes")
            else {
                unreachable!("record is an object")
            };
            let mut fields: BTreeMap<_, _> = value.into_iter().collect();
            for (source, target) in [
                ("dtype", "type"),
                ("column", "spelling"),
                ("source_key", "key"),
                ("valueset", "value_set"),
            ] {
                if let Some(value) = fields.remove(source) {
                    fields.insert(target.into(), value);
                }
            }
            if let Some(handle) = fields.get("table").and_then(Value::as_str) {
                let register = fields
                    .get("provider")
                    .and_then(Value::as_str)
                    .zip(fields.get("register").and_then(Value::as_str))
                    .and_then(|key| registers.get(&key))
                    .copied();
                let slug = if handle.is_empty() {
                    ledger
                        .public
                        .clone()
                        .or_else(|| register.map(|r| r.register.clone()))
                        .unwrap_or_default()
                } else {
                    ledger
                        .tables
                        .get(handle)
                        .cloned()
                        .unwrap_or_else(|| crate::slugify(handle))
                };
                fields.insert("table_slug".into(), Value::String(slug));
            }
            fields.insert(
                "period".into(),
                period
                    .as_ref()
                    .map_or(Value::Null, |p| serde_json::json!([p.0, p.1])),
            );
            fields.insert("coverage".into(), serde_json::json!(bounds(&fields)));
            Some(Source {
                fields,
                locators,
                period,
            })
        })
        .collect();
    for register in registers.values() {
        if tables.contains_key(&(register.provider.as_str(),register.register.as_str(),"")) || !records.iter().any(|r| matches!(r,Record::Occurrence(o) if o.table.is_empty() && o.provider == register.provider && o.register == register.register)) { continue; }
        if let Some(source) = sources.iter().find(|s| {
            s.text("kind") == Some("register")
                && s.text("provider") == Some(register.provider.as_str())
                && s.text("register") == Some(register.register.as_str())
        }) {
            let mut fields = source.fields.clone();
            fields.insert("kind".into(), Value::String("table".into()));
            fields.insert("table".into(), Value::String(String::new()));
            fields.insert(
                "table_slug".into(),
                Value::String(
                    ledger
                        .public
                        .clone()
                        .unwrap_or_else(|| register.register.clone()),
                ),
            );
            sources.push(Source {
                fields,
                locators: source.locators.clone(),
                period: source.period.clone(),
            });
        }
    }
    sources
}

#[derive(Clone)]
struct Match {
    source: usize,
    /// Selector-clipped source periods, used only for conflict/ambiguity checks.
    spans: Vec<Interval>,
}

fn guard_name(field: GuardField) -> &'static str {
    match field {
        GuardField::Label => "label",
        GuardField::Type => "type",
        GuardField::Unit => "unit",
        GuardField::Key => "key",
        GuardField::Release => "release",
    }
}

/// Candidate rows from one immutable phase snapshot. The selector still checks
/// guards and periods; indexes only remove rows that cannot match.
struct SelectorIndex {
    addresses: BTreeMap<(String, String, String), Vec<usize>>,
    variables: BTreeMap<String, Vec<usize>>,
}

impl SelectorIndex {
    fn new(view: &[Source], variables: &BTreeMap<String, BTreeSet<crate::OccurrenceKey>>) -> Self {
        let mut addresses = BTreeMap::<_, Vec<_>>::new();
        let mut memberships = BTreeMap::<_, Vec<_>>::new();
        for (variable, members) in variables {
            for member in members {
                memberships.entry(member).or_default().push(variable);
            }
        }
        let mut selected_variables = BTreeMap::<String, Vec<usize>>::new();
        for (index, source) in view.iter().enumerate() {
            let kind = source.text("kind").unwrap_or_default();
            let table = source.text("table_slug").unwrap_or_default();
            let detail = match kind {
                "occurrence" => match source.text("spelling").unwrap_or_default() {
                    "" => "<none>".into(),
                    spelling => fold_identity(spelling),
                },
                "release" => source.text("release").unwrap_or_default().into(),
                _ => String::new(),
            };
            addresses
                .entry((kind.into(), table.into(), detail))
                .or_default()
                .push(index);
            if !memberships.is_empty()
                && let Some(identity) = crate::OccurrenceIdentity::from_fields(&source.fields)
            {
                let mut names = BTreeSet::new();
                for at in &source.locators {
                    if let Some(variables) =
                        memberships.get(&crate::OccurrenceKey::new(identity.clone(), at))
                    {
                        names.extend(variables.iter().copied());
                    }
                }
                for name in names {
                    selected_variables
                        .entry(name.clone())
                        .or_default()
                        .push(index);
                }
            }
        }
        Self {
            addresses,
            variables: selected_variables,
        }
    }

    fn select(
        &self,
        address: &Address,
        view: &[Source],
        variables: &BTreeMap<String, BTreeSet<crate::OccurrenceKey>>,
    ) -> Vec<Match> {
        let mut candidates = Vec::new();
        let mut add = |kind: &str, table: &str, detail: &str| {
            if let Some(indices) = self
                .addresses
                .get(&(kind.into(), table.into(), detail.into()))
            {
                candidates.extend(indices.iter().copied());
            }
        };
        match address {
            Address::Occurrence {
                table, spellings, ..
            } => {
                for spelling in spellings {
                    add("occurrence", table, spelling);
                }
            }
            Address::Table(table) => add("table", table, ""),
            Address::Release { table, handle } => add("release", table, handle),
            Address::Variable(id) => {
                candidates.extend(self.variables.get(id).into_iter().flatten().copied());
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        select_from(address, view, variables, candidates.into_iter())
    }
}

fn select(
    address: &Address,
    view: &[Source],
    variables: &BTreeMap<String, BTreeSet<crate::OccurrenceKey>>,
) -> Vec<Match> {
    select_from(address, view, variables, 0..view.len())
}

fn select_from(
    address: &Address,
    view: &[Source],
    variables: &BTreeMap<String, BTreeSet<crate::OccurrenceKey>>,
    candidates: impl Iterator<Item = usize>,
) -> Vec<Match> {
    let scope = match address {
        Address::Occurrence {
            periods: Some(scope),
            ..
        } => Some(periods(scope)),
        _ => None,
    };
    candidates
        .filter_map(|index| {
            let source = &view[index];
            let selected = match address {
                Address::Occurrence {
                    table,
                    spellings,
                    guards,
                    ..
                } => {
                    source.text("kind") == Some("occurrence")
                        && source.text("table_slug") == Some(table)
                        && source.text("spelling").is_some_and(|s| {
                            spellings.contains(&if s.is_empty() {
                                "<none>".into()
                            } else {
                                fold_identity(s)
                            })
                        })
                        && guards.iter().all(|g| {
                            source.text(guard_name(g.field)).is_some_and(|value| {
                                match g.comparison {
                                    Comparison::Equal => value == g.value,
                                    Comparison::Prefix => value.starts_with(&g.value),
                                }
                            })
                        })
                }
                Address::Table(table) => {
                    source.text("kind") == Some("table") && source.text("table_slug") == Some(table)
                }
                Address::Release { table, handle } => {
                    source.text("kind") == Some("release")
                        && source.text("table_slug") == Some(table)
                        && source.text("release") == Some(handle)
                }
                Address::Variable(id) => variables.get(id).is_some_and(|members| {
                    crate::OccurrenceIdentity::from_fields(&source.fields).is_some_and(|identity| {
                        source.locators.iter().any(|at| {
                            members.contains(&crate::OccurrenceKey::new(identity.clone(), at))
                        })
                    })
                }),
            };
            if !selected {
                return None;
            }
            let spans = match (address, &source.period) {
                (
                    Address::Occurrence {
                        periods: Some(_), ..
                    },
                    Some(period),
                ) => scope
                    .as_ref()
                    .expect("scoped selector")
                    .iter()
                    .filter_map(|s| intersect(s, period))
                    .collect(),
                (
                    Address::Occurrence {
                        periods: Some(_), ..
                    },
                    None,
                ) => return None,
                (_, Some(period)) => vec![period.clone()],
                (_, None) => vec![(OPEN_START.into(), OPEN_END.into())],
            };
            (!spans.is_empty()).then_some(Match {
                source: index,
                spans,
            })
        })
        .collect()
}

fn whole_occurrences(matches: &[Match], view: &[Source]) -> Result<(), String> {
    for matched in matches {
        let source = &view[matched.source];
        if source.text("kind") == Some("occurrence")
            && let Some(period) = &source.period
            && !reg_core::gaps(std::slice::from_ref(period), matched.spans.clone()).is_empty()
        {
            return Err(format!(
                "selector period cuts occurrence {}:{}; select whole occurrences",
                source.locators[0].input, source.locators[0].at
            ));
        }
    }
    Ok(())
}

fn overlaps(a: &Match, b: &Match) -> bool {
    !reg_core::overlap(&a.spans, &b.spans).is_empty()
}

fn same_subject(a: &Source, b: &Source) -> bool {
    if a.text("kind") == Some("occurrence") || b.text("kind") == Some("occurrence") {
        return crate::OccurrenceIdentity::from_fields(&a.fields)
            .zip(crate::OccurrenceIdentity::from_fields(&b.fields))
            .is_some_and(|(a, b)| a == b)
            && a.locators.iter().any(|l| b.locators.contains(l));
    }
    a.text("kind") == b.text("kind")
        && ["provider", "register", "table", "release"]
            .iter()
            .all(|field| {
                a.fields
                    .get(*field)
                    .zip(b.fields.get(*field))
                    .is_none_or(|(a, b)| a == b)
            })
        && a.locators.iter().any(|l| b.locators.contains(l))
}

fn ambiguity(matches: &[Match], view: &[Source]) -> Result<(), String> {
    let mut candidates = BTreeSet::new();
    let mut ambiguous = false;
    let mut coordinates: BTreeMap<(&str, String), Vec<&Match>> = BTreeMap::new();
    for selected in matches {
        let source = &view[selected.source];
        if source.text("kind") == Some("occurrence") {
            coordinates
                .entry((
                    source.text("table").unwrap_or_default(),
                    fold_identity(source.text("spelling").unwrap_or_default()),
                ))
                .or_default()
                .push(selected);
        }
    }
    // simplify: pair scan within one spelling; use an interval sweep if a single
    // spelling routinely spans thousands of overlapping source records.
    for matches in coordinates.values() {
        for (i, a) in matches.iter().enumerate() {
            let a_source = &view[a.source];
            for b in &matches[i + 1..] {
                let b_source = &view[b.source];
                if overlaps(a, b)
                    && (a_source.fields.get("key") != b_source.fields.get("key")
                        || a_source.fields.get("label") != b_source.fields.get("label"))
                {
                    ambiguous = true;
                    for source in [a_source, b_source] {
                        for field in ["label", "key", "release"] {
                            if let Some(value) = source.text(field)
                                && !value.contains(['\'', '\n', '\r'])
                            {
                                candidates.insert(format!("where {field} = '{value}'"));
                            }
                        }
                    }
                }
            }
        }
    }
    if ambiguous {
        Err(format!(
            "ambiguous: add a `where` guard; candidates: {}",
            candidates.into_iter().collect::<Vec<_>>().join(", ")
        ))
    } else {
        Ok(())
    }
}

fn stated_fields(fields: &SetFields) -> BTreeMap<String, Value> {
    let Value::Object(fields) = serde_json::to_value(fields).expect("fields serialize") else {
        unreachable!("fields object")
    };
    fields.into_iter().filter(|(_, v)| !v.is_null()).collect()
}

fn field_equal(source: &Source, field: &str, expected: &Value) -> bool {
    if matches!(field, "period" | "coverage") {
        expected
            .as_str()
            .and_then(|s| s.parse::<PeriodSet>().ok())
            .is_some_and(|p| {
                source
                    .period
                    .as_ref()
                    .is_some_and(|span| reg_core::merge(periods(&p)) == vec![span.clone()])
            })
    } else {
        source.fields.get(field).unwrap_or(&Value::Null) == expected
    }
}

fn premise(matches: &[Match], view: &[Source], fields: &BTreeSet<String>) -> Vec<SourcePremise> {
    let mut out = Vec::new();
    for selected in matches {
        let source = &view[selected.source];
        let fields = fields
            .iter()
            .map(|f| {
                (
                    f.clone(),
                    source.fields.get(f).cloned().unwrap_or(Value::Null),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for locator in &source.locators {
            out.push(SourcePremise {
                locator: locator.clone(),
                fields: fields.clone(),
            });
        }
    }
    out.sort_by_cached_key(reg_frame::canonical_line);
    out.dedup();
    out
}

fn dependency_fields(address: &Address, fields: &mut BTreeSet<String>) {
    if let Address::Occurrence { guards, .. } = address {
        fields.extend(guards.iter().map(|g| guard_name(g.field).into()));
    }
}

/// Capture raw evidence behind a curated field, including defaults derived from
/// source facts. Keep derived values out of the source premise.
fn field_dependencies(fields: &mut BTreeSet<String>) {
    if fields.contains("classification") || fields.contains("coded") {
        fields.insert("code_system".into());
    }
    if fields.contains("role") {
        fields.insert("identifier".into());
    }
    if fields.contains("coded") || fields.contains("role") {
        fields.insert("value_set".into());
    }
    if fields.contains("value_set") {
        fields.insert("population_valuesets".into());
    }
    if fields.contains("period") {
        fields.extend(["grain", "name", "from", "to"].map(str::to_owned));
    }
    if fields.contains("coverage") {
        fields.extend(["from", "to"].map(str::to_owned));
    }
}

fn states_value_set(source: &Source, items: &[reg_frame::Item], records: &[Record]) -> bool {
    let populations = source
        .fields
        .get("population_valuesets")
        .and_then(Value::as_array);
    let hashes = source.text("value_set").into_iter().chain(
        populations
            .into_iter()
            .flatten()
            .filter_map(|p| p.get("valueset").and_then(Value::as_str)),
    );
    let mut expected = items.to_vec();
    expected.sort();
    hashes.into_iter().any(|hash| {
        records.iter().any(|record| {
            let Record::ValueSet(set) = record else {
                return false;
            };
            if set.hash != hash {
                return false;
            }
            let mut stated = set.items.clone();
            stated.sort();
            stated == expected
        })
    })
}

fn effects(statement: &Statement) -> BTreeSet<String> {
    match statement {
        Statement::Set { fields, reject, .. } => {
            let mut result: BTreeSet<_> = stated_fields(fields).into_keys().collect();
            if *reject {
                result.insert("reject".into());
            }
            result
        }
        Statement::Assert { fact, .. } => match fact {
            AssertFact::Occurrence { fields } | AssertFact::Table { fields } => {
                stated_fields(fields).into_keys().collect()
            }
            AssertFact::ValueSet { .. } => BTreeSet::from(["value_set".into()]),
            _ => BTreeSet::from(["assertion".into()]),
        },
        Statement::Link {
            relation, object, ..
        } => {
            let object = object
                .parse::<Address>()
                .map_or_else(|_| object.clone(), |a| a.to_string());
            BTreeSet::from([format!("relation:{relation:?}:{object}")])
        }
        Statement::Ack { finding, .. } => BTreeSet::from([format!("finding:{finding}")]),
    }
}

fn moves_table(statement: &Statement) -> bool {
    matches!(statement, Statement::Set { fields, .. } if fields.table.is_some())
}

fn effects_overlap(a: &BTreeSet<String>, b: &BTreeSet<String>) -> bool {
    !a.is_disjoint(b) || a.contains("reject") || b.contains("reject")
}

/// A field introduced by an accepted table move or assertion, in final source coordinates.
/// `statement` indexes the evaluated statements supplied to [`applied_conflicts`].
pub struct AppliedClaim<'a> {
    pub statement: usize,
    pub kind: &'a str,
    pub locator: &'a Locator,
    pub identity: Option<&'a crate::OccurrenceIdentity>,
    pub provider: Option<&'a str>,
    pub register: Option<&'a str>,
    pub table: Option<&'a str>,
    pub release: Option<&'a str>,
    pub field: &'a str,
    pub period: Option<&'a Interval>,
}

fn premise_source(premise: &SourcePremise) -> Source {
    let period = premise
        .fields
        .get("period")
        .and_then(Value::as_array)
        .and_then(|p| {
            p.first()
                .and_then(Value::as_str)
                .zip(p.get(1).and_then(Value::as_str))
                .map(|(a, b)| (a.to_owned(), b.to_owned()))
        });
    Source {
        fields: premise.fields.clone(),
        locators: vec![premise.locator.clone()],
        period,
    }
}

fn subject_spans(statement: &Statement, source: &Source) -> Vec<Interval> {
    let span = source
        .period
        .clone()
        .unwrap_or_else(|| (OPEN_START.into(), OPEN_END.into()));
    let mut selected = match subject(statement) {
        Address::Occurrence {
            periods: Some(scope),
            ..
        } => periods(scope)
            .iter()
            .filter_map(|p| intersect(&span, p))
            .collect(),
        _ => vec![span],
    };
    if let Statement::Link {
        at: Some(scope), ..
    } = statement
    {
        selected = reg_core::overlap(&selected, &periods(scope));
    }
    selected
}

fn premise_overlap(
    left: &Statement,
    left_premise: &Premise,
    right: &Statement,
    right_premise: &Premise,
) -> bool {
    let overlaps = |left_rows: &[SourcePremise], right_rows: &[SourcePremise]| {
        left_rows.iter().any(|a| {
            let a = premise_source(a);
            right_rows.iter().any(|b| {
                let b = premise_source(b);
                same_subject(&a, &b)
                    && !reg_core::overlap(&subject_spans(left, &a), &subject_spans(right, &b))
                        .is_empty()
            })
        })
    };
    let selects_moved = |moving: &Statement, premise: &Premise, other: &Statement| {
        let Statement::Set { fields, .. } = moving else {
            return false;
        };
        let Some(table) = &fields.table else {
            return false;
        };
        let address = subject(other);
        premise.subjects.iter().any(|row| {
            let mut source = premise_source(row);
            source
                .fields
                .insert("table_slug".into(), Value::String(table.clone()));
            // Table moves detach dated release rows; a release guard no longer matches.
            source.fields.insert("release".into(), Value::Null);
            !select(address, &[source], &BTreeMap::new()).is_empty()
        })
    };
    overlaps(&left_premise.subjects, &right_premise.subjects)
        || (moves_table(left)
            && moves_table(right)
            && (selects_moved(left, left_premise, right)
                || selects_moved(right, right_premise, left)))
}

/// Find overlaps between already-applied source-shaped claims and evaluated
/// statements. Uses the same effects, source identity and interval overlap as the
/// register-local evaluator; the build boundary adds authoring locations.
#[must_use]
pub fn applied_conflicts(
    statements: &[(&Statement, &Premise)],
    claims: &[AppliedClaim<'_>],
) -> Vec<(usize, usize)> {
    let mut conflicts = BTreeSet::new();
    let evaluated: Vec<_> = statements
        .iter()
        .map(|(statement, premise)| {
            (
                effects(statement),
                premise
                    .subjects
                    .iter()
                    .map(|row| {
                        let source = premise_source(row);
                        let spans = subject_spans(statement, &source);
                        (source, spans)
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let mut by_locator = BTreeMap::<_, Vec<_>>::new();
    for (index, (_, sources)) in evaluated.iter().enumerate() {
        for (row, (source, _)) in sources.iter().enumerate() {
            for locator in &source.locators {
                by_locator.entry(locator).or_default().push((index, row));
            }
        }
    }
    for claim in claims {
        let mut fields = BTreeMap::from([("kind".into(), Value::String(claim.kind.into()))]);
        for (field, value) in [
            ("provider", claim.provider),
            ("register", claim.register),
            ("table", claim.table),
            ("release", claim.release),
        ] {
            if let Some(value) = value {
                fields.insert(field.into(), Value::String(value.into()));
            }
        }
        if let Some(identity) = claim.identity {
            fields.insert("spelling".into(), Value::String(identity.column.clone()));
            fields.insert("key".into(), Value::String(identity.source_key.clone()));
            fields.insert("period".into(), serde_json::json!(identity.period));
        }
        let source = Source {
            fields,
            locators: vec![claim.locator.clone()],
            period: claim.period.cloned(),
        };
        let claimed = Match {
            source: 0,
            spans: vec![
                claim
                    .period
                    .cloned()
                    .unwrap_or_else(|| (OPEN_START.into(), OPEN_END.into())),
            ],
        };
        let field = BTreeSet::from([claim.field.to_owned()]);
        for &(index, row) in by_locator.get(claim.locator).into_iter().flatten() {
            let (effects, sources) = &evaluated[index];
            if index == claim.statement || !effects_overlap(effects, &field) {
                continue;
            }
            let (selected, spans) = &sources[row];
            if same_subject(selected, &source)
                && overlaps(
                    &Match {
                        source: 0,
                        spans: spans.clone(),
                    },
                    &claimed,
                )
            {
                conflicts.insert((index.min(claim.statement), index.max(claim.statement)));
            }
        }
    }
    conflicts.into_iter().collect()
}

/// Classification versions of one family have distinct names and disjoint periods.
#[must_use]
pub fn classification_conflicts(classes: &[apply::Classification]) -> Vec<(usize, usize)> {
    let mut conflicts = Vec::new();
    for (a, left) in classes.iter().enumerate() {
        for (b, right) in classes.iter().enumerate().skip(a + 1) {
            if left.family == right.family
                && (left.version == right.version
                    || intersect(&left.period, &right.period).is_some())
            {
                conflicts.push((a, b));
            }
        }
    }
    conflicts
}

fn subject(statement: &Statement) -> &Address {
    match statement {
        Statement::Assert { subject, .. }
        | Statement::Set { subject, .. }
        | Statement::Link { subject, .. }
        | Statement::Ack { subject, .. } => subject,
    }
}

fn addresses_overlap(a: &Address, b: &Address) -> bool {
    match (a, b) {
        (
            Address::Occurrence {
                table: a_table,
                spellings: a_spellings,
                periods: a_periods,
                guards: a_guards,
            },
            Address::Occurrence {
                table: b_table,
                spellings: b_spellings,
                periods: b_periods,
                guards: b_guards,
            },
        ) => {
            a_table == b_table
                && a_spellings.iter().any(|s| b_spellings.contains(s))
                && !a_guards.iter().any(|a| {
                    b_guards.iter().any(|b| {
                        a.field == b.field
                            && match (a.comparison, b.comparison) {
                                (Comparison::Equal, Comparison::Equal) => a.value != b.value,
                                (Comparison::Equal, Comparison::Prefix) => {
                                    !a.value.starts_with(&b.value)
                                }
                                (Comparison::Prefix, Comparison::Equal) => {
                                    !b.value.starts_with(&a.value)
                                }
                                (Comparison::Prefix, Comparison::Prefix) => {
                                    !a.value.starts_with(&b.value) && !b.value.starts_with(&a.value)
                                }
                            }
                    })
                })
                && match (a_periods, b_periods) {
                    (Some(a), Some(b)) => periods(a)
                        .iter()
                        .any(|a| periods(b).iter().any(|b| intersect(a, b).is_some())),
                    _ => true,
                }
        }
        (a, b) => a == b,
    }
}

/// Validate the complete assignment phase with the same rules as catalog resolution.
///
/// # Errors
/// Assignments that cannot preserve valid occurrence membership.
pub fn validate_assignments(
    records: &[Record],
    ledger: &Ledger,
    assignments: &BTreeMap<crate::OccurrenceKey, String>,
) -> Result<(), String> {
    crate::resolve(records, ledger, &BTreeMap::new(), assignments).map(|_| ())
}

fn variable_members(
    records: &[Record],
    ledger: &Ledger,
    assignments: &BTreeMap<crate::OccurrenceKey, String>,
) -> Result<BTreeMap<String, BTreeSet<crate::OccurrenceKey>>, String> {
    let resolved = crate::resolve(records, ledger, &BTreeMap::new(), assignments)?;
    let mut variables: BTreeMap<String, BTreeSet<crate::OccurrenceKey>> = BTreeMap::new();
    let context = crate::OccurrenceContext::new(records);
    for occurrence in &resolved.occurrences {
        if let Some(index) = occurrence.variable {
            let variable = &resolved.variables[index];
            if variable.pinned {
                let members = variables.entry(variable.slug.clone()).or_default();
                let row = &occurrence.occurrence;
                let identity = context.identity(row);
                members.extend(
                    std::iter::once(&row.locator)
                        .chain(&row.also)
                        .map(|at| crate::OccurrenceKey::new(identity.clone(), at)),
                );
            }
        }
    }
    Ok(variables)
}

/// Snapshot the source facts behind a register or ledger variable link target. The
/// build boundary supplies these snapshots for external FQID objects; this module
/// never reads another register or resolves a class reference by guessing.
///
/// # Errors
/// An unresolved ledger variable, invalid source register or an empty source target.
pub fn reference(
    records: &[Record],
    ledger: &Ledger,
    variable: Option<&str>,
    assignments: &BTreeMap<crate::OccurrenceKey, String>,
) -> Result<Vec<SourcePremise>, String> {
    let view = sources(records, ledger);
    let variables = if variable.is_some() {
        variable_members(records, ledger, assignments)?
    } else {
        BTreeMap::new()
    };
    let matches = if let Some(variable) = variable {
        select(&Address::Variable(variable.to_owned()), &view, &variables)
    } else {
        view.iter()
            .enumerate()
            .filter(|(_, s)| s.text("kind") == Some("register"))
            .map(|(source, s)| Match {
                source,
                spans: vec![
                    s.period
                        .clone()
                        .unwrap_or_else(|| (OPEN_START.into(), OPEN_END.into())),
                ],
            })
            .collect()
    };
    if matches.is_empty() {
        return Err("unresolved source reference".into());
    }
    let fields = matches
        .iter()
        .flat_map(|m| {
            view[m.source]
                .fields
                .keys()
                .filter(|k| !matches!(k.as_str(), "locator" | "also"))
                .cloned()
        })
        .collect();
    Ok(premise(&matches, &view, &fields))
}

type DependencyCoordinate = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn coordinate(source: &Source, register: &str, table: Option<&str>) -> DependencyCoordinate {
    let kind = source.text("kind").unwrap_or_default();
    (
        kind.to_owned(),
        source.text("provider").unwrap_or_default().to_owned(),
        register.to_owned(),
        table.map(str::to_owned),
        if kind == "release" {
            source.text("release").map(str::to_owned)
        } else {
            None
        },
        if kind == "occurrence" {
            source.text("key").map(str::to_owned)
        } else {
            None
        },
        if kind == "occurrence" {
            source.text("spelling").map(fold_identity)
        } else {
            None
        },
    )
}

#[derive(Default)]
struct DependencyIndex {
    coordinates: BTreeMap<DependencyCoordinate, Vec<usize>>,
    table_occurrences: BTreeMap<(String, String, Option<String>), Vec<usize>>,
    sets: BTreeMap<String, Vec<usize>>,
}

impl DependencyIndex {
    fn new(view: &[Source]) -> Self {
        let mut out = Self::default();
        for (index, source) in view.iter().enumerate() {
            if let Some(register) = source.text("register") {
                out.coordinates
                    .entry(coordinate(source, register, source.text("table")))
                    .or_default()
                    .push(index);
                if source.text("kind") == Some("occurrence") {
                    out.table_occurrences
                        .entry((
                            source.text("provider").unwrap_or_default().to_owned(),
                            register.to_owned(),
                            source.text("table").map(str::to_owned),
                        ))
                        .or_default()
                        .push(index);
                }
            } else if source.text("kind") == Some("value_set")
                && let Some(hash) = source.text("hash")
            {
                out.sets.entry(hash.to_owned()).or_default().push(index);
            }
        }
        out
    }
}

/// Extra source facts consumed while applying a table move or assertion. Keep these out of
/// `subjects`: they are evidence, never additional rows selected for rewriting.
#[allow(clippy::too_many_lines)]
fn application_dependencies(
    records: &[Record],
    ledger: &Ledger,
    statement: &Statement,
    selected: &[Match],
    view: &[Source],
    index: &DependencyIndex,
) -> Result<Vec<SourcePremise>, String> {
    if !matches!(statement, Statement::Assert { .. }) && !moves_table(statement) {
        return Ok(Vec::new());
    }
    let DependencyIndex {
        coordinates,
        table_occurrences,
        sets,
    } = index;
    let mut needed = BTreeSet::new();
    let mut contexts = BTreeSet::new();
    let mut hashes = BTreeSet::new();
    let context = |s: &Source| {
        s.text("provider").zip(s.text("register")).map(|(p, r)| {
            (
                p.to_owned(),
                r.to_owned(),
                s.text("table").map(str::to_owned),
                s.text("release").map(str::to_owned),
            )
        })
    };
    for matched in selected {
        if let Some(c) = context(&view[matched.source]) {
            contexts.insert(c);
        }
    }
    match statement {
        Statement::Set { fields, .. } if fields.table.is_some() => {
            let destination =
                apply::table_handle(records, ledger, fields.table.as_ref().expect("table set"))?;
            for matched in selected {
                let source = &view[matched.source];
                let Some((provider, register, _table, release)) = context(source) else {
                    continue;
                };
                let target_register = register;
                let target_table = Some(destination.clone());
                contexts.insert((
                    provider.clone(),
                    target_register.clone(),
                    target_table.clone(),
                    release.clone(),
                ));
                for index in coordinates
                    .get(&coordinate(
                        source,
                        &target_register,
                        target_table.as_deref(),
                    ))
                    .into_iter()
                    .flatten()
                    .copied()
                {
                    let candidate = &view[index];
                    if source.text("kind") == Some("occurrence")
                        && candidate.period.as_ref().is_some_and(|p| {
                            !matched
                                .spans
                                .iter()
                                .any(|scope| intersect(p, scope).is_some())
                        })
                    {
                        continue;
                    }
                    needed.insert(index);
                    if let Some(c) = context(candidate) {
                        contexts.insert(c);
                    }
                }
            }
        }
        Statement::Assert { subject, fact, .. } => {
            if let Some(register) = records.iter().find_map(|r| match r {
                Record::Register(r) => Some(r),
                _ => None,
            }) {
                let table = match subject {
                    Address::Occurrence { table, .. }
                    | Address::Release { table, .. }
                    | Address::Table(table) => Some(apply::table_handle(records, ledger, table)?),
                    Address::Variable(_) => None,
                };
                let release = match subject {
                    Address::Release { handle, .. } => Some(handle.clone()),
                    Address::Occurrence { guards, .. } => guards
                        .iter()
                        .find(|g| g.field == GuardField::Release)
                        .map(|g| g.value.clone()),
                    _ => None,
                };
                contexts.insert((
                    register.provider.clone(),
                    register.register.clone(),
                    table,
                    release,
                ));
            }
            if let AssertFact::Occurrence { fields } = fact
                && let Some(hash) = &fields.value_set
            {
                hashes.insert(hash.clone());
            }
        }
        _ => return Ok(Vec::new()),
    }
    for (provider, register, table, release) in &contexts {
        let key = |kind: &str, table: Option<String>, release: Option<String>| {
            (
                kind.to_owned(),
                provider.clone(),
                register.clone(),
                table,
                release,
                None,
                None,
            )
        };
        needed.extend(
            coordinates
                .get(&key("register", None, None))
                .into_iter()
                .flatten()
                .copied(),
        );
        let table_rows = coordinates.get(&key("table", table.clone(), None));
        needed.extend(table_rows.into_iter().flatten().copied());
        if table.is_some() && table_rows.is_none() {
            needed.extend(
                table_occurrences
                    .get(&(provider.clone(), register.clone(), table.clone()))
                    .into_iter()
                    .flatten()
                    .copied(),
            );
        }
        if release.is_some() {
            needed.extend(
                coordinates
                    .get(&key("release", table.clone(), release.clone()))
                    .into_iter()
                    .flatten()
                    .copied(),
            );
        }
    }
    for index in selected
        .iter()
        .map(|m| m.source)
        .chain(needed.iter().copied())
    {
        let source = &view[index];
        if let Some(hash) = source.text("value_set") {
            hashes.insert(hash.to_owned());
        }
        if let Some(populations) = source
            .fields
            .get("population_valuesets")
            .and_then(Value::as_array)
        {
            hashes.extend(
                populations
                    .iter()
                    .filter_map(|p| p.get("valueset").and_then(Value::as_str))
                    .map(str::to_owned),
            );
        }
    }
    for hash in &hashes {
        needed.extend(sets.get(hash.as_str()).into_iter().flatten().copied());
    }
    let matches = needed
        .into_iter()
        .map(|source| Match {
            source,
            spans: Vec::new(),
        })
        .collect::<Vec<_>>();
    let fields = matches
        .iter()
        .flat_map(|m| view[m.source].fields.keys().cloned())
        .collect();
    let mut dependencies = premise(&matches, view, &fields);
    dependencies.sort_by_cached_key(reg_frame::canonical_line);
    dependencies.dedup();
    Ok(dependencies)
}

fn conflict_candidates(
    statements: &[&Statement],
    premises: &[&Premise],
) -> BTreeSet<(usize, usize)> {
    let mut locators = BTreeMap::<_, BTreeSet<_>>::new();
    let mut assertions = BTreeMap::<_, BTreeSet<_>>::new();
    let mut moves = BTreeSet::new();
    for (index, (statement, premise)) in statements.iter().zip(premises).enumerate() {
        for row in &premise.subjects {
            locators.entry(&row.locator).or_default().insert(index);
        }
        if moves_table(statement) {
            moves.insert(index);
        }
        if !matches!(statement, Statement::Assert { .. }) {
            continue;
        }
        match subject(statement) {
            Address::Occurrence {
                table, spellings, ..
            } => {
                for spelling in spellings {
                    assertions
                        .entry((0, table.as_str(), spelling.as_str()))
                        .or_default()
                        .insert(index);
                }
            }
            Address::Table(table) => {
                assertions
                    .entry((1, table.as_str(), ""))
                    .or_default()
                    .insert(index);
            }
            Address::Release { table, handle } => {
                assertions
                    .entry((2, table.as_str(), handle.as_str()))
                    .or_default()
                    .insert(index);
            }
            Address::Variable(variable) => {
                assertions
                    .entry((3, variable.as_str(), ""))
                    .or_default()
                    .insert(index);
            }
        }
    }
    let mut candidates = BTreeSet::new();
    // A shared locator is necessary even when missing coordinates act as wildcards.
    // Assertions also conflict at empty addresses; chained moves can select new coordinates.
    // simplify: pair moves within a register; index destinations if thousands of moves accumulate.
    for bucket in locators
        .values()
        .chain(assertions.values())
        .chain(std::iter::once(&moves))
    {
        for &a in bucket {
            for &b in bucket.range((std::ops::Bound::Excluded(a), std::ops::Bound::Unbounded)) {
                candidates.insert((a, b));
            }
        }
    }
    candidates
}

/// Compute premises in statement order. This function performs no IO and applies no
/// statements. The caller supplies the immutable view for the current application
/// phase; ordinary selectors see the moved frame plus asserted facts.
///
/// Empty ordinary selectors, ambiguous source coordinates and source-redundant
/// assertions are refusals. Conflicts point to both statement indices on success.
///
/// # Panics
/// Only if the typed statement/source model gains content that JSON cannot serialize.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn resolve(
    records: &[Record],
    ledger: &Ledger,
    statements: &[&Statement],
    references: &BTreeMap<String, Vec<SourcePremise>>,
    assignments: &BTreeMap<crate::OccurrenceKey, String>,
) -> Vec<Result<ResolvedStatement, String>> {
    if statements.is_empty() {
        return Vec::new();
    }
    let view = sources(records, ledger);
    let needs_variables = statements.iter().any(|statement| {
        matches!(subject(statement), Address::Variable(_)) || matches!(statement, Statement::Link { object, .. } if matches!(object.parse::<Address>(), Ok(Address::Variable(_))))
    });
    let variable_result = if needs_variables {
        variable_members(records, ledger, assignments)
    } else {
        Ok(BTreeMap::new())
    };
    let empty_variables = BTreeMap::new();
    let variables = variable_result.as_ref().unwrap_or(&empty_variables);
    let index = SelectorIndex::new(&view, variables);
    let dependencies = if statements
        .iter()
        .any(|s| matches!(s, Statement::Assert { .. }) || moves_table(s))
    {
        DependencyIndex::new(&view)
    } else {
        DependencyIndex::default()
    };
    let all_fields: BTreeSet<_> = view.iter().flat_map(|s| s.fields.keys().cloned()).collect();
    let evidence_fields: BTreeSet<_> = all_fields
        .iter()
        .filter(|k| !matches!(k.as_str(), "locator" | "also"))
        .cloned()
        .collect();
    let statement_effects: Vec<_> = statements.iter().map(|s| effects(s)).collect();
    let mut out = Vec::new();
    let mut selected = Vec::new();
    for (statement_index, statement) in statements.iter().enumerate() {
        let address = subject(statement);
        let mut matches = index.select(address, &view, variables);
        let whole = whole_occurrences(&matches, &view);
        if let Statement::Link {
            at: Some(scope), ..
        } = statement
        {
            for selected in &mut matches {
                selected.spans = selected
                    .spans
                    .iter()
                    .flat_map(|span| {
                        periods(scope)
                            .iter()
                            .filter_map(|s| intersect(span, s))
                            .collect::<Vec<_>>()
                    })
                    .collect();
            }
            matches.retain(|m| !m.spans.is_empty());
        }
        selected.push(Premise {
            subjects: premise(&matches, &view, &all_fields),
            objects: Vec::new(),
        });
        let result = (|| {
            if matches!(address, Address::Variable(_))
                && let Err(error) = &variable_result
            {
                return Err(format!("variable selector: {error}"));
            }
            if matches.is_empty() && !matches!(statement, Statement::Assert { .. }) {
                return Err("empty match".into());
            }
            ambiguity(&matches, &view)?;
            whole?;
            let mut fields: BTreeSet<String> = [
                "kind",
                "provider",
                "register",
                "table",
                "table_slug",
                "spelling",
                "key",
                "label",
                "release",
                "period",
                "conflicts",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect();
            let mut affected = statement_effects[statement_index].clone();
            field_dependencies(&mut affected);
            fields.extend(affected);
            dependency_fields(address, &mut fields);
            let mut redundant = false;
            match statement {
                Statement::Set {
                    fields: values,
                    reject,
                    ..
                } => {
                    if values.table.is_some() {
                        fields.extend(all_fields.iter().cloned());
                    }
                    if values.variable.is_some() {
                        fields.extend(
                            ["type", "value_set", "population_valuesets"].map(str::to_owned),
                        );
                    }
                    if (values.table.is_some() || values.variable.is_some())
                        && !matches!(address, Address::Occurrence { .. })
                    {
                        return Err(
                            "table and variable fields require an occurrence selector".into()
                        );
                    }
                    if values.variable.is_some()
                        && matches
                            .iter()
                            .any(|m| view[m.source].text("spelling") == Some(""))
                    {
                        return Err(
                            "set variable cannot target a gap marker; ack the finding instead"
                                .into(),
                        );
                    }
                    if values.variable.as_deref().is_some_and(|v| v != "none")
                        && matches.iter().any(|m| view[m.source].period.is_none())
                    {
                        return Err("set variable cannot target an unplaced occurrence".into());
                    }
                    if let Some(variable) = &values.variable
                        && variable != "none"
                        && !ledger.variables.contains_key(variable)
                    {
                        return Err(format!("variable {variable:?} is not a ledger slug"));
                    }
                    let values = stated_fields(values);
                    redundant = !reject
                        && !values.is_empty()
                        && matches.iter().all(|m| {
                            values
                                .iter()
                                .all(|(f, v)| field_equal(&view[m.source], f, v))
                        });
                }
                Statement::Assert { fact, .. } => {
                    if matches!(
                        fact,
                        AssertFact::ValueSet { .. } | AssertFact::ClassificationVersion { .. }
                    ) {
                        fields.extend(
                            ["value_set", "population_valuesets", "code_system"].map(str::to_owned),
                        );
                    }
                    let values = match fact {
                        AssertFact::Occurrence { fields } | AssertFact::Table { fields } => {
                            stated_fields(fields)
                        }
                        AssertFact::Release { name, period } => BTreeMap::from([
                            ("name".into(), Value::String(name.clone())),
                            ("period".into(), Value::String(period.to_string())),
                        ]),
                        AssertFact::ValueSet { items }
                        | AssertFact::ClassificationVersion { items, .. } => BTreeMap::from([(
                            "items".into(),
                            serde_json::to_value(items).expect("items serialize"),
                        )]),
                    };
                    fields.extend(values.keys().cloned());
                    if let AssertFact::ValueSet { items } = fact
                        && matches
                            .iter()
                            .any(|m| states_value_set(&view[m.source], items, records))
                    {
                        return Err("redundant assert: the source now states this value set".into());
                    }
                    if matches.iter().any(|m| {
                        values
                            .iter()
                            .all(|(f, v)| field_equal(&view[m.source], f, v))
                    }) {
                        return Err("redundant assert: the source now states this fact".into());
                    }
                }
                Statement::Ack { .. } | Statement::Link { .. } => {
                    fields.extend(evidence_fields.iter().cloned());
                }
            }
            let objects = if let Statement::Link { object, .. } = statement {
                if let Ok(address) = object.parse::<Address>() {
                    let objects = index.select(&address, &view, variables);
                    if objects.is_empty() {
                        return Err("empty match for link object".into());
                    }
                    ambiguity(&objects, &view)?;
                    whole_occurrences(&objects, &view)?;
                    dependency_fields(&address, &mut fields);
                    premise(&objects, &view, &fields)
                } else {
                    references
                        .get(object)
                        .cloned()
                        .filter(|matches| !matches.is_empty())
                        .ok_or_else(|| {
                            format!("unresolved source reference for link object {object:?}")
                        })?
                }
            } else {
                application_dependencies(
                    records,
                    ledger,
                    statement,
                    &matches,
                    &view,
                    &dependencies,
                )?
            };
            Ok(ResolvedStatement {
                premise: Premise {
                    subjects: premise(&matches, &view, &fields),
                    objects,
                },
                redundant,
                conflicts: Vec::new(),
            })
        })();
        out.push(result);
    }
    let premises = out
        .iter()
        .zip(&selected)
        .map(|(result, fallback)| result.as_ref().map_or(fallback, |r| &r.premise))
        .collect::<Vec<_>>();
    let candidates = conflict_candidates(statements, &premises);
    for (a, b) in candidates {
        if !effects_overlap(&statement_effects[a], &statement_effects[b])
            || [a, b]
                .into_iter()
                .any(|i| out[i].as_ref().is_err_and(|error| error != "empty match"))
        {
            continue;
        }
        if premise_overlap(
            statements[a],
            out[a].as_ref().map_or(&selected[a], |r| &r.premise),
            statements[b],
            out[b].as_ref().map_or(&selected[b], |r| &r.premise),
        ) || (matches!(statements[a], Statement::Assert { .. })
            && matches!(statements[b], Statement::Assert { .. })
            && addresses_overlap(subject(statements[a]), subject(statements[b])))
        {
            for i in [a, b] {
                if out[i].is_err() {
                    out[i] = Ok(ResolvedStatement {
                        premise: selected[i].clone(),
                        redundant: false,
                        conflicts: Vec::new(),
                    });
                }
            }
            out[a]
                .as_mut()
                .expect("successful evaluation")
                .conflicts
                .push(b);
            out[b]
                .as_mut()
                .expect("successful evaluation")
                .conflicts
                .push(a);
        }
    }
    out
}
