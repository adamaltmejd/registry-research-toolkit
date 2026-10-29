//! Apply source-shaped curation before the ordinary resolver sees the frame.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::Interval;
use reg_frame::address::{Address, Comparison, GuardField};
use reg_frame::statement::{AssertFact, Statement};
use reg_frame::{Locator, OPEN_END, OPEN_START, Occurrence, Record, ValueSet};
use serde_json::{Value, json};

use super::{Premise, periods, stated_fields};
use crate::Ledger;

/// A curated winner, at the final frame coordinate, for the existing override table.
#[derive(Debug, Clone)]
pub struct Change {
    pub kind: String,
    pub locator: Locator,
    pub identity: Option<crate::OccurrenceIdentity>,
    pub provider: Option<String>,
    pub register: Option<String>,
    pub table: Option<String>,
    pub release: Option<String>,
    pub period: Option<Interval>,
    pub field: String,
    pub value: Option<String>,
    pub source_value: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Classification {
    pub family: String,
    pub version: String,
    pub valueset: String,
    pub locator: Locator,
    pub period: Interval,
    pub name: Option<String>,
    pub family_name: Option<String>,
    pub publisher: Option<String>,
    pub succeeds: Vec<String>,
}

impl Classification {
    /// Effective served family facts, shared by versions and inline assertions.
    #[must_use]
    pub fn family_facts<'a>(classes: &'a [Self], family: &'a str) -> (&'a str, Option<&'a str>) {
        let versions = || classes.iter().filter(|c| c.family == family);
        (
            versions()
                .find_map(|c| c.family_name.as_deref())
                .unwrap_or(family),
            versions().find_map(|c| c.publisher.as_deref()),
        )
    }

    /// Facts emitted in classification tables and curated succession relations.
    #[must_use]
    pub fn facts(&self, classes: &[Self]) -> BTreeMap<String, Value> {
        let (family_name, publisher) = Self::family_facts(classes, &self.family);
        BTreeMap::from([
            ("family".into(), json!(self.family)),
            ("version".into(), json!(self.version)),
            ("valueset".into(), json!(self.valueset)),
            (
                "name".into(),
                json!(self.name.as_deref().unwrap_or(&self.version)),
            ),
            ("family_name".into(), json!(family_name)),
            ("publisher".into(), json!(publisher)),
            ("period".into(), json!(self.period)),
            ("succeeds".into(), json!(self.succeeds)),
        ])
    }
}

/// Shared construction for hand-authored assertions and inventory reader references.
///
/// # Errors
/// The statement is not a classification assertion or has disjoint existence periods.
pub fn classification(statement: &Statement, at: &Locator) -> Result<Classification, String> {
    let Statement::Assert {
        subject,
        fact: AssertFact::ClassificationVersion {
            family, version, ..
        },
        ..
    } = statement
    else {
        return Err("classification construction requires assert classification-version".into());
    };
    let p = match subject {
        Address::Occurrence {
            periods: Some(p), ..
        } => periods(p),
        _ => vec![(OPEN_START.into(), OPEN_END.into())],
    };
    if p.len() != 1 {
        return Err("assert classification version requires one contiguous period".into());
    }
    let set = asserted_value_set(statement, at)
        .ok_or("classification construction requires asserted items")?;
    Ok(Classification {
        family: family.clone(),
        version: version.clone(),
        valueset: set.hash,
        locator: at.clone(),
        period: p[0].clone(),
        name: None,
        family_name: None,
        publisher: None,
        succeeds: Vec::new(),
    })
}

/// Typed curation facts that need the occurrence's resolved variable or subject.
#[derive(Debug, Clone)]
pub enum MetadataValue {
    Sets(BTreeSet<String>),
    Tags(BTreeSet<String>),
    Synonyms(BTreeSet<String>),
    Notice(String),
}

#[derive(Debug, Clone)]
pub struct Metadata {
    pub key: crate::OccurrenceKey,
    pub value: MetadataValue,
}

#[derive(Debug, Default)]
pub struct Applied {
    pub records: Vec<Record>,
    pub changes: Vec<Change>,
    pub classifications: Vec<Classification>,
    pub assignments: BTreeMap<crate::OccurrenceKey, String>,
    pub metadata: Vec<Metadata>,
    /// Complete sentinel declarations for shared value sets, without changing hashes.
    pub sentinels: BTreeMap<String, BTreeSet<String>>,
}

/// Validate a set-wide sentinel declaration without changing the set's identity.
///
/// # Errors
/// A declared code is absent from the addressed value set.
pub fn validate_sentinels(
    hash: &str,
    items: &[reg_frame::Item],
    codes: &BTreeSet<String>,
) -> Result<(), String> {
    if let Some(code) = codes
        .iter()
        .find(|code| !items.iter().any(|item| &item.code == *code))
    {
        return Err(format!(
            "sentinel-code-missing: code {code:?} is not in value set {hash}"
        ));
    }
    Ok(())
}

/// Assign variables against one immutable phase snapshot without copying its facts.
pub struct AssignmentPhase<'a> {
    records: &'a [Record],
    context: crate::OccurrenceContext<'a>,
    locators: BTreeMap<&'a Locator, Vec<usize>>,
}

impl<'a> AssignmentPhase<'a> {
    #[must_use]
    pub fn new(records: &'a [Record]) -> Self {
        let mut locators = BTreeMap::<_, Vec<_>>::new();
        for (index, record) in records.iter().enumerate() {
            if let Record::Occurrence(row) = record {
                for at in std::iter::once(&row.locator).chain(&row.also) {
                    locators.entry(at).or_default().push(index);
                }
            }
        }
        Self {
            records,
            context: crate::OccurrenceContext::new(records),
            locators,
        }
    }

    /// Return assignment/provenance changes only; the source records stay unchanged.
    ///
    /// # Errors
    /// The statement is not a pure variable assignment or its source cannot serialize.
    ///
    /// # Panics
    /// Only if a typed frame record stops serializing to its declared JSON shape.
    pub fn apply(&self, statement: &Statement, premise: &Premise) -> Result<Applied, String> {
        let Statement::Set { fields, .. } = statement else {
            return Err("assignment phase requires set variable".into());
        };
        let Some(variable) = fields
            .variable
            .as_deref()
            .filter(|_| fields.table.is_none())
        else {
            return Err("assignment phase requires set variable without set table".into());
        };
        let candidates = premise
            .subjects
            .iter()
            .flat_map(|p| self.locators.get(&p.locator).into_iter().flatten().copied())
            .collect::<BTreeSet<_>>();
        let mut out = Applied::default();
        for index in candidates {
            let record = &self.records[index];
            let Record::Occurrence(row) = record else {
                unreachable!("locator index contains occurrences")
            };
            let value = tree(record);
            if premise.subjects.iter().any(|p| {
                matches_occurrence(
                    &p.fields,
                    std::slice::from_ref(&p.locator),
                    row,
                    &value,
                    &self.context,
                )
            }) {
                let period = span(&value, self.records, &self.context)?;
                assign(
                    &mut out.assignments,
                    &mut out.changes,
                    &self.context,
                    row,
                    &value,
                    variable,
                    period,
                )?;
            }
        }
        Ok(out)
    }
}

/// Define an asserted code list before applying occurrences that consume its hash.
/// Subject attachment and provenance remain part of the statement's application.
#[must_use]
pub fn asserted_value_set(statement: &Statement, at: &Locator) -> Option<ValueSet> {
    match statement {
        Statement::Assert {
            fact: AssertFact::ValueSet { items } | AssertFact::ClassificationVersion { items, .. },
            ..
        } => Some(ValueSet::new(items.clone(), None, at.clone())),
        _ => None,
    }
}

fn tree(record: &Record) -> Value {
    serde_json::to_value(record).expect("frame record serializes")
}

fn text<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

fn locator(value: &Value) -> Locator {
    serde_json::from_value(value["locator"].clone()).expect("frame locator")
}

/// Render a field for stored curation provenance.
#[must_use]
pub fn rendered(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        v => Some(v.to_string()),
    }
}

fn span(
    value: &Value,
    records: &[Record],
    context: &crate::OccurrenceContext<'_>,
) -> Result<Interval, String> {
    if let Record::Occurrence(row) = decode(value.clone())? {
        return Ok(context
            .period(&row)
            .unwrap_or_else(|| (OPEN_START.into(), OPEN_END.into())));
    }
    let register = records.iter().find_map(|r| match r {
        Record::Register(r)
            if value["provider"] == r.provider && value["register"] == r.register =>
        {
            Some(r)
        }
        _ => None,
    });
    Ok((
        text(value, "from")
            .or_else(|| register.and_then(|r| r.from.as_deref()))
            .unwrap_or(OPEN_START)
            .into(),
        text(value, "to")
            .or_else(|| register.and_then(|r| r.to.as_deref()))
            .unwrap_or(OPEN_END)
            .into(),
    ))
}

fn matches_occurrence(
    fields: &BTreeMap<String, Value>,
    locators: &[Locator],
    row: &Occurrence,
    value: &Value,
    context: &crate::OccurrenceContext<'_>,
) -> bool {
    crate::OccurrenceIdentity::from_fields(fields)
        .is_some_and(|identity| identity == context.identity(row))
        && (locators.contains(&row.locator) || row.also.iter().any(|at| locators.contains(at)))
        && [("label", "label"), ("type", "dtype"), ("unit", "unit")]
            .iter()
            .all(|(field, raw)| {
                fields
                    .get(*field)
                    .is_none_or(|expected| expected == &value[*raw])
            })
}

fn change(
    context: &crate::OccurrenceContext<'_>,
    value: &Value,
    field: &str,
    winning: &Value,
    losing: &Value,
    period: Option<Interval>,
) -> Result<Change, String> {
    let kind = text(value, "kind").expect("record kind");
    let identity = if kind == "occurrence" {
        let Record::Occurrence(row) = decode(value.clone())? else {
            unreachable!()
        };
        Some(context.identity(&row))
    } else {
        None
    };
    Ok(Change {
        identity,
        kind: if kind == "value_set" {
            "valueset"
        } else {
            kind
        }
        .into(),
        locator: locator(value),
        provider: text(value, "provider").map(str::to_owned),
        register: text(value, "register").map(str::to_owned),
        table: text(value, "table").map(str::to_owned),
        release: text(value, "release").map(str::to_owned),
        period: if kind == "value_set" { None } else { period },
        field: match field {
            "dtype" => "type",
            "column" => "spelling",
            "valueset" => "value_set",
            f => f,
        }
        .into(),
        value: rendered(winning),
        source_value: rendered(losing),
    })
}

fn assign(
    assignments: &mut BTreeMap<crate::OccurrenceKey, String>,
    changes: &mut Vec<Change>,
    context: &crate::OccurrenceContext<'_>,
    row: &Occurrence,
    value: &Value,
    variable: &str,
    period: Interval,
) -> Result<(), String> {
    assignments.insert(
        crate::OccurrenceKey::new(context.identity(row), &row.locator),
        variable.into(),
    );
    changes.push(change(
        context,
        value,
        "variable",
        &json!(variable),
        &Value::Null,
        Some(period),
    )?);
    Ok(())
}

fn decode(value: Value) -> Result<Record, String> {
    serde_json::from_value(value).map_err(|e| format!("asserted frame: {e}"))
}

fn table_handles(records: &[Record]) -> impl Iterator<Item = &str> {
    records.iter().filter_map(|record| match record {
        Record::Table(table) => Some(table.table.as_str()),
        Record::Occurrence(occurrence) => Some(occurrence.table.as_str()),
        Record::Release(release) => Some(release.table.as_str()),
        _ => None,
    })
}

pub(super) fn table_handle(
    records: &[Record],
    ledger: &Ledger,
    slug: &str,
) -> Result<String, String> {
    let candidates: BTreeSet<_> = table_handles(records)
        .filter(|handle| {
            if handle.is_empty() {
                let register = records.iter().find_map(|r| match r {
                    Record::Register(r) => Some(r),
                    _ => None,
                });
                ledger
                    .public
                    .as_deref()
                    .or_else(|| register.map(|r| r.register.as_str()))
                    == Some(slug)
            } else {
                ledger
                    .tables
                    .get(*handle)
                    .cloned()
                    .unwrap_or_else(|| crate::slugify(handle))
                    == slug
            }
        })
        .map(str::to_owned)
        .collect();
    match candidates.len() {
        0 => Ok(slug.into()),
        1 => Ok(candidates.into_iter().next().expect("one handle")),
        _ => Err(format!("ambiguous table slug {slug:?}")),
    }
}

fn frame_guard_field(field: GuardField) -> &'static str {
    match field {
        GuardField::Label => "label",
        GuardField::Type => "dtype",
        GuardField::Unit => "unit",
        GuardField::Key => "source_key",
        GuardField::Release => "release",
    }
}

fn assert_fields(
    fields: &reg_frame::statement::AssertFields,
    supported: &[&str],
) -> Result<BTreeMap<String, Value>, String> {
    let fields = stated_fields(fields);
    for field in fields.keys() {
        if !supported.contains(&field.as_str()) {
            return Err(format!(
                "assert {field:?} is not a source-shaped field for this entity"
            ));
        }
    }
    Ok(fields)
}

/// Apply one already-reviewed statement. This stage does no IO and never accepts a
/// premise; the caller supplies the lock-checked premise and statement location.
///
/// # Errors
/// Unsupported source shapes, missing parent coordinates or conflicting source facts.
///
/// # Panics
/// Only if a typed frame record stops serializing to its declared JSON shape.
#[allow(clippy::too_many_lines)]
pub fn apply(
    records: &[Record],
    ledger: &Ledger,
    statement: &Statement,
    at: &Locator,
    premise: &Premise,
) -> Result<Applied, String> {
    let context = crate::OccurrenceContext::new(records);
    let mut out = Applied {
        records: records.to_vec(),
        changes: Vec::new(),
        classifications: Vec::new(),
        assignments: BTreeMap::new(),
        metadata: Vec::new(),
        sentinels: BTreeMap::new(),
    };
    match statement {
        Statement::Set { fields, .. } if fields.table.is_some() || fields.variable.is_some() => {
            if stated_fields(fields)
                .keys()
                .any(|field| !matches!(field.as_str(), "table" | "variable"))
            {
                return Err(
                    "set table/variable with other fields requires separate statements".into(),
                );
            }
            let target_handle = fields
                .table
                .as_ref()
                .map(|target| table_handle(records, ledger, target))
                .transpose()?;
            if let Some(target) = &target_handle
                && !table_handles(records).any(|handle| handle == target)
            {
                return Err("set table requires an existing table; assert the table first".into());
            }
            for record in &mut out.records {
                let Record::Occurrence(row) = record else {
                    continue;
                };
                let value = tree(&Record::Occurrence(row.clone()));
                let selected = premise.subjects.iter().any(|p| {
                    matches_occurrence(
                        &p.fields,
                        std::slice::from_ref(&p.locator),
                        row,
                        &value,
                        &context,
                    )
                });
                if !selected {
                    continue;
                }
                let period = span(&value, records, &context)?;
                if let Some(target_handle) = &target_handle
                    && row.table != *target_handle
                {
                    if let Some(handle) = &row.release {
                        let release = records
                            .iter()
                            .find_map(|r| match r {
                                Record::Release(r)
                                    if r.provider == row.provider
                                        && r.register == row.register
                                        && r.table == row.table
                                        && r.release == *handle =>
                                {
                                    Some(r)
                                }
                                _ => None,
                            })
                            .ok_or("set table occurrence names an unknown release")?;
                        if crate::placed(release).is_none() {
                            return Err(
                                "set table cannot detach an unplaced release; set its period first"
                                    .into(),
                            );
                        }
                        if !row.population_valuesets.is_empty() {
                            return Err("set table cannot detach population-specific value sets from their release".into());
                        }
                        row.release = None;
                    }
                    row.table.clone_from(target_handle);
                    row.from = Some(period.0.clone());
                    row.to = Some(period.1.clone());
                    let moved = tree(&Record::Occurrence(row.clone()));
                    check_table_coverage(&moved, &period, records)?;
                    let identity = context.identity(row);
                    if records.iter().any(|record| {
                        matches!(record, Record::Occurrence(existing)
                            if existing.locator == row.locator
                                && existing.label == row.label
                                && context.identity(existing) == identity)
                    }) {
                        return Err(format!(
                            "set table would collide with an existing occurrence in destination table {target_handle:?}"
                        ));
                    }
                    for field in ["table", "release"] {
                        if moved[field] != value[field] {
                            out.changes.push(change(
                                &context,
                                &moved,
                                field,
                                &moved[field],
                                &value[field],
                                Some(period.clone()),
                            )?);
                        }
                    }
                    if moved["from"] != value["from"] || moved["to"] != value["to"] {
                        out.changes.push(change(
                            &context,
                            &moved,
                            "period",
                            &json!(period),
                            &json!(span(&value, records, &context)?),
                            Some(period.clone()),
                        )?);
                    }
                }
                if let Some(variable) = &fields.variable {
                    let value = tree(&Record::Occurrence(row.clone()));
                    assign(
                        &mut out.assignments,
                        &mut out.changes,
                        &context,
                        row,
                        &value,
                        variable,
                        period,
                    )?;
                }
            }
        }
        Statement::Set { fields, .. } => {
            apply_fields(records, fields, premise, &context, &mut out)?;
        }
        Statement::Assert { subject, fact, .. } => {
            let register = records
                .iter()
                .find_map(|r| match r {
                    Record::Register(r) => Some(r),
                    _ => None,
                })
                .ok_or("assert requires an existing register")?;
            let (table, scope) = match subject {
                Address::Table(t) | Address::Release { table: t, .. } => {
                    (table_handle(records, ledger, t)?, None)
                }
                Address::Occurrence {
                    table, periods: p, ..
                } => (
                    table_handle(records, ledger, table)?,
                    p.as_ref().map(periods),
                ),
                Address::Variable(_) => {
                    return Err(
                        "assert requires a source table, release or occurrence address".into(),
                    );
                }
            };
            let mut value = match fact {
                AssertFact::Table { fields } => {
                    if !matches!(subject, Address::Table(_)) {
                        return Err("assert table requires a table address".into());
                    }
                    let fields =
                        assert_fields(fields, &["name", "description", "coverage", "period"])?;
                    let mut value = json!({"kind":"table","provider":register.provider,"register":register.register,"table":table,"name":null,"description":null,"from":null,"to":null,"cadence":null,"locator":at});
                    fill_fields(&mut value, fields)?;
                    value
                }
                AssertFact::Occurrence { fields } => {
                    let Address::Occurrence {
                        spellings, guards, ..
                    } = subject
                    else {
                        return Err("assert occurrence requires an occurrence address".into());
                    };
                    if spellings.len() != 1 {
                        return Err("assert occurrence requires one spelling".into());
                    }
                    let fields = assert_fields(
                        fields,
                        &[
                            "label",
                            "definition",
                            "operational_definition",
                            "unit",
                            "type",
                            "length",
                            "spelling",
                            "period",
                            "value_set",
                            "coded",
                            "sensitive",
                            "identifier",
                        ],
                    )?;
                    let mut value = json!({"kind":"occurrence","provider":register.provider,"register":register.register,"table":table,"release":null,"column":if spellings[0] == "<none>" { "" } else { &spellings[0] },"source_key":format!("stmt:{}",statement.scoped_identity(&register.provider, &register.register)),"from":null,"to":null,"label":null,"definition":null,"operational_definition":null,"unit":null,"dtype":null,"length":null,"valueset":null,"population_valuesets":[],"coded":null,"code_system":null,"sensitive":null,"identifier":null,"locator":at,"also":[],"conflicts":[]});
                    for guard in guards {
                        if guard.comparison != Comparison::Equal {
                            return Err(
                                "assert occurrence cannot invent a prefix-guard value".into()
                            );
                        }
                        let field = frame_guard_field(guard.field);
                        value[field] = json!(guard.value);
                    }
                    fill_fields(&mut value, fields)?;
                    let column = text(&value, "column").unwrap_or_default();
                    let spelling = if spellings[0] == "<none>" {
                        ""
                    } else {
                        &spellings[0]
                    };
                    if reg_core::fold_identity(column) != spelling {
                        return Err("assert occurrence spelling differs from its subject".into());
                    }
                    for guard in guards {
                        let field = frame_guard_field(guard.field);
                        if text(&value, field) != Some(guard.value.as_str()) {
                            return Err(
                                "assert occurrence fact contradicts its subject guard".into()
                            );
                        }
                    }
                    if let Some(scope) = &scope
                        && !value["from"].is_null()
                        && !reg_core::gaps(&[span(&value, records, &context)?], scope.clone())
                            .is_empty()
                    {
                        return Err(
                            "assert occurrence period lies outside its subject scope".into()
                        );
                    }
                    if let Some(hash) = text(&value, "valueset")
                        && !records
                            .iter()
                            .any(|r| matches!(r, Record::ValueSet(v) if v.hash == hash))
                    {
                        return Err("assert occurrence names an unknown value set; assert the code list first".into());
                    }
                    value
                }
                AssertFact::Release { name, period } => {
                    if !table_handles(records).any(|handle| handle == table) {
                        return Err(
                            "assert release requires an existing table; assert the table first"
                                .into(),
                        );
                    }
                    let Address::Release { handle, .. } = subject else {
                        return Err("assert release requires a release address".into());
                    };
                    if records.iter().any(|r| matches!(r,Record::Release(r) if r.table == table && r.release == *handle)) { return Err("assert release cannot replace a source release; use set".into()); }
                    let p = periods(period);
                    if p.len() != 1 {
                        return Err("assert release requires one contiguous period".into());
                    }
                    json!({"kind":"release","provider":register.provider,"register":register.register,"table":table,"release":handle,"name":name,"description":null,"from":p[0].0,"to":p[0].1,"grain":"span","edition":"unstated","qualifier":null,"populations":[],"locator":at})
                }
                AssertFact::ValueSet { .. } | AssertFact::ClassificationVersion { .. } => {
                    let set = asserted_value_set(statement, at).expect("code-list assertion");
                    if let AssertFact::ClassificationVersion {
                        family, version, ..
                    } = fact
                    {
                        let class = classification(statement, at)?;
                        let p = [class.period.clone()];
                        out.classifications.push(class);
                        for (field, winning) in [
                            ("family", json!(family)),
                            ("version", json!(version)),
                            ("value_set", json!(set.hash)),
                            ("period", json!(p[0])),
                        ] {
                            out.changes.push(Change {
                                identity: None,
                                kind: "class_version".into(),
                                locator: at.clone(),
                                provider: None,
                                register: None,
                                table: None,
                                release: None,
                                period: Some(p[0].clone()),
                                field: field.into(),
                                value: rendered(&winning),
                                source_value: None,
                            });
                        }
                    } else {
                        attach_valueset(&mut out, subject, ledger, &set.hash)?;
                    }
                    tree(&Record::ValueSet(set))
                }
            };
            let own_scope = if text(&value, "kind") == Some("occurrence")
                && value["from"].is_null()
                && value["to"].is_null()
            {
                scope.clone()
            } else {
                None
            };
            let spans = match own_scope {
                Some(spans) => spans,
                None => vec![span(&value, records, &context)?],
            };
            for p in spans {
                if text(&value, "kind") == Some("occurrence") {
                    if records.iter().any(|record| {
                        let Record::Occurrence(row) = record else {
                            return false;
                        };
                        context.period(row).is_some_and(|existing| {
                            !reg_core::overlap(&[existing], std::slice::from_ref(&p)).is_empty()
                        }) && premise.subjects.iter().any(|source| {
                            matches_occurrence(
                                &source.fields,
                                std::slice::from_ref(&source.locator),
                                row,
                                &tree(record),
                                &context,
                            )
                        })
                    }) {
                        return Err(
                            "assert occurrence cannot replace a source occurrence; use set".into(),
                        );
                    }
                    if !table_handles(records).any(|handle| handle == table) {
                        return Err(
                            "assert occurrence requires an existing table; assert the table first"
                                .into(),
                        );
                    }
                    if value["release"].is_null() {
                        value["from"] = json!(p.0);
                        value["to"] = json!(p.1);
                    } else {
                        let release = records
                            .iter()
                            .find_map(|r| match r {
                                Record::Release(r)
                                    if value["provider"] == r.provider
                                        && value["register"] == r.register
                                        && r.table == table
                                        && Some(r.release.as_str()) == text(&value, "release") =>
                                {
                                    Some(r)
                                }
                                _ => None,
                            })
                            .ok_or("assert occurrence names an unknown release")?;
                        let release_period = crate::placed(release);
                        if (scope.is_some() || !value["from"].is_null() || !value["to"].is_null())
                            && (release_period.as_ref() != Some(&p)
                                || text(&value, "from").is_some_and(|from| {
                                    release_period.as_ref().is_none_or(|p| from != p.0)
                                })
                                || text(&value, "to").is_some_and(|to| {
                                    release_period.as_ref().is_none_or(|p| to != p.1)
                                }))
                        {
                            return Err(
                                "assert occurrence period differs from its source release".into()
                            );
                        }
                        value["from"] = Value::Null;
                        value["to"] = Value::Null;
                    }
                    check_table_coverage(&value, &p, records)?;
                }
                if text(&value, "kind") == Some("table")
                    && let Some(index) = out.records.iter().position(|r| {
                        let existing = tree(r);
                        text(&existing, "kind") == Some("table")
                            && ["provider", "register", "table"]
                                .iter()
                                .all(|f| existing[*f] == value[*f])
                    })
                {
                    let mut existing = tree(&out.records[index]);
                    let previous_period = json!([existing["from"], existing["to"]]);
                    for (field, winning) in value.as_object().expect("table object") {
                        if winning.is_null()
                            || matches!(
                                field.as_str(),
                                "kind" | "locator" | "provider" | "register" | "table"
                            )
                        {
                            continue;
                        }
                        if existing[field] == *winning {
                            continue;
                        }
                        if !existing[field].is_null() {
                            return Err(format!(
                                "assert table cannot replace source {field}; use set"
                            ));
                        }
                        existing[field] = winning.clone();
                        if !matches!(field.as_str(), "from" | "to") {
                            out.changes.push(change(
                                &context,
                                &existing,
                                field,
                                winning,
                                &Value::Null,
                                Some(p.clone()),
                            )?);
                        }
                    }
                    let final_period = json!([existing["from"], existing["to"]]);
                    if final_period != previous_period {
                        out.changes.push(change(
                            &context,
                            &existing,
                            "period",
                            &final_period,
                            &previous_period,
                            Some(p.clone()),
                        )?);
                    }
                    out.records[index] = decode(existing)?;
                    continue;
                }
                // A new source-shaped fact owns every stated field, including its
                // coordinates. Nulls state no value and therefore have no winner.
                for (field, winning) in value.as_object().expect("record object") {
                    if !winning.is_null()
                        && !matches!(
                            field.as_str(),
                            "kind" | "locator" | "also" | "conflicts" | "from" | "to"
                        )
                    {
                        out.changes.push(change(
                            &context,
                            &value,
                            field,
                            winning,
                            &Value::Null,
                            Some(p.clone()),
                        )?);
                    }
                }
                if !value["from"].is_null() || !value["to"].is_null() {
                    out.changes.push(change(
                        &context,
                        &value,
                        "period",
                        &json!(p),
                        &Value::Null,
                        Some(p.clone()),
                    )?);
                }
                let record = decode(value.clone())?;
                if !out.records.contains(&record) {
                    out.records.push(record);
                }
            }
        }
        _ => {}
    }
    Ok(out)
}

fn selected_record(
    record: &Record,
    value: &Value,
    premise: &Premise,
    context: &crate::OccurrenceContext<'_>,
) -> bool {
    premise.subjects.iter().any(|source| {
        if let Record::Occurrence(row) = record {
            return matches_occurrence(
                &source.fields,
                std::slice::from_ref(&source.locator),
                row,
                value,
                context,
            );
        }
        source.locator == locator(value)
            && ["kind", "provider", "register", "table", "release"]
                .iter()
                .all(|field| source.fields.get(*field).unwrap_or(&Value::Null) == &value[*field])
    })
}

fn source_period(record: &Record, context: &crate::OccurrenceContext<'_>) -> Option<Interval> {
    match record {
        Record::Occurrence(row) => context.period(row),
        Record::Release(row) => crate::placed(row),
        Record::Table(row) => Some((
            row.from.clone().unwrap_or_else(|| OPEN_START.into()),
            row.to.clone().unwrap_or_else(|| OPEN_END.into()),
        )),
        _ => None,
    }
}

fn target_field(field: &str) -> &str {
    match field {
        "type" => "dtype",
        "spelling" => "column",
        "value_set" => "valueset",
        field => field,
    }
}

#[allow(clippy::too_many_lines)]
fn apply_fields(
    records: &[Record],
    fields: &reg_frame::statement::SetFields,
    premise: &Premise,
    context: &crate::OccurrenceContext<'_>,
    out: &mut Applied,
) -> Result<(), String> {
    let metadata_fields = fields;
    let fields = stated_fields(fields);
    if (fields.contains_key("coverage") || fields.contains_key("period")) && fields.len() != 1 {
        return Err("set coverage/period with other fields requires separate statements".into());
    }
    let mut selected = false;
    for (index, record) in records.iter().enumerate() {
        let before = tree(record);
        if !selected_record(record, &before, premise, context) {
            continue;
        }
        selected = true;
        let kind = text(&before, "kind").expect("record kind");
        for field in fields.keys() {
            let supported = match kind {
                "occurrence" => matches!(
                    field.as_str(),
                    "label"
                        | "definition"
                        | "description"
                        | "operational_definition"
                        | "unit"
                        | "value_set"
                        | "coded"
                        | "spelling"
                        | "identifier"
                        | "sensitive"
                        | "format"
                        | "type"
                        | "length"
                        | "period"
                        | "sets"
                        | "tags"
                        | "synonyms"
                        | "sentinels"
                        | "role"
                        | "classification"
                        | "notice"
                ),
                "table" => matches!(
                    field.as_str(),
                    "description" | "coverage" | "name" | "role" | "unit_type"
                ),
                "release" => matches!(field.as_str(), "description" | "period" | "name"),
                _ => false,
            };
            if !supported {
                return Err(format!("set {field} is not supported on {kind}"));
            }
        }
        let mut after = before.clone();
        for (field, winning) in &fields {
            match field.as_str() {
                "sets" | "tags" => {
                    if winning
                        .as_array()
                        .expect("typed membership list")
                        .iter()
                        .any(|value| {
                            !reg_core::is_slug(value.as_str().expect("typed membership slug"))
                        })
                    {
                        return Err(format!("set {field} requires catalog slugs"));
                    }
                }
                "role" => {
                    crate::role_value(kind, winning.as_str().expect("typed role"))?;
                    after["role"] = winning.clone();
                }
                "unit_type" => {
                    if !reg_core::is_slug(winning.as_str().expect("typed unit type")) {
                        return Err("set unit_type requires a catalog slug".into());
                    }
                    if kind == "table" {
                        after["unit_type"] = winning.clone();
                    }
                }
                "name" if winning.as_str().is_none_or(|v| v.trim().is_empty()) => {
                    return Err("set name requires a nonblank entity name".into());
                }
                "sentinels" => {
                    let Record::Occurrence(occurrence) = record else {
                        unreachable!("sentinels apply only to occurrences")
                    };
                    let hashes: BTreeSet<_> = occurrence
                        .valueset
                        .iter()
                        .chain(occurrence.population_valuesets.iter().map(|p| &p.valueset))
                        .collect();
                    if hashes.is_empty() {
                        return Err("set sentinels requires an occurrence with a value set".into());
                    }
                    let codes = metadata_fields
                        .sentinels
                        .as_ref()
                        .expect("typed sentinel list")
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    for hash in hashes {
                        let items = premise
                            .objects
                            .iter()
                            .find_map(|p| {
                                (p.fields.get("hash").and_then(Value::as_str) == Some(hash))
                                    .then(|| p.fields.get("items").and_then(Value::as_array))
                                    .flatten()
                            })
                            .ok_or_else(|| {
                                format!("set sentinels has no reviewed value set {hash:?}")
                            })?;
                        let items: Vec<reg_frame::Item> =
                            serde_json::from_value(Value::Array(items.clone()))
                                .map_err(|e| e.to_string())?;
                        validate_sentinels(hash, &items, &codes)?;
                        out.sentinels.insert(hash.clone(), codes.clone());
                    }
                }
                "synonyms" | "notice" => {}
                "description" if kind == "occurrence" => {}
                "period" | "coverage" => {
                    let period = winning
                        .as_str()
                        .expect("typed period")
                        .parse::<reg_frame::address::PeriodSet>()?;
                    let spans = periods(&period);
                    let [span] = &spans[..] else {
                        return Err(format!("set {field} requires one contiguous period"));
                    };
                    if source_period(record, context).as_ref() == Some(span) {
                        continue;
                    }
                    if field == "period" {
                        check_table_coverage(&before, span, records)?;
                    }
                    if kind == "occurrence" && !before["release"].is_null() {
                        if before["population_valuesets"]
                            .as_array()
                            .is_some_and(|v| !v.is_empty())
                        {
                            return Err("set period cannot detach population-specific value sets from their release".into());
                        }
                        after["release"] = Value::Null;
                    }
                    after["from"] = json!(span.0);
                    after["to"] = json!(span.1);
                    if kind == "release" {
                        after["grain"] = json!("span");
                    }
                }
                "spelling"
                    if winning
                        .as_str()
                        .is_none_or(|value| value.trim().is_empty() || value == "<none>") =>
                {
                    return Err("set spelling requires a nonblank delivered column spelling".into());
                }
                "value_set" => {
                    let hash = winning.as_str().expect("typed content hash");
                    if !records
                        .iter()
                        .any(|r| matches!(r, Record::ValueSet(v) if v.hash == hash))
                    {
                        return Err(
                            "set value_set names an unknown value set; assert the code list first"
                                .into(),
                        );
                    }
                    after["valueset"] = winning.clone();
                }
                _ => after[target_field(field)] = winning.clone(),
            }
        }
        if fields.get("coded") == Some(&Value::Bool(true))
            && after["valueset"].is_null()
            && after["code_system"].is_null()
            && after["population_valuesets"]
                .as_array()
                .is_none_or(Vec::is_empty)
        {
            return Err("set coded=true requires a known value set or source code system".into());
        }
        if let Some(conflicts) = after.get_mut("conflicts").and_then(Value::as_array_mut) {
            conflicts.retain(|field| {
                !matches!(field.as_str(), Some("type" | "dtype")) || !fields.contains_key("type")
            });
            conflicts
                .retain(|field| field.as_str() != Some("length") || !fields.contains_key("length"));
        }
        let after_record = decode(after.clone())?;
        let period = if matches!(record, Record::Occurrence(_) | Record::Release(_)) {
            source_period(&after_record, context)
        } else {
            Some(span(&after, records, context)?)
        };
        let first_change = out.changes.len();
        for (field, winning) in &fields {
            let (winning, losing) = if field == "coverage" {
                (
                    json!([after["from"], after["to"]]),
                    json!([before["from"], before["to"]]),
                )
            } else if field == "period" {
                (
                    json!(period),
                    source_period(record, context).map_or(Value::Null, |p| json!(p)),
                )
            } else {
                (winning.clone(), before[target_field(field)].clone())
            };
            let resolves_conflict = matches!(field.as_str(), "type" | "length")
                && before["conflicts"].as_array().is_some_and(|conflicts| {
                    conflicts.iter().any(|conflict| {
                        conflict.as_str().is_some_and(|conflict| {
                            conflict == field || conflict == target_field(field)
                        })
                    })
                });
            if winning != losing || resolves_conflict {
                out.changes.push(change(
                    context,
                    &after,
                    field,
                    &winning,
                    &losing,
                    period.clone(),
                )?);
            }
        }
        if before["release"] != after["release"] {
            out.changes.push(change(
                context,
                &after,
                "release",
                &after["release"],
                &before["release"],
                period.clone(),
            )?);
        }
        if let Record::Occurrence(row) = record {
            let identity = context.identity(row);
            let key = crate::OccurrenceKey::new(identity.clone(), &row.locator);
            if let Some(sets) = &metadata_fields.sets {
                out.metadata.push(Metadata {
                    key: key.clone(),
                    value: MetadataValue::Sets(sets.iter().cloned().collect()),
                });
            }
            if let Some(tags) = &metadata_fields.tags {
                out.metadata.push(Metadata {
                    key: key.clone(),
                    value: MetadataValue::Tags(tags.iter().cloned().collect()),
                });
            }
            if let Some(synonyms) = &metadata_fields.synonyms {
                out.metadata.push(Metadata {
                    key: key.clone(),
                    value: MetadataValue::Synonyms(synonyms.iter().cloned().collect()),
                });
            }
            if let Some(notice) = &metadata_fields.notice {
                out.metadata.push(Metadata {
                    key,
                    value: MetadataValue::Notice(notice.clone()),
                });
            }
            for claim in &mut out.changes[first_change..] {
                claim.identity = Some(identity.clone());
            }
        }
        out.records[index] = after_record;
    }
    if !selected {
        return Err("set has no selected source-shaped entity".into());
    }
    Ok(())
}

/// Carry identity assignments and field provenance through a phase's coordinate
/// changes. Source values remain the evidence reviewed before application.
///
/// # Errors
/// A field change collapses distinct source occurrences onto one occurrence key.
///
/// # Panics
/// The caller must preserve frame length and order when merging field changes.
pub fn rekey(
    before: &[Record],
    after: &[Record],
    assignments: &mut BTreeMap<crate::OccurrenceKey, String>,
    claims: &mut [crate::provenance::Claim],
    metadata: &mut [Metadata],
) -> Result<(), Locator> {
    assert_eq!(
        before.len(),
        after.len(),
        "field phase preserves record order"
    );
    let old_context = crate::OccurrenceContext::new(before);
    let new_context = crate::OccurrenceContext::new(after);
    let releases: BTreeMap<_, _> = after
        .iter()
        .filter_map(|record| match record {
            Record::Release(release) => Some((
                (
                    Some(release.provider.as_str()),
                    Some(release.register.as_str()),
                    Some(release.table.as_str()),
                    Some(release.release.as_str()),
                    &release.locator,
                ),
                crate::placed(release),
            )),
            _ => None,
        })
        .collect();
    let mut changed = BTreeMap::new();
    let mut owners = BTreeMap::new();
    let mut destinations = BTreeMap::new();
    for (before, after) in before.iter().zip(after) {
        let (Record::Occurrence(before), Record::Occurrence(after)) = (before, after) else {
            continue;
        };
        let old = old_context.identity(before);
        let new = new_context.identity(after);
        for at in std::iter::once(&before.locator).chain(&before.also) {
            let old_key = crate::OccurrenceKey::new(old.clone(), at);
            let new_key = crate::OccurrenceKey::new(new.clone(), at);
            if destinations
                .insert(old_key.clone(), new_key.clone())
                .is_some_and(|previous| previous != new_key)
            {
                return Err(at.clone());
            }
            if let Some(previous) = owners.insert(new_key, old_key.clone())
                && previous != old_key
            {
                return Err(at.clone());
            }
            if old != new {
                changed.insert(old_key, new.clone());
            }
        }
    }
    *assignments = std::mem::take(assignments)
        .into_iter()
        .map(|(mut key, value)| {
            if let Some(identity) = changed.get(&key) {
                key.identity = identity.clone();
            }
            (key, value)
        })
        .collect();
    for value in metadata {
        if let Some(identity) = changed.get(&value.key) {
            value.key.identity = identity.clone();
        }
    }
    for claim in claims {
        if claim.kind == "release"
            && let Some(period) = releases.get(&(
                claim.provider.as_deref(),
                claim.register.as_deref(),
                claim.table.as_deref(),
                claim.release.as_deref(),
                &claim.locator,
            ))
        {
            claim.period.clone_from(period);
        }
        let Some(identity) = &claim.identity else {
            continue;
        };
        let key = crate::OccurrenceKey::new(identity.clone(), &claim.locator);
        if let Some(identity) = changed.get(&key) {
            claim.identity = Some(identity.clone());
            claim.table = Some(identity.table.clone());
            claim.release.clone_from(&identity.release);
            claim.period.clone_from(&identity.period);
        }
    }
    Ok(())
}

fn check_table_coverage(value: &Value, p: &Interval, records: &[Record]) -> Result<(), String> {
    let register = records.iter().find_map(|r| match r {
        Record::Register(r)
            if value["provider"] == r.provider && value["register"] == r.register =>
        {
            Some(r)
        }
        _ => None,
    });
    let table = records.iter().find_map(|r| match r {
        Record::Table(t)
            if value["provider"] == t.provider
                && value["register"] == t.register
                && value["table"] == t.table =>
        {
            Some(t)
        }
        _ => None,
    });
    let from = table
        .and_then(|t| t.from.as_ref())
        .or_else(|| register.and_then(|r| r.from.as_ref()));
    let to = table
        .and_then(|t| t.to.as_ref())
        .or_else(|| register.and_then(|r| r.to.as_ref()));
    if from.is_some_and(|from| p.0 < *from) || to.is_some_and(|to| p.1 > *to) {
        return Err("curated occurrence exceeds target table coverage; use set coverage".into());
    }
    Ok(())
}

fn fill_fields(value: &mut Value, fields: BTreeMap<String, Value>) -> Result<(), String> {
    for (field, v) in fields {
        if matches!(field.as_str(), "coverage" | "period") {
            let set = v
                .as_str()
                .ok_or("period must be a string")?
                .parse::<reg_frame::address::PeriodSet>()?;
            let p = periods(&set);
            if p.len() != 1 {
                return Err("assert fact fields require one contiguous period".into());
            }
            value["from"] = json!(p[0].0);
            value["to"] = json!(p[0].1);
        } else {
            let target = match field.as_str() {
                "type" => "dtype",
                "spelling" => "column",
                "value_set" => "valueset",
                name => name,
            };
            value[target] = v;
        }
    }
    Ok(())
}

fn attach_valueset(
    out: &mut Applied,
    subject: &Address,
    ledger: &Ledger,
    hash: &str,
) -> Result<(), String> {
    let context = crate::OccurrenceContext::new(&out.records);
    let view = super::sources(&out.records, ledger);
    let selected = super::select(subject, &view, &BTreeMap::new());
    let mut output = Vec::new();
    let mut attached = false;
    for record in &out.records {
        let Record::Occurrence(o) = record else {
            output.push(record.clone());
            continue;
        };
        let value = tree(record);
        let original = span(&value, &out.records, &context)?;
        let matches = selected
            .iter()
            .filter(|m| {
                let source = &view[m.source];
                matches_occurrence(&source.fields, &source.locators, o, &value, &context)
            })
            .collect::<Vec<_>>();
        if matches.is_empty() {
            output.push(record.clone());
            continue;
        }
        let spans = matches
            .iter()
            .flat_map(|m| m.spans.iter().cloned())
            .collect();
        if !reg_core::gaps(std::slice::from_ref(&original), spans).is_empty() {
            return Err(format!(
                "selector period cuts occurrence {}; select whole occurrences",
                o.column
            ));
        }
        if o.valueset.is_some() || !o.population_valuesets.is_empty() {
            return Err(
                "assert value set cannot replace a source-stated value set; use set".into(),
            );
        }
        let mut value = value;
        value["valueset"] = json!(hash);
        out.changes.push(change(
            &context,
            &value,
            "value_set",
            &value["valueset"],
            &Value::Null,
            Some(original),
        )?);
        output.push(decode(value)?);
        attached = true;
    }
    if !attached {
        return Err("assert value set requires an existing occurrence subject".into());
    }
    out.records = output;
    Ok(())
}
