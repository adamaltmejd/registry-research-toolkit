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
}

#[derive(Debug)]
pub struct Applied {
    pub records: Vec<Record>,
    pub changes: Vec<Change>,
    pub classifications: Vec<Classification>,
    pub assignments: BTreeMap<crate::OccurrenceKey, String>,
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

fn rendered(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        v => Some(v.to_string()),
    }
}

fn span(value: &Value, records: &[Record]) -> Interval {
    if text(value, "kind") == Some("occurrence") {
        let parent = if let Some(release) = text(value, "release") {
            records.iter().find_map(|r| {
                let v = tree(r);
                (text(&v, "kind") == Some("release")
                    && v["provider"] == value["provider"]
                    && v["register"] == value["register"]
                    && v["table"] == value["table"]
                    && text(&v, "release") == Some(release))
                .then_some(v)
            })
        } else {
            records.iter().find_map(|r| {
                let v = tree(r);
                (text(&v, "kind") == Some("table")
                    && v["provider"] == value["provider"]
                    && v["register"] == value["register"]
                    && v["table"] == value["table"])
                    .then_some(v)
            })
        };
        if let Some(parent) = parent {
            let mut merged = parent;
            for field in ["from", "to"] {
                if !value[field].is_null() {
                    merged[field] = value[field].clone();
                }
            }
            return span(&merged, records);
        }
    }
    let register = records.iter().find_map(|r| match r {
        Record::Register(r)
            if value["provider"] == r.provider && value["register"] == r.register =>
        {
            Some(r)
        }
        _ => None,
    });
    (
        text(value, "from")
            .or_else(|| register.and_then(|r| r.from.as_deref()))
            .unwrap_or(OPEN_START)
            .into(),
        text(value, "to")
            .or_else(|| register.and_then(|r| r.to.as_deref()))
            .unwrap_or(OPEN_END)
            .into(),
    )
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
    fields: &reg_frame::statement::SetFields,
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
    };
    match statement {
        Statement::Set { fields, .. } if fields.table.is_some() || fields.variable.is_some() => {
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
                let period = span(&value, records);
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
                        if release.from.is_none() || release.to.is_none() {
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
                            &json!(span(&value, records)),
                            Some(period.clone()),
                        )?);
                    }
                }
                if let Some(variable) = &fields.variable {
                    out.assignments.insert(
                        crate::OccurrenceKey::new(context.identity(row), &row.locator),
                        variable.clone(),
                    );
                    let value = tree(&Record::Occurrence(row.clone()));
                    out.changes.push(change(
                        &context,
                        &value,
                        "variable",
                        &json!(variable),
                        &Value::Null,
                        Some(period),
                    )?);
                }
            }
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
                        && !reg_core::gaps(&[span(&value, records)], scope.clone()).is_empty()
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
                    if !records.iter().any(|r| match r {
                        Record::Table(t) => {
                            t.provider == register.provider
                                && t.register == register.register
                                && t.table == table
                        }
                        Record::Occurrence(o) => {
                            o.provider == register.provider
                                && o.register == register.register
                                && o.table == table
                        }
                        _ => false,
                    }) {
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
                        let p = scope
                            .clone()
                            .unwrap_or_else(|| vec![(OPEN_START.into(), OPEN_END.into())]);
                        if p.len() != 1 {
                            return Err(
                                "assert classification version requires one contiguous period"
                                    .into(),
                            );
                        }
                        out.classifications.push(Classification {
                            family: family.clone(),
                            version: version.clone(),
                            valueset: set.hash.clone(),
                            locator: at.clone(),
                            period: p[0].clone(),
                        });
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
            let spans = own_scope.unwrap_or_else(|| vec![span(&value, records)]);
            for p in spans {
                if text(&value, "kind") == Some("occurrence") {
                    if !records.iter().any(|r| match r {
                        Record::Table(t) => t.table == table,
                        Record::Occurrence(o) => o.table == table,
                        _ => false,
                    }) {
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
                        let release_period = release
                            .from
                            .as_ref()
                            .zip(release.to.as_ref())
                            .map(|(from, to)| (from.clone(), to.clone()));
                        if (scope.is_some() || !value["from"].is_null() || !value["to"].is_null())
                            && release_period.as_ref() != Some(&p)
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
        let original = span(&value, &out.records);
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
