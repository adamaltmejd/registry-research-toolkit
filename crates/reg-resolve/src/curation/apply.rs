//! Apply source-shaped curation before the ordinary resolver sees the frame.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::{Interval, intersect, merge, next_iso_day, prev_iso_day};
use reg_frame::address::{Address, Comparison, GuardField};
use reg_frame::statement::{AssertFact, BindTarget, Statement};
use reg_frame::{Locator, OPEN_END, OPEN_START, Record, ValueSet};
use serde_json::{Value, json};

use super::{Premise, periods, stated_fields};
use crate::Ledger;

/// A curated winner, at the final frame coordinate, for the existing override table.
#[derive(Debug, Clone)]
pub struct Change {
    pub kind: String,
    pub locator: Locator,
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

fn change(
    value: &Value,
    field: &str,
    winning: &Value,
    losing: &Value,
    period: Option<Interval>,
) -> Change {
    Change {
        kind: text(value, "kind").expect("record kind").into(),
        locator: locator(value),
        provider: text(value, "provider").map(str::to_owned),
        register: text(value, "register").map(str::to_owned),
        table: text(value, "table").map(str::to_owned),
        release: text(value, "release").map(str::to_owned),
        period,
        field: match field {
            "dtype" => "type",
            "column" => "spelling",
            "valueset" => "value_set",
            f => f,
        }
        .into(),
        value: rendered(winning),
        source_value: rendered(losing),
    }
}

fn decode(value: Value) -> Result<Record, String> {
    serde_json::from_value(value).map_err(|e| format!("asserted frame: {e}"))
}

fn subtract(base: &Interval, selected: &[Interval]) -> Vec<Interval> {
    let mut residual = vec![base.clone()];
    for cut in selected {
        residual = residual
            .into_iter()
            .flat_map(|p| {
                let Some(hit) = intersect(&p, cut) else {
                    return vec![p];
                };
                let mut out = Vec::new();
                if p.0 < hit.0 {
                    out.push((p.0, prev_iso_day(&hit.0)));
                }
                if hit.1 < p.1 {
                    out.push((next_iso_day(&hit.1), p.1));
                }
                out
            })
            .collect();
    }
    residual
}

fn scoped(mut value: Value, period: &Interval, original: &Interval) -> Value {
    if period != original {
        value["from"] = json!(period.0);
        value["to"] = json!(period.1);
        if text(&value, "kind") == Some("release") {
            value["grain"] = json!("span");
        }
    }
    value
}

fn table_handle(records: &[Record], ledger: &Ledger, slug: &str) -> Result<String, String> {
    let candidates: BTreeSet<_> = records
        .iter()
        .filter_map(|r| match r {
            Record::Table(t) => Some(t.table.as_str()),
            Record::Occurrence(o) => Some(o.table.as_str()),
            Record::Release(r) => Some(r.table.as_str()),
            _ => None,
        })
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
    priorities: &BTreeMap<Locator, i64>,
    source_records: &[Record],
) -> Result<Applied, String> {
    let mut out = Applied {
        records: records.to_vec(),
        changes: Vec::new(),
        classifications: Vec::new(),
    };
    match statement {
        Statement::Bind {
            source,
            subject,
            target,
            period,
            priority,
            ..
        } => {
            let field = match target {
                BindTarget::Register { .. } => "register",
                BindTarget::Table { .. } => "table",
            };
            let (BindTarget::Register { id } | BindTarget::Table { id }) = target;
            if subject.is_some()
                && !matches!(
                    (subject, target),
                    (Some(Address::Occurrence { .. }), BindTarget::Table { .. })
                )
            {
                return Err(
                    "bind subject must be an occurrence selector with a table target".into(),
                );
            }
            let target_handle = if field == "table" {
                table_handle(records, ledger, id)?
            } else {
                id.clone()
            };
            let mut scope = period.as_ref().map(periods);
            if let Some(Address::Occurrence {
                periods: Some(p), ..
            }) = subject
            {
                scope = Some(match scope {
                    Some(s) => s
                        .iter()
                        .flat_map(|s| {
                            periods(p)
                                .iter()
                                .filter_map(|p| intersect(s, p))
                                .collect::<Vec<_>>()
                        })
                        .collect(),
                    None => periods(p),
                });
            }
            let reviewed = |value: &Value| {
                premise.subjects.iter().any(|p| {
                    (p.locator == locator(value)
                        || value["also"].as_array().is_some_and(|a| {
                            a.iter().any(|l| {
                                serde_json::from_value::<Locator>(l.clone())
                                    .is_ok_and(|l| l == p.locator)
                            })
                        }))
                        && ["provider", "register", "table", "release"]
                            .iter()
                            .all(|field| {
                                p.fields
                                    .get(*field)
                                    .is_none_or(|expected| expected == &value[*field])
                            })
                })
            };
            let mut output = Vec::new();
            let mut moved = Vec::new();
            for record in records {
                let value = tree(record);
                let kind = text(&value, "kind").unwrap_or_default();
                let selected = if let Some(source) = source {
                    matches!(kind, "register" | "table" | "release" | "occurrence")
                        && text(&value, field) == Some(source)
                        && reviewed(&value)
                } else {
                    kind == "occurrence" && reviewed(&value)
                };
                if !selected {
                    output.push(record.clone());
                    continue;
                }
                let original = span(&value, records);
                let spans = scope.as_ref().map_or_else(
                    || vec![original.clone()],
                    |scope| {
                        merge(
                            scope
                                .iter()
                                .filter_map(|s| intersect(&original, s))
                                .collect(),
                        )
                    },
                );
                if spans.is_empty() {
                    output.push(record.clone());
                    continue;
                }
                if (kind == "release" || (kind == "occurrence" && !value["release"].is_null()))
                    && spans != vec![original.clone()]
                {
                    return Err(
                        "bind period may select a release, but cannot split its stated period"
                            .into(),
                    );
                }
                let spans = if matches!(kind, "register" | "table") {
                    if !subtract(&original, &spans).is_empty() {
                        output.push(record.clone());
                    }
                    vec![(
                        spans.first().expect("selected span").0.clone(),
                        spans.last().expect("selected span").1.clone(),
                    )]
                } else {
                    for residual in subtract(&original, &spans) {
                        output.push(decode(scoped(value.clone(), &residual, &original))?);
                    }
                    spans
                };
                for selected in spans {
                    let mut bound = scoped(value.clone(), &selected, &original);
                    bound[field] = json!(target_handle);
                    if kind == "occurrence" && value["release"].is_null() {
                        bound["from"] = json!(selected.0);
                        bound["to"] = json!(selected.1);
                    }
                    if kind == "occurrence" {
                        check_table_coverage(&bound, &selected, records)?;
                    }
                    // A row moved out of its release becomes its own dated documented
                    // occurrence; the original delivery does not describe the new table.
                    if subject.is_some() && kind == "occurrence" && !bound["release"].is_null() {
                        bound["release"] = Value::Null;
                        bound["from"] = json!(selected.0);
                        bound["to"] = json!(selected.1);
                    }
                    for changed in [field, "release", "grain"] {
                        if bound[changed] != value[changed] {
                            out.changes.push(change(
                                &bound,
                                changed,
                                &bound[changed],
                                &value[changed],
                                Some(selected.clone()),
                            ));
                        }
                    }
                    if bound["from"] != value["from"] || bound["to"] != value["to"] {
                        out.changes.push(change(
                            &bound,
                            "period",
                            &json!(selected),
                            &json!(original),
                            Some(selected.clone()),
                        ));
                    }
                    moved.push(decode(bound)?);
                }
            }
            if moved.is_empty() {
                return Err("empty match".into());
            }
            if field == "table"
                && !output
                    .iter()
                    .chain(&moved)
                    .any(|r| matches!(r,Record::Table(t) if t.table == target_handle))
            {
                let row = moved
                    .iter()
                    .find_map(|r| match r {
                        Record::Occurrence(o) => Some(o),
                        _ => None,
                    })
                    .ok_or("table binding states no rows")?;
                let table = json!({"kind":"table","provider":row.provider,"register":row.register,"table":target_handle,"name":id,"description":null,"from":null,"to":null,"cadence":null,"locator":at});
                for field in ["provider", "register", "table", "name"] {
                    out.changes
                        .push(change(&table, field, &table[field], &Value::Null, None));
                }
                output.push(decode(table)?);
            }
            merge_bound(
                &mut output,
                moved,
                *priority,
                priorities,
                source_records,
                &mut out.changes,
            )?;
            if let Some(source) = source {
                rebind_endpoints(
                    &mut output,
                    records,
                    source,
                    field,
                    &target_handle,
                    scope.as_deref(),
                )?;
            }
            out.records = output;
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
                    let mut value = json!({"kind":"occurrence","provider":register.provider,"register":register.register,"table":table,"release":null,"column":if spellings[0] == "<none>" { "" } else { &spellings[0] },"source_key":format!("stmt:{}",statement.identity()),"from":null,"to":null,"label":null,"definition":null,"operational_definition":null,"unit":null,"dtype":null,"length":null,"valueset":null,"population_valuesets":[],"coded":null,"code_system":null,"sensitive":null,"identifier":null,"locator":at,"also":[],"conflicts":[]});
                    for guard in guards {
                        if guard.comparison != Comparison::Equal {
                            return Err(
                                "assert occurrence cannot invent a prefix-guard value".into()
                            );
                        }
                        let field = match guard.field {
                            GuardField::Label => "label",
                            GuardField::Type => "dtype",
                            GuardField::Unit => "unit",
                            GuardField::Key => "source_key",
                            GuardField::Release => "release",
                        };
                        value[field] = json!(guard.value);
                    }
                    fill_fields(&mut value, fields)?;
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
                AssertFact::ValueSet { items }
                | AssertFact::ClassificationVersion { items, .. } => {
                    let set = ValueSet::new(items.clone(), None, at.clone());
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
                        if !records.iter().any(|r|matches!(r,Record::Release(r) if r.table == table && Some(r.release.as_str()) == text(&value,"release"))) { return Err("assert occurrence names an unknown release".into()); }
                        let release_period = span(&value, records);
                        if (scope.is_some() || !value["from"].is_null() || !value["to"].is_null())
                            && p != release_period
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
                                &existing,
                                field,
                                winning,
                                &Value::Null,
                                Some(p.clone()),
                            ));
                        }
                    }
                    let final_period = json!([existing["from"], existing["to"]]);
                    if final_period != previous_period {
                        out.changes.push(change(
                            &existing,
                            "period",
                            &final_period,
                            &previous_period,
                            Some(p.clone()),
                        ));
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
                            &value,
                            field,
                            winning,
                            &Value::Null,
                            Some(p.clone()),
                        ));
                    }
                }
                if !value["from"].is_null() || !value["to"].is_null() {
                    out.changes.push(change(
                        &value,
                        "period",
                        &json!(p),
                        &Value::Null,
                        Some(p.clone()),
                    ));
                }
                out.records.push(decode(value.clone())?);
            }
        }
        _ => {}
    }
    Ok(out)
}

fn rebind_endpoints(
    output: &mut [Record],
    records: &[Record],
    source: &str,
    field: &str,
    target: &str,
    scope: Option<&[Interval]>,
) -> Result<(), String> {
    let source_registers = records
        .iter()
        .filter_map(|r| match r {
            Record::Register(r) if field == "table" || r.register == source => Some(r),
            _ => None,
        })
        .collect::<Vec<_>>();
    let matches_register = |endpoint: &reg_frame::Endpoint| {
        source_registers.iter().any(|r| {
            r.provider == endpoint.provider
                && (endpoint.register.as_deref() == Some(r.register.as_str())
                    || endpoint
                        .register_name
                        .as_ref()
                        .zip(r.name.as_ref())
                        .is_some_and(|(a, b)| {
                            reg_core::fold_identity(a) == reg_core::fold_identity(b)
                        }))
        })
    };
    let update = |endpoint: &mut reg_frame::Endpoint, at: Option<&str>| -> Result<(), String> {
        if !matches_register(endpoint)
            || (field == "table" && endpoint.table.as_deref() != Some(source))
        {
            return Ok(());
        }
        if let Some(scope) = scope {
            let Some(at) = at else {
                return Err("scoped bind has an undated source link or document subject; cannot choose its target coordinate".into());
            };
            if !scope
                .iter()
                .any(|p| p.0.as_str() <= at && at <= p.1.as_str())
            {
                return Ok(());
            }
        }
        if field == "register" {
            endpoint.register = Some(target.into());
            endpoint.register_name = None;
        } else {
            endpoint.table = Some(target.into());
        }
        Ok(())
    };
    for record in output {
        match record {
            Record::Link(link) => {
                update(&mut link.subject, link.at.as_deref())?;
                update(&mut link.object, link.at.as_deref())?;
            }
            Record::Notice(notice) => update(&mut notice.subject, notice.at.as_deref())?,
            Record::Doc(doc) => {
                for subject in &mut doc.subjects {
                    update(&mut subject.endpoint, None)?;
                }
            }
            _ => {}
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
    let view = super::sources(&out.records, ledger);
    let selected = super::select(subject, &view, &BTreeMap::new());
    let mut output = Vec::new();
    let mut attached = false;
    for record in &out.records {
        let Record::Occurrence(o) = record else {
            output.push(record.clone());
            continue;
        };
        let spans = merge(
            selected
                .iter()
                .filter(|m| {
                    view[m.source].locators.contains(&o.locator)
                        || o.also.iter().any(|l| view[m.source].locators.contains(l))
                })
                .flat_map(|m| m.spans.clone())
                .collect(),
        );
        if spans.is_empty() {
            output.push(record.clone());
            continue;
        }
        if o.valueset.is_some() || !o.population_valuesets.is_empty() {
            return Err(
                "assert value set cannot replace a source-stated value set; use set".into(),
            );
        }
        let original = span(&tree(record), &out.records);
        if o.release.is_some() && spans != vec![original.clone()] {
            return Err("assert value set period cannot split a stated release; use set".into());
        }
        for residual in subtract(&original, &spans) {
            output.push(decode(scoped(tree(record), &residual, &original))?);
        }
        for p in spans {
            let mut value = scoped(tree(record), &p, &original);
            value["valueset"] = json!(hash);
            out.changes.push(change(
                &value,
                "value_set",
                &value["valueset"],
                &tree(record)["valueset"],
                Some(p.clone()),
            ));
            if value["from"] != tree(record)["from"] || value["to"] != tree(record)["to"] {
                out.changes.push(change(
                    &value,
                    "period",
                    &json!(p),
                    &json!(original),
                    Some(p.clone()),
                ));
            }
            output.push(decode(value)?);
        }
        attached = true;
    }
    if !attached {
        return Err("assert value set requires an existing occurrence subject".into());
    }
    out.records = output;
    Ok(())
}

// Partition only competing documented occurrences. This leaves unrelated source
// rows untouched and makes source priority a per-period decision, not a hull-wide
// choice that could discard the other source's non-overlapping coverage.
#[allow(clippy::too_many_lines)]
fn partition_overlaps(
    output: &mut Vec<Record>,
    moved: &mut Vec<Record>,
    changes: &mut Vec<Change>,
) -> Result<(), String> {
    let all = output
        .iter()
        .chain(moved.iter())
        .cloned()
        .collect::<Vec<_>>();
    let coordinate = |o: &reg_frame::Occurrence| {
        (
            o.provider.clone(),
            o.register.clone(),
            o.table.clone(),
            o.source_key.clone(),
            reg_core::fold_identity(&o.column),
        )
    };
    let mut groups: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for record in &all {
        if let Record::Occurrence(o) = record {
            groups.entry(coordinate(o)).or_default().push(record);
        }
    }
    let source_key = |value: &Value, at: &Locator| {
        reg_frame::canonical_line(&(
            at,
            &value["provider"],
            &value["register"],
            &value["table"],
            &value["release"],
            value
                .get("source_key")
                .or_else(|| value.get("key"))
                .unwrap_or(&Value::Null),
            &value["from"],
            &value["to"],
        ))
    };
    let mut spans_by_source = BTreeMap::new();
    for source in super::sources(&all, &Ledger::default()) {
        if let Some(period) = source.period {
            let value = serde_json::to_value(&source.fields).expect("source fields serialize");
            for at in source.locators {
                spans_by_source.insert(source_key(&value, &at), period.clone());
            }
        }
    }
    let cached_span = |record: &Record| {
        let value = tree(record);
        spans_by_source
            .get(&source_key(&value, &locator(&value)))
            .cloned()
            .unwrap_or_else(|| span(&value, &all))
    };
    let partition = |records: &[Record],
                     changes: &mut Vec<Change>|
     -> Result<Vec<Record>, String> {
        let mut result = Vec::new();
        for record in records {
            let Record::Occurrence(o) = record else {
                result.push(record.clone());
                continue;
            };
            let original = cached_span(record);
            let rivals = groups.get(&coordinate(o)).into_iter().flatten().copied().filter(|r| matches!(r,Record::Occurrence(other) if other.locator != o.locator || other.from != o.from || other.to != o.to)).filter_map(|r|intersect(&original,&cached_span(r)).map(|p|(r,p))).collect::<Vec<_>>();
            if rivals.is_empty() {
                result.push(record.clone());
                continue;
            }
            let mut edges: BTreeSet<String> = [original.0.clone()].into_iter().collect();
            for (_, p) in &rivals {
                edges.insert(p.0.clone());
                if p.1 < original.1 {
                    edges.insert(next_iso_day(&p.1));
                }
            }
            let edges = edges.into_iter().collect::<Vec<_>>();
            let detach = o.release.is_some() && (edges.len()>1 || rivals.iter().any(|(r,_)|matches!(r,Record::Occurrence(other) if other.release != o.release)));
            for (index, start) in edges.iter().enumerate() {
                let p = (
                    start.clone(),
                    edges
                        .get(index + 1)
                        .map_or_else(|| original.1.clone(), |next| prev_iso_day(next)),
                );
                let mut value = scoped(tree(record), &p, &original);
                // Releases own a whole source period; a priority-split row is a
                // dated curated occurrence while the delivery remains evidence.
                if detach {
                    value["release"] = Value::Null;
                    value["from"] = json!(p.0);
                    value["to"] = json!(p.1);
                }
                // A non-release row with inherited bounds needs explicit coordinates
                // too, so all pieces at the same period reach one merge coordinate.
                if value["release"].is_null() {
                    value["from"] = json!(p.0);
                    value["to"] = json!(p.1);
                }
                if value["from"] != tree(record)["from"] || value["to"] != tree(record)["to"] {
                    changes.push(change(
                        &value,
                        "period",
                        &json!(p),
                        &json!(original),
                        Some(p.clone()),
                    ));
                }
                if detach {
                    changes.push(change(
                        &value,
                        "release",
                        &Value::Null,
                        &json!(o.release),
                        Some(p.clone()),
                    ));
                }
                result.push(decode(value)?);
            }
        }
        Ok(result)
    };
    *output = partition(output, changes)?;
    *moved = partition(moved, changes)?;
    Ok(())
}

// Source priorities apply only while merging source facts. Curated winners never
// enter here as another competing source: the caller rejects overlapping binds.
#[allow(clippy::too_many_lines)]
fn merge_bound(
    output: &mut Vec<Record>,
    moved: Vec<Record>,
    priority: Option<i64>,
    priorities: &BTreeMap<Locator, i64>,
    source_records: &[Record],
    changes: &mut Vec<Change>,
) -> Result<(), String> {
    let mut raw_fields: BTreeMap<Locator, Vec<Value>> = BTreeMap::new();
    for source in source_records {
        let value = tree(source);
        if value["locator"].is_null() {
            continue;
        }
        let mut locators = vec![locator(&value)];
        if let Some(also) = value["also"].as_array() {
            locators.extend(
                also.iter()
                    .filter_map(|l| serde_json::from_value::<Locator>(l.clone()).ok()),
            );
        }
        for at in locators {
            raw_fields.entry(at).or_default().push(value.clone());
        }
    }
    let mut moved = moved;
    partition_overlaps(output, &mut moved, changes)?;
    for record in moved {
        let mut incoming = tree(&record);
        let kind = text(&incoming, "kind").unwrap_or_default().to_owned();
        let existing = output.iter().position(|r| {
            let old = tree(r);
            text(&old, "kind") == Some(kind.as_str())
                && ["provider", "register", "table", "release", "source_key"]
                    .iter()
                    .all(|f| old[*f] == incoming[*f])
                && (kind != "occurrence"
                    || (["from", "to"].iter().all(|f| old[*f] == incoming[*f])
                        && reg_core::fold_identity(text(&old, "column").unwrap_or_default())
                            == reg_core::fold_identity(
                                text(&incoming, "column").unwrap_or_default(),
                            )))
        });
        if let Some(index) = existing {
            let old = tree(&output[index]);
            let rank = |v: &Value, field: &str| {
                let mut locators = vec![locator(v)];
                if let Some(also) = v["also"].as_array() {
                    locators.extend(
                        also.iter()
                            .filter_map(|l| serde_json::from_value::<Locator>(l.clone()).ok()),
                    );
                }
                // A merged row may borrow a missing label from a lower-priority
                // source. Rank each stated field by its actual original holders,
                // rather than lending the row's highest source rank to that label.
                locators
                    .iter()
                    .filter(|at| {
                        raw_fields.get(*at).is_some_and(|sources| {
                            sources.iter().any(|source| source[field] == v[field])
                        })
                    })
                    .map(|at| priorities.get(at).copied().unwrap_or(0))
                    .max()
                    .unwrap_or(0)
            };
            let old_conflicts: BTreeSet<String> =
                serde_json::from_value(old["conflicts"].clone()).unwrap_or_default();
            let incoming_conflicts: BTreeSet<String> =
                serde_json::from_value(incoming["conflicts"].clone()).unwrap_or_default();
            let mut conflicts = old_conflicts
                .union(&incoming_conflicts)
                .cloned()
                .collect::<BTreeSet<_>>();
            for (field, source) in old.as_object().expect("record object") {
                if matches!(field.as_str(), "locator" | "also" | "conflicts") {
                    continue;
                }
                let incoming_rank = rank(&incoming, field);
                let old_rank = rank(&old, field);
                // A silent higher-priority row cannot resolve a disagreement in a
                // field it borrows. A genuinely higher stated value can, including
                // agreement with the former provisional winner.
                if !incoming[field].is_null()
                    && incoming_rank > old_rank
                    && !incoming_conflicts.contains(field)
                {
                    conflicts.remove(field);
                }
                if !source.is_null() && old_rank > incoming_rank && !old_conflicts.contains(field) {
                    conflicts.remove(field);
                }
                if source == &incoming[field] || source.is_null() {
                    continue;
                }
                if !incoming[field].is_null() && incoming_rank == old_rank && kind == "occurrence" {
                    conflicts.insert(field.clone());
                }
                let losing = incoming[field].clone();
                if incoming[field].is_null()
                    || incoming_rank < old_rank
                    || (incoming_rank == old_rank && locator(&old) < locator(&incoming))
                {
                    incoming[field] = source.clone();
                }
                if priority.is_some() && incoming_rank != old_rank && !losing.is_null() {
                    changes.push(change(
                        &incoming,
                        field,
                        &incoming[field],
                        if incoming_rank <= old_rank {
                            &losing
                        } else {
                            source
                        },
                        None,
                    ));
                }
            }
            if kind == "occurrence" {
                incoming["conflicts"] = json!(conflicts);
                let mut locators: Vec<Locator> =
                    serde_json::from_value(incoming["also"].clone()).expect("also locators");
                locators.push(locator(&old));
                locators.extend(
                    serde_json::from_value::<Vec<Locator>>(old["also"].clone())
                        .expect("also locators"),
                );
                locators.sort();
                locators.dedup();
                incoming["also"] = json!(locators);
            }
            output[index] = decode(incoming)?;
        } else {
            output.push(record);
        }
    }
    Ok(())
}
