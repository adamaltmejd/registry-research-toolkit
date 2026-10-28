//! Curated relation rows share the link stage's endpoint and graph constraints.
use super::{Entity, Relation, pairs, reaches, resolve};
use std::collections::{BTreeMap, BTreeSet};

use reg_frame::{Record, address::Address, statement::Statement};
use reg_readers::curation::LocatedStatement;
use reg_resolve::{Resolved, curation::Premise};

#[derive(Default)]
pub(crate) struct Curated {
    pub relations: Vec<Relation>,
    pub statements: Vec<crate::emit::StatementRow>,
    pub refusals: Vec<(String, String)>,
}

fn relation_kind(relation: reg_frame::statement::Relation) -> &'static str {
    match relation {
        reg_frame::statement::Relation::Continues => "continues",
        reg_frame::statement::Relation::DerivedFrom => "derived_from",
        reg_frame::statement::Relation::Equivalent => "equivalent",
        reg_frame::statement::Relation::LabelOf => "label_of",
    }
}

fn endpoints(
    registers: &[Resolved],
    provider: &str,
    register: &str,
    address: &str,
    premise: &[reg_resolve::curation::SourcePremise],
) -> Result<Vec<Entity>, String> {
    if let Ok(fqid) = address.parse::<reg_core::Fqid>() {
        let (provider, register, variable) = match fqid {
            reg_core::Fqid::Register { provider, register } => (provider, register, None),
            reg_core::Fqid::Variable {
                provider,
                register,
                variable,
            } => (provider, register, Some(variable)),
            _ => return Err("link object needs a resolved register or variable".into()),
        };
        let (ri, resolved) = registers
            .iter()
            .enumerate()
            .find(|(_, r)| r.register.provider == provider && r.public == register)
            .ok_or("unresolved catalog endpoint")?;
        return match variable {
            None => Ok(vec![Entity::Register(ri)]),
            Some(variable) => resolved
                .variables
                .iter()
                .position(|v| v.slug == variable)
                .map(|v| vec![Entity::Variable(ri, v)])
                .ok_or_else(|| "unresolved variable endpoint".into()),
        };
    }
    let (ri, resolved) = registers
        .iter()
        .enumerate()
        .find(|(_, r)| r.register.provider == provider && r.register.register == register)
        .ok_or("unresolved local register")?;
    match address.parse::<Address>()? {
        Address::Variable(variable) => resolved
            .variables
            .iter()
            .position(|v| v.slug == variable)
            .map(|v| vec![Entity::Variable(ri, v)])
            .ok_or_else(|| "unresolved variable endpoint".into()),
        Address::Table(slug) => {
            let table = resolved
                .tables
                .iter()
                .find(|t| t.slug.as_deref().unwrap_or(&resolved.public) == slug)
                .ok_or("unresolved table endpoint")?;
            super::Index::new(registers).entities(&reg_frame::Endpoint {
                kind: "table".into(),
                provider: provider.into(),
                register: Some(register.into()),
                register_name: None,
                table: Some(table.table.table.clone()),
                key: None,
            })
        }
        Address::Release { .. } => Err("release-kind curated link".into()),
        Address::Occurrence { .. } => {
            let mut variables = BTreeSet::new();
            for p in premise {
                let text = |field| p.fields.get(field).and_then(serde_json::Value::as_str);
                for o in &resolved.occurrences {
                    let row = &o.occurrence;
                    if (row.locator == p.locator || row.also.contains(&p.locator))
                        && text("table") == Some(row.table.as_str())
                        && text("spelling") == Some(row.column.as_str())
                        && text("key") == Some(row.source_key.as_str())
                        && text("release") == row.release.as_deref()
                        && text("from") == row.from.as_deref()
                        && text("to") == row.to.as_deref()
                    {
                        let variable = o
                            .variable
                            .ok_or("link endpoint occurrence has no variable")?;
                        variables.insert(Entity::Variable(ri, variable));
                    }
                }
            }
            if variables.is_empty() {
                return Err("unresolved occurrence endpoint".into());
            }
            Ok(variables.into_iter().collect())
        }
    }
}

fn validity(at: Option<&reg_frame::address::PeriodSet>) -> Vec<(Option<String>, Option<String>)> {
    let Some(at) = at else {
        return vec![(None, None)];
    };
    let intervals = at
        .to_string()
        .split(',')
        .map(|token| {
            if let Some(end) = token.strip_prefix("..") {
                (
                    reg_frame::OPEN_START.into(),
                    end.parse::<reg_core::Period>()
                        .expect("typed period")
                        .iso_bounds()
                        .1,
                )
            } else if let Some(start) = token.strip_suffix("..") {
                (
                    start
                        .parse::<reg_core::Period>()
                        .expect("typed period")
                        .iso_bounds()
                        .0,
                    reg_frame::OPEN_END.into(),
                )
            } else {
                token
                    .parse::<reg_core::Period>()
                    .expect("typed period")
                    .iso_bounds()
            }
        })
        .collect();
    reg_core::merge(intervals)
        .into_iter()
        .map(|(start, end)| (Some(start), Some(end)))
        .collect()
}

/// Validate both endpoints, curated conflicts and the combined succession graph before
/// locks are written. Refused statements have no effect; dev callers rerun without them.
pub(crate) fn curate(
    root: &std::path::Path,
    registers: &[Resolved],
    records: &[Record],
    statements: &[(LocatedStatement, Premise)],
) -> Result<Curated, String> {
    let source = resolve(registers, records, &[])?;
    let mut result = Curated::default();
    let mut curated = Vec::new();
    let mut origins = BTreeMap::new();
    for (statement, premise) in statements {
        let Statement::Link {
            subject,
            relation,
            object,
            at,
            evidence: _,
            caveat,
        } = &statement.statement
        else {
            continue;
        };
        let hash = statement
            .statement
            .scoped_identity(&statement.provider, &statement.register);
        let kind = relation_kind(*relation);
        let matched = endpoints(
            registers,
            &statement.provider,
            &statement.register,
            &subject.to_string(),
            &premise.subjects,
        )
        .and_then(|subjects| {
            let objects = endpoints(
                registers,
                &statement.provider,
                &statement.register,
                object,
                &premise.objects,
            )?;
            pairs(kind, &subjects, &objects)
        });
        origins.insert(hash.clone(), statement);
        match matched {
            Err(reason) => result.refusals.push((
                hash.clone(),
                format!(
                    "{}:{}: curated-link: {reason}",
                    statement.path.display(),
                    statement.line
                ),
            )),
            Ok(pairs) => {
                for (subject, object) in pairs {
                    for (at, end) in validity(at.as_ref()) {
                        curated.push(Relation {
                            kind,
                            subject,
                            object,
                            at,
                            end,
                            note: caveat.clone(),
                            statement: Some(hash.clone()),
                        });
                    }
                }
                result
                    .statements
                    .push(crate::curation::statement_row(root, statement)?);
            }
        }
    }
    check_conflicts(&curated, &origins, &mut result);
    let mut relations = source.relations;
    relations.extend(curated.iter().cloned());
    validate_graph(relations, &curated, &origins, &mut result);
    result.refusals.sort();
    result.refusals.dedup();
    Ok(result)
}

fn refuse(
    result: &mut Curated,
    origins: &BTreeMap<String, &LocatedStatement>,
    rel: &Relation,
    reason: &str,
) {
    let hash = rel.statement.as_ref().expect("curated relation");
    let statement = origins[hash];
    result.refusals.push((
        hash.clone(),
        format!(
            "{}:{}: curated-link: {reason}",
            statement.path.display(),
            statement.line
        ),
    ));
}

fn check_conflicts(
    curated: &[Relation],
    origins: &BTreeMap<String, &LocatedStatement>,
    result: &mut Curated,
) {
    for (i, left) in curated.iter().enumerate() {
        for right in &curated[i + 1..] {
            let overlap = match (&left.at, &left.end, &right.at, &right.end) {
                (Some(a), Some(b), Some(c), Some(d)) => a <= d && c <= b,
                _ => true,
            };
            if left.kind == right.kind
                && left.subject == right.subject
                && left.object == right.object
                && overlap
            {
                refuse(
                    result,
                    origins,
                    left,
                    "overlapping curated relations for one subject",
                );
                refuse(
                    result,
                    origins,
                    right,
                    "overlapping curated relations for one subject",
                );
            }
        }
    }
}

fn validate_graph(
    relations: Vec<Relation>,
    curated: &[Relation],
    origins: &BTreeMap<String, &LocatedStatement>,
    result: &mut Curated,
) {
    for rel in curated.iter().filter(|rel| rel.kind == "continues") {
        let mut edges = BTreeMap::<_, Vec<_>>::new();
        for other in relations.iter().filter(|other| other.kind == "continues") {
            if matches!(rel.subject, Entity::Table(..))
                && other.object == rel.object
                && other.at == rel.at
                && other.subject != rel.subject
            {
                refuse(
                    result,
                    origins,
                    rel,
                    "successors of one table on the same day",
                );
            }
            if other.subject != rel.subject || other.object != rel.object {
                edges.entry(other.subject).or_default().push(other.object);
            }
        }
        if reaches(&edges, rel.object, rel.subject) {
            refuse(result, origins, rel, "continues closes a cycle");
        }
    }
    // An exact source edge gains curated provenance without changing topology.
    let mut distinct = BTreeMap::new();
    for rel in relations {
        distinct.insert((rel.kind, rel.subject, rel.object, rel.at.clone()), rel);
    }
    result.relations = distinct.into_values().collect();
}
