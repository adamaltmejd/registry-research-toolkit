//! Curated endpoint decisions share source-link validation and keep losing links as evidence.
use super::{Entity, Relation, pairs, reaches, resolve_inner, successions};
use std::collections::{BTreeMap, BTreeSet};

use reg_frame::{Record, address::Address, statement::Statement};
use reg_readers::curation::LocatedStatement;
use reg_resolve::{Resolved, curation::Premise, provenance};

#[derive(Default)]
pub(crate) struct Curated {
    pub relations: Vec<Relation>,
    pub statements: Vec<crate::emit::StatementRow>,
    pub evidence: Vec<provenance::FinalValue>,
    pub refusals: Vec<(String, String)>,
    pub succession_warnings: Vec<String>,
}

fn relation_kind(relation: &reg_frame::statement::Relation) -> &'static str {
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
    let source = resolve_inner(registers, records, &[], false)?;
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
        let kind = relation_kind(relation);
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
                            locator: reg_frame::Locator {
                                input: statement.path.to_string_lossy().into_owned(),
                                at: format!("line={}", statement.line),
                            },
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
    let mut relations = source_precedence(registers, source.relations, &curated, &mut result);
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
            if left.kind == "continues"
                && right.kind == "continues"
                && matches!(left.subject, Entity::Table(..))
                && left.object == right.object
                && left.at == right.at
            {
                refuse(
                    result,
                    origins,
                    left,
                    "successors of one table on the same day",
                );
                refuse(
                    result,
                    origins,
                    right,
                    "successors of one table on the same day",
                );
            }
        }
    }
}

fn source_precedence(
    registers: &[Resolved],
    source: Vec<Relation>,
    curated: &[Relation],
    result: &mut Curated,
) -> Vec<Relation> {
    let mut relations = Vec::new();
    for rel in source {
        let winners = curated
            .iter()
            .filter(|c| {
                (c.kind == rel.kind && c.subject == rel.subject)
                    || (c.kind == "continues"
                        && rel.kind == "continues"
                        && matches!(c.subject, Entity::Table(..))
                        && c.object == rel.object
                        && c.at == rel.at)
            })
            .collect::<Vec<_>>();
        if winners.is_empty() {
            relations.push(rel);
            continue;
        }
        {
            if rel.kind == "continues" && winners.iter().all(|winner| winner.subject != rel.subject)
            {
                result.succession_warnings.push(super::warning(
                    &rel.locator,
                    "unchosen successor of one table on the same day",
                ));
            }
            let subject = match rel.subject {
                Entity::Register(r) => provenance::Subject::Register(r),
                Entity::Table(r, t) => provenance::Subject::Table(r, t),
                Entity::Variable(r, v) => provenance::Subject::Variable(r, v),
                Entity::Release(..) => unreachable!("release relations refused"),
            };
            for winner in winners {
                let hash = winner.statement.clone().expect("curated");
                result.evidence.push(provenance::FinalValue {
                    subject: subject.clone(), field: format!("relation:{}:{hash}", rel.kind),
                    value: Some(serde_json::json!({"object":entity_name(registers, winner.object),"at":winner.at,"end":winner.end}).to_string()),
                    source_value: Some(serde_json::json!({"object":entity_name(registers, rel.object),"at":rel.at,"note":rel.note,"locator":rel.locator}).to_string()),
                    origin: provenance::Origin::Curated(hash),
                });
            }
        }
    }
    relations
}

fn collect_evidence(result: &mut Curated) {
    // One override cell holds losing edges for this subject/relation/statement.
    // The override table is keyed by subject and field, not by relation endpoint.
    let mut evidence = BTreeMap::<_, (Vec<serde_json::Value>, BTreeSet<String>, String)>::new();
    for value in result.evidence.drain(..) {
        let provenance::Origin::Curated(hash) = value.origin else {
            unreachable!("curated evidence")
        };
        let entry = evidence
            .entry((value.subject, value.field))
            .or_insert_with(|| (Vec::new(), BTreeSet::new(), hash));
        entry.0.push(
            serde_json::from_str(value.source_value.as_deref().expect("source link"))
                .expect("serialized link"),
        );
        if let Some(value) = value.value {
            entry.1.insert(value);
        }
    }
    for ((subject, field), (mut source, winners, hash)) in evidence {
        source.sort_by_key(reg_frame::canonical_line);
        source.dedup();
        result.evidence.push(provenance::FinalValue {
            subject,
            field,
            value: Some(serde_json::to_string(&winners).expect("endpoints serialize")),
            source_value: Some(serde_json::to_string(&source).expect("links serialize")),
            origin: provenance::Origin::Curated(hash),
        });
    }
}

fn validate_graph(
    mut relations: Vec<Relation>,
    curated: &[Relation],
    origins: &BTreeMap<String, &LocatedStatement>,
    result: &mut Curated,
) {
    let mut candidates = super::Relations::new();
    for rel in &relations {
        let locator = rel.locator.clone();
        candidates.insert(
            (rel.kind, rel.subject, rel.object, rel.at.clone()),
            (locator, rel.note.clone()),
        );
    }
    let dropped = super::drop_same_day(&mut candidates);
    result.succession_warnings.extend(
        dropped
            .iter()
            .map(|(locator, reason)| super::warning(locator, reason)),
    );
    relations.retain(|rel| {
        candidates.contains_key(&(rel.kind, rel.subject, rel.object, rel.at.clone()))
    });
    for rel in curated {
        if rel.kind != "continues" {
            continue;
        }
        let mut edges = BTreeMap::<_, Vec<_>>::new();
        for other in &relations {
            if other.kind == "continues"
                && !(other.subject == rel.subject
                    && other.object == rel.object
                    && other.at == rel.at)
            {
                edges.entry(other.subject).or_default().push(other.object);
            }
        }
        if reaches(&edges, rel.object, rel.subject) {
            refuse(result, origins, rel, "continues closes a cycle");
        }
    }
    collect_evidence(result);
    successions(&mut candidates, &mut result.succession_warnings);
    result.relations = relations
        .into_iter()
        .filter(|rel| candidates.contains_key(&(rel.kind, rel.subject, rel.object, rel.at.clone())))
        .collect();
}

fn entity_name(registers: &[Resolved], entity: Entity) -> String {
    let r = &registers[entity.register()];
    let prefix = format!("{}/{}", r.register.provider, r.public);
    match entity {
        Entity::Register(_) => prefix,
        Entity::Variable(_, v) => format!("{prefix}/{}", r.variables[v].slug),
        Entity::Table(_, t) => format!(
            "{prefix}:table:{}",
            r.tables[t].slug.as_deref().unwrap_or_default()
        ),
        Entity::Release(_, x) => format!("{prefix}:release:{}", r.releases[x].release.release),
    }
}

/// Link review pins only source candidates affected by precedence or the selected
/// predecessor/day choice. A new or changed candidate reopens that review.
pub(crate) fn review(
    registers: &[Resolved],
    records: &[Record],
    statement: &LocatedStatement,
    premise: &mut Premise,
) -> Result<(), String> {
    let Statement::Link {
        subject,
        relation,
        object,
        at,
        ..
    } = &statement.statement
    else {
        return Ok(());
    };
    let subjects = endpoints(
        registers,
        &statement.provider,
        &statement.register,
        &subject.to_string(),
        &premise.subjects,
    )?;
    let objects = endpoints(
        registers,
        &statement.provider,
        &statement.register,
        object,
        &premise.objects,
    )?;
    let kind = relation_kind(relation);
    pairs(kind, &subjects, &objects)?;
    let index = super::Index::new(registers);
    let days = validity(at.as_ref());
    let mut candidates = Vec::new();
    for record in records {
        let Record::Link(link) = record else {
            continue;
        };
        if link.kind != kind {
            continue;
        }
        let selected_subject = index
            .entities(&link.subject)
            .is_ok_and(|ends| ends.iter().any(|end| subjects.contains(end)));
        let selected_choice = kind == "continues"
            && subjects.iter().any(|s| matches!(s, Entity::Table(..)))
            && index
                .entities(&link.object)
                .is_ok_and(|ends| ends.iter().any(|end| objects.contains(end)))
            && days.iter().any(|(day, _)| *day == link.at);
        if selected_subject || selected_choice {
            candidates.push(serde_json::to_value(link).map_err(|e| e.to_string())?);
        }
    }
    candidates.sort_by_key(reg_frame::canonical_line);
    if let Some(first) = premise.subjects.first_mut() {
        first
            .fields
            .insert("source_links".into(), serde_json::Value::Array(candidates));
    }
    Ok(())
}
