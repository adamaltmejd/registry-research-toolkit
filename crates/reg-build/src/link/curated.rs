//! Curated relation rows share the link stage's endpoint and graph constraints.
use super::{Break, Entity, Relation, pairs, reaches, resolve};
use std::collections::{BTreeMap, BTreeSet};

use reg_frame::{Record, address::Address, statement::Statement};
use reg_readers::curation::LocatedStatement;
use reg_resolve::{Resolved, curation::Premise};

#[derive(Default)]
pub(crate) struct Curated {
    pub relations: Vec<Relation>,
    /// Every published series break, the source's included (`breaks`).
    pub breaks: Vec<Break>,
    pub statements: Vec<crate::emit::StatementRow>,
    pub refusals: Vec<(String, String)>,
    pub external: Vec<crate::external::Relation>,
}

fn relation_kind(relation: reg_frame::statement::Relation) -> &'static str {
    match relation {
        reg_frame::statement::Relation::Continues => "continues",
        reg_frame::statement::Relation::DerivedFrom => "derived_from",
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
                table: Some(table.table.table.clone()),
                ..reg_frame::Endpoint::new("table", provider, Some(register))
            })
        }
        Address::Set(_) => {
            Err("sets are presentation only and cannot be relation endpoints".into())
        }
        Address::Release { table, handle } => {
            let t = resolved
                .tables
                .iter()
                .position(|t| t.slug.as_deref().unwrap_or(&resolved.public) == table)
                .ok_or("unresolved table endpoint")?;
            resolved
                .releases
                .iter()
                .position(|r| r.table == t && r.release.release == handle)
                .map(|x| vec![Entity::Release(ri, x)])
                .ok_or_else(|| "unresolved release endpoint".into())
        }
        Address::TableVariable { .. } => {
            Err("a <table>: var: address is not a link endpoint".into())
        }
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
    reg_core::merge(at.intervals())
        .into_iter()
        .map(|(start, end)| (Some(start), Some(end)))
        .collect()
}

/// Validate both endpoints, curated conflicts and the combined succession graph before
/// locks are written. Refused statements have no effect; dev callers rerun without them.
#[allow(clippy::too_many_lines)]
pub(crate) fn curate(
    root: &std::path::Path,
    registers: &[Resolved],
    records: &[Record],
    statements: &[(LocatedStatement, Premise)],
    base: Option<&crate::external::Context>,
) -> Result<Curated, String> {
    let source = resolve(registers, records, &[])?;
    let mut result = Curated::default();
    let mut curated = Vec::new();
    let mut origins = BTreeMap::new();
    let mut dropped = BTreeSet::new();
    for (statement, premise) in statements {
        if let Statement::Set {
            subject, fields, ..
        } = &statement.statement
            && let Some(stated) = &fields.series_break
        {
            let hash = statement
                .statement
                .scoped_identity(&statement.provider, &statement.register);
            let at = stated
                .at
                .as_ref()
                .and_then(|at| at.intervals().into_iter().next())
                .map(|(start, _)| start);
            let placed = endpoints(
                registers,
                &statement.provider,
                &statement.register,
                &subject.to_string(),
                &premise.subjects,
            )
            .and_then(|subjects| {
                let named = replaced(registers, statement, &subjects, &source.breaks, stated)?;
                Ok((subjects, named))
            });
            match placed {
                Ok((subjects, named)) => {
                    dropped.extend(named);
                    result
                        .breaks
                        .extend(subjects.into_iter().map(|subject| Break {
                            subject,
                            at: at.clone(),
                            text: Some(stated.text.trim().to_owned()),
                            statement: Some(hash.clone()),
                        }));
                    result
                        .statements
                        .push(crate::curation::statement_row(root, statement)?);
                }
                Err(reason) => result.refusals.push((
                    hash,
                    format!(
                        "{}:{}: curated-series-break: {reason}",
                        statement.path.display(),
                        statement.line
                    ),
                )),
            }
            continue;
        }
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
        origins.insert(hash.clone(), statement);
        let kind = relation_kind(*relation);
        if endpoints(
            registers,
            &statement.provider,
            &statement.register,
            object,
            &premise.objects,
        )
        .is_err()
            && let Some(base) = base
            && let Some(object_kind) = base.kind(object)?
        {
            let external = external_link(registers, statement, premise, kind);
            match external {
                Ok(subjects) => {
                    for (subject_kind, subject_pid) in subjects {
                        for (start, end) in validity(at.as_ref()) {
                            result.external.push(crate::external::Relation {
                                subject_kind: subject_kind.clone(),
                                subject_pid: subject_pid.clone(),
                                object_kind: object_kind.clone(),
                                object_pid: object.clone(),
                                start,
                                end,
                                statement: hash.clone(),
                                caveat: caveat.clone(),
                            });
                        }
                    }
                    result
                        .statements
                        .push(crate::curation::statement_row(root, statement)?);
                }
                Err(reason) => result.refusals.push((
                    hash,
                    format!(
                        "{}:{}: curated-link: {reason}",
                        statement.path.display(),
                        statement.line
                    ),
                )),
            }
            continue;
        }
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
    result.breaks = breaks(
        registers,
        source.breaks,
        std::mem::take(&mut result.breaks),
        &dropped,
    );
    check_conflicts(&curated, &origins, &mut result);
    let mut relations = source.relations;
    relations.extend(curated.iter().cloned());
    validate_graph(relations, &curated, &origins, &mut result);
    result.refusals.sort();
    result.refusals.dedup();
    Ok(result)
}

/// Whether a break on `on` is one on `subject`: the subject itself or, for a table, one of
/// its releases.
fn covers(registers: &[Resolved], on: Entity, subject: Entity) -> bool {
    on == subject
        || matches!((on, subject),
            (Entity::Table(r, t), Entity::Release(rr, x))
                if r == rr && registers[r].releases[x].table == t)
}

/// The source breaks a curated break's `replaces` names. Each entry must name at least
/// one, so a source change that drops or rewords it refuses the statement.
fn replaced(
    registers: &[Resolved],
    statement: &LocatedStatement,
    subjects: &[Entity],
    source: &[Break],
    stated: &reg_frame::statement::SeriesBreak,
) -> Result<Vec<Break>, String> {
    let mut named = Vec::new();
    for entry in &stated.replaces {
        let on = match &entry.on {
            Some(on) => endpoints(registers, &statement.provider, &statement.register, on, &[])?,
            None => subjects.to_vec(),
        };
        let text = entry.text.trim();
        let found: Vec<Break> = source
            .iter()
            .filter(|s| {
                s.text.as_deref() == Some(text)
                    && on.iter().any(|&o| covers(registers, o, s.subject))
            })
            .cloned()
            .collect();
        if found.is_empty() {
            return Err(format!(
                "replaces names no source break: {:?} on {}",
                text,
                entry.on.as_deref().unwrap_or("the subject")
            ));
        }
        named.extend(found);
    }
    Ok(named)
}

/// The published breaks: the source's and the curated ones, distinct and sorted. A dated
/// curated break replaces each source break of the same text on its subject or, for a
/// table, on one of its releases: it dates what the source left undated. It also
/// replaces the source breaks its `replaces` names (`dropped`).
fn breaks(
    registers: &[Resolved],
    source: Vec<Break>,
    curated: Vec<Break>,
    dropped: &BTreeSet<Break>,
) -> Vec<Break> {
    let replaces = |c: &Break, s: &Break| {
        c.at.is_some() && c.text == s.text && covers(registers, c.subject, s.subject)
    };
    let kept: Vec<Break> = source
        .into_iter()
        .filter(|s| !dropped.contains(s) && !curated.iter().any(|c| replaces(c, s)))
        .collect();
    kept.into_iter()
        .chain(curated)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn external_link(
    registers: &[Resolved],
    statement: &LocatedStatement,
    premise: &Premise,
    kind: &str,
) -> Result<Vec<(String, String)>, String> {
    if kind != "derived_from" {
        return Err("only derived_from may target the immutable reference catalog".into());
    }
    let Statement::Link { subject, .. } = &statement.statement else {
        unreachable!("link statement")
    };
    endpoints(
        registers,
        &statement.provider,
        &statement.register,
        &subject.to_string(),
        &premise.subjects,
    )?
    .into_iter()
    .map(|subject| match subject {
        Entity::Release(..) => Err("release-kind derived_from".into()),
        _ => Ok((subject.kind().into(), subject.pid(registers))),
    })
    .collect()
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

fn overlap(
    left: (&Option<String>, &Option<String>),
    right: (&Option<String>, &Option<String>),
) -> bool {
    match (left, right) {
        ((Some(a), Some(b)), (Some(c), Some(d))) => a <= d && c <= b,
        _ => true,
    }
}

fn check_conflicts(
    curated: &[Relation],
    origins: &BTreeMap<String, &LocatedStatement>,
    result: &mut Curated,
) {
    for (i, left) in curated.iter().enumerate() {
        for right in &curated[i + 1..] {
            if left.kind == right.kind
                && left.subject == right.subject
                && left.object == right.object
                && overlap((&left.at, &left.end), (&right.at, &right.end))
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
    let external = result.external.clone();
    for (i, left) in external.iter().enumerate() {
        for right in &external[i + 1..] {
            if left.subject_kind == right.subject_kind
                && left.subject_pid == right.subject_pid
                && left.object_kind == right.object_kind
                && left.object_pid == right.object_pid
                && overlap((&left.start, &left.end), (&right.start, &right.end))
            {
                for relation in [left, right] {
                    let statement = origins[relation.statement.as_str()];
                    result.refusals.push((
                        relation.statement.clone(),
                        format!(
                            "{}:{}: curated-link: overlapping curated relations for one subject",
                            statement.path.display(),
                            statement.line
                        ),
                    ));
                }
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
