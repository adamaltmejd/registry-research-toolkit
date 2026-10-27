//! Premise lock IO and located diagnostics. Selector semantics live in reg-resolve.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use reg_frame::Record;
use reg_readers::curation::LocatedStatement;
use reg_resolve::curation::Premise;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Lock {
    version: u32,
    statements: BTreeMap<String, Premise>,
}

fn location(statement: &LocatedStatement) -> String {
    format!("{}:{}", statement.path.display(), statement.line)
}

fn belongs(record: &Record, provider: &str, register: &str) -> bool {
    let coordinates = match record {
        Record::Register(r) => (&r.provider, &r.register),
        Record::Table(r) => (&r.provider, &r.register),
        Record::Release(r) => (&r.provider, &r.register),
        Record::Occurrence(r) => (&r.provider, &r.register),
        // Value-set assertions and source links may depend on shared source facts.
        Record::ValueSet(_) | Record::Link(_) | Record::Notice(_) => return true,
        Record::Doc(_) => return false,
    };
    coordinates.0 == provider && coordinates.1 == register
}

/// Catalog references resolve through each register's ledger, then snapshot source
/// facts. Classes/providers without source entities cannot silently acquire a premise.
fn references(
    root: &Path,
    records: &[Record],
    statements: &[&LocatedStatement],
    classifications: &[reg_resolve::curation::apply::Classification],
) -> Result<BTreeMap<String, Vec<reg_resolve::curation::SourcePremise>>, String> {
    let mut references = BTreeMap::new();
    let objects = statements
        .iter()
        .filter_map(|s| match &s.statement {
            reg_frame::statement::Statement::Link { object, .. } => Some(object),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for object in objects {
        if let Ok(reg_core::Fqid::ClassVersion { family, version }) = object.parse() {
            if let Some(class) = classifications
                .iter()
                .find(|c| c.family == family && c.version == version)
            {
                let fields = BTreeMap::from([
                    ("family".into(), serde_json::json!(class.family)),
                    ("version".into(), serde_json::json!(class.version)),
                    ("period".into(), serde_json::json!(class.period)),
                    (
                        "items".into(),
                        records
                            .iter()
                            .find_map(|r| match r {
                                Record::ValueSet(v) if v.hash == class.valueset => {
                                    Some(serde_json::json!(v.items))
                                }
                                _ => None,
                            })
                            .unwrap_or(serde_json::Value::Null),
                    ),
                ]);
                references.insert(
                    object.clone(),
                    vec![reg_resolve::curation::SourcePremise {
                        locator: class.locator.clone(),
                        fields,
                    }],
                );
            }
            continue;
        }
        let (provider, register, variable) = match object.parse::<reg_core::Fqid>() {
            Ok(reg_core::Fqid::Register { provider, register }) => (provider, register, None),
            Ok(reg_core::Fqid::Variable {
                provider,
                register,
                variable,
            }) => (provider, register, Some(variable)),
            _ => continue,
        };
        let mut matches = Vec::new();
        for source in records {
            let Record::Register(source) = source else {
                continue;
            };
            if source.provider != provider {
                continue;
            }
            let path = super::ledger_path(root, &provider, &source.register);
            let ledger: reg_resolve::Ledger = if path.is_file() {
                super::toml_file(&path)?
            } else {
                reg_resolve::Ledger::default()
            };
            if ledger.public.as_deref().unwrap_or(&source.register) != register {
                continue;
            }
            let records = records
                .iter()
                .filter(|r| belongs(r, &provider, &source.register))
                .cloned()
                .collect::<Vec<_>>();
            matches.push(reg_resolve::curation::reference(
                &records,
                &ledger,
                variable.as_deref(),
            ));
        }
        if let [Ok(premise)] = &matches[..] {
            references.insert(object.clone(), premise.clone());
        }
    }
    Ok(references)
}

/// Field-level old/new values, including additions and removals. Array indexes are
/// stable because the resolver sorts source premises by locator and canonical fields.
fn changes(old: &serde_json::Value, new: &serde_json::Value, path: &str, out: &mut Vec<String>) {
    if old == new {
        return;
    }
    match (old, new) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            for key in a.keys().chain(b.keys()).collect::<BTreeSet<_>>() {
                changes(
                    a.get(key).unwrap_or(&serde_json::Value::Null),
                    b.get(key).unwrap_or(&serde_json::Value::Null),
                    &format!("{path}/{key}"),
                    out,
                );
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            for i in 0..a.len().max(b.len()) {
                changes(
                    a.get(i).unwrap_or(&serde_json::Value::Null),
                    b.get(i).unwrap_or(&serde_json::Value::Null),
                    &format!("{path}/{i}"),
                    out,
                );
            }
        }
        _ => out.push(format!("{path}: {old} -> {new}")),
    }
}

/// The accepted frame and its curated provenance. No reader output is mutated.
#[derive(Default)]
pub(super) struct Applied {
    pub records: Vec<Record>,
    pub findings: Vec<String>,
    pub statements: Vec<crate::emit::StatementRow>,
    pub overrides: Vec<crate::emit::CuratedValue>,
    pub classifications: Vec<reg_resolve::curation::apply::Classification>,
}

fn phase(statement: &reg_frame::statement::Statement) -> u8 {
    use reg_frame::statement::{BindTarget, Statement};
    match statement {
        Statement::Bind {
            target: BindTarget::Register { .. },
            ..
        } => 0,
        Statement::Bind { .. } => 1,
        Statement::Assert { .. } => 2,
        _ => 3,
    }
}

fn provider_of(record: &Record) -> Option<&str> {
    match record {
        Record::Register(r) => Some(&r.provider),
        Record::Table(r) => Some(&r.provider),
        Record::Release(r) => Some(&r.provider),
        Record::Occurrence(r) => Some(&r.provider),
        _ => None,
    }
}

fn previous(path: &Path, record: bool) -> Result<BTreeMap<String, Premise>, String> {
    if record || !path.try_exists().map_err(|e| e.to_string())? {
        return Ok(BTreeMap::new());
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let lock: Lock = serde_json::from_str(&text)
        .map_err(|e| format!("{}: invalid premise lock: {e}", path.display()))?;
    if lock.version != 1 {
        return Err(format!(
            "{}: unsupported premise lock version {}",
            path.display(),
            lock.version
        ));
    }
    Ok(lock.statements)
}

fn checked_premise(
    statement: &LocatedStatement,
    premise: &Premise,
    old: &BTreeMap<String, Premise>,
    path: &Path,
) -> Result<(), String> {
    match old.get(&statement.identity) {
        None => Err(format!(
            "premise-missing: no reviewed premise in {}; run reg-build lock",
            path.display()
        )),
        Some(previous) if previous != premise => {
            let mut diff = Vec::new();
            changes(
                &serde_json::to_value(previous).map_err(|e| e.to_string())?,
                &serde_json::to_value(premise).map_err(|e| e.to_string())?,
                "premise",
                &mut diff,
            );
            Err(format!("premise-changed:\n  {}", diff.join("\n  ")))
        }
        Some(_) => Ok(()),
    }
}

fn remap(
    values: &mut [crate::emit::CuratedValue],
    changes: &[reg_resolve::curation::apply::Change],
) {
    for change in changes {
        if !matches!(change.field.as_str(), "table" | "register") {
            continue;
        }
        for value in values
            .iter_mut()
            .filter(|v| v.kind == change.kind && v.locator == change.locator)
        {
            if change.field == "table" && value.table == change.source_value {
                value.table.clone_from(&change.value);
            }
            if change.field == "register" && value.register == change.source_value {
                value.register.clone_from(&change.value);
            }
        }
    }
}

pub(super) fn run(
    root: &Path,
    statements: &[LocatedStatement],
    outputs: &[reg_readers::ReadOutput],
    record: bool,
    registers: &[String],
    release: bool,
) -> Result<Applied, String> {
    run_selected(
        root,
        &statements.iter().collect::<Vec<_>>(),
        outputs,
        record,
        registers,
        release,
    )
}

/// Bind source handles, move occurrence rows, then add asserted facts. Each phase
/// evaluates an immutable snapshot before applying accepted statements. Other kinds
/// see only that resulting source view. `record` is explicit review acceptance.
#[allow(clippy::too_many_lines)]
fn run_selected(
    root: &Path,
    statements: &[&LocatedStatement],
    outputs: &[reg_readers::ReadOutput],
    record: bool,
    registers: &[String],
    release: bool,
) -> Result<Applied, String> {
    let mut records = outputs
        .iter()
        .flat_map(|o| o.frame.records.iter().cloned())
        .collect::<Vec<_>>();
    if statements.is_empty() {
        return Ok(Applied {
            records,
            ..Applied::default()
        });
    }
    let source_records = records.clone();
    let mut result = Applied::default();
    let mut evaluated = Vec::new();
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for statement in statements {
        groups
            .entry((&statement.provider, &statement.register))
            .or_default()
            .push(*statement);
    }
    let mut selected = BTreeSet::new();
    let mut pending = Vec::new();
    let mut findings = Vec::new();
    let mut refusals = Vec::new();
    let mut locks = BTreeMap::new();
    let mut premises = BTreeMap::<_, BTreeMap<_, _>>::new();
    for (provider, register) in groups.keys() {
        let name = format!("{provider}/{register}");
        if !registers.is_empty() && !registers.iter().any(|r| r == &name || r == *register) {
            continue;
        }
        selected.insert(name.clone());
        selected.insert((*register).clone());
        let directory = root.join("registers").join(provider).join(register);
        let path = directory.join("curation.lock");
        let ledger_path = directory.join("ids.toml");
        let ledger = if ledger_path.is_file() {
            super::toml_file(&ledger_path)?
        } else {
            reg_resolve::Ledger::default()
        };
        let old = previous(&path, record)?;
        locks.insert((provider, register), (path, ledger, old));
    }
    let mut priorities = BTreeMap::new();
    let mut origins = BTreeMap::new();
    for stage in 0..4 {
        // Resolve every group before changing this phase's frame. This makes file
        // order immaterial, including a bind that moves a foreign source register.
        let mut accepted = Vec::new();
        for ((provider, register), group) in &groups {
            let Some((path, ledger, old)) = locks.get(&(provider, register)) else {
                continue;
            };
            let group = group
                .iter()
                .copied()
                .filter(|s| phase(&s.statement) == stage)
                .collect::<Vec<_>>();
            if group.is_empty() {
                continue;
            }
            let source = records
                .iter()
                .filter(|r| {
                    if stage == 0 {
                        provider_of(r).is_none_or(|p| p == provider.as_str())
                    } else {
                        belongs(r, provider, register)
                            || (stage == 1 && matches!(r, Record::Doc(_)))
                    }
                })
                .cloned()
                .collect::<Vec<_>>();
            let content = group.iter().map(|s| &s.statement).collect::<Vec<_>>();
            let references = references(root, &records, &group, &result.classifications)?;
            let evaluations =
                reg_resolve::curation::resolve(&source, ledger, &content, &references);
            for (statement, evaluation) in group.iter().zip(evaluations) {
                let at = location(statement);
                let resolved = match evaluation {
                    Ok(r) => r,
                    Err(e) => {
                        refusals.push(format!("{at}: {e}"));
                        continue;
                    }
                };
                if !resolved.conflicts.is_empty() {
                    for other in &resolved.conflicts {
                        refusals.push(format!(
                            "{at}: curation-overlap: overlapping overrides with {}",
                            location(group[*other])
                        ));
                    }
                    continue;
                }
                if !record && let Err(e) = checked_premise(statement, &resolved.premise, old, path)
                {
                    refusals.push(format!("{at}: {e}"));
                    continue;
                }
                if resolved.redundant {
                    findings.push(format!("{at}: redundant-set: set equals the source"));
                }
                accepted.push((*statement, resolved.premise, provider, register));
            }
        }
        for (statement, premise, _, _) in &accepted {
            if let reg_frame::statement::Statement::Bind {
                priority: Some(priority),
                ..
            } = &statement.statement
            {
                for source in &premise.subjects {
                    priorities.insert(source.locator.clone(), *priority);
                }
            }
        }
        if stage == 2 {
            accepted.sort_by_key(|(s, ..)| {
                use reg_frame::statement::{AssertFact, Statement};
                match &s.statement {
                    Statement::Assert {
                        fact: AssertFact::Table { .. },
                        ..
                    } => (0, &s.identity),
                    Statement::Assert {
                        fact: AssertFact::Release { .. },
                        ..
                    } => (1, &s.identity),
                    Statement::Assert {
                        fact: AssertFact::Occurrence { .. },
                        ..
                    } => (2, &s.identity),
                    _ => (3, &s.identity),
                }
            });
        }
        for (statement, premise, provider, register) in accepted {
            let (_, ledger, _) = &locks[&(provider, register)];
            let source = records
                .iter()
                .filter(|r| {
                    if stage == 0 {
                        provider_of(r).is_none_or(|p| p == provider.as_str())
                    } else {
                        belongs(r, provider, register)
                            || (stage == 1 && matches!(r, Record::Doc(_)))
                    }
                })
                .cloned()
                .collect::<Vec<_>>();
            let file = statement
                .path
                .strip_prefix(root)
                .map_err(|_| {
                    format!(
                        "{}: statement is outside curation root {}",
                        statement.path.display(),
                        root.display()
                    )
                })?
                .to_string_lossy()
                .into_owned();
            let locator = reg_frame::Locator {
                input: file.clone(),
                at: format!("line={}", statement.line),
            };
            let mut content = statement.statement.clone();
            if let reg_frame::statement::Statement::Bind {
                target: reg_frame::statement::BindTarget::Register { id },
                ..
            } = &mut content
                && ledger.public.as_deref() == Some(id.as_str())
            {
                // The statement names the catalog id; frame coordinates and the
                // owning ledger directory keep the source handle.
                id.clone_from(register);
            }
            let applied = match reg_resolve::curation::apply::apply(
                &source,
                ledger,
                &content,
                &locator,
                &premise,
                &priorities,
                &source_records,
                &mut origins,
            ) {
                Ok(a) => a,
                Err(e) => {
                    let refusal = format!("{}: {e}", location(statement));
                    if record || release {
                        refusals.push(refusal);
                        return Err(refusals.join("\n"));
                    }
                    // Recompute from the original inputs: a refused binding must
                    // contribute neither facts nor source-priority ranks.
                    let retained = statements
                        .iter()
                        .copied()
                        .filter(|s| s.identity != statement.identity)
                        .collect::<Vec<_>>();
                    let mut rerun =
                        run_selected(root, &retained, outputs, record, registers, release)?;
                    rerun
                        .findings
                        .push(format!("{refusal} (statement skipped)"));
                    return Ok(rerun);
                }
            };
            evaluated.push((statement, premise.clone()));
            premises
                .entry((provider, register))
                .or_default()
                .insert(statement.identity.clone(), premise);
            if stage < 3 {
                // Follow earlier winners through later coordinate moves. Their
                // statement and losing source value remain the original evidence.
                remap(&mut result.overrides, &applied.changes);
                let value =
                    serde_json::to_value(&statement.statement).map_err(|e| e.to_string())?;
                result.statements.push(crate::emit::StatementRow {
                    hash: statement.identity.clone(),
                    kind: value["kind"].as_str().expect("statement kind").into(),
                    file,
                    line: statement.line,
                    evidence: value["evidence"]
                        .as_str()
                        .expect("required evidence")
                        .into(),
                    caveat: value["caveat"].as_str().map(str::to_owned),
                });
                result.overrides.extend(applied.changes.iter().map(|c| {
                    crate::emit::CuratedValue {
                        kind: c.kind.clone(),
                        locator: c.locator.clone(),
                        provider: c.provider.clone(),
                        register: c.register.clone(),
                        table: c.table.clone(),
                        release: c.release.clone(),
                        period: c.period.clone(),
                        field: c.field.clone(),
                        value: c.value.clone(),
                        source_value: c.source_value.clone(),
                        statement: statement.identity.clone(),
                    }
                }));
                result
                    .classifications
                    .extend(applied.classifications.iter().cloned());
            }
            let before = source.into_iter().collect::<BTreeSet<_>>();
            records.retain(|r| !before.contains(r));
            records.extend(applied.records);
        }
    }
    let mut conflicts = BTreeSet::new();
    let indices = evaluated
        .iter()
        .enumerate()
        .map(|(i, (s, _))| (s.identity.as_str(), i))
        .collect::<BTreeMap<_, _>>();
    let claims = result
        .overrides
        .iter()
        .map(|value| reg_resolve::curation::AppliedClaim {
            statement: indices[value.statement.as_str()],
            kind: &value.kind,
            locator: &value.locator,
            provider: value.provider.as_deref(),
            register: value.register.as_deref(),
            table: value.table.as_deref(),
            release: value.release.as_deref(),
            field: &value.field,
            period: value.period.as_ref(),
        })
        .collect::<Vec<_>>();
    let resolved = evaluated
        .iter()
        .map(|(s, p)| (&s.statement, p))
        .collect::<Vec<_>>();
    for (a, b) in reg_resolve::curation::applied_conflicts(&resolved, &claims) {
        let (left, right) = (evaluated[a].0, evaluated[b].0);
        conflicts.insert(left.identity.clone());
        conflicts.insert(right.identity.clone());
        refusals.push(format!(
            "{}: curation-overlap: overlapping overrides with {}",
            location(left),
            location(right)
        ));
    }
    for (a, b) in reg_resolve::curation::classification_conflicts(&result.classifications) {
        let (left, right) = (&result.classifications[a], &result.classifications[b]);
        for class in [left, right] {
            if let Some(statement) = result.statements.iter().find(|s| {
                s.file == class.locator.input && format!("line={}", s.line) == class.locator.at
            }) {
                conflicts.insert(statement.hash.clone());
            }
        }
        refusals.push(format!(
            "{}:{}: curation-overlap: classification version overlaps {}:{}",
            left.locator.input, left.locator.at, right.locator.input, right.locator.at
        ));
    }
    if !conflicts.is_empty() {
        if record || release {
            return Err(refusals.join("\n"));
        }
        let retained = statements
            .iter()
            .copied()
            .filter(|s| !conflicts.contains(&s.identity))
            .collect::<Vec<_>>();
        let mut rerun = run_selected(root, &retained, outputs, record, registers, release)?;
        rerun.findings.extend(
            refusals
                .into_iter()
                .map(|r| format!("{r} (statement skipped)")),
        );
        return Ok(rerun);
    }

    for ((provider, register), (path, _, _)) in locks {
        pending.push((
            path,
            Lock {
                version: 1,
                statements: premises.remove(&(provider, register)).unwrap_or_default(),
            },
        ));
    }
    if record {
        for register in registers {
            if !selected.contains(register) {
                // A register without statements still exists as a curation directory.
                let found = if let Some((provider, register)) = register.split_once('/') {
                    reg_core::is_slug(provider)
                        && reg_core::is_slug(register)
                        && root
                            .join("registers")
                            .join(provider)
                            .join(register)
                            .is_dir()
                } else {
                    std::fs::read_dir(root.join("registers"))
                        .map_err(|e| e.to_string())?
                        .filter_map(Result::ok)
                        .any(|p| p.path().join(register).is_dir())
                };
                if !found {
                    return Err(format!("unknown curation register {register:?}"));
                }
            }
        }
    }
    if !refusals.is_empty() && (record || release) {
        return Err(refusals.join("\n"));
    }
    findings.extend(
        refusals
            .into_iter()
            .map(|r| format!("{r} (statement skipped)")),
    );
    if record {
        // Prepare all serializations before writing; a refused register never causes
        // a partial review update in another register.
        let files = pending
            .into_iter()
            .map(|(path, lock)| (path, reg_frame::canonical_line(&lock)))
            .collect::<Vec<_>>();
        let mut existing = BTreeSet::new();
        if root.join("registers").is_dir() {
            super::tree(&root.join("registers"), "", &mut existing)?;
        }
        for file in existing {
            let Some(name) = file.strip_suffix("/curation.lock") else {
                continue;
            };
            let path = root.join("registers").join(&file);
            if (registers.is_empty()
                || registers
                    .iter()
                    .any(|r| r == name || name.rsplit('/').next() == Some(r)))
                && !files.iter().any(|(p, _)| p == &path)
            {
                std::fs::remove_file(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            }
        }
        for (path, bytes) in files {
            std::fs::write(&path, bytes).map_err(|e| format!("{}: {e}", path.display()))?;
        }
    }
    for finding in &findings {
        eprintln!("reg-build: {finding}");
    }
    result.findings = findings;
    result.records = records;
    Ok(result)
}
