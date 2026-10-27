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
    outputs: &[reg_readers::ReadOutput],
    statements: &[&LocatedStatement],
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
        for source in outputs.iter().flat_map(|o| &o.frame.records) {
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
            let records = outputs
                .iter()
                .flat_map(|o| &o.frame.records)
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

/// Evaluate against an immutable source view. D-3a plugs its bound frame plus
/// asserted facts in here; no other statement's output may enter this view.
/// `record` is explicit review acceptance and never used by a build.
#[allow(clippy::too_many_lines)]
pub(super) fn run(
    root: &Path,
    statements: &[LocatedStatement],
    outputs: &[reg_readers::ReadOutput],
    record: bool,
    registers: &[String],
    release: bool,
) -> Result<Vec<String>, String> {
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for statement in statements {
        groups
            .entry((&statement.provider, &statement.register))
            .or_default()
            .push(statement);
    }
    let mut selected = BTreeSet::new();
    let mut pending = Vec::new();
    let mut findings = Vec::new();
    let mut refusals = Vec::new();
    for ((provider, register), group) in groups {
        let name = format!("{provider}/{register}");
        if !registers.is_empty() && !registers.iter().any(|r| r == &name || r == register) {
            continue;
        }
        selected.insert(name.clone());
        selected.insert(register.clone());
        let directory = root.join("registers").join(provider).join(register);
        let path = directory.join("curation.lock");
        let ledger_path = directory.join("ids.toml");
        let ledger = if ledger_path.is_file() {
            super::toml_file(&ledger_path)?
        } else {
            reg_resolve::Ledger::default()
        };
        let records = outputs
            .iter()
            .flat_map(|output| &output.frame.records)
            .filter(|r| belongs(r, provider, register))
            .cloned()
            .collect::<Vec<_>>();
        let content = group.iter().map(|s| &s.statement).collect::<Vec<_>>();
        let references = references(root, outputs, &group)?;
        let evaluations = reg_resolve::curation::resolve(&records, &ledger, &content, &references);
        let previous = if !record && path.try_exists().map_err(|e| e.to_string())? {
            let text =
                std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            let lock: Lock = serde_json::from_str(&text)
                .map_err(|e| format!("{}: invalid premise lock: {e}", path.display()))?;
            if lock.version != 1 {
                return Err(format!(
                    "{}: unsupported premise lock version {}",
                    path.display(),
                    lock.version
                ));
            }
            lock.statements
        } else {
            BTreeMap::new()
        };
        let mut premises = BTreeMap::new();
        for (statement, evaluation) in group.iter().zip(evaluations) {
            let at = location(statement);
            let resolved = match evaluation {
                Ok(resolved) => resolved,
                Err(error) => {
                    refusals.push(format!("{at}: {error}"));
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
            if !record {
                match previous.get(&statement.identity) {
                    None => {
                        refusals.push(format!(
                            "{at}: premise-missing: no reviewed premise in {}; run reg-build lock",
                            path.display()
                        ));
                        continue;
                    }
                    Some(old) if old != &resolved.premise => {
                        let mut diff = Vec::new();
                        changes(
                            &serde_json::to_value(old).map_err(|e| e.to_string())?,
                            &serde_json::to_value(&resolved.premise).map_err(|e| e.to_string())?,
                            "premise",
                            &mut diff,
                        );
                        refusals.push(format!("{at}: premise-changed:\n  {}", diff.join("\n  ")));
                        continue;
                    }
                    Some(_) => {}
                }
            }
            if resolved.redundant {
                findings.push(format!("{at}: redundant-set: set equals the source"));
            }
            premises.insert(statement.identity.clone(), resolved.premise);
        }
        pending.push((
            path,
            Lock {
                version: 1,
                statements: premises,
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
    Ok(findings)
}
