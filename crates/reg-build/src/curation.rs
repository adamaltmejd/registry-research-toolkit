//! Premise lock IO and located diagnostics. Selector semantics live in reg-resolve.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use reg_frame::Record;
use reg_readers::curation::LocatedStatement;
use reg_resolve::curation::Premise;
use serde::{Deserialize, Serialize};
use sha2::Digest;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Lock {
    version: u32,
    statements: BTreeMap<String, Premise>,
}

fn location(statement: &LocatedStatement) -> String {
    format!("{}:{}", statement.path.display(), statement.line)
}

/// Locks live inside a register; published statements and cross-register processing
/// need that same scope in their identity, independent of file paths and line numbers.
fn publication_identity(statement: &LocatedStatement) -> String {
    statement
        .statement
        .scoped_identity(&statement.provider, &statement.register)
}

fn statement_locator(
    root: &Path,
    statement: &LocatedStatement,
) -> Result<reg_frame::Locator, String> {
    let file = statement.path.strip_prefix(root).map_err(|_| {
        format!(
            "{}: statement is outside curation root {}",
            statement.path.display(),
            root.display()
        )
    })?;
    Ok(reg_frame::Locator {
        input: file.to_string_lossy().into_owned(),
        at: format!("line={}", statement.line),
    })
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

/// Application reads local coordinates and explicitly named code lists. Other
/// shared facts remain in the frame without being copied for each statement.
fn application_record(record: &Record, provider: &str, register: &str, hash: Option<&str>) -> bool {
    match record {
        Record::ValueSet(set) => hash == Some(set.hash.as_str()),
        Record::Link(_) | Record::Notice(_) | Record::Doc(_) => false,
        _ => belongs(record, provider, register),
    }
}

/// Catalog references resolve through each register's ledger, then snapshot source
/// facts. Classes/providers without source entities cannot silently acquire a premise.
fn references(
    root: &Path,
    records: &[Record],
    statements: &[&LocatedStatement],
    classifications: &[reg_resolve::curation::apply::Classification],
    variables: &BTreeMap<reg_resolve::OccurrenceKey, String>,
    shared: &reg_resolve::curation::SharedSources,
) -> Result<BTreeMap<String, Vec<reg_resolve::curation::SourcePremise>>, String> {
    let mut references = BTreeMap::new();
    let objects = statements
        .iter()
        .filter_map(|s| match &s.statement {
            reg_frame::statement::Statement::Link { object, .. } => Some(object),
            reg_frame::statement::Statement::Set { fields, .. } => fields.classification.as_ref(),
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
                    ("inventory".into(), class.metadata.clone()),
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
                .filter(|r| application_record(r, &provider, &source.register, None))
                .cloned()
                .collect::<Vec<_>>();
            matches.push(reg_resolve::curation::reference(
                &records,
                &ledger,
                variable.as_deref(),
                variables,
                shared,
            ));
        }
        if let [Ok(premise)] = &matches[..] {
            references.insert(object.clone(), premise.clone());
        }
    }
    Ok(references)
}

/// Resolve code-list references before register curation, through the same constructor
/// used by inline classification assertions. No applicability is inferred here.
#[allow(clippy::too_many_lines)]
fn inventory(root: &Path, records: &[Record]) -> Result<Applied, String> {
    use reg_frame::statement::{AssertFact, Statement};
    let mut out = Applied::default();
    let entries = reg_readers::curation::load_classifications(root).map_err(|e| e.to_string())?;
    let mut families = BTreeMap::new();
    for entry in entries {
        let c = &entry.classification;
        let at = reg_frame::Locator {
            input: "classifications.toml".into(),
            at: format!("line={}", entry.line),
        };
        let located = |message: String| {
            format!(
                "{}:{}: {message}",
                root.join(&at.input).display(),
                entry.line
            )
        };
        for (field, value) in [("family_name", &c.family_name), ("publisher", &c.publisher)] {
            if let Some(value) = value {
                let key = (c.family.clone(), field);
                if let Some((previous, line)) = families.get(&key)
                    && previous != value
                {
                    return Err(located(format!(
                        "classification-family-conflict: {field} conflicts with classifications.toml:{line}"
                    )));
                }
                families.insert(key, (value.clone(), entry.line));
            }
        }
        let sets = records
            .iter()
            .filter_map(|r| match r {
                Record::ValueSet(v) if v.locator.input == c.value_set => Some((v.hash.as_str(), v)),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        let [set] = sets.values().copied().collect::<Vec<_>>()[..] else {
            return Err(located(format!(
                "classification-reference-unresolved: value_set {:?} must name exactly one reader value set",
                c.value_set
            )));
        };
        let scope = c
            .period
            .as_ref()
            .map_or_else(String::new, |p| format!(" @{p}"));
        let statement = Statement::Assert {
            subject: format!("inventory: code{scope}").parse().map_err(located)?,
            fact: AssertFact::ClassificationVersion {
                family: c.family.clone(),
                version: c.version.clone(),
                items: set.items.clone(),
            },
            evidence: c.evidence.clone(),
            caveat: None,
        };
        statement.validate().map_err(located)?;
        let mut class =
            reg_resolve::curation::apply::classification(&statement, &at).map_err(located)?;
        class.metadata = serde_json::to_value(c).map_err(|e| located(e.to_string()))?;
        let hash = reg_frame::hex(&sha2::Sha256::digest(reg_frame::canonical_line(c)));
        out.statements.push(crate::emit::StatementRow {
            hash: hash.clone(),
            kind: "assert".into(),
            file: at.input.clone(),
            line: entry.line,
            evidence: Some(c.evidence.clone()),
            caveat: None,
        });
        out.overrides.push(reg_resolve::provenance::Claim {
            kind: "class_version".into(),
            locator: at,
            identity: None,
            provider: None,
            register: None,
            table: None,
            release: None,
            period: Some(class.period.clone()),
            field: "inventory".into(),
            value: Some(class.metadata.to_string()),
            source_value: None,
            statement: hash,
        });
        out.classifications.push(class);
    }
    let known = out
        .classifications
        .iter()
        .map(|c| format!("class/{}/{}", c.family, c.version))
        .collect::<BTreeSet<_>>();
    for class in &out.classifications {
        for target in class.metadata["succeeds"].as_array().into_iter().flatten() {
            if !known.contains(target.as_str().expect("typed inventory reference")) {
                return Err(format!(
                    "{}:{}: classification-reference-unresolved: predecessor {target}",
                    root.join(&class.locator.input).display(),
                    class
                        .locator
                        .at
                        .strip_prefix("line=")
                        .unwrap_or(&class.locator.at)
                ));
            }
        }
    }
    let predecessors = out
        .classifications
        .iter()
        .map(|c| {
            (
                format!("class/{}/{}", c.family, c.version),
                c.metadata["succeeds"]
                    .as_array()
                    .expect("typed succession list")
                    .iter()
                    .map(|v| v.as_str().expect("typed succession reference").to_owned())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for class in &out.classifications {
        let origin = format!("class/{}/{}", class.family, class.version);
        let mut pending = predecessors[&origin].clone();
        let mut visited = BTreeSet::new();
        while let Some(node) = pending.pop() {
            if node == origin {
                return Err(format!(
                    "{}:{}: classification-succession-cycle: {origin}",
                    root.join(&class.locator.input).display(),
                    class
                        .locator
                        .at
                        .strip_prefix("line=")
                        .unwrap_or(&class.locator.at)
                ));
            }
            if visited.insert(node.clone()) {
                pending.extend(predecessors[&node].iter().cloned());
            }
        }
    }
    Ok(out)
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
    pub overrides: Vec<reg_resolve::provenance::Claim>,
    pub classifications: Vec<reg_resolve::curation::apply::Classification>,
    pub variables: BTreeMap<reg_resolve::OccurrenceKey, String>,
    pub metadata: Vec<reg_resolve::curation::apply::Metadata>,
    pub links: Vec<(LocatedStatement, Premise)>,
    pub linked: Option<crate::link::Curated>,
    pub acks:
        BTreeMap<(String, String), BTreeMap<usize, reg_resolve::curation::ack::Acknowledgement>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Parents,
    Coverage,
    Periods,
    Moves,
    Facts,
    Variables,
    Fields,
    Relations,
}

fn phase(statement: &reg_frame::statement::Statement) -> Phase {
    use reg_frame::statement::{AssertFact, Statement};
    match statement {
        Statement::Assert {
            fact: AssertFact::Table { .. } | AssertFact::Release { .. },
            ..
        } => Phase::Parents,
        Statement::Set { fields, .. } if fields.coverage.is_some() => Phase::Coverage,
        Statement::Set { fields, .. } if fields.period.is_some() => Phase::Periods,
        Statement::Set { fields, .. } if fields.table.is_some() => Phase::Moves,
        Statement::Assert { .. } => Phase::Facts,
        Statement::Set { fields, .. } if fields.variable.is_some() => Phase::Variables,
        Statement::Link { .. } | Statement::Ack { .. } => Phase::Relations,
        Statement::Set { .. } => Phase::Fields,
    }
}

/// Keep field applications small and based on the same immutable phase evidence.
fn field_application(
    records: &[Record],
    statement: &LocatedStatement,
    ledger: &reg_resolve::Ledger,
    premise: &Premise,
    locators: &BTreeMap<reg_frame::Locator, Vec<usize>>,
    root: &Path,
) -> Result<
    (
        Vec<usize>,
        Vec<Record>,
        reg_resolve::curation::apply::Applied,
    ),
    String,
> {
    let indices = premise
        .subjects
        .iter()
        .chain(&premise.objects)
        .flat_map(|p| locators.get(&p.locator).into_iter().flatten().copied())
        .filter(|i| belongs(&records[*i], &statement.provider, &statement.register))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let source = indices
        .iter()
        .map(|i| records[*i].clone())
        .collect::<Vec<_>>();
    let at = statement_locator(root, statement)?;
    let applied =
        reg_resolve::curation::apply::apply(&source, ledger, &statement.statement, &at, premise)?;
    Ok((indices, source, applied))
}

/// Independent fields merge as patches, never as whole-record replacements.
fn merge_fields(current: &Record, before: &Record, after: &Record) -> Result<Record, String> {
    let mut current = serde_json::to_value(current).map_err(|e| e.to_string())?;
    let before = serde_json::to_value(before).map_err(|e| e.to_string())?;
    let after = serde_json::to_value(after).map_err(|e| e.to_string())?;
    for (field, value) in after.as_object().expect("frame object") {
        if before.get(field) != Some(value) {
            if field == "conflicts" {
                // Independent corrections remove markers from the same source snapshot.
                if let Some(conflicts) = current
                    .get_mut(field)
                    .and_then(serde_json::Value::as_array_mut)
                {
                    conflicts.retain(|field| {
                        value
                            .as_array()
                            .expect("source conflict list")
                            .contains(field)
                    });
                }
            } else {
                current[field] = value.clone();
            }
        }
    }
    serde_json::from_value(current).map_err(|e| e.to_string())
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

/// Apply parent, bound, period, move, fact, identity and field stages in order. Each phase
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
    let mut result = inventory(root, &records)?;
    let mut evaluated: Vec<(&LocatedStatement, Premise)> = Vec::new();
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
    let mut conflicts = BTreeSet::new();
    let mut locks = BTreeMap::new();
    let mut premises = BTreeMap::<_, BTreeMap<_, _>>::new();
    for (provider, register) in groups.keys() {
        let name = format!("{provider}/{register}");
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
    let mut conflict_overrides = None;
    for stage in [
        Phase::Parents,
        Phase::Coverage,
        Phase::Periods,
        Phase::Moves,
        Phase::Facts,
        Phase::Variables,
        Phase::Fields,
        Phase::Relations,
    ] {
        let fields_stage = matches!(stage, Phase::Coverage | Phase::Periods | Phase::Fields);
        let structural_stage = matches!(stage, Phase::Parents | Phase::Moves | Phase::Facts);
        let mut field_snapshot = Vec::new();
        if !conflicts.is_empty() {
            break;
        }

        let shared = if statements.iter().any(|s| phase(&s.statement) == stage) {
            reg_resolve::curation::SharedSources::new(&records)
        } else {
            reg_resolve::curation::SharedSources::default()
        };
        // Evaluate every register, even when recording only selected locks: linked
        // premises must see the same curated facts as an ordinary build.
        let mut accepted = Vec::new();
        let mut field_applications = BTreeMap::new();
        let mut ack_indices = BTreeMap::new();
        if fields_stage
            && statements.iter().any(|s| {
                matches!(s.statement, reg_frame::statement::Statement::Set { .. })
                    && phase(&s.statement) == stage
            })
        {
            field_snapshot = records
                .iter()
                .filter(|r| {
                    matches!(
                        r,
                        Record::Register(_)
                            | Record::Table(_)
                            | Record::Release(_)
                            | Record::Occurrence(_)
                    )
                })
                .cloned()
                .collect();
        }
        let mut locators = BTreeMap::<_, Vec<_>>::new();
        if fields_stage {
            for (i, record) in records.iter().enumerate() {
                let at = match record {
                    Record::Register(r) => Some(&r.locator),
                    Record::Table(r) => Some(&r.locator),
                    Record::Release(r) => Some(&r.locator),
                    Record::Occurrence(r) => Some(&r.locator),
                    Record::ValueSet(r) => Some(&r.locator),
                    _ => None,
                };
                if let Some(at) = at {
                    locators.entry(at.clone()).or_default().push(i);
                }
                if let Record::Occurrence(r) = record {
                    for at in &r.also {
                        locators.entry(at.clone()).or_default().push(i);
                    }
                }
            }
        }
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
                .filter(|r| application_record(r, provider, register, None))
                .cloned()
                .collect::<Vec<_>>();
            if stage == Phase::Fields
                && group
                    .iter()
                    .any(|s| matches!(s.statement, reg_frame::statement::Statement::Set { .. }))
            {
                for (variable, members) in
                    reg_resolve::curation::variable_members(&source, ledger, &result.variables)?
                {
                    for member in members {
                        result
                            .variables
                            .entry(member)
                            .or_insert_with(|| variable.clone());
                    }
                }
            }
            let finding_view = if stage == Phase::Relations
                && group
                    .iter()
                    .any(|s| matches!(s.statement, reg_frame::statement::Statement::Ack { .. }))
            {
                let sets = records
                    .iter()
                    .filter_map(|r| match r {
                        Record::ValueSet(v) => Some((v.hash.as_str(), v)),
                        _ => None,
                    })
                    .collect();
                let mut resolved = reg_resolve::resolve(&source, ledger, &sets, &result.variables)?;
                reg_resolve::curation::apply_metadata(&mut resolved, &result.metadata);
                Some(resolved)
            } else {
                None
            };
            let content = group.iter().map(|s| &s.statement).collect::<Vec<_>>();
            let references = references(
                root,
                &records,
                &group,
                &result.classifications,
                &result.variables,
                &shared,
            )?;
            let evaluations = reg_resolve::curation::resolve(
                &source,
                ledger,
                &content,
                &references,
                &result.variables,
                &shared,
            );
            for (statement, evaluation) in group.iter().zip(evaluations) {
                let at = location(statement);
                let mut resolved = match evaluation {
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
                if fields_stage
                    && matches!(
                        statement.statement,
                        reg_frame::statement::Statement::Set { .. }
                    )
                {
                    match field_application(
                        &records,
                        statement,
                        ledger,
                        &resolved.premise,
                        &locators,
                        root,
                    ) {
                        Ok(application) => {
                            field_applications.insert(publication_identity(statement), application);
                        }
                        Err(error) => {
                            refusals.push(format!("{at}: {error}"));
                            continue;
                        }
                    }
                }
                if let Some(view) = &finding_view
                    && matches!(
                        statement.statement,
                        reg_frame::statement::Statement::Ack { .. }
                    )
                {
                    match reg_resolve::curation::ack::review(
                        view,
                        &statement.statement,
                        &mut resolved.premise,
                    ) {
                        Ok(indices) => {
                            ack_indices.insert(publication_identity(statement), indices);
                        }
                        Err(error) => {
                            refusals.push(format!("{at}: {error}"));
                            continue;
                        }
                    }
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
        accepted.sort_by_key(|(s, ..)| {
            use reg_frame::statement::{AssertFact, Statement};
            let dependency = match &s.statement {
                Statement::Assert {
                    fact: AssertFact::Table { .. },
                    ..
                } => 0,
                Statement::Assert {
                    fact: AssertFact::Release { .. },
                    ..
                } => 1,
                Statement::Assert {
                    fact: AssertFact::ClassificationVersion { .. },
                    ..
                } => 2,
                Statement::Assert {
                    fact: AssertFact::Occurrence { .. },
                    ..
                } => 3,
                _ => 4,
            };
            (dependency, publication_identity(s))
        });
        if stage == Phase::Facts {
            // Code lists are definitions consumed by occurrence assertions. Their
            // subject attachment follows occurrences, including newly asserted ones.
            for (statement, ..) in &accepted {
                let at = statement_locator(root, statement)?;
                if let Some(set) =
                    reg_resolve::curation::apply::asserted_value_set(&statement.statement, &at)
                {
                    records.push(Record::ValueSet(set));
                }
            }
        }
        // Identity assignments share the phase snapshot and never rewrite facts.
        let mut assignments = BTreeMap::new();
        if stage == Phase::Variables {
            for (provider, register) in groups.keys() {
                let group = accepted
                    .iter()
                    .filter(|(_, _, p, r)| *p == provider && *r == register)
                    .collect::<Vec<_>>();
                if group.is_empty() {
                    continue;
                }
                let source = records
                    .iter()
                    .filter(|r| application_record(r, provider, register, None))
                    .cloned()
                    .collect::<Vec<_>>();
                let phase = reg_resolve::curation::apply::AssignmentPhase::new(&source);
                for (statement, premise, _, _) in group {
                    assignments.insert(
                        publication_identity(statement),
                        phase.apply(&statement.statement, premise),
                    );
                }
            }
        }
        for (statement, premise, provider, register) in accepted {
            let (_, ledger, _) = &locks[&(provider, register)];
            let locator = statement_locator(root, statement)?;
            let file = locator.input.clone();
            let asserted =
                reg_resolve::curation::apply::asserted_value_set(&statement.statement, &locator);
            let hash = match &statement.statement {
                reg_frame::statement::Statement::Assert {
                    fact: reg_frame::statement::AssertFact::Occurrence { fields },
                    ..
                } => fields.value_set.as_deref(),
                _ => asserted.as_ref().map(|set| set.hash.as_str()),
            };
            let source = if structural_stage {
                records
                    .iter()
                    .filter(|r| application_record(r, provider, register, hash))
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if stage == Phase::Relations {
                match &statement.statement {
                    reg_frame::statement::Statement::Link { .. } => {
                        result.links.push(((*statement).clone(), premise.clone()));
                    }
                    reg_frame::statement::Statement::Ack { caveat, .. } => {
                        let indices = ack_indices
                            .remove(&publication_identity(statement))
                            .expect("evaluated ack");
                        for index in indices {
                            result
                                .acks
                                .entry(((*provider).clone(), (*register).clone()))
                                .or_default()
                                .insert(
                                    index,
                                    reg_resolve::curation::ack::Acknowledgement {
                                        statement: publication_identity(statement),
                                        caveat: caveat.clone(),
                                    },
                                );
                        }
                        result.statements.push(statement_row(root, statement)?);
                    }
                    _ => unreachable!("post-resolve phase"),
                }
            }
            let application = match stage {
                Phase::Variables => assignments
                    .remove(&publication_identity(statement))
                    .expect("evaluated assignment"),
                Phase::Relations => Ok(reg_resolve::curation::apply::Applied::default()),
                Phase::Coverage | Phase::Periods | Phase::Fields => {
                    if let Some((indices, before, applied)) =
                        field_applications.remove(&publication_identity(statement))
                    {
                        for ((index, before), after) in
                            indices.into_iter().zip(&before).zip(&applied.records)
                        {
                            records[index] = merge_fields(&records[index], before, after)?;
                        }
                        Ok(applied)
                    } else {
                        Ok(reg_resolve::curation::apply::Applied::default())
                    }
                }
                _ => reg_resolve::curation::apply::apply(
                    &source,
                    ledger,
                    &statement.statement,
                    &locator,
                    &premise,
                ),
            };
            let application = application.and_then(|applied| {
                if stage == Phase::Moves {
                    reg_resolve::curation::apply::rekey(
                        &source,
                        &applied.records,
                        &mut result.variables,
                        &mut result.overrides,
                        &mut result.metadata,
                    )
                    .map_err(|at| {
                        format!(
                            "curation-coordinate-collision: set coordinates collide at {}:{}",
                            at.input, at.at
                        )
                    })?;
                }
                Ok(applied)
            });
            let applied = match application {
                Ok(a) => a,
                Err(e) => {
                    let refusal = format!("{}: {e}", location(statement));
                    if record || release {
                        refusals.push(refusal);
                        return Err(refusals.join("\n"));
                    }
                    // Refused statements contribute neither facts nor provenance.
                    let retained = statements
                        .iter()
                        .copied()
                        .filter(|s| publication_identity(s) != publication_identity(statement))
                        .collect::<Vec<_>>();
                    let mut rerun =
                        run_selected(root, &retained, outputs, record, registers, release)?;
                    rerun
                        .findings
                        .push(format!("{refusal} (statement skipped)"));
                    return Ok(rerun);
                }
            };
            result.variables.extend(applied.assignments);
            result.metadata.extend(applied.metadata);
            evaluated.push((statement, premise.clone()));
            premises
                .entry((provider, register))
                .or_default()
                .insert(statement.identity.clone(), premise);
            if !applied.changes.is_empty() {
                let value =
                    serde_json::to_value(&statement.statement).map_err(|e| e.to_string())?;
                result.statements.push(crate::emit::StatementRow {
                    hash: publication_identity(statement),
                    kind: value["kind"].as_str().expect("statement kind").into(),
                    file,
                    line: statement.line,
                    evidence: value["evidence"].as_str().map(str::to_owned),
                    caveat: value["caveat"].as_str().map(str::to_owned),
                });
                result.overrides.extend(applied.changes.iter().map(|c| {
                    reg_resolve::provenance::Claim {
                        kind: c.kind.clone(),
                        locator: c.locator.clone(),
                        identity: c.identity.clone(),
                        provider: c.provider.clone(),
                        register: c.register.clone(),
                        table: c.table.clone(),
                        release: c.release.clone(),
                        period: c.period.clone(),
                        field: c.field.clone(),
                        value: c.value.clone(),
                        source_value: c.source_value.clone(),
                        statement: publication_identity(statement),
                    }
                }));
                result
                    .classifications
                    .extend(applied.classifications.iter().cloned());
            }
            if structural_stage {
                records.retain(|r| !application_record(r, provider, register, hash));
                records.extend(applied.records);
            }
        }
        if fields_stage && !field_snapshot.is_empty() {
            if stage == Phase::Fields {
                // Conflict checks retain the field phase view; links and acks see final identities.
                conflict_overrides = Some(result.overrides.clone());
            }
            let final_coordinates = records
                .iter()
                .filter(|r| {
                    matches!(
                        r,
                        Record::Register(_)
                            | Record::Table(_)
                            | Record::Release(_)
                            | Record::Occurrence(_)
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            if let Err(at) = reg_resolve::curation::apply::rekey(
                &field_snapshot,
                &final_coordinates,
                &mut result.variables,
                &mut result.overrides,
                &mut result.metadata,
            ) {
                for (statement, premise) in &evaluated {
                    let reg_frame::statement::Statement::Set { fields, .. } = &statement.statement
                    else {
                        continue;
                    };
                    if phase(&statement.statement) != stage {
                        continue;
                    }
                    let affected = premise.subjects.iter().any(|subject| {
                        if subject.locator == at {
                            return true;
                        }
                        let text =
                            |key| subject.fields.get(key).and_then(serde_json::Value::as_str);
                        field_snapshot.iter().any(|record| {
                            let Record::Occurrence(row) = record else {
                                return false;
                            };
                            (row.locator == at || row.also.contains(&at))
                                && text("provider") == Some(row.provider.as_str())
                                && text("register") == Some(row.register.as_str())
                                && text("table") == Some(row.table.as_str())
                                && ((fields.coverage.is_some() && text("kind") == Some("table"))
                                    || (fields.period.is_some()
                                        && text("kind") == Some("release")
                                        && text("release") == row.release.as_deref()))
                        })
                    });
                    if (fields.spelling.is_some()
                        || fields.period.is_some()
                        || fields.coverage.is_some())
                        && affected
                    {
                        conflicts.insert(publication_identity(statement));
                        refusals.push(format!(
                            "{}: curation-coordinate-collision: set coordinates collide at {}:{}",
                            location(statement),
                            at.input,
                            at.at
                        ));
                    }
                }
            }
        }
        if stage == Phase::Variables {
            for ((provider, register), (_, ledger, _)) in &locks {
                let assignments = evaluated
                    .iter()
                    .filter(|(s, _)| {
                        s.provider.as_str() == provider.as_str()
                            && s.register.as_str() == register.as_str()
                            && matches!(&s.statement, reg_frame::statement::Statement::Set { fields, .. } if fields.variable.is_some())
                    })
                    .collect::<Vec<_>>();
                if assignments.is_empty() {
                    continue;
                }
                let source = records
                    .iter()
                    .filter(|r| application_record(r, provider, register, None))
                    .cloned()
                    .collect::<Vec<_>>();
                if let Err(error) =
                    reg_resolve::curation::validate_assignments(&source, ledger, &result.variables)
                {
                    for (statement, _) in assignments {
                        conflicts.insert(publication_identity(statement));
                        refusals.push(format!("{}: {error}", location(statement)));
                    }
                }
            }
            if !conflicts.is_empty() {
                break;
            }
        }
    }
    if !result.links.is_empty() {
        let resolved = resolve_catalog(root, &records, &result.variables, &result.metadata)?;
        let linked = crate::link::curate(root, &resolved, &records, &result.links)?;
        if !linked.refusals.is_empty() {
            let messages = linked
                .refusals
                .iter()
                .map(|(_, message)| message.clone())
                .collect::<Vec<_>>();
            if record || release {
                return Err(messages.join("\n"));
            }
            let refused = linked
                .refusals
                .iter()
                .map(|(hash, _)| hash)
                .collect::<BTreeSet<_>>();
            let retained = statements
                .iter()
                .copied()
                .filter(|s| !refused.contains(&publication_identity(s)))
                .collect::<Vec<_>>();
            let mut rerun = run_selected(root, &retained, outputs, record, registers, release)?;
            rerun.findings.extend(
                messages
                    .into_iter()
                    .map(|message| format!("{message} (statement skipped)")),
            );
            return Ok(rerun);
        }
        result.linked = Some(linked);
    }
    let indices = evaluated
        .iter()
        .enumerate()
        .map(|(i, (s, _))| (publication_identity(s), i))
        .collect::<BTreeMap<_, _>>();
    let claims = conflict_overrides
        .as_ref()
        .unwrap_or(&result.overrides)
        .iter()
        // Shared inventory claims have no register evaluation index. Their version
        // identity conflicts are checked below; keep their claims for provenance.
        .filter(|value| {
            !(value.kind == "class_version" && value.locator.input == "classifications.toml")
        })
        .map(|value| reg_resolve::curation::AppliedClaim {
            statement: indices[value.statement.as_str()],
            kind: &value.kind,
            locator: &value.locator,
            identity: value.identity.as_ref(),
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
        conflicts.insert(publication_identity(left));
        conflicts.insert(publication_identity(right));
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
            .filter(|s| !conflicts.contains(&publication_identity(s)))
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
        if !registers.is_empty()
            && !registers
                .iter()
                .any(|r| r == &format!("{provider}/{register}") || r == *register)
        {
            continue;
        }
        let reviewed = premises.remove(&(provider, register)).unwrap_or_default();
        if reviewed.is_empty() {
            continue;
        }
        pending.push((
            path,
            Lock {
                version: 1,
                statements: reviewed,
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

/// Resolve the curated source snapshot for post-resolve curation validation.
pub(super) fn resolve_catalog(
    root: &Path,
    records: &[Record],
    variables: &BTreeMap<reg_resolve::OccurrenceKey, String>,
    metadata: &[reg_resolve::curation::apply::Metadata],
) -> Result<Vec<reg_resolve::Resolved>, String> {
    let sets = records
        .iter()
        .filter_map(|r| match r {
            Record::ValueSet(v) => Some((v.hash.as_str(), v)),
            _ => None,
        })
        .collect();
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for record in records {
        let key = match record {
            Record::Register(r) => (&r.provider, &r.register),
            Record::Table(r) => (&r.provider, &r.register),
            Record::Release(r) => (&r.provider, &r.register),
            Record::Occurrence(r) => (&r.provider, &r.register),
            _ => continue,
        };
        groups.entry(key).or_default().push(record.clone());
    }
    groups
        .into_iter()
        .map(|((provider, register), source)| {
            let path = super::ledger_path(root, provider, register);
            let ledger = if path.is_file() {
                super::toml_file(&path)?
            } else {
                reg_resolve::Ledger::default()
            };
            let mut resolved = reg_resolve::resolve(&source, &ledger, &sets, variables)?;
            reg_resolve::curation::apply_metadata(&mut resolved, metadata);
            Ok(resolved)
        })
        .collect()
}

pub(super) fn statement_row(
    root: &Path,
    statement: &LocatedStatement,
) -> Result<crate::emit::StatementRow, String> {
    let value = serde_json::to_value(&statement.statement).map_err(|e| e.to_string())?;
    Ok(crate::emit::StatementRow {
        hash: publication_identity(statement),
        kind: value["kind"].as_str().expect("statement kind").into(),
        file: statement_locator(root, statement)?.input,
        line: statement.line,
        evidence: value["evidence"].as_str().map(str::to_owned),
        caveat: value["caveat"].as_str().map(str::to_owned),
    })
}
