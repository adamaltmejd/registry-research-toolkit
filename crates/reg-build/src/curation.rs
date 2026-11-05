//! Premise lock IO and located diagnostics. Selector semantics live in reg-resolve.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use crate::error::Result;
use reg_frame::Record;
use reg_readers::curation::LocatedStatement;
use reg_resolve::curation::{Premise, SourcePremise, variable_level_setter};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Lock {
    version: u32,
    objects: BTreeMap<String, SourcePremise>,
    statements: BTreeMap<String, StoredPremise>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPremise {
    subjects: LockedSubjects,
    objects: Vec<String>,
}

impl Lock {
    fn new(premises: BTreeMap<String, LockedPremise>) -> Self {
        let mut objects = BTreeMap::new();
        let statements = premises
            .into_iter()
            .map(|(identity, premise)| {
                let mut keys = Vec::new();
                for object in premise.objects {
                    let key = object_key(&object);
                    objects.entry(key.clone()).or_insert(object);
                    keys.push(key);
                }
                keys.sort();
                (
                    identity,
                    StoredPremise {
                        subjects: premise.subjects,
                        objects: keys,
                    },
                )
            })
            .collect();
        Self {
            version: 3,
            objects,
            statements,
        }
    }
}

/// A verified lock, held as canonical JSON text: parsed into `serde_json` maps, the
/// real seed's locks take about ten times their file size for the whole curation run
/// (#2196). A statement's premise is parsed back when that statement is checked.
#[derive(Default)]
struct Reviewed {
    objects: BTreeMap<String, String>,
    statements: BTreeMap<String, (String, Vec<String>)>,
}

impl Reviewed {
    fn new(lock: Lock) -> Result<Self> {
        let mut objects = BTreeMap::new();
        for (key, object) in lock.objects {
            let line = reg_frame::canonical_line(&object);
            if reg_core::sha256_hex(&line) != key {
                return Err(format!("object digest mismatch for {key}; run reg-build lock").into());
            }
            objects.insert(key, line);
        }
        let mut statements = BTreeMap::new();
        for (identity, premise) in lock.statements {
            if let Some(key) = premise.objects.iter().find(|k| !objects.contains_key(*k)) {
                return Err(format!(
                    "statement {identity}: missing object {key}; run reg-build lock"
                )
                .into());
            }
            statements.insert(
                identity,
                (
                    reg_frame::canonical_line(&premise.subjects),
                    premise.objects,
                ),
            );
        }
        Ok(Self {
            objects,
            statements,
        })
    }

    /// One statement's reviewed premise, as the lock stored it.
    fn premise(&self, identity: &str) -> Option<LockedPremise> {
        let (subjects, keys) = self.statements.get(identity)?;
        let mut lines = keys
            .iter()
            .map(|key| &self.objects[key])
            .collect::<Vec<_>>();
        // Digest order is only a storage convention. Restore the resolver's
        // canonical object order for unchanged field-level diagnostics.
        lines.sort();
        Some(LockedPremise {
            subjects: parse_line(subjects),
            objects: lines.into_iter().map(|line| parse_line(line)).collect(),
        })
    }
}

fn parse_line<T: serde::de::DeserializeOwned>(line: &str) -> T {
    serde_json::from_str(line).expect("a canonical lock line")
}

fn object_key(object: &SourcePremise) -> String {
    reg_core::sha256_hex(reg_frame::canonical_line(object))
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LockedPremise {
    subjects: LockedSubjects,
    objects: Vec<SourcePremise>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged, deny_unknown_fields)]
enum LockedSubjects {
    Rows(Vec<SourcePremise>),
    Variable {
        variable: String,
        count: usize,
        locators_sha256: String,
    },
}

impl LockedPremise {
    fn new(statement: &LocatedStatement, premise: &Premise) -> Self {
        let variable = variable_level_setter(&statement.statement);
        let subjects = if let Some(variable) = variable {
            let locators = premise
                .subjects
                .iter()
                .map(|row| reg_frame::canonical_line(&row.locator))
                .collect::<BTreeSet<_>>();
            LockedSubjects::Variable {
                variable: variable.to_owned(),
                count: locators.len(),
                locators_sha256: reg_core::sha256_hex(
                    locators
                        .iter()
                        .map(|l| l.clone() + "\n")
                        .collect::<String>(),
                ),
            }
        } else {
            LockedSubjects::Rows(premise.subjects.clone())
        };
        let mut objects = premise
            .objects
            .iter()
            .filter(|object| {
                variable.is_none()
                    || !matches!(
                        object
                            .fields
                            .get("kind")
                            .and_then(serde_json::Value::as_str),
                        Some("register" | "table" | "release")
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        objects.sort_by_cached_key(reg_frame::canonical_line);
        Self { subjects, objects }
    }
}

fn location(statement: &LocatedStatement) -> String {
    format!("{}:{}", statement.path.display(), statement.line)
}

/// Locks live inside a register; published statements and cross-register processing
/// need that same scope in their identity, independent of file paths and line numbers.
/// `Statement::scoped_identity` from the identity the loader already computed: the
/// statement is not re-serialized per call, which cost a large share of the Fields
/// stage (#2196).
fn publication_identity(statement: &LocatedStatement) -> String {
    reg_core::sha256_hex(reg_frame::canonical_line(&(
        &statement.provider,
        &statement.register,
        &statement.identity,
    )))
}

fn statement_locator(root: &Path, statement: &LocatedStatement) -> Result<reg_frame::Locator> {
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

/// A register-scoped record's provider, register handle and locator.
// simplify: reg-resolve matches the same four variants; move this to reg_frame::Record
// once a third crate needs it.
pub(crate) fn coordinates(record: &Record) -> Option<(&String, &String, &reg_frame::Locator)> {
    match record {
        Record::Register(r) => Some((&r.provider, &r.register, &r.locator)),
        Record::Table(r) => Some((&r.provider, &r.register, &r.locator)),
        Record::Release(r) => Some((&r.provider, &r.register, &r.locator)),
        Record::Occurrence(r) => Some((&r.provider, &r.register, &r.locator)),
        Record::ValueSet(_) | Record::Link(_) | Record::Notice(_) | Record::Doc(_) => None,
    }
}

fn belongs(record: &Record, provider: &str, register: &str) -> bool {
    match coordinates(record) {
        Some((p, r, _)) => p == provider && r == register,
        // Value-set assertions and source links may depend on shared source facts.
        None => !matches!(record, Record::Doc(_)),
    }
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

/// A structural stage's records, indexed by `application_record`'s keys: a statement's
/// register coordinates and its code list. Replacing a statement's records leaves
/// tombstones and appends, so `into_records` keeps the order of a `retain` then `extend`
/// over the whole record list, without scanning it per statement (#2196).
struct Slots {
    records: Vec<Option<Record>>,
    coordinates: BTreeMap<(String, String), Vec<usize>>,
    value_sets: BTreeMap<String, Vec<usize>>,
}

impl Slots {
    fn new(records: Vec<Record>) -> Self {
        let mut slots = Self {
            records: Vec::with_capacity(records.len()),
            coordinates: BTreeMap::new(),
            value_sets: BTreeMap::new(),
        };
        slots.extend(records);
        slots
    }

    fn extend(&mut self, records: Vec<Record>) {
        for record in records {
            let at = self.records.len();
            if let Some((provider, register, _)) = coordinates(&record) {
                self.at(provider, register).push(at);
            } else if let Record::ValueSet(v) = &record {
                self.value_sets.entry(v.hash.clone()).or_default().push(at);
            }
            self.records.push(Some(record));
        }
    }

    fn at(&mut self, provider: &str, register: &str) -> &mut Vec<usize> {
        self.coordinates
            .entry((provider.to_owned(), register.to_owned()))
            .or_default()
    }

    /// Positions of `application_record(_, provider, register, hash)`, in record order.
    fn positions(&self, provider: &str, register: &str, hash: Option<&str>) -> Vec<usize> {
        let empty = Vec::new();
        let own = self
            .coordinates
            .get(&(provider.to_owned(), register.to_owned()))
            .unwrap_or(&empty);
        let set = hash.and_then(|h| self.value_sets.get(h)).unwrap_or(&empty);
        let mut positions = own.iter().chain(set).copied().collect::<Vec<_>>();
        positions.sort_unstable();
        positions
    }

    fn source(&self, provider: &str, register: &str, hash: Option<&str>) -> Vec<Record> {
        self.positions(provider, register, hash)
            .into_iter()
            .map(|at| self.records[at].clone().expect("a live position"))
            .collect()
    }

    fn replace(&mut self, provider: &str, register: &str, hash: Option<&str>, by: Vec<Record>) {
        for at in self.positions(provider, register, hash) {
            self.records[at] = None;
        }
        self.coordinates
            .remove(&(provider.to_owned(), register.to_owned()));
        if let Some(hash) = hash {
            self.value_sets.remove(hash);
        }
        self.extend(by);
    }

    fn into_records(self) -> Vec<Record> {
        self.records.into_iter().flatten().collect()
    }
}

fn ledger(
    root: &Path,
    provider: &str,
    register: &str,
    retired: &[crate::retired::Retired],
) -> Result<reg_resolve::Ledger> {
    let path = super::ledger_path(root, provider, register);
    let mut ledger: reg_resolve::Ledger = if path.is_file() {
        super::toml_file(&path)?
    } else {
        reg_resolve::Ledger::default()
    };
    let prefix = format!("{provider}/{}/", ledger.public_name(register));
    ledger.retired.extend(
        retired
            .iter()
            .filter_map(|r| r.pid.strip_prefix(&prefix).map(str::to_owned)),
    );
    Ok(ledger)
}

/// One curation stage's reference lookups: each provider's source registers with their
/// ledgers, in record order, and each source register's view. Valid while the stage's
/// records, shared sources and variable assignments stay unchanged (#2196).
#[derive(Default)]
struct ReferenceCache {
    ledgers: BTreeMap<String, Vec<(String, reg_resolve::Ledger)>>,
    views: BTreeMap<(String, String), reg_resolve::curation::ReferenceView>,
}

/// Catalog references resolve through each register's ledger, then snapshot source
/// facts. Classes/providers without source entities cannot silently acquire a premise.
#[allow(clippy::too_many_arguments)]
fn references(
    cache: &mut ReferenceCache,
    root: &Path,
    records: &[Record],
    statements: &[&LocatedStatement],
    classifications: &[reg_resolve::curation::apply::Classification],
    variables: &BTreeMap<reg_resolve::OccurrenceKey, String>,
    shared: &reg_resolve::curation::SharedSources,
    retired: &[crate::retired::Retired],
    base: Option<&crate::external::Context>,
) -> Result<BTreeMap<String, Vec<reg_resolve::curation::SourcePremise>>> {
    let mut references = BTreeMap::new();
    let objects = statements
        .iter()
        .filter_map(|s| match &s.statement {
            reg_frame::statement::Statement::Link { object, .. } => Some(object),
            reg_frame::statement::Statement::Set { fields, .. } => fields.classification.as_ref(),
            reg_frame::statement::Statement::Assert { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    for object in objects {
        if let Ok(reg_core::Fqid::ClassVersion { family, version }) = object.parse() {
            if let Some(class) = classifications
                .iter()
                .find(|c| c.family == family && c.version == version)
            {
                let mut fields = class.facts(classifications);
                fields.insert(
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
                );
                references.insert(
                    object.clone(),
                    vec![reg_resolve::curation::SourcePremise {
                        locator: class.locator.clone(),
                        fields: fields.into(),
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
        if !cache.ledgers.contains_key(&provider) {
            let mut ledgers = Vec::new();
            for source in records {
                let Record::Register(source) = source else {
                    continue;
                };
                if source.provider == provider {
                    ledgers.push((
                        source.register.clone(),
                        ledger(root, &provider, &source.register, retired)?,
                    ));
                }
            }
            cache.ledgers.insert(provider.clone(), ledgers);
        }
        let mut matches = Vec::new();
        for (source, ledger) in &cache.ledgers[&provider] {
            if ledger.public_name(source) != register {
                continue;
            }
            let own = || {
                records
                    .iter()
                    .filter(|r| application_record(r, &provider, source, None))
                    .cloned()
                    .collect::<Vec<_>>()
            };
            let view = cache
                .views
                .entry((provider.clone(), source.clone()))
                .or_insert_with(|| {
                    reg_resolve::curation::ReferenceView::new(&own(), ledger, shared)
                });
            let premise = match variable.as_deref() {
                Some(_) if !view.has_variables() => view
                    .with_variables(&own(), ledger, variables)
                    .and_then(|()| view.premise(variable.as_deref())),
                variable => view.premise(variable),
            };
            matches.push(premise);
        }
        if let [Ok(premise)] = &matches[..] {
            references.insert(object.clone(), premise.clone());
        } else if let Some(base) = base
            && let Some(premise) = base.premise(object)?
        {
            references.insert(object.clone(), premise);
        }
    }
    Ok(references)
}

fn inventory_error(
    root: &Path,
    at: &reg_frame::Locator,
    message: impl std::fmt::Display,
) -> String {
    format!(
        "{}:{}: {message}",
        root.join(&at.input).display(),
        at.at.strip_prefix("line=").unwrap_or(&at.at)
    )
}

/// Resolve code-list references before register curation, through the same constructor
/// used by inline classification assertions. No applicability is inferred here.
#[allow(clippy::too_many_lines)]
fn inventory(root: &Path, records: &[Record]) -> Result<Applied> {
    use reg_frame::statement::{AssertFact, Statement};
    let mut out = Applied::default();
    let entries = reg_readers::curation::load_classifications(root)?;
    let mut families = BTreeMap::new();
    for entry in entries {
        let c = &entry.classification;
        let at = reg_frame::Locator {
            input: "classifications.toml".into(),
            at: format!("line={}", entry.line),
        };
        let located = |message: String| inventory_error(root, &at, message);
        for (field, value) in [
            ("family_name", &c.family_name),
            ("family_description", &c.family_description),
            ("publisher", &c.publisher),
        ] {
            if let Some(value) = value {
                let key = (c.family.clone(), field);
                if let Some((previous, line)) = families.get(&key)
                    && previous != value
                {
                    return Err(located(format!(
                        "classification-family-conflict: {field} conflicts with classifications.toml:{line}"
                    )).into());
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
            )).into());
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
        class.name.clone_from(&c.name);
        class.family_name.clone_from(&c.family_name);
        class.family_description.clone_from(&c.family_description);
        class.part.clone_from(&c.part);
        class.publisher.clone_from(&c.publisher);
        class.succeeds.clone_from(&c.succeeds);
        class.sentinel_codes = c
            .sentinel_codes
            .as_ref()
            .map(|codes| codes.iter().cloned().collect());
        let hash = reg_core::sha256_hex(reg_frame::canonical_line(c));
        out.statements.push(crate::emit::StatementRow {
            hash: hash.clone(),
            kind: "assert".into(),
            file: at.input.clone(),
            line: entry.line,
            evidence: Some(c.evidence.clone()),
            caveat: None,
        });
        out.classifications.push(class);
    }
    let known = out
        .classifications
        .iter()
        .map(|c| format!("class/{}/{}", c.family, c.version))
        .collect::<BTreeSet<_>>();
    for class in &out.classifications {
        for target in &class.succeeds {
            if !known.contains(target) {
                return Err(inventory_error(
                    root,
                    &class.locator,
                    format!("classification-reference-unresolved: predecessor {target:?}"),
                )
                .into());
            }
        }
    }
    // A family's versions are grouped into parts on its page: a version with no part
    // in a family that has them would belong to no line. Fails fast here, located;
    // a register's inline version is checked against this list later.
    let mut parted = BTreeMap::<&str, (bool, &reg_frame::Locator)>::new();
    for class in &out.classifications {
        let has = class.part.is_some();
        match parted.get(class.family.as_str()) {
            Some((first, at)) if *first != has => {
                return Err(inventory_error(
                    root,
                    &class.locator,
                    format!(
                        "classification-part-partial: class/{} states part on some versions only (see {})",
                        class.family, at.at
                    ),
                )
                .into());
            }
            Some(_) => {}
            None => {
                parted.insert(&class.family, (has, &class.locator));
            }
        }
    }
    let predecessors = out
        .classifications
        .iter()
        .map(|c| {
            (
                format!("class/{}/{}", c.family, c.version),
                c.succeeds.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    // simplify: repeat the per-version walk because post-write SQL checks cannot
    // locate inventory entries; remove this walk when verify reports source locations.
    for class in &out.classifications {
        let origin = format!("class/{}/{}", class.family, class.version);
        let mut pending = predecessors[&origin].clone();
        let mut visited = BTreeSet::new();
        while let Some(node) = pending.pop() {
            if node == origin {
                return Err(inventory_error(
                    root,
                    &class.locator,
                    format!("classification-succession-cycle: {origin}"),
                )
                .into());
            }
            if visited.insert(node.clone()) {
                pending.extend(predecessors[&node].iter().cloned());
            }
        }
    }
    for class in &out.classifications {
        let statement = crate::emit::StatementRow::for_locator(&out.statements, &class.locator)
            .expect("inventory classifications have statements");
        let facts = class.facts(&out.classifications);
        for (field, value) in facts {
            out.overrides.push(reg_resolve::provenance::Claim {
                change: reg_resolve::curation::apply::Change {
                    kind: "class_version".into(),
                    locator: class.locator.clone(),
                    identity: None,
                    provider: None,
                    register: None,
                    table: None,
                    release: None,
                    period: Some(class.period.clone()),
                    field,
                    value: reg_resolve::curation::apply::rendered(&value),
                    source_value: None,
                },
                statement: statement.hash.clone(),
            });
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

pub(super) struct Sentinels {
    pub hash: String,
    pub codes: BTreeSet<String>,
    pub locator: reg_frame::Locator,
    pub statement: String,
    pub inventory: Option<reg_frame::Locator>,
}

/// Classification exemptions describe observed column sets, never the canonical book.
fn bound_sentinels(records: &[Record], applied: &mut Applied) -> Result<Vec<String>> {
    let sets = records
        .iter()
        .filter_map(|record| match record {
            Record::ValueSet(set) => Some((set.hash.as_str(), set)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut bindings = BTreeMap::<_, BTreeSet<_>>::new();
    for claim in applied
        .overrides
        .iter()
        .filter(|claim| claim.change.kind == "occurrence" && claim.change.field == "classification")
    {
        if let Some(class) = claim.change.value.as_deref() {
            bindings
                .entry((&claim.change.locator, class))
                .or_default()
                .insert(claim.statement.as_str());
        }
    }
    let mut bound = BTreeMap::<_, BTreeSet<_>>::new();
    for record in records {
        let Record::Occurrence(occurrence) = record else {
            continue;
        };
        let Some(class) = occurrence.classification.as_deref() else {
            continue;
        };
        let statements = std::iter::once(&occurrence.locator)
            .chain(&occurrence.also)
            .filter_map(|at| bindings.get(&(at, class)))
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        for hash in occurrence
            .valueset
            .iter()
            .chain(occurrence.population_valuesets.iter().map(|p| &p.valueset))
        {
            if statements.is_empty() {
                return Err(format!(
                    "{}:{}: classification-sentinel-binding-missing: {class} has no curated binding statement",
                    occurrence.locator.input, occurrence.locator.at
                ).into());
            }
            bound
                .entry((class, hash.as_str()))
                .or_default()
                .extend(statements.iter().copied());
        }
    }
    let rows = applied
        .statements
        .iter()
        .map(|statement| (statement.hash.as_str(), statement))
        .collect::<BTreeMap<_, _>>();
    let mut findings = Vec::new();
    for class in &applied.classifications {
        let fqid = format!("class/{}/{}", class.family, class.version);
        let declared = class.sentinel_codes.clone().unwrap_or_default();
        let mut used = BTreeSet::new();
        let mut n_sets = 0;
        for ((_, hash), statements) in bound.iter().filter(|((version, _), _)| *version == fqid) {
            let set = sets
                .get(hash)
                .ok_or_else(|| format!("classification-sentinel-valueset-missing: {hash}"))?;
            let codes = declared
                .iter()
                .filter(|code| set.items.iter().any(|item| &item.code == *code))
                .cloned()
                .collect::<BTreeSet<_>>();
            used.extend(codes.iter().cloned());
            n_sets += 1;
            for statement in statements {
                let row = rows.get(statement).ok_or_else(|| {
                    format!("classification-sentinel-binding-missing: {statement}")
                })?;
                applied.sentinels.push(Sentinels {
                    hash: (*hash).into(),
                    codes: codes.clone(),
                    locator: reg_frame::Locator {
                        input: row.file.clone(),
                        at: format!("line={}", row.line),
                    },
                    statement: (*statement).into(),
                    inventory: Some(class.locator.clone()),
                });
            }
        }
        let unused = declared.difference(&used).count();
        if unused > 0 {
            findings.push(format!("{}:{}: classification-sentinels-unused: {unused} declared codes absent from all bound value sets ({fqid}; {n_sets} bound sets)", class.locator.input, class.locator.at));
        }
    }
    Ok(findings)
}

/// Local setters are complete claims; classification lists are additive exemptions.
#[allow(clippy::too_many_lines)]
fn sentinel_conflicts(
    root: &Path,
    records: &[Record],
    applied: &Applied,
) -> Result<(BTreeSet<String>, Vec<String>)> {
    let sets = records
        .iter()
        .filter_map(|record| match record {
            Record::ValueSet(set) => Some((set.hash.as_str(), set)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let classes = applied
        .classifications
        .iter()
        .map(|class| (&class.locator, class))
        .collect::<BTreeMap<_, _>>();
    let mut canonical = BTreeMap::new();
    for (at, class) in &classes {
        let set = sets.get(class.valueset.as_str()).ok_or_else(|| {
            inventory_error(
                root,
                at,
                format!(
                    "classification-reference-unresolved: value set {}",
                    class.valueset
                ),
            )
        })?;
        canonical.insert(
            *at,
            set.items
                .iter()
                .map(|item| item.code.as_str())
                .collect::<BTreeSet<_>>(),
        );
    }
    let mut groups = BTreeMap::<&str, Vec<&Sentinels>>::new();
    for declaration in &applied.sentinels {
        groups
            .entry(&declaration.hash)
            .or_default()
            .push(declaration);
    }
    let mut conflicts = BTreeSet::new();
    let mut refusals = BTreeSet::new();
    let mut refuse = |declaration: &Sentinels, other: &Sentinels, detail: String| {
        conflicts.insert(declaration.statement.clone());
        refusals.insert(inventory_error(
            root,
            &declaration.locator,
            format!(
                "sentinel-conflict: shared value set {} {detail}; other binding {}:{}",
                declaration.hash,
                other.locator.input,
                other
                    .locator
                    .at
                    .strip_prefix("line=")
                    .unwrap_or(&other.locator.at),
            ),
        ));
    };
    for declarations in groups.values() {
        let local = declarations
            .iter()
            .copied()
            .filter(|d| d.inventory.is_none())
            .collect::<Vec<_>>();
        if let Some(first) = local.first()
            && let Some(rival) = local.iter().find(|d| d.codes != first.codes)
        {
            for declaration in &local {
                let other = if declaration.codes == first.codes {
                    *rival
                } else {
                    *first
                };
                refuse(
                    declaration,
                    other,
                    "has disagreeing local sentinel lists".into(),
                );
            }
        }
        let mut versions = BTreeMap::<_, Vec<_>>::new();
        for declaration in declarations {
            if let Some(at) = &declaration.inventory {
                versions.entry(at).or_default().push(*declaration);
            }
        }
        for (at, claims) in &versions {
            for (other_at, other_claims) in &versions {
                let (class, other) = (classes[at], classes[other_at]);
                let bad = claims[0]
                    .codes
                    .iter()
                    .filter(|code| canonical[other_at].contains(code.as_str()))
                    .collect::<Vec<_>>();
                if bad.is_empty() {
                    continue;
                }
                let detail = format!(
                    "lists codes {bad:?} from class/{}/{} that are canonical in class/{}/{} (inventory {}:{} and {}:{})",
                    class.family,
                    class.version,
                    other.family,
                    other.version,
                    at.input,
                    at.at.strip_prefix("line=").unwrap_or(&at.at),
                    other_at.input,
                    other_at.at.strip_prefix("line=").unwrap_or(&other_at.at),
                );
                for declaration in claims {
                    refuse(declaration, other_claims[0], detail.clone());
                }
                for declaration in other_claims {
                    refuse(declaration, claims[0], detail.clone());
                }
            }
        }
    }
    Ok((conflicts, refusals.into_iter().collect()))
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Refusal {
    pub register: Option<String>,
    pub message: String,
}

impl Refusal {
    fn scoped(statement: &LocatedStatement, message: String) -> Self {
        Self {
            register: Some(format!("{}/{}", statement.provider, statement.register)),
            message,
        }
    }
}

fn refusal_findings(refusals: &[Refusal], record: bool) -> Result<Vec<String>> {
    if record && !refusals.is_empty() {
        return Err(refusals
            .iter()
            .map(|r| r.message.as_str())
            .collect::<Vec<_>>()
            .join("\n")
            .into());
    }
    Ok(refusals
        .iter()
        .map(|r| format!("{} (statement skipped)", r.message))
        .collect())
}

/// The accepted frame and its curated provenance. No reader output is mutated.
#[derive(Default)]
pub(super) struct Applied {
    pub sets: Vec<reg_resolve::curation::apply::SetPresentation>,
    pub records: Vec<Record>,
    pub findings: Vec<String>,
    pub refusals: Vec<Refusal>,
    pub statements: Vec<crate::emit::StatementRow>,
    pub overrides: Vec<reg_resolve::provenance::Claim>,
    pub classifications: Vec<reg_resolve::curation::apply::Classification>,
    pub variables: BTreeMap<reg_resolve::OccurrenceKey, String>,
    pub metadata: Vec<reg_resolve::curation::apply::Metadata>,
    pub sentinels: Vec<Sentinels>,
    pub links: Vec<(LocatedStatement, Premise)>,
    pub linked: Option<crate::link::Curated>,
    /// The final records resolved, once computed: later checks and the build reuse it
    /// rather than resolving the whole catalog again (#2196).
    pub resolved: Option<Vec<reg_resolve::Resolved>>,
}

/// Every curated sentinel code by value-set hash.
pub(super) fn sentinel_codes<'a>(
    declarations: impl IntoIterator<Item = &'a Sentinels>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut sets = BTreeMap::<_, BTreeSet<_>>::new();
    for declaration in declarations {
        sets.entry(declaration.hash.clone())
            .or_default()
            .extend(declaration.codes.iter().cloned());
    }
    sets
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Parents,
    Coverage,
    Periods,
    Moves,
    Facts,
    Variables,
    Fields,
    Sentinels,
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
        Statement::Set { fields, .. } if fields.sentinels.is_some() => Phase::Sentinels,
        Statement::Link { .. } => Phase::Relations,
        Statement::Set { .. } => Phase::Fields,
    }
}

/// Keep field applications small and based on the same immutable phase evidence.
fn field_application(
    records: &[Record],
    statement: &LocatedStatement,
    ledger: &reg_resolve::Ledger,
    premise: &Premise,
    locators: &BTreeMap<&reg_frame::Locator, Vec<usize>>,
    root: &Path,
) -> Result<(
    Vec<usize>,
    Vec<FieldDelta>,
    reg_resolve::curation::apply::Applied,
)> {
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
    let mut applied =
        reg_resolve::curation::apply::apply(&source, ledger, &statement.statement, &at, premise)?;
    // Unchanged dependency records need no merge; retaining them repeats source text
    // once per setter. Keep metadata and provenance even when no record changed. A
    // changed record keeps only its changed fields, not both full copies (#2196).
    let mut changed_indices = Vec::new();
    let mut deltas = Vec::new();
    for ((index, before), after) in indices
        .into_iter()
        .zip(source)
        .zip(std::mem::take(&mut applied.records))
    {
        if before != after {
            changed_indices.push(index);
            deltas.push(field_delta(&before, &after)?);
        }
    }
    Ok((changed_indices, deltas, applied))
}

/// The serialized fields of a set's output record that differ from its source record, in
/// the output's field order.
type FieldDelta = Vec<(String, serde_json::Value)>;

fn field_delta(before: &Record, after: &Record) -> Result<FieldDelta> {
    let before = serde_json::to_value(before)?;
    let after = serde_json::to_value(after)?;
    let serde_json::Value::Object(after) = after else {
        unreachable!("frame records serialize as objects");
    };
    Ok(after
        .into_iter()
        .filter(|(field, value)| before.get(field) != Some(value))
        .collect())
}

/// The subject fields that the checks after a field stage read: coordinate collisions,
/// assignment validation (`affects`) and applied conflicts (`same_subject`, spans).
/// Keep this list in step with those readers.
const COORDINATE_FIELDS: [&str; 8] = [
    "kind", "provider", "register", "table", "release", "spelling", "key", "period",
];

/// A premise reduced to its subjects' locators and coordinate fields. Absent fields stay
/// absent, and subjects that shared one field map still share one.
fn coordinate_premise(premise: Premise) -> Premise {
    type Fields = std::sync::Arc<BTreeMap<String, serde_json::Value>>;
    // Keyed by the source map's address; the entry holds the source `Arc`, so that
    // address cannot be freed and reused while projecting.
    let mut projected =
        BTreeMap::<*const BTreeMap<String, serde_json::Value>, (Fields, Fields)>::new();
    let subjects = premise
        .subjects
        .into_iter()
        .map(|subject| {
            let (_, fields) = projected
                .entry(std::sync::Arc::as_ptr(&subject.fields))
                .or_insert_with(|| {
                    let fields = COORDINATE_FIELDS
                        .iter()
                        .filter_map(|&key| {
                            subject.fields.get(key).map(|v| (key.to_owned(), v.clone()))
                        })
                        .collect::<BTreeMap<_, _>>();
                    (subject.fields.clone(), std::sync::Arc::new(fields))
                });
            reg_resolve::curation::SourcePremise {
                locator: subject.locator,
                fields: fields.clone(),
            }
        })
        .collect();
    Premise {
        subjects,
        objects: Vec::new(),
    }
}

/// Independent fields merge as patches, never as whole-record replacements.
fn merge_fields(current: &Record, delta: &FieldDelta) -> Result<Record> {
    let mut current = serde_json::to_value(current)?;
    for (field, value) in delta {
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
    Ok(serde_json::from_value(current)?)
}

fn previous(path: &Path, record: bool) -> Result<Reviewed> {
    if record || !path.try_exists()? {
        return Ok(Reviewed::default());
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| format!("{}: invalid premise lock: {e}", path.display()))?;
    if value["version"] != 3 {
        return Err(format!(
            "{}: unsupported premise lock version {}; run reg-build lock to re-lock with version 3",
            path.display(),
            value["version"]
        )
        .into());
    }
    let lock: Lock = serde_json::from_value(value)
        .map_err(|e| format!("{}: invalid premise lock: {e}", path.display()))?;
    Ok(
        Reviewed::new(lock)
            .map_err(|e| format!("{}: invalid premise lock: {e}", path.display()))?,
    )
}

fn checked_premise(
    statement: &LocatedStatement,
    premise: &Premise,
    old: &Reviewed,
    path: &Path,
) -> Result<()> {
    let premise = LockedPremise::new(statement, premise);
    match old.premise(&statement.identity).as_ref() {
        None => Err(format!(
            "premise-missing: no reviewed premise in {}; run reg-build lock",
            path.display()
        )
        .into()),
        Some(previous) if previous != &premise => {
            let mut diff = Vec::new();
            if let (
                LockedSubjects::Variable {
                    count: old_count, ..
                },
                LockedSubjects::Variable {
                    variable, count, ..
                },
            ) = (&previous.subjects, &premise.subjects)
                && previous.subjects != premise.subjects
            {
                diff.push(format!(
                    "var:{variable}: matched rows changed (old count {old_count}, new count {count})"
                ));
            }
            changes(
                &serde_json::to_value(previous)?,
                &serde_json::to_value(&premise)?,
                "premise",
                &mut diff,
            );
            Err(format!("premise-changed:\n  {}", diff.join("\n  ")).into())
        }
        Some(_) => Ok(()),
    }
}

pub(super) fn run(
    root: &Path,
    retired: &[crate::retired::Retired],
    statements: &[LocatedStatement],
    outputs: &[reg_readers::ReadOutput],
    record: bool,
    registers: &[String],
    base: Option<&crate::external::Context>,
) -> Result<Applied> {
    let mut applied = run_selected(
        root,
        retired,
        &statements.iter().collect::<Vec<_>>(),
        outputs,
        record,
        registers,
        base,
    )?;
    applied.refusals.sort();
    applied.refusals.dedup();
    Ok(applied)
}

/// Apply parent, bound, period, move, fact, identity and field stages in order. Each phase
/// evaluates an immutable snapshot before applying accepted statements. Other kinds
/// see only that resulting source view. `record` is explicit review acceptance.
#[allow(clippy::too_many_lines)]
fn run_selected(
    root: &Path,
    retired: &[crate::retired::Retired],
    statements: &[&LocatedStatement],
    outputs: &[reg_readers::ReadOutput],
    record: bool,
    registers: &[String],
    base: Option<&crate::external::Context>,
) -> Result<Applied> {
    // Refused statements contribute neither facts nor provenance: the stages run again
    // without them, carrying the refusals and their skipped-statement findings.
    let rerun_without = |refused: &dyn Fn(&LocatedStatement) -> bool, refusals: Vec<Refusal>| {
        let skipped = refusal_findings(&refusals, record)?;
        let retained = statements
            .iter()
            .copied()
            .filter(|s| !refused(s))
            .collect::<Vec<_>>();
        let mut rerun = run_selected(root, retired, &retained, outputs, record, registers, base)?;
        rerun.findings.extend(skipped);
        rerun.refusals.extend(refusals);
        Ok::<_, crate::error::Error>(rerun)
    };
    let t = std::time::Instant::now();
    let mut records = outputs
        .iter()
        .flat_map(|o| o.frame.records.iter().cloned())
        .collect::<Vec<_>>();
    let mut result = inventory(root, &records)?;
    crate::timing::done("curation inventory", t);
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
        let ledger = ledger(root, provider, register, retired)?;
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
        Phase::Sentinels,
        Phase::Relations,
    ] {
        let fields_stage = matches!(
            stage,
            Phase::Coverage | Phase::Periods | Phase::Fields | Phase::Sentinels
        );
        let structural_stage = matches!(stage, Phase::Parents | Phase::Moves | Phase::Facts);
        let mut field_snapshot = Vec::new();
        if !conflicts.is_empty() {
            break;
        }
        let stage_start = std::time::Instant::now();
        let (mut in_references, mut in_resolve) = (Duration::ZERO, Duration::ZERO);
        let mut reference_cache = ReferenceCache::default();

        let shared = if statements.iter().any(|s| phase(&s.statement) == stage) {
            reg_resolve::curation::SharedSources::new(&records)
        } else {
            reg_resolve::curation::SharedSources::default()
        };
        // Evaluate every register, even when recording only selected locks: linked
        // premises must see the same curated facts as an ordinary build.
        let mut accepted = Vec::new();
        let mut field_applications = BTreeMap::new();
        if fields_stage
            && statements.iter().any(|s| {
                matches!(s.statement, reg_frame::statement::Statement::Set { .. })
                    && phase(&s.statement) == stage
            })
        {
            field_snapshot = reg_resolve::curation::apply::source_keys(
                &records
                    .iter()
                    .filter(|r| coordinates(r).is_some())
                    .collect::<Vec<_>>(),
            );
        }
        // Borrowed from `records`, which the group loop below only reads.
        let mut locators = BTreeMap::<_, Vec<_>>::new();
        if fields_stage {
            for (i, record) in records.iter().enumerate() {
                let at = match record {
                    Record::ValueSet(r) => Some(&r.locator),
                    _ => coordinates(record).map(|(.., at)| at),
                };
                if let Some(at) = at {
                    locators.entry(at).or_default().push(i);
                }
                if let Record::Occurrence(r) = record {
                    for at in &r.also {
                        locators.entry(at).or_default().push(i);
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
            if matches!(stage, Phase::Fields | Phase::Sentinels)
                && group
                    .iter()
                    .any(|s| matches!(s.statement, reg_frame::statement::Statement::Set { .. }))
            {
                let assigned = result.variables.len();
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
                // Views resolved variables under the old assignments.
                if result.variables.len() != assigned {
                    reference_cache.views.clear();
                }
            }
            let content = group.iter().map(|s| &s.statement).collect::<Vec<_>>();
            let t = std::time::Instant::now();
            let references = references(
                &mut reference_cache,
                root,
                &records,
                &group,
                &result.classifications,
                &result.variables,
                &shared,
                retired,
                base,
            )?;
            in_references += t.elapsed();
            let t = std::time::Instant::now();
            let evaluations = reg_resolve::curation::resolve(
                &source,
                ledger,
                &content,
                &references,
                &result.variables,
                &shared,
            );
            in_resolve += t.elapsed();
            for (statement, evaluation) in group.iter().zip(evaluations) {
                let at = location(statement);
                let resolved = match evaluation {
                    Ok(r) => r,
                    Err(e) => {
                        refusals.push(Refusal::scoped(statement, format!("{at}: {e}")));
                        continue;
                    }
                };
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
                            refusals.push(Refusal::scoped(statement, format!("{at}: {error}")));
                            continue;
                        }
                    }
                }
                if !resolved.conflicts.is_empty() {
                    for other in &resolved.conflicts {
                        refusals.push(Refusal::scoped(
                            statement,
                            format!(
                                "{at}: curation-overlap: overlapping overrides with {}",
                                location(group[*other])
                            ),
                        ));
                    }
                    continue;
                }
                if !record && let Err(e) = checked_premise(statement, &resolved.premise, old, path)
                {
                    refusals.push(Refusal::scoped(statement, format!("{at}: {e}")));
                    continue;
                }
                if resolved.redundant {
                    findings.push(format!("{at}: redundant-set: set equals the source"));
                }
                // A build applies field-stage statements from `field_applications`, so
                // later readers need only each subject's coordinates (#2196).
                let premise = if fields_stage && !record {
                    coordinate_premise(resolved.premise)
                } else {
                    resolved.premise
                };
                accepted.push((*statement, premise, provider, register));
            }
            if stage == Phase::Fields {
                crate::sets::memberships(
                    provider,
                    ledger.public_name(register),
                    &result.variables,
                    group.iter().flat_map(|statement| {
                        field_applications
                            .get(&publication_identity(statement))
                            .into_iter()
                            .flat_map(|(_, _, applied)| {
                                applied
                                    .metadata
                                    .iter()
                                    .map(|metadata| (*statement, metadata))
                            })
                    }),
                )?;
            }
        }
        accepted.sort_by_cached_key(|(s, ..)| {
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
        let mut slots = structural_stage.then(|| Slots::new(std::mem::take(&mut records)));
        for (statement, premise, provider, register) in accepted {
            let (_, ledger, _) = &locks[&(provider, register)];
            let locator = statement_locator(root, statement)?;
            let asserted =
                reg_resolve::curation::apply::asserted_value_set(&statement.statement, &locator);
            let hash = asserted.as_ref().map(|set| set.hash.as_str());
            let source = slots
                .as_ref()
                .map_or_else(Vec::new, |slots| slots.source(provider, register, hash));
            if stage == Phase::Relations {
                result.links.push(((*statement).clone(), premise.clone()));
            }
            let application = match stage {
                Phase::Variables => assignments
                    .remove(&publication_identity(statement))
                    .expect("evaluated assignment"),
                Phase::Relations => Ok(reg_resolve::curation::apply::Applied::default()),
                Phase::Coverage | Phase::Periods | Phase::Fields | Phase::Sentinels => {
                    if let Some((indices, deltas, applied)) =
                        field_applications.remove(&publication_identity(statement))
                    {
                        for (index, delta) in indices.into_iter().zip(&deltas) {
                            records[index] = merge_fields(&records[index], delta)?;
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
                        &reg_resolve::curation::apply::source_keys(
                            &source.iter().collect::<Vec<_>>(),
                        ),
                        &applied.records.iter().collect::<Vec<_>>(),
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
                    let refusal =
                        Refusal::scoped(statement, format!("{}: {e}", location(statement)));
                    refusals.push(refusal);
                    let refused = publication_identity(statement);
                    return rerun_without(&|s| publication_identity(s) == refused, refusals);
                }
            };
            result.sets.extend(applied.sets);
            result.variables.extend(applied.assignments);
            result.metadata.extend(applied.metadata);
            result.sentinels.extend(
                applied
                    .sentinels
                    .into_iter()
                    .map(|(hash, codes)| Sentinels {
                        hash,
                        codes,
                        locator: locator.clone(),
                        statement: publication_identity(statement),
                        inventory: None,
                    }),
            );
            // Only `lock` writes reviewed premises; a build keeps none (#2196).
            if record {
                premises.entry((provider, register)).or_default().insert(
                    statement.identity.clone(),
                    LockedPremise::new(statement, &premise),
                );
            }
            evaluated.push((statement, premise));
            if !applied.changes.is_empty() {
                result.statements.push(statement_row(root, statement)?);
                result
                    .overrides
                    .extend(
                        applied
                            .changes
                            .iter()
                            .map(|change| reg_resolve::provenance::Claim {
                                change: change.clone(),
                                statement: publication_identity(statement),
                            }),
                    );
                result
                    .classifications
                    .extend(applied.classifications.iter().cloned());
            }
            if let Some(slots) = &mut slots {
                slots.replace(provider, register, hash, applied.records);
            }
        }
        if let Some(slots) = slots {
            records = slots.into_records();
        }
        if fields_stage && !field_snapshot.is_empty() {
            if stage == Phase::Fields {
                // Conflict checks retain the field phase view; links see final identities.
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
                        field_snapshot.iter().flatten().any(|key| {
                            let row = &key.identity;
                            key.locators.contains(&at)
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
                        refusals.push(Refusal::scoped(statement, format!(
                            "{}: curation-coordinate-collision: set coordinates collide at {}:{}",
                            location(statement),
                            at.input,
                            at.at
                        )));
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
                    for (statement, premise) in assignments {
                        if error.affects(premise) {
                            conflicts.insert(publication_identity(statement));
                            refusals.push(Refusal::scoped(
                                statement,
                                format!("{}: {}", location(statement), error.message),
                            ));
                        }
                    }
                }
            }
            if !conflicts.is_empty() {
                break;
            }
        }
        crate::timing::done(
            &format!(
                "curation stage {stage:?} (references {:.1}s, statement resolve {:.1}s)",
                in_references.as_secs_f64(),
                in_resolve.as_secs_f64()
            ),
            stage_start,
        );
    }
    let t = std::time::Instant::now();
    if !result.links.is_empty() {
        let resolved = resolve_catalog(
            root,
            retired,
            &records,
            &result.variables,
            &result.metadata,
            &result.classifications,
        )?;
        crate::timing::done("curation links: resolve catalog", t);
        let linked = crate::link::curate(root, &resolved, &records, &result.links, base)?;
        if !linked.refusals.is_empty() {
            let messages = linked
                .refusals
                .iter()
                .map(|(hash, message)| Refusal {
                    register: result
                        .links
                        .iter()
                        .find(|(statement, _)| publication_identity(statement) == *hash)
                        .map(|(statement, _)| {
                            format!("{}/{}", statement.provider, statement.register)
                        }),
                    message: message.clone(),
                })
                .collect::<Vec<_>>();
            if record {
                refusal_findings(&messages, record)?;
            }
            let refused = linked
                .refusals
                .iter()
                .map(|(hash, _)| hash)
                .collect::<BTreeSet<_>>();
            refusals.extend(messages);
            return rerun_without(&|s| refused.contains(&publication_identity(s)), refusals);
        }
        result.linked = Some(linked);
        result.resolved = Some(resolved);
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
        .filter(|claim| {
            !(claim.change.kind == "class_version"
                && claim.change.locator.input == "classifications.toml")
        })
        .map(|claim| (indices[claim.statement.as_str()], &claim.change))
        .collect::<Vec<_>>();
    let resolved = evaluated
        .iter()
        .map(|(s, p)| (&s.statement, p))
        .collect::<Vec<_>>();
    for (a, b) in reg_resolve::curation::applied_conflicts(&resolved, &claims) {
        let (left, right) = (evaluated[a].0, evaluated[b].0);
        conflicts.insert(publication_identity(left));
        conflicts.insert(publication_identity(right));
        refusals.push(Refusal::scoped(
            left,
            format!(
                "{}: curation-overlap: overlapping overrides with {}",
                location(left),
                location(right)
            ),
        ));
    }
    for (a, b) in reg_resolve::curation::classification_conflicts(&result.classifications) {
        let (left, right) = (&result.classifications[a], &result.classifications[b]);
        for class in [left, right] {
            if let Some(statement) =
                crate::emit::StatementRow::for_locator(&result.statements, &class.locator)
            {
                conflicts.insert(statement.hash.clone());
            }
        }
        refusals.push(Refusal {
            register: None,
            message: format!(
                "{}:{}: curation-overlap: classification version overlaps {}:{}",
                left.locator.input, left.locator.at, right.locator.input, right.locator.at
            ),
        });
    }
    // A register's inline `assert classification-version` states no part, so it may
    // break the part rule of a family classifications.toml gives parts. The inventory
    // comes first in the list and already holds the rule (`inventory`), so the
    // offending entry is always a register statement, which a rerun drops.
    let mut parted = BTreeMap::<&str, &reg_resolve::curation::apply::Classification>::new();
    for class in &result.classifications {
        let first = *parted.entry(class.family.as_str()).or_insert(class);
        if first.part.is_some() == class.part.is_some() {
            continue;
        }
        if let Some(statement) =
            crate::emit::StatementRow::for_locator(&result.statements, &class.locator)
        {
            conflicts.insert(statement.hash.clone());
        }
        refusals.push(Refusal {
            register: None,
            message: format!(
                "{}:{}: classification-part-partial: class/{} states part on some versions only (see {}:{})",
                class.locator.input,
                class.locator.at,
                class.family,
                first.locator.input,
                first.locator.at
            ),
        });
    }
    findings.extend(bound_sentinels(&records, &mut result)?);
    let (sentinel_conflicts, sentinel_refusals) = sentinel_conflicts(root, &records, &result)?;
    conflicts.extend(sentinel_conflicts);
    refusals.extend(sentinel_refusals.into_iter().map(|message| Refusal {
        register: None,
        message,
    }));
    crate::timing::done("curation links and conflicts", t);
    if !conflicts.is_empty() {
        eprintln!(
            "reg-build: timing curation rerun after {} conflicts",
            conflicts.len()
        );
        return rerun_without(&|s| conflicts.contains(&publication_identity(s)), refusals);
    }

    if !result.sets.is_empty()
        || result.metadata.iter().any(|m| {
            matches!(
                m.value,
                reg_resolve::curation::apply::MetadataValue::Facets { .. }
            )
        })
    {
        let t = std::time::Instant::now();
        let resolved = match result.resolved.take() {
            Some(resolved) => resolved,
            None => resolve_catalog(
                root,
                retired,
                &records,
                &result.variables,
                &result.metadata,
                &result.classifications,
            )?,
        };
        let invalid = reg_resolve::curation::validate_set_presentation(&resolved, &result.sets);
        result.resolved = Some(resolved);
        crate::timing::done("curation set presentation", t);
        if !invalid.is_empty() {
            let by_location = evaluated
                .iter()
                .map(|(statement, _)| statement_locator(root, statement).map(|at| (at, *statement)))
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let mut refused = BTreeSet::new();
            for error in invalid {
                let statement = by_location.get(&error.at).ok_or_else(|| {
                    format!(
                        "{}:{}: presentation refusal has no originating statement",
                        error.at.input, error.at.at
                    )
                })?;
                refused.insert(publication_identity(statement));
                refusals.push(Refusal::scoped(
                    statement,
                    format!("{}:{}: {}", error.at.input, error.at.at, error.message),
                ));
            }
            eprintln!(
                "reg-build: timing curation rerun after {} presentation refusals",
                refused.len()
            );
            return rerun_without(&|s| refused.contains(&publication_identity(s)), refusals);
        }
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
        pending.push((path, Lock::new(reviewed)));
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
                    std::fs::read_dir(root.join("registers"))?
                        .filter_map(Result::ok)
                        .any(|p| p.path().join(register).is_dir())
                };
                if !found {
                    return Err(format!("unknown curation register {register:?}").into());
                }
            }
        }
    }
    findings.extend(refusal_findings(&refusals, record)?);
    result.refusals = refusals;
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
    result.findings = findings;
    result.records = records;
    Ok(result)
}

/// The complete review surface for publication.
pub(super) fn complete_findings(
    root: &Path,
    retired: &[crate::retired::Retired],
    records: &[Record],
    applied: &Applied,
    resolved: Option<Vec<reg_resolve::Resolved>>,
    base: Option<&crate::external::Context>,
) -> Result<Vec<reg_resolve::Resolved>> {
    let t = std::time::Instant::now();
    let mut resolved = match resolved {
        Some(resolved) => resolved,
        None => resolve_catalog(
            root,
            retired,
            records,
            &applied.variables,
            &applied.metadata,
            &applied.classifications,
        )?,
    };
    crate::timing::done("findings: resolve catalog", t);
    let t = std::time::Instant::now();
    let mut sets = BTreeMap::new();
    for record in records {
        if let Record::ValueSet(set) = record {
            sets.entry(set.hash.as_str()).or_insert(set);
        }
    }
    let mut linked = crate::link::resolve(&resolved, records, &[])?;
    crate::timing::done("findings: link", t);
    let t = std::time::Instant::now();
    // The curation run already curated these links against the same records.
    if let Some(curated) = &applied.linked {
        linked.relations.clone_from(&curated.relations);
    } else if !applied.links.is_empty() {
        linked.relations =
            crate::link::curate(root, &resolved, records, &applied.links, base)?.relations;
    }
    crate::timing::done("findings: curate links", t);
    let t = std::time::Instant::now();
    crate::proposals::generate(&mut resolved, &applied.classifications, &sets, &linked)?;
    crate::timing::done("findings: proposals", t);
    Ok(resolved)
}

/// Resolve the curated source snapshot for post-resolve curation validation.
pub(super) fn resolve_catalog(
    root: &Path,
    retired: &[crate::retired::Retired],
    records: &[Record],
    variables: &BTreeMap<reg_resolve::OccurrenceKey, String>,
    metadata: &[reg_resolve::curation::apply::Metadata],
    classifications: &[reg_resolve::curation::apply::Classification],
) -> Result<Vec<reg_resolve::Resolved>> {
    let sets = records
        .iter()
        .filter_map(|r| match r {
            Record::ValueSet(v) => Some((v.hash.as_str(), v)),
            _ => None,
        })
        .collect();
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for record in records {
        let Some((provider, register, _)) = coordinates(record) else {
            continue;
        };
        groups
            .entry((provider, register))
            .or_default()
            .push(record.clone());
    }
    groups
        .into_iter()
        .map(|((provider, register), source)| {
            let ledger = ledger(root, provider, register, retired)?;
            let mut resolved =
                reg_resolve::resolve(&source, &ledger, &sets, variables, classifications)?;
            reg_resolve::curation::apply_metadata(&mut resolved, metadata)?;
            Ok(resolved)
        })
        .collect()
}

pub(super) fn statement_row(
    root: &Path,
    statement: &LocatedStatement,
) -> Result<crate::emit::StatementRow> {
    let value = serde_json::to_value(&statement.statement)?;
    Ok(crate::emit::StatementRow {
        hash: publication_identity(statement),
        kind: value["kind"].as_str().expect("statement kind").into(),
        file: statement_locator(root, statement)?.input,
        line: statement.line,
        evidence: value["evidence"].as_str().map(str::to_owned),
        caveat: value["caveat"].as_str().map(str::to_owned),
    })
}
