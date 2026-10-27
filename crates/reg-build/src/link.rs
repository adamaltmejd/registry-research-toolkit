//! The link stage (`crates/DESIGN.md`, "The link stage"): source-stated links and notices,
//! matched against every resolved register's public surface. It decides only endpoint
//! matching and rewrites no link into another kind. A malformed record is refused; an
//! endpoint that matches nothing, or too much, is one warning line.

use std::collections::{BTreeMap, BTreeSet};

use reg_frame::{Endpoint, Locator, Record};
use reg_resolve::Resolved;

/// A matched endpoint, by index into the resolved registers and their entities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Entity {
    Register(usize),
    Table(usize, usize),
    Release(usize, usize),
    Variable(usize, usize),
}

impl Entity {
    /// The `subject_kind` / `object_kind` the schema writes.
    pub fn kind(self) -> &'static str {
        match self {
            Entity::Register(_) => "register",
            Entity::Table(..) => "table",
            Entity::Release(..) => "release",
            Entity::Variable(..) => "variable",
        }
    }

    /// The index of the entity's register.
    pub fn register(self) -> usize {
        match self {
            Entity::Register(r)
            | Entity::Table(r, _)
            | Entity::Release(r, _)
            | Entity::Variable(r, _) => r,
        }
    }
}

/// One `relation` row before row ids.
pub struct Relation {
    pub kind: &'static str,
    pub subject: Entity,
    pub object: Entity,
    pub at: Option<String>,
    pub note: Option<String>,
}

/// One notice finding before row ids.
pub struct Notice {
    pub subject: Entity,
    pub at: Option<String>,
    pub text: String,
}

#[derive(Default)]
pub struct Linked {
    /// Distinct by (kind, subject, object, at), sorted by it.
    pub relations: Vec<Relation>,
    /// Distinct, sorted.
    pub notices: Vec<Notice>,
    /// One line per unresolved link or notice, in record order.
    pub warnings: Vec<String>,
}

const LINK_KINDS: [&str; 2] = ["continues", "derived_from"];
const ENDPOINT_KINDS: [&str; 4] = ["register", "table", "release", "variable"];

/// Match every link and notice of `records` against `registers`.
///
/// # Errors
/// A record the stage cannot read: an unknown link or endpoint kind, an endpoint without
/// exactly one of `register` and `register_name` or without the handle its kind needs,
/// or an `at` that is not an ISO day.
pub fn resolve(registers: &[Resolved], records: &[Record]) -> Result<Linked, String> {
    let index = Index::new(registers);
    let mut linked = Linked::default();
    let mut relations: BTreeMap<(&'static str, Entity, Entity, Option<String>), _> =
        BTreeMap::new();
    let mut notices = BTreeSet::new();
    for record in records {
        match record {
            Record::Link(link) => {
                let located =
                    |m: String| format!("{}:{}: {m}", link.locator.input, link.locator.at);
                let kind = LINK_KINDS
                    .into_iter()
                    .find(|k| *k == link.kind)
                    .ok_or_else(|| located(format!("unknown link kind {:?}", link.kind)))?;
                check(&link.subject).map_err(located)?;
                check(&link.object).map_err(located)?;
                day(link.at.as_ref()).map_err(located)?;
                let ends = index
                    .entities(&link.subject)
                    .and_then(|subject| Ok((subject, index.entities(&link.object)?)));
                let pairs = ends.and_then(|(subject, object)| pairs(kind, &subject, &object));
                match pairs {
                    Ok(pairs) => {
                        for (subject, object) in pairs {
                            let entry = (link.locator.clone(), link.note.clone());
                            relations
                                .entry((kind, subject, object, link.at.clone()))
                                .and_modify(|first: &mut (Locator, Option<String>)| {
                                    if entry.0 < first.0 {
                                        *first = entry.clone();
                                    }
                                })
                                .or_insert(entry);
                        }
                    }
                    Err(reason) => linked.warnings.push(warning(&link.locator, &reason)),
                }
            }
            Record::Notice(notice) => {
                let located =
                    |m: String| format!("{}:{}: {m}", notice.locator.input, notice.locator.at);
                check(&notice.subject).map_err(located)?;
                day(notice.at.as_ref()).map_err(located)?;
                match index.entities(&notice.subject) {
                    Ok(subjects) => {
                        for subject in subjects {
                            notices.insert((subject, notice.at.clone(), notice.text.clone()));
                        }
                    }
                    Err(reason) => linked.warnings.push(warning(&notice.locator, &reason)),
                }
            }
            _ => {}
        }
    }
    successions(&mut relations, &mut linked.warnings);
    linked.relations = relations
        .into_iter()
        .map(|((kind, subject, object, at), (_, note))| Relation {
            kind,
            subject,
            object,
            at,
            note,
        })
        .collect();
    linked.notices = notices
        .into_iter()
        .map(|(subject, at, text)| Notice { subject, at, text })
        .collect();
    Ok(linked)
}

/// A relation by (kind, subject, object, at), with its first link's locator and note.
type Relations =
    BTreeMap<(&'static str, Entity, Entity, Option<String>), (Locator, Option<String>)>;

/// Drop the `continues` relations the post-write checks would refuse, each as one
/// warning: tables that take over from one table on the same day (none is the
/// successor), then, in locator order, a link that closes a cycle.
fn successions(relations: &mut Relations, warnings: &mut Vec<String>) {
    let mut same_day: BTreeMap<(Entity, Option<String>), Vec<_>> = BTreeMap::new();
    for (key, (locator, _)) in relations.iter() {
        if key.0 == "continues" && matches!(key.1, Entity::Table(..)) {
            same_day
                .entry((key.2, key.3.clone()))
                .or_default()
                .push((locator.clone(), key.clone()));
        }
    }
    let mut dropped: Vec<(Locator, &str)> = Vec::new();
    for mut group in same_day.into_values().filter(|g| g.len() > 1) {
        group.sort();
        for (locator, key) in group {
            relations.remove(&key);
            dropped.push((locator, "successors of one table on the same day"));
        }
    }

    let mut edges: Vec<_> = relations
        .iter()
        .filter(|(key, _)| key.0 == "continues")
        .map(|(key, (locator, _))| (locator.clone(), key.clone()))
        .collect();
    edges.sort();
    let mut kept: BTreeMap<Entity, Vec<Entity>> = BTreeMap::new();
    for (locator, key) in edges {
        let (subject, object) = (key.1, key.2);
        if reaches(&kept, object, subject) {
            relations.remove(&key);
            dropped.push((locator, "continues closes a cycle"));
        } else {
            kept.entry(subject).or_default().push(object);
        }
    }
    dropped.sort();
    warnings.extend(dropped.iter().map(|(l, reason)| warning(l, reason)));
}

/// Whether `to` is reachable from `from` over `edges` (or is `from`).
fn reaches(edges: &BTreeMap<Entity, Vec<Entity>>, from: Entity, to: Entity) -> bool {
    let mut seen = BTreeSet::new();
    let mut stack = vec![from];
    while let Some(node) = stack.pop() {
        if node == to {
            return true;
        }
        if seen.insert(node) {
            stack.extend(edges.get(&node).into_iter().flatten().copied());
        }
    }
    false
}

fn warning(locator: &Locator, reason: &str) -> String {
    format!("link: {}:{}: {reason}", locator.input, locator.at)
}

/// Refuse an endpoint the stage cannot read.
fn check(end: &Endpoint) -> Result<(), String> {
    if !ENDPOINT_KINDS.contains(&end.kind.as_str()) {
        return Err(format!("unknown endpoint kind {:?}", end.kind));
    }
    if end.register.is_some() == end.register_name.is_some() {
        return Err("an endpoint needs exactly one of register and register_name".to_owned());
    }
    let needs_table = matches!(end.kind.as_str(), "table" | "release");
    let needs_key = matches!(end.kind.as_str(), "release" | "variable");
    if needs_table && end.table.is_none() {
        return Err(format!("a {} endpoint needs a table", end.kind));
    }
    if needs_key && end.key.is_none() {
        return Err(format!("a {} endpoint needs a key", end.kind));
    }
    Ok(())
}

fn day(at: Option<&String>) -> Result<(), String> {
    match at {
        Some(at) if reg_core::iso_day_number(at).is_none() => {
            Err(format!("{at:?} is not an ISO calendar day"))
        }
        _ => Ok(()),
    }
}

/// The (subject, object) pairs a link gives, or why it gives none.
fn pairs(
    kind: &str,
    subject: &[Entity],
    object: &[Entity],
) -> Result<Vec<(Entity, Entity)>, String> {
    let has_release = |ends: &[Entity]| ends.iter().any(|e| matches!(e, Entity::Release(..)));
    if has_release(subject) || has_release(object) {
        return Err(format!("release-kind {kind}"));
    }
    let [object] = object else {
        return Err(format!("{} objects, not one", object.len()));
    };
    if kind == "continues" {
        let [subject] = subject else {
            return Err(format!("{} subjects, not one", subject.len()));
        };
        if subject.kind() != object.kind() {
            return Err(format!(
                "continues from a {} to a {}",
                subject.kind(),
                object.kind()
            ));
        }
        return Ok(vec![(*subject, *object)]);
    }
    Ok(subject.iter().map(|s| (*s, *object)).collect())
}

/// Every register's public surface, keyed by source handles.
struct Index<'a> {
    registers: BTreeMap<(&'a str, &'a str), usize>,
    names: BTreeMap<(&'a str, String), Vec<usize>>,
    tables: BTreeMap<(usize, &'a str), usize>,
    releases: BTreeMap<(usize, &'a str, &'a str), usize>,
    variables: BTreeMap<(usize, &'a str), Vec<usize>>,
}

impl<'a> Index<'a> {
    fn new(registers: &'a [Resolved]) -> Self {
        let mut index = Index {
            registers: BTreeMap::new(),
            names: BTreeMap::new(),
            tables: BTreeMap::new(),
            releases: BTreeMap::new(),
            variables: BTreeMap::new(),
        };
        for (ri, r) in registers.iter().enumerate() {
            let reg = &r.register;
            index
                .registers
                .insert((reg.provider.as_str(), reg.register.as_str()), ri);
            if let Some(name) = &reg.name {
                index
                    .names
                    .entry((reg.provider.as_str(), reg_core::fold_identity(name.trim())))
                    .or_default()
                    .push(ri);
            }
            for (t, table) in r.tables.iter().enumerate() {
                index.tables.insert((ri, table.table.table.as_str()), t);
            }
            for (x, release) in r.releases.iter().enumerate() {
                let rel = &release.release;
                index
                    .releases
                    .insert((ri, rel.table.as_str(), rel.release.as_str()), x);
            }
            for (v, variable) in r.variables.iter().enumerate() {
                index
                    .variables
                    .entry((ri, variable.source_key.as_str()))
                    .or_default()
                    .push(v);
            }
        }
        index
    }

    /// The entities an endpoint (already checked) names: one, or several pieces of a
    /// split key.
    fn entities(&self, end: &Endpoint) -> Result<Vec<Entity>, String> {
        let ri = match (&end.register, &end.register_name) {
            (Some(handle), _) => *self
                .registers
                .get(&(end.provider.as_str(), handle.as_str()))
                .ok_or_else(|| format!("no register {}/{handle}", end.provider))?,
            (None, Some(name)) => {
                let key = (end.provider.as_str(), reg_core::fold_identity(name.trim()));
                match self.names.get(&key).map(Vec::as_slice) {
                    Some([ri]) => *ri,
                    Some(_) => return Err(format!("ambiguous name {name:?}")),
                    None => {
                        return Err(format!("no register named {name:?} in {}", end.provider));
                    }
                }
            }
            (None, None) => unreachable!("an endpoint is checked at load"),
        };
        let table = end.table.as_deref().unwrap_or_default();
        let key = end.key.as_deref().unwrap_or_default();
        let register = end
            .register
            .as_deref()
            .or(end.register_name.as_deref())
            .unwrap_or_default();
        let missing = |what: &str| format!("no {what} in {}/{register}", end.provider);
        match end.kind.as_str() {
            "register" => Ok(vec![Entity::Register(ri)]),
            "table" => self
                .tables
                .get(&(ri, table))
                .map(|&t| vec![Entity::Table(ri, t)])
                .ok_or_else(|| missing(&format!("table {table:?}"))),
            "release" => self
                .releases
                .get(&(ri, table, key))
                .map(|&x| vec![Entity::Release(ri, x)])
                .ok_or_else(|| missing(&format!("release {key:?} of table {table:?}"))),
            _ => self
                .variables
                .get(&(ri, key))
                .map(|vs| vs.iter().map(|&v| Entity::Variable(ri, v)).collect())
                .ok_or_else(|| missing(&format!("variable with key {key:?}"))),
        }
    }
}
