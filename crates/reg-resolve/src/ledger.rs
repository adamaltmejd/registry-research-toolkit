//! The register ledger (`ids.toml`) and the assignment of public ids.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::fold_identity;
use serde::Deserialize;

use crate::{Report, ResolvedOccurrence, ResolvedTable, Variable, slugify};

/// A register's `ids.toml`: public ids of its tables and variables. An anchor is
/// `<table>: <spelling>`, or a bare spelling for the implicit table, matched by fold.
/// Where several source keys share the spelling in that table, `# <source key>` names
/// one and `@ <day>` one day (`<table>: <spelling> # <key> @ <day>`).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    #[serde(default)]
    pub tables: BTreeMap<String, String>,
    #[serde(default)]
    pub variables: BTreeMap<String, Vec<String>>,
}

/// A ledger anchor, parsed: table, folded spelling, source key and day.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Anchor<'a> {
    table: &'a str,
    spelling: String,
    key: Option<&'a str>,
    day: Option<&'a str>,
}

impl<'a> Anchor<'a> {
    /// `None` when its `@` part is not a day.
    fn parse(text: &'a str) -> Option<Self> {
        let (rest, day) = match text.rsplit_once(" @ ") {
            Some((rest, day)) => {
                let day = day.trim();
                reg_core::iso_day_number(day)?;
                (rest, Some(day))
            }
            None => (text, None),
        };
        let (rest, key) = match rest.rsplit_once(" # ") {
            Some((rest, key)) => (rest, Some(key.trim())),
            None => (rest, None),
        };
        let (table, spelling) = rest.rsplit_once(": ").unwrap_or(("", rest));
        Some(Self {
            table: table.trim(),
            spelling: fold_identity(spelling.trim()),
            key,
            day,
        })
    }
}

/// Ledger ids onto variables by anchor; proposed ids for the rest.
pub(crate) fn assign_ids(
    variables: &mut [Variable],
    resolved: &[ResolvedOccurrence],
    tables: &[ResolvedTable],
    ledger: &Ledger,
    report: &mut Report,
) -> Result<(), String> {
    // Tables and variables share the register's namespace, and a ledger id stays
    // reserved even where it anchors nothing.
    let table_ids: BTreeSet<String> = tables.iter().filter_map(|t| t.slug.clone()).collect();
    for id in table_ids.iter().chain(ledger.variables.keys()) {
        if !reg_core::is_slug(id) {
            return Err(format!("id {id:?} is not a slug"));
        }
    }
    let mut taken = table_ids.clone();
    for id in ledger.variables.keys() {
        if !taken.insert(id.clone()) {
            return Err(format!("ledger id {id:?} is also a table id"));
        }
    }
    // Anchors match the folded spellings of each table's placed columns; a keyed anchor
    // only that source key's variables, a dated one only those placed there that day.
    // A spelling's source keys count every occurrence, placed or not.
    let mut by_anchor: BTreeMap<(&str, String), Vec<_>> = BTreeMap::new();
    let mut keys: BTreeMap<(&str, String), BTreeSet<&str>> = BTreeMap::new();
    for r in resolved.iter().filter(|r| !r.occurrence.column.is_empty()) {
        let at = (
            tables[r.table].table.table.as_str(),
            fold_identity(&r.occurrence.column),
        );
        keys.entry(at.clone())
            .or_default()
            .insert(r.occurrence.source_key.as_str());
        if let Some(v) = r.variable {
            by_anchor.entry(at).or_default().push((
                v,
                r.period.as_ref(),
                r.occurrence.source_key.as_str(),
            ));
        }
    }
    // Two ids on one anchor would leave the id to key order: refused, as is a key where
    // the spelling has only one.
    let mut anchored: BTreeMap<Anchor, &str> = BTreeMap::new();
    for (id, anchors) in &ledger.variables {
        let mut targets = BTreeSet::new();
        for text in anchors {
            let anchor = Anchor::parse(text)
                .ok_or_else(|| format!("ledger id {id:?}: anchor {text:?} names no day"))?;
            let (key, day) = (anchor.key, anchor.day);
            let at = (anchor.table, anchor.spelling.clone());
            let placed = by_anchor.get(&at).map_or(&[][..], Vec::as_slice);
            let shared = keys.get(&at).is_some_and(|k| k.len() > 1);
            if let Some(other) = anchored.insert(anchor, id) {
                return Err(format!(
                    "ledger-anchor-shared: ledger ids {other:?} and {id:?} both anchor {text:?}"
                ));
            }
            if key.is_some() && !shared {
                return Err(format!(
                    "ledger-anchor-key-unneeded: ledger id {id:?}: anchor {text:?} names a source key where one key has the spelling"
                ));
            }
            targets.extend(
                placed
                    .iter()
                    .filter(|&&(_, period, k)| {
                        key.is_none_or(|key| key == k)
                            && day.is_none_or(|d| {
                                period.is_some_and(|p| p.0.as_str() <= d && d <= p.1.as_str())
                            })
                    })
                    .map(|&(v, _, _)| v),
            );
        }
        if targets.len() > 1 {
            report.split_ids.push(id.clone());
        }
        let Some(&target) = targets.iter().find(|&&v| !variables[v].pinned) else {
            if targets.is_empty() {
                report.unplaced_ids.push(id.clone());
            } else {
                report.merged_ids.push(id.clone());
            }
            continue;
        };
        variables[target].slug.clone_from(id);
        variables[target].pinned = true;
    }
    for v in variables.iter_mut().filter(|v| !v.pinned) {
        let base = slugify(&v.fold);
        let mut slug = base.clone();
        let mut n = 2;
        while taken.contains(&slug) || !reg_core::is_slug(&slug) {
            slug = format!("{base}-{n}");
            n += 1;
        }
        report.unminted.push(slug.clone());
        taken.insert(slug.clone());
        v.slug = slug;
    }
    Ok(())
}
