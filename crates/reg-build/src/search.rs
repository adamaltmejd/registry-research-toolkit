//! Search rows (`search_item` and the contentless FTS index `search`, schema §2.1).
//! The builder only writes rows: ranking and matching belong to the reader. Ids follow
//! `sort_key` byte order across all kinds, so they are stable for the same pins.

use std::collections::{BTreeMap, BTreeSet};

use reg_resolve::provenance::Subject;

use crate::emit::{self, Catalog};

/// One searchable entity, before its id is known.
pub struct Item {
    pub kind: &'static str,
    pub entity_id: Option<i64>,
    pub register_id: Option<i64>,
    /// The half-open coverage hull, for the period filter.
    pub span: Option<(i64, i64)>,
    /// A 'code' row's (code, label) pair; the label is `None` when the item has none.
    pub pair: Option<(String, Option<String>)>,
    pub sort_key: String,
    pub name: String,
    pub label: String,
    pub body: String,
    /// Supplementary occurrence text, indexed separately from the primary row.
    pub description: String,
}

/// The row ids the entities were given, by the emitter's natural-key order.
pub struct Ids<'a> {
    pub register: &'a BTreeMap<String, i64>,
    pub table: &'a BTreeMap<(usize, usize), i64>,
    pub variable: &'a BTreeMap<(usize, usize), i64>,
    /// A document's id → the register of its first resolved subject.
    pub doc_register: &'a BTreeMap<i64, i64>,
}

/// How much of a document's text its search row carries; `doc_fts` holds the whole.
// simplify: 2000 characters is a guess at what ranks a document well; measure search
// quality on the real catalog and raise or drop the cap if long documents rank badly.
const DOC_BODY_CHARS: usize = 2000;

fn occurrence_descriptions(catalog: &Catalog) -> BTreeMap<(usize, usize), BTreeSet<&str>> {
    let mut descriptions = BTreeMap::<_, BTreeSet<&str>>::new();
    for value in &catalog.provenance.values {
        if value.field == "description"
            && let Subject::Occurrence(ri, oi) = &value.subject
            && let Some(description) = value.value.as_deref().filter(|d| !d.is_empty())
        {
            let occurrence = &catalog.registers[*ri].occurrences[*oi];
            if occurrence.status == "used"
                && let Some(variable) = occurrence.variable
            {
                descriptions
                    .entry((*ri, variable))
                    .or_default()
                    .insert(description);
            }
        }
    }
    descriptions
}

/// Every item, in `sort_key` order. The id of an item is its position, from 1.
///
/// # Errors
/// Two items with the same `sort_key`.
pub fn items(catalog: &Catalog, ids: &Ids) -> Result<Vec<Item>, String> {
    let descriptions = occurrence_descriptions(catalog);
    let mut items = Vec::new();
    for (ri, r) in catalog.registers.iter().enumerate() {
        let reg_pid = emit::pid_of_register(r);
        let register_id = ids.register[&reg_pid];
        let reg = &r.register;
        items.push(Item {
            kind: "register",
            entity_id: Some(register_id),
            register_id: Some(register_id),
            span: Some((emit::start(&r.coverage.0), emit::end(&r.coverage.1))),
            pair: None,
            sort_key: reg_pid.clone(),
            name: reg_pid,
            label: reg.name.clone().unwrap_or_default(),
            body: reg.description.clone().unwrap_or_default(),
            description: String::new(),
        });
        for (t, table) in r.tables.iter().enumerate() {
            // The implicit table has its register's pid and no row of its own.
            if table.slug.is_none() {
                continue;
            }
            let pid = emit::pid_of_table(r, t);
            items.push(Item {
                kind: "table",
                entity_id: Some(ids.table[&(ri, t)]),
                register_id: Some(register_id),
                span: Some(emit::hull(&table.spans)),
                pair: None,
                sort_key: pid.clone(),
                name: pid,
                label: table.table.name.clone().unwrap_or_default(),
                body: table.table.description.clone().unwrap_or_default(),
                description: String::new(),
            });
        }
        for (v, variable) in r.variables.iter().enumerate() {
            let pid = emit::pid_of_variable(r, v);
            let mut name = pid.clone();
            for spelling in &variable.spellings {
                name.push(' ');
                name.push_str(spelling);
            }
            let (label, definitions) = variable_texts(variable);
            let description = descriptions
                .get(&(ri, v))
                .into_iter()
                .flatten()
                .filter(|description| !definitions.contains(description))
                .copied()
                .collect::<Vec<_>>()
                .join("\n");
            items.push(Item {
                kind: "variable",
                entity_id: Some(ids.variable[&(ri, v)]),
                register_id: Some(register_id),
                span: Some((emit::start(&variable.hull.0), emit::end(&variable.hull.1))),
                pair: None,
                sort_key: pid,
                name,
                label,
                body: definitions.join("\n"),
                description,
            });
        }
    }
    items.extend(code_items(catalog));
    for (d, document) in catalog.docs.iter().enumerate() {
        let doc = &document.doc;
        let doc_id = emit::count(d) + 1;
        let text: Vec<&str> = doc.sections.iter().map(|s| s.body.as_str()).collect();
        items.push(Item {
            kind: "document",
            entity_id: Some(doc_id),
            register_id: ids.doc_register.get(&doc_id).copied(),
            span: None,
            pair: None,
            sort_key: format!("doc:{}", doc.hash),
            name: String::new(),
            label: doc.title.clone(),
            body: text.join("\n").chars().take(DOC_BODY_CHARS).collect(),
            description: String::new(),
        });
    }
    items.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
    if let Some(w) = items.windows(2).find(|w| w[0].sort_key == w[1].sort_key) {
        return Err(format!("duplicate search key: {:?}", w[0].sort_key));
    }
    Ok(items)
}

fn variable_texts(variable: &reg_resolve::Variable) -> (String, Vec<&str>) {
    let mut labels = vec![variable.label.as_str()];
    labels.extend(variable.synonyms.iter().map(String::as_str));
    let mut definitions = Vec::new();
    for run in &variable.texts {
        if !labels.contains(&run.label.as_str()) {
            labels.push(&run.label);
        }
        // A term may live only in the operational definition.
        for d in [&run.definition, &run.operational_definition] {
            if let Some(d) = d.as_deref().filter(|d| !d.is_empty())
                && !definitions.contains(&d)
            {
                definitions.push(d);
            }
        }
    }
    (labels.join("\n"), definitions)
}

/// Code rows: one per distinct (code, label) of the value sets some segment states.
/// Pairs compare as text, as `vs_item_code` does. A value set only occurrences reach
/// adds none.
fn code_items(catalog: &Catalog) -> Vec<Item> {
    let referenced: BTreeSet<&str> = catalog
        .registers
        .iter()
        .flat_map(|r| &r.variables)
        .flat_map(|v| &v.segments)
        .filter_map(|s| s.valueset.as_deref())
        .collect();
    let mut pairs = BTreeSet::new();
    for (hash, (set, _)) in &catalog.valuesets {
        if referenced.contains(hash.as_str()) {
            pairs.extend(
                set.items
                    .iter()
                    .map(|i| (i.code.as_str(), i.label.as_deref())),
            );
        }
    }
    pairs
        .into_iter()
        .map(|(code, label)| {
            // U+001F cannot occur in either text. A missing label has no separator, so
            // its key differs from the empty label's.
            let sort_key = label.map_or_else(
                || format!("code:{code}"),
                |l| format!("code:{code}\u{1f}{l}"),
            );
            Item {
                kind: "code",
                entity_id: None,
                register_id: None,
                span: None,
                pair: Some((code.to_owned(), label.map(str::to_owned))),
                sort_key,
                name: code.to_owned(),
                label: label.unwrap_or_default().to_owned(),
                body: String::new(),
                description: String::new(),
            }
        })
        .collect()
}
