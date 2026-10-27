//! Search rows (`search_item` and the contentless FTS index `search`, schema §2.1).
//! The builder only writes rows: ranking and matching belong to the reader. Ids follow
//! `sort_key` byte order across all kinds, so they are stable for the same pins.

use std::collections::{BTreeMap, BTreeSet};

use crate::emit::{self, Catalog};

/// One searchable entity, before its id is known.
pub struct Item {
    pub kind: &'static str,
    pub entity_id: Option<i64>,
    pub register_id: Option<i64>,
    /// The half-open coverage hull, for the period filter.
    pub span: Option<(i64, i64)>,
    pub valueset_id: Option<i64>,
    /// The code of a `code` row.
    pub code: Option<String>,
    /// The item's position in its value set, of a `code` row.
    pub ord: Option<i64>,
    pub sort_key: String,
    pub name: String,
    pub label: String,
    pub body: String,
}

/// The row ids the entities were given, by the emitter's natural-key order.
pub struct Ids<'a> {
    pub register: &'a BTreeMap<String, i64>,
    pub table: &'a BTreeMap<(usize, usize), i64>,
    pub variable: &'a BTreeMap<(usize, usize), i64>,
    pub valueset: &'a BTreeMap<&'a str, i64>,
}

/// Every item, in `sort_key` order. The id of an item is its position, from 1.
///
/// # Errors
/// Two items with the same `sort_key`.
pub fn items(catalog: &Catalog, ids: &Ids) -> Result<Vec<Item>, String> {
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
            valueset_id: None,
            code: None,
            ord: None,
            sort_key: reg_pid.clone(),
            name: reg_pid,
            label: reg.name.clone().unwrap_or_default(),
            body: reg.description.clone().unwrap_or_default(),
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
                valueset_id: None,
                code: None,
                ord: None,
                sort_key: pid.clone(),
                name: pid,
                label: table.table.name.clone().unwrap_or_default(),
                body: table.table.description.clone().unwrap_or_default(),
            });
        }
        for (v, variable) in r.variables.iter().enumerate() {
            let pid = emit::pid_of_variable(r, v);
            let mut name = pid.clone();
            for spelling in &variable.spellings {
                name.push(' ');
                name.push_str(spelling);
            }
            let mut labels = vec![variable.label.as_str()];
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
            items.push(Item {
                kind: "variable",
                entity_id: Some(ids.variable[&(ri, v)]),
                register_id: Some(register_id),
                span: Some((emit::start(&variable.hull.0), emit::end(&variable.hull.1))),
                valueset_id: None,
                code: None,
                ord: None,
                sort_key: pid,
                name,
                label: labels.join("\n"),
                body: definitions.join("\n"),
            });
        }
    }
    code_items(catalog, ids, &mut items);
    items.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
    if let Some(w) = items.windows(2).find(|w| w[0].sort_key == w[1].sort_key) {
        return Err(format!("duplicate search key: {:?}", w[0].sort_key));
    }
    Ok(items)
}

/// One row per item of each value set some segment references. A value set only
/// shadowed or ambiguous occurrences reference gets no code rows (plan decision C-10).
fn code_items(catalog: &Catalog, ids: &Ids, items: &mut Vec<Item>) {
    let referenced: BTreeSet<&str> = catalog
        .registers
        .iter()
        .flat_map(|r| &r.variables)
        .flat_map(|v| &v.segments)
        .filter_map(|s| s.valueset.as_deref())
        .collect();
    for hash in referenced {
        let (set, _) = &catalog.valuesets[hash];
        for (ord, item) in set.items.iter().enumerate() {
            items.push(Item {
                kind: "code",
                entity_id: None,
                register_id: None,
                span: None,
                valueset_id: Some(ids.valueset[hash]),
                code: Some(item.code.clone()),
                ord: Some(emit::count(ord)),
                sort_key: format!("vs:{hash}#{}", item.code),
                // A blank code is a real item: it is reached through its label.
                name: item.code.clone(),
                label: item.label.clone().unwrap_or_default(),
                body: String::new(),
            });
        }
    }
}
