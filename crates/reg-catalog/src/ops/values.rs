//! `values`: a value set's items by content, or the value sets that hold a
//! (code, label) pair.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;
use utoipa::openapi::{OneOfBuilder, RefOr, Schema};

use super::segments::cursor_context;
use super::shape::{self, ValueSetRef};
use super::{
    Cache, Components, Operation, Param, Params, Server, Type, component, cursor, hash, limit, refs,
};
use crate::{Code, Error};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "values",
    paths: &["/api/values"],
    tool: Some("values"),
    description: "A value set's items, by its hash or by a classification version's ref, \
        with a hierarchical set paged one level at a time by `parent` and searched flat by \
        `q`; or, given a `code` and optionally its `label`, the value sets that hold that \
        pair. The blank code is an item like any other.",
    params: &[
        Param::optional("value_set", Type::Hash),
        Param::optional("classification", Type::Ref),
        Param::optional("code", Type::String),
        Param::optional("label", Type::String),
        Param::optional("level", Type::String),
        Param::optional("parent", Type::String),
        Param::optional("q", Type::String),
        Param::optional("limit", Type::Limit),
        Param::optional("cursor", Type::Cursor),
    ],
    cache: Cache::Minute,
    run: values,
    result,
}];

/// One value set's items, or the value sets that hold a pair.
fn result(components: &mut Components) -> RefOr<Schema> {
    let items = component::<ValueItemsPage>(components);
    let sets = component::<ValueSetsPage>(components);
    RefOr::T(Schema::OneOf(
        OneOfBuilder::new().item(items).item(sets).build(),
    ))
}

/// `Page<ValueItem>` plus the value set and `total`.
#[derive(Serialize, ToSchema)]
pub struct ValueItemsPage {
    items: Vec<ValueItem>,
    #[schema(required = true)]
    next_cursor: Option<String>,
    value_set: ValueSetRef,
    /// The items matching `q` in the whole set; the set's size without `q`.
    total: i64,
}

/// `Page<ValueSetRef>`, ordered by hash.
#[derive(Serialize, ToSchema)]
pub struct ValueSetsPage {
    items: Vec<ValueSetRef>,
    #[schema(required = true)]
    next_cursor: Option<String>,
}

/// `shape.ValueItem`.
#[derive(Clone, Serialize, ToSchema)]
pub struct ValueItem {
    /// May be `""`: a real item.
    code: String,
    #[schema(required = true)]
    label: Option<String>,
    level: i64,
    /// The parent's code.
    #[schema(required = true)]
    parent: Option<String>,
    sentinel: bool,
    /// `null` for a flat set.
    #[schema(required = true)]
    child_count: Option<i64>,
}

/// The mode parameters, in the order a conflict names the later one.
const MODES: [&str; 3] = ["value_set", "classification", "code"];

fn values(server: &Server, params: &Params) -> Result<Value, Error> {
    let present = |name: &str| params.get(name).is_some();
    let modes: Vec<&str> = MODES.into_iter().filter(|m| present(m)).collect();
    match modes.as_slice() {
        [] => return Err(Error::invalid_parameter("value_set")),
        [_] => {}
        [.., later] => return Err(Error::invalid_parameter(later)),
    }
    if present("label") && !present("code") {
        return Err(Error::invalid_parameter("label"));
    }
    if present("code")
        && let Some(applied) = ["level", "parent", "q"].into_iter().find(|p| present(p))
    {
        return Err(Error::invalid_parameter(applied));
    }
    let by_hash = hash(params, "value_set")?;
    let level = params
        .get("level")
        .map(|level| {
            level
                .parse::<i64>()
                .map_err(|_| Error::invalid_parameter("level"))
        })
        .transpose()?;
    let conn = server.catalog.connect()?;
    let page = if let Some(&code) = params.get("code") {
        pair(server, &conn, params, code)?
    } else {
        let valueset_id = match by_hash {
            Some(hash) => by_content(&conn, hash)?,
            None => classification_set(&conn, params["classification"])?,
        };
        content(server, &conn, params, valueset_id, level)?
    };
    Ok(page)
}

/// The id of the value set with `hash`.
fn by_content(conn: &Connection, hash: &str) -> Result<i64, Error> {
    conn.query_row("SELECT id FROM valueset WHERE hash = ?", [hash], |row| {
        row.get(0)
    })
    .optional()?
    .ok_or_else(|| {
        Error::new(
            Code::NotFound,
            format!("No value set has hash {hash}."),
            vec![hash.into()],
        )
    })
}

/// The id of the value set of the class version `given` names.
fn classification_set(conn: &Connection, given: &str) -> Result<i64, Error> {
    let version = refs::resolve(conn, given)?;
    if version.kind != "class_version" {
        return Err(Error::new(
            Code::InvalidRef,
            format!(
                "{given:?} is a {}, not a classification version.",
                version.kind
            ),
            vec![given.into()],
        ));
    }
    Ok(conn.query_row(
        "SELECT valueset_id FROM class_version WHERE id = ?",
        [version.id],
        |row| row.get(0),
    )?)
}

/// One decoded keyset position, or `None` for the first page.
fn position(server: &Server, params: &Params) -> Result<Option<String>, Error> {
    params
        .get("cursor")
        .map(|token| {
            cursor::decode(
                token,
                server.catalog.generation(),
                &cursor_context(params),
                usize::MAX,
            )
            .map(|(_, position)| position)
        })
        .transpose()
}

/// A value set's items in `ord` order (so an item's index is its `ord`), each with its
/// parent's `ord`, the parent's code and, for a hierarchical set, its child count.
///
/// The set is bounded by `n_items` and has no text index, so it is read whole by its
/// primary-key prefix.
fn load_items(
    conn: &Connection,
    valueset_id: i64,
    hierarchical: bool,
) -> Result<Vec<(ValueItem, Option<usize>)>, Error> {
    let mut items: Vec<(ValueItem, Option<usize>)> = conn
        .prepare(
            "SELECT c.body, l.body, i.level, i.parent_ord, i.sentinel
             FROM vs_item i
               JOIN text c ON c.id = i.code_text
               LEFT JOIN text l ON l.id = i.label_text
             WHERE i.valueset_id = ? ORDER BY i.ord",
        )?
        .query_map([valueset_id], |row| {
            Ok((
                ValueItem {
                    code: row.get(0)?,
                    label: row.get(1)?,
                    level: row.get(2)?,
                    parent: None,
                    sentinel: row.get(4)?,
                    child_count: hierarchical.then_some(0),
                },
                row.get::<_, Option<i64>>(3)?
                    .and_then(|ord| usize::try_from(ord).ok()),
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    for ord in 0..items.len() {
        if let Some(parent) = items[ord].1 {
            items[ord].0.parent = Some(items[parent].0.code.clone());
            if let Some(count) = items[parent].0.child_count.as_mut() {
                *count += 1;
            }
        }
    }
    Ok(items)
}

/// A value set's items in `ord` order, filtered, as a page with the set and `total`.
fn content(
    server: &Server,
    conn: &Connection,
    params: &Params,
    valueset_id: i64,
    level: Option<i64>,
) -> Result<Value, Error> {
    let limit = limit(params)?;
    let after = position(server, params)?
        .map(|p| {
            p.parse::<usize>()
                .map_err(|_| cursor::invalid("Cursor is malformed."))
        })
        .transpose()?;
    let (n_items, hierarchical): (i64, bool) = conn.query_row(
        "SELECT n_items, hierarchical FROM valueset WHERE id = ?",
        [valueset_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let items = load_items(conn, valueset_id, hierarchical)?;
    let parents: Vec<Option<usize>> = items.iter().map(|(_, parent)| *parent).collect();
    let codes: Vec<String> = items.iter().map(|(item, _)| item.code.clone()).collect();

    let query = params.get("q").filter(|q| !q.is_empty());
    let selected: Vec<usize> = if let Some(q) = query {
        // A search reads the set flat, ignoring `level` and `parent`.
        let needle = reg_core::fold_identity(q);
        (0..items.len())
            .filter(|&ord| {
                let item = &items[ord].0;
                reg_core::fold_identity(&item.code).contains(&needle)
                    || item
                        .label
                        .as_deref()
                        .is_some_and(|l| reg_core::fold_identity(l).contains(&needle))
            })
            .collect()
    } else {
        // An empty `parent` is the roots, a code its first item's children; an unknown
        // code has none.
        let parent = params
            .get("parent")
            .map(|parent| (!parent.is_empty()).then(|| codes.iter().position(|c| c == parent)));
        (0..items.len())
            .filter(|&ord| {
                level.is_none_or(|level| items[ord].0.level == level)
                    && match parent {
                        None => true,
                        Some(None) => parents[ord].is_none(),
                        Some(Some(Some(wanted))) => parents[ord] == Some(wanted),
                        Some(Some(None)) => false,
                    }
            })
            .collect()
    };
    let total = if query.is_some() {
        i64::try_from(selected.len()).expect("a count fits i64")
    } else {
        n_items
    };
    let mut page: Vec<usize> = selected
        .into_iter()
        .filter(|&ord| after.is_none_or(|after| ord > after))
        .take(limit + 1)
        .collect();
    let next_cursor = (page.len() > limit).then(|| {
        page.truncate(limit);
        cursor::encode(
            server.catalog.generation(),
            &cursor_context(params),
            limit,
            &page[limit - 1].to_string(),
        )
    });
    let items = page.into_iter().map(|ord| items[ord].0.clone()).collect();
    Ok(serde_json::to_value(ValueItemsPage {
        items,
        next_cursor,
        value_set: shape::value_set_ref(conn, valueset_id)?,
        total,
    })
    .expect("a page serializes"))
}

/// The value sets some segment references that hold the pair (`code`, `label`), by hash.
fn pair(server: &Server, conn: &Connection, params: &Params, code: &str) -> Result<Value, Error> {
    let limit = limit(params)?;
    let after = position(server, params)?.unwrap_or_default();
    let label = params.get("label").copied();
    // `text` has no index on `body`, so the pair's text ids come from the FTS index the
    // builder wrote for code rows: the code in `name`, or the label in `label` when the
    // code is blank. The bodies are then compared exactly.
    // simplify: a pair whose text has no token (punctuation only, or a blank code with
    // no label) is not reachable this way and answers an empty page; index `text(body)`
    // or key pairs by id if such pairs need a drill-down.
    let phrase = if code.chars().any(char::is_alphanumeric) {
        Some(format!("name : {}", quoted(code)))
    } else {
        label
            .filter(|l| l.chars().any(char::is_alphanumeric))
            .map(|l| format!("label : {}", quoted(l)))
    };
    let mut found: BTreeMap<String, i64> = BTreeMap::new();
    if let Some(phrase) = phrase {
        let pairs = conn
            .prepare(
                "SELECT si.code_text, si.label_text
                 FROM search s
                   JOIN search_item si ON si.id = s.rowid AND si.kind = 'code'
                   JOIN text c ON c.id = si.code_text
                   LEFT JOIN text l ON l.id = si.label_text
                 WHERE search MATCH ? AND c.body = ? AND l.body IS ?",
            )?
            .query_map(rusqlite::params![phrase, code, label], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut sets = conn.prepare(
            "SELECT v.id, v.hash FROM vs_item i JOIN valueset v ON v.id = i.valueset_id
             WHERE i.code_text = ? AND i.label_text IS ? AND v.hash > ?
               AND EXISTS (SELECT 1 FROM segment s WHERE s.valueset_id = v.id)
             GROUP BY v.id ORDER BY v.hash LIMIT ?",
        )?;
        for (code_text, label_text) in pairs {
            let rows = sets.query_map(
                rusqlite::params![
                    code_text,
                    label_text,
                    after,
                    i64::try_from(limit + 1).expect("a limit fits i64")
                ],
                |row| Ok((row.get::<_, String>(1)?, row.get::<_, i64>(0)?)),
            )?;
            for row in rows {
                let (hash, id) = row?;
                found.insert(hash, id);
            }
        }
    }
    let mut page: Vec<(String, i64)> = found.into_iter().take(limit + 1).collect();
    let next_cursor = (page.len() > limit).then(|| {
        page.truncate(limit);
        cursor::encode(
            server.catalog.generation(),
            &cursor_context(params),
            limit,
            &page[limit - 1].0,
        )
    });
    let items = page
        .into_iter()
        .map(|(_, id)| shape::value_set_ref(conn, id))
        .collect::<Result<_, _>>()?;
    Ok(serde_json::to_value(ValueSetsPage { items, next_cursor }).expect("a page serializes"))
}

/// `text` as an FTS5 phrase.
fn quoted(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}
