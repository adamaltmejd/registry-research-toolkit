//! `reg-build diff --holdings OLD NEW`: what changed between two holdings catalogs
//! (release step 8g; `crates/DESIGN.md`, "Release diff").
//!
//! Census columns are keyed by (physical table, column name), text and notices by the
//! natural keys below, never by row id. Only counts and keys are printed: no text body
//! and no census or storage value.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use super::{Item, Pair, Res, Section, query};

/// (`table_pid`, `variable_pid`, start, end), the bounds as text.
type Placement = (String, String, String, String);
type Column = (String, String);
/// (code, subject key, start, end) -> occurrences.
type Bag = BTreeMap<(String, String, String, String), i64>;

/// Reader-visible text by kind, as SQL returning (natural key, body).
const TEXT: [(&str, &str); 10] = [
    (
        "occurrence label",
        "SELECT t.pid || '/' || o.source_key || '@' || ifnull(o.start, '') || '-' || ifnull(o.end, ''), d.body FROM occurrence o JOIN tbl t ON t.id = o.table_id JOIN text d ON d.id = o.label_text",
    ),
    (
        "occurrence definition",
        "SELECT t.pid || '/' || o.source_key || '@' || ifnull(o.start, '') || '-' || ifnull(o.end, ''), d.body FROM occurrence o JOIN tbl t ON t.id = o.table_id JOIN text d ON d.id = o.definition_text",
    ),
    (
        "occurrence operational definition",
        "SELECT t.pid || '/' || o.source_key || '@' || ifnull(o.start, '') || '-' || ifnull(o.end, ''), d.body FROM occurrence o JOIN tbl t ON t.id = o.table_id JOIN text d ON d.id = o.opdef_text",
    ),
    (
        "occurrence unit",
        "SELECT t.pid || '/' || o.source_key || '@' || ifnull(o.start, '') || '-' || ifnull(o.end, ''), d.body FROM occurrence o JOIN tbl t ON t.id = o.table_id JOIN text d ON d.id = o.unit_text",
    ),
    (
        "variable label",
        "SELECT v.pid, x.body FROM variable v JOIN text x ON x.id = v.label_text",
    ),
    (
        "variable definition",
        "SELECT v.pid, x.body FROM variable v JOIN text x ON x.id = v.definition_text",
    ),
    (
        "variable description",
        "SELECT v.pid, x.body FROM variable v JOIN text x ON x.id = v.description_text",
    ),
    (
        "table description",
        "SELECT t.pid, x.body FROM tbl t JOIN text x ON x.id = t.description_text",
    ),
    (
        "register description",
        "SELECT r.pid, x.body FROM register r JOIN text x ON x.id = r.description_text",
    ),
    (
        "mapping caveat",
        "SELECT t.physical_name || '/' || c.name || ' ' || m.table_pid || ' ' || m.variable_pid || '@' || m.start || '-' || m.end, m.caveat FROM holding_mapping m JOIN holding_column c ON c.id = m.column_id JOIN holding_table t ON t.id = c.holding_id WHERE m.caveat IS NOT NULL",
    ),
];

/// Finding subjects by kind, as SQL returning (row id, key that survives a rebuild).
const SUBJECTS: [(&str, &str); 9] = [
    ("register", "SELECT id, pid FROM register"),
    ("table", "SELECT id, pid FROM tbl"),
    (
        "release",
        "SELECT r.id, t.pid || '/' || r.source_key FROM release r JOIN tbl t ON t.id = r.table_id",
    ),
    ("variable", "SELECT id, pid FROM variable"),
    (
        "segment",
        "SELECT s.id, v.pid || '@' || t.pid || '@' || s.start FROM segment s JOIN variable v ON v.id = s.variable_id JOIN tbl t ON t.id = s.table_id",
    ),
    (
        "occurrence",
        "SELECT o.id, t.pid || '/' || o.source_key || '@' || ifnull(o.start, '') FROM occurrence o JOIN tbl t ON t.id = o.table_id",
    ),
    ("valueset", "SELECT id, hash FROM valueset"),
    ("set", "SELECT id, pid FROM var_set"),
    ("class_version", "SELECT id, pid FROM class_version"),
];

fn subject_cte() -> String {
    SUBJECTS
        .iter()
        .map(|(kind, sql)| format!("SELECT '{kind}', * FROM ({sql})"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

/// A statement is keyed by its file, kind and the stable keys of what it won (overrides,
/// relations, acks), not by its hash: the hash covers the whole statement, so a non-text
/// edit would read as removed and added text, and file and kind alone merge statements.
fn statement_sql() -> String {
    format!(
        "WITH subject(kind, id, key) AS ({}), target(sid, t) AS (\
         SELECT o.statement_id, o.subject_kind || ':' || ifnull(k.key, o.subject_id) || '.' || o.field \
         FROM override o LEFT JOIN subject k ON k.kind = o.subject_kind AND k.id = o.subject_id \
         WHERE o.statement_id IS NOT NULL \
         UNION SELECT r.statement_id, r.kind || ' ' || ifnull(a.key, r.subject_id) || ' -> ' || ifnull(b.key, r.object_id) \
         FROM relation r LEFT JOIN subject a ON a.kind = r.subject_kind AND a.id = r.subject_id \
         LEFT JOIN subject b ON b.kind = r.object_kind AND b.id = r.object_id \
         WHERE r.statement_id IS NOT NULL \
         UNION SELECT f.ack_statement_id, 'ack ' || f.code || ' ' || ifnull(k.key, f.subject_id) \
         FROM finding f LEFT JOIN subject k ON k.kind = f.subject_kind AND k.id = f.subject_id \
         WHERE f.ack_statement_id IS NOT NULL) \
         SELECT s.file || ' ' || s.kind || ' ' || ifnull((SELECT group_concat(t, '; ') \
         FROM (SELECT t FROM target WHERE sid = s.id ORDER BY t)), ''), \
         json_array(ifnull(e.body, ''), ifnull(c.body, '')) FROM statement s \
         LEFT JOIN text e ON e.id = s.evidence_text LEFT JOIN text c ON c.id = s.caveat_text",
        subject_cte()
    )
}

fn schema(conn: &rusqlite::Connection) -> Res<(i64, Option<String>)> {
    let user = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    let meta = query(
        conn,
        "SELECT value FROM meta WHERE key = 'schema_version'",
        |r| r.get(0),
    )?;
    Ok((user, meta.into_iter().next()))
}

fn placements(conn: &rusqlite::Connection) -> Res<BTreeMap<Column, (String, BTreeSet<Placement>)>> {
    let mut out: BTreeMap<Column, (String, BTreeSet<Placement>)> = BTreeMap::new();
    for (table, column, disposition) in query(
        conn,
        "SELECT t.physical_name, c.name, c.disposition FROM holding_column c \
         JOIN holding_table t ON t.id = c.holding_id",
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )? {
        out.insert((table, column), (disposition, BTreeSet::new()));
    }
    for (table, column, placement) in query(
        conn,
        "SELECT t.physical_name, c.name, m.table_pid, m.variable_pid, \
         ifnull(CAST(m.start AS TEXT), ''), ifnull(CAST(m.end AS TEXT), '') FROM holding_mapping m \
         JOIN holding_column c ON c.id = m.column_id JOIN holding_table t ON t.id = c.holding_id",
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                (r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?),
            ))
        },
    )? {
        if let Some((_, set)) = out.get_mut(&(table, column)) {
            set.insert(placement);
        }
    }
    Ok(out)
}

/// Placed: at least one mapping. Held: hold/unmap disposition, no mapping. Unplaced:
/// a candidate with no mapping.
fn status(disposition: &str, placed: &BTreeSet<Placement>) -> &'static str {
    if !placed.is_empty() {
        "placed"
    } else if matches!(disposition, "hold" | "unmap") {
        "held"
    } else {
        "unplaced"
    }
}

fn table_pids(placed: &BTreeSet<Placement>) -> String {
    let pids: BTreeSet<&str> = placed.iter().map(|p| p.0.as_str()).collect();
    if pids.is_empty() {
        "(none)".to_owned()
    } else {
        pids.into_iter().collect::<Vec<_>>().join(", ")
    }
}

const CLASSES: [&str; 4] = [
    "newly placed",
    "folded or renamed",
    "variable or interval changed",
    "unmapped",
];

/// The reason class of a placement change, from its table pids alone.
fn reason(old: &str, new: &str) -> &'static str {
    match (old, new) {
        ("(none)", _) => "newly placed",
        (_, "(none)") => "unmapped",
        (o, n) if o == n => "variable or interval changed",
        _ => "folded or renamed",
    }
}

fn shown(item: Item) -> Item {
    Item {
        counts: false,
        ..item
    }
}

type Census = BTreeMap<Column, (String, BTreeSet<Placement>)>;

/// Placed, held and unplaced totals, and the columns on one side only.
fn totals(old: &Census, new: &Census) -> Vec<Item> {
    let count = |side: &Census| {
        let mut t = BTreeMap::from([("held", 0), ("placed", 0), ("unplaced", 0)]);
        for (d, set) in side.values() {
            *t.entry(status(d, set)).or_default() += 1;
        }
        t
    };
    let (to, tn) = (count(old), count(new));
    let mut counts: Vec<Item> = ["placed", "held", "unplaced"]
        .into_iter()
        .map(|s| {
            let item = Item::new(
                json!({"status": s, "old": to[s], "new": tn[s]}),
                format!("{s}: {} → {}", to[s], tn[s]),
            );
            if to[s] == tn[s] { shown(item) } else { item }
        })
        .collect();
    let only = |a: &BTreeMap<Column, _>, b: &BTreeMap<Column, _>| -> Vec<Value> {
        a.keys()
            .filter(|k| !b.contains_key(*k))
            .map(|(t, c)| json!({"table": t, "column": c}))
            .collect()
    };
    for (name, list) in [
        ("only in old", only(old, new)),
        ("only in new", only(new, old)),
    ] {
        if !list.is_empty() {
            counts.push(Item::new(
                json!({"status": name, "columns": list}),
                format!("columns {name}: {}", list.len()),
            ));
        }
    }
    counts
}

fn census(p: &Pair) -> Res<Vec<Section>> {
    let (old, new) = (placements(&p.old)?, placements(&p.new)?);
    let mut counts = totals(&old, &new);

    let mut moves: BTreeMap<(String, String), Vec<Value>> = BTreeMap::new();
    let mut statuses: BTreeMap<(&str, &str), Vec<Value>> = BTreeMap::new();
    for (key, (od, op)) in &old {
        let Some((nd, np)) = new.get(key) else {
            continue;
        };
        let column = json!({"table": key.0, "column": key.1});
        if op != np {
            let mut c = column;
            c["old"] = json!(op);
            c["new"] = json!(np);
            moves
                .entry((table_pids(op), table_pids(np)))
                .or_default()
                .push(c);
        } else if status(od, op) != status(nd, np) {
            statuses
                .entry((status(od, op), status(nd, np)))
                .or_default()
                .push(column);
        }
    }
    let mut moves: Vec<_> = moves.into_iter().collect();
    // Largest groups first, ties in key order.
    moves.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.0.cmp(&b.0)));
    let mut classes: BTreeMap<&str, Vec<Item>> = BTreeMap::new();
    for ((o, n), columns) in moves {
        let class = reason(&o, &n);
        classes.entry(class).or_default().push(Item::new(
            json!({"old": o, "new": n, "columns": columns}),
            format!("{o} → {n}: {} columns", columns.len()),
        ));
    }
    let status_items = statuses
        .into_iter()
        .map(|((o, n), columns)| {
            Item::new(
                json!({"old": o, "new": n, "columns": columns}),
                format!("{o} → {n}: {} columns", columns.len()),
            )
        })
        .collect();
    for class in CLASSES {
        let n: usize = classes.get(class).map_or(0, |items| {
            items
                .iter()
                .map(|i| i.json["columns"].as_array().map_or(0, Vec::len))
                .sum()
        });
        if n > 0 {
            counts.push(Item::new(
                json!({"status": "placement changed", "class": class, "columns": n}),
                format!("placement changed, {class}: {n} columns"),
            ));
        }
    }
    let mut placement = Section {
        key: "placement_changes",
        title: "changed placements (by old → new table_pid)",
        groups: Vec::new(),
    };
    for class in CLASSES {
        if let Some(items) = classes.remove(class) {
            placement.groups.push((class, items));
        }
    }
    if placement.groups.is_empty() {
        placement.groups.push(("", Vec::new()));
    }
    Ok(vec![
        Section::flat("placement", "census placement", counts),
        placement,
        Section::flat(
            "status_changes",
            "held/unplaced status changes",
            status_items,
        ),
    ])
}

fn text_maps(conn: &rusqlite::Connection, sql: &str) -> Res<BTreeMap<String, BTreeSet<String>>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (k, v) in query(conn, sql, |r| Ok((r.get(0)?, r.get(1)?)))? {
        out.entry(k).or_default().insert(v);
    }
    Ok(out)
}

/// Per kind: counts, and the keys (never the bodies) changed, added and removed.
fn texts(p: &Pair) -> Res<Section> {
    let statement = statement_sql();
    let kinds = TEXT
        .iter()
        .copied()
        .chain([("statement", statement.as_str())]);
    let mut items = Vec::new();
    for (kind, sql) in kinds {
        let (old, new) = (text_maps(&p.old, sql)?, text_maps(&p.new, sql)?);
        let changed: Vec<&String> = old
            .iter()
            .filter(|(k, v)| new.get(*k).is_some_and(|n| n != *v))
            .map(|(k, _)| k)
            .collect();
        let added: Vec<&String> = new.keys().filter(|k| !old.contains_key(*k)).collect();
        let removed: Vec<&String> = old.keys().filter(|k| !new.contains_key(*k)).collect();
        let item = Item::new(
            json!({"kind": kind, "old": old.len(), "new": new.len(),
                   "changed": changed, "added": added, "removed": removed}),
            format!(
                "{kind}: {} → {}; changed {}, added {}, removed {}",
                old.len(),
                new.len(),
                changed.len(),
                added.len(),
                removed.len()
            ),
        );
        items.push(
            if changed.is_empty() && added.is_empty() && removed.is_empty() {
                shown(item)
            } else {
                item
            },
        );
    }
    Ok(Section::flat("text", "reader-visible text", items))
}

fn bag(conn: &rusqlite::Connection, sql: &str) -> Res<Bag> {
    let mut out = Bag::new();
    for k in query(conn, sql, |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
    })? {
        *out.entry(k).or_default() += 1;
    }
    Ok(out)
}

/// Per-code counts and the (code, subject, period) multiset differences.
fn delta(key: &'static str, title: &'static str, pair: &Pair, sql: &str) -> Res<Section> {
    let (old, new) = (bag(&pair.old, sql)?, bag(&pair.new, sql)?);
    let keys: BTreeSet<_> = old.keys().chain(new.keys()).collect();
    let mut by_code: BTreeMap<&str, [i64; 4]> = BTreeMap::new();
    let mut lists: BTreeMap<&str, (Vec<Value>, Vec<Value>)> = BTreeMap::new();
    for k in keys {
        let (o, n) = (
            old.get(k).copied().unwrap_or(0),
            new.get(k).copied().unwrap_or(0),
        );
        let e = by_code.entry(&k.0).or_default();
        e[0] += o;
        e[1] += n;
        let row = json!([k.1, k.2, k.3]);
        let l = lists.entry(&k.0).or_default();
        for _ in 0..(n - o).max(0) {
            e[2] += 1;
            l.0.push(row.clone());
        }
        for _ in 0..(o - n).max(0) {
            e[3] += 1;
            l.1.push(row.clone());
        }
    }
    let items = by_code
        .into_iter()
        .filter(|(_, c)| c[2] > 0 || c[3] > 0)
        .map(|(code, [o, n, a, r])| {
            let (added, removed) = lists.remove(code).unwrap_or_default();
            Item::new(
                json!({"code": code, "old": o, "new": n, "added": added, "removed": removed}),
                format!("{code}: {o} → {n}; added {a}, removed {r}"),
            )
        })
        .collect();
    Ok(Section::flat(key, title, items))
}

pub(super) fn sections(p: &Pair, old_path: &str, new_path: &str) -> Res<Vec<Section>> {
    let (os, ns) = (schema(&p.old)?, schema(&p.new)?);
    if os != ns {
        return Err(format!(
            "holdings schema mismatch: (user_version, schema_version) {old_path} {os:?}, {new_path} {ns:?}"
        ));
    }
    let meta_sql = "SELECT key, value FROM meta WHERE key IN \
                    ('generation_id', 'steward_commit', 'compiler_digest') ORDER BY key";
    let meta = |conn| {
        query(conn, meta_sql, |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })
    };
    let (om, nm) = (meta(&p.old)?, meta(&p.new)?);
    let mut sides = vec![shown(Item::new(
        json!({"user_version": os.0, "schema_version": os.1}),
        format!(
            "schema: user_version {}, schema_version {}",
            os.0,
            os.1.as_deref().unwrap_or("-")
        ),
    ))];
    for (side, m) in [("old", om), ("new", nm)] {
        let text = m
            .iter()
            .map(|(k, v)| format!("{k} {}", v.as_deref().unwrap_or("-")))
            .collect::<Vec<_>>()
            .join("; ");
        let obj: serde_json::Map<String, Value> =
            m.into_iter().map(|(k, v)| (k, json!(v))).collect();
        sides.push(shown(Item::new(
            json!({"side": side, "meta": obj}),
            format!("{side}: {text}"),
        )));
    }
    let subjects = subject_cte();
    let findings_sql = format!(
        "WITH subject(kind, id, key) AS ({subjects}) \
         SELECT f.code, f.subject_kind || ':' || ifnull(s.key, f.subject_id), \
         ifnull(CAST(f.start AS TEXT), ''), ifnull(CAST(f.end AS TEXT), '') FROM finding f \
         LEFT JOIN subject s ON s.kind = f.subject_kind AND s.id = f.subject_id"
    );
    let notices_sql = "SELECT n.code, t.physical_name || '/' || ifnull(c.name, ''), \
                       ifnull(CAST(n.start AS TEXT), ''), ifnull(CAST(n.end AS TEXT), '') \
                       FROM holding_notice n JOIN holding_table t ON t.id = n.holding_id \
                       LEFT JOIN holding_column c ON c.id = n.column_id";
    let mut out = vec![Section::flat("meta", "holdings", sides)];
    out.extend(census(p)?);
    out.push(texts(p)?);
    out.push(delta(
        "findings",
        "findings (by code, subject, period)",
        p,
        &findings_sql,
    )?);
    out.push(delta(
        "notices",
        "holdings notices (by code, table/column, period)",
        p,
        notices_sql,
    )?);
    Ok(out)
}
