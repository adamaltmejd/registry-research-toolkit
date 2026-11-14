//! `reg-build diff [--holdings] OLD NEW [--json] [-n N]`: what changed between two
//! catalogs, or with `--holdings` two holdings catalogs (`holdings.rs`)
//! (`crates/DESIGN.md`, "Release diff").
//!
//! Everything is compared by natural key (pid, table name, meta key), never by row id:
//! row ids are renumbered every build. Each section lists at most `N` differences in
//! text; `--json` prints every difference as one object. Exit 0 when nothing differs, 1
//! when something does, 2 on a usage or IO error or an incomparable schema version
//! (OLD may be on an older catalog schema; `catalog` says how that is reported).

use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;

use reg_core::issue::iso_day;
use rusqlite::{Connection, OpenFlags, Row};
use serde_json::{Value, json};

mod holdings;
use crate::error::Result;

const USAGE: &str = "usage: reg-build diff [--holdings] OLD NEW [--json] [-n N]";
/// Meta keys that differ on every release: shown, but never a difference.
const VOLATILE_META: [&str; 4] = [
    "generation_id",
    "catalog_version",
    "builder_version",
    "curation_digest",
];
const FTS_SHADOWS: [&str; 5] = ["_data", "_idx", "_docsize", "_config", "_content"];
/// Codes listed per side for a value set whose code list changed.
const CODES_SHOWN: usize = 10;

struct Args {
    old: String,
    new: String,
    json: bool,
    holdings: bool,
    limit: usize,
}

fn parse(args: &[String]) -> Result<Args> {
    let mut paths = Vec::new();
    let (mut json, mut holdings, mut limit) = (false, false, 20);
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--holdings" => holdings = true,
            "-n" => {
                limit = it
                    .next()
                    .and_then(|v| v.parse().ok())
                    .ok_or(format!("-n needs a number\n{USAGE}"))?;
            }
            flag if flag.starts_with('-') => {
                return Err(format!("unknown flag {flag:?}\n{USAGE}").into());
            }
            path => paths.push(path.to_owned()),
        }
    }
    let [old, new] = <[String; 2]>::try_from(paths).map_err(|_| USAGE.to_owned())?;
    Ok(Args {
        old,
        new,
        json,
        holdings,
        limit,
    })
}

/// One difference. `counts` is false for those that never make the exit code 1.
struct Item {
    json: Value,
    text: String,
    counts: bool,
}

impl Item {
    fn new(json: Value, text: String) -> Self {
        Self {
            json,
            text,
            counts: true,
        }
    }
}

struct Section {
    key: &'static str,
    title: &'static str,
    /// Named groups print under their name and make a JSON object; a section of one
    /// unnamed group is a JSON array.
    groups: Vec<(&'static str, Vec<Item>)>,
}

impl Section {
    fn flat(key: &'static str, title: &'static str, items: Vec<Item>) -> Self {
        Self {
            key,
            title,
            groups: vec![("", items)],
        }
    }

    fn differs(&self) -> bool {
        self.groups.iter().any(|(_, g)| g.iter().any(|i| i.counts))
    }

    fn text(&self, limit: usize) -> String {
        let mut lines = vec![format!("== {} ==", self.title)];
        if self.groups.iter().all(|(_, g)| g.is_empty()) {
            lines.push("no change".to_owned());
        }
        // The cap is per section: the groups share one budget.
        let mut left = limit;
        for (name, items) in self.groups.iter().filter(|(_, g)| !g.is_empty()) {
            let pad = if name.is_empty() {
                ""
            } else {
                lines.push(format!("{name}:"));
                "  "
            };
            let shown = items.len().min(left);
            for item in &items[..shown] {
                lines.extend(item.text.lines().map(|line| format!("{pad}{line}")));
            }
            left -= shown;
            if items.len() > shown {
                lines.push(format!("{pad}… and {} more", items.len() - shown));
            }
        }
        lines.join("\n") + "\n"
    }

    fn json(&self) -> Value {
        let list = |items: &[Item]| Value::Array(items.iter().map(|i| i.json.clone()).collect());
        match self.groups.as_slice() {
            [("", items)] => list(items),
            groups => Value::Object(
                groups
                    .iter()
                    .map(|(name, items)| ((*name).to_owned(), list(items)))
                    .collect(),
            ),
        }
    }
}

/// The sections, and the (old, current) schema versions of a cross-schema diff.
type Report = (Vec<Section>, Option<(u32, u32)>);

/// The old and the new catalog, both read-only.
struct Pair {
    old: Connection,
    new: Connection,
}

fn open(path: &str) -> Result<Connection> {
    if !std::path::Path::new(path).is_file() {
        return Err(format!("{path}: no such file").into());
    }
    Ok(
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| format!("{path}: {e}"))?,
    )
}

fn query<T>(
    conn: &Connection,
    sql: &str,
    f: impl Fn(&Row) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    let mut stmt = conn.prepare(sql).map_err(|e| format!("{sql}: {e}"))?;
    let rows = stmt.query_map([], f).map_err(|e| format!("{sql}: {e}"))?;
    Ok(rows
        .collect::<rusqlite::Result<_>>()
        .map_err(|e| format!("{sql}: {e}"))?)
}

impl Pair {
    /// `sql` on both sides, collected into a map by `f`'s key.
    fn maps<K: Ord, V>(
        &self,
        sql: &str,
        f: impl Fn(&Row) -> rusqlite::Result<(K, V)>,
    ) -> Result<(BTreeMap<K, V>, BTreeMap<K, V>)> {
        let load = |c| query(c, sql, &f).map(|v| v.into_iter().collect());
        Ok((load(&self.old)?, load(&self.new)?))
    }
}

fn schema_version(conn: &Connection, path: &str) -> Result<String> {
    let version: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .map_err(|e| format!("{path}: no meta.schema_version ({e})"))?;
    version.ok_or(format!("{path}: no meta.schema_version").into())
}

/// The keys whose values differ, in key order; a side that lacks the key is `None`.
fn changed<'a, K: Ord, V: PartialEq>(
    old: &'a BTreeMap<K, V>,
    new: &'a BTreeMap<K, V>,
) -> Vec<(&'a K, Option<&'a V>, Option<&'a V>)> {
    let keys: BTreeSet<&K> = old.keys().chain(new.keys()).collect();
    keys.into_iter()
        .filter_map(|k| {
            let (o, n) = (old.get(k), new.get(k));
            (o != n).then_some((k, o, n))
        })
        .collect()
}

fn schema(p: &Pair) -> Result<Section> {
    let (old, new) = p.maps("SELECT type, name, sql FROM sqlite_master", |r| {
        Ok((
            (r.get::<_, String>(0)?, r.get::<_, String>(1)?),
            r.get::<_, Option<String>>(2)?,
        ))
    })?;
    let items = changed(&old, &new)
        .into_iter()
        .map(|((kind, name), o, n)| {
            let change = match (o, n) {
                (None, _) => "added",
                (_, None) => "removed",
                _ => "changed",
            };
            Item::new(
                json!({"type": kind, "name": name, "change": change, "old_sql": o, "new_sql": n}),
                format!("{change} {kind} {name}"),
            )
        })
        .collect();
    Ok(Section::flat("schema", "schema", items))
}

fn meta(p: &Pair) -> Result<Section> {
    let (old, new) = p.maps("SELECT key, value FROM meta", |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    let items = changed(&old, &new)
        .into_iter()
        .map(|(key, o, n)| {
            let counts = !VOLATILE_META.contains(&key.as_str());
            let note = if counts { "" } else { " (not counted)" };
            Item {
                json: json!({"key": key, "old": o, "new": n, "counted": counts}),
                text: format!("{key}: {} → {}{note}", show(o), show(n)),
                counts,
            }
        })
        .collect();
    Ok(Section::flat("meta", "meta", items))
}

fn show(v: Option<&String>) -> String {
    v.map_or_else(|| "(absent)".to_owned(), Clone::clone)
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The ordinary tables of a catalog: no virtual (FTS) tables and no FTS shadow tables.
fn ordinary_tables(conn: &Connection) -> Result<BTreeSet<String>> {
    let defs: BTreeMap<String, String> = query(
        conn,
        "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )?
    .into_iter()
    .collect();
    let virtuals: Vec<&String> = defs
        .iter()
        .filter(|(_, sql)| sql.starts_with("CREATE VIRTUAL"))
        .map(|(name, _)| name)
        .collect();
    Ok(defs
        .iter()
        .filter(|(name, sql)| {
            !sql.starts_with("CREATE VIRTUAL")
                && !virtuals.iter().any(|v| {
                    name.strip_prefix(v.as_str())
                        .is_some_and(|s| FTS_SHADOWS.contains(&s))
                })
        })
        .map(|(name, _)| name.clone())
        .collect())
}

/// Row counts per table; `skip` holds the one-sided tables of a cross-schema diff,
/// reported under schema changes instead.
fn rows(p: &Pair, skip: &BTreeSet<String>) -> Result<Section> {
    // Both sides' tables: a table dropped by a same-major revision still reports its old count.
    let names: BTreeSet<String> = ordinary_tables(&p.old)?
        .union(&ordinary_tables(&p.new)?)
        .filter(|name| !skip.contains(*name))
        .cloned()
        .collect();
    let count = |conn: &Connection, name: &str| -> Result<Option<i64>> {
        let exists: i64 = conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [name],
            |r| r.get(0),
        )?;
        if exists == 0 {
            return Ok(None);
        }
        Ok(conn
            .query_row(
                &format!("SELECT count(*) FROM {}", quote_ident(name)),
                [],
                |r| r.get(0),
            )
            .map(Some)
            .map_err(|e| format!("{name}: {e}"))?)
    };
    let mut items = Vec::new();
    for name in &names {
        let (o, n) = (count(&p.old, name)?, count(&p.new, name)?);
        if o != n {
            let fmt = |v: Option<i64>| v.map_or_else(|| "(absent)".to_owned(), |v| v.to_string());
            items.push(Item::new(
                json!({"table": name, "old": o, "new": n}),
                format!("{name}: {} → {}", fmt(o), fmt(n)),
            ));
        }
    }
    Ok(Section::flat("rows", "rows", items))
}

fn ids(p: &Pair) -> Result<Section> {
    let pair = |r: &Row| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?));
    let (old, new) = p.maps("SELECT pid, kind FROM ident", pair)?;
    let (old_retired, new_retired) = p.maps("SELECT pid, kind FROM ident_retired", pair)?;
    let item = |pid: &str, kind: &str, text: String| {
        Item::new(
            json!({"pid": pid, "kind": kind}),
            format!("{pid} ({kind}){text}"),
        )
    };
    let (mut added, mut removed, mut kind_changed) = (Vec::new(), Vec::new(), Vec::new());
    for (pid, o, n) in changed(&old, &new) {
        match (o, n) {
            (None, Some(kind)) => added.push(item(pid, kind, String::new())),
            // A pid that moved to ident_retired is listed there, not as removed.
            (Some(kind), None) if !new_retired.contains_key(pid) => {
                removed.push(item(pid, kind, String::new()));
            }
            (Some(o), Some(n)) => kind_changed.push(Item::new(
                json!({"pid": pid, "old": o, "new": n}),
                format!("{pid}: {o} → {n}"),
            )),
            _ => {}
        }
    }
    let retired = |from: &BTreeMap<String, String>, other: &BTreeMap<String, String>| {
        from.iter()
            .filter(|(pid, _)| !other.contains_key(*pid))
            .map(|(pid, kind)| item(pid, kind, String::new()))
            .collect()
    };
    let moved = crate::id_targets::moved(&p.old, "main", &p.new, "main")?
        .into_iter()
        .map(|m| {
            Item::new(
                json!({"pid": m.pid, "kind": m.kind, "old": m.old, "new": m.new}),
                m.text(),
            )
        })
        .collect();
    Ok(Section {
        key: "ids",
        title: "ids",
        // The breaking groups come first: they share one line budget with additions.
        groups: vec![
            ("removed", removed),
            ("kind_changed", kind_changed),
            ("moved", moved),
            ("added", added),
            ("retired_added", retired(&new_retired, &old_retired)),
            ("retired_removed", retired(&old_retired, &new_retired)),
        ],
    })
}

pub(super) fn span(start: i64, end: i64) -> String {
    format!("[{}, {})", iso_day(start), iso_day(end))
}

#[derive(PartialEq)]
struct TableInfo {
    name: String,
    description: Option<String>,
    role: String,
    timeless: i64,
    cadence: Option<String>,
    spans: Vec<(i64, i64)>,
    /// Deliveries whose edition disagrees with the primary (`release.edition_disagreement`,
    /// read by the order check's warning). A schema without the column has none.
    disagreeing: Vec<String>,
}

impl TableInfo {
    fn json(&self) -> Value {
        json!({"name": self.name, "description": self.description,
               "role": self.role, "timeless": self.timeless, "cadence": self.cadence,
               "spans": self.spans.iter().map(|&(s, e)| json!([iso_day(s), iso_day(e)])).collect::<Vec<_>>(),
               "edition_disagreement": self.disagreeing})
    }

    fn text(&self) -> String {
        format!(
            "name={:?} description={:?} role={} timeless={} cadence={} spans={} edition_disagreement={:?}",
            self.name,
            self.description,
            self.role,
            self.timeless,
            self.cadence.as_deref().unwrap_or("-"),
            self.spans
                .iter()
                .map(|&(s, e)| span(s, e))
                .collect::<String>(),
            self.disagreeing
        )
    }
}

fn table_infos(conn: &Connection) -> Result<BTreeMap<String, TableInfo>> {
    let mut infos: BTreeMap<String, TableInfo> = query(
        conn,
        "SELECT t.pid, n.body, d.body, t.role, t.timeless, t.cadence
                     FROM tbl t
                     JOIN text n ON n.id = t.name_text
                     LEFT JOIN text d ON d.id = t.description_text",
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                TableInfo {
                    name: r.get(1)?,
                    description: r.get(2)?,
                    role: r.get(3)?,
                    timeless: r.get(4)?,
                    cadence: r.get(5)?,
                    spans: Vec::new(),
                    disagreeing: Vec::new(),
                },
            ))
        },
    )?
    .into_iter()
    .collect();
    let spans = query(
        conn,
        "SELECT t.pid, s.start, s.end FROM tbl_span s JOIN tbl t ON t.id = s.table_id
         ORDER BY t.pid, s.start",
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        },
    )?;
    for (pid, start, end) in spans {
        if let Some(info) = infos.get_mut(&pid) {
            info.spans.push((start, end));
        }
    }
    let flagged: bool = conn.query_row(
        "SELECT count(*) = 1 FROM pragma_table_info('release')
         WHERE name = 'edition_disagreement'",
        [],
        |r| r.get(0),
    )?;
    if flagged {
        let rows = query(
            conn,
            "SELECT t.pid, n.body FROM release r JOIN tbl t ON t.id = r.table_id
             JOIN text n ON n.id = r.name_text
             WHERE r.edition_disagreement = 1 ORDER BY t.pid, n.body",
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )?;
        for (pid, name) in rows {
            if let Some(info) = infos.get_mut(&pid) {
                info.disagreeing.push(name);
            }
        }
    }
    Ok(infos)
}

fn tables(p: &Pair) -> Result<Section> {
    let (old, new) = (table_infos(&p.old)?, table_infos(&p.new)?);
    let items = changed(&old, &new)
        .into_iter()
        .filter_map(|(pid, o, n)| Some((pid, o?, n?)))
        .map(|(pid, o, n)| {
            Item::new(
                json!({"pid": pid, "old": o.json(), "new": n.json()}),
                format!("{pid}: {} → {}", o.text(), n.text()),
            )
        })
        .collect();
    Ok(Section::flat("tables", "tables", items))
}

/// A segment as users see it: the unit the variables section compares.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Seg {
    table: String,
    start: i64,
    end: i64,
    spelling: String,
    dtype: Option<String>,
    valueset: String,
    state: String,
    coded: String,
    sensitive: Option<i64>,
    identifier: Option<i64>,
    role: String,
}

impl Seg {
    fn json(&self) -> Value {
        json!({"table": self.table, "start": iso_day(self.start), "end": iso_day(self.end),
               "spelling": self.spelling, "dtype": self.dtype, "valueset": self.valueset,
               "state": self.state, "coded": self.coded, "sensitive": self.sensitive,
               "identifier": self.identifier, "role": self.role})
    }

    fn text(&self) -> String {
        let flag = |v: Option<i64>| v.map_or_else(|| "-".to_owned(), |v| v.to_string());
        format!(
            "{} {} {} dtype={} vs={} {} coded={} sensitive={} identifier={} {}",
            self.table,
            span(self.start, self.end),
            self.spelling,
            self.dtype.as_deref().unwrap_or("-"),
            self.valueset.get(..12).unwrap_or(&self.valueset),
            self.state,
            self.coded,
            flag(self.sensitive),
            flag(self.identifier),
            self.role
        )
    }
}

fn segments(conn: &Connection) -> Result<BTreeMap<String, BTreeSet<Seg>>> {
    let rows = query(
        conn,
        "SELECT v.pid, t.pid, s.start, s.end, sp.exact, s.dtype, coalesce(vs.hash, ''),
                s.state, s.coded, s.sensitive, s.identifier, s.role
         FROM segment s
         JOIN variable v ON v.id = s.variable_id
         JOIN tbl t ON t.id = s.table_id
         JOIN spelling sp ON sp.id = s.spelling_id
         LEFT JOIN valueset vs ON vs.id = s.valueset_id",
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                Seg {
                    table: r.get(1)?,
                    start: r.get(2)?,
                    end: r.get(3)?,
                    spelling: r.get(4)?,
                    dtype: r.get(5)?,
                    valueset: r.get(6)?,
                    state: r.get(7)?,
                    coded: r.get(8)?,
                    sensitive: r.get(9)?,
                    identifier: r.get(10)?,
                    role: r.get(11)?,
                },
            ))
        },
    )?;
    let mut out: BTreeMap<String, BTreeSet<Seg>> = BTreeMap::new();
    for (pid, seg) in rows {
        out.entry(pid).or_default().insert(seg);
    }
    Ok(out)
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Run {
    start: i64,
    end: i64,
    label: String,
    definition: Option<String>,
    opdef: Option<String>,
    unit: Option<String>,
}

impl Run {
    fn json(&self) -> Value {
        json!({"start": iso_day(self.start), "end": iso_day(self.end), "label": self.label,
               "definition": self.definition, "opdef": self.opdef, "unit": self.unit})
    }
}

fn runs(conn: &Connection) -> Result<BTreeMap<String, BTreeSet<Run>>> {
    let rows = query(
        conn,
        "SELECT v.pid, vt.start, vt.end, l.body, d.body, o.body, u.body
         FROM variable_text vt
         JOIN variable v ON v.id = vt.variable_id
         JOIN text l ON l.id = vt.label_text
         LEFT JOIN text d ON d.id = vt.definition_text
         LEFT JOIN text o ON o.id = vt.opdef_text
         LEFT JOIN text u ON u.id = vt.unit_text",
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                Run {
                    start: r.get(1)?,
                    end: r.get(2)?,
                    label: r.get(3)?,
                    definition: r.get(4)?,
                    opdef: r.get(5)?,
                    unit: r.get(6)?,
                },
            ))
        },
    )?;
    let mut out: BTreeMap<String, BTreeSet<Run>> = BTreeMap::new();
    for (pid, run) in rows {
        out.entry(pid).or_default().insert(run);
    }
    Ok(out)
}

/// A segment whose only change is its value-set hash.
struct HashChange {
    pid: String,
    table: String,
    start: i64,
    end: i64,
    old: String,
    new: String,
}

/// The segments of a variable that differ only in value-set hash, paired old to new by
/// everything else. Segments that changed in other ways stay unpaired and are reported
/// by the variables section alone.
fn hash_changes(pid: &str, old: &[&Seg], new: &[&Seg]) -> Vec<HashChange> {
    let masked = |s: &Seg| Seg {
        valueset: String::new(),
        ..s.clone()
    };
    let by_mask: BTreeMap<Seg, &Seg> = new.iter().map(|s| (masked(s), *s)).collect();
    old.iter()
        .filter_map(|o| by_mask.get(&masked(o)).map(|n| (*o, *n)))
        .map(|(o, n)| HashChange {
            pid: pid.to_owned(),
            table: o.table.clone(),
            start: o.start,
            end: o.end,
            old: o.valueset.clone(),
            new: n.valueset.clone(),
        })
        .collect()
}

fn variables(p: &Pair) -> Result<(Section, Vec<HashChange>)> {
    let pids = |c: &Connection| -> Result<BTreeSet<String>> {
        Ok(query(c, "SELECT pid FROM variable", |r| r.get(0))?
            .into_iter()
            .collect())
    };
    let (old_pids, new_pids) = (pids(&p.old)?, pids(&p.new)?);
    let (old_segs, new_segs) = (segments(&p.old)?, segments(&p.new)?);
    let (old_runs, new_runs) = (runs(&p.old)?, runs(&p.new)?);
    let (old_labels, new_labels) = p.maps(
        "SELECT v.pid, t.body FROM variable v JOIN text t ON t.id = v.label_text",
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )?;
    let (old_descriptions, new_descriptions) = p.maps(
        "SELECT v.pid, d.body FROM variable v LEFT JOIN text d ON d.id = v.description_text",
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
    )?;
    let (no_segs, no_runs) = (BTreeSet::new(), BTreeSet::new());
    let (mut items, mut changes) = (Vec::new(), Vec::new());
    for pid in old_pids.intersection(&new_pids) {
        let (so, sn) = (
            old_segs.get(pid).unwrap_or(&no_segs),
            new_segs.get(pid).unwrap_or(&no_segs),
        );
        let seg_old: Vec<&Seg> = so.difference(sn).collect();
        let seg_new: Vec<&Seg> = sn.difference(so).collect();
        let (ro, rn) = (
            old_runs.get(pid).unwrap_or(&no_runs),
            new_runs.get(pid).unwrap_or(&no_runs),
        );
        let (run_old, run_new): (Vec<&Run>, Vec<&Run>) =
            (ro.difference(rn).collect(), rn.difference(ro).collect());
        let (label_old, label_new) = (old_labels.get(pid), new_labels.get(pid));

        let mut facets = serde_json::Map::new();
        let mut lines = vec![pid.clone()];
        if !seg_old.is_empty() || !seg_new.is_empty() {
            facets.insert(
                "segments".into(),
                json!({"old_only": seg_old.iter().map(|s| s.json()).collect::<Vec<_>>(),
                       "new_only": seg_new.iter().map(|s| s.json()).collect::<Vec<_>>()}),
            );
            lines.extend(seg_old.iter().map(|s| format!("  segment - {}", s.text())));
            lines.extend(seg_new.iter().map(|s| format!("  segment + {}", s.text())));
            changes.extend(hash_changes(pid, &seg_old, &seg_new));
        }
        if label_old != label_new {
            facets.insert("label".into(), json!({"old": label_old, "new": label_new}));
            lines.push(format!(
                "  label: {} → {}",
                show(label_old),
                show(label_new)
            ));
        }
        let (description_old, description_new) = (
            old_descriptions.get(pid).and_then(Option::as_ref),
            new_descriptions.get(pid).and_then(Option::as_ref),
        );
        if description_old != description_new {
            facets.insert(
                "description".into(),
                json!({"old": description_old, "new": description_new}),
            );
            lines.push(format!(
                "  description: {} → {}",
                show(description_old),
                show(description_new)
            ));
        }
        if !run_old.is_empty() || !run_new.is_empty() {
            let n = run_old.len().max(run_new.len());
            facets.insert(
                "texts".into(),
                json!({"changed": n,
                       "old": ro.iter().map(Run::json).collect::<Vec<_>>(),
                       "new": rn.iter().map(Run::json).collect::<Vec<_>>()}),
            );
            lines.push(format!("  texts: {n} changed runs"));
        }
        if !facets.is_empty() {
            facets.insert("pid".into(), json!(pid));
            items.push(Item::new(Value::Object(facets), lines.join("\n")));
        }
    }
    Ok((Section::flat("variables", "variables", items), changes))
}

fn short(hash: &str) -> &str {
    hash.get(..12).unwrap_or(hash)
}

/// Set-valued metadata, compared by its published entity key and member string.
pub(super) fn memberships(
    conn: &Connection,
    sql: &str,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (key, member) in query(conn, sql, |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })? {
        out.entry(key).or_default().insert(member);
    }
    Ok(out)
}

/// The codes of the wanted value sets, in one pass over `vs_item`.
fn codes(
    conn: &Connection,
    wanted: &BTreeSet<String>,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    if wanted.is_empty() {
        return Ok(out);
    }
    let sql = "SELECT vs.hash, t.body FROM vs_item i
               JOIN valueset vs ON vs.id = i.valueset_id
               JOIN text t ON t.id = i.code_text";
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let hash: String = row.get(0)?;
        if wanted.contains(&hash) {
            out.entry(hash).or_default().insert(row.get(1)?);
        }
    }
    Ok(out)
}

fn valuesets(p: &Pair, changes: &[HashChange]) -> Result<Section> {
    let (old, new) = p.maps("SELECT hash, n_items FROM valueset", |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    })?;
    let sentinel_codes = "SELECT v.hash, t.body FROM vs_item i
                          JOIN valueset v ON v.id = i.valueset_id
                          JOIN text t ON t.id = i.code_text WHERE i.sentinel = 1";
    let (old_sentinels, new_sentinels) = (
        memberships(&p.old, sentinel_codes)?,
        memberships(&p.new, sentinel_codes)?,
    );
    let sentinels = changed(&old_sentinels, &new_sentinels)
        .into_iter()
        .map(|(hash, old, new)| {
            Item::new(
                json!({"hash": hash, "old": old, "new": new}),
                format!("{} sentinels: {old:?} → {new:?}", short(hash)),
            )
        })
        .collect();
    let only = |from: &BTreeMap<String, i64>, other: &BTreeMap<String, i64>| -> Vec<Item> {
        from.iter()
            .filter(|(h, _)| !other.contains_key(*h))
            .map(|(hash, n)| {
                Item::new(
                    json!({"hash": hash, "n_items": n}),
                    format!("{} ({n} items)", short(hash)),
                )
            })
            .collect()
    };
    let gather = |pick: fn(&HashChange) -> &String| -> BTreeSet<String> {
        changes.iter().map(|c| pick(c).clone()).collect()
    };
    let (old_codes, new_codes) = (
        codes(&p.old, &gather(|c| &c.old))?,
        codes(&p.new, &gather(|c| &c.new))?,
    );
    let no_codes = BTreeSet::new();
    let mut code_items = Vec::new();
    for c in changes {
        let before = old_codes.get(&c.old).unwrap_or(&no_codes);
        let after = new_codes.get(&c.new).unwrap_or(&no_codes);
        let added: Vec<&String> = after.difference(before).collect();
        let removed: Vec<&String> = before.difference(after).collect();
        let list = |sign: char, codes: &[&String]| {
            let shown: Vec<String> = codes
                .iter()
                .take(CODES_SHOWN)
                .map(|c| format!("{sign}{c}"))
                .collect();
            let more = codes.len().saturating_sub(CODES_SHOWN);
            let tail = if more > 0 {
                format!(" … {more} more")
            } else {
                String::new()
            };
            format!("{}{tail}", shown.join(" "))
        };
        code_items.push(Item::new(
            json!({"variable": c.pid, "table": c.table, "start": iso_day(c.start), "end": iso_day(c.end),
                   "old_hash": c.old, "new_hash": c.new,
                   "added": added, "removed": removed}),
            format!(
                "{} {} {}: {} | {}",
                c.pid,
                c.table,
                span(c.start, c.end),
                list('+', &added),
                list('-', &removed)
            ),
        ));
    }
    Ok(Section {
        key: "valuesets",
        title: "valuesets",
        groups: vec![
            ("removed", only(&old, &new)),
            ("added", only(&new, &old)),
            ("codes", code_items),
            ("sentinels", sentinels),
        ],
    })
}

/// Provider, register and set names, descriptions and urls, keyed by pid.
fn registers(p: &Pair) -> Result<Section> {
    // Fields stay separate (and NULL apart from text), so no value can mimic another.
    type Fields = Vec<(&'static str, Option<String>)>;
    let load = |conn: &Connection| -> Result<BTreeMap<(&'static str, String), Fields>> {
        let providers = query(
            conn,
            "SELECT p.pid, n.body, p.url FROM provider p JOIN text n ON n.id = p.name_text",
            |r| {
                Ok((
                    ("provider", r.get::<_, String>(0)?),
                    vec![("name", r.get(1)?), ("url", r.get(2)?)],
                ))
            },
        )?;
        let registers = query(
            conn,
            "SELECT r.pid, n.body, d.body FROM register r
             JOIN text n ON n.id = r.name_text
             LEFT JOIN text d ON d.id = r.description_text",
            |r| {
                Ok((
                    ("register", r.get::<_, String>(0)?),
                    vec![("name", r.get(1)?), ("description", r.get(2)?)],
                ))
            },
        )?;
        let sets = query(
            conn,
            "SELECT s.pid, n.body, d.body FROM var_set s
             JOIN text n ON n.id = s.name_text
             LEFT JOIN text d ON d.id = s.description_text WHERE s.kind = 'set'",
            |r| {
                Ok((
                    ("set", r.get::<_, String>(0)?),
                    vec![("name", r.get(1)?), ("description", r.get(2)?)],
                ))
            },
        )?;
        Ok(providers.into_iter().chain(registers).chain(sets).collect())
    };
    let (old, new) = (load(&p.old)?, load(&p.new)?);
    let render = |f: Option<&Fields>| {
        f.map_or_else(
            || "(absent)".to_owned(),
            |f| {
                f.iter()
                    .map(|(k, v)| format!("{k}={v:?}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            },
        )
    };
    let object = |f: Option<&Fields>| {
        f.map(|f| Value::Object(f.iter().map(|(k, v)| ((*k).to_owned(), json!(v))).collect()))
    };
    let items = changed(&old, &new)
        .into_iter()
        .map(|((kind, pid), o, n)| {
            Item::new(
                json!({"kind": kind, "pid": pid, "old": object(o), "new": object(n)}),
                format!("{kind} {pid}: {} → {}", render(o), render(n)),
            )
        })
        .collect();
    Ok(Section::flat("registers", "registers", items))
}

/// Series breaks added and removed, by subject kind; a release is named by its table and
/// delivery name. An old catalog without the table has none.
fn series_breaks(p: &Pair) -> Result<Section> {
    type Key = (String, String, Option<String>, Option<String>, String);
    let load = |conn: &Connection| -> Result<BTreeSet<Key>> {
        if !ordinary_tables(conn)?.contains("series_break") {
            return Ok(BTreeSet::new());
        }
        let found: Vec<Key> = query(
            conn,
            "SELECT b.subject_kind,
               CASE b.subject_kind
                 WHEN 'register' THEN (SELECT pid FROM register WHERE id = b.subject_id)
                 WHEN 'table' THEN (SELECT pid FROM tbl WHERE id = b.subject_id)
                 WHEN 'variable' THEN (SELECT pid FROM variable WHERE id = b.subject_id)
                 ELSE (SELECT t.pid || ' @ ' || n.body FROM release x
                       JOIN tbl t ON t.id = x.table_id JOIN text n ON n.id = x.name_text
                       WHERE x.id = b.subject_id) END,
               b.at, t.body, b.prov
             FROM series_break b LEFT JOIN text t ON t.id = b.text_id",
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get::<_, Option<i64>>(2)?.map(iso_day),
                    r.get(3)?,
                    r.get(4)?,
                ))
            },
        )?;
        Ok(found.into_iter().collect())
    };
    let (old, new) = (load(&p.old)?, load(&p.new)?);
    let mut groups = Vec::new();
    for kind in ["register", "table", "release", "variable"] {
        let item = |sign: &str, (_, pid, at, text, prov): &Key| {
            let text_shown: String = text
                .as_deref()
                .unwrap_or("(no text)")
                .chars()
                .take(80)
                .collect();
            Item::new(
                json!({"change": if sign == "+" { "added" } else { "removed" },
                       "subject": pid, "at": at, "text": text, "provenance": prov}),
                format!(
                    "{sign} {pid} {} [{prov}]: {text_shown}",
                    at.as_deref().unwrap_or("undated")
                ),
            )
        };
        let items: Vec<Item> = old
            .difference(&new)
            .filter(|k| k.0 == kind)
            .map(|k| item("-", k))
            .chain(
                new.difference(&old)
                    .filter(|k| k.0 == kind)
                    .map(|k| item("+", k)),
            )
            .collect();
        groups.push((kind, items));
    }
    Ok(Section {
        key: "series_breaks",
        title: "series breaks",
        groups,
    })
}

/// The columns of each ordinary table.
fn columns(conn: &Connection) -> Result<BTreeMap<String, BTreeSet<String>>> {
    ordinary_tables(conn)?
        .into_iter()
        .map(|table| {
            let cols = query(
                conn,
                &format!(
                    "SELECT name FROM pragma_table_info({})",
                    quote_literal(&table)
                ),
                |r| r.get(0),
            )?;
            Ok((table, cols.into_iter().collect()))
        })
        .collect()
}

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Tables and columns on one side only, across a schema bump. The other sections read
/// only what both sides have, so these are named here once and their rows never diffed.
fn schema_changes(p: &Pair) -> Result<(Section, BTreeSet<String>)> {
    let (old, new) = (columns(&p.old)?, columns(&p.new)?);
    let mut one_sided = BTreeSet::new();
    let (mut tables_removed, mut tables_added) = (Vec::new(), Vec::new());
    let (mut columns_removed, mut columns_added) = (Vec::new(), Vec::new());
    let table = |name: &str| Item::new(json!({"table": name}), name.to_owned());
    let column = |t: &str, c: &str| Item::new(json!({"table": t, "column": c}), format!("{t}.{c}"));
    for (name, cols) in &old {
        match new.get(name) {
            None => {
                tables_removed.push(table(name));
                one_sided.insert(name.clone());
            }
            Some(other) => columns_removed.extend(cols.difference(other).map(|c| column(name, c))),
        }
    }
    for (name, cols) in &new {
        match old.get(name) {
            None => {
                tables_added.push(table(name));
                one_sided.insert(name.clone());
            }
            Some(other) => columns_added.extend(cols.difference(other).map(|c| column(name, c))),
        }
    }
    let section = Section {
        key: "schema_changes",
        title: "schema changes",
        groups: vec![
            ("tables_removed", tables_removed),
            ("tables_added", tables_added),
            ("columns_removed", columns_removed),
            ("columns_added", columns_added),
        ],
    };
    Ok((section, one_sided))
}

/// The catalog sections, after the schema-version check. NEW must have this build's
/// schema; OLD may have an older one (a release diffs across a schema bump) but never a
/// newer one. Returns (old, current) schema versions when they differ.
fn catalog(pair: &Pair, args: &Args) -> Result<Report> {
    let expected = reg_core::CATALOG_SCHEMA_VERSION;
    let refuse = |path: &str, version: &str| -> crate::error::Error {
        format!("{path}: schema version {version}, this build diffs {expected}").into()
    };
    let new = schema_version(&pair.new, &args.new)?;
    if new != expected.to_string() {
        return Err(refuse(&args.new, &new));
    }
    let old_text = schema_version(&pair.old, &args.old)?;
    let old = match old_text.parse::<u32>() {
        Ok(v) if v <= expected => v,
        _ => return Err(refuse(&args.old, &old_text)),
    };
    if old == expected {
        let (variables, hash_changes) = variables(pair)?;
        return Ok((
            vec![
                schema(pair)?,
                meta(pair)?,
                rows(pair, &BTreeSet::new())?,
                ids(pair)?,
                registers(pair)?,
                series_breaks(pair)?,
                tables(pair)?,
                variables,
                valuesets(pair, &hash_changes)?,
            ],
            None,
        ));
    }
    // A query the older schema cannot answer is a located refusal, not a silent gap.
    let sections = (|| -> Result<Vec<Section>> {
        let (changes, one_sided) = schema_changes(pair)?;
        let mut schema = schema(pair)?;
        for (_, items) in &mut schema.groups {
            items.retain(|i| {
                !(i.json["type"] == "table"
                    && one_sided.contains(i.json["name"].as_str().unwrap_or_default()))
            });
        }
        let (variables, hash_changes) = variables(pair)?;
        Ok(vec![
            changes,
            schema,
            meta(pair)?,
            rows(pair, &one_sided)?,
            ids(pair)?,
            registers(pair)?,
            series_breaks(pair)?,
            tables(pair)?,
            variables,
            valuesets(pair, &hash_changes)?,
        ])
    })()
    .map_err(|e| format!("{}: schema {old} is not comparable: {e}", args.old))?;
    Ok((sections, Some((old, expected))))
}

/// The report: text or JSON, and whether any counted difference exists.
fn diff(args: &Args) -> Result<(String, bool)> {
    let pair = Pair {
        old: open(&args.old)?,
        new: open(&args.new)?,
    };
    let (sections, cross) = if args.holdings {
        holdings::sections(&pair, &args.old, &args.new)?
    } else {
        catalog(&pair, args)?
    };
    let differs = sections.iter().any(Section::differs);
    let out = if args.json {
        let mut tree: serde_json::Map<String, Value> = sections
            .iter()
            .map(|s| (s.key.to_owned(), s.json()))
            .collect();
        if let Some((old, current)) = cross {
            tree.insert(
                "cross_schema".into(),
                json!({"previous": old, "current": current}),
            );
        }
        let mut tree = Value::Object(tree);
        tree.sort_all_objects();
        tree.to_string() + "\n"
    } else {
        let head = cross.map_or_else(String::new, |(old, current)| {
            format!("cross-schema comparison: previous schema {old}, current schema {current}\n\n")
        });
        head + &sections
            .iter()
            .map(|s| s.text(args.limit))
            .collect::<Vec<_>>()
            .join("\n")
    };
    Ok((out, differs))
}

pub fn run(args: &[String]) -> ExitCode {
    match parse(args).and_then(|a| diff(&a)) {
        Ok((out, differs)) => {
            print!("{out}");
            ExitCode::from(u8::from(differs))
        }
        Err(e) => {
            eprintln!("reg-build: {e}");
            ExitCode::from(2)
        }
    }
}
