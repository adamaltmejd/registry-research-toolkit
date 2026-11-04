//! `graph`: the typed relations around a variable, a table or a register.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, named_params};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::{Cache, Operation, Param, Params, Period, Server, Type, component, refs};
use crate::{Code, Error, rows};

/// `relation.kind`.
const KINDS: &[&str] = &["continues", "equivalent", "derived_from", "label_of"];

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "graph",
    paths: &["/api/graph/{ref}"],
    tool: Some("graph"),
    description: "The relations around a variable, a table or a register: what it \
        continues or is continued by, what it is derived from, with the day a succession \
        starts and the source's own words about it. `kind=derived_from` is the lineage.",
    params: &[
        Param::required("ref", Type::Ref),
        Param::optional("kind", Type::Enum(KINDS)),
    ],
    cache: Cache::Minute,
    run: graph,
    result: component::<Graph>,
}];

/// `shape.Graph`: the focus, its nodes and the relations between them.
#[derive(Serialize, ToSchema)]
pub struct Graph {
    /// The ref's fqid.
    focus: String,
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
}

#[derive(Serialize, ToSchema)]
pub struct GraphNode {
    fqid: String,
    /// `variable`, `table`, `register` or `class_version`.
    kind: String,
    /// A register's or table's name, a variable's or class version's label.
    name: String,
}

#[derive(Serialize, ToSchema)]
pub struct GraphEdge {
    /// `continues`, `equivalent`, `derived_from` or `label_of`.
    kind: String,
    /// The subject's fqid.
    from: String,
    /// The object's fqid.
    to: String,
    /// From the day a succession takes over; `null` when the source states no day.
    #[schema(required = true)]
    at: Option<Period>,
    /// `source`, `curated` or `rule`.
    prov: String,
    /// The source's own words about the relation.
    #[schema(required = true)]
    caveat: Option<String>,
}

/// An entity as the `relation` table names it: its kind and row id.
type End = (String, i64);
type Nodes = BTreeMap<(String, String), String>;
type Edge = ((String, String, String, Option<i64>), GraphEdge);

fn graph(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect_for_ref(params["ref"])?;
    let value = params["ref"];
    let kind = params.get("kind").copied();
    if kind.is_some_and(|k| !KINDS.contains(&k)) {
        return Err(Error::invalid_parameter("kind"));
    }
    let entity = refs::resolve(&conn, value)?;
    let mut ends: Vec<End> = vec![(entity.kind.clone(), entity.id)];
    match entity.kind.as_str() {
        "variable" | "table" => {}
        "register" => {
            // A register with an implicit table has that table's relations too: the
            // table carries the register's pid.
            let implicit = rows(
                &conn,
                "SELECT id FROM tbl WHERE pid = ?",
                [&entity.pid],
                |row| row.get::<_, i64>(0),
            )?;
            ends.extend(implicit.into_iter().map(|id| ("table".to_owned(), id)));
        }
        // simplify: classification graphs (versions and their correspondences) are not
        // served; serve them when a reader needs version correspondences.
        "class_family" | "class_version" => {
            return Err(Error::new(
                Code::InternalError,
                format!("{value:?}: classification graphs are not served yet."),
                vec![],
            ));
        }
        other => {
            return Err(Error::new(
                Code::InvalidRef,
                format!("{value:?} is a {other}, not a variable, a table or a register."),
                vec![value.into()],
            ));
        }
    }

    let mut nodes = Nodes::new();
    let (pid, name) = node(&conn, &(entity.kind.clone(), entity.id))?;
    nodes.insert((pid, entity.kind.clone()), name);
    let mut edges = Vec::new();
    let mut seen = BTreeSet::new();
    for end in &ends {
        let mut statement = conn.prepare(
            "SELECT r.id, r.kind, r.subject_kind, r.subject_id, r.object_kind, r.object_id,
                    r.at_start, r.at_end, r.prov, c.body
             FROM relation r LEFT JOIN text c ON c.id = r.caveat_text
             WHERE ((r.subject_kind = :kind AND r.subject_id = :id)
                    OR (r.object_kind = :kind AND r.object_id = :id))
               AND (:relation IS NULL OR r.kind = :relation)",
        )?;
        let rows = statement
            .query_map(
                named_params! {":kind": end.0, ":id": end.1, ":relation": kind},
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        (row.get::<_, String>(1)?, row.get::<_, Option<i64>>(6)?),
                        (row.get::<_, String>(2)?, row.get::<_, i64>(3)?),
                        (row.get::<_, String>(4)?, row.get::<_, i64>(5)?),
                        (row.get::<_, Option<i64>>(7)?, row.get::<_, String>(8)?),
                        row.get::<_, Option<String>>(9)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (id, (relation, at_start), subject, object, (at_end, prov), caveat) in rows {
            if !seen.insert(id) {
                continue;
            }
            let (from, from_name) = node(&conn, &subject)?;
            let (to, to_name) = node(&conn, &object)?;
            nodes.insert((from.clone(), subject.0), from_name);
            nodes.insert((to.clone(), object.0), to_name);
            let at = at_start.map(|start| server.period(start, at_end.unwrap_or(server.open.1)));
            edges.push((
                (relation.clone(), from.clone(), to.clone(), at_start),
                GraphEdge {
                    kind: relation,
                    from,
                    to,
                    at,
                    prov,
                    caveat,
                },
            ));
        }
    }
    external_relations(server, &entity.pid, &ends, kind, &mut nodes, &mut edges)?;
    // NULL `at_start` sorts first; the key is the contract's, which `relation`'s
    // uniqueness makes total up to the implicit table's shared pid.
    edges.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(serde_json::to_value(Graph {
        focus: entity.pid,
        nodes: nodes
            .into_iter()
            .map(|((fqid, kind), name)| GraphNode { fqid, kind, name })
            .collect(),
        edges: edges.into_iter().map(|(_, edge)| edge).collect(),
    })
    .expect("a graph serializes"))
}

/// Qualified relations keep each endpoint in its own database origin. The base
/// concept also sees the reverse edge; possessing a slot never grants the concept.
fn external_relations(
    server: &Server,
    focus: &str,
    ends: &[End],
    kind: Option<&str>,
    nodes: &mut Nodes,
    edges: &mut Vec<Edge>,
) -> Result<(), Error> {
    let Some(extra) = server.catalog.connect_holdings()? else {
        return Ok(());
    };
    let base = server.catalog.connect()?;
    let mut query = extra.prepare(
        "SELECT kind, subject_kind, subject_pid, object_kind, object_pid,
                at_start, at_end, caveat
         FROM holding_relation
         WHERE (subject_pid = ?1 OR object_pid = ?1)
           AND (?2 IS NULL OR kind = ?2)
         ORDER BY kind, subject_pid, object_pid, at_start",
    )?;
    let rows = query.query_map((focus, kind), |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, Option<i64>>(5)?,
            r.get::<_, Option<i64>>(6)?,
            r.get::<_, Option<String>>(7)?,
        ))
    })?;
    for row in rows {
        let (relation, subject_kind, from, object_kind, to, start, end, caveat) = row?;
        let selected = |pid: &str, kind: &str| pid == focus && ends.iter().any(|e| e.0 == kind);
        if !selected(&from, &subject_kind) && !selected(&to, &object_kind) {
            continue;
        }
        let (_, from_name) = node_by_pid(&extra, &subject_kind, &from)?;
        let (_, to_name) = node_by_pid(&base, &object_kind, &to)?;
        nodes.insert((from.clone(), subject_kind), from_name);
        nodes.insert((to.clone(), object_kind), to_name);
        edges.push((
            (relation.clone(), from.clone(), to.clone(), start),
            GraphEdge {
                kind: relation,
                from,
                to,
                at: start.map(|s| server.period(s, end.unwrap_or(server.open.1))),
                prov: "curated".to_owned(),
                caveat,
            },
        ));
    }
    Ok(())
}

fn node_by_pid(conn: &Connection, kind: &str, pid: &str) -> Result<(String, String), Error> {
    let (table, _) = node_table(kind);
    let id = conn.query_row(
        &format!("SELECT id FROM {table} WHERE pid = ?"),
        [pid],
        |r| r.get(0),
    )?;
    node(conn, &(kind.to_owned(), id))
}

fn node_table(kind: &str) -> (&'static str, &'static str) {
    match kind {
        "variable" => ("variable", "label_text"),
        "table" => ("tbl", "name_text"),
        "register" => ("register", "name_text"),
        _ => ("class_version", "label_text"),
    }
}

/// An entity's public id and display name: a register's or table's name, a variable's
/// or class version's label.
fn node(conn: &Connection, (kind, id): &End) -> Result<(String, String), Error> {
    let (table, column) = node_table(kind);
    Ok(conn.query_row(
        &format!(
            "SELECT e.pid, t.body FROM {table} e JOIN text t ON t.id = e.{column} WHERE e.id = ?"
        ),
        [id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}
