//! `graph`: the typed relations around a variable, a table or a register.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, named_params};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::{Cache, Operation, Param, Params, Period, Server, Type, component, refs};
use crate::{Code, Error};

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

fn graph(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
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
            let implicit = conn
                .prepare("SELECT id FROM tbl WHERE pid = ?")?
                .query_map([&entity.pid], |row| row.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            ends.extend(implicit.into_iter().map(|id| ("table".to_owned(), id)));
        }
        // simplify: classification graphs (versions and their correspondences) land in D
        // with curated versions; C has no `class_*` rows, so these refs cannot resolve.
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

    let mut nodes: BTreeMap<(String, String), String> = BTreeMap::new();
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

/// An entity's public id and display name: a register's or table's name, a variable's
/// or class version's label.
fn node(conn: &Connection, (kind, id): &End) -> Result<(String, String), Error> {
    let (table, column) = match kind.as_str() {
        "variable" => ("variable", "label_text"),
        "table" => ("tbl", "name_text"),
        "register" => ("register", "name_text"),
        _ => ("class_version", "label_text"),
    };
    Ok(conn.query_row(
        &format!(
            "SELECT e.pid, t.body FROM {table} e JOIN text t ON t.id = e.{column} WHERE e.id = ?"
        ),
        [id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}
