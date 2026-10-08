//! `graph`: today's relationship graph (`reg_meta.graph`) of a variable, a
//! classification or a group ref. Nodes are variables (with their whole state
//! history folded into representation runs) and classification editions; the one
//! edge kind is succession. A variable unites its concept group's members, a
//! classification its curated groups; nodes and edges keep their insertion order
//! and are deduplicated by id.

use std::collections::{BTreeSet, HashMap, HashSet};

use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs::{self, Target, fqid};
use super::show::{self, Facet, Group};
use super::{Params, Server, states};
use crate::{Code, Error, Scope};

/// The start of a state with an unknown start, and the end of an open one.
const UNKNOWN_FROM: &str = "0001-01-01";
const OPEN_TO: &str = "9999-12-31";

/// The graph; no nodes means there is nothing to draw.
#[derive(Serialize, ToSchema)]
pub struct Graph {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
    /// The node of the requested variable or classification; none for a group.
    focus_id: Option<String>,
}

#[derive(Clone, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Node {
    Variable(VariableNode),
    Classification(ClassificationNode),
}

/// A variable with its state history. A succession edition that is not live in
/// scope is a bare node: no states, group or metadata.
#[derive(Clone, Serialize, ToSchema)]
pub struct VariableNode {
    /// The variable's FQID.
    id: String,
    fqid: String,
    label: String,
    /// `<provider>/<register>/<key>` of its concept group.
    group_key: Option<String>,
    definition: Option<String>,
    description: Option<String>,
    operational_definition: Option<String>,
    states: Vec<GraphState>,
    same_as: Vec<SameAs>,
    /// Its facets in its concept group (the first member that is this variable).
    facets: Vec<Facet>,
    group_label: Option<String>,
}

/// A classification edition, a point in time.
#[derive(Clone, Serialize, ToSchema)]
pub struct ClassificationNode {
    /// `class/<slug>`.
    id: String,
    fqid: String,
    label: String,
    /// `class/<key>` of the classification group or family it is drawn in.
    group_key: Option<String>,
    short_name: Option<String>,
    version_year: Option<i64>,
    is_current: bool,
    group_label: Option<String>,
}

/// One emitted state; states sharing `representation_run_id` form one cell.
#[derive(Clone, Serialize, ToSchema)]
pub struct GraphState {
    #[serde(serialize_with = "states::storage_id")]
    #[schema(value_type = String)]
    state_id: i64,
    period_scope: String,
    variant: String,
    variant_label: Option<String>,
    variant_family: Option<String>,
    variant_family_label: Option<String>,
    /// Increments at a change of variant, period scope, or (between states) value
    /// set, version label, classifications or delivery column.
    representation_run_id: i64,
    delivery_column_name: Option<String>,
    #[serde(serialize_with = "states::optional_storage_id")]
    #[schema(value_type = Option<String>)]
    value_set_id: Option<i64>,
    value_set_version_label: String,
    classification_slugs: Vec<String>,
    /// None for an unknown start.
    valid_from: Option<String>,
    /// None for an open end.
    valid_to: Option<String>,
}

#[derive(Clone, Serialize, ToSchema)]
pub struct SameAs {
    fqid: String,
    /// The register slug.
    register: String,
}

/// A directed succession edge, predecessor to successor. A representation edge
/// names its columns and, when scoped, its variant.
#[derive(Clone, Serialize, ToSchema)]
pub struct Edge {
    id: String,
    kind: &'static str,
    source: String,
    target: String,
    label: Option<String>,
    effective_year: Option<i64>,
    source_column: Option<String>,
    target_column: Option<String>,
    variant: Option<String>,
}

pub fn graph(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    let value = params.get("ref").copied().unwrap_or_default();
    let mut builder = Builder::new(&conn, scope);
    let focus = match refs::resolve(&conn, scope, Some(value))? {
        Target::Variable { id } => {
            let slugs = slugs(&conn, id)?;
            let focus = builder.add_variable(&slugs)?;
            if let Some(key) = group_key(&conn, id)?
                && let Some(group) = builder.group(&slugs[0], &slugs[1], &key)?
            {
                builder.add_members(&group)?;
            }
            focus
        }
        Target::Classification { slug, .. } => {
            let focus = builder.add_classification(&slug, None)?;
            let fqid = format!("class/{slug}");
            for group in show::classification_groups(&conn)? {
                if group.members.iter().any(|m| m.fqid == fqid) {
                    builder.add_classification_group(&group.key, &group.label, &group)?;
                }
            }
            focus
        }
        Target::Group {
            provider,
            register,
            key,
            ..
        } => {
            let group = builder
                .group(&provider, &register, &key)?
                .ok_or_else(|| refs::not_found(value))?;
            builder.add_members(&group)?;
            None
        }
        Target::ClassificationGroup { key } => {
            if let Some(group) = show::classification_groups(&conn)?
                .into_iter()
                .find(|g| g.key == key)
            {
                builder.add_classification_group(&group.key, &group.label, &group)?;
            } else {
                let family = show::families(&conn)?
                    .into_iter()
                    .find(|f| f.key == key)
                    .ok_or_else(|| refs::not_found(value))?;
                let members: BTreeSet<String> =
                    family.editions.iter().map(|e| e.slug.clone()).collect();
                let grouping = (format!("class/{key}"), family.label.clone(), members);
                for edition in &family.editions {
                    builder.add_classification(&edition.slug, Some(&grouping))?;
                }
            }
            None
        }
        _ => {
            return Err(Error::new(
                Code::InvalidRef,
                format!("{value:?} is not a variable, classification or group ref."),
                vec![value.into()],
            ));
        }
    };
    Ok(serde_json::to_value(builder.build(focus)).expect("Graph serializes"))
}

/// A variable's `[provider, register, variable]` slugs.
fn slugs(conn: &Connection, id: i64) -> Result<[String; 3], Error> {
    Ok(conn.query_row(
        "SELECT p.slug, r.slug, v.slug FROM variable v JOIN register r USING(register_id) \
         JOIN provider p USING(provider_id) WHERE v.variable_id = ?",
        [id],
        |row| Ok([row.get(0)?, row.get(1)?, row.get(2)?]),
    )?)
}

/// The key of a variable's concept group (`show`'s `group`).
fn group_key(conn: &Connection, id: i64) -> Result<Option<String>, Error> {
    Ok(conn
        .query_row(
            "SELECT DISTINCT g.group_key FROM concept_group_variable m \
             JOIN concept_group g ON g.group_id = m.group_id \
             WHERE m.variable_id = ? AND g.kind = 'variable' ORDER BY g.group_key",
            [id],
            |row| row.get(0),
        )
        .optional()?)
}

/// A `variable_replaced_by` edge seen from one end: the other end's slugs, the
/// effective year and the reason.
type Succession = (Vec<String>, Option<i64>, Option<String>);

/// A `representation_replaced_by` row: the predecessor's slugs and column, the
/// successor's, the variant scope, the effective year and the reason.
type Representation = (
    [Option<String>; 3],
    String,
    [Option<String>; 3],
    String,
    Option<String>,
    Option<i64>,
    Option<String>,
);

/// A classification group's key, label and member slugs.
type Grouping = (String, String, BTreeSet<String>);

struct Builder<'c> {
    conn: &'c Connection,
    scope: Scope,
    nodes: Vec<Node>,
    index: HashMap<String, usize>,
    edges: Vec<Edge>,
    edge_ids: HashSet<String>,
    /// Variable nodes with their states; a bare node is replaced when its
    /// variable turns out live.
    hydrated: HashSet<String>,
    /// Classification slugs whose chain was read: a split's other branches are
    /// read from the split edition's own chain, so presence of a node is not enough.
    anchors: HashSet<String>,
    /// Classification nodes carrying their group's key.
    grouped: HashSet<String>,
    /// Variables whose representation edges were read, and the edges walked (a
    /// representation may return to an earlier column).
    representation_anchors: HashSet<String>,
    representation_edges: HashSet<[String; 5]>,
    /// A register's concept groups in scope, by (provider, register).
    groups: HashMap<(String, String), Vec<Group>>,
}

impl<'c> Builder<'c> {
    fn new(conn: &'c Connection, scope: Scope) -> Self {
        Self {
            conn,
            scope,
            nodes: Vec::new(),
            index: HashMap::new(),
            edges: Vec::new(),
            edge_ids: HashSet::new(),
            hydrated: HashSet::new(),
            anchors: HashSet::new(),
            grouped: HashSet::new(),
            representation_anchors: HashSet::new(),
            representation_edges: HashSet::new(),
            groups: HashMap::new(),
        }
    }

    /// Insert or replace the node `id`, keeping its place.
    fn put(&mut self, id: &str, node: Node) {
        if let Some(&i) = self.index.get(id) {
            self.nodes[i] = node;
        } else {
            self.index.insert(id.to_owned(), self.nodes.len());
            self.nodes.push(node);
        }
    }

    fn edge(&mut self, edge: Edge) {
        if self.edge_ids.insert(edge.id.clone()) {
            self.edges.push(edge);
        }
    }

    /// The concept group `key` of a register, members in scope.
    fn group(&mut self, provider: &str, register: &str, key: &str) -> Result<Option<Group>, Error> {
        let cache_key = (provider.to_owned(), register.to_owned());
        if !self.groups.contains_key(&cache_key) {
            let found = show::concept_groups(self.conn, self.scope, provider, register, None)?;
            self.groups.insert(cache_key.clone(), found);
        }
        Ok(self.groups[&cache_key]
            .iter()
            .find(|g| g.key == key)
            .cloned())
    }

    fn add_members(&mut self, group: &Group) -> Result<(), Error> {
        for member in &group.members {
            let slugs: Vec<String> = member.fqid.split('/').map(str::to_owned).collect();
            self.add_variable(&slugs)?;
        }
        Ok(())
    }

    /// The node of the variable at `slugs` when it is live in scope (today's
    /// `add_variable`), with its succession chain and representation edges.
    fn add_variable(&mut self, slugs: &[String]) -> Result<Option<String>, Error> {
        let Some(id) = refs::variable_id(self.conn, self.scope, slugs)? else {
            return Ok(None);
        };
        let node_id = slugs.join("/");
        let first = !self.index.contains_key(&node_id);
        if !self.hydrated.contains(&node_id) {
            let node = self.variable_node(id, slugs)?;
            self.put(&node_id, Node::Variable(node));
            self.hydrated.insert(node_id.clone());
        }
        if first {
            self.add_succession(slugs)?;
        }
        self.add_representations(&node_id, slugs)?;
        Ok(Some(node_id))
    }

    fn variable_node(&mut self, id: i64, slugs: &[String]) -> Result<VariableNode, Error> {
        let node_id = slugs.join("/");
        let (name, definition, description, operational_definition, register_id) =
            self.conn.query_row(
                "SELECT name, definition, description, operational_definition, register_id \
                 FROM variable WHERE variable_id = ?",
                [id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )?;
        let (group_key, facets, group_label) = match group_key(self.conn, id)? {
            Some(key) => {
                let group = self.group(&slugs[0], &slugs[1], &key)?;
                let member = group
                    .as_ref()
                    .and_then(|g| g.members.iter().find(|m| m.fqid == node_id));
                let (facets, label) = match (member, &group) {
                    (Some(m), Some(g)) => (m.facets.clone(), Some(g.label.clone())),
                    _ => (Vec::new(), None),
                };
                (
                    Some(format!("{}/{}/{key}", slugs[0], slugs[1])),
                    facets,
                    label,
                )
            }
            None => (None, Vec::new(), None),
        };
        let same_as = show::rows(
            self.conn,
            "SELECT b_provider, b_register, b_variable FROM variable_same_as \
             WHERE a_provider = ? AND a_register = ? AND a_variable = ? \
             ORDER BY b_provider, b_register, b_variable",
            [&slugs[0], &slugs[1], &slugs[2]],
            |row| {
                let register: String = row.get(1)?;
                Ok(fqid(&[row.get(0)?, Some(register.clone()), row.get(2)?])
                    .map(|fqid| SameAs { fqid, register }))
            },
        )?
        .into_iter()
        .flatten()
        .collect();
        Ok(VariableNode {
            label: name.unwrap_or_else(|| node_id.clone()),
            id: node_id.clone(),
            fqid: node_id,
            group_key,
            definition,
            description,
            operational_definition,
            states: self.states(id, register_id, &slugs[0], &slugs[1])?,
            same_as,
            facets,
            group_label,
        })
    }

    /// The variable's emitted states in scope, folded into representation runs
    /// (today's `_graph_states`).
    fn states(
        &self,
        id: i64,
        register_id: i64,
        provider: &str,
        register: &str,
    ) -> Result<Vec<GraphState>, Error> {
        struct Row {
            variant: String,
            variant_label: Option<String>,
            value_set_id: Option<i64>,
            value_set_version_label: String,
            /// The coded window's key start, or None for the state's own coding.
            coded_from: Option<String>,
        }
        let rows: HashMap<i64, Row> = show::rows(
            self.conn,
            "SELECT es.expanded_state_id, rv.slug, rv.name, \
             CASE WHEN aw.variable_id IS NULL THEN vs.value_set_id ELSE aw.value_set_id END, \
             CASE WHEN aw.variable_id IS NULL THEN vs.value_set_version_label \
             ELSE aw.value_set_version_label END, \
             CASE WHEN aw.variable_id IS NULL THEN NULL ELSE es.window_valid_from END \
             FROM expanded_state es JOIN variable_state vs USING(state_id) \
             JOIN register_variant rv ON rv.register_variant_id = es.register_variant_id \
             LEFT JOIN variable_alias_window aw ON es.kind = 'coded_window' \
             AND aw.variable_id = es.variable_id \
             AND aw.register_variant_id = es.register_variant_id \
             AND aw.delivery_column_name = es.delivery_column_name \
             AND aw.valid_from = es.window_valid_from WHERE es.variable_id = ?",
            [id],
            |row| {
                Ok((
                    row.get(0)?,
                    Row {
                        variant: row.get(1)?,
                        variant_label: row.get(2)?,
                        value_set_id: row.get(3)?,
                        value_set_version_label: row
                            .get::<_, Option<String>>(4)?
                            .unwrap_or_default(),
                        coded_from: row.get(5)?,
                    },
                ))
            },
        )?
        .into_iter()
        .collect();
        let families = show::variant_families(self.conn, register_id, provider, register)?;
        let mut seen = HashSet::new();
        let mut states: Vec<GraphState> = Vec::new();
        for emitted in states::emitted(self.conn, self.scope, id, None, None)? {
            if !seen.insert((emitted.state_id, emitted.delivery_column_name.clone())) {
                continue;
            }
            let row = &rows[&emitted.expanded_state_id];
            let classification_slugs = match &row.coded_from {
                Some(from) => show::rows(
                    self.conn,
                    "SELECT c.slug FROM alias_window_classification ac \
                     JOIN classification c ON c.id = ac.classification_id \
                     WHERE ac.variable_id = ? AND ac.register_variant_id = ? \
                     AND ac.delivery_column_name = ? AND ac.valid_from = ? ORDER BY c.slug",
                    rusqlite::params![
                        id,
                        emitted.register_variant_id,
                        emitted.delivery_column_name,
                        from
                    ],
                    |row| row.get(0),
                )?,
                None => show::rows(
                    self.conn,
                    "SELECT c.slug FROM state_classification sc \
                     JOIN classification c ON c.id = sc.classification_id \
                     WHERE sc.state_id = ? ORDER BY c.slug",
                    [emitted.state_id],
                    |row| row.get(0),
                )?,
            };
            let (variant_family, variant_family_label) = families
                .get(&row.variant)
                .cloned()
                .map_or((None, None), |(key, label)| (Some(key), Some(label)));
            states.push(GraphState {
                state_id: emitted.state_id,
                period_scope: emitted.period_scope,
                variant: row.variant.clone(),
                variant_label: row.variant_label.clone(),
                variant_family,
                variant_family_label,
                representation_run_id: 0,
                delivery_column_name: emitted.delivery_column_name,
                value_set_id: row.value_set_id,
                value_set_version_label: row.value_set_version_label.clone(),
                classification_slugs,
                valid_from: emitted.valid_from.filter(|v| v != UNKNOWN_FROM),
                valid_to: emitted.valid_to.filter(|v| v != OPEN_TO),
            });
        }
        fold_runs(&mut states);
        Ok(states)
    }
    /// The variable succession chain through `slugs` and its edges: back to the
    /// root through the first predecessor, forward through every successor (a
    /// split fans out). Today's `variable_chain` follows the first successor only.
    fn add_succession(&mut self, slugs: &[String]) -> Result<(), Error> {
        let start = slugs.to_vec();
        let mut seen = HashSet::from([start.clone()]);
        let mut backward = Vec::new();
        let mut current = start.clone();
        while let Some((pred, ..)) = self
            .variable_edges(&current, Side::Predecessors)?
            .into_iter()
            .next()
        {
            if !seen.insert(pred.clone()) {
                break;
            }
            backward.push(pred.clone());
            current = pred;
        }
        let mut chain: Vec<Vec<String>> = backward.into_iter().rev().collect();
        chain.push(start);
        let mut stack = vec![chain.last().cloned().expect("the start")];
        let mut forward = Vec::new();
        while let Some(node) = stack.pop() {
            let next: Vec<Vec<String>> = self
                .variable_edges(&node, Side::Successors)?
                .into_iter()
                .map(|(s, ..)| s)
                .filter(|s| seen.insert(s.clone()))
                .collect();
            // Depth first, in successor order.
            for successor in next.into_iter().rev() {
                stack.push(successor);
            }
            if node != *chain.last().expect("the start") {
                forward.push(node);
            }
        }
        chain.extend(forward);
        if chain.len() < 2 {
            return Ok(());
        }
        let mut placed: Vec<(Vec<String>, String)> = Vec::new();
        for edition in &chain {
            let Some(node_id) = self.ensure_edition(edition)? else {
                continue;
            };
            for (pred, year, reason) in self.variable_edges(edition, Side::Predecessors)? {
                if let Some((_, pred_id)) = placed.iter().find(|(p, _)| *p == pred) {
                    self.edge(succession(pred_id, &node_id, reason, year));
                }
            }
            placed.push((edition.clone(), node_id));
        }
        Ok(())
    }

    /// One side of a variable's `variable_replaced_by` edges, ordered by the other
    /// end: `(slugs, effective_year, reason)`.
    fn variable_edges(&self, slugs: &[String], side: Side) -> Result<Vec<Succession>, Error> {
        let (this, other) = match side {
            Side::Successors => ("predecessor", "successor"),
            Side::Predecessors => ("successor", "predecessor"),
        };
        show::rows(
            self.conn,
            &format!(
                "SELECT {other}_provider, {other}_register, {other}_variable, effective_year, \
                 beskrivning FROM variable_replaced_by WHERE {this}_provider = ? \
                 AND {this}_register = ? AND {this}_variable = ? ORDER BY 1, 2, 3"
            ),
            [&slugs[0], &slugs[1], &slugs[2]],
            |row| {
                Ok((
                    vec![row.get(0)?, row.get(1)?, row.get(2)?],
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
    }

    /// A succession edition's node: the full node when live in scope, else a bare
    /// one (today's `_ensure_edition_node`).
    fn ensure_edition(&mut self, slugs: &[String]) -> Result<Option<String>, Error> {
        let opt: Vec<Option<String>> = slugs.iter().cloned().map(Some).collect();
        let Some(node_id) = fqid(&opt) else {
            return Ok(None);
        };
        if self.hydrated.contains(&node_id) {
            return Ok(Some(node_id));
        }
        if let Some(id) = refs::variable_id(self.conn, self.scope, slugs)? {
            let node = self.variable_node(id, slugs)?;
            self.put(&node_id, Node::Variable(node));
            self.hydrated.insert(node_id.clone());
            self.add_representations(&node_id, slugs)?;
        } else if !self.index.contains_key(&node_id) {
            let name: Option<String> = self
                .conn
                .query_row(
                    "SELECT v.name FROM variable v JOIN register r USING(register_id) \
                     JOIN provider p USING(provider_id) \
                     WHERE p.slug = ? AND r.slug = ? AND v.slug = ?",
                    [&slugs[0], &slugs[1], &slugs[2]],
                    |row| row.get(0),
                )
                .optional()?
                .flatten();
            let node = VariableNode {
                id: node_id.clone(),
                fqid: node_id.clone(),
                label: name.unwrap_or_else(|| node_id.clone()),
                group_key: None,
                definition: None,
                description: None,
                operational_definition: None,
                states: Vec::new(),
                same_as: Vec::new(),
                facets: Vec::new(),
                group_label: None,
            };
            self.put(&node_id, Node::Variable(node));
        }
        Ok(Some(node_id))
    }

    /// The representation succession edges touching a variable, once per
    /// variable; each end joins the graph when live.
    fn add_representations(&mut self, node_id: &str, slugs: &[String]) -> Result<(), Error> {
        if !self.representation_anchors.insert(node_id.to_owned()) {
            return Ok(());
        }
        let found: Vec<Representation> = show::rows(
            self.conn,
            "SELECT predecessor_provider, predecessor_register, predecessor_variable, \
             predecessor_column, successor_provider, successor_register, successor_variable, \
             successor_column, variant, effective_year, beskrivning \
             FROM representation_replaced_by \
             WHERE (predecessor_provider = ?1 AND predecessor_register = ?2 \
             AND predecessor_variable = ?3) OR (successor_provider = ?1 \
             AND successor_register = ?2 AND successor_variable = ?3) \
             ORDER BY effective_year IS NULL, effective_year, predecessor_provider, \
             predecessor_register, predecessor_variable, predecessor_column, \
             successor_provider, successor_register, successor_variable, successor_column, \
             variant",
            [&slugs[0], &slugs[1], &slugs[2]],
            |row| {
                Ok((
                    [row.get(0)?, row.get(1)?, row.get(2)?],
                    row.get(3)?,
                    [row.get(4)?, row.get(5)?, row.get(6)?],
                    row.get(7)?,
                    row.get::<_, Option<String>>(8)?.filter(|v| !v.is_empty()),
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )?;
        for (pred, pred_column, succ, succ_column, variant, year, reason) in found {
            let (Some(source), Some(target)) = (fqid(&pred), fqid(&succ)) else {
                continue;
            };
            let walked = [
                source.clone(),
                pred_column.clone(),
                target.clone(),
                succ_column.clone(),
                variant.clone().unwrap_or_default(),
            ];
            if !self.representation_edges.insert(walked) {
                continue;
            }
            let split = |f: &str| f.split('/').map(str::to_owned).collect::<Vec<_>>();
            let source_id = self.add_variable(&split(&source))?;
            let target_id = self.add_variable(&split(&target))?;
            if source_id.is_none() || target_id.is_none() {
                continue;
            }
            let scope = variant.clone().unwrap_or_default();
            self.edge(Edge {
                id: format!(
                    "succession:representation:{source}:{pred_column}:{scope}->\
                     {target}:{succ_column}:{scope}"
                ),
                kind: "succession",
                source,
                target,
                label: reason,
                effective_year: year,
                source_column: Some(pred_column),
                target_column: Some(succ_column),
                variant,
            });
        }
        Ok(())
    }

    /// Every edition on `slug`'s anchored `classification_chain`, and the
    /// succession edges among them; a member of `grouping` carries its key and
    /// label (today's `add_classification`). The edition's node id, or None when
    /// it has no chain.
    fn add_classification(
        &mut self,
        slug: &str,
        grouping: Option<&Grouping>,
    ) -> Result<Option<String>, Error> {
        let self_id = format!("class/{slug}");
        if !self.anchors.insert(slug.to_owned()) {
            // A member reached first by another walk still takes its group's key.
            if grouping.is_some_and(|(_, _, members)| members.contains(slug)) {
                self.apply_grouping(&self_id, grouping);
            }
            return Ok(Some(self_id));
        }
        let chain = show::rows(
            self.conn,
            "SELECT ch.slug, c.name, c.short_name, c.valid_from, ch.is_current \
             FROM classification_chain ch LEFT JOIN classification c ON c.slug = ch.slug \
             WHERE ch.anchor_slug = ? ORDER BY ch.position",
            [slug],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            },
        )?;
        if chain.is_empty() {
            return Ok(None);
        }
        let in_chain: BTreeSet<&str> = chain.iter().map(|(s, ..)| s.as_str()).collect();
        for (edition, name, short_name, version_year, is_current) in &chain {
            let node_id = format!("class/{edition}");
            let member = grouping.filter(|(_, _, members)| members.contains(edition));
            if !self.index.contains_key(&node_id) {
                self.put(
                    &node_id,
                    Node::Classification(ClassificationNode {
                        id: node_id.clone(),
                        fqid: node_id.clone(),
                        label: name
                            .clone()
                            .or_else(|| short_name.clone())
                            .unwrap_or_else(|| node_id.clone()),
                        group_key: member.map(|(key, ..)| key.clone()),
                        short_name: short_name.clone(),
                        version_year: *version_year,
                        is_current: *is_current,
                        group_label: member.map(|(_, label, _)| label.clone()),
                    }),
                );
                if member.is_some() {
                    self.grouped.insert(node_id);
                }
            } else if member.is_some() {
                self.apply_grouping(&node_id, member);
            }
        }
        // Each edition's inbound edges, so a split's branches join correctly.
        for (edition, ..) in &chain {
            let preds = show::rows(
                self.conn,
                "SELECT predecessor_slug, effective_year, note FROM classification_replaced_by \
                 WHERE successor_slug = ? ORDER BY predecessor_slug",
                [edition],
                |row| Ok((row.get::<_, String>(0)?, row.get(1)?, row.get(2)?)),
            )?;
            for (pred, year, note) in preds {
                if in_chain.contains(pred.as_str()) {
                    let source = format!("class/{pred}");
                    self.edge(succession(&source, &format!("class/{edition}"), note, year));
                }
            }
        }
        Ok(Some(self_id))
    }

    /// Give a classification node built outside its group the group's key, once.
    fn apply_grouping(&mut self, node_id: &str, grouping: Option<&Grouping>) {
        let Some((key, label, _)) = grouping else {
            return;
        };
        if self.grouped.contains(node_id) {
            return;
        }
        if let Some(&i) = self.index.get(node_id)
            && let Node::Classification(node) = &mut self.nodes[i]
        {
            node.group_key = Some(key.clone());
            node.group_label = Some(label.clone());
            self.grouped.insert(node_id.to_owned());
        }
    }

    fn add_classification_group(
        &mut self,
        key: &str,
        label: &str,
        group: &Group,
    ) -> Result<(), Error> {
        let slugs: Vec<String> = group
            .members
            .iter()
            .filter_map(|m| m.fqid.strip_prefix("class/").map(str::to_owned))
            .collect();
        let grouping = (
            format!("class/{key}"),
            label.to_owned(),
            slugs.iter().cloned().collect(),
        );
        for slug in &slugs {
            self.add_classification(slug, Some(&grouping))?;
        }
        Ok(())
    }

    /// The graph; a lone node with no edges is nothing to draw when it is a
    /// classification, or an ungrouped variable in one representation run.
    fn build(self, focus: Option<String>) -> Graph {
        if let ([only], []) = (self.nodes.as_slice(), self.edges.as_slice()) {
            let empty = match only {
                Node::Classification(_) => true,
                Node::Variable(v) => {
                    v.group_key.is_none() && v.states.iter().all(|s| s.representation_run_id == 0)
                }
            };
            if empty {
                return Graph {
                    nodes: Vec::new(),
                    edges: Vec::new(),
                    focus_id: None,
                };
            }
        }
        Graph {
            nodes: self.nodes,
            edges: self.edges,
            focus_id: focus,
        }
    }
}

#[derive(Clone, Copy)]
enum Side {
    Successors,
    Predecessors,
}

fn succession(source: &str, target: &str, label: Option<String>, year: Option<i64>) -> Edge {
    Edge {
        id: format!("succession:{source}->{target}"),
        kind: "succession",
        source: source.to_owned(),
        target: target.to_owned(),
        label,
        effective_year: year,
        source_column: None,
        target_column: None,
        variant: None,
    }
}

/// Order a variable's states by variant, period scope and start, and number their
/// representation runs (today's `_graph_states`): a run ends at a change of variant
/// or period scope, or between two states at a change of value set, version label,
/// classifications or delivery column.
fn fold_runs(states: &mut [GraphState]) {
    let key = |s: &GraphState| {
        (
            s.variant.clone(),
            s.period_scope.clone(),
            s.valid_from.clone().unwrap_or_default(),
        )
    };
    states.sort_by_cached_key(key);
    let mut run = 0;
    for i in 1..states.len() {
        let (prev, cur) = (&states[i - 1], &states[i]);
        // Windows sharing a state are one representation delivered under several
        // columns, not a boundary.
        let boundary = cur.state_id != prev.state_id
            && (cur.value_set_id != prev.value_set_id
                || cur.value_set_version_label != prev.value_set_version_label
                || cur.classification_slugs != prev.classification_slugs
                || cur.delivery_column_name != prev.delivery_column_name);
        if cur.variant != prev.variant || cur.period_scope != prev.period_scope || boundary {
            run += 1;
        }
        states[i].representation_run_id = run;
    }
}
