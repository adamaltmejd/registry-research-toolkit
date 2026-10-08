//! `show`: the summary of any ref (`shape.Show`), from today's `CatalogNode` and group
//! node models (`reg_webapp/routes/catalog.py`) without the parts the facet
//! operations serve (states, lineage, chains, codes, warnings).

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, OptionalExtension, Row};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

mod browse;

use browse::{Coverage, Delivery, RegisterCoverage};

use super::refs::{self, Target, fqid};
use super::{Params, Server};
use crate::held::{self, Narrow};
use crate::{Error, Scope};

/// A `kind`-tagged summary of a ref.
#[derive(Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Show {
    Root(Root),
    Provider(Provider),
    Register(Register),
    Variable(Variable),
    ClassificationRoot(ClassificationRoot),
    Classification(Classification),
    ConceptGroup(ConceptGroup),
    ClassificationGroup(ClassificationGroup),
    ClassificationFamily(Family),
}

/// The catalog root: the providers in scope and the classification root.
#[derive(Serialize, ToSchema)]
pub struct Root {
    children: Vec<RootChild>,
}

#[derive(Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RootChild {
    Provider { fqid: String, name: Option<String> },
    ClassificationRoot { fqid: String, name: String },
}

/// A provider and its registers in scope.
#[derive(Serialize, ToSchema)]
pub struct Provider {
    fqid: String,
    name: Option<String>,
    children: Vec<RegisterChild>,
}

#[derive(Serialize, ToSchema)]
pub struct RegisterChild {
    fqid: String,
    name: Option<String>,
    purpose: Option<String>,
    tags: Vec<Tag>,
    coverage: Option<RegisterCoverage>,
}

/// A register: its variables, concept groups and variants in scope.
#[derive(Serialize, ToSchema)]
pub struct Register {
    fqid: String,
    name: Option<String>,
    purpose: Option<String>,
    tags: Vec<Tag>,
    children: Vec<VariableChild>,
    groups: Vec<Group>,
    variants: Vec<Variant>,
}

#[derive(Serialize, ToSchema)]
pub struct VariableChild {
    fqid: String,
    name: Option<String>,
    /// Null in holdings for a variable with no held delivery.
    coverage: Option<Coverage>,
    /// Every `(variant, column)` the variable is delivered under in scope.
    deliveries: Vec<Delivery>,
}

/// A variable's shared metadata. Its states, lineage and succession chain are the
/// `states`, `lineage` and `graph` operations.
#[derive(Serialize, ToSchema)]
pub struct Variable {
    fqid: String,
    name: Option<String>,
    definition: Option<String>,
    description: Option<String>,
    operational_definition: Option<String>,
    measurement_unit: Option<String>,
    is_sensitive: bool,
    is_identifier: bool,
    deprecated: bool,
    source_register_text: Option<String>,
    /// The FQIDs of the variables curated as the same variable.
    same_as: Vec<String>,
    /// The ref of the concept group the variable belongs to.
    group: Option<String>,
    tags: Vec<Tag>,
}

/// Every current classification, with the classification groups and families.
/// Editions a family stands for are reached through it.
#[derive(Serialize, ToSchema)]
pub struct ClassificationRoot {
    fqid: String,
    name: String,
    children: Vec<ClassificationChild>,
    groups: Vec<Group>,
    families: Vec<Family>,
}

#[derive(Serialize, ToSchema)]
pub struct ClassificationChild {
    fqid: String,
    short_name: Option<String>,
    name: Option<String>,
}

/// A classification edition. Its codes are `values`, its edition chain `graph`.
#[derive(Serialize, ToSchema)]
pub struct Classification {
    fqid: String,
    short_name: Option<String>,
    name: Option<String>,
    /// The classification groups it belongs to.
    dimensions: Vec<Group>,
    /// The succession family it belongs to.
    family: Option<Family>,
    derived_from: Vec<Derivation>,
    derivatives: Vec<Derivation>,
    /// The variables in scope with a state coded by it (today's `get classification
    /// --variables`), unpaged.
    variables: Vec<OwningVariable>,
}

/// A non-temporal derivation link between classifications.
#[derive(Serialize, ToSchema)]
pub struct Derivation {
    fqid: Option<String>,
    short_name: Option<String>,
    name: Option<String>,
    note: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct OwningVariable {
    fqid: Option<String>,
    name: Option<String>,
    register_name: Option<String>,
}

/// A register's concept group: its members in scope.
#[derive(Serialize, ToSchema)]
pub struct ConceptGroup {
    fqid: String,
    /// The register's FQID.
    register: String,
    key: String,
    label: String,
    source: String,
    axes: Vec<Axis>,
    members: Vec<Member>,
    tags: Vec<Tag>,
}

/// A curated group of classifications.
#[derive(Serialize, ToSchema)]
pub struct ClassificationGroup {
    fqid: String,
    key: String,
    label: String,
    source: String,
    axes: Vec<Axis>,
    members: Vec<Member>,
}

/// A group as listed by its register, classification root or member classification.
#[derive(Clone, Serialize, ToSchema)]
pub struct Group {
    fqid: String,
    key: String,
    label: String,
    source: String,
    axes: Vec<Axis>,
    members: Vec<Member>,
    tags: Vec<Tag>,
}

/// A one-dimensional classification succession family (ICD, LKF, SNI, SSYK).
#[derive(Clone, Serialize, ToSchema)]
pub struct Family {
    fqid: String,
    key: String,
    label: String,
    /// Its editions in chain order.
    editions: Vec<FamilyEdition>,
}

/// An edition of a succession family.
#[derive(Clone, Serialize, ToSchema)]
pub struct FamilyEdition {
    slug: String,
    fqid: String,
    name: Option<String>,
    short_name: Option<String>,
    /// The year of the edge by which the edition is superseded on the chain.
    effective_year: Option<i64>,
    /// The edition's own vintage year.
    version_year: Option<i64>,
    /// Started and without an active successor at the policy year.
    is_current: bool,
    /// The edition the family's chain was read from.
    is_self: bool,
}

#[derive(Clone, Serialize, ToSchema)]
pub struct Axis {
    name: String,
    label: String,
}

/// A group member; two members of one variable differ by `delivery_column`.
#[derive(Clone, Serialize, ToSchema)]
pub struct Member {
    fqid: String,
    name: Option<String>,
    facets: Vec<Facet>,
    delivery_column: Option<String>,
    /// Present on a group's own page only: the member's coverage, its column's
    /// for a representation member (empty when no state delivers the column).
    #[serde(skip_serializing_if = "Option::is_none")]
    coverage: Option<Coverage>,
}

#[derive(Clone, Serialize, ToSchema)]
pub struct Facet {
    axis: Option<String>,
    value: String,
    label: String,
}

/// A tag membership: the tag and this member's rank, star and note.
#[derive(Clone, Serialize, ToSchema)]
pub struct Tag {
    slug: String,
    label: String,
    rank: i64,
    starred: bool,
    note: Option<String>,
}

/// A register variant (the `variant` filter of `states`), with its versions' prose.
#[derive(Serialize, ToSchema)]
pub struct Variant {
    slug: String,
    name: Option<String>,
    description: Option<String>,
    display_group: Option<String>,
    /// The key of the variant's succession family: its first head.
    #[serde(rename = "variant_family")]
    family: Option<String>,
    #[serde(rename = "variant_family_label")]
    family_label: Option<String>,
    panel_entity_key: Option<PanelKey>,
    panel_time_key: Option<PanelKey>,
    panel_time_grain: Option<String>,
    versions: Vec<Version>,
}

/// A panel key: a variable slug (or `period`), or several (a composite key).
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub enum PanelKey {
    One(String),
    Composite(Vec<String>),
}

#[derive(Serialize, ToSchema)]
pub struct Version {
    name: Option<String>,
    description: Option<String>,
    measurement_information: Option<String>,
    populations: Vec<Population>,
    object_types: Vec<ObjectType>,
}

#[derive(Serialize, ToSchema)]
pub struct Population {
    name: String,
    definition: Option<String>,
    comment: Option<String>,
    date_range: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct ObjectType {
    name: String,
    definition: Option<String>,
}

pub fn show(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    let value = params.get("ref").copied();
    let not_found = || refs::not_found(value.unwrap_or_default());
    let show = match refs::resolve(&conn, scope, value)? {
        Target::Root => Show::Root(root(&conn, scope)?),
        Target::Provider { id, slug } => Show::Provider(provider(&conn, scope, id, &slug)?),
        Target::Register { id, provider, slug } => {
            Show::Register(register(&conn, scope, id, &provider, &slug)?)
        }
        Target::Variable { id } => Show::Variable(variable(&conn, scope, id)?),
        Target::ClassificationRoot => Show::ClassificationRoot(classification_root(&conn)?),
        Target::Classification { id, slug } => {
            Show::Classification(classification(&conn, scope, id, &slug)?)
        }
        Target::Group {
            register_id,
            provider,
            register,
            key,
        } => {
            let group = concept_groups(&conn, scope, &provider, &register, Some(register_id))?
                .into_iter()
                .find(|g| g.key == key)
                .ok_or_else(not_found)?;
            Show::ConceptGroup(ConceptGroup {
                fqid: group.fqid,
                register: format!("{provider}/{register}"),
                key: group.key,
                label: group.label,
                source: group.source,
                axes: group.axes,
                members: group.members,
                tags: group.tags,
            })
        }
        Target::ClassificationGroup { key } => {
            if let Some(group) = classification_groups(&conn)?
                .into_iter()
                .find(|g| g.key == key)
            {
                Show::ClassificationGroup(ClassificationGroup {
                    fqid: group.fqid,
                    key: group.key,
                    label: group.label,
                    source: group.source,
                    axes: group.axes,
                    members: group.members,
                })
            } else {
                let family = families(&conn)?
                    .into_iter()
                    .find(|f| f.key == key)
                    .ok_or_else(not_found)?;
                Show::ClassificationFamily(family)
            }
        }
    };
    Ok(serde_json::to_value(show).expect("Show serializes"))
}

fn rows<T>(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
    map: impl FnMut(&Row) -> rusqlite::Result<T>,
) -> Result<Vec<T>, Error> {
    let mut stmt = conn.prepare(sql)?;
    Ok(stmt
        .query_map(params, map)?
        .collect::<rusqlite::Result<_>>()?)
}

fn root(conn: &Connection, scope: Scope) -> Result<Root, Error> {
    let sql = format!(
        "SELECT slug, name FROM provider p WHERE {} ORDER BY slug",
        held::provider(scope, "p.provider_id")
    );
    let mut children = rows(conn, &sql, [], |row| {
        Ok(RootChild::Provider {
            fqid: row.get(0)?,
            name: row.get(1)?,
        })
    })?;
    children.push(RootChild::ClassificationRoot {
        fqid: "class".to_owned(),
        name: "Classifications".to_owned(),
    });
    Ok(Root { children })
}

fn provider(conn: &Connection, scope: Scope, id: i64, slug: &str) -> Result<Provider, Error> {
    let name = conn.query_row(
        "SELECT name FROM provider WHERE provider_id = ?",
        [id],
        |row| row.get(0),
    )?;
    let sql = format!(
        "SELECT r.register_id, r.slug, r.name, r.purpose FROM register r \
         WHERE r.provider_id = ? AND r.slug IS NOT NULL AND {} ORDER BY r.slug",
        held::register(scope, "r.register_id")
    );
    let registers = rows(conn, &sql, [id], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get(2)?,
            row.get(3)?,
        ))
    })?;
    let mut coverage = browse::register_coverage(conn, scope, id)?;
    let children = registers
        .into_iter()
        .map(|(register_id, register, name, purpose)| {
            Ok(RegisterChild {
                fqid: format!("{slug}/{register}"),
                name,
                purpose,
                tags: register_tags(conn, register_id)?,
                coverage: coverage.remove(&register_id),
            })
        })
        .collect::<Result<_, Error>>()?;
    Ok(Provider {
        fqid: slug.to_owned(),
        name,
        children,
    })
}

fn tag(row: &Row) -> rusqlite::Result<Tag> {
    Ok(Tag {
        slug: row.get(0)?,
        label: row.get(1)?,
        rank: row.get(2)?,
        starred: row.get(3)?,
        note: row.get(4)?,
    })
}

/// A register's own tags, by rank then slug.
fn register_tags(conn: &Connection, register_id: i64) -> Result<Vec<Tag>, Error> {
    rows(
        conn,
        "SELECT t.slug, t.label, tm.rank, tm.starred, tm.note FROM tag_member tm \
         JOIN tag t USING(tag_id) WHERE tm.register_id = ? ORDER BY tm.rank, t.slug",
        [register_id],
        tag,
    )
}

/// Today's `_aggregate_tag_memberships`: one tag per slug over member-grain rows
/// `(slug, label, rank, starred, note, member variable id)`, at its lowest rank,
/// starred when any member is, with the note of its strongest membership (a starred
/// note, then any note; then by rank, member and slug).
fn aggregate_tags(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Vec<Tag>, Error> {
    type NoteKey = (u8, i64, i64, String);
    let members = rows(conn, sql, params, |row| {
        Ok((tag(row)?, row.get::<_, i64>(5)?))
    })?;
    let has_note = |t: &Tag| t.note.as_deref().is_some_and(|n| !n.is_empty());
    let note_key = |t: &Tag, member: i64| -> NoteKey {
        let bucket = match (t.starred, has_note(t)) {
            (true, true) => 0,
            (_, true) => 1,
            _ => 2,
        };
        (bucket, t.rank, member, t.slug.clone())
    };
    let mut tags: BTreeMap<String, (Tag, NoteKey)> = BTreeMap::new();
    for (member_tag, member) in members {
        let key = note_key(&member_tag, member);
        match tags.get_mut(&member_tag.slug) {
            None => {
                tags.insert(member_tag.slug.clone(), (member_tag, key));
            }
            Some((current, current_key)) => {
                current.rank = current.rank.min(member_tag.rank);
                current.starred |= member_tag.starred;
                if has_note(&member_tag) && key < *current_key {
                    current.note = member_tag.note;
                    *current_key = key;
                }
            }
        }
    }
    let mut tags: Vec<Tag> = tags.into_values().map(|(t, _)| t).collect();
    tags.sort_by(|a, b| (a.rank, &a.slug).cmp(&(b.rank, &b.slug)));
    Ok(tags)
}

/// Today's `_group_member_predicate`: a group member `alias` is in scope when its
/// variable is, and a representation member when its column is held in some
/// variant of the register.
fn member_in_scope(scope: Scope, alias: &str) -> String {
    let variable = held::variable(scope, &format!("{alias}.variable_id"), Narrow::default());
    if scope == Scope::Reference {
        return variable;
    }
    let column = format!("{alias}.delivery_column_name");
    let representation = held::variable(
        scope,
        &format!("{alias}.variable_id"),
        Narrow {
            variant: Some("gv.register_variant_id"),
            column: Some(&column),
            ..Narrow::default()
        },
    );
    format!(
        "{variable} AND ({column} IS NULL OR EXISTS (SELECT 1 FROM register_variant gv \
         WHERE gv.register_id = (SELECT register_id FROM variable \
         WHERE variable_id = {alias}.variable_id) AND {representation}))"
    )
}

fn register(
    conn: &Connection,
    scope: Scope,
    id: i64,
    provider: &str,
    slug: &str,
) -> Result<Register, Error> {
    let (name, purpose) = conn.query_row(
        "SELECT name, purpose FROM register WHERE register_id = ?",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let sql = format!(
        "SELECT v.variable_id, v.slug, v.name FROM variable v WHERE v.register_id = ? \
         AND v.slug IS NOT NULL AND {} ORDER BY v.slug",
        held::variable(scope, "v.variable_id", Narrow::default())
    );
    let mut deliveries = browse::deliveries(conn, scope, id)?;
    let coverage = browse::variable_coverage(conn, scope, id, &deliveries)?;
    let children = rows(conn, &sql, [id], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get(2)?))
    })?
    .into_iter()
    .map(|(variable_id, variable, name)| VariableChild {
        fqid: format!("{provider}/{slug}/{variable}"),
        name,
        coverage: coverage.get(&variable_id).cloned(),
        deliveries: deliveries.remove(&variable_id).unwrap_or_default(),
    })
    .collect();
    Ok(Register {
        fqid: format!("{provider}/{slug}"),
        name,
        purpose,
        tags: register_tags(conn, id)?,
        children,
        groups: concept_groups(conn, scope, provider, slug, None)?,
        variants: variants(conn, scope, id, provider, slug)?,
    })
}

/// Today's `list_concept_groups`: a register's concept groups with their members in
/// scope, by key. Members order by their first facet value, FQID and column; a
/// group's tags aggregate its members' tags. With `coverage_of` (the register's id,
/// for a group's own page) each member carries its coverage, as today's group node.
fn concept_groups(
    conn: &Connection,
    scope: Scope,
    provider: &str,
    register: &str,
    coverage_of: Option<i64>,
) -> Result<Vec<Group>, Error> {
    /// A group being assembled: members by member id (with their variable id) and
    /// axes by ordinal.
    struct Acc {
        key: String,
        label: String,
        source: String,
        members: BTreeMap<i64, (i64, Member)>,
        axes: BTreeMap<i64, Axis>,
    }
    let sql = format!(
        "SELECT g.group_id, g.group_key, g.label, g.source, m.member_id, \
         m.delivery_column_name, m.variable_id, v.slug, v.name, f.axis, f.value, f.label, \
         a.ordinal, a.label \
         FROM concept_group g JOIN register r ON g.register_id = r.register_id \
         JOIN provider p ON r.provider_id = p.provider_id \
         JOIN concept_group_variable m ON m.group_id = g.group_id \
         JOIN variable v ON v.variable_id = m.variable_id \
         LEFT JOIN concept_group_variable_facet f ON f.member_id = m.member_id \
         LEFT JOIN concept_group_axis a ON a.group_id = g.group_id AND a.axis = f.axis \
         WHERE p.slug = ? AND r.slug = ? AND g.kind = 'variable' AND v.slug IS NOT NULL \
         AND {} ORDER BY g.group_key, m.member_id, a.ordinal",
        member_in_scope(scope, "m")
    );
    let mut groups: BTreeMap<String, Acc> = BTreeMap::new();
    let mut stmt = conn.prepare(&sql)?;
    let mut cursor = stmt.query([provider, register])?;
    while let Some(row) = cursor.next()? {
        let key: String = row.get(1)?;
        let group = match groups.get_mut(&key) {
            Some(group) => group,
            None => groups.entry(key.clone()).or_insert(Acc {
                key,
                label: row.get(2)?,
                source: row.get(3)?,
                members: BTreeMap::new(),
                axes: BTreeMap::new(),
            }),
        };
        let variable_slug: String = row.get(7)?;
        let member = &mut group
            .members
            .entry(row.get(4)?)
            .or_insert((
                row.get(6)?,
                Member {
                    fqid: format!("{provider}/{register}/{variable_slug}"),
                    name: row.get(8)?,
                    facets: Vec::new(),
                    delivery_column: row.get(5)?,
                    coverage: None,
                },
            ))
            .1;
        if let Some(axis) = row.get::<_, Option<String>>(9)? {
            member.facets.push(Facet {
                axis: Some(axis.clone()),
                value: row.get(10)?,
                label: row.get(11)?,
            });
            group.axes.insert(
                row.get(12)?,
                Axis {
                    name: axis,
                    label: row.get(13)?,
                },
            );
        }
    }
    drop(cursor);
    if let Some(register_id) = coverage_of {
        let coverage = browse::MemberCoverage::new(conn, scope, register_id)?;
        for (id, member) in groups.values_mut().flat_map(|g| g.members.values_mut()) {
            member.coverage = coverage.of(*id, member.delivery_column.as_deref());
        }
    }
    groups
        .into_values()
        .map(|group| {
            let ids: BTreeSet<i64> = group.members.values().map(|(id, _)| *id).collect();
            let mut members: Vec<Member> = group.members.into_values().map(|(_, m)| m).collect();
            members.sort_by_cached_key(|m| {
                let first = m.facets.first().map(|f| f.value.clone());
                let column = m.delivery_column.clone();
                (
                    first.unwrap_or_default(),
                    m.fqid.clone(),
                    column.unwrap_or_default(),
                )
            });
            Ok(Group {
                fqid: format!("group/{provider}/{register}/{}", group.key),
                key: group.key,
                label: group.label,
                source: group.source,
                axes: group.axes.into_values().collect(),
                members,
                tags: variable_tags(conn, &ids)?,
            })
        })
        .collect()
}

/// The aggregated tags of a set of variables (a group's members).
fn variable_tags(conn: &Connection, ids: &BTreeSet<i64>) -> Result<Vec<Tag>, Error> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let list = ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",");
    aggregate_tags(
        conn,
        &format!(
            "SELECT DISTINCT t.slug, t.label, tm.rank, tm.starred, tm.note, tm.variable_id \
             FROM tag_member tm JOIN tag t USING(tag_id) WHERE tm.variable_id IN ({list}) \
             ORDER BY tm.rank, t.slug, tm.variable_id"
        ),
        [],
    )
}

/// Today's `list_variants`: a register's variants in scope, by slug, with their
/// succession family and versions' prose.
fn variants(
    conn: &Connection,
    scope: Scope,
    register_id: i64,
    provider: &str,
    register: &str,
) -> Result<Vec<Variant>, Error> {
    let in_scope = held::variant(scope, "rv.register_variant_id");
    let sql = format!(
        "SELECT rv.register_variant_id, rv.slug, rv.name, rv.description, rv.display_group, \
         rv.panel_entity_key, rv.panel_time_key, rv.panel_time_grain FROM register_variant rv \
         WHERE rv.register_id = ? AND rv.slug IS NOT NULL AND {in_scope} ORDER BY rv.slug"
    );
    let families = variant_families(conn, register_id, provider, register)?;
    let found = rows(conn, &sql, [register_id], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get(7)?,
        ))
    })?;
    found
        .into_iter()
        .map(
            |(id, slug, name, description, display_group, entity, time, grain)| {
                let (family, family_label) = families
                    .get(&slug)
                    .cloned()
                    .map_or((None, None), |(key, label)| (Some(key), Some(label)));
                Ok(Variant {
                    versions: versions(conn, id)?,
                    slug,
                    name,
                    description,
                    display_group,
                    family,
                    family_label,
                    panel_entity_key: panel_key(entity),
                    panel_time_key: panel_key(time),
                    panel_time_grain: grain,
                })
            },
        )
        .collect()
}

/// A stored panel key: a JSON array (a composite key), any other string as is.
fn panel_key(raw: Option<String>) -> Option<PanelKey> {
    raw.map(|raw| match serde_json::from_str(&raw) {
        Ok(slugs) if raw.starts_with('[') => PanelKey::Composite(slugs),
        _ => PanelKey::One(raw),
    })
}

/// Today's `_variant_families_for_register_id`: the register's variant succession
/// components of two or more, each keyed by its first head (a variant without a
/// successor) and labelled by `family_label`.
pub(super) fn variant_families(
    conn: &Connection,
    register_id: i64,
    provider: &str,
    register: &str,
) -> Result<BTreeMap<String, (String, String)>, Error> {
    // A variant's label is its display group, else its name, else its slug (empty
    // text counts as absent, as in today's `or` chain).
    let labels: BTreeMap<String, String> = rows(
        conn,
        "SELECT slug, display_group, name FROM register_variant \
         WHERE register_id = ? AND slug IS NOT NULL",
        [register_id],
        |row| {
            let slug: String = row.get(0)?;
            let label = [row.get::<_, Option<String>>(1)?, row.get(2)?]
                .into_iter()
                .flatten()
                .find(|l| !l.is_empty())
                .unwrap_or_else(|| slug.clone());
            Ok((slug, label))
        },
    )?
    .into_iter()
    .collect();
    let edges = rows(
        conn,
        "SELECT predecessor_variant, successor_variant FROM variant_replaced_by \
         WHERE predecessor_provider = ?1 AND predecessor_register = ?2 \
         AND successor_provider = ?1 AND successor_register = ?2",
        [provider, register],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    let mut outgoing = BTreeSet::new();
    let mut neighbors: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (from, to) in &edges {
        if labels.contains_key(from) && labels.contains_key(to) {
            outgoing.insert(from.as_str());
            neighbors.entry(from).or_default().insert(to);
            neighbors.entry(to).or_default().insert(from);
        }
    }
    let mut out = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for &start in neighbors.keys() {
        if seen.contains(start) {
            continue;
        }
        let mut component = BTreeSet::new();
        let mut stack = vec![start];
        while let Some(node) = stack.pop() {
            if component.insert(node) {
                stack.extend(neighbors.get(node).into_iter().flatten().copied());
            }
        }
        seen.extend(component.iter().copied());
        if component.len() < 2 {
            continue;
        }
        let key = component
            .iter()
            .find(|v| !outgoing.contains(*v))
            .or_else(|| component.first())
            .map(|v| (*v).to_owned())
            .unwrap_or_default();
        let label = family_label(component.iter().map(|v| labels[*v].as_str()));
        for variant in component {
            out.insert(variant.to_owned(), (key.clone(), label.clone()));
        }
    }
    Ok(out)
}

/// Today's `_variant_family_label`: the shared label, else the shared part before a
/// comma, else the first label.
fn family_label<'a>(labels: impl Iterator<Item = &'a str>) -> String {
    let labels: Vec<&str> = labels.filter(|l| !l.is_empty()).collect();
    let Some(first) = labels.first() else {
        return String::new();
    };
    if labels.iter().all(|l| l == first) {
        return (*first).to_owned();
    }
    let stem = |l: &str| l.split(',').next().unwrap_or_default().trim().to_owned();
    let first_stem = stem(first);
    if !first_stem.is_empty() && labels.iter().all(|l| stem(l) == first_stem) {
        return first_stem;
    }
    (*first).to_owned()
}

/// A variant's register versions with their populations and object types, the
/// entries without text left out.
fn versions(conn: &Connection, variant_id: i64) -> Result<Vec<Version>, Error> {
    let found = rows(
        conn,
        "SELECT regver_id, registerversionnamn, registerversionbeskrivning, \
         registerversionmatinformation FROM register_version WHERE register_variant_id = ? \
         ORDER BY registerversionnamn, regver_id",
        [variant_id],
        |row| Ok((row.get::<_, i64>(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    let text = |values: &[&Option<String>]| {
        values
            .iter()
            .any(|v| v.as_deref().is_some_and(|v| !v.is_empty()))
    };
    found
        .into_iter()
        .map(|(id, name, description, measurement_information)| {
            let populations = rows(
                conn,
                "SELECT name, definition, comment, date_range FROM population \
                 WHERE regver_id = ? ORDER BY name",
                [id],
                |row| {
                    Ok(Population {
                        name: row.get(0)?,
                        definition: row.get(1)?,
                        comment: row.get(2)?,
                        date_range: row.get(3)?,
                    })
                },
            )?
            .into_iter()
            .filter(|p| {
                text(&[
                    &Some(p.name.clone()),
                    &p.definition,
                    &p.comment,
                    &p.date_range,
                ])
            })
            .collect();
            let object_types = rows(
                conn,
                "SELECT name, definition FROM object_type WHERE regver_id = ? ORDER BY name",
                [id],
                |row| {
                    Ok(ObjectType {
                        name: row.get(0)?,
                        definition: row.get(1)?,
                    })
                },
            )?
            .into_iter()
            .filter(|o| text(&[&Some(o.name.clone()), &o.definition]))
            .collect();
            Ok(Version {
                name,
                description,
                measurement_information,
                populations,
                object_types,
            })
        })
        .collect()
}

fn variable(conn: &Connection, scope: Scope, id: i64) -> Result<Variable, Error> {
    let ([p, r, v], mut variable) = conn.query_row(
        "SELECT p.slug, r.slug, v.slug, v.name, v.definition, v.description, \
         v.operational_definition, v.measurement_unit, v.is_sensitive, v.is_identifier, \
         v.deprecated, v.source_register_text FROM variable v \
         JOIN register r USING(register_id) JOIN provider p USING(provider_id) \
         WHERE v.variable_id = ?",
        [id],
        |row| {
            let slugs: [String; 3] = [row.get(0)?, row.get(1)?, row.get(2)?];
            Ok((
                slugs.clone(),
                Variable {
                    fqid: slugs.join("/"),
                    name: row.get(3)?,
                    definition: row.get(4)?,
                    description: row.get(5)?,
                    operational_definition: row.get(6)?,
                    measurement_unit: row.get(7)?,
                    is_sensitive: row.get(8)?,
                    is_identifier: row.get(9)?,
                    deprecated: row.get(10)?,
                    source_register_text: row.get(11)?,
                    same_as: Vec::new(),
                    group: None,
                    tags: Vec::new(),
                },
            ))
        },
    )?;
    variable.same_as = rows(
        conn,
        "SELECT b_provider, b_register, b_variable FROM variable_same_as \
         WHERE a_provider = ? AND a_register = ? AND a_variable = ? \
         ORDER BY b_provider, b_register, b_variable",
        [&p, &r, &v],
        |row| Ok(fqid(&[row.get(0)?, row.get(1)?, row.get(2)?])),
    )?
    .into_iter()
    .flatten()
    .collect();
    variable.group = conn
        .query_row(
            "SELECT DISTINCT g.group_key FROM concept_group_variable m \
             JOIN concept_group g ON g.group_id = m.group_id \
             WHERE m.variable_id = ? AND g.kind = 'variable' ORDER BY g.group_key",
            [id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|key| format!("group/{p}/{r}/{key}"));
    // Today's `tags_for_variable`: the variable's own tags, then the tags of its
    // group's members in scope as neutral memberships.
    let own = rows(
        conn,
        "SELECT t.slug, t.label, tm.rank, tm.starred, tm.note FROM tag_member tm \
         JOIN tag t USING(tag_id) WHERE tm.variable_id = ? ORDER BY tm.rank, t.slug",
        [id],
        tag,
    )?;
    let inherited = aggregate_tags(
        conn,
        &format!(
            "SELECT DISTINCT t.slug, t.label, tm.rank, tm.starred, tm.note, tm.variable_id \
             FROM concept_group_variable target_member \
             JOIN concept_group_variable group_member \
             ON group_member.group_id = target_member.group_id \
             JOIN tag_member tm ON tm.variable_id = group_member.variable_id \
             JOIN tag t ON t.tag_id = tm.tag_id \
             WHERE target_member.variable_id = ? AND {} AND {} \
             ORDER BY tm.rank, t.slug, tm.variable_id",
            member_in_scope(scope, "group_member"),
            member_in_scope(scope, "target_member"),
        ),
        [id],
    )?;
    let own_slugs: BTreeSet<String> = own.iter().map(|t| t.slug.clone()).collect();
    let mut tags = own;
    tags.extend(
        inherited
            .into_iter()
            .filter(|t| !own_slugs.contains(&t.slug))
            .map(|t| Tag {
                starred: false,
                note: None,
                ..t
            }),
    );
    tags.sort_by(|a, b| (a.rank, &a.slug).cmp(&(b.rank, &b.slug)));
    variable.tags = tags;
    Ok(variable)
}

/// The one-dimensional succession families (today's `list_classification_families`),
/// by key, from the compiled `classification_family`.
pub(super) fn families(conn: &Connection) -> Result<Vec<Family>, Error> {
    let found = rows(
        conn,
        "SELECT f.family_key, f.family_label, f.slug, c.name, c.short_name, \
         f.effective_year, c.valid_from, f.is_current, f.is_self \
         FROM classification_family f LEFT JOIN classification c ON c.slug = f.slug \
         ORDER BY f.family_key, f.position",
        [],
        |row| {
            let slug: String = row.get(2)?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                FamilyEdition {
                    fqid: format!("class/{slug}"),
                    slug,
                    name: row.get(3)?,
                    short_name: row.get(4)?,
                    effective_year: row.get(5)?,
                    version_year: row.get(6)?,
                    is_current: row.get(7)?,
                    is_self: row.get(8)?,
                },
            ))
        },
    )?;
    let mut families: Vec<Family> = Vec::new();
    for (key, label, edition) in found {
        if families.last().is_none_or(|f| f.key != key) {
            families.push(Family {
                fqid: format!("group/class/{key}"),
                key,
                label,
                editions: Vec::new(),
            });
        }
        families
            .last_mut()
            .expect("a family")
            .editions
            .push(edition);
    }
    Ok(families)
}

/// Today's `list_classification_groups`: the curated classification groups, by key,
/// members by facet value then slug.
fn classification_groups(conn: &Connection) -> Result<Vec<Group>, Error> {
    let found = rows(
        conn,
        "SELECT g.group_key, g.label, g.source, a.axis, a.label, c.slug, c.name, \
         m.facet_value, m.facet_label FROM concept_group g \
         LEFT JOIN concept_group_axis a ON a.group_id = g.group_id \
         JOIN concept_group_classification m ON m.group_id = g.group_id \
         JOIN classification c ON c.id = m.classification_id \
         WHERE g.kind = 'classification' AND c.slug IS NOT NULL \
         ORDER BY g.group_key, m.facet_value, c.slug",
        [],
        |row| {
            Ok((
                [row.get::<_, String>(0)?, row.get(1)?, row.get(2)?],
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                Member {
                    fqid: format!("class/{}", row.get::<_, String>(5)?),
                    name: row.get(6)?,
                    facets: vec![Facet {
                        axis: row.get(3)?,
                        value: row.get(7)?,
                        label: row.get(8)?,
                    }],
                    delivery_column: None,
                    coverage: None,
                },
            ))
        },
    )?;
    let mut groups: Vec<Group> = Vec::new();
    for ([key, label, source], axis, axis_label, member) in found {
        if groups.last().is_none_or(|g| g.key != key) {
            groups.push(Group {
                fqid: format!("group/class/{key}"),
                key,
                label,
                source,
                axes: axis
                    .map(|name| Axis {
                        name,
                        label: axis_label.unwrap_or_default(),
                    })
                    .into_iter()
                    .collect(),
                members: Vec::new(),
                tags: Vec::new(),
            });
        }
        groups.last_mut().expect("a group").members.push(member);
    }
    Ok(groups)
}

/// Today's `classification_root`: the classifications nothing supersedes and no
/// family stands for, by short name, with the groups and families.
fn classification_root(conn: &Connection) -> Result<ClassificationRoot, Error> {
    let families = families(conn)?;
    let editions: BTreeSet<&str> = families
        .iter()
        .flat_map(|f| f.editions.iter().map(|e| e.slug.as_str()))
        .collect();
    let children = rows(
        conn,
        "SELECT c.slug, c.short_name, c.name FROM classification c WHERE c.slug IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM classification s WHERE s.supersedes_id = c.id) \
         ORDER BY c.short_name",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get(1)?, row.get(2)?)),
    )?
    .into_iter()
    .filter(|(slug, ..)| !editions.contains(slug.as_str()))
    .map(|(slug, short_name, name)| ClassificationChild {
        fqid: format!("class/{slug}"),
        short_name,
        name,
    })
    .collect();
    Ok(ClassificationRoot {
        fqid: "class".to_owned(),
        name: "Classifications".to_owned(),
        children,
        groups: classification_groups(conn)?,
        families,
    })
}

fn classification(
    conn: &Connection,
    scope: Scope,
    id: i64,
    slug: &str,
) -> Result<Classification, Error> {
    let (short_name, name) = conn.query_row(
        "SELECT short_name, name FROM classification WHERE id = ?",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let fqid = format!("class/{slug}");
    let dimensions = classification_groups(conn)?
        .into_iter()
        .filter(|g| g.members.iter().any(|m| m.fqid == fqid))
        .collect();
    let family = families(conn)?
        .into_iter()
        .find(|f| f.editions.iter().any(|e| e.slug == slug));
    let derivation = |sql: &str| {
        rows(conn, sql, [slug], |row| {
            Ok(Derivation {
                fqid: refs::fqid(&[Some("class".to_owned()), row.get(0)?]),
                short_name: row.get(1)?,
                name: row.get(2)?,
                note: row.get(3)?,
            })
        })
    };
    let derived_from = derivation(
        "SELECT e.source_slug, c.short_name, c.name, e.note FROM classification_derived_from e \
         JOIN classification c ON c.slug = e.source_slug WHERE e.derived_slug = ? \
         ORDER BY e.source_slug",
    )?;
    let derivatives = derivation(
        "SELECT e.derived_slug, c.short_name, c.name, e.note FROM classification_derived_from e \
         JOIN classification c ON c.slug = e.derived_slug WHERE e.source_slug = ? \
         ORDER BY e.derived_slug",
    )?;
    // Today's `search_variables_by_classification`, unpaged, with a slug tiebreak
    // after its name order: the (variable, variant) pairs with a state coded by the
    // classification or overlapping a window coded by it, driven from the
    // classification's links rather than from every state.
    let sql = format!(
        "WITH owned(variable_id, register_variant_id) AS ( \
         SELECT vs.variable_id, vs.register_variant_id FROM state_classification sc \
         JOIN variable_state vs ON vs.state_id = sc.state_id WHERE sc.classification_id = ?1 \
         UNION SELECT vs.variable_id, vs.register_variant_id \
         FROM alias_window_classification ac JOIN variable_alias_window aw \
         ON aw.variable_id = ac.variable_id AND aw.register_variant_id = ac.register_variant_id \
         AND aw.delivery_column_name = ac.delivery_column_name AND aw.valid_from = ac.valid_from \
         JOIN variable_state vs ON vs.variable_id = ac.variable_id \
         AND vs.register_variant_id = ac.register_variant_id \
         AND aw.valid_from <= vs.valid_to AND aw.valid_to >= vs.valid_from \
         WHERE ac.classification_id = ?1) \
         SELECT DISTINCT p.slug, r.slug, v.slug, v.name, r.name FROM owned o \
         JOIN variable v ON v.variable_id = o.variable_id \
         JOIN register r ON v.register_id = r.register_id \
         JOIN provider p ON r.provider_id = p.provider_id WHERE {} \
         ORDER BY r.name, v.name, p.slug, r.slug, v.slug",
        held::variable(
            scope,
            "v.variable_id",
            Narrow {
                variant: Some("o.register_variant_id"),
                ..Narrow::default()
            }
        )
    );
    let variables = rows(conn, &sql, [id], |row| {
        Ok(OwningVariable {
            fqid: refs::fqid(&[row.get(0)?, row.get(1)?, row.get(2)?]),
            name: row.get(3)?,
            register_name: row.get(4)?,
        })
    })?;
    Ok(Classification {
        fqid,
        short_name,
        name,
        dimensions,
        family,
        derived_from,
        derivatives,
        variables,
    })
}
