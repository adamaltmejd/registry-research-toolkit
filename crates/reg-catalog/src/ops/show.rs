//! `show`: the summary of any ref; no ref is the catalog root.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs::{self, Entity};
use super::segments::{self, Segment};
use super::shape::{self, Release};
use super::show_evidence::{self, VariableEvidence};
use super::{Cache, Operation, Param, Params, Period, Server, Type, component, flag};
use crate::{Code, Error, rows};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "show",
    paths: &["/api/catalog", "/api/catalog/{ref}"],
    tool: Some("show"),
    description: "Describe a provider, register, table, variable or set by id or name (a set \
        only by `set/<provider>/<register>/<slug>`; it carries each member's whole \
        `segments` list, so no `segments` call per member is needed). No `ref` lists the \
        providers. Variables show curated metadata, with the provider's text only as a \
        fallback. `evidence=true` adds the provider's original texts, column names, curation \
        locators and linked documents.",
    params: &[
        Param::optional("ref", Type::Ref),
        Param::optional("evidence", Type::Boolean),
    ],
    cache: Cache::Minute,
    run: show,
    result: component::<Show>,
}];

/// A summary tagged by `kind`. Every kind but `root` carries the resolved `fqid`, so a bare-name request gets the id back.
#[derive(Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Show {
    /// On a data holder's catalog, the holder's own providers too.
    Root { providers: Vec<ShowProviderRef> },
    Provider {
        fqid: String,
        name: String,
        /// On a data holder's catalog, the holder's own registers too.
        registers: Vec<ShowRegisterRef>,
    },
    Register {
        fqid: String,
        name: String,
        #[schema(required = true)]
        description: Option<String>,
        period: Period,
        tables: Vec<ShowTable>,
        variables: Vec<ShowVariableRef>,
        /// The register's curated sets (groups), in id order.
        sets: Vec<ShowRegisterSet>,
        /// The union of its variables' tags, in public-id order.
        tags: Vec<ShowMembership>,
        /// Names of deliveries with no known period.
        unplaced: Vec<String>,
    },
    Table {
        fqid: String,
        name: String,
        #[schema(required = true)]
        description: Option<String>,
        /// The register's fqid.
        register: String,
        role: String,
        implicit: bool,
        timeless: bool,
        #[schema(required = true)]
        unit_type: Option<String>,
        #[schema(required = true)]
        cadence: Option<String>,
        spans: Vec<Period>,
        /// Absent when none, and on a data holder's catalog.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        releases: Vec<Release>,
        row_keys: Vec<ShowRowKey>,
    },
    Set {
        fqid: String,
        name: String,
        /// The curated description; without one, the description every member states
        /// alike.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(nullable = false)]
        description: Option<String>,
        /// The register's fqid.
        register: String,
        axes: Vec<ShowSetAxis>,
        members: Vec<ShowSetMember>,
        /// The union of the members' tags, in public-id order.
        tags: Vec<ShowMembership>,
    },
    Variable {
        fqid: String,
        name: String,
        /// The register's fqid.
        register: String,
        /// The whole span; `coverage` has the gaps.
        period: Period,
        /// The curated description.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(nullable = false)]
        description: Option<String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        sets: Vec<ShowMembership>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tags: Vec<ShowMembership>,
        /// Only with `evidence=true`.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(nullable = false)]
        evidence: Option<VariableEvidence>,
    },
}

#[derive(Serialize, ToSchema)]
pub struct ShowProviderRef {
    fqid: String,
    name: String,
}

#[derive(Serialize, ToSchema)]
pub struct ShowRegisterRef {
    fqid: String,
    name: String,
    period: Period,
}

#[derive(Serialize, ToSchema)]
pub struct ShowVariableRef {
    fqid: String,
    name: String,
    period: Period,
    /// The tables the variable appears in.
    tables: Vec<String>,
}

/// A register's table; the implicit one has the register's fqid.
#[derive(Serialize, ToSchema)]
pub struct ShowTable {
    fqid: String,
    name: String,
    role: String,
    implicit: bool,
    timeless: bool,
    #[schema(required = true)]
    unit_type: Option<String>,
    #[schema(required = true)]
    cadence: Option<String>,
    period: Period,
}

/// The row-key column's period in this table.
#[derive(Serialize, ToSchema)]
pub struct ShowRowKey {
    fqid: String,
    spelling: String,
    period: Period,
}

/// A curated set (group) or tag the variable belongs to.
#[derive(Serialize, ToSchema)]
pub struct ShowMembership {
    fqid: String,
    name: String,
}

/// A register's set; `show` at its fqid has the axes and member facets.
#[derive(Serialize, ToSchema)]
pub struct ShowRegisterSet {
    fqid: String,
    name: String,
    /// The members' variable fqids, in public-id order.
    members: Vec<String>,
}

/// A set's distinguishing axis, in curated order.
#[derive(Serialize, ToSchema)]
pub struct ShowSetAxis {
    id: String,
    label: String,
}

#[derive(Serialize, ToSchema)]
pub struct ShowSetMember {
    fqid: String,
    name: String,
    /// The curated description, only where it differs from the set's.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    description: Option<String>,
    /// The member's whole per-period list, as `segments` serves it without evidence.
    segments: Vec<Segment>,
    /// The member's coordinates in the set's axis order.
    facets: Vec<ShowSetFacet>,
}

#[derive(Serialize, ToSchema)]
pub struct ShowSetFacet {
    axis: String,
    value: String,
    label: String,
}

fn show(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = match params.get("ref") {
        Some(value) => server.catalog.connect_for_ref(value)?,
        None => server.catalog.connect()?,
    };
    let evidence = flag(params, "evidence")?;
    let shown = match params.get("ref") {
        None => {
            let mut providers = provider_refs(&conn)?;
            if let Some(extra) = server.catalog.connect_holdings()? {
                let known: BTreeSet<_> = providers.iter().map(|p| p.fqid.clone()).collect();
                providers.extend(
                    provider_refs(&extra)?
                        .into_iter()
                        .filter(|p| !known.contains(&p.fqid)),
                );
                providers.sort_by(|a, b| a.fqid.cmp(&b.fqid));
            }
            providers.retain(|p| server.catalog.serves("provider", &p.fqid));
            Show::Root { providers }
        }
        Some(value) => {
            let entity = refs::resolve(&conn, value)?;
            match entity.kind.as_str() {
                "provider" => provider(server, &conn, &entity)?,
                "register" => register(server, &conn, &entity)?,
                "table" => table(server, &conn, &entity)?,
                "variable" => variable(server, &conn, &entity, evidence)?,
                "set" => set(server, &conn, &entity)?,
                // simplify: classification summaries land with their data.
                kind => {
                    return Err(Error::new(
                        Code::InternalError,
                        format!("show has no summary for a {kind} yet."),
                        vec![],
                    ));
                }
            }
        }
    };
    Ok(serde_json::to_value(shown).expect("Show serializes"))
}

fn provider_refs(conn: &Connection) -> Result<Vec<ShowProviderRef>, Error> {
    Ok(rows(
        conn,
        "SELECT p.pid, n.body FROM provider p JOIN text n ON n.id = p.name_text
             ORDER BY p.pid",
        [],
        |row| {
            Ok(ShowProviderRef {
                fqid: row.get(0)?,
                name: row.get(1)?,
            })
        },
    )?)
}

fn register_refs(
    server: &Server,
    conn: &Connection,
    provider: &str,
) -> Result<Vec<ShowRegisterRef>, Error> {
    Ok(rows(
        conn,
        "SELECT r.pid, n.body, r.start, r.end FROM register r
             JOIN provider p ON p.id = r.provider_id JOIN text n ON n.id = r.name_text
             WHERE p.pid = ? ORDER BY r.pid",
        [provider],
        |row| {
            let fqid: String = row.get(0)?;
            Ok(ShowRegisterRef {
                period: server.hull("register", &fqid, row.get(2)?, row.get(3)?),
                fqid,
                name: row.get(1)?,
            })
        },
    )?)
}

fn provider(server: &Server, conn: &Connection, entity: &Entity) -> Result<Show, Error> {
    let name = conn.query_row(
        "SELECT n.body FROM provider p JOIN text n ON n.id = p.name_text WHERE p.id = ?",
        [entity.id],
        |row| row.get(0),
    )?;
    let mut registers = register_refs(server, conn, &entity.pid)?;
    if let Some(extra) = server.catalog.connect_holdings()? {
        // A register in both origins is a steward overlay of the reference one: the
        // reference row carries its official name (the overlay's is its slug).
        let known: BTreeSet<_> = registers.iter().map(|r| r.fqid.clone()).collect();
        registers.extend(
            register_refs(server, &extra, &entity.pid)?
                .into_iter()
                .filter(|r| !known.contains(&r.fqid)),
        );
        registers.sort_by(|a, b| a.fqid.cmp(&b.fqid));
    }
    registers.retain(|r| server.catalog.serves("register", &r.fqid));
    Ok(Show::Provider {
        fqid: entity.pid.clone(),
        name,
        registers,
    })
}

fn register(server: &Server, conn: &Connection, entity: &Entity) -> Result<Show, Error> {
    let (name, description, start, end): (String, Option<String>, i64, i64) = conn.query_row(
        "SELECT n.body, d.body, r.start, r.end
         FROM register r JOIN text n ON n.id = r.name_text
           LEFT JOIN text d ON d.id = r.description_text
         WHERE r.id = ?",
        [entity.id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    let (mut tables, mut variables) = contents(server, conn, &entity.pid)?;
    let (mut sets, mut tags) = groupings(conn, &entity.pid)?;
    // Steward mode: tables and variables the steward defines inside a reference
    // register join it; a pid both origins define keeps the reference row.
    if let Some(extra) = server.catalog.connect_holdings()? {
        let (steward_tables, steward_variables) = contents(server, &extra, &entity.pid)?;
        let known: BTreeSet<_> = tables.iter().map(|t| t.fqid.clone()).collect();
        tables.extend(
            steward_tables
                .into_iter()
                .filter(|t| !known.contains(&t.fqid)),
        );
        tables.sort_by(|a, b| a.fqid.cmp(&b.fqid));
        // A shared variable keeps the reference row but gains the tables the steward
        // attests it in (a membership the reference member list omits).
        let mut steward_variables: BTreeMap<_, _> = steward_variables
            .into_iter()
            .map(|v| (v.fqid.clone(), v))
            .collect();
        for variable in &mut variables {
            if let Some(steward) = steward_variables.remove(&variable.fqid) {
                variable.tables.extend(steward.tables);
                variable.tables.sort();
                variable.tables.dedup();
            }
        }
        variables.extend(steward_variables.into_values());
        variables.sort_by(|a, b| a.fqid.cmp(&b.fqid));
        let (steward_sets, steward_tags) = groupings(&extra, &entity.pid)?;
        let known: BTreeSet<_> = sets.iter().map(|s| s.fqid.clone()).collect();
        sets.extend(
            steward_sets
                .into_iter()
                .filter(|s| !known.contains(&s.fqid)),
        );
        sets.sort_by(|a, b| a.fqid.cmp(&b.fqid));
        let known: BTreeSet<_> = tags.iter().map(|t| t.fqid.clone()).collect();
        tags.extend(
            steward_tags
                .into_iter()
                .filter(|t| !known.contains(&t.fqid)),
        );
        tags.sort_by(|a, b| a.fqid.cmp(&b.fqid));
    }
    // Steward mode lists only what the steward holds (`Catalog::serves`); a set
    // keeps its held members and goes when none is held.
    tables.retain(|t| server.catalog.serves("table", &t.fqid));
    variables.retain(|v| server.catalog.serves("variable", &v.fqid));
    for variable in &mut variables {
        let fqid = &variable.fqid;
        variable
            .tables
            .retain(|table| server.catalog.serves_in(fqid, table));
    }
    for set in &mut sets {
        set.members
            .retain(|member| server.catalog.serves("variable", member));
    }
    sets.retain(|set| !set.members.is_empty());
    // Unplaced names are the provider's releases, not the steward's files
    // (maintainer 2026-11-04), so a steward deployment serves none.
    let unplaced = if server.catalog.steward().is_some() {
        vec![]
    } else {
        rows(
            conn,
            "SELECT DISTINCT n.body
                 FROM release r JOIN tbl t ON t.id = r.table_id
                   JOIN text n ON n.id = r.name_text
                 WHERE t.register_id = ? AND r.parse_state = 'unparsed' ORDER BY n.body",
            [entity.id],
            |row| row.get(0),
        )?
    };
    Ok(Show::Register {
        fqid: entity.pid.clone(),
        name,
        description,
        period: server.hull("register", &entity.pid, start, end),
        tables,
        variables,
        sets,
        tags,
        unplaced,
    })
}

/// A register's sets and its variables' tags in one origin, by the register's public
/// id. Register tags are derived, never curated: a register carries a tag iff one of
/// its variables does.
fn groupings(
    conn: &Connection,
    register: &str,
) -> Result<(Vec<ShowRegisterSet>, Vec<ShowMembership>), Error> {
    let mut sets: Vec<ShowRegisterSet> = vec![];
    let mut rows = conn.prepare(
        "SELECT s.pid, n.body, v.pid FROM var_set s
         JOIN register r ON r.id = s.register_id JOIN text n ON n.id = s.name_text
         JOIN var_set_member m ON m.set_id = s.id JOIN variable v ON v.id = m.variable_id
         WHERE s.kind = 'set' AND r.pid = ? ORDER BY s.pid, v.pid",
    )?;
    for row in rows.query_map([register], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })? {
        let (fqid, name, member) = row?;
        match sets.last_mut() {
            Some(set) if set.fqid == fqid => set.members.push(member),
            _ => sets.push(ShowRegisterSet {
                fqid,
                name,
                members: vec![member],
            }),
        }
    }
    let tags = crate::rows(
        conn,
        "SELECT DISTINCT t.pid, n.body FROM variable v
             JOIN register r ON r.id = v.register_id
             JOIN var_set_member m ON m.variable_id = v.id
             JOIN var_set t ON t.id = m.set_id AND t.kind = 'tag'
             JOIN text n ON n.id = t.name_text WHERE r.pid = ? ORDER BY t.pid",
        [register],
        |row| {
            Ok(ShowMembership {
                fqid: row.get(0)?,
                name: row.get(1)?,
            })
        },
    )?;
    Ok((sets, tags))
}

/// A register's tables and variables in one origin, by the register's public id.
fn contents(
    server: &Server,
    conn: &Connection,
    register: &str,
) -> Result<(Vec<ShowTable>, Vec<ShowVariableRef>), Error> {
    let tables = rows(
        conn,
        "SELECT t.pid, n.body, t.role, t.implicit, t.timeless, u.slug, t.cadence,
                    t.start, t.end
             FROM tbl t JOIN register r ON r.id = t.register_id
               JOIN text n ON n.id = t.name_text
               LEFT JOIN unit_type u ON u.id = t.unit_type_id
             WHERE r.pid = ? ORDER BY t.pid",
        [register],
        |row| {
            let fqid: String = row.get(0)?;
            Ok(ShowTable {
                period: server.hull("table", &fqid, row.get(7)?, row.get(8)?),
                fqid,
                name: row.get(1)?,
                role: row.get(2)?,
                implicit: row.get(3)?,
                timeless: row.get(4)?,
                unit_type: row.get(5)?,
                cadence: row.get(6)?,
            })
        },
    )?;
    let mut members: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in conn
        .prepare(
            "SELECT DISTINCT v.pid, t.pid FROM segment s
             JOIN variable v ON v.id = s.variable_id JOIN tbl t ON t.id = s.table_id
             JOIN register r ON r.id = v.register_id
             WHERE r.pid = ? ORDER BY v.pid, t.pid",
        )?
        .query_map([register], |row| Ok((row.get(0)?, row.get(1)?)))?
    {
        let (variable, table) = row?;
        members.entry(variable).or_default().push(table);
    }
    let variables = rows(
        conn,
        "SELECT v.pid, l.body, v.start, v.end
             FROM variable v JOIN register r ON r.id = v.register_id
               JOIN text l ON l.id = v.label_text
             WHERE r.pid = ? ORDER BY v.pid",
        [register],
        |row| {
            let fqid: String = row.get(0)?;
            Ok(ShowVariableRef {
                tables: members.remove(&fqid).unwrap_or_default(),
                period: server.hull("variable", &fqid, row.get(2)?, row.get(3)?),
                fqid,
                name: row.get(1)?,
            })
        },
    )?;
    Ok((tables, variables))
}

fn table(server: &Server, conn: &Connection, entity: &Entity) -> Result<Show, Error> {
    type Head = (
        String,
        Option<String>,
        String,
        String,
        bool,
        bool,
        Option<String>,
        Option<String>,
    );
    let (name, description, register, role, implicit, timeless, unit_type, cadence): Head = conn
        .query_row(
            "SELECT n.body, d.body, r.pid, t.role, t.implicit, t.timeless, u.slug, t.cadence
             FROM tbl t JOIN text n ON n.id = t.name_text
               LEFT JOIN text d ON d.id = t.description_text
               JOIN register r ON r.id = t.register_id
               LEFT JOIN unit_type u ON u.id = t.unit_type_id
             WHERE t.id = ?",
            [entity.id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )?;
    let spans = match server.catalog.held_spans("table", &entity.pid) {
        Some(held) => held.iter().map(|&(s, e)| server.period(s, e)).collect(),
        None => rows(
            conn,
            "SELECT start, end FROM tbl_span WHERE table_id = ? ORDER BY start",
            [entity.id],
            |row| Ok(server.period(row.get(0)?, row.get(1)?)),
        )?,
    };
    // By table pid: in steward mode the holdings origin adds the key memberships the
    // steward attests on a reference table.
    let keys = |conn: &Connection| {
        rows(
            conn,
            "SELECT v.pid, sp.exact, s.start, s.end
                 FROM segment s JOIN variable v ON v.id = s.variable_id
                   JOIN spelling sp ON sp.id = s.spelling_id
                   JOIN tbl t ON t.id = s.table_id
                 WHERE t.pid = ? AND s.role = 'row_key'",
            [&entity.pid],
            |row| {
                Ok((
                    row.get(2)?,
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get(3)?,
                ))
            },
        )
    };
    let mut found: Vec<(i64, String, String, i64)> = keys(conn)?;
    if let Some(extra) = server.catalog.connect_holdings()? {
        found.extend(keys(&extra)?);
    }
    found.sort();
    found.dedup();
    // Steward mode: a key column whose variable is not held in this table is not shown.
    let row_keys: Vec<ShowRowKey> = found
        .into_iter()
        .filter(|(_, fqid, _, _)| server.catalog.serves_in(fqid, &entity.pid))
        .map(|(start, fqid, spelling, end)| ShowRowKey {
            fqid,
            spelling,
            period: server.period(start, end),
        })
        .collect();
    Ok(Show::Table {
        fqid: entity.pid.clone(),
        name,
        description,
        register,
        role,
        implicit,
        timeless,
        unit_type,
        cadence,
        spans,
        // The provider's releases are not the steward's files (maintainer 2026-11-04).
        releases: if server.catalog.steward().is_some() {
            vec![]
        } else {
            shape::releases(server, conn, entity.id)?
        },
        row_keys,
    })
}

fn variable(
    server: &Server,
    conn: &Connection,
    entity: &Entity,
    include_evidence: bool,
) -> Result<Show, Error> {
    type Head = (String, Option<String>, String, i64, i64);
    let (name, description, register, start, end): Head = conn.query_row(
        "SELECT l.body, c.body, r.pid, v.start, v.end
         FROM variable v JOIN text l ON l.id = v.label_text
           LEFT JOIN text c ON c.id = v.description_text
           JOIN register r ON r.id = v.register_id
         WHERE v.id = ?",
        [entity.id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    Ok(Show::Variable {
        fqid: entity.pid.clone(),
        name,
        register,
        period: server.hull("variable", &entity.pid, start, end),
        description,
        sets: memberships(conn, entity.id, "set")?,
        tags: memberships(conn, entity.id, "tag")?,
        evidence: include_evidence
            .then(|| show_evidence::load(server, conn, entity))
            .transpose()?,
    })
}

fn memberships(conn: &Connection, variable: i64, kind: &str) -> Result<Vec<ShowMembership>, Error> {
    Ok(rows(
        conn,
        "SELECT s.pid, n.body FROM var_set_member m JOIN var_set s ON s.id = m.set_id
             JOIN text n ON n.id = s.name_text
             WHERE m.variable_id = ? AND s.kind = ? ORDER BY s.pid",
        rusqlite::params![variable, kind],
        |row| {
            Ok(ShowMembership {
                fqid: row.get(0)?,
                name: row.get(1)?,
            })
        },
    )?)
}

/// Members by their facet values in axis order, then name, then fqid.
fn by_axes(axes: &[ShowSetAxis], a: &ShowSetMember, b: &ShowSetMember) -> std::cmp::Ordering {
    axes.iter()
        .map(|axis| {
            let value = |m: &ShowSetMember| {
                m.facets
                    .iter()
                    .find(|f| f.axis == axis.id)
                    .map(|f| f.value.clone())
            };
            value(a).cmp(&value(b))
        })
        .find(|order| !order.is_eq())
        .unwrap_or_else(|| a.name.cmp(&b.name))
        .then_with(|| a.fqid.cmp(&b.fqid))
}

fn set(server: &Server, conn: &Connection, entity: &Entity) -> Result<Show, Error> {
    let (name, description, register): (String, Option<String>, String) = conn.query_row(
        "SELECT n.body, d.body, r.pid FROM var_set s
         JOIN text n ON n.id = s.name_text
         LEFT JOIN text d ON d.id = s.description_text
         JOIN register r ON r.id = s.register_id WHERE s.id = ?",
        [entity.id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let axes: Vec<ShowSetAxis> = rows(
        conn,
        "SELECT a.axis, l.body FROM var_set_axis a
         JOIN text l ON l.id = a.label_text WHERE a.set_id = ? ORDER BY a.ord",
        [entity.id],
        |row| {
            Ok(ShowSetAxis {
                id: row.get(0)?,
                label: row.get(1)?,
            })
        },
    )?;
    let mut members = set_members(server, conn, entity)?;
    members.sort_by(|(_, a), (_, b)| by_axes(&axes, a, b));
    // The curated description, else one every member states alike, is sent once.
    let description = description.or_else(|| {
        let (_, first) = members.first()?;
        let shared = first.description.as_ref()?;
        members
            .iter()
            .all(|(_, m)| m.description.as_ref() == Some(shared))
            .then(|| shared.clone())
    });
    for (_, member) in &mut members {
        if member.description == description {
            member.description = None;
        }
    }
    let tags = rows(
        conn,
        "SELECT DISTINCT t.pid, n.body FROM var_set_member m
         JOIN var_set_member tm ON tm.variable_id = m.variable_id
         JOIN var_set t ON t.id = tm.set_id AND t.kind = 'tag'
         JOIN text n ON n.id = t.name_text WHERE m.set_id = ? ORDER BY t.pid",
        [entity.id],
        |row| {
            Ok(ShowMembership {
                fqid: row.get(0)?,
                name: row.get(1)?,
            })
        },
    )?;
    Ok(Show::Set {
        fqid: entity.pid.clone(),
        name,
        description,
        register,
        axes,
        members: members.into_iter().map(|(_, member)| member).collect(),
        tags,
    })
}

/// A set's members with their descriptions, facets and per-period lists; in steward
/// mode only the held members, each over its held columns and years (`not_found` when
/// none is held).
fn set_members(
    server: &Server,
    conn: &Connection,
    entity: &Entity,
) -> Result<Vec<(i64, ShowSetMember)>, Error> {
    let mut members: Vec<(i64, ShowSetMember)> = rows(
        conn,
        "SELECT v.id, v.pid, l.body, d.body FROM var_set_member m
         JOIN variable v ON v.id = m.variable_id
         JOIN text l ON l.id = v.label_text
         LEFT JOIN text d ON d.id = v.description_text
         WHERE m.set_id = ? ORDER BY v.pid",
        [entity.id],
        |row| {
            Ok((
                row.get(0)?,
                ShowSetMember {
                    fqid: row.get(1)?,
                    name: row.get(2)?,
                    description: row.get(3)?,
                    segments: vec![],
                    facets: vec![],
                },
            ))
        },
    )?;
    let mut facets = conn.prepare(
        "SELECT f.axis, f.value, l.body FROM var_set_member_facet f
         JOIN var_set_axis a ON a.set_id = f.set_id AND a.axis = f.axis
         JOIN text l ON l.id = f.label_text
         WHERE f.set_id = ? AND f.variable_id = ? ORDER BY a.ord",
    )?;
    members.retain(|(_, member)| server.catalog.serves("variable", &member.fqid));
    if server.catalog.steward().is_some() && members.is_empty() {
        return Err(Error::new(
            Code::NotFound,
            format!("Nothing is named {:?}.", entity.pid),
            vec![entity.pid.clone().into()],
        ));
    }
    let origins = segments::origins(server, server.catalog.connect()?)?;
    // simplify: one segments list per member; batch by variable id if a set reaches
    // hundreds of members.
    for (id, member) in &mut members {
        member.segments = segments::all(server, &origins, &member.fqid)?;
        member.facets = facets
            .query_map(rusqlite::params![entity.id, *id], |row| {
                Ok(ShowSetFacet {
                    axis: row.get(0)?,
                    value: row.get(1)?,
                    label: row.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
    }
    Ok(members)
}
