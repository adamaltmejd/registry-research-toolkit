//! `show`: the summary of any ref; no ref is the catalog root.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs::{self, Entity};
use super::shape::{self, Release};
use super::show_evidence::{self, SourceDefinition, VariableEvidence};
use super::{Cache, Operation, Param, Params, Period, Server, Type, component, flag};
use crate::{Code, Error};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "show",
    paths: &["/api/catalog", "/api/catalog/{ref}"],
    tool: Some("show"),
    description: "The summary of a provider, register, table or variable by public id or bare \
                  name; a set only by set/<provider>/<register>/<slug>. Without a ref, \
                  the catalog's providers. Variables show curated \
                  metadata by default, with marked provider text only as a fallback. \
                  Use evidence=true for provider text history, source keys/spellings, \
                  curation statement locators and linked documents.",
    params: &[
        Param::optional("ref", Type::Ref),
        Param::optional("evidence", Type::Boolean),
    ],
    cache: Cache::Minute,
    run: show,
    result: component::<Show>,
}];

/// `shape.Show`: a summary tagged by `kind`. Every kind but `root` carries the
/// resolved `fqid`, so a bare-name request gets the id back.
#[derive(Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Show {
    /// In steward mode, the steward's own providers too.
    Root { providers: Vec<ShowProviderRef> },
    Provider {
        fqid: String,
        name: String,
        /// In steward mode, the steward's own registers too.
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
        /// The literal names of the releases the reader could not place in time.
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
        releases: Vec<Release>,
        row_keys: Vec<ShowRowKey>,
    },
    Set {
        fqid: String,
        name: String,
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
        /// The coverage hull; `coverage` supplies detailed spans and gaps.
        period: Period,
        /// The one curated description, separate from source text runs.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(nullable = false)]
        description: Option<String>,
        /// Provider text fallback only when no curated description is available.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(nullable = false)]
        source_definition: Option<SourceDefinition>,
        sets: Vec<ShowMembership>,
        tags: Vec<ShowMembership>,
        /// Provenance is present only when requested with `evidence=true`.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(nullable = false)]
        evidence: Option<VariableEvidence>,
        /// The fqids of the tables the variable has segments in.
        tables: Vec<String>,
    },
}

#[derive(Serialize, ToSchema)]
pub struct ShowProviderRef {
    fqid: String,
    name: String,
    /// In steward mode, whether the steward holds any of its registers.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    held: Option<bool>,
}

#[derive(Serialize, ToSchema)]
pub struct ShowRegisterRef {
    fqid: String,
    name: String,
    period: Period,
    /// In steward mode, whether the steward holds any of its tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    held: Option<bool>,
}

#[derive(Serialize, ToSchema)]
pub struct ShowVariableRef {
    fqid: String,
    name: String,
    period: Period,
    /// The fqids of the register's tables the variable has segments in.
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

/// A segment of the table that holds its rows' key.
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
    /// The member's columns, ordered by table then spelling.
    columns: Vec<ShowSetColumn>,
    /// The member's coordinates in the set's axis order.
    facets: Vec<ShowSetFacet>,
}

/// One (table, spelling) a set member has segments in.
#[derive(Serialize, ToSchema)]
pub struct ShowSetColumn {
    /// The table's fqid.
    table: String,
    spelling: String,
    /// The column's segment periods, in time order.
    periods: Vec<Period>,
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
                let held = Held::read(&conn, &extra)?;
                let known: BTreeSet<_> = providers.iter().map(|p| p.fqid.clone()).collect();
                providers.extend(
                    provider_refs(&extra)?
                        .into_iter()
                        .filter(|p| !known.contains(&p.fqid)),
                );
                providers.sort_by(|a, b| a.fqid.cmp(&b.fqid));
                for p in &mut providers {
                    p.held = Some(held.providers.contains(&p.fqid));
                }
            }
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

/// SWECOV steward registers that are compile scaffolding, not data: columns the
/// steward compile could not place on a global variable. Left out of provider
/// listings (and so the browse tree); links and search still reach them.
// simplify: hard-coded because nothing in the artifact marks them; delete when the
// steward places these columns on their source registers (#2134 step 2, 0.73.0).
const STEWARD_SCAFFOLD: [&str; 7] = [
    "swecov/received-row-keys",
    "swecov/scb-agi-delivery",
    "swecov/scb-agi-slots",
    "swecov/scb-geography-delivery",
    "swecov/scb-innovation-foretag-current-coding",
    "swecov/sos-bu-current-coding",
    "swecov/sos-par-slots",
];

/// Steward mode: the registers and providers with an accepted placement. A
/// placement names its table by public id, in either origin.
struct Held {
    registers: BTreeSet<String>,
    providers: BTreeSet<String>,
}

impl Held {
    // simplify: recomputed per request (~1k table ids); cache on the server if the
    // root or provider summaries show up in latency.
    fn read(base: &Connection, extra: &Connection) -> Result<Self, Error> {
        let tables: String = extra.query_row(
            "SELECT json_group_array(DISTINCT table_pid) FROM holding_mapping",
            [],
            |r| r.get(0),
        )?;
        let mut held = Self {
            registers: BTreeSet::new(),
            providers: BTreeSet::new(),
        };
        for conn in [base, extra] {
            let mut query = conn.prepare(
                "SELECT DISTINCT r.pid, p.pid FROM json_each(?) j
                 JOIN tbl t ON t.pid = j.value JOIN register r ON r.id = t.register_id
                 JOIN provider p ON p.id = r.provider_id",
            )?;
            for row in query.query_map([&tables], |r| Ok((r.get(0)?, r.get(1)?)))? {
                let (register, provider) = row?;
                held.registers.insert(register);
                held.providers.insert(provider);
            }
        }
        Ok(held)
    }
}

fn provider_refs(conn: &Connection) -> Result<Vec<ShowProviderRef>, Error> {
    Ok(conn
        .prepare(
            "SELECT p.pid, n.body FROM provider p JOIN text n ON n.id = p.name_text
             ORDER BY p.pid",
        )?
        .query_map([], |row| {
            Ok(ShowProviderRef {
                fqid: row.get(0)?,
                name: row.get(1)?,
                held: None,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

fn register_refs(
    server: &Server,
    conn: &Connection,
    provider: &str,
) -> Result<Vec<ShowRegisterRef>, Error> {
    Ok(conn
        .prepare(
            "SELECT r.pid, n.body, r.start, r.end FROM register r
             JOIN provider p ON p.id = r.provider_id JOIN text n ON n.id = r.name_text
             WHERE p.pid = ? ORDER BY r.pid",
        )?
        .query_map([provider], |row| {
            Ok(ShowRegisterRef {
                fqid: row.get(0)?,
                name: row.get(1)?,
                period: server.period(row.get(2)?, row.get(3)?),
                held: None,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

fn provider(server: &Server, conn: &Connection, entity: &Entity) -> Result<Show, Error> {
    let name = conn.query_row(
        "SELECT n.body FROM provider p JOIN text n ON n.id = p.name_text WHERE p.id = ?",
        [entity.id],
        |row| row.get(0),
    )?;
    let mut registers = register_refs(server, conn, &entity.pid)?;
    if let Some(extra) = server.catalog.connect_holdings()? {
        let base = server.catalog.connect()?;
        // A register in both origins is a steward overlay of the reference one: the
        // reference row carries its official name (the overlay's is its slug).
        let known: BTreeSet<_> = registers.iter().map(|r| r.fqid.clone()).collect();
        registers.extend(
            register_refs(server, &extra, &entity.pid)?
                .into_iter()
                .filter(|r| !known.contains(&r.fqid)),
        );
        registers.sort_by(|a, b| a.fqid.cmp(&b.fqid));
        registers.retain(|r| !STEWARD_SCAFFOLD.contains(&r.fqid.as_str()));
        let held = Held::read(&base, &extra)?;
        for r in &mut registers {
            r.held = Some(held.registers.contains(&r.fqid));
        }
    }
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
    let tables = conn
        .prepare(
            "SELECT t.pid, n.body, t.role, t.implicit, t.timeless, u.slug, t.cadence,
                    t.start, t.end
             FROM tbl t JOIN text n ON n.id = t.name_text
               LEFT JOIN unit_type u ON u.id = t.unit_type_id
             WHERE t.register_id = ? ORDER BY t.pid",
        )?
        .query_map([entity.id], |row| {
            Ok(ShowTable {
                fqid: row.get(0)?,
                name: row.get(1)?,
                role: row.get(2)?,
                implicit: row.get(3)?,
                timeless: row.get(4)?,
                unit_type: row.get(5)?,
                cadence: row.get(6)?,
                period: server.period(row.get(7)?, row.get(8)?),
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut members: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in conn
        .prepare(
            "SELECT DISTINCT v.pid, t.pid FROM segment s
             JOIN variable v ON v.id = s.variable_id JOIN tbl t ON t.id = s.table_id
             WHERE v.register_id = ? ORDER BY v.pid, t.pid",
        )?
        .query_map([entity.id], |row| Ok((row.get(0)?, row.get(1)?)))?
    {
        let (variable, table) = row?;
        members.entry(variable).or_default().push(table);
    }
    let variables = conn
        .prepare(
            "SELECT v.pid, l.body, v.start, v.end
             FROM variable v JOIN text l ON l.id = v.label_text
             WHERE v.register_id = ? ORDER BY v.pid",
        )?
        .query_map([entity.id], |row| {
            let fqid: String = row.get(0)?;
            Ok(ShowVariableRef {
                tables: members.remove(&fqid).unwrap_or_default(),
                fqid,
                name: row.get(1)?,
                period: server.period(row.get(2)?, row.get(3)?),
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    let unplaced = conn
        .prepare(
            "SELECT DISTINCT n.body
             FROM release r JOIN tbl t ON t.id = r.table_id
               JOIN text n ON n.id = r.name_text
             WHERE t.register_id = ? AND r.parse_state = 'unparsed' ORDER BY n.body",
        )?
        .query_map([entity.id], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(Show::Register {
        fqid: entity.pid.clone(),
        name,
        description,
        period: server.period(start, end),
        tables,
        variables,
        unplaced,
    })
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
    let spans = conn
        .prepare("SELECT start, end FROM tbl_span WHERE table_id = ? ORDER BY start")?
        .query_map([entity.id], |row| {
            Ok(server.period(row.get(0)?, row.get(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let row_keys = conn
        .prepare(
            "SELECT v.pid, sp.exact, s.start, s.end
             FROM segment s JOIN variable v ON v.id = s.variable_id
               JOIN spelling sp ON sp.id = s.spelling_id
             WHERE s.table_id = ? AND s.role = 'row_key' ORDER BY s.start, v.pid",
        )?
        .query_map([entity.id], |row| {
            Ok(ShowRowKey {
                fqid: row.get(0)?,
                spelling: row.get(1)?,
                period: server.period(row.get(2)?, row.get(3)?),
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
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
        releases: shape::releases(server, conn, entity.id)?,
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
    let source_definition = if description.is_none() {
        show_evidence::latest_source_text(server, conn, entity.id)?
    } else {
        None
    };
    let tables = conn
        .prepare(
            "SELECT DISTINCT t.pid FROM segment s JOIN tbl t ON t.id = s.table_id
             WHERE s.variable_id = ? ORDER BY t.pid",
        )?
        .query_map([entity.id], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(Show::Variable {
        fqid: entity.pid.clone(),
        name,
        register,
        period: server.period(start, end),
        description,
        source_definition,
        sets: memberships(conn, entity.id, "set")?,
        tags: memberships(conn, entity.id, "tag")?,
        evidence: include_evidence
            .then(|| show_evidence::load(server, conn, entity))
            .transpose()?,
        tables,
    })
}

fn memberships(conn: &Connection, variable: i64, kind: &str) -> Result<Vec<ShowMembership>, Error> {
    Ok(conn
        .prepare(
            "SELECT s.pid, n.body FROM var_set_member m JOIN var_set s ON s.id = m.set_id
             JOIN text n ON n.id = s.name_text
             WHERE m.variable_id = ? AND s.kind = ? ORDER BY s.pid",
        )?
        .query_map(rusqlite::params![variable, kind], |row| {
            Ok(ShowMembership {
                fqid: row.get(0)?,
                name: row.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

/// A variable's columns from its segments as `segments` reads them, one per (table,
/// spelling); the open start sorts first.
fn columns(
    server: &Server,
    segments: &mut rusqlite::Statement,
    variable: i64,
) -> Result<Vec<ShowSetColumn>, Error> {
    let rows = segments
        .query_map([variable], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                server.period(row.get(2)?, row.get(3)?),
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut columns: Vec<ShowSetColumn> = vec![];
    for (table, spelling, period) in rows {
        match columns.last_mut() {
            Some(c) if c.table == table && c.spelling == spelling => c.periods.push(period),
            _ => columns.push(ShowSetColumn {
                table,
                spelling,
                periods: vec![period],
            }),
        }
    }
    Ok(columns)
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
    let axes: Vec<ShowSetAxis> = conn
        .prepare(
            "SELECT a.axis, l.body FROM var_set_axis a
         JOIN text l ON l.id = a.label_text WHERE a.set_id = ? ORDER BY a.ord",
        )?
        .query_map([entity.id], |row| {
            Ok(ShowSetAxis {
                id: row.get(0)?,
                label: row.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut members: Vec<(i64, ShowSetMember)> = conn
        .prepare(
            "SELECT v.id, v.pid, l.body FROM var_set_member m
         JOIN variable v ON v.id = m.variable_id
         JOIN text l ON l.id = v.label_text WHERE m.set_id = ? ORDER BY v.pid",
        )?
        .query_map([entity.id], |row| {
            Ok((
                row.get(0)?,
                ShowSetMember {
                    fqid: row.get(1)?,
                    name: row.get(2)?,
                    columns: vec![],
                    facets: vec![],
                },
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut facets = conn.prepare(
        "SELECT f.axis, f.value, l.body FROM var_set_member_facet f
         JOIN var_set_axis a ON a.set_id = f.set_id AND a.axis = f.axis
         JOIN text l ON l.id = f.label_text
         WHERE f.set_id = ? AND f.variable_id = ? ORDER BY a.ord",
    )?;
    let mut segments = conn.prepare(
        "SELECT t.pid, sp.exact, s.start, s.end FROM segment s
         JOIN tbl t ON t.id = s.table_id
         JOIN spelling sp ON sp.id = s.spelling_id
         WHERE s.variable_id = ? ORDER BY t.pid, sp.exact, s.start, s.end",
    )?;
    for (id, member) in &mut members {
        member.columns = columns(server, &mut segments, *id)?;
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
    members.sort_by(|(_, a), (_, b)| by_axes(&axes, a, b));
    let tags = conn
        .prepare(
            "SELECT DISTINCT t.pid, n.body FROM var_set_member m
         JOIN var_set_member tm ON tm.variable_id = m.variable_id
         JOIN var_set t ON t.id = tm.set_id AND t.kind = 'tag'
         JOIN text n ON n.id = t.name_text WHERE m.set_id = ? ORDER BY t.pid",
        )?
        .query_map([entity.id], |row| {
            Ok(ShowMembership {
                fqid: row.get(0)?,
                name: row.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
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
