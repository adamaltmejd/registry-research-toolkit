//! `show`: the summary of any ref; no ref is the catalog root.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs::{self, Entity};
use super::segments::{self, Segment};
use super::shape::{self, Release, ValueSetRef};
use super::show_evidence::{self, VariableEvidence};
use super::{Cache, HeldClip, Operation, Param, Params, Period, Server, Type, component, flag};
use crate::{Error, rows};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "show",
    paths: &["/api/catalog", "/api/catalog/{ref}"],
    tool: Some("show"),
    description: "Describe a provider, register, table, variable, set or classification by id \
        or name (a set only by `set/<provider>/<register>/<slug>`; it carries each member's \
        whole `segments` list, so no `segments` call per member is needed). A classification \
        (`class/<family>`) or one of its versions lists its versions, each with its part \
        and the versions it succeeds, and the variables that use them, each with its rows \
        that carry the classification; `class` lists the classifications with their \
        versions. No `ref` lists the providers, each with its registers. Variables show curated metadata, with the provider's text only as a \
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

/// A summary tagged by `kind`. Every kind but `root` and `class_families` carries the resolved `fqid`, so a bare-name request gets the id back.
#[derive(Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Show {
    /// On a data holder's catalog, the holder's own providers too.
    Root { providers: Vec<ShowProviderRef> },
    Provider {
        fqid: String,
        name: String,
        /// On a data holder's catalog, the holder's own registers too.
        registers: Vec<ShowProviderRegister>,
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
        cadence: Option<String>,
        spans: Vec<Period>,
        /// Absent when none, and on a data holder's catalog.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        releases: Vec<Release>,
        row_keys: Vec<ShowRowKey>,
        /// The variables in this table, each over the years it has here.
        variables: Vec<ShowTableVariable>,
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
    /// The classifications, at ref `class`, in fqid order. On a data holder's catalog
    /// only those the holder's variables use.
    ClassFamilies { families: Vec<ShowClassFamilyRef> },
    /// A classification. Classifications are shared, so every version is listed; on a
    /// data holder's catalog only the holder's variables are.
    ClassFamily {
        fqid: String,
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(nullable = false)]
        description: Option<String>,
        /// Who publishes it.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(nullable = false)]
        publisher: Option<String>,
        /// In start order.
        versions: Vec<ShowClassVersion>,
        variables: Vec<ShowClassUser>,
    },
    ClassVersion {
        fqid: String,
        name: String,
        /// The version's validity.
        period: Period,
        value_set: ValueSetRef,
        family: ShowMembership,
        /// The line it belongs to, in a classification with several.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(nullable = false)]
        part: Option<String>,
        /// The versions it directly succeeds, in fqid order.
        succeeds: Vec<String>,
        /// The variables that use this version.
        variables: Vec<ShowClassUser>,
    },
}

/// A variable in a table, over the periods it has in that table.
#[derive(Serialize, ToSchema)]
pub struct ShowTableVariable {
    fqid: String,
    name: String,
    /// The periods its segments here cover, merged, in start order. Gaps stay gaps.
    periods: Vec<Period>,
    /// On a data holder with several databases, those periods per database that holds
    /// the variable here, in config order (`coverage`'s rule).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    databases: Vec<ShowTableDatabase>,
}

/// A variable's periods in a table, in one of the data holder's databases.
#[derive(Serialize, ToSchema)]
pub struct ShowTableDatabase {
    /// The database's id, as `steward_config.spaces` lists it.
    space: String,
    periods: Vec<Period>,
}

#[derive(Serialize, ToSchema)]
pub struct ShowClassFamilyRef {
    fqid: String,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    description: Option<String>,
    /// How many versions it has.
    version_count: u32,
    /// From the first version's start to the last one's end.
    period: Period,
    /// In start order.
    versions: Vec<ShowClassFamilyVersion>,
}

/// A version in the list of classifications.
#[derive(Serialize, ToSchema)]
pub struct ShowClassFamilyVersion {
    fqid: String,
    name: String,
    /// The version's validity.
    period: Period,
    /// The line it belongs to, in a classification with several.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    part: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct ShowClassVersion {
    fqid: String,
    name: String,
    /// The version's validity.
    period: Period,
    value_set: ValueSetRef,
    /// The line it belongs to, in a classification with several.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    part: Option<String>,
    /// The versions it directly succeeds, in fqid order.
    succeeds: Vec<String>,
    /// Whether one of `variables` uses this version.
    used: bool,
}

/// A variable that uses the classification, with its rows that carry it.
#[derive(Serialize, ToSchema)]
pub struct ShowClassUser {
    fqid: String,
    name: String,
    /// Its rows, as `segments` serves them, whose `classification` is the family's (or
    /// the version's).
    segments: Vec<Segment>,
}

/// A root provider, with its registers as its provider summary lists them, so the
/// root alone draws the whole register list (#2232).
#[derive(Serialize, ToSchema)]
pub struct ShowProviderRef {
    fqid: String,
    name: String,
    registers: Vec<ShowRegisterRef>,
}

#[derive(Serialize, ToSchema)]
pub struct ShowRegisterRef {
    fqid: String,
    name: String,
    period: Period,
}

/// A provider's register with its variables, so the provider alone lists every
/// variable of its registers.
#[derive(Serialize, ToSchema)]
pub struct ShowProviderRegister {
    fqid: String,
    name: String,
    /// The register's description, as its own `show` carries it.
    description: Option<String>,
    /// The periods its dated tables are delivered, merged, in start order. Gaps stay gaps.
    periods: Vec<Period>,
    /// How many of its tables deliver a listed variable: the tables its own `show`
    /// offers as a filter.
    table_count: usize,
    variables: Vec<ShowProviderVariable>,
}

/// A variable in a provider's register list: what the list shows (the name, and the
/// years where two names repeat), not the tables `show` on the register lists.
#[derive(Serialize, ToSchema)]
pub struct ShowProviderVariable {
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
    let evidence = flag(params, "evidence")?;
    if params.get("ref").is_some_and(|r| *r == "class") {
        let families = class_families(server)?;
        return Ok(serde_json::to_value(Show::ClassFamilies { families }).expect("Show serializes"));
    }
    let conn = match params.get("ref") {
        Some(value) => server.catalog.connect_for_ref(value)?,
        None => server.catalog.connect()?,
    };
    let shown = match params.get("ref") {
        None => {
            let extra = server.catalog.connect_holdings()?;
            let mut providers = provider_refs(&conn)?;
            if let Some(extra) = &extra {
                union_by_fqid(&mut providers, provider_refs(extra)?, |p| &p.fqid);
            }
            providers.retain(|p| server.catalog.serves("provider", &p.fqid));
            let mut registers = served_registers(server, &conn, extra.as_ref(), None)?;
            for provider in &mut providers {
                provider.registers = registers.remove(&provider.fqid).unwrap_or_default();
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
                "class_family" => class_family(server, &conn, &entity)?,
                "class_version" => class_version(server, &conn, &entity)?,
                kind => {
                    return Err(Error::internal(format!(
                        "show has no summary for a {kind}."
                    )));
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
                registers: vec![],
            })
        },
    )?)
}

/// One origin's registers, each with its provider's pid; one provider's when given.
fn register_refs(
    server: &Server,
    conn: &Connection,
    provider: Option<&str>,
) -> Result<Vec<(String, ShowRegisterRef)>, Error> {
    Ok(rows(
        conn,
        "SELECT p.pid, r.pid, n.body, r.start, r.end FROM register r
             JOIN provider p ON p.id = r.provider_id JOIN text n ON n.id = r.name_text
             WHERE ?1 IS NULL OR p.pid = ?1 ORDER BY r.pid",
        [provider],
        |row| {
            let fqid: String = row.get(1)?;
            Ok((
                row.get(0)?,
                ShowRegisterRef {
                    period: server.hull("register", &fqid, row.get(3)?, row.get(4)?),
                    fqid,
                    name: row.get(2)?,
                },
            ))
        },
    )?)
}

/// The served registers by provider pid, each list in pid order; one provider's when
/// given. A register in both origins is a steward overlay of the reference one: the
/// reference row carries its official name (the overlay's is its slug).
fn served_registers(
    server: &Server,
    conn: &Connection,
    extra: Option<&Connection>,
    provider: Option<&str>,
) -> Result<BTreeMap<String, Vec<ShowRegisterRef>>, Error> {
    let mut found = register_refs(server, conn, provider)?;
    if let Some(extra) = extra {
        union_by_fqid(
            &mut found,
            register_refs(server, extra, provider)?,
            |(_, r)| &r.fqid,
        );
    }
    let mut by_provider: BTreeMap<String, Vec<ShowRegisterRef>> = BTreeMap::new();
    for (provider, register) in found {
        if server.catalog.serves("register", &register.fqid) {
            by_provider.entry(provider).or_default().push(register);
        }
    }
    Ok(by_provider)
}

/// Steward mode: the holdings origin's rows join the base list, a pid both origins
/// define keeps the base (reference) row, and the list is re-sorted by pid.
fn union_by_fqid<T>(base: &mut Vec<T>, extra: Vec<T>, fqid: impl Fn(&T) -> &String) {
    let known: BTreeSet<String> = base.iter().map(|row| fqid(row).clone()).collect();
    base.extend(extra.into_iter().filter(|row| !known.contains(fqid(row))));
    base.sort_by(|a, b| fqid(a).cmp(fqid(b)));
}

/// A register's delivered periods: in steward mode the years it is held, else the merged
/// spans of its dated (not timeless) tables.
fn register_periods(
    server: &Server,
    conn: &Connection,
    register: &str,
) -> Result<Vec<Period>, Error> {
    let spans: Vec<reg_core::days::Span> = match server.catalog.held_spans("register", register) {
        Some(held) => held.to_vec(),
        None => rows(
            conn,
            "SELECT s.start, s.end FROM tbl_span s JOIN tbl t ON t.id = s.table_id
               JOIN register r ON r.id = t.register_id
             WHERE r.pid = ? AND NOT t.timeless",
            [register],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?,
    };
    Ok(reg_core::days::merge(spans)
        .into_iter()
        .map(|(start, end)| server.period(start, end))
        .collect())
}

fn provider(server: &Server, conn: &Connection, entity: &Entity) -> Result<Show, Error> {
    let name = conn.query_row(
        "SELECT n.body FROM provider p JOIN text n ON n.id = p.name_text WHERE p.id = ?",
        [entity.id],
        |row| row.get(0),
    )?;
    let extra = server.catalog.connect_holdings()?;
    let registers = served_registers(server, conn, extra.as_ref(), Some(&entity.pid))?
        .remove(&entity.pid)
        .unwrap_or_default()
        .into_iter()
        .map(|register| {
            let variables = served_variables(server, conn, extra.as_ref(), &register.fqid)?;
            // The register page's table filter offers the served tables a served
            // variable sits in (`served_variables` keeps only those memberships).
            let tables: BTreeSet<&String> = variables.iter().flat_map(|v| &v.tables).collect();
            Ok(ShowProviderRegister {
                description: register_description(conn, extra.as_ref(), &register.fqid)?,
                table_count: tables.len(),
                variables: variables
                    .into_iter()
                    .map(|v| ShowProviderVariable {
                        fqid: v.fqid,
                        name: v.name,
                        period: v.period,
                    })
                    .collect(),
                periods: register_periods(server, conn, &register.fqid)?,
                fqid: register.fqid,
                name: register.name,
            })
        })
        .collect::<Result<_, Error>>()?;
    Ok(Show::Provider {
        fqid: entity.pid.clone(),
        name,
        registers,
    })
}

/// A register's description from the origin its own `show` reads: the reference row
/// when both origins define the pid (`Catalog::connect_for_ref`), else the steward's.
fn register_description(
    conn: &Connection,
    extra: Option<&Connection>,
    register: &str,
) -> Result<Option<String>, Error> {
    let query = |conn: &Connection| -> Result<Option<Option<String>>, Error> {
        Ok(conn
            .query_row(
                "SELECT d.body FROM register r LEFT JOIN text d ON d.id = r.description_text
                 WHERE r.pid = ?",
                [register],
                |row| row.get(0),
            )
            .optional()?)
    };
    Ok(match (query(conn)?, extra) {
        (Some(description), _) => description,
        (None, Some(extra)) => query(extra)?.flatten(),
        (None, None) => None,
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
    let mut tables = register_tables(server, conn, &entity.pid)?;
    let extra = server.catalog.connect_holdings()?;
    let variables = served_variables(server, conn, extra.as_ref(), &entity.pid)?;
    let (mut sets, mut tags) = groupings(conn, &entity.pid)?;
    // Steward mode: tables the steward defines inside a reference register join it;
    // a pid both origins define keeps the reference row.
    if let Some(extra) = &extra {
        union_by_fqid(
            &mut tables,
            register_tables(server, extra, &entity.pid)?,
            |t| &t.fqid,
        );
        let (steward_sets, steward_tags) = groupings(extra, &entity.pid)?;
        union_by_fqid(&mut sets, steward_sets, |s| &s.fqid);
        union_by_fqid(&mut tags, steward_tags, |t| &t.fqid);
    }
    // Steward mode lists only what the steward holds (`Catalog::serves`); a set
    // keeps its held members and goes when fewer than two are held (#2410).
    tables.retain(|t| server.catalog.serves("table", &t.fqid));
    for set in &mut sets {
        set.members
            .retain(|member| server.catalog.serves("variable", member));
    }
    sets.retain(|set| server.catalog.serves("set", &set.fqid));
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

/// A register's served variables, both origins merged: a pid both define keeps the
/// reference row but gains the tables the steward attests it in (a membership the
/// reference member list omits). Steward mode lists only what the steward holds
/// (`Catalog::serves`).
fn served_variables(
    server: &Server,
    conn: &Connection,
    extra: Option<&Connection>,
    register: &str,
) -> Result<Vec<ShowVariableRef>, Error> {
    let mut variables = register_variables(server, conn, register)?;
    if let Some(extra) = extra {
        let mut steward_variables: BTreeMap<_, _> = register_variables(server, extra, register)?
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
    }
    variables.retain(|v| server.catalog.serves("variable", &v.fqid));
    for variable in &mut variables {
        let fqid = &variable.fqid;
        variable
            .tables
            .retain(|table| server.catalog.serves_in(fqid, table));
    }
    Ok(variables)
}

/// A register's tables in one origin, by the register's public id.
fn register_tables(
    server: &Server,
    conn: &Connection,
    register: &str,
) -> Result<Vec<ShowTable>, Error> {
    Ok(rows(
        conn,
        "SELECT t.pid, n.body, t.role, t.implicit, t.timeless, t.cadence, t.start, t.end
             FROM tbl t JOIN register r ON r.id = t.register_id
               JOIN text n ON n.id = t.name_text
             WHERE r.pid = ? ORDER BY t.pid",
        [register],
        |row| {
            let fqid: String = row.get(0)?;
            Ok(ShowTable {
                period: server.hull("table", &fqid, row.get(6)?, row.get(7)?),
                fqid,
                name: row.get(1)?,
                role: row.get(2)?,
                implicit: row.get(3)?,
                timeless: row.get(4)?,
                cadence: row.get(5)?,
            })
        },
    )?)
}

/// A register's variables in one origin, each with the tables it appears in, by the
/// register's public id.
fn register_variables(
    server: &Server,
    conn: &Connection,
    register: &str,
) -> Result<Vec<ShowVariableRef>, Error> {
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
    Ok(rows(
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
    )?)
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
    );
    let (name, description, register, role, implicit, timeless, cadence): Head = conn.query_row(
        "SELECT n.body, d.body, r.pid, t.role, t.implicit, t.timeless, t.cadence
             FROM tbl t JOIN text n ON n.id = t.name_text
               LEFT JOIN text d ON d.id = t.description_text
               JOIN register r ON r.id = t.register_id
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
    let row_keys = row_keys(server, conn, &entity.pid)?;
    let variables = table_variables(server, conn, &entity.pid)?;
    Ok(Show::Table {
        fqid: entity.pid.clone(),
        name,
        description,
        register,
        role,
        implicit,
        timeless,
        cadence,
        spans,
        // The provider's releases are not the steward's files (maintainer 2026-11-04).
        releases: if server.catalog.steward().is_some() {
            vec![]
        } else {
            shape::releases(server, conn, entity.id)?
        },
        row_keys,
        variables,
    })
}

/// A table's variables, each over the merged periods its segments here hold, from
/// both origins; in steward mode only the pairs the steward places, clipped as
/// `row_keys` clips.
fn table_variables(
    server: &Server,
    conn: &Connection,
    pid: &str,
) -> Result<Vec<ShowTableVariable>, Error> {
    let spans = |conn: &Connection| {
        rows(
            conn,
            "SELECT v.pid, s.start, s.end
                 FROM segment s JOIN variable v ON v.id = s.variable_id
                   JOIN tbl t ON t.id = s.table_id
                 WHERE t.pid = ?",
            [pid],
            |row| Ok((row.get::<_, String>(0)?, row.get(1)?, row.get(2)?)),
        )
    };
    let mut found: Vec<(String, i64, i64)> = spans(conn)?;
    let extra = server.catalog.connect_holdings()?;
    if let Some(extra) = &extra {
        found.extend(spans(extra)?);
    }
    let mut held = extra.as_ref().map(HeldClip::new).transpose()?;
    type Span = reg_core::days::Span;
    let mut spans_of: BTreeMap<String, Vec<Span>> = BTreeMap::new();
    let mut by_space: BTreeMap<String, BTreeMap<(i64, String), Vec<Span>>> = BTreeMap::new();
    for (fqid, start, end) in found {
        if !server.catalog.serves_in(&fqid, pid) {
            continue;
        }
        let days = match held.as_mut() {
            Some(held) => held.clip_each(pid, &fqid, (start, end))?,
            None => vec![(None, (start, end))],
        };
        for (space, days) in days {
            if let Some(space) = space {
                by_space
                    .entry(fqid.clone())
                    .or_default()
                    .entry(space)
                    .or_default()
                    .push(days);
            }
            spans_of.entry(fqid.clone()).or_default().push(days);
        }
    }
    let periods = |spans: Vec<Span>| -> Vec<Period> {
        reg_core::days::merge(spans)
            .into_iter()
            .map(|(start, end)| server.period(start, end))
            .collect()
    };
    let names = refs::names(server, spans_of.keys().map(String::as_str))?;
    spans_of
        .into_iter()
        .map(|(fqid, spans)| {
            Ok(ShowTableVariable {
                name: names.of(refs::Named::Variable, &fqid)?,
                periods: periods(spans),
                databases: by_space
                    .remove(&fqid)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|((_, space), spans)| ShowTableDatabase {
                        space,
                        periods: periods(spans),
                    })
                    .collect(),
                fqid,
            })
        })
        .collect()
}

fn class_family(server: &Server, conn: &Connection, entity: &Entity) -> Result<Show, Error> {
    type Row = (String, String, i64, i64, i64, Option<String>);
    let (name, description, publisher): (String, Option<String>, Option<String>) = conn.query_row(
        "SELECT n.body, d.body, p.body FROM class_family f JOIN text n ON n.id = f.name_text
               LEFT JOIN text d ON d.id = f.description_text
               LEFT JOIN text p ON p.id = f.publisher_text
             WHERE f.id = ?",
        [entity.id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let versions: Vec<Row> = rows(
        conn,
        "SELECT c.pid, n.body, c.start, c.end, c.valueset_id, p.body
             FROM class_version c JOIN text n ON n.id = c.label_text
               LEFT JOIN text p ON p.id = c.part_text
             WHERE c.family_id = ? ORDER BY c.start, c.pid",
        [entity.id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    let mut succeeds = successions(
        conn,
        "WHERE s.family_id = (SELECT id FROM class_family WHERE pid = ?1)",
        &entity.pid,
    )?;
    let pids: BTreeSet<String> = versions.iter().map(|v| v.0.clone()).collect();
    let variables = class_users(server, &pids)?;
    let versions = versions
        .into_iter()
        .map(|(fqid, name, start, end, valueset, part)| {
            Ok(ShowClassVersion {
                used: variables.iter().any(|v| {
                    v.segments
                        .iter()
                        .any(|s| s.classification() == Some(fqid.as_str()))
                }),
                succeeds: succeeds.remove(&fqid).unwrap_or_default(),
                fqid,
                name,
                period: server.period(start, end),
                value_set: shape::value_set_ref(conn, valueset)?,
                part,
            })
        })
        .collect::<Result<_, Error>>()?;
    Ok(Show::ClassFamily {
        fqid: entity.pid.clone(),
        name,
        description,
        publisher,
        versions,
        variables,
    })
}

fn class_version(server: &Server, conn: &Connection, entity: &Entity) -> Result<Show, Error> {
    type Head = (String, i64, i64, i64, String, String, Option<String>);
    let (name, start, end, valueset, family, family_name, part): Head = conn.query_row(
        "SELECT n.body, c.start, c.end, c.valueset_id, f.pid, fam.body, p.body
             FROM class_version c JOIN text n ON n.id = c.label_text
               JOIN class_family f ON f.id = c.family_id
               JOIN text fam ON fam.id = f.name_text
               LEFT JOIN text p ON p.id = c.part_text
             WHERE c.id = ?",
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
            ))
        },
    )?;
    let succeeds = successions(conn, "WHERE s.pid = ?1", &entity.pid)?
        .remove(&entity.pid)
        .unwrap_or_default();
    Ok(Show::ClassVersion {
        fqid: entity.pid.clone(),
        name,
        period: server.period(start, end),
        value_set: shape::value_set_ref(conn, valueset)?,
        family: ShowMembership {
            fqid: family,
            name: family_name,
        },
        part,
        succeeds,
        variables: class_users(server, &BTreeSet::from([entity.pid.clone()]))?,
    })
}

/// The curated succession edges of the class versions `filter` selects (on `s`, the
/// successor, bound to `?1`): each successor's pid with the pids it directly succeeds,
/// in pid order.
fn successions(
    conn: &Connection,
    filter: &str,
    bound: &str,
) -> Result<BTreeMap<String, Vec<String>>, Error> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (subject, object) in rows(
        conn,
        &format!(
            "SELECT s.pid, o.pid FROM relation r
                 JOIN class_version s ON s.id = r.subject_id
                 JOIN class_version o ON o.id = r.object_id
             {filter}
               AND r.kind = 'continues' AND r.subject_kind = 'class_version'
               AND r.object_kind = 'class_version'
             ORDER BY s.pid, o.pid"
        ),
        [bound],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )? {
        out.entry(subject).or_default().push(object);
    }
    Ok(out)
}

/// Every classification with a version, or on a data holder's catalog those with a row in a placement
/// the holder holds. Both origins are read by family pid, the way the root reads
/// providers; the first origin's family wins whole, versions included.
fn class_families(server: &Server) -> Result<Vec<ShowClassFamilyRef>, Error> {
    let origins = segments::origins(server, server.catalog.connect()?)?;
    let steward = server.catalog.steward().is_some();
    let mut families: BTreeMap<String, ShowClassFamilyRef> = BTreeMap::new();
    for conn in &origins {
        let found = rows(
            conn,
            "SELECT f.pid, n.body, d.body, count(*), min(c.start), max(c.end)
                 FROM class_family f JOIN text n ON n.id = f.name_text
                   LEFT JOIN text d ON d.id = f.description_text
                   JOIN class_version c ON c.family_id = f.id
                 GROUP BY f.id",
            [],
            |row| {
                Ok(ShowClassFamilyRef {
                    fqid: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    version_count: row.get(3)?,
                    period: server.period(row.get(4)?, row.get(5)?),
                    versions: Vec::new(),
                })
            },
        )?;
        let mut versions: BTreeMap<String, Vec<ShowClassFamilyVersion>> = BTreeMap::new();
        for (family, version) in rows(
            conn,
            "SELECT f.pid, c.pid, n.body, c.start, c.end, p.body
                 FROM class_version c JOIN class_family f ON f.id = c.family_id
                   JOIN text n ON n.id = c.label_text
                   LEFT JOIN text p ON p.id = c.part_text
                 ORDER BY f.pid, c.start, c.pid",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    ShowClassFamilyVersion {
                        fqid: row.get(1)?,
                        name: row.get(2)?,
                        period: server.period(row.get(3)?, row.get(4)?),
                        part: row.get(5)?,
                    },
                ))
            },
        )? {
            versions.entry(family).or_default().push(version);
        }
        for mut family in found {
            family.versions = versions.remove(&family.fqid).unwrap_or_default();
            families.entry(family.fqid.clone()).or_insert(family);
        }
    }
    if steward {
        // simplify: a held placement counts whatever its years; scope by held years if
        // a listed classification's page turns up with no variables.
        let mut used = BTreeSet::new();
        for conn in &origins {
            for (family, variable, table) in rows(
                conn,
                "SELECT DISTINCT f.pid, v.pid, t.pid FROM segment s
                     JOIN class_version c ON c.id = s.class_version_id
                     JOIN class_family f ON f.id = c.family_id
                     JOIN variable v ON v.id = s.variable_id
                     JOIN tbl t ON t.id = s.table_id",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )? {
                if server.catalog.serves_in(&variable, &table) {
                    used.insert(family);
                }
            }
        }
        families.retain(|fqid, _| used.contains(fqid));
    }
    Ok(families.into_values().collect())
}

/// The served variables whose rows carry one of `versions`, each with those rows, in
/// fqid order. Candidates come from both origins by version pid (row ids are per
/// origin); the rows are the scoped `segments` list, so steward mode keeps only held
/// placements over held years.
fn class_users(server: &Server, versions: &BTreeSet<String>) -> Result<Vec<ShowClassUser>, Error> {
    let origins = segments::origins(server, server.catalog.connect()?)?;
    let wanted = serde_json::Value::from(versions.iter().cloned().collect::<Vec<_>>()).to_string();
    let mut candidates = BTreeSet::new();
    for conn in &origins {
        candidates.extend(rows(
            conn,
            "SELECT DISTINCT v.pid FROM json_each(?1) x
                 JOIN class_version c ON c.pid = x.value
                 JOIN segment s ON s.class_version_id = c.id
                 JOIN variable v ON v.id = s.variable_id",
            [&wanted],
            |row| row.get::<_, String>(0),
        )?);
    }
    candidates.retain(|fqid| server.catalog.serves("variable", fqid));
    let names = refs::names(server, candidates.iter().map(String::as_str))?;
    // simplify: one segments list per candidate; batch by variable if a classification
    // reaches hundreds of served users and show times it.
    let mut users = Vec::new();
    for fqid in candidates {
        let mut segments = segments::all(server, &origins, &fqid)?;
        segments.retain(|s| s.classification().is_some_and(|c| versions.contains(c)));
        if !segments.is_empty() {
            users.push(ShowClassUser {
                name: names.of(refs::Named::Variable, &fqid)?,
                fqid,
                segments,
            });
        }
    }
    Ok(users)
}

/// A table's row keys. By table pid: in steward mode the holdings origin adds the key
/// memberships the steward attests on a reference table.
fn row_keys(server: &Server, conn: &Connection, pid: &str) -> Result<Vec<ShowRowKey>, Error> {
    let keys = |conn: &Connection| {
        rows(
            conn,
            "SELECT v.pid, sp.exact, s.start, s.end
                 FROM segment s JOIN variable v ON v.id = s.variable_id
                   JOIN spelling sp ON sp.id = s.spelling_id
                   JOIN tbl t ON t.id = s.table_id
                 WHERE t.pid = ? AND s.role = 'row_key'",
            [pid],
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
    let extra = server.catalog.connect_holdings()?;
    if let Some(extra) = &extra {
        found.extend(keys(extra)?);
    }
    // Steward mode: a key is shown over the years its placements in this table hold,
    // as `schema` clips a line; a key no placement holds here is not shown. Duplicates
    // go after the clip: two origins' segments of one key can clip to the same years.
    let mut held = extra.as_ref().map(HeldClip::new).transpose()?;
    let mut clipped = Vec::with_capacity(found.len());
    for (start, fqid, spelling, end) in found {
        let days = match held.as_mut() {
            Some(held) => held.clip(pid, &fqid, (start, end))?,
            None => Some((start, end)),
        };
        if let Some(days) = days {
            clipped.push((days.0, fqid, spelling, days.1));
        }
    }
    clipped.sort();
    clipped.dedup();
    let row_keys = clipped
        .into_iter()
        .map(|(start, fqid, spelling, end)| ShowRowKey {
            fqid,
            spelling,
            period: server.period(start, end),
        })
        .collect();
    Ok(row_keys)
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
        sets: memberships(conn, entity.id, "set")?
            .into_iter()
            .filter(|set| server.catalog.serves("set", &set.fqid))
            .collect(),
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
/// mode only the held members, each over its held columns and years.
fn set_members(
    server: &Server,
    conn: &Connection,
    entity: &Entity,
) -> Result<Vec<(i64, ShowSetMember)>, Error> {
    let mut texts = conn.prepare(
        "SELECT l.body, d.body FROM variable v
         JOIN text l ON l.id = v.label_text
         LEFT JOIN text d ON d.id = v.description_text
         WHERE v.id = ?",
    )?;
    let mut members = refs::set_members(server, conn, entity)?
        .into_iter()
        .map(|(id, fqid)| {
            let (name, description) =
                texts.query_row([id], |row| Ok((row.get(0)?, row.get(1)?)))?;
            Ok((
                id,
                ShowSetMember {
                    fqid,
                    name,
                    description,
                    segments: vec![],
                    facets: vec![],
                },
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let mut facets = conn.prepare(
        "SELECT f.axis, f.value, l.body FROM var_set_member_facet f
         JOIN var_set_axis a ON a.set_id = f.set_id AND a.axis = f.axis
         JOIN text l ON l.id = f.label_text
         WHERE f.set_id = ? AND f.variable_id = ? ORDER BY a.ord",
    )?;
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
