//! `search` (`operations.toml`): its parameters, cursor paging and the variable arm,
//! ported from today's `reg_meta.queries.search` with `field="description"`,
//! `type="variable"` and concept-group folding (the webapp's variable group). The
//! other arms and the untyped ranking are package 3a.6; until then an untyped search
//! is the variable arm and another `type` has no items.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use reg_core::{Fqid, Period, fold_search, fts_match_query, fts_terms};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, Row, params_from_iter};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use super::{Params, Server};
use crate::held::{self, Narrow};
use crate::{Code, Error, Scope};

pub const TYPES: &[&str] = &[
    "register",
    "variable",
    "classification",
    "classification_code",
    "register_value",
];
const MAX_QUERY_CHARS: usize = 200;
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;
/// Paging stops at this depth (today's `_MAX_CURSOR_POSITION`).
const DEPTH: usize = 1000;
/// Each arm's bounded candidate prefix: one row past the depth.
const HORIZON: usize = DEPTH + 1;
/// Prefix and group-label promotion apply only while at most this many candidates
/// match by identity (today's `_MAX_IDENTITY_PROMOTION_MATCHES`).
const MAX_PROMOTED: usize = 50;
const EXACT: i64 = 1000;

/// `Page<SearchHit>`.
#[derive(Serialize, ToSchema)]
pub struct SearchPage {
    items: Vec<SearchHit>,
    next_cursor: Option<String>,
}

/// `shape.SearchHit`, of the types the variable arm returns.
#[derive(Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SearchHit {
    Variable {
        fqid: Option<String>,
        name: Option<String>,
        register_name: Option<String>,
        definition: Option<String>,
        operational_definition: Option<String>,
        delivery_column_names: Vec<String>,
    },
    Group {
        kind: String,
        key: String,
        label: String,
        register_name: Option<String>,
        matched_count: usize,
        members: Vec<GroupMember>,
    },
}

#[derive(Serialize, ToSchema)]
pub struct GroupMember {
    fqid: String,
    name: Option<String>,
    delivery_column: Option<String>,
}

/// A candidate before paging: a variable hit or a folded concept group.
enum Hit {
    Variable(VariableHit),
    Group(GroupHit),
}

struct VariableHit {
    variable_id: i64,
    register_id: i64,
    fqid: Option<String>,
    name: Option<String>,
    register_name: Option<String>,
    definition: Option<String>,
    description: Option<String>,
    operational_definition: Option<String>,
    rank: f64,
    /// The in-scope delivery columns the identity score reads.
    columns: Vec<String>,
}

struct GroupHit {
    kind: String,
    key: String,
    label: String,
    register_id: Option<i64>,
    register_name: Option<String>,
    members: Vec<Member>,
    matched_count: usize,
    label_matched: bool,
    rank: f64,
}

struct Member {
    fqid: String,
    name: Option<String>,
    delivery_column: Option<String>,
    /// `(value, label)` per axis, in axis order.
    facets: Vec<(String, String)>,
}

/// A `concept_group` row.
struct Group {
    id: i64,
    kind: String,
    key: String,
    label: String,
    register_id: Option<i64>,
    register_name: Option<String>,
}

impl Group {
    fn read(row: &Row, at: usize) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(at)?,
            kind: row.get(at + 1)?,
            key: row.get(at + 2)?,
            label: row.get(at + 3)?,
            register_id: row.get(at + 4)?,
            register_name: row.get(at + 5)?,
        })
    }
}

const GROUP_COLUMNS: &str = "g.group_id, g.kind, g.group_key, g.label, g.register_id, r.name";

/// The request, validated.
struct Request<'a> {
    q: &'a str,
    variables: bool,
    limit: usize,
    years: Option<(u16, u16)>,
    register: Option<i64>,
    /// Where the page starts and the identity of the row before it.
    after: Option<(usize, String)>,
}

pub fn search(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let catalog = &server.catalog;
    let q = params["q"];
    if q.contains('\0') || q.chars().count() > MAX_QUERY_CHARS {
        return Err(Error::invalid_parameter("q"));
    }
    let ty = params.get("type").copied();
    if ty.is_some_and(|t| !TYPES.contains(&t)) {
        return Err(Error::invalid_parameter("type"));
    }
    let limit = match params.get("limit") {
        None => DEFAULT_LIMIT,
        Some(v) => v
            .parse()
            .ok()
            .filter(|n| (1..=MAX_LIMIT).contains(n))
            .ok_or_else(|| Error::invalid_parameter("limit"))?,
    };
    let period = params
        .get("period")
        .map(|p| {
            p.parse::<Period>().map_err(|err| {
                Error::new(
                    Code::InvalidPeriod,
                    format!("Invalid period {p:?}: {err}."),
                    vec!["period".into()],
                )
            })
        })
        .transpose()?;
    let conn = catalog.connect()?;
    let register = params
        .get("register")
        .map(|r| resolve_register(&conn, scope, r))
        .transpose()?;
    // The cursor binds what selects and orders the rows, never `limit`.
    let context = json!([
        reg_core::normalized_search_query(q),
        ty,
        params.get("register"),
        period.map(|p| p.to_string()),
        scope,
    ]);
    let context = hex(&Sha256::digest(context.to_string().as_bytes()));
    let after = params
        .get("cursor")
        .map(|c| decode_cursor(c, catalog.generation(), &context))
        .transpose()?;
    let request = Request {
        q,
        variables: ty.is_none_or(|t| t == "variable"),
        limit,
        years: period.map(Period::years),
        register,
        after,
    };
    let page = page(&conn, scope, &request, |offset, identity| {
        encode_cursor(catalog.generation(), &context, offset, identity)
    })?;
    Ok(serde_json::to_value(page).expect("SearchPage serializes"))
}

/// Today's register lookup for `--register`, as a ref: a FQID with a `/` resolves
/// by slugs; a one-segment ref is a bare name, matched on `fold_identity`. Either
/// must name a register in scope.
fn resolve_register(conn: &Connection, scope: Scope, value: &str) -> Result<i64, Error> {
    let in_scope = held::register(scope, "r.register_id");
    let not_found = || {
        Error::new(
            Code::NotFound,
            format!("No register {value:?} in this scope."),
            vec![value.into()],
        )
    };
    if value.contains('/') {
        let Ok(Fqid::Register { provider, register }) = value.parse() else {
            return Err(Error::new(
                Code::InvalidRef,
                format!("{value:?} is not a register FQID or name."),
                vec![value.into()],
            ));
        };
        let sql = format!(
            "SELECT r.register_id FROM register r JOIN provider p USING(provider_id) \
             WHERE p.slug = ? AND r.slug = ? AND {in_scope}"
        );
        let id: Option<i64> = conn
            .query_row(&sql, [provider, register], |row| row.get(0))
            .map(Some)
            .or_else(|err| match err {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                err => Err(err),
            })?;
        return id.ok_or_else(not_found);
    }
    let sql = format!(
        "SELECT r.register_id, p.slug, r.slug, r.name FROM register r \
         JOIN provider p USING(provider_id) \
         WHERE fold_identity(r.name) = fold_identity(?) AND {in_scope} ORDER BY r.register_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let found: Vec<(i64, Option<String>, String)> = stmt
        .query_map([value], |row| {
            let fqid = fqid(&[row.get(1)?, row.get(2)?]);
            Ok((row.get(0)?, fqid, row.get(3)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    match found.as_slice() {
        [] => Err(not_found()),
        [(id, ..)] => Ok(*id),
        _ => Err(Error::new(
            Code::AmbiguousRef,
            format!("{} registers are named {value:?}.", found.len()),
            vec![
                value.into(),
                found
                    .iter()
                    .map(|(_, fqid, name)| json!({"fqid": fqid, "kind": "register", "name": name}))
                    .collect(),
            ],
        )),
    }
}

/// The page the request names: candidates in their published order, cut at the
/// cursor and `limit`, with delivery-column chips on the shown variables.
fn page(
    conn: &Connection,
    scope: Scope,
    request: &Request,
    cursor: impl Fn(usize, &str) -> String,
) -> Result<SearchPage, Error> {
    let fts = fts_match_query(&fold_search(request.q));
    let mut hits = match fts {
        Some(fts) if request.variables => variable_arm(conn, scope, request, &fts)?,
        _ => Vec::new(),
    };
    hits = rank(request.q, hits);
    let offset = match &request.after {
        None => 0,
        Some((offset, after)) => {
            if *offset > hits.len() || (*offset > 0 && identity(&hits[offset - 1]) != *after) {
                return Err(invalid_cursor(
                    "Search cursor no longer matches the result ordering.",
                ));
            }
            *offset
        }
    };
    let end = (offset + request.limit).min(DEPTH);
    let more = end < DEPTH && hits.len() > end;
    let shown: Vec<Hit> = hits.drain(offset..end.min(hits.len())).collect();
    let next_cursor = match shown.last() {
        Some(last) if more => Some(cursor(offset + shown.len(), &identity(last))),
        _ => None,
    };
    let terms = fts_terms(request.q);
    let items = shown
        .into_iter()
        .map(|hit| match hit {
            Hit::Variable(v) => {
                let public = [
                    &v.name,
                    &v.definition,
                    &v.description,
                    &v.operational_definition,
                ];
                let matched = matched_columns(&v.columns, &terms, &public);
                SearchHit::Variable {
                    fqid: v.fqid,
                    name: v.name,
                    register_name: v.register_name,
                    definition: v.definition,
                    operational_definition: v.operational_definition,
                    // The columns that satisfied the query, else all of them.
                    delivery_column_names: if matched.is_empty() {
                        v.columns
                    } else {
                        matched
                    },
                }
            }
            Hit::Group(g) => SearchHit::Group {
                kind: g.kind,
                key: g.key,
                label: g.label,
                register_name: g.register_name,
                matched_count: g.matched_count,
                members: g
                    .members
                    .into_iter()
                    .map(|m| GroupMember {
                        fqid: m.fqid,
                        name: m.name,
                        delivery_column: m.delivery_column,
                    })
                    .collect(),
            },
        })
        .collect();
    Ok(SearchPage { items, next_cursor })
}

/// Variable hits and the concept groups they fold into (today's
/// `_search_description_variables`, `_search_group_labels` and
/// `_fold_concept_groups`), each arm bounded at the horizon after its filters.
fn variable_arm(
    conn: &Connection,
    scope: Scope,
    request: &Request,
    fts: &str,
) -> Result<Vec<Hit>, Error> {
    let folded = fold_search(request.q);
    let years = request.years;
    let alias_held = |alias: &str| {
        let (variant, column) = (
            format!("{alias}.register_variant_id"),
            format!("{alias}.delivery_column_name"),
        );
        held::variable(
            scope,
            &format!("{alias}.variable_id"),
            Narrow {
                years,
                variant: Some(&variant),
                column: Some(&column),
            },
        )
    };
    let mut args: Vec<Sql> = vec![fts.to_owned().into(), folded.into()];
    let mut filters = String::new();
    if let Some(register) = request.register {
        filters += " AND vf.register_id = ?";
        args.push(register.into());
    }
    if let Some((lo, hi)) = years {
        // Today's `_year_scope_filter`: some state of the variable overlaps.
        filters += " AND EXISTS (SELECT 1 FROM variable_state vs WHERE vs.variable_id = vf.rowid \
             AND CAST(substr(vs.valid_from, 1, 4) AS INTEGER) <= ? \
             AND CAST(substr(vs.valid_to, 1, 4) AS INTEGER) >= ?)";
        args.extend([i64::from(hi).into(), i64::from(lo).into()]);
    }
    if scope == Scope::Holdings {
        // Each term must match the variable's public text or a held alias.
        for term in fts_terms(request.q) {
            write!(
                filters,
                " AND (vf.rowid IN (SELECT rowid FROM variable_fts WHERE variable_fts MATCH ?) \
                 OR EXISTS (SELECT 1 FROM variable_alias ea WHERE ea.variable_id = v.variable_id \
                 AND fts_term(ea.delivery_column_name, ?) AND {}))",
                alias_held("ea")
            )
            .expect("write to String");
            args.push(
                format!(
                    "{{name definition description operational_definition}}: \"{}\"*",
                    term.replace('"', "\"\"")
                )
                .into(),
            );
            args.push(term.into());
        }
    }
    // Exact-name admission (#1180): variables whose name or held delivery column
    // folds to the query are ordered ahead of the bound.
    let sql = format!(
        "WITH exact(variable_id) AS (SELECT v.variable_id FROM variable_fts vf \
         JOIN variable v ON v.variable_id = vf.rowid WHERE variable_fts MATCH ?1 \
         AND (fold_search(v.name) = ?2 OR EXISTS (SELECT 1 FROM variable_alias va \
         WHERE va.variable_id = v.variable_id AND fold_search(va.delivery_column_name) = ?2 \
         AND {exact_held})) ORDER BY v.variable_id LIMIT {HORIZON}) \
         SELECT vf.register_id, vf.rowid, vt.name, vt.definition, vt.description, \
         vt.operational_definition, bm25(variable_fts, 0.2, 0.2, 6.0, 4.0, 2.0, 1.0, 0.4), \
         r.name, p.slug, r.slug, v.slug \
         FROM variable_fts vf JOIN register r ON vf.register_id = r.register_id \
         JOIN provider p ON p.provider_id = r.provider_id \
         JOIN variable v ON v.variable_id = vf.rowid \
         JOIN variable_search_text vt ON vt.variable_id = vf.rowid \
         WHERE variable_fts MATCH ?1 AND {held}{filters} \
         ORDER BY vf.rowid NOT IN (SELECT variable_id FROM exact), 7, vf.rowid LIMIT {HORIZON}",
        exact_held = alias_held("va"),
        held = held::variable(
            scope,
            "v.variable_id",
            Narrow {
                years,
                ..Narrow::default()
            }
        ),
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut variables: Vec<VariableHit> = stmt
        .query_map(params_from_iter(&args), |row| {
            Ok(VariableHit {
                register_id: row.get(0)?,
                variable_id: row.get(1)?,
                name: row.get(2)?,
                definition: row.get(3)?,
                description: row.get(4)?,
                operational_definition: row.get(5)?,
                rank: row.get(6)?,
                register_name: row.get(7)?,
                fqid: fqid(&[row.get(8)?, row.get(9)?, row.get(10)?]),
                columns: Vec::new(),
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    let ids: Vec<i64> = variables.iter().map(|v| v.variable_id).collect();
    let mut columns = delivery_columns(conn, scope, years, &ids)?;
    for v in &mut variables {
        v.columns = columns.remove(&v.variable_id).unwrap_or_default();
    }
    let labels = label_hits(conn, scope, request)?;
    fold(conn, scope, variables, &labels)
}

/// Variable concept groups whose label (folded by `fold_identity`) or key contains
/// the raw query, with a member in scope (and, under `period`, a member state
/// overlapping it).
fn label_hits(conn: &Connection, scope: Scope, request: &Request) -> Result<Vec<Group>, Error> {
    let mut pattern = String::from("%");
    for c in request.q.chars() {
        if matches!(c, '\\' | '%' | '_') {
            pattern.push('\\');
        }
        pattern.push(c);
    }
    pattern.push('%');
    let mut args: Vec<Sql> = vec![pattern.into()];
    let mut filters = String::new();
    if let Some(register) = request.register {
        filters += " AND g.register_id = ?";
        args.push(register.into());
    }
    if let Some((lo, hi)) = request.years {
        filters += " AND EXISTS (SELECT 1 FROM concept_group_variable cgv \
             JOIN variable_state vs ON vs.variable_id = cgv.variable_id \
             WHERE cgv.group_id = g.group_id \
             AND CAST(substr(vs.valid_from, 1, 4) AS INTEGER) <= ? \
             AND CAST(substr(vs.valid_to, 1, 4) AS INTEGER) >= ?)";
        args.extend([i64::from(hi).into(), i64::from(lo).into()]);
    }
    let held = held::variable(
        scope,
        "gv.variable_id",
        Narrow {
            years: request.years,
            ..Narrow::default()
        },
    );
    let sql = format!(
        "SELECT {GROUP_COLUMNS} FROM concept_group g \
         LEFT JOIN register r ON r.register_id = g.register_id \
         WHERE (fold_identity(g.label) LIKE fold_identity(?1) ESCAPE '\\' \
         OR g.group_key LIKE ?1 ESCAPE '\\') AND g.kind = 'variable' \
         AND EXISTS (SELECT 1 FROM concept_group_variable gm JOIN variable gv USING(variable_id) \
         WHERE gm.group_id = g.group_id AND {held}){filters} \
         ORDER BY g.kind, g.group_key, g.group_id LIMIT {HORIZON}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(&args), |row| Group::read(row, 0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}

/// Collapse sibling hits under their concept group: a group replaces its member
/// hits where two distinct members matched or its label did; a lone member hit stays
/// a variable. Label-only groups follow, in label order.
fn fold(
    conn: &Connection,
    scope: Scope,
    variables: Vec<VariableHit>,
    labels: &[Group],
) -> Result<Vec<Hit>, Error> {
    let ids: Vec<i64> = variables.iter().map(|v| v.variable_id).collect();
    let mut stmt = conn.prepare(&format!(
        "SELECT cgv.variable_id, {GROUP_COLUMNS} FROM concept_group_variable cgv \
         JOIN concept_group g ON g.group_id = cgv.group_id \
         LEFT JOIN register r ON r.register_id = g.register_id \
         WHERE cgv.variable_id IN (SELECT value FROM json_each(?))"
    ))?;
    // A variable in several groups keys on the last one read, as today.
    let membership: BTreeMap<i64, Group> = stmt
        .query_map([json!(ids).to_string()], |row| {
            Ok((row.get(0)?, Group::read(row, 1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut matched: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
    for v in &variables {
        if let Some(group) = membership.get(&v.variable_id) {
            matched.entry(group.id).or_default().insert(v.variable_id);
        }
    }
    let label_ids: BTreeSet<i64> = labels.iter().map(|g| g.id).collect();
    let folded =
        |id: i64| label_ids.contains(&id) || matched.get(&id).is_some_and(|m| m.len() >= 2);
    let mut ranks: BTreeMap<i64, f64> = BTreeMap::new();
    for v in &variables {
        if let Some(group) = membership.get(&v.variable_id) {
            let rank = ranks.entry(group.id).or_insert(v.rank);
            *rank = rank.min(v.rank);
        }
    }
    let mut out = Vec::new();
    let mut emitted = BTreeSet::new();
    for v in variables {
        match membership.get(&v.variable_id) {
            Some(group) if folded(group.id) => {
                if emitted.insert(group.id) {
                    out.push(group_hit(conn, scope, group, &matched, &label_ids, &ranks)?);
                }
            }
            _ => out.push(Hit::Variable(v)),
        }
    }
    for group in labels {
        if emitted.insert(group.id) {
            out.push(group_hit(conn, scope, group, &matched, &label_ids, &ranks)?);
        }
    }
    Ok(out)
}

fn group_hit(
    conn: &Connection,
    scope: Scope,
    group: &Group,
    matched: &BTreeMap<i64, BTreeSet<i64>>,
    labels: &BTreeSet<i64>,
    ranks: &BTreeMap<i64, f64>,
) -> Result<Hit, Error> {
    Ok(Hit::Group(GroupHit {
        kind: group.kind.clone(),
        key: group.key.clone(),
        label: group.label.clone(),
        register_id: group.register_id,
        register_name: group.register_name.clone(),
        members: members(conn, scope, group.id)?,
        matched_count: matched.get(&group.id).map_or(0, BTreeSet::len),
        label_matched: labels.contains(&group.id),
        rank: ranks.get(&group.id).copied().unwrap_or(0.0),
    }))
}

/// A group's members in scope (today's `Catalog.list_concept_groups`), ordered by
/// first facet value, FQID and delivery column.
fn members(conn: &Connection, scope: Scope, group: i64) -> Result<Vec<Member>, Error> {
    let member_held = match scope {
        Scope::Reference => "1".to_owned(),
        Scope::Holdings => format!(
            "{} AND (m.delivery_column_name IS NULL OR EXISTS (SELECT 1 FROM register_variant gv \
             WHERE gv.register_id = v.register_id AND {}))",
            held::variable(scope, "m.variable_id", Narrow::default()),
            held::variable(
                scope,
                "m.variable_id",
                Narrow {
                    variant: Some("gv.register_variant_id"),
                    column: Some("m.delivery_column_name"),
                    ..Narrow::default()
                }
            )
        ),
    };
    let sql = format!(
        "SELECT m.member_id, p.slug, r.slug, v.slug, v.name, m.delivery_column_name, \
         f.value, f.label FROM concept_group g \
         JOIN register r ON g.register_id = r.register_id \
         JOIN provider p ON r.provider_id = p.provider_id \
         JOIN concept_group_variable m ON m.group_id = g.group_id \
         JOIN variable v ON v.variable_id = m.variable_id \
         LEFT JOIN concept_group_variable_facet f ON f.member_id = m.member_id \
         LEFT JOIN concept_group_axis a ON a.group_id = g.group_id AND a.axis = f.axis \
         WHERE g.group_id = ? AND g.kind = 'variable' AND r.slug IS NOT NULL \
         AND v.slug IS NOT NULL AND {member_held} ORDER BY m.member_id, a.ordinal"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([group])?;
    let mut by_id: BTreeMap<i64, Member> = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let member = match by_id.entry(row.get(0)?) {
            std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::btree_map::Entry::Vacant(e) => {
                let (p, r, v): (String, String, String) = (row.get(1)?, row.get(2)?, row.get(3)?);
                e.insert(Member {
                    fqid: format!("{p}/{r}/{v}"),
                    name: row.get(4)?,
                    delivery_column: row.get(5)?,
                    facets: Vec::new(),
                })
            }
        };
        if let (Some(value), Some(label)) = (row.get(6)?, row.get(7)?) {
            member.facets.push((value, label));
        }
    }
    let mut members: Vec<Member> = by_id.into_values().collect();
    members.sort_by(|a, b| {
        let first = |m: &Member| m.facets.first().map_or(String::new(), |f| f.0.clone());
        (
            first(a),
            &a.fqid,
            a.delivery_column.as_deref().unwrap_or(""),
        )
            .cmp(&(
                first(b),
                &b.fqid,
                b.delivery_column.as_deref().unwrap_or(""),
            ))
    });
    Ok(members)
}

/// Each variable's distinct delivery columns in scope, sorted: canonical spellings
/// in holdings (narrowed to `years`), as delivered in reference.
fn delivery_columns(
    conn: &Connection,
    scope: Scope,
    years: Option<(u16, u16)>,
    ids: &[i64],
) -> Result<BTreeMap<i64, Vec<String>>, Error> {
    let sql = format!(
        "SELECT DISTINCT va.variable_id, {} AS delivery_column_name FROM variable_alias va \
         WHERE va.variable_id IN (SELECT value FROM json_each(?)) AND {} \
         ORDER BY va.variable_id, delivery_column_name",
        held::shown_column(scope, "va"),
        held::variable(
            scope,
            "va.variable_id",
            Narrow {
                years,
                variant: Some("va.register_variant_id"),
                column: Some("va.delivery_column_name"),
            }
        ),
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([json!(ids).to_string()])?;
    let mut out: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    while let Some(row) = rows.next()? {
        out.entry(row.get(0)?).or_default().push(row.get(1)?);
    }
    Ok(out)
}

/// Today's `_matched_delivery_column_names`: for the terms no public text matches,
/// the columns that match some of them and whose matched terms are not a strict
/// subset of another column's.
fn matched_columns(
    columns: &[String],
    terms: &[String],
    public: &[&Option<String>],
) -> Vec<String> {
    let matches = |text: &str, term: &str| fts_terms(text).iter().any(|t| t.starts_with(term));
    let required: BTreeSet<&str> = terms
        .iter()
        .map(String::as_str)
        .filter(|term| {
            !public
                .iter()
                .any(|t| t.as_deref().is_some_and(|t| matches(t, term)))
        })
        .collect();
    let sets: Vec<(&String, BTreeSet<&str>)> = columns
        .iter()
        .map(|c| {
            (
                c,
                required.iter().copied().filter(|t| matches(c, t)).collect(),
            )
        })
        .filter(|(_, set): &(_, BTreeSet<&str>)| !set.is_empty())
        .collect();
    sets.iter()
        .filter(|(_, set)| {
            !sets
                .iter()
                .any(|(_, other)| set.is_subset(other) && set != other)
        })
        .map(|(c, _)| (*c).clone())
        .collect()
}

/// Sort into the published order: display score descending, then bm25, then the
/// identity string (today's `_search_display_score` sort).
fn rank(q: &str, hits: Vec<Hit>) -> Vec<Hit> {
    let folded = fold_search(q);
    let scores: Vec<i64> = hits.iter().map(|h| identity_score(&folded, h)).collect();
    let promote = scores.iter().filter(|&&s| s > 0).count() <= MAX_PROMOTED;
    let mut keyed: Vec<(i64, f64, String, Hit)> = hits
        .into_iter()
        .zip(scores)
        .map(|(hit, score)| {
            let display = if promote {
                score + authority(&hit)
            } else if score >= EXACT {
                score
            } else {
                0
            };
            let rank = match &hit {
                Hit::Variable(v) => v.rank,
                Hit::Group(g) => g.rank,
            };
            (-display, rank, identity(&hit), hit)
        })
        .collect();
    keyed.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.total_cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
    });
    keyed.into_iter().map(|k| k.3).collect()
}

/// Today's `_search_identity_score`: 1000 when an identity text folds to the
/// query, else 100 when one starts with it.
fn identity_score(folded: &str, hit: &Hit) -> i64 {
    let mut texts: Vec<&str> = Vec::new();
    match hit {
        Hit::Variable(v) => {
            if let Some(fqid) = &v.fqid {
                texts.extend([fqid.as_str(), leaf(fqid)]);
            }
            texts.extend(v.name.as_deref());
            texts.extend(v.columns.iter().map(String::as_str));
        }
        Hit::Group(g) => {
            texts.extend([g.key.as_str(), g.label.as_str()]);
            for m in &g.members {
                texts.push(&m.fqid);
                texts.extend(m.name.as_deref());
                texts.extend(m.delivery_column.as_deref());
                for (value, label) in &m.facets {
                    texts.extend([value.as_str(), label.as_str()]);
                }
            }
        }
    }
    let texts: Vec<String> = texts.into_iter().map(fold_search).collect();
    if folded.is_empty() {
        0
    } else if texts.iter().any(|t| t == folded) {
        EXACT
    } else if texts.iter().any(|t| t.starts_with(folded)) {
        100
    } else {
        0
    }
}

/// Today's group authority bonus: a label-matched group gains 50 plus its matched
/// members, at most 50.
fn authority(hit: &Hit) -> i64 {
    match hit {
        Hit::Group(g) if g.label_matched => {
            50 + i64::try_from(g.matched_count.min(50)).expect("at most 50")
        }
        _ => 0,
    }
}

/// The last segment of a FQID.
fn leaf(fqid: &str) -> &str {
    fqid.rsplit('/').next().unwrap_or(fqid)
}

/// The deterministic final tie-breaker, and what a cursor records of the row
/// before the page (today's `_search_result_identity`).
fn identity(hit: &Hit) -> String {
    match hit {
        Hit::Variable(VariableHit {
            fqid: Some(fqid), ..
        }) => format!("variable:{fqid}"),
        Hit::Variable(v) => format!(
            "variable:{}:{}:::{}",
            v.register_id,
            v.variable_id,
            v.name.as_deref().unwrap_or("None")
        ),
        Hit::Group(g) => format!(
            "group:{}:{}:{}",
            g.kind,
            g.register_id.map_or("None".to_owned(), |id| id.to_string()),
            g.key
        ),
    }
}

/// A FQID from its slugs, or None when one is missing or not a slug (today's
/// `try_emit`).
fn fqid(slugs: &[Option<String>]) -> Option<String> {
    let joined = slugs
        .iter()
        .map(|s| s.as_deref().filter(|s| !s.is_empty()))
        .collect::<Option<Vec<_>>>()?
        .join("/");
    joined.parse::<Fqid>().ok().map(|_| joined)
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        write!(out, "{byte:02x}").expect("write to String");
    }
    out
}

fn invalid_cursor(message: &str) -> Error {
    Error::new(Code::InvalidCursor, message, vec![])
}

/// A cursor is the hex of `generation.context.offset.identity`: the generation it
/// was issued on, the request it continues (`context`), where the next page starts
/// and the identity of the row before it.
fn encode_cursor(generation: &str, context: &str, offset: usize, identity: &str) -> String {
    hex(format!("{generation}.{context}.{offset}.{identity}").as_bytes())
}

/// The cursor's offset and identity.
///
/// # Errors
///
/// `stale_cursor` for another generation's cursor; `invalid_cursor` for one that
/// does not decode, continues another request or passes the depth.
fn decode_cursor(cursor: &str, generation: &str, context: &str) -> Result<(usize, String), Error> {
    let malformed = || invalid_cursor("Search cursor is malformed.");
    let bytes = (0..cursor.len())
        .step_by(2)
        .map(|i| {
            cursor
                .get(i..i + 2)
                .and_then(|b| u8::from_str_radix(b, 16).ok())
        })
        .collect::<Option<Vec<u8>>>()
        .ok_or_else(malformed)?;
    let text = String::from_utf8(bytes).map_err(|_| malformed())?;
    let mut parts = text.splitn(4, '.');
    let (Some(issued), Some(issued_for), Some(offset), Some(after)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(malformed());
    };
    if issued != generation {
        return Err(Error::new(
            Code::StaleCursor,
            "The catalog changed since this cursor was issued.",
            vec![generation.into()],
        ));
    }
    if issued_for != context {
        return Err(invalid_cursor(
            "Search cursor was issued for other parameters or another scope.",
        ));
    }
    let offset = offset
        .parse()
        .ok()
        .filter(|o| (1..=DEPTH).contains(o))
        .ok_or_else(malformed)?;
    Ok((offset, after.to_owned()))
}
