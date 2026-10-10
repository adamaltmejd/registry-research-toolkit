//! `search` (`operations.toml`): its parameters, cursor paging and five arms, ported
//! from the retired Python reader's search (`field="description"` for the register,
//! variable and classification arms; `field="value"` split by code owner for the code
//! arms) and the webapp's per-type groups, pins and best-bets ranking
//! (`reg_webapp/routes/search.py`). With `type`, one arm's list: its pins, then its
//! rows in today's order. Without, one ranked list of every arm's first rows.

mod classification;
mod code;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use reg_core::{Period, fold_search, fts_match_query, fts_terms, py_isdecimal, py_strip};
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, Row, params_from_iter};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use super::refs::{self, fqid};
use super::{Params, Server, cursor};
use crate::held::{self, Narrow};
use crate::{Error, Scope, hex};
use classification::{ClassificationHit, Edition, SuccessionHit};
use code::{CodeClassification, CodeHit, CodeVariable};

/// The `type` values, in arm order (an untyped search's tie-break).
pub const TYPES: &[&str] = &[
    "register",
    "variable",
    "classification",
    "classification_code",
    "register_value",
];
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

/// `shape.SearchHit`.
#[derive(Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SearchHit {
    Register {
        fqid: Option<String>,
        name: Option<String>,
        purpose: Option<String>,
    },
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
    Classification {
        fqid: Option<String>,
        short_name: Option<String>,
        name: Option<String>,
        terminal_fqid: Option<String>,
    },
    ClassificationSuccession {
        fqid: Option<String>,
        short_name: Option<String>,
        name: Option<String>,
        matched_count: usize,
        editions: Vec<Edition>,
    },
    Code {
        code: String,
        label: String,
        code_system: Option<String>,
        variable_count: i64,
        variables: Vec<CodeVariable>,
        classification_count: i64,
        classifications: Vec<CodeClassification>,
    },
}

#[derive(Serialize, ToSchema)]
pub struct GroupMember {
    fqid: String,
    name: Option<String>,
    delivery_column: Option<String>,
}

/// A candidate before paging.
enum Hit {
    Register(RegisterHit),
    Variable(VariableHit),
    Group(GroupHit),
    Classification(ClassificationHit),
    Succession(SuccessionHit),
    Code(CodeHit),
}

impl Hit {
    /// The id a concept group lists this hit by: a variable's, or a classification's
    /// (a succession row's terminal edition).
    fn member_id(&self) -> Option<i64> {
        match self {
            Self::Variable(v) => Some(v.variable_id),
            Self::Classification(c) => Some(c.id),
            Self::Succession(s) => s.id,
            _ => None,
        }
    }

    /// bm25 or the arm's own rank: smaller sorts first.
    fn rank(&self) -> f64 {
        match self {
            Self::Register(r) => r.rank,
            Self::Variable(v) => v.rank,
            Self::Group(g) => g.rank,
            Self::Classification(c) => c.rank,
            Self::Succession(s) => s.rank,
            Self::Code(c) => c.rank,
        }
    }
}

struct RegisterHit {
    register_id: i64,
    fqid: Option<String>,
    name: Option<String>,
    purpose: Option<String>,
    rank: f64,
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
    ty: Option<&'a str>,
    limit: usize,
    years: Option<(u16, u16)>,
    register: Option<i64>,
    /// Where the page starts and the position of the row before it: its identity
    /// with `type`, its untyped sort key (JSON) without.
    after: Option<(usize, String)>,
}

/// An untyped row's place in the ranked list: pins first in pin order (`0`, 0, arm,
/// position), then the rest (`1`, -score, arm, position). Arm and position are
/// unique, so no further tie-break can decide.
type Key = (u8, i64, usize, usize);

pub fn search(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let catalog = &server.catalog;
    let q = super::q(params)?;
    let ty = params.get("type").copied();
    if ty.is_some_and(|t| !TYPES.contains(&t)) {
        return Err(Error::invalid_parameter("type"));
    }
    let limit = super::limit(params)?;
    let period = super::period(params, "period")?;
    let conn = catalog.connect()?;
    let register = params
        .get("register")
        .map(|r| refs::register(&conn, scope, r).map(|register| register.id))
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
        .map(|c| cursor::decode(c, catalog.generation(), &context, DEPTH))
        .transpose()?;
    let request = Request {
        q,
        ty,
        limit,
        years: period.map(Period::years),
        register,
        after,
    };
    let page = page(&conn, scope, &request, |offset, position| {
        cursor::encode(catalog.generation(), &context, offset, position)
    })?;
    Ok(serde_json::to_value(page).expect("SearchPage serializes"))
}

/// The page the request names: the typed list or the untyped ranking, cut at the
/// cursor and `limit`, with code owners on the shown codes.
fn page(
    conn: &Connection,
    scope: Scope,
    request: &Request,
    cursor: impl Fn(usize, &str) -> String,
) -> Result<SearchPage, Error> {
    let terms = fts_terms(request.q);
    let (mut hits, keys) = match (fts_match_query(&fold_search(request.q)), request.ty) {
        (None, _) => (Vec::new(), Vec::new()),
        (Some(fts), Some(ty)) => (arm(conn, scope, request, ty, &fts)?.0, Vec::new()),
        (Some(fts), None) => untyped(conn, scope, request, &fts, &terms)?
            .into_iter()
            .map(|(key, hit)| (hit, key))
            .unzip(),
    };
    let offset = match (&request.after, request.ty) {
        (None, _) => 0,
        (Some((offset, after)), Some(_)) => {
            if *offset > hits.len() || (*offset > 0 && identity(&hits[offset - 1]) != *after) {
                return Err(cursor::invalid(
                    "Cursor no longer matches the result ordering.",
                ));
            }
            *offset
        }
        (Some((_, after)), None) => {
            let after: Key =
                serde_json::from_str(after).map_err(|_| cursor::invalid("Cursor is malformed."))?;
            keys.partition_point(|key| *key <= after)
        }
    };
    let end = (offset + request.limit).min(DEPTH);
    let more = end < DEPTH && hits.len() > end;
    let end = end.min(hits.len()).max(offset);
    let next_cursor = (more && end > offset).then(|| {
        let position = match request.ty {
            Some(_) => identity(&hits[end - 1]),
            None => serde_json::to_string(&keys[end - 1]).expect("a key serializes"),
        };
        cursor(end, &position)
    });
    let mut shown: Vec<Hit> = hits.drain(offset..end).collect();
    code::annotate(conn, request.register, &mut shown)?;
    if matches!(request.ty, Some("classification_code" | "register_value")) {
        // Today's `_rank_codes`: owner counts reorder the shown page only, so the
        // cursor follows the arm's order.
        shown.sort_by_key(Hit::owner_rank);
    }
    let items = shown.into_iter().map(|hit| item(hit, &terms)).collect();
    Ok(SearchPage { items, next_cursor })
}

/// One arm's list for a request with a searchable token: its pins (count returned),
/// then its rows in today's order.
fn arm(
    conn: &Connection,
    scope: Scope,
    request: &Request,
    ty: &str,
    fts: &str,
) -> Result<(Vec<Hit>, usize), Error> {
    let (pins, rows) = match ty {
        "register" => (
            register_pins(conn, scope, request)?,
            register_arm(conn, scope, request, fts)?,
        ),
        "variable" => (Vec::new(), variable_arm(conn, scope, request, fts)?),
        // Classifications carry no register and no validity window.
        "classification" if request.register.is_none() && request.years.is_none() => (
            classification::pins(conn, request.q)?,
            classification::arm(conn, scope, request, fts)?,
        ),
        "classification" => (Vec::new(), Vec::new()),
        _ => (
            Vec::new(),
            code::arm(conn, request, fts, ty == "classification_code")?,
        ),
    };
    let pinned = pins.len();
    let mut list = pins;
    list.extend(rank(request.q, rows));
    Ok((list, pinned))
}

/// Every arm's first `DEPTH` rows in one stable total order (decision 17): pins
/// first in pin order, then best-bets score descending, arm order and the arm's own
/// position. Rows with today's candidate key once (the first), and variables that a
/// listed variable group holds hidden.
fn untyped(
    conn: &Connection,
    scope: Scope,
    request: &Request,
    fts: &str,
    terms: &[String],
) -> Result<Vec<(Key, Hit)>, Error> {
    let folded = fold_search(request.q);
    let mut rows = Vec::new();
    for (arm_order, ty) in TYPES.iter().enumerate() {
        let (list, pinned) = arm(conn, scope, request, ty, fts)?;
        for (position, hit) in list.into_iter().take(DEPTH).enumerate() {
            let key = if position < pinned {
                (0, 0, arm_order, position)
            } else {
                (1, -best_bet(&folded, &hit, terms), arm_order, position)
            };
            rows.push((key, hit));
        }
    }
    rows.sort_by_key(|row| row.0);
    let mut seen = BTreeSet::new();
    rows.retain(|(key, hit)| seen.insert(candidate_key(hit, key.2, key.3)));
    let grouped: BTreeSet<String> = rows
        .iter()
        .filter_map(|(_, hit)| match hit {
            Hit::Group(g) if g.kind == "variable" => Some(g.members.iter().map(|m| m.fqid.clone())),
            _ => None,
        })
        .flatten()
        .collect();
    rows.retain(|(_, hit)| {
        !matches!(hit, Hit::Variable(VariableHit { fqid: Some(f), .. }) if grouped.contains(f))
    });
    Ok(rows)
}

/// The public shape of a hit.
fn item(hit: Hit, terms: &[String]) -> SearchHit {
    match hit {
        Hit::Register(r) => SearchHit::Register {
            fqid: r.fqid,
            name: r.name,
            purpose: r.purpose,
        },
        Hit::Variable(v) => SearchHit::Variable {
            delivery_column_names: shown_columns(&v, terms),
            fqid: v.fqid,
            name: v.name,
            register_name: v.register_name,
            definition: v.definition,
            operational_definition: v.operational_definition,
        },
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
        Hit::Classification(c) => SearchHit::Classification {
            fqid: c.fqid,
            short_name: c.short_name,
            name: c.name,
            terminal_fqid: c.terminal_fqid,
        },
        Hit::Succession(s) => SearchHit::ClassificationSuccession {
            fqid: s.fqid,
            short_name: s.short_name,
            name: s.name,
            matched_count: s.matched_count,
            editions: s.editions,
        },
        Hit::Code(c) => c.into_item(),
    }
}

/// The register arm's filters on `r` beyond the query: scope (in tables of the
/// period), `register`, and a period some state of the register's variables
/// overlaps (today's register-wide `_year_scope_filter`).
fn register_filters(scope: Scope, request: &Request) -> (String, Vec<Sql>) {
    let mut sql = format!(
        " AND {}",
        held::register_in(scope, "r.register_id", request.years)
    );
    let mut args: Vec<Sql> = Vec::new();
    if let Some(register) = request.register {
        sql += " AND r.register_id = ?";
        args.push(register.into());
    }
    if let Some((lo, hi)) = request.years {
        sql += &year_filter(
            "variable_state vs JOIN variable v_year ON v_year.variable_id = vs.variable_id",
            "v_year.register_id = r.register_id",
        );
        args.extend([i64::from(hi).into(), i64::from(lo).into()]);
    }
    (sql, args)
}

fn read_register(row: &Row, rank: f64) -> rusqlite::Result<Hit> {
    Ok(Hit::Register(RegisterHit {
        register_id: row.get(0)?,
        name: row.get(1)?,
        purpose: row.get(2)?,
        fqid: fqid(&[row.get(3)?, row.get(4)?]),
        rank,
    }))
}

/// The registers pinned for the query that pass the arm's filters, in pin order.
fn register_pins(conn: &Connection, scope: Scope, request: &Request) -> Result<Vec<Hit>, Error> {
    let (filters, mut args) = register_filters(scope, request);
    args.insert(0, fold_search(request.q).into());
    let sql = format!(
        "SELECT r.register_id, r.name, r.purpose, p.slug, r.slug FROM search_pin sp \
         JOIN provider p JOIN register r ON r.provider_id = p.provider_id \
         AND sp.entity = p.slug || '/' || r.slug \
         WHERE sp.key = ? AND sp.type = 'register'{filters} ORDER BY sp.position"
    );
    let mut stmt = conn.prepare(&sql)?;
    let pins = stmt
        .query_map(params_from_iter(&args), |row| read_register(row, 0.0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(pins)
}

/// Today's `_search_description_registers`, pinned registers excluded before the
/// bound.
fn register_arm(
    conn: &Connection,
    scope: Scope,
    request: &Request,
    fts: &str,
) -> Result<Vec<Hit>, Error> {
    let (filters, filter_args) = register_filters(scope, request);
    let mut args: Vec<Sql> = vec![fts.to_owned().into(), fold_search(request.q).into()];
    args.extend(filter_args);
    let sql = format!(
        "SELECT rf.register_id, r.name, r.purpose, p.slug, r.slug, rf.rank \
         FROM register_fts rf JOIN register r ON r.register_id = rf.register_id \
         JOIN provider p ON p.provider_id = r.provider_id \
         WHERE register_fts MATCH ? AND NOT EXISTS (SELECT 1 FROM search_pin sp \
         WHERE sp.key = ? AND sp.type = 'register' AND sp.entity = p.slug || '/' || r.slug)\
         {filters} ORDER BY rf.rank, p.slug, r.slug LIMIT {HORIZON}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(&args), |row| {
            read_register(row, row.get(5)?)
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
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
        // Some state of the variable overlaps.
        filters += &year_filter("variable_state vs", "vs.variable_id = vf.rowid");
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
    // folds to the query are ordered ahead of the bound; ties break on the FQID.
    let sql = format!(
        "WITH exact(variable_id) AS (SELECT v.variable_id FROM variable_fts vf \
         JOIN variable v ON v.variable_id = vf.rowid JOIN register r USING(register_id) \
         JOIN provider p USING(provider_id) WHERE variable_fts MATCH ?1 \
         AND (fold_search(v.name) = ?2 OR EXISTS (SELECT 1 FROM variable_alias va \
         WHERE va.variable_id = v.variable_id AND fold_search(va.delivery_column_name) = ?2 \
         AND {exact_held})) ORDER BY p.slug, r.slug, v.slug LIMIT {HORIZON}) \
         SELECT vf.register_id, vf.rowid, vt.name, vt.definition, vt.description, \
         vt.operational_definition, bm25(variable_fts, 0.2, 0.2, 6.0, 4.0, 2.0, 1.0, 0.4), \
         r.name, p.slug, r.slug, v.slug \
         FROM variable_fts vf JOIN register r ON vf.register_id = r.register_id \
         JOIN provider p ON p.provider_id = r.provider_id \
         JOIN variable v ON v.variable_id = vf.rowid \
         JOIN variable_search_text vt ON vt.variable_id = vf.rowid \
         WHERE variable_fts MATCH ?1 AND {held}{filters} \
         ORDER BY vf.rowid NOT IN (SELECT variable_id FROM exact), 7, p.slug, r.slug, v.slug \
         LIMIT {HORIZON}",
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
    let hits = variables.into_iter().map(Hit::Variable).collect();
    fold(conn, scope, request, "variable", hits, |g| {
        members(conn, scope, g.id)
    })
}

/// Concept groups of `kind` whose label (folded by `fold_identity`) or key contains
/// the raw query. A variable group needs a member in scope (and, under `period`, a
/// member state overlapping it) and sits in the `register` filter's register.
fn label_hits(
    conn: &Connection,
    scope: Scope,
    request: &Request,
    kind: &str,
) -> Result<Vec<Group>, Error> {
    let mut args: Vec<Sql> = vec![like_contains(request.q).into(), kind.to_owned().into()];
    let mut filters = String::new();
    if kind == "variable" {
        let held = held::variable(
            scope,
            "gv.variable_id",
            Narrow {
                years: request.years,
                ..Narrow::default()
            },
        );
        write!(
            filters,
            " AND EXISTS (SELECT 1 FROM concept_group_variable gm JOIN variable gv \
             USING(variable_id) WHERE gm.group_id = g.group_id AND {held})"
        )
        .expect("write to String");
        if let Some(register) = request.register {
            filters += " AND g.register_id = ?";
            args.push(register.into());
        }
        if let Some((lo, hi)) = request.years {
            filters += &year_filter(
                "concept_group_variable cgv JOIN variable_state vs ON vs.variable_id = cgv.variable_id",
                "cgv.group_id = g.group_id",
            );
            args.extend([i64::from(hi).into(), i64::from(lo).into()]);
        }
    }
    let sql = format!(
        "SELECT {GROUP_COLUMNS} FROM concept_group g \
         LEFT JOIN register r ON r.register_id = g.register_id \
         LEFT JOIN provider p ON p.provider_id = r.provider_id \
         WHERE (fold_identity(g.label) LIKE fold_identity(?1) ESCAPE '\\' \
         OR g.group_key LIKE ?1 ESCAPE '\\') AND g.kind = ?2{filters} \
         ORDER BY g.kind, g.group_key, p.slug, r.slug LIMIT {HORIZON}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(&args), |row| Group::read(row, 0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}

/// `%q%` with the LIKE metacharacters escaped (today's `_escape_like`).
fn like_contains(q: &str) -> String {
    format!("%{}%", like_escape(q))
}

fn like_escape(q: &str) -> String {
    let mut out = String::new();
    for c in q.chars() {
        if matches!(c, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Today's `_year_scope_filter`: some state `vs` reachable from `source` and tied to
/// the outer row by `correlation` overlaps the years (bind `hi`, then `lo`).
fn year_filter(source: &str, correlation: &str) -> String {
    format!(
        " AND EXISTS (SELECT 1 FROM {source} WHERE {correlation} \
         AND CAST(substr(vs.valid_from, 1, 4) AS INTEGER) <= ? \
         AND CAST(substr(vs.valid_to, 1, 4) AS INTEGER) >= ?)"
    )
}

/// Collapse sibling hits under their concept group of `kind`: a group replaces its
/// member hits where two distinct members matched or its label did ([`label_hits`]);
/// a lone member hit stays a leaf. Label-only groups follow, in label order.
/// `members` lists a group's members.
fn fold(
    conn: &Connection,
    scope: Scope,
    request: &Request,
    kind: &str,
    hits: Vec<Hit>,
    members: impl Fn(&Group) -> Result<Vec<Member>, Error>,
) -> Result<Vec<Hit>, Error> {
    let labels = label_hits(conn, scope, request, kind)?;
    let (table, column) = match kind {
        "variable" => ("concept_group_variable", "variable_id"),
        _ => ("concept_group_classification", "classification_id"),
    };
    let ids: Vec<i64> = hits.iter().filter_map(Hit::member_id).collect();
    let mut stmt = conn.prepare(&format!(
        "SELECT m.{column}, {GROUP_COLUMNS} FROM {table} m \
         JOIN concept_group g ON g.group_id = m.group_id \
         LEFT JOIN register r ON r.register_id = g.register_id \
         WHERE m.{column} IN (SELECT value FROM json_each(?))"
    ))?;
    // A variable in several groups keys on the last one read, as today.
    let membership: BTreeMap<i64, Group> = stmt
        .query_map([json!(ids).to_string()], |row| {
            Ok((row.get(0)?, Group::read(row, 1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let group_of = |hit: &Hit| {
        hit.member_id()
            .and_then(|id| Some((id, membership.get(&id)?)))
    };
    let mut matched: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
    let mut ranks: BTreeMap<i64, f64> = BTreeMap::new();
    for hit in &hits {
        if let Some((id, group)) = group_of(hit) {
            matched.entry(group.id).or_default().insert(id);
            let rank = ranks.entry(group.id).or_insert(hit.rank());
            *rank = rank.min(hit.rank());
        }
    }
    let label_ids: BTreeSet<i64> = labels.iter().map(|g| g.id).collect();
    let folded =
        |id: i64| label_ids.contains(&id) || matched.get(&id).is_some_and(|m| m.len() >= 2);
    let group_hit = |group: &Group| -> Result<Hit, Error> {
        Ok(Hit::Group(GroupHit {
            kind: group.kind.clone(),
            key: group.key.clone(),
            label: group.label.clone(),
            register_id: group.register_id,
            register_name: group.register_name.clone(),
            members: members(group)?,
            matched_count: matched.get(&group.id).map_or(0, BTreeSet::len),
            label_matched: label_ids.contains(&group.id),
            rank: ranks.get(&group.id).copied().unwrap_or(0.0),
        }))
    };
    let mut out = Vec::new();
    let mut emitted = BTreeSet::new();
    for hit in hits {
        match group_of(&hit) {
            Some((_, group)) if folded(group.id) => {
                if emitted.insert(group.id) {
                    out.push(group_hit(group)?);
                }
            }
            _ => out.push(hit),
        }
    }
    for group in &labels {
        if emitted.insert(group.id) {
            out.push(group_hit(group)?);
        }
    }
    Ok(out)
}

/// A variable group's members in scope (today's `Catalog.list_concept_groups`),
/// ordered by first facet value, FQID and delivery column.
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

/// The delivery columns a variable hit shows: those that satisfied the query, else
/// all of them.
fn shown_columns(v: &VariableHit, terms: &[String]) -> Vec<String> {
    let public = [
        &v.name,
        &v.definition,
        &v.description,
        &v.operational_definition,
    ];
    let matched = matched_columns(&v.columns, terms, &public);
    if matched.is_empty() {
        v.columns.clone()
    } else {
        matched
    }
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

/// Sort an arm's rows into today's order: display score descending, then rank, then
/// the tie key (today's `_search_display_score` sort, per `type`).
fn rank(q: &str, hits: Vec<Hit>) -> Vec<Hit> {
    let folded = fold_search(q);
    let scores: Vec<i64> = hits.iter().map(|h| identity_score(&folded, h)).collect();
    let promote = scores.iter().filter(|&&s| s > 0).count() <= MAX_PROMOTED;
    let mut keyed: Vec<(i64, f64, (String, String), Hit)> = hits
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
            (-display, hit.rank(), tie_key(&hit), hit)
        })
        .collect();
    keyed.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.total_cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
    });
    keyed.into_iter().map(|k| k.3).collect()
}

/// `fqid` and its last segment, when present.
fn with_leaf(fqid: Option<&String>) -> impl Iterator<Item = &str> {
    fqid.into_iter().flat_map(|f| [f.as_str(), leaf(f)])
}

/// Today's `_search_identity_score`: 1000 when an identity text folds to the
/// query, else 100 when one starts with it; a code's SQL rank is final, so 0.
fn identity_score(folded: &str, hit: &Hit) -> i64 {
    let mut texts: Vec<&str> = Vec::new();
    match hit {
        Hit::Register(r) => {
            texts.extend(with_leaf(r.fqid.as_ref()));
            texts.extend(r.name.as_deref());
        }
        Hit::Variable(v) => {
            texts.extend(with_leaf(v.fqid.as_ref()));
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
        Hit::Classification(c) => {
            texts.extend(with_leaf(c.fqid.as_ref()));
            texts.extend(
                [&c.short_name, &c.name, &c.terminal_fqid]
                    .into_iter()
                    .flatten()
                    .map(String::as_str),
            );
        }
        Hit::Succession(s) => {
            texts.extend(with_leaf(s.fqid.as_ref()));
            texts.extend(
                [&s.short_name, &s.name]
                    .into_iter()
                    .flatten()
                    .map(String::as_str),
            );
            for e in &s.editions {
                texts.extend(e.fqid.as_deref());
                texts.push(&e.slug);
                texts.extend(e.name.as_deref());
            }
        }
        Hit::Code(_) => return 0,
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

/// The webapp's `_best_bet_score`: a type prior (register 40, variable and variable
/// group 30, classification and classification group 25, code 10), plus 1000 when an
/// identity text folds to the query or else 100 when one starts with it (a code's
/// prefix reads its code only), plus the group authority bonus.
fn best_bet(folded: &str, hit: &Hit, terms: &[String]) -> i64 {
    let mut texts: Vec<String> = Vec::new();
    let mut push = |text: Option<&str>| texts.extend(text.map(str::to_owned));
    let prior = match hit {
        Hit::Register(r) => {
            with_leaf(r.fqid.as_ref()).for_each(|t| push(Some(t)));
            push(r.name.as_deref());
            40
        }
        Hit::Variable(v) => {
            with_leaf(v.fqid.as_ref()).for_each(|t| push(Some(t)));
            push(v.name.as_deref());
            for c in shown_columns(v, terms) {
                push(Some(&c));
            }
            30
        }
        Hit::Group(g) => {
            push(Some(&g.key));
            push(fqid_leaf(&g.key));
            push(Some(&g.label));
            for m in &g.members {
                push(Some(&m.fqid));
                push(fqid_leaf(&m.fqid));
                push(m.name.as_deref());
                push(m.delivery_column.as_deref());
                for (value, label) in &m.facets {
                    push(Some(value));
                    push(Some(label));
                }
            }
            if g.kind == "variable" { 30 } else { 25 }
        }
        Hit::Classification(c) => {
            with_leaf(c.fqid.as_ref()).for_each(|t| push(Some(t)));
            push(c.short_name.as_deref());
            push(c.name.as_deref());
            push(c.terminal_fqid.as_deref());
            push(c.terminal_fqid.as_deref().and_then(fqid_leaf));
            25
        }
        Hit::Succession(s) => {
            with_leaf(s.fqid.as_ref()).for_each(|t| push(Some(t)));
            push(s.short_name.as_deref());
            push(s.name.as_deref());
            for e in &s.editions {
                push(e.fqid.as_deref());
                push(e.fqid.as_deref().and_then(fqid_leaf));
                push(Some(&e.slug));
                push(e.name.as_deref());
            }
            25
        }
        Hit::Code(c) => {
            push(Some(&c.code));
            push(Some(&c.label));
            push(c.code_system.as_deref());
            10
        }
    };
    let texts: Vec<String> = texts.iter().map(|t| fold_search(t)).collect();
    if folded.is_empty() || texts.is_empty() {
        return prior;
    }
    let exact = texts.iter().any(|t| t == folded);
    let prefix = match hit {
        Hit::Code(c) => fold_search(&c.code).starts_with(folded),
        _ => texts.iter().any(|t| t.starts_with(folded)),
    };
    prior
        + if exact {
            EXACT
        } else if prefix {
            100
        } else {
            0
        }
        + authority(hit)
}

/// The webapp's `_top_candidate_key`: an untyped row's identity for deduplication.
/// A variable group is keyed by its members' registers too, so same-key groups of
/// different registers stay apart.
fn candidate_key(hit: &Hit, arm_order: usize, position: usize) -> String {
    let unresolved = format!("unresolved:{arm_order}:{position}");
    let (ty, fqid) = match hit {
        Hit::Group(g) if g.kind == "classification" => {
            return format!("group:{}:{}", g.kind, g.key);
        }
        Hit::Group(g) => {
            let scopes: BTreeSet<String> = g
                .members
                .iter()
                .filter_map(|m| match m.fqid.split('/').collect::<Vec<_>>()[..] {
                    [p, r, ..] if !p.is_empty() && !r.is_empty() => Some(format!("{p}/{r}")),
                    _ => None,
                })
                .collect();
            let scopes = if scopes.is_empty() {
                unresolved
            } else {
                scopes.into_iter().collect::<Vec<_>>().join(",")
            };
            return format!("group:{}:{scopes}:{}", g.kind, g.key);
        }
        Hit::Code(c) => {
            return format!(
                "code:{}:{}:{}",
                c.code,
                c.label,
                c.code_system.as_deref().unwrap_or("None")
            );
        }
        Hit::Register(r) => ("register", &r.fqid),
        Hit::Variable(v) => ("variable", &v.fqid),
        Hit::Classification(c) => ("classification", &c.fqid),
        Hit::Succession(s) => ("classification_succession", &s.fqid),
    };
    match fqid {
        Some(fqid) => format!("{ty}:{fqid}"),
        None => format!("{ty}:{unresolved}"),
    }
}

/// The last segment of a FQID.
fn leaf(fqid: &str) -> &str {
    fqid.rsplit('/').next().unwrap_or(fqid)
}

/// The webapp's `_fqid_leaf`: the last segment, when not empty.
fn fqid_leaf(value: &str) -> Option<&str> {
    Some(leaf(value)).filter(|l| !l.is_empty())
}

/// The deterministic final tie-breaker of an arm: a code's (code, label) as a tuple,
/// so code `1` sorts before `10`; any other hit's identity.
fn tie_key(hit: &Hit) -> (String, String) {
    match hit {
        Hit::Code(c) => (c.code.clone(), c.label.clone()),
        _ => (identity(hit), String::new()),
    }
}

/// What a typed cursor records of the row before the page (today's
/// `_search_result_identity`), and every non-code hit's tie key.
fn identity(hit: &Hit) -> String {
    match hit {
        Hit::Register(RegisterHit {
            fqid: Some(fqid), ..
        }) => format!("register:{fqid}"),
        Hit::Register(r) => format!("register:{}::::", r.register_id),
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
        Hit::Classification(ClassificationHit {
            fqid: Some(fqid), ..
        }) => format!("classification:{fqid}"),
        Hit::Classification(c) => format!("classification:::{}::", c.id),
        Hit::Succession(s) => format!(
            "classification_succession:{}",
            s.fqid.as_deref().unwrap_or("None")
        ),
        // JSON, so a `:` inside a code or label cannot make two codes collide.
        Hit::Code(c) => format!("code:{}", json!([c.code, c.label])),
    }
}

/// Today's `_is_code_shaped`: at least three characters once stripped, one a decimal
/// digit.
fn is_code_shaped(q: &str) -> bool {
    let q = py_strip(q);
    q.chars().count() >= 3 && q.chars().any(py_isdecimal)
}
