//! `search` and `search_facets`: one ranked FTS5 query over `search` and `search_item`
//! (crates/DESIGN.md, "Search", rules 1-7).

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::{Cache, Operation, Param, Params, Period, Server, Type, component, cursor, fts, refs};
use crate::{Code, Error, hex};

/// The `type` values: a `search_item.kind` each.
const TYPES: &[&str] = &[
    "register",
    "table",
    "variable",
    "classification",
    "code",
    "document",
];

/// How deep a ranked list pages (`operations.toml`).
const DEPTH: usize = 1000;

/// The rank (rule 2) and its ties (rule 3).
const ORDER: &str = "bm25(search, 10.0, 5.0, 1.0), i.kind, i.sort_key";

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "search",
        paths: &["/api/search"],
        tool: Some("search"),
        description: "Full-text search over registers, tables, variables, value codes and \
                      documents, ranked by relevance. Exact column names are `resolve`.",
        params: &[
            Param::required("q", Type::String),
            Param::optional("type", Type::Enum(TYPES)),
            Param::optional("register", Type::Ref),
            Param::optional("period", Type::Period),
            Param::optional("limit", Type::Limit),
            Param::optional("cursor", Type::Cursor),
        ],
        cache: Cache::Minute,
        run: search,
        result: component::<SearchPage>,
    },
    Operation {
        name: "search_facets",
        paths: &["/api/search/facets"],
        tool: None,
        description: "Per register, how many variables a search query matches, for the \
                      SPA's register filter.",
        params: &[
            Param::required("q", Type::String),
            Param::optional("register", Type::Ref),
            Param::optional("limit", Type::Limit),
        ],
        cache: Cache::Minute,
        run: search_facets,
        result: component::<RegisterFacets>,
    },
];

/// `Page<SearchHit>`.
#[derive(Serialize, ToSchema)]
pub struct SearchPage {
    items: Vec<SearchHit>,
    #[schema(required = true)]
    next_cursor: Option<String>,
}

/// `shape.SearchHit`: one hit, tagged by its kind.
#[derive(Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SearchHit {
    Register {
        fqid: String,
        name: String,
        #[schema(required = true)]
        description: Option<String>,
        period: Period,
    },
    Table {
        fqid: String,
        name: String,
        register_name: String,
        role: String,
        period: Period,
    },
    Variable {
        fqid: String,
        /// The label.
        name: String,
        register_name: String,
        #[schema(required = true)]
        definition: Option<String>,
        /// The folded names it was delivered under, sorted.
        spellings: Vec<String>,
        period: Period,
    },
    Classification {
        fqid: String,
        name: String,
        family: String,
    },
    /// One distinct (code, label) pair, not a value set; its value sets are
    /// `values?code=&label=`.
    Code {
        code: String,
        #[schema(required = true)]
        label: Option<String>,
        /// The value sets some segment references that hold the pair.
        value_set_count: i64,
    },
    Document {
        /// The document's hash.
        doc: String,
        title: String,
        /// The best-matching section's heading; `null` when no section matches.
        #[schema(required = true)]
        heading: Option<String>,
        #[schema(required = true)]
        page: Option<i64>,
    },
}

/// `search_facets`' result.
#[derive(Serialize, ToSchema)]
pub struct RegisterFacets {
    registers: Vec<RegisterFacet>,
}

/// One register's matched variables; `name` and `variables` only where the mode lists
/// them (rule 7).
#[derive(Serialize, ToSchema)]
pub struct RegisterFacet {
    fqid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    variable_matches: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    variables: Option<Vec<FacetVariable>>,
}

/// A matched variable: its FQID and label.
#[derive(Serialize, ToSchema)]
pub struct FacetVariable {
    fqid: String,
    name: String,
}

/// The register a `register` parameter names (rule 4).
fn register(conn: &Connection, value: &str) -> Result<i64, Error> {
    let entity = refs::resolve(conn, value)?;
    if entity.kind != "register" {
        return Err(Error::new(
            Code::InvalidRef,
            format!("{value:?} is a {}, not a register.", entity.kind),
            vec![value.into()],
        ));
    }
    Ok(entity.id)
}

/// The cursor context: every parameter but `cursor` and `limit`, sorted by name. Hex,
/// because `cursor::decode` splits on `.`, which a `q` may hold.
fn context(params: &Params) -> String {
    let pairs: Vec<String> = ["period", "q", "register", "type"]
        .into_iter()
        .filter_map(|name| params.get(name).map(|value| format!("{name}={value}")))
        .collect();
    hex(pairs.join("&").as_bytes())
}

fn search(server: &Server, params: &Params) -> Result<Value, Error> {
    let expression = fts::query(params["q"])?;
    let kind = params
        .get("type")
        .map(|t| {
            TYPES
                .contains(t)
                .then_some(*t)
                .ok_or_else(|| Error::invalid_parameter("type"))
        })
        .transpose()?;
    let period = super::period(params, "period")?.map(|p| {
        let (first, last) = p.iso_bounds();
        let day = |iso: &str| reg_core::iso_day_number(iso).expect("a grammar period's day");
        (day(&first), day(&last) + 1)
    });
    let limit = super::limit(params)?;
    let generation = server.catalog.generation();
    let context = context(params);
    let offset = params
        .get("cursor")
        .map(|c| cursor::decode(c, generation, &context, DEPTH))
        .transpose()?
        .map_or(0, |(offset, _)| offset);
    let conn = server.catalog.connect()?;
    let register = params
        .get("register")
        .map(|r| register(&conn, r))
        .transpose()?;
    let Some(expression) = expression else {
        return Ok(page(vec![], None));
    };

    // A page never passes the DEPTH-th hit; one extra row tells whether more follow.
    let take = limit.min(DEPTH - offset);
    let (start, end) = period.unzip();
    let sql_int = |n: usize| i64::try_from(n).expect("bounded by DEPTH");
    let mut rows: Vec<(i64, String, Option<i64>, String)> = conn
        .prepare(&format!(
            "SELECT i.id, i.kind, i.entity_id, i.sort_key
             FROM search JOIN search_item i ON i.id = search.rowid
             WHERE search MATCH ?1
               AND (?2 IS NULL OR i.kind = ?2)
               AND (?3 IS NULL OR i.register_id = ?3)
               AND (?4 IS NULL OR (i.start < ?5 AND ?4 < i.end))
             ORDER BY {ORDER} LIMIT ?6 OFFSET ?7"
        ))?
        .query_map(
            params![
                expression,
                kind,
                register,
                start,
                end,
                sql_int(take + 1),
                sql_int(offset)
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?
        .collect::<rusqlite::Result<_>>()?;
    let more = rows.len() > take;
    rows.truncate(take);
    let next = (more && offset + take < DEPTH).then(|| {
        let last = &rows.last().expect("a page before more hits").3;
        cursor::encode(generation, &context, offset + take, last)
    });
    let items = rows
        .iter()
        .map(|(id, kind, entity, _)| hit(server, &conn, &expression, *id, kind, *entity))
        .collect::<Result<_, _>>()?;
    Ok(page(items, next))
}

fn page(items: Vec<SearchHit>, next_cursor: Option<String>) -> Value {
    serde_json::to_value(SearchPage { items, next_cursor }).expect("a page serializes")
}

/// A row's hit, with the fields rule 6 lists.
fn hit(
    server: &Server,
    conn: &Connection,
    expression: &str,
    id: i64,
    kind: &str,
    entity: Option<i64>,
) -> Result<SearchHit, Error> {
    let entity = || entity.ok_or_else(|| internal(format!("search row {id} has no entity")));
    Ok(match kind {
        "register" => conn.query_row(
            "SELECT r.pid, n.body, d.body, r.start, r.end
             FROM register r
               JOIN text n ON n.id = r.name_text
               LEFT JOIN text d ON d.id = r.description_text
             WHERE r.id = ?",
            [entity()?],
            |row| {
                Ok(SearchHit::Register {
                    fqid: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    period: server.period(row.get(3)?, row.get(4)?),
                })
            },
        )?,
        "table" => conn.query_row(
            "SELECT t.pid, n.body, rn.body, t.role, t.start, t.end
             FROM tbl t
               JOIN text n ON n.id = t.name_text
               JOIN register r ON r.id = t.register_id
               JOIN text rn ON rn.id = r.name_text
             WHERE t.id = ?",
            [entity()?],
            |row| {
                Ok(SearchHit::Table {
                    fqid: row.get(0)?,
                    name: row.get(1)?,
                    register_name: row.get(2)?,
                    role: row.get(3)?,
                    period: server.period(row.get(4)?, row.get(5)?),
                })
            },
        )?,
        "variable" => {
            let variable = entity()?;
            let spellings = conn
                .prepare(
                    "SELECT n.folded FROM alias a JOIN name_fold n ON n.id = a.fold_id
                     WHERE a.variable_id = ? ORDER BY n.folded",
                )?
                .query_map([variable], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            conn.query_row(
                "SELECT v.pid, l.body, rn.body, d.body, v.start, v.end
                 FROM variable v
                   JOIN text l ON l.id = v.label_text
                   JOIN register r ON r.id = v.register_id
                   JOIN text rn ON rn.id = r.name_text
                   LEFT JOIN text d ON d.id = v.definition_text
                 WHERE v.id = ?",
                [variable],
                |row| {
                    Ok(SearchHit::Variable {
                        fqid: row.get(0)?,
                        name: row.get(1)?,
                        register_name: row.get(2)?,
                        definition: row.get(3)?,
                        spellings,
                        period: server.period(row.get(4)?, row.get(5)?),
                    })
                },
            )?
        }
        "code" => code_hit(conn, id)?,
        "document" => document_hit(conn, expression, entity()?)?,
        // simplify: classification and set hits land in D, with the curation that
        // creates their rows; until then such a row is a defect of the artifact.
        other => return Err(internal(format!("search row {id} has kind {other:?}"))),
    })
}

/// A code row's pair and the count of referenced value sets that hold it.
fn code_hit(conn: &Connection, id: i64) -> rusqlite::Result<SearchHit> {
    let (code_text, label_text, code, label): (i64, Option<i64>, String, Option<String>) = conn
        .query_row(
            "SELECT i.code_text, i.label_text, c.body, l.body
             FROM search_item i
               JOIN text c ON c.id = i.code_text
               LEFT JOIN text l ON l.id = i.label_text
             WHERE i.id = ?",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    let value_set_count = conn.query_row(
        "SELECT COUNT(DISTINCT valueset_id) FROM vs_item
         WHERE code_text = ? AND label_text IS ?
           AND EXISTS (SELECT 1 FROM segment WHERE segment.valueset_id = vs_item.valueset_id)",
        params![code_text, label_text],
        |row| row.get(0),
    )?;
    Ok(SearchHit::Code {
        code,
        label,
        value_set_count,
    })
}

/// A document, with its best section for `expression`.
fn document_hit(conn: &Connection, expression: &str, doc: i64) -> rusqlite::Result<SearchHit> {
    let (hash, title): (String, String) = conn.query_row(
        "SELECT d.hash, t.body FROM doc d JOIN text t ON t.id = d.title_text WHERE d.id = ?",
        [doc],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let section: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT s.heading, s.page
             FROM doc_fts JOIN doc_section s ON s.id = doc_fts.rowid
             WHERE doc_fts MATCH ? AND s.doc_id = ?
             ORDER BY bm25(doc_fts), s.ord LIMIT 1",
            params![expression, doc],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (heading, page) = section.unwrap_or_default();
    Ok(SearchHit::Document {
        doc: hash,
        title,
        heading,
        page,
    })
}

fn internal(message: String) -> Error {
    Error::new(Code::InternalError, message, vec![])
}

fn search_facets(server: &Server, params: &Params) -> Result<Value, Error> {
    let expression = fts::query(params["q"])?;
    let limit = params
        .get("limit")
        .is_some()
        .then(|| super::limit(params))
        .transpose()?;
    let conn = server.catalog.connect()?;
    let register = params
        .get("register")
        .map(|r| register(&conn, r))
        .transpose()?;
    let registers = match expression {
        None => vec![],
        Some(expression) => facets(&conn, &expression, register, limit)?,
    };
    Ok(serde_json::to_value(RegisterFacets { registers }).expect("facets serialize"))
}

/// Rule 7: each register's matched variables, counted; with `register` that register's
/// every match listed; else with `limit` its first `limit` and its name.
fn facets(
    conn: &Connection,
    expression: &str,
    register: Option<i64>,
    limit: Option<usize>,
) -> Result<Vec<RegisterFacet>, Error> {
    let mut registers: Vec<(i64, RegisterFacet)> = conn
        .prepare(
            "SELECT r.id, r.pid, n.body, m.matches
             FROM (SELECT i.register_id AS id, COUNT(*) AS matches
                   FROM search JOIN search_item i ON i.id = search.rowid
                   WHERE search MATCH ?1 AND i.kind = 'variable'
                     AND (?2 IS NULL OR i.register_id = ?2)
                   GROUP BY i.register_id) m
               JOIN register r ON r.id = m.id
               JOIN text n ON n.id = r.name_text
             ORDER BY r.pid",
        )?
        .query_map(params![expression, register], |row| {
            Ok((
                row.get(0)?,
                RegisterFacet {
                    fqid: row.get(1)?,
                    name: row.get(2)?,
                    variable_matches: row.get(3)?,
                    variables: None,
                },
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    // A named register lists every match; otherwise `limit` per register, or none.
    let listed = match (register, limit) {
        (Some(_), _) => Some(i64::MAX),
        (None, Some(limit)) => Some(i64::try_from(limit).expect("a limit fits")),
        (None, None) => None,
    };
    let Some(listed) = listed else {
        for (_, facet) in &mut registers {
            facet.name = None;
        }
        return Ok(registers.into_iter().map(|(_, facet)| facet).collect());
    };
    let mut variables: BTreeMap<i64, Vec<FacetVariable>> = BTreeMap::new();
    let rows = conn
        .prepare(
            "SELECT register_id, pid, label FROM (
               SELECT i.register_id, v.pid, l.body AS label,
                      ROW_NUMBER() OVER (PARTITION BY i.register_id ORDER BY v.pid) AS n
               FROM search JOIN search_item i ON i.id = search.rowid
                 JOIN variable v ON v.id = i.entity_id
                 JOIN text l ON l.id = v.label_text
               WHERE search MATCH ?1 AND i.kind = 'variable'
                 AND (?2 IS NULL OR i.register_id = ?2))
             WHERE n <= ?3
             ORDER BY register_id, pid",
        )?
        .query_map(params![expression, register, listed], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                FacetVariable {
                    fqid: row.get(1)?,
                    name: row.get(2)?,
                },
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, variable) in rows {
        variables.entry(id).or_default().push(variable);
    }
    Ok(registers
        .into_iter()
        .map(|(id, mut facet)| {
            if register.is_some() {
                facet.name = None;
            }
            facet.variables = Some(variables.remove(&id).unwrap_or_default());
            facet
        })
        .collect())
}
