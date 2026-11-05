//! `search` and `search_facets`: one ranked FTS5 query over `search` and `search_item`
//! (crates/DESIGN.md, "Search", rules 1-7).

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::{Cache, Operation, Param, Params, Period, Server, Type, component, cursor, fts, refs};
use crate::{Code, Error, rows};

/// The `type` values: a `search_item.kind` each.
const TYPES: &[&str] = &["register", "table", "variable", "code", "document"];

/// How deep a ranked list pages (`operations.toml`).
const DEPTH: usize = 1000;

/// The rank (rule 2) and its ties (rule 3).
// simplify: exact match (diacritics kept) is one sort key ahead of bm25; revisit with
// smarter ranking.
const ORDER: &str = "m.tier, m.exact DESC, m.rank, i.kind, i.sort_key";

// Keep the original corpus and its scores unchanged. Only description matches that
// miss the primary index follow it, with base text retained for cross-field ANDs.
// `exact`: the row also matches with diacritics kept (`search_exact`).
const MATCHES: &str = "WITH folded AS (
    SELECT rowid AS id, 0 AS tier, bm25(search, 10.0, 5.0, 1.0) AS rank
    FROM search WHERE search MATCH ?1
    UNION ALL
    SELECT rowid, 1, bm25(search_description, 10.0, 5.0, 1.0)
    FROM search_description WHERE search_description MATCH ?1
      AND rowid NOT IN (SELECT rowid FROM search WHERE search MATCH ?1)
), matched AS (
    SELECT f.*, f.id IN (SELECT rowid FROM search_exact WHERE search_exact MATCH ?1) AS exact
    FROM folded f
)";

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "search",
        paths: &["/api/search"],
        tool: Some("search"),
        description: "Search registers, tables, variables, codes and documents by text, best \
            match first. `tag` keeps only that tag's variables. For exact column names use \
            `resolve`.",
        params: &[
            Param::required("q", Type::String),
            Param::optional("type", Type::Enum(TYPES)),
            Param::optional("register", Type::Ref),
            Param::optional("period", Type::Period),
            Param::optional("tag", Type::String),
            Param::optional("limit", Type::Limit),
            Param::optional("cursor", Type::Cursor),
            Param::optional("steward_cursor", Type::Cursor),
        ],
        cache: Cache::Minute,
        run: search,
        result: component::<SearchPage>,
    },
    Operation {
        name: "search_facets",
        paths: &["/api/search/facets"],
        tool: None,
        description: "Per register, how many variables a search query matches. `register` \
            or `provider` lists every match in that register, or in each of the \
            provider's registers.",
        params: &[
            Param::required("q", Type::String),
            Param::optional("register", Type::Ref),
            Param::optional("provider", Type::Ref),
            Param::optional("limit", Type::Limit),
        ],
        cache: Cache::Minute,
        run: search_facets,
        result: component::<RegisterFacets>,
    },
];

#[derive(Serialize, ToSchema)]
pub struct SearchPage {
    items: Vec<SearchHit>,
    #[schema(required = true)]
    next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    steward: Option<StewardSearchPage>,
}

/// Matches in the data holder's own data, with their own count and cursor.
#[derive(Serialize, ToSchema)]
pub struct StewardSearchPage {
    owner: String,
    items: Vec<SearchHit>,
    next_cursor: Option<String>,
    total: i64,
}

/// One hit, tagged by its kind.
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
        /// Its column names, sorted.
        spellings: Vec<String>,
        period: Period,
    },
    /// One distinct (code, label) pair, not a value set; its value sets are
    /// `values?code=&label=`.
    Code {
        code: String,
        #[schema(required = true)]
        label: Option<String>,
        /// How many value sets in use contain this code and label.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    steward: Option<StewardRegisterFacets>,
}

#[derive(Serialize, ToSchema)]
pub struct StewardRegisterFacets {
    owner: String,
    registers: Vec<RegisterFacet>,
}

/// One register's matched variables. `name` and `variables` are present only in the
/// modes that list them, never `null`.
#[derive(Serialize, ToSchema)]
pub struct RegisterFacet {
    fqid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
    name: Option<String>,
    variable_matches: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = false)]
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
    entity_of(conn, value, "register")
}

/// The entity of `kind` a parameter names.
fn entity_of(conn: &Connection, value: &str, kind: &str) -> Result<i64, Error> {
    let entity = refs::resolve(conn, value)?;
    if entity.kind != kind {
        return Err(Error::invalid_ref(
            value,
            format!("{value:?} is a {}, not a {kind}.", entity.kind),
        ));
    }
    Ok(entity.id)
}

fn search(server: &Server, params: &Params) -> Result<Value, Error> {
    // Steward mode serves only what the steward holds (maintainer 2026-11-04).
    let held = server.catalog.steward().is_some();
    let conn = server.catalog.connect()?;
    if let Some(tag) = params.get("tag") {
        require_tag(server, &conn, tag)?;
    }
    let mut page = search_origin(server, params, &conn, "cursor", "reference", held)?;
    if let Some(extra) = server.catalog.connect_holdings()? {
        let supplemental =
            search_origin(server, params, &extra, "steward_cursor", "steward", held)?;
        page.steward = Some(StewardSearchPage {
            owner: server.catalog.steward().expect("admitted holdings").owner,
            items: supplemental.items,
            next_cursor: supplemental.next_cursor,
            total: supplemental.total.expect("paired search has a count"),
        });
    } else if params.get("steward_cursor").is_some() {
        return Err(Error::invalid_parameter("steward_cursor"));
    }
    Ok(serde_json::to_value(page).expect("a page serializes"))
}

/// Tags are search-only refs, so their published vocabulary lives in `var_set`,
/// without `ident` rows. A paired catalog may define the tag in either origin.
fn require_tag(server: &Server, conn: &Connection, value: &str) -> Result<(), Error> {
    let pid = format!("tag/{}", value.strip_prefix("tag/").unwrap_or(value));
    let exists = |conn: &Connection| -> Result<bool, Error> {
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM var_set WHERE kind = 'tag' AND pid = ?)",
            [&pid],
            |row| row.get(0),
        )?)
    };
    if exists(conn)? {
        return Ok(());
    }
    if let Some(extra) = server.catalog.connect_holdings()?
        && exists(&extra)?
    {
        return Ok(());
    }
    Err(Error::new(
        Code::NotFound,
        format!("Nothing is named {value:?}."),
        vec![value.into()],
    ))
}

fn search_origin(
    server: &Server,
    params: &Params,
    conn: &Connection,
    cursor_name: &str,
    origin: &str,
    held: bool,
) -> Result<SearchPage, Error> {
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
    let period = super::days(params, "period")?;
    let limit = super::limit(params)?;
    let paired = server.catalog.steward().is_some();
    let context = if paired {
        format!("{}&origin={origin}", cursor::context(params))
    } else {
        cursor::context(params)
    };
    let generation = server.catalog.generation();
    let offset = params
        .get(cursor_name)
        .map(|c| cursor::decode(c, generation, &context, DEPTH))
        .transpose()?
        .map_or(0, |(offset, _)| offset);
    let register = match params
        .get("register")
        .map(|r| register(conn, r))
        .transpose()
    {
        Ok(id) => id,
        Err(err) if paired && err.kind() == Code::NotFound => {
            // A register in the other database has no matches in this group.
            server.catalog.connect_for_ref(params["register"])?;
            return Ok(SearchPage {
                items: vec![],
                next_cursor: None,
                total: Some(0),
                steward: None,
            });
        }
        Err(err) => return Err(err),
    };
    let Some(expression) = expression else {
        return Ok(SearchPage {
            items: vec![],
            next_cursor: None,
            total: paired.then_some(0),
            steward: None,
        });
    };
    let tag = params
        .get("tag")
        .map(|tag| format!("tag/{}", tag.strip_prefix("tag/").unwrap_or(tag)));
    let filter = search_filter(server, conn, held)?;
    let (start, end) = period.unzip();
    let total = paired.then(|| conn.query_row(
        &format!("{MATCHES} SELECT COUNT(*) FROM matched m JOIN search_item i ON i.id = m.id WHERE {filter}"),
        params![expression, kind, register, start, end, tag], |r| r.get(0))).transpose()?;
    let take = limit.min(DEPTH - offset);
    let sql_int = |n: usize| i64::try_from(n).expect("bounded by DEPTH");
    let mut rows: Vec<(i64, String, Option<i64>, String)> = rows(
        conn,
        &format!(
            "{MATCHES} SELECT i.id, i.kind, i.entity_id, i.sort_key
         FROM matched m JOIN search_item i ON i.id = m.id
         WHERE {filter} ORDER BY {ORDER} LIMIT ?7 OFFSET ?8"
        ),
        params![
            expression,
            kind,
            register,
            start,
            end,
            tag,
            sql_int(take + 1),
            sql_int(offset)
        ],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    let next_cursor = cursor::next(&mut rows, take, |last| {
        (offset + take < DEPTH)
            .then(|| cursor::encode(generation, &context, offset + take, &last.3))
    });
    let mut items: Vec<SearchHit> = rows
        .iter()
        .map(|(id, kind, entity, _)| hit(server, conn, &expression, *id, kind, *entity))
        .collect::<Result<_, _>>()?;
    name_hits(server, &mut items)?;
    Ok(SearchPage {
        items,
        next_cursor,
        total,
        steward: None,
    })
}

/// Count and page share all filters. In steward mode a register, table or variable
/// matches only when held (by public id, over the requested interval, which its
/// placements answer alone: a year held through steward attestation can lie outside
/// the reference interval); codes and documents are shared and always match.
///
/// The held set is an uncorrelated `IN`, built once per query: a correlated `EXISTS`
/// scanned `holding_mapping` once per FTS match, and a one-letter query has ~10^5.
fn search_filter(server: &Server, conn: &Connection, held: bool) -> Result<String, Error> {
    let (scope, interval) = if held {
        server.catalog.attach_holdings(conn)?;
        let from = "FROM held.holding_mapping hm
            JOIN variable hv ON hv.pid = hm.variable_pid
            JOIN tbl ht ON ht.pid = hm.table_pid
            WHERE (?4 IS NULL OR (hm.start < ?5 AND ?4 < hm.end))";
        (
            format!(
                " AND (i.kind NOT IN ('register', 'table', 'variable')
            OR (i.kind, i.entity_id) IN (
                SELECT 'variable', hv.id {from}
                UNION ALL SELECT 'table', ht.id {from}
                UNION ALL SELECT 'register', ht.register_id {from}))"
            ),
            "i.kind IN ('register', 'table', 'variable') OR ",
        )
    } else {
        (String::new(), "")
    };
    Ok(format!(
        "(?2 IS NULL OR i.kind = ?2)
        AND (?3 IS NULL OR i.register_id = ?3)
        AND (?4 IS NULL OR {interval}(i.start < ?5 AND ?4 < i.end))
        AND (?6 IS NULL OR (i.kind = 'variable' AND EXISTS (
            SELECT 1 FROM var_set_member tm JOIN var_set t ON t.id = tm.set_id
            WHERE tm.variable_id = i.entity_id AND t.kind = 'tag' AND t.pid = ?6))){scope}"
    ))
}

/// Names a page's register, table and variable hits, and their registers, as `show`
/// does: in steward mode an id the reference catalog defines keeps its reference name,
/// never the holdings overlay's (often its slug, #2252). `hit` leaves each
/// `register_name` holding the register's fqid for this pass.
fn name_hits(server: &Server, items: &mut [SearchHit]) -> Result<(), Error> {
    let fqids: Vec<String> = items
        .iter()
        .flat_map(|item| match item {
            SearchHit::Register { fqid, .. } => vec![fqid.clone()],
            SearchHit::Table {
                fqid,
                register_name,
                ..
            }
            | SearchHit::Variable {
                fqid,
                register_name,
                ..
            } => {
                vec![fqid.clone(), register_name.clone()]
            }
            _ => vec![],
        })
        .collect();
    let names = refs::names(server, fqids.iter().map(String::as_str))?;
    for item in items {
        match item {
            SearchHit::Register { fqid, name, .. } => {
                *name = names.of(refs::Named::Register, fqid)?;
            }
            SearchHit::Table {
                fqid,
                name,
                register_name,
                ..
            } => {
                *name = names.of(refs::Named::Table, fqid)?;
                *register_name = names.of(refs::Named::Register, register_name)?;
            }
            SearchHit::Variable {
                fqid,
                name,
                register_name,
                ..
            } => {
                *name = names.of(refs::Named::Variable, fqid)?;
                *register_name = names.of(refs::Named::Register, register_name)?;
            }
            _ => {}
        }
    }
    Ok(())
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
                let fqid: String = row.get(0)?;
                Ok(SearchHit::Register {
                    period: server.hull("register", &fqid, row.get(3)?, row.get(4)?),
                    fqid,
                    name: row.get(1)?,
                    description: row.get(2)?,
                })
            },
        )?,
        "table" => conn.query_row(
            "SELECT t.pid, n.body, r.pid, t.role, t.start, t.end
             FROM tbl t
               JOIN text n ON n.id = t.name_text
               JOIN register r ON r.id = t.register_id
             WHERE t.id = ?",
            [entity()?],
            |row| {
                let fqid: String = row.get(0)?;
                Ok(SearchHit::Table {
                    period: server.hull("table", &fqid, row.get(4)?, row.get(5)?),
                    fqid,
                    name: row.get(1)?,
                    register_name: row.get(2)?,
                    role: row.get(3)?,
                })
            },
        )?,
        "variable" => {
            let variable = entity()?;
            let spellings = rows(
                conn,
                "SELECT n.folded FROM alias a JOIN name_fold n ON n.id = a.fold_id
                     WHERE a.variable_id = ? ORDER BY n.folded",
                [variable],
                |row| row.get(0),
            )?;
            conn.query_row(
                "SELECT v.pid, l.body, r.pid, d.body, v.start, v.end
                 FROM variable v
                   JOIN text l ON l.id = v.label_text
                   JOIN register r ON r.id = v.register_id
                   LEFT JOIN text d ON d.id = v.definition_text
                 WHERE v.id = ?",
                [variable],
                |row| {
                    let fqid: String = row.get(0)?;
                    Ok(SearchHit::Variable {
                        period: server.hull("variable", &fqid, row.get(4)?, row.get(5)?),
                        fqid,
                        name: row.get(1)?,
                        register_name: row.get(2)?,
                        definition: row.get(3)?,
                        spellings,
                    })
                },
            )?
        }
        "code" => code_hit(conn, id)?,
        "document" => document_hit(conn, expression, entity()?)?,
        // The schema admits no other kind; such a row is a defect of the artifact.
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
    // Steward mode serves only what the steward holds (maintainer 2026-11-04).
    let held = server.catalog.steward().is_some();
    let run = |conn: &Connection| -> Result<Vec<RegisterFacet>, Error> {
        // A scope one origin lacks matches nothing there, once the other has it.
        let mut scope = [None, None];
        for (at, kind) in ["register", "provider"].into_iter().enumerate() {
            scope[at] = match params
                .get(kind)
                .map(|r| entity_of(conn, r, kind))
                .transpose()
            {
                Ok(id) => id,
                Err(err) if server.catalog.steward().is_some() && err.kind() == Code::NotFound => {
                    server.catalog.connect_for_ref(params[kind])?;
                    return Ok(vec![]);
                }
                Err(err) => return Err(err),
            };
        }
        let [register, provider] = scope;
        if held {
            server.catalog.attach_holdings(conn)?;
        }
        match &expression {
            None => Ok(vec![]),
            Some(expression) => facets(conn, expression, register, provider, limit, held),
        }
    };
    let registers = run(&server.catalog.connect()?)?;
    let steward = server
        .catalog
        .connect_holdings()?
        .map(|extra| {
            Ok::<_, Error>(StewardRegisterFacets {
                owner: server.catalog.steward().expect("admitted pair").owner,
                registers: run(&extra)?,
            })
        })
        .transpose()?;
    Ok(serde_json::to_value(RegisterFacets { registers, steward }).expect("facets serialize"))
}

/// Rule 7: each register's matched variables, counted; with `register` that register's
/// every match listed, with `provider` each of its registers' every match and name;
/// else with `limit` its first `limit` and its name.
fn facets(
    conn: &Connection,
    expression: &str,
    register: Option<i64>,
    provider: Option<i64>,
    limit: Option<usize>,
    held: bool,
) -> Result<Vec<RegisterFacet>, Error> {
    let held_scope = if held {
        // Uncorrelated, like `search_filter`'s: no index leads with `variable_pid`.
        " AND v.pid IN (SELECT variable_pid FROM held.holding_mapping)"
    } else {
        ""
    };
    // The provider's parameter is the query's last: ?3 in the count, ?4 in the list.
    let scope = |at: u8| {
        format!(
            "{held_scope} AND (?{at} IS NULL OR i.register_id IN \
             (SELECT id FROM register WHERE provider_id = ?{at}))"
        )
    };
    let counted = scope(3);
    let mut registers: Vec<(i64, RegisterFacet)> = rows(
        conn,
        &format!(
            "{MATCHES}
             SELECT r.id, r.pid, n.body, m.matches
             FROM (SELECT i.register_id AS id, COUNT(*) AS matches
                   FROM matched m JOIN search_item i ON i.id = m.id
                     JOIN variable v ON v.id = i.entity_id
                   WHERE i.kind = 'variable'
                     AND (?2 IS NULL OR i.register_id = ?2){counted}
                   GROUP BY i.register_id) m
               JOIN register r ON r.id = m.id
               JOIN text n ON n.id = r.name_text
             ORDER BY r.pid"
        ),
        params![expression, register, provider],
        |row| {
            Ok((
                row.get(0)?,
                RegisterFacet {
                    fqid: row.get(1)?,
                    name: row.get(2)?,
                    variable_matches: row.get(3)?,
                    variables: None,
                },
            ))
        },
    )?;
    // A named register or provider lists every match; otherwise `limit` per register,
    // or none.
    let listed = match (register.or(provider), limit) {
        (Some(_), _) => Some(i64::MAX),
        (None, Some(limit)) => Some(i64::try_from(limit).expect("a limit fits")),
        (None, None) => None,
    };
    let listing = scope(4);
    let Some(listed) = listed else {
        for (_, facet) in &mut registers {
            facet.name = None;
        }
        return Ok(registers.into_iter().map(|(_, facet)| facet).collect());
    };
    let mut variables: BTreeMap<i64, Vec<FacetVariable>> = BTreeMap::new();
    let rows = rows(
        conn,
        &format!(
            "{MATCHES}
             SELECT register_id, pid, label FROM (
               SELECT i.register_id, v.pid, l.body AS label,
                      ROW_NUMBER() OVER (PARTITION BY i.register_id ORDER BY v.pid) AS n
               FROM matched m JOIN search_item i ON i.id = m.id
                 JOIN variable v ON v.id = i.entity_id
                 JOIN text l ON l.id = v.label_text
               WHERE i.kind = 'variable'
                 AND (?2 IS NULL OR i.register_id = ?2){listing})
             WHERE n <= ?3
             ORDER BY register_id, pid"
        ),
        params![expression, register, listed, provider],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                FacetVariable {
                    fqid: row.get(1)?,
                    name: row.get(2)?,
                },
            ))
        },
    )?;
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
