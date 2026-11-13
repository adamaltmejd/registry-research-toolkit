//! `search`: one ranked FTS5 query over `search` and `search_item`
//! (crates/DESIGN.md, "Search", rules 1-6).

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
const ORDER: &str = "m.tier, m.rank, i.kind, i.sort_key";

// Keep the original corpus and its scores unchanged. Only description matches that
// miss the primary index follow it, with base text retained for cross-field ANDs.
const MATCHES: &str = "WITH matched AS (
    SELECT rowid AS id, 0 AS tier, bm25(search, 10.0, 5.0, 1.0) AS rank
    FROM search WHERE search MATCH ?1
    UNION ALL
    SELECT rowid, 1, bm25(search_description, 10.0, 5.0, 1.0)
    FROM search_description WHERE search_description MATCH ?1
      AND rowid NOT IN (SELECT rowid FROM search WHERE search MATCH ?1)
)";

// The fallback when no row matches every term (rule 2): `?1` is the JSON array of the
// terms, and a row matching any term ranks by how many it matches (most first, as a
// negative tier), then by the sum of its best score per term over either index.
const ANY_MATCHES: &str = "WITH hits AS (
    SELECT s.rowid AS id, t.key AS term, bm25(search, 10.0, 5.0, 1.0) AS rank
    FROM json_each(?1) t JOIN search s ON search MATCH t.value
    UNION ALL
    SELECT d.rowid, t.key, bm25(search_description, 10.0, 5.0, 1.0)
    FROM json_each(?1) t JOIN search_description d ON search_description MATCH t.value
), per_term AS (
    SELECT id, term, MIN(rank) AS rank FROM hits GROUP BY id, term
), matched AS (
    SELECT id, -COUNT(*) AS tier, SUM(rank) AS rank FROM per_term GROUP BY id
)";

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "search",
    paths: &["/api/search"],
    tool: Some("search"),
    description: "Search registers, tables, variables, codes and documents by text, best \
            match first. A hit matches every word of `q`; when nothing does, hits matching \
            the most words come first. `tag` keeps only that tag's variables. For exact \
            column names use `resolve`.",
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
}];

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

/// The register a `register` parameter names (rule 4).
fn register(conn: &Connection, value: &str) -> Result<i64, Error> {
    Ok(refs::expect(conn, value, &["register"], "not a register")?.id)
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
    let pid = format!("tag/{}", value.trim_prefix("tag/"));
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
    Err(Error::not_found(value))
}

fn search_origin(
    server: &Server,
    params: &Params,
    conn: &Connection,
    cursor_name: &str,
    origin: &str,
    held: bool,
) -> Result<SearchPage, Error> {
    let terms = fts::terms(params["q"])?;
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
    let origin = if paired {
        format!("&origin={origin}")
    } else {
        String::new()
    };
    let offset = cursor::start(server, params, cursor_name, &origin, DEPTH)?.map_or(0, |(o, _)| o);
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
    if terms.is_empty() {
        return Ok(SearchPage {
            items: vec![],
            next_cursor: None,
            total: paired.then_some(0),
            steward: None,
        });
    }
    let tag = params
        .get("tag")
        .map(|tag| format!("tag/{}", tag.trim_prefix("tag/")));
    let filter = search_filter(server, conn, held)?;
    let (start, end) = period.unzip();
    let (matches, bound, expression) = matching(terms, |every| {
        Ok(conn.query_row(
            &format!(
                "{MATCHES} SELECT EXISTS (SELECT 1 FROM matched m \
                 JOIN search_item i ON i.id = m.id WHERE {filter})"
            ),
            params![every, kind, register, start, end, tag],
            |r| r.get::<_, bool>(0),
        )?)
    })?;
    let total = paired.then(|| conn.query_row(
        &format!("{matches} SELECT COUNT(*) FROM matched m JOIN search_item i ON i.id = m.id WHERE {filter}"),
        params![bound, kind, register, start, end, tag], |r| r.get(0))).transpose()?;
    let take = limit.min(DEPTH - offset);
    let sql_int = |n: usize| i64::try_from(n).expect("bounded by DEPTH");
    let mut rows: Vec<(i64, String, Option<i64>, String)> = rows(
        conn,
        &format!(
            "{matches} SELECT i.id, i.kind, i.entity_id, i.sort_key
         FROM matched m JOIN search_item i ON i.id = m.id
         WHERE {filter} ORDER BY {ORDER} LIMIT ?7 OFFSET ?8"
        ),
        params![
            bound,
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
            .then(|| cursor::token(server, params, &origin, offset + take, &last.3))
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

/// The matching CTE, its `?1` and the FTS expression documents are matched with: every
/// term (`MATCHES`), or the any-term fallback (`ANY_MATCHES`) when a query of several
/// terms has no row matching them all under the filters (`whole`, given the every-term
/// expression). Every page of one query decides alike, so its cursor's offset stays in
/// one list.
fn matching(
    terms: Vec<String>,
    whole: impl FnOnce(&str) -> Result<bool, Error>,
) -> Result<(&'static str, String, String), Error> {
    let every = terms.join(" ");
    if terms.len() < 2 || whole(&every)? {
        return Ok((MATCHES, every.clone(), every));
    }
    // A repeated word counts once.
    // simplify: one FTS scan per distinct term; ten one-letter terms take ~2 s on the
    // 0.84 catalog. Cap or prune short terms if real queries come near that.
    let mut terms = terms;
    let mut seen = std::collections::HashSet::new();
    terms.retain(|t| seen.insert(t.clone()));
    let json = serde_json::to_string(&terms).expect("terms serialize");
    Ok((ANY_MATCHES, json, terms.join(" OR ")))
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
    let entity = || entity.ok_or_else(|| Error::internal(format!("search row {id} has no entity")));
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
        other => {
            return Err(Error::internal(format!(
                "search row {id} has kind {other:?}"
            )));
        }
    })
}

/// A code row's pair and the count of referenced value sets that hold it.
fn code_hit(conn: &Connection, id: i64) -> rusqlite::Result<SearchHit> {
    let (code, label, value_set_count): (String, Option<String>, i64) = conn.query_row(
        "SELECT c.body, l.body,
                (SELECT COUNT(*) FROM search_code_valueset v WHERE v.item_id = i.id)
         FROM search_item i
           JOIN text c ON c.id = i.code_text
           LEFT JOIN text l ON l.id = i.label_text
         WHERE i.id = ?",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
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
