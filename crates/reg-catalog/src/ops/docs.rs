//! `docs_search`, `docs_get`, `docs_related` and the related-document download over
//! the docs database: today's `reg_meta.doc_queries` and `reg_webapp/routes/docs.py`.

use reg_core::{fold_search, fts_match_query, normalized_search_query, py_strip};
use rusqlite::types::Value as Sql;
use rusqlite::{OptionalExtension, Row, params_from_iter};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use utoipa::openapi::{ArrayBuilder, RefOr, Schema};

use super::{Components, Params, Raw, Server, component, cursor, refs};
use crate::{Code, Docs, Error, Scope, hex};

/// The SPA's preview of a document (today's `_EXCERPT_CHARS`).
const EXCERPT_CHARS: usize = 500;
/// The longest query, as `search`'s.
const MAX_QUERY_CHARS: usize = 200;

/// One documentation hit: today's `DocResult` without the webapp's per-route `fuzzy`
/// flag.
#[derive(Serialize, ToSchema)]
pub struct DocResult {
    register: String,
    variable: Option<String>,
    filename: String,
    display_name: String,
    tags: Vec<String>,
    /// About 24 words of the plain text around the match, matched words in `**`; none
    /// without `q`.
    snippet: Option<String>,
    source: Option<String>,
    source_url: Option<String>,
    source_title: Option<String>,
}

/// `docs_search`'s page: its hits, the total they page through, and whether
/// `register` has any documentation (false without `register`).
#[derive(Serialize, ToSchema)]
pub struct DocPage {
    items: Vec<DocResult>,
    next_cursor: Option<String>,
    total: usize,
    register_ingested: bool,
}

/// Today's `doc_search` (with `q`, best match first) and `doc_list` (without, by
/// filename), optionally of one register, paged by cursor. The docs index holds
/// `fold_search` text, so the query is folded the same way; a query without a
/// searchable token finds nothing.
pub fn search(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let q = params.get("q").copied();
    if q.is_some_and(|q| q.contains('\0') || q.chars().count() > MAX_QUERY_CHARS) {
        return Err(Error::invalid_parameter("q"));
    }
    let limit = super::limit(params)?;
    let catalog = &server.catalog;
    let register = params
        .get("register")
        .map(|r| refs::register(&catalog.connect()?, scope, r))
        .transpose()?;
    let conn = docs(server)?.connect()?;
    // The cursor binds what selects and orders the rows, never `limit`.
    let context = json!([
        q.map(normalized_search_query),
        params.get("register"),
        scope
    ]);
    let context = hex(&Sha256::digest(context.to_string().as_bytes()));
    let after = params
        .get("cursor")
        .map(|c| cursor::decode(c, catalog.generation(), &context, usize::MAX))
        .transpose()?;

    let mut filters = Vec::new();
    let mut args: Vec<Sql> = Vec::new();
    let (from, snippet, order) = match q {
        Some(q) => {
            let Some(fts) = fts_match_query(&fold_search(q)) else {
                return Ok(page(Vec::new(), None, 0, false));
            };
            filters.push("doc_fts MATCH ?");
            args.push(fts.into());
            (
                "doc_fts JOIN doc d ON d.doc_id = doc_fts.rowid",
                "snippet(doc_fts, 2, '**', '**', '…', 24)",
                "doc_fts.rank, d.doc_id",
            )
        }
        None => ("doc d", "NULL", "d.filename"),
    };
    let mut register_ingested = false;
    if let Some(register) = register {
        register_ingested = conn
            .query_row(
                "SELECT 1 FROM doc WHERE register = ? LIMIT 1",
                [&register.slug],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        filters.push("d.register = ?");
        args.push(register.slug.into());
    }
    let filter = if filters.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", filters.join(" AND "))
    };

    // simplify: the whole match list is read to count and to check the cursor's
    // position; the docs database holds about 500 documents.
    let order_sql = format!("SELECT d.filename FROM {from} {filter} ORDER BY {order}");
    let filenames: Vec<String> = conn
        .prepare(&order_sql)?
        .query_map(params_from_iter(&args), |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let offset = match &after {
        None => 0,
        Some((offset, after)) => {
            if *offset > filenames.len() || filenames[offset - 1] != *after {
                return Err(cursor::invalid(
                    "Cursor no longer matches the result ordering.",
                ));
            }
            *offset
        }
    };
    let end = (offset + limit).min(filenames.len());
    let next_cursor = (end < filenames.len())
        .then(|| cursor::encode(catalog.generation(), &context, end, &filenames[end - 1]));
    let page_sql = format!(
        "SELECT d.register, d.variable, d.filename, d.display_name, d.tags, d.source, \
         d.source_url, d.source_title, {snippet} FROM {from} {filter} ORDER BY {order} \
         LIMIT ? OFFSET ?"
    );
    args.push(i64::try_from(end - offset).expect("a page fits i64").into());
    args.push(i64::try_from(offset).expect("an offset fits i64").into());
    let items = conn
        .prepare(&page_sql)?
        .query_map(params_from_iter(&args), |row| {
            Ok(DocResult {
                register: row.get(0)?,
                variable: row.get(1)?,
                filename: row.get(2)?,
                display_name: row.get(3)?,
                tags: tags(row, 4)?,
                source: row.get(5)?,
                source_url: row.get(6)?,
                source_title: row.get(7)?,
                snippet: row.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(page(items, next_cursor, filenames.len(), register_ingested))
}

fn page(
    items: Vec<DocResult>,
    next_cursor: Option<String>,
    total: usize,
    register_ingested: bool,
) -> Value {
    serde_json::to_value(DocPage {
        items,
        next_cursor,
        total,
        register_ingested,
    })
    .expect("DocPage serializes")
}

/// A row's JSON `tags` array.
fn tags(row: &Row, at: usize) -> rusqlite::Result<Vec<String>> {
    let tags: String = row.get(at)?;
    serde_json::from_str(&tags).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(at, rusqlite::types::Type::Text, err.into())
    })
}

// Today's `DocDetail` without its response tag `kind`, plus `body` (today's `docs
// get`). A plain comment: doc comments become the published schema's descriptions.
/// A documentation entry: its metadata, a preview and the full markdown.
#[derive(Serialize, ToSchema)]
pub struct DocDetail {
    register: String,
    variable: Option<String>,
    filename: String,
    display_name: String,
    tags: Vec<String>,
    source: Option<String>,
    source_url: Option<String>,
    source_title: Option<String>,
    /// The plain text's first 500 characters, for the SPA.
    excerpt: Option<String>,
    /// The full markdown, for agents.
    body: String,
}

/// A related document's metadata; the download route serves its bytes.
#[derive(Serialize, ToSchema)]
pub struct RelatedDocument {
    title: String,
    filename: String,
    source_url: String,
    license: String,
    fetched: String,
    sha256: String,
    byte_size: i64,
}

/// `docs_related`'s result: a register's documents, unpaged.
pub fn related_schema(components: &mut Components) -> RefOr<Schema> {
    ArrayBuilder::new()
        .items(component::<RelatedDocument>(components))
        .into()
}

fn docs(server: &Server) -> Result<&Docs, Error> {
    server.docs.as_ref().ok_or_else(|| {
        Error::new(
            Code::DocsUnavailable,
            "This catalog has no documentation database.",
            vec![],
        )
    })
}

/// Today's `doc_get`: the doc whose variable is `identifier`, else the one whose
/// filename is `identifier` with or without `.md`, both ASCII case-insensitive.
pub fn get(server: &Server, _: Scope, params: &Params) -> Result<Value, Error> {
    let identifier = params["identifier"];
    let conn = docs(server)?.connect()?;
    let select = |filter: &str| {
        // `ORDER BY doc_id` keeps a case-insensitive tie deterministic; today's
        // `LIMIT 1` scans in the same order.
        let sql = format!(
            "SELECT register, variable, filename, display_name, tags, source, source_url, \
             source_title, body, body_clean FROM doc WHERE {filter} ORDER BY doc_id LIMIT 1"
        );
        conn.query_row(&sql, [identifier], detail).optional()
    };
    let found = match select("variable = ?1 COLLATE NOCASE")? {
        Some(found) => Some(found),
        None => select("filename = ?1 COLLATE NOCASE OR filename = ?1 || '.md' COLLATE NOCASE")?,
    };
    let detail = found.ok_or_else(|| {
        Error::new(
            Code::NotFound,
            format!("No documentation for {identifier:?}."),
            vec![identifier.into()],
        )
    })?;
    Ok(serde_json::to_value(detail).expect("DocDetail serializes"))
}

fn detail(row: &Row) -> rusqlite::Result<DocDetail> {
    Ok(DocDetail {
        register: row.get(0)?,
        variable: row.get(1)?,
        filename: row.get(2)?,
        display_name: row.get(3)?,
        tags: tags(row, 4)?,
        source: row.get(5)?,
        source_url: row.get(6)?,
        source_title: row.get(7)?,
        excerpt: excerpt(&row.get::<_, String>(9)?),
        body: row.get(8)?,
    })
}

/// Today's `_excerpt`: the stripped plain text, cut to its first 500 characters and
/// marked with an ellipsis when longer; none when empty.
fn excerpt(body_clean: &str) -> Option<String> {
    let text = py_strip(body_clean);
    if text.is_empty() {
        return None;
    }
    if text.chars().count() <= EXCERPT_CHARS {
        return Some(text.to_owned());
    }
    let cut: String = text.chars().take(EXCERPT_CHARS).collect();
    // The cut starts where the stripped text does, so stripping both of its ends is
    // today's `rstrip`.
    Some(format!("{}…", py_strip(&cut)))
}

/// A register's related documents, in curation order.
pub fn related(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let register = refs::register(&server.catalog.connect()?, scope, params["ref"])?;
    let conn = docs(server)?.connect()?;
    let mut stmt = conn.prepare(
        "SELECT title, filename, source_url, license, fetched, sha256, byte_size \
         FROM related_document WHERE register = ? ORDER BY id",
    )?;
    let documents: Vec<RelatedDocument> = stmt
        .query_map([register.slug], |row| {
            Ok(RelatedDocument {
                title: row.get(0)?,
                filename: row.get(1)?,
                source_url: row.get(2)?,
                license: row.get(3)?,
                fetched: row.get(4)?,
                sha256: row.get(5)?,
                byte_size: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(serde_json::to_value(documents).expect("RelatedDocument serializes"))
}

/// One related document's bytes, served inline.
pub fn file(server: &Server, scope: Scope, params: &Params) -> Result<Raw, Error> {
    let (reference, filename) = (params["ref"], params["filename"]);
    let register = refs::register(&server.catalog.connect()?, scope, reference)?;
    let conn = docs(server)?.connect()?;
    let found: Option<(String, Vec<u8>)> = conn
        .query_row(
            "SELECT filename, content FROM related_document WHERE register = ? AND filename = ?",
            (register.slug, filename),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (filename, bytes) = found.ok_or_else(|| {
        Error::new(
            Code::NotFound,
            format!("No related document {filename:?} for register {reference:?}."),
            vec![format!("{reference}/{filename}").into()],
        )
    })?;
    Ok(Raw {
        bytes,
        headers: vec![
            ("content-disposition", content_disposition(&filename)),
            ("x-content-type-options", "nosniff".to_owned()),
        ],
    })
}

/// Today's `_content_disposition`: inline, with a printable-ASCII fallback name and
/// the exact name as RFC 8187 UTF-8 percent-encoding. The docs build stores only
/// non-empty basenames, so the fallback is never empty.
fn content_disposition(filename: &str) -> String {
    let fallback: String = filename
        .chars()
        .map(|c| match c {
            ' '..='~' if c != '"' && c != '\\' => c,
            _ => '_',
        })
        .collect();
    // Python's `quote(filename, safe="")`: every byte but the unreserved ones.
    let encoded: String = filename
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect();
    format!("inline; filename=\"{fallback}\"; filename*=UTF-8''{encoded}")
}
