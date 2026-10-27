//! `docs_search`, `docs_get` and `docs_related` over the catalog's document tables
//! (`doc`, `doc_section`, `doc_fts`, `doc_link`), and the PDF download.

use rusqlite::{Connection, OptionalExtension, params_from_iter, types::Value as Sql};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;
use utoipa::openapi::{ArrayBuilder, RefOr, Schema};

use super::refs::{self, Entity};
use super::{
    Cache, Components, Download, Operation, Param, Params, Raw, Server, Type, component, cursor,
    fts, hash, limit,
};
use crate::{Code, Error};

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "docs_search",
        paths: &["/api/docs/search"],
        tool: Some("docs"),
        description: "Search the documents the catalog holds, by the words of their sections; \
                      without `q`, list them by title. `register` narrows to the documents \
                      linked to a register, its tables or its variables.",
        params: &[
            Param::optional("q", Type::String),
            Param::optional("register", Type::Ref),
            Param::optional("limit", Type::Limit),
            Param::optional("cursor", Type::Cursor),
        ],
        cache: Cache::Day,
        run: search,
        result: component::<DocPage>,
    },
    Operation {
        name: "docs_get",
        paths: &["/api/docs/doc/{doc}"],
        tool: Some("docs"),
        description: "A document by its content hash: its metadata, the entities it is about, \
                      a 500-character excerpt and, for a parsed document, its full text.",
        params: &[Param::required("doc", Type::Hash)],
        cache: Cache::Day,
        run: get,
        result: component::<DocDetail>,
    },
    Operation {
        name: "docs_related",
        paths: &["/api/docs/related/{ref}"],
        tool: Some("docs"),
        description: "The documents linked to a register, table, variable or classification \
                      family, one row per link; a register also takes the documents of its \
                      tables and variables.",
        params: &[Param::required("ref", Type::Ref)],
        cache: Cache::Day,
        run: related,
        result: related_schema,
    },
];

pub const DOWNLOADS: &[Download] = &[Download {
    path: "/api/docs/file/{doc}",
    description: "The bytes of a PDF document by its content hash, shown inline.",
    media_type: "application/pdf",
    params: &[Param::required("doc", Type::Hash)],
    cache: Cache::Day,
    run: file,
}];

/// The longest `excerpt`, in characters.
const EXCERPT_CHARS: usize = 500;
/// `docs_search` is ranked, so it is offset-paged; the deepest page it reaches.
const SEARCH_DEPTH: usize = 1000;

/// `shape.DocHit`.
#[derive(Serialize, ToSchema)]
pub struct DocHit {
    doc: String,
    title: String,
    kind: String,
    #[schema(required = true)]
    heading: Option<String>,
    #[schema(required = true)]
    page: Option<i64>,
    #[schema(required = true)]
    snippet: Option<String>,
}

/// `docs_search`'s `Page<DocHit>` with its `total` and `register_ingested`.
#[derive(Serialize, ToSchema)]
pub struct DocPage {
    items: Vec<DocHit>,
    #[schema(required = true)]
    next_cursor: Option<String>,
    total: i64,
    /// Whether `register` has any linked document; `null` without `register`.
    #[schema(required = true)]
    register_ingested: Option<bool>,
}

/// One entity a document is about (`shape.DocDetail.subjects`).
#[derive(Serialize, ToSchema)]
pub struct DocSubject {
    kind: String,
    fqid: String,
    page: i64,
}

/// `shape.DocDetail`.
#[derive(Serialize, ToSchema)]
pub struct DocDetail {
    doc: String,
    title: String,
    kind: String,
    licence: String,
    #[schema(required = true)]
    url: Option<String>,
    bytes: i64,
    subjects: Vec<DocSubject>,
    excerpt: String,
    /// The sections joined by a blank line; `null` for a pdf.
    #[schema(required = true)]
    body: Option<String>,
}

/// `shape.RelatedDocument`.
#[derive(Serialize, ToSchema)]
pub struct RelatedDocument {
    doc: String,
    title: String,
    kind: String,
    licence: String,
    #[schema(required = true)]
    url: Option<String>,
    bytes: i64,
    /// 0 = the whole document.
    page: i64,
}

fn related_schema(components: &mut Components) -> RefOr<Schema> {
    let item = component::<RelatedDocument>(components);
    ArrayBuilder::new().items(item).into()
}

/// `docs_unavailable` unless the catalog holds a document. A per-request query: a
/// catalog is never changed under a running server.
fn require_docs(conn: &Connection) -> Result<(), Error> {
    let any: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM doc)", [], |row| row.get(0))?;
    if any {
        Ok(())
    } else {
        Err(Error::new(
            Code::DocsUnavailable,
            "This catalog has no documents.",
            vec![],
        ))
    }
}

/// The `doc_link` rows `l` of an entity, as a SQL condition on `l` over `?1`, the
/// entity's row id: its own, and for a register those of its tables (the implicit one
/// included) and variables.
fn links_of(kind: &str) -> &'static str {
    match kind {
        "register" => {
            "(l.subject_kind = 'register' AND l.subject_id = ?1)
             OR (l.subject_kind = 'table'
                 AND l.subject_id IN (SELECT id FROM tbl WHERE register_id = ?1))
             OR (l.subject_kind = 'variable'
                 AND l.subject_id IN (SELECT id FROM variable WHERE register_id = ?1))"
        }
        "table" => "l.subject_kind = 'table' AND l.subject_id = ?1",
        "variable" => "l.subject_kind = 'variable' AND l.subject_id = ?1",
        _ => "l.subject_kind = 'class_family' AND l.subject_id = ?1",
    }
}

fn invalid_ref(value: &str, entity: &Entity) -> Error {
    Error::new(
        Code::InvalidRef,
        format!(
            "{value:?} is a {}, which this operation does not take.",
            entity.kind
        ),
        vec![value.into()],
    )
}

fn search(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    require_docs(&conn)?;
    let q = params.get("q").copied().filter(|q| !q.is_empty());
    let expression = q.map(fts::query).transpose()?.flatten();
    let register = match params.get("register") {
        Some(value) => {
            let entity = refs::resolve(&conn, value)?;
            if entity.kind != "register" {
                return Err(invalid_ref(value, &entity));
            }
            Some(entity.id)
        }
        None => None,
    };
    let limit = limit(params)?;
    let context = cursor::context(params);
    let offset = match params.get("cursor") {
        Some(cursor) => {
            cursor::decode(cursor, server.catalog.generation(), &context, SEARCH_DEPTH)?.0
        }
        None => 0,
    };

    // A page never passes the SEARCH_DEPTH-th hit; one extra row tells whether more follow.
    let take = limit.min(SEARCH_DEPTH - offset);
    let (mut items, total) = hits(
        &conn,
        register,
        (q.is_some(), expression.as_deref()),
        take + 1,
        offset,
    )?;
    let more = items.len() > take;
    items.truncate(take);
    let next_cursor = (more && offset + take < SEARCH_DEPTH).then(|| {
        cursor::encode(
            server.catalog.generation(),
            &context,
            offset + take,
            items.last().map_or("", |hit| hit.doc.as_str()),
        )
    });
    let register_ingested = register
        .map(|id| {
            conn.query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM doc_link l WHERE {})",
                    links_of("register")
                ),
                [id],
                |row| row.get::<_, bool>(0),
            )
        })
        .transpose()?;
    Ok(serde_json::to_value(DocPage {
        items,
        next_cursor,
        total,
        register_ingested,
    })
    .expect("DocPage serializes"))
}

/// `rows` hits from `offset`, and the total, of the documents of `register` (a row
/// id) that match `query`: (whether `q` was given, its FTS5 expression). A `q` with no
/// letter or digit has no expression and matches nothing; without `q`, a hit is a
/// document.
fn hits(
    conn: &Connection,
    register: Option<i64>,
    query: (bool, Option<&str>),
    rows: usize,
    offset: usize,
) -> Result<(Vec<DocHit>, i64), Error> {
    let mut args: Vec<Sql> = Vec::new();
    let mut conditions: Vec<String> = Vec::new();
    // `links_of` reads the register's id as `?1`: bind it first, then the match.
    if let Some(id) = register {
        args.push(id.into());
        conditions.push(format!(
            "d.id IN (SELECT l.doc_id FROM doc_link l WHERE {})",
            links_of("register")
        ));
    }
    // The columns of a hit, the rows they come from and their order.
    let (columns, from, order) = match query {
        (_, Some(expression)) => {
            args.push(expression.to_owned().into());
            conditions.push(format!("doc_fts MATCH ?{}", args.len()));
            (
                "d.hash, t.body, d.kind, s.heading, s.page, snippet(doc_fts, 1, '', '', '…', 16)",
                "FROM doc_fts
                   JOIN doc_section s ON s.id = doc_fts.rowid
                   JOIN doc d ON d.id = s.doc_id
                   JOIN text t ON t.id = d.title_text",
                "ORDER BY bm25(doc_fts), d.hash, s.ord",
            )
        }
        (true, None) => return Ok((Vec::new(), 0)),
        (false, None) => (
            "d.hash, t.body, d.kind, NULL, NULL, NULL",
            "FROM doc d JOIN text t ON t.id = d.title_text",
            "ORDER BY t.body, d.hash",
        ),
    };
    let filter = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    let total: i64 = conn.query_row(
        &format!("SELECT COUNT(*) {from} {filter}"),
        params_from_iter(args.iter()),
        |row| row.get(0),
    )?;
    args.push(i64::try_from(rows).expect("a limit fits").into());
    args.push(i64::try_from(offset).expect("an offset fits").into());
    let items = conn
        .prepare(&format!(
            "SELECT {columns} {from} {filter} {order} LIMIT ?{} OFFSET ?{}",
            args.len() - 1,
            args.len()
        ))?
        .query_map(params_from_iter(args.iter()), |row| {
            Ok(DocHit {
                doc: row.get(0)?,
                title: row.get(1)?,
                kind: row.get(2)?,
                heading: row.get(3)?,
                page: row.get(4)?,
                snippet: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((items, total))
}

fn not_found(doc: &str) -> Error {
    Error::new(
        Code::NotFound,
        format!("No document has the hash {doc}."),
        vec![doc.into()],
    )
}

fn get(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    require_docs(&conn)?;
    let doc = hash(params, "doc")?.expect("a required parameter");
    let (id, kind, title, licence, url, bytes) = conn
        .query_row(
            "SELECT d.id, d.kind, t.body, d.licence, d.url, d.bytes
             FROM doc d JOIN text t ON t.id = d.title_text WHERE d.hash = ?",
            [doc],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| not_found(doc))?;
    // A value-set subject is addressed by its hash.
    let subjects = conn
        .prepare(
            "SELECT kind, fqid, page FROM (
               SELECT l.subject_kind AS kind, l.page AS page,
                      CASE l.subject_kind
                        WHEN 'register' THEN (SELECT pid FROM register WHERE id = l.subject_id)
                        WHEN 'table' THEN (SELECT pid FROM tbl WHERE id = l.subject_id)
                        WHEN 'variable' THEN (SELECT pid FROM variable WHERE id = l.subject_id)
                        WHEN 'class_family' THEN (SELECT pid FROM class_family WHERE id = l.subject_id)
                        ELSE (SELECT hash FROM valueset WHERE id = l.subject_id)
                      END AS fqid
               FROM doc_link l WHERE l.doc_id = ?)
             ORDER BY kind, fqid, page",
        )?
        .query_map([id], |row| {
            Ok(DocSubject {
                kind: row.get(0)?,
                fqid: row.get(1)?,
                page: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let body = (kind == "parsed")
        .then(|| {
            conn.prepare("SELECT body FROM doc_section WHERE doc_id = ? ORDER BY ord")?
                .query_map([id], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map(|sections| sections.join("\n\n"))
        })
        .transpose()?;
    let excerpt = body.as_deref().map_or_else(String::new, |body| {
        body.chars().take(EXCERPT_CHARS).collect()
    });
    Ok(serde_json::to_value(DocDetail {
        doc: doc.to_owned(),
        title,
        kind,
        licence,
        url,
        bytes,
        subjects,
        excerpt,
        body,
    })
    .expect("DocDetail serializes"))
}

fn related(server: &Server, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    require_docs(&conn)?;
    let value = params["ref"];
    let entity = refs::resolve(&conn, value)?;
    if !matches!(
        entity.kind.as_str(),
        "register" | "table" | "variable" | "class_family"
    ) {
        return Err(invalid_ref(value, &entity));
    }
    // DISTINCT: a register's own link and its table's link to one page of one
    // document are one row to the reader.
    let rows = conn
        .prepare(&format!(
            "SELECT DISTINCT d.hash, t.body, d.kind, d.licence, d.url, d.bytes, l.page
             FROM doc_link l
               JOIN doc d ON d.id = l.doc_id
               JOIN text t ON t.id = d.title_text
             WHERE {}
             ORDER BY t.body, d.hash, l.page",
            links_of(&entity.kind)
        ))?
        .query_map([entity.id], |row| {
            Ok(RelatedDocument {
                doc: row.get(0)?,
                title: row.get(1)?,
                kind: row.get(2)?,
                licence: row.get(3)?,
                url: row.get(4)?,
                bytes: row.get(5)?,
                page: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(serde_json::to_value(rows).expect("RelatedDocument serializes"))
}

/// `docs_get`'s file: the stored bytes of a `pdf` document, shown inline.
fn file(server: &Server, params: &Params) -> Result<Raw, Error> {
    let conn = server.catalog.connect()?;
    require_docs(&conn)?;
    let doc = hash(params, "doc")?.expect("a required parameter");
    let bytes: Option<Vec<u8>> = conn
        .query_row(
            "SELECT content FROM doc WHERE hash = ? AND kind = 'pdf'",
            [doc],
            |row| row.get(0),
        )
        .optional()?;
    bytes
        .map(|bytes| Raw {
            bytes,
            headers: vec![("content-disposition", "inline".into())],
        })
        .ok_or_else(|| not_found(doc))
}
