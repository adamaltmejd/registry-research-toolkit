//! Slice 3a's operations: `search`, `search_facets` and `context`.

use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::search::{RegisterFacets, SearchPage, TYPES, facets, search};
use super::{Cache, Download, Operation, Param, Params, Raw, Server, Steward, Type, component};
use crate::held::{self, Narrow};
use crate::{Code, Error, Scope};

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "search",
        paths: &["/api/search"],
        tool: Some("search"),
        description: "Search the catalog's registers, variables, classifications and codes \
            in one ranked list. `q` is free text; `type` keeps one kind of hit; `register` \
            (a FQID or a register name, case-insensitive; a slug only where it is also the \
            name) and `period` (2019, 2015..2019, LA2019, 2019-03) filter; pass \
            `next_cursor` back as `cursor` for the next page.",
        params: &[
            Param::required("q", Type::String),
            Param::optional("type", Type::Enum(TYPES)),
            Param::optional("register", Type::Ref),
            Param::optional("period", Type::Period),
            Param::optional("scope", Type::Scope),
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
        description: "Per register, how many of its variables match `q` under `search`'s \
            variable matching, every match counted (no paging) and before concept groups \
            fold; registers without a match are absent. With `register` (a FQID or a \
            register name, case-insensitive), only that register, with its matched \
            variables' FQIDs.",
        params: &[
            Param::required("q", Type::String),
            Param::optional("register", Type::Ref),
            Param::optional("scope", Type::Scope),
        ],
        cache: Cache::Minute,
        run: facets,
        result: component::<RegisterFacets>,
    },
    Operation {
        name: "context",
        paths: &["/api/context"],
        tool: None,
        description: "The catalog's branding, identity and headline counts, for the SPA.",
        params: &[Param::optional("scope", Type::Scope)],
        cache: Cache::Revalidate,
        run: context,
        result: component::<Context>,
    },
];

pub const DOWNLOADS: &[Download] = &[Download {
    path: "/api/steward/logo.svg",
    description: "The steward's logo: the compact top bar's mark and the SPA's favicon.",
    media_type: "image/svg+xml",
    params: &[Param::optional("scope", Type::Scope)],
    cache: Cache::Day,
    run: logo,
}];

/// `shape.Context`: branding, artifact identity and headline counts for the SPA.
#[derive(Serialize, ToSchema)]
pub struct Context {
    steward: Steward,
    schema_version: String,
    import_date: String,
    period_span: Option<PeriodSpan>,
    reg_meta_version: String,
    sizes: Sizes,
}

/// The catalog's first and last year: a steward's held periods, else the reference
/// catalog's delivered windows. It bounds the SPA's study window.
#[derive(Serialize, ToSchema)]
pub struct PeriodSpan {
    from: i64,
    to: i64,
}

/// Browse-addressable (slugged) providers, registers and variables in the scope.
#[derive(Serialize, ToSchema)]
pub struct Sizes {
    providers: i64,
    registers: i64,
    variables: i64,
}

/// The steward's `logo.svg` bytes. Opened directly, an SVG is a document that can
/// run script, so it is served sandboxed.
fn logo(server: &Server, _: Scope, _: &Params) -> Result<Raw, Error> {
    let steward = server.steward.as_ref().ok_or_else(|| {
        Error::new(
            Code::NotFound,
            "This server has no steward, so no logo.".to_owned(),
            vec!["logo".into()],
        )
    })?;
    Ok(Raw {
        bytes: steward.logo.clone(),
        headers: vec![
            ("x-content-type-options", "nosniff".to_owned()),
            (
                "content-security-policy",
                "default-src 'none'; style-src 'unsafe-inline'; sandbox".to_owned(),
            ),
        ],
    })
}

fn context(server: &Server, scope: Scope, _: &Params) -> Result<Value, Error> {
    let catalog = &server.catalog;
    let conn = catalog.connect()?;
    let period_span = period_span(&conn, catalog.is_steward(), catalog.manifest("import_date"))?;
    let count = |sql: String| conn.query_row(&sql, [], |row| row.get::<_, i64>(0));
    let sizes = Sizes {
        providers: count(format!(
            "SELECT COUNT(*) FROM provider p WHERE {}",
            held::provider(scope, "p.provider_id")
        ))?,
        registers: count(format!(
            "SELECT COUNT(*) FROM register r WHERE slug IS NOT NULL AND {}",
            held::register(scope, "r.register_id")
        ))?,
        variables: count(format!(
            "SELECT COUNT(*) FROM variable v JOIN register r ON v.register_id = r.register_id \
             WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL AND {}",
            held::variable(scope, "v.variable_id", Narrow::default())
        ))?,
    };
    let context = Context {
        // `serve` always loads branding at startup and `context` has no MCP tool, so
        // only an `mcp` process lacks it, and that process never routes here.
        steward: server.steward.clone().ok_or_else(|| {
            Error::new(
                Code::InternalError,
                "No steward branding is loaded.",
                vec![],
            )
        })?,
        schema_version: catalog.manifest("schema_version").to_owned(),
        import_date: catalog.manifest("import_date").to_owned(),
        period_span,
        reg_meta_version: server.version.to_owned(),
        sizes,
    };
    Ok(serde_json::to_value(context).expect("Context serializes"))
}

/// The catalog's year span, clipped to the import year. A steward's is the years of
/// its admitted physical tables (never semantic validity): today's
/// `catalog_period_span`. A reference catalog's is the years of its delivered
/// windows (`delivery_window`, what browse draws coverage from); an open-ended
/// window (`9999-12-31`) reaches the import year through the clip.
fn period_span(
    conn: &Connection,
    steward: bool,
    import_date: &str,
) -> Result<Option<PeriodSpan>, Error> {
    let sql = if steward {
        "SELECT MIN(hp.lo), MAX(hp.hi) FROM holding_period hp \
         JOIN holding_table ht USING(table_id) \
         WHERE ht.scope != 'unknown' AND EXISTS (SELECT 1 FROM holding_column hc \
         JOIN holding_mapping hm USING(column_id) WHERE hc.table_id = ht.table_id)"
    } else {
        "SELECT MIN(dw.valid_from), MAX(dw.valid_to) FROM delivery_window dw \
         JOIN browse_delivery bd USING(browse_delivery_id) WHERE bd.scope = 'reference'"
    };
    let (lo, hi): (Option<String>, Option<String>) =
        conn.query_row(sql, [], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let (Some(lo), Some(hi)) = (lo, hi) else {
        return Ok(None);
    };
    let year = |date: &str| {
        date.get(..4)
            .and_then(|y| y.parse::<i64>().ok())
            .ok_or_else(|| {
                Error::new(
                    Code::InternalError,
                    format!("Not an ISO date: {date:?}"),
                    vec![],
                )
            })
    };
    let (from, to) = (year(&lo)?, year(&hi)?.min(year(import_date)?));
    Ok((from <= to).then_some(PeriodSpan { from, to }))
}
