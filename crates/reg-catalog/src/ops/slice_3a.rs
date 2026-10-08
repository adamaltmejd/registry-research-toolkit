//! Slice 3a's operations: `search` and `context`.

use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::search::{SearchPage, TYPES, search};
use super::{Cache, Operation, Param, Params, Server, Steward, Type, component};
use crate::held::{self, Narrow};
use crate::{Code, Error, Scope};

pub const OPERATIONS: &[Operation] = &[
    Operation {
        name: "search",
        path: "/api/search",
        tool: Some("search"),
        description: "Search the catalog's registers, variables, classifications and codes \
            in one ranked list. `q` is free text; `type` keeps one kind of hit; `register` \
            (a FQID or a bare name) and `period` (2019, 2015..2019, LA2019, 2019-03) \
            filter; pass `next_cursor` back as `cursor` for the next page.",
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
        name: "context",
        path: "/api/context",
        tool: None,
        description: "The catalog's branding, identity and headline counts, for the SPA.",
        params: &[Param::optional("scope", Type::Scope)],
        cache: Cache::Revalidate,
        run: context,
        result: component::<Context>,
    },
];

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

/// The first and last year of the steward's held periods.
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

fn context(server: &Server, scope: Scope, _: &Params) -> Result<Value, Error> {
    let catalog = &server.catalog;
    let conn = catalog.connect()?;
    let period_span = if catalog.is_steward() {
        period_span(&conn, catalog.manifest("import_date"))?
    } else {
        None
    };
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

/// The years of the admitted physical tables (never semantic validity), clipped to
/// the import year: today's `catalog_period_span`.
fn period_span(conn: &Connection, import_date: &str) -> Result<Option<PeriodSpan>, Error> {
    let (lo, hi): (Option<String>, Option<String>) = conn.query_row(
        "SELECT MIN(hp.lo), MAX(hp.hi) FROM holding_period hp \
         JOIN holding_table ht USING(table_id) \
         WHERE ht.scope != 'unknown' AND EXISTS (SELECT 1 FROM holding_column hc \
         JOIN holding_mapping hm USING(column_id) WHERE hc.table_id = ht.table_id)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
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
