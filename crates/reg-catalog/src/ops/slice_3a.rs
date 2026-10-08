//! Slice 3a's operations: `context` (and, from 3a.5, `search`).

use rusqlite::Connection;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::{Operation, Param, Params, Server, Steward, component};
use crate::{Code, Error, Scope};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "context",
    path: "/api/context",
    params: &[Param {
        name: "scope",
        required: false,
    }],
    run: context,
    result: component::<Context>,
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
            in_scope(scope, Held::Provider)
        ))?,
        registers: count(format!(
            "SELECT COUNT(*) FROM register r WHERE slug IS NOT NULL AND {}",
            in_scope(scope, Held::Register)
        ))?,
        variables: count(format!(
            "SELECT COUNT(*) FROM variable v JOIN register r ON v.register_id = r.register_id \
             WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL AND {}",
            in_scope(scope, Held::Variable)
        ))?,
    };
    let context = Context {
        steward: server.steward.clone(),
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

enum Held {
    Provider,
    Register,
    Variable,
}

/// Today's `scope_predicate` for providers (`p`), registers (`r`) and variables
/// (`v`): reference admits everything; holdings admits what an authored mapping of
/// a known-scope table holds.
fn in_scope(scope: Scope, kind: Held) -> String {
    let held = |join: &str, anchor: &str| {
        format!(
            "EXISTS (SELECT 1 FROM {join} JOIN holding_column hc USING(column_id) \
             JOIN holding_table ht USING(table_id) WHERE {anchor} AND ht.scope != 'unknown')"
        )
    };
    let register = |id: &str| {
        held(
            "variable hv JOIN holding_mapping hm ON hm.variable_id = hv.variable_id",
            &format!("hv.register_id = {id}"),
        )
    };
    match (scope, kind) {
        (Scope::Reference, _) => "1".to_owned(),
        (Scope::Holdings, Held::Variable) => {
            held("holding_mapping hm", "hm.variable_id = v.variable_id")
        }
        (Scope::Holdings, Held::Register) => register("r.register_id"),
        (Scope::Holdings, Held::Provider) => format!(
            "EXISTS (SELECT 1 FROM register hr WHERE hr.provider_id = p.provider_id AND {})",
            register("hr.register_id")
        ),
    }
}
