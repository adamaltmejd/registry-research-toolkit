//! `context`: the catalog's identity and counts, computed once when the catalog is
//! admitted and answered from memory.

use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::{Cache, Operation, Params, Period, Server, component};
use crate::{Catalog, Error};

pub const OPERATIONS: &[Operation] = &[Operation {
    name: "context",
    paths: &["/api/context"],
    tool: None,
    description: "The catalog's identity and headline counts, for the SPA.",
    params: &[],
    cache: Cache::Revalidate,
    run: context,
    result: component::<Context>,
}];

/// `shape.Context`.
#[derive(Serialize, ToSchema)]
pub struct Context {
    catalog_version: String,
    schema_version: String,
    /// The hull of every register; `null` for a catalog without registers.
    #[schema(required = true)]
    period_span: Option<Period>,
    /// The server's version, which seeds a new project's `reg_meta_version`.
    reg_meta_version: String,
    sizes: Sizes,
}

/// Every provider, register, table (implicit ones included) and variable.
#[derive(Serialize, ToSchema)]
pub struct Sizes {
    providers: i64,
    registers: i64,
    tables: i64,
    variables: i64,
}

impl Context {
    /// `catalog`'s context; `open` is its `open_start` and `open_end`.
    pub(crate) fn load(catalog: &Catalog, open: (i64, i64), version: &str) -> Result<Self, Error> {
        let conn = catalog.connect()?;
        let count = |table: &str| {
            conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get::<_, i64>(0)
            })
        };
        let sizes = Sizes {
            providers: count("provider")?,
            registers: count("register")?,
            tables: count("tbl")?,
            variables: count("variable")?,
        };
        let hull: (Option<i64>, Option<i64>) =
            conn.query_row("SELECT MIN(start), MAX(end) FROM register", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
        Ok(Self {
            catalog_version: catalog.meta("catalog_version").to_owned(),
            schema_version: catalog.meta("schema_version").to_owned(),
            period_span: match hull {
                (Some(start), Some(end)) => Some(Period::new(open, start, end)),
                _ => None,
            },
            reg_meta_version: version.to_owned(),
            sizes,
        })
    }
}

#[expect(clippy::unnecessary_wraps, reason = "an operation's `Run` signature")]
fn context(server: &Server, _: &Params) -> Result<Value, Error> {
    Ok(serde_json::to_value(&server.context).expect("Context serializes"))
}
